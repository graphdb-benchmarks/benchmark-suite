"""Load the normalized snapshot through each published engine's bulk API."""

from collections import defaultdict
from contextlib import ExitStack, closing, nullcontext
import csv
from itertools import batched
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

from .datasets import LABELS, create_schema, finbench_entities, id_indexes, props, rows, snb_entities
from .engines import TYPED

LOAD_METHODS = {
    **dict.fromkeys(TYPED, "copy_parquet"),
    "velr": "velr-admin",
    "relate": "batched_cypher",
    "grafeo": "typed_csv_and_native_node_batches",
    "arcadedb": "graph_batch",
    "graphdblite": "native_batches_and_multilabel_cypher",
}
GRAFEO_TYPES = {"INT64": "int64", "STRING": "string", "DOUBLE": "float64", "BOOL": "bool"}


def node_groups(entity, subtypes, size):
    for batch in batched(rows(entity), size):
        groups = defaultdict(list)
        for row in batch:
            subtype = row.pop("_label", None)
            if subtype is None:
                label = entity.label
            else:
                label = subtype.title()
                if label not in LABELS[entity.label]:
                    raise ValueError(f"Unexpected {entity.label} subtype {subtype}")
                subtypes[entity.label][row["id"]] = label
            groups[label].append(row)
        yield from groups.items()


def edge_groups(entity, subtypes, size):
    for batch in batched(rows(entity), size):
        groups = defaultdict(list)
        for row in batch:
            src, dst = entity.spaces
            if src in subtypes:
                src = subtypes[src][row["_src"]]
            if dst in subtypes:
                dst = subtypes[dst][row["_dst"]]
            if (src, dst) not in entity.pairs:
                raise ValueError(f"Unexpected endpoints {(src, dst)} for {entity.label}")
            groups[(src, dst)].append(row)
        yield from groups.items()


def node_labels(entity, label, suite):
    if suite == "snb" and label in {"Post", "Comment"}:
        return [label, "Message"]
    if label != entity.label:
        return [label, entity.label]
    return [label]


def non_null(row):
    return {key: value for key, value in row.items() if value is not None}


def mapped_ids(connection, label, external_ids):
    keys = set(external_ids)
    found = {}
    limit = connection.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) - 1
    for group in batched(keys, limit):
        placeholders = ",".join("?" for _ in group)
        found.update(connection.execute(
            f"SELECT external, internal FROM ids WHERE label = ? AND external IN ({placeholders})", (label, *group),
        ))
    if missing := keys - found.keys():
        raise ValueError(f"Missing {label} endpoint {min(missing)}")
    return found


def insert_node_batch(db, arcade, mapping, labels, group, columns):
    if arcade is not None:
        internal_ids = arcade.create_vertices(labels[0], [non_null(row) for row in group])
    elif db.engine == "graphdblite" and len(labels) == 1:
        with db.transaction():
            internal_ids = db.tx.batch_create_nodes(labels[0], [non_null(row) for row in group])
    else:
        query = f"UNWIND $rows AS r CREATE (n:{':'.join(labels)} {{{props(columns)}}})"
        if mapping is None:
            db.execute(query, {"rows": group}, write=True)
            return
        # GraphDBLite's native batch takes a single label. CREATE attaches both labels together.
        inserted = db.execute(query + " RETURN n.id AS external, id(n) AS internal", {"rows": group}, write=True)
        internal_by_external = dict(inserted)
        internal_ids = [internal_by_external[row["id"]] for row in group]
    mapping.executemany(
        "INSERT INTO ids VALUES (?, ?, ?)",
        ((labels[0], row["id"], internal) for row, internal in zip(group, internal_ids, strict=True)),
    )


def insert_edge_batch(db, arcade, mapping, entity, endpoints, group, columns):
    src, dst = endpoints
    if mapping is None:
        query = (
            f"UNWIND $rows AS r MATCH (a:{src} {{id: r._src}}) "
            f"MATCH (b:{dst} {{id: r._dst}}) CREATE (a)-[:{entity.label} {{{props(columns)}}}]->(b)"
        )
        db.execute(query, {"rows": group}, write=True)
        return
    source = mapped_ids(mapping, src, (row["_src"] for row in group))
    target = mapped_ids(mapping, dst, (row["_dst"] for row in group))
    sources = [source[row["_src"]] for row in group]
    targets = [target[row["_dst"]] for row in group]
    properties = [non_null({key: row[key] for key in columns}) for row in group] if columns else None
    if arcade is not None:
        arcade.new_edges(sources, entity.label, targets, properties)
    else:
        payload = list(zip(sources, targets, properties)) if properties is not None else list(zip(sources, targets))
        with db.transaction():
            db.tx.batch_create_edges(entity.label, payload)


def import_batches(db, suite, nodes, edges, tables, subtypes, counts, size):
    uses_mapping = db.engine in {"arcadedb", "graphdblite"}
    with ExitStack() as stack:
        mapping = None
        if uses_mapping:
            mapping = stack.enter_context(closing(sqlite3.connect("")))
            mapping.execute(
                "CREATE TABLE ids(label TEXT, external INTEGER, internal NOT NULL, "
                "PRIMARY KEY(label, external)) WITHOUT ROWID"
            )
        arcade = stack.enter_context(db.db.graph_batch(use_wal=True) if db.engine == "arcadedb" else nullcontext())
        for entity in nodes:
            columns = [key for key, _ in entity.columns if key != "_label"]
            for label, group in node_groups(entity, subtypes, size):
                labels = [label] if arcade is not None else node_labels(entity, label, suite)
                insert_node_batch(db, arcade, mapping, labels, group, columns)
                counts["node", label] += len(group)
        id_indexes(db, tables)
        for entity in edges:
            columns = [key for key, _ in entity.columns if not key.startswith("_")]
            for endpoints, group in edge_groups(entity, subtypes, size):
                insert_edge_batch(db, arcade, mapping, entity, endpoints, group, columns)
                counts["edge", entity.label] += len(group)


def stage_parquet(folder, groups, columns):
    import pyarrow as pa
    import pyarrow.parquet as pq

    types = {"INT64": pa.int64(), "DOUBLE": pa.float64(), "BOOL": pa.bool_(),
             "STRING": pa.string(), "STRING[]": pa.list_(pa.string())}
    schema = pa.schema([(name, types[kind]) for name, kind in columns.items()])
    files = {}
    counts = defaultdict(int)
    with ExitStack() as stack:
        writers = {}
        for key, group in groups:
            if key not in writers:
                path = folder / f"{len(writers)}.parquet"
                writers[key] = stack.enter_context(pq.ParquetWriter(path, schema))
                files[key] = path
            writers[key].write_table(pa.Table.from_pylist(group, schema))
            counts[key] += len(group)
    return files, counts


def import_typed(db, folder, nodes, edges, relationships, subtypes, counts, size):
    for entity in nodes:
        columns = {key: kind for key, kind in entity.columns if key != "_label"}
        files, staged = stage_parquet(folder, node_groups(entity, subtypes, size), columns)
        for label, path in files.items():
            db.execute(f"COPY {label} FROM {json.dumps(str(path))}", write=True)
            counts["node", label] += staged[label]
            path.unlink()

    for entity in edges:
        pairs, properties = relationships[entity.label]
        columns = {"_src": "INT64", "_dst": "INT64", **properties}
        files, staged = stage_parquet(folder, edge_groups(entity, subtypes, size), columns)
        for (src, dst), path in files.items():
            options = f" (FROM='{src}', TO='{dst}')" if len(pairs) > 1 else ""
            db.execute(f"COPY {entity.label} FROM {json.dumps(str(path))}{options}", write=True)
            counts["edge", entity.label] += staged[src, dst]
            path.unlink()


def grafeo_csv(path, columns, group):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(columns)
        for row in group:
            writer.writerow(str(row[key]).lower() if isinstance(row[key], bool) else row[key] for key in columns)
    return {key: GRAFEO_TYPES[kind] for key, kind in columns.items()}


def check_import_count(report, expected):
    if report["created"] != expected or report["skipped"]:
        raise ValueError(f"Bulk import expected {expected} rows, got {report}")


def import_grafeo(db, suite, folder, nodes, edges, tables, subtypes, counts, size):
    path = folder / "load.csv"
    file_size = max(size, 250_000)
    for entity in nodes:
        columns = {key: kind for key, kind in entity.columns if key != "_label"}
        if "STRING[]" in columns.values():
            # The typed CSV API accepts scalar properties. Native node batches retain list types.
            for label, group in node_groups(entity, subtypes, size):
                labels = node_labels(entity, label, suite)
                db.db.batch_create_nodes_with_props(labels, [non_null(row) for row in group])
                counts["node", label] += len(group)
            continue
        for label, group in node_groups(entity, subtypes, file_size):
            kinds = grafeo_csv(path, columns, group)
            report = db.db.load_nodes_csv(path, node_labels(entity, label, suite), key="id", types=kinds)
            check_import_count(report, len(group))
            counts["node", label] += len(group)
    id_indexes(db, tables)
    for entity in edges:
        columns = dict(entity.columns)
        for (src, dst), group in edge_groups(entity, subtypes, file_size):
            kinds = grafeo_csv(path, columns, group)
            report = db.db.load_edges_csv(path, entity.label, (src, "id", "_src"), (dst, "id", "_dst"), types=kinds)
            check_import_count(report, len(group))
            counts["edge", entity.label] += len(group)


def load_snapshot(db, suite, root, batch_size, *, workdir=None):
    entities = list((snb_entities if suite == "snb" else finbench_entities)(root))
    nodes = [entity for entity in entities if not entity.pairs]
    edges = [entity for entity in entities if entity.pairs]
    tables = {
        label: {key: typ for key, typ in entity.columns if key != "_label"}
        for entity in nodes for label in (LABELS.get(entity.label, (entity.label,)) if suite == "snb" else (entity.label,))
    }
    relationships = {}
    for entity in edges:
        pairs, columns = relationships.setdefault(entity.label, (set(), {}))
        pairs.update(entity.pairs)
        columns.update((key, kind) for key, kind in entity.columns if not key.startswith("_"))
    create_schema(db, tables, relationships)
    subtypes = {"Organisation": {}, "Place": {}} if suite == "snb" else {}
    counts = defaultdict(int)
    if db.engine in TYPED or db.engine == "grafeo":
        with TemporaryDirectory(prefix="bulk-", dir=workdir) as directory:
            folder = Path(directory)
            if db.engine in TYPED:
                import_typed(db, folder, nodes, edges, relationships, subtypes, counts, max(65_536, batch_size))
            else:
                import_grafeo(db, suite, folder, nodes, edges, tables, subtypes, counts, batch_size)
    else:
        import_batches(db, suite, nodes, edges, tables, subtypes, counts, batch_size)
    return counts

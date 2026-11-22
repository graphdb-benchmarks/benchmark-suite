"""Vendor bulk import paths for the server adapters."""

from ast import literal_eval
from collections import Counter
from contextlib import ExitStack
import csv
from importlib import metadata
from itertools import batched
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import perf_counter_ns

from .bulk_import import edge_groups, node_groups, node_labels
from .datasets import finbench_entities, snb_entities
from .servers import SERVER_VERSIONS, native_executable


def stage_csvs(suite, dataset, directory, batch_size, header, field):
    entities = list((snb_entities if suite == "snb" else finbench_entities)(dataset))
    subtypes = {"Organisation": {}, "Place": {}} if suite == "snb" else {}
    counts = Counter()
    staged = []
    for entity in sorted(entities, key=lambda entity: bool(entity.pairs)):
        columns = [(name, kind) for name, kind in entity.columns if name != "_label"]
        groups = edge_groups if entity.pairs else node_groups
        with ExitStack() as files:
            writers = {}
            for group, batch in groups(entity, subtypes, batch_size):
                if group not in writers:
                    file = directory / f"{len(staged)}.csv"
                    stream = files.enter_context(file.open("w", encoding="utf-8", newline=""))
                    writer = csv.writer(stream, delimiter="|", lineterminator="\n")
                    writer.writerow([header(entity, name, kind) for name, kind in columns])
                    writers[group] = writer
                    staged.append((entity, group, columns, file))
                writers[group].writerows([field(row[name]) for name, _ in columns] for row in batch)
                key = ("edge", entity.label) if entity.pairs else ("node", group)
                counts[key] += len(batch)
    return staged, counts


def falkor_header(entity, name, kind):
    reserved = {"id": f"id:ID({entity.label})"}
    if entity.pairs:
        reserved = {"_src": f":START_ID({entity.spaces[0]})", "_dst": f":END_ID({entity.spaces[1]})"}
    field_type = {"INT64": "LONG", "STRING[]": "ARRAY"}.get(kind, kind)
    return reserved.get(name, f"{name}:{field_type}")


def csv_value(value, list_encoder):
    if isinstance(value, list):
        return list_encoder(value)
    if isinstance(value, bool):
        return str(value).lower()
    return value


def restore_falkor_values(db, staged, batch_size):
    restored = 0
    for entity, label, columns, file in staged:
        text = [(falkor_header(entity, name, kind), name, kind) for name, kind in columns
                if kind in {"STRING", "STRING[]"}]
        if not text:
            continue
        id_header = falkor_header(entity, "id", "INT64")
        with file.open(encoding="utf-8", newline="") as stream:
            for batch in batched(csv.DictReader(stream, delimiter="|"), batch_size):
                changed = []
                for row in batch:
                    # The bulk loader strips strings and infers types within arrays.
                    properties = {name: literal_eval(value) if kind == "STRING[]" else value
                                  for header, name, kind in text
                                  if (value := row[header]) and (kind == "STRING[]" or value != value.strip())}
                    if properties:
                        if entity.pairs:
                            raise ValueError(f"FalkorDB bulk loader would change {entity.label} edge properties")
                        changed.append({"id": int(row[id_header]), "properties": properties})
                if changed:
                    result = db.execute(f"UNWIND $rows AS row MATCH (n:{label} {{id: row.id}}) "
                                        "SET n += row.properties RETURN count(n)",
                                        {"rows": changed}, write=True)
                    if result != [[len(changed)]]:
                        raise ValueError(f"Could not restore all {label} text properties")
                    restored += len(changed)
    return restored


def import_falkordb(db, suite, dataset, batch_size, workdir):
    loader_version = metadata.version("falkordb-bulk-loader")
    if loader_version != "1.2.0":
        raise ValueError(f"Expected falkordb-bulk-loader 1.2.0, got {loader_version}")
    server = db.db
    graph = server.graph.name
    if graph in server.client.list_graphs():
        server.graph.delete()
    with TemporaryDirectory(prefix="falkordb-csv-", dir=workdir) as staging:
        start = perf_counter_ns()
        staged, counts = stage_csvs(suite, dataset, Path(staging), batch_size, falkor_header,
                                    lambda value: csv_value(value, repr))
        staged_at = perf_counter_ns()
        inputs, labels = [], set()
        for entity, group, _, file in staged:
            if entity.pairs:
                inputs.extend(("--relations-with-type", entity.label, str(file)))
            else:
                names = node_labels(entity, group, suite)
                labels.update(names)
                inputs.extend(("--nodes-with-label", ":".join(names), str(file)))
        subprocess.run([
            sys.executable, "-m", "falkordb_bulk_loader.bulk_insert", graph,
            "--server-url", f"redis://127.0.0.1:{server.port}", "--separator", "|",
            "--enforce-schema", "--id-type", "INTEGER", *inputs,
        ], check=True)
        for label in sorted(labels):
            db.execute(f"CREATE INDEX FOR (n:{label}) ON (n.id)", write=True)
        restored = restore_falkor_values(db, staged, batch_size)
        server.client.connection.save()
        finished = perf_counter_ns()
    return counts, {"name": "falkordb-bulk-insert", "version": loader_version,
                    "csv_stage_ns": staged_at - start, "import_ns": finished - staged_at,
                    "nodes_restored_after_bulk_import": restored}


def memgraph_properties(columns):
    values = []
    for name, kind in columns:
        if name.startswith("_"):
            continue
        value = f"row.`{name}`"
        if kind == "STRING[]":
            value = f"split({value}, ';')"
        elif kind != "STRING":
            converter = {"INT64": "toInteger", "DOUBLE": "toFloat", "BOOL": "toBoolean"}[kind]
            value = f"{converter}({value})"
        values.append(f"`{name}`: {value}")
    return "{" + ", ".join(values) + "}"


def import_memgraph(db, suite, dataset, batch_size, workdir):
    with TemporaryDirectory(prefix="memgraph-csv-", dir=workdir) as staging:
        start = perf_counter_ns()
        staged, counts = stage_csvs(suite, dataset, Path(staging), batch_size, lambda _, name, kind: name,
                                    lambda value: csv_value(value, ";".join))
        staged_at = perf_counter_ns()
        db.execute("STORAGE MODE IN_MEMORY_ANALYTICAL", write=True)
        labels = set()
        for entity, group, columns, file in staged:
            if entity.pairs:
                continue
            properties = memgraph_properties(columns)
            names = node_labels(entity, group, suite)
            labels.update(names)
            db.execute(memgraph_csv(file) + f"CREATE (n:{':'.join(names)} {properties})", write=True)
        for label in sorted(labels):
            db.execute(f"CREATE INDEX ON :{label}(id)", write=True)
        for entity, group, columns, file in staged:
            if not entity.pairs:
                continue
            src, dst = group
            properties = memgraph_properties(columns)
            db.execute(memgraph_csv(file) + f"MATCH (a:{src} {{id: toInteger(row._src)}}) "
                       f"MATCH (b:{dst} {{id: toInteger(row._dst)}}) "
                       f"CREATE (a)-[:{entity.label} {properties}]->(b)", write=True)
        db.execute("STORAGE MODE IN_MEMORY_TRANSACTIONAL", write=True)
        db.execute("CREATE SNAPSHOT", write=True)
        finished = perf_counter_ns()
    return counts, {"name": "LOAD CSV in IN_MEMORY_ANALYTICAL", "csv_stage_ns": staged_at - start,
                    "import_ns": finished - staged_at}


def memgraph_csv(file):
    return f'LOAD CSV FROM {json.dumps(str(file))} WITH HEADER DELIMITER "|" NULLIF "" AS row '


def import_tugraph(suite, dataset, path):
    executable = native_executable("SHOOTOUT_TUGRAPH_IMPORT_BIN", "lgraph_import")
    reserved = {"_src": "SRC_ID", "_dst": "DST_ID"}
    with TemporaryDirectory(prefix="tugraph-csv-", dir=path.parent) as staging:
        directory = Path(staging)
        start = perf_counter_ns()
        staged, counts = stage_csvs(suite, dataset, directory, 65_536,
                                    lambda _, name, kind: reserved.get(name, name),
                                    lambda value: csv_value(value, json.dumps))
        tables, properties, sources = {}, {}, []
        for entity, group, columns, file in staged:
            label = entity.label if entity.pairs else group
            source = {"path": str(file), "label": label, "format": "CSV", "header": 1,
                      "columns": [reserved.get(name, name) for name, _ in columns]}
            sources.append(source)
            if entity.pairs:
                source.update(zip(reserved.values(), group, strict=True))
                table = tables.setdefault(("edge", label), {"label": label, "type": "EDGE", "constraints": []})
                table["constraints"].append(list(group))
            else:
                table = tables.setdefault(("node", label), {"label": label, "type": "VERTEX", "primary": "id"})
            fields = properties.setdefault(label, {})
            for name, kind in columns:
                if name not in reserved:
                    fields[name] = {"name": name, "type": "STRING" if kind == "STRING[]" else kind,
                                    "optional": bool(entity.pairs) or name != "id"}
        for table in tables.values():
            if fields := properties[table["label"]]:
                table["properties"] = list(fields.values())
        config = directory / "import.json"
        config.write_text(json.dumps({"schema": list(tables.values()), "files": sources}))
        staged_at = perf_counter_ns()
        subprocess.run([executable, "--online", "false", "--config_file", config,
                        "--dir", path, "--delimiter", "|"], check=True)
        finished = perf_counter_ns()
    return counts, {"name": "lgraph_import", "version": SERVER_VERSIONS["tugraph"],
                    "csv_stage_ns": staged_at - start, "import_ns": finished - staged_at,
                    "schema_limitations": ["single-label vertices", "string arrays imported as JSON strings"]
                    if suite == "snb" else []}

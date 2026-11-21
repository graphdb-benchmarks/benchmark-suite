"""Stream the upstream SNB v1 composite and FinBench snapshot CSV layouts."""

from collections import defaultdict
from contextlib import closing
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import gzip
from itertools import batched
from pathlib import Path
import re
import sqlite3

from .catalog import ROOT
from .engines import TYPED

ID = "INT64"
TEXT = "STRING"
LABELS = {
    "Place": ("City", "Country", "Continent"),
    "Organisation": ("Company", "University"),
    "Post": ("Post",),
    "Comment": ("Comment",),
}
SNB_PAIRS = {
    "organisation_isLocatedIn_place": (("Company", "Country"), ("University", "City")),
    "place_isPartOf_place": (("City", "Country"), ("Country", "Continent")),
    "person_isLocatedIn_place": (("Person", "City"),),
    "post_isLocatedIn_place": (("Post", "Country"),),
    "comment_isLocatedIn_place": (("Comment", "Country"),),
    "person_studyAt_organisation": (("Person", "University"),),
    "person_workAt_organisation": (("Person", "Company"),),
}
FIN_NODES = {
    "Person": {"personId": "id", "personName": "name"},
    "Company": {"companyId": "id", "companyName": "name"},
    "Account": {"accountId": "id", "accoutType": "type"},
    "Loan": {"loanId": "id"},
    "Medium": {"mediumId": "id", "mediumType": "type"},
}
FIN_EDGES = {
    "AccountRepayLoan": ("Account", "Loan"),
    "AccountTransferAccount": ("Account", "Account"),
    "AccountWithdrawAccount": ("Account", "Account"),
    "CompanyApplyLoan": ("Company", "Loan"),
    "CompanyGuaranteeCompany": ("Company", "Company"),
    "CompanyInvestCompany": ("Company", "Company"),
    "CompanyOwnAccount": ("Company", "Account"),
    "LoanDepositAccount": ("Loan", "Account"),
    "MediumSignInAccount": ("Medium", "Account"),
    "PersonApplyLoan": ("Person", "Loan"),
    "PersonGuaranteePerson": ("Person", "Person"),
    "PersonInvestCompany": ("Person", "Company"),
    "PersonOwnAccount": ("Person", "Account"),
}
FIN_RELATIONSHIPS = {
    "AccountRepayLoan": "repay",
    "AccountTransferAccount": "transfer",
    "AccountWithdrawAccount": "withdraw",
    "CompanyApplyLoan": "apply",
    "CompanyGuaranteeCompany": "guarantee",
    "CompanyInvestCompany": "invest",
    "CompanyOwnAccount": "own",
    "LoanDepositAccount": "deposit",
    "MediumSignInAccount": "signIn",
    "PersonApplyLoan": "apply",
    "PersonGuaranteePerson": "guarantee",
    "PersonInvestCompany": "invest",
    "PersonOwnAccount": "own",
}
FIN_TYPES = {
    **dict.fromkeys(("id", "createTime", "timestamp", "lastLoginTime", "birthday", "orderNum"), ID),
    **dict.fromkeys(("amount", "balance", "loanAmount", "interestRate", "ratio"), "DOUBLE"),
    "isBlocked": "BOOL",
}


@dataclass(frozen=True)
class Entity:
    files: tuple
    columns: tuple  # (property or reserved field, Cypher type)
    label: str
    pairs: tuple = ()
    spaces: tuple = ()


def open_csv(path):
    opener = gzip.open if path.suffix == ".gz" else open
    return opener(path, "rt", encoding="utf-8-sig", newline="")


def value(text, kind):
    if not text:
        return None
    if kind == ID:
        if re.fullmatch(r"-?\d+", text):
            return int(text)
        date = datetime.fromisoformat(text)
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        delta = date - datetime(1970, 1, 1, tzinfo=timezone.utc)
        return delta // timedelta(milliseconds=1)
    if kind == "DOUBLE":
        return float(text)
    if kind == "BOOL":
        return {"true": True, "false": False, "1": True, "0": False}[text.lower()]
    if kind == "STRING[]":
        return text.split(";")
    return text


def rows(entity):
    for path in entity.files:
        with open_csv(path) as file:
            reader = csv.reader(file, delimiter="|")
            header = next(reader, None)
            if len(header or ()) != len(entity.columns):
                raise ValueError(f"{path}: unexpected header width")
            for line in reader:
                if len(line) != len(entity.columns):
                    raise ValueError(f"{path}:{reader.line_num}: unexpected row width")
                yield {key: value(cell, kind) for (key, kind), cell in zip(entity.columns, line)}


def snb_entities(root):
    root = root / "social_network" if (root / "social_network").is_dir() else root
    for entry in (ROOT / "queries/snb/headers.txt").read_text().splitlines():
        stem, header = entry.split()
        pattern = re.compile(rf"{re.escape(Path(stem).name)}_\d+_\d+\.csv(?:\.gz)?")
        files = tuple(sorted(p for p in root.glob(f"{stem}_*.csv*") if pattern.fullmatch(p.name)))
        if not files:
            raise FileNotFoundError(f"Missing SNB composite CSV {root / stem}_*_*.csv")
        columns = []
        spaces = []
        label = ""
        for item in header.split("|"):
            name, _, kind = item.partition(":")
            if kind.startswith("ID("):
                label = kind[3:-1]
                kind = ID
            elif kind.startswith(("START_ID(", "END_ID(")):
                name = "_src" if not spaces else "_dst"
                spaces.append(kind.split("(", 1)[1][:-1])
                kind = ID
            elif kind == "LABEL":
                name, kind = "_label", TEXT
            columns.append((name, ID if kind in {"LONG", "INT"} else kind))
        if spaces:
            filename = stem.split("/")[1]
            label = re.sub(r"([a-z])([A-Z])", r"\1_\2", filename.split("_")[1]).upper()
            yield Entity(files, tuple(columns), label, SNB_PAIRS.get(filename, (tuple(spaces),)), tuple(spaces))
        else:
            yield Entity(files, tuple(columns), label)


def finbench_entities(root):
    root = root / "snapshot" if (root / "snapshot").is_dir() else root
    for label in [*FIN_NODES, *FIN_EDGES]:
        path = root / f"{label}.csv"
        with open_csv(path) as file:
            fields = next(csv.reader(file, delimiter="|"))
        if label in FIN_NODES:
            mapped = [FIN_NODES[label].get(name, name) for name in fields]
            pairs = spaces = ()
        else:
            mapped = ["_src", "_dst", *("timestamp" if f == "createTime" else f for f in fields[2:])]
            spaces = FIN_EDGES[label]
            pairs = (spaces,)
        yield Entity((path,), tuple((f, ID if f.startswith("_") else FIN_TYPES.get(f, TEXT)) for f in mapped),
                     FIN_RELATIONSHIPS.get(label, label), pairs, spaces)


def props(columns):
    return ", ".join(f"`{key}`: r.`{key}`" for key in columns)


def types(columns):
    return ", ".join(f"`{key}` {kind}" for key, kind in columns.items())


def create_schema(db, nodes, edges):
    if db.engine in TYPED:
        for label, columns in nodes.items():
            db.execute(f"CREATE NODE TABLE {label}({types(columns)}, PRIMARY KEY (id))", write=True)
        for rel, (pairs, columns) in edges.items():
            args = [*(f"FROM {src} TO {dst}" for src, dst in sorted(pairs))]
            if columns:
                args.append(types(columns))
            db.execute(f"CREATE REL TABLE {rel}({', '.join(args)})", write=True)
    elif db.engine == "arcadedb":
        # The engine stores each vertex in a type and supports type inheritance.
        parents = {sub: base for base, subs in LABELS.items() for sub in subs if base != sub}
        parents.update(Post="Message", Comment="Message")
        needed = {parents[label] for label in nodes if label in parents}
        for parent in sorted(needed):
            arcade_sql(db, f"CREATE VERTEX TYPE {parent}")
        for label in nodes:
            extends = f" EXTENDS {parents[label]}" if label in parents else ""
            arcade_sql(db, f"CREATE VERTEX TYPE {label}{extends}")
            arcade_sql(db, f"CREATE PROPERTY {label}.id LONG")
            arcade_sql(db, f"CREATE INDEX ON {label} (id) UNIQUE")
        for rel in edges:
            arcade_sql(db, f"CREATE EDGE TYPE `{rel}`")


def arcade_sql(db, text):
    with db.db.command("sql", text) as result:
        list(result.iter_dicts())


def id_indexes(db, labels):
    if db.engine == "grafeo":
        db.db.create_property_index("id")
    elif db.engine == "graphdblite":
        with db.transaction():
            for label in labels:
                db.tx.create_index(label, "id")
    elif db.engine in {"velr", "relate"}:
        for label in labels:
            db.execute(f"CREATE INDEX FOR (n:{label}) ON (n.id)", write=True)


def load_snapshot(db, suite, root: Path, batch_size):
    if db.engine == "graphdblite":
        # Resolve external IDs for the native bulk edge API without keeping the graph in Python memory.
        with closing(sqlite3.connect("")) as ids:
            ids.execute("CREATE TABLE ids(label TEXT, external INTEGER, internal INTEGER, PRIMARY KEY(label, external))")
            return _load_snapshot(db, suite, root, batch_size, ids)
    return _load_snapshot(db, suite, root, batch_size, None)


def _load_snapshot(db, suite, root, batch_size, ids):
    entities = list((snb_entities if suite == "snb" else finbench_entities)(root))
    nodes = [entity for entity in entities if not entity.pairs]
    edges = [entity for entity in entities if entity.pairs]
    tables = {
        label: {key: typ for key, typ in e.columns if key != "_label"}
        for e in nodes for label in (LABELS.get(e.label, (e.label,)) if suite == "snb" else (e.label,))
    }
    relationships = {}
    for e in edges:
        pairs, columns = relationships.setdefault(e.label, (set(), {}))
        pairs.update(e.pairs)
        columns.update((key, typ) for key, typ in e.columns if not key.startswith("_"))
    create_schema(db, tables, relationships)
    typed = db.engine in TYPED
    types_only = typed or db.engine == "arcadedb"
    subtypes = {"Organisation": {}, "Place": {}}
    counts = defaultdict(int)
    for e in nodes:
        cols = [k for k, _ in e.columns if k != "_label"]
        for batch in batched(rows(e), batch_size):
            groups = defaultdict(list)
            for row in batch:
                subtype = row.pop("_label", None)
                label = e.label
                if subtype is not None:
                    label = subtype.title()
                    if label not in LABELS[e.label]:
                        raise ValueError(f"Unexpected {e.label} subtype {subtype}")
                    subtypes[e.label][row["id"]] = label
                groups[label].append(row)
            for label, group in groups.items():
                qualifier = label
                if suite == "snb" and not types_only:
                    if label in {"Post", "Comment"}:
                        qualifier += ":Message"
                    elif label != e.label:
                        qualifier += f":{e.label}"
                query = f"UNWIND $rows AS r CREATE (n:{qualifier} {{{props(cols)}}})"
                if ids is None:
                    db.execute(query, {"rows": group}, write=True)
                else:
                    inserted = db.execute(query + " RETURN n.id AS external, id(n) AS internal",
                                          {"rows": group}, write=True)
                    ids.executemany("INSERT INTO ids VALUES (?, ?, ?)", ((label, ext, internal) for ext, internal in inserted))
                counts[("node", label)] += len(group)
    id_indexes(db, tables)
    for e in edges:
        cols = [key for key, _ in e.columns if not key.startswith("_")]
        for batch in batched(rows(e), batch_size):
            groups = defaultdict(list)
            for row in batch:
                src, dst = e.spaces
                src = subtypes[src][row["_src"]] if suite == "snb" and src in subtypes else src
                dst = subtypes[dst][row["_dst"]] if suite == "snb" and dst in subtypes else dst
                if (src, dst) not in e.pairs:
                    raise ValueError(f"Unexpected endpoints {(src, dst)} for {e.label}")
                groups[(src, dst)].append(row)
            for (src, dst), group in groups.items():
                query = (
                    f"UNWIND $rows AS r MATCH (a:{src} {{id: r._src}}) "
                    f"MATCH (b:{dst} {{id: r._dst}}) CREATE (a)-[:{e.label} {{{props(cols)}}}]->(b)"
                )
                if ids is None:
                    db.execute(query, {"rows": group}, write=True)
                else:
                    def lookup(label, external):
                        found = ids.execute("SELECT internal FROM ids WHERE label = ? AND external = ?", (label, external)).fetchone()
                        if found is None:
                            raise ValueError(f"Missing {label} endpoint {external}")
                        return found[0]

                    payload = [(lookup(src, row["_src"]), lookup(dst, row["_dst"]),
                                {key: row[key] for key in cols if row[key] is not None}) for row in group]
                    with db.transaction():
                        db.tx.batch_create_edges(e.label, payload)
                counts[("edge", e.label)] += len(group)
    return counts


def verify_snapshot(db, counts):
    for (kind, label), expected in counts.items():
        pattern, alias = (f"(x:{label})", "x") if kind == "node" else (f"()-[x:{label}]->()", "x")
        number = db.execute(f"MATCH {pattern} RETURN count({alias}) AS n")
        if number != [[expected]]:
            raise ValueError(f"{kind} {label}: snapshot has {number}, expected {expected}")

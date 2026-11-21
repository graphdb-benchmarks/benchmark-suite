"""Public in-process Python APIs. Workload Cypher always comes from the catalog."""

from contextlib import contextmanager
from importlib import import_module, metadata
from pathlib import Path

ENGINES = {
    "velr": "velr",
    "relate": "relate-db",
    "ladybug": "ladybug",
    "kuzu": "kuzu",
    "grafeo": "grafeo",
    "arcadedb": "arcadedb",
    "graphdblite": "graphdblite",
    "ryugraph": "ryugraph",
}
TYPED = {"ladybug", "kuzu", "ryugraph"}
PINS = dict(
    line.split("==", 1)
    for line in (Path(__file__).resolve().parents[1] / "requirements.txt").read_text().splitlines()
)


def version(engine):
    package = ENGINES[engine]
    installed = metadata.version(package)
    if installed != PINS[package]:
        raise RuntimeError(f"{package}=={installed} installed, expected {PINS[package]}")
    return installed


class Database:
    def __init__(self, engine, path, threads):
        self.engine = engine
        self.connection = None
        self.tx = None
        path = str(path)
        if engine in TYPED:
            mod = import_module(engine)
            self.db = mod.Database(path, max_num_threads=threads)
            self.connection = mod.Connection(self.db, threads)
            self.connection.set_query_timeout(0)
        elif engine == "velr":
            from velr.driver import Velr

            self.db = Velr.open(path)
        elif engine == "relate":
            import relate

            self.db = relate.connect(path)
        elif engine == "grafeo":
            from grafeo import GrafeoDB

            self.db = GrafeoDB(path, query_timeout=None)
        elif engine == "arcadedb":
            import arcadedb

            opener = arcadedb.open_database if Path(path).exists() else arcadedb.create_database
            self.db = opener(path)
        elif engine == "graphdblite":
            from graphdblite import Database as GraphDBLite

            self.db = GraphDBLite(path)
        else:
            raise ValueError(f"Unknown engine {engine}")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        try:
            if self.connection is not None:
                self.connection.close()
        finally:
            self.db.close()

    def execute(self, cypher, params=None, *, write=False):
        params = params or {}
        engine = self.engine
        client = self.tx if self.tx is not None else self.db
        if engine in TYPED:
            result = self.connection.execute(cypher, params)
            if isinstance(result, list):
                for item in result:
                    item.close()
                raise RuntimeError("Expected a single Cypher statement")
            try:
                return [result.get_next() for _ in range(result.get_num_tuples())]
            finally:
                result.close()
        if engine == "velr":
            rows = []
            with client.exec(cypher, params=params) as stream:
                while (table := stream.next_table()) is not None:
                    with table:
                        rows.extend(table.to_rows())
            return rows
        if engine == "relate":
            cursor = client.execute(cypher, params)
            try:
                return [[record[key] for key in record.keys()] for record in cursor.fetchall()] if cursor.description else []
            finally:
                cursor.close()
        if engine == "grafeo":
            return [list(row.values()) for row in client.execute_cypher(cypher, params)]
        if engine == "graphdblite":
            if write and self.tx is None:
                with self.transaction():
                    return self.execute(cypher, params, write=True)
            return [list(row.values()) for row in client.query(cypher, params)]
        call = client.command if write else client.query
        with call("opencypher", cypher, params) as result:
            return [list(row.values()) for row in result.iter_dicts()]

    @contextmanager
    def transaction(self):
        engine = self.engine
        client = self.db
        if engine in TYPED:
            self.execute("BEGIN TRANSACTION", write=True)
            commit = lambda: self.execute("COMMIT", write=True)
            rollback = lambda: self.execute("ROLLBACK", write=True)
        elif engine == "relate":
            client.autocommit = False
            commit, rollback = client.commit, client.rollback
        elif engine == "arcadedb":
            client.begin()
            commit, rollback = client.commit, client.rollback
        else:
            begin = {
                "velr": "begin_tx",
                "grafeo": "begin_transaction",
                "graphdblite": "begin_write",
            }[engine]
            self.tx = getattr(client, begin)()
            commit, rollback = self.tx.commit, self.tx.rollback
        try:
            yield
            commit()
        except BaseException:
            try:
                rollback()
            except Exception:
                pass  # The engine may already have rolled back the failed transaction.
            raise
        finally:
            self.tx = None
            if engine == "relate":
                client.autocommit = True

from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

from shootout.catalog import load
from shootout.datasets import create_schema
from shootout.engines import Database, DEFAULT_ENGINES
from shootout.run import bind, disposition


class QuerySources(unittest.TestCase):
    def test_reference_inventory_and_omissions(self):
        self.assertEqual(len(load("snb")), 29)
        self.assertEqual(len(load("finbench")), 36)
        self.assertIn("apoc.coll.sum", disposition("finbench", {"op": "TCR11"})[1])
        self.assertIn("gds.similarity.jaccard", disposition("finbench", {"op": "TCR10"})[1])
        self.assertEqual(disposition("finbench", {"op": "TW9", "ldbc_op": "TW14"}), ("TW14", None))
        with self.assertRaises(ValueError):
            disposition("finbench", {"op": "TCR11", "ldbc_op": "TCR4"})

    def test_deviations_are_opt_in_and_scoped(self):
        upstream = load("snb")["IS4"]
        typed = load("snb", "kuzu", ["snb-typed-message"])["IS4"]
        other = load("snb", "velr", ["snb-typed-message"])["IS4"]
        self.assertEqual(upstream.text, other.text)
        self.assertEqual(upstream.upstream_sha256, typed.upstream_sha256)
        self.assertEqual(upstream.text.replace(":Message", ":Post:Comment"), typed.text)

    def test_jaccard_on_published_drivers(self):
        for engine in DEFAULT_ENGINES:
            # Kùzu and RyuGraph register native types with the same names.
            with self.subTest(engine=engine):
                run = subprocess.run([sys.executable, "-m", "tests.test_query_sources", engine],
                                     capture_output=True, text=True, timeout=30)
                self.assertEqual(run.returncode, 0, run.stdout + run.stderr)


def check_jaccard(engine):
    query = load("finbench", engine, ["finbench-cypher-jaccard"])["TCR10"]
    records = [
        (1, 1, 5), (1, 1, 6), (1, 2, 5), (2, 2, 5), (2, 3, 5), (2, 1, 0), (2, 1, 10),
        (3, 4, 5), (4, 4, 7), (7, 5, 5), (7, 6, 5), (8, 5, 5), (8, 6, 5),
    ]
    with TemporaryDirectory() as directory, Database(engine, Path(directory) / "db", 1) as db:
        create_schema(db, {"Person": {"id": "INT64"}, "Company": {"id": "INT64"}},
                      {"invest": ({("Person", "Company")}, {"timestamp": "INT64"})})
        for label in ("Person", "Company"):
            db.execute(f"UNWIND $ids AS n CREATE (:{label} {{id:n}})", {"ids": list(range(1, 10))}, write=True)
        payload = [dict(src=person, dst=company, timestamp=timestamp) for person, company, timestamp in records]
        db.execute(
            "UNWIND $rows AS r MATCH (a:Person {id:r.src}), (b:Company {id:r.dst}) "
            "CREATE (a)-[:invest {timestamp:r.timestamp}]->(b)", {"rows": payload}, write=True,
        )
        for start, end in ((0, 10), (5, 100)):
            for first, second in ((1, 2), (1, 3), (3, 4), (4, 5), (5, 4), (5, 6), (7, 7), (7, 8)):
                a = {company for person, company, time in records if person == first and start < time < end}
                b = {company for person, company, time in records if person == second and start < time < end}
                expected = round(len(a & b) / len(a | b), 3) if a | b else 0.0
                params = {"id1": first, "id2": second, "start_time": start, "end_time": end}
                actual = db.execute(query.text, bind(query, params))
                if len(actual) != 1 or len(actual[0]) != 1 or abs(actual[0][0] - expected) > 1e-12:
                    raise AssertionError((engine, params, actual, expected))


if __name__ == "__main__":
    check_jaccard(sys.argv[1])

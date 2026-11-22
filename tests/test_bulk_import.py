from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from shootout.datasets import verify_snapshot
from shootout.engines import Database


class SnapshotVerification(unittest.TestCase):
    def test_graphdblite_verifies_above_the_cypher_scan_limit(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "graph"
            with Database("graphdblite", path, 1) as db:
                for start in range(0, 100_001, 5000):
                    with db.transaction():
                        db.tx.batch_create_nodes(
                            "Person", [{"id": number} for number in range(start, min(start + 5000, 100_001))],
                        )
                with db.transaction():
                    db.tx.create_index("Person", "id")
                    db.tx.batch_create_edges("KNOWS", [(1, 2), (1, 2, {"since": 2026})])
                    db.tx.batch_create_edges("LIKES", [(2, 1)])
            expected = Counter({("node", "Person"): 100_001, ("edge", "KNOWS"): 2, ("edge", "LIKES"): 1})
            with Database("graphdblite", path, 1) as db:
                self.assertEqual(verify_snapshot(db, expected, path), "sqlite_storage_counts")
                self.assertEqual(db.execute("MATCH (n:Person {id: $id}) RETURN n.id", {"id": 3}), [[3]])
                expected["edge", "LIKES"] = 2
                with self.assertRaisesRegex(ValueError, "edge LIKES"):
                    verify_snapshot(db, expected, path)

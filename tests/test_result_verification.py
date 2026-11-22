from contextlib import nullcontext
from io import StringIO
import json
import unittest
from unittest.mock import Mock, patch

from shootout.catalog import load
from shootout.chart import aggregate, aggregate_chart, operation_chart, outcome
from shootout.run import run_cases
from shootout.verification import equal_rows, rows_digest


class ResultVerification(unittest.TestCase):
    def test_runner_passes_only_when_complete_rows_match(self):
        listed = {number: {"op": "IS1", "params": {"personId": number}} for number in range(1, 5)}
        listed[4]["expected"] = []
        db = Mock()
        db.execute.side_effect = [[["right"]], [["wrong"]], []]
        output = StringIO()
        with patch("shootout.run.query_window", return_value=nullcontext()):
            run_cases(db, "snb", load("snb"), listed, {1: [["right"]], 2: [["right"]]}, output, None, 1)
        events = map(json.loads, output.getvalue().splitlines())
        rows = [event for event in events if event["event"] == "case"]
        self.assertEqual([event["status"] for event in rows], ["verified", "mismatch", "verification_error", "verified"])
        self.assertEqual(rows[1]["actual"], [["wrong"]])
        self.assertEqual([call.args[1] for call in db.execute.call_args_list], [
            {"personId": 1}, {"personId": 2}, {"personId": 4},
        ])

    def test_tied_paths_keep_path_order_and_multiplicity(self):
        first, second, third = [[1, 2, 5], 3.0], [[1, 3, 5], 3.0], [[1, 4, 5], 2.0]
        expected = [first, second, third]
        compare = {"suite": "snb", "operation": "IC14"}
        self.assertTrue(equal_rows([second, first, third], expected, **compare))
        self.assertFalse(equal_rows([third, second, first], expected, **compare))
        self.assertFalse(equal_rows([first, first, third], expected, **compare))
        self.assertFalse(equal_rows([[[5, 2, 1], 3.0], second, third], expected, **compare))

    def test_legacy_fingerprint_only_passes_with_a_matching_answer(self):
        event = {"status": "unverified", "duration_ns": 52, "rows_sha256": rows_digest([[1]])}
        self.assertEqual(outcome(event, {}, [[1]], 100, "complete")[0], "pass")
        self.assertEqual(outcome(event, {}, [[2]], 100, "complete")[0], "fail")
        self.assertEqual(outcome(event, {}, None, 100, "complete")[0], "fail")

    def test_timeout_reaches_the_cap_and_failure_has_only_a_marker(self):
        engines = [
            {"name": "velr", "cases": {1: {"outcome": "timeout", "duration_ns": 30_000_000_000, "reason": "Deadline"}}},
            {"name": "grafeo", "cases": {1: {"outcome": "fail", "duration_ns": 2100, "reason": "Wrong result"}}},
        ]
        chart = operation_chart("IC14", [{"number": 1, "params": {}}], engines, 30, "test")
        self.assertIn('y="20.00" width="22" height="230.00"', chart)
        self.assertEqual(chart.count("<rect"), 1)
        self.assertIn("×</text>", chart)
        self.assertIn("FAIL</text>", chart)

    def test_operation_uses_verified_median_and_maximum_measured_rss(self):
        engine = {"name": "velr", "cases": {
            1: {"outcome": "pass", "duration_ns": 10_000_000, "peak_query_rss_bytes": 42_000_000},
            2: {"outcome": "pass", "duration_ns": 30_000_000, "peak_query_rss_bytes": 120_000_000},
            3: {"outcome": "fail", "duration_ns": 3, "peak_query_rss_bytes": None},
            4: {"outcome": "timeout", "duration_ns": 30_000_000_000, "peak_query_rss_bytes": 900_000_000},
        }}
        result = aggregate(engine, [{"number": case} for case in range(1, 5)])
        self.assertEqual(result["outcomes"], {"pass": 2, "fail": 1, "timeout": 1, "omitted": 0})
        self.assertEqual(result["median_verified_ns"], 20_000_000)
        self.assertEqual((result["peak_query_rss_bytes"], result["rss_cases"]), (900_000_000, 3))
        timing = aggregate_chart("IC14", [result], 30, 15_000_000_000)
        self.assertEqual(timing.count("<rect"), 1)
        self.assertIn('y="22.00" width="34" height="200.00"', timing)
        self.assertIn('class="median"', timing)
        self.assertIn('>FAIL</text>', timing)
        memory = aggregate_chart("IC14", [result], 30, 15_000_000_000, memory=True)
        self.assertIn("900 MB", memory)
        self.assertIn("3/4 sampled", memory)
        result["peak_query_rss_bytes"] = None
        self.assertIn("No per-operation RSS samples", aggregate_chart("IC14", [result], 30, 15_000_000_000, memory=True))

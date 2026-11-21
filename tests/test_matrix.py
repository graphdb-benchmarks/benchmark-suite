from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from shootout import matrix
from shootout.__main__ import main
from shootout.engines import ENGINES


class Matrix(unittest.TestCase):
    def test_all_lists_every_workload_and_engine_without_creating_output(self):
        with TemporaryDirectory() as directory:
            out = Path(directory) / "new results"
            stdout = io.StringIO()
            with patch("sys.argv", ["shootout", "matrix", "--all-workloads", "--dry-run", "--out", str(out)]), \
                    redirect_stdout(stdout):
                self.assertEqual(main(), 0)
            plan = json.loads(stdout.getvalue())
            self.assertEqual(plan["engines"], list(ENGINES))
            self.assertEqual(plan["rss_limit_bytes"], 15_000_000_000)
            self.assertEqual(plan["query_timeout"], 30)
            self.assertEqual(plan["query_deadline_policy"], "supervisor_process_group")
            self.assertEqual({job["workload"] for job in plan["jobs"]}, {
                "snb-sf1", "snb-sf10", "finbench-sf1", "finbench-sf10",
            })
            self.assertFalse(out.exists())

    def test_selected_continues_after_a_workload_preparation_failure(self):
        with TemporaryDirectory() as directory:
            out = Path(directory) / "selection"
            argv = ["shootout", "matrix", "--workload", "snb-sf10", "--workload", "finbench-sf1",
                    "--engine", "relate", "--engine", "velr", "--op", "IC1", "--op", "TSR1",
                    "--rss-limit-gb", "24.5", "--query-timeout", "45", "--per-read", "1", "--out", str(out)]
            with patch("sys.argv", argv), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()), \
                    patch.object(matrix.fetch, "snb", side_effect=OSError("download failed")), \
                    patch.object(matrix.fetch, "finbench") as fetch_finbench, \
                    patch.object(matrix, "write_case_file") as write_cases, \
                    patch.object(matrix, "orchestrate", return_value=0) as replay:
                self.assertEqual(main(), 1)
            fetch_finbench.assert_called_once()
            self.assertEqual(write_cases.call_args.kwargs["operations"], ["TSR1"])
            self.assertEqual(write_cases.call_args.kwargs["per_read"], 1)
            job = replay.call_args.args[0]
            self.assertEqual((job.suite, job.scale, job.engine), ("finbench", "1", ["velr", "relate"]))
            self.assertEqual(job.rss_limit_bytes, 24_500_000_000)
            self.assertEqual(job.query_timeout, 45)
            self.assertEqual(job.out, out / "finbench-sf1")
            summary = [json.loads(line) for line in (out / "matrix-summary.jsonl").read_text().splitlines()]
            self.assertEqual([row["status"] for row in summary], ["error", "complete"])

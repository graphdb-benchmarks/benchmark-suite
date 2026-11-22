import unittest

from shootout.leaderboard import render, score_workload


def measurement(engine, *, passed=5, failed=0, omitted=0, latency=15, peak=20, samples=5):
    return {"engine": engine, "cases": 5, "median_verified_ns": latency,
            "rss_cases": samples, "peak_query_rss_bytes": peak,
            "outcomes": {"pass": passed, "fail": failed, "timeout": 5 - passed - failed - omitted, "omitted": omitted}}


def workload(operations):
    cases = {row["engine"]: {} for rows in operations.values() for row in rows}
    schedule = {}
    offset = 1
    for operation, rows in operations.items():
        numbers = range(offset, offset + rows[0]["cases"])
        schedule[operation] = [{"number": number} for number in numbers]
        offset += len(numbers)
        for row in rows:
            outcomes = [outcome for outcome, count in row["outcomes"].items() for _ in range(count)]
            for number, outcome in zip(numbers, outcomes, strict=True):
                duration = row["median_verified_ns"] if outcome == "pass" else 1
                cases[row["engine"]][number] = {"outcome": outcome, "duration_ns": duration}
    return {"workload": "snb-sf1", "reference": None, "deadline_seconds": 30, "rss_limit_bytes": 15_000_000_000,
            "engines": [{"name": name, "version": "1.0", "directory": "run/snb-sf1", "cases": rows}
                        for name, rows in cases.items()],
            "operations": schedule, "aggregates": operations}


class Leaderboard(unittest.TestCase):
    def test_wrong_and_timed_out_cases_earn_zero(self):
        report = workload({"IC1": [
            measurement("velr", latency=80, peak=70),
            measurement("grafeo", latency=250_000_000, peak=250_000_000),
            measurement("graphqlite", passed=3, failed=1, latency=2, peak=1),
        ]})
        points = {row["engine"]: row["points"] for row in score_workload(report)["standings"]}
        self.assertEqual(points["velr"], {"correct": 1, "under_latency_target": 1, "p99_under_25_ms": 1})
        self.assertEqual(points["grafeo"], {"correct": 1, "under_latency_target": 0, "p99_under_25_ms": 0})
        self.assertEqual(sum(points["graphqlite"].values()), 0)
        alternate = score_workload(report, latency_target_ms=500)
        grafeo = next(row for row in alternate["standings"] if row["engine"] == "grafeo")
        self.assertEqual(grafeo["points"]["under_latency_target"], 1)
        self.assertIn("p99 under 500 ms", render(alternate))

    def test_rss_measurements_do_not_affect_the_score(self):
        report = workload({
            "IC10": [measurement("velr"), measurement("grafeo", passed=0, latency=None)],
            "IC11": [measurement("velr", samples=4, peak=10), measurement("grafeo", peak=30)],
        })
        scores = score_workload(report)["categories"]
        self.assertEqual(scores["IC10"][0]["total"], 3)
        self.assertEqual([row["total"] for row in scores["IC11"]], [3, 3])

    def test_p99_under_25_ms_counts_only_fully_correct_categories(self):
        report = workload({
            "IC1": [measurement("velr", latency=24_999_999)],
            "IC2": [measurement("velr", latency=24_999_999)],
            "IC3": [measurement("velr", passed=4, failed=1, latency=24_999_999)],
        })
        report["engines"][0]["cases"][6]["duration_ns"] = 25_000_000
        result = score_workload(report)
        self.assertEqual(result["standings"][0]["points"]["p99_under_25_ms"], 1)
        self.assertEqual(result["categories"]["IC1"][0]["p99_ns"], 24_999_999)
        self.assertEqual(result["categories"]["IC2"][0]["p99_ns"], 25_000_000)
        self.assertEqual(result["categories"]["IC2"][0]["points"]["p99_under_25_ms"], 0)
        self.assertIsNone(result["categories"]["IC3"][0]["p99_ns"])
        self.assertEqual(result["categories"]["IC3"][0]["total"], 0)
        self.assertIn("9 possible points", render(result))

    def test_equal_scores_share_rank_and_universal_omissions_are_excluded(self):
        report = workload({
            "IC1": [measurement("velr"), measurement("grafeo")],
            "IS1": [measurement("velr", passed=0, omitted=5), measurement("grafeo", passed=0, omitted=5)],
        })
        result = score_workload(report)
        self.assertEqual(list(result["categories"]), ["IC1"])
        self.assertEqual(result["max_points"], 3)
        self.assertEqual(result["case_count"], 5)
        self.assertEqual([(row["rank"], row["total"]) for row in result["standings"]], [(1, 3), (1, 3)])

    def test_platform_cohorts_each_render_their_own_first_place(self):
        macos = score_workload(workload({"IC1": [measurement("velr")]}))
        linux = score_workload(workload({"IC1": [measurement("memgraph", latency=250_000_000)]}))
        document = render({**macos, "cohort": "macOS ARM64"}, {**linux, "cohort": "Linux ARM64 container"})
        self.assertIn('<a href="#cohort-1">macOS ARM64</a>', document)
        self.assertIn('<a href="#cohort-2">Linux ARM64 container</a>', document)
        self.assertEqual(document.count('<th scope="row">1</th>'), 2)
        self.assertNotIn("Run source", document)

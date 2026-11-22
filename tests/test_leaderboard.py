import unittest

from shootout.leaderboard import render, score_workload


def measurement(engine, *, passed=5, failed=0, omitted=0, latency=15, peak=20, samples=5, cases=5):
    return {"engine": engine, "cases": cases, "median_verified_ns": latency,
            "rss_cases": samples, "peak_query_rss_bytes": peak,
            "outcomes": {"pass": passed, "fail": failed, "timeout": cases - passed - failed - omitted, "omitted": omitted}}


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
            measurement("grafeo", latency=2_500_000_000),
            measurement("graphqlite", passed=4, failed=1, latency=2),
            measurement("relate", passed=4, latency=2),
        ]})
        result = score_workload(report)
        standings = {row["engine"]: (row["total"], row["verified_operations"]) for row in result["standings"]}
        self.assertEqual(standings, {"velr": (3, 1), "grafeo": (0, 1), "graphqlite": (0, 0), "relate": (0, 0)})
        p99s = {row["engine"]: row["p99_ns"] for row in result["categories"]["IC1"]}
        self.assertEqual(p99s, {"velr": 80, "grafeo": 2_500_000_000, "graphqlite": None, "relate": None})

    def test_rss_measurements_do_not_affect_the_score(self):
        report = workload({
            "IC10": [measurement("velr"), measurement("grafeo", passed=0, latency=None)],
            "IC11": [measurement("velr", samples=4, peak=10), measurement("grafeo", peak=30)],
        })
        scores = score_workload(report)["categories"]
        self.assertEqual(scores["IC10"][0]["total"], 3)
        self.assertEqual([row["total"] for row in scores["IC11"]], [3, 3])

    def test_small_sample_uses_maximum_as_p99(self):
        report = workload({"IC1": [measurement("velr", latency=15_000_000)]})
        report["engines"][0]["cases"][2]["duration_ns"] = 2_500_000_000
        result = score_workload(report)
        self.assertEqual(result["categories"]["IC1"][0]["p99_ns"], 2_500_000_000)
        self.assertEqual(result["standings"][0]["total"], 0)

    def test_expanded_sample_uses_nearest_rank_p99(self):
        report = workload({"IC1": [measurement("velr", passed=500, cases=500, latency=15_000_000)]})
        for number in range(496, 501):
            report["engines"][0]["cases"][number]["duration_ns"] = 1_000_000_000
        result = score_workload(report)
        score, = result["categories"]["IC1"]
        self.assertEqual(score["p99_ns"], 15_000_000)
        self.assertEqual(score["total"], 3)
        self.assertIn("500 scheduled measurements", render(result))

    def test_latency_points_are_logarithmic_between_the_anchors(self):
        latencies_ns = [0, 15_000_000, 30_000_000, 60_000_000, 193_649_167, 2_500_000_000, 3_000_000_000]
        report = workload({f"IC{index}": [measurement("velr", latency=latency)]
                           for index, latency in enumerate(latencies_ns, 1)})
        result = score_workload(report)
        scores = [rows[0]["total"] for rows in result["categories"].values()]
        self.assertEqual(scores[:2] + scores[-2:], [3, 3, 0, 0])
        self.assertGreater(scores[2], scores[3])
        self.assertAlmostEqual(scores[1] - scores[2], scores[2] - scores[3])
        self.assertAlmostEqual(scores[4], 1.5)
        document = render(result)
        self.assertIn("0 points at 2.5 s", document)
        self.assertIn("at 15 ms or faster", document)

    def test_equal_latency_products_share_a_rank(self):
        report = workload({
            "IC1": [measurement("velr", latency=100_000_000), measurement("grafeo", latency=200_000_000),
                    measurement("graphqlite", latency=100_000_000)],
            "IC2": [measurement("velr", latency=400_000_000), measurement("grafeo", latency=200_000_000),
                    measurement("graphqlite", latency=400_000_001)],
        })
        standings = score_workload(report)["standings"]
        self.assertEqual([(row["engine"], row["rank"]) for row in standings], [("grafeo", 1), ("velr", 1), ("graphqlite", 3)])
        self.assertAlmostEqual(standings[0]["total"], standings[1]["total"])
        self.assertGreater(standings[1]["total"], standings[2]["total"])

    def test_geometric_mean_shares_only_verified_operations(self):
        report = workload({
            "IC1": [measurement("velr", latency=100), measurement("grafeo", latency=900)],
            "IC2": [measurement("velr", latency=900), measurement("grafeo", latency=100)],
            "IC3": [measurement("velr", latency=10), measurement("grafeo", passed=4, failed=1)],
        })
        result = score_workload(report)
        self.assertEqual(result["common_operations"], ["IC1", "IC2"])
        self.assertEqual([row["common_geomean_p99_ns"] for row in result["standings"]], [300, 300])
        self.assertIn("2 of 3 scored operations", render(result))

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

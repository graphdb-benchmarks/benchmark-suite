"""Rank comparable saved runs of one workload within each platform cohort."""

import argparse
from html import escape
import json
from pathlib import Path

from .chart import NAMES, latency_label, read_schedule, read_workload, rss_label
from .engines import ENGINES

CRITERIA = ("correct", "under_latency_target", "p99_under_25_ms")
COLORS = ("#265991", "#127467", "#9b4c22")
DEFAULT_LATENCY_TARGET_MS = 250
NS_PER_MS = 1_000_000
FAST_RESPONSE_MS = 25


def load_cohort(directories, reference, excluded):
    primary, *additional = directories
    settings, _ = read_schedule(primary)
    seen = set(settings["engines"]) - set(excluded)
    overrides = {}
    for directory in additional:
        other, _ = read_schedule(directory)
        for engine in other["engines"]:
            if engine in excluded:
                continue
            if engine in seen:
                raise ValueError(f"Choose one source for {engine} because multiple runs supply it")
            seen.add(engine)
            overrides[engine] = directory
    report = read_workload(primary, overrides, reference, excluded)
    return report, settings["cases_sha256"]


def score_workload(report, *, latency_target_ms=DEFAULT_LATENCY_TARGET_MS):
    threshold_ns = latency_target_ms * NS_PER_MS
    fast_response_ns = FAST_RESPONSE_MS * NS_PER_MS
    weights = dict.fromkeys(CRITERIA, 1)
    recorded = {engine["name"]: engine["cases"] for engine in report["engines"]}
    totals = {engine["name"]: dict.fromkeys(CRITERIA, 0) for engine in report["engines"]}
    categories = {}
    case_count = 0
    for operation, rows in report["aggregates"].items():
        if all(row["outcomes"]["omitted"] == row["cases"] for row in rows):
            continue
        numbers = [item["number"] for item in report["operations"][operation]]
        case_count += len(numbers)
        entries = []
        for row in rows:
            engine = row["engine"]
            correct = row["outcomes"]["pass"] == row["cases"]
            durations = sorted(recorded[engine][number]["duration_ns"] for number in numbers) if correct else []
            p99 = durations[(99 * len(durations) - 1) // 100] if durations else None
            earned = {
                "correct": correct,
                "under_latency_target": p99 is not None and p99 < threshold_ns,
                "p99_under_25_ms": p99 is not None and p99 < fast_response_ns,
            }
            points = {criterion: int(earns) for criterion, earns in earned.items()}
            for criterion, value in points.items():
                totals[engine][criterion] += value
            entries.append({**row, "points": points, "total": sum(points.values()), "p99_ns": p99})
        categories[operation] = entries

    standings = []
    for engine in report["engines"]:
        name = engine["name"]
        points = totals[name]
        standings.append({"engine": name, "version": engine["version"], "source": engine["directory"],
                          "deployment": engine.get("deployment", "embedded"),
                          "points": points, "total": sum(points.values())})
    standings.sort(key=lambda row: (-row["total"], row["engine"]))
    previous = None
    for index, row in enumerate(standings, 1):
        if row["total"] != previous:
            rank = index
        row["rank"] = rank
        previous = row["total"]
    host = next((engine for engine in report["engines"] if engine.get("platform")), {})
    return {"workload": report["workload"], "reference": report["reference"],
            "deadline_seconds": report["deadline_seconds"], "rss_limit_bytes": report["rss_limit_bytes"],
            "platform": host.get("platform"), "python": host.get("python"),
            "threshold_ns": threshold_ns,
            "fast_response_threshold_ns": fast_response_ns, "case_count": case_count,
            "weights": weights, "max_points": len(categories) * sum(weights.values()),
            "standings": standings, "categories": categories}


def score_chart(score, labels):
    left, top, bottom, step = 42, 26, 224, 108
    width = left + step * len(score["standings"]) + 20
    maximum = score["max_points"]
    if not maximum:
        return ""

    def y(points):
        return bottom - (bottom - top) * points / maximum

    svg = [f'<svg viewBox="0 0 {width} 267" width="{width}" role="img" aria-label="Total and component points by engine">']
    for points in sorted({maximum * i // 4 for i in range(5)}):
        svg.append(f'<path d="M {left} {y(points):.2f} H {width - 12}" stroke="#d8e2df"/>'
                   f'<text x="{left - 8}" y="{y(points) + 4:.2f}" text-anchor="end">{points}</text>')
    for index, row in enumerate(score["standings"]):
        name = NAMES[row["engine"]]
        x = left + (index + 0.5) * step
        height = bottom
        for criterion, label, color in zip(CRITERIA, labels, COLORS, strict=True):
            value = row["points"][criterion]
            if value:
                size = (bottom - top) * value / maximum
                height -= size
                svg.append(f'<rect x="{x - 26:.1f}" y="{height:.2f}" width="52" height="{size:.2f}" fill="{color}">'
                           f'<title>{name} · {label} · {value} points</title></rect>')
        svg.append(f'<text x="{x:.1f}" y="{y(row["total"]) - 8:.2f}" text-anchor="middle" class="total">{row["total"]}</text>'
                   f'<text x="{x:.1f}" y="249" text-anchor="middle">{name}</text>')
    return "".join([*svg, "</svg>"])


def render_cohort(score, labels, anchor):
    heading = escape(score.get("cohort", "Total points"))
    evidence = ""
    if score.get("platform"):
        evidence = f' · {escape(score["platform"])}'
    if score.get("python"):
        evidence += f' · Python {escape(score["python"])}'
    standings = []
    for row in score["standings"]:
        name = f'{NAMES[row["engine"]]} {row["version"] or ""}'.strip()
        points = "".join(f'<td>{row["points"][key]}</td>' for key in CRITERIA)
        standings.append(f'<tr><th scope="row">{row["rank"]}</th><td>{escape(name)}</td><td>{escape(row["deployment"])}</td>'
                         f'<td class="total">{row["total"]}</td>{points}</tr>')
    details = []
    for operation, rows in score["categories"].items():
        entries = []
        for row in rows:
            counts = row["outcomes"]
            time = latency_label(row["median_verified_ns"]) if row["median_verified_ns"] is not None else "—"
            p99 = latency_label(row["p99_ns"]) if row["p99_ns"] is not None else "—"
            peak = rss_label(row["peak_query_rss_bytes"]) if row["peak_query_rss_bytes"] is not None else "—"
            breakdown = ", ".join(label for key, label in zip(CRITERIA, labels, strict=True) if row["points"][key]) or "—"
            entries.append(f'<tr><th scope="row">{NAMES[row["engine"]]}</th><td>{counts["pass"]}/{row["cases"]}</td>'
                           f'<td>{p99}</td><td>{time}</td><td>{peak} ({row["rss_cases"]}/{row["cases"]})</td>'
                           f'<td>{row["total"]}</td><td>{breakdown}</td></tr>')
        details.append(f'<details><summary>{escape(operation)}</summary><table><thead><tr><th>Engine</th><th>Passed</th>'
                       '<th>Correct category p99</th><th>Verified median</th><th>Peak RSS (coverage)</th>'
                       f'<th>Points</th><th>Criteria earned</th></tr></thead>'
                       f'<tbody>{"".join(entries)}</tbody></table></details>')

    return f'''<section id="{anchor}"><h2>{heading}</h2>
<p>{len(score['categories'])} scored operations · {score['max_points']} possible points per engine{evidence}</p>
<div class="plot">{score_chart(score, labels)}</div>
<div class="table"><table><thead><tr><th>Rank</th><th>Engine</th><th>Deployment</th><th>Total</th><th>Correct</th><th>{labels[1]}</th>
<th>{labels[2]}</th></tr></thead><tbody>{''.join(standings)}</tbody></table></div>
<details><summary>Points by operation</summary>{''.join(details)}</details></section>'''


def render(score, *other_cohorts):
    target_ms = score["threshold_ns"] // NS_PER_MS
    labels = ("Correct", f"p99 under {target_ms} ms", f"p99 under {FAST_RESPONSE_MS} ms")
    legends = "".join(f'<li><span style="background:{color}"></span>{label}</li>'
                      for label, color in zip(labels, COLORS, strict=True))
    cohorts = [(f"cohort-{index}", result) for index, result in enumerate((score, *other_cohorts), 1)]
    navigation = ""
    if other_cohorts:
        links = "".join(f'<a href="#{anchor}">{escape(result["cohort"])}</a>' for anchor, result in cohorts)
        navigation = f'<nav aria-label="Benchmark platforms">{links}</nav>'
    sections = "".join(render_cohort(result, labels, anchor) for anchor, result in cohorts)
    title = f'{escape(score["workload"])} runtime leaderboard'
    return f'''<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>''' + '''
<style>
*{box-sizing:border-box}body{margin:0;color:#182b27;background:#f3f6f4;font:16px/1.5 system-ui,sans-serif}
main{max-width:1100px;margin:auto;padding:32px 20px}h1,h2{margin:0 0 .45rem}h1{font-size:clamp(1.85rem,4vw,2.65rem)}
p{margin:0 0 1rem;color:#41574f}section{background:#fff;border:1px solid #d8e2df;border-radius:9px;padding:20px;margin:18px 0}
.plot,.table{overflow:auto}svg{display:block;font:12px system-ui,sans-serif;fill:#182b27}.total{font-weight:750}
table{border-collapse:collapse;width:100%;font-size:.9rem}th,td{padding:.45rem .55rem;text-align:left;border-bottom:1px solid #d8e2df}
.legend{display:flex;flex-wrap:wrap;gap:8px 18px;list-style:none;padding:0}.legend span{display:inline-block;width:.8em;height:.8em;margin-right:.4em}
nav{display:flex;flex-wrap:wrap;gap:8px 18px}nav a{color:#265991}
details{margin:.5rem 0}summary{cursor:pointer;font-weight:650}
</style>
''' + f'''<main><h1>{title}</h1>
<p>{score['deadline_seconds']:g} s query deadline · {score['rss_limit_bytes'] / 1e9:g} GB RSS cap</p>
{navigation}<ul class="legend">{legends}</ul>{sections}
<section><h2>Scoring rules</h2>
<p>Each category is one query operation. All scheduled cases must pass verification to earn any points.
A complete pass earns 1 point. Nearest-rank p99 strictly below {target_ms} ms earns 1 point, and p99 strictly below {FAST_RESPONSE_MS} ms earns 1 more.</p>
<p>With five scheduled cases, the p99 is the slowest response. Operations omitted by every engine are outside the report.</p>
</section></main></html>
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--run", action="append", type=Path, help="saved workload directory; repeat within one platform")
    inputs.add_argument("--cohort", action="append", metavar="NAME=DIR",
                        help="saved workload directory in a named ranking; repeat a name to combine runs within a platform")
    parser.add_argument("--exclude-engine", choices=ENGINES, action="append", default=[], help="repeat to omit engines")
    parser.add_argument("--reference", type=Path, help="expected rows for historical runs without a saved reference")
    parser.add_argument("--latency-target-ms", type=int, default=DEFAULT_LATENCY_TARGET_MS,
                        help="exclusive category p99 cutoff in milliseconds (default: %(default)s)")
    parser.add_argument("--out", type=Path, default=Path("results/leaderboard.html"), help="standalone HTML leaderboard")
    parser.add_argument("--json", type=Path, help="also export the scores and per-operation evidence as JSON")
    args = parser.parse_args()
    if args.latency_target_ms <= 0:
        parser.error("--latency-target-ms must be positive")
    if args.json and args.json.resolve() == args.out.resolve():
        parser.error("The HTML and JSON require different output files")
    try:
        groups = {None: args.run} if args.run else {}
        for item in args.cohort or ():
            name, _, directory = item.partition("=")
            if not name.strip() or not directory:
                raise ValueError("Use --cohort NAME=DIR with a name and saved workload directory")
            groups.setdefault(name.strip(), []).append(Path(directory))
        scores = []
        for name, directories in groups.items():
            report, case_hash = load_cohort(directories, args.reference, args.exclude_engine)
            score = score_workload(report, latency_target_ms=args.latency_target_ms)
            score["cases_sha256"] = case_hash
            if name:
                score["cohort"] = name
            scores.append(score)
        shared = ("workload", "cases_sha256", "reference", "deadline_seconds", "rss_limit_bytes")
        if any(score[key] != scores[0][key] for score in scores[1:] for key in shared):
            raise ValueError("Cohorts must use the same workload, case schedule, expected answers, and caps")
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(render(*scores), encoding="utf-8")
        if args.json:
            args.json.parent.mkdir(parents=True, exist_ok=True)
            data = {"cohorts": scores} if args.cohort else scores[0]
            args.json.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    except (KeyError, OSError, ValueError) as error:
        parser.error(str(error))
    for score in scores:
        if "cohort" in score:
            print(score["cohort"])
        for row in score["standings"]:
            print(f'{row["rank"]}. {NAMES[row["engine"]]} · {row["total"]}/{score["max_points"]}')
    print(args.out)


if __name__ == "__main__":
    main()

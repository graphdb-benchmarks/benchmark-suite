"""Rank comparable saved runs of one workload within each platform cohort."""

import argparse
from html import escape
import json
from math import fsum, log, prod
from pathlib import Path
from statistics import geometric_mean

from .chart import NAMES, cpu_description, latency_label, read_schedule, read_workload, rss_label
from .engines import ENGINES

MAX_POINTS_PER_OPERATION = 3
ZERO_LATENCY_SCORE_NS = 2_500_000_000
FULL_LATENCY_SCORE_NS = 15_000_000
LOG_LATENCY_RANGE = log(ZERO_LATENCY_SCORE_NS / FULL_LATENCY_SCORE_NS)


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


def scoring_latency(p99_ns):
    if p99_ns is None:
        return ZERO_LATENCY_SCORE_NS
    return min(ZERO_LATENCY_SCORE_NS, max(FULL_LATENCY_SCORE_NS, p99_ns))


def score_workload(report):
    engines = {engine["name"]: engine for engine in report["engines"]}
    scores = {name: [] for name in engines}
    common_operations = []
    common_p99s = {name: [] for name in engines}
    categories = {}
    case_count = 0
    for operation, rows in report["aggregates"].items():
        if all(row["outcomes"]["omitted"] == row["cases"] for row in rows):
            continue
        numbers = [item["number"] for item in report["operations"][operation]]
        case_count += len(numbers)
        entries = []
        for row in rows:
            name = row["engine"]
            correct = row["outcomes"]["pass"] == row["cases"]
            recorded = engines[name]["cases"]
            durations = sorted(recorded[number]["duration_ns"] for number in numbers) if correct else []
            p99 = durations[(99 * len(durations) - 1) // 100] if durations else None
            fraction = log(ZERO_LATENCY_SCORE_NS / scoring_latency(p99)) / LOG_LATENCY_RANGE
            entry = {**row, "total": MAX_POINTS_PER_OPERATION * fraction, "p99_ns": p99}
            scores[name].append(entry)
            entries.append(entry)
        if all(row["p99_ns"] is not None for row in entries):
            common_operations.append(operation)
            for row in entries:
                common_p99s[row["engine"]].append(row["p99_ns"])
        categories[operation] = entries

    # A sum of logarithms ranks by the corresponding integer product, so exact ties stay tied.
    ranking = sorted((prod(scoring_latency(row["p99_ns"]) for row in rows), name) for name, rows in scores.items())
    standings = []
    previous = None
    for index, (product, name) in enumerate(ranking, 1):
        if product != previous:
            rank = index
        previous = product
        engine = engines[name]
        p99s = common_p99s[name]
        shared_p99 = None
        if p99s:
            shared_p99 = round(geometric_mean(p99s)) if all(p99s) else 0
        standings.append({"rank": rank, "engine": name, "version": engine["version"], "source": engine["directory"],
                          "deployment": engine.get("deployment", "embedded"),
                          "query_packs": engine.get("query_packs", []), "engine_settings": engine.get("engine_settings", {}),
                          "verified_operations": sum(row["p99_ns"] is not None for row in scores[name]),
                          "total": fsum(row["total"] for row in scores[name]),
                          "common_geomean_p99_ns": shared_p99})
    host = next((engine for engine in report["engines"] if engine.get("platform")), {})
    return {"workload": report["workload"], "reference": report["reference"],
            **({"cpu_ids": report["cpu_ids"]} if "cpu_ids" in report else {}),
            "deadline_seconds": report["deadline_seconds"], "rss_limit_bytes": report["rss_limit_bytes"],
            "platform": host.get("platform"), "python": host.get("python"),
            "scoring": "logarithmic_p99", "zero_latency_score_ns": ZERO_LATENCY_SCORE_NS,
            "full_latency_score_ns": FULL_LATENCY_SCORE_NS, "case_count": case_count,
            "max_points_per_operation": MAX_POINTS_PER_OPERATION,
            "max_points": len(categories) * MAX_POINTS_PER_OPERATION,
            "standings": standings, "categories": categories, "common_operations": common_operations}


def points_label(value):
    return f"{value:.3f}".rstrip("0").rstrip(".")


def score_chart(score):
    left, top, bottom, step = 42, 26, 224, 108
    width = left + step * len(score["standings"]) + 20
    maximum = score["max_points"]
    if not maximum:
        return ""

    def y(points):
        return bottom - (bottom - top) * points / maximum

    svg = [f'<svg viewBox="0 0 {width} 267" width="{width}" role="img" aria-label="Total points by engine">']
    for points in sorted({maximum * i // 4 for i in range(5)}):
        svg.append(f'<path d="M {left} {y(points):.2f} H {width - 12}" stroke="#d8e2df"/>'
                   f'<text x="{left - 8}" y="{y(points) + 4:.2f}" text-anchor="end">{points}</text>')
    for index, row in enumerate(score["standings"]):
        name = NAMES[row["engine"]]
        x = left + (index + 0.5) * step
        bar_top = y(row["total"])
        points = points_label(row["total"])
        if row["total"]:
            svg.append(f'<rect x="{x - 26:.1f}" y="{bar_top:.2f}" width="52" height="{bottom - bar_top:.2f}" fill="#265991">'
                       f'<title>{name} · {points} points</title></rect>')
        svg.append(f'<text x="{x:.1f}" y="{bar_top - 8:.2f}" text-anchor="middle" class="total">{points}</text>'
                   f'<text x="{x:.1f}" y="249" text-anchor="middle">{name}</text>')
    return "".join([*svg, "</svg>"])


def render_cohort(score, anchor):
    heading = escape(score.get("cohort", "Total points"))
    evidence = ""
    if score.get("platform"):
        evidence = f' · {escape(score["platform"])}'
    if score.get("python"):
        evidence += f' · Python {escape(score["python"])}'
    standings = []
    for row in score["standings"]:
        name = f'{NAMES[row["engine"]]} {row["version"] or ""}'.strip()
        shared_p99 = row["common_geomean_p99_ns"]
        geometric_p99 = latency_label(shared_p99) if shared_p99 is not None else "—"
        packs = escape(", ".join(row["query_packs"]) or "Canonical")
        standings.append(f'<tr><th scope="row">{row["rank"]}</th><td>{escape(name)}</td><td>{escape(row["deployment"])}</td>'
                         f'<td>{packs}</td><td class="total">{points_label(row["total"])}</td>'
                         f'<td>{row["verified_operations"]}/{len(score["categories"])}</td><td>{geometric_p99}</td></tr>')
    details = []
    for operation, rows in score["categories"].items():
        entries = []
        for row in rows:
            counts = row["outcomes"]
            time = latency_label(row["median_verified_ns"]) if row["median_verified_ns"] is not None else "—"
            p99 = latency_label(row["p99_ns"]) if row["p99_ns"] is not None else "—"
            peak = rss_label(row["peak_query_rss_bytes"]) if row["peak_query_rss_bytes"] is not None else "—"
            packs = escape(", ".join(row.get("query_packs", ())) or "Canonical")
            entries.append(f'<tr><th scope="row">{NAMES[row["engine"]]}</th><td>{packs}</td><td>{counts["pass"]}/{row["cases"]}</td>'
                           f'<td>{counts["timeout"]}</td><td>{p99}</td><td>{time}</td>'
                           f'<td>{peak} ({row["rss_cases"]}/{row["cases"]})</td>'
                           f'<td>{points_label(row["total"])}</td></tr>')
        details.append(f'<details><summary>{escape(operation)} · {rows[0]["cases"]} scheduled measurements per engine</summary>'
                       '<table><thead><tr><th>Engine</th><th>Query variant</th><th>Verified</th><th>Timeouts</th>'
                       '<th>Verified p99</th><th>Verified median</th><th>Peak RSS (coverage)</th>'
                       '<th>Points</th></tr></thead>'
                       f'<tbody>{"".join(entries)}</tbody></table></details>')
    common = score["common_operations"]
    common_detail = f' ({escape(", ".join(common))})' if common else ""

    return f'''<section id="{anchor}"><h2>{heading}</h2>
<p>{len(score['categories'])} scored operations · {score['max_points']} possible points per engine{evidence}{cpu_description(score)}</p>
<div class="plot">{score_chart(score)}</div>
<div class="table"><table><thead><tr><th>Rank</th><th>Engine</th><th>Deployment</th><th>Query packs</th><th>Total points</th>
<th>Verified operations</th><th>Shared p99 geometric mean</th></tr></thead><tbody>{''.join(standings)}</tbody></table></div>
<p>The geometric mean uses {len(common)} of {len(score['categories'])} scored operations with a verified p99 on every engine{common_detail}.</p>
<details><summary>Points by operation</summary>{''.join(details)}</details></section>'''


def render(score, *other_cohorts):
    zero_latency = latency_label(score["zero_latency_score_ns"])
    full_latency = latency_label(score["full_latency_score_ns"])
    cohorts = [(f"cohort-{index}", result) for index, result in enumerate((score, *other_cohorts), 1)]
    navigation = ""
    if other_cohorts:
        links = "".join(f'<a href="#{anchor}">{escape(result["cohort"])}</a>' for anchor, result in cohorts)
        navigation = f'<nav aria-label="Benchmark platforms">{links}</nav>'
    sections = "".join(render_cohort(result, anchor) for anchor, result in cohorts)
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
nav{display:flex;flex-wrap:wrap;gap:8px 18px}nav a{color:#265991}
details{margin:.5rem 0}summary{cursor:pointer;font-weight:650}
</style>
''' + f'''<main><h1>{title}</h1>
<p>{score['deadline_seconds']:g} s query deadline · {score['rss_limit_bytes'] / 1e9:g} GB RSS cap</p>
{navigation}{sections}
<section><h2>Scoring rules</h2>
<p>All scheduled measurements for an operation must pass verification to earn points.
A verified nearest-rank p99 earns 0 points at {zero_latency} or slower and {score['max_points_per_operation']} points
at {full_latency} or faster. Points increase logarithmically between those endpoints.
Rank uses the sum of unrounded points across operations.</p>
<p>Each operation shows its scheduled measurement count. Below 100 verified measurements, nearest-rank p99 is the maximum.
Operations omitted by every engine are outside the report.</p>
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
    parser.add_argument("--out", type=Path, default=Path("results/leaderboard.html"), help="standalone HTML leaderboard")
    parser.add_argument("--json", type=Path, help="also export the scores and per-operation evidence as JSON")
    args = parser.parse_args()
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
            score = score_workload(report)
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
            print(f'{row["rank"]}. {NAMES[row["engine"]]} · {points_label(row["total"])}/{score["max_points"]}')
    print(args.out)


if __name__ == "__main__":
    main()

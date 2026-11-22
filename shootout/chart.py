"""Render deterministic vertical bar charts from saved benchmark events."""

import argparse
from collections import Counter, defaultdict
from hashlib import sha256
from html import escape
import json
from math import ceil, log1p
from os.path import relpath
from pathlib import Path
from statistics import median
from urllib.parse import quote

from .catalog import load
from .engines import ENGINES
from .run import cases, describe_case, disposition
from .verification import load_reference, rows_digest

COLORS = ("#146a62", "#315cab", "#7453a1", "#a04469", "#586e28", "#00748c", "#895327",
          "#665b3d", "#754880", "#425c81", "#1d6985", "#97322b", "#424694", "#a46820")
NAMES = {"velr": "Velr", "relate": "Relate", "ladybug": "Ladybug", "kuzu": "Kùzu", "grafeo": "Grafeo",
         "arcadedb": "ArcadeDB", "graphdblite": "GraphDBLite", "ryugraph": "RyuGraph", "graphqlite": "GraphQLite",
         "neug": "NeuG", "neo4j": "Neo4j Community", "falkordb": "FalkorDB", "memgraph": "Memgraph", "tugraph": "TuGraph"}
SHORT_NAMES = {"velr": "VE", "relate": "RE", "ladybug": "LA", "kuzu": "KU", "grafeo": "GR",
               "arcadedb": "AR", "graphdblite": "GL", "ryugraph": "RY", "graphqlite": "GQ", "neug": "NG",
               "neo4j": "N4", "falkordb": "FK", "memgraph": "MG", "tugraph": "TU"}
COMMON_SETTINGS = ("suite", "scale", "cases_sha256", "container", "query_timeout", "query_deadline_policy", "rss_limit_bytes",
                   "rss_sample_interval_seconds", "query_rss_policy", "worker_timeout_seconds", "threads", "batch_size", "packs")
FAILURES = {
    "error": "Query error", "mismatch": "Wrong result", "verification_error": "Missing verification evidence",
    "skipped_after_write_failure": "Skipped after a failed write", "missing": "Case did not finish",
    "reference_missing": "No expected answer for the recorded result",
    "fingerprint_differs": "Recorded fingerprint differs from the reference. The log retained only the fingerprint.",
}


def read_json(path):
    return json.loads(path.read_bytes())


def workload_name(settings):
    return f"{settings['suite']}-sf{settings['scale']}"


def read_schedule(directory):
    settings = read_json(directory / "run.json")
    schedule = directory / "cases.jsonl"
    listed = dict(cases(schedule))
    if sha256(schedule.read_bytes()).hexdigest() != settings["cases_sha256"] or len(listed) != settings["case_count"]:
        raise ValueError(f"{directory}: case schedule does not match the saved run settings")
    return settings, listed


def read_answers(directory, path, settings, listed, queries):
    saved = directory / "reference.json"
    if settings.get("reference_sha256"):
        if sha256(saved.read_bytes()).hexdigest() != settings["reference_sha256"]:
            raise ValueError(f"{directory}: saved reference fingerprint differs")
        if path and saved.read_bytes() != path.read_bytes():
            raise ValueError(f"{directory}: chart reference differs from the reference used during replay")
        path = saved
    if path is None:
        return {}, None
    answers = load_reference(path, settings["suite"], settings["scale"], settings["cases_sha256"], listed, queries)
    return answers, {"sha256": sha256(path.read_bytes()).hexdigest(), "source": read_json(path)["source"]}


def outcome(event, item, expected, cap_ns, worker_status):
    status = event.get("status", "missing")
    duration = event.get("duration_ns")
    if status == "timeout":
        if duration != cap_ns:
            raise ValueError("Recorded query timeout differs from the run deadline")
        return "timeout", duration, "External query deadline reached"
    if status == "omitted":
        return "omitted", None, event.get("reason", "Documented omission")
    if status == "verified" and (
        (event.get("verification") in {"reference", "expected"} and expected is not None)
        or (event.get("verification") == "postcondition" and item.get("postcondition"))
    ):
        verdict, reason = "pass", "Verified during replay"
    elif status in {"unverified", "verified"}:
        if expected is None:
            status = "reference_missing"
        elif rows_digest(expected) != event.get("rows_sha256"):
            status = "fingerprint_differs"
        else:
            status = "verified"
        verdict = "pass" if status == "verified" else "fail"
        reason = "Recorded result fingerprint matches the expected rows" if verdict == "pass" else FAILURES[status]
    else:
        verdict = "fail"
        reason = FAILURES[status]
        detail = event.get("message") or (worker_status if status == "missing" else "")
        if detail:
            reason = f"{reason}. {detail}"
    if verdict == "pass" and (not isinstance(duration, int) or not 0 <= duration <= cap_ns):
        raise ValueError("Verified case duration is missing or outside the run deadline")
    return verdict, duration, reason


def engine_results(engine, directory, settings, listed, reference, queries):
    summary_path = directory / "summary.jsonl"
    summaries = {row["engine"]: row for _, row in cases(summary_path)} if summary_path.exists() else {}
    summary = summaries.get(engine, {})
    worker_status = summary.get("status", "No worker summary")
    case_rss = summary.get("case_peak_sampled_query_rss_bytes", {})
    observations, worker, imported = {}, {}, {}
    event_file = directory / f"{engine}.jsonl"
    if event_file.exists():
        for _, event in cases(event_file, allow_interrupted_line=worker_status != "complete"):
            if event["event"] == "worker":
                worker = event
            if event["event"] == "load":
                imported = event
            if event["event"] != "case":
                continue
            number = event["case"]
            if number in observations or number not in listed:
                raise ValueError(f"{event_file}: duplicate or unexpected case {number}")
            expected, query = describe_case(settings["suite"], queries, number, listed[number])
            for field in ("ldbc_op", "params", "canonical_sha256", "executed_sha256"):
                if event.get(field) != expected.get(field):
                    raise ValueError(f"{event_file}: case {number} has different {field}")
            observations[number] = event
    if worker and (worker["engine"], worker["workload"], worker["scale"]) != (engine, settings["suite"], settings["scale"]):
        raise ValueError(f"{event_file}: worker metadata differs from the schedule")
    rows = {}
    cap_ns = settings["query_timeout"] * 1_000_000_000
    for number, item in listed.items():
        planned, _ = describe_case(settings["suite"], queries, number, item)
        event = observations.get(number, planned if planned.get("status") == "omitted" else {})
        expected = reference.get(number, item.get("expected"))
        verdict, duration, reason = outcome(event, item, expected, cap_ns, worker_status)
        rows[number] = {"outcome": verdict, "duration_ns": duration, "reason": reason,
                        "peak_query_rss_bytes": case_rss.get(str(number))}
    return {"name": engine, "version": worker.get("version"), "platform": worker.get("platform"),
            "deployment": worker.get("deployment", "embedded"), "driver_version": worker.get("driver_version"),
            "query_rss_scope": worker.get("query_rss_scope", "worker_process_tree"),
            "python": worker.get("python"), "worker_status": worker_status,
            "directory": f"{directory.parent.name}/{directory.name}", "cases": rows,
            "import": {"loader": imported.get("loader", settings.get("loaders", {}).get(engine)),
                       "duration_ns": imported.get("duration_ns"),
                       "peak_rss_bytes": summary.get("import_peak_sampled_tree_rss_bytes"),
                       "configured_batch_size": settings.get("batch_size"),
                       "importer": imported.get("importer")}}


def read_workload(directory, overrides, reference_path, excluded=()):
    settings, listed = read_schedule(directory)
    suite = settings["suite"]
    canonical = load(suite)
    source = None
    operations = defaultdict(list)
    for number, item in listed.items():
        operation, _ = disposition(suite, item)
        operations[operation].append({"number": number, "params": item["params"]})
    engines = []
    selected = [name for name in dict.fromkeys([*settings["engines"], *overrides]) if name not in excluded]
    if not selected:
        raise ValueError(f"{directory}: select at least one engine for the report")
    unknown = overrides.keys() - set(selected)
    if unknown:
        raise ValueError(f"Engine override is outside the run selection: {', '.join(sorted(unknown))}")
    for name in selected:
        root = overrides.get(name, directory)
        run, _ = read_schedule(root) if root != directory else (settings, listed)
        if name not in run["engines"] or any(run.get(key) != settings.get(key) for key in COMMON_SETTINGS):
            raise ValueError(f"{root}: {name} did not run the same workload under the same limits")
        answers, provenance = read_answers(root, reference_path, run, listed, canonical)
        if engines and source != provenance:
            raise ValueError("Compared engines must use the same expected answers")
        source = provenance
        engines.append(engine_results(name, root, run, listed, answers, load(suite, name, run["packs"])))
    hosts = {(engine["platform"], engine["python"]) for engine in engines if engine["platform"] is not None}
    if len(hosts) > 1:
        raise ValueError("Compared engines must use the same host platform and Python version")
    aggregates = {op: [aggregate(engine, inputs) for engine in engines] for op, inputs in operations.items()}
    return {"workload": workload_name(settings), "suite": suite, "scale": settings["scale"],
            "deadline_seconds": settings["query_timeout"], "rss_limit_bytes": settings["rss_limit_bytes"],
            "reference": source, "operations": dict(operations), "engines": engines, "aggregates": aggregates}


def aggregate(engine, inputs):
    results = [engine["cases"][item["number"]] for item in inputs]
    counts = Counter(result["outcome"] for result in results)
    verified = [result["duration_ns"] for result in results if result["outcome"] == "pass"]
    memory = [row["peak_query_rss_bytes"] for row in results if row.get("peak_query_rss_bytes") is not None]
    return {"engine": engine["name"], "cases": len(results),
            "outcomes": {outcome: counts[outcome] for outcome in ("pass", "fail", "timeout", "omitted")},
            "median_verified_ns": median(verified) if verified else None,
            "peak_query_rss_bytes": max(memory, default=None), "rss_cases": len(memory)}


def latency_label(ns):
    if ns >= 1e9:
        return f"{ns / 1e9:.4g} s"
    if ns >= 1e6:
        return f"{ns / 1e6:.4g} ms"
    if ns >= 1e3:
        return f"{ns / 1e3:.4g} µs"
    return f"{ns} ns"


def rss_label(size):
    return f"{size / 1e9:.3g} GB" if size >= 1e9 else f"{size / 1e6:.3g} MB"


def import_chart(engines, field, label, axis_max, title):
    top, baseline, left = 22, 182, 67
    step = 76
    width = left + step * len(engines) + 10
    if axis_max == 0:
        return '<p class="unavailable">No measurements in these runs.</p>'

    def y(value):
        return baseline - (baseline - top) * value / axis_max

    svg = [f'<svg viewBox="0 0 {width} 226" width="{width}" role="img" aria-label="{title}">']
    for index in range(5):
        value = axis_max * index / 4
        tick = label(value) if value else "0"
        svg.append(f'<path class="grid" d="M {left} {y(value):.2f} H {width - 8}"/>'
                   f'<text x="{left - 7}" y="{y(value) + 4:.2f}" text-anchor="end">{tick}</text>')
    for index, engine in enumerate(engines):
        name = engine["name"]
        value = engine["import"][field]
        caption = label(value) if value is not None else "—"
        x = left + (index + 0.5) * step
        height = y(value) if value is not None else baseline
        loader = escape(engine["import"].get("loader") or "Loader unavailable")
        size = engine["import"]["configured_batch_size"]
        if size is not None and engine["import"]["loader"] != "velr-admin":
            loader += f" · configured batch size {size:,}"
        svg.append(f'<g><title>{NAMES[name]} · {caption} · {loader}</title>')
        if value is not None:
            svg.append(f'<rect x="{x - 17:.1f}" y="{height:.2f}" width="34" height="{baseline - height:.2f}" fill="{COLORS[index]}"/>')
        svg.append(f'<text x="{x:.1f}" y="{height - 6:.2f}" text-anchor="middle">{caption}</text>'
                   f'<text class="engine-label" x="{x:.1f}" y="{baseline + 21}" text-anchor="middle">{SHORT_NAMES[name]}</text></g>')
    return "".join([*svg, "</svg>"])


def import_panel(engines, rss_limit):
    duration = max(engine["import"]["duration_ns"] or 0 for engine in engines)
    peak = max(engine["import"]["peak_rss_bytes"] or 0 for engine in engines)
    time_chart = import_chart(engines, "duration_ns", latency_label, ceil(duration / 60e9) * 60e9, "Import wall time")
    rss_chart = import_chart(engines, "peak_rss_bytes", rss_label, max(peak, rss_limit) if peak else 0, "Import peak RSS")
    return ('<section><h3>Snapshot import</h3><p>Wall time includes preparation, bulk loading, indexes, '
            'and closing the loaded store. Peak RSS covers the same phase, including child processes.</p>'
            f'<div class="comparison"><figure><figcaption>Import wall time</figcaption><div class="plot">{time_chart}</div></figure>'
            f'<figure><figcaption>Import peak RSS</figcaption><div class="plot">{rss_chart}</div></figure></div></section>')


def latency_ticks(cap_ns, y, top):
    ticks = [0, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000, 10_000_000_000]
    return [*(tick for tick in ticks if tick < cap_ns and y(tick) > top + 18), cap_ns]


def aggregate_chart(operation, results, deadline, rss_limit, *, memory=False):
    top, baseline, left = 22, 222, 65
    step = 76
    width = left + step * len(results) + 10
    cap = deadline * 1_000_000_000
    ylabel = "Verified latency (log-scaled)"
    label = latency_label
    if memory:
        peak = max((item["peak_query_rss_bytes"] or 0 for item in results), default=0)
        if peak == 0:
            return '<p class="unavailable">No per-operation RSS samples in these runs.</p>'
        # Use a linear axis spanning the configured cap and any recorded overrun.
        cap = max(rss_limit, ceil(peak / 1e9) * 1_000_000_000 if peak > rss_limit else peak)
        ylabel = "Process-tree RSS"
        label = rss_label

    def y(value):
        fraction = value / cap if memory else log1p(value / 1000) / log1p(cap / 1000)
        return baseline - (baseline - top) * fraction

    unit_ticks = ([cap * index / 4 for index in range(5)] if memory else latency_ticks(cap, y, top))
    svg = [f'<svg viewBox="0 0 {width} 284" width="{width}" role="img" aria-label="{escape(operation)} {ylabel}">']
    for value in unit_ticks:
        tick = label(value) if value else "0"
        svg.append(f'<path class="grid" d="M {left} {y(value):.2f} H {width - 8}"/>'
                   f'<text x="{left - 7}" y="{y(value) + 4:.2f}" text-anchor="end">{tick}</text>')
    if memory and cap > rss_limit:
        svg.append(f'<path class="cap" d="M {left} {y(rss_limit):.2f} H {width - 8}"><title>RSS cap</title></path>')
    for index, result in enumerate(results):
        engine = result["engine"]
        x = left + (index + 0.5) * step
        counts = result["outcomes"]
        duration = result["median_verified_ns"]
        amount = result["peak_query_rss_bytes"] if memory else duration
        caption = label(amount) if amount is not None else "—"
        timeout = counts["timeout"] and not memory
        if timeout:
            amount = cap
            caption = f'{counts["timeout"]}/{result["cases"]} TO'
        footer = f'{result["rss_cases"]}/{result["cases"]} sampled' if memory else (
            f'{counts["pass"]}P {counts["fail"]}F {counts["timeout"]}T' +
            (f' {counts["omitted"]}O' if counts["omitted"] else "")
        )
        median_detail = f'Median {latency_label(duration)} · ' if duration is not None and timeout else ""
        svg.append(f'<g><title>{NAMES[engine]} · {caption} · {median_detail}{footer}</title>')
        if amount is not None:
            height = min(baseline - 1, y(amount))
            color = "#b55708" if timeout else COLORS[index]
            svg.append(f'<rect x="{x - 17:.1f}" y="{height:.2f}" width="34" height="{baseline - height:.2f}" fill="{color}"/>')
            if timeout and duration is not None:
                height = min(baseline - 1, y(duration))
                svg.append(f'<path class="median" d="M {x - 17:.1f} {height:.2f} H {x + 17:.1f}"><title>Verified median</title></path>')
            svg.append(f'<text class="{"inside" if timeout else ""}" x="{x:.1f}" '
                       f'y="{top + 15 if timeout else max(13, height - 6):.2f}" text-anchor="middle">{caption}</text>')
        elif not memory and counts["fail"]:
            svg.append(f'<text class="fail symbol" x="{x:.1f}" y="{baseline - 5}" text-anchor="middle">×</text>')
        else:
            caption = "OMIT" if not memory and counts["omitted"] else "—"
            svg.append(f'<text x="{x:.1f}" y="{baseline - 8}" text-anchor="middle">{caption}</text>')
        if not memory and counts["fail"]:
            svg.append(f'<text class="fail marker" x="{x:.1f}" y="{baseline + 12}" text-anchor="middle">FAIL</text>')
        svg.append(f'<text class="engine-label" x="{x:.1f}" y="{baseline + 31}" text-anchor="middle">{SHORT_NAMES[engine]}</text>'
                   f'<text class="engine-label" x="{x:.1f}" y="{baseline + 48}" text-anchor="middle">{footer}</text></g>')
    return "".join([*svg, "</svg>"])


def aggregate_panel(operation, results, deadline, rss_limit, anchor):
    latency = aggregate_chart(operation, results, deadline, rss_limit)
    memory = aggregate_chart(operation, results, deadline, rss_limit, memory=True)
    return (f'<section id="{anchor}"><h3>{escape(operation)} <small>{results[0]["cases"]} inputs per engine</small></h3>'
            f'<div class="comparison"><figure><figcaption>Median verified latency · log scale</figcaption><div class="plot">{latency}</div></figure>'
            f'<figure><figcaption>Peak RSS</figcaption><div class="plot">{memory}</div></figure></div></section>')


def operation_chart(operation, inputs, engines, deadline, anchor):
    top, baseline, left = 20, 250, 85
    group = max(96, 34 * len(engines))
    width = left + group * len(inputs) + 18
    cap_ns = deadline * 1_000_000_000

    def y(ns):
        return baseline - (baseline - top) * log1p(ns / 1000) / log1p(cap_ns / 1000)

    chart = [f'<svg viewBox="0 0 {width} 310" width="{width}" role="img" aria-label="{escape(operation)} query latencies">',
             '<text x="15" y="137" transform="rotate(-90 15 137)" text-anchor="middle">Latency (log-scaled)</text>']
    for value in latency_ticks(cap_ns, y, top):
        height = y(value)
        label = "0" if value == 0 else latency_label(value)
        chart.append(f'<path class="grid" d="M {left} {height:.2f} H {width - 12}"/>'
                     f'<text x="{left - 8}" y="{height + 4:.2f}" text-anchor="end">{label}</text>')
    details = []
    counts = Counter()
    for index, item in enumerate(inputs):
        center = left + group * (index + 0.5)
        number = item["number"]
        chart.append(f'<text x="{center:.1f}" y="296" text-anchor="middle">Case {number}</text>')
        for position, engine in enumerate(engines):
            x = center + (position - (len(engines) - 1) / 2) * 32
            result = engine["cases"][number]
            verdict = result["outcome"]
            counts[verdict] += 1
            duration = result["duration_ns"]
            label = {"pass": "PASS", "timeout": "TIMEOUT", "fail": "FAIL", "omitted": "OMIT"}[verdict]
            name = NAMES[engine["name"]]
            reason = escape(result["reason"])
            elapsed = latency_label(duration) if duration is not None else "—"
            rss = result.get("peak_query_rss_bytes")
            peak = rss_label(rss) if rss is not None else "—"
            chart.append(f'<text class="engine-label" x="{x:.1f}" y="278" text-anchor="middle">'
                         f'{SHORT_NAMES[engine["name"]]}</text>')
            chart.append(f'<g class="{verdict}"><title>{name} · Case {number} · {label} · {elapsed} · {reason}</title>')
            if verdict in {"pass", "timeout"}:
                height = top if verdict == "timeout" else min(baseline - 1, y(duration))
                color = "#b55708" if verdict == "timeout" else COLORS[position]
                chart.append(f'<rect x="{x - 11:.1f}" y="{height:.2f}" width="22" height="{baseline - height:.2f}" fill="{color}"/>')
                if verdict == "timeout":
                    chart.append(f'<text class="marker" x="{x:.1f}" y="{top + 15}" text-anchor="middle" fill="white">TO</text>')
            else:
                symbol = "×" if verdict == "fail" else "–"
                chart.append(f'<text class="symbol" x="{x:.1f}" y="{baseline - 5}" text-anchor="middle">{symbol}</text>'
                             f'<text class="marker" x="{x:.1f}" y="{baseline + 13}" text-anchor="middle">{label}</text>')
            chart.append('</g>')
            details.append(f'<tr><td>{number}</td><td>{name}</td><td class="{verdict}">{label}</td>'
                           f'<td title="{duration or 0} ns">{elapsed}</td><td>{peak}</td><td>{reason}</td></tr>')
    chart.append('</svg>')
    counts_text = ' · '.join(f'{counts[key]} {label}' for key, label in (
        ("pass", "passed"), ("fail", "failed"), ("timeout", "timed out"), ("omitted", "omitted"),
    ))
    return (f'<section id="{anchor}"><h3>{escape(operation)}</h3><p>{counts_text}</p><div class="plot">{"".join(chart)}</div>'
            f'<details><summary>Case details and inputs</summary><table><thead><tr><th>Case</th><th>Engine</th>'
            f'<th>Outcome</th><th>Recorded time</th><th>Peak RSS</th><th>Evidence</th></tr></thead><tbody>{"".join(details)}</tbody></table>'
            f'<pre>{escape(json.dumps(inputs, indent=2, sort_keys=True))}</pre></details></section>')


def render(reports, missing=(), *, companion, appendix=False):
    blocks = []
    for index, report in enumerate(reports, 1):
        engines = report["engines"]
        deadline = report["deadline_seconds"]
        workload = escape(report["workload"])
        headings = "".join(f'<th>{key}</th>' for key in ("Engine", "Deployment", "Passed", "Failed", "Timed out", "Omitted"))
        if appendix:
            headings += "<th>Run</th>"
        rows, legend = [], []
        for position, engine in enumerate(engines):
            name = f'{NAMES[engine["name"]]} {engine["version"] or ""}'.strip()
            counts = Counter(case["outcome"] for case in engine["cases"].values())
            totals = "".join(f"<td>{counts[key]}</td>" for key in ("pass", "fail", "timeout", "omitted"))
            run_cell = f'<td>{escape(engine["directory"])}</td>' if appendix else ""
            rows.append(f'<tr><td>{escape(name)}</td><td>{escape(engine["deployment"])}</td>{totals}{run_cell}</tr>')
            legend.append(f'<li><i style="background:{COLORS[position]}"></i>{escape(name)} ({SHORT_NAMES[engine["name"]]})</li>')
        if report["reference"]:
            source = report["reference"]["source"]
            evidence = f'Expected answers from {source["engine"]} {source["version"]}.'
        else:
            evidence = 'Expected answers from the saved case schedule.'
        panels = [(f"workload-{index}-operation-{position}", op, inputs)
                  for position, (op, inputs) in enumerate(report["operations"].items(), 1)]
        links = "".join(f'<a href="#{anchor}">{escape(op)}</a>' for anchor, op, _ in panels)
        if appendix:
            charts = (operation_chart(op, inputs, engines, deadline, anchor) for anchor, op, inputs in panels)
            imports = ""
        else:
            charts = (aggregate_panel(op, report["aggregates"][op], deadline, report["rss_limit_bytes"], anchor)
                      for anchor, op, _ in panels)
            imports = import_panel(engines, report["rss_limit_bytes"])
        blocks.append(f'<article><h2>{workload}</h2><p>{deadline:g} s query deadline · '
                      f'{report["rss_limit_bytes"] / 1e9:g} GB RSS cap. {escape(evidence)}</p>'
                      f'<table><thead><tr>{headings}</tr></thead><tbody>{"".join(rows)}</tbody></table>'
                      f'<ul class="legend">{"".join(legend)}</ul>{imports}<nav aria-label="Queries">{links}</nav>'
                      f'{"".join(charts)}</article>')
    unavailable = ''.join(f'<li>{escape(name)} · {escape(reason)}</li>' for name, reason in missing)
    if unavailable:
        blocks.append(f'<h2>Workload preparation failures</h2><ul>{unavailable}</ul>')
    if appendix:
        title = "Cypher benchmark · Case appendix"
        companion_label = "Operation summary"
        intro = "Each input has its recorded outcome, time, evidence, and peak RSS when available."
    else:
        title = "Cypher benchmark · Operation summary"
        companion_label = "Case-level appendix"
        intro = ("Each operation shows one aggregate per engine. Latency bars use the median of verified cases. "
                 "An amber bar reaches the deadline when any case timed out. Its dark line shows the verified median when available. "
                 "FAIL marks any failed case. P, F, T, and O show the pass, fail, timeout, and omission counts. "
                 "Query peak RSS covers the database server and its subprocesses for server deployments. "
                 "For embedded deployments it covers the Python worker and its subprocesses.")
    return f'''<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
''' + '''<style>
*{box-sizing:border-box}body{margin:0;color:#172820;background:#f3f6f2;font:16px/1.5 system-ui,sans-serif}
main{max-width:1280px;margin:auto;padding:36px 24px}h1{font-size:clamp(2rem,4vw,3rem);line-height:1.12;margin:0}
h2,h3{margin:0}h3{font-size:1.4rem}p{margin:.35rem 0 1rem;color:#41574d}
article,section{margin:1.4rem 0;padding:20px;background:#fff;border:1px solid #d6e1d8;border-radius:10px}
section{padding-bottom:15px}.plot{overflow:auto}svg{display:block;font:12px system-ui,sans-serif}svg .grid{stroke:#d6e1d8;fill:none}
table{border-collapse:collapse;width:100%;font-size:.88rem}th,td{text-align:left;padding:.35rem .55rem;border-bottom:1px solid #d6e1d8}
.legend{display:flex;flex-wrap:wrap;gap:8px 20px;list-style:none;padding:0}.legend i{display:inline-block;width:10px;height:13px;margin-right:6px}
.engine-label{font-size:10px;fill:#46594e}nav{display:flex;flex-wrap:wrap;gap:.55rem 1rem}a{color:#146a62}
.marker{font-size:9px;font-weight:750}.symbol{font-size:22px;font-weight:700}.fail{color:#ab2433;fill:#ab2433}
.timeout{color:#9a4607}.pass{color:#146a62}.omitted{color:#4a5750;fill:#4a5750}summary{cursor:pointer}pre{overflow:auto}
.comparison{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;margin-top:10px}
figure{min-width:0;margin:0;padding:12px;border:1px solid #d6e1d8;border-radius:8px}figcaption{font-weight:650}
h3 small{font-size:.68em;font-weight:500;color:#41574d;margin-left:8px}.unavailable{margin-top:16px}
.inside{fill:#fff;font-size:10px;font-weight:700}.median{stroke:#172820;stroke-width:3}.cap{stroke:#b55708;stroke-dasharray:4 3}
@media(max-width:850px){.comparison{grid-template-columns:minmax(0,1fr)}}
</style>
''' + (f'<main><h1>{title}</h1><p>{intro}</p><p><a href="{escape(companion)}">{companion_label}</a></p>\n'
       + "\n".join(blocks) + "\n</main></html>\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, action="append", type=Path, help="saved run or matrix directory; repeatable")
    parser.add_argument("--engine-from", action="append", default=[], metavar="ENGINE=DIR",
                        help="add or override one engine's source in a single workload comparison")
    parser.add_argument("--exclude-engine", choices=ENGINES, action="append", default=[],
                        help="leave an engine out of the generated report; repeatable")
    parser.add_argument("--reference", action="append", type=Path, default=[], help="expected rows for a historical workload")
    parser.add_argument("--out", required=True, type=Path, help="standalone HTML report")
    parser.add_argument("--appendix", type=Path, help="case-level HTML (defaults to <out-stem>-appendix.html)")
    parser.add_argument("--summary", type=Path, help="also write machine-readable aggregates and case verdicts")
    args = parser.parse_args()
    try:
        directories, missing = [], []
        for root in args.run:
            matrix = root / "matrix.json"
            if (root / "run.json").is_file():
                directories.append(root)
            elif matrix.is_file():
                summary = root / "matrix-summary.jsonl"
                statuses = {row["workload"]: row for _, row in cases(summary, allow_interrupted_line=True)} if summary.is_file() else {}
                for job in read_json(matrix)["jobs"]:
                    name = job["workload"]
                    directory = root / name
                    if (directory / "run.json").is_file():
                        directories.append(directory)
                    else:
                        missing.append((name, statuses.get(name, {}).get("message", "Replay setup did not finish")))
            else:
                raise ValueError(f"{root}: no saved run or matrix")
        overrides = {}
        for option in args.engine_from:
            name, _, path = option.partition("=")
            if name in overrides or name not in ENGINES or not path or len(directories) != 1:
                raise ValueError("Use each ENGINE=DIR override once with a single workload")
            overrides[name] = Path(path)
        references = {}
        for path in args.reference:
            key = workload_name(read_json(path))
            if key in references:
                raise ValueError(f"Multiple references supplied for {key}")
            references[key] = path
        reports = []
        for root in directories:
            if not (root / "run.json").is_file():
                raise ValueError(f"{root}: workload did not reach replay setup")
            key = workload_name(read_json(root / "run.json"))
            reports.append(read_workload(root, overrides, references.get(key), args.exclude_engine))
        if unused := references.keys() - {report["workload"] for report in reports}:
            raise ValueError(f"Unused reference for {', '.join(sorted(unused))}")
        appendix = args.appendix or args.out.with_name(f"{args.out.stem}-appendix.html")
        outputs = [path.resolve() for path in (args.out, appendix, args.summary) if path is not None]
        if len(set(outputs)) != len(outputs):
            raise ValueError("Report, appendix and summary require distinct output files")
        args.out.parent.mkdir(parents=True, exist_ok=True)
        appendix.parent.mkdir(parents=True, exist_ok=True)
        appendix_url = quote(relpath(appendix.resolve(), args.out.resolve().parent))
        report_url = quote(relpath(args.out.resolve(), appendix.resolve().parent))
        args.out.write_text(render(reports, missing, companion=appendix_url), encoding="utf-8")
        appendix.write_text(render(reports, missing, companion=report_url, appendix=True), encoding="utf-8")
        if args.summary:
            args.summary.parent.mkdir(parents=True, exist_ok=True)
            args.summary.write_text(json.dumps(reports, indent=2) + "\n", encoding="utf-8")
    except (KeyError, OSError, ValueError) as error:
        parser.error(str(error))
    print(args.out)


if __name__ == "__main__":
    main()

"""Run each engine in its own process and retain per-case evidence."""

from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
import platform
import resource
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import monotonic_ns, perf_counter_ns
from datetime import date, datetime, timedelta

from .catalog import SOURCES, load, parameters
from .datasets import id_indexes, load_snapshot, verify_snapshot
from .engines import Database, version
from .supervision import (
    BYTES_PER_GB, NS_PER_SECOND, QUERY_DEADLINE_POLICY, RSS_SAMPLE_INTERVAL,
    check_rss_monitor, query_window, supervise_worker,
)
from .velr_import import offline_import


def rss_bytes(scope=resource.RUSAGE_SELF):
    size = resource.getrusage(scope).ru_maxrss
    return size if sys.platform == "darwin" else size * 1024


def encode(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return {"duration_seconds": value.total_seconds()}
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"Can't serialize {type(value).__name__}")


def write_line(stream, data):
    print(json.dumps(data, default=encode, sort_keys=True), file=stream, flush=True)


def cases(path, *, allow_interrupted_line=False):
    with path.open("rb") as stream:
        for index, line in enumerate(stream, 1):
            if allow_interrupted_line and not line.endswith(b"\n"):
                break
            if line.strip():
                yield index, json.loads(line)


def bind(query, params):
    names = parameters(query.text)
    missing = names.difference(params)
    if missing:
        raise ValueError(f"Missing parameters for {query.operation}: {', '.join(sorted(missing))}")
    return {name: params[name] for name in sorted(names)}


def disposition(suite, item):
    source = SOURCES[suite]
    template = item["op"]
    official = item.get("ldbc_op")
    omissions = source.get("omissions", {})
    bindings = source.get("bindings", {})
    if item.get("omitted"):
        if official not in omissions or item["omitted"] != omissions[official]:
            raise ValueError(f"Unknown omission for {official}")
        return official, omissions[official]

    reverse = {filename: op for op, filename in bindings.items()}
    expected = reverse.get(template, template)
    if official is not None and official != expected:
        raise ValueError(f"{official} is not mapped to template {template}")
    return expected, source.get("draft_only", {}).get(template) or omissions.get(expected)


def equal_rows(rows, expected, unordered):
    def serialized(row):
        return json.dumps(row, default=encode, sort_keys=True)

    actual_rows = [serialized(row) for row in rows]
    wanted_rows = [serialized(row) for row in expected]
    return Counter(actual_rows) == Counter(wanted_rows) if unordered else actual_rows == wanted_rows


def describe_case(suite, queries, number, case):
    official, omission = disposition(suite, case)
    query = queries.get(case["op"])
    event = {"event": "case", "case": number, "ldbc_op": official, "params": case["params"]}
    if query is not None:
        event.update(query.provenance())
    approved_pack = SOURCES[suite].get("replacements", {}).get(official)
    active_pack = query.deviation["pack"] if query is not None and query.deviation else None
    if omission and (approved_pack is None or active_pack != approved_pack or case.get("omitted")):
        event.update(status="omitted", reason=omission)
    return event, query


def run_cases(db, suite, queries, path, events, channel, start_case):
    diverged = False
    for number, case in cases(path):
        if number < start_case:
            continue
        event, query = describe_case(suite, queries, number, case)
        if event.get("status") == "omitted":
            write_line(events, event)
            continue
        if diverged:
            write_line(events, {**event, "status": "skipped_after_write_failure"})
            continue
        write_line(events, {"event": "case_start", "case": number, "op": query.operation})
        elapsed = None
        start = perf_counter_ns()
        try:
            bound = bind(query, case["params"])
            with query_window(channel):
                start = perf_counter_ns()
                try:
                    result = db.execute(query.text, bound, write=query.kind == "write")
                finally:
                    elapsed = perf_counter_ns() - start
            encoded = json.dumps(result, default=encode, sort_keys=True)
            status = "unverified"
            if "expected" in case:
                status = "verified" if equal_rows(result, case["expected"], case.get("unordered", False)) else "mismatch"
            event.update(status=status, duration_ns=elapsed, row_count=len(result),
                         rows_sha256=sha256(encoded.encode()).hexdigest())
            if status == "mismatch":
                event.update(expected=case["expected"], actual=result)
        except Exception as error:
            event.update(status="error", error_type=type(error).__name__, message=str(error),
                         duration_ns=elapsed if elapsed is not None else perf_counter_ns() - start)
        diverged = query.kind != "read" and event["status"] in {"error", "mismatch"}
        write_line(events, event)


def prepare_snapshot(config, events, path):
    engine, threads = config["engine"], config["threads"]
    start = perf_counter_ns()
    empty_path = path.parent / "empty" if engine == "velr" else path
    with Database(engine, empty_path, threads) as db:
        opened = perf_counter_ns()
        if db.execute("RETURN 1 AS n") != [[1]]:
            raise ValueError("First query returned an unexpected result")
        first = perf_counter_ns()
        write_line(events, {"event": "empty_startup", "open_ns": opened - start,
                            "first_result_ns": first - opened, "peak_rss_bytes": rss_bytes(),
                            "launch_to_first_result_ns": first - config["launch_ns"]})
        start = perf_counter_ns()
        if engine != "velr":
            counts = load_snapshot(db, config["suite"], Path(config["dataset"]), config["batch_size"])
    importer = None
    if engine == "velr":
        start = perf_counter_ns()
        counts, importer = offline_import(config["suite"], Path(config["dataset"]), path)
        importer["child_peak_rss_bytes"] = rss_bytes(resource.RUSAGE_CHILDREN)
        with Database(engine, path, threads) as db:
            id_indexes(db, sorted(label for kind, label in counts if kind == "node"))
    write_line(events, {"event": "load", "duration_ns": perf_counter_ns() - start,
                        "peak_rss_bytes": rss_bytes(), "importer": importer,
                        "counts": {f"{kind}:{label}": count for (kind, label), count in counts.items()}})
    return counts


def worker(config):
    engine = config["engine"]
    queries = load(config["suite"], engine, config["packs"])
    path = Path(config["workdir"]) / "database"
    start_case = config["start_case"]
    mode = "x" if start_case == 1 else "a"
    with socket.socket(fileno=config["channel"]) as channel, Path(config["events"]).open(mode, encoding="utf-8") as events:
        if start_case == 1:
            write_line(events, {"event": "worker", "engine": engine, "version": version(engine),
                                "python": platform.python_version(), "platform": platform.platform(),
                                "requested_threads": config["threads"], "baseline_peak_rss_bytes": rss_bytes(),
                                "workload": config["suite"], "scale": config["scale"]})
            counts = prepare_snapshot(config, events, path)
        start = perf_counter_ns()
        with Database(engine, path, config["threads"]) as db:
            opened = perf_counter_ns()
            if start_case == 1:
                if not db.execute("MATCH (n:Person) RETURN n.id AS id LIMIT 1"):
                    raise ValueError("Reopened snapshot contains no Person")
                write_line(events, {"event": "reopen", "open_ns": opened - start,
                                    "first_result_ns": perf_counter_ns() - opened})
                verify_snapshot(db, counts)
                write_line(events, {"event": "snapshot_verified"})
            else:
                write_line(events, {"event": "resume", "case": start_case, "open_ns": opened - start})
            run_cases(db, config["suite"], queries, Path(config["cases"]), events, channel, start_case)
        write_line(events, {"event": "complete", "peak_rss_bytes": rss_bytes()})


def engine_order(engines):
    return sorted(engines, key=lambda name: name != "velr")


def observed_cases(events, status):
    observed = Counter()
    last_case = None
    if events.exists():
        for _, event in cases(events, allow_interrupted_line=status != "complete"):
            if event["event"] == "case_start":
                last_case = {"op": event["op"], "case": event["case"]}
            elif event["event"] == "case":
                observed[event["status"]] += 1
                last_case = None
    return observed, last_case


def record_timeout(config, events, queries, listed, number):
    event, query = describe_case(config["suite"], queries, number, listed[number])
    with events.open("a", encoding="utf-8") as stream:
        write_line(stream, {**event, "status": "timeout", "error_type": "QueryDeadlineExceeded",
                            "message": "External query deadline exceeded",
                            "duration_ns": config["query_timeout"] * NS_PER_SECOND})
        if query.kind != "read":
            for following, case in listed.items():
                if following > number:
                    skipped, _ = describe_case(config["suite"], queries, following, case)
                    skipped.setdefault("status", "skipped_after_write_failure")
                    write_line(stream, skipped)
    return query.kind == "read"


def replay_engine(config, queries, listed, log):
    start_case = 1
    peak_rss = 0
    deadline_stops = 0
    deadline = monotonic_ns() + config["worker_timeout_seconds"] * NS_PER_SECOND
    events = Path(config["events"])
    while monotonic_ns() < deadline:
        parent, child = socket.socketpair(type=socket.SOCK_DGRAM)
        with parent, child:
            job = json.dumps({**config, "start_case": start_case, "channel": child.fileno(),
                              "launch_ns": perf_counter_ns()})
            with subprocess.Popen([sys.executable, "-m", "shootout", "_worker", job],
                                  stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                                  pass_fds=(child.fileno(),)) as process:
                child.close()
                status, returncode, attempt_peak = supervise_worker(
                    process, timeout=max(0, deadline - monotonic_ns()) / NS_PER_SECOND,
                    rss_limit_bytes=config["rss_limit_bytes"], channel=parent,
                    query_timeout=config["query_timeout"],
                )
        peak_rss = max(peak_rss, attempt_peak)
        if status != "query_timeout":
            return status, returncode, peak_rss, deadline_stops
        deadline_stops += 1
        _, interrupted = observed_cases(events, status)
        if interrupted is None:
            raise RuntimeError("Deadline exceeded without an active case")
        number = interrupted["case"]
        if not record_timeout(config, events, queries, listed, number) or number == max(listed):
            return "complete", None, peak_rss, deadline_stops
        start_case = number + 1
    return "worker_timeout", None, peak_rss, deadline_stops


def orchestrate(args):
    suite, case_path = args.suite, args.cases
    engines = engine_order(args.engine)
    baselines = load(suite)
    listed = dict(cases(case_path))
    if not listed:
        raise ValueError("Case file is empty")
    for i, item in listed.items():
        if "op" not in item or not isinstance(item.get("params"), dict):
            raise ValueError(f"Invalid case on line {i}")
        _, omission = disposition(suite, item)
        if item["op"] not in baselines and not omission:
            raise ValueError(f"Unknown template on line {i}: {item['op']}")
    queries = {engine: load(suite, engine, args.pack) for engine in engines}
    check_rss_monitor()
    out = args.out.resolve()
    out.mkdir(parents=True)
    data = case_path.read_bytes()
    saved_cases = out / "cases.jsonl"
    saved_cases.write_bytes(data)
    common = {
        "suite": suite, "scale": args.scale, "cases": str(saved_cases),
        "cases_sha256": sha256(data).hexdigest(), "dataset": str(args.dataset.resolve()),
        "packs": args.pack, "batch_size": args.batch_size, "threads": args.threads,
        "case_count": len(listed), "worker_timeout_seconds": args.timeout, "query_timeout": args.query_timeout,
        "query_deadline_policy": QUERY_DEADLINE_POLICY,
        "rss_limit_bytes": args.rss_limit_bytes, "rss_sample_interval_seconds": RSS_SAMPLE_INTERVAL,
    }
    (out / "run.json").write_text(json.dumps({**common, "engines": engines}, indent=2) + "\n")

    failures = False
    for engine in engines:
        events = out / f"{engine}.jsonl"
        print(f"Running {suite} scale {args.scale} with {engine}", flush=True)
        start = perf_counter_ns()
        with TemporaryDirectory(prefix=f"{engine}-", dir=out) as scratch, (out / f"{engine}.log").open("w", encoding="utf-8") as log:
            config = {**common, "engine": engine, "events": str(events), "workdir": scratch}
            status, returncode, peak_rss, deadline_stops = replay_engine(config, queries[engine], listed, log)
        observed, last_case = observed_cases(events, status)
        with (out / "summary.jsonl").open("a", encoding="utf-8") as stream:
            write_line(stream, {"engine": engine, "status": status, "returncode": returncode,
                                "worker_wall_ns": perf_counter_ns() - start, "cases": observed,
                                "rss_limit_bytes": args.rss_limit_bytes, "peak_sampled_tree_rss_bytes": peak_rss,
                                "interrupted_case": last_case, "query_deadline_stops": deadline_stops})
        detail = ""
        if status == "worker_rss_limit":
            detail = f" ({peak_rss / BYTES_PER_GB:g} GB RSS, limit {args.rss_limit_bytes / BYTES_PER_GB:g} GB)"
        print(f"{engine}: {status}{detail} {dict(observed)}", flush=True)
        failures |= status != "complete"
    return int(failures)

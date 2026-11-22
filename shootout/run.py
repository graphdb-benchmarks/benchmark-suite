"""Run each engine in its own process and retain per-case evidence."""

from collections import Counter
from contextlib import ExitStack
from hashlib import sha256
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import monotonic_ns, perf_counter_ns

from .bulk_import import LOAD_METHODS, load_snapshot
from .catalog import SOURCES, load, parameters
from .datasets import first_person_id, id_indexes, verify_snapshot
from .engines import Database, version
from .server_import import import_falkordb, import_memgraph, import_tugraph
from .servers import SERVER_VERSIONS, import_neo4j, validate_server_platform
from .supervision import (
    BYTES_PER_GB, IMPORT_RSS_KEY, NS_PER_SECOND, QUERY_DEADLINE_POLICY, QUERY_RSS_POLICY, RSS_SAMPLE_INTERVAL,
    check_rss_monitor, import_window, query_window, supervise_worker,
)
from .velr_import import offline_import
from .verification import answer_rows, encode, equal_rows, load_reference, rows_digest


def rss_bytes(scope=resource.RUSAGE_SELF):
    size = resource.getrusage(scope).ru_maxrss
    return size if sys.platform == "darwin" else size * 1024


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


def run_cases(db, suite, queries, listed, answers, events, channel, start_case):
    diverged = False
    for number, case in listed.items():
        if number < start_case:
            continue
        event, query = describe_case(suite, queries, number, case)
        if event.get("status") == "omitted":
            write_line(events, event)
            continue
        if diverged:
            write_line(events, {**event, "status": "skipped_after_write_failure"})
            continue
        is_write = query.kind != "read"
        expected = answers.get(number, case.get("expected"))
        postcondition = case.get("postcondition")
        has_answer = answer_rows(expected)
        if is_write:
            has_answer = (isinstance(postcondition, dict) and isinstance(postcondition.get("query"), str)
                          and bool(postcondition["query"].strip()) and answer_rows(postcondition.get("expected")))
        if not has_answer:
            message = "Missing expected graph state for write" if is_write else "Missing expected result rows"
            write_line(events, {**event, "status": "verification_error", "message": message})
            diverged = is_write
            continue
        write_line(events, {"event": "case_start", "case": number, "op": query.operation})
        elapsed = None
        start = perf_counter_ns()
        try:
            bound = bind(query, case["params"])
            with query_window(channel, number, rss_pid=getattr(db, "query_rss_pid", 0)):
                start = perf_counter_ns()
                try:
                    result = db.execute(query.text, bound, write=is_write)
                finally:
                    elapsed = perf_counter_ns() - start
            event.update(duration_ns=elapsed, row_count=len(result), rows_sha256=rows_digest(result))
            checks_result = expected is None or equal_rows(
                result, expected, case.get("unordered", False), suite=suite, operation=query.operation,
            )
            event["status"] = "verified" if checks_result else "mismatch"
            if not checks_result:
                event.update(expected=expected, actual=result)
            elif is_write:
                with query_window(channel):
                    observed = db.execute(postcondition["query"], postcondition.get("params", {}))
                if not equal_rows(observed, postcondition["expected"], postcondition.get("unordered", False)):
                    event.update(status="mismatch", message="Write postcondition differed",
                                 expected=postcondition["expected"], actual=observed)
            if event["status"] == "verified":
                event["verification"] = "postcondition" if is_write else "reference" if number in answers else "expected"
        except Exception as error:
            event.update(status="error", error_type=type(error).__name__, message=str(error),
                         duration_ns=elapsed if elapsed is not None else perf_counter_ns() - start)
        diverged = is_write and event["status"] != "verified"
        write_line(events, event)


def prepare_snapshot(config, events, path, channel):
    engine, threads = config["engine"], config["threads"]
    offline = {"velr": offline_import, "neo4j": import_neo4j, "tugraph": import_tugraph}.get(engine)
    server_loader = {"falkordb": import_falkordb, "memgraph": import_memgraph}.get(engine)
    empty_path = path.parent / "empty" if offline else path
    importer = None
    with ExitStack() as importing:
        start = perf_counter_ns()
        with Database(engine, empty_path, threads) as db:
            opened = perf_counter_ns()
            if db.execute("RETURN 1 AS n") != [[1]]:
                raise ValueError("First query returned an unexpected result")
            first = perf_counter_ns()
            write_line(events, {"event": "empty_startup", "open_ns": opened - start,
                                "first_result_ns": first - opened, "peak_rss_bytes": rss_bytes(),
                                "launch_to_first_result_ns": first - config["launch_ns"]})
            if offline is None:
                importing.enter_context(import_window(channel))
                start = perf_counter_ns()
                if server_loader:
                    counts, importer = server_loader(db, config["suite"], Path(config["dataset"]),
                                                    config["batch_size"], path.parent)
                else:
                    counts = load_snapshot(db, config["suite"], Path(config["dataset"]), config["batch_size"],
                                           workdir=path.parent)
        if offline:
            importing.enter_context(import_window(channel))
            start = perf_counter_ns()
            counts, importer = offline(config["suite"], Path(config["dataset"]), path)
            importer["child_peak_rss_bytes"] = rss_bytes(resource.RUSAGE_CHILDREN)
            with Database(engine, path, threads) as db:
                id_indexes(db, sorted(label for kind, label in counts if kind == "node"))
        elapsed = perf_counter_ns() - start
    write_line(events, {"event": "load", "duration_ns": elapsed, "loader": LOAD_METHODS[engine],
                        "peak_rss_bytes": rss_bytes(), "importer": importer,
                        "counts": {f"{kind}:{label}": count for (kind, label), count in counts.items()}})
    return counts


def worker(config):
    engine = config["engine"]
    suite = config["suite"]
    queries = load(suite, engine, config["packs"])
    listed = dict(cases(Path(config["cases"])))
    answers = load_reference(Path(config["reference"]), suite, config["scale"], config["cases_sha256"], listed,
                             load(suite)) if config.get("reference") else {}
    path = Path(config["workdir"]) / "database"
    start_case = config["start_case"]
    mode = "x" if start_case == 1 else "a"
    with socket.socket(fileno=config["channel"]) as channel, Path(config["events"]).open(mode, encoding="utf-8") as events:
        if start_case == 1:
            driver_version = version(engine)
            deployment = {"deployment": "embedded"}
            if engine in SERVER_VERSIONS:
                deployment = {"deployment": "server", "driver_version": driver_version,
                              "query_rss_scope": "server_process_tree"}
            write_line(events, {"event": "worker", "engine": engine,
                                "version": SERVER_VERSIONS.get(engine, driver_version), **deployment,
                                "python": platform.python_version(), "platform": platform.platform(),
                                "requested_threads": config["threads"], "baseline_peak_rss_bytes": rss_bytes(),
                                "workload": config["suite"], "scale": config["scale"]})
            counts = prepare_snapshot(config, events, path, channel)
            person_id = first_person_id(config["suite"], Path(config["dataset"]))
        start = perf_counter_ns()
        with Database(engine, path, config["threads"]) as db:
            opened = perf_counter_ns()
            if start_case == 1:
                first = db.execute("MATCH (n:Person {id: $id}) RETURN n.id AS id", {"id": person_id})
                if first != [[person_id]]:
                    raise ValueError("Reopened snapshot did not return the expected Person")
                write_line(events, {"event": "reopen", "open_ns": opened - start,
                                    "first_result_ns": perf_counter_ns() - opened})
                verification = verify_snapshot(db, counts, path)
                write_line(events, {"event": "snapshot_verified", "method": verification})
            else:
                write_line(events, {"event": "resume", "case": start_case, "open_ns": opened - start})
            run_cases(db, suite, queries, listed, answers, events, channel, start_case)
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


def replay_engine(config, queries, listed, log, case_rss):
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
                    query_timeout=config["query_timeout"], case_rss=case_rss,
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
    validate_server_platform(engines)
    baselines = load(suite)
    data = case_path.read_bytes()
    case_hash = sha256(data).hexdigest()
    listed = dict(cases(case_path))
    if not listed:
        raise ValueError("Case file is empty")
    for i, item in listed.items():
        if "op" not in item or not isinstance(item.get("params"), dict):
            raise ValueError(f"Invalid case on line {i}")
        if "expected" in item and not answer_rows(item["expected"]):
            raise ValueError(f"Expected result must be an array of rows on line {i}")
        _, omission = disposition(suite, item)
        if item["op"] not in baselines and not omission:
            raise ValueError(f"Unknown template on line {i}: {item['op']}")
    queries = {engine: load(suite, engine, args.pack) for engine in engines}
    reference = getattr(args, "reference", None)
    if reference:
        load_reference(reference, suite, args.scale, case_hash, listed, baselines)
    check_rss_monitor()
    out = args.out.resolve()
    out.mkdir(parents=True)
    saved_cases = out / "cases.jsonl"
    saved_cases.write_bytes(data)
    common = {
        "suite": suite, "scale": args.scale, "cases": str(saved_cases),
        "cases_sha256": case_hash, "dataset": str(args.dataset.resolve()),
        "packs": args.pack, "batch_size": args.batch_size, "threads": args.threads,
        "case_count": len(listed), "worker_timeout_seconds": args.timeout, "query_timeout": args.query_timeout,
        "query_deadline_policy": QUERY_DEADLINE_POLICY,
        "query_rss_policy": QUERY_RSS_POLICY,
        "query_rss_scope": {engine: "server_process_tree" if engine in SERVER_VERSIONS
                            else "worker_process_tree" for engine in engines},
        "rss_limit_bytes": args.rss_limit_bytes, "rss_sample_interval_seconds": RSS_SAMPLE_INTERVAL,
    }
    if reference:
        saved_reference = out / "reference.json"
        reference_data = reference.read_bytes()
        saved_reference.write_bytes(reference_data)
        common.update(reference=str(saved_reference), reference_sha256=sha256(reference_data).hexdigest())
    if image := os.environ.get("SHOOTOUT_CONTAINER_IMAGE"):
        common["container"] = {
            "image": image,
            "platform": os.environ["SHOOTOUT_CONTAINER_PLATFORM"],
            "cpus": len(os.sched_getaffinity(0)),
            "memory_bytes": os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"),
        }
    settings = {**common, "engines": engines, "loaders": {engine: LOAD_METHODS[engine] for engine in engines}}
    (out / "run.json").write_text(json.dumps(settings, indent=2) + "\n")

    if image:
        with TemporaryDirectory(prefix="shootout-") as workspace:
            dataset = shutil.copytree(common["dataset"], Path(workspace) / "dataset")
            return run_engines(engines, {**common, "dataset": str(dataset)}, queries, listed, out, workspace)
    return run_engines(engines, common, queries, listed, out, out)


def run_engines(engines, common, queries, listed, out, workspace):
    failures = False
    rss_limit = common["rss_limit_bytes"]
    for engine in engines:
        events = out / f"{engine}.jsonl"
        case_rss = {}
        print(f"Running {common['suite']} scale {common['scale']} with {engine}", flush=True)
        start = perf_counter_ns()
        with TemporaryDirectory(prefix=f"{engine}-", dir=workspace) as scratch, (out / f"{engine}.log").open("w", encoding="utf-8") as log:
            config = {**common, "engine": engine, "events": str(events), "workdir": scratch}
            status, returncode, peak_rss, deadline_stops = replay_engine(config, queries[engine], listed, log, case_rss)
        observed, last_case = observed_cases(events, status)
        import_peak = case_rss.pop(IMPORT_RSS_KEY, None)
        with (out / "summary.jsonl").open("a", encoding="utf-8") as stream:
            write_line(stream, {"engine": engine, "status": status, "returncode": returncode,
                                "worker_wall_ns": perf_counter_ns() - start, "cases": observed,
                                "rss_limit_bytes": rss_limit, "peak_sampled_tree_rss_bytes": peak_rss,
                                "import_peak_sampled_tree_rss_bytes": import_peak,
                                "case_peak_sampled_query_rss_bytes": case_rss,
                                "interrupted_case": last_case, "query_deadline_stops": deadline_stops})
        detail = ""
        if status == "worker_rss_limit":
            detail = f" ({peak_rss / BYTES_PER_GB:g} GB RSS, limit {rss_limit / BYTES_PER_GB:g} GB)"
        print(f"{engine}: {status}{detail} {dict(observed)}", flush=True)
        failures |= status != "complete"
    return int(failures)

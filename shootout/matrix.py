"""Run selected full-scale workloads through the existing replay runner."""

from argparse import Namespace
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

from . import fetch
from .cases import write_case_file
from .catalog import SOURCES
from .run import engine_order, orchestrate, write_line
from .supervision import QUERY_DEADLINE_POLICY, RSS_SAMPLE_INTERVAL

WORKLOADS = {f"{suite}-sf{scale}": (suite, scale) for suite in SOURCES for scale in ("1", "10")}


def published_operations(suite):
    source = SOURCES[suite]
    bindings = source.get("bindings", {})
    originals = set(source["operations"]).difference(source.get("draft_only", {}), bindings.values())
    return originals.union(bindings, source.get("omissions", {}))


def make_plan(args):
    workloads = list(WORKLOADS) if args.all_workloads else args.workload
    if len(set(workloads)) != len(workloads):
        raise ValueError("remove duplicate --workload options")
    requested = list(dict.fromkeys(args.op or ()))
    supported = set().union(*(published_operations(WORKLOADS[name][0]) for name in workloads))
    if unknown := set(requested) - supported:
        raise ValueError(f"unknown operation for the selected workloads: {', '.join(sorted(unknown))}")
    if not args.mixed and any(op.startswith(("IU", "TW", "TRW")) for op in requested):
        raise ValueError("write operations require --mixed")

    destination = args.out or Path("results/all" if args.all_workloads else "results/selected")
    jobs = []
    for name in workloads:
        suite, scale = WORKLOADS[name]
        operations = [op for op in requested if op in published_operations(suite)]
        if requested and not operations:
            raise ValueError(f"select at least one operation for {name}")
        directory = args.data_root / suite
        dataset = directory / f"sf{scale}"
        params = directory / (f"sf{scale}_params" if suite == "snb" else f"sf{scale}_read_params")
        updates = directory / f"sf{scale}_updates" if suite == "snb" else dataset / "incremental"
        jobs.append({
            "workload": name, "suite": suite, "scale": scale, "operations": operations,
            "dataset": str(dataset), "params": str(params),
            "updates": str(updates) if args.mixed else None, "out": str(destination / name),
        })
    return {
        "out": str(destination), "engines": engine_order(args.engine), "packs": args.pack,
        "per_read": args.per_read, "max_updates": args.max_updates, "mixed": args.mixed,
        "skip_fetch": args.skip_fetch, "batch_size": args.batch_size, "threads": args.threads,
        "query_timeout": args.query_timeout, "worker_timeout": args.timeout,
        "query_deadline_policy": QUERY_DEADLINE_POLICY,
        "rss_limit_bytes": args.rss_limit_bytes, "rss_sample_interval_seconds": RSS_SAMPLE_INTERVAL,
        "jobs": jobs,
    }


def run_matrix(args, plan):
    destination = Path(plan["out"])
    destination.mkdir(parents=True)
    (destination / "matrix.json").write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")
    failed = False
    with (destination / "matrix-summary.jsonl").open("x", encoding="utf-8") as summary:
        with TemporaryDirectory(prefix="cases-", dir=destination) as scratch:
            case_file = Path(scratch) / "cases.jsonl"
            for job in plan["jobs"]:
                print(f"Preparing {job['workload']}", flush=True)
                report = {"workload": job["workload"], "out": job["out"]}
                try:
                    suite, scale = job["suite"], job["scale"]
                    if not args.skip_fetch:
                        archives = args.data_root / "downloads"
                        if suite == "snb":
                            fetch.snb(scale, args.data_root / suite, archives, args.mixed)
                        else:
                            fetch.finbench(scale, args.data_root / suite, archives)
                    dataset = Path(job["dataset"])
                    write_case_file(
                        suite, dataset, Path(job["params"]), case_file,
                        per_read=args.per_read, updates=Path(job["updates"]) if job["updates"] else None,
                        max_updates=args.max_updates, operations=job["operations"],
                    )
                    result = orchestrate(Namespace(
                        suite=suite, scale=scale, dataset=dataset, cases=case_file, out=Path(job["out"]),
                        engine=plan["engines"], pack=args.pack, batch_size=args.batch_size, threads=args.threads,
                        query_timeout=args.query_timeout, timeout=args.timeout,
                        rss_limit_bytes=plan["rss_limit_bytes"],
                    ))
                    report["status"] = "worker_failure" if result else "complete"
                except Exception as error:
                    report.update(status="error", error_type=type(error).__name__, message=str(error))
                    print(f"{job['workload']}: {error}", file=sys.stderr, flush=True)
                failed |= report["status"] != "complete"
                write_line(summary, report)
    return int(failed)

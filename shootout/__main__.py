"""Command-line interface for the Cypher benchmark."""

import argparse
from decimal import Decimal, DecimalException
import json
from pathlib import Path
import sys

from . import fetch
from .cases import write_case_file
from .catalog import SOURCES, load
from .engines import DEFAULT_ENGINES, ENGINES, PINS
from .matrix import WORKLOADS, make_plan, run_matrix
from .reference import build_reference
from .run import orchestrate, worker, write_line
from .servers import fetch_neo4j
from .supervision import BYTES_PER_GB, DEFAULT_RSS_LIMIT_BYTES


def positive(text):
    number = int(text)
    if number < 1:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return number


def nonnegative(text):
    number = int(text)
    if number < 0:
        raise argparse.ArgumentTypeError("expected a nonnegative integer")
    return number


def rss_limit(text):
    try:
        number = int(Decimal(text) * BYTES_PER_GB)
    except (DecimalException, OverflowError, ValueError) as error:
        raise argparse.ArgumentTypeError("expected a positive RSS limit in GB") from error
    if number < 1:
        raise argparse.ArgumentTypeError("expected a positive RSS limit in GB")
    return number


def make_cases(args):
    write_case_file(
        args.suite, args.dataset, args.params, args.out, per_read=args.per_read,
        updates=args.updates, max_updates=args.max_updates, operations=args.op,
    )
    print(args.out)


def add_case_options(parser):
    parser.add_argument("--max-updates", type=nonnegative, default=200,
                        help="maximum number of source update events before filtering")
    parser.add_argument("--op", action="append", help="keep only the named workload operation; repeatable")
    parser.add_argument("--per-read", type=positive, default=5, help="parameters per read operation")


def add_run_options(parser):
    parser.add_argument("--engine", choices=ENGINES, action="append", help="repeat to select engines, including opt-in candidates")
    parser.add_argument("--pack", action="append", default=[], help="opt in to a deviation pack; repeatable")
    parser.add_argument("--batch-size", type=positive, default=5000)
    parser.add_argument("--threads", type=positive, default=1, help="query thread limit for typed engines")
    parser.add_argument("--query-timeout", type=positive, default=30, help="external deadline in seconds for each query (default: 30)")
    parser.add_argument("--timeout", type=positive, default=7200, help="wall-clock limit in seconds per engine, including restarts")
    parser.add_argument("--rss-limit-gb", dest="rss_limit_bytes", type=rss_limit, default=DEFAULT_RSS_LIMIT_BYTES,
                        metavar="GB", help="combined worker and child RSS limit in decimal GB (default: 15)")


def select_engines(args, parser):
    args.engine = args.engine or list(DEFAULT_ENGINES)
    if len(set(args.engine)) != len(args.engine):
        parser.error("remove duplicate --engine options")


def catalog(args):
    for suite in SOURCES:
        for query in load(suite, args.engine, args.pack).values():
            write_line(sys.stdout, {"suite": suite, "kind": query.kind, **query.provenance()})
        for op, reason in SOURCES[suite].get("omissions", {}).items():
            write_line(sys.stdout, {"suite": suite, "ldbc_op": op, "status": "omitted", "reason": reason})
        for op, reason in SOURCES[suite].get("draft_only", {}).items():
            write_line(sys.stdout, {"suite": suite, "template": op, "status": "draft_only", "reason": reason})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    show = commands.add_parser("catalog", help="verify canonical query hashes and print provenance")
    show.add_argument("--engine", choices=ENGINES)
    show.add_argument("--pack", action="append", default=[])
    show.set_defaults(action=catalog)

    pins = commands.add_parser("versions", help="print pinned PyPI engine releases")
    pins.set_defaults(action=lambda _: print(json.dumps(PINS, indent=2)))

    get = commands.add_parser("fetch", help="download benchmark data or the pinned Neo4j distribution")
    get.add_argument("dataset", choices=("fixtures", "snb", "finbench", "neo4j"))
    get.add_argument("scale", nargs="?", choices=("1", "10"))
    get.add_argument("--updates", action="store_true", help="fetch the one-partition SNB update streams")
    get.add_argument("--root", type=Path, default=Path("data"))
    get.add_argument("--archives", type=Path, default=Path("data/downloads"))

    def download(args):
        if args.dataset == "fixtures":
            fetch.fixtures(args.root / "fixtures", args.archives)
        elif args.dataset == "neo4j":
            fetch_neo4j(args.root / "servers", args.archives)
        elif not args.scale:
            get.error("SNB and FinBench need scale 1 or 10")
        elif args.dataset == "snb":
            fetch.snb(args.scale, args.root / "snb", args.archives, args.updates)
        else:
            fetch.finbench(args.scale, args.root / "finbench", args.archives)

    get.set_defaults(action=download)

    sequence = commands.add_parser("cases", help="prepare a shared ordered JSONL replay")
    sequence.add_argument("suite", choices=SOURCES)
    sequence.add_argument("--dataset", type=Path, required=True)
    sequence.add_argument("--params", type=Path)
    sequence.add_argument("--updates", type=Path)
    add_case_options(sequence)
    sequence.add_argument("--out", type=Path, required=True)
    sequence.set_defaults(action=make_cases)

    reference = commands.add_parser("reference-snb", help="generate SNB read answers on an independent Neo4j snapshot")
    reference.add_argument("--scale", choices=("fixture", "1", "10"), required=True)
    reference.add_argument("--cases", type=Path, required=True)
    reference.add_argument("--archive", type=Path, required=True, help="published snapshot archive used to load Neo4j")
    reference.add_argument("--nodes", type=positive, required=True, help="expected snapshot node count")
    reference.add_argument("--relationships", type=positive, required=True, help="expected snapshot relationship count")
    reference.add_argument("--neo4j", default="http://127.0.0.1:7474", help="HTTP address of the reference server")
    reference.add_argument("--database", default="neo4j")
    reference.add_argument("--out", type=Path, required=True)
    reference.set_defaults(action=build_reference)

    run = commands.add_parser("run", help="load and run each engine in a fresh worker process")
    run.add_argument("suite", choices=SOURCES)
    run.add_argument("--scale", choices=("fixture", "0.01", "1", "10"), required=True)
    run.add_argument("--dataset", type=Path, required=True)
    run.add_argument("--cases", type=Path, required=True)
    run.add_argument("--reference", type=Path, help="expected answers for this exact case schedule")
    run.add_argument("--out", type=Path, required=True, help="new results directory")
    add_run_options(run)

    def invoke(args):
        select_engines(args, run)
        if args.scale == "fixture" and args.suite != "snb" or args.scale == "0.01" and args.suite != "finbench":
            run.error("use the scale label for this workload")
        try:
            return orchestrate(args)
        except (ValueError, OSError, RuntimeError) as error:
            run.error(str(error))

    run.set_defaults(action=invoke)

    matrix = commands.add_parser("matrix", help="prepare and run selected official workload and scale combinations")
    selection = matrix.add_mutually_exclusive_group(required=True)
    selection.add_argument("--workload", action="append", choices=WORKLOADS, help="workload and scale; repeatable")
    selection.add_argument("--all-workloads", action="store_true", help="both suites at SF1 and SF10")
    matrix.add_argument("--out", type=Path, help="new results directory (default results/selected or results/all)")
    matrix.add_argument("--data-root", type=Path, default=Path("data"), help="data and archive cache root")
    matrix.add_argument("--skip-fetch", action="store_true", help="use datasets already extracted in the cache")
    matrix.add_argument("--mixed", action="store_true", help="append updates after the read sweep")
    matrix.add_argument("--dry-run", action="store_true", help="print the plan without downloading or running")
    matrix.add_argument("--reference", action="append", default=[], metavar="WORKLOAD=FILE",
                        help="expected answers for a selected workload and exact case schedule; repeatable")
    add_case_options(matrix)
    add_run_options(matrix)

    def invoke_matrix(args):
        select_engines(args, matrix)
        try:
            plan = make_plan(args)
            if args.dry_run:
                print(json.dumps(plan, indent=2))
                return 0
            return run_matrix(args, plan)
        except (ValueError, OSError) as error:
            matrix.error(str(error))

    matrix.set_defaults(action=invoke_matrix)

    child = commands.add_parser("_worker", help=argparse.SUPPRESS)
    child.add_argument("job")
    child.set_defaults(action=lambda args: worker(json.loads(args.job)))

    args = parser.parse_args()
    return args.action(args)


if __name__ == "__main__":
    sys.exit(main())

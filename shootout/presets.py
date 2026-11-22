"""Run the convenience recipes inside one benchmark container."""

from pathlib import Path
import subprocess
import sys

from .__main__ import main as benchmark_cli
from .engines import DEFAULT_ENGINES

SF1_COHORT_ENGINES = (
    "arcadedb", "falkordb", "grafeo", "graphqlite", "ladybug", "memgraph", "neo4j", "neug", "relate", "tugraph", "velr",
)
SF1_P99_CASE_OPTIONS = ("--per-read", "50", "--sample-seed", "42", "--read-passes", "10", "--warmup")


def cli(*arguments):
    subprocess.run([sys.executable, "-m", "shootout", *arguments], check=True)


def smoke(out):
    cli("fetch", "fixtures")
    for suite, scale, fixture, operations in (
        ("snb", "fixture", "snb/vanilla", ("--op", "IS1", "--op", "IS4")),
        ("finbench", "0.01", "finbench/sf0.01", ("--op", "TSR1")),
    ):
        dataset = Path("data/fixtures") / fixture
        cases = dataset.parent / "cases.jsonl"
        params = ("--params", dataset.parent / "substitution_parameters") if suite == "snb" else ()
        cli("cases", suite, "--dataset", dataset, *params, "--per-read", "1", *operations, "--out", cases)
        cli("run", suite, "--scale", scale, "--dataset", dataset, "--cases", cases,
            "--query-timeout", "5", "--timeout", "60", "--out", Path(out) / suite)


def prepare_workload(preset, scale, cases, case_options=()):
    suite, _, mixed = preset.partition("-")
    dataset = Path("data") / suite / f"sf{scale}"
    params = dataset.with_name(dataset.name + ("_params" if suite == "snb" else "_read_params"))
    cli("fetch", suite, scale, *(("--updates",) if mixed and suite == "snb" else ()))
    updates = dataset.with_name(dataset.name + "_updates") if suite == "snb" else dataset / "incremental"
    cli("cases", suite, "--dataset", dataset, "--params", params, "--out", cases,
        *(("--updates", updates) if mixed else ()), *case_options)
    return suite, dataset


def workload(preset, scale, out, options, case_options=(), *, default_engines=DEFAULT_ENGINES):
    cases = Path(f"{out}.cases.jsonl")
    suite, dataset = prepare_workload(preset, scale, cases, case_options)
    return benchmark_cli(["run", suite, "--scale", scale, "--dataset", str(dataset), "--cases", str(cases),
                          "--out", out, *options], default_engines=default_engines)


def main():
    preset, *arguments = sys.argv[1:]
    try:
        if preset == "smoke":
            smoke(*arguments)
        elif preset == "sf1-p99-cases":
            out, = arguments
            prepare_workload("snb", "1", Path(out), SF1_P99_CASE_OPTIONS)
        elif preset in ("sf1-p99", "sf1-quick"):
            out, *options = arguments
            case_options = ()
            if preset == "sf1-p99":
                options = ["--timeout", "86400", *options]
                case_options = SF1_P99_CASE_OPTIONS
            return workload("snb", "1", out, options, case_options, default_engines=SF1_COHORT_ENGINES)
        elif preset == "run":
            suite, scale, dataset, cases, out, *options = arguments
            cli("run", suite, "--scale", scale, "--dataset", dataset, "--cases", cases, "--out", out, *options)
        else:
            scale, out, *options = arguments
            return workload(preset, scale, out, options)
    except subprocess.CalledProcessError as error:
        return error.returncode


if __name__ == "__main__":
    sys.exit(main())

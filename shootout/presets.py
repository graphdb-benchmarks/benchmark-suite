"""Run the convenience recipes inside one benchmark container."""

from pathlib import Path
import subprocess
import sys


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


def workload(preset, scale, out, options):
    suite, _, mixed = preset.partition("-")
    dataset = Path("data") / suite / f"sf{scale}"
    params = dataset.with_name(dataset.name + ("_params" if suite == "snb" else "_read_params"))
    cases = Path(f"{out}.cases.jsonl")
    cli("fetch", suite, scale, *(("--updates",) if mixed and suite == "snb" else ()))
    updates = dataset.with_name(dataset.name + "_updates") if suite == "snb" else dataset / "incremental"
    cli("cases", suite, "--dataset", dataset, "--params", params, "--out", cases,
        *(("--updates", updates) if mixed else ()))
    cli("run", suite, "--scale", scale, "--dataset", dataset, "--cases", cases, "--out", out, *options)


def main():
    preset, *arguments = sys.argv[1:]
    try:
        if preset == "smoke":
            smoke(*arguments)
        elif preset == "run":
            suite, scale, dataset, cases, out, *options = arguments
            cli("run", suite, "--scale", scale, "--dataset", dataset, "--cases", cases, "--out", out, *options)
        else:
            scale, out, *options = arguments
            workload(preset, scale, out, options)
    except subprocess.CalledProcessError as error:
        return error.returncode


if __name__ == "__main__":
    sys.exit(main())

# Python 3.12 has published wheels for every pinned engine on supported hosts.
python := env_var_or_default("SHOOTOUT_PYTHON", "python3.12")
runner := env_var_or_default("SHOOTOUT_RUNNER", ".venv/bin/python")

# List the setup, smoke tests, and full-scale commands.
default:
    @just --list

# Install the pinned PyPI wheels in an isolated environment.
setup:
    {{python}} -m venv .venv
    .venv/bin/python -m pip install --index-url https://pypi.org/simple --only-binary=:all: -r requirements.txt -r requirements-data.txt

# Print the pinned engine versions.
versions:
    {{runner}} -m shootout versions

# Check the vendored query hashes and print source URLs.
queries:
    {{runner}} -m shootout catalog

# Check source hashes, omission rules, and the Jaccard deviation with all eight engines.
check:
    {{runner}} -m unittest discover -s tests

# Download the small public fixtures and run bounded smoke cases on all eight engines.
smoke out="results/smoke":
    {{runner}} -m shootout fetch fixtures
    {{runner}} -m shootout cases snb --dataset data/fixtures/snb/vanilla --params data/fixtures/snb/substitution_parameters --per-read 1 --op IC2 --op IS1 --op IS4 --out data/fixtures/snb/cases.jsonl
    {{runner}} -m shootout cases finbench --dataset data/fixtures/finbench/sf0.01 --per-read 1 --op TSR1 --op TSR4 --out data/fixtures/finbench/cases.jsonl
    {{runner}} -m shootout run snb --scale fixture --dataset data/fixtures/snb/vanilla --cases data/fixtures/snb/cases.jsonl --query-timeout 5 --timeout 60 --out "{{out}}/snb"
    {{runner}} -m shootout run finbench --scale 0.01 --dataset data/fixtures/finbench/sf0.01 --cases data/fixtures/finbench/cases.jsonl --query-timeout 5 --timeout 60 --out "{{out}}/finbench"

# Choose workloads, engines, and operations. Run `just selected --help` for options.
[positional-arguments]
selected *options:
    {{runner}} -m shootout matrix "$@"

# Run SNB and FinBench at SF1 and SF10 on all eight engines by default.
[positional-arguments]
all *options:
    {{runner}} -m shootout matrix --all-workloads "$@"

# Download official SF1 or SF10 data and run the default read sweep.
snb scale out *options:
    {{runner}} -m shootout fetch snb "{{scale}}"
    {{runner}} -m shootout cases snb --dataset "data/snb/sf{{scale}}" --params "data/snb/sf{{scale}}_params" --out "{{out}}.cases.jsonl"
    {{runner}} -m shootout run snb --scale "{{scale}}" --dataset "data/snb/sf{{scale}}" --cases "{{out}}.cases.jsonl" --out "{{out}}" {{options}}

# Download official SF1 or SF10 data and replay reads followed by updates.
snb-mixed scale out *options:
    {{runner}} -m shootout fetch snb "{{scale}}" --updates
    {{runner}} -m shootout cases snb --dataset "data/snb/sf{{scale}}" --params "data/snb/sf{{scale}}_params" --updates "data/snb/sf{{scale}}_updates" --out "{{out}}.cases.jsonl"
    {{runner}} -m shootout run snb --scale "{{scale}}" --dataset "data/snb/sf{{scale}}" --cases "{{out}}.cases.jsonl" --out "{{out}}" {{options}}

# Download FinBench SF1 or SF10 and run the default read sweep.
finbench scale out *options:
    {{runner}} -m shootout fetch finbench "{{scale}}"
    {{runner}} -m shootout cases finbench --dataset "data/finbench/sf{{scale}}" --params "data/finbench/sf{{scale}}_read_params" --out "{{out}}.cases.jsonl"
    {{runner}} -m shootout run finbench --scale "{{scale}}" --dataset "data/finbench/sf{{scale}}" --cases "{{out}}.cases.jsonl" --out "{{out}}" {{options}}

# Download FinBench SF1 or SF10 and replay reads followed by updates.
finbench-mixed scale out *options:
    {{runner}} -m shootout fetch finbench "{{scale}}"
    {{runner}} -m shootout cases finbench --dataset "data/finbench/sf{{scale}}" --params "data/finbench/sf{{scale}}_read_params" --updates "data/finbench/sf{{scale}}/incremental" --out "{{out}}.cases.jsonl"
    {{runner}} -m shootout run finbench --scale "{{scale}}" --dataset "data/finbench/sf{{scale}}" --cases "{{out}}.cases.jsonl" --out "{{out}}" {{options}}

# Replay a prepared JSONL workload, with optional flags for engines and deviation packs.
run suite scale dataset cases out *options:
    {{runner}} -m shootout run "{{suite}}" --scale "{{scale}}" --dataset "{{dataset}}" --cases "{{cases}}" --out "{{out}}" {{options}}

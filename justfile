# The host Python launches Docker and renders saved evidence.
python := env_var_or_default("SHOOTOUT_PYTHON", "python3.12")
runner := env_var_or_default("SHOOTOUT_RUNNER", "python3.12")
container := python + " -m shootout.container"

# List the setup, smoke tests, and full-scale commands.
default:
    @just --list

# Build the benchmark image matching the Docker host's architecture.
setup:
    {{container}} --build-only

alias setup-candidates := setup
alias setup-neo4j := setup
alias setup-servers := setup

# Print the pinned engine versions.
versions:
    {{runner}} -m shootout versions

# Check the vendored query hashes and print source URLs.
queries:
    {{runner}} -m shootout catalog

# Check source hashes, omission rules, and the Jaccard deviation with all eight engines.
check:
    {{container}} python -m unittest discover -s tests

# Download the small public fixtures and run bounded smoke cases on all eight engines.
[positional-arguments]
smoke out="results/smoke":
    {{container}} python -m shootout.presets smoke "$@"

# Choose workloads, engines, and operations. Run `just selected --help` for options.
[positional-arguments]
selected *options:
    {{container}} python -m shootout matrix "$@"

# Run SNB and FinBench at SF1 and SF10 on the eight default engines.
[positional-arguments]
all *options:
    {{container}} python -m shootout matrix --all-workloads "$@"

# Download official SF1 or SF10 data and run the default read sweep.
[positional-arguments]
snb scale out *options:
    {{container}} python -m shootout.presets snb "$@"

# Run the nine-engine SF1 comparison with 500 measured reads per operation.
[positional-arguments]
sf1-cohort out *options:
    {{container}} python -m shootout.presets sf1-p99 "$@" --reference references/snb-sf1-p99.json

# Run the nine-engine SF1 comparison with five inputs per operation.
[positional-arguments]
sf1-cohort-quick out *options:
    {{container}} python -m shootout.presets sf1-quick "$@" --reference references/snb-sf1.json

# Prepare the larger SF1 case file without running any engines.
[positional-arguments]
sf1-p99-cases out:
    {{container}} python -m shootout.presets sf1-p99-cases "$@"

# Download official SF1 or SF10 data and replay reads followed by updates.
[positional-arguments]
snb-mixed scale out *options:
    {{container}} python -m shootout.presets snb-mixed "$@"

# Download FinBench SF1 or SF10 and run the default read sweep.
[positional-arguments]
finbench scale out *options:
    {{container}} python -m shootout.presets finbench "$@"

# Download FinBench SF1 or SF10 and replay reads followed by updates.
[positional-arguments]
finbench-mixed scale out *options:
    {{container}} python -m shootout.presets finbench-mixed "$@"

# Replay a prepared JSONL workload, with optional flags for engines and deviation packs.
[positional-arguments]
run suite scale dataset cases out *options:
    {{container}} python -m shootout.presets run "$@"

# Generate independent expected answers for an SNB read schedule.
[positional-arguments]
reference-snb *options:
    {{runner}} -m shootout reference-snb "$@"

# Render operation summaries and a separate case-level appendix.
[positional-arguments]
chart *options:
    {{runner}} -m shootout.chart "$@"

# Score verified query results and render an engine leaderboard.
[positional-arguments]
leaderboard *options:
    {{runner}} -m shootout.leaderboard "$@"

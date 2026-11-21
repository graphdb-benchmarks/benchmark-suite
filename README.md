# Embedded Cypher benchmark

Run the same LDBC Cypher templates through eight published, in-process Python drivers. This repository provides an unaudited Python replay of selected operations from the published workloads. [LDBC fair use policy](https://ldbcouncil.org/benchmarks/fair-use-policies/)

## Run it

Install `just` and CPython 3.12. Run directly on Linux x86-64 with glibc 2.35 or newer, Linux arm64 with glibc 2.39 or newer, or Apple Silicon on macOS 15 or newer. The setup recipe installs the published PyPI wheels into `.venv`. The engine versions in [requirements.txt](requirements.txt) are the latest stable releases verified on 21 November 2026. The runner rejects a different installed version so a rerun keeps the same engine versions.

Install the published Velr CLI with `velr-admin` on `PATH` before running the Velr lane. On Homebrew, use `brew install velr-ai/velr/velr`. Set `VELR_ADMIN_BIN` to point to a specific importer binary. [Velr CLI releases](https://github.com/velr-ai/velr/releases)

```sh
just setup
just check
just smoke
```

The smoke run downloads the public fixtures, loads the same logical snapshot in each engine, and exercises a small set of canonical reads. It records query refusals, mismatches, and timeouts as benchmark outcomes. It uses smaller inputs and shorter time limits than the scale runs. Use a new output directory for each run, such as `just smoke results/smoke-repeat`.

```sh
just snb 1 results/snb-sf1
just snb 10 results/snb-sf10
just finbench 1 results/finbench-sf1
just finbench 10 results/finbench-sf10
```

Append `--engine velr` to any recipe above to run one engine. For a custom selection, `just selected` accepts repeated `--workload`, `--engine`, and `--op` flags. Use the published workload operation IDs, such as `IS1` or `TSR1`.

```sh
just selected --workload snb-sf1 --engine velr --op IS1 --per-read 1 --out results/velr-is1
just selected --workload snb-sf10 --workload finbench-sf1 --engine ladybug --out results/ladybug-selected
just all --dry-run
just all --out results/full-matrix
```

`just all` runs SNB Interactive and FinBench at SF1 and SF10 across all eight engines. Both commands use the read sweep by default. Add `--mixed` to append updates. `--max-updates` caps the source update prefix before operation filtering, so a selected write may need a longer prefix to appear. The timeout, thread, batch size, and deviation pack options are listed by `just selected --help`. Use `--skip-fetch` to run against already extracted datasets and `--data-root` to choose the cache location.

`--dry-run` prints the plan without downloads or worker processes. Each workload writes to its own directory under `--out`. The top-level `matrix.json` and `matrix-summary.jsonl` record the plan and outcomes. The matrix continues to later workloads when a workload fails and returns a nonzero exit code if a workload could not finish. Use a fresh output directory for each run.

The runner executes one engine at a time and starts each workload with Velr whenever it is selected. The remaining engines follow the requested order. The default RSS cap is **15 GB** (15 billion bytes) per worker and its subprocesses, including `velr-admin`. Every replay query has the same **30-second** external wall-clock deadline through result consumption. Use `--rss-limit-gb` and `--query-timeout` to change the caps for every selected engine and workload. The `--timeout` option covers all work for one engine, including initial load and any restarts, and defaults to two hours.

```sh
just all --rss-limit-gb 24 --query-timeout 90 --out results/full-matrix-24gb-90s
```

The guard samples combined RSS every 100 ms during startup, import, and replay. Shared pages can appear in more than one process's RSS, and brief spikes can occur between samples. When a sample exceeds the cap, the runner kills the worker's process group, records `worker_rss_limit`, and continues to the next engine. The run's `summary.jsonl` records the limit and the highest sampled combined RSS for each engine.

The same supervisor enforces each query deadline outside the database process, including for drivers that lack cancellation. After a timed-out read it kills the process group, records the timeout, and reopens the same persistent snapshot in a fresh worker to continue at the next case. A timed-out write stops replay for that engine because its commit outcome may be unknown. Resume events identify the connection changes so timing comparisons can use cases measured in matching conditions.

Each recipe downloads and caches the official datasets and parameters under `data/`. SNB uses the Interactive v1 **CsvComposite & LongDateFormatter** archives. FinBench uses its published transaction snapshots. Downloads print SHA-256 hashes. Leave room for the cached archives, extracted CSVs, and one engine's working database at a time. SF10 can take hours depending on the machine, engine, and query limits. [SNB datasets](https://ldbcouncil.org/benchmarks/snb/datasets/), [FinBench datasets](https://ldbcouncil.org/benchmarks/finbench/datasets/)

## Workload coverage

The read sweep takes five rows per published complex-read parameter file where available. For short reads, it samples IDs from the snapshot because these operations normally follow other operations in the official driver. FinBench short reads use a fixed time window and threshold. The case file preserves the exact parameters for every engine.

Add update replay with `just snb-mixed 1 results/snb-sf1-mixed` or `just finbench-mixed 1 results/finbench-sf1-mixed`. These recipes run the read sweep and then replay up to 200 time-ordered writes from the published streams. They use a sequential schedule with a fresh snapshot per engine. FinBench replays the four draft writes with a direct mapping to the published stream operations, TW14, TW15, TW18, and TW19. The other writes and all three read-write operations are listed as omissions. [SNB Interactive v1](https://ldbcouncil.org/benchmarks/snb/interactive/), [FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf)

Each run records `ldbc_op` for the published operation and `op` for the selected template ID. This matters for FinBench because the Neo4j draft numbering differs from the published workload. Under the canonical profile, the 18 FinBench reads yield eight scheduled operations and ten documented omissions. The plain-Cypher Jaccard pack adds one scheduled read. Some scheduled queries may still fail or return unexpected results through a driver. [Source catalog](queries/sources.json), [deviation and omission notes](deviations/README.md)

## Query sources and deviations

The [source catalog](queries/sources.json) pins the upstream revisions and hashes the query files byte for byte. SNB Interactive v1 uses LDBC's Neo4j/Cypher reference. FinBench uses the Neo4j Cypher draft in LDBC's reference implementations repository. The draft includes templates from an earlier workload revision. The catalog retains those templates for provenance and records which operations the published workload can replay. [SNB query source](https://github.com/ldbc/ldbc_snb_interactive_v1_impls/tree/11db98cc2ba14c33492f6c0c34e68c8be7e22e5f/cypher/queries), [FinBench query source](https://github.com/ldbc/ldbc_finbench_transaction_impls/tree/5dc5fb5d6a1fd6324c2dfcd555c5bb6985371a2c/neo4j)

Use `just queries` to verify the canonical hashes and print provenance and omissions. To run a documented deviation, request the pack explicitly. Each case records the canonical and executed hashes.

```sh
just snb 1 results/snb-sf1-typed --pack snb-typed-message
just finbench 1 results/finbench-sf1-jaccard --pack finbench-cypher-jaccard
```

The FinBench TCR10 deviation replaces Neo4j GDS's Jaccard function with standard Cypher.[^jaccard] The draft-only APOC operation stays recorded as an omission.[^apoc]

[^jaccard]: The Cypher form groups investments by company and tracks whether each person invested there, then divides the intersection count by the union count. It follows the specification's three-decimal rounding and gives zero for an empty union. The test fixture checks duplicate investments, overlapping and disjoint sets, empty sets, and time-window boundaries against an independent Python oracle on all eight engines. [Published FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf), [upstream TCR10](https://github.com/ldbc/ldbc_finbench_transaction_impls/blob/5dc5fb5d6a1fd6324c2dfcd555c5bb6985371a2c/neo4j/queries/tcr-10.cypher)

[^apoc]: Neo4j documents `reduce(total = 0.0, x IN values | total + x)` as the standard Cypher replacement for `apoc.coll.sum(values)` on a numeric list. The draft's `tcr-11.cypher` computes investment ratios. Published TCR11 is guarantee-chain detection, which the draft places in `tcr-12.cypher`. Replacing the APOC call would still leave an investment-ratio operation outside the current workload. [Neo4j APOC documentation](https://neo4j.com/docs/apoc/current/overview/apoc.coll/apoc.coll.sum/), [published FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf)

## Snapshot loading

The loaders share the same input schema, data normalization and snapshot checks. Load time includes preparation, writing, and index creation. The plan, settings and worker events record the chosen loader. Each store starts empty.

| Engine | Snapshot loader |
| --- | --- |
| Velr | Strict offline `velr-admin import` using staged typed CSV. The load event includes CSV staging time, native import time, importer version and binary hash, and child-process peak RSS. |
| Ladybug, Kùzu, RyuGraph | `COPY FROM` with staged Parquet. The column types preserve numbers, lists and nulls, and each relationship file has one source and target type. [Ladybug Parquet import](https://docs.ladybugdb.com/import/parquet/) |
| Grafeo | Native typed CSV loads for scalar node fields and edges. Native node batches preserve list-valued fields. [Grafeo import APIs](https://docs.grafeo.dev/languages/python/database/#data-import) |
| ArcadeDB | Native `graph_batch()` for vertices and edges with the write-ahead log enabled. [ArcadeDB bulk loads](https://docs.arcadedb.com/arcadedb-python/how-to/bulk-loads) |
| GraphDBLite | Native node and edge batches. Batched Cypher creates nodes with multiple labels in one operation. The loader resolves relationship endpoints in batches through a temporary SQLite index. |
| Relate | Parameterized `UNWIND` batches through the embedded Python driver. [Relate Python ingestion guide](https://graphacademy.neo4j.com/courses/relate-get-started-python/1-install-load-and-query/3-seed-data) |

## Reading the output

Each run has a `run.json` configuration, a copy of its case sequence, a JSONL summary, and individual engine logs and event files. The engine files include the first-result startup time, load time, process peak RSS, reopened snapshot counts, and per-case latency through full result consumption. Startup measurements distinguish empty process launch from reopening in the same worker. The supervisor records external deadlines separately from errors returned by a driver. Each externally timed-out case reports the configured deadline as its duration, and `query_deadline_stops` counts these stops in the engine summary.

The runner verifies snapshot counts and compares results for cases with an expected answer. Included source-data checks cover SNB IS1 and IS4 and FinBench TSR1. Other completed operations report raw timings and result fingerprints with status `unverified`. A write error stops later operations on that engine because their starting state may have changed. These recipes produce a reproducible workload replay and compatibility evidence. Full TCK execution, exhaustive transaction safety tests, and official-result validation are separate work.

For a custom shared schedule, use one JSON object per line with `op` and `params`. You can add `expected` as an array of result rows, or `unordered: true` when the query leaves row order unspecified. The runner compares row multiplicity. Use the canonical template's parameter names and include `ldbc_op` when it differs from `op`. To run just one engine, append `--engine <name>`.

```sh
just run snb 1 data/snb/sf1 results/snb-sf1.cases.jsonl results/velr-repeat --engine velr
```

Harness code is Apache 2.0. The vendored LDBC sources retain their respective license and notice files. Driver packages and engine runtimes retain their published licenses.

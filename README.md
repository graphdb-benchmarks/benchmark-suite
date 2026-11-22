# Graph DB Benchmark Suite

Open-source benchmark suite for graph databases, with reproducible runs and correctness checks.

The Python runner replays the same published Cypher queries against embedded and server databases. It checks results against expected answers and records query latency, import time, and peak memory. The LDBC workload replays are unaudited. [LDBC fair use policy](https://ldbcouncil.org/benchmarks/fair-use-policies/)

- **SNB Interactive v1** runs at SF1 and SF10. The default SF1 read schedule comes with checked-in expected answers. [SNB Interactive](https://ldbcouncil.org/benchmarks/snb/interactive/)
- **FinBench Transaction** runs at SF1 and SF10 with partial operation coverage. [FinBench](https://ldbcouncil.org/benchmarks/finbench/)
- **Graphalytics** is planned. [Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/)

## Run it

Install `just`, CPython 3.12, and Docker with a Linux runtime. `just setup` builds the pinned benchmark image for the Docker host's CPU, choosing Linux ARM64 or Linux x86-64. The launcher checks the image, Python, and server executables against that architecture. Set `SHOOTOUT_DOCKER_CONTEXT` to choose a Docker context. On macOS, give the Docker VM enough RAM to enforce the 15 GB default RSS cap, for example 18 GiB.

Each benchmark invocation runs all selected engines in one container. The image installs the pinned PyPI drivers and the published server and loader binaries, including `velr-admin` 0.2.61. The runner stages each dataset once on the container's Linux filesystem before timing any engine and creates a fresh store per engine. It saves evidence to the workspace and records the image ID, architecture, CPU count, and visible host memory in `run.json`. [Velr CLI releases](https://github.com/velr-ai/velr/releases)

Input and output paths under the repository are shared with the container. For paths outside the repository, set `SHOOTOUT_DOCKER_MOUNTS` to a colon-separated list of existing host directories.

```sh
just setup
just check
just smoke
just sf1-cohort results/snb-sf1-cohort
```

The smoke run downloads the public fixtures, loads the same logical snapshot in each engine, and exercises a small set of canonical reads. It records query refusals, mismatches, and timeouts as benchmark outcomes. It uses smaller inputs and shorter time limits than the scale runs. Use a new output directory for each run, such as `just smoke results/smoke-repeat`.

`just sf1-cohort` runs the ten engines in the SF1 leaderboard, starting with Velr. It verifies the default case schedule against the checked-in expected answers. Give it a new output directory for each run. To turn the saved results into a leaderboard, run:

```sh
just leaderboard --run results/snb-sf1-cohort --out results/snb-sf1-leaderboard.html
```

```sh
just snb 1 results/snb-sf1 --reference references/snb-sf1.json
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

`--dry-run` prints the plan inside the benchmark image. The recipe builds the image on first use. Each workload writes to its own directory under `--out`. The top-level `matrix.json` and `matrix-summary.jsonl` record the plan and outcomes. The matrix continues to later workloads when a workload fails and returns a nonzero exit code if a workload could not finish. Use a fresh output directory for each run.

GraphQLite 1.0.0 and NeuG 0.3.1 are opt-in candidates included in the image. This command replays the existing SF1 reference schedule on both candidates.

```sh
just selected --workload snb-sf1 --engine graphqlite --engine neug \
    --reference snb-sf1=references/snb-sf1.json --out results/snb-candidates
```

The runner executes one engine at a time and starts each workload with Velr whenever it is selected. The remaining engines follow the requested order. The default RSS cap is **15 GB** (15 billion bytes) per worker and its subprocesses, including `velr-admin`. Every replay query has the same **30-second** external wall-clock deadline through result consumption. Use `--rss-limit-gb` and `--query-timeout` to change the caps for every selected engine and workload. The `--timeout` option covers all work for one engine, including initial load and any restarts, and defaults to two hours.

```sh
just all --rss-limit-gb 24 --query-timeout 90 --out results/full-matrix-24gb-90s
```

The guard samples combined RSS every 100 ms during startup, import, and replay. It also samples at the end of import and at the end of each timed query while the worker still holds the result. Shared pages can appear in more than one process's RSS, and brief spikes can occur between samples. When a sample exceeds the cap, the runner kills the worker's process group, records `worker_rss_limit`, and continues to the next engine. The run's `summary.jsonl` records the engine-wide peak, `import_peak_sampled_tree_rss_bytes` for import, and `case_peak_sampled_query_rss_bytes` for the timed cases. Server query peaks measure the database server process and its descendants, excluding the Python client and, for Neo4j, the launcher JVM. Embedded query peaks measure the Python worker and its descendants.

## Server engines

Select any server with a repeated `--engine` option. The runner launches each server as a foreground subprocess on container loopback with a fresh store. Query time includes the local protocol round trip and full result consumption. The published query text, reference answers, and external caps are shared with the embedded runs.

| Engine | Server version in the image | PyPI driver | Bulk load |
| --- | --- | --- | --- |
| Neo4j Community | 2026.10.2 with Java from the published server image. | `neo4j==6.4.1` | Offline `neo4j-admin database import full`, then ID indexes and an index readiness check. [Neo4j distribution](https://neo4j.com/deployment-center/) |
| FalkorDB | 6.0.4 with Redis from the published server image. | `FalkorDB==1.8.2` | Vendor `falkordb-bulk-loader==1.2.0`, followed by ID indexes, batched restoration of source strings and arrays, and an RDB save. [FalkorDB release](https://github.com/FalkorDB/FalkorDB/releases/tag/v6.0.4) |
| Memgraph | 3.14.0 from the published server image. | `pymgclient==1.6.0` | Native `LOAD CSV` in analytical mode, then switch to transactional mode and create a snapshot. [CSV import](https://memgraph.com/docs/data-migration/csv) |
| TuGraph | 4.5.2 on Linux x86-64 and 4.5.1 on Linux ARM64, the newest vendor-published ARM64 build. | `TuGraphClient==2.0` | Offline `lgraph_import` with the generated schema and scoped endpoints. [x86-64 release](https://github.com/TuGraph-family/tugraph-db/releases/tag/v4.5.2), [ARM64 release source](https://github.com/TuGraph-family/tugraph-db/releases/tag/v4.5.1), [import guide](https://tugraph-db.readthedocs.io/en/latest/6.utility-tools/1.data-import.html) |

```sh
just selected --workload snb-sf1 --engine neo4j --engine falkordb \
    --reference snb-sf1=references/snb-sf1.json --out results/snb-sf1-servers
```

Neo4j uses a 1 GiB initial heap, 4 GiB maximum heap, and 4 GiB page cache. Its offline importer uses a 1 GiB Java heap and up to 2 GiB of off-heap memory. TuGraph uses single-label vertices and stores the SNB string arrays as JSON strings. The runner uses the driver's REST transport with bound parameters and verifies the canonical queries against the same expected rows. [TuGraph schema and transport notes](deviations/README.md)

The same supervisor enforces each query deadline outside the database process, including for drivers that lack cancellation. After a timed-out read it kills the process group, records the timeout, and reopens the same persistent snapshot in a fresh worker to continue at the next case. A timed-out write stops replay for that engine because its commit outcome may be unknown. Resume events identify the connection changes so timing comparisons can use cases measured in matching conditions.

Each recipe downloads and caches the official datasets and parameters under `data/`. SNB uses the Interactive v1 **CsvComposite & LongDateFormatter** archives. FinBench uses its published transaction snapshots. Downloads print SHA-256 hashes. Leave room for the cached archives, extracted CSVs, and one engine's working database at a time. SF10 can take hours depending on the machine, engine, and query limits. [SNB datasets](https://ldbcouncil.org/benchmarks/snb/datasets/), [FinBench datasets](https://ldbcouncil.org/benchmarks/finbench/datasets/)

## Workload coverage

The read sweep takes five rows per published complex-read parameter file where available. For short reads, it samples IDs from the snapshot because these operations normally follow other operations in the official driver. FinBench short reads use a fixed time window and threshold. The case file preserves the exact parameters for every engine.

Add update cases with `just snb-mixed 1 results/snb-sf1-mixed` or `just finbench-mixed 1 results/finbench-sf1-mixed`. These recipes append up to 200 time-ordered writes from the published streams. To execute writes, add verified postconditions to the case file and replay it with `just run`. Each engine starts with a fresh snapshot. FinBench has draft templates for TW14, TW15, TW18, and TW19. The other writes and all three read-write operations are listed as omissions. [SNB Interactive v1](https://ldbcouncil.org/benchmarks/snb/interactive/), [FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf)

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

The loaders share the same input schema, data normalization and snapshot checks. Import wall time and peak RSS cover preparation, loading, index creation, and closing the loaded store. Peak RSS includes the worker and any loader subprocesses, including `velr-admin`. Startup, reopen, and post-load verification are separate measurements. The plan, settings and worker events record the chosen loader. Each store starts empty.

| Engine | Snapshot loader |
| --- | --- |
| Velr | Strict offline `velr-admin import` using staged typed CSV. The load event includes CSV staging time, native import time, importer version and binary hash, and child-process peak RSS. |
| Ladybug, Kùzu, RyuGraph | `COPY FROM` with staged Parquet. The column types preserve numbers, lists and nulls, and each relationship file has one source and target type. [Ladybug Parquet import](https://docs.ladybugdb.com/import/parquet/) |
| Grafeo | Native typed CSV loads for scalar node fields and edges. Native node batches preserve list-valued fields. [Grafeo import APIs](https://docs.grafeo.dev/languages/python/database/#data-import) |
| ArcadeDB | Native `graph_batch()` with the write-ahead log enabled, serial flushing, and one commit per flush. The runner verifies the persisted edge counts after reopening. [ArcadeDB bulk loads](https://docs.arcadedb.com/arcadedb-python/how-to/bulk-loads) |
| GraphDBLite | Native node and edge batches. Batched Cypher creates nodes with multiple labels in one operation. The loader resolves relationship endpoints in batches through a temporary SQLite index. |
| Relate | Parameterized `UNWIND` batches through the embedded Python driver. [Relate Python ingestion guide](https://graphacademy.neo4j.com/courses/relate-get-started-python/1-install-load-and-query/3-seed-data) |
| GraphQLite | `insert_nodes_bulk` and `insert_edges_bulk` with the returned endpoint ID map. A final SQLite transaction removes the temporary text IDs and adds the shared SNB labels, preserving numeric `id` properties and one node per input row. [Bulk import API](https://colliery-io.github.io/graphqlite/how-to/bulk-import/), [SQLite storage schema](https://colliery-io.github.io/graphqlite/reference/database-schema/) |
| NeuG | Embedded `COPY FROM` on typed CSV, with node and relationship table creation and copies grouped in one explicit transaction. Nodes use one physical label, so canonical reads that require the shared `Message`, `Place`, or `Organisation` labels can fail. [NeuG bulk import](https://neug.io/docs/data_io/import_data/) |

GraphDBLite 0.2.0 caps the intermediate rows in a Cypher scan at 100,000. The runner verifies the same node and edge counts by reading the reopened SQLite store in read-only mode through Python. Every timed Cypher operation still runs through the pinned GraphDBLite driver.

## Reading the output

Each run has a `run.json` configuration, a copy of its case sequence, a JSONL summary, and individual engine logs and event files. The engine files include the first-result startup time, load time, process peak RSS, reopened snapshot counts, and per-case latency through full result consumption. Startup measurements distinguish empty process launch from reopening in the same worker. The supervisor records external deadlines separately from errors returned by a driver. Each externally timed-out case reports the configured deadline as its duration, and `query_deadline_stops` counts these stops in the engine summary.

## Verifying query results

The runner compares the complete result with expected rows after timing the query. A wrong result fails and records the actual and expected rows. A case without an expected answer fails before execution. Included source-data checks cover SNB IS1 and IS4 and FinBench TSR1. The checked-in SF1 reference covers all 105 reads in the default schedule. Its answers were generated with the pinned LDBC Cypher on Neo4j 5.26.31, on the same initial snapshot used by the embedded engines. LDBC also publishes mixed-workload validation data generated with its Neo4j implementation. [SF1 reference answers](references/snb-sf1.json), [LDBC validation data](https://github.com/ldbc/ldbc_snb_interactive_v1_impls#validation-parameters)

The runner preserves row multiplicity and ordering. For SNB it accepts arbitrary order inside the specified IC1 and IC12 collections and within equal sort-key groups in IC14 and IS7. The node sequence inside each IC14 path must still match. [SNB Interactive v1 specification](https://ldbcouncil.org/ldbc_snb_docs/workload-interactive-v1.pdf)

```sh
just selected --workload snb-sf1 --reference snb-sf1=references/snb-sf1.json --engine velr
just run snb 1 data/snb/sf1 results/snb-sf1.cases.jsonl results/velr-repeat \
    --reference references/snb-sf1.json --engine velr
```

For another SNB read schedule, load the matching starting snapshot on a separate Neo4j reference and run `just reference-snb --help`. The command checks node and relationship counts and ties its expected rows to the case-file and canonical-query hashes. The reference server is used during answer generation. Replay uses the saved answer file on the benchmark host.

For a custom case file, use one JSON object per line with `op` and `params`. Supply `expected` as an array of complete result rows and use `unordered: true` only when the query leaves the entire row order unspecified. Include `ldbc_op` when it differs from `op`. A write requires a `postcondition` object with a read-only `query`, its `params` if needed, and `expected` rows. The postcondition has its own external deadline and runs outside the timed write. A failed write or postcondition stops dependent cases. SF10 and FinBench require case-specific expected answers for reads beyond the built-in checks.

## Charting saved runs

```sh
just chart --run results/snb-sf1 --out results/snb-sf1-query-performance.html
```

`just chart` reads a saved run or matrix directory and writes an operation summary plus a separate case-level HTML appendix. The summary begins with import wall time and import peak RSS. Each query operation has one vertical latency bar and one peak RSS bar per engine where measurements exist. Latency uses the median of verified cases. Any timeout gives the engine a full-height amber bar at the deadline, with a line for the verified median when some cases passed. A red `FAIL` label marks any failed case. The chart shows pass, fail, timeout, and omission counts under each engine.

Query peak RSS is the maximum sampled RSS across an operation's measured query windows, including failed and timed-out queries. It includes memory already resident in the engine. Server deployments use the database process tree. Embedded deployments use the Python worker process tree. The chart shows the number of cases that contributed samples. Historical runs with only engine-wide peaks have no per-operation RSS.

The appendix lists every input, outcome, timing, and RSS reading, along with the run sources. It defaults to `<out-stem>-appendix.html`. Use `--appendix` to choose a path and `--summary` to export both aggregates and case records as JSON. `--exclude-engine` leaves an engine out of both views while preserving its raw run files.

For historical logs, supply `--reference references/snb-sf1.json` to check recorded result fingerprints against the expected rows. Only an exact fingerprint match can certify a historical result. A differing fingerprint fails the verification check because the original logs retained only the fingerprint. `--engine-from ENGINE=DIR` can add or replace an engine from a saved run when the case schedule, platform, and run limits match.

## Runtime leaderboard

```sh
just leaderboard --run results/snb-sf1 --out results/snb-sf1-leaderboard.html \
    --json results/snb-sf1-scores.json
```

The scorer awards up to three points per query operation. An engine must pass verification on every scheduled input before it can earn any points.

| Criterion | Default points | Rule |
| --- | ---: | --- |
| Correct | 1 | Every case passes verification. |
| p99 under 250 ms | 1 | Nearest-rank p99 over those cases is strictly below 250 ms by default. |
| p99 under 25 ms | 1 | Nearest-rank p99 over those cases is strictly below 25 ms. |

With five cases per operation, nearest-rank p99 is the slowest case. The two latency points are cumulative. Operations omitted by every engine are outside the report. The operation charts include query RSS and import measurements.

Repeat `--run` to combine engines from separate result directories. Each engine needs one source, and the runs must match on workload, scale, case schedule, expected answers, platform, Python version, and limits. Use `--exclude-engine` to leave an engine out of the cohort. Adjust the 250 ms scoring cutoff with `--latency-target-ms`. The JSON records the thresholds, each fully verified operation's p99, and the awarded points.

Use `--cohort NAME=DIR` to display multiple platform rankings on the same page. Repeat a name to supply several runs for that platform. Each cohort shares the workload, case schedule, expected answers, and caps. The JSON export contains a `cohorts` array with the standings and operation scores for each platform.

```sh
just leaderboard --cohort "Native macOS ARM64=results/snb-sf1-macos" \
    --cohort "Linux ARM64 container=results/snb-sf1-linux-arm64" \
    --out results/snb-sf1-leaderboard.html --json results/snb-sf1-scores.json
```

## License

The benchmark harness and original documentation are licensed under the Apache License, Version 2.0. [Repository license](LICENSE)

Vendored LDBC query templates carry their upstream license and notice files. [SNB license](queries/snb/LICENSE.txt) · [SNB notice](queries/snb/NOTICE.txt) · [FinBench license](queries/finbench/LICENSE.txt) · [FinBench notice](queries/finbench/NOTICE.txt)

External datasets, database engines, drivers, and runtime binaries are subject to their own terms.

# Graph DB Benchmark Suite

An open-source performance benchmark for labeled property graph (LPG) databases that support or advertise support for openCypher.

The Python runner replays the same published Cypher queries against embedded and server databases. It checks results against expected answers and records query latency, import time, and peak memory. The LDBC workload replays are unaudited. [LDBC fair use policy](https://ldbcouncil.org/benchmarks/fair-use-policies/)

- **SNB Interactive v1** runs at SF1 and SF10. The default SF1 read schedule comes with checked-in expected answers. [SNB Interactive](https://ldbcouncil.org/benchmarks/snb/interactive/)
- **FinBench Transaction** runs at SF1 and SF10 with partial operation coverage. [FinBench](https://ldbcouncil.org/benchmarks/finbench/)
- **Graphalytics** is planned. [Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/)

## Affiliation and purpose

We maintain this suite and develop Velr, one of the engines it tests. We built the suite for our own testing and made it public to share what we learn with the wider graph database community.

We use the same Cypher reference queries developed and published by LDBC as the baseline for every engine. This reduces bias from choosing queries tailored to a particular engine. We check results against shared expected answers and document every engine-specific rewrite. We publish the full test suite and methodology so you can inspect the setup, rerun the tests, and verify the results yourself. Each result reflects the workloads, data scales, hardware, and engine configurations used in that run. [Query sources and deviations](#query-sources-and-deviations) · [Reproduce a run](#run-it)

## Run it

Install `just`, CPython 3.12, and Docker with a Linux runtime. `just setup` builds the pinned benchmark image for the Docker host's CPU, choosing Linux ARM64 or Linux x86-64. The launcher checks the image, Python, and server executables against that architecture. Set `SHOOTOUT_DOCKER_CONTEXT` to choose a Docker context. On macOS, give the Docker VM enough RAM to enforce the 15 GB default RSS cap, for example 18 GiB.

Each benchmark invocation runs all selected engines in one container. The image includes the pinned PyPI drivers and the published server and loader binaries required by the engines. The runner stages each dataset once on the container's Linux filesystem before timing any engine and creates a fresh store for each engine in each trial. It saves results to the workspace and records the image ID, architecture, CPU count, and visible host memory in `run.json`.

Relate uses its default **256 MiB per-expression limit** for individual values such as intermediate lists, strings, and maps. The benchmark’s process-tree RSS cap applies to the whole worker. [Relate limits](https://relatedb.ai/docs/reference/storage/limits-and-sizing/)

Input and output paths under the repository are shared with the container. For paths outside the repository, set `SHOOTOUT_DOCKER_MOUNTS` to a colon-separated list of existing host directories.

```sh
just setup
just check
just smoke
just sf1-cohort results/snb-sf1-cohort
```

The smoke run downloads the public fixtures, loads the same logical snapshot in each engine, and exercises a small set of canonical reads. It records query refusals, mismatches, and timeouts as benchmark outcomes. It uses smaller inputs and shorter time limits than the scale runs. Use a new output directory for each run, such as `just smoke results/smoke-repeat`.

`just sf1-cohort` runs nine engines by default against checked-in expected answers for 50 distinct inputs per operation. The recipe samples inputs with seed 42, runs one warm-up query per operation, then shuffles the same inputs across ten measured passes. That gives 500 response measurements per operation and 10,500 per engine. The report excludes the warm-up cases, whose outcomes remain in the raw logs. The recipe allows up to 24 hours per engine by default. `just sf1-cohort-quick` runs the five-input schedule for quicker checks.

> [!NOTE]
> **Relate and GraphQLite can lengthen SF1 test runs.** The recorded Linux ARM64 SNB SF1 runs used 105 cases per engine.
>
> - **Relate took 33m 31s.** Import took 15m 47s. All five IC14 cases timed out, and the store took roughly two minutes to reopen after each timeout.
> - **GraphQLite took 24m 36s.** Import took 11m 50s, and 16 cases timed out across four operations.
>
> For targeted testing, pass `--engine` for each engine you want to include. This lets you skip Relate and GraphQLite when the test does not need them.

Give each run a fresh output directory. To turn the saved results into a leaderboard, run this command.

```sh
just leaderboard --run results/snb-sf1-cohort --out results/snb-sf1-leaderboard.html
```

```sh
just snb 1 results/snb-sf1 --reference references/snb-sf1.json
just snb 10 results/snb-sf10
just finbench 1 results/finbench-sf1
just finbench 10 results/finbench-sf10
```

Repeat `--engine` to select engines in the order you want to run them. The built-in cohorts use alphabetical engine order by default. For a custom selection, `just selected` accepts repeated `--workload`, `--engine`, and `--op` flags. Use the published workload operation IDs, such as `IS1` or `TSR1`.

```sh
just selected --workload snb-sf1 --engine velr --op IS1 --per-read 1 --out results/velr-is1
just selected --workload snb-sf10 --workload finbench-sf1 --engine ladybug --out results/ladybug-selected
just all --dry-run
just all --out results/full-matrix
```

`just all` runs SNB Interactive and FinBench at SF1 and SF10 on eight default engines. They are ArcadeDB, Grafeo, GraphDBLite, Kùzu, Ladybug, Relate, RyuGraph, and Velr. Both matrix commands use the read sweep by default. Add `--mixed` to append updates. `--max-updates` caps the source update prefix before operation filtering, so a selected write may need a longer prefix to appear. The timeout, thread, batch size, and deviation pack options are listed by `just selected --help`. Use `--skip-fetch` to run against already extracted datasets and `--data-root` to choose the cache location.

`--dry-run` prints the plan inside the benchmark image. The recipe builds the image on first use. Each workload writes to its own directory under `--out`. The top-level `matrix.json` and `matrix-summary.jsonl` record the plan and outcomes. The matrix continues to later workloads when a workload fails and returns a nonzero exit code if a workload could not finish. Use a fresh output directory for each run.

The suite supports **TuGraph** and **NeuG** as opt-in engines because both failed most queries in the canonical SNB SF1 run. TuGraph verified **6 of 105** cases and NeuG verified **5 of 105**. Each fully verified just one of the 21 read operations, IS1. Both loaders use a single label per node, which affects canonical queries that rely on shared SNB labels. [TuGraph schema notes](deviations/README.md#snapshot-wiring) · [NeuG loader](#snapshot-loading)

Run both engines against the checked-in five-input SF1 reference with explicit selections.

```sh
just sf1-cohort-quick results/snb-sf1-opt-in --engine tugraph --engine neug
```

The runner executes one engine at a time. Use `--trials N` to repeat a run with the same case schedule. The first trial uses the selected order, and each later trial moves the first engine to the end. A full rotation gives every engine one turn in each position. `trials.json` lists the trial directories and their orders. Each trial records its order in `run.json` and writes engine outcomes in execution order to `summary.jsonl`. `just chart --run` accepts the parent directory. For a leaderboard, point `--run` at an individual trial directory.

```sh
just sf1-cohort-quick results/sf1-order --trials 2 --engine grafeo --engine velr
just leaderboard --run results/sf1-order/trial-001 --out results/sf1-order-trial-001.html
```

On Linux, including the benchmark container, every worker and its subprocesses use the same set of **up to six logical cores**. The runner selects the first six available CPU IDs, or all available IDs on a smaller host, and records the mask in `run.json`. Kùzu-family engines and NeuG default to that many query threads. `--threads` can lower their internal thread limit. Native macOS runs report that CPU affinity is unavailable.

The default RSS cap is **15 GB** (15 billion bytes) per worker and its subprocesses. Every replay query has the same **30-second** external wall-clock deadline through result consumption. Use `--rss-limit-gb` and `--query-timeout` to change the caps for every selected engine and workload. The `--timeout` option covers all work for one engine, including initial load and any restarts, and defaults to two hours.

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

For Neo4j, the runner applies the heap and page cache values recommended by `neo4j-admin server memory-recommendation`. It passes the smaller of the shared RSS cap and the memory capacity visible to the benchmark, including any Linux cgroup limit. At the default **15 GB** cap on a host with enough RAM, Neo4j Community 2026.10.2 recommends a **4,900 MiB initial and maximum heap** and a **5,600 MiB page cache** (`--memory=14305m`). The recommendation leaves headroom for native memory and other processes. Each `run.json` records the budget and settings. The offline importer uses a 1 GiB Java heap and up to 2 GiB of off-heap memory. [Neo4j Admin memory recommendations](https://neo4j.com/docs/operations-manual/current/configuration/neo4j-admin-memrec/) · [Neo4j memory configuration](https://neo4j.com/docs/operations-manual/current/performance/memory-configuration/)

TuGraph uses single-label vertices and stores the SNB string arrays as JSON strings. The runner uses the driver's REST transport with bound parameters and verifies the canonical queries against the same expected rows. [TuGraph schema and transport notes](deviations/README.md)

The same supervisor enforces each query deadline outside the database process, including for drivers that lack cancellation. After a timed-out read it kills the process group, records the timeout, and reopens the same persistent snapshot in a fresh worker to continue at the next case. A timed-out write stops replay for that engine because its commit outcome may be unknown. Resume events identify the connection changes so timing comparisons can use cases measured in matching conditions.

Each recipe downloads and caches the official datasets and parameters under `data/`. SNB uses the Interactive v1 **CsvComposite & LongDateFormatter** archives. FinBench uses its published transaction snapshots. Downloads print SHA-256 hashes. Leave room for the cached archives, extracted CSVs, and one engine's working database at a time. SF10 can take hours depending on the machine, engine, and query limits. [SNB datasets](https://ldbcouncil.org/benchmarks/snb/datasets/), [FinBench datasets](https://ldbcouncil.org/benchmarks/finbench/datasets/)

## Workload coverage

The default read sweep takes the first five rows per published complex-read parameter file where available. For short reads, it takes IDs from the snapshot because these operations normally follow other operations in the official driver. FinBench short reads use a fixed time window and threshold. `--sample-seed` samples across each input source. `--per-read` sets the input count, `--read-passes` sets the number of measured passes, and `--warmup` adds one warm-up per operation. These flags are available on `just selected` and the `cases` command. The case file preserves the exact parameters and execution order for every engine.

Add update cases with `just snb-mixed 1 results/snb-sf1-mixed` or `just finbench-mixed 1 results/finbench-sf1-mixed`. These recipes append up to 200 time-ordered writes from the published streams. To execute writes, add verified postconditions to the case file and replay it with `just run`. Each engine starts with a fresh snapshot. FinBench has draft templates for TW14, TW15, TW18, and TW19. The other writes and all three read-write operations are listed as omissions. [SNB Interactive v1](https://ldbcouncil.org/benchmarks/snb/interactive/), [FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf)

Each run records `ldbc_op` for the published operation and `op` for the selected template ID. This matters for FinBench because the Neo4j draft numbering differs from the published workload. Under the canonical profile, the 18 FinBench reads yield eight scheduled operations and ten documented omissions. The plain-Cypher Jaccard pack adds one scheduled read. Some scheduled queries may still fail or return unexpected results through a driver. [Source catalog](queries/sources.json), [deviation and omission notes](deviations/README.md)

## Query sources and deviations

The [source catalog](queries/sources.json) pins the upstream revisions and hashes the query files byte for byte. SNB Interactive v1 uses LDBC's Neo4j/Cypher reference. FinBench uses the Neo4j Cypher draft in LDBC's reference implementations repository. The draft includes templates from an earlier workload revision. The catalog retains those templates for provenance and records which operations the published workload can replay. [SNB query source](https://github.com/ldbc/ldbc_snb_interactive_v1_impls/tree/11db98cc2ba14c33492f6c0c34e68c8be7e22e5f/cypher/queries), [FinBench query source](https://github.com/ldbc/ldbc_finbench_transaction_impls/tree/5dc5fb5d6a1fd6324c2dfcd555c5bb6985371a2c/neo4j)

The runner explicitly applies the documented Kùzu-family rewrites to **Kùzu, Ladybug, and RyuGraph**. In SNB, eight of the 21 canonical read operations use the logical `Message` label. Without `snb-typed-message`, all three engines rejected those eight operations in a fixture probe because the loaded schema has separate `Post` and `Comment` tables. The rewrite uses a table union, and replay still checks each response against the same expected rows. In FinBench, the default `finbench-map-projection` pack expresses TSR1’s result as a literal map for these engines. [Rewrite details and validation status](deviations/README.md)

Each case records the canonical and executed query hashes. The leaderboard and charts identify which packs were used. Use `--no-default-packs` to replay the canonical queries, `--pack` to request an additional documented deviation, and `just queries` to check upstream hashes and view provenance and omissions.

```sh
just sf1-cohort-quick results/snb-sf1-typed-canonical --no-default-packs \
    --engine kuzu --engine ladybug --engine ryugraph
just finbench 1 results/finbench-sf1-jaccard --pack finbench-cypher-jaccard
```

The FinBench TCR10 deviation replaces Neo4j GDS's Jaccard function with standard Cypher.[^jaccard] The draft-only APOC operation stays recorded as an omission.[^apoc]

[^jaccard]: The Cypher form groups investments by company and tracks whether each person invested there, then divides the intersection count by the union count. It follows the specification's three-decimal rounding and gives zero for an empty union. The test fixture checks duplicate investments, overlapping and disjoint sets, empty sets, and time-window boundaries against an independent Python oracle on all eight engines. [Published FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf), [upstream TCR10](https://github.com/ldbc/ldbc_finbench_transaction_impls/blob/5dc5fb5d6a1fd6324c2dfcd555c5bb6985371a2c/neo4j/queries/tcr-10.cypher)

[^apoc]: Neo4j documents `reduce(total = 0.0, x IN values | total + x)` as the standard Cypher replacement for `apoc.coll.sum(values)` on a numeric list. The draft's `tcr-11.cypher` computes investment ratios. Published TCR11 is guarantee-chain detection, which the draft places in `tcr-12.cypher`. Replacing the APOC call would still leave an investment-ratio operation outside the current workload. [Neo4j APOC documentation](https://neo4j.com/docs/apoc/current/overview/apoc.coll/apoc.coll.sum/), [published FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf)

## Snapshot loading

The loaders share the same input schema, data normalization and snapshot checks. Import wall time and peak RSS cover preparation, loading, index creation, and closing the loaded store. Peak RSS includes the worker and its loader subprocesses. Startup, reopen, and post-load verification are separate measurements. The plan, settings and worker events record the chosen loader. Each store starts empty.

| Engine | Snapshot loader |
| --- | --- |
| ArcadeDB | Native `graph_batch()` with the write-ahead log enabled, serial flushing, and one commit per flush. The runner verifies the persisted edge counts after reopening. [ArcadeDB bulk loads](https://docs.arcadedb.com/arcadedb-python/how-to/bulk-loads) |
| Grafeo | Native typed CSV loads for scalar node fields and edges. Native node batches preserve list-valued fields. [Grafeo import APIs](https://docs.grafeo.dev/languages/python/database/#data-import) |
| GraphDBLite | Native node and edge batches. Batched Cypher creates nodes with multiple labels in one operation. The loader resolves relationship endpoints in batches through a temporary SQLite index. |
| GraphQLite | `insert_nodes_bulk` and `insert_edges_bulk` with the returned endpoint ID map. A final SQLite transaction removes the temporary text IDs and adds the shared SNB labels, preserving numeric `id` properties and one node per input row. [Bulk import API](https://colliery-io.github.io/graphqlite/how-to/bulk-import/), [SQLite storage schema](https://colliery-io.github.io/graphqlite/reference/database-schema/) |
| Kùzu, Ladybug, RyuGraph | `COPY FROM` with staged Parquet. The column types preserve numbers, lists and nulls, and each relationship file has one source and target type. [Ladybug Parquet import](https://docs.ladybugdb.com/import/parquet/) |
| NeuG | Embedded `COPY FROM` on typed CSV, with node and relationship table creation and copies grouped in one explicit transaction. Nodes use one physical label, so canonical reads that require the shared `Message`, `Place`, or `Organisation` labels can fail. [NeuG bulk import](https://neug.io/docs/data_io/import_data/) |
| Relate | Parameterized `UNWIND` batches through the embedded Python driver. [Relate Python ingestion guide](https://graphacademy.neo4j.com/courses/relate-get-started-python/1-install-load-and-query/3-seed-data) |
| Velr | Offline import of staged typed CSV with `velr-admin` 0.2.61. The load event records staging time, import time, importer version and binary hash, and child-process peak RSS. [Velr CLI releases](https://github.com/velr-ai/velr/releases) |

GraphDBLite 0.2.0 caps the intermediate rows in a Cypher scan at 100,000. The runner verifies the same node and edge counts by reading the reopened SQLite store in read-only mode through Python. Every timed Cypher operation still runs through the pinned GraphDBLite driver.

## Reading the output

Each run has a `run.json` configuration, a copy of its case sequence, a JSONL summary, and individual engine logs and event files. The engine files include the first-result startup time, load time, process peak RSS, reopened snapshot counts, and per-case latency through full result consumption. Startup measurements distinguish empty process launch from reopening in the same worker. The supervisor records external deadlines separately from errors returned by a driver. Each externally timed-out case reports the configured deadline as its duration, and `query_deadline_stops` counts these stops in the engine summary.

## Verifying query results

The runner compares the complete result with expected rows after timing the query. A wrong result fails and records the actual and expected rows. A case without an expected answer fails before execution. Included source-data checks cover SNB IS1 and IS4 and FinBench TSR1. The checked-in SF1 references cover the five-input and 50-input schedules. Their answers come from the pinned LDBC Cypher on Neo4j 5.26.31 using the same initial snapshot as the benchmark engines. The larger reference stores one answer per distinct input. Replay still verifies every attempt. LDBC also publishes mixed-workload validation data generated with its Neo4j implementation. [Five-input reference](references/snb-sf1.json), [50-input reference](references/snb-sf1-p99.json), [LDBC validation data](https://github.com/ldbc/ldbc_snb_interactive_v1_impls#validation-parameters)

The runner preserves row multiplicity and ordering. For SNB it accepts arbitrary order inside the specified IC1 and IC12 collections and within equal sort-key groups in IC14 and IS7. The node sequence inside each IC14 path must still match. [SNB Interactive v1 specification](https://ldbcouncil.org/ldbc_snb_docs/workload-interactive-v1.pdf)

```sh
just selected --workload snb-sf1 --reference snb-sf1=references/snb-sf1.json --engine velr
just run snb 1 data/snb/sf1 results/snb-sf1.cases.jsonl results/velr-repeat \
    --reference references/snb-sf1.json --engine velr
```

For another SNB read schedule, load the matching starting snapshot on a separate Neo4j reference and run `just reference-snb --help`. The command checks node and relationship counts and ties its expected rows to the case-file and canonical-query hashes. The reference server is used during answer generation. Replay uses the saved answer file on the benchmark host.

To regenerate the larger SF1 schedule and its expected answers, run these commands with the reference server listening on local port 7474.

```sh
just sf1-p99-cases results/snb-sf1-p99.cases.jsonl
just reference-snb --scale 1 --cases results/snb-sf1-p99.cases.jsonl \
    --archive data/downloads/social_network-sf1-CsvComposite-LongDateFormatter.tar.zst \
    --nodes 3181724 --relationships 17256038 --out results/snb-sf1-p99-reference.json
```

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

An operation earns up to three points based on its nearest-rank p99. Every measured attempt must pass verification to earn points. The standings sum the unrounded operation scores and show how many operations each engine verified.

| Verified p99 | Points per operation |
| --- | ---: |
| 2.5 s or slower | 0 |
| 15 ms or faster | 3 |

Between these endpoints, points follow `3 × log(2500 / p99_ms) / log(2500 / 15)`. Equal proportional improvements earn equal point gains. With 500 verified measurements per operation, nearest-rank p99 is the 495th value in ascending order. With fewer than 100 measurements it is the maximum. The operation details show scheduled measurements, verified responses, timeouts, p99, and median. The standings also show the geometric mean of p99 across the operations fully verified by every engine, along with the shared operation IDs and coverage. Operations omitted by every engine are outside the report. The operation charts include query RSS and import measurements.

Repeat `--run` to combine engines from separate result directories. Each engine needs one source, and the runs must match on workload, scale, case schedule, expected answers, platform, Python version, and limits. Use `--exclude-engine` to leave an engine out of the cohort. The JSON records the scoring endpoints, each fully verified operation's p99, the awarded points, and the operations used in the geometric mean.

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

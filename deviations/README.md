# Query deviations and omissions

Engine adapters handle connection setup, parameter binding, result handling, and loading. A workload statement comes directly from the hashed canonical file or from an explicitly selected pack in this directory.

| Pack | Scope | Change |
| --- | --- | --- |
| [snb-typed-message](snb-typed-message/pack.json) | Ladybug, Kùzu, RyuGraph | In eight read templates, use the engines' table-union syntax `:Post:Comment` for the logical `:Message` label. The fixture checks the union and derives the IS4 answer from the CSV. Other results need workload-level validation. |
| [finbench-cypher-jaccard](finbench-cypher-jaccard/pack.json) | All eight engines | Implement published TCR10 in Cypher using distinct companies, intersection and union counts, and specification rounding. This pack supersedes the default omission of the GDS call. The independent fixture tests 16 inputs on each engine. |
| [finbench-map-projection](finbench-map-projection/pack.json) | GraphDBLite, Ladybug, Kùzu, RyuGraph | Return the same three TSR1 account fields as a literal map. The fixture checks the whole result against the input Account row. |

Each `pack.json` gives the suite, operation, target engines, original and replacement hashes, reason, and validation evidence. Replacement files contain complete statements for review. For example, `diff -u queries/finbench/tcr-10.cypher deviations/finbench-cypher-jaccard/TCR10.cypher` shows the Jaccard delta. The runner rejects packs with stale hashes or overlapping edits.

The typed message pack passed the IS4 fixture on Ladybug, Kùzu, and RyuGraph. All three reported an unsupported `toInteger` function for IC2 after the label change.

## FinBench draft gaps

- Published **TCR10** is omitted in canonical runs because the LDBC-maintained template uses `gds.similarity.jaccard`. Enable the named pack to execute the documented Cypher variant.
- Draft-only **tcr-11** uses `apoc.coll.sum`. It is an investment-ratio operation. Neo4j documents `reduce()` as a plain-Cypher equivalent for its numeric list sum. Published **TCR11** is a different guarantee-chain operation, corresponding to **tcr-12** in the draft. [APOC documentation](https://neo4j.com/docs/apoc/current/overview/apoc.coll/apoc.coll.sum/), [LDBC FinBench specification](https://ldbcouncil.org/ldbc_finbench_docs/ldbc-finbench-specification.pdf)
- Published **TCR1, TCR2, TCR5–9, TCR11, and TCR12** require per-hop truncation that the draft leaves unimplemented. **TCR9** also expects bounds absent from the published inputs. These operations are omitted.
- Draft **tsr-7** and **tsr-8** lie outside the published six simple reads. The mapped write-stream subset consists of **TW14, TW15, TW18, and TW19**. The draft has no mapped equivalents for the remaining 15 writes and three read-write transactions. [Neo4j reference draft](https://github.com/ldbc/ldbc_finbench_transaction_impls/tree/5dc5fb5d6a1fd6324c2dfcd555c5bb6985371a2c/neo4j)

## Snapshot wiring

SNB keeps the logical node labels of the upstream loader. The typed engines use concrete node tables and grouped relationship tables. ArcadeDB represents parent labels as vertex type inheritance. All FinBench engines use the short relationship names from the Neo4j templates and the same input types, including signed 64-bit IDs and epoch milliseconds. These are load-time mappings. The case texts stay in the query catalog and explicit packs. Velr uses the published `velr-admin` bulk importer against normalized CSV, then the pinned Python driver executes the workload.

Neo4j, FalkorDB, and Memgraph load the shared SNB labels alongside the concrete labels. TuGraph's typed schema assigns one label to each vertex, so its load uses concrete labels such as `Post` and `Comment`. The SNB `email` and `speaks` lists use JSON-formatted string properties because the TuGraph bulk schema supports scalar types. The replay uses the canonical statements and checks the complete returned values against the saved reference, including the difference between a string and a list. [TuGraph bulk schema](https://tugraph-db.readthedocs.io/en/latest/6.utility-tools/1.data-import.html)

The FalkorDB CSV bulk loader trims text fields and infers array element types. The timed import restores affected strings and string arrays in indexed batches before saving the store. [FalkorDB CSV bulk loader](https://github.com/FalkorDB/falkordb-bulk-loader)

The TuGraph Python SDK exposes Cypher over REST. The runner sends the REST API's `parameters` field through the pinned SDK transport so the server receives the catalog statement and its parameters separately. [TuGraph Python SDK](https://tugraph-db.readthedocs.io/en/latest/7.client-tools/1.python-client.html), [REST parameter binding](https://tugraph-db.readthedocs.io/en/v4.5.0/7.client-tools/9.restful-api-legacy.html)

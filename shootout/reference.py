"""Generate independent SNB read answers from the pinned LDBC Cypher."""

from hashlib import file_digest, sha256
import json
from urllib.request import Request, urlopen

from .catalog import load
from .run import bind, cases, describe_case
from .verification import answer_rows, equal_rows


def neo4j_rows(endpoint, query, params=None):
    payload = {"statements": [{"statement": query, "parameters": params or {}, "resultDataContents": ["row"]}]}
    request = Request(endpoint, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=600) as response:
        result = json.load(response)
    if result["errors"]:
        raise ValueError(result["errors"][0]["message"])
    return [item["row"] for item in result["results"][0]["data"]]


def build_reference(args):
    suite = "snb"
    queries = load(suite)
    endpoint = f"{args.neo4j.rstrip('/')}/db/{args.database}/tx/commit"
    version, edition = neo4j_rows(endpoint, "CALL dbms.components() YIELD versions, edition RETURN versions, edition")[0]
    node_count = neo4j_rows(endpoint, "MATCH (n) RETURN count(n)")[0][0]
    edge_count = neo4j_rows(endpoint, "MATCH ()-[r]->() RETURN count(r)")[0][0]
    if (node_count, edge_count) != (args.nodes, args.relationships):
        raise ValueError(f"Neo4j snapshot counts differ from the requested snapshot: {node_count} nodes, {edge_count} edges")
    answers = []
    for number, item in cases(args.cases):
        event, query = describe_case(suite, queries, number, item)
        if event.get("status") == "omitted":
            continue
        if query is None or query.kind != "read":
            raise ValueError(f"Reference generation requires a read-only schedule (case {number})")
        try:
            rows = neo4j_rows(endpoint, query.text, bind(query, item["params"]))
        except ValueError as error:
            raise ValueError(f"Case {number} ({query.operation}): {error}") from error
        if not answer_rows(rows) or ("expected" in item and not equal_rows(
            rows, item["expected"], item.get("unordered", False), suite=suite, operation=query.operation,
        )):
            raise ValueError(f"Reference contradicts the source-data answer for case {number} ({query.operation})")
        answers.append({"case": number, "op": query.operation, "canonical_sha256": query.upstream_sha256, "rows": rows})
        print(f"Reference {number} {query.operation}: {len(rows)} rows", flush=True)
    if not answers:
        raise ValueError("No SNB reads to validate")
    with args.archive.open("rb") as stream:
        archive_sha = file_digest(stream, "sha256").hexdigest()
    pack = {
        "format": 1, "suite": suite, "scale": args.scale, "cases_sha256": sha256(args.cases.read_bytes()).hexdigest(),
        "source": {"engine": "Neo4j", "version": version[0], "edition": edition, "nodes": node_count,
                   "relationships": edge_count, "archive": args.archive.name, "archive_sha256": archive_sha},
        "answers": answers,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(pack, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(args.out)

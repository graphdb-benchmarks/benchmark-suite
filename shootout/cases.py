"""Turn published parameters and update streams into a repeatable JSONL replay."""

import csv
from heapq import merge
from itertools import islice
import json
from pathlib import Path
from random import Random
import re
from tempfile import NamedTemporaryFile

from .catalog import SOURCES, load
from .datasets import finbench_entities, open_csv, rows, snb_entities, value

WINDOW = "startTime endTime"
TRUNCATION = "truncationLimit truncationOrder"
FIN_COLUMNS = {
    1: f"id {WINDOW} {TRUNCATION}",
    2: f"id {WINDOW} {TRUNCATION}",
    3: f"id1 id2 {WINDOW}",
    4: f"id1 id2 {WINDOW}",
    5: f"id {WINDOW} {TRUNCATION}",
    6: f"id threshold1 threshold2 {WINDOW} {TRUNCATION}",
    7: f"id threshold {WINDOW} {TRUNCATION}",
    8: f"id threshold {WINDOW} {TRUNCATION}",
    9: f"id threshold {WINDOW} {TRUNCATION}",
    10: f"pid1 pid2 {WINDOW}",
    11: f"id {WINDOW} {TRUNCATION}",
    12: f"id {WINDOW} {TRUNCATION}",
}
SNB_TEXT = {"firstName", "countryXName", "countryYName", "tagName", "tagClassName", "countryName"}
SNB_UPDATES = {
    1: "personId personFirstName personLastName gender birthday creationDate locationIP browserUsed cityId languages emails tagIds studyAt workAt",
    2: "personId postId creationDate",
    3: "personId commentId creationDate",
    4: "forumId forumTitle creationDate moderatorPersonId tagIds",
    5: "forumId personId joinDate",
    6: "postId imageFile creationDate locationIP browserUsed language content length authorPersonId forumId countryId tagIds",
    7: "commentId creationDate locationIP browserUsed content length authorPersonId countryId replyToPostId replyToCommentId tagIds",
    8: "person1Id person2Id creationDate",
}
SNB_UPDATE_TEXT = {
    "personFirstName", "personLastName", "gender", "locationIP", "browserUsed", "forumTitle", "imageFile", "language", "content",
}


def case(op, params, **extra):
    return {"op": op, "params": params, **extra}


def sample_rows(records, limit, rng):
    if rng is None:
        return list(islice(records, limit))
    sample = list(islice(records, limit))
    for index, record in enumerate(records, start=limit + 1):
        slot = rng.randrange(index)
        if slot < limit:
            sample[slot] = record
    return sample


def sample_nodes(dataset, suite, limit, *, seed=None, labels=()):
    entities = (snb_entities if suite == "snb" else finbench_entities)(dataset)
    return {
        e.label: sample_rows(rows(e), limit, Random(f"{seed}:{e.label}") if seed is not None else None)
        for e in entities if not e.pairs and (not labels or e.label in labels)
    }


def snb_reads(dataset, directory, limit, operations=(), *, seed=None):
    for number in range(1, 15):
        operation = f"IC{number}"
        if operations and operation not in operations:
            continue
        path = directory / f"interactive_{number}_param.txt"
        with open_csv(path) as stream:
            records = csv.DictReader(stream, delimiter="|")
            for row in sample_rows(records, limit, Random(f"{seed}:{operation}") if seed is not None else None):
                params = {k: (v if k in SNB_TEXT else int(v)) for k, v in row.items()}
                if "durationDays" in params:
                    params["endDate"] = params["startDate"] + params.pop("durationDays") * 86_400_000
                yield case(operation, params)

    shorts = [number for number in range(1, 8) if not operations or f"IS{number}" in operations]
    if not shorts:
        return
    labels = {"Person" if number <= 3 else "Comment" if number == 7 else "Post" for number in shorts}
    seeds = sample_nodes(dataset, "snb", limit, seed=seed, labels=labels)
    cities = {}
    if 1 in shorts:
        people = {p["id"] for p in seeds["Person"]}
        for entity in snb_entities(dataset):
            if entity.label == "IS_LOCATED_IN" and entity.spaces == ("Person", "Place"):
                for row in rows(entity):
                    if row["_src"] in people:
                        cities[row["_src"]] = row["_dst"]
                        if len(cities) == len(people):
                            break
    for number in shorts:
        if number <= 3:
            for person in seeds["Person"]:
                extra = {}
                if number == 1:
                    result = [person[key] for key in ("firstName", "lastName", "birthday", "locationIP", "browserUsed")]
                    result.extend((cities[person["id"]], person["gender"], person["creationDate"]))
                    extra["expected"] = [result]
                yield case(f"IS{number}", {"personId": person["id"]}, **extra)
        else:
            for msg in (seeds["Comment"] if number == 7 else seeds["Post"]):
                extra = {"expected": [[msg["creationDate"], msg["content"] or msg["imageFile"]]]} if number == 4 else {}
                yield case(f"IS{number}", {"messageId": msg["id"]}, **extra)


def fin_cell(key, cell):
    if key == "truncationOrder":
        return cell
    if key.startswith("threshold") or key.endswith("Threshold") or key in {"amount", "ratio", "loanAmount", "balance"}:
        return float(cell)
    if key in {"isBlocked", "accountBlocked"}:
        return value(cell, "BOOL")
    if key in {"personName", "companyName", "accountType", "mediumType"}:
        return cell
    return value(cell, "INT64")


def fin_reads(dataset, directory, limit, operations=(), *, seed=None):
    source = SOURCES["finbench"]
    if directory:
        for number, columns in FIN_COLUMNS.items():
            official = f"TCR{number}"
            if operations and official not in operations:
                continue
            template = source["bindings"].get(official, official)
            reason = source["omissions"].get(official)
            if reason and official not in source.get("replacements", {}):
                yield case(template, {}, ldbc_op=official, omitted=reason)
                continue
            path = directory / f"complex_{number}_param.csv"
            with open_csv(path) as stream:
                names = columns.split()
                records = (r for r in csv.reader(stream, delimiter="|") if r and r != ["..."])
                for record in sample_rows(records, limit, Random(f"{seed}:{official}") if seed is not None else None):
                    if len(record) != len(names):
                        raise ValueError(f"{path}: unexpected parameter width")
                    params = {k: fin_cell(k, v) for k, v in zip(names, record)}
                    params["start_time"] = params.pop("startTime")
                    params["end_time"] = params.pop("endTime")
                    if official == "TCR10":
                        params["id1"] = params.pop("pid1")
                        params["id2"] = params.pop("pid2")
                    yield case(template, params, ldbc_op=official)

    shorts = [number for number in range(1, 7) if not operations or f"TSR{number}" in operations]
    if not shorts:
        return
    accounts = sample_nodes(dataset, "finbench", limit, seed=seed, labels=("Account",))["Account"]
    for number in shorts:
        for account in accounts:
            params = {"id": account["id"], "start_time": 0, "end_time": 4_102_444_800_000, "threshold": 0.0}
            expected_map = {k: account[k] for k in ("createTime", "isBlocked", "type")}
            extra = {"expected": [[expected_map]]} if number == 1 else {}
            yield case(f"TSR{number}", params, ldbc_op=f"TSR{number}", **extra)


def snb_updates(path):
    def cell(key, text):
        if key in SNB_UPDATE_TEXT:
            return text
        if key in {"languages", "emails"}:
            return text.split(";") if text else []
        if key == "tagIds":
            return [int(v) for v in text.split(";") if v]
        if key in {"studyAt", "workAt"}:
            return [[int(v) for v in item.split(",")] for item in text.split(";") if item]
        return int(text)

    with open_csv(path) as stream:
        for line in csv.reader(stream, delimiter="|"):
            timestamp, _, kind = map(int, line[:3])
            names = SNB_UPDATES[kind].split()
            fields = line[3:]
            if len(fields) != len(names):
                raise ValueError(f"{path}: IU{kind} has unexpected parameter width {len(fields)}")
            yield timestamp, case(f"IU{kind}", {k: cell(k, v) for k, v in zip(names, fields)})


def fin_updates(path, official, template):
    with open_csv(path) as stream:
        for row in csv.DictReader(stream, delimiter="|"):
            timestamp = value(row["createTime"], "INT64")
            if official in {"TW14", "TW15"}:
                params = {"accountId": int(row.get("accountId") or row["account"]), "loanId": int(row["loanId"]),
                          "amt": float(row["amount"]), "currentTime": timestamp}
            else:
                key = "accountId" if official == "TW18" else "personId"
                params = {key: int(row[key])}
            yield timestamp, case(template, params, ldbc_op=official)


def update_cases(suite, directory: Path, limit):
    if suite == "snb":
        streams = [snb_updates(p) for p in sorted(directory.glob("updateStream_*.csv*"))]
    else:
        source = SOURCES["finbench"]
        streams = []
        for path in sorted(directory.glob("*.csv")):
            match = re.search(r"(ReadWrite|Write)(\d+)\.csv$", path.name)
            if match:
                official = ("TRW" if match[1] == "ReadWrite" else "TW") + match[2]
                if template := source["bindings"].get(official):
                    streams.append(fin_updates(path, official, template))
        for op, reason in source["omissions"].items():
            if op.startswith(("TW", "TRW")):
                yield case(op, {}, ldbc_op=op, omitted=reason)
    if not streams:
        raise ValueError(f"No supported {suite} update streams at {directory}")
    for _, item in islice(merge(*streams, key=lambda row: row[0]), limit):
        yield item


def build(suite, dataset, params, per_read, updates, max_updates, operations=(), *,
          sample_seed=None, read_passes=1, warmup=False):
    if suite == "snb":
        if params is None:
            raise ValueError("SNB needs the published substitution parameter directory")
        reads = snb_reads(dataset, params, per_read, operations, seed=sample_seed)
    else:
        reads = fin_reads(dataset, params, per_read, operations, seed=sample_seed)
    if read_passes == 1 and not warmup:
        yield from reads
    else:
        scheduled = list(reads)
        if warmup:
            by_operation = {}
            for item in scheduled:
                if not item.get("omitted"):
                    by_operation.setdefault(item["op"], item)
            yield from ({**item, "warmup": True} for item in by_operation.values())
        yield from (item for item in scheduled if item.get("omitted"))
        measured = [item for item in scheduled if not item.get("omitted")]
        rng = Random(sample_seed if sample_seed is not None else 0)
        for _ in range(read_passes):
            rng.shuffle(measured)
            yield from measured
    if updates is not None and (not operations or any(op.startswith(("IU", "TW", "TRW")) for op in operations)):
        yield from update_cases(suite, updates, max_updates)


def write_case_file(suite, dataset, params, out, *, per_read, updates, max_updates, operations,
                    sample_seed=None, read_passes=1, warmup=False):
    load(suite)
    out.parent.mkdir(parents=True, exist_ok=True)
    wanted = set(operations or ())
    seen = set()
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=out.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            for item in build(suite, dataset, params, per_read, updates, max_updates, wanted,
                              sample_seed=sample_seed, read_passes=read_passes, warmup=warmup):
                operation = item.get("ldbc_op", item["op"])
                if not wanted or operation in wanted:
                    seen.add(operation)
                    print(json.dumps(item, sort_keys=True), file=stream)
            if missing := wanted - seen:
                raise ValueError(f"No cases generated for {', '.join(sorted(missing))}")
            if not seen:
                raise ValueError("No cases generated")
            stream.close()
            temporary.replace(out)
        finally:
            temporary.unlink(missing_ok=True)

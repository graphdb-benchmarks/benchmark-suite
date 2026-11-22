"""Compare complete answers and bind independent answers to a replay schedule."""

from collections import Counter
from datetime import date, datetime, timedelta
from hashlib import sha256
from itertools import groupby
import json


# The SNB specification uses collections for these fields and leaves ties in
# these ORDER BY keys unspecified. Path node sequences retain their order.
SNB_COLLECTIONS = {"IC1": (8, 9, 11, 12), "IC12": (3,)}
SNB_TIES = {"IC14": (1,), "IS7": (2, 3)}


def encode(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return {"duration_seconds": value.total_seconds()}
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"Can't serialize {type(value).__name__}")


def serialized(value):
    return json.dumps(value, default=encode, sort_keys=True)


def rows_digest(rows):
    return sha256(serialized(rows).encode()).hexdigest()


def answer_rows(value):
    return isinstance(value, list) and all(isinstance(row, list) for row in value)


def read_case_key(item):
    return item["op"], serialized(item["params"])


def equal_rows(rows, expected, unordered=False, *, suite=None, operation=None):
    collections = SNB_COLLECTIONS.get(operation, ()) if suite == "snb" else ()
    ties = SNB_TIES.get(operation, ()) if suite == "snb" else ()

    def normalized(row):
        return serialized([
            sorted(value, key=serialized) if index in collections and isinstance(value, list) else value
            for index, value in enumerate(row)
        ])

    def ordered_groups(result):
        if not ties:
            return list(map(normalized, result))
        return [Counter(map(normalized, group)) for _, group in groupby(
            result, key=lambda row: tuple(row[i] for i in ties),
        )]

    largest_key = max(ties, default=-1)
    if any(len(row) <= largest_key for result in (rows, expected) for row in result):
        return False
    if unordered and not ties:
        return Counter(map(normalized, rows)) == Counter(map(normalized, expected))
    return ordered_groups(rows) == ordered_groups(expected)


def load_reference(path, suite, scale, schedule_sha, listed, queries):
    pack = json.loads(path.read_bytes())
    version = pack.get("format")
    if version not in (1, 2) or (pack.get("suite"), pack.get("scale"), pack.get("cases_sha256")) != (
        suite, scale, schedule_sha,
    ):
        raise ValueError(f"{path}: reference does not match this suite, scale and case schedule")
    answers = {}
    for entry in pack["answers"]:
        number = entry["case"]
        item = listed.get(number)
        query = queries.get(item["op"]) if item else None
        if (not query or query.kind != "read" or number in answers or entry["op"] != query.operation
                or entry["canonical_sha256"] != query.upstream_sha256 or not answer_rows(entry["rows"])):
            raise ValueError(f"{path}: invalid reference answer for case {number}")
        if "expected" in item and not equal_rows(
            entry["rows"], item["expected"], item.get("unordered", False), suite=suite, operation=query.operation,
        ):
            raise ValueError(f"{path}: reference disagrees with inline expected rows for case {number}")
        answers[number] = entry["rows"]
    if version == 2:
        unique = {read_case_key(listed[number]): rows for number, rows in answers.items()}
        if len(unique) != len(answers):
            raise ValueError(f"{path}: duplicate input in the reference answers")
        for number, item in listed.items():
            query = queries.get(item["op"])
            key = read_case_key(item)
            if query is None or query.kind != "read" or key not in unique:
                raise ValueError(f"{path}: repeated-read reference does not cover case {number}")
            rows = unique[key]
            if number not in answers and "expected" in item and not equal_rows(
                rows, item["expected"], item.get("unordered", False), suite=suite, operation=query.operation,
            ):
                raise ValueError(f"{path}: reference disagrees with inline expected rows for case {number}")
            answers[number] = rows
    return answers

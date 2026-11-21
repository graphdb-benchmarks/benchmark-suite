"""Prepare normalized CSVs for the Velr offline importer."""

from collections import defaultdict
import csv
from hashlib import file_digest
import os
from pathlib import Path
from shutil import which
import subprocess
from tempfile import TemporaryDirectory
from time import perf_counter_ns

from .datasets import finbench_entities, rows, snb_entities

TYPES = {"STRING": "string", "STRING[]": "string[]", "INT64": "long", "DOUBLE": "double", "BOOL": "boolean"}


def field(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ";".join(value)
    return value


def stage_snapshot(suite, dataset, folder):
    entities = (snb_entities if suite == "snb" else finbench_entities)(dataset)
    paths = []
    counts = defaultdict(int)
    for index, entity in enumerate(entities):
        path = folder / f"{index:02d}-{entity.label}.csv"
        paths.append(path)
        if entity.pairs:
            reserved = {"_src": f":START_ID({entity.spaces[0]})", "_dst": f":END_ID({entity.spaces[1]})"}
            extra_header = ":TYPE"
        else:
            reserved = {"id": f"id:ID({entity.label})"}
            extra_header = ":LABEL"

        properties = [(key, kind) for key, kind in entity.columns if key != "_label"]
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, delimiter="|")
            writer.writerow([*(reserved.get(key, f"{key}:{TYPES[kind]}") for key, kind in properties), extra_header])
            for row in rows(entity):
                if entity.pairs:
                    value = entity.label
                    key = ("edge", entity.label)
                else:
                    subtype = row.get("_label")
                    value = f"{entity.label};{subtype.title()}" if subtype else entity.label
                    if suite == "snb" and entity.label in {"Post", "Comment"}:
                        value += ";Message"
                    key = ("node", subtype.title() if subtype else entity.label)
                writer.writerow([*(field(row[name]) for name, _ in properties), value])
                counts[key] += 1
    return paths, counts


def offline_import(suite, dataset, path):
    command = os.environ.get("VELR_ADMIN_BIN", "velr-admin")
    executable = which(command)
    if executable is None:
        raise FileNotFoundError(f"{command} not found. Install the Velr CLI or set VELR_ADMIN_BIN.")
    version = subprocess.check_output([executable, "--version"], text=True).strip()
    with Path(executable).open("rb") as stream:
        binary_hash = file_digest(stream, "sha256").hexdigest()

    with TemporaryDirectory(prefix="velr-csv-", dir=path.parent) as staging:
        start = perf_counter_ns()
        files, counts = stage_snapshot(suite, dataset, Path(staging))
        staged = perf_counter_ns()
        subprocess.run([
            executable, "import", str(path), "--id-type=INTEGER", "--delimiter=|",
            "--ignore-empty-strings=true", "--multiline-fields=true", "--trim-strings=false",
            "--legacy-style-quoting=false", "--bad-tolerance=0",
            f"--report-file={Path(staging) / 'import.report'}", *map(str, files),
        ], check=True)
        finished = perf_counter_ns()
    return counts, {
        "name": "velr-admin", "version": version, "binary_sha256": binary_hash,
        "csv_stage_ns": staged - start, "import_ns": finished - staged,
    }

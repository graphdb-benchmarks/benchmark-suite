"""Validate canonical upstream bytes and resolve opt-in deviation manifests."""

from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
SOURCES = json.loads((ROOT / "queries" / "sources.json").read_text())
TOKENS = re.compile(r"""//[^\r\n]*|/\*.*?\*/|'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"|\x60(?:\x60\x60|[^\x60])*\x60|\$([a-zA-Z_]\w*)""", re.DOTALL)


def digest(raw):
    return sha256(raw).hexdigest()


def parameters(text):
    return {token[1] for token in TOKENS.finditer(text) if token[1]}


@dataclass(frozen=True)
class Query:
    operation: str
    kind: str
    text: str
    upstream_sha256: str
    source: str
    deviation: dict | None = None

    def provenance(self):
        return {
            "op": self.operation,
            "source": self.source,
            "canonical_sha256": self.upstream_sha256,
            "executed_sha256": digest(self.text.encode()),
            "deviation": self.deviation,
        }


def load(suite, engine=None, packs=()):
    entries = SOURCES[suite]["operations"]
    selected = {}
    for name in packs:
        if not re.fullmatch(r"[a-z0-9-]+", name):
            raise ValueError(f"Invalid deviation pack name {name!r}")
        folder = ROOT / "deviations" / name
        manifest = json.loads((folder / "pack.json").read_text())
        if engine not in manifest["engines"]:
            continue
        for entry in manifest["queries"]:
            if entry["suite"] != suite:
                continue
            op = entry["op"]
            if op not in entries or op in selected:
                raise ValueError(f"{name}: unknown or duplicate deviation for {suite}/{op}")
            if entry["canonical_sha256"] != entries[op]["sha256"]:
                raise ValueError(f"{name}: stale canonical hash for {op}")
            if not entry["reason"] or not entry["validation"]:
                raise ValueError(f"{name}: provide a reason and validation evidence or status")
            replacement = folder / entry["file"]
            if replacement.resolve().parent != folder.resolve():
                raise ValueError(f"{name}: replacement must be a file in its pack")
            raw = replacement.read_bytes()
            if digest(raw) != entry["sha256"]:
                raise ValueError(f"{name}: replacement hash mismatch for {op}")
            selected[op] = (raw, {"pack": name, **entry})

    source = SOURCES[suite]
    result = {}
    for op, entry in entries.items():
        raw = (ROOT / "queries" / suite / entry["file"]).read_bytes()
        actual = digest(raw)
        if actual != entry["sha256"]:
            raise ValueError(f"Canonical query changed on disk: {suite}/{op}")
        text, deviation = selected.get(op, (raw, None))
        url = f"{source['repo']}/blob/{source['revision']}/{source['directory']}/{entry['file']}"
        result[op] = Query(op, entry["kind"], text.decode(), actual, url, deviation)
    return result

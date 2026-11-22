"""Fetch official fixtures and FinBench data without extracting archive links."""

from contextlib import contextmanager
from hashlib import file_digest
from pathlib import Path, PurePosixPath
import shutil
import tarfile
from tempfile import TemporaryDirectory
from urllib.request import urlopen
from zipfile import ZipFile

from .catalog import SOURCES

BASE = "https://datasets.ldbcouncil.org"
FINBENCH = f"{BASE}/finbench"
SNB_FIXTURE_HASH = "034285587555672371183a230544d98e0a1806225726ebf05656982180c98acf"
FIN_FIXTURE_HASH = "888c8fbe06b68cc48de9f07fde8c0fd3295618fc41af13ae1aa216aaec1e0430"


def download(url, path, expected=None):
    if not path.exists():
        print(f"Downloading {url}", flush=True)
        with TemporaryDirectory(dir=path.parent) as work:
            partial = Path(work) / "download"
            with urlopen(url, timeout=120) as response, partial.open("wb") as stream:
                shutil.copyfileobj(response, stream)
            check_hash(partial, expected)
            partial.rename(path)
    else:
        check_hash(path, expected)


def check_hash(path, expected):
    with path.open("rb") as stream:
        actual = file_digest(stream, "sha256").hexdigest()
    if expected and expected != actual:
        raise ValueError(f"SHA-256 mismatch for {path}: {actual}")
    print(f"{path.name} sha256={actual}", flush=True)


@contextmanager
def open_tar(path):
    if path.suffix == ".zst":
        import zstandard

        with path.open("rb") as source, zstandard.ZstdDecompressor().stream_reader(source) as stream:
            with tarfile.open(fileobj=stream, mode="r|") as archive:
                yield archive
    else:
        with tarfile.open(path, "r|gz") as archive:
            yield archive


def tar_folder(path, root, destination, *, preserve_executable=False):
    if destination.is_dir():
        return
    prefix = PurePosixPath(root)
    with TemporaryDirectory(dir=destination.parent) as temp, open_tar(path) as archive:
        for member in archive:
            name = PurePosixPath(member.name)
            if not name.is_relative_to(prefix) or not member.isfile() or name.name.endswith(".crc"):
                continue
            relative = name.relative_to(prefix)
            if ".." in relative.parts or not relative.parts:
                raise ValueError(f"Unsafe archive member {member.name}")
            target = Path(temp, *relative.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, target.open("xb") as stream:
                shutil.copyfileobj(source, stream)
            if preserve_executable and member.mode & 0o111:
                target.chmod(0o755)
        if not any(Path(temp).iterdir()):
            raise ValueError(f"No files for {root} in {path}")
        Path(temp).rename(destination)


def finbench(scale, root, archives):
    root.mkdir(parents=True, exist_ok=True)
    archives.mkdir(parents=True, exist_ok=True)
    folder = f"sf{scale}"
    package = archives / f"finbench-{folder}.tar.gz"
    download(f"{FINBENCH}/{folder}.tar.gz", package, FIN_FIXTURE_HASH if scale == "0.01" else None)
    tar_folder(package, folder, root / folder)
    if scale != "0.01":
        params = f"{folder}_read_params"
        destination = root / params
        bundle = archives / f"finbench-{params}.zip"
        download(f"{FINBENCH}/{params}.zip", bundle)
        if not destination.is_dir():
            with TemporaryDirectory(dir=root) as temp, ZipFile(bundle) as archive:
                for index in range(1, 13):
                    name = f"complex_{index}_param.csv"
                    matches = [item for item in archive.namelist()
                               if item.endswith("/" + name) and "__MACOSX/" not in item]
                    if len(matches) != 1:
                        raise ValueError(f"Expected a unique {name} in {bundle}")
                    (Path(temp) / name).write_bytes(archive.read(matches[0]))
                Path(temp).rename(destination)


def fixtures(root, archives):
    archives.mkdir(parents=True, exist_ok=True)
    root.mkdir(parents=True, exist_ok=True)
    source = SOURCES["snb"]
    revision = source["revision"]
    repo = source["repo"].removeprefix("https://github.com/")
    archive = archives / "snb-fixture.tar.gz"
    download(f"https://codeload.github.com/{repo}/tar.gz/{revision}", archive, SNB_FIXTURE_HASH)
    tar_folder(archive, f"{repo.split('/')[1]}-{revision}/cypher/test-data", root / "snb")
    finbench("0.01", root / "finbench", archives)


def snb(scale, root, archives, updates=False):
    root.mkdir(parents=True, exist_ok=True)
    archives.mkdir(parents=True, exist_ok=True)
    bundles = {
        f"social_network-sf{scale}-CsvComposite-LongDateFormatter": ("snb-interactive-v1", f"sf{scale}"),
        f"substitution_parameters-sf{scale}": ("snb-interactive-v1-parameters", f"sf{scale}_params"),
    }
    for name, (folder, target) in bundles.items():
        package = archives / f"{name}.tar.zst"
        download(f"{BASE}/{folder}/{package.name}", package)
        tar_folder(package, name, root / target)
    if updates:
        name = f"social_network-sf{scale}-numpart-1"
        package = archives / f"{name}.tar.zst"
        download(f"{BASE}/snb-interactive-v1-updates/{package.name}", package)
        tar_folder(package, name, root / f"sf{scale}_updates")

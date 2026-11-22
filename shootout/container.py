"""Build and run the benchmark image for the Docker host's native CPU."""

import hashlib
import os
from pathlib import Path
import platform
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
ARCH = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}
RELEASES = {
    "arm64": ("aarch64", "tugraph/tugraph-runtime-arm64v8-centos7:4.5.1",
              "f33257cfdd9c6d4c11a2981019541daf72d43b465d72f01bbeb326c6dd739c82"),
    "amd64": ("x86_64", "tugraph/tugraph-runtime-centos7:4.5.2",
              "e60a171051db794f7c42dfc75b0c95cdf27b35952bc9b566d2e0793e383ec9cf"),
}
ELF_MACHINES = {"amd64": 62, "arm64": 183}
NATIVE_BINARIES = (
    "/opt/java/openjdk/bin/java", "/opt/shootout/bin/velr-admin",
    "/opt/shootout/bin/redis-server", "/opt/shootout/lib/falkordb.so",
    "/usr/lib/memgraph/memgraph", "/opt/tugraph/bin/lgraph_server", "/opt/tugraph/bin/lgraph_import",
)


def ensure_image(docker):
    host = subprocess.check_output([*docker, "info", "--format", "{{.OSType}} {{.Architecture}}"], text=True)
    system, machine = host.split()
    arch = ARCH.get(machine)
    if system != "linux" or not arch:
        raise ValueError(f"Docker host {system}/{machine} needs a Linux ARM64 or x86-64 runtime")
    target = f"linux/{arch}"
    velr_arch, tugraph_image, sha = RELEASES[arch]
    fingerprint = hashlib.sha256()
    for path in [ROOT / "Dockerfile", *sorted(ROOT.glob("requirements*.txt"))]:
        fingerprint.update(path.read_bytes())
    tag = f"shootout-benchmark:{arch}-{fingerprint.hexdigest()[:16]}"
    inspect = [*docker, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}} {{.Id}}", tag]
    result = subprocess.run(inspect, text=True, capture_output=True)
    if result.returncode:
        subprocess.run([*docker, "build", "--platform", target, "--tag", tag,
                        "--build-arg", f"TUGRAPH_IMAGE={tugraph_image}", "--build-arg", f"VELR_ARCH={velr_arch}",
                        "--build-arg", f"VELR_SHA256={sha}", str(ROOT)], check=True)
        result = subprocess.run(inspect, check=True, text=True, capture_output=True)
    image_platform, image_id = result.stdout.split()
    if image_platform != target:
        raise ValueError(f"Benchmark image has platform {image_platform}, expected {target}")
    return target, image_id


def inside(command):
    arch = ARCH.get(platform.machine())
    if sys.platform != "linux" or f"linux/{arch}" != os.environ["SHOOTOUT_CONTAINER_PLATFORM"]:
        raise ValueError("The benchmark container must run on the selected Linux architecture")
    for binary in (sys.executable, *NATIVE_BINARIES):
        with Path(binary).open("rb") as executable:
            header = executable.read(20)
        if header[:6] != b"\x7fELF\x02\x01" or int.from_bytes(header[18:20], "little") != ELF_MACHINES[arch]:
            raise ValueError(f"{binary} is not a native {arch} executable")
    os.execvp(command[0], command)


def workspace_mounts():
    directories = [ROOT]
    for entry in os.environ.get("SHOOTOUT_DOCKER_MOUNTS", "").split(os.pathsep):
        if entry:
            directory = Path(entry).expanduser().resolve()
            if not directory.is_dir():
                raise ValueError(f"Docker mount directory does not exist: {directory}")
            directories.append(directory)
    return [argument for path in dict.fromkeys(directories) for argument in ("--volume", f"{path}:{path}")]


def main():
    command = sys.argv[1:]
    docker = ["docker"]
    if context := os.environ.get("SHOOTOUT_DOCKER_CONTEXT"):
        docker.extend(("--context", context))
    try:
        if not command or command == ["--inside"]:
            raise ValueError("Specify a benchmark command or --build-only")
        if command[:1] == ["--inside"]:
            inside(command[1:])
        target, image_id = ensure_image(docker)
        if command == ["--build-only"]:
            print(f"{target} benchmark image {image_id}")
            return 0
        return subprocess.call([*docker, "run", "--rm", "--init", "--platform", target,
                                "--user", f"{os.getuid()}:{os.getgid()}", *workspace_mounts(),
                                "--workdir", str(ROOT), "--env", "HOME=/tmp",
                                "--env", f"SHOOTOUT_CONTAINER_PLATFORM={target}",
                                "--env", f"SHOOTOUT_CONTAINER_IMAGE={image_id}", image_id, *command])
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Benchmark container failed. {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

"""Detect the CPU and memory capacity available to a benchmark worker."""

import os
from pathlib import Path
import sys


MAX_CORES = 6


def worker_cpu_ids():
    if sys.platform == "linux":
        return sorted(os.sched_getaffinity(0))[:MAX_CORES]
    return None


def query_threads(requested, cpu_ids):
    cores = len(cpu_ids) if cpu_ids is not None else min(MAX_CORES, os.cpu_count() or 1)
    return min(requested, cores) if requested is not None else cores


def cgroup_memory_limits(mount, memberships):
    for line in memberships.splitlines():
        _, controllers, group = line.split(":", 2)
        if not controllers:
            root, filename = mount, "memory.max"
        elif "memory" in controllers.split(","):
            root, filename = mount / "memory", "memory.limit_in_bytes"
        else:
            continue
        directory = root
        for part in ("", *Path(group.lstrip("/")).parts):
            directory /= part
            try:
                limit = (directory / filename).read_text().strip()
            except FileNotFoundError:
                continue
            if limit.isdigit() and int(limit) > 0:
                yield int(limit)


def memory_capacity_bytes():
    physical = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    if sys.platform == "linux":
        memberships = Path("/proc/self/cgroup").read_text()
        return min(physical, min(cgroup_memory_limits(Path("/sys/fs/cgroup"), memberships), default=physical))
    return physical

"""Guard each benchmark worker and its subprocesses on Linux and macOS."""

from contextlib import contextmanager
import os
from select import select
import signal
from struct import Struct
from time import monotonic_ns

BYTES_PER_GB = 1_000_000_000
DEFAULT_RSS_LIMIT_BYTES = 15 * BYTES_PER_GB
RSS_SAMPLE_INTERVAL = 0.1
NS_PER_SECOND = 1_000_000_000
QUERY_DEADLINE_POLICY = "supervisor_process_group"
QUERY_RSS_POLICY = "periodic_and_query_completion"
IMPORT_RSS_KEY = 0
QUERY_EVENT = Struct("!cQQQ")


def process_tree_rss(pid):
    import psutil

    try:
        worker = psutil.Process(pid)
        members = [worker, *worker.children(recursive=True)]
    except psutil.NoSuchProcess:
        return 0
    total = 0
    for member in members:
        try:
            total += member.memory_info().rss
        except psutil.NoSuchProcess:
            continue
    return total


def check_rss_monitor():
    try:
        process_tree_rss(os.getpid())
    except ImportError as error:
        raise RuntimeError("The RSS monitor needs psutil. Run just setup.") from error
    except PermissionError as error:
        raise RuntimeError("The RSS monitor needs permission to inspect its process tree.") from error


def stop_worker(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError:
        if process.poll() is None:
            raise
    process.wait()


@contextmanager
def measurement_window(channel, begin, end, case_number=0, rss_pid=0):
    channel.send(QUERY_EVENT.pack(begin, monotonic_ns(), case_number, rss_pid))
    try:
        yield
    finally:
        channel.send(QUERY_EVENT.pack(end, monotonic_ns(), case_number, rss_pid))
        # Keep the result alive until the supervisor samples the process tree.
        if channel.recv(1) != b"A":
            raise RuntimeError("Query supervisor disconnected")


def query_window(channel, case_number=0, *, rss_pid=0):
    return measurement_window(channel, b"B", b"E", case_number, rss_pid)


def import_window(channel):
    return measurement_window(channel, b"I", b"D")


def supervise_worker(process, *, timeout, rss_limit_bytes, channel=None, query_timeout=None, case_rss=None):
    deadline = monotonic_ns() + int(timeout * NS_PER_SECOND)
    query_deadline = None
    next_sample = 0
    peak_rss = 0
    active_case = None
    query_rss_pid = 0
    channels = [] if channel is None else [channel]

    def sample_rss():
        nonlocal peak_rss
        rss = process_tree_rss(process.pid)
        peak_rss = max(peak_rss, rss)
        if case_rss is not None and active_case is not None:
            measured = process_tree_rss(query_rss_pid) if query_rss_pid else rss
            if measured:
                case_rss[active_case] = max(case_rss.get(active_case, 0), measured)
        return rss

    try:
        while (returncode := process.poll()) is None:
            now = monotonic_ns()
            if now >= next_sample:
                if sample_rss() > rss_limit_bytes:
                    stop_worker(process)
                    return "worker_rss_limit", process.returncode, peak_rss
                next_sample = now + int(RSS_SAMPLE_INTERVAL * NS_PER_SECOND)
            if now >= deadline:
                stop_worker(process)
                return "worker_timeout", None, peak_rss
            wake = min(next_sample, deadline, query_deadline or deadline)
            ready, _, _ = select(channels, [], [], max(0, wake - monotonic_ns()) / NS_PER_SECOND)
            if ready:
                try:
                    message = channel.recv(QUERY_EVENT.size)
                    if not message:
                        channels = []
                        continue
                    event, stamp, case_number, rss_pid = QUERY_EVENT.unpack(message)
                    if event == b"I":
                        active_case = IMPORT_RSS_KEY
                        continue
                    if event == b"B":
                        active_case = case_number or None
                        query_rss_pid = rss_pid
                        query_deadline = stamp + query_timeout * NS_PER_SECOND
                        continue
                    if event == b"D" or stamp <= query_deadline:
                        if active_case is not None and case_rss is not None and sample_rss() > rss_limit_bytes:
                            stop_worker(process)
                            return "worker_rss_limit", process.returncode, peak_rss
                        query_deadline = None
                        active_case = None
                        query_rss_pid = 0
                        channel.send(b"A")
                        continue
                except ConnectionError:
                    channels = []
                    continue
            elif query_deadline is None or monotonic_ns() < query_deadline:
                continue
            if active_case is not None and case_rss is not None:
                sample_rss()
            stop_worker(process)
            return "query_timeout", process.returncode, peak_rss
    except BaseException:
        stop_worker(process)
        raise

    if returncode < 0:
        stop_worker(process)
        return "crash", returncode, peak_rss
    return ("complete" if returncode == 0 else "worker_error"), returncode, peak_rss

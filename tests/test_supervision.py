from pathlib import Path
import signal
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

from shootout.run import cases, engine_order
from shootout.supervision import supervise_worker


class WorkerSupervision(unittest.TestCase):
    def test_velr_first_preserves_the_selection(self):
        self.assertEqual(engine_order(["kuzu", "velr", "relate"]), ["velr", "kuzu", "relate"])
        self.assertEqual(engine_order(["relate"]), ["relate"])

    def test_rss_limit_includes_the_child_process(self):
        allocate = "buffer = bytearray(56_000_000); buffer[::4096] = b'x' * len(buffer[::4096])"
        child = f"{allocate}; import time; time.sleep(20)"
        parent = f"{allocate}; import subprocess, sys; subprocess.run([sys.executable, '-S', '-c', {child!r}])"
        cap = 100_000_000
        with subprocess.Popen([sys.executable, "-S", "-c", parent], start_new_session=True) as process:
            status, returncode, peak = supervise_worker(process, timeout=5, rss_limit_bytes=cap)
        self.assertEqual((status, returncode), ("worker_rss_limit", -signal.SIGKILL))
        self.assertGreater(peak, cap)

    def test_interrupted_event_keeps_the_complete_rows(self):
        with TemporaryDirectory() as directory:
            events = Path(directory) / "worker.jsonl"
            events.write_bytes(b'{"event":"case_start","case":4}\n{"event":"case","message":"\xc3')
            self.assertEqual(list(cases(events, allow_interrupted_line=True)), [
                (1, {"event": "case_start", "case": 4}),
            ])

    def test_external_query_deadline_accepts_a_result_or_kills_a_blocked_worker(self):
        program = """
import socket
import sys
import time
from shootout.supervision import query_window

with socket.socket(fileno=int(sys.argv[1])) as channel:
    with query_window(channel):
        time.sleep(float(sys.argv[2]))
    print("result", flush=True)
"""
        for delay, expected in [(0, "complete"), (10, "query_timeout")]:
            with self.subTest(delay=delay):
                parent, child = socket.socketpair(type=socket.SOCK_DGRAM)
                with parent, child, TemporaryDirectory() as directory:
                    with (Path(directory) / "output").open("w+") as output:
                        with subprocess.Popen(
                            [sys.executable, "-c", program, str(child.fileno()), str(delay)],
                            pass_fds=(child.fileno(),), stdout=output, start_new_session=True,
                        ) as process:
                            child.close()
                            status, returncode, _ = supervise_worker(
                                process, timeout=5, rss_limit_bytes=1_000_000_000,
                                channel=parent, query_timeout=0.2,
                            )
                        output.seek(0)
                        self.assertEqual(status, expected)
                        self.assertEqual(returncode, 0 if delay == 0 else -signal.SIGKILL)
                        self.assertEqual(output.read(), "result\n" if delay == 0 else "")

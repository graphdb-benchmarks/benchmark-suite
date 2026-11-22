from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from shootout.resources import cgroup_memory_limits, query_threads, worker_cpu_ids
from shootout.servers import neo4j_config, neo4j_memory_settings


class ResourceLimits(unittest.TestCase):
    def test_six_cores_respect_the_inherited_linux_mask(self):
        with patch("shootout.resources.sys.platform", "linux"), \
                patch("shootout.resources.os.sched_getaffinity", create=True, return_value={2, 5, 8, 9, 10, 11, 15}):
            self.assertEqual(worker_cpu_ids(), [2, 5, 8, 9, 10, 11])
        self.assertEqual(query_threads(None, [2, 5]), 2)
        self.assertEqual(query_threads(6, [2, 5]), 2)
        self.assertEqual(query_threads(1, [2, 5]), 1)

    def test_cgroup_memory_checks_local_and_ancestor_limits(self):
        with TemporaryDirectory() as directory:
            mount = Path(directory)
            (mount / "parent" / "child").mkdir(parents=True)
            (mount / "memory.max").write_text("max\n")
            (mount / "parent" / "memory.max").write_text("4294967296\n")
            (mount / "parent" / "child" / "memory.max").write_text("8589934592\n")
            self.assertEqual(list(cgroup_memory_limits(mount, "0::/parent/child\n")), [4294967296, 8589934592])
            (mount / "memory" / "nested").mkdir(parents=True)
            (mount / "memory" / "nested" / "memory.limit_in_bytes").write_text("1073741824\n")
            self.assertEqual(list(cgroup_memory_limits(mount, "5:cpu:/nested\n6:memory:/nested\n")), [1073741824])

    def test_neo4j_applies_only_the_recommended_heap_and_cache(self):
        settings = {
            "server.memory.heap.initial_size": "4900m",
            "server.memory.heap.max_size": "4900m",
            "server.memory.pagecache.size": "5600m",
        }
        output = "\n".join(f"{key}={value}" for key, value in settings.items())
        output += "\nserver.jvm.additional=-XX:+ExitOnOutOfMemoryError\n"
        with TemporaryDirectory() as directory:
            root = Path(directory)
            home = root / "neo4j"
            conf = home / "conf"
            conf.mkdir(parents=True)
            for name in ("neo4j.conf", "server-logs.xml", "user-logs.xml", "neo4j-admin.conf"):
                (conf / name).write_text("server.memory.heap.max_size=4G\n" if name == "neo4j.conf" else "")
            with patch("shootout.servers.neo4j_home", return_value=home), \
                    patch("shootout.servers.subprocess.check_output", return_value=output) as execute:
                self.assertEqual(neo4j_memory_settings(15_000_000_000), settings)
                self.assertEqual(execute.call_args.args[0][-1], "--memory=14305m")
                with self.assertRaisesRegex(ValueError, "do not fit"):
                    neo4j_memory_settings(5_000_000_000)
                path = root / "database"
                neo4j_config(path, 7687, settings)
            config = (path / "conf" / "neo4j.conf").read_text().splitlines()
            self.assertTrue(all(f"{key}={value}" in config for key, value in settings.items()))
            self.assertEqual([line for line in config if line.startswith("server.memory.heap.max_size=")],
                             ["server.memory.heap.max_size=4900m"])

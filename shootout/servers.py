"""Foreground database servers owned by a single benchmark worker."""

from contextlib import closing, suppress
import json
import os
from pathlib import Path
from platform import machine
import shutil
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import monotonic, perf_counter_ns, sleep

from .fetch import download, tar_folder

SERVER_VERSIONS = {
    "neo4j": "2026.10.2",
    "falkordb": "4.16.3" if sys.platform == "darwin" else "6.0.4",
    "memgraph": "3.14.0",
    "tugraph": "4.5.1" if sys.platform == "linux" and machine() == "aarch64" else "4.5.2",
}
LINUX_ONLY_SERVERS = {"memgraph": "Memgraph", "tugraph": "TuGraph"}
NEO4J_ARCHIVE = "fcbccee832175fd8b873ca7d8228dbde4cb5cce43f9053833cc7517dc4dd6f31"
NEO4J_DIRECTORY = f"neo4j-community-{SERVER_VERSIONS['neo4j']}"
NEO4J_DEFAULT = Path(__file__).resolve().parents[1] / "data" / "servers" / NEO4J_DIRECTORY


def validate_server_platform(engines):
    if sys.platform == "linux":
        return
    names = [LINUX_ONLY_SERVERS[engine] for engine in engines if engine in LINUX_ONLY_SERVERS]
    if names:
        raise ValueError(f"Native benchmark runs for {', '.join(names)} require a Linux host.")


def fetch_neo4j(root, archives):
    root.mkdir(parents=True, exist_ok=True)
    archives.mkdir(parents=True, exist_ok=True)
    name = f"{NEO4J_DIRECTORY}-unix.tar.gz"
    archive = archives / name
    download(f"https://dist.neo4j.org/{name}", archive, NEO4J_ARCHIVE)
    tar_folder(archive, NEO4J_DIRECTORY, root / NEO4J_DIRECTORY, preserve_executable=True)


def loopback_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def native_executable(setting, default):
    executable = os.environ.get(setting, default)
    if not executable or (found := shutil.which(executable)) is None:
        raise FileNotFoundError(f"Install {default} or set {setting} to its native executable")
    return found


class ForegroundServer:
    def __init__(self, command, connect, unavailable, *, env=None, startup_timeout=90):
        # Inherit the worker's process group so supervisor limits include the server.
        self.process = subprocess.Popen(command, stdin=subprocess.DEVNULL, env=env)
        self.client = None
        try:
            deadline = monotonic() + startup_timeout
            while self.process.poll() is None:
                try:
                    self.client = connect()
                    return
                except unavailable as error:
                    if monotonic() >= deadline:
                        raise TimeoutError(f"{command[0]} did not accept connections") from error
                    sleep(0.1)
            raise RuntimeError(f"{command[0]} exited during startup ({self.process.returncode})")
        except BaseException:
            self.close()
            raise

    @property
    def query_pid(self):
        return self.process.pid

    def close(self):
        try:
            if self.client is not None:
                self.client.close()
        finally:
            if self.process.poll() is None:
                with suppress(ProcessLookupError):
                    self.process.terminate()
                try:
                    self.process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()


def neo4j_config(path, port):
    home = Path(os.environ.get("SHOOTOUT_NEO4J_HOME", NEO4J_DEFAULT)).expanduser().resolve()
    if not (home / "bin" / "neo4j").is_file():
        raise FileNotFoundError(f"{home}: install with just setup-neo4j or set SHOOTOUT_NEO4J_HOME")
    conf = path / "conf"
    conf.mkdir(parents=True, exist_ok=True)
    for name in ("server-logs.xml", "user-logs.xml", "neo4j-admin.conf"):
        shutil.copyfile(home / "conf" / name, conf / name)
    config = {
        **{f"server.directories.{name}": path / name for name in ("data", "logs", "run", "plugins")},
        "server.bolt.listen_address": f"127.0.0.1:{port}",
        "server.http.enabled": "false",
        "server.https.enabled": "false",
        "dbms.security.auth_enabled": "false",
        "dbms.usage_report.enabled": "false",
        "dbms.fleet_manager.enabled": "false",
        "server.fleet_discovery.enabled": "false",
        "server.memory.heap.initial_size": "1G",
        "server.memory.heap.max_size": "4G",
        "server.memory.pagecache.size": "4G",
    }
    defaults = (home / "conf" / "neo4j.conf").read_text().splitlines()
    (conf / "neo4j.conf").write_text("\n".join(
        [line for line in defaults if line.partition("=")[0] not in config]
        + [f"{key}={value}" for key, value in config.items()] + [""]
    ))
    return home, {**os.environ, "NEO4J_HOME": str(home), "NEO4J_CONF": str(conf)}


class Neo4jServer(ForegroundServer):
    def __init__(self, path):
        from neo4j import GraphDatabase
        from neo4j.exceptions import ServiceUnavailable

        port = loopback_port()
        home, env = neo4j_config(path, port)

        def connect():
            driver = GraphDatabase.driver(f"bolt://127.0.0.1:{port}", auth=None, telemetry_disabled=True,
                                          connection_timeout=1)
            try:
                with driver.session(database="neo4j") as session:
                    row = session.run(
                        "CALL dbms.components() YIELD name, versions, edition "
                        "WHERE name = 'Neo4j Kernel' RETURN versions[0], edition"
                    ).single(strict=True)
                    if row is None or row.values() != [SERVER_VERSIONS["neo4j"], "community"]:
                        raise ValueError(f"Expected Neo4j Community {SERVER_VERSIONS['neo4j']}, got {row}")
                return driver
            except BaseException:
                driver.close()
                raise

        super().__init__([home / "bin" / "neo4j", "console"], connect, ServiceUnavailable, env=env)
        self.session = self.client.session(database="neo4j")
        try:
            self.server_pid = int((path / "run" / "neo4j.pid").read_text())
        except BaseException:
            self.close()
            raise

    @property
    def query_pid(self):
        return self.server_pid

    def execute(self, cypher, params):
        return [record.values() for record in self.session.run(cypher, params)]

    def close(self):
        try:
            if hasattr(self, "session"):
                self.session.close()
        finally:
            super().close()


class FalkorDBServer(ForegroundServer):
    def __init__(self, path):
        from falkordb import FalkorDB
        from redis.backoff import NoBackoff
        from redis.exceptions import ConnectionError
        from redis.retry import Retry

        if sys.platform == "darwin":
            from falkordb_bin import get_falkordb_module, get_redis_server

            default_server, default_module = get_redis_server(), get_falkordb_module()
        else:
            default_server, default_module = "redis-server", "/var/lib/falkordb/bin/falkordb.so"
        redis = native_executable("SHOOTOUT_REDIS_BIN", default_server)
        module = Path(os.environ.get("SHOOTOUT_FALKORDB_MODULE", default_module)).expanduser().resolve()
        if not module.is_file():
            raise FileNotFoundError(f"{module}: set SHOOTOUT_FALKORDB_MODULE to the vendor release module")
        path.mkdir(exist_ok=True)
        self.port = loopback_port()

        def connect():
            client = FalkorDB(host="127.0.0.1", port=self.port, socket_connect_timeout=1, retry=Retry(NoBackoff(), 0))
            try:
                module_info = next(row for row in client.connection.module_list() if row["name"] == "graph")
                number = module_info["ver"]
                actual = f"{number // 10000}.{number // 100 % 100}.{number % 100}"
                if actual != SERVER_VERSIONS["falkordb"]:
                    raise ValueError(f"Expected FalkorDB {SERVER_VERSIONS['falkordb']}, got {actual}")
                return client
            except BaseException:
                client.close()
                raise

        super().__init__([
            redis, "--bind", "127.0.0.1", "--port", str(self.port), "--dir", str(path),
            "--dbfilename", "graph.rdb", "--save", "", "--appendonly", "no",
            "--loadmodule", str(module),
        ], connect, ConnectionError)
        self.graph = self.client.select_graph("shootout")

    def execute(self, cypher, params):
        return [list(row) for row in self.graph.query(cypher, params).result_set]


class MemgraphServer(ForegroundServer):
    def __init__(self, path):
        import mgclient

        executable = native_executable("SHOOTOUT_MEMGRAPH_BIN", "memgraph")
        expected = SERVER_VERSIONS["memgraph"]
        actual = subprocess.check_output([executable, "--version"], text=True).strip()
        if actual != f"memgraph version {expected}":
            raise ValueError(f"Expected Memgraph {expected}, got {actual}")
        path.mkdir(exist_ok=True)
        port = loopback_port()

        def connect():
            client = mgclient.connect(host="127.0.0.1", port=port, sslmode=mgclient.MG_SSLMODE_DISABLE)
            client.autocommit = True
            return client

        super().__init__([
            executable, f"--data-directory={path}", "--bolt-address=127.0.0.1", f"--bolt-port={port}",
            "--query-execution-timeout-sec=0", "--telemetry-enabled=false", "--allow-load-csv=true",
            "--storage-snapshot-on-exit=false", "--storage-wal-enabled=true",
            "--storage-snapshot-interval-sec=300", "--log-file=",
        ], connect, mgclient.OperationalError)

    def execute(self, cypher, params):
        with closing(self.client.cursor()) as cursor:
            cursor.execute(cypher, params)
            return [list(row) for row in cursor.fetchall()] if cursor.description else []


class TuGraphServer(ForegroundServer):
    def __init__(self, path):
        import httpx
        import TuGraphClient as sdk

        executable = native_executable("SHOOTOUT_TUGRAPH_BIN", "lgraph_server")
        expected = SERVER_VERSIONS["tugraph"]
        actual = subprocess.check_output([executable, "--version"], text=True).splitlines()[0]
        if not actual.startswith(f"TuGraph v{expected},"):
            raise ValueError(f"Expected TuGraph {expected}, got {actual}")
        path.mkdir(exist_ok=True)
        port = loopback_port()

        class Client(sdk.TuGraphClient):
            def execute(self, cypher, params):
                if not params:
                    return self.call_cypher(cypher)
                # The published SDK omits the documented REST parameter field.
                payload = {"script": cypher, "graph": self.graph, "timeout": 0,
                           "parameters": {f"${name}": value for name, value in params.items()}}
                return self._sync(lambda: self.__post_with_retry__("cypher", payload))

            def close(self):
                self._sync(sdk.requests.aclose)

        def connect():
            # Probe readiness before login to keep the SDK's retry loop out of timed queries.
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                pass
            return Client(f"127.0.0.1:{port}", "admin", "73@TuGraph", retry=1)

        sdk.requests = httpx.AsyncClient(timeout=httpx.Timeout(None, connect=1), trust_env=False)
        config = path / "shootout-server.json"
        config.write_text(json.dumps({
            "directory": str(path), "host": "127.0.0.1", "port": port, "enable_rpc": False,
            "enable_ha": False, "bolt_port": 0, "verbose": 1, "log_dir": "",
        }))
        try:
            super().__init__([executable, "-c", config, "-d", "run"], connect, (OSError, httpx.TransportError))
        except BaseException:
            import asyncio

            asyncio.get_event_loop().run_until_complete(sdk.requests.aclose())
            raise

    def execute(self, cypher, params):
        return self.client.execute(cypher, params)["result"]


SERVER_CLASSES = {
    "neo4j": Neo4jServer, "falkordb": FalkorDBServer, "memgraph": MemgraphServer, "tugraph": TuGraphServer,
}


def import_neo4j(suite, dataset, path):
    from .velr_import import stage_snapshot

    home, env = neo4j_config(path, 0)
    with TemporaryDirectory(prefix="neo4j-csv-", dir=path.parent) as staging:
        start = perf_counter_ns()
        files, counts = stage_snapshot(suite, dataset, Path(staging))
        staged = perf_counter_ns()
        inputs = []
        for file in files:
            with file.open() as stream:
                header = stream.readline()
            kind = "relationships" if ":START_ID(" in header else "nodes"
            inputs.append(f"--{kind}={file}")
        subprocess.run([
            home / "bin" / "neo4j-admin", "database", "import", "full", "neo4j",
            "--id-type=integer", "--ignore-empty-strings=true", "--legacy-style-quoting=false",
            "--bad-tolerance=0", "--delimiter=|", "--array-delimiter=;", "--multiline-fields=true",
            "--max-off-heap-memory=2G", f"--report-file={Path(staging) / 'import.report'}", *inputs,
        ], env={**env, "HEAP_SIZE": "1G"}, check=True)
        finished = perf_counter_ns()
    return counts, {"name": "neo4j-admin database import full", "version": SERVER_VERSIONS["neo4j"],
                    "csv_stage_ns": staged - start, "import_ns": finished - staged}

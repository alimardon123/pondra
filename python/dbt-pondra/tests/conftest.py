"""dbt's adapter suite on a Pondra node: one node for the whole run, started here and killed at the end.

PONDRA_BIN names the binary (default: `pondra` on the PATH, else the repository's release build). The node serves a lake folder named
`lake`, because the Postgres port's database is the lake's name: the profile says `dbname: lake`.
"""
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request

import pytest

pytest_plugins = ["dbt.tests.fixtures.project"]

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
PONDRA_BIN = os.environ.get("PONDRA_BIN") or shutil.which("pondra") or os.path.join(REPO, "target", "release", "pondra")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_ready(http_port: int, proc: subprocess.Popen, seconds: float = 60) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"pondra exited with {proc.returncode} before it was ready")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{http_port}/ready", timeout=2) as r:
                if r.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.2)
    raise RuntimeError("pondra was not ready within a minute")


def stop(proc: subprocess.Popen) -> None:
    """Kill the node by its PID: SIGTERM lets it release its term, SIGKILL if it doesn't stop."""
    if proc.poll() is not None:
        return
    os.kill(proc.pid, signal.SIGTERM)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()


@pytest.fixture(scope="session")
def pondra_node():
    root = tempfile.mkdtemp(prefix="dbt-pondra-")
    lake = os.path.join(root, "lake")
    http, pg = free_port(), free_port()
    env = {**os.environ, "PONDRA_CACHE_DIR": os.path.join(root, "cache")}
    log = open(os.path.join(root, "node.log"), "w")
    proc = subprocess.Popen(
        [PONDRA_BIN, "serve", "--lake", lake, "--addr", f"127.0.0.1:{http}", "--pg", f"127.0.0.1:{pg}"],
        stdout=log, stderr=subprocess.STDOUT, env=env,
    )
    try:
        wait_ready(http, proc)
        yield {"port": pg, "http": http, "pid": proc.pid}
    finally:
        stop(proc)
        log.close()
        shutil.rmtree(root, ignore_errors=True)


@pytest.fixture(scope="class")
def dbt_profile_target(pondra_node):
    # `type: pondra` takes the keys of `type: postgres`; any password, as a node without tokens does
    return {
        "type": "pondra",
        "threads": 4,
        "host": "127.0.0.1",
        "port": pondra_node["port"],
        "user": "u",
        "pass": "x",
        "dbname": "lake",
    }

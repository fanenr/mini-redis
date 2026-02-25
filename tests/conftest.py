from __future__ import annotations

import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Callable, Dict, Iterator

import pytest
import redis

HOST = "127.0.0.1"
READY_TIMEOUT_SEC = 10.0
STOP_TIMEOUT_SEC = 3.0


def _raw_response(response: object, **_: object) -> object:
    if isinstance(response, bytes):
        return response.decode("utf-8")
    return response


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _server_bin() -> Path:
    return _repo_root() / "build" / "server"


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        return int(sock.getsockname()[1])


def _read_stream(stream: object) -> str:
    if stream is None:
        return ""
    reader = getattr(stream, "read", None)
    if reader is None:
        return ""
    try:
        content = reader()
    except Exception:
        return ""
    return content if isinstance(content, str) else ""


def _wait_server_ready(process: subprocess.Popen[str], port: int) -> None:
    deadline = time.monotonic() + READY_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if process.poll() is not None:
            err = _read_stream(process.stderr).strip()
            msg = "server exited before ready"
            if err:
                msg += f": {err}"
            raise RuntimeError(msg)

        try:
            with socket.create_connection((HOST, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)

    raise RuntimeError("server did not become ready before timeout")


def _stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return

    process.terminate()
    try:
        process.wait(timeout=STOP_TIMEOUT_SEC)
        return
    except subprocess.TimeoutExpired:
        pass

    process.kill()
    process.wait(timeout=STOP_TIMEOUT_SEC)


@pytest.fixture(scope="session", autouse=True)
def _check_python_version() -> None:
    if sys.version_info < (3, 13):
        pytest.exit("Python 3.13+ is required for tests in ./tests", returncode=2)


@pytest.fixture(scope="session")
def server() -> Iterator[Dict[str, object]]:
    server_bin = _server_bin()
    if not server_bin.is_file():
        pytest.exit("missing ./build/server, run `make debug` first", returncode=2)

    port = _pick_free_port()
    process = subprocess.Popen(
        [str(server_bin), "--port", str(port)],
        cwd=str(_repo_root()),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_server_ready(process, port)
    except Exception as exc:
        _stop_process(process)
        pytest.exit(f"failed to start server: {exc}", returncode=2)

    info: Dict[str, object] = {"host": HOST, "port": port, "process": process}
    yield info
    _stop_process(process)


@pytest.fixture(scope="session")
def server_addr(server: Dict[str, object]) -> tuple[str, int]:
    host = str(server["host"])
    port = int(server["port"])
    return (host, port)


@pytest.fixture
def raw_socket(server_addr: tuple[str, int]) -> Iterator[socket.socket]:
    sock = socket.create_connection(server_addr, timeout=1.0)
    sock.settimeout(1.0)
    try:
        yield sock
    finally:
        try:
            sock.close()
        except Exception:
            pass


@pytest.fixture
def redis_client(server_addr: tuple[str, int]) -> Iterator[redis.Redis]:
    host, port = server_addr
    client = redis.Redis(
        host=host,
        port=port,
        decode_responses=True,
        socket_connect_timeout=1.0,
        socket_timeout=1.0,
    )
    for cmd in ("PING", "SET", "LSET", "SAVE", "LOAD"):
        client.set_response_callback(cmd, _raw_response)

    try:
        client.execute_command("PING")
    except Exception as exc:
        pytest.fail(f"cannot connect to server: {exc}")

    yield client

    try:
        client.close()
    except Exception:
        pass


@pytest.fixture
def make_key(
    redis_client: redis.Redis, request: pytest.FixtureRequest
) -> Iterator[Callable[[str], str]]:
    namespace = f"t:{request.node.name}:{uuid.uuid4().hex}"
    created: list[str] = []

    def _make(name: str) -> str:
        key = f"{namespace}:{name}"
        created.append(key)
        return key

    yield _make

    if not created:
        return

    try:
        redis_client.execute_command("DEL", *created)
    except Exception:
        pass

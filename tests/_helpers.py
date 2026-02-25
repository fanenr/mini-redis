from __future__ import annotations

import socket
import time


def assert_error_contains(error: BaseException, keyword: str) -> None:
    message = str(error).lower()
    expected = keyword.lower()
    assert expected in message, f"expected `{keyword}` in `{error}`"


def assert_in_range(value: int, min_value: int, max_value: int) -> None:
    assert min_value <= value <= max_value, (
        f"value {value} out of range [{min_value}, {max_value}]"
    )


def encode_resp_command(*parts: str) -> bytes:
    out = [f"*{len(parts)}\r\n".encode("utf-8")]
    for part in parts:
        payload = part.encode("utf-8")
        out.append(f"${len(payload)}\r\n".encode("utf-8"))
        out.append(payload)
        out.append(b"\r\n")
    return b"".join(out)


def recv_until_quiet(sock: socket.socket, quiet_sec: float = 0.2) -> bytes:
    chunks: list[bytes] = []
    deadline = time.monotonic() + quiet_sec
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break

        sock.settimeout(remaining)
        try:
            data = sock.recv(4096)
        except socket.timeout:
            break

        if data == b"":
            break

        chunks.append(data)
        deadline = time.monotonic() + quiet_sec

    return b"".join(chunks)


def send_and_read(
    sock: socket.socket, payload: bytes, quiet_sec: float = 0.2
) -> bytes:
    sock.sendall(payload)
    return recv_until_quiet(sock, quiet_sec=quiet_sec)


def assert_connection_closed(sock: socket.socket, timeout_sec: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        sock.settimeout(min(0.1, remaining))
        try:
            data = sock.recv(1)
        except socket.timeout:
            continue
        assert data == b"", f"expected closed connection, got data: {data!r}"
        return
    raise AssertionError("expected closed connection, but socket stayed open")

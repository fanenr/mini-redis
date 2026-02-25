from __future__ import annotations

from _helpers import (
    assert_connection_closed,
    encode_resp_command,
    send_and_read,
)


def _assert_protocol_error_and_closed(
    raw_socket, payload: bytes, expected_message: bytes, quiet_sec: float = 0.2
) -> None:
    response = send_and_read(raw_socket, payload, quiet_sec=quiet_sec)
    assert expected_message in response
    assert_connection_closed(raw_socket)


def test_unknown_prefix_triggers_protocol_error_and_closes(raw_socket) -> None:
    _assert_protocol_error_and_closed(
        raw_socket, b"?\r\n", b"ERR Protocol error: unknown prefix"
    )


def test_invalid_bulk_length_triggers_protocol_error_and_closes(raw_socket) -> None:
    _assert_protocol_error_and_closed(
        raw_socket, b"*1\r\n$-2\r\n", b"ERR Protocol error: invalid bulk length"
    )


def test_missing_bulk_length_triggers_protocol_error_and_closes(raw_socket) -> None:
    _assert_protocol_error_and_closed(
        raw_socket, b"*1\r\n$\r\n", b"ERR Protocol error: missing bulk length"
    )


def test_bad_bulk_string_encoding_triggers_protocol_error_and_closes(raw_socket) -> None:
    _assert_protocol_error_and_closed(
        raw_socket,
        b"*1\r\n$4\r\nPINGxx",
        b"ERR Protocol error: bad bulk string encoding",
    )


def test_missing_integer_triggers_protocol_error_and_closes(raw_socket) -> None:
    _assert_protocol_error_and_closed(
        raw_socket, b":\r\n", b"ERR Protocol error: missing integer"
    )


def test_invalid_integer_triggers_protocol_error_and_closes(raw_socket) -> None:
    _assert_protocol_error_and_closed(
        raw_socket, b":abc\r\n", b"ERR Protocol error: invalid integer"
    )


def test_missing_array_length_triggers_protocol_error_and_closes(raw_socket) -> None:
    _assert_protocol_error_and_closed(
        raw_socket, b"*\r\n", b"ERR Protocol error: missing array length"
    )


def test_bulk_length_exceeds_limit_triggers_protocol_error_and_closes(raw_socket) -> None:
    # Default proto_max_bulk_len is 512 * 1024 * 1024 in src/config.h.
    too_large_bulk_len = 512 * 1024 * 1024 + 1
    payload = f"*1\r\n${too_large_bulk_len}\r\n".encode("utf-8")
    _assert_protocol_error_and_closed(
        raw_socket, payload, b"ERR Protocol error: bulk length exceeds proto_max_bulk_len"
    )


def test_array_length_exceeds_limit_triggers_protocol_error_and_closes(raw_socket) -> None:
    # Default proto_max_array_len is 1024 * 1024 in src/config.h.
    too_large_array_len = 1024 * 1024 + 1
    payload = f"*{too_large_array_len}\r\n".encode("utf-8")
    _assert_protocol_error_and_closed(
        raw_socket, payload, b"ERR Protocol error: array length exceeds proto_max_array_len"
    )


def test_array_nesting_exceeds_limit_triggers_protocol_error_and_closes(raw_socket) -> None:
    # Default proto_max_nesting is 128 in src/config.h.
    too_deep_nesting = 129
    payload = b"*1\r\n" * too_deep_nesting
    _assert_protocol_error_and_closed(
        raw_socket, payload, b"ERR Protocol error: array nesting exceeds proto_max_nesting"
    )


def test_inline_length_exceeds_limit_triggers_protocol_error_and_closes(raw_socket) -> None:
    # Default proto_max_inline_len is 64 * 1024 in src/config.h.
    payload = b"+" + (b"x" * (64 * 1024 + 1024))
    _assert_protocol_error_and_closed(
        raw_socket,
        payload,
        b"ERR Protocol error: inline length exceeds proto_max_inline_len",
        quiet_sec=0.4,
    )


def test_non_array_request_returns_protocol_error_but_connection_stays_open(
    raw_socket,
) -> None:
    first = send_and_read(raw_socket, b"+PING\r\n")
    assert b"ERR Protocol error: expected array of bulk strings" in first

    second = send_and_read(raw_socket, encode_resp_command("PING"))
    assert b"+PONG\r\n" in second


def test_null_array_request_returns_protocol_error_but_connection_stays_open(
    raw_socket,
) -> None:
    first = send_and_read(raw_socket, b"*-1\r\n")
    assert b"ERR Protocol error: expected array of bulk strings" in first

    second = send_and_read(raw_socket, encode_resp_command("PING", "ok"))
    assert b"$2\r\nok\r\n" in second


def test_non_bulk_array_element_returns_protocol_error_but_connection_stays_open(
    raw_socket,
) -> None:
    first = send_and_read(raw_socket, b"*1\r\n:1\r\n")
    assert b"ERR Protocol error: expected array of bulk strings" in first

    second = send_and_read(raw_socket, encode_resp_command("PING"))
    assert b"+PONG\r\n" in second


def test_valid_and_invalid_request_in_same_packet_returns_both_and_closes(raw_socket) -> None:
    payload = encode_resp_command("PING") + b"?\r\n"
    response = send_and_read(raw_socket, payload)
    assert b"+PONG\r\n" in response
    assert b"ERR Protocol error: unknown prefix" in response
    assert response.find(b"+PONG\r\n") < response.find(b"ERR Protocol error: unknown prefix")
    assert_connection_closed(raw_socket)


def test_two_valid_requests_in_same_packet_both_succeed(raw_socket) -> None:
    payload = encode_resp_command("PING") + encode_resp_command("PING", "ok")
    response = send_and_read(raw_socket, payload)
    assert b"+PONG\r\n" in response
    assert b"$2\r\nok\r\n" in response
    assert response.find(b"+PONG\r\n") < response.find(b"$2\r\nok\r\n")

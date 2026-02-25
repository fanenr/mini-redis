from __future__ import annotations

import pytest
from redis.exceptions import ResponseError

from _helpers import assert_error_contains


def test_ping_returns_pong(redis_client) -> None:
    assert redis_client.execute_command("PING") == "PONG"


def test_ping_echoes_message(redis_client) -> None:
    assert redis_client.execute_command("PING", "hello") == "hello"


def test_ping_rejects_too_many_arguments(redis_client) -> None:
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("PING", "a", "b")
    assert_error_contains(exc_info.value, "wrong number of arguments")


def test_unknown_command_is_reported(redis_client) -> None:
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("NO_SUCH_COMMAND")
    assert_error_contains(exc_info.value, "unknown command")

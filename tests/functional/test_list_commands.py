from __future__ import annotations

import pytest
from redis.exceptions import ResponseError

from _helpers import assert_error_contains


def _seed_list(redis_client, key: str, values: list[str]) -> None:
    redis_client.execute_command("DEL", key)
    redis_client.execute_command("RPUSH", key, *values)


def test_lpush_rpush_and_llen_main_flow(redis_client, make_key) -> None:
    key = make_key("push")

    assert redis_client.execute_command("LPUSH", key, "a", "b") == 2
    assert redis_client.execute_command("RPUSH", key, "c", "d") == 4
    assert redis_client.execute_command("LLEN", key) == 4
    assert redis_client.execute_command("LRANGE", key, 0, -1) == ["b", "a", "c", "d"]


def test_lindex_with_positive_negative_and_out_of_range(redis_client, make_key) -> None:
    key = make_key("lindex")
    _seed_list(redis_client, key, ["a", "b", "c"])

    assert redis_client.execute_command("LINDEX", key, 0) == "a"
    assert redis_client.execute_command("LINDEX", key, -1) == "c"
    assert redis_client.execute_command("LINDEX", key, 99) is None


def test_lrange_with_negative_indexes(redis_client, make_key) -> None:
    key = make_key("lrange")
    _seed_list(redis_client, key, ["a", "b", "c", "d"])

    assert redis_client.execute_command("LRANGE", key, 1, 2) == ["b", "c"]
    assert redis_client.execute_command("LRANGE", key, -3, -2) == ["b", "c"]
    assert redis_client.execute_command("LRANGE", key, 9, 12) == []


def test_lset_main_and_boundaries(redis_client, make_key) -> None:
    key = make_key("lset")
    missing = make_key("missing")
    _seed_list(redis_client, key, ["a", "b", "c"])

    assert redis_client.execute_command("LSET", key, 1, "x") == "OK"
    assert redis_client.execute_command("LRANGE", key, 0, -1) == ["a", "x", "c"]

    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("LSET", missing, 0, "x")
    assert_error_contains(exc_info.value, "no such key")

    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("LSET", key, 100, "x")
    assert_error_contains(exc_info.value, "index out of range")


def test_lrem_zero_positive_negative_count(redis_client, make_key) -> None:
    key = make_key("lrem")
    values = ["a", "b", "a", "a", "c", "a"]

    _seed_list(redis_client, key, values)
    assert redis_client.execute_command("LREM", key, 0, "a") == 4
    assert redis_client.execute_command("LRANGE", key, 0, -1) == ["b", "c"]

    _seed_list(redis_client, key, values)
    assert redis_client.execute_command("LREM", key, 2, "a") == 2
    assert redis_client.execute_command("LRANGE", key, 0, -1) == ["b", "a", "c", "a"]

    _seed_list(redis_client, key, values)
    assert redis_client.execute_command("LREM", key, -2, "a") == 2
    assert redis_client.execute_command("LRANGE", key, 0, -1) == ["a", "b", "a", "c"]


def test_linsert_main_and_boundaries(redis_client, make_key) -> None:
    key = make_key("linsert")
    missing = make_key("linsert-missing")
    _seed_list(redis_client, key, ["a", "b", "c"])

    assert redis_client.execute_command("LINSERT", key, "BEFORE", "b", "x") == 4
    assert redis_client.execute_command("LINSERT", key, "AFTER", "b", "y") == 5
    assert redis_client.execute_command("LRANGE", key, 0, -1) == ["a", "x", "b", "y", "c"]

    assert redis_client.execute_command("LINSERT", key, "BEFORE", "missing", "z") == -1
    assert redis_client.execute_command("LINSERT", missing, "BEFORE", "p", "q") == 0

    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("LINSERT", key, "MIDDLE", "a", "x")
    assert_error_contains(exc_info.value, "syntax error")


def test_lpop_main_and_count(redis_client, make_key) -> None:
    key = make_key("lpop")
    missing = make_key("lpop-missing")
    _seed_list(redis_client, key, ["a", "b", "c"])

    assert redis_client.execute_command("LPOP", key) == "a"
    assert redis_client.execute_command("LPOP", key, 2) == ["b", "c"]
    assert redis_client.execute_command("LPOP", key) is None
    assert redis_client.execute_command("LPOP", missing, 2) is None


def test_rpop_main_and_count(redis_client, make_key) -> None:
    key = make_key("rpop")
    missing = make_key("rpop-missing")
    _seed_list(redis_client, key, ["a", "b", "c", "d"])

    assert redis_client.execute_command("RPOP", key) == "d"
    assert redis_client.execute_command("RPOP", key, 2) == ["c", "b"]
    assert redis_client.execute_command("RPOP", key) == "a"
    assert redis_client.execute_command("RPOP", key) is None
    assert redis_client.execute_command("RPOP", missing, 2) is None


@pytest.mark.parametrize("command", ["LPOP", "RPOP"])
@pytest.mark.parametrize("count", [0, -1])
def test_pop_count_must_be_positive(redis_client, make_key, command: str, count: int) -> None:
    key = make_key(f"bad-count-{command.lower()}")
    _seed_list(redis_client, key, ["x"])
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command(command, key, count)
    assert_error_contains(exc_info.value, "must be positive")


def test_list_commands_reject_wrong_type(redis_client, make_key) -> None:
    key = make_key("wrongtype")
    assert redis_client.execute_command("SET", key, "value") == "OK"

    commands = [
        ("LLEN", [key]),
        ("LINDEX", [key, 0]),
        ("LRANGE", [key, 0, -1]),
        ("LSET", [key, 0, "x"]),
        ("LREM", [key, 0, "x"]),
        ("LINSERT", [key, "BEFORE", "x", "y"]),
        ("LPUSH", [key, "x"]),
        ("RPUSH", [key, "x"]),
        ("LPOP", [key]),
        ("RPOP", [key]),
    ]

    for command, args in commands:
        with pytest.raises(ResponseError) as exc_info:
            redis_client.execute_command(command, *args)
        assert_error_contains(exc_info.value, "wrongtype")


@pytest.mark.parametrize(
    ("command", "args"),
    [
        ("LLEN", tuple()),
        ("LINDEX", ("k",)),
        ("LRANGE", ("k", 0)),
        ("LSET", ("k", 0)),
        ("LREM", ("k", 0)),
        ("LINSERT", ("k", "BEFORE", "x")),
        ("LPUSH", ("k",)),
        ("RPUSH", ("k",)),
        ("LPOP", ("k", 1, 2)),
        ("RPOP", ("k", 1, 2)),
    ],
)
def test_list_commands_validate_argument_count(
    redis_client, command: str, args: tuple[object, ...]
) -> None:
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command(command, *args)
    assert_error_contains(exc_info.value, "wrong number of arguments")

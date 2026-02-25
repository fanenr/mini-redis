from __future__ import annotations

import time

import pytest
from redis.exceptions import ResponseError

from _helpers import assert_error_contains


def test_set_and_get_basic(redis_client, make_key) -> None:
    key = make_key("basic")
    assert redis_client.execute_command("SET", key, "v1") == "OK"
    assert redis_client.execute_command("GET", key) == "v1"


def test_set_with_get_returns_old_value(redis_client, make_key) -> None:
    key = make_key("set-get")
    assert redis_client.execute_command("SET", key, "before") == "OK"
    assert redis_client.execute_command("SET", key, "after", "GET") == "before"
    assert redis_client.execute_command("GET", key) == "after"


def test_set_nx_and_xx_main_paths(redis_client, make_key) -> None:
    key = make_key("nx-xx")
    assert redis_client.execute_command("SET", key, "first", "NX") == "OK"
    assert redis_client.execute_command("SET", key, "second", "NX") is None
    assert redis_client.execute_command("SET", key, "third", "XX") == "OK"
    assert redis_client.execute_command("GET", key) == "third"


def test_set_keepttl_preserves_existing_ttl(redis_client, make_key) -> None:
    key = make_key("keepttl")
    assert redis_client.execute_command("SET", key, "v1", "PX", 2000) == "OK"
    ttl_before = int(redis_client.execute_command("PTTL", key))
    assert ttl_before > 0

    assert redis_client.execute_command("SET", key, "v2", "KEEPTTL") == "OK"
    ttl_after = int(redis_client.execute_command("PTTL", key))
    assert ttl_after > 0
    assert ttl_after <= 2000
    assert redis_client.execute_command("GET", key) == "v2"


def test_set_with_exat_and_pxat_sets_future_expiration(redis_client, make_key) -> None:
    key_exat = make_key("set-exat")
    key_pxat = make_key("set-pxat")
    now_s = int(time.time())
    now_ms = int(time.time() * 1000)

    assert redis_client.execute_command("SET", key_exat, "v-exat", "EXAT", now_s + 2) == "OK"
    assert redis_client.execute_command("SET", key_pxat, "v-pxat", "PXAT", now_ms + 1500) == "OK"

    ttl = int(redis_client.execute_command("TTL", key_exat))
    pttl = int(redis_client.execute_command("PTTL", key_pxat))

    assert 0 <= ttl <= 2
    assert 0 <= pttl <= 1500
    assert redis_client.execute_command("GET", key_exat) == "v-exat"
    assert redis_client.execute_command("GET", key_pxat) == "v-pxat"


def test_set_with_expiration_at_past_time_expires_immediately(redis_client, make_key) -> None:
    key_exat = make_key("past-exat")
    key_pxat = make_key("past-pxat")
    now_s = int(time.time())
    now_ms = int(time.time() * 1000)

    assert redis_client.execute_command("SET", key_exat, "gone", "EXAT", now_s - 1) == "OK"
    assert redis_client.execute_command("SET", key_pxat, "gone", "PXAT", now_ms - 1) == "OK"

    assert redis_client.execute_command("GET", key_exat) is None
    assert redis_client.execute_command("GET", key_pxat) is None


def test_set_rejects_conflicting_options(redis_client, make_key) -> None:
    key = make_key("conflict")
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("SET", key, "v", "NX", "XX")
    assert_error_contains(exc_info.value, "syntax error")


def test_set_rejects_keepttl_with_other_expire_options(redis_client, make_key) -> None:
    key = make_key("keepttl-conflict")
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("SET", key, "v", "KEEPTTL", "EXAT", int(time.time()) + 10)
    assert_error_contains(exc_info.value, "syntax error")


def test_set_rejects_non_positive_expiration(redis_client, make_key) -> None:
    key = make_key("expire-non-positive")
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("SET", key, "v", "EX", 0)
    assert_error_contains(exc_info.value, "not an integer or out of range")


def test_set_with_get_rejects_non_string_old_value(redis_client, make_key) -> None:
    key = make_key("wrongtype")
    assert redis_client.execute_command("LPUSH", key, "a") == 1
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("SET", key, "v", "GET")
    assert_error_contains(exc_info.value, "wrongtype")


def test_incr_decr_family_main_flow(redis_client, make_key) -> None:
    key = make_key("calc")

    assert redis_client.execute_command("INCR", key) == 1
    assert redis_client.execute_command("INCRBY", key, 9) == 10
    assert redis_client.execute_command("DECR", key) == 9
    assert redis_client.execute_command("DECRBY", key, 4) == 5
    assert redis_client.execute_command("GET", key) == "5"


def test_incr_by_and_decr_by_reject_non_integer_delta(redis_client, make_key) -> None:
    key = make_key("bad-delta")
    assert redis_client.execute_command("SET", key, "0") == "OK"

    for command in ("INCRBY", "DECRBY"):
        with pytest.raises(ResponseError) as exc_info:
            redis_client.execute_command(command, key, "not-int")
        assert_error_contains(exc_info.value, "not an integer or out of range")


def test_incr_rejects_non_integer_value(redis_client, make_key) -> None:
    key = make_key("bad-value")
    assert redis_client.execute_command("SET", key, "abc") == "OK"
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("INCR", key)
    assert_error_contains(exc_info.value, "not an integer or out of range")


def test_incr_and_decr_reject_wrong_type(redis_client, make_key) -> None:
    key = make_key("wrongtype-calc")
    assert redis_client.execute_command("LPUSH", key, "x") == 1

    for command in ("INCR", "DECR"):
        with pytest.raises(ResponseError) as exc_info:
            redis_client.execute_command(command, key)
        assert_error_contains(exc_info.value, "wrongtype")


def test_incr_overflow_is_reported(redis_client, make_key) -> None:
    key = make_key("overflow-plus")
    max_i64 = str(2**63 - 1)
    assert redis_client.execute_command("SET", key, max_i64) == "OK"
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("INCR", key)
    assert_error_contains(exc_info.value, "would overflow")


def test_decr_overflow_is_reported(redis_client, make_key) -> None:
    key = make_key("overflow-minus")
    min_i64 = str(-(2**63))
    assert redis_client.execute_command("SET", key, min_i64) == "OK"
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("DECR", key)
    assert_error_contains(exc_info.value, "would overflow")


@pytest.mark.parametrize(
    ("command", "args"),
    [
        ("GET", tuple()),
        ("INCR", tuple()),
        ("INCRBY", ("k",)),
        ("DECR", tuple()),
        ("DECRBY", ("k",)),
    ],
)
def test_string_commands_validate_argument_count(
    redis_client, command: str, args: tuple[object, ...]
) -> None:
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command(command, *args)
    assert_error_contains(exc_info.value, "wrong number of arguments")

from __future__ import annotations

import time

import pytest
from redis.exceptions import ResponseError

from _helpers import assert_error_contains, assert_in_range


def test_del_removes_existing_keys_and_counts(redis_client, make_key) -> None:
    k1 = make_key("k1")
    k2 = make_key("k2")
    k3 = make_key("k3")

    assert redis_client.execute_command("SET", k1, "v1") == "OK"
    assert redis_client.execute_command("SET", k2, "v2") == "OK"

    assert redis_client.execute_command("DEL", k1, k2, k3) == 2
    assert redis_client.execute_command("GET", k1) is None
    assert redis_client.execute_command("GET", k2) is None


def test_del_requires_at_least_one_key(redis_client) -> None:
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("DEL")
    assert_error_contains(exc_info.value, "wrong number of arguments")


def test_expire_and_ttl_main_flow(redis_client, make_key) -> None:
    key = make_key("expire")
    assert redis_client.execute_command("SET", key, "v") == "OK"
    assert redis_client.execute_command("EXPIRE", key, 2) == 1

    ttl = int(redis_client.execute_command("TTL", key))
    assert_in_range(ttl, 0, 2)


def test_pexpire_and_pttl_main_flow(redis_client, make_key) -> None:
    key = make_key("pexpire")
    assert redis_client.execute_command("SET", key, "v") == "OK"
    assert redis_client.execute_command("PEXPIRE", key, 1500) == 1

    pttl = int(redis_client.execute_command("PTTL", key))
    assert_in_range(pttl, 1, 1500)


def test_expireat_and_pexpireat_main_flow(redis_client, make_key) -> None:
    key_seconds = make_key("expireat")
    key_milliseconds = make_key("pexpireat")
    now_s = int(time.time())
    now_ms = int(time.time() * 1000)

    assert redis_client.execute_command("SET", key_seconds, "v") == "OK"
    assert redis_client.execute_command("SET", key_milliseconds, "v") == "OK"

    assert redis_client.execute_command("EXPIREAT", key_seconds, now_s + 2) == 1
    assert redis_client.execute_command("PEXPIREAT", key_milliseconds, now_ms + 1500) == 1

    ttl = int(redis_client.execute_command("TTL", key_seconds))
    pttl = int(redis_client.execute_command("PTTL", key_milliseconds))
    assert_in_range(ttl, 0, 2)
    assert_in_range(pttl, 1, 1500)


def test_expire_family_nx_xx_gt_lt_conditions(redis_client, make_key) -> None:
    key = make_key("conditions")
    key_without_ttl = make_key("conditions-no-ttl")

    assert redis_client.execute_command("SET", key, "v") == "OK"
    assert redis_client.execute_command("SET", key_without_ttl, "v") == "OK"

    assert redis_client.execute_command("PEXPIRE", key, 200, "NX") == 1
    assert redis_client.execute_command("PEXPIRE", key, 300, "NX") == 0
    assert redis_client.execute_command("PEXPIRE", key, 300, "XX") == 1
    assert redis_client.execute_command("PEXPIRE", key, 100, "GT") == 0
    assert redis_client.execute_command("PEXPIRE", key, 50, "LT") == 1

    # 与当前实现保持一致：没有过期时间时 LT 仍可设置
    assert redis_client.execute_command("PEXPIRE", key_without_ttl, 100, "LT") == 1


def test_expireat_in_past_deletes_key(redis_client, make_key) -> None:
    key = make_key("past")
    assert redis_client.execute_command("SET", key, "v") == "OK"
    assert redis_client.execute_command("EXPIREAT", key, int(time.time()) - 1) == 1
    assert redis_client.execute_command("GET", key) is None


def test_ttl_and_pttl_for_missing_and_no_expire(redis_client, make_key) -> None:
    key = make_key("ttl")
    missing = make_key("missing")
    assert redis_client.execute_command("SET", key, "v") == "OK"

    assert redis_client.execute_command("TTL", key) == -1
    assert redis_client.execute_command("PTTL", key) == -1
    assert redis_client.execute_command("TTL", missing) == -2
    assert redis_client.execute_command("PTTL", missing) == -2


@pytest.mark.parametrize("command", ["EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT"])
def test_expire_family_rejects_non_integer_timeout(
    redis_client, make_key, command: str
) -> None:
    key = make_key(f"bad-{command.lower()}")
    assert redis_client.execute_command("SET", key, "v") == "OK"
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command(command, key, "not-int")
    assert_error_contains(exc_info.value, "not an integer or out of range")


@pytest.mark.parametrize("command", ["EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT"])
def test_expire_family_validates_argument_count(
    redis_client, make_key, command: str
) -> None:
    key = make_key(f"argc-{command.lower()}")
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command(command, key)
    assert_error_contains(exc_info.value, "wrong number of arguments")


def test_expire_rejects_invalid_option(redis_client, make_key) -> None:
    key = make_key("bad-option")
    assert redis_client.execute_command("SET", key, "v") == "OK"
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("EXPIRE", key, 5, "BAD")
    assert_error_contains(exc_info.value, "syntax error")


@pytest.mark.parametrize("command", ["TTL", "PTTL"])
def test_ttl_family_validates_argument_count(redis_client, command: str) -> None:
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command(command)
    assert_error_contains(exc_info.value, "wrong number of arguments")

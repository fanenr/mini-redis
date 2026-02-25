from __future__ import annotations

from pathlib import Path

import pytest
from redis.exceptions import ResponseError

from _helpers import assert_error_contains


def _default_dump_path() -> Path:
    return Path(__file__).resolve().parents[2] / "dump.mrdb"


def test_save_and_load_roundtrip(redis_client, make_key, tmp_path) -> None:
    key = make_key("roundtrip")
    snapshot = tmp_path / "snapshot.mrdb"

    assert redis_client.execute_command("SET", key, "before") == "OK"
    assert redis_client.execute_command("SAVE", "TO", str(snapshot)) == "OK"

    assert redis_client.execute_command("SET", key, "after") == "OK"
    assert redis_client.execute_command("GET", key) == "after"

    assert redis_client.execute_command("LOAD", "FROM", str(snapshot)) == "OK"
    assert redis_client.execute_command("GET", key) == "before"


def test_save_and_load_roundtrip_with_default_path(redis_client, make_key, tmp_path) -> None:
    key = make_key("roundtrip-default-path")
    dump_path = _default_dump_path()
    backup_path = tmp_path / "dump.mrdb.bak"
    had_original_dump = dump_path.exists()

    if had_original_dump:
        backup_path.write_bytes(dump_path.read_bytes())

    try:
        assert redis_client.execute_command("SET", key, "before") == "OK"
        assert redis_client.execute_command("SAVE") == "OK"

        assert redis_client.execute_command("SET", key, "after") == "OK"
        assert redis_client.execute_command("GET", key) == "after"

        assert redis_client.execute_command("LOAD") == "OK"
        assert redis_client.execute_command("GET", key) == "before"
    finally:
        if had_original_dump:
            dump_path.write_bytes(backup_path.read_bytes())
        elif dump_path.exists():
            dump_path.unlink()


def test_save_rejects_invalid_option(redis_client, tmp_path) -> None:
    snapshot = tmp_path / "bad-save.mrdb"
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("SAVE", "FROM", str(snapshot))
    assert_error_contains(exc_info.value, "syntax error")


def test_load_rejects_invalid_option(redis_client, tmp_path) -> None:
    snapshot = tmp_path / "bad-load.mrdb"
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("LOAD", "TO", str(snapshot))
    assert_error_contains(exc_info.value, "syntax error")


def test_load_reports_missing_file(redis_client, tmp_path) -> None:
    snapshot = tmp_path / "missing.mrdb"
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command("LOAD", "FROM", str(snapshot))
    assert_error_contains(exc_info.value, "load failed: cannot open file")


@pytest.mark.parametrize(
    ("command", "args"),
    [
        ("SAVE", ("TO",)),
        ("SAVE", ("TO", "path", "extra")),
        ("LOAD", ("FROM",)),
        ("LOAD", ("FROM", "path", "extra")),
    ],
)
def test_save_and_load_validate_argument_count(redis_client, command: str, args: tuple[str, ...]) -> None:
    with pytest.raises(ResponseError) as exc_info:
        redis_client.execute_command(command, *args)
    assert_error_contains(exc_info.value, "syntax error")

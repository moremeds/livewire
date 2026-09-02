"""Tests for clients/ledger.py — the append-only run ledger."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from clients import ledger


@pytest.fixture
def root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    return tmp_path / "ledger"


def _run_row() -> dict:
    now = datetime(2026, 9, 2, 6, 0, tzinfo=UTC)
    return {
        "run_id": "daily-update-20260902T060000Z-42",
        "job": "daily-update",
        "host": "macmini",
        "release_sha": "abc123",
        "presets_sha": "p1",
        "registry_sha": "r1",
        "started": now,
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }


def test_a_run_row_round_trips(root: Path) -> None:
    path = ledger.emit("runs", [_run_row()], run_id="daily-update-20260902T060000Z-42")
    assert path.parent.parent == root / "runs"
    back = pq.read_table(path).to_pylist()
    assert back[0]["job"] == "daily-update"
    assert back[0]["verdict"] is None
    assert back[0]["seq"] == 0


@pytest.mark.parametrize("table", sorted(ledger.LEDGER_TABLES))
def test_every_table_round_trips_a_row(root: Path, table: str) -> None:
    path = ledger.emit(table, [ledger.example_row(table)], run_id="t-20260902T060000Z-1")
    assert pq.read_table(path).num_rows == 1


def test_an_extra_column_raises(root: Path) -> None:
    with pytest.raises(ValueError, match="unexpected column"):
        ledger.emit("runs", [_run_row() | {"nonsense": 1}], run_id="r1")


def test_a_missing_column_raises(root: Path) -> None:
    row = _run_row()
    del row["host"]
    with pytest.raises(ValueError, match="missing column"):
        ledger.emit("runs", [row], run_id="r1")


def test_a_caller_may_not_pass_seq(root: Path) -> None:
    with pytest.raises(ValueError, match="unexpected column"):
        ledger.emit("runs", [_run_row() | {"seq": 3}], run_id="r1")


def test_zero_rows_is_refused(root: Path) -> None:
    with pytest.raises(ValueError, match="zero rows"):
        ledger.emit("runs", [], run_id="r1")


def test_a_second_emit_from_one_run_never_rewrites_the_first(root: Path) -> None:
    first = ledger.emit("runs", [_run_row()], run_id="r1")
    second = ledger.emit("runs", [_run_row()], run_id="r1")
    assert first.name == "r1.parquet"
    assert second.name == "r1-1.parquet"
    assert first.exists() and second.exists()


def test_the_root_comes_from_the_environment(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "elsewhere"))
    assert ledger.ledger_root() == tmp_path / "elsewhere"


def test_the_default_root_is_under_the_lake(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("LW_LEDGER_ROOT", raising=False)
    monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path / "lake"))
    assert ledger.ledger_root() == tmp_path / "lake" / "ledger"


def test_a_run_id_carries_job_and_pid() -> None:
    rid = ledger.new_run_id("daily-update")
    assert rid.startswith("daily-update-") and rid.endswith(f"-{os.getpid()}")

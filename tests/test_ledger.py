"""Tests for clients/ledger.py — the append-only run ledger."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
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


def test_query_reads_back_emitted_rows(root: Path) -> None:
    ledger.emit("runs", [_run_row()], run_id="r1")
    assert ledger.query("select job, verdict from runs") == [{"job": "daily-update", "verdict": None}]


def test_a_table_with_no_files_is_an_empty_view_not_a_sql_error(root: Path) -> None:
    """A check against a table nothing wrote must read UNKNOWN, not explode."""
    ledger.emit("runs", [_run_row()], run_id="r1")
    assert ledger.query("select lane, outcome from lane_results") == []


def test_every_table_is_queryable_on_a_completely_empty_root(root: Path) -> None:
    for table in ledger.LEDGER_TABLES:
        assert ledger.query(f"select * from {table}") == []


def test_files_from_two_dates_are_one_view(root: Path) -> None:
    import shutil

    ledger.emit("runs", [_run_row()], run_id="r1")
    source = next((root / "runs").glob("date=*/*.parquet"))
    later = root / "runs" / "date=2099-01-01"
    later.mkdir(parents=True)
    shutil.copy(source, later / "r2.parquet")
    assert len(ledger.query("select run_id from runs")) == 2


# --- abandoned runs -------------------------------------------------------
# A terminal row is only ever written by the process that owns the run, so a
# SIGKILL leaves the entry row open forever and every reader has to infer
# "no close row and no process" by hand.
# pm:2026-09-16-interrupted-runs-never-closed


def _open_row(run_id: str, job: str = "daily-update", host: str = "macmini", minute: int = 0) -> dict:
    return _run_row() | {
        "run_id": run_id,
        "job": job,
        "host": host,
        "started": datetime(2026, 9, 2, 6, minute, tzinfo=UTC),
    }


def _terminal_rows(job: str = "daily-update") -> list[dict]:
    return ledger.query(f"select run_id, verdict, ended, started from runs where job = '{job}' and ended is not null")


def test_open_run_closes_a_predecessor_that_never_wrote_its_own_terminal_row(root: Path) -> None:
    ledger.emit("runs", [_open_row("daily-update-A")], run_id="daily-update-A")

    abandoned = ledger.open_run(_open_row("daily-update-B", minute=30))

    assert abandoned == ["daily-update-A"]
    closed = {row["run_id"]: row["verdict"] for row in _terminal_rows()}
    assert closed == {"daily-update-A": ledger.ABANDONED}


def test_the_abandoned_row_keeps_the_abandoned_runs_own_start_time(root: Path) -> None:
    """Readers order terminal rows by `started`; a row stamped with the closer's
    clock would sort ahead of a genuine close row and win the verdict."""
    ledger.emit("runs", [_open_row("daily-update-A")], run_id="daily-update-A")

    ledger.open_run(_open_row("daily-update-B", minute=30))

    (closed,) = _terminal_rows()
    assert closed["started"] == datetime(2026, 9, 2, 6, 0, tzinfo=UTC)
    assert closed["ended"] > closed["started"]


def test_a_run_that_closed_itself_is_never_abandoned(root: Path) -> None:
    row = _open_row("daily-update-A")
    ledger.emit("runs", [row], run_id="daily-update-A")
    ledger.emit(
        "runs",
        [row | {"ended": datetime(2026, 9, 2, 6, 20, tzinfo=UTC), "exit_code": 0, "verdict": "OK"}],
        run_id="daily-update-A",
    )

    assert ledger.open_run(_open_row("daily-update-B", minute=30)) == []
    assert {row["verdict"] for row in _terminal_rows()} == {"OK"}


def test_another_jobs_open_run_is_left_alone(root: Path) -> None:
    """Only the same job can state that an earlier run of itself is over."""
    ledger.emit("runs", [_open_row("intraday-A", job="intraday-catchup")], run_id="intraday-A")

    assert ledger.open_run(_open_row("daily-update-B", minute=30)) == []
    assert _terminal_rows("intraday-catchup") == []


def test_another_hosts_open_run_is_left_alone(root: Path) -> None:
    """Dev and prod share one lake; the MacBook must not close the mini's run."""
    ledger.emit("runs", [_open_row("daily-update-A", host="macbook")], run_id="daily-update-A")

    assert ledger.open_run(_open_row("daily-update-B", minute=30)) == []
    assert _terminal_rows() == []


def test_a_late_real_close_row_outranks_the_abandoned_one(root: Path) -> None:
    """Self-healing: a process wrongly declared abandoned was still running, so
    its own terminal row is written later and wins every reader's max(ended).

    The `ended` below is deliberately taken from the abandoned row rather than
    invented: the ordering this relies on is wall-clock, and a fabricated past
    timestamp would assert the opposite of what production does.
    """
    row = _open_row("daily-update-A")
    ledger.emit("runs", [row], run_id="daily-update-A")
    ledger.open_run(_open_row("daily-update-B", minute=30))

    (abandoned,) = _terminal_rows()
    ledger.emit(
        "runs",
        [row | {"ended": abandoned["ended"] + timedelta(minutes=1), "exit_code": 0, "verdict": "OK"}],
        run_id="daily-update-A",
    )

    (winner,) = ledger.query(
        "select verdict from runs where run_id = 'daily-update-A' and ended is not null order by ended desc limit 1"
    )
    assert winner["verdict"] == "OK"


def test_open_run_emits_the_entry_row_it_was_given(root: Path) -> None:
    ledger.open_run(_open_row("daily-update-B", minute=30))

    rows = ledger.query("select run_id, ended from runs where run_id = 'daily-update-B'")
    assert rows == [{"run_id": "daily-update-B", "ended": None}]


def test_open_run_closes_every_stale_predecessor_not_just_the_newest(root: Path) -> None:
    """2026-09-16 left four of them behind in one afternoon."""
    for index in range(4):
        ledger.emit("runs", [_open_row(f"daily-update-{index}", minute=index)], run_id=f"daily-update-{index}")

    abandoned = ledger.open_run(_open_row("daily-update-B", minute=30))

    assert sorted(abandoned) == [f"daily-update-{index}" for index in range(4)]
    assert len(_terminal_rows()) == 4


def test_no_entrypoint_opens_a_run_by_emitting_the_bare_entry_row() -> None:
    """The contract, enforced over the source so a sixth entrypoint cannot
    quietly reintroduce the bug: an entry row goes through `ledger.open_run`.

    Entry and terminal emits are distinguishable in the AST — a terminal row is
    always `run_row | {...}` (a BinOp), an entry row is the bare name. Anything
    emitting `"runs"` with a bare name is opening a run by hand, which is what
    left six rows open on the mini on 2026-09-16.
    pm:2026-09-16-interrupted-runs-never-closed
    """
    import ast

    repo_root = Path(__file__).resolve().parents[1]
    offenders: list[str] = []

    for path in sorted((repo_root / "livewire_scripts").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
            if name not in {"emit", "_emit_ledger"}:
                continue
            first = node.args[0]
            if not (isinstance(first, ast.Constant) and first.value == "runs"):
                continue
            rows = node.args[1] if len(node.args) > 1 else None
            if isinstance(rows, ast.List) and len(rows.elts) == 1 and isinstance(rows.elts[0], ast.Name):
                offenders.append(f"{path.relative_to(repo_root).as_posix()}:{node.lineno}")

    assert offenders == [], f"these open a run without ledger.open_run: {offenders}"


def test_every_module_that_opens_a_run_is_one_we_know_about() -> None:
    """If this list grows, the new entrypoint needs the `BaseException` close
    too — the helper only covers the half a dead process cannot do itself."""
    repo_root = Path(__file__).resolve().parents[1]
    callers = {
        path.relative_to(repo_root).as_posix()
        for path in (repo_root / "livewire_scripts").rglob("*.py")
        if "ledger.open_run(" in path.read_text(encoding="utf-8")
    }

    assert callers == {
        "livewire_scripts/fetch_eia_electricity.py",
        "livewire_scripts/fetch_researched_evidence.py",
        "livewire_scripts/import_researched_identities.py",
        "livewire_scripts/membership_sync.py",
        "livewire_scripts/run_daily_update_job.py",
        "livewire_scripts/security_master_sync.py",
        "livewire_scripts/sync_corporate_actions.py",
        "livewire_scripts/sync_runner.py",
    }


def test_every_module_that_opens_a_run_closes_it_on_a_keyboard_interrupt() -> None:
    """Ctrl-C is a BaseException. `except Exception` around the body skips the
    terminal row entirely — the direct cause of four open rows on 2026-09-16."""
    repo_root = Path(__file__).resolve().parents[1]
    offenders: list[str] = []

    for name in (
        "fetch_eia_electricity.py",
        "fetch_researched_evidence.py",
        "import_researched_identities.py",
        "membership_sync.py",
        "run_daily_update_job.py",
        "security_master_sync.py",
        "sync_corporate_actions.py",
        "sync_runner.py",
    ):
        source = (repo_root / "livewire_scripts" / name).read_text(encoding="utf-8")
        if "except BaseException" not in source:
            offenders.append(name)

    assert offenders == []

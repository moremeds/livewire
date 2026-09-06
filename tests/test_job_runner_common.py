from datetime import UTC, datetime
from pathlib import Path

import pytest

from livewire_scripts.job_runner_common import AlertRequest, build_alert_command, build_log_file


def _request(**overrides) -> AlertRequest:
    base = dict(
        run_date="2026-09-05",
        log_file=Path("/w/logs/daily_update_2026-09-05.log"),
        error_summary="equity lane timed out",
        repo_root=Path("/repo"),
    )
    base.update(overrides)
    return AlertRequest(**base)


def test_the_error_summary_stays_one_token():
    """A summary beginning with -- was unsendable (pm:2026-08-08)."""
    command = build_alert_command(
        "/py", Path("/repo/scripts/livewire_ops.py"), _request(error_summary="--weird"), job_name="daily_update"
    )

    assert "--error-summary=--weird" in command
    assert "--weird" not in [token for token in command if not token.startswith("--error-summary")]


def test_attempts_and_exit_code_are_omitted_when_unknown():
    command = build_alert_command("/py", Path("/a.py"), _request(), job_name="daily_update")

    assert "--attempts" not in command
    assert "--exit-code" not in command


def test_the_intraday_job_gets_the_command_it_always_got():
    command = build_alert_command(
        "/py", Path("/a.py"), _request(attempts=1, exit_code=124), job_name="intraday_catchup"
    )

    assert command[-6:] == ["--job-name", "intraday_catchup", "--attempts", "1", "--exit-code", "124"]


def test_build_log_file_takes_its_clock_from_the_shared_seam():
    path = build_log_file(Path("/w/logs"), "daily_update", now=datetime(2026, 9, 5, 6, tzinfo=UTC))

    assert path == Path("/w/logs/daily_update_2026-09-05.log")


#: Modules that still build the alert argv inline. They are one-shot reporters,
#: not the scheduled-job runners this module consolidated; folding them in is a
#: separate change. Frozen here so a NEW encoding of the contract fails the run.
_KNOWN_INLINE_ALERT_BUILDERS = {
    "coverage_report.py",
    "data_quality_report.py",
    "health_check.py",
    "universe_screener.py",
}


def test_only_one_module_encodes_the_alert_contract():
    """Neither scheduled-job runner may carry its own copy of the argv."""
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1]
    offenders = {
        path.name
        for path in sorted((root / "livewire_scripts").glob("*.py"))
        if path.name != "job_runner_common.py" and '"send-alert"' in path.read_text(encoding="utf-8")
    }

    assert offenders == _KNOWN_INLINE_ALERT_BUILDERS


class TestTheLakeLock:
    """One lock, two jobs. The wait is a number, not a guess (spec section 3)."""

    @pytest.fixture(autouse=True)
    def warehouse(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
        monkeypatch.setenv("LW_RUN_ID", "daily-update-20260906T050000Z-1")
        return tmp_path

    def test_a_free_lock_is_taken_at_once_and_the_wait_is_recorded(self):
        from clients import ledger
        from livewire_scripts.job_runner_common import lake_lock

        with lake_lock("equity", poll_s=1.0, budget_s=7200.0) as waited:
            assert waited is not None
            assert waited < 1.0

        assert ledger.query("select name, scope, unit, source from measurements where name = 'lake_lock_wait_s'") == [
            {"name": "lake_lock_wait_s", "scope": "equity", "unit": "s", "source": "measured"}
        ]

    def test_a_busy_lock_is_polled_until_the_holder_finishes(self):
        from contextlib import ExitStack

        from clients import ledger
        from clients.parquet_io import path_lock
        from livewire_scripts.job_runner_common import lake_lock
        from livewire_scripts.paths import lake_lock_path

        holder = ExitStack()
        holder.enter_context(path_lock(lake_lock_path()))
        now = [0.0]
        polls = []

        def _sleep(seconds):
            polls.append(seconds)
            now[0] += seconds
            holder.close()  # the holder finishes during the first poll

        with lake_lock(
            "corporate-actions",
            poll_s=60.0,
            budget_s=10800.0,
            sleep_fn=_sleep,
            monotonic=lambda: now[0],
        ) as waited:
            assert waited == 60.0

        assert polls == [60.0]
        assert ledger.query("select scope, value from measurements where name = 'lake_lock_wait_s'") == [
            {"scope": "corporate-actions", "value": 60.0}
        ]

    def test_a_wait_past_the_budget_yields_none_and_still_records_the_wait(self):
        from clients import ledger
        from clients.parquet_io import path_lock
        from livewire_scripts.job_runner_common import lake_lock
        from livewire_scripts.paths import lake_lock_path

        now = [0.0]

        def _sleep(seconds):
            now[0] += seconds

        with path_lock(lake_lock_path()):
            with lake_lock("cboe", poll_s=30.0, budget_s=60.0, sleep_fn=_sleep, monotonic=lambda: now[0]) as waited:
                assert waited is None

        assert ledger.query("select scope, value from measurements where name = 'lake_lock_wait_s'") == [
            {"scope": "cboe", "value": 60.0}
        ]

    def test_the_lock_is_released_when_the_lane_body_raises(self):
        import pytest as _pytest

        from clients.parquet_io import path_lock
        from livewire_scripts.job_runner_common import lake_lock
        from livewire_scripts.paths import lake_lock_path

        with _pytest.raises(RuntimeError):
            with lake_lock("silver", poll_s=1.0, budget_s=10.0):
                raise RuntimeError("lane blew up")

        with path_lock(lake_lock_path(), blocking=False) as held:
            assert held is True

    def test_a_ledger_failure_never_kills_the_lane(self, monkeypatch, capsys):
        from clients import ledger
        from livewire_scripts.job_runner_common import lake_lock

        def _boom(*args, **kwargs):
            raise OSError("ledger volume gone")

        monkeypatch.setattr(ledger, "emit", _boom)
        with lake_lock("fx", poll_s=1.0, budget_s=10.0) as waited:
            assert waited is not None
        assert "lake_lock_wait_s" in capsys.readouterr().err

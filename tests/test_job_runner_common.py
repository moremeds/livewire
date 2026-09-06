from datetime import UTC, datetime
from pathlib import Path

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

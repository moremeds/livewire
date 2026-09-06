"""Tests for livewire_scripts.nightly_digest."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from livewire_scripts import nightly_digest, run_daily_update_job, status
from livewire_scripts.nightly_digest import main


@pytest.fixture(autouse=True)
def _no_real_launchctl(tmp_path, monkeypatch):
    """build_digest reaches collect(), which shells out to launchctl AND opens
    the operator's real analytics.duckdb.

    Every digest assertion here would otherwise depend on which plists happen
    to be loaded on the machine running the test — green on this Mac, a
    different verdict on CI, and an unmocked subprocess either way.
    """
    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    monkeypatch.setenv("LW_RUN_ID", "daily-update-test")
    monkeypatch.setattr(
        status,
        "_coverage_headline",
        lambda _db: (_ for _ in ()).throw(FileNotFoundError("analytics.duckdb")),
    )
    monkeypatch.setattr(
        status.subprocess,
        "run",
        lambda *_a, **_kw: CompletedProcess(
            [], 0, stdout="".join(f"-\t0\t{label}\n" for label in status._LAUNCHD_JOBS), stderr=""
        ),
    )


def _body_file_from_cmd(cmd) -> Path:
    return Path(cmd[cmd.index("--body-file") + 1])


def test_disk_tripwire_warns_under_reserve(tmp_path, monkeypatch):
    """The reserve is read at call time, so the override bites without a reload.

    150 GiB free is far above the declared 25 GiB reserve; only an override
    read after this module was imported can turn it into a warning.
    """
    monkeypatch.setenv("LW_DECLARED_FLATFILE_MIN_FREE_GB", "100")

    from livewire_scripts import nightly_digest

    class _Usage:
        total = 400 * (1024**3)
        used = 250 * (1024**3)
        free = 150 * (1024**3)  # 150 GiB < 2*100, but well over 2*25

    monkeypatch.setattr(nightly_digest.shutil, "disk_usage", lambda p: _Usage())
    out = nightly_digest.build_digest(date(2026, 7, 2), tmp_path / "logs", tmp_path)
    assert "⚠" in out and "raw retention deferred" in out

    monkeypatch.delenv("LW_DECLARED_FLATFILE_MIN_FREE_GB")
    assert "raw retention deferred" not in nightly_digest.build_digest(date(2026, 7, 2), tmp_path / "logs", tmp_path)


def test_main_prints_and_no_email_by_default(tmp_path, capsys):
    rc = main(["--run-date", "2026-07-02", "--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)])
    assert rc == 0
    assert "Livewire nightly digest" in capsys.readouterr().out


def test_default_run_date_is_utc(tmp_path, capsys, monkeypatch):
    from livewire_scripts import nightly_digest

    class FrozenDateTime:
        @classmethod
        def now(cls, tz=None):
            if tz is UTC:
                return datetime(2026, 4, 6, 1, 0, tzinfo=UTC)
            return datetime(2026, 4, 5, 18, 0)

    monkeypatch.setattr(nightly_digest, "datetime", FrozenDateTime, raising=False)

    rc = nightly_digest.main(["--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)])

    assert rc == 0
    assert "Livewire nightly digest — 2026-04-06" in capsys.readouterr().out


def test_main_email_invokes_node_script(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "node")
    calls = []

    def fake_runner(cmd, **kwargs):
        calls.append(cmd)
        return CompletedProcess(args=cmd, returncode=0)

    log_dir = tmp_path / "logs"
    rc = main(
        ["--run-date", "2026-07-02", "--email", "--log-dir", str(log_dir), "--data-lake", str(tmp_path)],
        runner=fake_runner,
    )
    assert rc == 0
    assert len(calls) == 1
    cmd = calls[0]
    assert "--mode" in cmd and "digest" in cmd
    assert "--body-file" in cmd
    body_file = _body_file_from_cmd(cmd)
    assert body_file.parent == log_dir
    assert body_file.exists()
    assert "Livewire nightly digest" in body_file.read_text(encoding="utf-8")
    assert list(log_dir.glob("*.marker")) == []


def test_the_digest_lane_is_recorded_in_the_ledger(tmp_path, monkeypatch):
    from clients import ledger

    monkeypatch.setenv("LW_LEDGER_ROOT", str(tmp_path / "ledger"))
    monkeypatch.setenv("LW_RUN_ID", "daily-update-20260902T060000Z-1")
    config = run_daily_update_job.RunnerConfig(
        warehouse_dir=tmp_path,
        log_dir=tmp_path,
        daily_update_script=tmp_path / "livewire_ingest.py",
        alert_script=tmp_path / "livewire_ops.py",
        python_bin="python",
        node_bin="node",
        max_attempts=1,
        retry_delay_seconds=0,
    )
    run_daily_update_job.run_post_success_quality(
        config,
        tmp_path / "daily_update_2026-09-02.log",
        runner=lambda *args, **kwargs: CompletedProcess([], 0),
    )
    assert ledger.query("select lane, outcome from lane_results where lane = 'digest'") == [
        {"lane": "digest", "outcome": "done"}
    ]


def test_no_quality_marker_is_written_anywhere(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "/bin/true")
    nightly_digest.main(
        ["--run-date", "2026-09-02", "--log-dir", str(tmp_path), "--email"],
        runner=lambda *args, **kwargs: CompletedProcess([], 0),
    )
    assert list(tmp_path.glob("*.marker")) == []


def test_body_file_honors_log_dir_override(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "node")
    wrong_log_dir = tmp_path / "module_logs"
    custom_log_dir = tmp_path / "custom_logs"
    calls = []

    def fake_runner(cmd, **kwargs):
        calls.append(cmd)
        return CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setenv("MDW_LOG_DIR", str(wrong_log_dir))

    rc = main(
        ["--run-date", "2026-07-02", "--email", "--log-dir", str(custom_log_dir), "--data-lake", str(tmp_path)],
        runner=fake_runner,
    )

    assert rc == 0
    body_file = _body_file_from_cmd(calls[0])
    assert body_file.parent == custom_log_dir
    assert body_file.exists()
    assert not (wrong_log_dir / "nightly_digest_2026-07-02.txt").exists()

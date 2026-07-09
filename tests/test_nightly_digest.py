"""Tests for livewire_scripts.nightly_digest."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from subprocess import CompletedProcess

from livewire_scripts.daily_outcomes import SUMMARY_PREFIX, build_summary_line
from livewire_scripts.nightly_digest import build_digest, main


def _write_daily_log(log_dir, run, summaries):
    lines = ["=== Daily Update ==="]
    for s in summaries:
        lines.append("  AAPL: 1 bar published from Massive")
        lines.append(build_summary_line(**s))
    (log_dir / f"daily_update_{run}.log").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _daily_summary(**kw):
    base = dict(
        job="daily_update",
        asset_class="equity",
        source="massive",
        target_date="2026-07-02",
        updated=9091,
        no_trade=277,
        partial=95,
        errors=0,
        bars_inserted=9186,
        validation_issues=0,
        top_errors=[],
    )
    base.update(kw)
    return base


def _body_file_from_cmd(cmd) -> Path:
    return Path(cmd[cmd.index("--body-file") + 1])


def test_build_digest_renders_all_sections(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    run = "2026-07-02"
    _write_daily_log(
        log_dir,
        run,
        [_daily_summary(), _daily_summary(asset_class="futures", source="ib", updated=3, no_trade=0, partial=0)],
    )
    intraday = SUMMARY_PREFIX + (
        '{"job":"daily_backfill","target_date":"2026-07-02",'
        '"phases":[{"label":"daily_backfill_fred_rates","exit":0,"duration_s":4.6}],'
        '"failed":[]}'
    )
    (log_dir / f"intraday_catchup_{run}.log").write_text(intraday + "\n", encoding="utf-8")
    (log_dir / f"coverage_{run}.log").write_text(
        "2026-07-02 coverage: 1d=11840/11900 (99.50%)\n  1d missing: FOOU\n", encoding="utf-8"
    )

    out = build_digest(date(2026, 7, 2), log_dir, tmp_path)

    assert "Livewire nightly digest — 2026-07-02" in out
    assert "equity" in out and "updated=9091" in out and "no_trade=277" in out
    assert "futures" in out and "updated=3" in out
    assert "daily_backfill_fred_rates" in out and "ok" in out
    assert "2026-07-02 coverage: 1d=11840/11900 (99.50%)" in out
    assert "Disk:" in out


def test_build_digest_renders_failed_phases(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    run = "2026-07-02"
    intraday = SUMMARY_PREFIX + (
        '{"job":"daily_backfill","target_date":"2026-07-02",'
        '"phases":[{"label":"daily_backfill_fred_rates","exit":1,"duration_s":0.2}],'
        '"failed":["daily_backfill_fred_rates"]}'
    )
    (log_dir / f"intraday_catchup_{run}.log").write_text(intraday + "\n", encoding="utf-8")
    out = build_digest(date(2026, 7, 2), log_dir, tmp_path)
    assert "FAILED (exit 1)" in out
    assert "failed: daily_backfill_fred_rates" in out


def test_build_digest_missing_inputs_render_not_found(tmp_path):
    out = build_digest(date(2026, 7, 2), tmp_path / "empty_logs", tmp_path)
    assert isinstance(out, str)
    assert out.count("(not found)") == 3  # outcomes, phases, coverage
    assert "Disk:" in out


def test_disk_tripwire_warns_under_reserve(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_FLATFILE_MIN_FREE_GB", "25")
    import importlib

    from livewire_scripts import nightly_digest

    importlib.reload(nightly_digest)

    class _Usage:
        total = 200 * (1024**3)
        used = 170 * (1024**3)
        free = 30 * (1024**3)  # 30 GiB < 2*25

    monkeypatch.setattr(nightly_digest.shutil, "disk_usage", lambda p: _Usage())
    out = nightly_digest.build_digest(date(2026, 7, 2), tmp_path / "logs", tmp_path)
    assert "⚠" in out and "raw retention deferred" in out
    importlib.reload(nightly_digest)


def test_main_prints_and_no_email_by_default(tmp_path, capsys):
    rc = main(["--run-date", "2026-07-02", "--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)])
    assert rc == 0
    assert "Livewire nightly digest" in capsys.readouterr().out


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
    # Writes the marker the daily-update watchdog gates on only after send success.
    assert (log_dir / "quality_summary_2026-07-02.marker").exists()


def test_failed_send_does_not_write_marker(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "node")

    def fake_runner(cmd, **kwargs):
        return CompletedProcess(args=cmd, returncode=1)

    log_dir = tmp_path / "logs"
    rc = main(
        ["--run-date", "2026-07-02", "--email", "--log-dir", str(log_dir), "--data-lake", str(tmp_path)],
        runner=fake_runner,
    )

    assert rc == 1
    assert not (log_dir / "quality_summary_2026-07-02.marker").exists()


def test_body_file_honors_log_dir_override(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "node")
    wrong_log_dir = tmp_path / "module_logs"
    custom_log_dir = tmp_path / "custom_logs"
    calls = []

    def fake_runner(cmd, **kwargs):
        calls.append(cmd)
        return CompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr("livewire_scripts.nightly_digest._LOG_DIR", wrong_log_dir)

    rc = main(
        ["--run-date", "2026-07-02", "--email", "--log-dir", str(custom_log_dir), "--data-lake", str(tmp_path)],
        runner=fake_runner,
    )

    assert rc == 0
    body_file = _body_file_from_cmd(calls[0])
    assert body_file.parent == custom_log_dir
    assert body_file.exists()
    assert not (wrong_log_dir / "nightly_digest_2026-07-02.txt").exists()

"""Tests for livewire_scripts.nightly_digest."""

from __future__ import annotations

import json
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from clients import ledger
from livewire_scripts import nightly_digest, run_daily_update_job, status
from livewire_scripts.nightly_digest import main
from livewire_scripts.status import Section, Verdict


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
    receipt = ledger.query("select receipt_json from executions where script = 'nightly_digest'")
    assert json.loads(receipt[0]["receipt_json"])["delivery"] == "accepted"


def test_unchanged_warning_state_does_not_send_a_second_scheduled_email(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "node")
    calls = []

    def fake_runner(cmd, **kwargs):
        calls.append(cmd)
        return CompletedProcess(args=cmd, returncode=0)

    args = ["--run-date", "2026-07-02", "--email", "--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)]
    assert main(args, runner=fake_runner) == 0
    assert main(args, runner=fake_runner) == 0

    assert len(calls) == 1


def test_concurrent_digest_sends_are_serialized_by_delivery_receipt(tmp_path, monkeypatch):
    original_lock = nightly_digest.path_lock
    second_attempt = threading.Event()
    counter_lock = threading.Lock()
    attempts = 0
    calls = []
    monkeypatch.setattr(nightly_digest, "collect", lambda *_args: [Section("Test", Verdict.WARN)])

    @contextmanager
    def observed_lock(path):
        nonlocal attempts
        with counter_lock:
            attempts += 1
            if attempts == 2:
                second_attempt.set()
        with original_lock(path) as held:
            yield held

    monkeypatch.setattr(nightly_digest, "path_lock", observed_lock)

    def send(cmd, **kwargs):
        calls.append(cmd)
        assert second_attempt.wait(5), "second invocation never reached the delivery lock"
        return CompletedProcess(cmd, 0)

    args = ["--email", "--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(main, args, send) for _ in range(2)]
        assert [future.result(timeout=10) for future in futures] == [0, 0]
    assert len(calls) == 1
    assert len(ledger.query("select receipt_json from executions where script = 'nightly_digest'")) == 1


def test_changed_scope_with_same_verdict_sends_a_new_notification(tmp_path, monkeypatch):
    state = [{"verdict": "WARN", "lane": "silver", "failed_now": 1}]
    monkeypatch.setattr(
        nightly_digest,
        "collect",
        lambda *_args: [
            Section("Example", Verdict.WARN, notification_key=status._notification_key("Example", Verdict.WARN, state))
        ],
    )
    calls = []

    def send(cmd, **kwargs):
        calls.append(cmd)
        return CompletedProcess(cmd, 0)

    args = ["--email", "--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)]
    assert main(args, runner=send) == 0
    state[0].update(run_id="another-run", failed_sends=10)
    assert main(args, runner=send) == 0
    assert len(calls) == 1
    state[0]["failed_now"] = 10
    assert main(args, runner=send) == 0
    assert len(calls) == 2


def test_digest_timeout_records_no_delivery_and_remains_retryable(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_SYNC_PHASE_TIMEOUT_SECONDS", "7")
    monkeypatch.setattr(nightly_digest, "collect", lambda *_args: [])
    args = ["--email", "--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)]

    def timeout(cmd, **kwargs):
        assert kwargs["timeout"] == 7
        raise subprocess.TimeoutExpired(cmd, 7)

    assert main(args, runner=timeout) == nightly_digest.TIMEOUT_EXIT_CODE
    assert not ledger.query("select * from executions where script = 'nightly_digest'")
    assert main(args, runner=lambda cmd, **kwargs: CompletedProcess(cmd, 0)) == 0
    assert len(ledger.query("select * from executions where script = 'nightly_digest'")) == 1


def test_default_email_child_cleans_up_descendants_on_timeout(monkeypatch):
    from unittest.mock import MagicMock

    proc = MagicMock()
    proc.pid = 12345
    proc.__enter__.return_value = proc
    proc.communicate.side_effect = [subprocess.TimeoutExpired("node", 1), None]
    spawn = MagicMock(return_value=proc)
    kill = MagicMock()
    monkeypatch.setattr(nightly_digest.subprocess, "Popen", spawn)
    monkeypatch.setattr("livewire_scripts.job_runner_common.os.killpg", kill)
    with pytest.raises(subprocess.TimeoutExpired):
        nightly_digest._run_email_child(["node"], timeout=1)
    assert spawn.call_args.kwargs["start_new_session"] is True
    kill.assert_called_once()
    assert kill.call_args.args[0] == proc.pid


def test_force_email_sends_even_when_the_warning_state_is_unchanged(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "node")
    calls = []

    def fake_runner(cmd, **kwargs):
        calls.append(cmd)
        return CompletedProcess(args=cmd, returncode=0)

    shared = ["--run-date", "2026-07-02", "--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)]
    assert main([*shared, "--email"], runner=fake_runner) == 0
    assert main([*shared, "--force-email"], runner=fake_runner) == 0

    assert len(calls) == 2


def test_recovery_state_sends_a_new_digest_notification(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_NODE_BIN", "node")
    prior, _ = nightly_digest._warning_fingerprint(
        [Section("Silver publication", Verdict.BAD, notification_key="broken")]
    )
    nightly_digest._record_delivery(date(2026, 7, 1), prior, ["broken"])
    monkeypatch.setattr(
        nightly_digest, "collect", lambda *_args, **_kwargs: [Section("Silver publication", Verdict.OK)]
    )
    calls = []

    assert (
        main(
            ["--run-date", "2026-07-02", "--email", "--log-dir", str(tmp_path / "logs"), "--data-lake", str(tmp_path)],
            runner=lambda cmd, **kwargs: calls.append(cmd) or CompletedProcess(args=cmd, returncode=0),
        )
        == 0
    )

    assert len(calls) == 1


def test_the_tail_lane_is_recorded_in_the_ledger(tmp_path, monkeypatch):
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
    assert ledger.query("select lane, outcome from lane_results where lane = 'tail'") == [
        {"lane": "tail", "outcome": "done"}
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

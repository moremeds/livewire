from datetime import UTC, datetime
from pathlib import Path

import pytest

from livewire_scripts.job_runner_common import build_log_file, tail_of


@pytest.mark.parametrize("termination", ["interrupt", "sigterm", "leader-exited"])
def test_process_group_guard_cleans_descendants_and_restores_handler(monkeypatch, termination):
    import signal
    from unittest.mock import Mock

    from livewire_scripts.job_runner_common import process_group_guard

    proc = Mock(pid=123)
    kill = Mock(side_effect=ProcessLookupError if termination == "leader-exited" else None)
    monkeypatch.setattr("livewire_scripts.job_runner_common.os.killpg", kill)
    previous = signal.getsignal(signal.SIGTERM)
    with pytest.raises((KeyboardInterrupt, SystemExit)):
        with process_group_guard(proc):
            if termination == "sigterm":
                signal.raise_signal(signal.SIGTERM)
            else:
                raise KeyboardInterrupt
    kill.assert_called_once_with(123, signal.SIGKILL)
    proc.communicate.assert_called_once_with()
    assert signal.getsignal(signal.SIGTERM) == previous


@pytest.mark.parametrize("termination", ["interrupt", "nonzero"])
def test_process_group_guard_releases_a_real_descendant_lock(tmp_path, termination):
    import subprocess
    import sys
    import time

    from clients.parquet_io import path_lock
    from livewire_scripts.job_runner_common import process_group_guard

    lock = tmp_path / "descendant.lock"
    ready = tmp_path / "ready"
    child = """
import fcntl, pathlib, sys, time
with open(sys.argv[1], 'w') as handle:
    fcntl.flock(handle, fcntl.LOCK_EX)
    pathlib.Path(sys.argv[2]).touch()
    time.sleep(60)
"""
    leader = """
import pathlib, subprocess, sys, time
subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2], sys.argv[3]])
while not pathlib.Path(sys.argv[3]).exists():
    time.sleep(0.01)
if sys.argv[4] == 'nonzero':
    sys.exit(1)
time.sleep(60)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", leader, child, str(lock), str(ready), termination],
        start_new_session=True,
    )
    try:
        try:
            with process_group_guard(proc):
                deadline = time.monotonic() + 10
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert ready.exists()
                with path_lock(lock, blocking=False) as held:
                    assert not held
                if termination == "interrupt":
                    raise KeyboardInterrupt
                assert proc.wait(timeout=10) == 1
        except KeyboardInterrupt:
            assert termination == "interrupt"
        deadline = time.monotonic() + 10
        while True:
            with path_lock(lock, blocking=False) as held:
                if held:
                    break
            assert time.monotonic() < deadline, "descendant retained its write lock"
            time.sleep(0.01)
    finally:
        import os
        import signal

        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=10)


def test_run_in_own_process_group_returns_the_real_exit_code():
    import subprocess
    import sys

    from livewire_scripts.job_runner_common import run_in_own_process_group

    assert run_in_own_process_group(["true"]).returncode == 0
    assert run_in_own_process_group([sys.executable, "-c", "import sys; sys.exit(3)"]).returncode == 3
    with pytest.raises(subprocess.CalledProcessError) as raised:
        run_in_own_process_group([sys.executable, "-c", "import sys; sys.exit(3)"], check=True)
    assert raised.value.returncode == 3


def test_run_in_own_process_group_passes_env_and_returns_stdout():
    import os
    import subprocess
    import sys

    from livewire_scripts.job_runner_common import run_in_own_process_group

    out = run_in_own_process_group(
        [sys.executable, "-c", "import os; print(os.environ['MARK'])"],
        stdout=subprocess.PIPE,
        env={"MARK": "42", "PATH": os.environ["PATH"]},
    )
    assert out.stdout.strip() == "42"


def test_run_in_own_process_group_kills_the_whole_group_on_timeout(tmp_path):
    """A wedged lane's descendants die with it — the leaf lock is released."""
    import subprocess
    import sys
    import time

    from clients.parquet_io import path_lock
    from livewire_scripts.job_runner_common import run_in_own_process_group

    lock = tmp_path / "descendant.lock"
    ready = tmp_path / "ready"
    child = """
import fcntl, pathlib, sys, time
with open(sys.argv[1], 'w') as handle:
    fcntl.flock(handle, fcntl.LOCK_EX)
    pathlib.Path(sys.argv[2]).touch()
    time.sleep(60)
"""
    leader = """
import pathlib, subprocess, sys, time
subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2], sys.argv[3]])
while not pathlib.Path(sys.argv[3]).exists():
    time.sleep(0.01)
time.sleep(60)
"""
    with pytest.raises(subprocess.TimeoutExpired):
        run_in_own_process_group(
            [sys.executable, "-c", leader, child, str(lock), str(ready)],
            timeout=5,
        )

    # Without this the lock check below is vacuous: a descendant that never
    # acquired the lock also "releases" it.
    assert ready.exists(), "descendant never started — nothing held the lock"
    deadline = time.monotonic() + 10
    while True:
        with path_lock(lock, blocking=False) as held:
            if held:
                return
        assert time.monotonic() < deadline, "descendant retained its write lock"
        time.sleep(0.01)


def test_executing_code_sha_reads_the_release_dir_name_not_current(tmp_path, monkeypatch):
    from livewire_scripts.job_runner_common import executing_code_sha

    monkeypatch.delenv("LW_RELEASE_SHA", raising=False)
    sha = "a" * 40
    root = tmp_path / "releases" / sha
    root.mkdir(parents=True)
    assert executing_code_sha(root) == sha


def test_executing_code_sha_reports_a_checkout_head(tmp_path, monkeypatch):
    import subprocess

    from livewire_scripts.job_runner_common import executing_code_sha

    monkeypatch.delenv("LW_RELEASE_SHA", raising=False)
    root = tmp_path / "repo"
    root.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "x"],
        cwd=root,
        check=True,
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()
    assert executing_code_sha(root) == head


def test_executing_code_sha_never_reports_an_unverified_claim(tmp_path, monkeypatch):
    """A well-formed env sha with no physical identity is UNKNOWN, not a label."""
    from livewire_scripts.job_runner_common import executing_code_sha

    monkeypatch.setenv("LW_RELEASE_SHA", "f" * 40)
    root = tmp_path / "unversioned"
    root.mkdir()
    assert executing_code_sha(root) is None


def test_executing_code_sha_cross_checks_a_supplied_mismatch(tmp_path, monkeypatch, capsys):
    from livewire_scripts.job_runner_common import executing_code_sha

    sha = "a" * 40
    root = tmp_path / "releases" / sha
    root.mkdir(parents=True)
    monkeypatch.setenv("LW_RELEASE_SHA", "b" * 40)
    assert executing_code_sha(root) == sha
    assert "LW_RELEASE_SHA" in capsys.readouterr().err


def test_executing_code_sha_is_captured_once_per_root(tmp_path, monkeypatch):
    """A mid-run env change cannot relabel the identity a run already opened under."""
    from livewire_scripts.job_runner_common import executing_code_sha

    sha = "c" * 40
    root = tmp_path / "releases" / sha
    root.mkdir(parents=True)
    monkeypatch.delenv("LW_RELEASE_SHA", raising=False)
    assert executing_code_sha(root) == sha
    monkeypatch.setenv("LW_RELEASE_SHA", "d" * 40)
    assert executing_code_sha(root) == sha


def test_deployment_sha_reports_the_mutable_current_selection(tmp_path, monkeypatch):
    from livewire_scripts.job_runner_common import deployment_sha

    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
    monkeypatch.delenv("MDW_CURRENT_LINK", raising=False)
    assert deployment_sha() is None

    target = tmp_path / "releases" / ("e" * 40)
    target.mkdir(parents=True)
    (tmp_path / "current").symlink_to(target)
    assert deployment_sha() == "e" * 40


def test_pin_executing_sha_clears_an_unverifiable_claim(tmp_path, monkeypatch):
    import os

    from livewire_scripts.job_runner_common import pin_executing_sha

    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
    monkeypatch.setenv("LW_RELEASE_SHA", "f" * 40)
    root = tmp_path / "unversioned"
    root.mkdir()
    assert pin_executing_sha(root) is None
    assert "LW_RELEASE_SHA" not in os.environ


def test_hash_files_digests_sorted_contents_and_unknown_when_unreadable(tmp_path):
    import hashlib

    from livewire_scripts.job_runner_common import hash_files

    first = tmp_path / "a.json"
    second = tmp_path / "b.json"
    first.write_text("a", encoding="utf-8")
    second.write_text("b", encoding="utf-8")
    assert hash_files([second, first]) == hash_files([first, second]) == hashlib.sha256(b"ab").hexdigest()
    assert hash_files([first, tmp_path / "missing.json"]) is None


def test_emit_process_attempt_records_raw_and_effective(tmp_path, monkeypatch):
    from clients import ledger
    from livewire_scripts.job_runner_common import emit_process_attempt

    monkeypatch.setenv("LW_RUN_ID", "test-run")
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
    emit_process_attempt(
        script="equity",
        attempt=2,
        command=["python", "lane.py", "--tickers"] + [f"T{i}" for i in range(40)],
        started=datetime(2026, 9, 17, 1, 0, tzinfo=UTC),
        ended=datetime(2026, 9, 17, 1, 1, tzinfo=UTC),
        raw_exit_code=7,
        effective_exit_code=0,
        completion_reason="summary_override",
        receipt_extra={"summary": {"updated": 3}},
    )

    rows = ledger.query(
        "select script, attempt, exit_code, run_id, "
        "json_extract_string(receipt_json,'$.kind') as kind, "
        "cast(json_extract_string(receipt_json,'$.raw_exit_code') as integer) as raw, "
        "json_extract_string(receipt_json,'$.completion_reason') as reason, "
        "cast(json_extract_string(args_json,'$.truncated_args') as integer) as truncated "
        "from executions"
    )
    assert rows == [
        {
            "script": "equity",
            "attempt": 2,
            "exit_code": 0,
            "run_id": "test-run",
            "kind": "process_attempt",
            "raw": 7,
            "reason": "summary_override",
            "truncated": 19,
        }
    ]


def test_emit_process_attempt_without_a_run_id_is_a_noop(monkeypatch):
    from clients import ledger
    from livewire_scripts.job_runner_common import emit_process_attempt

    monkeypatch.delenv("LW_RUN_ID", raising=False)
    emit_process_attempt(
        script="x",
        attempt=1,
        command=["true"],
        started=datetime(2026, 9, 17, 1, 0, tzinfo=UTC),
        ended=datetime(2026, 9, 17, 1, 0, tzinfo=UTC),
        raw_exit_code=0,
        effective_exit_code=0,
        completion_reason="exit",
    )
    assert ledger.query("select * from executions") == []


def test_build_log_file_takes_its_clock_from_the_shared_seam():
    path = build_log_file(Path("/w/logs"), "daily_update", now=datetime(2026, 9, 5, 6, tzinfo=UTC))

    assert path == Path("/w/logs/daily_update_2026-09-05.log")


def test_tail_of_returns_the_last_n_lines(tmp_path):
    log_file = tmp_path / "lane.log"
    log_file.write_text("".join(f"line {i}\n" for i in range(100)), encoding="utf-8")

    assert tail_of(log_file, 3) == "line 97\nline 98\nline 99\n"


def test_tail_of_a_missing_log_is_empty(tmp_path):
    assert tail_of(tmp_path / "missing.log", 60) == ""


#: No module may build an alert argv inline — every send goes through
#: notify.py. Frozen empty so a NEW argv fails the run.
_KNOWN_INLINE_ALERT_BUILDERS: set[str] = set()


def test_no_scheduled_runner_encodes_the_alert_contract():
    """No module may carry its own copy of the alert argv — notify.py owns it."""
    import pathlib
    import re

    needle = re.compile(r"send[-_]alert")  # regex, so this file carries no literal hit
    root = pathlib.Path(__file__).resolve().parents[1]
    offenders = {
        path.name
        for path in sorted((root / "livewire_scripts").glob("*.py"))
        if path.name != "job_runner_common.py" and needle.search(path.read_text(encoding="utf-8"))
    }

    assert offenders == _KNOWN_INLINE_ALERT_BUILDERS

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

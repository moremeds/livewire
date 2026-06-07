# Livewire Intraday Catch-up Scheduler Implementation Plan

> **Status: implemented, then updated for full-market flat files.** Current
> equity-intraday catch-up behavior is documented in the corresponding
> [scheduler design](../specs/2026-06-05-livewire-intraday-catchup-scheduler-design.md)
> and the [full-market replacement plan](../../plans/2026-06-06-massive-flatfile-full-market.md).
> Historical ticker-scoped commands below are not current operator guidance.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a second launchd job (`com.livewire.intraday-catchup` at 16:00 PT) that runs the existing `daily-backfill` orchestrator so equity intraday parquet (1m/5m/1h), FRED rates, and CBOE volatility refresh daily — closing the gap left by the 13:05 PT daily-update which only catches up 1d.

**Architecture:** Thin Python wrapper at `livewire_scripts/run_intraday_catchup_job.py` that loads env files (via a newly extracted shared helper), spawns `python scripts/livewire_ingest.py daily-backfill` as a single attempt (no external retry — `daily-backfill` already has retry-until-done internally), and on non-zero exit dispatches one email alert through the existing `livewire_ops.py send-alert` Nodemailer pipeline. New `run-intraday-catchup-job` subcommand wires it into `scripts/livewire_ops.py`. A new `launchd/com.livewire.intraday-catchup.plist.example` mirrors the install pattern of the existing daily-update plist.

**Tech Stack:** Python 3.13, pytest + pytest-cov (100% fail-under enforced via `pyproject.toml`), launchd (macOS scheduler), the existing `livewire_scripts/sync_runner.py` (called via the `daily-backfill` subcommand of `scripts/livewire_ingest.py`).

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `livewire_scripts/scheduled_env.py` | Create | Shared env-loading helper (`_load_env_file`, `load_scheduled_env`) extracted from `livewire_ops.py`. Single source of truth for `~/.secrets → repo .env → ~/market-warehouse/.env` precedence. |
| `scripts/livewire_ops.py` | Modify | Delegate env loading to `scheduled_env.load_scheduled_env()`; add `run-intraday-catchup-job` entry to `COMMANDS`; load env for the new subcommand too. |
| `livewire_scripts/run_intraday_catchup_job.py` | Create | Thin wrapper: `IntradayCatchupConfig`, `build_config()`, `build_log_file()`, `build_intraday_catchup_command()`, `build_alert_command()`, `run_intraday_catchup()`, `main()`. Single-attempt; sends alert on failure. |
| `launchd/com.livewire.intraday-catchup.plist.example` | Create | Template plist for 16:00 PT daily trigger of `livewire_ops.py run-intraday-catchup-job`. |
| `tests/test_run_intraday_catchup_job.py` | Create | Unit tests for the new wrapper. 100% coverage on the new module. |
| `tests/test_livewire_entrypoints.py` | Modify | Add tests for `run-intraday-catchup-job` dispatch and shared env loader integration. |
| `tests/test_run_daily_update_job.py` | Modify | Update tests that previously exercised the private `_load_run_daily_env()` import path (now via shared module). |
| `README.md` | Modify | Add intraday-catchup install snippet next to the existing daily-update install snippet. |
| `CLAUDE.md` | Modify | Add intraday-catchup line to the "Scheduling with launchd" section under "Daily updates". |

---

## Task 1: Extract shared env loader

**Files:**
- Create: `livewire_scripts/scheduled_env.py`
- Test: `tests/test_scheduled_env.py`

- [ ] **Step 1: Write the failing test for the new module**

Create `tests/test_scheduled_env.py`:

```python
"""Tests for livewire_scripts/scheduled_env.py."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from livewire_scripts import scheduled_env


def test_load_env_file_handles_missing_path(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("LIVEWIRE_TEST_KEY", raising=False)
    scheduled_env._load_env_file(tmp_path / "missing.env")
    assert "LIVEWIRE_TEST_KEY" not in os.environ


def test_load_env_file_parses_export_and_quoted_values(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# comment line\n"
        "BARE=plain\n"
        "export QUOTED='hello world'\n"
        "EMPTY=\n"
        "=ignored\n"
        "BROKEN_LINE_NO_EQUALS\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("BARE", raising=False)
    monkeypatch.delenv("QUOTED", raising=False)
    monkeypatch.delenv("EMPTY", raising=False)

    scheduled_env._load_env_file(env_file)

    assert os.environ["BARE"] == "plain"
    assert os.environ["QUOTED"] == "hello world"
    assert os.environ["EMPTY"] == ""


def test_load_env_file_tolerates_unterminated_quote(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / "bad.env"
    env_file.write_text("BAD='unterminated\n", encoding="utf-8")
    monkeypatch.delenv("BAD", raising=False)

    scheduled_env._load_env_file(env_file)

    assert os.environ["BAD"] == "'unterminated"


def test_load_scheduled_env_priority(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    home.mkdir()
    secrets = home / ".secrets"
    repo = tmp_path / "repo"
    repo.mkdir()
    repo_env = repo / ".env"
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    warehouse_env = warehouse / ".env"

    secrets.write_text("FROM_SECRETS=from_secrets\nSHARED=secret\n", encoding="utf-8")
    repo_env.write_text("FROM_REPO=from_repo\nSHARED=repo\n", encoding="utf-8")
    warehouse_env.write_text("FROM_WAREHOUSE=from_warehouse\nSHARED=warehouse\n", encoding="utf-8")

    monkeypatch.setattr(scheduled_env.Path, "home", classmethod(lambda cls: home))
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))
    for key in ("FROM_SECRETS", "FROM_REPO", "FROM_WAREHOUSE", "SHARED"):
        monkeypatch.delenv(key, raising=False)

    scheduled_env.load_scheduled_env(repo)

    assert os.environ["FROM_SECRETS"] == "from_secrets"
    assert os.environ["FROM_REPO"] == "from_repo"
    assert os.environ["FROM_WAREHOUSE"] == "from_warehouse"
    # Last-set-wins precedence: warehouse loaded last
    assert os.environ["SHARED"] == "warehouse"


def test_load_scheduled_env_skips_missing_files(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    home.mkdir()
    repo = tmp_path / "repo"
    repo.mkdir()
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()

    monkeypatch.setattr(scheduled_env.Path, "home", classmethod(lambda cls: home))
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))

    # Must not raise
    scheduled_env.load_scheduled_env(repo)
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/test_scheduled_env.py -v
```

Expected: `ModuleNotFoundError: No module named 'livewire_scripts.scheduled_env'`

- [ ] **Step 3: Implement the module**

Create `livewire_scripts/scheduled_env.py`:

```python
"""Shared env-file loader for scheduled livewire jobs.

Single source of truth for the env precedence used by every launchd-driven
wrapper. Currently used by `scripts/livewire_ops.py` for both `run-daily-job`
and `run-intraday-catchup-job`. Keeping this in one module prevents the two
wrappers from drifting on env precedence when one is modified.
"""

from __future__ import annotations

import os
import shlex
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        try:
            parsed = shlex.split(value, comments=False, posix=True)
        except ValueError:
            parsed = [value.strip()]
        os.environ[key] = parsed[0] if parsed else ""


def load_scheduled_env(repo_root: Path) -> None:
    warehouse = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
    for env_file in (Path.home() / ".secrets", repo_root / ".env", warehouse / ".env"):
        _load_env_file(env_file.expanduser())
```

- [ ] **Step 4: Run the test to confirm it passes**

```bash
python -m pytest tests/test_scheduled_env.py -v
```

Expected: all 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/scheduled_env.py tests/test_scheduled_env.py
git -c user.name="Chenxi LI" -c user.email=eason.li@me.com commit -m "feat: extract shared scheduled-env loader

Move env precedence (~/.secrets → repo .env → ~/market-warehouse/.env) out
of the private livewire_ops._load_run_daily_env() helper into a public module
so future scheduled wrappers cannot drift on env loading semantics."
```

---

## Task 2: Wire `livewire_ops.py` to use the shared env loader

**Files:**
- Modify: `scripts/livewire_ops.py`
- Modify: `tests/test_livewire_entrypoints.py`

- [ ] **Step 1: Write the failing test for the integration**

In `tests/test_livewire_entrypoints.py`, just after the existing `test_ops_run_daily_job_loads_env_files_and_dispatches`, add:

```python
def test_ops_run_daily_job_uses_shared_scheduled_env_loader(monkeypatch, tmp_path) -> None:
    """The run-daily-job command must delegate env loading to
    livewire_scripts.scheduled_env so other scheduled wrappers reuse the same code path."""

    calls: list[Path] = []

    from livewire_scripts import scheduled_env

    def _fake_loader(repo_root: Path) -> None:
        calls.append(repo_root)

    monkeypatch.setattr(scheduled_env, "load_scheduled_env", _fake_loader)
    monkeypatch.setattr(livewire_ops, "load_scheduled_env", _fake_loader)
    monkeypatch.setattr(livewire_ops, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        livewire_ops.importlib,
        "import_module",
        lambda name: _fake_module([], name, accepts_argv=True),
    )

    livewire_ops.main(["run-daily-job"])

    assert calls == [tmp_path]
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
python -m pytest tests/test_livewire_entrypoints.py::test_ops_run_daily_job_uses_shared_scheduled_env_loader -v
```

Expected: AttributeError or failure on the `monkeypatch.setattr(livewire_ops, "load_scheduled_env", ...)` line because the symbol is not yet imported in `livewire_ops`.

- [ ] **Step 3: Refactor `scripts/livewire_ops.py` to delegate to the shared loader**

Replace lines 24–47 (the inline `_load_env_file` and `_load_run_daily_env` helpers) with an import + thin wrapper:

```python
from livewire_scripts.scheduled_env import _load_env_file, load_scheduled_env  # noqa: F401  (re-export for backwards-compatible tests)
```

Then replace the body of `_load_run_daily_env` (line 44) with:

```python
def _load_run_daily_env() -> None:
    """Backwards-compatible alias for legacy tests; delegates to the shared loader."""
    load_scheduled_env(REPO_ROOT)
```

In `main()`, line 84–86, replace:

```python
    if args.command == "run-daily-job":
        _load_run_daily_env()
    return _dispatch_module(COMMANDS[args.command], rest, f"livewire_ops.py {args.command}")
```

with:

```python
    if args.command == "run-daily-job":
        load_scheduled_env(REPO_ROOT)
    return _dispatch_module(COMMANDS[args.command], rest, f"livewire_ops.py {args.command}")
```

- [ ] **Step 4: Run the full test suite for ops + scheduled_env**

```bash
python -m pytest tests/test_livewire_entrypoints.py tests/test_scheduled_env.py -v
```

Expected: all tests pass, including the existing `test_ops_run_daily_job_loads_env_files_and_dispatches` (which still exercises real file IO through the new delegation path) and `test_ops_env_loader_ignores_missing_and_bad_quotes` (which still imports `livewire_ops._load_env_file` via the re-export).

- [ ] **Step 5: Commit**

```bash
git add scripts/livewire_ops.py tests/test_livewire_entrypoints.py
git -c user.name="Chenxi LI" -c user.email=eason.li@me.com commit -m "refactor: route livewire_ops env loading through scheduled_env

Delegates env precedence to livewire_scripts.scheduled_env.load_scheduled_env
so the next scheduled wrapper can reuse it without copying logic. Keeps
_load_env_file and _load_run_daily_env re-exported for the existing tests."
```

---

## Task 3: Add `run-intraday-catchup-job` subcommand dispatch

**Files:**
- Modify: `scripts/livewire_ops.py`
- Modify: `tests/test_livewire_entrypoints.py`

- [ ] **Step 1: Write the failing test for dispatch**

In `tests/test_livewire_entrypoints.py`, after the daily-job dispatch test, add:

```python
def test_ops_run_intraday_catchup_job_loads_env_files_and_dispatches(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, list[str]]] = []
    repo_env = tmp_path / ".env"
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    warehouse_env = warehouse / ".env"
    secrets = tmp_path / ".secrets"
    repo_env.write_text("export FROM_REPO='repo value'\n", encoding="utf-8")
    warehouse_env.write_text("FROM_WAREHOUSE=warehouse\n", encoding="utf-8")
    secrets.write_text("FROM_SECRET=secret\n", encoding="utf-8")

    monkeypatch.setattr(livewire_ops, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(livewire_ops.Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))
    monkeypatch.setattr(
        livewire_ops.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_ops.main(["run-intraday-catchup-job"]) == 7
    assert calls == [("livewire_scripts.run_intraday_catchup_job", [])]
    assert livewire_ops.os.environ["FROM_REPO"] == "repo value"
    assert livewire_ops.os.environ["FROM_WAREHOUSE"] == "warehouse"
    assert livewire_ops.os.environ["FROM_SECRET"] == "secret"


def test_ops_help_lists_intraday_catchup_command(capsys) -> None:
    livewire_ops.main(["-h"])
    captured = capsys.readouterr().out
    assert "run-intraday-catchup-job" in captured
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
python -m pytest tests/test_livewire_entrypoints.py::test_ops_run_intraday_catchup_job_loads_env_files_and_dispatches tests/test_livewire_entrypoints.py::test_ops_help_lists_intraday_catchup_command -v
```

Expected: argparse error (`invalid choice: 'run-intraday-catchup-job'`).

- [ ] **Step 3: Wire the new command into `COMMANDS` and trigger env loading**

In `scripts/livewire_ops.py`, update `COMMANDS` (line 19–21):

```python
COMMANDS = {
    "run-daily-job": "livewire_scripts.run_daily_update_job",
    "run-intraday-catchup-job": "livewire_scripts.run_intraday_catchup_job",
}
```

And update the env-loading guard in `main()` so both scheduled jobs load env:

```python
    if args.command in {"run-daily-job", "run-intraday-catchup-job"}:
        load_scheduled_env(REPO_ROOT)
    return _dispatch_module(COMMANDS[args.command], rest, f"livewire_ops.py {args.command}")
```

- [ ] **Step 4: Run the tests to confirm they pass**

```bash
python -m pytest tests/test_livewire_entrypoints.py -v
```

Expected: all tests pass, including the new two. The dispatch test passes because `_fake_module` returns a `SimpleNamespace` whose `main()` returns 7.

- [ ] **Step 5: Commit**

```bash
git add scripts/livewire_ops.py tests/test_livewire_entrypoints.py
git -c user.name="Chenxi LI" -c user.email=eason.li@me.com commit -m "feat: register run-intraday-catchup-job subcommand

Adds the COMMANDS entry and shared env loading for the new scheduled wrapper.
The wrapper module is created in the next commit."
```

---

## Task 4: Skeleton + config builder for `run_intraday_catchup_job`

**Files:**
- Create: `livewire_scripts/run_intraday_catchup_job.py`
- Create: `tests/test_run_intraday_catchup_job.py`

- [ ] **Step 1: Write failing tests for the config + builders**

Create `tests/test_run_intraday_catchup_job.py`:

```python
"""Tests for livewire_scripts/run_intraday_catchup_job.py."""

from __future__ import annotations

import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

import pytest

from livewire_scripts.run_intraday_catchup_job import (
    AlertRequest,
    IntradayCatchupConfig,
    build_alert_command,
    build_config,
    build_intraday_catchup_command,
    build_log_file,
    main,
    run_intraday_catchup,
)


def _config(tmp_path: Path, *, node_bin: str = "/opt/homebrew/bin/node") -> IntradayCatchupConfig:
    repo_root = tmp_path / "repo"
    script_dir = repo_root / "scripts"
    return IntradayCatchupConfig(
        warehouse_dir=tmp_path / "warehouse",
        log_dir=tmp_path / "warehouse" / "logs",
        ingest_script=script_dir / "livewire_ingest.py",
        alert_script=script_dir / "livewire_ops.py",
        python_bin="/usr/bin/python3",
        node_bin=node_bin,
    )


class TestBuildConfig:
    def test_defaults(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        for key in (
            "MDW_INTRADAY_CATCHUP_LOG_DIR",
            "MDW_INTRADAY_CATCHUP_SCRIPT",
            "MDW_INTRADAY_CATCHUP_ALERT_SCRIPT",
            "MDW_INTRADAY_CATCHUP_PYTHON_BIN",
            "MDW_NODE_BIN",
        ):
            monkeypatch.delenv(key, raising=False)

        with patch(
            "livewire_scripts.run_intraday_catchup_job.shutil.which",
            return_value="/usr/local/bin/node",
        ):
            config = build_config()

        assert config.warehouse_dir == tmp_path / "warehouse"
        assert config.log_dir == config.warehouse_dir / "logs"
        assert config.node_bin == "/usr/local/bin/node"
        assert config.python_bin == sys.executable
        assert config.ingest_script.name == "livewire_ingest.py"
        assert config.alert_script.name == "livewire_ops.py"

    def test_env_overrides(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.setenv("MDW_INTRADAY_CATCHUP_LOG_DIR", str(tmp_path / "custom-logs"))
        monkeypatch.setenv("MDW_INTRADAY_CATCHUP_SCRIPT", str(tmp_path / "ingest.py"))
        monkeypatch.setenv("MDW_INTRADAY_CATCHUP_ALERT_SCRIPT", str(tmp_path / "ops.py"))
        monkeypatch.setenv("MDW_INTRADAY_CATCHUP_PYTHON_BIN", "/venv/bin/python")
        monkeypatch.setenv("MDW_NODE_BIN", "/usr/bin/node")

        config = build_config()

        assert config.log_dir == tmp_path / "custom-logs"
        assert config.ingest_script == tmp_path / "ingest.py"
        assert config.alert_script == tmp_path / "ops.py"
        assert config.python_bin == "/venv/bin/python"
        assert config.node_bin == "/usr/bin/node"


class TestBuildLogFile:
    def test_uses_utc_date(self, tmp_path):
        log_dir = tmp_path / "logs"
        # 16:00 PT == 00:00 UTC next day during PST (Nov-Mar) or 23:00 UTC same day during PDT
        when = datetime(2026, 6, 5, 23, 0, tzinfo=UTC)
        path = build_log_file(log_dir, when)
        assert path == log_dir / "intraday_catchup_2026-06-05.log"


class TestBuildIntradayCatchupCommand:
    def test_invokes_daily_backfill(self, tmp_path):
        config = _config(tmp_path)
        cmd = build_intraday_catchup_command(config)
        assert cmd == [
            "/usr/bin/python3",
            str(config.ingest_script),
            "daily-backfill",
        ]


class TestBuildAlertCommand:
    def test_includes_job_name_intraday_catchup(self, tmp_path):
        config = _config(tmp_path)
        request = AlertRequest(
            run_date="2026-06-05",
            log_file=tmp_path / "log.log",
            exit_code=42,
            error_summary="something broke",
            repo_root=tmp_path / "repo",
        )
        cmd = build_alert_command(config, request)
        assert "--job-name" in cmd
        assert cmd[cmd.index("--job-name") + 1] == "intraday_catchup"
        assert "--attempts" in cmd
        assert cmd[cmd.index("--attempts") + 1] == "1"
        assert "--exit-code" in cmd
        assert cmd[cmd.index("--exit-code") + 1] == "42"
        assert "--run-date" in cmd
        assert cmd[cmd.index("--run-date") + 1] == "2026-06-05"
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
python -m pytest tests/test_run_intraday_catchup_job.py -v
```

Expected: `ModuleNotFoundError: No module named 'livewire_scripts.run_intraday_catchup_job'`

- [ ] **Step 3: Implement the skeleton**

Create `livewire_scripts/run_intraday_catchup_job.py`:

```python
#!/usr/bin/env python3
"""Single-shot runner for the scheduled intraday catch-up.

This wrapper exists to mirror the env loading and failure-alert pipeline of
`run_daily_update_job` while delegating all work to `daily-backfill`, which
already owns retry-until-done and activity-based stall detection. No external
retry loop here; one attempt, one alert on terminal failure.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
INGEST_SCRIPT = REPO_ROOT / "scripts" / "livewire_ingest.py"
OPS_SCRIPT = REPO_ROOT / "scripts" / "livewire_ops.py"


@dataclass(frozen=True)
class IntradayCatchupConfig:
    warehouse_dir: Path
    log_dir: Path
    ingest_script: Path
    alert_script: Path
    python_bin: str
    node_bin: str


@dataclass(frozen=True)
class AlertRequest:
    run_date: str
    log_file: Path
    exit_code: int
    error_summary: str
    repo_root: Path


def _utc_now() -> datetime:
    return datetime.now(UTC)


def build_config() -> IntradayCatchupConfig:
    warehouse_dir = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse"))).expanduser()
    log_dir = Path(os.getenv("MDW_INTRADAY_CATCHUP_LOG_DIR", str(warehouse_dir / "logs"))).expanduser()
    node_bin = os.getenv("MDW_NODE_BIN") or shutil.which("node") or "/opt/homebrew/bin/node"
    return IntradayCatchupConfig(
        warehouse_dir=warehouse_dir,
        log_dir=log_dir,
        ingest_script=Path(os.getenv("MDW_INTRADAY_CATCHUP_SCRIPT", str(INGEST_SCRIPT))).expanduser(),
        alert_script=Path(os.getenv("MDW_INTRADAY_CATCHUP_ALERT_SCRIPT", str(OPS_SCRIPT))).expanduser(),
        python_bin=os.getenv("MDW_INTRADAY_CATCHUP_PYTHON_BIN", sys.executable),
        node_bin=node_bin,
    )


def build_log_file(log_dir: Path, now: datetime | None = None) -> Path:
    current = now or _utc_now()
    return log_dir / f"intraday_catchup_{current:%Y-%m-%d}.log"


def build_intraday_catchup_command(config: IntradayCatchupConfig) -> list[str]:
    return [config.python_bin, str(config.ingest_script), "daily-backfill"]


def build_alert_command(config: IntradayCatchupConfig, request: AlertRequest) -> list[str]:
    return [
        config.python_bin,
        str(config.alert_script),
        "send-alert",
        "--run-date", request.run_date,
        "--log-file", str(request.log_file),
        "--error-summary", request.error_summary,
        "--repo-root", str(request.repo_root),
        "--job-name", "intraday_catchup",
        "--attempts", "1",
        "--exit-code", str(request.exit_code),
    ]


def run_intraday_catchup(  # pragma: no cover  (covered in Task 5)
    config: IntradayCatchupConfig,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
    now_fn: callable = _utc_now,
) -> int:
    raise NotImplementedError("Filled in Task 5")


def main(argv: Sequence[str] | None = None) -> int:  # pragma: no cover  (covered in Task 5)
    raise NotImplementedError("Filled in Task 5")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests to confirm they pass**

```bash
python -m pytest tests/test_run_intraday_catchup_job.py -v
```

Expected: 8 tests pass (all the `TestBuildConfig`, `TestBuildLogFile`, `TestBuildIntradayCatchupCommand`, `TestBuildAlertCommand` tests).

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/run_intraday_catchup_job.py tests/test_run_intraday_catchup_job.py
git -c user.name="Chenxi LI" -c user.email=eason.li@me.com commit -m "feat: intraday-catchup config + command builders

Skeleton dataclasses and pure builder functions for the scheduled intraday
catch-up wrapper. Orchestrator and main() are stubbed and filled in the next
commit. Covers env precedence and command construction with unit tests."
```

---

## Task 5: Orchestrator (`run_intraday_catchup`) + `main()`

**Files:**
- Modify: `livewire_scripts/run_intraday_catchup_job.py`
- Modify: `tests/test_run_intraday_catchup_job.py`

- [ ] **Step 1: Write failing tests for the orchestrator and main()**

Append to `tests/test_run_intraday_catchup_job.py`:

```python
def _append(handle, lines: list[str]) -> None:
    for line in lines:
        handle.write(line)
        handle.write("\n")


class TestRunIntradayCatchup:
    def test_success_no_alert(self, tmp_path, monkeypatch):
        config = _config(tmp_path)
        config.log_dir.mkdir(parents=True)
        runner_calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            runner_calls.append(list(cmd))
            handle = kwargs.get("stdout")
            if handle is not None and hasattr(handle, "write"):
                handle.write("ok\n")
            return CompletedProcess(args=cmd, returncode=0)

        rc = run_intraday_catchup(
            config,
            env={"FOO": "bar"},
            runner=fake_runner,
            now_fn=lambda: datetime(2026, 6, 5, 23, 0, tzinfo=UTC),
        )

        assert rc == 0
        # Only one subprocess invocation (the daily-backfill command) and no alert.
        assert len(runner_calls) == 1
        assert runner_calls[0] == build_intraday_catchup_command(config)

        log_file = config.log_dir / "intraday_catchup_2026-06-05.log"
        contents = log_file.read_text(encoding="utf-8")
        assert "=== Intraday Catchup" in contents
        assert "=== Done" in contents

    def test_failure_triggers_alert(self, tmp_path, monkeypatch):
        config = _config(tmp_path)
        config.log_dir.mkdir(parents=True)
        # Create alert script and node binary so the alert path runs.
        config.alert_script.parent.mkdir(parents=True, exist_ok=True)
        config.alert_script.write_text("# stub\n", encoding="utf-8")
        Path(config.node_bin).parent.mkdir(parents=True, exist_ok=True)
        Path(config.node_bin).write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")

        runner_calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            runner_calls.append(list(cmd))
            handle = kwargs.get("stdout")
            if handle is not None and hasattr(handle, "write"):
                handle.write("boom: ConnectionError: Socket disconnect\n")
            # Daily-backfill fails; alert subprocess succeeds.
            return CompletedProcess(
                args=cmd,
                returncode=2 if cmd[1] == str(config.ingest_script) else 0,
                stdout="alert sent",
            )

        rc = run_intraday_catchup(
            config,
            env=None,
            runner=fake_runner,
            now_fn=lambda: datetime(2026, 6, 5, 23, 0, tzinfo=UTC),
        )

        assert rc == 2
        assert len(runner_calls) == 2
        assert runner_calls[0] == build_intraday_catchup_command(config)
        # Alert invocation includes --job-name intraday_catchup and exit-code 2.
        alert_cmd = runner_calls[1]
        assert "--job-name" in alert_cmd and alert_cmd[alert_cmd.index("--job-name") + 1] == "intraday_catchup"
        assert "--exit-code" in alert_cmd and alert_cmd[alert_cmd.index("--exit-code") + 1] == "2"

    def test_failure_with_missing_node_skips_alert_and_returns_exit_code(self, tmp_path, monkeypatch):
        config = _config(tmp_path, node_bin="/does/not/exist/node")
        config.log_dir.mkdir(parents=True)

        runner_calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            runner_calls.append(list(cmd))
            return CompletedProcess(args=cmd, returncode=3)

        rc = run_intraday_catchup(
            config,
            runner=fake_runner,
            now_fn=lambda: datetime(2026, 6, 5, 23, 0, tzinfo=UTC),
        )

        assert rc == 3
        assert len(runner_calls) == 1  # alert was skipped

        log_file = config.log_dir / "intraday_catchup_2026-06-05.log"
        contents = log_file.read_text(encoding="utf-8")
        assert "node binary not found" in contents


class TestMain:
    def test_main_builds_config_and_dispatches(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
        monkeypatch.setenv("MDW_INTRADAY_CATCHUP_LOG_DIR", str(tmp_path / "warehouse" / "logs"))
        monkeypatch.delenv("MDW_INTRADAY_CATCHUP_SCRIPT", raising=False)
        monkeypatch.delenv("MDW_INTRADAY_CATCHUP_ALERT_SCRIPT", raising=False)
        monkeypatch.delenv("MDW_INTRADAY_CATCHUP_PYTHON_BIN", raising=False)
        monkeypatch.delenv("MDW_NODE_BIN", raising=False)

        captured: dict[str, object] = {}

        def fake_runner(cmd, **kwargs):
            captured["cmd"] = list(cmd)
            handle = kwargs.get("stdout")
            if handle is not None and hasattr(handle, "write"):
                handle.write("daily-backfill ok\n")
            return CompletedProcess(args=cmd, returncode=0)

        with patch(
            "livewire_scripts.run_intraday_catchup_job.subprocess.run",
            side_effect=fake_runner,
        ), patch(
            "livewire_scripts.run_intraday_catchup_job.shutil.which",
            return_value="/usr/local/bin/node",
        ):
            rc = main([])

        assert rc == 0
        assert captured["cmd"][1].endswith("scripts/livewire_ingest.py")
        assert captured["cmd"][2] == "daily-backfill"
```

- [ ] **Step 2: Run the new tests to confirm they fail**

```bash
python -m pytest tests/test_run_intraday_catchup_job.py::TestRunIntradayCatchup tests/test_run_intraday_catchup_job.py::TestMain -v
```

Expected: each test raises `NotImplementedError("Filled in Task 5")` because we stubbed in Task 4.

- [ ] **Step 3: Implement `run_intraday_catchup` and `main`**

Edit `livewire_scripts/run_intraday_catchup_job.py`. Remove the two `pragma: no cover` stubs at the bottom and replace with:

```python
def _append_log(log_file: Path, message: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(message)
        if not message.endswith("\n"):
            handle.write("\n")


def _node_binary_exists(node_bin: str) -> bool:
    if Path(node_bin).is_absolute():
        return Path(node_bin).exists()
    return shutil.which(node_bin) is not None


def _extract_error_summary(log_file: Path) -> str:
    try:
        lines = log_file.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return "Intraday catchup failed, and the log file was not found."
    for line in reversed(lines):
        stripped = line.strip()
        if stripped and not stripped.startswith("==="):
            return stripped
    return "Intraday catchup failed with no error summary captured in the log."


def run_intraday_catchup(
    config: IntradayCatchupConfig,
    env: dict[str, str] | None = None,
    runner: callable = subprocess.run,
    now_fn: callable = _utc_now,
) -> int:
    started_at = now_fn()
    log_file = build_log_file(config.log_dir, started_at)
    command = build_intraday_catchup_command(config)

    _append_log(log_file, f"=== Intraday Catchup {started_at:%Y-%m-%dT%H:%M:%SZ} ===")
    _append_log(log_file, f"Runner command: {' '.join(command)}")
    _append_log(log_file, f"hostname={socket.gethostname()}")

    with log_file.open("a", encoding="utf-8") as handle:
        result = runner(
            list(command),
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            check=False,
        )

    if result.returncode == 0:
        _append_log(log_file, f"=== Done {now_fn():%Y-%m-%dT%H:%M:%SZ} ===")
        return 0

    _append_log(
        log_file,
        f"=== Failed {now_fn():%Y-%m-%dT%H:%M:%SZ} (exit_code={result.returncode}) ===",
    )

    if not _node_binary_exists(config.node_bin):
        _append_log(log_file, f"WARNING: node binary not found at {config.node_bin}; skipping failure email")
        return result.returncode

    if not config.alert_script.exists():
        _append_log(log_file, f"WARNING: alert script not found at {config.alert_script}; skipping failure email")
        return result.returncode

    alert_request = AlertRequest(
        run_date=log_file.stem.removeprefix("intraday_catchup_"),
        log_file=log_file,
        exit_code=result.returncode,
        error_summary=_extract_error_summary(log_file),
        repo_root=REPO_ROOT,
    )
    alert_command = build_alert_command(config, alert_request)
    _append_log(log_file, f"Triggering failure alert via: {' '.join(alert_command)}")
    alert_result = runner(
        alert_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        check=False,
    )
    alert_output = (alert_result.stdout or "").strip()
    if alert_result.returncode == 0:
        _append_log(log_file, f"Failure alert sent successfully. {alert_output}".strip())
    else:
        _append_log(
            log_file,
            f"WARNING: failure alert returned non-zero exit code {alert_result.returncode}. {alert_output}".strip(),
        )
    return result.returncode


def main(argv: Sequence[str] | None = None) -> int:
    # No positional args expected; all behavior is env-driven and the daily-backfill
    # subcommand has no required CLI flags.
    _ = list(argv or sys.argv[1:])  # accepted but ignored, for symmetry with run-daily-job
    config = build_config()
    env = os.environ.copy()
    return run_intraday_catchup(config, env=env)
```

- [ ] **Step 4: Run the full module test suite**

```bash
python -m pytest tests/test_run_intraday_catchup_job.py -v -W error::RuntimeWarning
```

Expected: all tests pass, no RuntimeWarning leaks.

- [ ] **Step 5: Run full project coverage check on the new module**

```bash
python -m pytest tests/test_run_intraday_catchup_job.py --cov=livewire_scripts.run_intraday_catchup_job --cov-report=term-missing
```

Expected: 100% coverage on `livewire_scripts/run_intraday_catchup_job.py`.

- [ ] **Step 6: Commit**

```bash
git add livewire_scripts/run_intraday_catchup_job.py tests/test_run_intraday_catchup_job.py
git -c user.name="Chenxi LI" -c user.email=eason.li@me.com commit -m "feat: implement intraday-catchup orchestrator and main()

Single-attempt subprocess invocation of livewire_ingest.py daily-backfill.
Streams stdout+stderr to ~/market-warehouse/logs/intraday_catchup_<UTC date>.log
and on non-zero exit dispatches one send-alert subprocess with
--job-name intraday_catchup. Skips the email cleanly when node or alert
script is missing, matching the daily-update wrapper behavior."
```

---

## Task 6: Launchd plist template

**Files:**
- Create: `launchd/com.livewire.intraday-catchup.plist.example`
- Modify: `tests/test_run_intraday_catchup_job.py` (one sanity test for the plist)

- [ ] **Step 1: Write a sanity test for the plist template**

Append to `tests/test_run_intraday_catchup_job.py`:

```python
class TestLaunchdTemplate:
    def test_plist_template_exists_and_parses(self):
        repo_root = Path(__file__).resolve().parent.parent
        plist_path = repo_root / "launchd" / "com.livewire.intraday-catchup.plist.example"
        assert plist_path.exists(), f"missing plist template at {plist_path}"

        # Avoid plistlib XML strictness — just check the human-meaningful invariants.
        text = plist_path.read_text(encoding="utf-8")
        assert "<string>com.livewire.intraday-catchup</string>" in text
        assert "run-intraday-catchup-job" in text
        assert "<key>Hour</key>" in text
        assert "<integer>16</integer>" in text
        assert "<key>Minute</key>" in text
        assert "<integer>0</integer>" in text
        # Same /path/to/repo substitution sentinel as the daily-update example.
        assert "/path/to/repo" in text
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
python -m pytest tests/test_run_intraday_catchup_job.py::TestLaunchdTemplate -v
```

Expected: AssertionError: missing plist template at .../launchd/com.livewire.intraday-catchup.plist.example.

- [ ] **Step 3: Create the plist template**

Create `launchd/com.livewire.intraday-catchup.plist.example`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.livewire.intraday-catchup</string>

    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>-c</string>
        <!-- Replace /path/to/repo with your actual repo path -->
        <string>cd /path/to/repo &amp;&amp; /Users/chenxi/market-warehouse/.venv/bin/python scripts/livewire_ops.py run-intraday-catchup-job</string>
    </array>

    <key>StartCalendarInterval</key>
    <dict>
        <!-- 16:00 PT = 7:00 PM ET year-round (PT and ET always 3h apart) -->
        <!-- Runs after com.livewire.daily-update (13:05 PT) typically completes. -->
        <key>Hour</key>
        <integer>16</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <key>StandardOutPath</key>
    <string>/tmp/com.livewire.intraday-catchup.stdout.log</string>

    <key>StandardErrorPath</key>
    <string>/tmp/com.livewire.intraday-catchup.stderr.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
```

- [ ] **Step 4: Run the test to confirm it passes**

```bash
python -m pytest tests/test_run_intraday_catchup_job.py::TestLaunchdTemplate -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add launchd/com.livewire.intraday-catchup.plist.example tests/test_run_intraday_catchup_job.py
git -c user.name="Chenxi LI" -c user.email=eason.li@me.com commit -m "feat: launchd template for intraday-catchup at 16:00 PT

Mirrors the install pattern of com.livewire.daily-update.plist.example with
the /path/to/repo sed-substitution sentinel."
```

---

## Task 7: Documentation updates

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Find the existing daily-update install snippet in `README.md`**

```bash
grep -n "com.livewire.daily-update.plist.example\|launchctl load" README.md
```

Expected: one or two matches showing the existing install snippet block.

- [ ] **Step 2: Update `README.md`**

Locate the existing install block:

```bash
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.daily-update.plist.example > ~/Library/LaunchAgents/com.livewire.daily-update.plist
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.daily-update-watchdog.plist.example > ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update.plist
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
```

And replace it with a block that also installs the new plist:

```bash
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.daily-update.plist.example > ~/Library/LaunchAgents/com.livewire.daily-update.plist
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.daily-update-watchdog.plist.example > ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.intraday-catchup.plist.example > ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update.plist
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
launchctl load ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
```

If `README.md` does not contain that block (the snippet may live only in `CLAUDE.md`), skip this step.

- [ ] **Step 3: Update `CLAUDE.md`**

Inside the "Daily updates" section (search for `**Scheduling with launchd**`) replace the existing block with:

```bash
# Copy examples, replace /path/to/repo with your actual repo path
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.daily-update.plist.example > ~/Library/LaunchAgents/com.livewire.daily-update.plist
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.daily-update-watchdog.plist.example > ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.intraday-catchup.plist.example > ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update.plist
launchctl load ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
launchctl load ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
```

Immediately after the existing paragraph that begins `scripts/livewire_ops.py run-daily-job loads ~/.secrets, …`, append a second paragraph:

```markdown
A second scheduled job, `com.livewire.intraday-catchup`, runs at 16:00 Pacific local time daily and invokes `scripts/livewire_ops.py run-intraday-catchup-job`. This calls the existing `daily-backfill` orchestrator so equity intraday parquet (1m/5m/1h), FRED Treasury rates, and CBOE volatility all refresh through the Massive flat-file fast path (when `MASSIVE_S3_ACCESS_KEY` / `MASSIVE_S3_SECRET_KEY` are set) or the Massive REST path otherwise. The wrapper is single-attempt because `daily-backfill` already owns retry-until-done and activity-based stall detection; on terminal failure the wrapper sends one alert through the same Nodemailer pipeline as the daily-update wrapper, tagged `--job-name intraday_catchup`. Logs land at `~/market-warehouse/logs/intraday_catchup_YYYY-MM-DD.log` (UTC date). The default 7-day `MDW_DAILY_BACKFILL_INTRADAY_DAYS` lookback absorbs a single missed run; widen via `~/market-warehouse/.env` if you need more headroom.
```

- [ ] **Step 4: Verify the doc changes don't break anything**

```bash
grep -n "com.livewire.intraday-catchup" README.md CLAUDE.md launchd/
```

Expected: matches in CLAUDE.md (and README.md if applicable) plus the plist file itself.

- [ ] **Step 5: Commit**

```bash
git add README.md CLAUDE.md
git -c user.name="Chenxi LI" -c user.email=eason.li@me.com commit -m "docs: document com.livewire.intraday-catchup install + behavior"
```

---

## Task 8: Full project verification + manual smoke test

**Files:** none modified

- [ ] **Step 1: Run the full project test suite with coverage**

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/ -v --cov=clients --cov=scripts --cov=livewire_scripts --cov-report=term-missing
```

Expected:
- All tests pass.
- Coverage stays at 100% on every module the pyproject `fail_under` gate covers.
- New modules `livewire_scripts/scheduled_env.py` and `livewire_scripts/run_intraday_catchup_job.py` show 100%.

- [ ] **Step 2: Run the warning-as-error guard for the new module's tests**

```bash
python -m pytest tests/test_run_intraday_catchup_job.py tests/test_scheduled_env.py -v -W error::RuntimeWarning
```

Expected: no `RuntimeWarning: coroutine was never awaited` leaks — the wrapper never owns an async runner.

- [ ] **Step 3: Smoke-test the new command's help**

```bash
python scripts/livewire_ops.py -h
```

Expected: usage line lists both `run-daily-job` and `run-intraday-catchup-job` as valid choices.

- [ ] **Step 4: Dry-run the wrapper without actually invoking daily-backfill**

Use a no-op python to confirm the wrapper builds the command and writes a log:

```bash
MDW_INTRADAY_CATCHUP_PYTHON_BIN=/usr/bin/true \
MDW_INTRADAY_CATCHUP_SCRIPT=/usr/bin/true \
python scripts/livewire_ops.py run-intraday-catchup-job
```

Expected: exit 0, with a log file at `~/market-warehouse/logs/intraday_catchup_<today-UTC>.log` containing `=== Intraday Catchup ...` and `=== Done ...`.

- [ ] **Step 5: Install the plist on this Mac (manual confirmation)**

```bash
sed "s|/path/to/repo|$(pwd)|g" launchd/com.livewire.intraday-catchup.plist.example > ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
launchctl load ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
launchctl list | grep com.livewire.intraday-catchup
```

Expected: `launchctl list` shows the new job with exit status `0` (loaded, not yet run) or whatever the most recent run status is.

- [ ] **Step 6: Open the PR**

```bash
git push -u origin feat/intraday-catchup-scheduler
gh pr create --title "feat: scheduled intraday catch-up via daily-backfill" --body "$(cat <<'EOF'
## Summary

- Add `com.livewire.intraday-catchup` launchd job at 16:00 PT that runs the existing `daily-backfill` orchestrator
- Wraps `daily-backfill` in `scripts/livewire_ops.py run-intraday-catchup-job` so it picks up the same env loading (`~/.secrets` → repo `.env` → `~/market-warehouse/.env`) as the daily-update wrapper
- Closes the gap where equity intraday parquet (1m/5m/1h), FRED rates, and CBOE volatility had no scheduled refresh — only 1d was being caught up by the 13:05 PT job

## Design

See `docs/superpowers/specs/2026-06-05-livewire-intraday-catchup-scheduler-design.md` for the full spec and `docs/superpowers/plans/2026-06-05-livewire-intraday-catchup-scheduler.md` for the implementation plan.

## Test plan

- [ ] `pytest tests/test_scheduled_env.py tests/test_run_intraday_catchup_job.py tests/test_livewire_entrypoints.py -v`
- [ ] `pytest tests/ -v --cov=clients --cov=scripts --cov=livewire_scripts` (100% coverage gate)
- [ ] `pytest tests/test_run_intraday_catchup_job.py -W error::RuntimeWarning`
- [ ] Manual smoke: `python scripts/livewire_ops.py run-intraday-catchup-job` writes to `intraday_catchup_<UTC date>.log` and dispatches `daily-backfill`
- [ ] Manual smoke: `launchctl load ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist` succeeds and the job appears in `launchctl list`
EOF
)"
```

Expected: PR URL printed.

---

## Risks and Rollback

**Risk: Daily-update is still running at 16:00 PT.**
On heavy-fallback days the 13:05 PT job has been observed running past 16:00 PT. Both jobs run in parallel in that case. The overlap is safe because:
- Massive 1d catch-up with `--skip-existing` is near-noop once daily-update has finished its 1d pass.
- Intraday parquet files (`{1m,5m,1h}.parquet`) are different files from `1d.parquet`, so atomic `os.replace()` publication does not contend.

If empirical observation shows worse contention, push the intraday-catchup trigger to 17:00 or 18:00 PT by editing the plist `Hour` integer.

**Risk: `daily-backfill` runs longer than expected and overlaps the next morning's pre-market window.**
The orchestrator self-retries on transient failures but exits cleanly when its internal stale-round detection trips. If a run is still going at, say, 06:00 PT the next day, `launchctl list` will show the prior run's exit status. No specific mitigation is needed in the wrapper.

**Risk: Send-alert email failure when `CEREBRAS_API_KEY` is unset.**
The Nodemailer path falls back to a raw error summary (already verified by the 2026-06-03 incident report behavior). Acceptable.

**Rollback:**

```bash
launchctl unload ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
rm ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
```

The codebase still ships the plist template, the wrapper module, and the dispatch entry — those are inert when no plist references them. Reverting the merge commit is also safe because the existing `com.livewire.daily-update` job continues working through the same `livewire_ops.py` entry point.

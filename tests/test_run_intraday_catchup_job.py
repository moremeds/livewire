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
    _utc_now,
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


class TestUtcNow:
    def test_returns_utc_aware_datetime(self):
        result = _utc_now()
        assert result.tzinfo == UTC


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

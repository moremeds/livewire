from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


EXPECTED_SCRIPT_FILES = {
    "livewire_ingest.py",
    "livewire_ops.py",
    "livewire_quality.py",
    "livewire_store.py",
    "setup_market_warehouse.sh",
}


def test_scripts_directory_exposes_only_five_operator_entrypoints() -> None:
    script_files = {path.name for path in (REPO_ROOT / "scripts").iterdir() if path.is_file()}

    assert script_files == EXPECTED_SCRIPT_FILES


def test_operator_entrypoint_modules_are_importable() -> None:
    for module_name in (
        "scripts.livewire_ingest",
        "scripts.livewire_ops",
        "scripts.livewire_quality",
        "scripts.livewire_store",
    ):
        module = importlib.import_module(module_name)

        assert callable(module.main)


def test_operator_entrypoints_render_subcommand_help() -> None:
    expected_commands = {
        "livewire_ingest.py": ["daily", "historical", "robust", "cboe-vol", "intraday-backfill"],
        "livewire_quality.py": ["health", "coverage", "report", "weekly", "watchdog"],
        "livewire_ops.py": ["run-daily-job", "send-alert"],
        "livewire_store.py": ["rebuild-postgres", "smoke-postgres", "sync-r2", "migrate-parquet"],
    }

    for script_name, commands in expected_commands.items():
        result = subprocess.run(
            [sys.executable, str(REPO_ROOT / "scripts" / script_name), "--help"],
            check=True,
            capture_output=True,
            text=True,
        )

        for command in commands:
            assert command in result.stdout


def test_operator_entrypoints_forward_subcommand_help() -> None:
    examples = {
        "livewire_ingest.py": ("daily", "Daily market data update"),
        "livewire_quality.py": ("report", "Livewire data quality report"),
        "livewire_store.py": ("rebuild-postgres", "Rebuild Postgres analytical tables"),
    }

    for script_name, (command, expected) in examples.items():
        result = subprocess.run(
            [sys.executable, str(REPO_ROOT / "scripts" / script_name), command, "--help"],
            check=True,
            capture_output=True,
            text=True,
        )

        assert expected in result.stdout

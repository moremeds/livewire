from __future__ import annotations

import importlib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


EXPECTED_SCRIPT_FILES = {
    "livewire.py",
    "livewire_ingest.py",
    "livewire_ops.py",
    "livewire_quality.py",
    "livewire_store.py",
    "setup_market_warehouse.sh",
}


def test_scripts_directory_exposes_only_five_operator_entrypoints() -> None:
    script_files = {
        path.name for path in (REPO_ROOT / "scripts").iterdir() if path.is_file()
    }

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


def test_ingest_subcommands_include_flatfile_ingest() -> None:
    from scripts import livewire_ingest

    assert "flatfile-ingest" in livewire_ingest.COMMANDS

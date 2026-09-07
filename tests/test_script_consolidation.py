from __future__ import annotations

import importlib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


#: CLAUDE.md, "Where things are": scripts/ holds four entrypoints. A fifth
#: dispatcher (scripts/livewire.py) survived for months because this test
#: required it to exist. The set is now the contract, in both directions.
EXPECTED_SCRIPT_FILES = {
    "livewire_ingest.py",
    "livewire_ops.py",
    "livewire_quality.py",
    "livewire_store.py",
    "setup_market_warehouse.sh",
}


def test_scripts_directory_exposes_only_the_four_operator_entrypoints() -> None:
    script_files = {path.name for path in (REPO_ROOT / "scripts").iterdir() if path.is_file()}

    assert script_files == EXPECTED_SCRIPT_FILES
    assert sum(1 for name in script_files if name.endswith(".py")) == 4


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

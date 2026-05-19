"""Regression tests for retiring DuckDB from active Livewire paths."""

from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ACTIVE_RUNTIME_ROOTS = ("clients", "livewire_scripts", "scripts")


def _imports_duckdb(path: Path) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == "duckdb" or alias.name.startswith("duckdb.") for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            if node.module == "duckdb" or (node.module or "").startswith("duckdb."):
                return True
    return False


def test_active_python_sources_do_not_import_duckdb() -> None:
    offenders = []
    for root_name in ACTIVE_RUNTIME_ROOTS:
        for path in (REPO_ROOT / root_name).rglob("*.py"):
            if _imports_duckdb(path):
                offenders.append(path.relative_to(REPO_ROOT).as_posix())

    assert offenders == []


def test_storage_entrypoint_has_no_duckdb_rebuild_command() -> None:
    from scripts.livewire_store import COMMANDS

    assert "rebuild-duckdb" not in COMMANDS
    assert all("duckdb" not in module_name for module_name in COMMANDS.values())


def test_bootstrap_script_does_not_install_or_initialize_duckdb() -> None:
    setup_script = (REPO_ROOT / "scripts" / "setup_market_warehouse.sh").read_text(
        encoding="utf-8"
    )

    assert "duckdb" not in setup_script.lower()

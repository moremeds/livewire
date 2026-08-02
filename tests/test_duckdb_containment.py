"""Contract tests keeping DuckDB a query layer rather than a second warehouse.

DuckDB was retired in 2026-05 (`c9d5d86`) because it had grown into a parallel
copy of the lake: a `db_client.py` holding tables plus a
`rebuild_duckdb_from_parquet` job to fill them. It is back as a *catalog* — views
over parquet, plus a small coverage table of per-symbol file statistics. These
tests hold that line, so the earlier failure cannot recur quietly.

The rule: DuckDB is imported only where the catalog lives, and nothing
materialises bar data out of bronze.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ACTIVE_RUNTIME_ROOTS = ("clients", "livewire_scripts", "scripts")

# The only modules allowed to import duckdb.
ALLOWED_DUCKDB_IMPORTERS = {
    "clients/duckdb_catalog.py",
    "livewire_scripts/duckdb_catalog_cli.py",
}


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


def test_duckdb_is_imported_only_by_the_catalog() -> None:
    offenders = []
    for root_name in ACTIVE_RUNTIME_ROOTS:
        for path in (REPO_ROOT / root_name).rglob("*.py"):
            relative = path.relative_to(REPO_ROOT).as_posix()
            if _imports_duckdb(path) and relative not in ALLOWED_DUCKDB_IMPORTERS:
                offenders.append(relative)
    assert offenders == []


def test_no_command_materialises_bronze_into_duckdb() -> None:
    """`rebuild-duckdb` was the shape that made DuckDB a second warehouse."""
    from scripts.livewire_store import COMMANDS

    assert "rebuild-duckdb" not in COMMANDS
    assert "duckdb" in COMMANDS


def test_coverage_table_stores_statistics_not_bars() -> None:
    """The durable table holds one row per symbol, never per bar."""
    from clients.duckdb_catalog import _COVERAGE_DDL

    for column in ("view_name", "symbol", "n_rows", "first_date", "last_date"):
        assert column in _COVERAGE_DDL
    for price_column in ("open", "high", "low", "close", "adj_close", "volume"):
        assert price_column not in _COVERAGE_DDL


def test_bootstrap_script_does_not_provision_a_duckdb_warehouse() -> None:
    """The catalog is created on demand by `duckdb build`, not by bootstrap."""
    setup_script = (REPO_ROOT / "scripts" / "setup_market_warehouse.sh").read_text(encoding="utf-8")

    assert "rebuild_duckdb" not in setup_script.lower()
    assert "duckdb_client" not in setup_script.lower()


def test_postgres_layer_is_gone() -> None:
    """Postgres was the publish target DuckDB replaces; it must not linger."""
    for path in (
        "clients/postgres_client.py",
        "clients/postgres_schema.py",
        "livewire_scripts/rebuild_postgres_from_parquet.py",
        "livewire_scripts/smoke_postgres_analytical.py",
    ):
        assert not (REPO_ROOT / path).exists(), f"{path} still present"

    from scripts.livewire_store import COMMANDS

    assert not any("postgres" in name for name in COMMANDS)

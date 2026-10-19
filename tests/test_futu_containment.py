"""The futu SDK (and the pandas it returns) is imported by `clients/futu_client.py` only."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALLOWED = {ROOT / "clients" / "futu_client.py"}


def _imports(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Import):
            names |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module.split(".")[0])
    return names


def test_futu_and_pandas_are_imported_only_by_the_futu_client():
    """futu-api brought pandas in as a dependency; nothing else in livewire uses it."""
    offenders = [
        str(path.relative_to(ROOT))
        for folder in ("clients", "livewire_scripts", "scripts")
        for path in sorted((ROOT / folder).rglob("*.py"))
        if path not in ALLOWED and {"futu", "pandas"} & _imports(path)
    ]
    assert offenders == []

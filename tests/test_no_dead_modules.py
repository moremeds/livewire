"""Every module under clients/ and livewire_scripts/ must be reachable.

Reachable means: an AST walk of imports and string module paths, starting from
the four documented entrypoints, the package __init__ files, tests/conftest.py
and the six launchd templates, arrives at it.

There is no allowlist and there must never be one. Six modules
(scripts/livewire.py, clients/uw_client.py, clients/historical_provider.py,
livewire_scripts/{repair_yahoo_splits,daily_bronze_repair,validate_silver_canary}.py,
~1,500 lines) survived for months precisely because the only test that mentioned
them asserted they existed. If a module genuinely has to stay unreachable,
explain why in a comment here and make the walk find it — do not list it as an
exception.

__init__.py files are excluded as candidates: they are package export lists, not
modules, and clients/__init__.py is the authoritative export list per CLAUDE.md.
They are still walked, so anything they re-export stays reachable.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_PACKAGES = ("clients", "livewire_scripts", "scripts")
_ENTRYPOINTS = (
    "scripts.livewire_ingest",
    "scripts.livewire_ops",
    "scripts.livewire_quality",
    "scripts.livewire_store",
)


def _module_paths() -> dict[str, Path]:
    modules: dict[str, Path] = {}
    for package in _PACKAGES:
        for path in sorted((_REPO_ROOT / package).glob("*.py")):
            name = package if path.stem == "__init__" else f"{package}.{path.stem}"
            modules[name] = path
    return modules


def _references(path: Path, known: set[str]) -> set[str]:
    """Module names this file imports or names as a string."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
                # `from clients import constants` names a submodule, not a symbol.
                names.update(f"{node.module}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value.startswith(_PACKAGES):
                names.add(node.value)
    return names & known


def _launchd_entrypoints(known: set[str]) -> set[str]:
    """Whatever the six scheduled jobs actually run."""
    found: set[str] = set()
    for template in sorted((_REPO_ROOT / "launchd").glob("*.plist.example")):
        for match in re.finditer(r"scripts/(\w+)\.py", template.read_text(encoding="utf-8")):
            candidate = f"scripts.{match.group(1)}"
            if candidate in known:
                found.add(candidate)
    return found


def test_no_module_is_unreachable_from_the_entrypoints() -> None:
    modules = _module_paths()
    known = set(modules)

    conftest = _REPO_ROOT / "tests" / "conftest.py"
    modules["<conftest>"] = conftest

    seeds = set(_ENTRYPOINTS) | _launchd_entrypoints(known) | {"<conftest>"}
    seeds |= {package for package in _PACKAGES if package in known}

    reached: set[str] = set()
    stack = list(seeds)
    while stack:
        name = stack.pop()
        if name in reached:
            continue
        reached.add(name)
        stack.extend(_references(modules[name], known))

    candidates = {name for name, path in modules.items() if path.name != "__init__.py" and name != "<conftest>"}
    unreachable = sorted(candidates - reached)

    assert unreachable == []


def test_the_launchd_templates_run_only_the_four_entrypoints() -> None:
    """A scheduled job that runs a fifth script would keep it alive silently."""
    modules = _module_paths()

    assert _launchd_entrypoints(set(modules)) <= set(_ENTRYPOINTS)

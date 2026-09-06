from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest


@pytest.fixture
def paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    module = importlib.import_module("livewire_scripts.paths")
    monkeypatch.setattr(module.Path, "home", lambda: tmp_path / "home")
    for name in (
        "MDW_WAREHOUSE_DIR",
        "MDW_DATA_LAKE",
        "MDW_LOG_DIR",
        "MDW_CURSOR_DIR",
    ):
        monkeypatch.delenv(name, raising=False)
    return module


def test_default_paths_derive_from_home(paths, tmp_path: Path) -> None:
    warehouse = tmp_path / "home" / "market-warehouse"

    assert paths.warehouse_dir() == warehouse
    assert paths.data_lake_dir() == warehouse / "data-lake"
    assert paths.log_dir() == warehouse / "logs"
    assert paths.cursor_dir() == warehouse / "cursors"


def test_warehouse_override_cascades(paths, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse"
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))

    assert paths.warehouse_dir() == warehouse
    assert paths.data_lake_dir() == warehouse / "data-lake"
    assert paths.log_dir() == warehouse / "logs"
    assert paths.cursor_dir() == warehouse / "cursors"


@pytest.mark.parametrize(
    ("env_name", "resolver_name", "leaf"),
    (
        ("MDW_DATA_LAKE", "data_lake_dir", "lake"),
        ("MDW_LOG_DIR", "log_dir", "log-output"),
        ("MDW_CURSOR_DIR", "cursor_dir", "cursor-output"),
    ),
)
def test_specific_override_wins(
    paths,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_name: str,
    resolver_name: str,
    leaf: str,
) -> None:
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
    override = tmp_path / leaf
    monkeypatch.setenv(env_name, str(override))

    assert getattr(paths, resolver_name)() == override


def test_resolvers_read_environment_at_call_time(
    paths,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    assert paths.warehouse_dir() == tmp_path / "home" / "market-warehouse"

    warehouse = tmp_path / "loaded-after-import"
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))

    assert paths.warehouse_dir() == warehouse
    assert paths.data_lake_dir() == warehouse / "data-lake"


_REPO_ROOT = Path(__file__).resolve().parents[1]
#: Module-level names that used to shadow livewire_scripts.paths. A module that
#: reintroduces one has invented a second lake root that MDW_DATA_LAKE cannot
#: reach, which is how a test can pass against a path production never uses.
_FORBIDDEN_OVERRIDES = {"_DATA_LAKE", "DATA_LAKE", "_LOG_DIR", "_WAREHOUSE_DIR"}


def test_no_module_shadows_the_path_resolvers():
    offenders: list[str] = []
    for package in ("clients", "livewire_scripts"):
        for path in sorted((_REPO_ROOT / package).glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in tree.body:
                targets = []
                if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                    targets = [node.target.id]
                elif isinstance(node, ast.Assign):
                    targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
                offenders += [f"{path.name}:{name}" for name in targets if name in _FORBIDDEN_OVERRIDES]

    assert offenders == []


def test_data_lake_dir_follows_the_warehouse_override(monkeypatch, tmp_path):
    from livewire_scripts.paths import data_lake_dir

    monkeypatch.delenv("MDW_DATA_LAKE", raising=False)
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))

    assert data_lake_dir() == tmp_path / "data-lake"

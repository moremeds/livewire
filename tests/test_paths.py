from __future__ import annotations

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

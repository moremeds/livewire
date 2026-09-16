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


def test_the_lake_lock_lives_under_the_warehouse_not_the_lake(paths, tmp_path: Path) -> None:
    """The lock must not be one more entry in the exFAT directory it exists to protect."""
    lock = paths.lake_lock_path()

    assert lock == paths.warehouse_dir() / "locks" / "lake-io.lock"
    assert not lock.is_relative_to(paths.data_lake_dir())


def test_the_lake_lock_follows_the_warehouse_override(paths, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path / "warehouse"))
    monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path / "elsewhere"))

    assert paths.lake_lock_path() == tmp_path / "warehouse" / "locks" / "lake-io.lock"
    assert not paths.lake_lock_path().is_relative_to(paths.data_lake_dir())


def test_resolve_capacity_target_resolves_an_existing_path(paths, tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()

    assert paths.resolve_capacity_target(real) == real.resolve()


def test_resolve_capacity_target_follows_a_symlink(paths, tmp_path: Path) -> None:
    dest = tmp_path / "external"
    dest.mkdir()
    link = tmp_path / "link"
    link.symlink_to(dest)

    assert paths.resolve_capacity_target(link) == dest.resolve()


def test_resolve_capacity_target_missing_is_unknown_unless_planning(paths, tmp_path: Path) -> None:
    target = tmp_path / "a" / "b" / "c"

    assert paths.resolve_capacity_target(target) is None
    # allow_missing=True answers the nearest existing resolved ancestor — the
    # intended filesystem a genuinely new directory would land on.
    assert paths.resolve_capacity_target(target, allow_missing=True) == tmp_path.resolve()
    assert not (tmp_path / "a").exists()  # resolution must never create


def test_resolve_capacity_target_dangling_link_is_never_an_ancestor_fallback(paths, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    link = raw / "massive"
    link.symlink_to(tmp_path / "gone")

    assert paths.resolve_capacity_target(link) is None
    # Even for planning: falling back to raw/ would measure the internal disk.
    assert paths.resolve_capacity_target(link, allow_missing=True) is None
    # A dangling link mid-chain is a miss too.
    assert paths.resolve_capacity_target(link / "child", allow_missing=True) is None


def test_resolve_capacity_target_missing_child_of_a_symlink_uses_its_volume(paths, tmp_path: Path) -> None:
    dest = tmp_path / "external"
    dest.mkdir()
    link = tmp_path / "link"
    link.symlink_to(dest)

    assert paths.resolve_capacity_target(link / "new", allow_missing=True) == dest.resolve()

"""Tests for livewire_scripts/scheduled_env.py."""

from __future__ import annotations

import os
from pathlib import Path

from livewire_scripts import scheduled_env


def test_load_env_file_handles_missing_path(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("LIVEWIRE_TEST_KEY", raising=False)
    scheduled_env._load_env_file(tmp_path / "missing.env")
    assert "LIVEWIRE_TEST_KEY" not in os.environ


def test_load_env_file_parses_export_and_quoted_values(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# comment line\n"
        "BARE=plain\n"
        "export QUOTED='hello world'\n"
        "EMPTY=\n"
        "=ignored\n"
        "BROKEN_LINE_NO_EQUALS\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("BARE", raising=False)
    monkeypatch.delenv("QUOTED", raising=False)
    monkeypatch.delenv("EMPTY", raising=False)

    scheduled_env._load_env_file(env_file)

    assert os.environ["BARE"] == "plain"
    assert os.environ["QUOTED"] == "hello world"
    assert os.environ["EMPTY"] == ""


def test_load_env_file_tolerates_unterminated_quote(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / "bad.env"
    env_file.write_text("BAD='unterminated\n", encoding="utf-8")
    monkeypatch.delenv("BAD", raising=False)

    scheduled_env._load_env_file(env_file)

    assert os.environ["BAD"] == "'unterminated"


def test_load_scheduled_env_priority(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    home.mkdir()
    secrets = home / ".secrets"
    repo = tmp_path / "repo"
    repo.mkdir()
    repo_env = repo / ".env"
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    warehouse_env = warehouse / ".env"

    secrets.write_text("FROM_SECRETS=from_secrets\nSHARED=secret\n", encoding="utf-8")
    repo_env.write_text("FROM_REPO=from_repo\nSHARED=repo\n", encoding="utf-8")
    warehouse_env.write_text("FROM_WAREHOUSE=from_warehouse\nSHARED=warehouse\n", encoding="utf-8")

    monkeypatch.setattr(scheduled_env.Path, "home", classmethod(lambda cls: home))
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))
    for key in ("FROM_SECRETS", "FROM_REPO", "FROM_WAREHOUSE", "SHARED"):
        monkeypatch.delenv(key, raising=False)

    scheduled_env.load_scheduled_env(repo)

    assert os.environ["FROM_SECRETS"] == "from_secrets"
    assert os.environ["FROM_REPO"] == "from_repo"
    assert os.environ["FROM_WAREHOUSE"] == "from_warehouse"
    # Last-set-wins precedence: warehouse loaded last
    assert os.environ["SHARED"] == "warehouse"


def test_load_scheduled_env_skips_missing_files(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    home.mkdir()
    repo = tmp_path / "repo"
    repo.mkdir()
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()

    monkeypatch.setattr(scheduled_env.Path, "home", classmethod(lambda cls: home))
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))

    # Must not raise
    scheduled_env.load_scheduled_env(repo)

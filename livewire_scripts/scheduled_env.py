"""Shared env-file loader for scheduled livewire jobs.

Single source of truth for the env precedence used by launchd-driven
wrappers (`~/.secrets` → repo `.env` → `~/market-warehouse/.env`, last-set-wins).
Keeping this in one module prevents wrappers from drifting on env semantics
when one is modified.
"""

from __future__ import annotations

import os
import shlex
from pathlib import Path

from livewire_scripts.paths import warehouse_dir


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        try:
            parsed = shlex.split(value, comments=False, posix=True)
        except ValueError:
            parsed = [value.strip()]
        os.environ[key] = parsed[0] if parsed else ""


def load_scheduled_env(repo_root: Path) -> None:
    warehouse = warehouse_dir()
    for env_file in (Path.home() / ".secrets", repo_root / ".env", warehouse / ".env"):
        _load_env_file(env_file.expanduser())

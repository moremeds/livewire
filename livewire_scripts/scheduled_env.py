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


def find_repo_env(repo_root: Path) -> Path | None:
    """Return the nearest `.env` at or above *repo_root*, stopping at $HOME.

    A git worktree has no `.env` of its own — it is gitignored, so it exists
    only in the main checkout. When the launchd plists were pointed at
    `.worktrees/<branch>/`, every credential resolved to nothing: ingest died
    on MASSIVE_API_KEY and the failure alert died on MDW_ALERT_EMAIL_FROM, so
    the outage was silent for six days. Walking up finds the main repo's
    `.env` from any worktree or subdirectory.
    """
    home = Path.home().resolve()
    try:
        current = repo_root.resolve()
    except OSError:
        return None
    for candidate in (current, *current.parents):
        env_file = candidate / ".env"
        if env_file.is_file():
            return env_file
        if candidate == home:
            break
    return None


def load_scheduled_env(repo_root: Path) -> None:
    warehouse = warehouse_dir()
    repo_env = find_repo_env(repo_root)
    sources = [Path.home() / ".secrets"]
    if repo_env is not None:
        sources.append(repo_env)
    sources.append(warehouse / ".env")
    for env_file in sources:
        _load_env_file(env_file.expanduser())

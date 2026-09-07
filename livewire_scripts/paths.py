"""Call-time path resolution for the local Livewire warehouse."""

from __future__ import annotations

import os
from pathlib import Path


def warehouse_dir() -> Path:
    """Return the warehouse root, honoring the current process environment."""
    return Path(os.environ.get("MDW_WAREHOUSE_DIR", Path.home() / "market-warehouse")).expanduser()


def data_lake_dir() -> Path:
    """Return the canonical data-lake root."""
    return Path(os.environ.get("MDW_DATA_LAKE", warehouse_dir() / "data-lake")).expanduser()


def silver_dir() -> Path:
    """Return the Silver publish root."""
    return Path(os.environ.get("MDW_SILVER_DIR", data_lake_dir() / "silver")).expanduser()


def log_dir() -> Path:
    """Return the operational log directory."""
    return Path(os.environ.get("MDW_LOG_DIR", warehouse_dir() / "logs")).expanduser()


def cursor_dir() -> Path:
    """Return the cursor/state directory."""
    return Path(os.environ.get("MDW_CURSOR_DIR", warehouse_dir() / "cursors")).expanduser()


def lake_lock_path() -> Path:
    """The one lock every lane that touches the lake holds while it runs.

    Under the warehouse, never under the lake. The lake root is a directory of
    symlinks onto an exFAT volume whose directory operations are linear
    (pm:2026-09-05-source-evidence-flat-exfat-directory), so a lock file living
    there would be one more entry in the directory the lock exists to protect --
    and it would move with the volume, which is the thing that goes away. It
    honors MDW_WAREHOUSE_DIR like every other resolver, and deliberately does
    NOT honor MDW_DATA_LAKE.
    """
    return warehouse_dir() / "locks" / "lake-io.lock"

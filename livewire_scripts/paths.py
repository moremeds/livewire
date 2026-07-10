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


def log_dir() -> Path:
    """Return the operational log directory."""
    return Path(os.environ.get("MDW_LOG_DIR", warehouse_dir() / "logs")).expanduser()


def cursor_dir() -> Path:
    """Return the cursor/state directory."""
    return Path(os.environ.get("MDW_CURSOR_DIR", warehouse_dir() / "cursors")).expanduser()

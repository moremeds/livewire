"""History discovery and capacity planning for Massive flat files."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from clients import constants
from clients.massive_flatfile_client import MassiveFlatfileClient
from livewire_scripts.paths import resolve_capacity_target


@dataclass(frozen=True)
class FlatfilePlan:
    dates: tuple[date, ...]
    compressed_bytes: int
    free_bytes: int
    projected_bytes: int
    minimum_free_bytes: int

    @property
    def earliest(self) -> date:
        return self.dates[0]

    @property
    def latest(self) -> date:
        return self.dates[-1]

    @property
    def has_capacity(self) -> bool:
        return self.free_bytes - self.projected_bytes >= self.minimum_free_bytes


def date_from_key(key: str) -> date:
    return datetime.strptime(Path(key).name.removesuffix(".csv.gz"), "%Y-%m-%d").date()


def capacity_path(warehouse_dir: Path) -> Path | None:
    """The directory whose filesystem actually receives the raw flat files.

    `data-lake` is a symlink to the external volume in production
    (`/Volumes/DATA_LAKE/...`), so measuring `warehouse_dir` reports the internal
    disk — a filesystem the raw files never touch. Measured 2026-08-16: the
    warehouse root had 38 GiB free against the lake's 6.6 TiB, and the planner
    logged the former every night while writing to the latter.

    Both stores write under `data-lake/raw/massive/us_stocks_sip/...`, and
    `raw/massive` itself may be a child symlink onto yet another volume — the
    target is `raw/massive`, not `raw`. A genuinely new directory resolves to
    its nearest existing ancestor on the intended filesystem; a dangling
    configured link returns None rather than falling back to the internal disk.
    """
    return resolve_capacity_target(warehouse_dir / "data-lake" / "raw" / "massive", allow_missing=True)


def discover_plan(client: MassiveFlatfileClient, warehouse_dir: Path) -> FlatfilePlan:
    objects = client.list_objects()
    dated = sorted((date_from_key(obj["Key"]), int(obj["Size"])) for obj in objects if obj["Key"].endswith(".csv.gz"))
    if not dated:
        raise RuntimeError("Massive minute flat-file listing returned no objects")
    target = capacity_path(warehouse_dir)
    if target is None:
        raise RuntimeError(
            "Massive flat-file destination unreachable: "
            f"{warehouse_dir / 'data-lake' / 'raw' / 'massive'} resolves through a missing "
            "or dangling link — measure nothing rather than an internal volume"
        )
    usage = shutil.disk_usage(target)
    compressed_bytes = sum(size for _, size in dated)
    multiplier = float(os.getenv("MDW_FLATFILE_STORAGE_MULTIPLIER", "8"))
    minimum_free_bytes = int(constants.declared("flatfile_min_free_gb") * 1024**3)
    return FlatfilePlan(
        tuple(d for d, _ in dated),
        compressed_bytes,
        usage.free,
        int(compressed_bytes * multiplier),
        minimum_free_bytes,
    )


def require_capacity(plan: FlatfilePlan) -> None:
    if not plan.has_capacity:
        raise RuntimeError(
            "Insufficient disk for Massive flat-file backfill: "
            f"projected={plan.projected_bytes / 1024**3:.2f} GiB "
            f"free={plan.free_bytes / 1024**3:.2f} GiB "
            f"minimum_remaining={plan.minimum_free_bytes / 1024**3:.2f} GiB"
        )

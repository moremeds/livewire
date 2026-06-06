"""History discovery and capacity planning for Massive flat files."""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from clients.massive_flatfile_client import MassiveFlatfileClient


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


def discover_plan(client: MassiveFlatfileClient, warehouse_dir: Path) -> FlatfilePlan:
    objects = client.list_objects()
    dated = sorted((date_from_key(obj["Key"]), int(obj["Size"])) for obj in objects if obj["Key"].endswith(".csv.gz"))
    if not dated:
        raise RuntimeError("Massive minute flat-file listing returned no objects")
    usage = shutil.disk_usage(warehouse_dir if warehouse_dir.exists() else warehouse_dir.parent)
    compressed_bytes = sum(size for _, size in dated)
    multiplier = float(os.getenv("MDW_FLATFILE_STORAGE_MULTIPLIER", "8"))
    minimum_free_bytes = int(float(os.getenv("MDW_FLATFILE_MIN_FREE_GB", "25")) * 1024**3)
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

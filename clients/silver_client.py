"""Validated Parquet publishers for adjusted Silver artifacts."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow as pa

from clients.adjustment_engine import FactorInterval
from clients.parquet_io import publish_parquet, symbol_lock
from clients.symbol_paths import encode_symbol


@dataclass(frozen=True)
class PublishedArtifact:
    path: Path
    sha256: str
    row_count: int


class SilverClient:
    """Publish daily bars and compact factor intervals below one Silver root."""

    daily_schema = pa.schema(
        [
            pa.field("trade_date", pa.date32(), nullable=False),
            pa.field("symbol_id", pa.int64(), nullable=False),
            pa.field("open", pa.float64(), nullable=False),
            pa.field("high", pa.float64(), nullable=False),
            pa.field("low", pa.float64(), nullable=False),
            pa.field("close", pa.float64(), nullable=False),
            pa.field("adj_close", pa.float64(), nullable=False),
            pa.field("volume", pa.int64(), nullable=False),
            pa.field("price_adjustment_factor", pa.float64(), nullable=False),
            pa.field("split_volume_factor", pa.float64(), nullable=False),
            pa.field("adjustment_revision", pa.int64(), nullable=False),
        ]
    )
    factor_schema = pa.schema(
        [
            pa.field("effective_start", pa.date32()),
            pa.field("effective_end", pa.date32()),
            pa.field("price_adjustment_factor", pa.float64(), nullable=False),
            pa.field("split_volume_factor", pa.float64(), nullable=False),
            pa.field("adjustment_revision", pa.int64(), nullable=False),
        ]
    )

    def __init__(self, silver_root: Path):
        self.root = Path(silver_root)

    def daily_path(self, symbol: str) -> Path:
        return self.root / "asset_class=equity" / f"symbol={encode_symbol(symbol.upper())}" / "1d.parquet"

    def factor_path(self, symbol: str) -> Path:
        return (
            self.root
            / "adjustments"
            / "asset_class=equity"
            / f"symbol={encode_symbol(symbol.upper())}"
            / "factors.parquet"
        )

    def publish_daily(self, symbol: str, rows: list[dict]) -> PublishedArtifact:
        revisions = {int(row["adjustment_revision"]) for row in rows}
        if not revisions or min(revisions) <= 0 or len(revisions) != 1:
            raise ValueError("daily rows require one positive adjustment revision")
        ordered = sorted(rows, key=lambda row: row["trade_date"])
        table = pa.Table.from_pylist(ordered, schema=self.daily_schema)
        return self._publish(self.daily_path(symbol), table, "trade_date")

    def publish_factors(
        self,
        symbol: str,
        intervals: list[FactorInterval],
    ) -> PublishedArtifact:
        revisions = {interval.adjustment_revision for interval in intervals}
        if not revisions or min(revisions) <= 0 or len(revisions) != 1:
            raise ValueError("factor intervals require one positive adjustment revision")
        ordered = sorted(intervals, key=lambda interval: interval.effective_start)
        starts = [interval.effective_start for interval in ordered]
        if len(starts) != len(set(starts)):
            raise ValueError("duplicate factor interval start")
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if previous.effective_end >= current.effective_start:
                raise ValueError("factor intervals overlap")
        rows = [
            {
                **asdict(interval),
                "price_adjustment_factor": float(interval.price_adjustment_factor),
                "split_volume_factor": float(interval.split_volume_factor),
            }
            for interval in ordered
        ]
        table = pa.Table.from_pylist(rows, schema=self.factor_schema)
        return self._publish(self.factor_path(symbol), table, "effective_start")

    @staticmethod
    def _publish(path: Path, table: pa.Table, sort_column: str) -> PublishedArtifact:
        with symbol_lock(path):
            publish_parquet(path, table, sort_column=sort_column)
            checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        return PublishedArtifact(path=path, sha256=checksum, row_count=table.num_rows)

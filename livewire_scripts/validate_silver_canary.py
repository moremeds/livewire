#!/usr/bin/env python3
"""Validate Silver factors and adjusted bars without mutating warehouse data."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from collections.abc import Sequence
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pyarrow.parquet as pq

from clients.corporate_action_store import CorporateActionStore
from clients.symbol_paths import encode_symbol
from livewire_scripts.paths import data_lake_dir


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", nargs="+", default=["NVDA", "AAPL", "SPY"])
    parser.add_argument("--control", required=True, help="A bronze symbol with no active corporate actions")
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _factor_for(rows: list[dict], trade_date: date) -> dict | None:
    return next(
        (row for row in rows if row["effective_start"] <= trade_date <= row["effective_end"]),
        None,
    )


def _close_enough(left: float, right: float) -> bool:
    return math.isclose(float(left), float(right), rel_tol=1e-10, abs_tol=1e-10)


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    silver_root: Path | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else data_lake_dir()
    silver = (
        Path(silver_root)
        if silver_root is not None
        else Path(os.environ.get("MDW_SILVER_DIR", root / "silver")).expanduser()
    )
    symbols = list(dict.fromkeys([*(symbol.upper() for symbol in args.tickers), args.control.upper()]))
    control = args.control.upper()
    action_store = CorporateActionStore(root)
    bronze_paths = {
        symbol: root / "bronze/asset_class=equity" / f"symbol={encode_symbol(symbol)}" / "1d.parquet"
        for symbol in symbols
    }
    before = {symbol: _sha256(path) for symbol, path in bronze_paths.items() if path.exists()}
    results: dict[str, dict] = {}

    for symbol in symbols:
        errors: list[str] = []
        bronze_path = bronze_paths[symbol]
        daily_path = silver / "asset_class=equity" / f"symbol={encode_symbol(symbol)}" / "1d.parquet"
        factor_path = silver / "adjustments/asset_class=equity" / f"symbol={encode_symbol(symbol)}" / "factors.parquet"
        if not all(path.exists() for path in (bronze_path, daily_path, factor_path)):
            results[symbol] = {"passed": False, "errors": ["missing bronze or Silver artifact"]}
            continue

        bronze_rows = pq.ParquetFile(bronze_path).read().to_pylist()
        daily_rows = pq.ParquetFile(daily_path).read().to_pylist()
        factor_rows = pq.ParquetFile(factor_path).read().to_pylist()
        bronze_by_date = {row["trade_date"]: row for row in bronze_rows}
        daily_by_date = {row["trade_date"]: row for row in daily_rows}
        if set(bronze_by_date) != set(daily_by_date):
            errors.append("bronze and Silver dates differ")

        for trade_date, bronze_row in bronze_by_date.items():
            daily_row = daily_by_date.get(trade_date)
            factor = _factor_for(factor_rows, trade_date)
            if daily_row is None or factor is None:
                errors.append(f"missing adjusted row or factor for {trade_date}")
                continue
            price_factor = factor["price_adjustment_factor"]
            volume_factor = factor["split_volume_factor"]
            if not _close_enough(daily_row["price_adjustment_factor"], price_factor):
                errors.append(f"daily price factor mismatch for {trade_date}")
            if not _close_enough(daily_row["split_volume_factor"], volume_factor):
                errors.append(f"daily volume factor mismatch for {trade_date}")
            for column in ("open", "high", "low", "close"):
                if not _close_enough(daily_row[column], bronze_row[column] * price_factor):
                    errors.append(f"{column} adjustment mismatch for {trade_date}")
            expected_volume = int(
                (Decimal(str(bronze_row["volume"])) * Decimal(str(volume_factor))).to_integral_value(
                    rounding=ROUND_HALF_UP
                )
            )
            if daily_row["volume"] != expected_volume:
                errors.append(f"volume adjustment mismatch for {trade_date}")

        actions = action_store.latest_active(symbol)
        ex_date_returns = []
        ordered_dates = sorted(bronze_by_date)
        for action in actions:
            prior_dates = [item for item in ordered_dates if item < action.ex_date]
            if not prior_dates or action.ex_date not in bronze_by_date or action.ex_date not in daily_by_date:
                continue
            previous_date = prior_dates[-1]
            ex_date_returns.append(
                {
                    "action_id": action.action_id,
                    "ex_date": action.ex_date.isoformat(),
                    "raw_return": bronze_by_date[action.ex_date]["close"] / bronze_by_date[previous_date]["close"] - 1,
                    "adjusted_return": daily_by_date[action.ex_date]["close"] / daily_by_date[previous_date]["close"]
                    - 1,
                }
            )
        identity_control = (
            symbol == control
            and not actions
            and all(
                _close_enough(row["price_adjustment_factor"], 1) and _close_enough(row["split_volume_factor"], 1)
                for row in factor_rows
            )
        )
        if symbol == control and not identity_control:
            errors.append("control symbol is not an identity adjustment")
        results[symbol] = {
            "passed": not errors,
            "errors": errors,
            "action_count": len(actions),
            "ex_date_returns": ex_date_returns,
            "identity_control": identity_control,
        }

    after = {symbol: _sha256(path) for symbol, path in bronze_paths.items() if path.exists()}
    bronze_unchanged = before == after and len(before) == len(symbols)
    passed = bronze_unchanged and all(result["passed"] for result in results.values())
    print(
        json.dumps(
            {
                "passed": passed,
                "bronze_unchanged": bronze_unchanged,
                "symbols": results,
            },
            sort_keys=True,
        )
    )
    return 0 if passed else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

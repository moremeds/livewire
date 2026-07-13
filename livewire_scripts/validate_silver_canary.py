#!/usr/bin/env python3
"""Validate Silver factors and adjusted bars without mutating warehouse data."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from collections.abc import Sequence
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:  # pragma: no cover - direct script bootstrap
    sys.path.insert(0, str(REPO_ROOT))

from clients.adjustment_engine import FactorInterval, build_factor_intervals
from clients.corporate_action_store import CorporateActionStore
from clients.symbol_paths import encode_symbol
from livewire_scripts.paths import data_lake_dir

NEW_YORK = ZoneInfo("America/New_York")


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


def _factor_intervals_match(expected: list[FactorInterval], actual: list[dict]) -> bool:
    if len(expected) != len(actual):
        return False
    return all(
        item.effective_start == row["effective_start"]
        and item.effective_end == row["effective_end"]
        and _close_enough(float(item.price_adjustment_factor), row["price_adjustment_factor"])
        and _close_enough(float(item.split_volume_factor), row["split_volume_factor"])
        for item, row in zip(expected, actual, strict=True)
    )


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    silver_root: Path | None = None,
    as_of_date: date | None = None,
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
    effective_as_of = as_of_date or datetime.now(NEW_YORK).date()
    action_store = CorporateActionStore(root)
    bronze_paths = {
        symbol: root / "bronze/asset_class=equity" / f"symbol={encode_symbol(symbol)}" / "1d.parquet"
        for symbol in symbols
    }
    before = {symbol: _sha256(path) for symbol, path in bronze_paths.items() if path.exists()}
    results: dict[str, dict] = {}
    action_count = 0
    effective_action_count = 0
    future_action_count = 0

    for symbol in symbols:
        errors: list[str] = []
        actions = action_store.latest_active(symbol)
        symbol_effective_action_count = sum(action.ex_date <= effective_as_of for action in actions)
        symbol_future_action_count = len(actions) - symbol_effective_action_count
        action_count += len(actions)
        effective_action_count += symbol_effective_action_count
        future_action_count += symbol_future_action_count
        bronze_path = bronze_paths[symbol]
        daily_path = silver / "asset_class=equity" / f"symbol={encode_symbol(symbol)}" / "1d.parquet"
        factor_path = silver / "adjustments/asset_class=equity" / f"symbol={encode_symbol(symbol)}" / "factors.parquet"
        if not all(path.exists() for path in (bronze_path, daily_path, factor_path)):
            results[symbol] = {
                "passed": False,
                "errors": ["missing bronze or Silver artifact"],
                "action_count": len(actions),
                "effective_action_count": symbol_effective_action_count,
                "future_action_count": symbol_future_action_count,
            }
            continue

        bronze_rows = pq.ParquetFile(bronze_path).read().to_pylist()
        daily_rows = pq.ParquetFile(daily_path).read().to_pylist()
        factor_rows = pq.ParquetFile(factor_path).read().to_pylist()
        try:
            expected_factors = build_factor_intervals(bronze_rows, actions, effective_as_of)
            if not _factor_intervals_match(expected_factors, factor_rows):
                errors.append("factor intervals do not match causal expectation")
        except ValueError as exc:
            errors.append(f"causal factor construction failed: {exc}")
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
            "effective_action_count": symbol_effective_action_count,
            "ex_date_returns": ex_date_returns,
            "future_action_count": symbol_future_action_count,
            "identity_control": identity_control,
        }

    after = {symbol: _sha256(path) for symbol, path in bronze_paths.items() if path.exists()}
    bronze_unchanged = before == after and len(before) == len(symbols)
    passed = bronze_unchanged and all(result["passed"] for result in results.values())
    print(
        json.dumps(
            {
                "action_count": action_count,
                "as_of_date": effective_as_of.isoformat(),
                "passed": passed,
                "bronze_unchanged": bronze_unchanged,
                "effective_action_count": effective_action_count,
                "future_action_count": future_action_count,
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

"""Point-repair a single systematic-corruption daily bar per symbol from Yahoo true-raw.

The 2026-07-18 census found trade-date 2021-06-18 carries a modern split-ADJUSTED bar
inside otherwise-raw bronze for 77 equity symbols (ratios cluster on clean split factors).
This re-derives the true-raw OHLCV for exactly that date from Yahoo — raw = split-adjusted
x cumulative split multiplier for ex_dates AFTER the date — and overwrites just that bar
via ``merge_ticker_rows``. A recovered close that is not continuous with the known-good raw
neighbours is refused (``needs-review``), never written: the bronze write path has no OHLCV
sanity gate, so this is the only guard. Read-only in --dry-run (default); --apply backs up
each parquet before mutating. Resumable via a per-symbol cursor.
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from clients.bronze_client import BronzeClient
from clients.symbol_paths import encode_symbol
from clients.yahoo_client import YahooClient, YahooError, YahooNotFound
from livewire_scripts.paths import data_lake_dir
from livewire_scripts.repair_legacy_basis import _write_atomic, backup_symbol

# Prices tight (the reject gate); volume loose (recorded, never blocks) — per the
# "prices tight, volume loose" decision. The band matches the census's own definition
# of "neighbours consistent": a bar within [min*0.70, max*1.43] of its neighbours.
NEIGH_LO, NEIGH_HI = 0.70, 1.43
_ROW_KEYS = ("trade_date", "open", "high", "low", "close", "adj_close", "volume", "source", "price_basis")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-flags", type=Path, help="census flags.parquet; symbols filtered to --target-date")
    parser.add_argument("--tickers", nargs="+", help="explicit symbol list (overrides --audit-flags)")
    parser.add_argument("--target-date", required=True, help="the single ISO trade-date to repair, e.g. 2021-06-18")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--apply", action="store_true", help="back up and write bronze; default is a read-only dry-run")
    parser.add_argument("--pause", type=float, default=0.3, help="seconds between Yahoo requests (rate-limit courtesy)")
    return parser.parse_args(list(argv) if argv is not None else None)


def _symbols_for_date(flags_path: Path, target: date) -> list[str]:
    table = pq.read_table(flags_path, columns=["symbol", "trade_date"])
    iso = target.isoformat()
    out = {
        str(sym)
        for sym, td in zip(table.column("symbol").to_pylist(), table.column("trade_date").to_pylist(), strict=True)
        if str(td) == iso
    }
    return sorted(out)


def recover_raw_bar(client: YahooClient, symbol: str, target: date, end: date) -> dict[str, Any] | None:
    """Return the true-raw OHLCV dict for ``target``, or None if Yahoo has no such bar.

    Yahoo O/H/L/C/volume are split-adjusted; raw = adjusted x product(price_multiplier for
    splits with ex_date > target). Volume divides by that product (a forward split inflates
    adjusted volume). ``adj_close`` mirrors raw close — bronze raw carries no dividend adjustment.
    """
    bars, splits = client.get_daily_ohlcv(symbol, target - timedelta(days=3), end)
    bar = next((b for b in bars if b.trade_date == target), None)
    if bar is None:
        return None
    mult = 1.0
    for split in splits:
        if split.ex_date > target:
            mult *= split.price_multiplier
    raw_close = bar.close * mult
    return {
        "trade_date": target.isoformat(),
        "open": bar.open * mult,
        "high": bar.high * mult,
        "low": bar.low * mult,
        "close": raw_close,
        "adj_close": raw_close,
        "volume": round(bar.volume / mult) if mult else bar.volume,
        "source": "yahoo",
        "price_basis": "raw",
        "_yahoo_close": bar.close,
        "_mult": mult,
        "_yahoo_volume": bar.volume,
    }


def validate_bar(candidate: dict[str, Any], prev_row: dict[str, Any], next_row: dict[str, Any]) -> tuple[bool, str]:
    """Hard gate: recovered close continuous with raw neighbours + OHLC sanity. Prices only."""
    o, h, low_, c = candidate["open"], candidate["high"], candidate["low"], candidate["close"]
    if min(o, h, low_, c) <= 0:
        return False, "nonpositive price"
    if not (h >= max(o, c, low_) and low_ <= min(o, c, h)):
        return False, f"OHLC inconsistent (o={o:.4f} h={h:.4f} l={low_:.4f} c={c:.4f})"
    prev_c, next_c = float(prev_row["close"]), float(next_row["close"])
    lo, hi = min(prev_c, next_c) * NEIGH_LO, max(prev_c, next_c) * NEIGH_HI
    if not (lo <= c <= hi):
        return False, f"close {c:.4f} outside neighbour band [{lo:.4f}, {hi:.4f}] (prev={prev_c:.4f} next={next_c:.4f})"
    return True, "ok"


def _repair_one(
    symbol: str,
    target: date,
    *,
    bronze: BronzeClient,
    client: YahooClient,
    end: date,
    backup_dir: Path | None,
) -> tuple[str, dict]:
    """status in {'repaired','would-repair','needs-review','skip','failed'}."""
    existing = bronze.read_symbol_rows(symbol)
    if not existing:
        return "skip", {"symbol": symbol, "reason": "no_bronze_rows"}
    existing.sort(key=lambda r: r["trade_date"])
    iso = target.isoformat()
    idx = next((i for i, r in enumerate(existing) if r["trade_date"] == iso), None)
    if idx is None:
        return "skip", {"symbol": symbol, "reason": "target_date_absent_from_bronze"}
    if idx == 0 or idx == len(existing) - 1:
        return "needs-review", {"symbol": symbol, "reason": "target_at_series_edge"}
    prev_row, old_row, next_row = existing[idx - 1], existing[idx], existing[idx + 1]

    try:
        candidate = recover_raw_bar(client, symbol, target, end)
    except YahooNotFound:
        return "needs-review", {"symbol": symbol, "reason": "yahoo_symbol_not_found"}
    except YahooError as exc:
        return "failed", {"symbol": symbol, "reason": f"yahoo_error: {exc}"}
    if candidate is None:
        return "needs-review", {"symbol": symbol, "reason": "yahoo_has_no_bar_for_date"}

    ok, reason = validate_bar(candidate, prev_row, next_row)
    prev_v, next_v = float(prev_row["volume"]), float(next_row["volume"])
    vmean = (prev_v + next_v) / 2
    detail = {
        "symbol": symbol,
        "target_date": iso,
        "before": {k: old_row.get(k) for k in ("open", "high", "low", "close", "volume", "source", "price_basis")},
        "after": {k: candidate[k] for k in ("open", "high", "low", "close", "volume", "source", "price_basis")},
        "neighbours": {"prev_close": float(prev_row["close"]), "next_close": float(next_row["close"])},
        "yahoo": {
            "close": candidate["_yahoo_close"],
            "split_mult": candidate["_mult"],
            "volume": candidate["_yahoo_volume"],
        },
        "volume_vs_neighbour_mean": round(candidate["volume"] / vmean, 4) if vmean else None,
        "gate": reason,
    }
    if not ok:
        return "needs-review", detail
    if backup_dir is None:
        return "would-repair", detail

    saved = backup_symbol(bronze, symbol, backup_dir)
    _write_atomic(
        backup_dir.parent / "symbols" / f"{encode_symbol(symbol)}.json",
        {
            "symbol": symbol,
            "status": "in_progress",
            "backup_path": saved["backup_path"],
            "backup_sha256": saved["sha256"],
        },
    )
    bronze.merge_ticker_rows(symbol, [{k: candidate[k] for k in _ROW_KEYS}])
    detail["backup_path"], detail["backup_sha256"] = saved["backup_path"], saved["sha256"]
    return "repaired", detail


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    client_factory: Callable[[], YahooClient] = YahooClient,
    as_of_date: date | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else (args.data_lake_root or data_lake_dir())
    end = as_of_date or datetime.now(UTC).date()
    target = date.fromisoformat(args.target_date)
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")

    if args.tickers:
        symbols = sorted({t.upper() for t in args.tickers})
    elif args.audit_flags:
        symbols = _symbols_for_date(args.audit_flags, target)
    else:
        raise ValueError("provide --tickers or --audit-flags")

    cursor_path = args.output_dir / "cursor.json"
    cursor: dict[str, Any] = {"target_date": target.isoformat(), "data_lake_root": str(root.resolve()), "completed": {}}
    if cursor_path.is_file():
        if not args.resume:
            raise ValueError(f"cursor already exists in {args.output_dir}: pass --resume to continue it")
        loaded = json.loads(cursor_path.read_text())
        if loaded.get("target_date") != target.isoformat() or loaded.get("data_lake_root") != str(root.resolve()):
            raise ValueError("resume cursor does not match the active target-date / data-lake root")
        cursor = loaded

    client = client_factory()
    backup_dir = None if not args.apply else args.output_dir / "backup"
    counts: dict[str, int] = {}
    for symbol in symbols:
        done = cursor["completed"].get(symbol)
        if args.resume and done and done.get("status") in {"repaired", "needs-review", "skip"}:
            counts[done["status"]] = counts.get(done["status"], 0) + 1
            continue
        try:
            status, detail = _repair_one(symbol, target, bronze=bronze, client=client, end=end, backup_dir=backup_dir)
        except Exception as exc:  # noqa: BLE001 — one bad symbol must not abort the batch
            status, detail = "failed", {"symbol": symbol, "reason": f"exception: {exc}"}
        _write_atomic(
            args.output_dir / "symbols" / f"{encode_symbol(symbol)}.json",
            {**detail, "status": status, "data_lake_root": str(root.resolve()), "at": datetime.now(UTC).isoformat()},
        )
        cursor["completed"][symbol] = {"status": status}
        _write_atomic(cursor_path, cursor)
        counts[status] = counts.get(status, 0) + 1
        if args.pause and status not in {"skip"}:
            time.sleep(args.pause)

    _write_atomic(
        args.output_dir / "summary.json",
        {"target_date": target.isoformat(), "counts": counts, "symbols": len(symbols), "applied": bool(args.apply)},
    )
    print(
        json.dumps(
            {"target_date": target.isoformat(), "counts": counts, "symbols": len(symbols), "applied": bool(args.apply)},
            sort_keys=True,
        )
    )
    return 0 if counts.get("failed", 0) == 0 else 1


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

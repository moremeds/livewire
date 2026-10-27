"""Prepend Futu 1m history before a symbol's first Massive session in equity bronze.

Massive flat files reach back a rolling 5 years; Futu serves 1m for a rolling ~8 years
(regular session only, raw prices). This adds the Futu bars dated before the Massive file's
first session — append-only, never touching a Massive bar — and the 5m/30m/1h bars
aggregated from them. Each symbol is gated first:

- overlap: on the Massive file's first sessions, Futu's close matches Massive's within 1 cent
  (`futu_1m_prepend_min_close_match` over at least `..._min_overlap_sessions`). This proves
  the label shift, the price basis and the security at the seam.
- history: Futu's pre-floor daily closes move with bronze 1d (`..._min_return_match`), when
  bronze 1d has those days. Returns, not levels: 1d basis labels are not trustworthy there.
- floor: the file's first session must be on or before the oldest raw Massive partition. A
  later start is the ticker's own listing or rename, and Futu, which is keyed by security,
  would import the predecessor's history (DJT would get DWAC, XXI would get CEP).
- a symbol that also lives in bronze-delisted is refused: its history is split between the
  two trees and a prepend would duplicate it.

Dry-run by default; `--apply` writes. Exit 86 when OpenD has no session.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from clients import constants, ledger
from clients.futu_client import FutuUnavailable, QuoteSession
from clients.ingestion_common import load_preset
from clients.intraday_bronze_client import IntradayBronzeClient
from clients.massive_flatfile_store import MassiveFlatfileStore
from clients.symbol_paths import canonical_symbol, encode_symbol
from clients.timeframe_aggregator import aggregate_bars
from livewire_scripts.flatfile_publisher import DERIVED_TIMEFRAMES
from livewire_scripts.paths import data_lake_dir, warehouse_dir

EXIT_UNAVAILABLE = 86
NEW_YORK = ZoneInfo("America/New_York")
OVERLAP_DAYS = 90  # calendar days of Massive history compared against Futu
FUTU_START = date(2018, 1, 1)  # before Futu's rolling floor; Futu answers from its floor


def _day(ts: datetime) -> date:
    return ts.astimezone(NEW_YORK).date()


def _rth(ts: datetime) -> bool:
    et = ts.astimezone(NEW_YORK)
    return 570 <= et.hour * 60 + et.minute < 960


def _measure(name: str, scope: str, value: float, unit: str, run_id: str) -> dict:
    return {
        "name": name,
        "scope": scope,
        "measured_at": datetime.now(UTC),
        "value": float(value),
        "unit": unit,
        "source": "measured",
        "run_id": run_id,
    }


def _last_close_by_day(rows: list[dict]) -> dict[date, float]:
    out: dict[date, float] = {}
    for row in sorted(rows, key=lambda r: r["bar_timestamp"]):
        out[_day(row["bar_timestamp"])] = row["close"]
    return out


def return_match(futu_close: dict[date, float], daily_close: dict[date, float]) -> tuple[float | None, int]:
    """Share of consecutive common days whose close-to-close returns agree within 0.5%."""
    days = sorted(set(futu_close) & set(daily_close))
    pairs = list(zip(days, days[1:], strict=False))
    if not pairs:
        return None, 0
    agree = sum(
        abs(futu_close[b] / futu_close[a] - daily_close[b] / daily_close[a]) <= 0.005
        for a, b in pairs
        if futu_close[a] and daily_close[a]
    )
    return agree / len(pairs), len(pairs)


def plan(
    session, bronze: Path, delisted: Path, raw_floor: date | None, symbol: str, run_id: str
) -> tuple[list[dict], list[dict]]:
    """Return (rows to prepend, measurements). No rows means refused or nothing to add."""
    folder = f"symbol={encode_symbol(canonical_symbol(symbol))}"
    measures: list[dict] = []

    def refuse(reason: str) -> tuple[list[dict], list[dict]]:
        print(f"{symbol}: REFUSED {reason}")
        return [], measures + [_measure("futu_1m_prepend_refused", f"{symbol}:{reason}", 1, "count", run_id)]

    if (delisted / folder).is_dir():
        return refuse("history_split_with_bronze_delisted")
    path = bronze / folder / "1m.parquet"
    if not path.exists():
        return refuse("no_massive_1m")
    table = pq.read_table(path, columns=["bar_timestamp", "close", "symbol_id"])
    stamps = table.column("bar_timestamp").to_pylist()
    first = _day(min(stamps))
    if raw_floor is None or first > raw_floor:
        return refuse("massive_history_starts_after_the_raw_floor")
    window_end = first + timedelta(days=OVERLAP_DAYS)
    closes = table.column("close").to_pylist()
    massive = {ts: c for ts, c in zip(stamps, closes, strict=True) if _day(ts) <= window_end and _rth(ts)}

    futu = session.history_1m(f"US.{symbol}", FUTU_START, window_end)
    if not futu:
        return refuse("futu_returned_nothing")
    last_futu = _day(max(r["bar_timestamp"] for r in futu))
    futu_close = {r["bar_timestamp"]: r["close"] for r in futu}
    compared = {ts: c for ts, c in massive.items() if _day(ts) <= last_futu}
    sessions = len({_day(ts) for ts in compared})
    matched = sum(ts in futu_close and abs(futu_close[ts] - c) <= 0.0101 for ts, c in compared.items())
    close_match = matched / len(compared) if compared else 0.0
    measures.append(_measure("futu_1m_overlap_close_match", symbol, close_match, "ratio", run_id))
    measures.append(_measure("futu_1m_overlap_sessions", symbol, sessions, "sessions", run_id))
    if sessions < constants.declared("futu_1m_prepend_min_overlap_sessions"):
        return refuse("overlap_too_short")
    if close_match < constants.declared("futu_1m_prepend_min_close_match"):
        return refuse("overlap_close_mismatch")

    pre = [r for r in futu if _day(r["bar_timestamp"]) < first]
    if not pre:
        print(f"{symbol}: nothing before {first}")
        return [], measures + [_measure("futu_1m_prepend_bars", symbol, 0, "bars", run_id)]
    daily = bronze / folder / "1d.parquet"
    if daily.exists():
        d = pq.read_table(daily, columns=["trade_date", "close"])
        daily_close = dict(zip(d.column("trade_date").to_pylist(), d.column("close").to_pylist(), strict=True))
        ratio, pairs = return_match(_last_close_by_day(pre), daily_close)
        if ratio is not None:
            measures.append(_measure("futu_1m_pre_floor_return_match", symbol, ratio, "ratio", run_id))
            if ratio < constants.declared("futu_1m_prepend_min_return_match"):
                return refuse("pre_floor_daily_return_mismatch")
    symbol_id = int(table.column("symbol_id")[0].as_py())
    rows = [r | {"symbol_id": symbol_id} for r in sorted(pre, key=lambda r: r["bar_timestamp"])]
    days = len({_day(r["bar_timestamp"]) for r in rows})
    print(
        f"{symbol}: {len(rows)} bars over {days} sessions, {_day(rows[0]['bar_timestamp'])}..{first - timedelta(days=1)}"
    )
    measures.append(_measure("futu_1m_prepend_sessions", symbol, days, "sessions", run_id))
    return rows, measures


def publish(bronze: Path, symbol: str, rows: list[dict]) -> int:
    """Append-only: a timestamp already in a file keeps its existing bar."""
    added = IntradayBronzeClient(bronze_dir=bronze, timeframe="1m").merge_ticker_rows(
        symbol, rows, overwrite_existing=False
    )
    for timeframe in DERIVED_TIMEFRAMES:
        IntradayBronzeClient(bronze_dir=bronze, timeframe=timeframe).merge_ticker_rows(
            symbol, aggregate_bars(rows, source_tf="1m", target_tf=timeframe), overwrite_existing=False
        )
    return added


def main(argv: list[str] | None = None, *, session_factory=QuoteSession) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tickers", nargs="+")
    group.add_argument("--preset", type=Path)
    parser.add_argument("--apply", action="store_true", help="Write bronze; the default is a dry run")
    parser.add_argument("--host", default=os.getenv("LW_FUTU_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("LW_FUTU_PORT", "11111")))
    args = parser.parse_args(argv)
    tickers = args.tickers or load_preset(args.preset)[1]

    lake = data_lake_dir()
    bronze, delisted = lake / "bronze" / "asset_class=equity", lake / "bronze-delisted" / "asset_class=equity"
    raw_floor = MassiveFlatfileStore(warehouse_dir()).oldest_raw_date()
    run_id = ledger.new_run_id("futu-1m-history")
    run_row = {
        "run_id": run_id,
        "job": "futu-1m-history",
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": datetime.now(UTC),
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    ledger.open_run(run_row)
    rows: list[dict] = []
    failures = 0
    try:
        with session_factory(args.host, args.port) as session:
            session.require_logged_in()
            for symbol in tickers:
                try:
                    bars, measured = plan(session, bronze, delisted, raw_floor, symbol, run_id)
                    rows += measured
                    if bars and args.apply:
                        added = publish(bronze, symbol, bars)
                        rows.append(_measure("futu_1m_prepend_bars", symbol, added, "bars", run_id))
                except FutuUnavailable:
                    raise
                except Exception as exc:  # one symbol never costs the others
                    failures += 1
                    rows.append(
                        _measure("futu_1m_prepend_failed", f"{symbol}:{type(exc).__name__}", 1, "count", run_id)
                    )
                    print(f"{symbol}: FAILED {type(exc).__name__}: {exc}")
        code = 1 if failures else 0
    except FutuUnavailable as exc:
        print(f"futu-1m-history: SKIPPED {exc}")
        code = EXIT_UNAVAILABLE
    except BaseException:
        ledger.emit(
            "runs", [run_row | {"ended": datetime.now(UTC), "exit_code": 1, "verdict": "FAILED"}], run_id=run_id
        )
        if rows:
            ledger.emit("measurements", rows, run_id=run_id)
        raise
    if rows:
        ledger.emit("measurements", rows, run_id=run_id)
    verdict = {0: "OK", EXIT_UNAVAILABLE: "DEGRADED"}.get(code, "FAILED")
    ledger.emit("runs", [run_row | {"ended": datetime.now(UTC), "exit_code": code, "verdict": verdict}], run_id=run_id)
    if not args.apply:
        print("dry run: nothing written (add --apply)")
    return code


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

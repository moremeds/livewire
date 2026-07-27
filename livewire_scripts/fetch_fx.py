#!/usr/bin/env python3
"""Fetch DXY and FX currency pairs into bronze parquet.

Daily comes from Yahoo, which is the only source with deep history (DX-Y.NYB reaches
1971-01-04; Massive's FX daily is a 2-year rolling window). Intraday comes from Massive
for currency pairs, whose 2-year window is far deeper than Yahoo's 7-day 1m cap, and
from Yahoo for DXY, which Massive does not carry at all.

Both intraday sources are rolling windows, so history is accumulated by repeated merges
rather than fetched once. The floor bounds only the initial seed: every subsequent run
merges its window into the existing file, so held history grows past the floor.

Design and the measurements behind it:
docs/superpowers/specs/2026-07-27-yahoo-massive-fx-dxy-ingest-design.md
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from rich.console import Console

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients.bronze_client import BronzeClient
from clients.intraday_bronze_client import IntradayBronzeClient
from clients.massive_client import MassiveAuthError, MassiveClient
from clients.yahoo_client import YahooClient, YahooNotFound
from livewire_scripts.paths import warehouse_dir

ASSET_CLASS = "fx"
DEFAULT_PRESET = _PROJECT_ROOT / "presets" / "fx-pairs.json"

#: DXY is an index, not a six-letter pair, and only Yahoo carries it.
DXY_SYMBOL = "DXY"
YAHOO_DXY_SYMBOL = "DX-Y.NYB"

INTRADAY_TIMEFRAMES = ("1m", "5m", "30m", "1h")
ALL_TIMEFRAMES = ("1d", *INTRADAY_TIMEFRAMES)

#: Massive's FX entitlement floor is 2 years rolling (measured 2024-07-24 on 2026-07-27).
#: The seed reaches slightly past it and skips whatever 403s, so a moving floor needs no
#: code change.
MASSIVE_SEED_DAYS = 760

#: The REST plan allows 5 requests/minute (measured 2026-07-27: 5 succeed, the 6th 429s,
#: and no Retry-After header is sent). Every request therefore costs 12 seconds, so chunk
#: spans are sized to put as many bars in one response as the 50,000-row page allows —
#: FX trades ~24h/day, so bars/day is 1440, 288, 48 and 24 respectively.
MASSIVE_REQUESTS_PER_MINUTE = 5
MASSIVE_MIN_INTERVAL_SECONDS = 60.0 / MASSIVE_REQUESTS_PER_MINUTE
MASSIVE_CHUNK_DAYS = {"1m": 30, "5m": 150, "30m": 240}

#: Timeframes Yahoo serves for every symbol, pairs included. Yahoo's 1h window measured
#: deeper than Massive's entitlement floor (EURUSD 2023-10-09 and USDKRW 2023-10-10 versus
#: Massive's 2024-07-24) and costs one unthrottled request instead of several rate-limited
#: ones, so Massive is the wrong source at this timeframe even though it is right below it.
YAHOO_ONLY_TIMEFRAMES = ("1h",)

console = Console()


def yahoo_symbol(symbol: str) -> str:
    """Map a local symbol to Yahoo's.

    ``USDJPY=X`` and ``JPY=X`` return identical series, so the uniform ``<PAIR>=X`` rule
    holds for USD-base pairs too and no mapping table is needed.
    """
    return YAHOO_DXY_SYMBOL if symbol == DXY_SYMBOL else f"{symbol}=X"


def load_preset_tickers(path: Path) -> list[str]:
    payload = json.loads(path.read_text())
    tickers = payload.get("tickers") or []
    if not tickers:
        raise ValueError(f"preset {path} lists no tickers")
    return [str(ticker).upper() for ticker in tickers]


def daily_rows(bars, symbol_id: int) -> list[dict]:
    """Build fx daily bronze rows. FX has no splits or dividends, so adj_close == close."""
    return [
        {
            "trade_date": bar.timestamp.date(),
            "symbol_id": symbol_id,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "adj_close": bar.close,
            "volume": bar.volume,
        }
        for bar in bars
    ]


def intraday_rows(bars, symbol_id: int) -> list[dict]:
    """Build intraday bronze rows from either provider's bar objects."""
    return [
        {
            "bar_timestamp": getattr(bar, "bar_timestamp", None) or bar.timestamp,
            "symbol_id": symbol_id,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        }
        for bar in bars
    ]


def fetch_massive_intraday(
    massive: MassiveClient,
    pair: str,
    timeframe: str,
    start: date,
    end: date,
) -> list:
    """Fetch a pair's intraday bars, walking forward in chunks from ``start``.

    A chunk below the rolling entitlement floor returns HTTP 403, which is an entitlement
    boundary rather than a data gap. Skipping those chunks instead of aborting means the
    seed always reaches maximum available depth without hardcoding the floor date.
    """
    bars: list = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=MASSIVE_CHUNK_DAYS[timeframe]), end)
        try:
            bars.extend(massive.get_fx_intraday_bars(pair, timeframe, chunk_start, chunk_end))
        except MassiveAuthError:
            pass  # below the entitlement floor — later chunks may still succeed
        chunk_start = chunk_end + timedelta(days=1)
    return bars


def sync_daily(
    symbols: Sequence[str],
    bronze_dir: Path,
    yahoo: YahooClient,
    *,
    days: int | None,
) -> int:
    start = (datetime.now(tz=UTC).date() - timedelta(days=days)) if days else None
    failures = 0
    with BronzeClient(bronze_dir=bronze_dir, asset_class=ASSET_CLASS) as bronze:
        for symbol in symbols:
            try:
                bars = yahoo.get_daily_ohlcv(yahoo_symbol(symbol), start=start)
            except YahooNotFound:
                console.print(f"  [yellow]{symbol}: not found on Yahoo ({yahoo_symbol(symbol)})[/yellow]")
                failures += 1
                continue
            if not bars:
                console.print(f"  [yellow]{symbol}: no daily bars returned[/yellow]")
                continue
            rows = daily_rows(bars, bronze.get_symbol_id(symbol))
            inserted = bronze.merge_ticker_rows(symbol, rows)
            console.print(
                f"  {symbol}: {len(rows)} bars, +{inserted} new, "
                f"{bars[0].timestamp.date()} -> {bars[-1].timestamp.date()}"
            )
    return failures


def sync_intraday(
    symbols: Sequence[str],
    timeframes: Sequence[str],
    bronze_dir: Path,
    yahoo: YahooClient,
    massive: MassiveClient | None,
    *,
    days: int | None,
) -> int:
    today = datetime.now(tz=UTC).date()
    start = today - timedelta(days=days if days else MASSIVE_SEED_DAYS)
    failures = 0
    for timeframe in timeframes:
        client = IntradayBronzeClient(bronze_dir=bronze_dir, timeframe=timeframe)
        console.print(f"\n[bold]{timeframe}[/bold]")
        for symbol in symbols:
            # Yahoo owns 1h outright, and DXY at every timeframe since Massive does not
            # carry it. Massive owns 1m/5m/30m for pairs, where its window is far deeper.
            use_yahoo = timeframe in YAHOO_ONLY_TIMEFRAMES or symbol == DXY_SYMBOL
            try:
                if use_yahoo:
                    bars = yahoo.get_intraday(yahoo_symbol(symbol), timeframe)
                else:
                    if massive is None:
                        continue
                    bars = fetch_massive_intraday(massive, symbol, timeframe, start, today)
            except YahooNotFound:
                console.print(f"  [yellow]{symbol}: not found on Yahoo[/yellow]")
                failures += 1
                continue
            if not bars:
                console.print(f"  [yellow]{symbol}: no {timeframe} bars returned[/yellow]")
                continue
            rows = intraday_rows(bars, client.get_symbol_id(symbol))
            inserted = client.merge_ticker_rows(symbol, rows)
            first = rows[0]["bar_timestamp"]
            last = rows[-1]["bar_timestamp"]
            console.print(f"  {symbol}: {len(rows)} bars, +{inserted} new, {first:%Y-%m-%d} -> {last:%Y-%m-%d}")
    return failures


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--preset", type=Path, default=DEFAULT_PRESET, help=f"Preset JSON (default: {DEFAULT_PRESET})")
    parser.add_argument("--tickers", nargs="+", help="Explicit symbols, overriding --preset")
    parser.add_argument(
        "--timeframes",
        nargs="+",
        choices=ALL_TIMEFRAMES,
        default=list(ALL_TIMEFRAMES),
        help=f"Timeframes to sync (default: {' '.join(ALL_TIMEFRAMES)})",
    )
    parser.add_argument(
        "--days",
        type=int,
        help=(
            "Only fetch the last N days from Massive. Omit to seed maximum available "
            "depth. Yahoo-sourced series ignore this — its chart API accepts only a "
            "discrete set of ranges, so those always fetch their full window."
        ),
    )
    parser.add_argument("--warehouse", type=Path, default=warehouse_dir(), help=f"Default: {warehouse_dir()}")
    return parser.parse_args(list(argv) if argv is not None else None)


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    symbols = [t.upper() for t in args.tickers] if args.tickers else load_preset_tickers(args.preset)
    bronze_dir = args.warehouse / "data-lake" / "bronze" / f"asset_class={ASSET_CLASS}"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    yahoo = YahooClient()
    failures = 0

    if "1d" in args.timeframes:
        console.print(f"\n[bold]Daily (Yahoo) — {len(symbols)} symbols[/bold]")
        failures += sync_daily(symbols, bronze_dir, yahoo, days=args.days)

    intraday = [tf for tf in args.timeframes if tf in INTRADAY_TIMEFRAMES]
    if intraday:
        pairs = [s for s in symbols if s != DXY_SYMBOL]
        massive_timeframes = [tf for tf in intraday if tf not in YAHOO_ONLY_TIMEFRAMES]
        massive = None
        if pairs and massive_timeframes:
            try:
                massive = MassiveClient(min_interval_seconds=MASSIVE_MIN_INTERVAL_SECONDS)
            except MassiveAuthError as exc:
                console.print(f"[red]Massive unavailable, skipping pair intraday: {exc}[/red]")
                failures += 1
        span = args.days or MASSIVE_SEED_DAYS
        # A lower bound: counts chunks but not the extra pages a chunk needs when it
        # exceeds the 50,000-row limit, so a 1m seed runs longer than this says.
        chunks = len(pairs) * sum(-(-span // MASSIVE_CHUNK_DAYS[tf]) for tf in massive_timeframes)
        console.print(
            f"\n[bold]Intraday[/bold] — Yahoo: DXY + all {'/'.join(YAHOO_ONLY_TIMEFRAMES)}; "
            f"Massive: {len(pairs)} pairs × {len(massive_timeframes)} timeframes, "
            f"≥{chunks} requests at {MASSIVE_REQUESTS_PER_MINUTE}/min "
            f"(≥{chunks * MASSIVE_MIN_INTERVAL_SECONDS / 60:.0f} min)"
        )
        failures += sync_intraday(symbols, intraday, bronze_dir, yahoo, massive, days=args.days)

    if failures:
        console.print(f"\n[yellow]Completed with {failures} failure(s).[/yellow]")
        return 1
    console.print("\n[bold green]Done.[/bold green]")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

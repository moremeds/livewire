#!/usr/bin/env python3
"""Fetch historical daily OHLCV data from Interactive Brokers into bronze parquet.

Parallelises requests using ib_async's async API with a semaphore to respect
IB's pacing limit (~6 concurrent historical-data requests).

Publishes:
  - data-lake/bronze/asset_class=equity/symbol=<ticker>/1d.parquet

Requires IB Gateway or TWS running on localhost.

Usage:
    source ~/market-warehouse/.venv/bin/activate

    # Fetch Mag 7 (default, inception to present):
    python scripts/fetch_ib_historical.py

    # Custom tickers:
    python scripts/fetch_ib_historical.py --tickers AAPL NVDA

    # From a preset file (with cursor-based resume):
    python scripts/fetch_ib_historical.py --preset presets/sp500.json

    # Reset cursor and start fresh:
    python scripts/fetch_ib_historical.py --preset presets/sp500.json --reset

    # Custom batch size:
    python scripts/fetch_ib_historical.py --preset presets/sp500.json --batch-size 25

    # Custom IB Gateway port and concurrency:
    python scripts/fetch_ib_historical.py --port 7497 --max-concurrent 4

    # Backfill missing older data for tickers already in bronze parquet:
    python scripts/fetch_ib_historical.py --preset presets/sp500.json --backfill
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from ib_async import Contract, Forex, Future, Index, Stock
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

# Add project root to path so clients module is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.bronze_client import BronzeClient
from clients.ib_client import IBClient, IBError
from clients.quality_detector import _normalize_bars_for_detection, detect_all
from clients.quality_flags import alert_on_flag, append_audit, write_sidecar

_DEFAULT_STORAGE_CLIENT = BronzeClient
StorageClient = BronzeClient


def _storage_client():
    """Return the live storage client, allowing tests to patch the seam."""
    if BronzeClient is not _DEFAULT_STORAGE_CLIENT:
        return BronzeClient
    if StorageClient is not _DEFAULT_STORAGE_CLIENT:
        return StorageClient
    return BronzeClient

# ── Config ─────────────────────────────────────────────────────────────

DATA_LAKE = Path(os.getenv("MDW_DATA_LAKE", str(Path.home() / "market-warehouse" / "data-lake")))
BRONZE_DIR = DATA_LAKE / "bronze" / "asset_class=equity"
CURSOR_DIR = Path.home() / "market-warehouse" / "logs"

MAG7 = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]

# Earliest date IB historical data API supports. Used as fallback when
# reqHeadTimeStamp returns empty (e.g. BND, DVY — IB can serve bars but
# won't report a head timestamp). The fetch will still retrieve all available
# history; windows before the actual listing date simply return 0 bars.
IB_EARLIEST_DATE = datetime(1993, 1, 29)

console = Console()
_QUALITY_ENABLED = True


ROOT_EXCHANGE_MAP = {
    "ES": "CME", "NQ": "CME", "RTY": "CME",
    "YM": "CBOT", "ZB": "CBOT", "ZN": "CBOT", "ZF": "CBOT",
    "CL": "NYMEX", "NG": "NYMEX",
    "GC": "COMEX", "SI": "COMEX",
}
SUPPORTED_IB_FX_PAIRS = {
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD",
    "USDJPY", "USDCHF", "USDCAD", "USDHKD", "USDSGD", "USDSEK", "USDNOK",
    "USDDKK", "USDCNH", "USDMXN", "USDZAR",
    "EURGBP", "EURJPY", "EURCHF", "EURCAD", "EURAUD", "EURNZD",
    "GBPJPY", "GBPCHF", "GBPCAD", "GBPAUD", "GBPNZD",
    "AUDJPY", "AUDCHF", "AUDCAD", "AUDNZD",
    "NZDJPY", "NZDCHF", "NZDCAD",
    "CADJPY", "CADCHF", "CHFJPY",
}


def _resolve_fx_pair(ticker: str) -> tuple[str, bool]:
    """Return ``(ib_pair, invert)`` for a local six-letter FX pair."""
    pair = ticker.upper()
    if len(pair) != 6 or not pair.isalpha():
        raise ValueError(f"FX ticker must be a six-letter currency pair: {ticker!r}")
    if pair in SUPPORTED_IB_FX_PAIRS:
        return (pair, False)

    reversed_pair = pair[3:] + pair[:3]
    if reversed_pair in SUPPORTED_IB_FX_PAIRS:
        return (reversed_pair, True)

    raise ValueError(f"unsupported FX pair: {ticker!r}")


def _is_inverted_fx_pair(ticker: str) -> bool:
    """Return True when local FX rows must invert the source pair."""
    return _resolve_fx_pair(ticker)[1]


def _make_contract(ticker: str, asset_class: str = "equity", exchange: str | None = None):
    """Build an IB contract for the given *ticker* and *asset_class*."""
    if asset_class == "futures":
        root, expiry = ticker.rsplit("_", 1)
        exch = exchange or ROOT_EXCHANGE_MAP.get(root, "CME")
        return Future(root, expiry, exch, "USD")
    if asset_class == "cmdty":
        return Contract(secType="CMDTY", symbol=ticker.upper(), exchange=exchange or "SMART", currency="USD")
    if asset_class == "fx":
        source_pair, _ = _resolve_fx_pair(ticker)
        return Forex(source_pair)
    if asset_class == "volatility":
        return Index(ticker, "CBOE", "USD")
    return Stock(ticker, "SMART", "USD")


# ── Preset & cursor helpers ───────────────────────────────────────────


def load_preset(path: str | Path) -> tuple[str, list[str], dict[str, str]]:
    """Read a preset JSON file and return ``(name, tickers, exchange_map)``.

    Standard presets use a ``tickers`` array.  Futures presets use a
    ``contracts`` array of ``{root, exchange, expiry}`` dicts which are
    flattened into composite ticker strings (``ES_202506``) and an exchange
    map so ``_make_contract`` can resolve the correct venue.
    """
    p = Path(path)
    with p.open() as f:
        data = json.load(f)

    exchange_map: dict[str, str] = {}
    if "contracts" in data:
        tickers: list[str] = []
        for contract in data["contracts"]:
            composite = f"{contract['root']}_{contract['expiry']}"
            tickers.append(composite)
            exchange_map[composite] = contract.get("exchange", "CME")
        return (data["name"], tickers, exchange_map)

    return (data["name"], data["tickers"], exchange_map)


def _cursor_path(name: str) -> Path:
    """Return the cursor file path for a given run name."""
    return CURSOR_DIR / f"cursor_{name}.json"


def load_cursor(name: str) -> dict[str, list[str]]:
    """Load completed-timeframes-per-ticker cursor.

    Backward compatible: old format ``{"completed": ["AAPL", ...]}`` is migrated
    to the new format treating each listed ticker as fully complete (all known
    timeframes done).
    """
    path = _cursor_path(name)
    if not path.exists():
        return {}
    with path.open() as f:
        data = json.load(f)
    raw = data.get("completed")
    if isinstance(raw, list):
        # Legacy format — migrate
        return {ticker: ["1d", "1h", "5m"] for ticker in raw}
    if isinstance(raw, dict):
        return {k: list(v) for k, v in raw.items()}
    return {}


def save_cursor(name: str, completed: dict[str, list[str]], started_at: str) -> None:
    """Write per-timeframe cursor JSON atomically."""
    path = _cursor_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    payload = {
        "completed": {k: sorted(set(v)) for k, v in completed.items()},
        "started_at": started_at,
        "updated_at": datetime.now().isoformat(),
    }
    with tmp.open("w") as f:
        json.dump(payload, f, indent=2)
    tmp.rename(path)


def is_ticker_complete(
    cursor: dict[str, list[str]],
    ticker: str,
    required: tuple[str, ...],
) -> bool:
    """Return True if all required timeframes are completed for *ticker*."""
    done = set(cursor.get(ticker, []))
    return all(tf in done for tf in required)


def mark_timeframe_done(
    cursor: dict[str, list[str]], ticker: str, timeframe: str
) -> None:
    """Add *timeframe* to a ticker's completed list (idempotent)."""
    done = cursor.setdefault(ticker, [])
    if timeframe not in done:
        done.append(timeframe)


def clear_cursor(name: str) -> None:
    """Delete cursor file if it exists."""
    path = _cursor_path(name)
    if path.exists():
        path.unlink()


# ── Date windowing ────────────────────────────────────────────────────


def compute_date_windows(
    head_dt: datetime, end_dt: datetime
) -> list[tuple[str, str]]:
    """Generate ``("1 Y", end_datetime_str)`` tuples covering *head_dt* to *end_dt*.

    Walks backwards from *end_dt* in ~1-year steps.  Each window requests up to
    ``"1 Y"`` of data ending at the given date-time string.  The final (earliest)
    window is clamped so it doesn't extend before *head_dt*.

    Returns an empty list when *head_dt* >= *end_dt*.
    """
    if head_dt >= end_dt:
        return []

    windows: list[tuple[str, str]] = []
    cursor = end_dt

    while cursor > head_dt:
        end_str = cursor.strftime("%Y%m%d-%H:%M:%S")
        one_year_back = cursor - timedelta(days=365)

        if one_year_back <= head_dt:
            # Final window — remaining range fits in 1 Y
            windows.append(("1 Y", end_str))
            break
        else:
            windows.append(("1 Y", end_str))
            cursor = one_year_back

    return windows


def compute_intraday_chunks(
    timeframe: str,
    years_back: int,
) -> list[tuple[str, str]]:
    """Generate ``(duration_str, end_datetime_str)`` chunks for an intraday backfill.

    For 1m: walks backwards from now in 1-day chunks for ``years_back`` years.
    For 5m: walks backwards from now in 1-week chunks for ``years_back`` years.
    For 1h: walks backwards from now in 1-month chunks for ``years_back`` years.

    Raises ValueError on unsupported timeframe.
    """
    from clients.intraday_bronze_client import INTRADAY_MAX_REQUEST_DURATION

    if timeframe not in INTRADAY_MAX_REQUEST_DURATION:
        raise ValueError(f"unsupported intraday timeframe: {timeframe!r}")

    duration = INTRADAY_MAX_REQUEST_DURATION[timeframe]
    end_dt = datetime.now()

    _step_map = {"1m": timedelta(days=1), "5m": timedelta(weeks=1), "1h": timedelta(days=30)}
    step = _step_map[timeframe]

    head_dt = end_dt - timedelta(days=365 * years_back)

    chunks: list[tuple[str, str]] = []
    cursor = end_dt
    while cursor > head_dt:
        chunks.append((duration, cursor.strftime("%Y%m%d-%H:%M:%S")))
        cursor -= step

    return chunks


# ── Transform ──────────────────────────────────────────────────────────


def bars_to_rows(bars: list, symbol_id: int) -> list[dict]:
    """Convert IB BarData objects to md.equities_daily row dicts.

    IB BarData fields: date, open, high, low, close, volume (native types).
    """
    rows = []
    for bar in bars:
        rows.append(
            {
                "trade_date": str(bar.date),
                "symbol_id": symbol_id,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "adj_close": float(bar.close),
                "volume": int(bar.volume),
            }
        )
    return rows


def bars_to_futures_rows(
    bars: list, contract_id: int, root_symbol: str, expiry_date: str,
) -> list[dict]:
    """Convert IB BarData objects to md.futures_daily row dicts.

    settlement defaults to close; open_interest defaults to 0 (IB BarData
    does not include these fields for daily bars).
    """
    rows = []
    for bar in bars:
        rows.append(
            {
                "trade_date": str(bar.date),
                "contract_id": contract_id,
                "root_symbol": root_symbol,
                "expiry_date": expiry_date,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "settlement": float(bar.close),
                "volume": int(bar.volume),
                "open_interest": 0,
            }
        )
    return rows


def bars_to_midpoint_rows(bars: list, symbol_id: int, *, invert: bool = False) -> list[dict]:
    """Convert IB midpoint bars to daily bronze rows.

    IB midpoint bars report volume as ``-1``. Store volume as 0 because no
    exchange volume is available for this data type.
    """
    rows = bars_to_rows(bars, symbol_id)
    for row in rows:
        if invert:
            open_px = row["open"]
            high_px = row["high"]
            low_px = row["low"]
            close_px = row["close"]
            row["open"] = 1 / open_px
            row["high"] = 1 / low_px
            row["low"] = 1 / high_px
            row["close"] = 1 / close_px
            row["adj_close"] = row["close"]
        row["volume"] = 0
    return rows


# ── Async fetching ────────────────────────────────────────────────────


async def fetch_ticker_bars(
    ticker: str, ib: IBClient, semaphore: asyncio.Semaphore,
    max_years: int = 0,
    end_dt_override: datetime | None = None,
    asset_class: str = "equity",
    exchange: str | None = None,
) -> tuple[str, list]:
    """Fetch historical daily bars for *ticker*.

    When *max_years* > 0, caps lookback to that many years instead of inception.
    When *end_dt_override* is set, uses it as the end date and ignores *max_years*.
    Returns ``(ticker, bars)`` where bars are deduplicated IB BarData objects.
    """
    t0 = time.monotonic()
    contract = _make_contract(ticker, asset_class, exchange=exchange)
    await ib.ib.qualifyContractsAsync(contract)

    head_ts = await ib.get_head_timestamp_async(contract)
    if isinstance(head_ts, datetime):
        head_dt = head_ts.replace(tzinfo=None)
    else:
        head_str = str(head_ts)
        if not head_str or head_str == "[]":
            console.print(
                f"    [dim]{ticker}: no head timestamp — falling back to {IB_EARLIEST_DATE:%Y-%m-%d}[/dim]"
            )
            head_dt = IB_EARLIEST_DATE
        else:
            head_dt = datetime.strptime(head_str, "%Y%m%d-%H:%M:%S")

    if end_dt_override is not None:
        end_dt = end_dt_override
    else:
        end_dt = datetime.now()

        # Cap lookback if max_years is set (only in normal mode)
        if max_years > 0:
            earliest_allowed = end_dt - timedelta(days=max_years * 365)
            if head_dt < earliest_allowed:
                head_dt = earliest_allowed

    windows = compute_date_windows(head_dt, end_dt)
    console.print(
        f"    [dim]{ticker}: history {head_dt:%Y-%m-%d} → {end_dt:%Y-%m-%d}"
        f" ({len(windows)} window{'s' if len(windows) != 1 else ''})[/dim]"
    )

    async def _fetch_chunk(duration: str, end_str: str) -> list:
        async with semaphore:
            return await ib.get_historical_data_async(
                contract,
                duration=duration,
                bar_size="1 day",
                what_to_show="MIDPOINT" if asset_class in {"cmdty", "fx"} else "TRADES",
                end_date=end_str,
            )

    chunk_results = await asyncio.gather(
        *[_fetch_chunk(dur, end_str) for dur, end_str in windows]
    )

    # Flatten and deduplicate by date
    seen_dates: set[str] = set()
    all_bars: list = []
    for chunk in chunk_results:
        if chunk:
            for bar in chunk:
                date_key = str(bar.date)
                if date_key not in seen_dates:
                    seen_dates.add(date_key)
                    all_bars.append(bar)

    # Sort by date
    all_bars.sort(key=lambda b: str(b.date))
    elapsed = time.monotonic() - t0
    console.print(
        f"    [cyan]{ticker}[/cyan]: fetched {len(all_bars)} bars in {elapsed:.1f}s"
    )
    return (ticker, all_bars)


async def fetch_all_tickers(
    tickers: list[str], ib: IBClient, max_concurrent: int = 6,
    max_years: int = 0,
    end_dt_overrides: dict[str, datetime] | None = None,
    asset_class: str = "equity",
    exchange_map: dict[str, str] | None = None,
) -> dict[str, list]:
    """Fetch historical bars for all *tickers* concurrently.

    When *end_dt_overrides* is provided, each ticker uses its override as the
    end date (for backfill mode).

    Returns ``{ticker: bars}`` dict.  Per-ticker errors are logged and result
    in empty bar lists (the run continues for remaining tickers).
    """
    t0 = time.monotonic()
    mode_label = ", backfill" if end_dt_overrides else (f", {max_years}Y lookback" if max_years else ", inception")
    console.print(f"  [bold]Fetching {len(tickers)} tickers (max {max_concurrent} concurrent{mode_label})...[/bold]")
    semaphore = asyncio.Semaphore(max_concurrent)
    results: dict[str, list] = {}

    async def _safe_fetch(ticker: str) -> tuple[str, list]:
        try:
            edt = end_dt_overrides.get(ticker) if end_dt_overrides else None
            exch = (exchange_map or {}).get(ticker)
            return await fetch_ticker_bars(ticker, ib, semaphore, max_years=max_years, end_dt_override=edt, asset_class=asset_class, exchange=exch)
        except (IBError, Exception) as exc:
            console.print(f"    [red]{ticker}: {type(exc).__name__} — {exc}[/red]")
            return (ticker, [])

    gathered = await asyncio.gather(*[_safe_fetch(t) for t in tickers])
    for ticker, bars in gathered:
        results[ticker] = bars

    elapsed = time.monotonic() - t0
    ok = sum(1 for b in results.values() if b)
    fail = len(results) - ok
    console.print(
        f"  [bold]Fetch complete:[/bold] {ok} succeeded, {fail} failed/empty in {elapsed:.1f}s"
    )
    return results


# ── Per-ticker bronze ops ─────────────────────────────────────────────


def _bronze_parquet_path(ticker: str, bronze: BronzeClient) -> Path:
    base = getattr(bronze, "bronze_dir", BRONZE_DIR)
    if not isinstance(base, (str, Path)):
        base = BRONZE_DIR
    return Path(base) / f"symbol={ticker}" / "1d.parquet"


def _run_quality_detection(
    *,
    ticker: str,
    timeframe: str,
    asset_class: str,
    bars: list,
    parquet_path: Path,
    expected_start: date | None = None,
    ib_head_timestamp: date | None = None,
    source: str = "ib",
) -> None:
    """Run post-fetch quality detection and emit flags without blocking publish."""
    if not _QUALITY_ENABLED or not bars:
        return

    normalized = _normalize_bars_for_detection(bars)
    metadata = {
        "asset_class": asset_class,
        "ticker": ticker,
        "timeframe": timeframe,
        "source": source,
        "bars_received": len(bars),
        "errors_during_fetch": [],
        "expected_start": expected_start,
        "ib_head_timestamp": ib_head_timestamp,
    }
    try:
        flags = detect_all(bars=normalized, metadata=metadata, trading_calendar=None)
    except Exception as exc:  # pragma: no cover - detect_all wraps individual detectors
        console.print(f"  [yellow]quality detection raised: {exc}[/yellow]")
        return

    if not flags:
        return

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    write_sidecar(parquet_path, flags, metadata)
    for flag in flags:
        append_audit(
            flag,
            source=source,
            ticker=ticker,
            timeframe=timeframe,
            parquet_path=parquet_path,
        )
        alert_on_flag(flag, source=source, ticker=ticker)


def fetch_ticker(
    ticker: str,
    bars: list,
    bronze: BronzeClient,
    asset_class: str = "equity",
    *,
    expected_start: date | None = None,
    ib_head_timestamp: date | None = None,
) -> int:
    """Persist pre-fetched bars for *ticker* into bronze parquet."""
    if not bars:
        console.print(f"  [yellow]No bar data for {ticker}[/yellow]")
        return 0

    parquet_path = _bronze_parquet_path(ticker, bronze)
    _run_quality_detection(
        ticker=ticker,
        timeframe="1d",
        asset_class=asset_class,
        bars=bars,
        parquet_path=parquet_path,
        expected_start=expected_start,
        ib_head_timestamp=ib_head_timestamp,
    )

    symbol_id = bronze.get_symbol_id(ticker)
    if asset_class == "futures":
        root, expiry = ticker.rsplit("_", 1)
        expiry_date = f"{expiry[:4]}-{expiry[4:6]}-01"
        rows = bars_to_futures_rows(bars, symbol_id, root, expiry_date)
    elif asset_class in {"cmdty", "fx"}:
        rows = bars_to_midpoint_rows(bars, symbol_id, invert=asset_class == "fx" and _is_inverted_fx_pair(ticker))
    else:
        rows = bars_to_rows(bars, symbol_id)
    inserted = bronze.replace_ticker_rows(ticker, rows)
    if hasattr(bronze, "write_ticker_parquet"):
        bronze.write_ticker_parquet(ticker, symbol_id, BRONZE_DIR)
    return inserted


# ── Bronze helpers ────────────────────────────────────────────────────


def get_existing_symbols(bronze: BronzeClient) -> set[str]:
    """Return the set of ticker symbols that already have bronze data."""
    return bronze.get_existing_symbols()


def get_oldest_dates(bronze: BronzeClient) -> dict[str, str]:
    """Return ``{symbol: oldest_trade_date_str}`` for each ticker with data."""
    return bronze.get_oldest_dates()


def backfill_ticker(
    ticker: str,
    bars: list,
    bronze: BronzeClient,
    asset_class: str = "equity",
    *,
    expected_start: date | None = None,
    ib_head_timestamp: date | None = None,
) -> int:
    """Insert backfill bars for *ticker* without deleting existing data."""
    if not bars:
        console.print(f"  [yellow]No backfill data for {ticker}[/yellow]")
        return 0

    if expected_start is None:
        expected_start = IB_EARLIEST_DATE.date()
    parquet_path = _bronze_parquet_path(ticker, bronze)
    _run_quality_detection(
        ticker=ticker,
        timeframe="1d",
        asset_class=asset_class,
        bars=bars,
        parquet_path=parquet_path,
        expected_start=expected_start,
        ib_head_timestamp=ib_head_timestamp,
    )

    symbol_id = bronze.get_symbol_id(ticker)
    if asset_class == "futures":
        root, expiry = ticker.rsplit("_", 1)
        expiry_date = f"{expiry[:4]}-{expiry[4:6]}-01"
        rows = bars_to_futures_rows(bars, symbol_id, root, expiry_date)
    elif asset_class in {"cmdty", "fx"}:
        rows = bars_to_midpoint_rows(bars, symbol_id, invert=asset_class == "fx" and _is_inverted_fx_pair(ticker))
    else:
        rows = bars_to_rows(bars, symbol_id)
    inserted = bronze.merge_ticker_rows(ticker, rows)
    if hasattr(bronze, "write_ticker_parquet"):
        bronze.write_ticker_parquet(ticker, symbol_id, BRONZE_DIR)
    return inserted


# ── Main ──────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Fetch historical OHLCV from Interactive Brokers")
    ticker_group = parser.add_mutually_exclusive_group()
    ticker_group.add_argument(
        "--tickers",
        nargs="+",
        default=None,
        help=f"Tickers to fetch (default: {' '.join(MAG7)})",
    )
    ticker_group.add_argument(
        "--preset",
        type=str,
        default=None,
        help="Path to preset JSON file (reads .tickers array)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear existing cursor and start fresh",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip tickers that already have bronze parquet data",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=10,
        help="Max years of history to fetch (default: 10, 0=inception)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Tickers per async batch (default: 5)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("MDW_IB_HOST", "127.0.0.1"),
        help="IB Gateway host (default: $MDW_IB_HOST or 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MDW_IB_PORT", "4001")),
        help="IB Gateway port (default: $MDW_IB_PORT or 4001)",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=6,
        help="Max concurrent IB historical requests (default: 6)",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill mode: fetch only missing older data for tickers already in bronze parquet",
    )
    parser.add_argument(
        "--asset-class",
        choices=["equity", "volatility", "futures", "cmdty", "fx"],
        default="equity",
        help="Asset class to fetch (default: equity).",
    )
    parser.add_argument(
        "--no-quality",
        action="store_true",
        help="Disable the post-fetch quality detection hook (debug only).",
    )
    args = parser.parse_args()
    global _QUALITY_ENABLED
    _QUALITY_ENABLED = not args.no_quality

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    # ── Resolve tickers and cursor name ──────────────────────────────
    exchange_map: dict[str, str] = {}
    if args.preset:
        cursor_name, all_tickers, exchange_map = load_preset(args.preset)
        console.print(f"\n[bold]Preset:[/bold] {cursor_name} ({len(all_tickers)} tickers)")
    else:
        cursor_name = "custom"
        all_tickers = args.tickers if args.tickers else MAG7
        console.print(f"\n[bold]Tickers:[/bold] {' '.join(all_tickers)}")

    cursor_name_display = f"backfill_{cursor_name}" if args.backfill else cursor_name
    console.print(f"[bold]Cursor:[/bold]  {_cursor_path(cursor_name_display)}")
    years_label = f"{args.years}Y" if args.years else "inception"
    mode_label = "backfill" if args.backfill else "normal"
    console.print(
        f"[bold]Config:[/bold]  batch_size={args.batch_size}  max_concurrent={args.max_concurrent}"
        f"  host={args.host}  port={args.port}  years={years_label}  skip_existing={args.skip_existing}"
        f"  mode={mode_label}"
    )

    # ── Cursor management ────────────────────────────────────────────
    effective_cursor = f"backfill_{cursor_name}" if args.backfill else cursor_name

    if args.reset:
        clear_cursor(effective_cursor)
        console.print("[yellow]Cursor reset.[/yellow]")

    completed = load_cursor(effective_cursor)
    remaining = [t for t in all_tickers if not is_ticker_complete(completed, t, ("1d",))]

    n_completed = sum(1 for t in all_tickers if is_ticker_complete(completed, t, ("1d",)))
    console.print(
        f"\n[bold]{len(remaining)} of {len(all_tickers)} tickers remaining"
        f" ({n_completed} already completed via cursor)[/bold]"
    )

    if not remaining:
        console.print("[green bold]All tickers already completed. Use --reset to re-run.[/green bold]\n")
        return

    cursor_file = _cursor_path(effective_cursor)
    if completed and cursor_file.exists():
        with cursor_file.open() as f:
            started_at = json.load(f).get("started_at", datetime.now().isoformat())
    else:
        started_at = datetime.now().isoformat()

    # ── Live bronze publication ───────────────────────────────────────
    run_t0 = time.monotonic()
    asset_class = args.asset_class
    bronze_dir = DATA_LAKE / "bronze" / f"asset_class={asset_class}"

    with IBClient() as ib, _storage_client()(bronze_dir=bronze_dir, asset_class=asset_class) as bronze:
        ib.connect(host=args.host, port=args.port)

        if args.backfill:
            _run_backfill(args, ib, bronze, all_tickers, remaining, completed,
                          effective_cursor, started_at, asset_class=asset_class,
                          bronze_dir=bronze_dir, exchange_map=exchange_map)
        else:
            _run_normal(args, ib, bronze, all_tickers, remaining, completed,
                        effective_cursor, started_at, asset_class=asset_class,
                        bronze_dir=bronze_dir, exchange_map=exchange_map)

        run_elapsed = time.monotonic() - run_t0
        console.print(f"\n{'═' * 60}")
        console.print(f"[bold]Run elapsed:[/bold] {run_elapsed:.1f}s")

        # Summary
        summary = bronze.get_summary()
        if summary:
            console.print(f"\n[bold]Data summary ({len(summary)} symbols in bronze):[/bold]")
            for row in summary:
                console.print(
                    f"  {row['symbol']:6s}  {row['rows']:>6,d} rows  "
                    f"{row['earliest']} → {row['latest']}"
                )

    console.print("\n[green bold]Done.[/green bold]\n")


def _run_backfill(args, ib, bronze, all_tickers, remaining, completed,
                  cursor_name, started_at, *, asset_class="equity", bronze_dir=None,
                  exchange_map=None):
    """Backfill mode: fetch only missing older data for tickers already in bronze."""
    oldest_dates = get_oldest_dates(bronze)

    # Filter to tickers that already have bronze snapshots.
    backfill_tickers = [t for t in remaining if t in oldest_dates]
    skipped_new = [t for t in remaining if t not in oldest_dates]
    if skipped_new:
        console.print(
            f"[cyan]Skipping {len(skipped_new)} tickers not yet in bronze"
            f" (use normal fetch first):[/cyan] [dim]{' '.join(skipped_new)}[/dim]"
        )

    if not backfill_tickers:
        console.print("[green bold]No tickers to backfill.[/green bold]")
        return

    # Build end_dt_overrides: each ticker's oldest existing date
    end_dt_overrides: dict[str, datetime] = {}
    for ticker in backfill_tickers:
        end_dt_overrides[ticker] = datetime.strptime(oldest_dates[ticker], "%Y-%m-%d")

    console.print(f"[bold]{len(backfill_tickers)} tickers to backfill[/bold]")

    # Batch processing
    batches = [backfill_tickers[i:i + args.batch_size]
               for i in range(0, len(backfill_tickers), args.batch_size)]

    total_rows = 0
    total_ok = 0
    total_fail = 0

    for batch_idx, batch in enumerate(batches):
        batch_t0 = time.monotonic()
        console.print(
            f"\n{'─' * 60}\n"
            f"[bold]Backfill batch {batch_idx + 1}/{len(batches)}"
            f" ({len(batch)} tickers)[/bold]  "
            f"[dim]{' '.join(batch)}[/dim]"
        )

        batch_overrides = {t: end_dt_overrides[t] for t in batch}
        ticker_bars = ib.ib.run(
            fetch_all_tickers(batch, ib, max_concurrent=args.max_concurrent,
                              end_dt_overrides=batch_overrides,
                              asset_class=asset_class,
                              exchange_map=exchange_map)
        )

        batch_rows = 0
        batch_ok = 0
        batch_fail = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Backfilling...", total=len(batch))

            for ticker in batch:
                progress.update(task, description=f"Backfilling {ticker}...")
                bars = ticker_bars.get(ticker, [])
                count = backfill_ticker(ticker, bars, bronze, asset_class=asset_class)

                if count > 0:
                    mark_timeframe_done(completed, ticker, "1d")
                    save_cursor(cursor_name, completed, started_at)
                    console.print(f"  [green]{ticker}[/green]: {count:,} backfill rows inserted")
                    batch_ok += 1
                else:
                    console.print(f"  [yellow]{ticker}[/yellow]: 0 backfill rows (will retry next run)")
                    batch_fail += 1

                batch_rows += count
                progress.advance(task)

        batch_elapsed = time.monotonic() - batch_t0
        total_rows += batch_rows
        total_ok += batch_ok
        total_fail += batch_fail
        n_done = sum(1 for t in all_tickers if is_ticker_complete(completed, t, ("1d",)))
        console.print(
            f"\n  [bold]Batch {batch_idx + 1} done:[/bold] "
            f"{batch_ok} ok, {batch_fail} failed, "
            f"{batch_rows:,} rows in {batch_elapsed:.1f}s  "
            f"[dim]({n_done}/{len(all_tickers)} total completed)[/dim]"
        )

    n_done = sum(1 for t in all_tickers if is_ticker_complete(completed, t, ("1d",)))
    console.print(
        f"\n[bold]Backfill complete:[/bold] {total_ok} ok, {total_fail} failed, "
        f"{total_rows:,} rows"
    )
    console.print(f"[bold]Cursor:[/bold] {n_done}/{len(all_tickers)} tickers saved")


def _run_normal(args, ib, bronze, all_tickers, remaining, completed,
                cursor_name, started_at, *, asset_class="equity", bronze_dir=None,
                exchange_map=None):
    """Normal fetch mode: replace the per-ticker bronze snapshot."""
    if args.skip_existing:
        existing = get_existing_symbols(bronze)
        skipped = [t for t in remaining if t in existing]
        if skipped:
            for t in skipped:
                mark_timeframe_done(completed, t, "1d")
            save_cursor(cursor_name, completed, started_at)
            remaining = [t for t in remaining if t not in existing]
            console.print(
                f"[cyan]Skipped {len(skipped)} tickers already in bronze:[/cyan] "
                f"[dim]{' '.join(skipped)}[/dim]"
            )
        console.print(f"[bold]{len(remaining)} tickers to fetch after skip-existing[/bold]")

    if not remaining:
        console.print(
            "[green bold]All tickers already in bronze. Use --reset to re-run.[/green bold]\n"
        )
        return

    batches = [remaining[i:i + args.batch_size] for i in range(0, len(remaining), args.batch_size)]

    total_rows = 0
    total_ok = 0
    total_fail = 0

    for batch_idx, batch in enumerate(batches):
        batch_t0 = time.monotonic()
        console.print(
            f"\n{'─' * 60}\n"
            f"[bold]Batch {batch_idx + 1}/{len(batches)}"
            f" ({len(batch)} tickers)[/bold]  "
            f"[dim]{' '.join(batch)}[/dim]"
        )

        ticker_bars = ib.ib.run(
            fetch_all_tickers(batch, ib, max_concurrent=args.max_concurrent, max_years=args.years,
                              asset_class=asset_class, exchange_map=exchange_map)
        )

        batch_rows = 0
        batch_ok = 0
        batch_fail = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Inserting...", total=len(batch))

            for ticker in batch:
                progress.update(task, description=f"Inserting {ticker}...")
                bars = ticker_bars.get(ticker, [])
                count = fetch_ticker(ticker, bars, bronze, asset_class=asset_class)

                if count > 0:
                    mark_timeframe_done(completed, ticker, "1d")
                    save_cursor(cursor_name, completed, started_at)
                    console.print(f"  [green]{ticker}[/green]: {count:,} rows inserted")
                    batch_ok += 1
                else:
                    console.print(f"  [yellow]{ticker}[/yellow]: 0 rows (will retry next run)")
                    batch_fail += 1

                batch_rows += count
                progress.advance(task)

        batch_elapsed = time.monotonic() - batch_t0
        total_rows += batch_rows
        total_ok += batch_ok
        total_fail += batch_fail
        n_done = sum(1 for t in all_tickers if is_ticker_complete(completed, t, ("1d",)))
        console.print(
            f"\n  [bold]Batch {batch_idx + 1} done:[/bold] "
            f"{batch_ok} ok, {batch_fail} failed, "
            f"{batch_rows:,} rows in {batch_elapsed:.1f}s  "
            f"[dim]({n_done}/{len(all_tickers)} total completed)[/dim]"
        )

    n_done = sum(1 for t in all_tickers if is_ticker_complete(completed, t, ("1d",)))
    console.print(
        f"\n[bold]Run complete:[/bold] {total_ok} ok, {total_fail} failed, "
        f"{total_rows:,} rows"
    )
    console.print(f"[bold]Cursor:[/bold] {n_done}/{len(all_tickers)} tickers saved")


if __name__ == "__main__":
    main()

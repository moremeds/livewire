"""Shared ingestion helpers — contract creation, bar conversion, preset loading.

Extracted from daily_update and fetch_ib_historical to eliminate duplication.
"""

from __future__ import annotations

import json
from pathlib import Path

from ib_async import Contract, Forex, Future, Index, Stock

ROOT_EXCHANGE_MAP = {
    "ES": "CME",
    "NQ": "CME",
    "RTY": "CME",
    "YM": "CBOT",
    "ZB": "CBOT",
    "ZN": "CBOT",
    "ZF": "CBOT",
    "CL": "NYMEX",
    "NG": "NYMEX",
    "GC": "COMEX",
    "SI": "COMEX",
}

SUPPORTED_IB_FX_PAIRS = {
    "EURUSD",
    "GBPUSD",
    "AUDUSD",
    "NZDUSD",
    "USDJPY",
    "USDCHF",
    "USDCAD",
    "USDHKD",
    "USDSGD",
    "USDSEK",
    "USDNOK",
    "USDDKK",
    "USDCNH",
    "USDMXN",
    "USDZAR",
    "EURGBP",
    "EURJPY",
    "EURCHF",
    "EURCAD",
    "EURAUD",
    "EURNZD",
    "GBPJPY",
    "GBPCHF",
    "GBPCAD",
    "GBPAUD",
    "GBPNZD",
    "AUDJPY",
    "AUDCHF",
    "AUDCAD",
    "AUDNZD",
    "NZDJPY",
    "NZDCHF",
    "NZDCAD",
    "CADJPY",
    "CADCHF",
    "CHFJPY",
}


def resolve_fx_pair(ticker: str) -> tuple[str, bool]:
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


def is_inverted_fx_pair(ticker: str) -> bool:
    """Return True when local FX rows must invert the source pair."""
    return resolve_fx_pair(ticker)[1]


def make_contract(
    ticker: str, asset_class: str = "equity", exchange: str | None = None
) -> Contract:
    """Build an IB contract for the given *ticker* and *asset_class*."""
    if asset_class == "futures":
        root, expiry = ticker.rsplit("_", 1)
        exch = exchange or ROOT_EXCHANGE_MAP.get(root, "CME")
        return Future(root, expiry, exch, "USD")
    if asset_class == "cmdty":
        return Contract(
            secType="CMDTY",
            symbol=ticker.upper(),
            exchange=exchange or "SMART",
            currency="USD",
        )
    if asset_class == "fx":
        source_pair, _ = resolve_fx_pair(ticker)
        return Forex(source_pair)
    if asset_class == "volatility":
        return Index(ticker, exchange or "CBOE", "USD")
    return Stock(ticker, "SMART", "USD")


def bars_to_rows(bars: list, symbol_id: int) -> list[dict]:
    """Convert IB BarData objects to bronze row dicts."""
    return [
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
        for bar in bars
    ]


def bars_to_futures_rows(
    bars: list,
    contract_id: int,
    root_symbol: str,
    expiry_date: str,
) -> list[dict]:
    """Convert IB BarData objects to futures bronze row dicts."""
    return [
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
        for bar in bars
    ]


def bars_to_midpoint_rows(
    bars: list, symbol_id: int, *, invert: bool = False
) -> list[dict]:
    """Convert IB midpoint bars to daily bronze rows."""
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


def load_preset(path: str | Path) -> tuple[str, list[str], dict[str, str]]:
    """Read a preset JSON file and return ``(name, tickers, exchange_map)``.

    Standard presets use a ``tickers`` array.  Futures presets use a
    ``contracts`` array of ``{root, exchange, expiry}`` dicts which are
    flattened into composite ticker strings (``ES_202506``) and an exchange
    map so ``make_contract`` can resolve the correct venue.
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

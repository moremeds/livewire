"""Historical data provider abstraction.

Defines a clean interface for fetching IB historical data:

- IBProvider: direct IB Gateway connection via ib_async

Usage:
    provider = IBProvider(host, port)
    bars = await provider.get_historical_bars(spec, duration="1 Y")
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger("livewire.historical_provider")


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class BarRecord:
    """OHLCV bar record. Date is ISO format: YYYY-MM-DD for daily bars."""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int


# ---------------------------------------------------------------------------
# Contract spec helpers
# ---------------------------------------------------------------------------

def ib_contract_to_spec(contract) -> dict:
    """Convert an ib_async contract to a JSON-safe spec dict."""
    spec = {
        "sec_type": contract.secType or "STK",
        "symbol": contract.symbol,
        "exchange": contract.exchange or "SMART",
        "currency": contract.currency or "USD",
    }
    ltd = getattr(contract, "lastTradeDateOrContractMonth", "")
    if ltd:
        spec["last_trade_date"] = ltd
    return spec


def spec_to_ib_contract(spec: dict):
    """Convert a spec dict to an ib_async contract."""
    from ib_async import Stock, Future, Index

    sec_type = spec.get("sec_type", "STK")
    symbol = spec["symbol"]
    exchange = spec.get("exchange", "SMART")
    currency = spec.get("currency", "USD")

    if sec_type == "STK":
        return Stock(symbol, exchange, currency)
    elif sec_type == "FUT":
        return Future(symbol, spec.get("last_trade_date", ""), exchange, currency)
    elif sec_type == "IND":
        return Index(symbol, exchange, currency)
    raise ValueError(f"Unsupported sec_type: {sec_type}")


# ---------------------------------------------------------------------------
# Abstract interface
# ---------------------------------------------------------------------------

class HistoricalProvider(ABC):
    """Interface for fetching IB historical data."""

    @abstractmethod
    async def qualify_contract(self, contract_spec: dict) -> dict:
        """Qualify a contract. Returns dict with conId and other fields."""

    @abstractmethod
    async def get_head_timestamp(
        self, contract_spec: dict, what_to_show: str = "TRADES", use_rth: bool = True
    ) -> Optional[str]:
        """Get earliest available data date. Returns ISO datetime string or None."""

    @abstractmethod
    async def get_historical_bars(
        self,
        contract_spec: dict,
        end_date_time: str = "",
        duration: str = "1 D",
        bar_size: str = "1 day",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> List[BarRecord]:
        """Fetch historical OHLCV bars. Returns list of BarRecord with ISO dates."""

    @abstractmethod
    async def disconnect(self):
        """Clean up resources."""


# ---------------------------------------------------------------------------
# IBProvider — direct IB Gateway connection
# ---------------------------------------------------------------------------

class IBProvider(HistoricalProvider):
    """Fetches historical data via direct IB Gateway connection."""

    def __init__(self, host: str = "127.0.0.1", port: int = 4001):
        from clients.ib_client import IBClient
        self._client = IBClient()
        self._client.connect(host, port)
        self._host = host
        self._port = port

    async def qualify_contract(self, contract_spec: dict) -> dict:
        contract = spec_to_ib_contract(contract_spec)
        qualified = await asyncio.to_thread(
            self._client.qualify_contracts, contract
        )
        if qualified:
            c = qualified[0] if isinstance(qualified, list) else contract
            return {
                "conId": c.conId,
                "symbol": c.symbol,
                "secType": c.secType,
                "exchange": c.exchange,
                "currency": c.currency,
            }
        return contract_spec

    async def get_head_timestamp(self, contract_spec, what_to_show="TRADES", use_rth=True):
        contract = spec_to_ib_contract(contract_spec)
        await asyncio.to_thread(self._client.qualify_contracts, contract)
        ts = await self._client.get_head_timestamp_async(
            contract, what_to_show=what_to_show, use_rth=use_rth
        )
        if not ts:
            return None
        return str(ts)

    async def get_historical_bars(
        self, contract_spec, end_date_time="", duration="1 D",
        bar_size="1 day", what_to_show="TRADES", use_rth=True,
    ):
        contract = spec_to_ib_contract(contract_spec)
        await asyncio.to_thread(self._client.qualify_contracts, contract)
        bars = await self._client.get_historical_data_async(
            contract,
            end_date_time=end_date_time,
            duration=duration,
            bar_size=bar_size,
            what_to_show=what_to_show,
            use_rth=use_rth,
        )
        return [
            BarRecord(
                date=str(bar.date)[:10],  # Normalize to YYYY-MM-DD
                open=float(bar.open),
                high=float(bar.high),
                low=float(bar.low),
                close=float(bar.close),
                volume=int(bar.volume),
            )
            for bar in (bars or [])
        ]

    async def disconnect(self):
        await asyncio.to_thread(self._client.disconnect)

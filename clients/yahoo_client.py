"""Read-only Yahoo Finance chart client — an authoritative split-adjusted reference.

Yahoo's `close` column is **split-adjusted** (not raw): a split boundary is smooth,
not a jump. `adjclose` is split+dividend adjusted. The `events.splits` block carries
each split's exact ratio. So the true raw close of a legacy `price_basis='unknown'`
row is recoverable deterministically:

    raw_t = yahoo_close_t * product(numerator/denominator for splits with ex_date > t)

This dissolves the ambiguity of inferring basis from a single series' price step
(``clients.price_basis.classify_split_events``): two same-date series divided cancel
the real market move and leave only the adjustment. Validated penny-perfect against
Massive raw across AMC's 2023 reverse split.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time

import requests

_DEFAULT_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
# Yahoo rejects the default python-requests agent; a browser-ish UA is required.
_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


class YahooError(Exception):
    """Any Yahoo chart fetch or parse failure."""


class YahooNotFound(YahooError):
    """Yahoo has no chart for this symbol (delisted, wrong ticker)."""


@dataclass(frozen=True)
class YahooBar:
    trade_date: date
    close: float  # split-adjusted (Yahoo convention)
    adj_close: float  # split + dividend adjusted


@dataclass(frozen=True)
class YahooSplit:
    ex_date: date
    numerator: float
    denominator: float

    @property
    def price_multiplier(self) -> float:
        """Factor applied to a PRE-split raw close to reach it from Yahoo's split-adjusted
        close. Forward 2:1 → 2.0 (raw was higher); reverse 1:10 → 0.1 (raw was lower)."""
        return self.numerator / self.denominator


def _epoch(day: date) -> int:
    return int(datetime.combine(day, time.min, tzinfo=UTC).timestamp())


class YahooClient:
    """Synchronous read-only chart fetcher. No auth, no writes."""

    def __init__(self, base_url: str = _DEFAULT_BASE_URL, *, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": _USER_AGENT})

    def get_daily(self, symbol: str, start: date, end: date) -> tuple[list[YahooBar], list[YahooSplit]]:
        """Return (split-adjusted daily bars, splits) for ``symbol`` over ``[start, end]``."""
        params = {
            "period1": _epoch(start),
            "period2": _epoch(end) + 86_400,  # inclusive of `end`
            "interval": "1d",
            "events": "split",
            "includeAdjustedClose": "true",
        }
        try:
            response = self._session.get(f"{self._base_url}/{symbol.upper()}", params=params, timeout=self._timeout)
        except requests.RequestException as exc:
            raise YahooError(f"yahoo request failed for {symbol}: {exc}") from exc
        if response.status_code == 404:
            raise YahooNotFound(symbol.upper())
        if response.status_code != 200:
            raise YahooError(f"yahoo returned HTTP {response.status_code} for {symbol}")
        payload = response.json()
        chart = payload.get("chart") or {}
        if chart.get("error"):
            code = (chart["error"] or {}).get("code", "error")
            if code == "Not Found":
                raise YahooNotFound(symbol.upper())
            raise YahooError(f"yahoo chart error for {symbol}: {chart['error']}")
        results = chart.get("result") or []
        if not results:
            raise YahooNotFound(symbol.upper())
        result = results[0]
        timestamps = result.get("timestamp") or []
        indicators = result.get("indicators") or {}
        quote = (indicators.get("quote") or [{}])[0]
        closes = quote.get("close") or []
        adj_series = indicators.get("adjclose") or [{}]
        adj_closes = (adj_series[0] or {}).get("adjclose") or []
        bars: list[YahooBar] = []
        for index, ts in enumerate(timestamps):
            close = closes[index] if index < len(closes) else None
            if close is None:  # Yahoo pads holidays/halts with null — skip, don't fabricate
                continue
            adj = adj_closes[index] if index < len(adj_closes) and adj_closes[index] is not None else close
            bars.append(YahooBar(datetime.fromtimestamp(ts, tz=UTC).date(), float(close), float(adj)))
        splits: list[YahooSplit] = []
        for event in ((result.get("events") or {}).get("splits") or {}).values():
            splits.append(
                YahooSplit(
                    datetime.fromtimestamp(event["date"], tz=UTC).date(),
                    float(event["numerator"]),
                    float(event["denominator"]),
                )
            )
        bars.sort(key=lambda bar: bar.trade_date)
        splits.sort(key=lambda split: split.ex_date)
        return bars, splits

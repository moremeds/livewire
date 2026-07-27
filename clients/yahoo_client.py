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
class YahooOHLCV:
    """A full OHLCV bar. ``timestamp`` is tz-aware UTC; daily callers take ``.date()``."""

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


# Yahoo serves each intraday interval over a different maximum window. These are the
# widest `range=` values that return data, measured 2026-07-27 against DX-Y.NYB,
# EURUSD=X and USDKRW=X. All are rolling: history is accumulated, not fetched once.
YAHOO_INTRADAY_RANGE = {"1m": "7d", "5m": "60d", "30m": "60d", "1h": "730d"}


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

    def _chart(self, symbol: str, params: dict) -> dict:
        """Fetch one chart payload and return its `result` block."""
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
        return results[0]

    @staticmethod
    def _ohlcv(result: dict) -> list[YahooOHLCV]:
        """Build OHLCV bars from a chart result, skipping Yahoo's null padding."""
        quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
        series = {field: quote.get(field) or [] for field in ("open", "high", "low", "close", "volume")}
        bars: list[YahooOHLCV] = []
        for index, ts in enumerate(result.get("timestamp") or []):
            values = {}
            for field, column in series.items():
                values[field] = column[index] if index < len(column) else None
            # Yahoo pads holidays, halts and pre-open minutes with nulls. A bar missing
            # any price is skipped rather than back-filled — never fabricate a price.
            if any(values[field] is None for field in ("open", "high", "low", "close")):
                continue
            bars.append(
                YahooOHLCV(
                    timestamp=datetime.fromtimestamp(ts, tz=UTC),
                    open=float(values["open"]),
                    high=float(values["high"]),
                    low=float(values["low"]),
                    close=float(values["close"]),
                    # FX and index quotes carry no volume; Yahoo sends null.
                    volume=int(values["volume"] or 0),
                )
            )
        bars.sort(key=lambda bar: bar.timestamp)
        return bars

    def get_daily_ohlcv(self, symbol: str, start: date | None = None, end: date | None = None) -> list[YahooOHLCV]:
        """Return full OHLCV daily bars for ``symbol``.

        ``start=None`` requests the full available history. It uses ``period1=0`` rather
        than ``range=max`` deliberately: ``range=max`` silently downsamples granularity
        (DX-Y.NYB returned 168 rows for its 41-year span, versus 17,219 with period1=0).
        """
        params = {
            "period1": _epoch(start) if start else 0,
            "period2": (_epoch(end) + 86_400) if end else int(datetime.now(tz=UTC).timestamp()),
            "interval": "1d",
        }
        return self._ohlcv(self._chart(symbol, params))

    def get_intraday(self, symbol: str, interval: str) -> list[YahooOHLCV]:
        """Return intraday OHLCV bars for ``symbol`` over Yahoo's widest window.

        Intraday **must** use ``range=``. Passing ``period1``/``period2`` with an intraday
        interval makes Yahoo reject the request with "Unprocessable Entity".
        """
        if interval not in YAHOO_INTRADAY_RANGE:
            raise ValueError(f"unsupported interval: {interval!r}. Must be one of {sorted(YAHOO_INTRADAY_RANGE)}")
        params = {"range": YAHOO_INTRADAY_RANGE[interval], "interval": interval}
        return self._ohlcv(self._chart(symbol, params))

    def get_daily(self, symbol: str, start: date, end: date) -> tuple[list[YahooBar], list[YahooSplit]]:
        """Return (split-adjusted daily bars, splits) for ``symbol`` over ``[start, end]``."""
        result = self._chart(
            symbol,
            {
                "period1": _epoch(start),
                "period2": _epoch(end) + 86_400,  # inclusive of `end`
                "interval": "1d",
                "events": "split",
                "includeAdjustedClose": "true",
            },
        )
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

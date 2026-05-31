"""Universe data client — fetches live index constituents and ticker status.

Sources:
- S&P 500, Nasdaq-100: Wikipedia constituent tables
- Russell 2000: Slickcharts
- Ticker status (active/delisted): Polygon /v3/reference/tickers/{ticker}
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import requests
from lxml import html

log = logging.getLogger(__name__)

_TIMEOUT = 30
_USER_AGENT = "livewire/1.0 (market-data-warehouse)"

_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_NDX100_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"
_R2K_URL = "https://www.slickcharts.com/russell2000"

_POLYGON_BASE = "https://api.polygon.io"


class UniverseFetchError(Exception):
    """Failed to fetch index constituent data."""


@dataclass(frozen=True)
class TickerStatus:
    ticker: str
    active: bool
    delisted_utc: str | None = None
    name: str | None = None
    type: str | None = None
    market: str | None = None
    list_date: str | None = None


_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _get_html(url: str, label: str, browser_ua: bool = False) -> html.HtmlElement:
    headers = {"User-Agent": _BROWSER_UA if browser_ua else _USER_AGENT}
    try:
        resp = requests.get(url, headers=headers, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise UniverseFetchError(f"Failed to fetch {label}: {exc}") from exc
    return html.fromstring(resp.content)


def fetch_sp500() -> set[str]:
    tree = _get_html(_SP500_URL, "S&P 500")
    table = tree.cssselect("table#constituents")
    if not table:
        tables = tree.cssselect("table.wikitable")
        if not tables:
            raise UniverseFetchError("S&P 500: no constituent table found")
        table = [tables[0]]
    rows = table[0].cssselect("tbody tr")
    symbols: set[str] = set()
    for row in rows:
        cells = row.cssselect("td")
        if cells:
            text = cells[0].text_content().strip()
            if text:
                symbols.add(text)
    return symbols


def fetch_ndx100() -> set[str]:
    tree = _get_html(_NDX100_URL, "Nasdaq-100")
    table = tree.cssselect("table#constituents")
    if not table:
        tables = tree.cssselect("table.wikitable")
        if not tables:
            raise UniverseFetchError("Nasdaq-100: no constituent table found")
        table = [tables[0]]
    rows = table[0].cssselect("tbody tr")
    symbols: set[str] = set()
    for row in rows:
        cells = row.cssselect("td")
        if cells:
            text = cells[0].text_content().strip()
            if text:
                symbols.add(text)
    return symbols


def fetch_r2k() -> set[str]:
    tree = _get_html(_R2K_URL, "Russell 2000", browser_ua=True)
    tables = tree.cssselect("table.table")
    if not tables:
        raise UniverseFetchError("Russell 2000: no constituent table found")
    rows = tables[0].cssselect("tbody tr")
    symbols: set[str] = set()
    for row in rows:
        cells = row.cssselect("td")
        if len(cells) >= 3:
            text = cells[2].text_content().strip()
            if text:
                symbols.add(text)
    return symbols


def check_ticker_status(
    ticker: str,
    api_key: str | None = None,
) -> TickerStatus:
    key = api_key or os.environ.get("MASSIVE_API_KEY")
    if not key:
        raise UniverseFetchError("MASSIVE_API_KEY required for ticker status check")
    url = f"{_POLYGON_BASE}/v3/reference/tickers/{ticker.upper()}"
    try:
        resp = requests.get(url, params={"apiKey": key}, timeout=_TIMEOUT)
    except requests.RequestException as exc:
        raise UniverseFetchError(f"Polygon status check failed for {ticker}: {exc}") from exc
    if resp.status_code == 404:
        return TickerStatus(ticker=ticker.upper(), active=False)
    try:
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise UniverseFetchError(f"Polygon status check failed for {ticker}: {exc}") from exc
    data = resp.json().get("results", {})
    return TickerStatus(
        ticker=ticker.upper(),
        active=data.get("active", False),
        delisted_utc=data.get("delisted_utc"),
        name=data.get("name"),
        type=data.get("type"),
        market=data.get("market"),
        list_date=data.get("list_date"),
    )


_POLYGON_THROTTLE_SECONDS = 0.25


def check_tickers_bulk(
    tickers: list[str],
    api_key: str | None = None,
    throttle: float = _POLYGON_THROTTLE_SECONDS,
) -> dict[str, TickerStatus]:
    import time

    results: dict[str, TickerStatus] = {}
    for i, ticker in enumerate(tickers):
        try:
            results[ticker] = check_ticker_status(ticker, api_key=api_key)
        except UniverseFetchError:
            log.warning("Could not check status for %s, skipping", ticker)
        if i < len(tickers) - 1:
            time.sleep(throttle)
    return results

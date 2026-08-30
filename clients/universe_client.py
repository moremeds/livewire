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
from pathlib import Path

import requests
from lxml import html

from clients.mediawiki_client import MediaWikiClient, MediaWikiFetchError
from clients.source_evidence import SourceEvidenceStore

log = logging.getLogger(__name__)

_TIMEOUT = 30
_USER_AGENT = "livewire/1.0 (market-data-warehouse)"

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


def _data_lake_root() -> Path:
    return Path(os.environ.get("MDW_DATA_LAKE", Path.home() / "market-warehouse" / "data-lake")).expanduser()


def parse_constituent_table(content: str, label: str) -> set[str]:
    tree = html.fromstring(content)
    candidates = [*tree.cssselect("table#constituents"), *tree.cssselect("table.wikitable")]
    seen: set[int] = set()
    for table in candidates:
        if id(table) in seen:
            continue
        seen.add(id(table))
        first_row = table.cssselect("tr")[:1]
        if not first_row:
            continue
        headers = [" ".join(cell.text_content().split()).casefold() for cell in first_row[0].cssselect("th")]
        symbol_columns = [index for index, value in enumerate(headers) if value in {"symbol", "ticker"}]
        has_company = any(value in {"security", "company"} for value in headers)
        if len(symbol_columns) != 1 or not has_company:
            continue
        symbol_column = symbol_columns[0]
        symbols = {
            cells[symbol_column].text_content().strip()
            for row in table.cssselect("tbody tr")
            if len(cells := row.cssselect("td")) > symbol_column and cells[symbol_column].text_content().strip()
        }
        if symbols:
            return symbols
        raise UniverseFetchError(f"{label}: constituent table is empty")
    raise UniverseFetchError(f"{label}: no constituent table found")


_parse_constituent_table = parse_constituent_table


def _fetch_wikipedia_universe(title: str, label: str) -> set[str]:
    try:
        snapshot = MediaWikiClient(SourceEvidenceStore(_data_lake_root()), timeout=_TIMEOUT).snapshot(title)
    except MediaWikiFetchError as exc:
        raise UniverseFetchError(f"Failed to fetch {label}: {exc}") from exc
    return parse_constituent_table(snapshot.content, label)


def fetch_sp500() -> set[str]:
    """Fetch current S&P 500 members from one revision-bound snapshot."""

    return _fetch_wikipedia_universe("List of S&P 500 companies", "S&P 500")


def fetch_ndx100() -> set[str]:
    """Fetch current Nasdaq-100 members from one revision-bound snapshot."""

    return _fetch_wikipedia_universe("Nasdaq-100", "Nasdaq-100")


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

"""Universe data client — fetches live index constituents and ticker status.

Sources:
- S&P 500, Nasdaq-100: Wikipedia constituent tables
- Russell 2000, DJIA: Slickcharts
- Ticker status (active/delisted): Polygon /v3/reference/tickers/{ticker}
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path

import requests
from lxml import html

from clients.mediawiki_client import MediaWikiClient, MediaWikiFetchError
from clients.source_evidence import SourceEvidenceStore

log = logging.getLogger(__name__)

_TIMEOUT = 30
_USER_AGENT = "livewire/1.0 (market-data-warehouse)"

R2K_SLICKCHARTS_URL = "https://www.slickcharts.com/russell2000"
DJIA_SLICKCHARTS_URL = "https://www.slickcharts.com/dowjones"

_POLYGON_BASE = "https://api.polygon.io"


class UniverseFetchError(Exception):
    """Failed to fetch index constituent data.

    `status_code` is the provider's HTTP status when the failure was one (a 429
    is a pacing decision for the caller, a 5xx is a fetch failure); it is None
    for a transport error or a missing key.
    """

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


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


# The list lives in its own article, the same shape as the S&P 500 one above.
# The `Nasdaq-100` article itself no longer carries a Components section --
# measured 2026-09-02, its four wikitables are annual returns and closing
# milestones -- so parse_constituent_table found nothing and universe-sync
# exited 1 on every run. Note the capitalisation: `List of Nasdaq-100 companies`
# is a 404; only the all-caps form resolves.
NDX100_WIKIPEDIA_TITLE = "List of NASDAQ-100 companies"


def fetch_ndx100() -> set[str]:
    """Fetch current Nasdaq-100 members from one revision-bound snapshot."""

    return _fetch_wikipedia_universe(NDX100_WIKIPEDIA_TITLE, "Nasdaq-100")


def _slickcharts_constituents(tree: html.HtmlElement, label: str) -> set[str]:
    """The shared Slickcharts constituents table: `table.table`, symbol in td[2]."""
    tables = tree.cssselect("table.table")
    if not tables:
        raise UniverseFetchError(f"{label}: no constituent table found")
    return {
        cells[2].text_content().strip()
        for row in tables[0].cssselect("tbody tr")
        if len(cells := row.cssselect("td")) >= 3 and cells[2].text_content().strip()
    }


_DJIA_MIN_CONSTITUENTS = 30


def fetch_djia() -> set[str]:
    """Fetch current DJIA members from Slickcharts.

    The Wikipedia DJIA article dropped its components table (measured
    2026-09-13: only annual returns + a navbox remain), so the source is
    Slickcharts — approved deviation in
    docs/superpowers/plans/2026-09-13-dividend-fx-and-pit-membership.evidence.md.
    The index has exactly 30 constituents; a partial parse (<30 rows) means the
    table markup broke, so it raises rather than return a truncated set.
    """
    members = _slickcharts_constituents(_get_html(DJIA_SLICKCHARTS_URL, "DJIA", browser_ua=True), "DJIA")
    if len(members) < _DJIA_MIN_CONSTITUENTS:
        raise UniverseFetchError(f"DJIA: {len(members)} constituents parsed, below {_DJIA_MIN_CONSTITUENTS}")
    return members


def fetch_r2k() -> set[str]:
    return _slickcharts_constituents(_get_html(R2K_SLICKCHARTS_URL, "Russell 2000", browser_ua=True), "Russell 2000")


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


_REFERENCE_URL = f"{_POLYGON_BASE}/v3/reference/tickers"


@dataclass(frozen=True)
class IdentityRecord:
    """One Massive `/v3/reference/tickers` listing row, field-for-field.

    `existed_at` is the `date=` probe date when the record came back from that
    probe and from nowhere else — the only proof of a start date this provider
    gives for a record with no `list_date`.
    """

    ticker: str
    name: str | None
    cik: str | None
    composite_figi: str | None
    share_class_figi: str | None
    mic: str | None
    currency: str | None
    list_date: str | None
    delisted_utc: str | None
    existed_at: str | None


@dataclass(frozen=True)
class IdentityRecords:
    """The exact response bytes and the parsed records of one identity fetch.

    The bytes are returned untouched so the caller commits them to the evidence
    CAS; nothing here writes.
    """

    responses: list[bytes]
    records: list[IdentityRecord]
    # `date=` probes that returned no record, so the caller can persist that
    # the date was asked and not repeat the probe on the next run.
    empty_probes: tuple[str, ...] = ()


def _reference_page(ticker: str, key: str, params: dict[str, str]) -> tuple[bytes, list[dict]]:
    """One `/v3/reference/tickers` list call: exact bytes plus its results.

    A transport failure or a 5xx raises; a 404 or an empty `results` is data,
    never an exception — the twin of the `fetch_batch` rule, so a provider
    outage can never read as "this ticker does not exist". Emptiness is read
    from `results` only: an empty envelope carries no `count` key at all.
    """
    try:
        resp = requests.get(
            _REFERENCE_URL,
            params={"ticker": ticker.upper(), **params, "apiKey": key},
            timeout=_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise UniverseFetchError(f"Massive reference lookup failed for {ticker}: {exc}") from exc
    if resp.status_code == 404:
        return resp.content, []
    try:
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise UniverseFetchError(
            f"Massive reference lookup failed for {ticker}: {exc}",
            status_code=resp.status_code,
        ) from exc
    return resp.content, list(resp.json().get("results") or [])


def _identity_record(row: dict, ticker: str, existed_at: str | None) -> IdentityRecord:
    currency = row.get("currency_name")
    return IdentityRecord(
        ticker=(row.get("ticker") or ticker).upper(),
        name=row.get("name"),
        cik=row.get("cik"),
        composite_figi=row.get("composite_figi"),
        share_class_figi=row.get("share_class_figi"),
        # primary_exchange is already a MIC in Massive's payload (XNAS, XNYS,
        # ARCX, XASE, BATS) — used as-is, never remapped.
        mic=row.get("primary_exchange"),
        currency=None if currency is None else currency.upper(),
        list_date=row.get("list_date"),
        delisted_utc=row.get("delisted_utc"),
        existed_at=existed_at,
    )


def _identity_key(record: IdentityRecord) -> tuple:
    return (record.composite_figi, record.share_class_figi, record.cik, record.mic, record.delisted_utc)


def _is_same_listing(known: IdentityRecord, probed: IdentityRecord) -> bool:
    """Whether a `date=` record is the listing already seen, with fewer fields.

    A `date=` body may omit `primary_exchange` (measured 2026-09-15), so an
    exact key comparison would split one listing into two records. A field the
    probe did not return matches anything; a field it did return must agree.
    Agreement alone is not enough: a probe row carrying neither FIGI nor cik
    would match every listing, and on a reused ticker it would hand the old
    issuer's start date to the current one. The probe must share a FIGI or a
    cik with the listing it stamps; otherwise it stays its own record.
    """
    fields_agree = all(
        probed_field is None or probed_field == known_field
        for known_field, probed_field in zip(_identity_key(known), _identity_key(probed), strict=True)
    )
    affirmative = (probed.composite_figi is not None and probed.composite_figi == known.composite_figi) or (
        probed.cik is not None and probed.cik == known.cik
    )
    return fields_agree and affirmative


def fetch_ticker_identity(
    ticker: str,
    api_key: str | None = None,
    *,
    probe_dates: Sequence[str] = (),
    pace_fn: Callable[[], None] | None = None,
) -> IdentityRecords:
    """Fetch one ticker's listing identity from Massive reference data.

    `probe_dates` are tried in the order given until one returns a record;
    the caller passes the needed membership dates ascending, so the earliest
    date the provider can answer becomes `existed_at`. An earliest date below
    the provider's coverage must not cost the ticker its later memberships.

    `pace_fn` runs before every request: the declared rate is per request,
    and a ticker costs two or three of them, so pacing per ticker would run
    the endpoint at two to three times the declared rate.

    Two list calls, then — only when neither record carries a `list_date` and
    `probe_dates` is non-empty — historical `date=` probes, whose records are
    stamped `existed_at`. That
    probe is the only start date available for a record Massive lists without
    one; an empty probe proves nothing and appends nothing (spec §4). The probe
    sends `ticker` and `date` only: `date=` with `active=false` comes back
    empty even for a ticker that existed on that date.

    This is the list form of the endpoint with query parameters, a different
    code path from `check_ticker_status`'s single-ticker `/{T}` path form.
    """
    key = api_key or os.environ.get("MASSIVE_API_KEY")
    if not key:
        raise UniverseFetchError("MASSIVE_API_KEY required for ticker identity lookup")

    bodies: list[bytes] = []
    records: list[IdentityRecord] = []
    seen: set[tuple] = set()
    pace = pace_fn or (lambda: None)
    for params in ({}, {"active": "false"}):
        pace()
        body, rows = _reference_page(ticker, key, params)
        bodies.append(body)
        for row in rows:
            record = _identity_record(row, ticker, None)
            if _identity_key(record) not in seen:
                seen.add(_identity_key(record))
                records.append(record)

    if any(record.list_date for record in records):
        probe_dates = ()
    empty_probes: list[str] = []
    for probe_date in probe_dates:
        pace()
        body, rows = _reference_page(ticker, key, {"date": probe_date})
        bodies.append(body)
        if not rows:
            empty_probes.append(probe_date)
            continue
        for row in rows:
            probed = _identity_record(row, ticker, probe_date)
            candidates = [i for i, known in enumerate(records) if _is_same_listing(known, probed)]
            # A probe row with no FIGI matches every listing loosely; on a reused
            # ticker it must stamp the FIGI-less (old) record, not the new one.
            candidates.sort(key=lambda i: records[i].composite_figi != probed.composite_figi)
            index = candidates[0] if candidates else None
            if index is None:
                seen.add(_identity_key(probed))
                records.append(probed)
            else:
                # Same listing, now with a proven date it existed on.
                records[index] = replace(records[index], existed_at=probe_date)
        break

    return IdentityRecords(responses=bodies, records=records, empty_probes=tuple(empty_probes))


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

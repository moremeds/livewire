# Universe Sync & Tag Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatically sync S&P 500 / Nasdaq-100 / Russell 2000 preset files from authoritative sources, detect dead tickers via Polygon's reference API, handle index promotions/demotions, and introduce a tag registry so tickers can be queried by membership (e.g., "all sp500") without relying on preset files.

**Architecture:** A new `clients/universe_client.py` fetches live index constituents from Wikipedia (S&P 500, Nasdaq-100) and Slickcharts (Russell 2000), plus ticker status from Polygon `/v3/reference/tickers/{ticker}`. A tag registry at `~/market-warehouse/registry.json` maps each ticker to a set of tags (`sp500`, `ndx100`, `r2k`, `interest`, `delisted`). A new script `livewire_scripts/universe_sync.py` orchestrates: fetch constituents → diff against registry → detect dead tickers via Polygon → apply movements (promotions/demotions) → update presets → archive delisted bronze. The unified CLI adds `livewire check --universe-sync`.

**Tech Stack:** Python 3.13, requests, lxml (HTML parsing), Polygon reference API via `MASSIVE_API_KEY`, pytest with `responses` mock

---

## Current State → Target State

```
BEFORE                                         AFTER
──────────────────────────────────              ──────────────────────────────────
presets/sp500.json — static, manual             presets/sp500.json — auto-synced from Wikipedia
presets/ndx100.json — static, manual            presets/ndx100.json — auto-synced from Wikipedia
presets/r2k.json — static, manual               presets/r2k.json — auto-synced from Slickcharts
No tag registry                                 ~/market-warehouse/registry.json — per-ticker tags
universe_screener.py — IB scanner-based         universe_sync.py — constituent + dead-ticker sync
Dead tickers stay in bronze forever             Dead tickers archived to bronze-delisted/
No promotion/demotion tracking                  Movements logged (R2K→SP500, etc.)
No "interests" preset                           presets/interests.json — user watchlist (optional)
```

## Data Sources

| Index | Source | Method | Fallback |
|-------|--------|--------|----------|
| S&P 500 | Wikipedia `List_of_S&P_500_companies` | `pandas.read_html()` or `lxml` HTML table parse | None (Wikipedia is reliable) |
| Nasdaq-100 | Wikipedia `Nasdaq-100` | Same HTML table parse | None |
| Russell 2000 | Slickcharts `/russell2000` | HTML table parse | None |
| Ticker status | Polygon `/v3/reference/tickers/{ticker}` | REST via `MASSIVE_API_KEY` | Skip dead-ticker check if no key |

## File Structure

```
clients/
  universe_client.py          # CREATE — Wikipedia/Slickcharts scraper + Polygon status checker
livewire_scripts/
  universe_sync.py            # CREATE — orchestrator: fetch → diff → move → update presets → archive
presets/
  interests.json              # CREATE — optional user watchlist (empty template)
scripts/
  livewire.py                 # MODIFY — add universe-sync to CHECK_MODULES
tests/
  test_universe_client.py     # CREATE — unit tests for scraper + Polygon client
  test_universe_sync.py       # CREATE — unit tests for orchestrator
```

## Registry Schema

`~/market-warehouse/registry.json`:

```json
{
  "version": 1,
  "updated_at": "2026-05-31T12:00:00+00:00",
  "tickers": {
    "AAPL": {
      "tags": ["sp500", "ndx100"],
      "status": "active",
      "added_at": "2026-05-31",
      "last_verified": "2026-05-31"
    },
    "TWTR": {
      "tags": [],
      "status": "delisted",
      "delisted_at": "2022-10-28",
      "added_at": "2026-05-31",
      "last_verified": "2026-05-31"
    }
  },
  "changelog": [
    {
      "date": "2026-05-31",
      "type": "promotion",
      "ticker": "APP",
      "from_tags": ["r2k"],
      "to_tags": ["sp500"]
    }
  ]
}
```

Key design decisions:
- **Tags are sets, not exclusive.** A ticker can be in `sp500` AND `ndx100` simultaneously (e.g., AAPL, MSFT).
- **`interest` tag** is for the user's personal watchlist — never auto-removed by sync.
- **Changelog** is append-only, capped at last 500 entries. Enables "what changed" reporting.
- **`status`** is `active` or `delisted`. Only Polygon can flip this (or manual override).
- **Registry lives in warehouse dir**, not repo — it's data, not code. Preset files (repo) are regenerated from registry + curated structure.

---

### Task 1: Universe Client — Wikipedia Scraper

**Files:**
- Create: `clients/universe_client.py`
- Test: `tests/test_universe_client.py`

This task builds the data-fetching layer. Three functions: `fetch_sp500()`, `fetch_ndx100()`, `fetch_r2k()` — each returns a `set[str]` of ticker symbols. Plus `check_ticker_status()` for Polygon.

- [ ] **Step 1: Write failing test for `fetch_sp500`**

```python
"""Tests for clients/universe_client.py."""

from __future__ import annotations

import pytest
import responses

from clients.universe_client import (
    fetch_sp500,
    fetch_ndx100,
    fetch_r2k,
    check_ticker_status,
    TickerStatus,
    UniverseFetchError,
)

# ── Minimal HTML fixtures ──────────────────────────────────────────────────

SP500_HTML = """
<html><body>
<table id="constituents">
<thead><tr><th>Symbol</th><th>Security</th><th>GICS Sector</th></tr></thead>
<tbody>
<tr><td><a>AAPL</a></td><td>Apple Inc.</td><td>Information Technology</td></tr>
<tr><td><a>MSFT</a></td><td>Microsoft Corp.</td><td>Information Technology</td></tr>
<tr><td><a>BRK.B</a></td><td>Berkshire Hathaway</td><td>Financials</td></tr>
</tbody>
</table>
</body></html>
"""

NDX100_HTML = """
<html><body>
<table id="constituents">
<thead><tr><th>Ticker</th><th>Company</th></tr></thead>
<tbody>
<tr><td>AAPL</td><td>Apple Inc.</td></tr>
<tr><td>NVDA</td><td>NVIDIA Corp.</td></tr>
</tbody>
</table>
</body></html>
"""

R2K_HTML = """
<html><body>
<table class="table table-hover table-borderless table-sm">
<thead><tr><th>No.</th><th>Company</th><th>Symbol</th></tr></thead>
<tbody>
<tr><td>1</td><td>Acme Corp</td><td>ACME</td></tr>
<tr><td>2</td><td>Beta Inc</td><td>BETA</td></tr>
</tbody>
</table>
</body></html>
"""


class TestFetchSP500:
    @responses.activate
    def test_parses_wikipedia_table(self):
        responses.add(
            responses.GET,
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            body=SP500_HTML,
            status=200,
        )
        result = fetch_sp500()
        assert result == {"AAPL", "MSFT", "BRK.B"}

    @responses.activate
    def test_http_error_raises(self):
        responses.add(
            responses.GET,
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            status=500,
        )
        with pytest.raises(UniverseFetchError, match="S&P 500"):
            fetch_sp500()

    @responses.activate
    def test_fallback_to_wikitable_class(self):
        fallback_html = """
        <html><body>
        <table class="wikitable sortable">
        <thead><tr><th>Symbol</th><th>Security</th></tr></thead>
        <tbody>
        <tr><td>GOOG</td><td>Alphabet</td></tr>
        </tbody>
        </table>
        </body></html>
        """
        responses.add(
            responses.GET,
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            body=fallback_html,
            status=200,
        )
        result = fetch_sp500()
        assert result == {"GOOG"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_universe_client.py::TestFetchSP500 -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'clients.universe_client'`

- [ ] **Step 3: Write `fetch_sp500` implementation**

```python
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
from typing import Optional

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
    delisted_utc: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    market: Optional[str] = None
    list_date: Optional[str] = None


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
    api_key: Optional[str] = None,
) -> TickerStatus:
    key = api_key or os.environ.get("MASSIVE_API_KEY")
    if not key:
        raise UniverseFetchError("MASSIVE_API_KEY required for ticker status check")
    url = f"{_POLYGON_BASE}/v3/reference/tickers/{ticker.upper()}"
    try:
        resp = requests.get(url, params={"apiKey": key}, timeout=_TIMEOUT)
    except requests.RequestException as exc:
        raise UniverseFetchError(f"Polygon status check failed for {ticker}: {exc}") from exc
    # Polygon returns 404 for delisted tickers — treat as confirmed delisted
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
    api_key: Optional[str] = None,
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
```

- [ ] **Step 4: Install lxml if needed**

Run: `source ~/market-warehouse/.venv/bin/activate && pip install lxml cssselect 2>/dev/null; pip show lxml | head -3`

- [ ] **Step 5: Run tests to verify they pass**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_universe_client.py::TestFetchSP500 -v`
Expected: PASS

- [ ] **Step 6: Write remaining scraper tests**

Add to `tests/test_universe_client.py`:

```python
class TestFetchNDX100:
    @responses.activate
    def test_parses_wikipedia_table(self):
        responses.add(
            responses.GET,
            "https://en.wikipedia.org/wiki/Nasdaq-100",
            body=NDX100_HTML,
            status=200,
        )
        result = fetch_ndx100()
        assert result == {"AAPL", "NVDA"}

    @responses.activate
    def test_http_error_raises(self):
        responses.add(
            responses.GET,
            "https://en.wikipedia.org/wiki/Nasdaq-100",
            status=404,
        )
        with pytest.raises(UniverseFetchError, match="Nasdaq-100"):
            fetch_ndx100()


class TestFetchR2K:
    @responses.activate
    def test_parses_slickcharts_table(self):
        responses.add(
            responses.GET,
            "https://www.slickcharts.com/russell2000",
            body=R2K_HTML,
            status=200,
        )
        result = fetch_r2k()
        assert result == {"ACME", "BETA"}

    @responses.activate
    def test_http_error_raises(self):
        responses.add(
            responses.GET,
            "https://www.slickcharts.com/russell2000",
            status=403,
        )
        with pytest.raises(UniverseFetchError, match="Russell 2000"):
            fetch_r2k()
```

- [ ] **Step 7: Run all scraper tests**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_universe_client.py -v -k "not Polygon"`
Expected: PASS

- [ ] **Step 8: Write Polygon status check tests**

Add to `tests/test_universe_client.py`:

```python
class TestCheckTickerStatus:
    @responses.activate
    def test_active_ticker(self):
        responses.add(
            responses.GET,
            "https://api.polygon.io/v3/reference/tickers/AAPL",
            json={
                "results": {
                    "ticker": "AAPL",
                    "active": True,
                    "name": "Apple Inc.",
                    "type": "CS",
                    "market": "stocks",
                    "list_date": "1980-12-12",
                }
            },
            status=200,
        )
        status = check_ticker_status("AAPL", api_key="test-key")
        assert status.active is True
        assert status.ticker == "AAPL"
        assert status.name == "Apple Inc."
        assert status.delisted_utc is None

    @responses.activate
    def test_delisted_ticker_returns_404(self):
        responses.add(
            responses.GET,
            "https://api.polygon.io/v3/reference/tickers/TWTR",
            status=404,
        )
        status = check_ticker_status("TWTR", api_key="test-key")
        assert status.active is False
        assert status.ticker == "TWTR"

    def test_no_api_key_raises(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        with pytest.raises(UniverseFetchError, match="MASSIVE_API_KEY"):
            check_ticker_status("AAPL")

    @responses.activate
    def test_server_error_raises(self):
        responses.add(
            responses.GET,
            "https://api.polygon.io/v3/reference/tickers/FAKE",
            status=500,
        )
        with pytest.raises(UniverseFetchError, match="FAKE"):
            check_ticker_status("FAKE", api_key="test-key")


class TestCheckTickersBulk:
    @responses.activate
    def test_returns_dict_of_statuses(self):
        for ticker, active in [("AAPL", True), ("MSFT", True)]:
            responses.add(
                responses.GET,
                f"https://api.polygon.io/v3/reference/tickers/{ticker}",
                json={"results": {"ticker": ticker, "active": active}},
                status=200,
            )
        result = check_tickers_bulk(["AAPL", "MSFT"], api_key="test-key", throttle=0)
        assert len(result) == 2
        assert result["AAPL"].active is True

    @responses.activate
    def test_skips_failures(self):
        responses.add(
            responses.GET,
            "https://api.polygon.io/v3/reference/tickers/AAPL",
            json={"results": {"ticker": "AAPL", "active": True}},
            status=200,
        )
        responses.add(
            responses.GET,
            "https://api.polygon.io/v3/reference/tickers/BAD",
            status=500,
        )
        result = check_tickers_bulk(["AAPL", "BAD"], api_key="test-key", throttle=0)
        assert len(result) == 1
        assert "AAPL" in result
```

- [ ] **Step 9: Run full universe_client tests**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_universe_client.py -v`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add clients/universe_client.py tests/test_universe_client.py
git commit -m "feat: add universe_client for index constituent scraping and Polygon status"
```

---

### Task 2: Tag Registry

**Files:**
- Create: `clients/tag_registry.py`
- Test: `tests/test_tag_registry.py`

The registry is the single source of truth for per-ticker tag membership. Pure data operations — no I/O besides reading/writing the JSON file.

- [ ] **Step 1: Write failing tests for registry core**

```python
"""Tests for clients/tag_registry.py."""

from __future__ import annotations

import json
from datetime import date

import pytest

from clients.tag_registry import (
    TagRegistry,
    RegistryEntry,
    ChangelogEntry,
)


class TestRegistryLoadSave:
    def test_empty_registry(self, tmp_path):
        path = tmp_path / "registry.json"
        reg = TagRegistry(path)
        assert reg.all_tickers() == set()
        assert reg.by_tag("sp500") == set()

    def test_save_and_reload(self, tmp_path):
        path = tmp_path / "registry.json"
        reg = TagRegistry(path)
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        reg.save()

        reg2 = TagRegistry(path)
        assert reg2.by_tag("sp500") == {"AAPL"}
        assert reg2.by_tag("ndx100") == {"AAPL"}
        assert reg2.get("AAPL").status == "active"

    def test_load_corrupted_file(self, tmp_path):
        path = tmp_path / "registry.json"
        path.write_text("not json")
        reg = TagRegistry(path)
        assert reg.all_tickers() == set()


class TestRegistryOperations:
    def test_set_tags(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        assert reg.get("AAPL").tags == {"sp500", "ndx100"}

    def test_add_tag(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.add_tag("AAPL", "ndx100")
        assert reg.get("AAPL").tags == {"sp500", "ndx100"}

    def test_remove_tag(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        reg.remove_tag("AAPL", "sp500")
        assert reg.get("AAPL").tags == {"ndx100"}

    def test_by_tag(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        reg.set_tags("MSFT", {"sp500"}, status="active")
        reg.set_tags("ACME", {"r2k"}, status="active")
        assert reg.by_tag("sp500") == {"AAPL", "MSFT"}
        assert reg.by_tag("ndx100") == {"AAPL"}
        assert reg.by_tag("r2k") == {"ACME"}

    def test_by_tags_intersection(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500", "ndx100"}, status="active")
        reg.set_tags("MSFT", {"sp500"}, status="active")
        assert reg.by_tags({"sp500", "ndx100"}) == {"AAPL"}

    def test_active_only(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        reg.set_tags("TWTR", {"sp500"}, status="delisted")
        assert reg.by_tag("sp500", active_only=True) == {"AAPL"}
        assert reg.by_tag("sp500", active_only=False) == {"AAPL", "TWTR"}

    def test_get_unknown_ticker(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        assert reg.get("FAKE") is None

    def test_mark_delisted(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("TWTR", {"sp500"}, status="active")
        reg.mark_delisted("TWTR", delisted_at="2022-10-28")
        entry = reg.get("TWTR")
        assert entry.status == "delisted"
        assert entry.delisted_at == "2022-10-28"
        assert entry.tags == {"sp500"}


class TestChangelog:
    def test_log_promotion(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        reg.set_tags("APP", {"r2k"}, status="active")
        reg.log_change("promotion", "APP", from_tags=["r2k"], to_tags=["sp500"])
        reg.save()

        reg2 = TagRegistry(tmp_path / "r.json")
        assert len(reg2.changelog) == 1
        assert reg2.changelog[0].type == "promotion"
        assert reg2.changelog[0].ticker == "APP"

    def test_changelog_cap(self, tmp_path):
        reg = TagRegistry(tmp_path / "r.json")
        for i in range(600):
            reg.log_change("add", f"T{i}", from_tags=[], to_tags=["sp500"])
        reg.save()
        reg2 = TagRegistry(tmp_path / "r.json")
        assert len(reg2.changelog) == 500
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_tag_registry.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write TagRegistry implementation**

```python
"""Per-ticker tag registry — the single source of truth for index membership.

Stores per-ticker tags (sp500, ndx100, r2k, interest, delisted) with status
and a capped append-only changelog. Lives at ~/market-warehouse/registry.json.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_CHANGELOG_CAP = 500


@dataclass
class RegistryEntry:
    tags: set[str] = field(default_factory=set)
    status: str = "active"
    added_at: Optional[str] = None
    last_verified: Optional[str] = None
    delisted_at: Optional[str] = None


@dataclass(frozen=True)
class ChangelogEntry:
    date: str
    type: str
    ticker: str
    from_tags: list[str]
    to_tags: list[str]


class TagRegistry:
    def __init__(self, path: Path):
        self._path = Path(path)
        self._entries: dict[str, RegistryEntry] = {}
        self.changelog: list[ChangelogEntry] = []
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            with self._path.open() as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            log.warning("Corrupted registry at %s, starting fresh", self._path)
            return
        for ticker, entry_data in data.get("tickers", {}).items():
            self._entries[ticker] = RegistryEntry(
                tags=set(entry_data.get("tags", [])),
                status=entry_data.get("status", "active"),
                added_at=entry_data.get("added_at"),
                last_verified=entry_data.get("last_verified"),
                delisted_at=entry_data.get("delisted_at"),
            )
        for cl in data.get("changelog", []):
            self.changelog.append(ChangelogEntry(
                date=cl["date"],
                type=cl["type"],
                ticker=cl["ticker"],
                from_tags=cl.get("from_tags", []),
                to_tags=cl.get("to_tags", []),
            ))

    def save(self) -> None:
        import tempfile

        self._path.parent.mkdir(parents=True, exist_ok=True)
        tickers_data = {}
        for ticker, entry in sorted(self._entries.items()):
            tickers_data[ticker] = {
                "tags": sorted(entry.tags),
                "status": entry.status,
                "added_at": entry.added_at,
                "last_verified": entry.last_verified,
                "delisted_at": entry.delisted_at,
            }
        trimmed = self.changelog[-_CHANGELOG_CAP:]
        data = {
            "version": 1,
            "updated_at": datetime.now(UTC).isoformat(),
            "tickers": tickers_data,
            "changelog": [
                {
                    "date": cl.date,
                    "type": cl.type,
                    "ticker": cl.ticker,
                    "from_tags": cl.from_tags,
                    "to_tags": cl.to_tags,
                }
                for cl in trimmed
            ],
        }
        fd, tmp = tempfile.mkstemp(dir=str(self._path.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, str(self._path))
        except BaseException:
            os.unlink(tmp)
            raise
        self.changelog = trimmed

    def get(self, ticker: str) -> Optional[RegistryEntry]:
        return self._entries.get(ticker)

    def all_tickers(self) -> set[str]:
        return set(self._entries.keys())

    def set_tags(self, ticker: str, tags: set[str], status: str = "active") -> None:
        today = date.today().isoformat()
        existing = self._entries.get(ticker)
        self._entries[ticker] = RegistryEntry(
            tags=set(tags),
            status=status,
            added_at=existing.added_at if existing else today,
            last_verified=today,
            delisted_at=existing.delisted_at if existing else None,
        )

    def add_tag(self, ticker: str, tag: str) -> None:
        entry = self._entries.get(ticker)
        if entry:
            entry.tags.add(tag)
        else:
            self.set_tags(ticker, {tag})

    def remove_tag(self, ticker: str, tag: str) -> None:
        entry = self._entries.get(ticker)
        if entry:
            entry.tags.discard(tag)

    def by_tag(self, tag: str, active_only: bool = True) -> set[str]:
        result: set[str] = set()
        for ticker, entry in self._entries.items():
            if tag in entry.tags:
                if active_only and entry.status != "active":
                    continue
                result.add(ticker)
        return result

    def by_tags(self, tags: set[str], active_only: bool = True) -> set[str]:
        result: set[str] = set()
        for ticker, entry in self._entries.items():
            if tags.issubset(entry.tags):
                if active_only and entry.status != "active":
                    continue
                result.add(ticker)
        return result

    def mark_delisted(self, ticker: str, delisted_at: Optional[str] = None) -> None:
        entry = self._entries.get(ticker)
        if entry:
            entry.status = "delisted"
            entry.delisted_at = delisted_at or date.today().isoformat()

    def log_change(
        self, type_: str, ticker: str, from_tags: list[str], to_tags: list[str]
    ) -> None:
        self.changelog.append(ChangelogEntry(
            date=date.today().isoformat(),
            type=type_,
            ticker=ticker,
            from_tags=from_tags,
            to_tags=to_tags,
        ))
```

- [ ] **Step 4: Run tests**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_tag_registry.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add clients/tag_registry.py tests/test_tag_registry.py
git commit -m "feat: add per-ticker tag registry with changelog"
```

---

### Task 3: Universe Sync Orchestrator

**Files:**
- Create: `livewire_scripts/universe_sync.py`
- Test: `tests/test_universe_sync.py`

This is the orchestrator that ties everything together: fetch constituents → diff against registry → check dead tickers → handle movements → update presets → archive delisted.

- [ ] **Step 1: Write failing tests for the diff logic**

```python
"""Tests for livewire_scripts/universe_sync.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from clients.tag_registry import TagRegistry
from livewire_scripts.universe_sync import (
    compute_movements,
    apply_sync,
    update_preset_tickers,
    main,
    Movement,
)


class TestComputeMovements:
    def test_new_ticker(self):
        registry = {}
        live = {"sp500": {"AAPL", "NEW"}}
        existing = {"sp500": {"AAPL"}}
        moves = compute_movements(live, existing)
        assert Movement("add", "NEW", from_tags=[], to_tags=["sp500"]) in moves

    def test_removed_ticker(self):
        live = {"sp500": {"AAPL"}}
        existing = {"sp500": {"AAPL", "OLD"}}
        moves = compute_movements(live, existing)
        assert Movement("remove", "OLD", from_tags=["sp500"], to_tags=[]) in moves

    def test_promotion_r2k_to_sp500(self):
        live = {"sp500": {"AAPL", "APP"}, "r2k": {"BETA"}}
        existing = {"sp500": {"AAPL"}, "r2k": {"APP", "BETA"}}
        moves = compute_movements(live, existing)
        assert Movement("promotion", "APP", from_tags=["r2k"], to_tags=["sp500"]) in moves

    def test_demotion_sp500_to_r2k(self):
        live = {"sp500": {"AAPL"}, "r2k": {"BETA", "OLD"}}
        existing = {"sp500": {"AAPL", "OLD"}, "r2k": {"BETA"}}
        moves = compute_movements(live, existing)
        assert Movement("demotion", "OLD", from_tags=["sp500"], to_tags=["r2k"]) in moves

    def test_no_changes(self):
        live = {"sp500": {"AAPL"}}
        existing = {"sp500": {"AAPL"}}
        moves = compute_movements(live, existing)
        assert moves == []

    def test_multi_index_ticker_stays(self):
        live = {"sp500": {"AAPL"}, "ndx100": {"AAPL"}}
        existing = {"sp500": {"AAPL"}, "ndx100": {"AAPL"}}
        moves = compute_movements(live, existing)
        assert moves == []

    def test_drops_from_one_index_keeps_other(self):
        live = {"sp500": {"AAPL"}, "ndx100": set()}
        existing = {"sp500": {"AAPL"}, "ndx100": {"AAPL"}}
        moves = compute_movements(live, existing)
        assert len(moves) == 1
        assert moves[0].type == "demotion"
        assert moves[0].from_tags == ["sp500", "ndx100"]
        assert moves[0].to_tags == ["sp500"]

    def test_added_to_second_index(self):
        live = {"sp500": {"AAPL"}, "ndx100": {"AAPL"}}
        existing = {"sp500": {"AAPL"}, "ndx100": set()}
        moves = compute_movements(live, existing)
        assert len(moves) == 1
        assert moves[0].type == "promotion"


class TestUpdatePresetTickers:
    def test_updates_tickers_array(self, tmp_path):
        preset_path = tmp_path / "sp500.json"
        preset_path.write_text(json.dumps({
            "name": "sp500",
            "description": "S&P 500",
            "tickers": ["AAPL", "OLD"],
            "pairs": [["AAPL", "OLD"]],
            "groups": {},
            "source": "wikipedia",
        }))
        update_preset_tickers(preset_path, {"AAPL", "NEW"})
        data = json.loads(preset_path.read_text())
        assert data["tickers"] == ["AAPL", "NEW"]
        assert data["pairs"] == [["AAPL", "OLD"]]
        assert data["name"] == "sp500"

    def test_creates_new_preset(self, tmp_path):
        preset_path = tmp_path / "interests.json"
        update_preset_tickers(preset_path, {"AAPL", "TSLA"}, name="interests", description="Personal watchlist")
        data = json.loads(preset_path.read_text())
        assert data["tickers"] == ["AAPL", "TSLA"]
        assert data["name"] == "interests"


class TestApplySync:
    def test_adds_new_tickers_to_registry(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        reg.set_tags("AAPL", {"sp500"}, status="active")
        movements = [Movement("add", "NEW", from_tags=[], to_tags=["sp500"])]
        apply_sync(reg, movements)
        assert "sp500" in reg.get("NEW").tags

    def test_promotion_updates_tags(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        reg.set_tags("APP", {"r2k"}, status="active")
        movements = [Movement("promotion", "APP", from_tags=["r2k"], to_tags=["sp500"])]
        apply_sync(reg, movements)
        entry = reg.get("APP")
        assert "sp500" in entry.tags
        assert "r2k" not in entry.tags

    def test_demotion_updates_tags(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        reg.set_tags("OLD", {"sp500"}, status="active")
        movements = [Movement("demotion", "OLD", from_tags=["sp500"], to_tags=["r2k"])]
        apply_sync(reg, movements)
        entry = reg.get("OLD")
        assert "r2k" in entry.tags
        assert "sp500" not in entry.tags

    def test_remove_clears_index_tags(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        reg.set_tags("GONE", {"sp500"}, status="active")
        movements = [Movement("remove", "GONE", from_tags=["sp500"], to_tags=[])]
        apply_sync(reg, movements)
        entry = reg.get("GONE")
        assert "sp500" not in entry.tags

    def test_interest_tag_preserved_on_remove(self, tmp_path):
        reg = TagRegistry(tmp_path / "registry.json")
        reg.set_tags("GONE", {"sp500", "interest"}, status="active")
        movements = [Movement("remove", "GONE", from_tags=["sp500"], to_tags=[])]
        apply_sync(reg, movements)
        entry = reg.get("GONE")
        assert "interest" in entry.tags
        assert "sp500" not in entry.tags


class TestArchiveDelisted:
    def test_moves_bronze_to_delisted(self, tmp_path):
        from livewire_scripts.universe_sync import _archive_delisted

        data_lake = tmp_path / "data-lake"
        src = data_lake / "bronze" / "asset_class=equity" / "symbol=TWTR"
        src.mkdir(parents=True)
        (src / "1d.parquet").write_text("fake")

        assert _archive_delisted("TWTR", data_lake) is True
        assert not src.exists()
        assert (data_lake / "bronze-delisted" / "asset_class=equity" / "symbol=TWTR" / "1d.parquet").exists()

    def test_returns_false_if_no_bronze(self, tmp_path):
        from livewire_scripts.universe_sync import _archive_delisted

        assert _archive_delisted("FAKE", tmp_path) is False


class TestMain:
    def _setup_workspace(self, tmp_path, monkeypatch):
        warehouse = tmp_path / "warehouse"
        warehouse.mkdir()
        presets = tmp_path / "presets"
        presets.mkdir()
        for name, tickers in [("sp500", ["AAPL"]), ("ndx100", ["AAPL"]), ("r2k", ["ACME"])]:
            (presets / f"{name}.json").write_text(json.dumps({
                "name": name, "tickers": tickers, "pairs": [], "groups": {}, "source": "test"
            }))
        monkeypatch.setattr("livewire_scripts.universe_sync._WAREHOUSE_DIR", warehouse)
        monkeypatch.setattr("livewire_scripts.universe_sync._PRESET_DIR", presets)
        monkeypatch.setattr("livewire_scripts.universe_sync._DATA_LAKE", warehouse / "data-lake")
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        return warehouse, presets

    @patch("livewire_scripts.universe_sync.fetch_sp500", return_value=set(f"T{i}" for i in range(500)))
    @patch("livewire_scripts.universe_sync.fetch_ndx100", return_value=set(f"N{i}" for i in range(100)))
    @patch("livewire_scripts.universe_sync.fetch_r2k", return_value=set(f"R{i}" for i in range(1900)))
    def test_full_sync_dry_run(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        warehouse, _ = self._setup_workspace(tmp_path, monkeypatch)
        main(["--dry-run"])
        assert not (warehouse / "registry.json").exists()

    @patch("livewire_scripts.universe_sync.fetch_sp500", return_value=set(f"T{i}" for i in range(500)) | {"MSFT"})
    @patch("livewire_scripts.universe_sync.fetch_ndx100", return_value=set(f"N{i}" for i in range(100)))
    @patch("livewire_scripts.universe_sync.fetch_r2k", return_value=set(f"R{i}" for i in range(1900)))
    def test_full_sync_writes_registry(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        warehouse, _ = self._setup_workspace(tmp_path, monkeypatch)
        main([])
        assert (warehouse / "registry.json").exists()
        reg = TagRegistry(warehouse / "registry.json")
        assert "sp500" in reg.get("MSFT").tags

    @patch("livewire_scripts.universe_sync.fetch_sp500", return_value={"AAPL"})
    @patch("livewire_scripts.universe_sync.fetch_ndx100", return_value={"AAPL"})
    @patch("livewire_scripts.universe_sync.fetch_r2k", return_value={"ACME"})
    def test_aborts_on_suspiciously_few_tickers(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        self._setup_workspace(tmp_path, monkeypatch)
        with pytest.raises(SystemExit):
            main([])

    @patch("livewire_scripts.universe_sync.fetch_sp500", return_value=set(f"T{i}" for i in range(500)))
    @patch("livewire_scripts.universe_sync.fetch_ndx100", return_value=set(f"N{i}" for i in range(100)))
    @patch("livewire_scripts.universe_sync.fetch_r2k", return_value=set(f"R{i}" for i in range(1900)))
    def test_interests_flag(self, mock_r2k, mock_ndx, mock_sp, tmp_path, monkeypatch):
        warehouse, presets = self._setup_workspace(tmp_path, monkeypatch)
        main(["--interests", "TSLA", "GME"])
        reg = TagRegistry(warehouse / "registry.json")
        assert "interest" in reg.get("TSLA").tags
        assert "interest" in reg.get("GME").tags
        interests_preset = json.loads((presets / "interests.json").read_text())
        assert "TSLA" in interests_preset["tickers"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_universe_sync.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write universe_sync orchestrator**

```python
"""Universe sync — fetch live index constituents, detect dead tickers, update registry and presets.

Usage:
    source ~/market-warehouse/.venv/bin/activate
    python scripts/livewire_ingest.py universe-sync              # Full sync
    python scripts/livewire_ingest.py universe-sync --dry-run    # Report only
    python scripts/livewire_ingest.py universe-sync --skip-dead  # Skip Polygon dead-ticker check
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.table import Table

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.tag_registry import TagRegistry
from clients.universe_client import (
    fetch_sp500,
    fetch_ndx100,
    fetch_r2k,
    check_tickers_bulk,
    TickerStatus,
    UniverseFetchError,
)

log = logging.getLogger(__name__)
console = Console()

_WAREHOUSE_DIR = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
_DATA_LAKE = _WAREHOUSE_DIR / "data-lake"
_PRESET_DIR = PROJECT_ROOT / "presets"

INDEX_TAGS = ("sp500", "ndx100", "r2k")
INDEX_HIERARCHY = ["sp500", "ndx100", "r2k"]

MIN_EXPECTED_CONSTITUENTS = {
    "sp500": 400,
    "ndx100": 80,
    "r2k": 1500,
}


@dataclass(frozen=True)
class Movement:
    type: str
    ticker: str
    from_tags: list[str]
    to_tags: list[str]


def compute_movements(
    live: dict[str, set[str]],
    existing: dict[str, set[str]],
) -> list[Movement]:
    movements: list[Movement] = []
    all_tickers = set()
    for s in live.values():
        all_tickers |= s
    for s in existing.values():
        all_tickers |= s

    for ticker in sorted(all_tickers):
        live_in = [idx for idx in INDEX_HIERARCHY if ticker in live.get(idx, set())]
        was_in = [idx for idx in INDEX_HIERARCHY if ticker in existing.get(idx, set())]

        if live_in == was_in:
            continue

        if not was_in and live_in:
            movements.append(Movement("add", ticker, from_tags=[], to_tags=live_in))
        elif was_in and not live_in:
            movements.append(Movement("remove", ticker, from_tags=was_in, to_tags=[]))
        elif was_in and live_in:
            was_set = set(was_in)
            live_set = set(live_in)
            added = live_set - was_set
            dropped = was_set - live_set
            if added and not dropped:
                move_type = "promotion"
            elif dropped and not added:
                move_type = "demotion"
            elif added and dropped:
                was_best = min(INDEX_HIERARCHY.index(t) for t in was_in)
                live_best = min(INDEX_HIERARCHY.index(t) for t in live_in)
                move_type = "promotion" if live_best < was_best else "demotion"
            else:
                move_type = "move"
            movements.append(Movement(move_type, ticker, from_tags=was_in, to_tags=live_in))

    return movements


def apply_sync(registry: TagRegistry, movements: list[Movement]) -> None:
    for move in movements:
        for tag in move.from_tags:
            registry.remove_tag(move.ticker, tag)
        for tag in move.to_tags:
            registry.add_tag(move.ticker, tag)
        registry.log_change(move.type, move.ticker, move.from_tags, move.to_tags)


def update_preset_tickers(
    path: Path,
    tickers: set[str],
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> None:
    if path.exists():
        with path.open() as f:
            data = json.load(f)
        data["tickers"] = sorted(tickers)
    else:
        data = {
            "name": name or path.stem,
            "description": description or "",
            "tickers": sorted(tickers),
            "pairs": [],
            "groups": {},
            "source": "universe-sync",
        }
    with path.open("w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def _archive_delisted(ticker: str, data_lake: Path) -> bool:
    src = data_lake / "bronze" / "asset_class=equity" / f"symbol={ticker}"
    if not src.exists():
        return False
    dst = data_lake / "bronze-delisted" / "asset_class=equity" / f"symbol={ticker}"
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
    log.info("Archived %s to bronze-delisted/", ticker)
    return True


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Sync index constituents and update registry")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without modifying")
    parser.add_argument("--skip-dead", action="store_true", help="Skip Polygon dead-ticker check")
    parser.add_argument(
        "--interests",
        nargs="*",
        default=None,
        help="Tickers to add to the interests preset",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    registry_path = _WAREHOUSE_DIR / "registry.json"
    registry = TagRegistry(registry_path)

    # ── Fetch live constituents ──────────────────────────────────────────
    log.info("Fetching live index constituents...")
    live: dict[str, set[str]] = {}
    for idx, fetcher, label in [
        ("sp500", fetch_sp500, "S&P 500"),
        ("ndx100", fetch_ndx100, "Nasdaq-100"),
        ("r2k", fetch_r2k, "Russell 2000"),
    ]:
        try:
            tickers = fetcher()
            live[idx] = tickers
            log.info("%s: %d constituents", label, len(tickers))
        except UniverseFetchError as exc:
            log.error("Failed to fetch %s: %s", label, exc)
            sys.exit(1)

    # ── Sanity check: abort if any index returns suspiciously few tickers ──
    for idx, tickers in live.items():
        min_expected = MIN_EXPECTED_CONSTITUENTS.get(idx, 0)
        if len(tickers) < min_expected:
            log.error(
                "SAFETY: %s returned only %d tickers (expected >= %d). "
                "Aborting to prevent mass removal.",
                idx, len(tickers), min_expected,
            )
            sys.exit(1)

    # ── Build existing state from registry or presets ─────────────────────
    existing: dict[str, set[str]] = {}
    for idx in INDEX_TAGS:
        existing[idx] = registry.by_tag(idx, active_only=False)
        if not existing[idx]:
            preset_path = _PRESET_DIR / f"{idx}.json"
            if preset_path.exists():
                with preset_path.open() as f:
                    existing[idx] = set(json.load(f).get("tickers", []))

    # ── Compute movements ────────────────────────────────────────────────
    movements = compute_movements(live, existing)

    # ── Display movements ────────────────────────────────────────────────
    if movements:
        table = Table(title="Universe Changes")
        table.add_column("Type", style="bold")
        table.add_column("Ticker")
        table.add_column("From")
        table.add_column("To")
        for m in movements:
            style = {
                "add": "green",
                "remove": "red",
                "promotion": "cyan",
                "demotion": "yellow",
                "move": "blue",
            }.get(m.type, "white")
            table.add_row(
                m.type.upper(),
                m.ticker,
                ", ".join(m.from_tags) or "-",
                ", ".join(m.to_tags) or "-",
                style=style,
            )
        console.print(table)
        console.print(f"\n[bold]{len(movements)} changes[/bold] "
                       f"({sum(1 for m in movements if m.type == 'add')} adds, "
                       f"{sum(1 for m in movements if m.type == 'remove')} removes, "
                       f"{sum(1 for m in movements if m.type == 'promotion')} promotions, "
                       f"{sum(1 for m in movements if m.type == 'demotion')} demotions)")
    else:
        console.print("[green]No changes — all presets are current.[/green]")

    if args.dry_run:
        log.info("Dry run — no files modified.")
        return

    # ── Apply movements to registry ──────────────────────────────────────
    apply_sync(registry, movements)

    # ── Seed registry for tickers not yet tracked ────────────────────────
    for idx, tickers in live.items():
        for ticker in tickers:
            if registry.get(ticker) is None:
                registry.set_tags(ticker, {idx}, status="active")
            elif idx not in registry.get(ticker).tags:
                registry.add_tag(ticker, idx)

    # ── Dead ticker check via Polygon ────────────────────────────────────
    if not args.skip_dead and os.environ.get("MASSIVE_API_KEY"):
        removed_tickers = [m.ticker for m in movements if m.type == "remove"]
        orphan_tickers = [
            t for t in registry.all_tickers()
            if registry.get(t) and not registry.get(t).tags & set(INDEX_TAGS)
            and registry.get(t).status == "active"
        ]
        check_list = list(set(removed_tickers + orphan_tickers))
        if check_list:
            log.info("Checking %d tickers via Polygon for delisted status...", len(check_list))
            statuses = check_tickers_bulk(check_list)
            for ticker, status in statuses.items():
                if not status.active:
                    log.info("DELISTED: %s (delisted_utc=%s)", ticker, status.delisted_utc)
                    registry.mark_delisted(ticker, delisted_at=status.delisted_utc)
                    _archive_delisted(ticker, _DATA_LAKE)
    elif not args.skip_dead:
        log.info("MASSIVE_API_KEY not set — skipping dead-ticker check")

    # ── Handle interests ─────────────────────────────────────────────────
    if args.interests is not None:
        for ticker in args.interests:
            registry.add_tag(ticker.upper(), "interest")
        interest_tickers = registry.by_tag("interest")
        update_preset_tickers(
            _PRESET_DIR / "interests.json",
            interest_tickers,
            name="interests",
            description="Personal watchlist",
        )
        log.info("Interests preset: %d tickers", len(interest_tickers))

    # ── Update preset tickers arrays ─────────────────────────────────────
    for idx in INDEX_TAGS:
        active_tickers = registry.by_tag(idx, active_only=True)
        preset_path = _PRESET_DIR / f"{idx}.json"
        if preset_path.exists():
            update_preset_tickers(preset_path, active_tickers)
            log.info("Updated %s: %d tickers", preset_path.name, len(active_tickers))

    # ── Save registry ────────────────────────────────────────────────────
    registry.save()
    log.info("Registry saved to %s", registry_path)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_universe_sync.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/universe_sync.py tests/test_universe_sync.py
git commit -m "feat: add universe sync orchestrator with movement detection"
```

---

### Task 4: CLI Integration + Interests Preset

**Files:**
- Modify: `scripts/livewire.py` — add `universe-sync` to CHECK_MODULES
- Modify: `scripts/livewire_ingest.py` — add `universe-sync` command
- Modify: `clients/__init__.py` — export TagRegistry
- Create: `presets/interests.json` — empty template
- Modify: `tests/test_livewire_cli.py` — add test for new command
- Modify: `tests/test_livewire_entrypoints.py` — add test for new command

- [ ] **Step 1: Write test for CLI dispatch**

Add to `tests/test_livewire_cli.py` (following existing pattern):

```python
def test_check_universe_sync_dispatches(monkeypatch):
    dispatched = []

    def mock_dispatch(module_name, argv, display):
        dispatched.append(module_name)
        return 0

    monkeypatch.setattr("scripts.livewire._dispatch_module", mock_dispatch)
    from scripts.livewire import main

    main(["check", "--universe-sync"])
    assert dispatched == ["livewire_scripts.universe_sync"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_livewire_cli.py::test_check_universe_sync_dispatches -v`
Expected: FAIL

- [ ] **Step 3: Add universe-sync to CLI**

In `scripts/livewire.py`, add to `CHECK_MODULES`:

```python
CHECK_MODULES = {
    "health": "livewire_scripts.health_check",
    "coverage": "livewire_scripts.coverage_report",
    "report": "livewire_scripts.data_quality_report",
    "weekly": "livewire_scripts.weekly_quality_summary",
    "watchdog": "livewire_scripts.check_daily_update_watchdog",
    "universe": "livewire_scripts.universe_screener",
    "universe-sync": "livewire_scripts.universe_sync",
}
```

In `_dispatch_check`, add `--universe-sync` flag:

```python
parser.add_argument("--universe-sync", action="store_true")
```

And in the mode resolution:

```python
elif args.universe_sync:
    mode = "universe-sync"
```

In `scripts/livewire_ingest.py`, add to `COMMANDS`:

```python
"universe-sync": "livewire_scripts.universe_sync",
```

- [ ] **Step 4: Add to `clients/__init__.py`**

Add import and export:

```python
from clients.tag_registry import TagRegistry
```

Add `"TagRegistry"` to `__all__`.

- [ ] **Step 5: Create empty interests preset**

Create `presets/interests.json`:

```json
{
  "name": "interests",
  "description": "Personal watchlist — tickers you want to track regardless of index membership",
  "tickers": [],
  "source": "manual"
}
```

- [ ] **Step 6: Run CLI tests**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_livewire_cli.py tests/test_livewire_entrypoints.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add scripts/livewire.py scripts/livewire_ingest.py clients/__init__.py presets/interests.json tests/test_livewire_cli.py tests/test_livewire_entrypoints.py
git commit -m "feat: wire universe-sync into CLI and add interests preset"
```

---

### Task 5: Coverage Gate + CLAUDE.md Update

**Files:**
- Modify: `pyproject.toml` — add new source files to coverage
- Modify: `CLAUDE.md` — document universe sync

- [ ] **Step 1: Run full test suite with coverage**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/ -v --cov=clients --cov=livewire_scripts --cov-report=term-missing -W error::RuntimeWarning`
Expected: 100% on included sources

- [ ] **Step 2: Fix any coverage gaps**

Add tests for any uncovered branches found in step 1.

- [ ] **Step 3: Update CLAUDE.md**

Add a new section under "Data Ingestion" documenting the universe sync:

```markdown
### Universe sync

`livewire check --universe-sync` (or `scripts/livewire_ingest.py universe-sync`) fetches live S&P 500, Nasdaq-100, and Russell 2000 constituents and reconciles against the local tag registry and preset files.

\```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/livewire.py check --universe-sync                      # Full sync
python scripts/livewire.py check --universe-sync --dry-run            # Report only
python scripts/livewire.py check --universe-sync --skip-dead          # Skip Polygon check
python scripts/livewire.py check --universe-sync --interests TSLA GME # Add to personal watchlist
python scripts/livewire_ingest.py universe-sync --dry-run             # Via legacy CLI
\```

**Data sources:**
| Index | Source | Fallback |
|-------|--------|----------|
| S&P 500 | Wikipedia | — |
| Nasdaq-100 | Wikipedia | — |
| Russell 2000 | Slickcharts | — |
| Ticker status | Polygon `/v3/reference/tickers/{ticker}` | Skipped if no `MASSIVE_API_KEY` |

**Tag registry** at `~/market-warehouse/registry.json` tracks per-ticker index membership. Tags: `sp500`, `ndx100`, `r2k`, `interest`. Query via `TagRegistry.by_tag("sp500")`.

**Movements detected:**
- **add** — new constituent not previously tracked
- **remove** — dropped from all indexes
- **promotion** — moved to a higher-tier index (e.g., R2K → S&P 500)
- **demotion** — moved to a lower-tier index (e.g., S&P 500 → R2K)

**Dead ticker handling:** If `MASSIVE_API_KEY` is set, removed tickers are checked via Polygon. Confirmed delisted tickers are archived to `bronze-delisted/` and marked `status: delisted` in the registry.

**Interests preset:** `presets/interests.json` is a personal watchlist. Add tickers via `--interests TSLA GME`. Interest tags are never auto-removed by sync.
```

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml CLAUDE.md
git commit -m "docs: document universe sync, tag registry, and interests preset"
```

---

## Part 2: Gap-Based Backfill Completion

**Goal:** Each ticker's full daily history is fetched once. Gaps are detected and filled. Nothing is re-queried.

**Problem:** The current backfill doesn't know "what's the full expected range" — it just keeps asking for older data. When IB returns nothing, the ticker stays incomplete and gets retried every `backfill --full` run. ~630+ such tickers waste hours per full build.

**Architecture:** For each ticker, determine the earliest available date (IB head timestamp or Polygon `list_date`). Cache it in the tag registry. Compare expected trading days against bronze parquet. Fill only the gaps. A ticker is "complete" when its bronze covers from `earliest_available` to today with no gaps — derived from data, not cursor state.

### How completion works

```
For ticker AAPL:
  earliest_available = 1993-01-29  (from IB head timestamp, cached in registry)
  bronze dates       = {1993-01-29, 1993-02-01, ..., 2026-05-30}  (from parquet)
  expected           = NYSE trading days from 1993-01-29 to today
  gaps               = expected - bronze dates

  gaps == {} → COMPLETE, never touch again
  gaps == {2024-12-24, 2025-01-02} → fill those 2 days, then complete
```

### Resumable backfill via frontier tracking

The cursor tracks a **frontier date** — the oldest date the backfill has reached so far. If the process is interrupted, the next run resumes from the frontier instead of starting over from `oldest_bronze_date`.

```
NVDA: earliest_available=1999-01-22, bronze=2015-03-01→2026-05-30
      backfill was fetching older windows backward from 2015-03-01...
      merged down to 2008-06-15, then process killed

      cursor: { "status": "in_progress", "frontier_date": "2008-06-15" }

      next run: fetch 1999-01-22 → 2008-06-15 (remaining gap only)
      without frontier: would re-fetch 1999-01-22 → 2015-03-01 (wastes ~7yr of re-fetch)
```

Cursor entry per ticker:
```json
"NVDA": {
  "1d": {
    "status": "in_progress",
    "frontier_date": "2008-06-15",
    "earliest_available": "1999-01-22",
    "oldest_bronze": "2015-03-01"
  }
}
```

Status values:
- `done` — full range covered or IB confirmed no older data. Skip on future runs.
- `in_progress` — frontier is set, resume from there.
- Missing entry — not started yet, begin from `oldest_bronze_date`.

The frontier is updated after each batch of windows is merged into bronze (not after each individual window — the parquet merge is the durable checkpoint).

### Bar-count validation as completion signal

After every merge, run a quick check: does bronze have all expected trading days?

```python
expected = trading_days(earliest_available, today)  # set of dates
actual   = bronze_trade_dates(ticker)                # set of dates from parquet
if len(actual) >= len(expected):
    status = "done"  # complete — skip on future runs
```

This is the **primary** completion signal. It catches:
- Normal backfills that completed in one shot
- Interrupted backfills that resumed and finished across multiple runs
- Manual data imports or Massive-sourced fills
- Any other path that puts bars into bronze

The frontier is the **resume mechanism**. The bar count is the **completion check**. They work together:

```
After merge:
  1. Update frontier to oldest bar just inserted
  2. Re-read bronze dates for this ticker
  3. Compare against expected trading days from earliest_available
  4. If actual >= expected → mark done (even if frontier hasn't reached earliest_available)
```

This means a ticker backfilled via Massive (which may fill gaps in the middle, not just extend backward) gets marked done as soon as coverage is complete — no frontier math needed.

### What about tickers where IB genuinely has no older data?

The `_safe_fetch` problem: currently returns `(ticker, [])` for both "no data" and "fetch error" — can't tell them apart.

**Fix:** Return `None` on error instead of `[]`.

```python
# Before:
except (IBError, Exception):
    return (ticker, [])     # error and no-data look identical

# After:
except (IBError, Exception):
    return (ticker, None)   # None = error, [] = legitimate empty
```

Then in the batch loop:
- `bars is None` → error, don't update frontier, retry
- `bars == []` → clean empty, mark `done` (no older history exists)
- `bars has data` → merge, update frontier, check if we've reached `earliest_available`

---

### Task 6: Cache Earliest Available Date in Registry

**Files:**
- Modify: `clients/tag_registry.py` — add `earliest_available` field to `RegistryEntry`
- Modify: `clients/universe_client.py` — add `get_ticker_list_date()` using Polygon reference
- Modify: `tests/test_tag_registry.py` — test earliest_available persistence
- Modify: `tests/test_universe_client.py` — test list_date fetch

The registry (from Task 2) stores per-ticker metadata. Adding `earliest_available` lets gap detection work without re-fetching head timestamps every run.

- [ ] **Step 1: Add `earliest_available` to RegistryEntry**

```python
@dataclass
class RegistryEntry:
    tags: set[str] = field(default_factory=set)
    status: str = "active"
    added_at: Optional[str] = None
    last_verified: Optional[str] = None
    delisted_at: Optional[str] = None
    earliest_available: Optional[str] = None  # ISO date, from IB head_timestamp or Polygon list_date
    earliest_source: Optional[str] = None     # "ib" or "polygon"
```

Update `_load()` and `save()` to persist these fields.

- [ ] **Step 2: Write test for earliest_available round-trip**

```python
def test_earliest_available_persists(self, tmp_path):
    reg = TagRegistry(tmp_path / "r.json")
    reg.set_tags("AAPL", {"sp500"}, status="active")
    reg.set_earliest("AAPL", "1993-01-29", source="ib")
    reg.save()

    reg2 = TagRegistry(tmp_path / "r.json")
    assert reg2.get("AAPL").earliest_available == "1993-01-29"
    assert reg2.get("AAPL").earliest_source == "ib"
```

- [ ] **Step 3: Add `set_earliest` method**

```python
def set_earliest(self, ticker: str, date_str: str, source: str = "ib") -> None:
    entry = self._entries.get(ticker)
    if entry:
        entry.earliest_available = date_str
        entry.earliest_source = source
```

- [ ] **Step 4: Run tests → PASS**
- [ ] **Step 5: Commit**

```bash
git add clients/tag_registry.py tests/test_tag_registry.py
git commit -m "feat: add earliest_available to tag registry"
```

---

### Task 7: Gap Detection (`livewire check --gaps`)

**Files:**
- Create: `livewire_scripts/check_gaps.py` — gap detection and reporting
- Create: `tests/test_check_gaps.py`
- Modify: `scripts/livewire.py` — add `--gaps` to check command
- Uses: `clients/trading_calendar.py` (`is_trading_day` at line 86), `BronzeClient.get_trade_dates_by_symbol()`

- [ ] **Step 1: Write failing tests for gap detection**

```python
"""Tests for livewire_scripts/check_gaps.py."""

from datetime import date
import pytest
from livewire_scripts.check_gaps import compute_gaps, GapReport


class TestComputeGaps:
    def test_no_gaps(self):
        # Mon-Fri week, all present
        bronze_dates = {date(2026, 5, 25), date(2026, 5, 26), date(2026, 5, 27),
                        date(2026, 5, 28), date(2026, 5, 29)}
        report = compute_gaps("AAPL", "2026-05-25", bronze_dates, as_of=date(2026, 5, 29))
        assert report.gap_count == 0
        assert report.complete is True

    def test_with_gaps(self):
        bronze_dates = {date(2026, 5, 25), date(2026, 5, 27), date(2026, 5, 29)}
        report = compute_gaps("AAPL", "2026-05-25", bronze_dates, as_of=date(2026, 5, 29))
        assert report.gap_count == 2  # Tue + Thu missing
        assert date(2026, 5, 26) in report.missing_dates

    def test_no_earliest_returns_unknown(self):
        report = compute_gaps("AAPL", None, set(), as_of=date(2026, 5, 29))
        assert report.complete is False
        assert report.earliest_available is None

    def test_skips_weekends_and_holidays(self):
        # Sat + Sun not counted as gaps
        bronze_dates = {date(2026, 5, 29)}  # Friday only
        report = compute_gaps("AAPL", "2026-05-29", bronze_dates, as_of=date(2026, 5, 31))
        assert report.gap_count == 0  # Sat+Sun not gaps
```

- [ ] **Step 2: Implement gap detection**

```python
"""Gap detection — compare expected trading days vs bronze parquet."""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from rich.console import Console
from rich.table import Table

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients import BronzeClient
from clients.tag_registry import TagRegistry
from clients.trading_calendar import is_trading_day

log = logging.getLogger(__name__)
console = Console()

_WAREHOUSE_DIR = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))


@dataclass
class GapReport:
    ticker: str
    earliest_available: str | None
    bronze_start: str | None
    bronze_end: str | None
    bronze_count: int
    expected_count: int
    gap_count: int
    missing_dates: list[date] = field(default_factory=list)
    complete: bool = False


def _trading_days_in_range(start: date, end: date) -> set[date]:
    """Return all NYSE trading days from start to end (inclusive)."""
    days: set[date] = set()
    d = start
    while d <= end:
        if is_trading_day(d):
            days.add(d)
        d += timedelta(days=1)
    return days


def compute_gaps(
    ticker: str,
    earliest_available: str | None,
    bronze_dates: set[date],
    as_of: date | None = None,
) -> GapReport:
    today = as_of or date.today()

    if not earliest_available:
        return GapReport(
            ticker=ticker, earliest_available=None,
            bronze_start=min(bronze_dates).isoformat() if bronze_dates else None,
            bronze_end=max(bronze_dates).isoformat() if bronze_dates else None,
            bronze_count=len(bronze_dates), expected_count=0,
            gap_count=0, complete=False,
        )

    start = date.fromisoformat(earliest_available)
    expected = _trading_days_in_range(start, today)
    missing = sorted(expected - bronze_dates)

    return GapReport(
        ticker=ticker,
        earliest_available=earliest_available,
        bronze_start=min(bronze_dates).isoformat() if bronze_dates else None,
        bronze_end=max(bronze_dates).isoformat() if bronze_dates else None,
        bronze_count=len(bronze_dates),
        expected_count=len(expected),
        gap_count=len(missing),
        missing_dates=missing,
        complete=len(missing) == 0,
    )
```

- [ ] **Step 3: Add CLI main and rich table output**

```python
def main(argv: list[str] | None = None) -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Gap detection for bronze parquet")
    parser.add_argument("--preset", type=str, default=None, help="Limit to preset tickers")
    parser.add_argument("--show-gaps", action="store_true", help="Show individual missing dates")
    parser.add_argument("--incomplete-only", action="store_true", help="Only show tickers with gaps")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)

    registry = TagRegistry(_WAREHOUSE_DIR / "registry.json")
    bronze_dir = _WAREHOUSE_DIR / "data-lake" / "bronze" / "asset_class=equity"
    bronze = BronzeClient(bronze_dir=bronze_dir)
    all_dates = bronze.get_trade_dates_by_symbol()

    tickers = sorted(all_dates.keys())
    if args.preset:
        import json
        with open(args.preset) as f:
            preset_tickers = set(json.load(f).get("tickers", []))
        tickers = [t for t in tickers if t in preset_tickers]

    table = Table(title="Gap Report")
    table.add_column("Ticker", style="bold")
    table.add_column("Earliest", justify="right")
    table.add_column("Bronze", justify="right")
    table.add_column("Expected", justify="right")
    table.add_column("Gaps", justify="right")
    table.add_column("Status")

    n_complete, n_gaps, n_unknown = 0, 0, 0
    for ticker in tickers:
        entry = registry.get(ticker)
        earliest = entry.earliest_available if entry else None
        report = compute_gaps(ticker, earliest, set(all_dates.get(ticker, [])))

        if args.incomplete_only and report.complete:
            continue

        if report.complete:
            n_complete += 1
            status = "[green]complete[/green]"
        elif earliest is None:
            n_unknown += 1
            status = "[dim]no bounds[/dim]"
        else:
            n_gaps += 1
            status = f"[yellow]{report.gap_count} gaps[/yellow]"

        table.add_row(
            ticker, earliest or "?",
            f"{report.bronze_count:,}", f"{report.expected_count:,}",
            str(report.gap_count), status,
        )

    console.print(table)
    console.print(f"\n[bold]{n_complete} complete, {n_gaps} with gaps, {n_unknown} no bounds[/bold]")
```

- [ ] **Step 4: Wire into CLI**

Add to `CHECK_MODULES` in `scripts/livewire.py`:
```python
"gaps": "livewire_scripts.check_gaps",
```

Add `--gaps` flag to `_dispatch_check`.

- [ ] **Step 5: Run tests → PASS**
- [ ] **Step 6: Commit**

```bash
git add livewire_scripts/check_gaps.py tests/test_check_gaps.py scripts/livewire.py
git commit -m "feat: add gap detection for bronze parquet completeness"
```

---

### Task 8: Fix `_safe_fetch` + Gap-Fill Backfill

**Files:**
- Modify: `livewire_scripts/fetch_ib_historical.py` — `_safe_fetch` (line 448), backfill batch loop (line 931)
- Modify: `tests/test_fetch_ib_historical.py`

**Verified:** `_safe_fetch` (line 448) swallows all exceptions and returns `(ticker, [])`. The batch loop can't distinguish "no data" from "error." Three call sites consume `ticker_bars`: seed path (~line 780), IB backfill (~line 931), Massive backfill (~line 1049). `backfill_intraday.py` has its own `_safe_fetch` — not affected.

- [ ] **Step 1: Change `_safe_fetch` to return `None` on error**

```python
# Line 448, change:
except (IBError, Exception) as exc:
    console.print(f"    [red]{ticker}: {type(exc).__name__} — {exc}[/red]")
    return (ticker, None)  # was (ticker, [])
```

- [ ] **Step 2: Update all three consumers of ticker_bars**

Seed path (~line 780):
```python
bars = ticker_bars.get(ticker)
if bars is None:
    console.print(f"  [yellow]{ticker}[/yellow]: fetch error (will retry)")
    batch_fail += 1
    continue
```

IB backfill batch loop (~line 931) — with frontier tracking:
```python
bars = ticker_bars.get(ticker)
if bars is None:
    # Transient error — don't update frontier, retry next run
    console.print(f"  [yellow]{ticker}[/yellow]: fetch error (will retry)")
    batch_fail += 1
    continue

count = backfill_ticker(ticker, bars, bronze, asset_class=asset_class)

if count > 0:
    # Merged older rows — update frontier, then validate completeness
    oldest_inserted = min(str(b.date)[:10] for b in bars) if bars else None
    earliest = head_timestamps.get(ticker)

    # Bar-count validation: is bronze now complete?
    if earliest and _is_coverage_complete(ticker, earliest, bronze):
        completed.setdefault(ticker, {})["1d"] = {"status": "done"}
        console.print(f"  [green]{ticker}[/green]: {count:,} rows, coverage complete ✓")
    else:
        completed.setdefault(ticker, {})["1d"] = {
            "status": "in_progress",
            "frontier_date": oldest_inserted,
            "earliest_available": earliest,
        }
        console.print(f"  [green]{ticker}[/green]: {count:,} rows, frontier → {oldest_inserted}")
    save_cursor(cursor_name, completed, started_at)
    batch_ok += 1
else:
    # Clean fetch, zero rows — IB has no older data. Done.
    completed.setdefault(ticker, {})["1d"] = {"status": "done"}
    save_cursor(cursor_name, completed, started_at)
    console.print(f"  [dim]{ticker}[/dim]: no older history (done)")
    batch_ok += 1
```

Massive backfill path (~line 1049): Massive returns 200 with 0 bars for delisted/unknown tickers (never 404). Same `bars == []` → done logic applies:
```python
try:
    bars = massive.get_daily_bars(ticker, start, end)
except MassiveAPIError:
    bars = None  # transient error — retry
# bars == [] from 200 OK → clean no-data, same as IB empty
# The batch loop's existing if/else handles this correctly
```

- [ ] **Step 3: Update backfill to resume from frontier**

When determining the backfill range for a ticker, check the cursor for an existing frontier:
```python
# Before: always backfill from earliest → oldest_bronze_date
# After: resume from earliest → frontier_date (if exists)
entry = completed.get(ticker, {}).get("1d", {})
if entry.get("status") == "in_progress" and entry.get("frontier_date"):
    backfill_end = entry["frontier_date"]  # resume from where we stopped
else:
    backfill_end = oldest_dates.get(ticker)  # first run: from oldest bronze date
```

- [ ] **Step 4: Add `_is_coverage_complete` helper**

```python
def _is_coverage_complete(ticker: str, earliest_available: str, bronze: BronzeClient) -> bool:
    """Quick check: does bronze have all expected trading days for this ticker?"""
    from livewire_scripts.check_gaps import compute_gaps

    dates_by_sym = bronze.get_trade_dates_by_symbol()
    bronze_dates = set(dates_by_sym.get(ticker, []))
    report = compute_gaps(ticker, earliest_available, bronze_dates)
    return report.complete
```

- [ ] **Step 5: Write tests**

```python
class TestSafeFetchErrorSignal:
    def test_none_on_error(self):
        """_safe_fetch returns None for bars on exception."""

    def test_empty_list_on_clean_no_data(self):
        """_safe_fetch returns [] when IB has no data (clean execution)."""

    def test_backfill_marks_done_on_empty_bars(self):
        """Backfill loop marks ticker done when bars == [] (not None)."""

    def test_backfill_retries_on_none_bars(self):
        """Backfill loop does NOT mark done when bars is None."""

    def test_massive_empty_200_marks_done(self):
        """Massive returns 200 with 0 bars for delisted ticker → mark done."""

class TestFrontierTracking:
    def test_frontier_updates_after_merge(self):
        """After merging older bars, frontier_date is set to oldest inserted bar."""

    def test_resume_from_frontier(self):
        """Next backfill run starts from frontier_date, not oldest_bronze."""

    def test_interrupted_backfill_preserves_frontier(self):
        """If process stops between batches, frontier from last save is used."""


class TestBarCountValidation:
    def test_complete_coverage_marks_done(self):
        """When bronze has all expected trading days, status = done."""

    def test_incomplete_coverage_stays_in_progress(self):
        """When bronze is missing trading days, status = in_progress with frontier."""

    def test_validation_runs_after_every_merge(self):
        """Bar count check runs after each ticker merge, not just at end."""

    def test_no_earliest_available_skips_validation(self):
        """Without earliest_available, validation is skipped (frontier only)."""
```

- [ ] **Step 6: Run tests**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/test_fetch_ib_historical.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add livewire_scripts/fetch_ib_historical.py tests/test_fetch_ib_historical.py
git commit -m "feat: distinguish fetch error from no-data, mark complete on clean empty"
```

---

### Task 9: Populate Bounds During Backfill

**Files:**
- Modify: `livewire_scripts/fetch_ib_historical.py` — cache head_timestamp to registry after fetch
- Modify: `livewire_scripts/universe_sync.py` — populate `list_date` from Polygon during sync

The head timestamp is already fetched in `_fetch_single_ticker` (line 342). Cache it so gap detection works without re-fetching.

- [ ] **Step 1: Cache IB head_timestamp to registry**

In `_fetch_single_ticker`, after resolving `head_dt` (line 351):
```python
# Already computed: head_dt (the earliest available date for this ticker)
# Cache it to registry if available
_cache_earliest(ticker, head_dt)
```

Collect head timestamps during the fetch phase, then batch-write to registry once after all tickers:

```python
# In _fetch_single_ticker, stash head_dt in a module-level dict for later caching:
_head_timestamps: dict[str, str] = {}

# After resolving head_dt (line 351):
_head_timestamps[ticker] = head_dt.strftime("%Y-%m-%d")

# After the fetch_all_tickers gather completes, batch-write to registry:
def _cache_head_timestamps_to_registry(timestamps: dict[str, str]) -> None:
    """Batch-write head timestamps to registry. Called once per backfill run."""
    try:
        reg = TagRegistry(_WAREHOUSE_DIR / "registry.json")
        updated = 0
        for ticker, date_str in timestamps.items():
            entry = reg.get(ticker)
            if entry and not entry.earliest_available:
                reg.set_earliest(ticker, date_str, source="ib")
                updated += 1
        if updated:
            reg.save()
            log.info("Cached %d head timestamps to registry", updated)
    except Exception:
        pass
```

- [ ] **Step 2: Populate list_date from Polygon during universe-sync**

In `universe_sync.py`, during the dead-ticker Polygon check (which already calls `check_ticker_status`), also store `list_date`:

```python
for ticker, status in statuses.items():
    if status.list_date:
        registry.set_earliest(ticker, status.list_date, source="polygon")
    if not status.active:
        registry.mark_delisted(ticker, delisted_at=status.delisted_utc)
```

- [ ] **Step 3: Write test**

```python
def test_backfill_caches_head_timestamp(self, tmp_path, monkeypatch):
    """After IB fetch, earliest_available is saved to registry."""
```

- [ ] **Step 4: Run tests → PASS**
- [ ] **Step 5: Commit**

```bash
git add livewire_scripts/fetch_ib_historical.py livewire_scripts/universe_sync.py tests/
git commit -m "feat: cache earliest_available from IB head_timestamp and Polygon list_date"
```

---

### Task 10: Coverage Gate + CLAUDE.md Update

- [ ] **Step 1: Run full test suite with coverage**

Run: `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests/ -v --cov=clients --cov=livewire_scripts --cov-report=term-missing -W error::RuntimeWarning`
Expected: 100% on included sources

- [ ] **Step 2: Update CLAUDE.md**

Document: universe sync, tag registry, gap detection, backfill completion model.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml CLAUDE.md
git commit -m "docs: document universe sync + gap-based backfill completion"
```

---

## Dependency Notes

- **lxml** + **cssselect**: Required for HTML table parsing. Install via `pip install lxml cssselect` in `~/market-warehouse/.venv/`.
- **responses**: Already in test deps for HTTP mocking.
- **No new runtime deps** beyond `lxml`/`cssselect` — everything else (`requests`, `rich`, `json`) is already available.

## Summary of CLI Surface

```
COMMAND                                                    ACTION
──────────────────────────────────────────────────         ──────────
livewire check --universe-sync                             Sync index constituents + detect dead tickers
livewire check --universe-sync --dry-run                   Report changes only
livewire check --universe-sync --skip-dead                 Skip Polygon dead-ticker check
livewire check --universe-sync --interests TSLA GME        Add to personal watchlist
livewire check --gaps                                      Show gap report per ticker
livewire check --gaps --incomplete-only                    Only show tickers with gaps
livewire check --gaps --preset presets/sp500.json          Limit to preset
```

## How the pieces fit together

```
┌─────────────────────────────────────────────────────────┐
│  livewire check --universe-sync                         │
│                                                         │
│  1. Fetch live constituents (Wikipedia/Slickcharts)     │
│  2. Diff against registry → promotions/demotions/adds   │
│  3. Polygon: confirm dead tickers + cache list_date     │
│  4. Update preset files + registry                      │
└──────────────────────┬──────────────────────────────────┘
                       │ registry has: tags + earliest_available
                       ▼
┌─────────────────────────────────────────────────────────┐
│  livewire backfill --full                               │
│                                                         │
│  1. IB head_timestamp → cache to registry               │
│  2. Fetch bars (seed or backfill)                       │
│  3. _safe_fetch: None=error, []=no-data                 │
│  4. bars=None → retry.  bars=[] → done (no older data)  │
│  5. bars=[data] → merge to bronze → done                │
└──────────────────────┬──────────────────────────────────┘
                       │ bronze has: actual trade dates
                       ▼
┌─────────────────────────────────────────────────────────┐
│  livewire check --gaps                                  │
│                                                         │
│  expected = trading_days(earliest_available → today)     │
│  actual   = bronze trade_date column                    │
│  gaps     = expected - actual                           │
│                                                         │
│  gaps == {} → complete, never touch again               │
│  gaps != {} → fill with targeted fetch                  │
└─────────────────────────────────────────────────────────┘
```

## Registry Query Examples

```python
from clients import TagRegistry
from pathlib import Path

reg = TagRegistry(Path.home() / "market-warehouse" / "registry.json")

# All S&P 500 tickers
sp500 = reg.by_tag("sp500")

# Tickers in both S&P 500 and Nasdaq-100
overlap = reg.by_tags({"sp500", "ndx100"})

# Personal watchlist
interests = reg.by_tag("interest")

# Tickers with known bounds (for gap detection)
with_bounds = {t for t in reg.all_tickers() if reg.get(t).earliest_available}
```

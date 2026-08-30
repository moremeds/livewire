"""Tests for clients/universe_client.py."""

from __future__ import annotations

from urllib.parse import quote

import pytest
import responses

from clients.universe_client import (
    UniverseFetchError,
    check_ticker_status,
    check_tickers_bulk,
    fetch_ndx100,
    fetch_r2k,
    fetch_sp500,
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

MEDIAWIKI_ROOT = "https://en.wikipedia.org/w/rest.php/v1/page"


def mediawiki_url(title: str) -> str:
    return f"{MEDIAWIKI_ROOT}/{quote(title.replace(' ', '_'), safe='')}/html"


def mediawiki_payload(
    content: str,
    *,
    title: str = "List of S&P 500 companies",
    canonical_url: str = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
) -> bytes:
    return f"""<!DOCTYPE html>
    <html about="//en.wikipedia.org/wiki/Special:Redirect/revision/123">
      <head>
        <meta property="dc:modified" content="2026-08-30T12:00:00Z" />
        <link rel="dc:isVersionOf" href="{canonical_url}" />
        <title>{title}</title>
      </head>
      <body>{content}</body>
    </html>""".encode()


class TestFetchSP500:
    @responses.activate
    def test_parses_wikipedia_table(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        responses.add(
            responses.GET,
            mediawiki_url("List of S&P 500 companies"),
            body=mediawiki_payload(SP500_HTML),
            status=200,
        )
        result = fetch_sp500()
        assert result == {"AAPL", "MSFT", "BRK.B"}
        assert list((tmp_path / "raw" / "shepherd" / "sha256").glob("[0-9a-f]" * 64))

    @responses.activate
    def test_http_error_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        responses.add(
            responses.GET,
            mediawiki_url("List of S&P 500 companies"),
            status=500,
        )
        with pytest.raises(UniverseFetchError, match="S&P 500"):
            fetch_sp500()

    @responses.activate
    def test_fallback_to_wikitable_class(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
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
            mediawiki_url("List of S&P 500 companies"),
            body=mediawiki_payload(fallback_html),
            status=200,
        )
        result = fetch_sp500()
        assert result == {"GOOG"}

    @responses.activate
    def test_no_table_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        responses.add(
            responses.GET,
            mediawiki_url("List of S&P 500 companies"),
            body=mediawiki_payload("<p>No tables here</p>"),
            status=200,
        )
        with pytest.raises(UniverseFetchError, match="no constituent table"):
            fetch_sp500()


class TestFetchNDX100:
    @responses.activate
    def test_parses_wikipedia_table(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        responses.add(
            responses.GET,
            mediawiki_url("Nasdaq-100"),
            body=mediawiki_payload(
                NDX100_HTML,
                title="Nasdaq-100",
                canonical_url="https://en.wikipedia.org/wiki/Nasdaq-100",
            ),
            status=200,
        )
        result = fetch_ndx100()
        assert result == {"AAPL", "NVDA"}

    @responses.activate
    def test_http_error_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        responses.add(
            responses.GET,
            mediawiki_url("Nasdaq-100"),
            status=404,
        )
        with pytest.raises(UniverseFetchError, match="Nasdaq-100"):
            fetch_ndx100()

    @responses.activate
    def test_no_table_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        responses.add(
            responses.GET,
            mediawiki_url("Nasdaq-100"),
            body=mediawiki_payload(
                "<p>No table</p>",
                title="Nasdaq-100",
                canonical_url="https://en.wikipedia.org/wiki/Nasdaq-100",
            ),
            status=200,
        )
        with pytest.raises(UniverseFetchError, match="no constituent table"):
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

    @responses.activate
    def test_no_table_raises(self):
        responses.add(
            responses.GET,
            "https://www.slickcharts.com/russell2000",
            body="<html><body></body></html>",
            status=200,
        )
        with pytest.raises(UniverseFetchError, match="no constituent table"):
            fetch_r2k()


class TestFetchNDX100Fallback:
    @responses.activate
    def test_fallback_to_wikitable_class(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        fallback_html = """
        <html><body>
        <table class="wikitable sortable">
        <thead><tr><th>Ticker</th><th>Company</th></tr></thead>
        <tbody>
        <tr><td>NVDA</td><td>NVIDIA</td></tr>
        </tbody>
        </table>
        </body></html>
        """
        responses.add(
            responses.GET,
            mediawiki_url("Nasdaq-100"),
            body=mediawiki_payload(
                fallback_html,
                title="Nasdaq-100",
                canonical_url="https://en.wikipedia.org/wiki/Nasdaq-100",
            ),
            status=200,
        )
        result = fetch_ndx100()
        assert result == {"NVDA"}


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

    def test_connection_error_raises(self, monkeypatch):
        import requests as req_lib

        def mock_get(*args, **kwargs):
            raise req_lib.exceptions.ConnectionError("network down")

        monkeypatch.setattr(req_lib, "get", mock_get)
        with pytest.raises(UniverseFetchError, match="AAPL"):
            check_ticker_status("AAPL", api_key="test-key")

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

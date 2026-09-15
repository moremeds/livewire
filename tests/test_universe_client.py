"""Tests for clients/universe_client.py."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote

import pytest
import responses

from clients.universe_client import (
    NDX100_WIKIPEDIA_TITLE,
    UniverseFetchError,
    check_ticker_status,
    check_tickers_bulk,
    fetch_djia,
    fetch_ndx100,
    fetch_r2k,
    fetch_sp500,
    fetch_ticker_identity,
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
        assert list((tmp_path / "raw" / "shepherd" / "sha256").glob("[0-9a-f][0-9a-f]/[0-9a-f][0-9a-f]/*"))

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
    def test_the_title_names_the_article_that_still_carries_the_table(self):
        """Measured 2026-09-02: the `Nasdaq-100` article's Components section was
        split out, so the old title fetched 200 OK and parsed to nothing --
        universe-sync exited 1 every run and the ndx100 half of the denominator
        never refreshed. Every other test here mocks whatever URL the code asks
        for, so none of them could see it. `List of Nasdaq-100 companies` is a
        404; only this capitalisation resolves."""
        assert NDX100_WIKIPEDIA_TITLE == "List of NASDAQ-100 companies"

    @responses.activate
    def test_parses_wikipedia_table(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        responses.add(
            responses.GET,
            mediawiki_url(NDX100_WIKIPEDIA_TITLE),
            body=mediawiki_payload(
                NDX100_HTML,
                title=NDX100_WIKIPEDIA_TITLE,
                canonical_url="https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies",
            ),
            status=200,
        )
        result = fetch_ndx100()
        assert result == {"AAPL", "NVDA"}

    @responses.activate
    def test_ignores_unrelated_wikitables_before_semantic_constituent_table(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        content = NDX100_HTML.replace(
            '<table id="constituents">',
            """<table class="wikitable"><tbody><tr><th>Category</th><th>All-Time Highs</th></tr>
            <tr><td>Closing</td><td>30,000</td></tr></tbody></table>
            <table class="wikitable">""",
        )
        responses.add(
            responses.GET,
            mediawiki_url(NDX100_WIKIPEDIA_TITLE),
            body=mediawiki_payload(
                content,
                title=NDX100_WIKIPEDIA_TITLE,
                canonical_url="https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies",
            ),
            status=200,
        )
        assert fetch_ndx100() == {"AAPL", "NVDA"}

    @responses.activate
    def test_http_error_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        responses.add(
            responses.GET,
            mediawiki_url(NDX100_WIKIPEDIA_TITLE),
            status=404,
        )
        with pytest.raises(UniverseFetchError, match="Nasdaq-100"):
            fetch_ndx100()

    @responses.activate
    def test_no_table_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MDW_DATA_LAKE", str(tmp_path))
        responses.add(
            responses.GET,
            mediawiki_url(NDX100_WIKIPEDIA_TITLE),
            body=mediawiki_payload(
                "<p>No table</p>",
                title=NDX100_WIKIPEDIA_TITLE,
                canonical_url="https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies",
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


DJIA_HTML = (
    "<html><body><table class='table table-hover table-borderless table-sm'>"
    "<thead><tr><th>No.</th><th>Company</th><th>Symbol</th></tr></thead><tbody>"
    + "".join(f"<tr><td>{i}</td><td>Co {i}</td><td>DJ{i:02d}</td></tr>" for i in range(1, 31))
    + "</tbody></table></body></html>"
)


class TestFetchDJIA:
    @responses.activate
    def test_parses_slickcharts_table(self):
        responses.add(
            responses.GET,
            "https://www.slickcharts.com/dowjones",
            body=DJIA_HTML,
            status=200,
        )
        assert fetch_djia() == {f"DJ{i:02d}" for i in range(1, 31)}

    @responses.activate
    def test_http_error_raises(self):
        responses.add(
            responses.GET,
            "https://www.slickcharts.com/dowjones",
            status=403,
        )
        with pytest.raises(UniverseFetchError, match="DJIA"):
            fetch_djia()

    @responses.activate
    def test_no_table_raises(self):
        responses.add(
            responses.GET,
            "https://www.slickcharts.com/dowjones",
            body="<html><body></body></html>",
            status=200,
        )
        with pytest.raises(UniverseFetchError, match="no constituent table"):
            fetch_djia()

    @responses.activate
    def test_below_thirty_rows_fails_closed(self):
        # A partial parse means the table markup broke — a truncated set must
        # never diff against the membership store.
        responses.add(
            responses.GET,
            "https://www.slickcharts.com/dowjones",
            body=R2K_HTML,  # same table shape, only 2 rows
            status=200,
        )
        with pytest.raises(UniverseFetchError, match="below 30"):
            fetch_djia()


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
            mediawiki_url(NDX100_WIKIPEDIA_TITLE),
            body=mediawiki_payload(
                fallback_html,
                title=NDX100_WIKIPEDIA_TITLE,
                canonical_url="https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies",
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


# ── Frozen Massive reference bodies (tests/fixtures/massive_reference) ──────

MASSIVE_REFERENCE = Path(__file__).parent / "fixtures" / "massive_reference"
REFERENCE_URL = "https://api.polygon.io/v3/reference/tickers"


def _fixture(name: str) -> bytes:
    """Load one frozen real Massive response body by file name."""
    return (MASSIVE_REFERENCE / name).read_bytes()


class TestFetchTickerIdentity:
    @responses.activate
    def test_pace_fn_runs_before_every_request_not_once_per_ticker(self):
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("aapl-active-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("aapl-date-2010-01-04-2026-09-15.json"), status=200)
        paced: list[int] = []

        fetch_ticker_identity(
            "AAPL", api_key="test-key", probe_date="2010-01-04", pace_fn=lambda: paced.append(len(responses.calls))
        )

        # called before request 1, 2 and 3: the request count seen at each pace
        assert paced == [0, 1, 2]

    @responses.activate
    def test_a_listed_ticker_still_gets_the_date_probe_because_the_list_endpoint_has_no_list_date(self):
        # Measured 2026-09-15: no body from this endpoint carries list_date
        # (fixtures README F1), so the probe runs for every ticker, listed or
        # not, and existed_at is the only start date available.
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("aapl-active-2026-09-15.json"), status=200)
        # AAPL is listed, so the active=false page is an empty envelope; dell's
        # frozen body is that same empty envelope.
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("aapl-date-2010-01-04-2026-09-15.json"), status=200)

        result = fetch_ticker_identity("AAPL", api_key="test-key", probe_date="2010-01-04")

        # three calls: active, active=false, then the date probe.
        assert len(responses.calls) == 3
        assert responses.calls[0].request.params.get("active") is None
        assert responses.calls[1].request.params["active"] == "false"
        assert "date" not in responses.calls[1].request.params
        # The probe sends ticker and date only: date= with active=false comes
        # back empty even for a ticker that existed on that date (README F3).
        assert set(responses.calls[2].request.params) == {"ticker", "date", "apiKey"}
        assert responses.calls[2].request.params["date"] == "2010-01-04"
        assert len(result.responses) == 3
        # The probe body omits primary_exchange (README F4), so it is the same
        # listing seen with fewer fields, not a second listing: it merges onto
        # the active record and stamps it existed_at.
        assert len(result.records) == 1
        record = result.records[0]
        assert record.ticker == "AAPL"
        assert record.list_date is None  # the field exists in the spec; this endpoint never fills it
        assert record.existed_at == "2010-01-04"
        assert record.mic == "XNAS"  # primary_exchange is already a MIC, used as-is
        assert record.currency == "USD"  # currency_name uppercased

    @responses.activate
    def test_the_date_probe_runs_when_no_record_carries_a_list_date_which_is_always(self):
        # Both listing calls empty, so the date probe is the third call and its
        # record is stamped existed_at. No record ever carries list_date
        # (README F1), so the guard never suppresses the probe in practice.
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("aapl-date-2010-01-04-2026-09-15.json"), status=200)

        result = fetch_ticker_identity("DELL", api_key="test-key", probe_date="2010-01-04")

        assert len(responses.calls) == 3
        assert responses.calls[2].request.params["date"] == "2010-01-04"
        assert len(result.responses) == 3
        assert [record.existed_at for record in result.records] == ["2010-01-04"]

    @responses.activate
    def test_without_a_probe_date_there_is_no_third_call(self):
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)

        result = fetch_ticker_identity("DELL", api_key="test-key")

        assert len(responses.calls) == 2
        assert result.records == []

    @responses.activate
    def test_a_ticker_unknown_to_massive_is_an_empty_list_not_an_exception(self):
        for _ in range(3):
            responses.add(responses.GET, REFERENCE_URL, body=_fixture("aamrq-active-false-2026-09-15.json"), status=200)

        result = fetch_ticker_identity("AAMRQ", api_key="test-key", probe_date="2010-01-04")

        assert result.records == []
        assert len(result.responses) == 3
        # An empty envelope from this endpoint has no `count` key at all
        # (README F2): emptiness is decided from `results`, never from `count`.
        assert "count" not in json.loads(result.responses[0])

    @responses.activate
    def test_a_404_is_an_empty_result_not_an_exception(self):
        responses.add(responses.GET, REFERENCE_URL, status=404)
        responses.add(responses.GET, REFERENCE_URL, status=404)

        assert fetch_ticker_identity("NOSUCH", api_key="test-key").records == []

    def test_a_transport_error_raises(self, monkeypatch):
        """The fetch_batch rule's twin: an outage must not read as 'unknown ticker'."""
        import requests as req_lib

        def mock_get(*args, **kwargs):
            raise req_lib.exceptions.ConnectionError("network down")

        monkeypatch.setattr(req_lib, "get", mock_get)
        with pytest.raises(UniverseFetchError, match="AAPL"):
            fetch_ticker_identity("AAPL", api_key="test-key")

    @responses.activate
    def test_a_5xx_raises_and_carries_its_status_code(self):
        responses.add(responses.GET, REFERENCE_URL, status=503)
        with pytest.raises(UniverseFetchError) as excinfo:
            fetch_ticker_identity("AAPL", api_key="test-key")
        assert excinfo.value.status_code == 503

    @responses.activate
    def test_a_429_raises_and_is_recognisable_as_a_rate_limit(self):
        responses.add(responses.GET, REFERENCE_URL, status=429)
        with pytest.raises(UniverseFetchError) as excinfo:
            fetch_ticker_identity("AAPL", api_key="test-key")
        assert excinfo.value.status_code == 429

    def test_no_api_key_raises(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        with pytest.raises(UniverseFetchError, match="MASSIVE_API_KEY"):
            fetch_ticker_identity("AAPL")

    @responses.activate
    def test_a_figi_less_probe_row_stamps_the_figi_less_listing_not_the_reused_one(self):
        # test double for a reused ticker: the active page is the real AABA
        # body (FIGIs, XNAS), the delisted page the real YHOO body (no FIGI,
        # XNAS), the probe the real YHOO 2015 body (no FIGI, XNAS). A loose
        # match would stamp existed_at onto the first record; the exact-FIGI
        # preference must pick the FIGI-less one.
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("aaba-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("yhoo-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("yhoo-date-2015-06-01-2026-09-15.json"), status=200)

        result = fetch_ticker_identity("YHOO", api_key="test-key", probe_date="2015-06-01")

        assert [(r.ticker, r.existed_at) for r in result.records] == [("AABA", None), ("YHOO", "2015-06-01")]

    @responses.activate
    def test_a_delisted_record_carries_its_delisting_date(self):
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("yhoo-active-false-2026-09-15.json"), status=200)

        record = fetch_ticker_identity("YHOO", api_key="test-key").records[0]

        assert record.ticker == "YHOO"
        assert record.delisted_utc.startswith("2017-06-19")
        # Measured 2026-09-15: this delisted record carries no FIGI at all,
        # only a cik (README F5) — identity has to fall back to the provider
        # reference, so the parser must not assume a FIGI is present.
        assert record.composite_figi is None
        assert record.share_class_figi is None
        assert record.cik == "0000316736"

    @responses.activate
    def test_an_nyse_american_listing_keeps_its_mic(self):
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("imo-active-2026-09-15.json"), status=200)
        responses.add(responses.GET, REFERENCE_URL, body=_fixture("dell-active-false-2026-09-15.json"), status=200)

        assert fetch_ticker_identity("IMO", api_key="test-key").records[0].mic == "XASE"

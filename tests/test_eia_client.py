"""EiaClient against a real httpx.Client over MockTransport (the real call signature, not a MagicMock)."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import httpx
import pytest

import clients.eia_client as eia_client
from clients.eia_client import EiaClient

# Real EIA rows: electricity/rto/daily-region-data, respondent AECI, 2026-09-14..15,
# fetched from the mini on 2026-09-23 (40 rows: 2 days x 4 types x 5 timezones).
FIXTURE = json.loads((Path(__file__).parent / "fixtures/eia/daily-region-AECI-2026-09-14_15.json").read_text())
ROWS = FIXTURE["response"]["data"]
ROUTE = "electricity/rto/daily-region-data"
KEY = "test-eia-key"


def serve(rows=ROWS, *, totals=None, statuses=(), seen=None, body=None):
    """A MockTransport answering EIA's offset/length paging from `rows`."""
    queue = list(statuses)
    total_queue = list(totals or [])

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        if queue:
            return httpx.Response(queue.pop(0), request=request)
        if body is not None:
            return httpx.Response(200, content=body, request=request)
        offset, length = int(request.url.params["offset"]), int(request.url.params["length"])
        total = total_queue.pop(0) if total_queue else len(rows)
        return httpx.Response(
            200, json={"response": {"total": total, "data": rows[offset : offset + length]}}, request=request
        )

    return httpx.Client(transport=httpx.MockTransport(handler))


def client(http, *, sleeps=None, clock=lambda: 1000.0):
    return EiaClient(
        KEY, http_client=http, sleep=(sleeps.append if sleeps is not None else lambda _: None), clock=clock
    )


def fetch(c):
    return c.fetch(
        ROUTE,
        frequency="daily",
        start="2026-09-14",
        end="2026-09-15",
        sort_columns=("period", "respondent", "type", "timezone"),
    )


def test_an_api_key_is_required(monkeypatch):
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    with pytest.raises(ValueError, match="EIA_API_KEY"):
        EiaClient()


def test_pages_by_offset_under_a_total_order_until_eia_total(monkeypatch):
    monkeypatch.setattr(eia_client, "PAGE_ROWS", 15)
    seen: list[httpx.Request] = []
    rows, pages = fetch(client(serve(seen=seen)))

    assert rows == ROWS
    assert [int(r.url.params["offset"]) for r in seen] == [0, 15, 30]
    params = seen[0].url.params
    assert [params[f"sort[{i}][column]"] for i in range(4)] == ["period", "respondent", "type", "timezone"]
    assert params["data[]"] == "value" and params["frequency"] == "daily"
    assert seen[0].headers["Accept-Encoding"] == "gzip"
    assert params["api_key"] == KEY
    # Evidence: the exact decoded body, gzip-compressed; the recorded URL never carries the key.
    assert len(pages) == 3
    assert all("api_key" not in page.url for page in pages)
    assert json.loads(gzip.decompress(pages[0].body_gzip))["response"]["data"] == ROWS[:15]


def test_a_total_that_moves_mid_read_fails_rather_than_shifting_offsets(monkeypatch):
    monkeypatch.setattr(eia_client, "PAGE_ROWS", 15)
    with pytest.raises(ValueError, match="total moved from 40 to 41"):
        fetch(client(serve(totals=[40, 41])))


def test_an_empty_page_before_the_total_fails():
    with pytest.raises(ValueError, match="empty page at offset 40 of 100"):
        fetch(client(serve(totals=[100, 100])))


def test_a_429_is_waited_out_and_retried(monkeypatch):
    monkeypatch.setenv("LW_DECLARED_EIA_RETRY_BACKOFF_S", "30")
    sleeps: list[float] = []
    rows, _ = fetch(client(serve(statuses=[429]), sleeps=sleeps))
    assert rows == ROWS
    assert 30 in sleeps


def test_a_404_is_not_retried():
    seen: list[httpx.Request] = []
    with pytest.raises(httpx.HTTPStatusError):
        fetch(client(serve(statuses=[404], seen=seen)))
    assert len(seen) == 1


def test_requests_closer_than_the_declared_interval_are_spaced(monkeypatch):
    monkeypatch.setattr(eia_client, "PAGE_ROWS", 20)
    sleeps: list[float] = []
    fetch(client(serve(), sleeps=sleeps, clock=lambda: 1000.0))  # a frozen clock: no time passes
    assert sleeps == [0.5]


def test_a_body_that_echoes_the_key_is_never_kept():
    with pytest.raises(ValueError, match="contains the api key"):
        fetch(client(serve(body=json.dumps({"request": {"api_key": KEY}}).encode())))


def test_a_body_without_a_response_block_fails():
    with pytest.raises(ValueError, match="no response block"):
        fetch(client(serve(body=b'{"error": "invalid facet"}')))

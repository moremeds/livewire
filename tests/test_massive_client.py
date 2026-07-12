from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest
import responses
from requests.exceptions import ConnectionError as ReqConnectionError
from requests.exceptions import Timeout as ReqTimeout

from clients.massive_client import (
    _DEFAULT_BASE_URL,
    MassiveAPIError,
    MassiveAuthError,
    MassiveClient,
    MassiveMalformedBarError,
    MassiveNotFoundError,
    MassiveRateLimitError,
    MassiveServerError,
    MassiveValidationError,
)


def _make_client(**kwargs) -> MassiveClient:
    defaults = {"token": "test-token", "max_retries": 0, "backoff_factor": 0}
    defaults.update(kwargs)
    return MassiveClient(**defaults)


def _url(endpoint: str) -> str:
    return f"{_DEFAULT_BASE_URL}/{endpoint.lstrip('/')}"


_REST_T_20240603 = 1717444800000


class _Telemetry:
    def __init__(self):
        self.requests = []
        self.rate_limits = []

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def record_request(self, endpoint, status, dt_ms):
        self.requests.append((endpoint, status, dt_ms))

    def record_rate_limit(self, remaining, reset_at):
        self.rate_limits.append((remaining, reset_at))


def _payload(*results, adjusted=False):
    return {
        "ticker": "AAPL",
        "adjusted": adjusted,
        "queryCount": len(results),
        "resultsCount": len(results),
        "status": "OK",
        "results": list(results),
    }


def _bar(**kwargs):
    data = {
        "t": _REST_T_20240603,
        "o": 210.0,
        "h": 215.0,
        "l": 209.5,
        "c": 214.25,
        "v": 42247285.857671,
        "vw": 212.2,
        "n": 100,
    }
    data.update(kwargs)
    return data


def test_missing_token_raises(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    with pytest.raises(MassiveAuthError, match="MASSIVE_API_KEY"):
        MassiveClient(max_retries=0)


def test_token_from_env(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "env-token")
    client = MassiveClient(max_retries=0)
    assert client._token == "env-token"
    client.close()


def test_session_headers():
    client = _make_client()
    headers = client._session.headers
    assert headers["Authorization"] == "Bearer test-token"
    assert headers["Accept"] == "application/json"
    assert headers["User-Agent"] == "livewire/1.0"
    client.close()


def test_context_manager_closes_telemetry():
    telemetry = _Telemetry()
    with _make_client(telemetry=telemetry) as client:
        assert isinstance(client, MassiveClient)
    assert telemetry.started is True
    assert telemetry.stopped is True


@responses.activate
def test_get_daily_bars_uses_custom_aggregate_endpoint_and_normalizes():
    endpoint = "/v2/aggs/ticker/AAPL/range/1/day/2024-06-03/2024-06-03"
    responses.add(responses.GET, _url(endpoint), json=_payload(_bar()), status=200)

    with _make_client() as client:
        bars = client.get_daily_bars("aapl", date(2024, 6, 3), date(2024, 6, 3))

    assert len(bars) == 1
    bar = bars[0]
    assert bar.date == "2024-06-03"
    assert bar.trade_date == date(2024, 6, 3)
    assert bar.open == 210.0
    assert bar.high == 215.0
    assert bar.low == 209.5
    assert bar.close == 214.25
    assert bar.volume == 42247286
    assert bar.source == "massive"
    assert bar.metadata["raw_volume"] == 42247285.857671
    assert bar.metadata["volume_rounded"] is True
    request = responses.calls[0].request
    assert request.url is not None
    assert "adjusted=false" in request.url
    assert "sort=asc" in request.url
    assert "limit=50000" in request.url


@responses.activate
def test_get_grouped_daily_uses_grouped_endpoint_and_ticker_field():
    grouped_bar = _bar(T="MSFT")
    endpoint = "/v2/aggs/grouped/locale/us/market/stocks/2024-06-03"
    responses.add(responses.GET, _url(endpoint), json=_payload(grouped_bar), status=200)

    with _make_client() as client:
        bars = client.get_grouped_daily(date(2024, 6, 3))

    assert list(bars) == ["MSFT"]
    assert bars["MSFT"].date == "2024-06-03"
    assert bars["MSFT"].source == "massive"
    request = responses.calls[0].request
    assert request.url is not None
    assert "adjusted=false" in request.url
    assert "include_otc=false" in request.url


def test_normalize_daily_bar_uses_shared_trade_date_converter():
    observed_t_ms = 1717444800000

    with patch(
        "clients.massive_client.massive_timestamp_to_trade_date",
        return_value=date(2024, 6, 3),
    ) as convert:
        bar = MassiveClient.normalize_daily_bar(_bar(t=observed_t_ms), ticker="AAPL")

    convert.assert_called_once_with(datetime.fromtimestamp(observed_t_ms / 1000, UTC))
    assert bar.trade_date == date(2024, 6, 3)


@responses.activate
def test_telemetry_records_request_and_rate_limit_headers():
    telemetry = _Telemetry()
    responses.add(
        responses.GET,
        _url("/x"),
        json={"ok": True},
        status=200,
        headers={"X-RateLimit-Remaining": "42", "X-RateLimit-Reset": "1778875200"},
    )

    with _make_client(telemetry=telemetry) as client:
        assert client._get("/x") == {"ok": True}

    assert telemetry.requests[0][0] == "/x"
    assert telemetry.requests[0][1] == 200
    assert telemetry.rate_limits == [(42, 1778875200)]


@responses.activate
def test_429_retries_with_retry_after(monkeypatch):
    sleeps = []
    monkeypatch.setattr("clients.massive_client.time.sleep", sleeps.append)
    responses.add(
        responses.GET,
        _url("/x"),
        json={"message": "slow"},
        status=429,
        headers={"Retry-After": "2.5"},
    )
    responses.add(responses.GET, _url("/x"), json={"ok": True}, status=200)

    with _make_client(max_retries=1) as client:
        assert client._get("/x") == {"ok": True}

    assert sleeps == [2.5]


@responses.activate
def test_invalid_retry_after_falls_back_to_exponential_sleep(monkeypatch):
    sleeps = []
    monkeypatch.setattr("clients.massive_client.time.sleep", sleeps.append)
    responses.add(
        responses.GET,
        _url("/x"),
        json={"message": "slow"},
        status=429,
        headers={"Retry-After": "bad"},
    )
    responses.add(responses.GET, _url("/x"), json={"ok": True}, status=200)

    with _make_client(max_retries=1, backoff_factor=3) as client:
        assert client._get("/x") == {"ok": True}

    assert sleeps == [3]


@responses.activate
def test_429_exhausts_retries():
    responses.add(responses.GET, _url("/x"), json={"message": "slow"}, status=429)
    with _make_client(max_retries=0) as client:
        with pytest.raises(MassiveRateLimitError):
            client._get("/x")


@responses.activate
def test_status_errors_map_to_typed_exceptions():
    cases = [
        (401, MassiveAuthError),
        (403, MassiveAuthError),
        (404, MassiveNotFoundError),
        (422, MassiveValidationError),
        (500, MassiveServerError),
        (418, MassiveAPIError),
    ]
    for status, exc_type in cases:
        responses.add(responses.GET, _url(f"/{status}"), json={"message": str(status)}, status=status)
        with _make_client(max_retries=0) as client:
            with pytest.raises(exc_type):
                client._get(f"/{status}")


@responses.activate
def test_connection_errors_retry_then_raise():
    responses.add(responses.GET, _url("/x"), body=ReqConnectionError("down"))
    responses.add(responses.GET, _url("/x"), body=ReqTimeout("down"))
    with _make_client(max_retries=1, backoff_factor=0) as client:
        with pytest.raises(MassiveAPIError, match="Connection failed"):
            client._get("/x")


def test_negative_retries_hits_guard():
    with _make_client(max_retries=-2) as client:
        with pytest.raises(TypeError):
            client._get("/x")


def test_safe_json_returns_empty_for_invalid_or_non_dict():
    import requests

    invalid = requests.models.Response()
    invalid._content = b"not json"
    invalid.encoding = "utf-8"
    assert MassiveClient._safe_json(invalid) == {}

    array_payload = requests.models.Response()
    array_payload._content = b"[1, 2]"
    array_payload.encoding = "utf-8"
    assert MassiveClient._safe_json(array_payload) == {}


def test_record_rate_limit_ignores_missing_or_invalid_headers():
    telemetry = _Telemetry()
    client = _make_client(telemetry=telemetry)
    import requests

    no_headers = requests.models.Response()
    client._record_rate_limit(no_headers)
    invalid_headers = requests.models.Response()
    invalid_headers.headers["X-RateLimit-Remaining"] = "bad"
    invalid_headers.headers["X-RateLimit-Reset"] = "1778875200"
    client._record_rate_limit(invalid_headers)
    assert telemetry.rate_limits == []
    client.close()


@pytest.mark.parametrize(
    "bad_bar, message",
    [
        ({"t": _REST_T_20240603, "o": 1, "h": 1, "l": 1, "c": 1}, "v"),
        (_bar(o=0), "positive"),
        (_bar(h=1, l=2), "high"),
        (_bar(h=1000, l=999), "low"),
        (_bar(v=-1), "volume"),
        (_bar(t="not-int"), "timestamp"),
        (_bar(v="bad"), "numeric"),
        (_bar(v=float("inf")), "finite"),
    ],
)
def test_normalize_rejects_malformed_bars(bad_bar, message):
    with pytest.raises(MassiveMalformedBarError, match=message):
        MassiveClient.normalize_daily_bar(bad_bar, ticker="AAPL")


def test_normalize_rejects_grouped_bar_without_ticker():
    with pytest.raises(MassiveMalformedBarError, match="ticker"):
        MassiveClient.normalize_daily_bar(_bar(), ticker=None)


def test_empty_or_missing_results_return_empty_lists():
    assert MassiveClient._extract_results({"status": "OK"}) == []
    assert MassiveClient._extract_results({"status": "OK", "results": []}) == []


def test_delayed_payload_returns_results():
    payload = {"status": "DELAYED", "results": [{"T": "AAPL"}]}

    assert MassiveClient._extract_results(payload) == [{"T": "AAPL"}]


def test_non_ok_payload_raises():
    with pytest.raises(MassiveAPIError, match="NOT_AUTHORIZED"):
        MassiveClient._extract_results({"status": "NOT_AUTHORIZED"})


@responses.activate
def test_get_splits_follows_same_origin_pagination_and_preserves_auth():
    endpoint = "/v3/reference/splits"
    next_url = _url(f"{endpoint}?cursor=next")
    responses.add(
        responses.GET,
        _url(endpoint),
        json={
            "status": "OK",
            "results": [
                {
                    "id": "split-1",
                    "ticker": "nvda",
                    "execution_date": "2024-06-10",
                    "split_from": 1,
                    "split_to": 10,
                }
            ],
            "next_url": next_url,
        },
        status=200,
    )
    responses.add(
        responses.GET,
        next_url,
        json={
            "status": "OK",
            "results": [
                {
                    "id": "split-2",
                    "ticker": "NVDA",
                    "execution_date": "2021-07-20",
                    "split_from": "1",
                    "split_to": "4",
                }
            ],
        },
        status=200,
    )

    with _make_client() as client:
        splits = client.get_splits("nvda")

    assert [event.provider_event_id for event in splits] == ["split-1", "split-2"]
    assert splits[0].ticker == "NVDA"
    assert splits[0].execution_date == date(2024, 6, 10)
    assert splits[0].split_to == Decimal("10")
    assert all(call.request.headers["Authorization"] == "Bearer test-token" for call in responses.calls)
    assert "apiKey" not in responses.calls[1].request.url


@responses.activate
def test_get_dividends_normalizes_dates_currency_and_amount():
    responses.add(
        responses.GET,
        _url("/v3/reference/dividends"),
        json={
            "status": "OK",
            "results": [
                {
                    "id": "div-1",
                    "ticker": "spy",
                    "ex_dividend_date": "2026-06-20",
                    "cash_amount": "1.7611",
                    "currency": "usd",
                    "declaration_date": "2026-06-05",
                    "record_date": "2026-06-20",
                    "pay_date": "2026-07-31",
                }
            ],
        },
        status=200,
    )

    with _make_client() as client:
        dividends = client.get_dividends("spy")

    assert dividends[0].ticker == "SPY"
    assert dividends[0].cash_amount == Decimal("1.7611")
    assert dividends[0].currency == "USD"
    assert dividends[0].declaration_date == date(2026, 6, 5)


@pytest.mark.parametrize(
    "method,payload,message",
    [
        ("get_splits", {"id": "x", "ticker": "NVDA", "execution_date": "bad", "split_from": 1, "split_to": 10}, "date"),
        (
            "get_splits",
            {"id": "x", "ticker": "NVDA", "execution_date": "2024-06-10", "split_from": 0, "split_to": 10},
            "positive",
        ),
        (
            "get_dividends",
            {"id": "x", "ticker": "SPY", "ex_dividend_date": "2026-06-20", "cash_amount": -1, "currency": "USD"},
            "non-negative",
        ),
        (
            "get_dividends",
            {"id": "x", "ticker": "SPY", "ex_dividend_date": "2026-06-20", "cash_amount": 1, "currency": ""},
            "currency",
        ),
    ],
)
@responses.activate
def test_corporate_action_validation_rejects_malformed_payloads(method, payload, message):
    resource = "splits" if method == "get_splits" else "dividends"
    responses.add(
        responses.GET,
        _url(f"/v3/reference/{resource}"),
        json={"status": "OK", "results": [payload]},
        status=200,
    )
    with _make_client() as client:
        with pytest.raises(MassiveAPIError, match=message):
            getattr(client, method)(payload["ticker"])


@responses.activate
def test_pagination_rejects_cross_origin_next_url():
    responses.add(
        responses.GET,
        _url("/v3/reference/splits"),
        json={"status": "OK", "results": [], "next_url": "https://evil.example/steal"},
        status=200,
    )
    with _make_client() as client:
        with pytest.raises(MassiveAPIError, match="pagination URL"):
            client.get_splits("NVDA")

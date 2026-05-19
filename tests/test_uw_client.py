"""Unit tests for clients/uw_client.py — 100% coverage target.

All HTTP calls are mocked via the `responses` library. No real network I/O.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
import responses
from requests.exceptions import ConnectionError as ReqConnectionError, Timeout as ReqTimeout

from clients.uw_client import (
    UWAPIError,
    UWAuthError,
    UWClient,
    UWNotFoundError,
    UWRateLimitError,
    UWServerError,
    UWValidationError,
    _DEFAULT_BASE_URL,
)


# ── Helpers ────────────────────────────────────────────────────────────

def _make_client(**kwargs) -> UWClient:
    """Create a UWClient with zero retries and no backoff for fast tests."""
    defaults = {"token": "test-token", "max_retries": 0, "backoff_factor": 0}
    defaults.update(kwargs)
    return UWClient(**defaults)


def _url(endpoint: str) -> str:
    return f"{_DEFAULT_BASE_URL}/{endpoint}"


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


# ══════════════════════════════════════════════════════════════════════
# Construction / lifecycle
# ══════════════════════════════════════════════════════════════════════


class TestInit:
    def test_token_from_param(self):
        client = _make_client(token="my-token")
        assert client._token == "my-token"
        client.close()

    def test_token_from_env(self, monkeypatch):
        monkeypatch.setenv("UW_TOKEN", "env-token")
        client = UWClient(max_retries=0)
        assert client._token == "env-token"
        client.close()

    def test_missing_token_raises(self, monkeypatch):
        monkeypatch.delenv("UW_TOKEN", raising=False)
        with pytest.raises(UWAuthError, match="UW_TOKEN"):
            UWClient(max_retries=0)

    def test_base_url_trailing_slash_stripped(self):
        client = _make_client(base_url="https://example.com/api/")
        assert client._base_url == "https://example.com/api"
        client.close()

    def test_session_headers(self):
        client = _make_client()
        h = client._session.headers
        assert h["Authorization"] == "Bearer test-token"
        assert h["User-Agent"] == "livewire/1.0"
        assert h["Accept"] == "application/json"
        client.close()


class TestLifecycle:
    def test_close(self):
        client = _make_client()
        client.close()
        # After close, session adapter is removed — no assertion needed,
        # just verify it doesn't raise.

    def test_context_manager(self):
        with _make_client() as client:
            assert isinstance(client, UWClient)
        # client.close() was called by __exit__

    def test_context_manager_starts_and_stops_telemetry(self):
        telemetry = _Telemetry()
        with _make_client(telemetry=telemetry) as client:
            assert isinstance(client, UWClient)
        assert telemetry.started is True
        assert telemetry.stopped is True


# ══════════════════════════════════════════════════════════════════════
# _get — success & error mapping
# ══════════════════════════════════════════════════════════════════════


class TestGetSuccess:
    @responses.activate
    def test_200_returns_json(self):
        responses.add(responses.GET, _url("test"), json={"ok": True}, status=200)
        with _make_client() as client:
            assert client._get("test") == {"ok": True}

    @responses.activate
    def test_leading_slash_stripped(self):
        responses.add(responses.GET, _url("test"), json={"ok": True}, status=200)
        with _make_client() as client:
            assert client._get("/test") == {"ok": True}

    @responses.activate
    def test_telemetry_records_request_and_rate_limit_headers(self):
        telemetry = _Telemetry()
        responses.add(
            responses.GET,
            _url("test"),
            json={"ok": True},
            status=200,
            headers={"X-RateLimit-Remaining": "8", "X-RateLimit-Reset": "1778875200"},
        )
        with _make_client(telemetry=telemetry) as client:
            assert client._get("test") == {"ok": True}
        assert telemetry.requests[0][0] == "test"
        assert telemetry.requests[0][1] == 200
        assert telemetry.rate_limits == [(8, 1778875200)]

    @responses.activate
    def test_telemetry_ignores_missing_or_invalid_rate_limit_headers(self):
        telemetry = _Telemetry()
        responses.add(responses.GET, _url("missing"), json={"ok": True}, status=200)
        responses.add(
            responses.GET,
            _url("invalid"),
            json={"ok": True},
            status=200,
            headers={"X-RateLimit-Remaining": "bad", "X-RateLimit-Reset": "1778875200"},
        )
        with _make_client(telemetry=telemetry) as client:
            client._get("missing")
            client._get("invalid")
        assert telemetry.rate_limits == []


class TestGetErrorMapping:
    @responses.activate
    def test_401_raises_auth_error(self):
        responses.add(responses.GET, _url("x"), json={"message": "bad token"}, status=401)
        with _make_client() as c:
            with pytest.raises(UWAuthError, match="bad token") as exc_info:
                c._get("x")
            assert exc_info.value.status_code == 401

    @responses.activate
    def test_403_raises_auth_error(self):
        responses.add(responses.GET, _url("x"), json={"message": "forbidden"}, status=403)
        with _make_client() as c:
            with pytest.raises(UWAuthError):
                c._get("x")

    @responses.activate
    def test_404_raises_not_found(self):
        responses.add(responses.GET, _url("x"), json={"message": "nope"}, status=404)
        with _make_client() as c:
            with pytest.raises(UWNotFoundError, match="nope") as exc_info:
                c._get("x")
            assert exc_info.value.status_code == 404

    @responses.activate
    def test_422_raises_validation_error(self):
        responses.add(responses.GET, _url("x"), json={"message": "bad params"}, status=422)
        with _make_client() as c:
            with pytest.raises(UWValidationError, match="bad params"):
                c._get("x")

    @responses.activate
    def test_other_4xx_raises_generic_api_error(self):
        responses.add(responses.GET, _url("x"), json={"message": "teapot"}, status=418)
        with _make_client() as c:
            with pytest.raises(UWAPIError, match="teapot") as exc_info:
                c._get("x")
            assert exc_info.value.status_code == 418

    @responses.activate
    def test_unexpected_status_raises_generic_api_error(self):
        """Non-200, non-4xx, non-5xx status (e.g. 302)."""
        responses.add(responses.GET, _url("x"), json={"message": "redirect"}, status=302)
        with _make_client() as c:
            with pytest.raises(UWAPIError):
                c._get("x")

    @responses.activate
    def test_error_msg_falls_back_to_reason(self):
        responses.add(responses.GET, _url("x"), json={}, status=404)
        with _make_client() as c:
            with pytest.raises(UWNotFoundError):
                c._get("x")

    @responses.activate
    def test_error_msg_falls_back_to_http_status(self):
        """When JSON body has no 'message' and reason is empty."""
        resp = responses.add(responses.GET, _url("x"), body="", status=404)
        with _make_client() as c:
            with pytest.raises(UWNotFoundError):
                c._get("x")


# ══════════════════════════════════════════════════════════════════════
# _get — retries
# ══════════════════════════════════════════════════════════════════════


class TestGetRetries:
    @responses.activate
    def test_429_retries_then_succeeds(self):
        responses.add(responses.GET, _url("x"), json={"message": "slow down"}, status=429)
        responses.add(responses.GET, _url("x"), json={"ok": True}, status=200)
        with _make_client(max_retries=1, backoff_factor=0) as c:
            assert c._get("x") == {"ok": True}

    @responses.activate
    def test_429_exhausts_retries(self):
        responses.add(responses.GET, _url("x"), json={"message": "slow"}, status=429)
        responses.add(responses.GET, _url("x"), json={"message": "slow"}, status=429)
        with _make_client(max_retries=1, backoff_factor=0) as c:
            with pytest.raises(UWRateLimitError):
                c._get("x")

    @responses.activate
    def test_500_retries_then_succeeds(self):
        responses.add(responses.GET, _url("x"), json={"message": "oops"}, status=500)
        responses.add(responses.GET, _url("x"), json={"data": 1}, status=200)
        with _make_client(max_retries=1, backoff_factor=0) as c:
            assert c._get("x") == {"data": 1}

    @responses.activate
    def test_500_exhausts_retries(self):
        for _ in range(3):
            responses.add(responses.GET, _url("x"), json={"message": "down"}, status=500)
        with _make_client(max_retries=2, backoff_factor=0) as c:
            with pytest.raises(UWServerError):
                c._get("x")

    @responses.activate
    def test_connection_error_retries_then_raises(self):
        responses.add(responses.GET, _url("x"), body=ReqConnectionError("conn refused"))
        responses.add(responses.GET, _url("x"), body=ReqConnectionError("conn refused"))
        with _make_client(max_retries=1, backoff_factor=0) as c:
            with pytest.raises(UWAPIError, match="Connection failed"):
                c._get("x")

    @responses.activate
    def test_connection_error_retries_then_succeeds(self):
        responses.add(responses.GET, _url("x"), body=ReqConnectionError("down"))
        responses.add(responses.GET, _url("x"), json={"ok": True}, status=200)
        with _make_client(max_retries=1, backoff_factor=0) as c:
            assert c._get("x") == {"ok": True}

    @responses.activate
    def test_timeout_retries_then_raises(self):
        responses.add(responses.GET, _url("x"), body=ReqTimeout("timed out"))
        responses.add(responses.GET, _url("x"), body=ReqTimeout("timed out"))
        with _make_client(max_retries=1, backoff_factor=0) as c:
            with pytest.raises(UWAPIError, match="Connection failed"):
                c._get("x")

    def test_negative_retries_hits_guard(self):
        """When max_retries=-1, the loop never runs and the guard raises."""
        with _make_client(max_retries=-1) as c:
            with pytest.raises(TypeError):
                c._get("x")


# ══════════════════════════════════════════════════════════════════════
# Static / helper methods
# ══════════════════════════════════════════════════════════════════════


class TestHelpers:
    def test_build_params_filters_none(self):
        assert UWClient._build_params(a=1, b=None, c="x") == {"a": 1, "c": "x"}

    def test_build_params_empty(self):
        assert UWClient._build_params() == {}

    def test_safe_json_valid(self):
        import requests

        resp = requests.models.Response()
        resp._content = b'{"key": "val"}'
        resp.encoding = "utf-8"
        assert UWClient._safe_json(resp) == {"key": "val"}

    def test_safe_json_invalid(self):
        import requests

        resp = requests.models.Response()
        resp._content = b"not json"
        resp.encoding = "utf-8"
        assert UWClient._safe_json(resp) == {}

    def test_get_retry_delay_with_retry_after_header(self):
        import requests

        resp = requests.models.Response()
        resp.headers["Retry-After"] = "5"
        assert UWClient._get_retry_delay(resp, attempt=0) == 5.0

    def test_get_retry_delay_retry_after_floor(self):
        import requests

        resp = requests.models.Response()
        resp.headers["Retry-After"] = "0.5"
        assert UWClient._get_retry_delay(resp, attempt=0) == 1.0

    def test_get_retry_delay_invalid_retry_after(self):
        import requests

        resp = requests.models.Response()
        resp.headers["Retry-After"] = "not-a-number"
        assert UWClient._get_retry_delay(resp, attempt=2) == 4.0

    def test_get_retry_delay_no_header(self):
        import requests

        resp = requests.models.Response()
        assert UWClient._get_retry_delay(resp, attempt=1) == 2.0

    @patch("clients.uw_client.time.sleep")
    def test_sleep_backoff(self, mock_sleep):
        client = _make_client(backoff_factor=1.0)
        client._sleep_backoff(2)
        mock_sleep.assert_called_once_with(4.0)  # 1.0 * 2^2
        client.close()


# ══════════════════════════════════════════════════════════════════════
# Public endpoint methods
# ══════════════════════════════════════════════════════════════════════


class TestEndpoints:
    @responses.activate
    def test_get_stock_ohlc(self):
        responses.add(
            responses.GET,
            _url("stock/AAPL/ohlc/1d"),
            json={"data": [{"close": "150"}]},
            status=200,
        )
        with _make_client() as c:
            result = c.get_stock_ohlc("aapl", "1d")
            assert result == {"data": [{"close": "150"}]}

    @responses.activate
    def test_get_stock_ohlc_with_kwargs(self):
        responses.add(
            responses.GET,
            _url("stock/AAPL/ohlc/5m"),
            json={"data": []},
            status=200,
        )
        with _make_client() as c:
            c.get_stock_ohlc("aapl", "5m", limit=10)
            assert responses.calls[0].request.params == {"limit": "10"}

    @responses.activate
    def test_get_stock_info(self):
        responses.add(
            responses.GET,
            _url("stock/NVDA"),
            json={"ticker": "NVDA"},
            status=200,
        )
        with _make_client() as c:
            result = c.get_stock_info("nvda")
            assert result["ticker"] == "NVDA"


# ══════════════════════════════════════════════════════════════════════
# Exception hierarchy
# ══════════════════════════════════════════════════════════════════════


class TestExceptions:
    def test_api_error_attributes(self):
        err = UWAPIError("msg", status_code=500, response_body={"detail": "x"})
        assert str(err) == "msg"
        assert err.status_code == 500
        assert err.response_body == {"detail": "x"}

    def test_api_error_defaults(self):
        err = UWAPIError("msg")
        assert err.status_code is None
        assert err.response_body is None

    def test_subclass_hierarchy(self):
        assert issubclass(UWAuthError, UWAPIError)
        assert issubclass(UWRateLimitError, UWAPIError)
        assert issubclass(UWNotFoundError, UWAPIError)
        assert issubclass(UWValidationError, UWAPIError)
        assert issubclass(UWServerError, UWAPIError)

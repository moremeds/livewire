"""Tests for the shared transient-failure policy in ``clients/http_retry.py``."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from clients.http_retry import get_with_retry, is_transient

_REQUEST = httpx.Request("GET", "https://example.invalid/x")


def _status_response(status_code: int) -> httpx.Response:
    """A real httpx.Response so ``raise_for_status``/``is_transient`` see real attributes."""
    return httpx.Response(status_code, request=_REQUEST)


def _ok_response() -> httpx.Response:
    return httpx.Response(200, request=_REQUEST, json={"ok": True})


class TestIsTransient:
    @pytest.mark.parametrize("status_code", [500, 502, 503])
    def test_a_5xx_status_error_is_transient(self, status_code):
        response = _status_response(status_code)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            assert is_transient(exc) is True
        else:
            pytest.fail("expected raise_for_status to raise")

    @pytest.mark.parametrize("status_code", [400, 404, 429])
    def test_a_4xx_status_error_is_not_transient(self, status_code):
        """429 is deliberately NOT transient here: this module has no Retry-After policy."""
        response = _status_response(status_code)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            assert is_transient(exc) is False
        else:
            pytest.fail("expected raise_for_status to raise")

    def test_a_transport_error_is_transient(self):
        assert is_transient(httpx.ConnectTimeout("timed out", request=_REQUEST)) is True

    def test_an_unrelated_exception_is_not_transient(self):
        assert is_transient(ValueError("not an http error")) is False


class TestGetWithRetry:
    def test_a_5xx_is_retried_and_a_later_success_is_returned(self):
        ok = _ok_response()
        send = MagicMock(side_effect=[_status_response(503), ok])

        result = get_with_retry(send, attempts=3, backoff_s=0, sleep=MagicMock())

        assert result is ok
        assert send.call_count == 2

    def test_a_transport_error_is_retried_and_a_later_success_is_returned(self):
        ok = _ok_response()
        send = MagicMock(side_effect=[httpx.ConnectTimeout("timed out", request=_REQUEST), ok])

        result = get_with_retry(send, attempts=3, backoff_s=0, sleep=MagicMock())

        assert result is ok
        assert send.call_count == 2

    def test_a_4xx_is_raised_on_the_first_attempt_and_never_retried(self):
        send = MagicMock(return_value=_status_response(404))

        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            get_with_retry(send, attempts=3, backoff_s=0, sleep=MagicMock())

        assert excinfo.value.response.status_code == 404
        send.assert_called_once()

    def test_after_attempts_transient_failures_the_last_error_is_reraised(self):
        first = httpx.ConnectTimeout("first", request=_REQUEST)
        second = httpx.ConnectTimeout("second", request=_REQUEST)
        send = MagicMock(side_effect=[first, second])

        with pytest.raises(httpx.ConnectTimeout) as excinfo:
            get_with_retry(send, attempts=2, backoff_s=0, sleep=MagicMock())

        assert excinfo.value is second
        assert send.call_count == 2

    @pytest.mark.parametrize("attempts", [0, -1, -5])
    def test_attempts_of_zero_or_negative_still_sends_exactly_once(self, attempts):
        ok = _ok_response()
        send = MagicMock(return_value=ok)

        result = get_with_retry(send, attempts=attempts, backoff_s=0, sleep=MagicMock())

        assert result is ok
        send.assert_called_once()

    def test_a_negative_backoff_does_not_reach_a_raising_sleep(self, monkeypatch):
        """A raw ``time.sleep(-2.0)`` raises ValueError; the clamp must keep that from happening."""
        import clients.http_retry as http_retry

        def raising_sleep(seconds: float) -> None:
            if seconds < 0:
                raise ValueError("sleep length must be non-negative")

        monkeypatch.setattr(http_retry.time, "sleep", raising_sleep)
        send = MagicMock(side_effect=[httpx.ConnectTimeout("timeout", request=_REQUEST), _ok_response()])

        result = get_with_retry(send, attempts=2, backoff_s=-5)

        assert result is not None
        assert send.call_count == 2

    def test_the_backoff_is_linear_across_three_attempts(self):
        sleeps: list[float] = []
        failures = [httpx.ConnectTimeout(f"failure {i}", request=_REQUEST) for i in range(3)]
        send = MagicMock(side_effect=failures)

        with pytest.raises(httpx.ConnectTimeout):
            get_with_retry(send, attempts=3, backoff_s=2, sleep=sleeps.append)

        assert sleeps == [2.0, 4.0]

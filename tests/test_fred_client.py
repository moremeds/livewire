"""Tests for FRED API client."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from clients.constants import declared
from clients.fred_client import FRED_OBSERVATIONS_URL, FredClient, FredObservation


def test_requires_api_key(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)

    with pytest.raises(ValueError, match="FRED_API_KEY"):
        FredClient()


def test_uses_api_key_from_env(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "env-key")

    client = FredClient()

    assert client.api_key == "env-key"


def test_fetch_observations_parses_values_and_skips_missing():
    response = MagicMock()
    response.json.return_value = {
        "observations": [
            {"date": "2026-05-14", "value": "4.47"},
            {"date": "2026-05-15", "value": "."},
            {"date": "2026-05-18", "value": "4.59"},
        ]
    }
    response.raise_for_status = MagicMock()
    http = MagicMock()
    http.get.return_value = response

    client = FredClient(api_key="test-key", http_client=http)
    observations = client.fetch_observations(
        "DGS10",
        observation_start="2026-05-01",
        observation_end="2026-05-31",
    )

    assert observations == [
        FredObservation(date="2026-05-14", value=4.47),
        FredObservation(date="2026-05-18", value=4.59),
    ]
    http.get.assert_called_once()
    _, kwargs = http.get.call_args
    assert kwargs["params"]["series_id"] == "DGS10"
    assert kwargs["params"]["api_key"] == "test-key"
    assert kwargs["params"]["file_type"] == "json"
    assert kwargs["params"]["observation_start"] == "2026-05-01"
    assert kwargs["params"]["observation_end"] == "2026-05-31"


def test_frequency_validation():
    client = FredClient(api_key="test-key", http_client=MagicMock())

    with pytest.raises(ValueError, match="unsupported FRED frequency"):
        client.fetch_observations("DGS10", frequency="hourly")


def test_aggregation_method_validation():
    client = FredClient(api_key="test-key", http_client=MagicMock())

    with pytest.raises(ValueError, match="unsupported FRED aggregation_method"):
        client.fetch_observations("DGS10", aggregation_method="median")


def test_weekly_frequency_and_aggregation_are_forwarded():
    response = MagicMock()
    response.json.return_value = {"observations": [{"date": "2026-05-15", "value": "4.59"}]}
    response.raise_for_status = MagicMock()
    http = MagicMock()
    http.get.return_value = response

    client = FredClient(api_key="test-key", http_client=http)
    observations = client.fetch_observations(
        "DGS10",
        frequency="w",
        aggregation_method="eop",
    )

    assert observations == [FredObservation(date="2026-05-15", value=4.59)]
    _, kwargs = http.get.call_args
    assert kwargs["params"]["frequency"] == "w"
    assert kwargs["params"]["aggregation_method"] == "eop"


class _FakeHttp:
    """Stands in for the httpx module: returns or raises one scripted item per call."""

    def __init__(self, items: list) -> None:
        self._items = list(items)
        self.calls: list[dict] = []

    def get(self, url, *, params, timeout):
        self.calls.append(params)
        item = self._items[min(len(self.calls) - 1, len(self._items) - 1)]
        if isinstance(item, Exception):
            raise item
        return item


def _response(status_code: int, payload: dict | None = None) -> httpx.Response:
    request = httpx.Request("GET", FRED_OBSERVATIONS_URL)
    return httpx.Response(status_code, json=payload or {"observations": []}, request=request)


def test_a_502_is_retried_and_the_next_attempt_is_used(monkeypatch):
    """2026-09-14: one 502 on DGS5 failed the phase and skipped DGS10/DGS30."""
    monkeypatch.setattr("clients.fred_client.time.sleep", lambda _s: None)
    http = _FakeHttp(
        [
            _response(502),
            _response(200, {"observations": [{"date": "2026-09-10", "value": "4.95"}]}),
        ]
    )

    client = FredClient(api_key="test-key", http_client=http)
    observations = client.fetch_observations("DGS10")

    assert observations == [FredObservation(date="2026-09-10", value=4.95)]
    assert len(http.calls) == 2


def test_a_read_timeout_is_retried_and_gives_up_after_the_declared_attempts(monkeypatch):
    monkeypatch.setattr("clients.fred_client.time.sleep", lambda _s: None)
    http = _FakeHttp([httpx.ReadTimeout("The read operation timed out")])

    client = FredClient(api_key="test-key", http_client=http)
    with pytest.raises(httpx.ReadTimeout):
        client.fetch_observations("DGS10")

    assert len(http.calls) == int(declared("fred_retry_attempts"))


def test_a_4xx_is_never_retried(monkeypatch):
    """A bad key or a retired series is a request problem; retrying only burns time."""
    monkeypatch.setattr("clients.fred_client.time.sleep", lambda _s: None)
    http = _FakeHttp([_response(400)])

    client = FredClient(api_key="bad-key", http_client=http)
    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_observations("DGS10")

    assert len(http.calls) == 1


def test_the_retry_backs_off_between_attempts(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr("clients.fred_client.time.sleep", slept.append)
    http = _FakeHttp([_response(503)])

    client = FredClient(api_key="test-key", http_client=http)
    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_observations("DGS10")

    assert slept and all(delay > 0 for delay in slept)
    assert len(slept) == int(declared("fred_retry_attempts")) - 1


def test_an_attempts_override_below_one_still_makes_exactly_one_request(monkeypatch):
    """A typo'd override must not skip the request and raise `None` instead."""
    monkeypatch.setenv("LW_DECLARED_FRED_RETRY_ATTEMPTS", "0")
    monkeypatch.setattr("clients.fred_client.time.sleep", lambda _s: None)
    http = _FakeHttp([_response(503)])

    client = FredClient(api_key="test-key", http_client=http)
    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_observations("DGS10")

    assert len(http.calls) == 1


def test_a_malformed_payload_still_crashes_loudly_after_the_retry(monkeypatch):
    """A changed FRED schema is a bug, not a transient outage: it must not be retried."""
    monkeypatch.setattr("clients.fred_client.time.sleep", lambda _s: None)
    http = _FakeHttp([_response(200, {"observations": [{"value": "4.95"}]})])

    client = FredClient(api_key="test-key", http_client=http)
    with pytest.raises(KeyError):
        client.fetch_observations("DGS10")

    assert len(http.calls) == 1


def test_a_negative_backoff_override_still_raises_the_http_error_not_value_error(monkeypatch):
    """The twin of the attempts floor: time.sleep(-2.0) is a ValueError, which
    run()'s per-series catch would not absorb, and DGS10/DGS30 would be skipped again."""
    monkeypatch.setenv("LW_DECLARED_FRED_RETRY_BACKOFF_S", "-2")
    slept: list[float] = []
    monkeypatch.setattr("clients.fred_client.time.sleep", slept.append)
    http = _FakeHttp([_response(503)])
    client = FredClient(api_key="test-key", http_client=http)
    with pytest.raises(httpx.HTTPStatusError):
        client.fetch_observations("DGS10")
    assert len(http.calls) == int(declared("fred_retry_attempts"))
    assert slept and all(delay == 0.0 for delay in slept)

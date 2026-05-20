"""Tests for FRED API client."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from clients.fred_client import FredClient, FredObservation


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

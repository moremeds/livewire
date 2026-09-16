"""FRED API client for economic time series observations."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import httpx

from clients.constants import declared
from clients.http_retry import get_with_retry

FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_FREQUENCIES = {
    "d",
    "w",
    "bw",
    "m",
    "q",
    "sa",
    "a",
    "wef",
    "weth",
    "wew",
    "wetu",
    "wem",
    "wesu",
    "wesa",
    "bwew",
    "bwem",
}
FRED_AGGREGATION_METHODS = {"avg", "sum", "eop"}


@dataclass(frozen=True)
class FredObservation:
    """Single FRED time-series observation."""

    date: str
    value: float


class FredClient:
    """Small wrapper around FRED's official observations endpoint."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        http_client: Any | None = None,
        base_url: str = FRED_OBSERVATIONS_URL,
    ) -> None:
        self.api_key = api_key or os.environ.get("FRED_API_KEY")
        if not self.api_key:
            raise ValueError("FRED_API_KEY environment variable is not set")
        self._http = http_client or httpx
        self._base_url = base_url

    def _get_with_retry(self, params: dict) -> Any:
        """GET *params*, retrying a 5xx or a transport error a bounded number of times.

        FRED answers a well-formed request with a 502 often enough that a single
        attempt pages the operator over an outage that is gone a second later
        (pm:2026-09-14-fred-502-aborted-remaining-series). A 4xx is a request
        problem — a bad key, a retired series — and is raised on the first try.
        The retry policy itself is `clients.http_retry`, shared with CBOE; only
        the two declared bounds below are FRED's own.
        """
        return get_with_retry(
            lambda: self._http.get(self._base_url, params=params, timeout=30),
            attempts=declared("fred_retry_attempts"),
            backoff_s=declared("fred_retry_backoff_s"),
        )

    def fetch_observations(
        self,
        series_id: str,
        *,
        observation_start: str | None = None,
        observation_end: str | None = None,
        frequency: str | None = None,
        aggregation_method: str = "eop",
    ) -> list[FredObservation]:
        """Fetch observations for *series_id* from FRED.

        FRED returns missing values as "."; those observations are skipped so
        callers only publish numeric rows.
        """
        if frequency is not None and frequency not in FRED_FREQUENCIES:
            raise ValueError(f"unsupported FRED frequency: {frequency!r}")
        if aggregation_method not in FRED_AGGREGATION_METHODS:
            raise ValueError(f"unsupported FRED aggregation_method: {aggregation_method!r}")

        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "sort_order": "asc",
        }
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end
        if frequency:
            params["frequency"] = frequency
            params["aggregation_method"] = aggregation_method

        payload = self._get_with_retry(params).json()

        observations: list[FredObservation] = []
        for item in payload.get("observations", []):
            raw_value = item.get("value")
            if raw_value in (None, "."):
                continue
            observations.append(FredObservation(date=str(item["date"]), value=float(raw_value)))
        return observations

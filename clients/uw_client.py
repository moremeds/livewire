"""Unusual Whales API client.

Ported from convex-scavenger with updated User-Agent.
Provides authenticated access to UW REST API with retry and error handling.

Usage:
    from clients import UWClient

    with UWClient() as client:
        ohlc = client.get_stock_ohlc("AAPL", "1d")
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional

import requests
from requests.exceptions import ConnectionError as ReqConnectionError, Timeout as ReqTimeout

log = logging.getLogger(__name__)


# ── Exceptions ─────────────────────────────────────────────────────────

class UWAPIError(Exception):
    """Base exception for all UW API errors."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[dict] = None,
    ):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


class UWAuthError(UWAPIError):
    """Authentication or authorization failure (401/403)."""


class UWRateLimitError(UWAPIError):
    """Rate limit exceeded (429)."""


class UWNotFoundError(UWAPIError):
    """Resource not found (404)."""


class UWValidationError(UWAPIError):
    """Invalid parameters (422)."""


class UWServerError(UWAPIError):
    """Server-side error (5xx)."""


# Status codes that are safe to retry
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Default configuration
_DEFAULT_BASE_URL = "https://api.unusualwhales.com/api"
_DEFAULT_TIMEOUT = 30
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_BACKOFF_FACTOR = 1.0


class UWClient:
    """Unusual Whales REST API client.

    Features:
      - Connection-pooled requests.Session
      - Automatic retry with exponential backoff for transient errors
      - Rate-limit awareness (Retry-After header)
      - Clear exception hierarchy mapped to HTTP status codes
    """

    def __init__(
        self,
        token: Optional[str] = None,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: int = _DEFAULT_TIMEOUT,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        backoff_factor: float = _DEFAULT_BACKOFF_FACTOR,
    ):
        self._token = token or os.environ.get("UW_TOKEN")
        if not self._token:
            raise UWAuthError(
                "UW_TOKEN environment variable is not set. "
                "Export it via: export UW_TOKEN='your-api-key'"
            )

        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/json",
                "User-Agent": "livewire/1.0",
            }
        )

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "UWClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ── internal request layer ─────────────────────────────────────

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> dict:
        endpoint = endpoint.lstrip("/")
        url = f"{self._base_url}/{endpoint}"

        last_exc: Optional[Exception] = None

        for attempt in range(1 + self._max_retries):
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
            except (ReqConnectionError, ReqTimeout) as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    self._sleep_backoff(attempt)
                    continue
                raise UWAPIError(f"Connection failed after {attempt + 1} attempts: {exc}") from exc

            status = resp.status_code

            if status == 200:
                return resp.json()

            body = self._safe_json(resp)
            msg = body.get("message", "") or resp.reason or f"HTTP {status}"

            if status == 429:
                exc = UWRateLimitError(msg, status_code=status, response_body=body)
            elif status in (401, 403):
                raise UWAuthError(msg, status_code=status, response_body=body)
            elif status == 404:
                raise UWNotFoundError(msg, status_code=status, response_body=body)
            elif status == 422:
                raise UWValidationError(msg, status_code=status, response_body=body)
            elif status >= 500:
                exc = UWServerError(msg, status_code=status, response_body=body)
            elif status >= 400:
                raise UWAPIError(msg, status_code=status, response_body=body)
            else:
                raise UWAPIError(msg, status_code=status, response_body=body)

            last_exc = exc
            if attempt < self._max_retries:
                sleep_time = self._get_retry_delay(resp, attempt)
                time.sleep(sleep_time)
                continue

            raise exc

        raise last_exc  # type: ignore[misc]

    def _sleep_backoff(self, attempt: int) -> None:
        delay = self._backoff_factor * (2 ** attempt)
        time.sleep(delay)

    @staticmethod
    def _get_retry_delay(resp: requests.Response, attempt: int) -> float:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 1.0)
            except (ValueError, TypeError):
                pass
        return 1.0 * (2 ** attempt)

    @staticmethod
    def _safe_json(resp: requests.Response) -> dict:
        try:
            return resp.json()
        except Exception:
            return {}

    @staticmethod
    def _build_params(**kwargs: Any) -> Dict[str, Any]:
        return {k: v for k, v in kwargs.items() if v is not None}

    # ── Stock OHLC ─────────────────────────────────────────────────

    def get_stock_ohlc(self, ticker: str, candle_size: str = "1d", **kwargs) -> dict:
        """GET /api/stock/{ticker}/ohlc/{candle_size} - OHLC price data.

        Response contains a list of candles with:
          - start_time: ISO timestamp (e.g. "2023-09-07T20:10:00Z")
          - open, high, low, close: string prices
          - volume: int
          - total_volume: int (cumulative)
        """
        params = self._build_params(**kwargs)
        return self._get(f"stock/{ticker.upper()}/ohlc/{candle_size}", params=params)

    # ── Stock Info ─────────────────────────────────────────────────

    def get_stock_info(self, ticker: str) -> dict:
        """GET /api/stock/{ticker} - General stock info."""
        return self._get(f"stock/{ticker.upper()}")

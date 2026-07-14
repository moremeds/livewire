"""Massive REST client for stock daily OHLCV bars."""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import parse_qsl, urlparse

import requests
from requests.exceptions import (
    ConnectionError as ReqConnectionError,
)
from requests.exceptions import (
    Timeout as ReqTimeout,
)

from clients.massive_time import massive_timestamp_to_trade_date
from clients.symbol_paths import canonical_symbol

_DEFAULT_BASE_URL = "https://api.massive.com"
_DEFAULT_TIMEOUT = 30
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_BACKOFF_FACTOR = 1.0
_USER_AGENT = "livewire/1.0"
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class MassiveAPIError(Exception):
    """Base exception for Massive client failures."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        response_body: dict | None = None,
    ):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


class MassiveAuthError(MassiveAPIError):
    """Authentication or authorization failure."""


class MassiveRateLimitError(MassiveAPIError):
    """Rate limit failure."""


class MassiveNotFoundError(MassiveAPIError):
    """Requested Massive resource was not found."""


class MassiveValidationError(MassiveAPIError):
    """Massive rejected request parameters."""


class MassiveServerError(MassiveAPIError):
    """Massive returned a server-side error."""


class MassiveMalformedBarError(MassiveAPIError):
    """Massive returned a bar that cannot be stored safely."""


class MassiveMalformedCorporateActionError(MassiveAPIError):
    """Massive returned a corporate action that cannot be stored safely."""


class MassiveMalformedIndicatorError(MassiveAPIError):
    """Massive returned an indicator value that cannot be validated safely."""


@dataclass(frozen=True)
class MassiveDailyBar:
    """Normalized Massive daily bar."""

    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str = "massive"
    ticker: str | None = None
    metadata: dict[str, Any] | None = None

    @property
    def date(self) -> str:
        """Expose an IB BarData-like date attribute for daily update reuse."""
        return self.trade_date.isoformat()


@dataclass(frozen=True)
class MassiveSplit:
    provider_event_id: str
    ticker: str
    execution_date: date
    split_from: Decimal
    split_to: Decimal
    payload_hash: str


@dataclass(frozen=True)
class MassiveDividend:
    provider_event_id: str
    ticker: str
    ex_dividend_date: date
    cash_amount: Decimal
    currency: str | None
    declaration_date: date | None
    record_date: date | None
    pay_date: date | None
    payload_hash: str
    historical_adjustment_factor: Decimal | None = None


@dataclass(frozen=True)
class MassiveSMAValue:
    trade_date: date
    value: float


class MassiveClient:
    """Small Massive REST client for stock daily aggregate endpoints."""

    def __init__(
        self,
        token: str | None = None,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: int = _DEFAULT_TIMEOUT,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        backoff_factor: float = _DEFAULT_BACKOFF_FACTOR,
        telemetry: Any = None,
    ):
        self._token = token or os.environ.get("MASSIVE_API_KEY")
        if not self._token:
            raise MassiveAuthError(
                "MASSIVE_API_KEY environment variable is not set. Export it via: export MASSIVE_API_KEY='your-api-key'"
            )

        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._telemetry = telemetry
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/json",
                "User-Agent": _USER_AGENT,
            }
        )

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> MassiveClient:
        if self._telemetry is not None:
            self._telemetry.start()
        return self

    def __exit__(self, *exc) -> None:
        self.close()
        if self._telemetry is not None:
            self._telemetry.stop()

    def get_daily_bars(
        self,
        ticker: str,
        start: date,
        end: date,
        *,
        adjusted: bool = False,
    ) -> list[MassiveDailyBar]:
        endpoint = f"/v2/aggs/ticker/{ticker.upper()}/range/1/day/{start.isoformat()}/{end.isoformat()}"
        rows = [
            row
            for payload in self._iter_payloads(
                endpoint,
                {"adjusted": str(adjusted).lower(), "sort": "asc", "limit": 50000},
            )
            for row in self._extract_results(payload)
        ]
        return [self.normalize_daily_bar(row, ticker=ticker.upper()) for row in rows]

    def get_grouped_daily(
        self,
        trade_date: date,
        *,
        adjusted: bool = False,
        include_otc: bool = False,
    ) -> dict[str, MassiveDailyBar]:
        endpoint = f"/v2/aggs/grouped/locale/us/market/stocks/{trade_date.isoformat()}"
        payload = self._get(
            endpoint,
            params={
                "adjusted": str(adjusted).lower(),
                "include_otc": str(include_otc).lower(),
            },
        )
        bars = [self.normalize_daily_bar(row, ticker=None) for row in self._extract_results(payload)]
        return {bar.ticker or "": bar for bar in bars}

    def get_splits(self, ticker: str) -> list[MassiveSplit]:
        rows = self._get_paginated("/v3/reference/splits", {"ticker": canonical_symbol(ticker), "limit": 1000})
        return [self.normalize_split(row) for row in rows]

    def get_dividends(self, ticker: str) -> list[MassiveDividend]:
        rows = self._get_paginated("/v3/reference/dividends", {"ticker": canonical_symbol(ticker), "limit": 1000})
        return [self.normalize_dividend(row) for row in rows]

    def get_sma(
        self,
        ticker: str,
        window: int,
        start: date,
        end: date,
    ) -> list[MassiveSMAValue]:
        if window <= 0:
            raise MassiveValidationError("SMA window must be positive")
        endpoint = f"/v1/indicators/sma/{ticker.upper()}"
        params = {
            "adjusted": "true",
            "window": window,
            "series_type": "close",
            "order": "asc",
            "timestamp.gte": start.isoformat(),
            "timestamp.lte": end.isoformat(),
            "limit": 5000,
        }
        rows = [row for payload in self._iter_payloads(endpoint, params) for row in self._extract_sma_values(payload)]
        return [self.normalize_sma_value(row) for row in rows]

    def _get_paginated(self, endpoint: str, params: dict[str, Any]) -> list[dict]:
        return [row for payload in self._iter_payloads(endpoint, params) for row in self._extract_results(payload)]

    def _iter_payloads(self, endpoint: str, params: dict[str, Any]) -> Iterator[dict]:
        payload = self._get(endpoint, params=params)
        expected_host = urlparse(self._base_url).hostname
        for _ in range(100):
            yield payload
            next_url = payload.get("next_url")
            if not next_url:
                return
            parsed = urlparse(str(next_url))
            if parsed.scheme != "https" or parsed.hostname != expected_host:
                raise MassiveAPIError("invalid pagination URL")
            query = {key: value for key, value in parse_qsl(parsed.query) if key.casefold() != "apikey"}
            payload = self._get(parsed.path, params=query)
        raise MassiveAPIError("pagination exceeded 100 pages")

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        endpoint = "/" + endpoint.lstrip("/")
        url = f"{self._base_url}{endpoint}"
        last_exc: Exception | None = None

        for attempt in range(max(self._max_retries, -1) + 1):
            started = time.monotonic()
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
            except (ReqConnectionError, ReqTimeout) as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    self._sleep_backoff(attempt)
                    continue
                raise MassiveAPIError(f"Connection failed after {attempt + 1} attempts: {exc}") from exc

            self._record_request(endpoint, resp.status_code, started)
            self._record_rate_limit(resp)

            if resp.status_code == 200:
                return self._safe_json(resp)

            body = self._safe_json(resp)
            msg = body.get("message", "") or body.get("error", "") or resp.reason or f"HTTP {resp.status_code}"
            exc = self._exception_for_status(resp.status_code, msg, body)
            last_exc = exc

            if resp.status_code in _RETRYABLE_STATUS_CODES and attempt < self._max_retries:
                self._sleep_backoff(attempt, resp)
                continue
            raise exc

        raise last_exc  # type: ignore[misc]

    @staticmethod
    def _safe_json(resp: requests.Response) -> dict:
        try:
            payload = resp.json()
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _exception_for_status(status: int, msg: str, body: dict) -> MassiveAPIError:
        if status == 429:
            return MassiveRateLimitError(msg, status_code=status, response_body=body)
        if status in (401, 403):
            return MassiveAuthError(msg, status_code=status, response_body=body)
        if status == 404:
            return MassiveNotFoundError(msg, status_code=status, response_body=body)
        if status == 422:
            return MassiveValidationError(msg, status_code=status, response_body=body)
        if status >= 500:
            return MassiveServerError(msg, status_code=status, response_body=body)
        return MassiveAPIError(msg, status_code=status, response_body=body)

    def _sleep_backoff(self, attempt: int, resp: requests.Response | None = None) -> None:
        retry_after = None if resp is None else resp.headers.get("Retry-After")
        if retry_after is not None:
            try:
                time.sleep(float(retry_after))
                return
            except (TypeError, ValueError):
                pass
        time.sleep(self._backoff_factor * (2**attempt))

    def _record_request(self, endpoint: str, status: int, started: float) -> None:
        if self._telemetry is None:
            return
        dt_ms = int((time.monotonic() - started) * 1000)
        self._telemetry.record_request(endpoint=endpoint, status=status, dt_ms=dt_ms)

    def _record_rate_limit(self, resp: requests.Response) -> None:
        if self._telemetry is None:
            return
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset_at = resp.headers.get("X-RateLimit-Reset")
        if remaining is None or reset_at is None:
            return
        try:
            self._telemetry.record_rate_limit(remaining=int(remaining), reset_at=int(reset_at))
        except (TypeError, ValueError):
            return

    @staticmethod
    def _extract_results(payload: dict) -> list[dict]:
        status = payload.get("status")
        if status and status not in {"OK", "DELAYED"}:
            raise MassiveAPIError(str(status), response_body=payload)
        results = payload.get("results") or []
        return results if isinstance(results, list) else []

    @staticmethod
    def _extract_sma_values(payload: dict) -> list[dict]:
        status = payload.get("status")
        if status and status not in {"OK", "DELAYED"}:
            raise MassiveAPIError(str(status), response_body=payload)
        results = payload.get("results") or {}
        values = (results.get("values") or []) if isinstance(results, dict) else []
        return values if isinstance(values, list) else []

    @staticmethod
    def normalize_daily_bar(payload: dict, ticker: str | None = None) -> MassiveDailyBar:
        symbol = ticker or payload.get("T")
        if not symbol:
            raise MassiveMalformedBarError("grouped bar missing ticker")

        raw_ts = payload.get("t")
        if not isinstance(raw_ts, int):
            raise MassiveMalformedBarError("bar timestamp t must be an integer")
        trade_date = massive_timestamp_to_trade_date(datetime.fromtimestamp(raw_ts / 1000, UTC))

        open_px = MassiveClient._finite_float(payload, "o")
        high_px = MassiveClient._finite_float(payload, "h")
        low_px = MassiveClient._finite_float(payload, "l")
        close_px = MassiveClient._finite_float(payload, "c")
        raw_volume = MassiveClient._finite_float(payload, "v")

        if open_px <= 0 or close_px <= 0:
            raise MassiveMalformedBarError("open and close must be positive")
        if high_px < low_px or high_px < open_px or high_px < close_px:
            raise MassiveMalformedBarError("high must be >= low, open, and close")
        if low_px > open_px or low_px > close_px:
            raise MassiveMalformedBarError("low must be <= open and close")
        if raw_volume < 0:
            raise MassiveMalformedBarError("volume must be non-negative")

        volume = int(round(raw_volume))
        metadata = {
            "raw_volume": raw_volume,
            "volume_rounded": volume != raw_volume,
        }
        if "vw" in payload:
            metadata["vwap"] = payload["vw"]
        if "n" in payload:
            metadata["transactions"] = payload["n"]

        return MassiveDailyBar(
            trade_date=trade_date,
            open=open_px,
            high=high_px,
            low=low_px,
            close=close_px,
            volume=volume,
            ticker=str(symbol).upper(),
            metadata=metadata,
        )

    @staticmethod
    def normalize_split(payload: dict) -> MassiveSplit:
        event_id, ticker = MassiveClient._corporate_action_identity(payload)
        split_from = MassiveClient._decimal(payload, "split_from")
        split_to = MassiveClient._decimal(payload, "split_to")
        if split_from <= 0 or split_to <= 0:
            raise MassiveMalformedCorporateActionError("split ratios must be positive")
        return MassiveSplit(
            provider_event_id=event_id,
            ticker=ticker,
            execution_date=MassiveClient._iso_date(payload, "execution_date"),
            split_from=split_from,
            split_to=split_to,
            payload_hash=MassiveClient._payload_hash(payload),
        )

    @staticmethod
    def normalize_dividend(payload: dict) -> MassiveDividend:
        event_id, ticker = MassiveClient._corporate_action_identity(payload)
        cash_amount = MassiveClient._decimal(payload, "cash_amount")
        if cash_amount < 0:
            raise MassiveMalformedCorporateActionError("cash amount must be non-negative")
        raw_currency = payload.get("currency")
        currency = None if raw_currency is None else str(raw_currency).upper()
        if currency is not None and (len(currency) != 3 or not currency.isalpha()):
            raise MassiveMalformedCorporateActionError("currency must be a three-letter code")
        historical_adjustment_factor = MassiveClient._optional_decimal(payload, "historical_adjustment_factor")
        if historical_adjustment_factor is not None and historical_adjustment_factor <= 0:
            raise MassiveMalformedCorporateActionError("historical adjustment factor must be positive")
        return MassiveDividend(
            provider_event_id=event_id,
            ticker=ticker,
            ex_dividend_date=MassiveClient._iso_date(payload, "ex_dividend_date"),
            cash_amount=cash_amount,
            currency=currency,
            declaration_date=MassiveClient._optional_iso_date(payload, "declaration_date"),
            record_date=MassiveClient._optional_iso_date(payload, "record_date"),
            pay_date=MassiveClient._optional_iso_date(payload, "pay_date"),
            payload_hash=MassiveClient._payload_hash(payload),
            historical_adjustment_factor=historical_adjustment_factor,
        )

    @staticmethod
    def normalize_sma_value(payload: dict) -> MassiveSMAValue:
        raw_timestamp = payload.get("timestamp")
        if not isinstance(raw_timestamp, int):
            raise MassiveMalformedIndicatorError("SMA timestamp must be an integer")
        try:
            value = float(payload["value"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MassiveMalformedIndicatorError("SMA value must be numeric") from exc
        if not math.isfinite(value):
            raise MassiveMalformedIndicatorError("SMA value must be finite")
        trade_date = massive_timestamp_to_trade_date(datetime.fromtimestamp(raw_timestamp / 1000, UTC))
        return MassiveSMAValue(trade_date=trade_date, value=value)

    @staticmethod
    def _corporate_action_identity(payload: dict) -> tuple[str, str]:
        event_id = str(payload.get("id", "")).strip()
        ticker = canonical_symbol(str(payload.get("ticker", "")))
        if not event_id or not ticker:
            raise MassiveMalformedCorporateActionError("corporate action requires provider id and ticker")
        return event_id, ticker

    @staticmethod
    def _decimal(payload: dict, key: str) -> Decimal:
        try:
            value = Decimal(str(payload[key]))
        except (KeyError, InvalidOperation, ValueError) as exc:
            raise MassiveMalformedCorporateActionError(f"{key} must be decimal") from exc
        if not value.is_finite():
            raise MassiveMalformedCorporateActionError(f"{key} must be finite")
        return value

    @staticmethod
    def _optional_decimal(payload: dict, key: str) -> Decimal | None:
        return None if payload.get(key) in (None, "") else MassiveClient._decimal(payload, key)

    @staticmethod
    def _iso_date(payload: dict, key: str) -> date:
        try:
            return date.fromisoformat(str(payload[key]))
        except (KeyError, ValueError) as exc:
            raise MassiveMalformedCorporateActionError(f"{key} must be an ISO date") from exc

    @staticmethod
    def _optional_iso_date(payload: dict, key: str) -> date | None:
        return None if payload.get(key) in (None, "") else MassiveClient._iso_date(payload, key)

    @staticmethod
    def _payload_hash(payload: dict) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _finite_float(payload: dict, key: str) -> float:
        if key not in payload:
            raise MassiveMalformedBarError(f"bar missing {key}")
        try:
            value = float(payload[key])
        except (TypeError, ValueError) as exc:
            raise MassiveMalformedBarError(f"bar {key} must be numeric") from exc
        if not math.isfinite(value):
            raise MassiveMalformedBarError(f"bar {key} must be finite")
        return value

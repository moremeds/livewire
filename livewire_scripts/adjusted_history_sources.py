"""Read-only provider acquisition and cache helpers for adjusted validation."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Literal

from clients.adjusted_history_validation import build_split_only_rows
from clients.corporate_action_store import CorporateAction
from clients.ib_client import IBClient, IBTimeoutError
from clients.ingestion_common import bars_to_rows, make_contract
from clients.massive_client import MassiveClient
from clients.price_basis import prepare_ib_rows_for_publish
from livewire_scripts.fetch_ib_historical import compute_date_windows

CACHE_VERSION = 1
SourceStatus = Literal["ok", "empty", "timeout", "error"]


@dataclass(frozen=True)
class SourceEvidence:
    provider: str
    symbol: str
    requested_start: date
    requested_end: date
    actual_start: date | None
    actual_end: date | None
    complete_range: bool
    rows: tuple[dict[str, Any], ...]
    sma: dict[int, dict[date, float]]
    status: SourceStatus
    error: str | None = None
    sma_errors: dict[int, str] | None = None


@dataclass(frozen=True)
class ActionEvidence:
    symbol: str
    status: Literal["complete", "partial", "unavailable", "error"]
    missing_local_ids: tuple[str, ...]
    unexpected_provider_ids: tuple[str, ...]
    historical_adjustment_factors: dict[str, str]
    error: str | None = None


def _coerce_date(value: object) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _bar_row(bar, *, source: str, price_basis: str) -> dict[str, Any]:
    return {
        "trade_date": bar.trade_date,
        "symbol_id": 0,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "adj_close": bar.close,
        "volume": bar.volume,
        "source": source,
        "price_basis": price_basis,
        "currency": "USD",
    }


def fetch_massive_evidence(
    client: MassiveClient,
    symbol: str,
    start: date,
    end: date,
    *,
    windows: tuple[int, ...] = (20, 50, 200),
) -> SourceEvidence:
    """Fetch Massive split-adjusted bars and provider SMA diagnostics."""
    symbol = symbol.upper()
    try:
        bars = client.get_daily_bars(symbol, start, end, adjusted=True)
        rows = tuple(_bar_row(bar, source="massive", price_basis="split_adjusted") for bar in bars)
    except Exception as exc:
        return SourceEvidence("massive", symbol, start, end, None, None, False, (), {}, "error", str(exc))
    sma: dict[int, dict[date, float]] = {}
    sma_errors: dict[int, str] = {}
    for window in windows:
        try:
            sma[window] = {item.trade_date: item.value for item in client.get_sma(symbol, window, start, end)}
        except Exception as exc:
            sma_errors[window] = str(exc)
    dates = sorted(_coerce_date(row["trade_date"]) for row in rows)
    actual_start = dates[0] if dates else None
    actual_end = dates[-1] if dates else None
    return SourceEvidence(
        provider="massive",
        symbol=symbol,
        requested_start=start,
        requested_end=end,
        actual_start=actual_start,
        actual_end=actual_end,
        complete_range=bool(
            dates
            and actual_start is not None
            and actual_end is not None
            and actual_start <= start
            and actual_end >= end
        ),
        rows=rows,
        sma=sma,
        status="ok" if rows else "empty",
        sma_errors=sma_errors,
    )


def fetch_massive_action_evidence(
    client: MassiveClient,
    symbol: str,
    local_actions: Sequence[CorporateAction],
    as_of_date: date,
) -> ActionEvidence:
    """Compare fresh Massive action IDs with the effective local inventory."""
    symbol = symbol.upper()
    try:
        splits = [item for item in client.get_splits(symbol) if item.execution_date <= as_of_date]
        dividends = [item for item in client.get_dividends(symbol) if item.ex_dividend_date <= as_of_date]
    except Exception as exc:
        return ActionEvidence(symbol, "error", (), (), {}, str(exc))
    local_ids = {
        item.provider_event_id for item in local_actions if item.status == "active" and item.ex_date <= as_of_date
    }
    provider_ids = {item.provider_event_id for item in [*splits, *dividends]}
    missing = tuple(sorted(local_ids - provider_ids))
    unexpected = tuple(sorted(provider_ids - local_ids))
    factors = {
        item.provider_event_id: str(item.historical_adjustment_factor)
        for item in dividends
        if item.historical_adjustment_factor is not None
    }
    status: Literal["complete", "partial"] = "complete" if not missing and not unexpected else "partial"
    return ActionEvidence(symbol, status, missing, unexpected, factors)


def fetch_ib_evidence(
    fetcher: Callable[[str, date, date], list[dict[str, Any]]],
    symbol: str,
    start: date,
    end: date,
    actions: list[CorporateAction],
    as_of_date: date,
) -> SourceEvidence:
    """Fetch, classify, normalize, then split-adjust fresh IB daily history."""
    symbol = symbol.upper()
    split_dates = [
        item.ex_date
        for item in actions
        if item.status == "active" and item.action_type == "split" and start <= item.ex_date <= as_of_date
    ]
    context_start = min([start, *(item - timedelta(days=14) for item in split_dates)])
    context_end = max([end, *(min(as_of_date, item + timedelta(days=14)) for item in split_dates)])
    try:
        staged = fetcher(symbol, context_start, context_end)
        if not staged:
            return SourceEvidence("ib", symbol, start, end, None, None, False, (), {}, "empty")
        canonical_raw = prepare_ib_rows_for_publish(
            staged,
            existing_rows=[],
            actions=actions,
            as_of_date=as_of_date,
        )
        split_only = build_split_only_rows(canonical_raw, actions, as_of_date)
        rows = tuple(
            {
                **row,
                "trade_date": _coerce_date(row["trade_date"]),
                "price_basis": "split_adjusted",
            }
            for row in split_only
            if start <= _coerce_date(row["trade_date"]) <= end
        )
    except (TimeoutError, IBTimeoutError) as exc:
        return SourceEvidence("ib", symbol, start, end, None, None, False, (), {}, "timeout", str(exc))
    except Exception as exc:
        return SourceEvidence("ib", symbol, start, end, None, None, False, (), {}, "error", str(exc))
    dates = sorted(_coerce_date(row["trade_date"]) for row in rows)
    actual_start = dates[0] if dates else None
    actual_end = dates[-1] if dates else None
    return SourceEvidence(
        "ib",
        symbol,
        start,
        end,
        actual_start,
        actual_end,
        bool(
            dates
            and actual_start is not None
            and actual_end is not None
            and actual_start <= start
            and actual_end >= end
        ),
        rows,
        {},
        "ok" if rows else "empty",
    )


class IBHistoryFetcher:
    """Synchronous, read-only one-year chunk fetcher over an existing IB session."""

    def __init__(self, client: IBClient):
        self._client = client

    def __call__(self, symbol: str, start: date, end: date) -> list[dict[str, Any]]:
        contract = make_contract(symbol, "equity")
        self._client.ib.qualifyContracts(contract)
        start_dt = datetime.combine(start, time.min)
        end_dt = datetime.combine(end, time.max)
        bars_by_date: dict[str, Any] = {}
        for duration, end_date in compute_date_windows(start_dt, end_dt):
            bars = self._client.get_historical_data(
                contract,
                duration=duration,
                bar_size="1 day",
                what_to_show="TRADES",
                use_rth=True,
                end_date=end_date,
            )
            for bar in bars or []:
                bars_by_date[str(bar.date)] = bar
        rows = bars_to_rows(
            [bars_by_date[key] for key in sorted(bars_by_date)],
            0,
            source="ib",
            price_basis="split_adjusted",
        )
        return [
            {
                **row,
                "trade_date": _coerce_date(row["trade_date"]),
                "currency": "USD",
            }
            for row in rows
            if start <= _coerce_date(row["trade_date"]) <= end
        ]


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def write_cached_evidence(path: Path, identity: dict[str, Any], payload: dict[str, Any]) -> None:
    """Atomically persist a content-checked cache envelope."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    envelope = {
        "identity": identity,
        "payload": payload,
        "payload_sha256": hashlib.sha256(_canonical_json(payload)).hexdigest(),
        "written_at": datetime.now(UTC).isoformat(),
    }
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(envelope, handle, sort_keys=True, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def load_cached_evidence(path: Path, identity: dict[str, Any]) -> dict[str, Any]:
    """Load a cache only when identity and payload hash still match."""
    envelope = json.loads(Path(path).read_text(encoding="utf-8"))
    if envelope.get("identity") != identity:
        raise ValueError("cache identity mismatch")
    payload = envelope.get("payload")
    observed = hashlib.sha256(_canonical_json(payload)).hexdigest()
    if observed != envelope.get("payload_sha256"):
        raise ValueError("cache payload hash mismatch")
    if not isinstance(payload, dict):
        raise ValueError("cache payload must be an object")
    return payload

"""Pure quality-flag detection. No I/O.

See: docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from clients.timeutils import utc_iso

try:
    from clients.trading_calendar import is_trading_day as _default_is_trading_day
except ImportError:  # pragma: no cover - exercised only before T5 helper extraction
    _default_is_trading_day = None

_RANGE_SHORTFALL_WARNING_DAYS = 5
_RANGE_SHORTFALL_CRITICAL_DAYS = 30
_logger = logging.getLogger("livewire.quality")


@dataclass(frozen=True)
class QualityFlag:
    category: str
    severity: str
    detail: dict[str, Any] = field(default_factory=dict)
    ts: str = field(default_factory=utc_iso)


def detect_range_shortfall(
    expected_start: date,
    actual_start: date,
    ib_head_timestamp: date | None,
) -> QualityFlag | None:
    """Flag when actual_start is materially later than expected_start.

    If ib_head_timestamp equals actual_start, treat as "IB has no older data" (clean).
    Otherwise severity follows the shortfall-size thresholds.
    """
    if actual_start <= expected_start:
        return None
    shortfall_days = (actual_start - expected_start).days
    if ib_head_timestamp is not None and ib_head_timestamp >= actual_start:
        return None
    if shortfall_days > _RANGE_SHORTFALL_CRITICAL_DAYS:
        severity = "critical"
    elif shortfall_days > _RANGE_SHORTFALL_WARNING_DAYS:
        severity = "warning"
    else:
        return None
    return QualityFlag(
        category="range_shortfall",
        severity=severity,
        detail={
            "expected_start": expected_start.isoformat(),
            "actual_start": actual_start.isoformat(),
            "shortfall_days": shortfall_days,
            "ib_head_timestamp": ib_head_timestamp.isoformat() if ib_head_timestamp else None,
        },
    )


_INTERIOR_GAPS_WARNING_DAYS = 1
_INTERIOR_GAPS_CRITICAL_CONSECUTIVE = 10
_INTERIOR_GAPS_CRITICAL_TOTAL = 30


def _coerce_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value)[:10])


def _normalize_bars_for_detection(bars: list) -> list:
    """Produce objects exposing ``.trade_date`` as an ISO date string."""
    out = []
    for b in bars:
        if hasattr(b, "trade_date"):
            out.append(b)
        elif hasattr(b, "date"):
            d = b.date
            out.append(SimpleNamespace(trade_date=str(d)[:10]))
        elif isinstance(b, dict):
            ts = b.get("trade_date") or b.get("bar_timestamp")
            out.append(SimpleNamespace(trade_date=str(ts)[:10]))
        else:  # pragma: no cover - defensive
            continue
    return out


def detect_interior_gaps(
    bars: list,
    trading_calendar=None,
) -> QualityFlag | None:
    """Find missing trading days inside the bar range."""
    if not bars or len(bars) < 2:
        return None
    is_trading_day = trading_calendar or _default_is_trading_day
    if is_trading_day is None:
        return QualityFlag(
            category="interior_gaps",
            severity="info",
            detail={"status": "gap_detection_unavailable", "reason": "no_calendar"},
        )
    try:
        dates = sorted({_coerce_date(b.trade_date) for b in bars})
        start, end = dates[0], dates[-1]
        present = set(dates)
        cursor = start + timedelta(days=1)
        missing: list[date] = []
        max_consecutive = 0
        current_run = 0
        while cursor < end:
            if is_trading_day(cursor):
                if cursor in present:
                    current_run = 0
                else:
                    missing.append(cursor)
                    current_run += 1
                    max_consecutive = max(max_consecutive, current_run)
            cursor += timedelta(days=1)
    except Exception as exc:
        return QualityFlag(
            category="interior_gaps",
            severity="info",
            detail={"status": "gap_detection_unavailable", "reason": str(exc)},
        )
    if not missing:
        return None
    if max_consecutive >= _INTERIOR_GAPS_CRITICAL_CONSECUTIVE or len(missing) >= _INTERIOR_GAPS_CRITICAL_TOTAL:
        severity = "critical"
    elif len(missing) > _INTERIOR_GAPS_WARNING_DAYS:
        severity = "warning"
    else:
        # `_INTERIOR_GAPS_WARNING_DAYS` was declared here from the start and
        # never read, so a SINGLE missing interior day scored `warning` — and
        # `MDW_ALERT_SEVERITY_THRESHOLD` defaults to `warning`, so every such
        # symbol emailed. On 2026-07-19 that sent ~150 emails in 20 minutes
        # (SAAQW, SBCWW, SLND.WS, WENC.U, TDACU, XRPNU …), all with
        # missing_days_count 1 or 2, and left 4,408 more undelivered. The
        # rate limiter cannot help: its key is (source, ticker, category), so
        # a sweep across ~13K distinct tickers never repeats a key.
        #
        # One absent day on an illiquid warrant is a no-trade day, which the
        # coverage pipeline already refuses to count as missing ("absent from
        # the day's raw traded set" is not a gap). Grading it `info` keeps the
        # flag in the parquet sidecar and the audit JSONL — nothing stops
        # being *detected* — it stops being paged.
        severity = "info"
    return QualityFlag(
        category="interior_gaps",
        severity=severity,
        detail={
            "missing_days_count": len(missing),
            "max_consecutive_missing": max_consecutive,
            "first_missing": missing[0].isoformat(),
            "last_missing": missing[-1].isoformat(),
        },
    )


_FETCH_TAINT_WARNING_COUNT = 1
_FETCH_TAINT_CRITICAL_COUNT = 5


def detect_fetch_tainting(errors_during_fetch: list[dict]) -> QualityFlag | None:
    if not errors_during_fetch:
        return None
    total = sum(int(e.get("count", 1)) for e in errors_during_fetch)
    codes = sorted({int(e["code"]) for e in errors_during_fetch if "code" in e})
    if total >= _FETCH_TAINT_CRITICAL_COUNT:
        severity = "critical"
    elif total >= _FETCH_TAINT_WARNING_COUNT:
        severity = "warning"
    else:  # pragma: no cover - unreachable given total >=1 entry
        return None
    return QualityFlag(
        category="fetch_tainted",
        severity=severity,
        detail={"error_count": total, "codes": codes},
    )


def detect_row_count_anomaly(
    bars: list,
    reference_source=None,
) -> QualityFlag | None:
    """Flag material count differences against a supplied second-source reference."""
    if reference_source is None:
        return None
    expected_count = int(reference_source.get("expected_count"))
    if expected_count <= 0:
        raise ValueError("reference_source.expected_count must be positive")

    actual_count = reference_source.get("actual_count")
    if actual_count is None:
        actual_count = len(bars)
    actual_count = int(actual_count)

    pct_delta = abs(expected_count - actual_count) / expected_count * 100.0
    if pct_delta > 5.0:
        severity = "critical"
    elif pct_delta > 1.0:
        severity = "warning"
    else:
        return None

    expected_dates = reference_source.get("expected_dates") or []
    actual_dates = reference_source.get("actual_dates")
    if actual_dates is None:
        actual_dates = [str(b.trade_date)[:10] for b in _normalize_bars_for_detection(bars)]
    missing_dates = sorted(set(expected_dates) - set(actual_dates))
    extra_dates = sorted(set(actual_dates) - set(expected_dates)) if expected_dates else []

    return QualityFlag(
        category="row_count_anomaly",
        severity=severity,
        detail={
            "reference_source": reference_source.get("source"),
            "expected_count": expected_count,
            "actual_count": actual_count,
            "percent_delta": round(pct_delta, 4),
            "warning_threshold_pct": 1.0,
            "critical_threshold_pct": 5.0,
            "missing_dates": missing_dates,
            "extra_dates": extra_dates,
        },
    )


def detect_all(
    bars: list,
    metadata: dict,
    trading_calendar=None,
) -> list[QualityFlag]:
    flags: list[QualityFlag] = []

    def _safe(name: str, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            _logger.warning("detector %s raised: %s", name, exc)
            flags.append(
                QualityFlag(
                    category="detector_error",
                    severity="warning",
                    detail={"detector": name, "error": str(exc)},
                )
            )
            return None

    expected_start = metadata.get("expected_start")
    if expected_start is not None and bars:
        actual_start = _coerce_date(bars[0].trade_date)
        f = _safe(
            "range_shortfall",
            detect_range_shortfall,
            expected_start,
            actual_start,
            metadata.get("ib_head_timestamp"),
        )
        if f:
            flags.append(f)

    if len(bars) >= 2:
        f = _safe("interior_gaps", detect_interior_gaps, bars, trading_calendar)
        if f:
            flags.append(f)

    errors = metadata.get("errors_during_fetch") or []
    f = _safe("fetch_tainted", detect_fetch_tainting, errors)
    if f:
        flags.append(f)

    f = _safe(
        "row_count_anomaly",
        detect_row_count_anomaly,
        bars,
        metadata.get("reference_source"),
    )
    if f:
        flags.append(f)

    return flags


#: Set to False by `--no-quality`. The gate applies to every caller: it means
#: "do not run the detector this run", not "not on this one code path". Before
#: consolidation only fetch_ib_historical honoured it.
QUALITY_ENABLED: bool = True


def run_detection(
    *,
    ticker: str,
    asset_class: str,
    timeframe: str,
    bars: list,
    parquet_path: Path,
    source: str = "ib",
    expected_start: date | None = None,
    ib_head_timestamp: date | None = None,
    reference_source: dict | None = None,
    errors_during_fetch: list[dict] | None = None,
    alerts_enabled: bool = True,
    on_error: Callable[[Exception], None] | None = None,
) -> None:
    """Run detection and emit flags without ever blocking a publish."""
    if not QUALITY_ENABLED or not bars:
        return

    # Imported inside the function: clients.quality_flags imports QualityFlag
    # from this module, so a module-level import would be a cycle.
    from clients.quality_flags import alert_on_flag, append_audit, write_sidecar

    metadata: dict = {
        "asset_class": asset_class,
        "ticker": ticker,
        "timeframe": timeframe,
        "source": source,
        "bars_received": len(bars),
        "errors_during_fetch": errors_during_fetch or [],
        "expected_start": expected_start,
        "ib_head_timestamp": ib_head_timestamp,
    }
    if reference_source is not None:
        metadata["reference_source"] = reference_source

    try:
        flags = detect_all(bars=_normalize_bars_for_detection(bars), metadata=metadata, trading_calendar=None)
    except Exception as exc:  # pragma: no cover - detect_all wraps individual detectors
        if on_error is not None:
            on_error(exc)
        return

    if not flags:
        return

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    write_sidecar(parquet_path, flags, metadata)
    for flag in flags:
        append_audit(flag, source=source, ticker=ticker, timeframe=timeframe, parquet_path=parquet_path)
        if alerts_enabled:
            alert_on_flag(flag, source=source, ticker=ticker)

"""Shared outcome schema for daily jobs.

One machine-readable SUMMARY_JSON line per run is the contract between
daily_update.py (producer) and the wrapper / digest (consumers). Downstream
consumers parse this line instead of regexing human-readable prose — that
regex is what once reported 9,091 success lines as the "dominant error".
"""

from __future__ import annotations

import json

SUMMARY_PREFIX = "SUMMARY_JSON "

_ERROR_ABS_TOLERANCE = 50
_ERROR_RATE_TOLERANCE = 0.05


def build_summary_line(
    *,
    job: str,
    asset_class: str,
    source: str,
    target_date: str,
    updated: int,
    no_trade: int,
    partial: int,
    errors: int,
    bars_inserted: int,
    validation_issues: int,
    top_errors: list[tuple[str, int]],
    scanned: int | None = None,
    up_to_date: int | None = None,
) -> str:
    """Build the single machine-readable SUMMARY_JSON line for a run.

    `scanned`/`up_to_date` are the DENOMINATOR, and without them the four
    outcome counters cannot be read. Only symbols with a gap are fetched, so
    2026-08-17 reported `no_trade=974` for a universe of 13,385 of which 12,411
    were already current — a reader cannot tell that from "we only looked at
    974". Optional because they were added later: old log lines lack the keys
    and every consumer must keep parsing those.
    """
    payload = {
        "job": job,
        "asset_class": asset_class,
        "source": source,
        "target_date": target_date,
        "updated": updated,
        "no_trade": no_trade,
        "partial": partial,
        "errors": errors,
        "bars_inserted": bars_inserted,
        "validation_issues": validation_issues,
        "top_errors": [[msg, count] for msg, count in top_errors],
    }
    if scanned is not None:
        payload["scanned"] = scanned
    if up_to_date is not None:
        payload["up_to_date"] = up_to_date
    return SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":"))


def parse_last_summary_json(text: str) -> dict | None:
    """Return the last well-formed SUMMARY_JSON payload in *text*, or None."""
    result = None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith(SUMMARY_PREFIX):
            continue
        try:
            result = json.loads(stripped[len(SUMMARY_PREFIX) :])
        except json.JSONDecodeError:
            continue
    return result


def parse_all_summary_json(text: str) -> list[dict]:
    """Return every well-formed SUMMARY_JSON payload in *text*, in order."""
    results: list[dict] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith(SUMMARY_PREFIX):
            continue
        try:
            results.append(json.loads(stripped[len(SUMMARY_PREFIX) :]))
        except json.JSONDecodeError:
            continue
    return results


def resolve_exit_code(*, updated: int, no_trade: int, partial: int, errors: int) -> int:
    """Return 1 only for systemic failure; no_trade/partial never fail a run.

    Fails when there are zero updates on a processed run with any error, or
    when the error count exceeds max(50, 5% of processed).
    """
    processed = updated + no_trade + partial + errors
    if errors == 0:
        return 0
    if updated == 0 and processed > 0:
        return 1
    if errors > max(_ERROR_ABS_TOLERANCE, _ERROR_RATE_TOLERANCE * processed):
        return 1
    return 0

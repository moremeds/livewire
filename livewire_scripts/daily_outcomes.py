"""Shared outcome schema for daily jobs.

One machine-readable SUMMARY_JSON line per run is the contract between
daily_update.py (producer) and the wrapper / digest (consumers). Downstream
consumers parse this line instead of regexing human-readable prose — that
regex is what once reported 9,091 success lines as the "dominant error".
"""

from __future__ import annotations

import json
import re

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


# --- Meaningful error extraction -------------------------------------------
#
# A failure email that names the failing lane but not the error is a page that
# tells you to go read a log. Both job runners (daily and intraday) build their
# summary from these two functions, so the daily and intraday emails cannot
# drift apart. See pm:2026-09-08-failure-email-named-the-lane-not-the-error.

MAX_ERROR_LINES = 10
MAX_ERROR_LINE_CHARS = 400

_SECTION_HEADER = re.compile(r"^=== (?P<title>.+?) ===\s*$")
_EXIT_CODE = re.compile(r"exit_code=(\d+)")
_TRAILING_TIMESTAMP = re.compile(r"\s*\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z.*$")
_TRACEBACK_HEADER = "Traceback (most recent call last)"
_TRACEBACK_CHAIN = ("During handling of the above exception", "The above exception was the direct cause")
_ERROR_MARKERS = ("ERROR", "CRITICAL", "FATAL")


MAX_LOG_TAIL_BYTES = 262_144


def read_log_tail(log_file, max_bytes: int = MAX_LOG_TAIL_BYTES) -> str:
    """Read at most the last *max_bytes* of *log_file*, decoded leniently.

    A phase log is appended to for weeks — `daily_backfill_equity_union.log`
    was 44 MB on 2026-09-08 — and the error that ended the run is always at the
    end. Reading the whole file to quote two lines of it is how an alert path
    becomes the slow thing in a failing run.
    """
    from pathlib import Path

    path = Path(log_file)
    size = path.stat().st_size
    with path.open("rb") as handle:
        if size > max_bytes:
            handle.seek(size - max_bytes)
        raw = handle.read()
    return raw.decode("utf-8", errors="replace")


def clamp_lines(
    lines: list[str],
    *,
    max_lines: int = MAX_ERROR_LINES,
    max_chars: int = MAX_ERROR_LINE_CHARS,
) -> list[str]:
    """Keep the last *max_lines* non-empty lines, each truncated to *max_chars*.

    The email must stay small: a lane log is measured in megabytes and an
    unbounded quote is how a page becomes an attachment nobody opens.
    """
    kept = [line.rstrip() for line in lines if line.strip()][-max_lines:]
    return [line if len(line) <= max_chars else line[: max_chars - 1] + "…" for line in kept]


def _last_traceback(lines: list[str]) -> list[str]:
    start = None
    for index, line in enumerate(lines):
        if line.strip().startswith(_TRACEBACK_HEADER):
            start = index
    if start is None:
        return []
    block = [lines[start]]
    for line in lines[start + 1 :]:
        block.append(line)
        stripped = line.strip()
        if not stripped or line[0].isspace():
            continue
        if stripped.startswith(_TRACEBACK_HEADER) or stripped.startswith(_TRACEBACK_CHAIN):
            continue
        break  # the exception line closes the block

    # The innermost frame and the exception, not the whole stack: ten frames of
    # `livewire_store.py -> _dispatch_module -> main` are the same every time
    # and push the one line that differs out of a bounded email.
    frames = [index for index, line in enumerate(block) if line.strip().startswith('File "')]
    if not frames:
        return block
    tail = [line for line in block[frames[-1] + 1 :] if line and not line[0].isspace()]
    return [block[frames[-1]].strip(), *tail]


def extract_error_lines(
    text: str,
    *,
    max_lines: int = MAX_ERROR_LINES,
    max_chars: int = MAX_ERROR_LINE_CHARS,
) -> list[str]:
    """Return the lines of *text* that actually say what went wrong.

    In order of authority: the last traceback (its innermost frames and the
    exception line), then the aggregated `top_errors` of the section's own
    SUMMARY_JSON, then ERROR/CRITICAL/FATAL log lines. Empty when the section
    carries none of those — the caller decides what to say instead.
    """
    lines = text.splitlines()
    traceback_block = _last_traceback(lines)
    if traceback_block:
        return clamp_lines(traceback_block, max_lines=max_lines, max_chars=max_chars)

    summary = parse_last_summary_json(text)
    top_errors = (summary or {}).get("top_errors") or []
    if top_errors:
        return clamp_lines(
            [
                f'{"dominant" if index == 0 else "also"} error ({count}x): "{message}"'
                for index, (message, count) in enumerate(top_errors)
            ],
            max_lines=max_lines,
            max_chars=max_chars,
        )

    error_lines = [line for line in lines if any(marker in line for marker in _ERROR_MARKERS)]
    return clamp_lines(error_lines, max_lines=max_lines, max_chars=max_chars)


def last_failed_section(text: str) -> tuple[str | None, int | None, str]:
    """Split *text* at its last `=== ... Failed ... ===` marker.

    Returns `(unit, exit_code, section)` where *unit* is the lane/phase whose
    own `=== <unit> <ts> ===` header opened the failing section. Quoting the
    whole file instead is how a failing DuckDB build got reported with the
    counters of the Silver lane that succeeded before it.
    """
    lines = text.splitlines()
    headers = [
        (index, match.group("title")) for index, line in enumerate(lines) if (match := _SECTION_HEADER.match(line))
    ]
    failures = [(index, title) for index, title in headers if "failed" in title.lower()]
    if not failures:
        return None, None, text

    marker_index, marker_title = failures[-1]
    exit_match = _EXIT_CODE.search(marker_title)
    exit_code = int(exit_match.group(1)) if exit_match else None

    opening = [(index, title) for index, title in headers if index < marker_index]
    if opening:
        start, title = opening[-1]
        unit = _TRAILING_TIMESTAMP.sub("", title).strip() or None
        section = "\n".join(lines[start + 1 : marker_index])
    else:
        unit = None
        section = "\n".join(lines[:marker_index])
    return unit, exit_code, section


def last_meaningful_line(text: str) -> str | None:
    """The last non-empty line that is not a `=== ... ===` section marker."""
    for line in reversed(text.splitlines()):
        stripped = line.strip()
        if stripped and not stripped.startswith("==="):
            return stripped
    return None

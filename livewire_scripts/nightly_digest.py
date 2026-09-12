#!/usr/bin/env python3
"""The unconditional daily digest: today's coverage, what changed, what mailed.

One ``executions(script='notify', kind='digest')`` row every run — ``force=True``
means an unchanged day still produces the email and the receipt; the receipt's
``verdicts`` map is what tomorrow's CHANGED block diffs against.

Every block renders UNKNOWN, never blanks: a missing measurement is a fact
about coverage, not a hole in the email.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients import ledger
from livewire_scripts import notify
from livewire_scripts.paths import data_lake_dir, log_dir
from livewire_scripts.status import Section, Verdict, collect

_EPOCH = date(1970, 1, 1)
_SCOPES = ("1d", "1m", "5m", "30m", "1h")
_COVERAGE_NAMES = (
    "coverage_pct",
    "coverage_total",
    "coverage_recovery_deferred",
    "coverage_still_missing",
    "coverage_scan_ok",
    "coverage_elapsed_s",
    "stale_non_equity",
    "last_session",
)
# last_session is scoped by lane; stale_non_equity by registry asset class.
_LANE_FOR_CLASS = {"volatility": "cboe", "corporate_action": "corporate-actions"}


def _coverage_rows() -> list[dict]:
    """Latest two of each coverage measurement per scope, newest first."""
    names = ",".join(f"'{n}'" for n in _COVERAGE_NAMES)
    return ledger.query(
        "select name, scope, value, measured_at from ("
        "  select name, scope, value, measured_at, "
        "    row_number() over (partition by name, scope order by measured_at desc, seq desc) as rn "
        f"  from measurements where name in ({names})"
        ") where rn <= 2 order by name, scope, measured_at desc"
    )


def _previous_verdicts() -> dict:
    """{check: verdict} from the last successful digest row, else {}."""
    rows = ledger.query(
        "select receipt_json from executions where script = 'notify' and exit_code = 0 "
        "and json_extract_string(receipt_json,'$.kind') = 'digest' "
        "and json_extract_string(receipt_json,'$.skipped') = 'false' "
        "order by started desc limit 1"
    )
    if not rows:
        return {}
    try:
        verdicts = json.loads(str(rows[0]["receipt_json"])).get("verdicts")
        return dict(verdicts) if isinstance(verdicts, dict) else {}
    except (TypeError, ValueError):
        return {}


def _latest(rows: list[dict], name: str, scope: str) -> dict | None:
    for row in rows:  # _coverage_rows returns newest first per (name, scope)
        if row["name"] == name and row["scope"] == scope:
            return row
    return None


def _coverage_block(rows: list[dict]) -> str:
    as_of = next((r["measured_at"] for r in rows if r["name"] == "coverage_pct"), None)
    scan = _latest(rows, "coverage_scan_ok", "all")
    elapsed = _latest(rows, "coverage_elapsed_s", "all")
    stamp = (
        f"{as_of:%Y-%m-%d %H:%M}Z" if isinstance(as_of, datetime) else (str(as_of) if as_of is not None else "UNKNOWN")
    )
    parts = []
    if scan is not None:
        parts.append(f"scan ok={int(scan['value'])}")
    if elapsed is not None:
        parts.append(f"{elapsed['value']:.0f}s")
    lines = [f"COVERAGE as of {stamp} ({', '.join(parts) or 'no scan row'})"]
    for scope in _SCOPES:
        pcts = [r for r in rows if r["name"] == "coverage_pct" and r["scope"] == scope]
        total = _latest(rows, "coverage_total", scope)
        missing = _latest(rows, "coverage_still_missing", scope)
        if not pcts and total is None:
            lines.append(f"  {scope:<4} UNKNOWN")
            continue
        n = int(total["value"]) if total else 0
        count = f"{n - int(missing['value'])}/{n}" if missing is not None else f"{n} total"
        line = f"  {scope:<4} {count:>15}  {pcts[0]['value']:.2%}" if pcts else f"  {scope:<4} {count:>15}  UNKNOWN"
        if len(pcts) > 1:
            line += f"   (prev {pcts[1]['value']:.2%})"
        deferred = sum(
            1 for r in rows if r["name"] == "coverage_recovery_deferred" and r["scope"] == scope and r["value"]
        )
        if deferred:
            line += f"  recovery=deferred x{deferred}"
        if missing is not None and missing["value"]:
            line += f"  still_missing={int(missing['value'])}"
        lines.append(line)
    stale = {r["scope"]: r["value"] for r in rows if r["name"] == "stale_non_equity"}
    sessions = {}
    for r in rows:  # newest first — first write per scope wins
        if r["name"] == "last_session" and r["scope"] not in sessions:
            sessions[r["scope"]] = _EPOCH + timedelta(days=int(r["value"]))
    if stale:
        rendered = []
        for ac in sorted(stale):
            lane = ac if ac in sessions else _LANE_FOR_CLASS.get(ac)
            stamp = f"@{sessions[lane]:%m-%d}" if lane in sessions else ""
            rendered.append(f"{ac} missing={int(stale[ac])}{stamp}")
        lines.append("  non-equity: " + "  ".join(rendered))
    return "\n".join(lines)


def _changed_block(sections: list[Section], previous: dict) -> str:
    lines = ["CHANGED since yesterday's digest"]
    diffs = [
        f"  {s.name}: {previous[s.name]} -> {s.verdict.name}"
        for s in sections
        if s.name in previous and previous[s.name] != s.verdict.name
    ]
    new = [f"  {s.name}: (new) {s.verdict.name}" for s in sections if s.name not in previous]
    body = diffs + new
    lines.extend(body or ["  (unchanged)"])
    return "\n".join(lines)


def _sent_block(sent_rows: list[dict]) -> str:
    lines = ["SENT to you in the last 24h"]
    for row in sent_rows:
        started = row["started"]
        stamp = f"{started:%Y-%m-%d %H:%M}Z" if isinstance(started, datetime) else str(started)
        if str(row.get("skipped")) == "true":
            lines.append(f"  {stamp} {row['kind']:<6} (skipped, same state)")
        else:
            lines.append(f"  {stamp} {row['kind']:<6} {row.get('subject') or ''}   exit={row['exit_code']}")
    if not sent_rows:
        lines.append("  (nothing sent)")
    return "\n".join(lines)


def _status_block(sections: list[Section]) -> str:
    lines = ["STATUS (every check)"]
    for section in sections:
        headline = section.lines[0] if section.lines else "(no detail)"
        lines.append(f"[{section.verdict.glyph}] {section.name}: {headline}")
        lines.extend(f"  {line}" for line in section.lines[1:])
        if section.fix and section.verdict is not Verdict.OK:
            lines.append(f"  fix: {section.fix}")
    if not sections:
        lines.append("  (no checks collected)")
    return "\n".join(lines)


def build_digest(
    run_date: date,
    sections: list[Section],
    *,
    previous_verdicts: dict,
    coverage_rows: list[dict],
    sent_rows: list[dict],
    now: datetime | None = None,
) -> str:
    """Assemble the digest text. Pure render — never raises on missing inputs."""
    now = now or datetime.now(UTC)
    blocks = [f"Livewire digest — {run_date.isoformat()} (sent {now:%H:%M}Z)"]
    blocks.append(_coverage_block(coverage_rows))
    blocks.append(_changed_block(sections, previous_verdicts))
    blocks.append(_sent_block(sent_rows))
    blocks.append(_status_block(sections))
    return "\n\n".join(blocks) + "\n"


def main(argv=None, runner=None) -> int:
    parser = argparse.ArgumentParser(description="Build and send the Livewire daily digest")
    parser.add_argument("--run-date", type=date.fromisoformat, default=datetime.now(UTC).date())
    parser.add_argument("--email", action="store_true", help="Send via notify.send (always; no dedup)")
    parser.add_argument("--body-out", type=Path, help="Render to PATH; no send, no ledger write")
    parser.add_argument("--log-dir", type=Path, default=log_dir())
    parser.add_argument("--data-lake", type=Path, default=data_lake_dir())
    args = parser.parse_args(argv)

    sections = collect(args.run_date, args.log_dir, args.data_lake)
    body = build_digest(
        args.run_date,
        sections,
        previous_verdicts=_previous_verdicts(),
        coverage_rows=_coverage_rows(),
        sent_rows=notify.sent_within(24),
    )
    print(body)
    if args.body_out:
        args.body_out.parent.mkdir(parents=True, exist_ok=True)
        args.body_out.write_text(body, encoding="utf-8")
    if not args.email:
        return 0
    os.environ.setdefault("LW_RUN_ID", ledger.new_run_id("digest"))
    notice = notify.Notice(
        "digest",
        f"Digest {args.run_date.isoformat()}",
        body,
        notify.fingerprint("digest", [args.run_date.isoformat()]),
    )
    return notify.send(
        notice,
        force=True,
        runner=runner,
        receipt_extra={"verdicts": {s.name: s.verdict.name for s in sections}},
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

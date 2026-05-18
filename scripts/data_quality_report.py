#!/usr/bin/env python
"""Unified CLI for telemetry + quality-audit aggregation.

Views: summary | flap | quality
Sources: ib | uw | massive | all
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TELEMETRY = Path.home() / "market-warehouse" / "logs" / "telemetry.jsonl"
DEFAULT_AUDIT = Path.home() / "market-warehouse" / "logs" / "quality_audit.jsonl"
_EMAIL_SCRIPT = REPO_ROOT / "scripts" / "send_daily_update_failure_email.mjs"

_SINCE_RE = re.compile(r"^(\d+)\s*([smhd])$")


def _parse_since(raw: str) -> timedelta:
    match = _SINCE_RE.match(raw.strip())
    if not match:
        raise argparse.ArgumentTypeError(f"invalid --since: {raw!r}")
    n, unit = int(match.group(1)), match.group(2)
    return {
        "s": timedelta(seconds=n),
        "m": timedelta(minutes=n),
        "h": timedelta(hours=n),
        "d": timedelta(days=n),
    }[unit]


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Livewire data quality report")
    p.add_argument("--view", choices=["summary", "flap", "quality"], required=True)
    p.add_argument("--since", default="24h", type=_parse_since)
    p.add_argument("--source", default="all", choices=["all", "ib", "uw", "massive"])
    p.add_argument("--severity", default=None, choices=[None, "info", "warning", "critical"])
    p.add_argument("--telemetry-path", type=Path, default=DEFAULT_TELEMETRY)
    p.add_argument("--audit-path", type=Path, default=DEFAULT_AUDIT)
    p.add_argument(
        "--email",
        action="store_true",
        help="Render HTML and spawn Nodemailer daily-summary",
    )
    return p.parse_args(argv)


def _iter_jsonl(path: Path) -> Iterable[dict]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _load_since(path: Path, *, since: datetime) -> list[dict]:
    out = []
    for row in _iter_jsonl(path):
        ts = row.get("ts")
        if not ts:
            continue
        try:
            parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed >= since:
            row["_ts"] = parsed
            out.append(row)
    return out


def load_telemetry(path: Path, *, since: datetime) -> list[dict]:
    return _load_since(path, since=since)


def load_audit(path: Path, *, since: datetime) -> list[dict]:
    return _load_since(path, since=since)


def _compute_farm_uptime(
    transitions: list[tuple[datetime, str]],
    window_start: datetime,
    window_end: datetime,
) -> float:
    transitions = sorted(transitions, key=lambda x: x[0])
    denom_start = max(window_start, transitions[0][0])
    ok_seconds = 0.0
    prev_t, prev_state = transitions[0]
    for t, state in transitions[1:]:
        segment_start = max(prev_t, denom_start)
        segment_end = min(t, window_end)
        if prev_state == "ok" and segment_end > segment_start:
            ok_seconds += (segment_end - segment_start).total_seconds()
        prev_t, prev_state = t, state
    segment_start = max(prev_t, denom_start)
    if prev_state == "ok" and window_end > segment_start:
        ok_seconds += (window_end - segment_start).total_seconds()
    denom = max(1.0, (window_end - denom_start).total_seconds())
    return 100.0 * ok_seconds / denom


def _compute_flap_count(transitions: list[tuple[datetime, str]]) -> int:
    """A flap burst is at least 3 transitions with each pair under 10 minutes apart."""
    if len(transitions) < 3:
        return 0
    transitions = sorted(transitions)
    bursts = 0
    burst_len = 1
    for i in range(1, len(transitions)):
        if (transitions[i][0] - transitions[i - 1][0]).total_seconds() < 600:
            burst_len += 1
        else:
            if burst_len >= 3:
                bursts += 1
            burst_len = 1
    if burst_len >= 3:
        bursts += 1
    return bursts


def compute_summary(
    telemetry: list[dict],
    audit: list[dict],
    *,
    window_start: datetime,
    window_end: datetime,
) -> dict:
    by_source_farm: dict[tuple[str, Optional[str]], list[tuple[datetime, str]]] = defaultdict(list)
    by_source_events: Counter = Counter()
    for row in telemetry:
        source = row.get("source", "?")
        by_source_events[source] += 1
        if row.get("event") == "farm_state":
            by_source_farm[(source, row.get("farm"))].append(
                (row["_ts"], row.get("state", "?"))
            )

    sources = []
    source_names = sorted({source for source, _ in by_source_farm} | set(by_source_events))
    for source in source_names:
        farms = []
        for (farm_source, farm), transitions in by_source_farm.items():
            if farm_source != source:
                continue
            farms.append({
                "farm": farm or "(unknown)",
                "uptime_pct": round(
                    _compute_farm_uptime(transitions, window_start, window_end),
                    1,
                ),
                "flap_count": _compute_flap_count(transitions),
                "mtbd_seconds": None,
            })
        sources.append({
            "source": source,
            "connection_events": by_source_events[source],
            "farms": farms,
        })

    flag_counts: Counter = Counter()
    ticker_counts: Counter = Counter()
    for row in audit:
        flag_counts[row.get("category", "?")] += 1
        ticker_counts[row.get("ticker", "?")] += 1

    return {
        "window": f"{window_start.isoformat()} -> {window_end.isoformat()}",
        "sources": sources,
        "flag_counts_by_category": dict(flag_counts),
        "top_tickers": [
            {"ticker": ticker, "flag_count": count}
            for ticker, count in ticker_counts.most_common(10)
        ],
    }


def render_summary_text(summary: dict) -> str:
    lines = ["=== Livewire Data Quality Summary ===", f"Window: {summary['window']}", ""]
    for source in summary["sources"]:
        lines.append(f"[{source['source']}] events={source['connection_events']}")
        for farm in source["farms"]:
            lines.append(
                f"  farm={farm['farm']} uptime={farm['uptime_pct']}% "
                f"flaps={farm['flap_count']}"
            )
    lines.append("")
    lines.append("Quality flags by category:")
    for category, count in summary["flag_counts_by_category"].items():
        lines.append(f"  {category}: {count}")
    lines.append("")
    lines.append("Top affected tickers:")
    for ticker in summary["top_tickers"]:
        lines.append(f"  {ticker['ticker']}: {ticker['flag_count']} flag(s)")
    return "\n".join(lines)


def render_flap_view(telemetry: list[dict]) -> str:
    farm_events: dict[str, list[dict]] = defaultdict(list)
    for row in telemetry:
        if row.get("event") != "farm_state":
            continue
        farm_events[row.get("farm") or "(unknown)"].append(row)

    lines = ["=== Flap windows (chronological) ==="]
    for farm in sorted(farm_events):
        events = sorted(farm_events[farm], key=lambda x: x["_ts"])
        lines.append(f"\n[{farm}] {len(events)} transitions")
        for event in events:
            lines.append(
                f"  {event['_ts'].strftime('%Y-%m-%dT%H:%M:%SZ')} "
                f"state={event.get('state')} code={event.get('code')}"
            )
    return "\n".join(lines)


def render_quality_view(
    audit: list[dict],
    *,
    severity_filter: Optional[str] = None,
) -> str:
    rows = sorted(audit, key=lambda x: x["_ts"])
    if severity_filter:
        order = {"info": 0, "warning": 1, "critical": 2}
        rows = [
            row
            for row in rows
            if order.get(row.get("severity"), 0) >= order.get(severity_filter, 0)
        ]

    lines = ["=== Quality flags ==="]
    for row in rows:
        lines.append(
            f"{row['_ts'].strftime('%Y-%m-%dT%H:%M:%SZ')} "
            f"[{row.get('severity', '?'):>8}] "
            f"{row.get('source')}/{row.get('ticker')}/"
            f"{row.get('timeframe', '1d')} {row.get('category')}"
        )
    return "\n".join(lines)


def _resolve_log_dir() -> Path:
    raw = os.environ.get(
        "MDW_LOG_DIR",
        str(Path.home() / "market-warehouse" / "logs"),
    )
    return Path(raw).expanduser()


def _send_email(summary: dict) -> bool:
    payload = json.dumps(summary, default=str)
    cmd = [
        "node",
        str(_EMAIL_SCRIPT),
        "--mode",
        "daily-summary",
        "--payload",
        payload,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=60)
    except (subprocess.SubprocessError, OSError) as exc:
        print(f"daily-summary email spawn failed: {exc}", file=sys.stderr)
        return False
    if result.returncode != 0:
        print(
            f"daily-summary email returned {result.returncode}: {result.stderr!r}",
            file=sys.stderr,
        )
        return False

    log_dir = _resolve_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    (log_dir / f"quality_summary_{date_str}.marker").write_text(
        "ok\n",
        encoding="utf-8",
    )
    return True


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    window_start = now - args.since
    telemetry = load_telemetry(args.telemetry_path, since=window_start)
    audit = load_audit(args.audit_path, since=window_start)

    if args.source != "all":
        telemetry = [row for row in telemetry if row.get("source") == args.source]
        audit = [row for row in audit if row.get("source") == args.source]

    if args.view == "summary":
        summary = compute_summary(
            telemetry,
            audit,
            window_start=window_start,
            window_end=now,
        )
        print(render_summary_text(summary))
        if args.email:
            _send_email(summary)
    elif args.view == "flap":
        print(render_flap_view(telemetry))
    elif args.view == "quality":
        print(render_quality_view(audit, severity_filter=args.severity))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

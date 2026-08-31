"""Detect coverage gaps from the denominator and route them by tier.

Tier A becomes a manifest for the existing shepherd_repair executor.
Tier B becomes a decision request that nothing consumes in Phase 1 — its
queue depth is the measurement that decides whether an agent lane is worth
building.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from clients.coverage_denominator import build_denominator
from clients.gap_engine import (
    Finding,
    actual_sessions,
    classify,
    load_unresolved,
    suppress_unresolved,
)
from clients.gap_registry import load_registry

# measured, docs/audits/2026-07-11-daily-bronze-repair.md:58
MASSIVE_FLOOR = date(2021, 7, 12)


def scan(
    bronze_root: Path,
    registry_path: Path,
    presets_dir: Path,
    start: date,
    end: date,
    as_of: date,
    massive_floor: date = MASSIVE_FLOOR,
    unresolved_path: Path | None = None,
) -> list[Finding]:
    unresolved = load_unresolved(unresolved_path) if unresolved_path else set()
    findings: list[Finding] = []
    for row in load_registry(registry_path):
        preset_paths = [presets_dir / f"{name}.json" for name in row.universe]
        for series in build_denominator(
            preset_paths, row.asset_class, row.timeframe, start, end, as_of
        ):
            present = actual_sessions(bronze_root, series)
            findings.extend(classify(series, present, massive_floor))
    return sorted(
        suppress_unresolved(findings, unresolved), key=lambda f: f.heal_by_days
    )


def _entry(finding: Finding) -> dict:
    return {
        "symbol": finding.symbol,
        "asset_class": finding.asset_class,
        "timeframe": finding.timeframe,
        "gap": finding.gap,
        "sessions": [session.isoformat() for session in finding.sessions],
        "heal_by_days": finding.heal_by_days,
    }


def write_tier_a_manifest(findings: list[Finding], path: Path) -> None:
    repairs = [
        _entry(f)
        for f in sorted(
            (f for f in findings if f.tier == "A"), key=lambda f: f.heal_by_days
        )
    ]
    Path(path).write_text(json.dumps({"repairs": repairs}, indent=2))


def write_decision_requests(findings: list[Finding], path: Path) -> None:
    """Tier B queue. Verdict vocabulary is triage_breaks.py's, not a new one."""
    requests = [
        dict(_entry(f), verdict="inconclusive") for f in findings if f.tier == "B"
    ]
    Path(path).write_text(json.dumps(requests, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="livewire_quality.py gap-scan")
    parser.add_argument("--bronze-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, default=Path("registry/gaps.json"))
    parser.add_argument("--presets-dir", type=Path, default=Path("presets"))
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    parser.add_argument("--unresolved", type=Path, default=None)
    parser.add_argument("--manifest-out", type=Path, required=True)
    parser.add_argument("--decisions-out", type=Path, required=True)
    args = parser.parse_args(argv)

    findings = scan(
        args.bronze_root,
        args.registry,
        args.presets_dir,
        args.start,
        args.end,
        args.as_of,
        unresolved_path=args.unresolved,
    )
    write_tier_a_manifest(findings, args.manifest_out)
    write_decision_requests(findings, args.decisions_out)
    print(
        json.dumps(
            {
                "findings": len(findings),
                "tier_a": sum(1 for f in findings if f.tier == "A"),
                "tier_b": sum(1 for f in findings if f.tier == "B"),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Batch-triage audit-reported discontinuities against Massive as a second source.

Reads a legacy-basis audit manifest and, for every break it recorded — one symbol may
have several — asks `clients.break_triage` whether that break is a real market move,
bad bronze data, or a corporate action our store lacks. Read-only with respect to
bronze: it emits a verdict manifest that the silver-window resolver consumes to decide
what to keep. Resumable — each verdict is checkpointed as it lands, so a provider
rate-limit stall resumes instead of re-spending the whole population.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from clients.break_triage import DEFAULT_TOLERANCE, RetryableProviderError, triage_break
from clients.massive_client import MassiveAuthError, MassiveClient
from livewire_scripts.paths import data_lake_dir

SCHEMA_VERSION = 1
VERDICTS = ("real_move", "bad_data", "missing_action", "inconclusive")
# A liquid, long-listed symbol whose recent bars are inside any entitlement window.
CREDENTIAL_PROBE_SYMBOL = "AAPL"
CREDENTIAL_PROBE_DAYS = 10


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-lake-root", type=Path)
    parser.add_argument("--tickers", nargs="+", help="narrow to these symbols (default: every flagged symbol)")
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE)
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _key(candidate: dict) -> str:
    """Cursor key. Per (symbol, break) — a symbol may have several breaks to triage."""
    return f"{candidate['symbol']}@{candidate['date']}"


def _fetcher(client: Any, *, adjusted: bool) -> Callable[[str, date, date], list[dict]]:
    """Adapt MassiveClient's dataclass bars to the plain rows `triage_break` reads."""

    def fetch(symbol: str, start: date, end: date) -> list[dict]:
        bars = client.get_daily_bars(symbol, start, end, adjusted=adjusted)
        return [{"trade_date": bar.trade_date.isoformat(), "close": bar.close} for bar in bars]

    return fetch


def assert_credentials_work(client: Any, as_of: date) -> None:
    """Prove the key is accepted for a date INSIDE the entitlement window.

    ``triage_break`` reads MassiveAuthError as "not entitled for this date" and
    records a final `inconclusive`, which trims. That is right for the rolling ~5y
    floor, which only rejects OLD dates — but a present-but-invalid key 401s on
    EVERY date, so without this probe a bad key would checkpoint the whole
    population as inconclusive and amputate it permanently. The operator cannot
    catch it by eye either: a large inconclusive count is the expected shape.
    """
    try:
        client.get_daily_bars(
            CREDENTIAL_PROBE_SYMBOL, as_of - timedelta(days=CREDENTIAL_PROBE_DAYS), as_of, adjusted=False
        )
    except MassiveAuthError as exc:
        raise ValueError(
            f"Massive rejected {CREDENTIAL_PROBE_SYMBOL} for the last {CREDENTIAL_PROBE_DAYS} days, which is inside "
            f"any entitlement window: the credentials are bad, not the date range. Refusing to triage — every "
            f"candidate would record a final 'inconclusive' and be trimmed. ({exc})"
        ) from exc


def run(
    argv: Sequence[str] | None = None,
    *,
    data_lake_root: Path | None = None,
    massive_factory: Callable[[], Any] = MassiveClient,
    as_of_date: date | None = None,
) -> int:
    args = parse_args(argv)
    root = Path(data_lake_root) if data_lake_root is not None else (args.data_lake_root or data_lake_dir())
    as_of = as_of_date or datetime.now(UTC).date()
    audit = json.loads(args.audit_manifest.read_text())
    audit_sha256 = _sha256(args.audit_manifest)
    # Same guard as repair: a manifest audited against another lake must never
    # decide what this lake trims.
    manifest_root = audit.get("data_lake_root")
    if manifest_root is not None and manifest_root != str(root.resolve()):
        raise ValueError(f"audit manifest data_lake_root {manifest_root} does not match active root {root.resolve()}")

    # One candidate per (symbol, break) — NOT per symbol. A multi-break symbol whose
    # later break is never triaged has that break trimmed away with its real history.
    # Breaks with no ratio (a non-positive close) are unusable as a second-source
    # comparison and are left to the window, which drops them regardless.
    candidates = [
        {"symbol": entry["symbol"], "date": str(brk["date"])[:10], "ratio": float(brk["ratio"])}
        for entry in audit["symbols"]
        for brk in entry.get("breaks") or []
        if brk.get("ratio") is not None
    ]
    if args.tickers:
        wanted = {t.upper() for t in args.tickers}
        candidates = [c for c in candidates if c["symbol"] in wanted]

    identity = {"schema_version": SCHEMA_VERSION, "audit_sha256": audit_sha256, "data_lake_root": str(root.resolve())}
    cursor_path = args.output.with_name(f"{args.output.name}.cursor.json")
    cursor: dict = {"identity": identity, "verdicts": {}}
    if args.resume and cursor_path.is_file():
        loaded = json.loads(cursor_path.read_text())
        if loaded.get("identity") != identity:
            raise ValueError("resume cursor does not match the active audit manifest")
        cursor = loaded

    todo = [c for c in candidates if _key(c) not in cursor["verdicts"]]
    aborted = False
    if todo:
        # Construct the client OUTSIDE triage_break: a missing MASSIVE_API_KEY raises
        # MassiveAuthError here and aborts, rather than reading as N "inconclusive"
        # verdicts that would silently trim the entire population.
        with massive_factory() as client:
            assert_credentials_work(client, as_of)
            fetch_raw = _fetcher(client, adjusted=False)
            fetch_adjusted = _fetcher(client, adjusted=True)
            for candidate in todo:
                try:
                    verdict = triage_break(
                        candidate["symbol"],
                        candidate["date"],
                        candidate["ratio"],
                        fetch_raw=fetch_raw,
                        fetch_adjusted=fetch_adjusted,
                        tolerance=args.tolerance,
                    )
                except RetryableProviderError as exc:
                    # Leave this candidate un-cursored so --resume re-asks. Checkpointing
                    # it would bake a transient outage into a permanent trim.
                    print(f"transient provider failure, aborting run: {exc}", file=sys.stderr)
                    aborted = True
                    break
                cursor["verdicts"][_key(candidate)] = verdict
                _write_atomic(cursor_path, cursor)  # checkpoint per break, not per run

    verdicts = [cursor["verdicts"][_key(c)] for c in candidates if _key(c) in cursor["verdicts"]]
    counts = {name: sum(v["verdict"] == name for v in verdicts) for name in VERDICTS}
    _write_atomic(
        args.output,
        {
            "schema_version": SCHEMA_VERSION,
            "data_lake_root": str(root.resolve()),
            "audit_sha256": audit_sha256,
            "generated_at": datetime.now(UTC).isoformat(),
            "tolerance": args.tolerance,
            "complete": not aborted and len(verdicts) >= len(candidates),
            "counts": counts,
            "verdicts": sorted(verdicts, key=lambda v: (v["symbol"], v["date"])),
        },
    )
    print(json.dumps({**counts, "output": str(args.output), "aborted": aborted}, sort_keys=True))
    return 1 if aborted else 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

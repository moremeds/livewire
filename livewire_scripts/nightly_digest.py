#!/usr/bin/env python3
"""Build the single nightly digest for Livewire.

Assembles one plain-text report from the machine-readable SUMMARY_JSON lines the
daily jobs now emit, plus the day's coverage line and disk headroom. This
replaces the noisy per-warrant daily-summary email: one digest on success, and
a rare truthful failure mail only on systemic failure.

Every section renders "(not found)" for missing inputs — build_digest never
raises, so a missing log can never suppress the whole digest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients import ledger
from clients.parquet_io import path_lock
from livewire_scripts.job_runner_common import process_group_guard
from livewire_scripts.paths import data_lake_dir, log_dir
from livewire_scripts.status import Section, Verdict, collect
from livewire_scripts.sync_runner import TIMEOUT_EXIT_CODE, phase_timeout_seconds

_FAILURE_EMAIL_SCRIPT = _PROJECT_ROOT / "livewire_node" / "send_daily_update_failure_email.mjs"


def build_digest(run_date: date, log_dir: Path, data_lake: Path, *, sections: list[Section] | None = None) -> str:
    """Assemble the nightly digest text. Never raises on missing inputs.

    Renders exactly what `livewire_ops.py status` renders — same checks, same
    verdicts, same fixes. Anything added to collect() reaches both surfaces or
    neither; there is no list here to forget to update.
    """
    blocks = [f"Livewire nightly digest — {run_date.isoformat()}"]
    for section in sections if sections is not None else collect(run_date, log_dir, data_lake):
        headline = section.lines[0] if section.lines else f"{section.name}: (no detail)"
        lines = [f"[{section.verdict.glyph}] {headline}", *section.lines[1:]]
        # Same rule as render(): a fix line on a green section is noise, and
        # noise on the green path is what trains a reader to skim the email.
        if section.fix and section.verdict is not Verdict.OK:
            lines.append(f"  fix: {section.fix}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks) + "\n"


def _warning_fingerprint(sections: list[Section]) -> tuple[str, list[str]]:
    """Return a stable semantic state for scheduled-digest deduplication.

    Receipt records deliberately contain no timestamps, occurrence counts, run IDs, or
    raw error strings: those change during one sustained incident and would
    recreate the email storm this gate prevents.  A section can provide a
    precise key when its impact changes within the same verdict, including
    measured failure counts and affected scope. Unstructured legacy sections
    retain their name/verdict identity.
    """
    warnings = sorted(
        section.notification_key or f"{section.name}:{section.verdict.name}"
        for section in sections
        if section.verdict is not Verdict.OK
    )
    payload = json.dumps({"warnings": warnings}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest(), warnings


def _last_delivered_fingerprint() -> str | None:
    """Read the last successful digest receipt from existing ledger fields."""
    try:
        rows = ledger.query(
            "select receipt_json from executions where script = 'nightly_digest' and exit_code = 0 "
            "order by ended desc nulls last, started desc limit 1"
        )
        if not rows:
            return None
        receipt = json.loads(str(rows[0]["receipt_json"]))
        value = receipt.get("warning_fingerprint")
        return str(value) if value else None
    except Exception as exc:  # a state lookup must never suppress a digest
        print(f"digest delivery-state lookup failed: {exc}", file=sys.stderr)
        return None


def _record_delivery(run_date: date, fingerprint: str, warnings: list[str]) -> None:
    """Append success evidence after Nodemailer accepts the digest.

    ``receipt_json.warning_fingerprint`` is the stable notification state;
    ``receipt_json.warning_keys`` makes the semantic state inspectable without
    parsing an email body.  A failed record is intentionally not written here:
    the outer job keeps its existing failed-send evidence and retry path.
    """
    try:
        run = os.environ.get("LW_RUN_ID") or ledger.new_run_id("nightly-digest")
        now = datetime.now(UTC)
        ledger.emit(
            "executions",
            [
                {
                    "evidence_hash": f"sha256:{fingerprint}",
                    "script": "nightly_digest",
                    "attempt": 1,
                    "args_json": json.dumps({"mode": "digest", "run_date": run_date.isoformat()}),
                    "release_sha": None,
                    "started": now,
                    "ended": now,
                    "exit_code": 0,
                    "receipt_json": json.dumps(
                        {
                            "delivery": "accepted",
                            "warning_fingerprint": fingerprint,
                            "warning_keys": warnings,
                        },
                        sort_keys=True,
                    ),
                    "run_id": run,
                }
            ],
            run_id=run,
        )
    except Exception as exc:  # notification evidence is independent of delivery
        print(f"digest delivery receipt failed: {exc}", file=sys.stderr)


def _run_email_child(command, *, check=False, timeout):
    with subprocess.Popen(command, start_new_session=True) as proc:
        with process_group_guard(proc):
            proc.communicate(timeout=timeout)
        result = subprocess.CompletedProcess(command, proc.returncode)
    if check and result.returncode:
        raise subprocess.CalledProcessError(result.returncode, command)
    return result


def _send_email(body: str, run_date: date, node_bin: str, log_dir: Path, runner) -> int:
    body_file = log_dir / f"nightly_digest_{run_date.isoformat()}.txt"
    body_file.parent.mkdir(parents=True, exist_ok=True)
    body_file.write_text(body, encoding="utf-8")
    result = runner(
        [
            node_bin,
            str(_FAILURE_EMAIL_SCRIPT),
            "--mode",
            "digest",
            "--run-date",
            run_date.isoformat(),
            "--body-file",
            str(body_file),
        ],
        check=False,
        timeout=phase_timeout_seconds(),
    )
    return int(result.returncode or 0)


def main(argv=None, runner=None) -> int:
    parser = argparse.ArgumentParser(description="Build the Livewire nightly digest")
    parser.add_argument("--run-date", type=date.fromisoformat, default=datetime.now(UTC).date())
    parser.add_argument("--email", action="store_true", help="Send the digest via Nodemailer")
    parser.add_argument("--force-email", action="store_true", help="Send even when the warning state is unchanged")
    parser.add_argument("--log-dir", type=Path, default=log_dir())
    parser.add_argument("--data-lake", type=Path, default=data_lake_dir())
    args = parser.parse_args(argv)

    sections = collect(args.run_date, args.log_dir, args.data_lake)
    body = build_digest(args.run_date, args.log_dir, args.data_lake, sections=sections)
    print(body)
    if args.email or args.force_email:
        fingerprint, warnings = _warning_fingerprint(sections)
        # Delivery is the resource: serialize only this recipient's check,
        # body-file write, send and receipt. No lake or ingestion lock is held.
        with path_lock(ledger.ledger_root() / "nightly_digest_delivery.lock"):
            if not args.force_email and fingerprint == _last_delivered_fingerprint():
                print("digest notification unchanged; no email sent")
                return 0
            node_bin = os.getenv("MDW_NODE_BIN") or shutil.which("node") or "/opt/homebrew/bin/node"
            try:
                result = _send_email(body, args.run_date, node_bin, args.log_dir, runner or _run_email_child)
            except subprocess.TimeoutExpired:
                print("digest email timed out; no delivery receipt recorded", file=sys.stderr)
                return TIMEOUT_EXIT_CODE
            except OSError as exc:
                print(f"digest email failed: {exc}", file=sys.stderr)
                return 1
            if result == 0:
                _record_delivery(args.run_date, fingerprint, warnings)
            return result
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

"""One sender, one receipt: every email is an ``executions(script='notify')`` row.

Page (state is BAD now, deduped 24h by fingerprint) and digest (unconditional,
``force=True``) are the only kinds. The row is written on success, failure AND
skip — a skipped send is itself a fact. Nothing else in the repo sends mail.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from clients import ledger
from livewire_scripts.job_runner_common import process_group_guard
from livewire_scripts.paths import log_dir, warehouse_dir
from livewire_scripts.status import Section, Verdict

KINDS = ("page", "digest")

_SEND_MAIL = Path(__file__).resolve().parent.parent / "livewire_node" / "send_mail.mjs"


@dataclass(frozen=True)
class Notice:
    kind: str            # "page" | "digest"
    subject: str
    body: str
    fingerprint: str     # sha256 of the semantic state (page) or of run_date (digest)


def node_bin() -> str:
    """The one resolver: env, PATH, then the Homebrew default."""
    return os.environ.get("MDW_NODE_BIN") or shutil.which("node") or "/opt/homebrew/bin/node"


def fingerprint(kind: str, keys: list[str]) -> str:
    return hashlib.sha256("|".join([kind, *keys]).encode()).hexdigest()


def already_sent(fp: str, *, within_hours: int = 24) -> bool:
    rows = ledger.query(
        "select 1 as hit from executions where script='notify' and exit_code=0 "
        f"and started >= now() - interval {int(within_hours)} hour "
        f"and json_extract_string(receipt_json,'$.fingerprint') = '{fp}' "
        "and json_extract_string(receipt_json,'$.skipped') = 'false' limit 1"
    )
    return bool(rows)


def sent_within(hours: int = 24) -> list[dict]:
    """Every notify row in the window — the digest's 'SENT to you' block."""
    return ledger.query(
        "select started, exit_code, "
        "json_extract_string(receipt_json,'$.kind') as kind, "
        "json_extract_string(receipt_json,'$.subject') as subject, "
        "json_extract_string(receipt_json,'$.skipped') as skipped "
        "from executions where script='notify' "
        f"and started >= now() - interval {int(hours)} hour order by started"
    )


def page_from_sections(sections: list[Section], run_date: date) -> Notice | None:
    """A page for the current BAD state, or None when nothing is BAD."""
    bad = [s for s in sections if s.verdict == Verdict.BAD]
    if not bad:
        return None
    keys = sorted(s.notification_key or f"{s.name}:{s.verdict.name}" for s in bad)
    subject = f"PAGE {run_date}: " + "; ".join(s.name for s in bad)[:120]
    body_lines = [f"Livewire page — {run_date}", ""]
    for section in bad:
        body_lines.append(f"[{section.verdict.name}] {section.name}")
        body_lines.extend(f"  {line}" for line in section.lines)
        if section.fix:
            body_lines.append(f"  fix: {section.fix}")
        body_lines.append("")
    return Notice("page", subject, "\n".join(body_lines), fingerprint("page", keys))


def page_for_lane(run_date: date, lane: str, exit_code: int, error_summary: str, log_tail: str) -> Notice:
    """A page for one lane failure; dedup key is the current run + lane."""
    rid = os.environ.get("LW_RUN_ID") or "no-run"
    subject = f"PAGE {run_date}: lane {lane} failed (exit {exit_code})"
    body = (
        f"lane={lane} exit_code={exit_code} run={rid} date={run_date}\n\n"
        f"error summary:\n{error_summary}\n\nlog tail:\n{log_tail}\n"
    )
    return Notice("page", subject, body, fingerprint("page", [rid, lane]))


def _release_sha() -> str | None:
    try:
        return Path(os.readlink(warehouse_dir() / "current")).name
    except OSError:
        return None


def _run_child(command: list[str], *, timeout: int) -> subprocess.CompletedProcess:
    """Send in its own process group; the guard kills the group on timeout."""
    with subprocess.Popen(
        command, start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    ) as proc:
        with process_group_guard(proc):
            out, _ = proc.communicate(timeout=timeout)
    return subprocess.CompletedProcess(command, proc.returncode, out)


def _record(notice: Notice, started: datetime, ended: datetime, exit_code: int, receipt: dict) -> None:
    """Append the send's executions row; a failed write reports, never raises."""
    row = {
        "evidence_hash": "sha256:" + hashlib.sha256(notice.body.encode()).hexdigest(),
        "script": "notify",
        "attempt": 1,
        "args_json": json.dumps(
            {"kind": notice.kind, "subject": notice.subject, "body_file": receipt.get("body_file")}
        ),
        "release_sha": _release_sha(),
        "started": started,
        "ended": ended,
        "exit_code": exit_code,
        "receipt_json": json.dumps(receipt),
        "run_id": os.environ.get("LW_RUN_ID") or ledger.new_run_id("notify"),
    }
    try:
        ledger.emit("executions", [row], run_id=row["run_id"])
    except Exception as exc:  # pragma: no cover - a failed receipt cannot fail the caller
        print(f"notify: could not record send: {exc}", file=sys.stderr)


def send(
    notice: Notice,
    *,
    force: bool = False,
    runner=None,
    timeout_s: int = 120,
    body_path: Path | None = None,
    receipt_extra: dict | None = None,
) -> int:
    """Send one notice through send_mail.mjs and record the outcome.

    Dedup: a successful row with the same fingerprint in 24h makes the second
    send a no-op that is itself recorded (skipped=true). ``force`` bypasses.
    """
    if notice.kind not in KINDS:
        raise ValueError(f"unknown notice kind {notice.kind!r}")
    started = datetime.now(UTC)
    if not force and already_sent(notice.fingerprint):
        print(f"notify: {notice.kind} {notice.fingerprint[:12]} already sent within 24h; skipped")
        receipt = {
            "kind": notice.kind,
            "fingerprint": notice.fingerprint,
            "subject": notice.subject,
            "skipped": True,
        }
        if receipt_extra:
            receipt.update(receipt_extra)
        _record(notice, started, datetime.now(UTC), 0, receipt)
        return 0

    path = body_path or log_dir() / f"notify_{notice.kind}_{started:%Y%m%dT%H%M%SZ}.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(notice.body, encoding="utf-8")

    command = [node_bin(), str(_SEND_MAIL), f"--subject={notice.subject}", f"--body-file={path}"]
    stdout, error = "", None
    try:
        result = (runner or _run_child)(command, timeout=timeout_s)
        exit_code = int(result.returncode or 0)
        raw = result.stdout or ""
        stdout = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    except (subprocess.TimeoutExpired, OSError) as exc:
        exit_code, error = 1, str(exc)

    receipt = {
        "kind": notice.kind,
        "fingerprint": notice.fingerprint,
        "subject": notice.subject,
        "node_exit": exit_code,
        "node_stdout": stdout[:2000],
        "skipped": False,
        "body_file": str(path),
    }
    if error:
        receipt["error"] = error
    if receipt_extra:
        receipt.update(receipt_extra)
    _record(notice, started, datetime.now(UTC), exit_code, receipt)
    return exit_code

"""notify.py: one sender, one executions row per send — success, failure, skip."""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import date
from pathlib import Path

import pytest

from clients import ledger
from livewire_scripts import notify
from livewire_scripts.notify import Notice
from livewire_scripts.status import Section, Verdict


def _notice(kind="page", subject="PAGE 2026-09-12: Coverage BAD", body="revision=28 rebuilt=10\n", fp_keys=("k",)):
    return Notice(kind, subject, body, notify.fingerprint(kind, list(fp_keys)))


def _fake_runner(results):
    calls = []

    def run(cmd, *, timeout):
        calls.append(cmd)
        outcome = results[min(len(calls), len(results)) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return subprocess.CompletedProcess(cmd, outcome, stdout="node-out")

    return run, calls


def _rows(sql_extra=""):
    return ledger.query(
        "select exit_code, args_json, receipt_json, evidence_hash, script "
        f"from executions where script='notify' {sql_extra} order by started"
    )


def _section(name, verdict, *, key=None, lines=()):
    return Section(name=name, verdict=verdict, lines=list(lines), notification_key=key)


def test_send_records_success_and_failure_rows(tmp_path):
    runner, calls = _fake_runner([0, 1])
    n1 = _notice(fp_keys=["a"])
    n2 = _notice(subject="PAGE 2026-09-12: y", fp_keys=["b"])

    assert notify.send(n1, runner=runner) == 0
    assert notify.send(n2, runner=runner) == 1

    rows = _rows()
    assert sorted(r["exit_code"] for r in rows) == [0, 1]
    for row, notice in zip(rows, [n1, n2], strict=True):
        receipt = json.loads(row["receipt_json"])
        assert row["script"] == "notify"
        assert receipt["fingerprint"] == notice.fingerprint
        assert receipt["kind"] == "page"
        assert receipt["skipped"] is False
        assert row["evidence_hash"] == "sha256:" + __import__("hashlib").sha256(notice.body.encode()).hexdigest()
    # body landed in log_dir and the path was handed to node as --body-file=
    body_arg = next(a for a in calls[0] if a.startswith("--body-file="))
    assert Path(body_arg.split("=", 1)[1]).read_text() == n1.body
    assert "notify_page_" in body_arg


def test_same_fingerprint_within_24h_is_skipped_and_the_skip_is_recorded():
    runner, calls = _fake_runner([0])
    notice = _notice(fp_keys=["same"])

    assert notify.send(notice, runner=runner) == 0
    assert notify.send(notice, runner=runner) == 0

    assert len(calls) == 1
    rows = _rows()
    assert len(rows) == 2
    assert json.loads(rows[0]["receipt_json"])["skipped"] is False
    second = json.loads(rows[1]["receipt_json"])
    assert second["skipped"] is True and rows[1]["exit_code"] == 0


def test_force_bypasses_dedup():
    runner, calls = _fake_runner([0, 0])
    notice = _notice(fp_keys=["forced"])
    assert notify.send(notice, runner=runner) == 0
    assert notify.send(notice, runner=runner, force=True) == 0
    assert len(calls) == 2
    rows = _rows()
    assert all(json.loads(r["receipt_json"])["skipped"] is False for r in rows)


def test_a_failed_dedup_lookup_sends_and_records_dedup_error(monkeypatch, capsys):
    def boom(*args, **kwargs):
        raise RuntimeError("ledger down")

    original_query = notify.ledger.query
    monkeypatch.setattr(notify.ledger, "query", boom)
    runner, calls = _fake_runner([0])

    assert notify.send(_notice(fp_keys=["dedup-fails"]), runner=runner) == 0
    assert len(calls) == 1
    assert "dedup lookup failed: ledger down; sending" in capsys.readouterr().err

    monkeypatch.setattr(notify.ledger, "query", original_query)  # restore, then read the row back
    rows = _rows()
    assert len(rows) == 1
    receipt = json.loads(rows[0]["receipt_json"])
    assert receipt["dedup_error"] == "ledger down"
    assert receipt["skipped"] is False


def test_timeout_is_exit_1_with_error_in_receipt():
    runner, _ = _fake_runner([subprocess.TimeoutExpired(["node"], 120)])
    assert notify.send(_notice(fp_keys=["t"]), runner=runner) == 1
    rows = _rows()
    assert len(rows) == 1 and rows[0]["exit_code"] == 1
    assert "timed out" in json.loads(rows[0]["receipt_json"])["error"]


def test_page_from_sections_is_none_when_nothing_is_bad():
    sections = [_section("Coverage", Verdict.OK), _section("Disk", Verdict.WARN)]
    assert notify.page_from_sections(sections, date(2026, 9, 12)) is None


def test_page_fingerprint_ignores_timestamps():
    day = date(2026, 9, 12)
    a = [_section("Coverage", Verdict.BAD, key="coverage:1m=0.36", lines=["measured_at 11:02Z"])]
    b = [_section("Coverage", Verdict.BAD, key="coverage:1m=0.36", lines=["measured_at 12:14Z"])]
    # notification_key wins over name:verdict; detail lines are not identity.
    assert notify.page_from_sections(a, day).fingerprint == notify.page_from_sections(b, day).fingerprint

    c = [_section("Coverage", Verdict.BAD, lines=["measured_at 11:02Z"])]
    d = [_section("Coverage", Verdict.BAD, lines=["different detail entirely"])]
    assert notify.page_from_sections(c, day).fingerprint == notify.page_from_sections(d, day).fingerprint

    e = a + [_section("Digest sent today", Verdict.BAD, key="digest:missing")]
    assert notify.page_from_sections(e, day).fingerprint != notify.page_from_sections(a, day).fingerprint


def test_page_subjects_and_sent_within():
    day = date(2026, 9, 12)
    notice = notify.page_from_sections(
        [_section("Coverage recovery", Verdict.BAD, key="cov-rec:1m"), _section("Disk", Verdict.OK)], day
    )
    assert notice.subject.startswith("PAGE 2026-09-12:")
    assert "Coverage recovery" in notice.subject

    lane = notify.page_for_lane(day, "equity", 1, "boom", "last line")
    assert "equity" in lane.subject
    assert lane.kind == "page"

    runner, _ = _fake_runner([0])
    assert notify.send(notice, runner=runner) == 0
    sent = notify.sent_within(24)
    assert len(sent) == 1
    assert sent[0]["kind"] == "page" and sent[0]["exit_code"] == 0
    assert "PAGE 2026-09-12" in sent[0]["subject"]


def test_real_node_seam(tmp_path, monkeypatch):
    """No mock: the real send_mail.mjs over streamTransport into a real ledger."""
    resolved = notify.node_bin()
    if not (Path(resolved).exists() or shutil.which(resolved)):
        pytest.skip(f"node not resolvable: node_bin() -> {resolved}")
    monkeypatch.setenv("MDW_ALERT_TRANSPORT", "stream")
    monkeypatch.setenv("MDW_ALERT_EMAIL_FROM", "a@b")
    monkeypatch.setenv("MDW_ALERT_EMAIL_TO", "c@d")

    notice = _notice(kind="digest", subject="digest 2026-09-12", fp_keys=["seam"])
    assert notify.send(notice, body_path=tmp_path / "digest.txt") == 0

    rows = _rows()
    assert len(rows) == 1 and rows[0]["exit_code"] == 0
    receipt = json.loads(rows[0]["receipt_json"])
    assert "Subject: [Livewire] " in receipt["node_stdout"]
    assert "revision=28" in receipt["node_stdout"]
    assert json.loads(rows[0]["args_json"])["body_file"] == str(tmp_path / "digest.txt")

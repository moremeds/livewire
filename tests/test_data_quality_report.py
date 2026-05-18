import json
from datetime import datetime, timedelta

import pytest

from livewire_scripts.data_quality_report import (
    compute_summary,
    load_audit,
    load_telemetry,
    parse_args,
    render_summary_text,
)


def _utc(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def test_parse_args_summary_defaults():
    args = parse_args(["--view", "summary", "--since", "2h", "--source", "ib"])
    assert args.view == "summary"
    assert args.since == timedelta(hours=2)
    assert args.source == "ib"
    assert args.email is False


def test_parse_args_rejects_invalid_since():
    with pytest.raises(SystemExit):
        parse_args(["--view", "summary", "--since", "soon"])


def test_load_telemetry_skips_malformed_lines(tmp_path):
    f = tmp_path / "t.jsonl"
    f.write_text(
        '{"ts":"2026-05-17T00:00:00Z","source":"ib","event":"connected"}\n'
        '{"source":"ib","event":"missing_ts"}\n'
        '{"ts":"bad","source":"ib","event":"bad_ts"}\n'
        "NOT JSON LINE\n"
        '{"ts":"2026-05-17T00:01:00Z","source":"ib","event":"farm_state",'
        '"code":2106,"state":"ok","farm":"ushmds"}\n'
    )
    rows = load_telemetry(f, since=_utc("2026-05-16T00:00:00Z"))
    assert len(rows) == 2


def test_load_audit_filters_old_and_bad_rows(tmp_path):
    f = tmp_path / "audit.jsonl"
    f.write_text(
        '{"ts":"2026-05-15T00:00:00Z","ticker":"OLD","category":"range_shortfall"}\n'
        '{"ts":"bad","ticker":"BAD","category":"range_shortfall"}\n'
        '{"ts":"2026-05-17T00:00:00Z","ticker":"SMH","category":"range_shortfall"}\n'
        "\n"
    )
    rows = load_audit(f, since=_utc("2026-05-16T00:00:00Z"))
    assert [r["ticker"] for r in rows] == ["SMH"]


def test_load_missing_jsonl_returns_empty(tmp_path):
    assert load_telemetry(tmp_path / "missing.jsonl", since=_utc("2026-05-16T00:00:00Z")) == []


def test_compute_summary_uptime():
    rows = [
        {
            "ts": "2026-05-17T00:00:00Z",
            "_ts": _utc("2026-05-17T00:00:00Z"),
            "source": "ib",
            "event": "farm_state",
            "code": 2106,
            "state": "ok",
            "farm": "ushmds",
        },
        {
            "ts": "2026-05-17T01:00:00Z",
            "_ts": _utc("2026-05-17T01:00:00Z"),
            "source": "ib",
            "event": "farm_state",
            "code": 2105,
            "state": "broken",
            "farm": "ushmds",
        },
        {
            "ts": "2026-05-17T01:30:00Z",
            "_ts": _utc("2026-05-17T01:30:00Z"),
            "source": "ib",
            "event": "farm_state",
            "code": 2106,
            "state": "ok",
            "farm": "ushmds",
        },
    ]
    audit_rows = []
    window_start = _utc("2026-05-17T00:00:00Z")
    window_end = _utc("2026-05-17T02:00:00Z")
    summary = compute_summary(rows, audit_rows, window_start=window_start, window_end=window_end)
    ib = next(s for s in summary["sources"] if s["source"] == "ib")
    farm = next(f for f in ib["farms"] if f["farm"] == "ushmds")
    assert abs(farm["uptime_pct"] - 75.0) < 0.5


def test_compute_summary_flap_count():
    rows = []
    base = _utc("2026-05-17T00:00:00Z")
    for i, state in enumerate(["ok", "broken", "ok", "broken", "ok"]):
        code = 2106 if state == "ok" else 2105
        ts = base + timedelta(minutes=i * 2)
        rows.append({
            "ts": ts.isoformat().replace("+00:00", "Z"),
            "_ts": ts,
            "source": "ib",
            "event": "farm_state",
            "code": code,
            "state": state,
            "farm": "ushmds",
        })
    summary = compute_summary(rows, [], window_start=base, window_end=base + timedelta(hours=1))
    ib = next(s for s in summary["sources"] if s["source"] == "ib")
    farm = next(f for f in ib["farms"] if f["farm"] == "ushmds")
    assert farm["flap_count"] == 1


def test_compute_summary_no_flap_when_transitions_are_spaced_out():
    rows = []
    base = _utc("2026-05-17T00:00:00Z")
    for i, state in enumerate(["ok", "broken", "ok"]):
        ts = base + timedelta(minutes=i * 20)
        rows.append({
            "ts": ts.isoformat().replace("+00:00", "Z"),
            "_ts": ts,
            "source": "ib",
            "event": "farm_state",
            "state": state,
            "farm": "ushmds",
        })
    summary = compute_summary(rows, [], window_start=base, window_end=base + timedelta(hours=1))
    ib = next(s for s in summary["sources"] if s["source"] == "ib")
    farm = next(f for f in ib["farms"] if f["farm"] == "ushmds")
    assert farm["flap_count"] == 0


def test_compute_summary_short_transition_list_has_no_flap():
    base = _utc("2026-05-17T00:00:00Z")
    rows = [
        {
            "ts": base.isoformat().replace("+00:00", "Z"),
            "_ts": base,
            "source": "ib",
            "event": "farm_state",
            "state": "ok",
            "farm": "ushmds",
        }
    ]
    summary = compute_summary(rows, [], window_start=base, window_end=base + timedelta(hours=1))
    farm = summary["sources"][0]["farms"][0]
    assert farm["flap_count"] == 0


def test_compute_summary_counts_multiple_flap_bursts_and_sources():
    base = _utc("2026-05-17T00:00:00Z")
    rows = []
    for minute in [0, 1, 2, 30, 31, 32]:
        ts = base + timedelta(minutes=minute)
        rows.append({
            "ts": ts.isoformat().replace("+00:00", "Z"),
            "_ts": ts,
            "source": "ib",
            "event": "farm_state",
            "state": "ok" if minute % 2 == 0 else "broken",
            "farm": "ushmds",
        })
    rows.append({
        "ts": base.isoformat().replace("+00:00", "Z"),
        "_ts": base,
        "source": "uw",
        "event": "connected",
    })
    summary = compute_summary(rows, [], window_start=base, window_end=base + timedelta(hours=1))
    ib = next(s for s in summary["sources"] if s["source"] == "ib")
    uw = next(s for s in summary["sources"] if s["source"] == "uw")
    assert ib["farms"][0]["flap_count"] == 2
    assert uw["farms"] == []


def test_compute_summary_counts_flags_and_tickers():
    base = _utc("2026-05-17T00:00:00Z")
    audit = [
        {"_ts": base, "ticker": "SMH", "category": "range_shortfall"},
        {"_ts": base, "ticker": "SMH", "category": "interior_gaps"},
        {"_ts": base, "ticker": "AAPL", "category": "range_shortfall"},
    ]
    summary = compute_summary([], audit, window_start=base, window_end=base + timedelta(hours=1))
    assert summary["flag_counts_by_category"] == {"range_shortfall": 2, "interior_gaps": 1}
    assert summary["top_tickers"][0] == {"ticker": "SMH", "flag_count": 2}


def test_render_summary_text_includes_uptime():
    summary = {
        "window": "24h",
        "sources": [
            {
                "source": "ib",
                "connection_events": 142,
                "farms": [
                    {
                        "farm": "ushmds",
                        "uptime_pct": 97.2,
                        "flap_count": 3,
                        "mtbd_seconds": 1800,
                    }
                ],
            },
        ],
        "flag_counts_by_category": {"range_shortfall": 1},
        "top_tickers": [{"ticker": "SMH", "flag_count": 2}],
    }
    text = render_summary_text(summary)
    assert "97.2" in text
    assert "SMH" in text


def test_render_flap_view_chronological():
    from livewire_scripts.data_quality_report import render_flap_view

    rows = [
        {
            "_ts": _utc("2026-05-17T00:00:30Z"),
            "source": "ib",
            "event": "connected",
        },
    ] + [
        {
            "_ts": _utc(f"2026-05-17T00:0{i}:00Z"),
            "source": "ib",
            "event": "farm_state",
            "state": "ok" if i % 2 == 0 else "broken",
            "farm": "ushmds",
            "code": 2106,
        }
        for i in range(6)
    ]
    text = render_flap_view(list(reversed(rows)))
    assert "ushmds" in text
    assert text.count("00:") >= 1
    assert text.index("00:00") < text.index("00:05")


def test_render_quality_view_severity_filter():
    from livewire_scripts.data_quality_report import render_quality_view

    audit = [
        {
            "_ts": _utc("2026-05-17T00:00:00Z"),
            "source": "ib",
            "ticker": "SMH",
            "category": "range_shortfall",
            "severity": "critical",
            "detail": {},
        },
        {
            "_ts": _utc("2026-05-17T00:01:00Z"),
            "source": "ib",
            "ticker": "NVDA",
            "category": "interior_gaps",
            "severity": "warning",
            "detail": {},
        },
    ]
    text = render_quality_view(audit, severity_filter="critical")
    assert "SMH" in text
    assert "NVDA" not in text


def test_render_quality_view_defaults_timeframe():
    from livewire_scripts.data_quality_report import render_quality_view

    audit = [{
        "_ts": _utc("2026-05-17T00:00:00Z"),
        "source": "ib",
        "ticker": "SMH",
        "category": "range_shortfall",
        "severity": "critical",
    }]
    assert "ib/SMH/1d" in render_quality_view(audit)


def test_main_dispatch_summary(tmp_path, capsys):
    from livewire_scripts.data_quality_report import main

    t = tmp_path / "telemetry.jsonl"
    t.write_text(json.dumps({
        "ts": "2026-05-17T00:00:00Z",
        "source": "ib",
        "event": "connected",
    }) + "\n")
    a = tmp_path / "audit.jsonl"
    a.write_text("")
    rc = main([
        "--view",
        "summary",
        "--since",
        "30d",
        "--telemetry-path",
        str(t),
        "--audit-path",
        str(a),
    ])
    captured = capsys.readouterr()
    assert rc == 0
    assert "Livewire Data Quality Summary" in captured.out


def test_main_dispatch_flap_with_source_filter(tmp_path, capsys):
    from livewire_scripts.data_quality_report import main

    t = tmp_path / "telemetry.jsonl"
    t.write_text(
        json.dumps({
            "ts": "2026-05-17T00:00:00Z",
            "source": "ib",
            "event": "farm_state",
            "state": "ok",
            "farm": "ushmds",
        }) + "\n" +
        json.dumps({
            "ts": "2026-05-17T00:00:00Z",
            "source": "uw",
            "event": "farm_state",
            "state": "broken",
            "farm": "uw-api",
        }) + "\n"
    )
    rc = main([
        "--view",
        "flap",
        "--since",
        "30d",
        "--source",
        "ib",
        "--telemetry-path",
        str(t),
        "--audit-path",
        str(tmp_path / "missing-audit.jsonl"),
    ])
    out = capsys.readouterr().out
    assert rc == 0
    assert "ushmds" in out
    assert "uw-api" not in out


def test_main_dispatch_quality_with_filter(tmp_path, capsys):
    from livewire_scripts.data_quality_report import main

    a = tmp_path / "audit.jsonl"
    a.write_text(json.dumps({
        "ts": "2026-05-17T00:00:00Z",
        "source": "ib",
        "ticker": "SMH",
        "severity": "critical",
        "category": "range_shortfall",
    }) + "\n")
    rc = main([
        "--view",
        "quality",
        "--since",
        "30d",
        "--severity",
        "critical",
        "--telemetry-path",
        str(tmp_path / "missing-telemetry.jsonl"),
        "--audit-path",
        str(a),
    ])
    out = capsys.readouterr().out
    assert rc == 0
    assert "SMH" in out


def test_email_mode_spawns_nodemailer_and_writes_marker(tmp_path, monkeypatch):
    from livewire_scripts.data_quality_report import main

    t = tmp_path / "t.jsonl"
    t.write_text("")
    a = tmp_path / "a.jsonl"
    a.write_text("")
    marker_dir = tmp_path / "markers"
    monkeypatch.setenv("MDW_LOG_DIR", str(marker_dir))

    spawned = []

    def fake_run(*args, **kwargs):
        spawned.append(args)
        from subprocess import CompletedProcess

        return CompletedProcess(args=[], returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setattr("subprocess.run", fake_run)

    rc = main([
        "--view",
        "summary",
        "--since",
        "1h",
        "--email",
        "--telemetry-path",
        str(t),
        "--audit-path",
        str(a),
    ])
    assert rc == 0
    assert spawned, "Nodemailer should be invoked"
    cmd = spawned[0][0]
    assert "daily-summary" in cmd
    markers = list(marker_dir.glob("quality_summary_*.marker"))
    assert markers, "marker file should be written"


def test_send_email_failure_returns_false(monkeypatch, capsys):
    from livewire_scripts.data_quality_report import _send_email

    def fake_run(*args, **kwargs):
        raise OSError("node missing")

    monkeypatch.setattr("subprocess.run", fake_run)
    assert _send_email({"sources": []}) is False
    assert "node missing" in capsys.readouterr().err


def test_send_email_nonzero_returns_false(monkeypatch, capsys):
    from livewire_scripts.data_quality_report import _send_email
    from subprocess import CompletedProcess

    monkeypatch.setattr(
        "subprocess.run",
        lambda *a, **kw: CompletedProcess(args=[], returncode=1, stdout=b"", stderr=b"bad"),
    )
    assert _send_email({"sources": []}) is False
    assert "returned 1" in capsys.readouterr().err


def test_resolve_log_dir_default(monkeypatch):
    from livewire_scripts.data_quality_report import _resolve_log_dir

    monkeypatch.delenv("MDW_LOG_DIR", raising=False)
    assert _resolve_log_dir().as_posix().endswith("/market-warehouse/logs")

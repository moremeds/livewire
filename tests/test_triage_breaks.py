"""Tests for the batch triage command. The provider is faked; no network."""

import json
from datetime import date

import pytest

from clients.massive_client import MassiveAuthError, MassiveRateLimitError
from livewire_scripts import triage_breaks

AS_OF = date(2026, 7, 17)

# Real Massive NVDA closes across its 2021-07-20 4:1 split, both bases, frozen
# 2026-07-17. adjusted/raw steps 0.0250 -> 0.1000 exactly at the ex-date.
NVDA_RAW = [("2021-07-19", 751.19), ("2021-07-20", 186.12)]
NVDA_ADJ = [("2021-07-19", 18.7798), ("2021-07-20", 18.612)]
# Recent AAPL bars stand in for the credential probe's window.
AAPL_RECENT = [("2026-07-15", 212.44), ("2026-07-16", 210.02)]


class _FakeBar:
    def __init__(self, trade_date, close):
        self.trade_date = trade_date
        self.close = close


class _FakeMassive:
    """Stands in for MassiveClient: same context-manager + get_daily_bars shape."""

    def __init__(self, series, *, errors=None):
        self._series = series  # {(ticker, adjusted): [(iso_date, close), ...]}
        self._errors = errors or {}  # {ticker: exception}
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_daily_bars(self, ticker, start, end, *, adjusted=False):
        self.calls.append((ticker, adjusted))
        if ticker.upper() in self._errors:
            raise self._errors[ticker.upper()]
        return [_FakeBar(date.fromisoformat(d), c) for d, c in self._series.get((ticker.upper(), adjusted), [])]


def _audit(tmp_path, breaks_by_symbol):
    payload = {
        "schema_version": 1,
        "data_lake_root": str(tmp_path.resolve()),
        "counts": {"clean": 0, "mixed": len(breaks_by_symbol), "error": 0},
        "symbols": [
            {"symbol": symbol, "klass": "mixed", "breaks": breaks}
            for symbol, breaks in sorted(breaks_by_symbol.items())
        ],
    }
    path = tmp_path / "audit.json"
    path.write_text(json.dumps(payload))
    return path


def _client(**kwargs):
    series = {
        ("NVDA", False): NVDA_RAW,
        ("NVDA", True): NVDA_ADJ,
        ("AAPL", False): AAPL_RECENT,
    }
    return _FakeMassive(series, **kwargs)


def test_triage_writes_a_verdict_manifest(tmp_path):
    audit = _audit(tmp_path, {"NVDA": [{"date": "2021-07-20", "ratio": 4.04, "reason": "x"}]})
    output = tmp_path / "triage.json"

    rc = triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(output)],
        data_lake_root=tmp_path,
        massive_factory=_client,
        as_of_date=AS_OF,
    )

    assert rc == 0
    manifest = json.loads(output.read_text())
    assert manifest["counts"]["missing_action"] == 1
    assert manifest["complete"] is True
    assert manifest["verdicts"][0]["symbol"] == "NVDA"


def test_every_break_of_a_multi_break_symbol_becomes_a_candidate(tmp_path):
    """One candidate per (symbol, break) — a break never triaged is trimmed blind."""
    audit = _audit(
        tmp_path,
        {"NVDA": [{"date": "2021-07-20", "ratio": 4.04}, {"date": "2024-06-10", "ratio": 9.98}]},
    )
    output = tmp_path / "triage.json"

    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(output)],
        data_lake_root=tmp_path,
        massive_factory=_client,
        as_of_date=AS_OF,
    )

    manifest = json.loads(output.read_text())
    assert sorted(v["date"] for v in manifest["verdicts"]) == ["2021-07-20", "2024-06-10"]


def test_breaks_without_a_ratio_are_not_candidates(tmp_path):
    """A non-positive close has nothing to compare against a second source."""
    audit = _audit(tmp_path, {"NVDA": [{"date": "2021-07-20", "ratio": None, "reason": "non-positive close"}]})
    output = tmp_path / "triage.json"

    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(output)],
        data_lake_root=tmp_path,
        massive_factory=_client,
        as_of_date=AS_OF,
    )

    assert json.loads(output.read_text())["verdicts"] == []


def test_transient_failure_aborts_without_checkpointing_the_candidate(tmp_path):
    """A rate-limit must leave the candidate un-cursored so --resume re-asks it,
    rather than baking one bad afternoon into a permanent trim."""
    audit = _audit(tmp_path, {"NVDA": [{"date": "2021-07-20", "ratio": 4.04}]})
    output = tmp_path / "triage.json"

    def _rate_limited():
        return _client(errors={"NVDA": MassiveRateLimitError("429 slow down", status_code=429)})

    rc = triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(output)],
        data_lake_root=tmp_path,
        massive_factory=_rate_limited,
        as_of_date=AS_OF,
    )

    assert rc == 1
    manifest = json.loads(output.read_text())
    assert manifest["complete"] is False
    assert manifest["verdicts"] == []
    cursor = (
        json.loads((tmp_path / "triage.json.cursor.json").read_text())
        if (tmp_path / "triage.json.cursor.json").is_file()
        else {"verdicts": {}}
    )
    assert cursor["verdicts"] == {}  # nothing durable was written for NVDA


def test_bad_credentials_abort_instead_of_trimming_the_whole_population(tmp_path):
    """A present-but-invalid key 401s on every date, which reads exactly like the
    entitlement floor — and the operator is told to expect a large inconclusive
    count, so nothing would flag it. Probe an entitled date and refuse."""
    audit = _audit(tmp_path, {"NVDA": [{"date": "2021-07-20", "ratio": 4.04}]})
    output = tmp_path / "triage.json"

    def _bad_key():
        return _FakeMassive({}, errors={"AAPL": MassiveAuthError("401 unauthorized", status_code=401)})

    with pytest.raises(ValueError, match="credentials are bad, not the date range"):
        triage_breaks.run(
            ["--audit-manifest", str(audit), "--output", str(output)],
            data_lake_root=tmp_path,
            massive_factory=_bad_key,
            as_of_date=AS_OF,
        )
    assert not output.exists()  # no verdict manifest, so nothing can trim on it


def test_resume_skips_already_triaged_breaks(tmp_path):
    audit = _audit(tmp_path, {"NVDA": [{"date": "2021-07-20", "ratio": 4.04}]})
    output = tmp_path / "triage.json"
    first = _client()
    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(output)],
        data_lake_root=tmp_path,
        massive_factory=lambda: first,
        as_of_date=AS_OF,
    )
    second = _client()

    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(output), "--resume"],
        data_lake_root=tmp_path,
        massive_factory=lambda: second,
        as_of_date=AS_OF,
    )

    assert second.calls == []  # nothing re-fetched, not even the credential probe


def test_resume_cursor_from_another_audit_is_rejected(tmp_path):
    audit = _audit(tmp_path, {"NVDA": [{"date": "2021-07-20", "ratio": 4.04}]})
    output = tmp_path / "triage.json"
    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(output)],
        data_lake_root=tmp_path,
        massive_factory=_client,
        as_of_date=AS_OF,
    )
    audit.write_text(json.dumps({**json.loads(audit.read_text()), "tampered": True}))

    with pytest.raises(ValueError, match="resume cursor does not match"):
        triage_breaks.run(
            ["--audit-manifest", str(audit), "--output", str(output), "--resume"],
            data_lake_root=tmp_path,
            massive_factory=_client,
            as_of_date=AS_OF,
        )


def test_audit_from_another_lake_is_rejected(tmp_path):
    audit = _audit(tmp_path, {"NVDA": [{"date": "2021-07-20", "ratio": 4.04}]})
    other = tmp_path / "other"
    other.mkdir()

    with pytest.raises(ValueError, match="does not match active root"):
        triage_breaks.run(
            ["--audit-manifest", str(audit), "--output", str(tmp_path / "t.json")],
            data_lake_root=other,
            massive_factory=_client,
            as_of_date=AS_OF,
        )


def test_tickers_filter_narrows_the_population(tmp_path):
    audit = _audit(
        tmp_path,
        {
            "NVDA": [{"date": "2021-07-20", "ratio": 4.04}],
            "EQIX": [{"date": "2003-01-02", "ratio": 24.95}],
        },
    )
    output = tmp_path / "triage.json"

    triage_breaks.run(
        ["--audit-manifest", str(audit), "--output", str(output), "--tickers", "NVDA"],
        data_lake_root=tmp_path,
        massive_factory=_client,
        as_of_date=AS_OF,
    )

    manifest = json.loads(output.read_text())
    assert [v["symbol"] for v in manifest["verdicts"]] == ["NVDA"]


def test_main_delegates_to_run(monkeypatch):
    seen = {}

    def _fake_run(argv):
        seen["argv"] = argv
        return 0

    monkeypatch.setattr(triage_breaks, "run", _fake_run)
    assert triage_breaks.main(["--audit-manifest", "a.json", "--output", "o.json"]) == 0
    assert seen["argv"] == ["--audit-manifest", "a.json", "--output", "o.json"]

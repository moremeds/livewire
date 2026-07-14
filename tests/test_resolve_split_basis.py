from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace

from clients.bronze_client import BronzeClient
from livewire_scripts import audit_split_basis, resolve_split_basis
from tests.test_audit_split_basis import _seed


class _FakeIB:
    def __init__(self):
        self.connected = False
        self.disconnected = False

    def connect(self, **_kwargs):
        self.connected = True

    def disconnect(self):
        self.disconnected = True


def _ambiguous_audit(tmp_path):
    path = _seed(tmp_path)
    client = BronzeClient(path.parents[1], "equity")
    rows = client.read_symbol_rows("AAPL")
    rows[1]["close"] = rows[1]["open"] = rows[1]["high"] = rows[1]["low"] = rows[1]["adj_close"] = 17.5
    client.replace_ticker_rows("AAPL", rows)
    audit = tmp_path / "audit.json"
    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(audit)], data_lake_root=tmp_path) == 1
    return audit


def _forced_ambiguous_audit(tmp_path, *, raw_bronze: bool = False):
    path = _seed(tmp_path)
    if raw_bronze:
        client = BronzeClient(path.parents[1], "equity")
        rows = client.read_symbol_rows("AAPL")
        rows[0].update({column: 100.0 for column in ("open", "high", "low", "close", "adj_close")})
        client.replace_ticker_rows("AAPL", rows)
    audit = tmp_path / "forced-ambiguous-audit.json"
    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(audit)], data_lake_root=tmp_path) == 0
    payload = json.loads(audit.read_text())
    payload["symbols"][0]["classifications"][0]["treatment"] = "ambiguous"
    payload["symbols"][0]["eligible"] = False
    payload["symbols"][0]["replacements"] = []
    audit.write_text(json.dumps(payload, sort_keys=True))
    return audit


def test_resolver_persists_repeated_ib_consensus_and_resumes(tmp_path):
    audit = _forced_ambiguous_audit(tmp_path)
    output = tmp_path / "evidence"
    ib = _FakeIB()
    calls = []

    def fetcher(symbol, start, end):
        calls.append((symbol, start, end))
        return [
            {"trade_date": date(2020, 8, 28), "close": 25.0},
            {"trade_date": date(2020, 8, 31), "close": 26.0},
        ]

    args = [
        "--audit-manifest",
        str(audit),
        "--output-dir",
        str(output),
        "--data-lake-root",
        str(tmp_path),
    ]
    assert (
        resolve_split_basis.run(
            args,
            ib_factory=lambda: ib,
            ib_fetcher_factory=lambda _client: fetcher,
        )
        == 0
    )
    detail = json.loads((output / "symbols/AAPL.json").read_text())
    assert detail["status"] == "resolved"
    assert detail["events"][0]["classification"]["treatment"] == "adjusted"
    assert len(detail["events"][0]["provider_runs"]) == 2
    assert len(calls) == 2
    assert ib.connected is True
    assert ib.disconnected is True

    assert (
        resolve_split_basis.run(
            [*args, "--resume"],
            ib_factory=lambda: ib,
            ib_fetcher_factory=lambda _client: fetcher,
        )
        == 0
    )
    assert len(calls) == 2


def test_resolver_classifies_bronze_against_raw_ib_reference(tmp_path):
    audit = _forced_ambiguous_audit(tmp_path, raw_bronze=True)
    output = tmp_path / "evidence"

    def fetcher(_symbol, _start, _end):
        return [
            {"trade_date": date(2020, 8, 28), "close": 100.0},
            {"trade_date": date(2020, 8, 31), "close": 26.0},
        ]

    assert (
        resolve_split_basis.run(
            [
                "--audit-manifest",
                str(audit),
                "--output-dir",
                str(output),
                "--data-lake-root",
                str(tmp_path),
            ],
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: fetcher,
        )
        == 0
    )

    event = json.loads((output / "symbols/AAPL.json").read_text())["events"][0]
    assert event["reference_basis"] == "raw"
    assert event["classification"]["treatment"] == "raw"


def test_resume_replays_legacy_resolved_evidence_without_refetch(tmp_path):
    audit = _forced_ambiguous_audit(tmp_path, raw_bronze=True)
    output = tmp_path / "evidence"

    def fetcher(_symbol, _start, _end):
        return [
            {"trade_date": date(2020, 8, 28), "close": 100.0},
            {"trade_date": date(2020, 8, 31), "close": 26.0},
        ]

    args = [
        "--audit-manifest",
        str(audit),
        "--output-dir",
        str(output),
        "--data-lake-root",
        str(tmp_path),
    ]
    assert (
        resolve_split_basis.run(
            args,
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: fetcher,
        )
        == 0
    )
    detail_path = output / "symbols/AAPL.json"
    detail = json.loads(detail_path.read_text())
    detail.pop("evidence_version", None)
    detail["events"][0]["classification"]["treatment"] = "adjusted"
    detail_path.write_text(json.dumps(detail, sort_keys=True))
    cursor_path = output / "cursor.json"
    cursor = json.loads(cursor_path.read_text())
    cursor["completed"]["AAPL"]["detail_sha256"] = resolve_split_basis._sha256(detail_path)
    cursor_path.write_text(json.dumps(cursor, sort_keys=True))

    assert (
        resolve_split_basis.run(
            [*args, "--resume"],
            ib_factory=lambda: (_ for _ in ()).throw(AssertionError("IB must not connect")),
        )
        == 0
    )
    migrated = json.loads(detail_path.read_text())
    assert migrated["evidence_version"] == 7
    assert migrated["events"][0]["reference_basis"] == "raw"
    assert migrated["events"][0]["classification"]["treatment"] == "raw"


def _post_history_audit(tmp_path):
    path = _seed(tmp_path)
    client = BronzeClient(path.parents[1], "equity")
    client.replace_ticker_rows("AAPL", client.read_symbol_rows("AAPL")[:1])
    audit = tmp_path / "post-history-audit.json"
    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(audit)], data_lake_root=tmp_path) == 1
    return audit


def test_resolver_keeps_post_history_split_pending_without_reference_post_bar(tmp_path):
    audit = _post_history_audit(tmp_path)
    output = tmp_path / "evidence"

    assert (
        resolve_split_basis.run(
            [
                "--audit-manifest",
                str(audit),
                "--output-dir",
                str(output),
                "--data-lake-root",
                str(tmp_path),
            ],
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: lambda _symbol, _start, _end: [],
            massive_factory=lambda: (_ for _ in ()).throw(RuntimeError("provider unavailable")),
        )
        == 1
    )

    detail = json.loads((output / "symbols/AAPL.json").read_text())
    assert detail["status"] == "pending"
    assert detail["events"][0]["reason"] == "awaiting_post_split_reference"


def test_resolver_classifies_post_history_split_from_pre_only_reference(tmp_path):
    audit = _post_history_audit(tmp_path)
    output = tmp_path / "evidence"

    def fetcher(_symbol, _start, _end):
        return [
            {"trade_date": date(2020, 8, 28), "close": 6.25},
            {"trade_date": date(2020, 8, 31), "close": 7.0},
        ]

    assert (
        resolve_split_basis.run(
            [
                "--audit-manifest",
                str(audit),
                "--output-dir",
                str(output),
                "--data-lake-root",
                str(tmp_path),
            ],
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: fetcher,
        )
        == 0
    )

    event = json.loads((output / "symbols/AAPL.json").read_text())["events"][0]
    assert event["status"] == "resolved"
    assert event["classification"]["reason"] == "reference_consensus_pre_only"
    assert event["classification"]["treatment"] == "raw"


def test_resolver_rejects_stale_bronze_before_ib_fetch(tmp_path):
    audit = _ambiguous_audit(tmp_path)
    path = tmp_path / "bronze/asset_class=equity/symbol=AAPL/1d.parquet"
    client = BronzeClient(path.parents[1], "equity")
    rows = client.read_symbol_rows("AAPL")
    rows[0]["volume"] += 1
    client.replace_ticker_rows("AAPL", rows)
    output = tmp_path / "evidence"

    assert (
        resolve_split_basis.run(
            [
                "--audit-manifest",
                str(audit),
                "--output-dir",
                str(output),
                "--data-lake-root",
                str(tmp_path),
            ],
            ib_factory=lambda: (_ for _ in ()).throw(AssertionError("IB must not connect")),
        )
        == 1
    )

    detail = json.loads((output / "symbols/AAPL.json").read_text())
    assert detail["status"] == "error"
    assert detail["events"] == []
    assert detail["reason"] == "stale_bronze_source"


def test_resume_retries_transient_provider_error(tmp_path):
    audit = _forced_ambiguous_audit(tmp_path)
    output = tmp_path / "evidence"
    attempts = 0

    def fetcher(_symbol, _start, _end):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TimeoutError("temporary pacing")
        return [
            {"trade_date": date(2020, 8, 28), "close": 25.0},
            {"trade_date": date(2020, 8, 31), "close": 26.0},
        ]

    args = [
        "--audit-manifest",
        str(audit),
        "--output-dir",
        str(output),
        "--data-lake-root",
        str(tmp_path),
    ]
    assert (
        resolve_split_basis.run(
            args,
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: fetcher,
        )
        == 1
    )
    assert (
        resolve_split_basis.run(
            [*args, "--resume"],
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: fetcher,
        )
        == 0
    )
    assert attempts == 3


def test_resolver_recovers_invalid_ohlc_from_repeated_ib_rows(tmp_path):
    path = _seed(tmp_path)
    client = BronzeClient(path.parents[1], "equity")
    rows = client.read_symbol_rows("AAPL")
    rows[0]["low"] = 0.0
    client.replace_ticker_rows("AAPL", rows)
    audit = tmp_path / "audit.json"
    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(audit)], data_lake_root=tmp_path) == 1
    output = tmp_path / "evidence"

    def fetcher(_symbol, _start, _end):
        return [
            {
                "trade_date": date(2020, 8, 28),
                "open": 25.0,
                "high": 25.0,
                "low": 24.0,
                "close": 25.0,
                "adj_close": 25.0,
                "volume": 400,
                "source": "ib",
                "price_basis": "split_adjusted",
            }
        ]

    assert (
        resolve_split_basis.run(
            [
                "--audit-manifest",
                str(audit),
                "--output-dir",
                str(output),
                "--data-lake-root",
                str(tmp_path),
            ],
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: fetcher,
        )
        == 0
    )

    detail = json.loads((output / "symbols/AAPL.json").read_text())
    assert detail["status"] == "resolved"
    assert detail["ohlc_corrections"][0]["correction"]["proposed_values"] == {"low": 24.0}

    resolved_audit = tmp_path / "resolved-audit.json"
    assert (
        audit_split_basis.run(
            [
                "--tickers",
                "AAPL",
                "--output",
                str(resolved_audit),
                "--evidence-dir",
                str(output),
            ],
            data_lake_root=tmp_path,
        )
        == 0
    )
    item = json.loads(resolved_audit.read_text())["symbols"][0]
    assert item["eligible"] is True
    assert item["error"] is None
    repaired = next(entry for entry in item["replacements"] if entry["trade_date"] == "2020-08-28")
    assert repaired["proposed"]["low"] == 96.0


def test_resolver_uses_massive_adjusted_fallback_when_ib_has_no_boundary(tmp_path):
    audit = _ambiguous_audit(tmp_path)
    output = tmp_path / "evidence"

    class Massive:
        def get_daily_bars(self, _symbol, _start, _end, *, adjusted):
            assert adjusted is True
            return [
                SimpleNamespace(
                    trade_date=date(2020, 8, 28),
                    open=25.0,
                    high=25.0,
                    low=25.0,
                    close=25.0,
                    volume=400,
                ),
                SimpleNamespace(
                    trade_date=date(2020, 8, 31),
                    open=17.5,
                    high=17.5,
                    low=17.5,
                    close=17.5,
                    volume=500,
                ),
            ]

        def close(self):
            pass

    assert (
        resolve_split_basis.run(
            [
                "--audit-manifest",
                str(audit),
                "--output-dir",
                str(output),
                "--data-lake-root",
                str(tmp_path),
            ],
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: (
                lambda _symbol, _start, _end: [
                    {
                        "trade_date": date(2020, 8, 28),
                        "open": 25.0,
                        "high": 25.0,
                        "low": 0.0,
                        "close": 25.0,
                        "adj_close": 25.0,
                        "volume": 400,
                        "source": "ib",
                        "price_basis": "split_adjusted",
                    }
                ]
            ),
            massive_factory=Massive,
        )
        == 0
    )

    event = json.loads((output / "symbols/AAPL.json").read_text())["events"][0]
    assert event["provider"] == "massive"
    assert event["status"] == "resolved"


def test_invalid_ohlc_uses_massive_fallback_when_ib_row_is_unavailable(tmp_path):
    path = _seed(tmp_path)
    client = BronzeClient(path.parents[1], "equity")
    rows = client.read_symbol_rows("AAPL")
    rows[0]["low"] = 0.0
    client.replace_ticker_rows("AAPL", rows)
    audit = tmp_path / "audit.json"
    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(audit)], data_lake_root=tmp_path) == 1
    output = tmp_path / "evidence"

    class Massive:
        def get_daily_bars(self, _symbol, _start, _end, *, adjusted):
            assert adjusted is True
            return [
                SimpleNamespace(
                    trade_date=date(2020, 8, 28),
                    open=25.0,
                    high=25.0,
                    low=24.0,
                    close=25.0,
                    volume=400,
                )
            ]

        def close(self):
            pass

    assert (
        resolve_split_basis.run(
            [
                "--audit-manifest",
                str(audit),
                "--output-dir",
                str(output),
                "--data-lake-root",
                str(tmp_path),
            ],
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: (
                lambda _symbol, _start, _end: [
                    {
                        "trade_date": date(2020, 8, 28),
                        "open": 25.0,
                        "high": 25.0,
                        "low": 0.0,
                        "close": 25.0,
                        "adj_close": 25.0,
                        "volume": 400,
                        "source": "ib",
                        "price_basis": "split_adjusted",
                    }
                ]
            ),
            massive_factory=Massive,
        )
        == 0
    )

    correction = json.loads((output / "symbols/AAPL.json").read_text())["ohlc_corrections"][0]
    assert correction["provider"] == "massive"
    assert correction["status"] == "resolved"


def test_resolver_uses_massive_when_ib_hypotheses_remain_ambiguous(tmp_path):
    audit = _ambiguous_audit(tmp_path)
    output = tmp_path / "evidence"

    class Massive:
        def get_daily_bars(self, _symbol, _start, _end, *, adjusted):
            assert adjusted is True
            return [
                SimpleNamespace(
                    trade_date=date(2020, 8, 28),
                    open=25.0,
                    high=25.0,
                    low=25.0,
                    close=25.0,
                    volume=400,
                ),
                SimpleNamespace(
                    trade_date=date(2020, 8, 31),
                    open=17.5,
                    high=17.5,
                    low=17.5,
                    close=17.5,
                    volume=500,
                ),
            ]

        def close(self):
            pass

    def ambiguous_ib(_symbol, _start, _end):
        return [
            {"trade_date": date(2020, 8, 28), "close": 20.0},
            {"trade_date": date(2020, 8, 31), "close": 17.5},
        ]

    assert (
        resolve_split_basis.run(
            [
                "--audit-manifest",
                str(audit),
                "--output-dir",
                str(output),
                "--data-lake-root",
                str(tmp_path),
            ],
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: ambiguous_ib,
            massive_factory=Massive,
        )
        == 0
    )

    event = json.loads((output / "symbols/AAPL.json").read_text())["events"][0]
    assert event["provider"] == "massive"
    assert event["status"] == "resolved"


def test_resume_reuses_saved_ambiguous_ib_rows_before_massive_fallback(tmp_path):
    audit = _ambiguous_audit(tmp_path)
    output = tmp_path / "evidence"

    def ambiguous_ib(_symbol, _start, _end):
        return [
            {"trade_date": date(2020, 8, 28), "close": 20.0},
            {"trade_date": date(2020, 8, 31), "close": 17.5},
        ]

    args = [
        "--audit-manifest",
        str(audit),
        "--output-dir",
        str(output),
        "--data-lake-root",
        str(tmp_path),
    ]
    assert (
        resolve_split_basis.run(
            args,
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: ambiguous_ib,
            massive_factory=lambda: (_ for _ in ()).throw(RuntimeError("fallback unavailable")),
        )
        == 1
    )

    class Massive:
        def get_daily_bars(self, _symbol, _start, _end, *, adjusted):
            assert adjusted is True
            return [
                SimpleNamespace(
                    trade_date=date(2020, 8, 28),
                    open=25.0,
                    high=25.0,
                    low=25.0,
                    close=25.0,
                    volume=400,
                ),
                SimpleNamespace(
                    trade_date=date(2020, 8, 31),
                    open=17.5,
                    high=17.5,
                    low=17.5,
                    close=17.5,
                    volume=500,
                ),
            ]

        def close(self):
            pass

    assert (
        resolve_split_basis.run(
            [*args, "--resume"],
            ib_factory=lambda: (_ for _ in ()).throw(AssertionError("IB must not connect")),
            massive_factory=Massive,
        )
        == 0
    )
    event = json.loads((output / "symbols/AAPL.json").read_text())["events"][0]
    assert event["provider"] == "massive"
    assert event["status"] == "resolved"


def test_resolver_expands_ib_windows_across_stored_boundary_gap(tmp_path):
    path = _seed(tmp_path)
    client = BronzeClient(path.parents[1], "equity")
    rows = client.read_symbol_rows("AAPL")
    rows[0]["trade_date"] = "2020-01-02"
    rows[1]["trade_date"] = "2021-01-04"
    client.replace_ticker_rows("AAPL", rows)
    audit = tmp_path / "gap-audit.json"
    assert audit_split_basis.run(["--tickers", "AAPL", "--output", str(audit)], data_lake_root=tmp_path) == 0
    payload = json.loads(audit.read_text())
    payload["symbols"][0]["classifications"][0]["treatment"] = "ambiguous"
    payload["symbols"][0]["eligible"] = False
    payload["symbols"][0]["replacements"] = []
    audit.write_text(json.dumps(payload, sort_keys=True))
    output = tmp_path / "evidence"
    calls = []

    def fetcher(_symbol, start, end):
        calls.append((start, end))
        if (end - start).days < 150:
            return [
                {"trade_date": date(2020, 8, 28), "close": 25.0},
                {"trade_date": date(2020, 8, 31), "close": 26.0},
            ]
        return [
            {"trade_date": date(2020, 1, 2), "close": 25.0},
            {"trade_date": date(2020, 8, 28), "close": 25.0},
            {"trade_date": date(2020, 8, 31), "close": 26.0},
            {"trade_date": date(2021, 1, 4), "close": 26.0},
        ]

    assert (
        resolve_split_basis.run(
            [
                "--audit-manifest",
                str(audit),
                "--output-dir",
                str(output),
                "--data-lake-root",
                str(tmp_path),
            ],
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: fetcher,
            massive_factory=lambda: (_ for _ in ()).throw(AssertionError("Massive must not be used")),
        )
        == 0
    )

    event = json.loads((output / "symbols/AAPL.json").read_text())["events"][0]
    assert event["provider"] == "ib"
    assert event["status"] == "resolved"
    assert len(calls) == 4


def test_empty_ib_windows_skip_unproductive_wide_retry(tmp_path):
    audit = _ambiguous_audit(tmp_path)
    output = tmp_path / "evidence"
    calls = 0

    def empty_ib(_symbol, _start, _end):
        nonlocal calls
        calls += 1
        return []

    class Massive:
        def get_daily_bars(self, _symbol, _start, _end, *, adjusted):
            assert adjusted is True
            return [
                SimpleNamespace(
                    trade_date=date(2020, 8, 28),
                    open=25.0,
                    high=25.0,
                    low=25.0,
                    close=25.0,
                    volume=400,
                ),
                SimpleNamespace(
                    trade_date=date(2020, 8, 31),
                    open=17.5,
                    high=17.5,
                    low=17.5,
                    close=17.5,
                    volume=500,
                ),
            ]

        def close(self):
            pass

    assert (
        resolve_split_basis.run(
            [
                "--audit-manifest",
                str(audit),
                "--output-dir",
                str(output),
                "--data-lake-root",
                str(tmp_path),
            ],
            ib_factory=_FakeIB,
            ib_fetcher_factory=lambda _client: empty_ib,
            massive_factory=Massive,
        )
        == 0
    )
    assert calls == 2

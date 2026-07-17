import json
from datetime import UTC, date, datetime
from decimal import Decimal

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveSplit
from livewire_scripts import repair_legacy_basis


def _seed_mixed(root, ticker):
    # NVDA-style mixed legacy/raw around a 4:1 split.
    rows = [
        {
            "trade_date": d,
            "symbol_id": 1,
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "adj_close": c,
            "volume": 100,
            "source": "legacy",
            "price_basis": "raw",
        }
        for d, c in (("2021-06-17", 746.29), ("2021-06-18", 18.64), ("2021-06-21", 737.09))
    ]
    BronzeClient(root / "bronze/asset_class=equity", "equity").replace_ticker_rows(ticker, rows)
    split = MassiveSplit(
        provider_event_id=f"{ticker}-2021",
        ticker=ticker,
        execution_date=date(2021, 7, 20),
        split_from=Decimal("1"),
        split_to=Decimal("4"),
        payload_hash="s",
    )
    CorporateActionStore(root).reconcile(ticker, [split], datetime(2021, 7, 20, tzinfo=UTC))


def _audit_manifest(root, ticker):
    bronze = BronzeClient(root / "bronze/asset_class=equity", "equity")
    import hashlib

    path = bronze.symbol_path(ticker)
    manifest = {
        "schema_version": 1,
        "data_lake_root": str(root.resolve()),
        "symbols": [
            {
                "symbol": ticker,
                "path": str(path),
                "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "klass": "mixed",
                "break_date": "2021-06-18",
            }
        ],
    }
    p = root / "audit.json"
    p.write_text(json.dumps(manifest))
    return p


def _clean_ib_fetcher(rows_by_symbol):
    # A stub IBHistoryFetcher: callable(symbol, start, end) -> list[dict] of clean IB TRADES rows.
    class _Stub:
        def __init__(self, client):  # ignore client
            pass

        def __call__(self, symbol, start, end):
            return [dict(r) for r in rows_by_symbol.get(symbol, []) if start <= r["trade_date"] <= end]

    return _Stub


def test_repair_rewrites_mixed_symbol_to_clean_raw(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    # Clean IB history: adjusted-basis closes spanning the 2021-07-20 split so the
    # classifier sees bars on both sides and resolves "adjusted" → normalized true-raw.
    ib_rows = [
        {
            "trade_date": d,
            "symbol_id": 0,
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "adj_close": c,
            "volume": 100,
            "source": "ib",
            "price_basis": "split_adjusted",
            "currency": "USD",
        }
        for d, c in (
            (date(2021, 6, 17), 186.57),
            (date(2021, 6, 18), 186.4),
            (date(2021, 6, 21), 184.27),
            (date(2021, 7, 21), 185.0),
        )
    ]

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": ib_rows}),
    )
    assert rc == 0
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert cursor["completed"]["NVDA"]["status"] == "done"
    # Bronze NVDA now has the IB-derived rows merged in (source ib, canonical raw).
    merged = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").read_symbol_rows("NVDA")
    by_date = {r["trade_date"]: r for r in merged}
    assert by_date["2021-06-18"]["source"] == "ib"
    assert by_date["2021-06-18"]["price_basis"] == "raw"


def test_resume_skips_completed_symbol(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    ib_rows = [
        {
            "trade_date": d,
            "symbol_id": 0,
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "adj_close": c,
            "volume": 100,
            "source": "ib",
            "price_basis": "split_adjusted",
            "currency": "USD",
        }
        for d, c in (
            (date(2021, 6, 17), 186.57),
            (date(2021, 6, 18), 186.4),
            (date(2021, 6, 21), 184.27),
            (date(2021, 7, 21), 185.0),
        )
    ]
    common = dict(data_lake_root=tmp_path, ib_factory=lambda: object())
    repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": ib_rows}),
        **common,
    )

    calls = {"n": 0}

    def _counting_factory(client):
        calls["n"] += 1
        return _clean_ib_fetcher({"NVDA": ib_rows})(client)

    repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir), "--resume"],
        ib_fetcher_factory=_counting_factory,
        **common,
    )
    assert calls["n"] == 0  # completed symbol not re-fetched


def test_ib_connection_failure_marks_failed_not_crash(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"

    def _boom():
        raise ConnectionError("gateway unreachable")

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=_boom,
        ib_fetcher_factory=_clean_ib_fetcher({}),
    )
    assert rc == 1
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert cursor["completed"]["NVDA"]["status"] == "failed"


def test_priority_orders_sp500_before_ndx_before_r2k_before_rest():
    # tiers: sp500=0, ndx100=1, r2k=2, unranked=len(presets)=3
    rank = {"AAPL": 0, "ZM": 1, "IWM": 2}
    assert repair_legacy_basis._order_symbols(["ZZZ", "IWM", "AAPL", "ZM"], rank) == ["AAPL", "ZM", "IWM", "ZZZ"]


def _split_only_ib_rows():
    # IB rows all BEFORE the 2021-07-20 split → classifier has no post-split bar,
    # so split treatment is ambiguous and prepare_ib_rows_for_publish raises.
    return [
        {
            "trade_date": d,
            "symbol_id": 0,
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "adj_close": c,
            "volume": 100,
            "source": "ib",
            "price_basis": "split_adjusted",
            "currency": "USD",
        }
        for d, c in ((date(2021, 6, 17), 186.57), (date(2021, 6, 18), 186.4), (date(2021, 6, 21), 184.27))
    ]


def test_ib_no_data_marks_failed(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({}),  # NVDA absent → empty history
    )
    assert rc == 1
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert cursor["completed"]["NVDA"]["status"] == "failed"


def test_ambiguous_classification_fails_closed_without_writing(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    bronze = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity")
    before = bronze.symbol_path("NVDA").read_bytes()
    output_dir = tmp_path / "out"
    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _split_only_ib_rows()}),
    )
    assert rc == 0  # ambiguous is not a systemic failure
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert cursor["completed"]["NVDA"]["status"] == "ambiguous"
    assert bronze.symbol_path("NVDA").read_bytes() == before  # never wrote


def test_post_merge_discontinuity_fails_closed(tmp_path):
    # IB returns clean rows but does NOT cover the corrupt 2021-06-18 legacy bar,
    # so the post-merge adjusted series still has the >6x jump → fail closed.
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    bronze = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity")
    before = bronze.symbol_path("NVDA").read_bytes()
    output_dir = tmp_path / "out"
    ib_rows = [
        {
            "trade_date": d,
            "symbol_id": 0,
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "adj_close": c,
            "volume": 100,
            "source": "ib",
            "price_basis": "split_adjusted",
            "currency": "USD",
        }
        for d, c in ((date(2021, 6, 17), 186.57), (date(2021, 6, 21), 184.27), (date(2021, 7, 21), 185.0))
    ]  # 06-18 deliberately absent
    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": ib_rows}),
    )
    assert rc == 0
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert cursor["completed"]["NVDA"]["status"] == "ambiguous"
    assert bronze.symbol_path("NVDA").read_bytes() == before  # never wrote


def test_per_symbol_fetch_exception_marks_failed(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"

    def _raising_factory(client):
        def _fetch(symbol, start, end):
            raise RuntimeError("HMDS query error")

        return _fetch

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_raising_factory,
    )
    assert rc == 1
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert cursor["completed"]["NVDA"]["status"] == "failed"


def test_connect_and_disconnect_lifecycle(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    ib_rows = [
        {
            "trade_date": d,
            "symbol_id": 0,
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "adj_close": c,
            "volume": 100,
            "source": "ib",
            "price_basis": "split_adjusted",
            "currency": "USD",
        }
        for d, c in (
            (date(2021, 6, 17), 186.57),
            (date(2021, 6, 18), 186.4),
            (date(2021, 6, 21), 184.27),
            (date(2021, 7, 21), 185.0),
        )
    ]

    events = []

    class _Client:
        def connect(self, host, port):
            events.append(("connect", host, port))

        def disconnect(self):
            events.append(("disconnect",))

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir), "--host", "10.0.0.9", "--port", "4002"],
        data_lake_root=tmp_path,
        ib_factory=_Client,
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": ib_rows}),
    )
    assert rc == 0
    assert events == [("connect", "10.0.0.9", 4002), ("disconnect",)]


def test_resume_cursor_identity_mismatch_raises(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    ib_rows = [
        {
            "trade_date": d,
            "symbol_id": 0,
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "adj_close": c,
            "volume": 100,
            "source": "ib",
            "price_basis": "split_adjusted",
            "currency": "USD",
        }
        for d, c in (
            (date(2021, 6, 17), 186.57),
            (date(2021, 6, 18), 186.4),
            (date(2021, 6, 21), 184.27),
            (date(2021, 7, 21), 185.0),
        )
    ]
    common = dict(
        data_lake_root=tmp_path, ib_factory=lambda: object(), ib_fetcher_factory=_clean_ib_fetcher({"NVDA": ib_rows})
    )
    repair_legacy_basis.run(["--audit-manifest", str(manifest), "--output-dir", str(output_dir)], **common)
    # Mutate the manifest so its sha256 (part of cursor identity) no longer matches.
    manifest.write_text(manifest.read_text() + "\n")
    import pytest

    with pytest.raises(ValueError, match="does not match"):
        repair_legacy_basis.run(
            ["--audit-manifest", str(manifest), "--output-dir", str(output_dir), "--resume"], **common
        )


def test_summarize_progress_quantifies_tail():
    # Full audit saw 300 mixed of 8305; first batch attempted 100, 8 ambiguous.
    audit = {"counts": {"clean": 8000, "mixed": 300, "error": 5}}
    batch = {"counts": {"done": 90, "ambiguous": 8, "failed": 2}}
    s = repair_legacy_basis.summarize_progress(audit, batch)
    assert s["audit_mixed"] == 300
    assert s["audit_mixed_rate"] == round(300 / 8305, 4)
    assert s["batch_attempted"] == 100
    assert s["batch_ambiguous_rate"] == 0.08
    assert s["tail_mixed_exact"] == 200  # 300 total mixed − 100 attempted
    assert s["tail_estimated_unrepairable"] == 16  # 200 × 0.08, rounded


def test_priority_only_skips_unranked_tail_symbols(tmp_path):
    _seed_mixed(tmp_path, "AAPL")  # sp500 member
    _seed_mixed(tmp_path, "ZZZQ")  # in no priority preset
    bronze = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity")
    import hashlib

    def _entry(sym):
        p = bronze.symbol_path(sym)
        return {
            "symbol": sym,
            "path": str(p),
            "source_sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
            "klass": "mixed",
            "break_date": "2021-06-18",
        }

    manifest_path = tmp_path / "audit.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "data_lake_root": str(tmp_path.resolve()),
                "symbols": [_entry("AAPL"), _entry("ZZZQ")],
            }
        )
    )
    ib_rows = {
        s: [
            {
                "trade_date": d,
                "symbol_id": 0,
                "open": c,
                "high": c,
                "low": c,
                "close": c,
                "adj_close": c,
                "volume": 100,
                "source": "ib",
                "price_basis": "split_adjusted",
                "currency": "USD",
            }
            for d, c in ((date(2021, 6, 17), 186.57), (date(2021, 6, 18), 186.4), (date(2021, 6, 21), 184.27))
        ]
        for s in ("AAPL", "ZZZQ")
    }
    output_dir = tmp_path / "out"
    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest_path), "--output-dir", str(output_dir), "--priority-only"],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher(ib_rows),
    )
    assert rc == 0
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert "AAPL" in cursor["completed"]  # ranked → processed
    assert "ZZZQ" not in cursor["completed"]  # unranked tail → deferred


def test_main_delegates_to_run(monkeypatch):
    seen = {}

    def _fake_run(argv):
        seen["argv"] = argv
        return 0

    monkeypatch.setattr(repair_legacy_basis, "run", _fake_run)
    assert repair_legacy_basis.main(["--audit-manifest", "a.json", "--output-dir", "out"]) == 0
    assert seen["argv"] == ["--audit-manifest", "a.json", "--output-dir", "out"]

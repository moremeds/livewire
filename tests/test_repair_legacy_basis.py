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


def _clean_ib_rows_for(symbol):
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
        for d, c in (
            (date(2021, 6, 17), 186.57),
            (date(2021, 6, 18), 186.4),
            (date(2021, 6, 21), 184.27),
            (date(2021, 7, 21), 185.0),
        )
    ]


def test_mid_run_ib_session_drop_aborts(tmp_path):
    # Three unranked symbols → deterministic alphabetical order ZZZA, ZZZB, ZZZC.
    for sym in ("ZZZA", "ZZZB", "ZZZC"):
        _seed_mixed(tmp_path, sym)
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
                "symbols": [_entry("ZZZA"), _entry("ZZZB"), _entry("ZZZC")],
            }
        )
    )
    output_dir = tmp_path / "out"

    calls = {"n": 0}

    def _dropping_factory(client):
        def _fetch(symbol, start, end):
            calls["n"] += 1
            if calls["n"] == 2:  # session drops on the 2nd symbol
                raise ConnectionError("socket closed mid-run")
            return [dict(r) for r in _clean_ib_rows_for(symbol) if start <= r["trade_date"] <= end]

        return _fetch

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest_path), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_dropping_factory,
    )
    assert rc == 1
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert cursor["completed"]["ZZZB"]["status"] == "failed"  # dropped symbol marked failed
    assert "ZZZC" not in cursor["completed"]  # 3rd symbol never attempted (aborted)
    assert calls["n"] == 2  # loop stopped after the drop


def test_mismatched_data_lake_root_raises_before_mutation(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    payload = json.loads(manifest.read_text())
    payload["data_lake_root"] = str((tmp_path / "elsewhere").resolve())
    manifest.write_text(json.dumps(payload))
    bronze = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity")
    before = bronze.symbol_path("NVDA").read_bytes()
    output_dir = tmp_path / "out"
    import pytest

    with pytest.raises(ValueError, match="does not match active root"):
        repair_legacy_basis.run(
            ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
            data_lake_root=tmp_path,
            ib_factory=lambda: object(),
            ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
        )
    assert bronze.symbol_path("NVDA").read_bytes() == before  # no mutation before the guard


def test_ib_connection_error_mid_run_aborts_the_batch(tmp_path):
    """IBConnectionError is the codebase's real session-drop signal, and it does NOT
    subclass OSError — so it fell through to the per-symbol branch and the run ground
    through every remaining symbol on a dead socket."""
    import hashlib

    from clients.ib_client import IBConnectionError

    for sym in ("AAPL", "AMZN", "NVDA"):
        _seed_mixed(tmp_path, sym)
    bronze = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity")

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
                "symbols": [_entry(s) for s in ("AAPL", "AMZN", "NVDA")],
            }
        )
    )
    calls: list[str] = []

    def _dropping_factory(client):
        def _fetch(symbol, start, end):
            calls.append(symbol)
            raise IBConnectionError("socket closed")

        return _fetch

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest_path), "--output-dir", str(tmp_path / "out")],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_dropping_factory,
        as_of_date=date(2026, 7, 17),
    )

    assert rc == 1
    assert calls == ["AAPL"]  # aborted after the first, did not grind through AMZN/NVDA


def test_missing_preset_dir_with_priority_only_is_an_error_not_an_empty_run(tmp_path):
    import pytest

    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    with pytest.raises(ValueError, match="no priority preset found"):
        repair_legacy_basis.run(
            [
                "--audit-manifest",
                str(manifest),
                "--output-dir",
                str(tmp_path / "out"),
                "--priority-only",
                "--presets-dir",
                str(tmp_path / "nope"),
            ],
            data_lake_root=tmp_path,
            ib_factory=lambda: object(),
            ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
        )


def test_manifest_without_data_lake_root_is_rejected(tmp_path):
    import pytest

    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    payload = json.loads(manifest.read_text())
    del payload["data_lake_root"]
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="audit manifest has no data_lake_root"):
        repair_legacy_basis.run(
            ["--audit-manifest", str(manifest), "--output-dir", str(tmp_path / "out")],
            data_lake_root=tmp_path,
            ib_factory=lambda: object(),
            ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
        )


def test_existing_cursor_without_resume_is_rejected(tmp_path):
    import pytest

    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
    )
    with pytest.raises(ValueError, match="cursor already exists"):
        repair_legacy_basis.run(
            ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
            data_lake_root=tmp_path,
            ib_factory=lambda: object(),
            ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
        )


def test_bronze_changed_since_the_audit_is_skipped_not_repaired(tmp_path):
    """The audit's verdict describes bytes that no longer exist."""
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    path = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").symbol_path("NVDA")
    path.write_bytes(path.read_bytes() + b"\0")
    after_tamper = path.read_bytes()

    repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
    )

    sidecar = json.loads((output_dir / "symbols" / "NVDA.json").read_text())
    assert sidecar["status"] == "failed"
    assert "changed since the audit" in sidecar["reason"]
    assert path.read_bytes() == after_tamper  # refused to repair, wrote nothing


def test_repair_backs_up_bronze_before_mutating(tmp_path):
    import hashlib

    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    path = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").symbol_path("NVDA")
    before = path.read_bytes()

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
    )

    assert rc == 0
    assert path.read_bytes() != before  # the repair really did mutate the system of record
    backup = output_dir / "backup" / "NVDA.1d.parquet"
    assert backup.is_file()
    assert backup.read_bytes() == before
    sidecar = json.loads((output_dir / "symbols" / "NVDA.json").read_text())
    assert sidecar["backup_sha256"] == hashlib.sha256(before).hexdigest()


def test_dry_run_makes_no_bronze_write_and_no_backup(tmp_path):
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    path = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").symbol_path("NVDA")
    before = path.read_bytes()

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir), "--dry-run"],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
    )

    assert rc == 0
    assert path.read_bytes() == before
    assert not (output_dir / "backup").exists()
    assert json.loads((output_dir / "symbols" / "NVDA.json").read_text())["status"] == "would-repair"


def test_dry_run_is_not_treated_as_completed_work_on_resume(tmp_path):
    """`would-repair` must not let --resume skip a symbol that was never repaired."""
    _seed_mixed(tmp_path, "NVDA")
    manifest = _audit_manifest(tmp_path, "NVDA")
    output_dir = tmp_path / "out"
    path = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").symbol_path("NVDA")
    before = path.read_bytes()
    repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir), "--dry-run"],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
    )

    repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir), "--resume"],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"NVDA": _clean_ib_rows_for("NVDA")}),
    )

    assert path.read_bytes() != before  # the real run was not skipped
    assert json.loads((output_dir / "cursor.json").read_text())["completed"]["NVDA"]["status"] == "done"


def _seed_aph(root):
    """Real APH bronze (production, frozen 2026-07-17): pre-window rows are IB
    back-adjusted for the real 2024-06-12 1:2 split yet labelled raw, post-window
    rows are genuine raw. The 2024-06-11/12 pair straddles the split so the IB
    classifier can resolve treatment instead of bailing out ambiguous."""
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
        for d, c in (
            ("2021-06-09", 34.07),
            ("2021-06-10", 34.13),
            ("2021-06-11", 68.45),
            ("2021-06-14", 68.31),
            ("2024-06-11", 134.43),
            ("2024-06-12", 68.69),
        )
    ]
    BronzeClient(root / "bronze/asset_class=equity", "equity").replace_ticker_rows("APH", rows)
    split = MassiveSplit(
        provider_event_id="APH-2024",
        ticker="APH",
        execution_date=date(2024, 6, 12),
        split_from=Decimal("1"),
        split_to=Decimal("2"),
        payload_hash="aph-2024-06-12",
    )
    CorporateActionStore(root).reconcile("APH", [split], datetime(2024, 6, 12, tzinfo=UTC))


def test_partial_ib_refetch_leaving_a_2x_seed_residual_is_ambiguous_not_done(tmp_path):
    """The 6.0 self-check cannot see a 2x residual, so without the deterministic seed
    check this repair records `done` while 2021-06-09/10 stay double-adjusted."""
    _seed_aph(tmp_path)
    manifest = _audit_manifest(tmp_path, "APH")
    output_dir = tmp_path / "out"
    # IB comes back short: nothing before 2021-06-11, so the corrupt pre-window rows
    # survive the merge. The rows it does return straddle the split, so classification
    # succeeds ("raw") and the run reaches the post-merge self-check.
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
            (date(2021, 6, 11), 68.45),
            (date(2021, 6, 14), 68.31),
            (date(2024, 6, 11), 134.43),
            (date(2024, 6, 12), 68.69),
        )
    ]

    rc = repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(output_dir)],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_clean_ib_fetcher({"APH": ib_rows}),
        as_of_date=date(2026, 7, 17),
    )

    assert rc == 0
    cursor = json.loads((output_dir / "cursor.json").read_text())
    assert cursor["completed"]["APH"]["status"] == "ambiguous"
    # Pin the branch: it must fail on the SEED check, not on classification and not on
    # the 6.0 heuristic — otherwise this test would pass with the detector removed.
    sidecar = json.loads((output_dir / "symbols" / "APH.json").read_text())
    assert "seed-boundary basis break at 2021-06-11" in sidecar["reason"]
    # And bronze is untouched: a fail-closed repair writes zero bytes.
    surviving = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").read_symbol_rows("APH")
    assert {r["source"] for r in surviving} == {"legacy"}


def test_main_delegates_to_run(monkeypatch):
    seen = {}

    def _fake_run(argv):
        seen["argv"] = argv
        return 0

    monkeypatch.setattr(repair_legacy_basis, "run", _fake_run)
    assert repair_legacy_basis.main(["--audit-manifest", "a.json", "--output-dir", "out"]) == 0
    assert seen["argv"] == ["--audit-manifest", "a.json", "--output-dir", "out"]

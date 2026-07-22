"""Unit tests for the read-only Yahoo basis resolver (dry-run).

Fixtures are REAL AMC split-adjusted closes across its 2023-08-24 1:10 reverse split
(frozen 2026-07-17). No network — the Yahoo client is a fake returning frozen bars.
"""

import json
from datetime import date

import pytest

from clients.bronze_client import BronzeClient
from clients.yahoo_client import YahooBar, YahooSplit
from livewire_scripts import resolve_yahoo_basis, rollback_legacy_basis
from tests.test_rebuild_silver import _seed_bronze, _seed_split

AS_OF = date(2026, 7, 17)

# Real AMC Yahoo split-adjusted closes; the 1:10 reverse split multiplier is 0.1,
# so the true raw pre-split close of 19.60 is 1.96.
_AMC_BARS = [
    YahooBar(date(2023, 8, 23), 19.60, 19.60),
    YahooBar(date(2023, 8, 24), 14.37, 14.37),
    YahooBar(date(2023, 8, 25), 12.43, 12.43),
]
_AMC_SPLIT = [YahooSplit(date(2023, 8, 24), 1.0, 10.0)]


class _FakeYahoo:
    def __init__(self, bars=_AMC_BARS, splits=_AMC_SPLIT):
        self._bars, self._splits = bars, splits

    def get_daily(self, symbol, start, end):
        return self._bars, self._splits


def _run(tmp_path, *, yahoo=None):
    output = tmp_path / "manifest.json"
    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(output)],
        data_lake_root=tmp_path,
        yahoo_factory=lambda: yahoo or _FakeYahoo(),
        as_of_date=AS_OF,
    )
    manifest = __import__("json").loads(output.read_text())
    return next(s for s in manifest["symbols"] if s["symbol"] == "AMC")


def test_adjusted_deep_row_is_flagged_rewrite_and_symbol_would_resolve(tmp_path):
    # Bronze stores the pre-split row at its ADJUSTED value (19.60) — the exact bug.
    _seed_bronze(
        tmp_path,
        "AMC",
        [("2023-08-23", 19.60), ("2023-08-24", 14.37), ("2023-08-25", 12.43)],
        source="legacy",
        price_basis="unknown",
    )
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)  # reverse 10:1 in store terms
    entry = _run(tmp_path)
    assert entry["status"] == "would_resolve"
    assert entry["rewrite"] == 1  # only 2023-08-23 (it has a split ahead and is adjusted)
    assert entry["relabel"] == 2  # the ex-date row and the one after have no split ahead


def test_already_raw_series_only_relabels(tmp_path):
    # Bronze already stores the raw pre-split value (1.96) — nothing to rewrite.
    _seed_bronze(
        tmp_path,
        "AMC",
        [("2023-08-23", 1.96), ("2023-08-24", 14.37), ("2023-08-25", 12.43)],
        source="legacy",
        price_basis="unknown",
    )
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)
    entry = _run(tmp_path)
    assert entry["status"] == "would_resolve"
    assert entry["rewrite"] == 0
    assert entry["relabel"] == 3


def test_split_the_store_lacks_fails_reconciliation(tmp_path):
    _seed_bronze(
        tmp_path, "AMC", [("2023-08-23", 19.60), ("2023-08-24", 14.37)], source="legacy", price_basis="unknown"
    )
    # Store has NO split → Yahoo's split is unreconciled → fail closed.
    entry = _run(tmp_path)
    assert entry["status"] == "split_mismatch"
    assert entry["store_missing_splits"] == [["2023-08-24", 0.1]]


def test_large_mismatch_fraction_fails_high_mismatch(tmp_path):
    # 5.00 matches neither raw 1.96 nor adjusted 19.60; 1 of 2 rows = 50% → not isolated.
    _seed_bronze(tmp_path, "AMC", [("2023-08-23", 5.00), ("2023-08-24", 14.37)], source="legacy", price_basis="unknown")
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)
    entry = _run(tmp_path)
    assert entry["status"] == "high_mismatch"
    assert entry["mismatch"] == 1


def test_isolated_mismatch_row_is_flagged_and_kept_not_failed(tmp_path, monkeypatch):
    # One off row (99.0 vs Yahoo 12.43) stays at its bronze value, is flagged, and the
    # symbol still resolves — the operator decision: never overwrite bronze on Yahoo alone.
    monkeypatch.setattr(resolve_yahoo_basis, "_MAX_MISMATCH_FRACTION", 0.9)
    _seed_bronze(
        tmp_path,
        "AMC",
        [("2023-08-23", 19.60), ("2023-08-24", 14.37), ("2023-08-25", 99.0)],
        source="legacy",
        price_basis="unknown",
    )
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)
    entry = _run(tmp_path)
    assert entry["status"] == "would_resolve"
    assert entry["mismatch"] == 1
    assert entry["flagged"] == [["2023-08-25", 99.0, 12.43]]


def test_symbol_absent_from_bronze_is_reported(tmp_path):
    entry = _run(tmp_path)  # nothing seeded
    assert entry["status"] == "no_bronze_rows"


def test_yahoo_missing_is_reported(tmp_path):
    _seed_bronze(tmp_path, "AMC", [("2023-08-23", 1.96)], source="legacy", price_basis="unknown")

    class _Missing:
        def get_daily(self, symbol, start, end):
            from clients.yahoo_client import YahooNotFound

            raise YahooNotFound(symbol)

    entry = _run(tmp_path, yahoo=_Missing())
    assert entry["status"] == "yahoo_missing"


def _bronze_basis(tmp_path, symbol):
    rows = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").read_symbol_rows(symbol)
    return {str(r["price_basis"]) for r in rows}


def test_apply_relabel_only_flips_basis_without_touching_prices(tmp_path):
    # A pure-relabel symbol: bronze already holds raw values (1.96 pre-split), basis unknown.
    _seed_bronze(
        tmp_path,
        "AMC",
        [("2023-08-23", 1.96), ("2023-08-24", 14.37), ("2023-08-25", 12.43)],
        source="legacy",
        price_basis="unknown",
    )
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)
    out = tmp_path / "manifest.json"
    output_dir = tmp_path / "apply"
    before = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").read_symbol_rows("AMC")
    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(out), "--apply", "--output-dir", str(output_dir),
         "--relabel-only", "--ib-verify", "--ib-min-overlap", "1"],
        data_lake_root=tmp_path,
        yahoo_factory=_FakeYahoo,
        ib_factory=_FakeIB,
        ib_fetcher_factory=_fetcher(_AMC_IB_MATCH),
        as_of_date=AS_OF,
    )
    after = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").read_symbol_rows("AMC")
    assert _bronze_basis(tmp_path, "AMC") == {"raw"}  # basis flipped
    # prices untouched
    assert [r["close"] for r in sorted(after, key=lambda x: str(x["trade_date"]))] == [
        r["close"] for r in sorted(before, key=lambda x: str(x["trade_date"]))
    ]
    # rollback restores the original bytes (basis back to unknown)
    assert rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path) == 0
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}


def test_apply_without_relabel_only_is_refused(tmp_path):
    _seed_bronze(tmp_path, "AMC", [("2023-08-23", 1.96)], source="legacy", price_basis="unknown")
    with __import__("pytest").raises(ValueError, match="relabel-only"):
        resolve_yahoo_basis.run(
            ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply", "--output-dir", str(tmp_path / "a")],
            data_lake_root=tmp_path,
            yahoo_factory=_FakeYahoo,
            as_of_date=AS_OF,
        )


def _bronze_row(tmp_path, symbol, iso_date):
    rows = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").read_symbol_rows(symbol)
    return next(r for r in rows if str(r["trade_date"])[:10] == iso_date)


def test_scale_row_scales_ohlc_by_factor_and_volume_inversely():
    # A 10:1 reverse split fold is 0.1: prices ×0.1, volume ÷0.1 (=×10).
    row = {
        "open": 20.0,
        "high": 22.0,
        "low": 19.0,
        "close": 19.6,
        "adj_close": 19.6,
        "volume": 1000,
        "price_basis": "unknown",
    }
    scaled = resolve_yahoo_basis._scale_row(row, 0.1)
    assert scaled["open"] == pytest.approx(2.0) and scaled["high"] == pytest.approx(2.2)
    assert scaled["low"] == pytest.approx(1.9) and scaled["close"] == pytest.approx(1.96)
    assert scaled["volume"] == 10000 and scaled["price_basis"] == "raw"


def test_apply_rewrite_rewrites_full_ohlcv_to_raw_and_rolls_back(tmp_path):
    # Bronze stores the pre-split row at its ADJUSTED value (19.60); the ex-date and later
    # rows are already post-split. apply_rewrite must fold ONLY the pre-split row to raw.
    _seed_bronze(
        tmp_path,
        "AMC",
        [("2023-08-23", 19.60), ("2023-08-24", 14.37), ("2023-08-25", 12.43)],
        source="legacy",
        price_basis="unknown",
    )
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)
    out, output_dir = tmp_path / "m.json", tmp_path / "rewrite"
    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(out), "--apply", "--output-dir", str(output_dir),
         "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "1"],
        data_lake_root=tmp_path,
        yahoo_factory=_FakeYahoo,
        ib_factory=_FakeIB,
        ib_fetcher_factory=_fetcher(_AMC_IB_MATCH),
        as_of_date=AS_OF,
    )
    pre = _bronze_row(tmp_path, "AMC", "2023-08-23")  # folded ×0.1 → 1.96, volume ÷0.1 → 10000
    assert pre["close"] == pytest.approx(1.96) and pre["open"] == pytest.approx(1.96)
    assert pre["high"] == pytest.approx(1.96) and pre["low"] == pytest.approx(1.96)
    assert pre["volume"] == 10000 and pre["price_basis"] == "raw"
    ex = _bronze_row(tmp_path, "AMC", "2023-08-24")  # ex-date row untouched but relabeled
    assert ex["close"] == pytest.approx(14.37) and ex["volume"] == 1000 and ex["price_basis"] == "raw"
    assert _bronze_basis(tmp_path, "AMC") == {"raw"}
    # rollback restores the original adjusted value + unknown basis
    assert rollback_legacy_basis.run(["--output-dir", str(output_dir)], data_lake_root=tmp_path) == 0
    assert _bronze_row(tmp_path, "AMC", "2023-08-23")["close"] == pytest.approx(19.60)
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}


def test_apply_never_writes_an_unresolvable_symbol(tmp_path):
    # Store lacks the split → split_mismatch → apply must leave bronze alone.
    _seed_bronze(
        tmp_path, "AMC", [("2023-08-23", 19.60), ("2023-08-24", 14.37)], source="legacy", price_basis="unknown"
    )
    out = tmp_path / "m.json"
    rc = resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(out), "--apply", "--output-dir", str(tmp_path / "x"),
         "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "1"],
        data_lake_root=tmp_path, yahoo_factory=_FakeYahoo,
        ib_factory=_FakeIB, ib_fetcher_factory=_fetcher(_AMC_IB_MATCH), as_of_date=AS_OF,
    )
    assert rc == 0
    entry = next(s for s in json.loads(out.read_text())["symbols"] if s["symbol"] == "AMC")
    assert entry["status"] == "split_mismatch" and "applied" not in entry
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}


def test_reads_symbols_file_and_main(tmp_path, monkeypatch):
    _seed_bronze(tmp_path, "AMC", [("2023-08-23", 1.96), ("2023-08-24", 14.37)], source="legacy", price_basis="unknown")
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)
    symbols_file = tmp_path / "syms.json"
    symbols_file.write_text('{"RESOLVED_validated": ["AMC"]}')
    output = tmp_path / "m.json"
    monkeypatch.setattr(resolve_yahoo_basis, "data_lake_dir", lambda: tmp_path)
    monkeypatch.setattr(resolve_yahoo_basis, "YahooClient", _FakeYahoo)
    assert (
        resolve_yahoo_basis.run(["--symbols-file", str(symbols_file), "--output", str(output)], as_of_date=AS_OF) == 0
    )
    assert __import__("json").loads(output.read_text())["counts"]["would_resolve"] == 1


# REAL rebuild-silver --failure-output shape, copied from a rev manifest generated
# 2026-07-18 (keys: "failures" / "error" / "data_lake_root" — NOT "failed" / "reason").
def _failure_manifest(tmp_path, root=None):
    manifest = tmp_path / "fail.json"
    manifest.write_text(json.dumps({
        "as_of_date": "2026-07-18",
        "data_lake_root": str(root if root is not None else tmp_path),
        "schema_version": 1,
        "failures": [
            {"symbol": "AMC", "error": "unknown price_basis for split-affected row 2021-06-11",
             "error_type": "ValueError"},
            {"symbol": "KO", "error": "dividend currency does not match bronze currency",
             "error_type": "ValueError"},
        ],
    }))
    return manifest


def test_failure_manifest_filters_to_split_affected_unknown(tmp_path):
    from livewire_scripts.resolve_yahoo_basis import _symbols, parse_args

    args = parse_args(["--failure-manifest", str(_failure_manifest(tmp_path)),
                       "--output", str(tmp_path / "o.json")])
    assert _symbols(args, root=tmp_path) == ["AMC"]  # KO's dividend reason is out of scope


def test_failure_manifest_with_no_matching_failures_errors_not_silently_empty(tmp_path):
    """Schema drift or a clean manifest must never read as a successful zero-symbol run."""
    from livewire_scripts.resolve_yahoo_basis import _symbols, parse_args

    manifest = tmp_path / "f.json"
    manifest.write_text(json.dumps({"data_lake_root": str(tmp_path), "failures": [
        {"symbol": "KO", "error": "dividend currency does not match bronze currency"},
    ]}))
    args = parse_args(["--failure-manifest", str(manifest), "--output", str(tmp_path / "o.json")])
    with pytest.raises(ValueError, match="no 'unknown price_basis"):
        _symbols(args, root=tmp_path)


def test_failure_manifest_from_another_data_lake_is_refused(tmp_path):
    from livewire_scripts.resolve_yahoo_basis import _symbols, parse_args

    args = parse_args(["--failure-manifest", str(_failure_manifest(tmp_path, root=tmp_path / "elsewhere")),
                       "--output", str(tmp_path / "o.json")])
    with pytest.raises(ValueError, match="active root"):
        _symbols(args, root=tmp_path)


def test_limit_caps_processed_symbols(tmp_path):
    for sym, close in [("AMC", 1.96), ("BBW", 20.0)]:
        _seed_bronze(tmp_path, sym, [("2026-07-14", close)], source="legacy", price_basis="unknown")
    out = tmp_path / "m.json"
    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "BBW", "--limit", "1", "--output", str(out)],
        data_lake_root=tmp_path, yahoo_factory=_FakeYahoo, as_of_date=AS_OF,
    )
    assert len(json.loads(out.read_text())["symbols"]) == 1


def test_resume_skips_symbol_marked_done_in_cursor(tmp_path):
    _seed_bronze(tmp_path, "AMC", [("2023-08-23", 1.96)], source="legacy", price_basis="unknown")
    output_dir = tmp_path / "batch"
    output_dir.mkdir()
    identity = {"data_lake_root": str(tmp_path.resolve())}
    (output_dir / "cursor.json").write_text(json.dumps({"identity": identity, "completed": {"AMC": {"status": "done"}}}))
    calls = {"n": 0}
    class _CountingYahoo(_FakeYahoo):
        def get_daily(self, *a, **k):
            calls["n"] += 1
            return super().get_daily(*a, **k)
    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--output-dir", str(output_dir), "--resume"],
        data_lake_root=tmp_path, yahoo_factory=_CountingYahoo, as_of_date=AS_OF,
    )
    assert calls["n"] == 0  # AMC already done → never re-fetched
    assert json.loads((tmp_path / "m.json").read_text())["symbols"] == []


def test_priority_order_orders_by_preset(tmp_path, monkeypatch):
    from livewire_scripts import resolve_yahoo_basis as R
    monkeypatch.setattr(R, "_priority_rank", lambda d: {"BBW": 0, "AMC": 1})
    from livewire_scripts.resolve_yahoo_basis import _ordered_symbols, parse_args
    args = parse_args(["--tickers", "AMC", "BBW", "--priority-order", "--output", str(tmp_path / "o.json")])
    assert _ordered_symbols(args, ["AMC", "BBW"]) == ["BBW", "AMC"]


def test_relabel_only_defers_rewrite_symbol(tmp_path):
    # A rewrite>0 symbol under --relabel-only is deferred (skipped_rewrite), bronze untouched.
    _seed_bronze(
        tmp_path,
        "AMC",
        [("2023-08-23", 19.60), ("2023-08-24", 14.37), ("2023-08-25", 12.43)],
        source="legacy",
        price_basis="unknown",
    )
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)
    out = tmp_path / "m.json"
    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(out), "--apply", "--output-dir", str(tmp_path / "a"),
         "--relabel-only", "--ib-verify", "--ib-min-overlap", "1"],
        data_lake_root=tmp_path,
        yahoo_factory=_FakeYahoo,
        ib_factory=_FakeIB,
        ib_fetcher_factory=_fetcher(_AMC_IB_MATCH),
        as_of_date=AS_OF,
    )
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}  # deferred, not mutated
    import json

    entry = next(s for s in json.loads(out.read_text())["symbols"] if s["symbol"] == "AMC")
    assert entry["applied"] == "skipped_rewrite"


# --- IB anchor gate wired into apply (Task 3) ---

# Real frozen AMC closes: pre-split row ADJUSTED (19.60), post-split rows at raw values.
_AMC_MULTI = [("2023-08-23", 19.60), ("2023-08-24", 14.37), ("2023-08-25", 12.43),
              ("2023-08-28", 11.90), ("2023-08-29", 11.50), ("2023-08-30", 11.20), ("2023-08-31", 11.00)]
# IB post-last-split window (definitionally raw there); matches the reconstruction.
_AMC_IB_MATCH = [{"trade_date": date.fromisoformat(d), "close": c} for d, c in _AMC_MULTI if d > "2023-08-24"]


class _FakeIB:
    def connect(self, **k):
        pass

    def disconnect(self):
        pass


def _fetcher(rows):
    return lambda client: (lambda symbol, start, end: [r for r in rows if start <= r["trade_date"] <= end])


def _multi_yahoo():
    return _FakeYahoo(bars=[YahooBar(date.fromisoformat(d), c, c) for d, c in _AMC_MULTI], splits=_AMC_SPLIT)


def _seed_amc_multi(tmp_path):
    _seed_bronze(tmp_path, "AMC", _AMC_MULTI, source="legacy", price_basis="unknown")
    _seed_split(tmp_path, "AMC", "2023-08-24", 10, 1)


def test_apply_requires_ib_verify(tmp_path):
    _seed_bronze(tmp_path, "AMC", [("2023-08-23", 1.96)], source="legacy", price_basis="unknown")
    with pytest.raises(ValueError, match="ib-verify"):
        resolve_yahoo_basis.run(
            ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
             "--output-dir", str(tmp_path / "a"), "--allow-rewrite"],
            data_lake_root=tmp_path, yahoo_factory=_FakeYahoo, as_of_date=AS_OF,
        )


def test_ib_verified_symbol_is_written(tmp_path):
    _seed_amc_multi(tmp_path)
    rc = resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
         "--output-dir", str(tmp_path / "a"), "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "5"],
        data_lake_root=tmp_path, yahoo_factory=_multi_yahoo,
        ib_factory=_FakeIB, ib_fetcher_factory=_fetcher(_AMC_IB_MATCH), as_of_date=AS_OF,
    )
    assert rc == 0
    assert _bronze_basis(tmp_path, "AMC") == {"raw"}  # published


def test_ib_mismatch_leaves_bronze_untouched(tmp_path):
    _seed_amc_multi(tmp_path)
    bad_ib = [{**r, "close": r["close"] * 0.5} for r in _AMC_IB_MATCH]  # wrong entity
    out = tmp_path / "m.json"
    rc = resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(out), "--apply",
         "--output-dir", str(tmp_path / "a"), "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "5"],
        data_lake_root=tmp_path, yahoo_factory=_multi_yahoo,
        ib_factory=_FakeIB, ib_fetcher_factory=_fetcher(bad_ib), as_of_date=AS_OF,
    )
    assert rc == 0
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}  # NOT written
    entry = next(s for s in json.loads(out.read_text())["symbols"] if s["symbol"] == "AMC")
    assert entry["ib_verdict"] == "ib_mismatch" and entry["applied"] == "withheld_ib"
    # the review queue must say WHY, not just that it failed
    assert entry["ib_overlap"] == 5 and entry["ib_window_start"] == "2023-08-25"
    assert entry["ib_mismatch_sample"][0] == ["2023-08-25", 12.43, 6.215]


def test_ib_connection_failure_aborts_without_checkpoint(tmp_path):
    _seed_amc_multi(tmp_path)

    class _DeadIB:
        def connect(self, **k):
            from clients.ib_client import IBConnectionError
            raise IBConnectionError("gateway down / 2FA")

        def disconnect(self):
            pass

    output_dir = tmp_path / "a"
    rc = resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
         "--output-dir", str(output_dir), "--allow-rewrite", "--ib-verify"],
        data_lake_root=tmp_path, yahoo_factory=_multi_yahoo,
        ib_factory=_DeadIB, ib_fetcher_factory=_fetcher(_AMC_IB_MATCH), as_of_date=AS_OF,
    )
    assert rc == 1  # aborted
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}  # untouched
    cursor = output_dir / "cursor.json"
    assert not cursor.is_file() or "AMC" not in json.loads(cursor.read_text()).get("completed", {})


def test_ib_session_lost_midrun_aborts(tmp_path):
    _seed_amc_multi(tmp_path)

    def _dead_fetch(client):
        def _f(symbol, start, end):
            from clients.ib_client import IBConnectionError
            raise IBConnectionError("session dropped mid-run")
        return _f

    rc = resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
         "--output-dir", str(tmp_path / "a"), "--allow-rewrite", "--ib-verify"],
        data_lake_root=tmp_path, yahoo_factory=_multi_yahoo,
        ib_factory=_FakeIB, ib_fetcher_factory=_dead_fetch, as_of_date=AS_OF,
    )
    assert rc == 1
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}  # untouched


def test_unexpected_ib_exception_is_a_verdict_not_a_run_abort(tmp_path):
    """Any non-connection exception out of the fetcher must withhold that one symbol and
    let the batch continue — aborting would also lose the manifest, which is written
    only after the loop. (IBContractError stands in for the unenumerable ib_async surface.)"""
    _seed_amc_multi(tmp_path)

    def _unqualifiable(client):
        def _f(symbol, start, end):
            from clients.ib_client import IBContractError

            raise IBContractError("ambiguous contract")

        return _f

    out = tmp_path / "m.json"
    rc = resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(out), "--apply",
         "--output-dir", str(tmp_path / "a"), "--allow-rewrite", "--ib-verify"],
        data_lake_root=tmp_path, yahoo_factory=_multi_yahoo,
        ib_factory=_FakeIB, ib_fetcher_factory=_unqualifiable, as_of_date=AS_OF,
    )
    assert rc == 0  # the sweep survives
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}  # untouched
    entry = next(s for s in json.loads(out.read_text())["symbols"] if s["symbol"] == "AMC")
    assert entry["ib_verdict"].startswith("ib_error:") and entry["applied"] == "withheld_ib"


def test_ib_fetch_is_scoped_to_the_post_split_window(tmp_path):
    """The IB request must start inside the anchor window, not at the series start —
    a full-history request chunks into one IB call per year for a 250-day compare."""
    _seed_amc_multi(tmp_path)
    seen = []

    def _recording(client):
        def _f(symbol, start, end):
            seen.append(start)
            return [r for r in _AMC_IB_MATCH if start <= r["trade_date"] <= end]

        return _f

    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
         "--output-dir", str(tmp_path / "a"), "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "5"],
        data_lake_root=tmp_path, yahoo_factory=_multi_yahoo,
        ib_factory=_FakeIB, ib_fetcher_factory=_recording, as_of_date=AS_OF,
    )
    assert seen == [date(2023, 8, 25)]  # strictly after the 2023-08-24 split, not 2023-08-23


def test_row_yahoo_cannot_cover_before_a_split_fails_closed(tmp_path):
    """Yahoo history starting later than bronze leaves deep rows with NO reference. Those
    rows must never be stamped raw: a split lies ahead of them, so Silver would adjust a
    row that may already be adjusted. Fail closed instead of publishing an unverified series."""
    _seed_amc_multi(tmp_path)
    _seed_bronze(tmp_path, "AMC", [("2019-01-02", 15.00), *_AMC_MULTI], source="legacy", price_basis="unknown")
    out = tmp_path / "m.json"
    resolve_yahoo_basis.run(  # Yahoo still only serves the 2023 window
        ["--tickers", "AMC", "--output", str(out)],
        data_lake_root=tmp_path, yahoo_factory=_multi_yahoo, as_of_date=AS_OF,
    )
    entry = next(s for s in json.loads(out.read_text())["symbols"] if s["symbol"] == "AMC")
    assert entry["status"] == "unmatched_split_affected"
    assert entry["unverified_sample"] == ["2019-01-02"]
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}  # untouched


def test_rerunning_a_batch_without_resume_refuses_to_clobber_backups(tmp_path):
    _seed_amc_multi(tmp_path)
    output_dir = tmp_path / "a"
    argv = ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
            "--output-dir", str(output_dir), "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "5"]
    kwargs = dict(data_lake_root=tmp_path, yahoo_factory=_multi_yahoo,
                  ib_factory=_FakeIB, ib_fetcher_factory=_fetcher(_AMC_IB_MATCH), as_of_date=AS_OF)
    assert resolve_yahoo_basis.run(argv, **kwargs) == 0
    original = (output_dir / "backup" / "AMC.1d.parquet").read_bytes()
    with pytest.raises(ValueError, match="pass --resume"):
        resolve_yahoo_basis.run(argv, **kwargs)
    assert (output_dir / "backup" / "AMC.1d.parquet").read_bytes() == original  # pristine


def test_zero_min_overlap_cannot_certify_a_symbol_ib_returned_nothing_for(tmp_path):
    """min_overlap=0 makes `0 < 0` false — without the explicit empty-overlap guard an
    empty IB response would produce an empty mismatch list and read as 'verified'."""
    _seed_amc_multi(tmp_path)
    out = tmp_path / "m.json"
    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(out), "--apply",
         "--output-dir", str(tmp_path / "a"), "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "0"],
        data_lake_root=tmp_path, yahoo_factory=_multi_yahoo,
        ib_factory=_FakeIB, ib_fetcher_factory=_fetcher([]), as_of_date=AS_OF,  # IB has nothing
    )
    entry = next(s for s in json.loads(out.read_text())["symbols"] if s["symbol"] == "AMC")
    assert entry["ib_verdict"] == "ib_insufficient_overlap" and entry["applied"] == "withheld_ib"
    assert _bronze_basis(tmp_path, "AMC") == {"unknown"}


def test_ib_request_ends_at_the_window_not_at_as_of(tmp_path):
    """A symbol whose bronze stops in 2023 must not be requested through the 2026 as_of —
    IBHistoryFetcher chunks the calendar range into ~one request per year."""
    _seed_amc_multi(tmp_path)
    seen = []

    def _recording(client):
        def _f(symbol, start, end):
            seen.append((start, end))
            return [r for r in _AMC_IB_MATCH if start <= r["trade_date"] <= end]

        return _f

    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
         "--output-dir", str(tmp_path / "a"), "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "5"],
        data_lake_root=tmp_path, yahoo_factory=_multi_yahoo,
        ib_factory=_FakeIB, ib_fetcher_factory=_recording, as_of_date=AS_OF,
    )
    assert seen == [(date(2023, 8, 25), date(2023, 8, 31))]  # not (…, 2026-07-17)


def test_applied_symbol_is_resolved_exactly_once(tmp_path):
    """The published bytes must be the ones the IB anchor verified. A second resolve would
    refetch Yahoo and reread bronze/actions, so anything that changed in between would be
    published unanchored. Counting the Yahoo fetches locks that structure in place."""
    _seed_amc_multi(tmp_path)
    calls = []

    class _CountingYahoo(_FakeYahoo):
        def get_daily(self, symbol, start, end):
            calls.append(symbol)
            return super().get_daily(symbol, start, end)

    resolve_yahoo_basis.run(
        ["--tickers", "AMC", "--output", str(tmp_path / "m.json"), "--apply",
         "--output-dir", str(tmp_path / "a"), "--allow-rewrite", "--ib-verify", "--ib-min-overlap", "5"],
        data_lake_root=tmp_path,
        yahoo_factory=lambda: _CountingYahoo(
            bars=[YahooBar(date.fromisoformat(d), c, c) for d, c in _AMC_MULTI], splits=_AMC_SPLIT
        ),
        ib_factory=_FakeIB, ib_fetcher_factory=_fetcher(_AMC_IB_MATCH), as_of_date=AS_OF,
    )
    assert _bronze_basis(tmp_path, "AMC") == {"raw"}  # it did publish
    assert calls == ["AMC"]  # exactly one resolve, not two

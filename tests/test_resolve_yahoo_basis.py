"""Unit tests for the read-only Yahoo basis resolver (dry-run).

Fixtures are REAL AMC split-adjusted closes across its 2023-08-24 1:10 reverse split
(frozen 2026-07-17). No network — the Yahoo client is a fake returning frozen bars.
"""

from datetime import date

from clients.yahoo_client import YahooBar, YahooSplit
from livewire_scripts import resolve_yahoo_basis
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

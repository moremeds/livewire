import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveSplit
from livewire_scripts import audit_legacy_basis


def _seed_symbol(root, ticker, rows_spec, split=None):
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
        for d, c in rows_spec
    ]
    bronze = root / "bronze/asset_class=equity"
    BronzeClient(bronze, "equity").replace_ticker_rows(ticker, rows)
    if split is not None:
        CorporateActionStore(root).reconcile(ticker, [split], datetime(2021, 7, 20, tzinfo=UTC))
    return bronze / f"symbol={ticker}/1d.parquet"


_seed_bronze = _seed_symbol


def _seed_split(root, symbol, ex_date, split_from, split_to):
    event = MassiveSplit(
        provider_event_id=f"{symbol}-{ex_date}",
        ticker=symbol,
        execution_date=date.fromisoformat(ex_date),
        split_from=Decimal(str(split_from)),
        split_to=Decimal(str(split_to)),
        payload_hash=f"{symbol}-{ex_date}-hash",
    )
    CorporateActionStore(root).reconcile(symbol, [event], datetime(2026, 1, 4, tzinfo=UTC))


def _entry(output_path, symbol):
    """The manifest entry for one symbol."""
    manifest = json.loads(output_path.read_text())
    return next(item for item in manifest["symbols"] if item["symbol"] == symbol)


def test_mixed_symbol_classified_mixed_and_audit_is_read_only(tmp_path):
    split = MassiveSplit(
        provider_event_id="nvda",
        ticker="NVDA",
        execution_date=date(2021, 7, 20),
        split_from=Decimal("1"),
        split_to=Decimal("4"),
        payload_hash="s",
    )
    path = _seed_symbol(
        tmp_path, "NVDA", [("2021-06-17", 746.29), ("2021-06-18", 18.64), ("2021-06-21", 737.09)], split
    )
    before = path.read_bytes()
    output = tmp_path / "audit.json"

    assert audit_legacy_basis.run(["--tickers", "NVDA", "--output", str(output)], data_lake_root=tmp_path) == 0

    assert path.read_bytes() == before  # read-only
    manifest = json.loads(output.read_text())
    item = manifest["symbols"][0]
    assert item["symbol"] == "NVDA"
    assert item["klass"] == "mixed"
    assert item["break_date"] == "2021-06-18"
    assert item["source_sha256"] == hashlib.sha256(before).hexdigest()
    assert manifest["counts"]["mixed"] == 1


def test_clean_symbol_classified_clean(tmp_path):
    _seed_symbol(tmp_path, "MSFT", [("2021-06-17", 258.0), ("2021-06-18", 259.4), ("2021-06-21", 259.9)])
    output = tmp_path / "audit.json"
    assert audit_legacy_basis.run(["--tickers", "MSFT", "--output", str(output)], data_lake_root=tmp_path) == 0
    manifest = json.loads(output.read_text())
    assert manifest["symbols"][0]["klass"] == "clean"
    assert manifest["counts"] == {"clean": 1, "mixed": 0, "error": 0}


def test_unknown_basis_symbol_classified_error_not_crash(tmp_path):
    # A price_basis='unknown' row + an applicable split makes build_factor_intervals
    # raise (that's WS3's 593 territory, not a legacy-basis mix). Audit must isolate
    # it as klass='error' and still exit 0 — never abort the whole --full run.
    split = MassiveSplit(
        provider_event_id="xyz",
        ticker="XYZ",
        execution_date=date(2021, 7, 20),
        split_from=Decimal("1"),
        split_to=Decimal("4"),
        payload_hash="s",
    )
    rows = [
        {
            "trade_date": "2021-06-17",
            "symbol_id": 1,
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "adj_close": 10.0,
            "volume": 100,
            "source": "legacy",
            "price_basis": "unknown",
        }
    ]
    BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").replace_ticker_rows("XYZ", rows)
    CorporateActionStore(tmp_path).reconcile("XYZ", [split], datetime(2021, 7, 20, tzinfo=UTC))
    output = tmp_path / "audit.json"
    assert audit_legacy_basis.run(["--tickers", "XYZ", "--output", str(output)], data_lake_root=tmp_path) == 0
    manifest = json.loads(output.read_text())
    assert manifest["symbols"][0]["klass"] == "error"
    assert manifest["counts"]["error"] == 1


def test_symbol_with_zero_rows_is_error_not_clean(tmp_path):
    # A 0-row parquet is discoverable but read_symbol_rows returns []. It is a broken
    # symbol, not a clean one: calling it clean silently passes it through the audit
    # and it never reaches repair. BronzeClient refuses to write an empty snapshot,
    # so this shape can only be produced by writing the parquet directly.
    bronze = BronzeClient(tmp_path / "bronze/asset_class=equity", "equity")
    path = bronze.symbol_path("EMPTY")
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"trade_date": pa.array([], type=pa.string())}), path)
    output = tmp_path / "audit.json"
    assert audit_legacy_basis.run(["--tickers", "EMPTY", "--output", str(output)], data_lake_root=tmp_path) == 0
    entry = _entry(output, "EMPTY")
    assert entry["klass"] == "error"
    assert entry["error"] == "no bronze rows"


def test_seed_boundary_symbol_classified_mixed_below_continuity_threshold(tmp_path):
    """A 2x seed fold is invisible to the 6.0 gate but must be caught. Real APH
    closes (production bronze, 2026-07-17) + its real 2024-06-12 1:2 split."""
    root = tmp_path / "lake"
    _seed_bronze(
        root, "APH", [("2021-06-09", 34.07), ("2021-06-10", 34.13), ("2021-06-11", 68.45), ("2021-06-14", 68.31)]
    )
    _seed_split(root, "APH", "2024-06-12", 1, 2)
    output = tmp_path / "audit.json"
    assert (
        audit_legacy_basis.run(
            ["--tickers", "APH", "--output", str(output)], data_lake_root=root, as_of_date=date(2026, 7, 17)
        )
        == 0
    )
    entry = _entry(output, "APH")
    assert entry["klass"] == "mixed"
    assert entry["detector"] == "seed_boundary"
    assert entry["seed_boundary"]["fold"] == pytest.approx(2.0)


def test_predicted_fold_with_flat_boundary_stays_clean(tmp_path):
    """KLAC has a 10:1 split after the window but was re-pulled raw — not corrupt."""
    root = tmp_path / "lake"
    _seed_bronze(
        root, "KLAC", [("2021-06-09", 314.16), ("2021-06-10", 319.31), ("2021-06-11", 320.11), ("2021-06-14", 325.21)]
    )
    _seed_split(root, "KLAC", "2026-06-12", 1, 10)
    output = tmp_path / "audit.json"
    assert (
        audit_legacy_basis.run(
            ["--tickers", "KLAC", "--output", str(output)], data_lake_root=root, as_of_date=date(2026, 7, 17)
        )
        == 0
    )
    assert _entry(output, "KLAC")["klass"] == "clean"


def test_requested_symbol_absent_from_bronze_is_recorded_as_error(tmp_path):
    root = tmp_path / "lake"
    _seed_bronze(root, "AAPL", [("2021-06-10", 126.11), ("2021-06-11", 127.35)])
    output = tmp_path / "audit.json"
    assert (
        audit_legacy_basis.run(
            ["--tickers", "AAPL", "NOTATICKER", "--output", str(output)],
            data_lake_root=root,
            as_of_date=date(2026, 7, 17),
        )
        == 0
    )
    entry = _entry(output, "NOTATICKER")
    assert entry["klass"] == "error"
    assert "not in bronze" in entry["error"]


def test_audit_records_every_break_not_just_the_first(tmp_path):
    """A single break_date under-reports multi-break symbols and starves the triage."""
    root = tmp_path / "lake"
    _seed_bronze(
        root, "AAPL", [("2001-01-02", 1.00), ("2001-01-03", 50.00), ("2002-01-02", 51.00), ("2002-01-03", 4.00)]
    )
    output = tmp_path / "audit.json"
    audit_legacy_basis.run(
        ["--tickers", "AAPL", "--output", str(output)], data_lake_root=root, as_of_date=date(2026, 7, 17)
    )
    entry = _entry(output, "AAPL")
    assert [b["date"] for b in entry["breaks"]] == ["2001-01-03", "2002-01-03"]
    assert entry["break_date"] == "2001-01-03"  # first break, kept for repair compatibility


def test_full_scope_scans_all_existing_symbols(tmp_path):
    _seed_symbol(tmp_path, "MSFT", [("2021-06-17", 258.0), ("2021-06-18", 259.4)])
    _seed_symbol(tmp_path, "AAPL", [("2021-06-17", 130.0), ("2021-06-18", 131.0)])
    output = tmp_path / "audit.json"
    assert audit_legacy_basis.run(["--full", "--output", str(output)], data_lake_root=tmp_path) == 0
    manifest = json.loads(output.read_text())
    assert {s["symbol"] for s in manifest["symbols"]} == {"AAPL", "MSFT"}
    assert manifest["counts"]["clean"] == 2


def test_preset_scope_limits_to_preset_tickers(tmp_path):
    _seed_symbol(tmp_path, "MSFT", [("2021-06-17", 258.0), ("2021-06-18", 259.4)])
    _seed_symbol(tmp_path, "AAPL", [("2021-06-17", 130.0), ("2021-06-18", 131.0)])
    preset = tmp_path / "mini.json"
    preset.write_text(json.dumps({"name": "mini", "tickers": ["MSFT"]}))
    output = tmp_path / "audit.json"
    assert audit_legacy_basis.run(["--preset", str(preset), "--output", str(output)], data_lake_root=tmp_path) == 0
    manifest = json.loads(output.read_text())
    assert [s["symbol"] for s in manifest["symbols"]] == ["MSFT"]


def test_main_wraps_run_with_explicit_data_lake_root(tmp_path):
    _seed_symbol(tmp_path, "MSFT", [("2021-06-17", 258.0), ("2021-06-18", 259.4)])
    output = tmp_path / "audit.json"
    assert (
        audit_legacy_basis.main(["--tickers", "MSFT", "--output", str(output), "--data-lake-root", str(tmp_path)]) == 0
    )
    assert json.loads(output.read_text())["symbols"][0]["klass"] == "clean"

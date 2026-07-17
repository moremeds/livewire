import json
from datetime import UTC, date, datetime
from decimal import Decimal

from clients.bronze_client import BronzeClient
from clients.corporate_action_store import CorporateActionStore
from clients.massive_client import MassiveSplit
from livewire_scripts import audit_legacy_basis, rebuild_silver, repair_legacy_basis


def test_mixed_symbol_is_repaired_then_published_clean(tmp_path):
    # 1. seed mixed NVDA
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
    BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").replace_ticker_rows("NVDA", rows)
    split = MassiveSplit(
        provider_event_id="nvda",
        ticker="NVDA",
        execution_date=date(2021, 7, 20),
        split_from=Decimal("1"),
        split_to=Decimal("4"),
        payload_hash="s",
    )
    CorporateActionStore(tmp_path).reconcile("NVDA", [split], datetime(2021, 7, 20, tzinfo=UTC))

    # 2. audit → NVDA is mixed
    manifest = tmp_path / "audit.json"
    audit_legacy_basis.run(["--tickers", "NVDA", "--output", str(manifest)], data_lake_root=tmp_path)
    assert json.loads(manifest.read_text())["symbols"][0]["klass"] == "mixed"

    # 3. repair from clean stub IB. A post-split bar (2021-07-21) is included so
    # classify_split_events can resolve the IB history basis (adjusted) → canonical raw
    # instead of failing closed as ambiguous.
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

    class _Stub:
        def __init__(self, client): ...
        def __call__(self, symbol, start, end):
            return [dict(r) for r in ib_rows if start <= r["trade_date"] <= end]

    repair_legacy_basis.run(
        ["--audit-manifest", str(manifest), "--output-dir", str(tmp_path / "out")],
        data_lake_root=tmp_path,
        ib_factory=lambda: object(),
        ib_fetcher_factory=_Stub,
    )

    # 4. rebuild → NVDA now publishes (not quarantined), revision advances
    silver_root = tmp_path / "silver"
    rc = rebuild_silver.run(["--tickers", "NVDA"], data_lake_root=tmp_path, silver_root=silver_root)
    assert rc == 0
    assert (silver_root / "asset_class=equity/symbol=NVDA/1d.parquet").exists()


def test_quarantined_symbol_is_absent_from_published_manifest(tmp_path):
    # Guard the fail-closed lifecycle contract (plan Task 5): a symbol quarantined at
    # the continuity gate must be omitted from the published rev manifest that Apex
    # reads (manifest-driven serving), not merely skipped on disk. Seed a clean MSFT
    # (publishes) alongside a still-mixed NVDA (no repair → quarantines), rebuild, and
    # assert MSFT appears in current.json artifacts while NVDA does not.
    clean = [
        {
            "trade_date": d,
            "symbol_id": 2,
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "adj_close": c,
            "volume": 100,
            "source": "legacy",
            "price_basis": "raw",
        }
        for d, c in (("2021-06-17", 258.0), ("2021-06-18", 259.4), ("2021-06-21", 259.9))
    ]
    BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").replace_ticker_rows("MSFT", clean)

    # A staging FAILURE, not a discontinuity: real INTC closes across its real
    # 2000-07-31 1:2 split with price_basis='unknown', which makes
    # build_factor_intervals raise. A discontinuity no longer quarantines — it
    # publishes a trimmed window (rebuild_silver's two-trim staging) — so the
    # fail-closed contract has to be exercised with something that cannot stage.
    unknown_basis = [
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
            "price_basis": "unknown",
        }
        for d, c in (("2000-07-27", 137.00), ("2000-07-28", 129.13), ("2000-07-31", 66.75), ("2000-08-01", 64.63))
    ]
    BronzeClient(tmp_path / "bronze/asset_class=equity", "equity").replace_ticker_rows("INTC", unknown_basis)
    split = MassiveSplit(
        provider_event_id="intc",
        ticker="INTC",
        execution_date=date(2000, 7, 31),
        split_from=Decimal("1"),
        split_to=Decimal("2"),
        payload_hash="s",
    )
    CorporateActionStore(tmp_path).reconcile("INTC", [split], datetime(2000, 7, 31, tzinfo=UTC))

    silver_root = tmp_path / "silver"
    rc = rebuild_silver.run(["--tickers", "MSFT", "INTC"], data_lake_root=tmp_path, silver_root=silver_root)
    assert rc == 0  # quarantining one symbol while another publishes is not systemic failure

    manifest = json.loads((silver_root / "revisions/current.json").read_text())
    paths = [artifact["path"] for artifact in manifest["artifacts"]]
    assert any("symbol=MSFT/" in p for p in paths)  # published → served by Apex
    assert not any("symbol=INTC/" in p for p in paths)  # quarantined → absent, Apex fails closed

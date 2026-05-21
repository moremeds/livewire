"""Tests for the static warehouse health HTML report."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from livewire_scripts.warehouse_health_report import (
    RepairAction,
    RepairOptions,
    ScanOptions,
    _build_repair_env,
    build_report,
    plan_warehouse_repairs,
    repair_warehouse,
    main,
    render_html,
    scan_warehouse,
    write_html_report,
)


_DAILY_SCHEMA = pa.schema(
    [
        ("trade_date", pa.date32()),
        ("symbol_id", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("adj_close", pa.float64()),
        ("volume", pa.int64()),
    ]
)

_INTRADAY_SCHEMA = pa.schema(
    [
        ("bar_timestamp", pa.timestamp("us", tz="UTC")),
        ("symbol_id", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
    ]
)


def _write_daily(root: Path, asset_class: str, symbol: str, dates: list[date]) -> Path:
    path = root / f"asset_class={asset_class}" / f"symbol={symbol}" / "1d.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "trade_date": d,
            "symbol_id": 1,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "adj_close": 1.5,
            "volume": 100,
        }
        for d in dates
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=_DAILY_SCHEMA), path)
    return path


def _write_intraday(root: Path, symbol: str, timeframe: str, timestamps: list[datetime]) -> Path:
    path = root / "asset_class=equity" / f"symbol={symbol}" / f"{timeframe}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "bar_timestamp": ts,
            "symbol_id": 1,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 100,
        }
        for ts in timestamps
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=_INTRADAY_SCHEMA), path)
    return path


def test_scan_warehouse_reports_daily_and_intraday_snapshots(tmp_path: Path) -> None:
    _write_daily(
        tmp_path,
        "equity",
        "AAPL",
        [date(2026, 1, 5), date(2026, 1, 7)],
    )
    _write_intraday(
        tmp_path,
        "AAPL",
        "5m",
        [
            datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 1, 5, 14, 35, tzinfo=timezone.utc),
        ],
    )

    snapshots = scan_warehouse(ScanOptions(bronze_root=tmp_path, target_date=date(2026, 1, 8)))

    daily = next(s for s in snapshots if s.timeframe == "1d")
    intraday = next(s for s in snapshots if s.timeframe == "5m")
    assert daily.asset_class == "equity"
    assert daily.symbol == "AAPL"
    assert daily.rows == 2
    assert daily.expected_rows == 3
    assert daily.coverage_ratio == 2 / 3
    assert daily.status == "thin"
    assert intraday.rows == 2
    assert intraday.key_column == "bar_timestamp"


def test_build_report_summarizes_symbols_and_rows(tmp_path: Path) -> None:
    _write_daily(tmp_path, "equity", "AAPL", [date(2026, 1, 5)])
    _write_daily(tmp_path, "rates", "DGS10", [date(2026, 1, 5), date(2026, 1, 6)])

    report = build_report(ScanOptions(bronze_root=tmp_path, target_date=date(2026, 1, 7)))

    assert report.total_snapshots == 2
    assert report.total_symbols == 2
    assert report.total_rows == 3
    assert report.by_asset["equity"].symbols == 1
    assert report.by_asset["rates"].rows == 2


def test_render_html_contains_summary_and_searchable_table(tmp_path: Path) -> None:
    _write_daily(tmp_path, "equity", "AAPL", [date(2026, 1, 5)])
    _write_intraday(
        tmp_path,
        "AAPL",
        "1m",
        [datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)],
    )
    report = build_report(ScanOptions(bronze_root=tmp_path, target_date=date(2026, 1, 6)))

    html = render_html(report)

    assert "<title>Livewire Warehouse Health</title>" in html
    assert "AAPL" in html
    assert "Asset Summary" in html
    assert "Ticker Summary" in html
    assert 'data-asset-group="equity"' in html
    assert "<details" in html
    assert "Per-File Details" in html
    assert "searchInput" in html
    assert "data-sort" in html
    assert ">Density<" in html
    assert ">Reason<" in html
    assert "coverage-bar" in html


def test_write_html_report_creates_parent_directory(tmp_path: Path) -> None:
    _write_daily(tmp_path / "bronze", "equity", "AAPL", [date(2026, 1, 5)])
    output = tmp_path / "reports" / "warehouse.html"

    report = write_html_report(
        ScanOptions(bronze_root=tmp_path / "bronze", target_date=date(2026, 1, 6)),
        output,
    )

    assert output.exists()
    assert "Livewire Warehouse Health" in output.read_text(encoding="utf-8")
    assert report.total_snapshots == 1


def test_intraday_sparse_trade_bars_are_thin_when_fresh(tmp_path: Path) -> None:
    _write_intraday(
        tmp_path,
        "AAPL",
        "1m",
        [
            datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 1, 5, 19, 59, tzinfo=timezone.utc),
        ],
    )

    snapshots = scan_warehouse(ScanOptions(bronze_root=tmp_path, target_date=date(2026, 1, 5)))

    assert snapshots[0].rows == 2
    assert snapshots[0].expected_rows > snapshots[0].rows
    assert snapshots[0].status == "thin"
    assert "not an actionable repair" in snapshots[0].reason


def test_orphan_intraday_snapshot_without_daily_is_not_actionable_warning(tmp_path: Path) -> None:
    _write_intraday(
        tmp_path,
        "OLD",
        "1m",
        [datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)],
    )

    snapshots = scan_warehouse(ScanOptions(bronze_root=tmp_path, target_date=date(2026, 1, 12)))

    assert snapshots[0].has_daily_snapshot is False
    assert snapshots[0].status == "thin"
    assert "intraday-only snapshot" in snapshots[0].reason


def test_plan_warehouse_repairs_groups_stale_actionable_snapshots(tmp_path: Path) -> None:
    _write_daily(tmp_path, "equity", "AAPL", [date(2026, 1, 5)])
    _write_daily(tmp_path, "equity", "MSFT", [date(2026, 1, 5)])
    _write_intraday(
        tmp_path,
        "MSFT",
        "5m",
        [datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)],
    )
    report = build_report(ScanOptions(bronze_root=tmp_path, target_date=date(2026, 1, 12)))

    actions = plan_warehouse_repairs(report)

    assert actions == [
        RepairAction(
            label="equity daily recent repair",
            command=[
                "python",
                "scripts/livewire_ingest.py",
                "daily",
                "--source",
                "massive",
                "--force",
                "--target-date",
                "2026-01-12",
                "--tickers",
                "AAPL",
                "MSFT",
            ],
            symbols=["AAPL", "MSFT"],
            reason="stale equity 1d snapshots",
        ),
        RepairAction(
            label="equity 5m intraday repair",
            command=[
                "python",
                "scripts/livewire_ingest.py",
                "intraday-backfill",
                "--timeframe",
                "5m",
                "--source",
                "massive",
                "--asset-class",
                "equity",
                "--days",
                "7",
                "--tickers",
                "MSFT",
            ],
            symbols=["MSFT"],
            reason="stale equity 5m snapshots",
        ),
    ]


def test_repair_warehouse_executes_until_report_is_clean(monkeypatch, tmp_path: Path) -> None:
    dirty_report = build_report_for_test(
        [
            snapshot_for_test(
                asset_class="equity",
                symbol="AAPL",
                timeframe="1d",
                latest_date=date(2026, 1, 5),
                target_date=date(2026, 1, 12),
                status="error",
            )
        ],
        target_date=date(2026, 1, 12),
        bronze_root=tmp_path,
    )
    clean_report = build_report_for_test([], target_date=date(2026, 1, 12), bronze_root=tmp_path)
    reports = iter([dirty_report, clean_report])
    commands: list[list[str]] = []

    monkeypatch.setattr(
        "livewire_scripts.warehouse_health_report.build_report",
        lambda options: next(reports),
    )
    monkeypatch.setattr(
        "livewire_scripts.warehouse_health_report._run_repair_action",
        lambda action: commands.append(action.command) or 0,
    )

    outcome = repair_warehouse(RepairOptions(scan_options=ScanOptions(bronze_root=tmp_path)))

    assert outcome.before_errors == 1
    assert outcome.after_errors == 0
    assert outcome.cleared is True
    assert commands == [[
        "python",
        "scripts/livewire_ingest.py",
        "daily",
        "--source",
        "massive",
        "--force",
        "--target-date",
        "2026-01-12",
        "--tickers",
        "AAPL",
    ]]


def test_build_repair_env_loads_repo_and_warehouse_env(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    warehouse = tmp_path / "warehouse"
    repo_root.mkdir()
    warehouse.mkdir()
    (repo_root / ".env").write_text(
        "MASSIVE_API_KEY='repo value'\nMDW_WAREHOUSE_DIR=\"$HOME/warehouse\"\n",
        encoding="utf-8",
    )
    (warehouse / ".env").write_text("FRED_API_KEY=warehouse-value\n", encoding="utf-8")

    monkeypatch.setattr("livewire_scripts.warehouse_health_report.PROJECT_ROOT", repo_root)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.delenv("FRED_API_KEY", raising=False)

    env = _build_repair_env()

    assert env["MASSIVE_API_KEY"] == "repo value"
    assert env["FRED_API_KEY"] == "warehouse-value"
    assert env["MDW_WAREHOUSE_DIR"] == str(tmp_path / "warehouse")


def test_main_writes_report_and_prints_path(tmp_path: Path, capsys) -> None:
    _write_daily(tmp_path / "bronze", "equity", "AAPL", [date(2026, 1, 5)])
    output = tmp_path / "report.html"

    rc = main([
        "--bronze-root",
        str(tmp_path / "bronze"),
        "--output",
        str(output),
        "--target-date",
        "2026-01-06",
    ])

    captured = capsys.readouterr()
    assert rc == 0
    assert output.exists()
    assert str(output) in captured.out


def snapshot_for_test(
    *,
    asset_class: str,
    symbol: str,
    timeframe: str,
    latest_date: date,
    target_date: date,
    status: str,
):
    from livewire_scripts.warehouse_health_report import SnapshotHealth

    return SnapshotHealth(
        asset_class=asset_class,
        symbol=symbol,
        timeframe=timeframe,
        path=Path(f"/tmp/{symbol}/{timeframe}.parquet"),
        rows=1,
        expected_rows=1,
        coverage_ratio=1.0,
        missing_estimate=0,
        first=latest_date.isoformat(),
        latest=latest_date.isoformat(),
        latest_date=latest_date,
        stale_days=(target_date - latest_date).days,
        size_bytes=100,
        key_column="trade_date",
        status=status,
        reason="test reason",
    )


def build_report_for_test(snapshots, *, target_date: date, bronze_root: Path):
    from livewire_scripts.warehouse_health_report import WarehouseReport
    from datetime import datetime, timezone

    return WarehouseReport(
        generated_at=datetime(2026, 1, 12, tzinfo=timezone.utc),
        bronze_root=bronze_root,
        target_date=target_date,
        snapshots=list(snapshots),
        by_asset={},
        by_timeframe={},
    )

"""Static HTML health report for the local parquet warehouse."""

from __future__ import annotations

import argparse
import html
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(PROJECT_ROOT))

import pyarrow.parquet as pq

from clients.intraday_bronze_client import INTRADAY_TIMEFRAMES
from livewire_scripts.daily_update import is_trading_day, previous_trading_day, session_close_time

_WAREHOUSE_DIR = Path(os.getenv("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
_DEFAULT_BRONZE_ROOT = _WAREHOUSE_DIR / "data-lake" / "bronze"
_DEFAULT_OUTPUT = _WAREHOUSE_DIR / "reports" / "warehouse_health.html"
_ET = ZoneInfo("America/New_York")
_INTRADAY_STEPS = {"1m": timedelta(minutes=1), "5m": timedelta(minutes=5), "1h": timedelta(hours=1)}


@dataclass(frozen=True)
class ScanOptions:
    bronze_root: Path = _DEFAULT_BRONZE_ROOT
    target_date: date | None = None


@dataclass(frozen=True)
class SnapshotHealth:
    asset_class: str
    symbol: str
    timeframe: str
    path: Path
    rows: int
    expected_rows: int
    coverage_ratio: float
    missing_estimate: int
    first: str
    latest: str
    latest_date: date | None
    stale_days: int | None
    size_bytes: int
    key_column: str
    status: str
    has_daily_snapshot: bool = True
    reason: str = ""


@dataclass(frozen=True)
class GroupSummary:
    snapshots: int
    symbols: int
    rows: int
    ok: int
    thin: int
    warn: int
    error: int
    coverage_ratio: float


@dataclass(frozen=True)
class WarehouseReport:
    generated_at: datetime
    bronze_root: Path
    target_date: date
    snapshots: list[SnapshotHealth]
    by_asset: dict[str, GroupSummary]
    by_timeframe: dict[str, GroupSummary]

    @property
    def total_snapshots(self) -> int:
        return len(self.snapshots)

    @property
    def total_symbols(self) -> int:
        return len({(item.asset_class, item.symbol) for item in self.snapshots})

    @property
    def total_rows(self) -> int:
        return sum(item.rows for item in self.snapshots)

    @property
    def total_expected_rows(self) -> int:
        return sum(item.expected_rows for item in self.snapshots)

    @property
    def coverage_ratio(self) -> float:
        return _coverage(self.total_rows, self.total_expected_rows)

    @property
    def status_counts(self) -> dict[str, int]:
        counts = {"ok": 0, "thin": 0, "warn": 0, "error": 0}
        for item in self.snapshots:
            counts[item.status] += 1
        return counts


@dataclass(frozen=True)
class RepairAction:
    label: str
    command: list[str]
    symbols: list[str]
    reason: str


@dataclass(frozen=True)
class RepairOptions:
    scan_options: ScanOptions = ScanOptions()
    dry_run: bool = False


@dataclass(frozen=True)
class RepairOutcome:
    before_errors: int
    before_warnings: int
    after_errors: int
    after_warnings: int
    actions: list[RepairAction]
    exit_codes: list[int]

    @property
    def cleared(self) -> bool:
        return self.after_errors == 0 and self.after_warnings == 0


def scan_warehouse(options: ScanOptions | None = None) -> list[SnapshotHealth]:
    """Scan actual bronze parquet snapshots and return per-file health rows."""
    options = options or ScanOptions()
    bronze_root = Path(options.bronze_root).expanduser()
    target_date = options.target_date or date.today()
    snapshots: list[SnapshotHealth] = []

    if not bronze_root.exists():
        return snapshots

    for path in sorted(bronze_root.glob("asset_class=*/symbol=*/*.parquet")):
        asset_part = path.parents[1].name
        symbol_part = path.parent.name
        if not asset_part.startswith("asset_class=") or not symbol_part.startswith("symbol="):
            continue
        asset_class = asset_part.split("=", 1)[1]
        symbol = symbol_part.split("=", 1)[1]
        timeframe = path.stem
        snapshots.append(_scan_snapshot(path, asset_class, symbol, timeframe, target_date))

    return snapshots


def build_report(options: ScanOptions | None = None) -> WarehouseReport:
    """Build the report model from scanned parquet snapshots."""
    options = options or ScanOptions()
    target_date = options.target_date or _default_target_date()
    resolved_options = ScanOptions(bronze_root=options.bronze_root, target_date=target_date)
    snapshots = scan_warehouse(resolved_options)
    return WarehouseReport(
        generated_at=datetime.now(timezone.utc),
        bronze_root=Path(options.bronze_root).expanduser(),
        target_date=target_date,
        snapshots=snapshots,
        by_asset=_summarize(snapshots, lambda item: item.asset_class),
        by_timeframe=_summarize(snapshots, lambda item: item.timeframe),
    )


def write_html_report(options: ScanOptions, output_path: Path) -> WarehouseReport:
    """Write the static HTML report and return the report model."""
    report = build_report(options)
    output_path = Path(output_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_html(report), encoding="utf-8")
    return report


def plan_warehouse_repairs(report: WarehouseReport) -> list[RepairAction]:
    """Return grouped repair actions for actionable warn/error snapshots."""
    stale = [
        item
        for item in report.snapshots
        if item.status in {"warn", "error"} and item.stale_days is not None and item.stale_days > 0
    ]
    actions: list[RepairAction] = []

    equity_daily = sorted({item.symbol for item in stale if item.asset_class == "equity" and item.timeframe == "1d"})
    if equity_daily:
        actions.append(
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
                    report.target_date.isoformat(),
                    "--tickers",
                    *equity_daily,
                ],
                symbols=equity_daily,
                reason="stale equity 1d snapshots",
            )
        )

    for timeframe in INTRADAY_TIMEFRAMES:
        symbols = sorted({
            item.symbol
            for item in stale
            if item.asset_class == "equity" and item.timeframe == timeframe
        })
        if symbols:
            actions.append(
                RepairAction(
                    label=f"equity {timeframe} intraday repair",
                    command=[
                        "python",
                        "scripts/livewire_ingest.py",
                        "intraday-backfill",
                        "--timeframe",
                        timeframe,
                        "--source",
                        "massive",
                        "--asset-class",
                        "equity",
                        "--days",
                        "7",
                        "--tickers",
                        *symbols,
                    ],
                    symbols=symbols,
                    reason=f"stale equity {timeframe} snapshots",
                )
            )

    rate_symbols = sorted({item.symbol for item in stale if item.asset_class == "rates"})
    if rate_symbols:
        actions.append(
            RepairAction(
                label="rates daily repair",
                command=["python", "scripts/livewire_ingest.py", "fred-rates"],
                symbols=rate_symbols,
                reason="stale rates snapshots",
            )
        )

    vol_daily = sorted({item.symbol for item in stale if item.asset_class == "volatility" and item.timeframe == "1d"})
    if vol_daily:
        actions.append(
            RepairAction(
                label="volatility daily repair",
                command=["python", "scripts/livewire_ingest.py", "cboe-vol"],
                symbols=vol_daily,
                reason="stale volatility 1d snapshots",
            )
        )

    for timeframe in ("5m", "1h"):
        vol_intraday = sorted({
            item.symbol
            for item in stale
            if item.asset_class == "volatility" and item.timeframe == timeframe
        })
        if vol_intraday:
            actions.append(
                RepairAction(
                    label=f"volatility {timeframe} intraday repair",
                    command=[
                        "python",
                        "scripts/livewire_ingest.py",
                        "intraday-backfill",
                        "--preset",
                        "presets/volatility-intraday.json",
                        "--timeframe",
                        timeframe,
                        "--source",
                        "ib",
                        "--asset-class",
                        "volatility",
                        "--days",
                        "7",
                        "--tickers",
                        *vol_intraday,
                    ],
                    symbols=vol_intraday,
                    reason=f"stale volatility {timeframe} snapshots",
                )
            )

    for asset_class in ("cmdty", "fx", "futures"):
        symbols = sorted({item.symbol for item in stale if item.asset_class == asset_class and item.timeframe == "1d"})
        if symbols:
            actions.append(
                RepairAction(
                    label=f"{asset_class} daily repair",
                    command=[
                        "python",
                        "scripts/livewire_ingest.py",
                        "historical",
                        "--asset-class",
                        asset_class,
                        "--tickers",
                        *symbols,
                    ],
                    symbols=symbols,
                    reason=f"stale {asset_class} 1d snapshots",
                )
            )

    return actions


def repair_warehouse(options: RepairOptions | None = None) -> RepairOutcome:
    """Plan and optionally execute repairs, then rescan the warehouse."""
    options = options or RepairOptions()
    before = build_report(options.scan_options)
    before_counts = before.status_counts
    actions = plan_warehouse_repairs(before)
    exit_codes: list[int] = []
    if not options.dry_run:
        for action in actions:
            exit_codes.append(_run_repair_action(action))
    after = build_report(options.scan_options)
    after_counts = after.status_counts
    return RepairOutcome(
        before_errors=before_counts["error"],
        before_warnings=before_counts["warn"],
        after_errors=after_counts["error"],
        after_warnings=after_counts["warn"],
        actions=actions,
        exit_codes=exit_codes,
    )


def render_html(report: WarehouseReport) -> str:
    """Render a self-contained HTML report."""
    rows = "\n".join(_render_snapshot_row(report.bronze_root, item) for item in report.snapshots)
    asset_rows = _render_asset_summary_rows(report.by_asset)
    ticker_sections = _render_ticker_summary_sections(report.snapshots)
    asset_cards = "\n".join(_render_group_card(name, summary) for name, summary in report.by_asset.items())
    timeframe_cards = "\n".join(
        _render_group_card(name, summary) for name, summary in report.by_timeframe.items()
    )
    status_counts = report.status_counts
    payload = json.dumps(
        {
            "snapshots": report.total_snapshots,
            "symbols": report.total_symbols,
            "rows": report.total_rows,
            "coverage": report.coverage_ratio,
        },
        separators=(",", ":"),
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Livewire Warehouse Health</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f7fb;
      --panel: #ffffff;
      --ink: #17202a;
      --muted: #637083;
      --line: #dce3ec;
      --ok: #15803d;
      --warn: #b45309;
      --error: #b91c1c;
      --accent: #0f766e;
      --thin: #2563eb;
      --accent-soft: #d8f3ef;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.45;
    }}
    header {{
      background: #102a43;
      color: #f8fbff;
      padding: 28px 36px 24px;
    }}
    header h1 {{
      margin: 0 0 8px;
      font-size: 28px;
      font-weight: 700;
      letter-spacing: 0;
    }}
    header p {{ margin: 4px 0; color: #cfe0f4; }}
    main {{ padding: 24px 36px 36px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
      margin-bottom: 22px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      box-shadow: 0 1px 2px rgba(16, 42, 67, 0.05);
    }}
    .label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; }}
    .value {{ margin-top: 6px; font-size: 26px; font-weight: 700; }}
    h2 {{ margin: 28px 0 12px; font-size: 18px; }}
    .status-ok {{ color: var(--ok); }}
    .status-thin {{ color: var(--thin); }}
    .status-warn {{ color: var(--warn); }}
    .status-error {{ color: var(--error); }}
    .coverage-bar {{
      width: 100%;
      height: 8px;
      overflow: hidden;
      border-radius: 999px;
      background: #e8eef5;
      margin-top: 8px;
    }}
    .coverage-fill {{ height: 100%; background: var(--accent); }}
    .toolbar {{
      display: flex;
      align-items: center;
      gap: 12px;
      margin: 20px 0 10px;
    }}
    input {{
      width: min(460px, 100%);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px 12px;
      font-size: 14px;
      background: #fff;
    }}
    .table-wrap {{
      overflow: auto;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }}
    details {{
      margin-top: 24px;
    }}
    summary {{
      cursor: pointer;
    }}
    summary h2 {{
      display: inline-block;
      margin-bottom: 12px;
    }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1120px; }}
    .compact-table table {{ min-width: 760px; }}
    .ticker-groups {{
      display: grid;
      gap: 12px;
    }}
    .asset-ticker-group {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }}
    .asset-ticker-group summary {{
      padding: 12px 14px;
      background: #f9fbfd;
      color: #314154;
      font-weight: 700;
    }}
    .asset-ticker-group .table-wrap {{
      border: 0;
      border-top: 1px solid var(--line);
      border-radius: 0;
    }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: left; }}
    th {{
      position: sticky;
      top: 0;
      background: #f9fbfd;
      color: #314154;
      font-size: 12px;
      text-transform: uppercase;
      cursor: pointer;
      user-select: none;
    }}
    td {{ font-size: 13px; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .badge {{
      display: inline-block;
      min-width: 52px;
      border-radius: 999px;
      padding: 3px 8px;
      text-align: center;
      font-size: 12px;
      font-weight: 700;
      background: var(--accent-soft);
      color: var(--accent);
    }}
    .badge.warn {{ background: #fff3d6; color: var(--warn); }}
    .badge.error {{ background: #ffe4e6; color: var(--error); }}
    .badge.thin {{ background: #dbeafe; color: var(--thin); }}
    code {{ color: #334155; }}
    @media (max-width: 700px) {{
      header, main {{ padding-left: 18px; padding-right: 18px; }}
      header h1 {{ font-size: 24px; }}
    }}
  </style>
</head>
<body data-report='{html.escape(payload, quote=True)}'>
  <header>
    <h1>Livewire Warehouse Health</h1>
    <p>Bronze root: <code>{_e(str(report.bronze_root))}</code></p>
    <p>Generated: {_e(report.generated_at.isoformat())} · Target date: {_e(report.target_date.isoformat())}</p>
  </header>
  <main>
    <section class="grid" aria-label="Summary">
      {_summary_card("Snapshots", f"{report.total_snapshots:,}")}
      {_summary_card("Symbols", f"{report.total_symbols:,}")}
      {_summary_card("Rows", f"{report.total_rows:,}")}
      {_summary_card("Density", _pct(report.coverage_ratio))}
      {_summary_card("OK", f"{status_counts['ok']:,}", "status-ok")}
      {_summary_card("Thin", f"{status_counts['thin']:,}", "status-thin")}
      {_summary_card("Warn", f"{status_counts['warn']:,}", "status-warn")}
      {_summary_card("Error", f"{status_counts['error']:,}", "status-error")}
    </section>

    <h2>By Asset Class</h2>
    <section class="grid">{asset_cards or _empty_card("No asset classes found")}</section>

    <h2>By Timeframe</h2>
    <section class="grid">{timeframe_cards or _empty_card("No timeframes found")}</section>

    <h2>Asset Summary</h2>
    <div class="table-wrap compact-table">
      <table>
        <thead>
          <tr>
            <th>Asset</th>
            <th class="num">Symbols</th>
            <th class="num">Snapshots</th>
            <th class="num">Rows</th>
            <th class="num">Density</th>
            <th class="num">OK</th>
            <th class="num">Thin</th>
            <th class="num">Warn</th>
            <th class="num">Error</th>
          </tr>
        </thead>
        <tbody>{asset_rows}</tbody>
      </table>
    </div>

    <h2>Ticker Summary</h2>
    <section class="ticker-groups">{ticker_sections}</section>

    <details>
      <summary><h2>Per-File Details</h2></summary>
    <div class="toolbar">
      <input id="searchInput" type="search" placeholder="Filter symbol, asset class, timeframe, status, path">
    </div>
    <div class="table-wrap">
      <table id="healthTable">
        <thead>
          <tr>
            <th data-sort="asset">Asset</th>
            <th data-sort="symbol">Symbol</th>
            <th data-sort="timeframe">Timeframe</th>
            <th data-sort="status">Status</th>
            <th data-sort="reason">Reason</th>
            <th data-sort="rows" class="num">Rows</th>
            <th data-sort="expected" class="num">Expected</th>
            <th data-sort="coverage" class="num">Density</th>
            <th data-sort="missing" class="num">Missing Est.</th>
            <th data-sort="first">First</th>
            <th data-sort="latest">Latest</th>
            <th data-sort="stale" class="num">Stale Days</th>
            <th data-sort="size" class="num">Size</th>
            <th data-sort="path">Path</th>
          </tr>
        </thead>
        <tbody>
          {rows}
        </tbody>
      </table>
    </div>
    </details>
  </main>
  <script>
    const input = document.getElementById('searchInput');
    const table = document.getElementById('healthTable');
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    input.addEventListener('input', () => {{
      const q = input.value.trim().toLowerCase();
      for (const row of rows) {{
        row.style.display = row.innerText.toLowerCase().includes(q) ? '' : 'none';
      }}
    }});
    for (const th of table.querySelectorAll('th[data-sort]')) {{
      th.addEventListener('click', () => {{
        const key = th.dataset.sort;
        const numeric = ['rows', 'expected', 'coverage', 'missing', 'stale', 'size'].includes(key);
        const direction = th.dataset.direction === 'asc' ? -1 : 1;
        th.dataset.direction = direction === 1 ? 'asc' : 'desc';
        const sorted = Array.from(tbody.querySelectorAll('tr')).sort((a, b) => {{
          const av = a.dataset[key] || '';
          const bv = b.dataset[key] || '';
          if (numeric) return direction * ((Number(av) || 0) - (Number(bv) || 0));
          return direction * av.localeCompare(bv);
        }});
        tbody.replaceChildren(...sorted);
      }});
    }}
  </script>
</body>
</html>
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a static HTML warehouse health report")
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=_DEFAULT_BRONZE_ROOT,
        help=f"Bronze root to scan (default: {_DEFAULT_BRONZE_ROOT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help=f"HTML output path (default: {_DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--target-date",
        type=date.fromisoformat,
        help="Date used for staleness checks (YYYY-MM-DD). Defaults to previous complete U.S. trading day.",
    )
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Run systematic repairs for actionable warning/error rows before writing the report.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --repair, print planned repairs without running them.",
    )
    args = parser.parse_args(argv)

    options = ScanOptions(bronze_root=args.bronze_root, target_date=args.target_date)
    if args.repair:
        outcome = repair_warehouse(RepairOptions(scan_options=options, dry_run=args.dry_run))
        print(
            f"Repair plan: {len(outcome.actions)} actions; "
            f"before={outcome.before_errors} errors/{outcome.before_warnings} warnings; "
            f"after={outcome.after_errors} errors/{outcome.after_warnings} warnings"
        )
        for action in outcome.actions:
            print(f"- {action.label}: {len(action.symbols)} symbols ({action.reason})")
        if args.dry_run:
            for action in outcome.actions:
                print("  " + " ".join(action.command))
    report = write_html_report(options, args.output)
    print(
        f"Wrote {args.output.expanduser()} "
        f"({report.total_snapshots:,} snapshots, {report.total_symbols:,} symbols, "
        f"{report.total_rows:,} rows, density={_pct(report.coverage_ratio)})"
    )
    return 0


def _scan_snapshot(
    path: Path,
    asset_class: str,
    symbol: str,
    timeframe: str,
    target_date: date,
) -> SnapshotHealth:
    parquet_file = pq.ParquetFile(path)
    rows = int(parquet_file.metadata.num_rows)
    key_column = _key_column(parquet_file)
    first_value, latest_value = _column_bounds(path, parquet_file, key_column)
    latest_date = _value_date(latest_value)
    stale_days = (target_date - latest_date).days if latest_date else None
    expected_rows = _expected_rows(asset_class, timeframe, first_value, latest_value)
    coverage_ratio = _coverage(rows, expected_rows)
    missing_estimate = max(expected_rows - rows, 0)
    has_daily_snapshot = _has_daily_snapshot(path, timeframe)
    status = _status(rows, coverage_ratio, stale_days, timeframe, has_daily_snapshot)
    reason = _reason(
        rows=rows,
        coverage_ratio=coverage_ratio,
        stale_days=stale_days,
        timeframe=timeframe,
        has_daily_snapshot=has_daily_snapshot,
        status=status,
    )
    return SnapshotHealth(
        asset_class=asset_class,
        symbol=symbol,
        timeframe=timeframe,
        path=path,
        rows=rows,
        expected_rows=expected_rows,
        coverage_ratio=coverage_ratio,
        missing_estimate=missing_estimate,
        first=_format_value(first_value),
        latest=_format_value(latest_value),
        latest_date=latest_date,
        stale_days=stale_days,
        size_bytes=path.stat().st_size,
        key_column=key_column,
        status=status,
        has_daily_snapshot=has_daily_snapshot,
        reason=reason,
    )


def _key_column(parquet_file: pq.ParquetFile) -> str:
    names = set(parquet_file.schema_arrow.names)
    if "bar_timestamp" in names:
        return "bar_timestamp"
    if "trade_date" in names:
        return "trade_date"
    raise ValueError("parquet snapshot has no trade_date or bar_timestamp column")


def _column_bounds(path: Path, parquet_file: pq.ParquetFile, column: str) -> tuple[object | None, object | None]:
    idx = parquet_file.schema_arrow.names.index(column)
    lows: list[object] = []
    highs: list[object] = []
    for i in range(parquet_file.metadata.num_row_groups):
        stats = parquet_file.metadata.row_group(i).column(idx).statistics
        if stats is None or not stats.has_min_max:
            return _read_column_bounds(path, column)
        lows.append(stats.min)
        highs.append(stats.max)
    if not lows:
        return None, None
    return min(lows), max(highs)


def _read_column_bounds(path: Path, column: str) -> tuple[object | None, object | None]:
    values = pq.read_table(path, columns=[column]).column(column).to_pylist()
    if not values:
        return None, None
    return min(values), max(values)


def _expected_rows(asset_class: str, timeframe: str, first: object | None, latest: object | None) -> int:
    if first is None or latest is None:
        return 0
    if timeframe in INTRADAY_TIMEFRAMES:
        first_dt = _coerce_datetime(first)
        latest_dt = _coerce_datetime(latest)
        if first_dt is None or latest_dt is None:
            return 0
        return _expected_intraday_rows(first_dt, latest_dt, timeframe)
    first_date = _coerce_date(first)
    latest_date = _coerce_date(latest)
    if first_date is None or latest_date is None:
        return 0
    return _expected_daily_rows(asset_class, first_date, latest_date)


def _expected_daily_rows(asset_class: str, start: date, end: date) -> int:
    if end < start:
        return 0
    current = start
    count = 0
    while current <= end:
        if _is_expected_daily(asset_class, current):
            count += 1
        current += timedelta(days=1)
    return count


def _expected_intraday_rows(first: datetime, latest: datetime, timeframe: str) -> int:
    if latest < first:
        return 0
    step = _INTRADAY_STEPS[timeframe]
    first_utc = first.astimezone(timezone.utc)
    latest_utc = latest.astimezone(timezone.utc)
    current_day = first_utc.astimezone(_ET).date()
    end_day = latest_utc.astimezone(_ET).date()
    total = 0
    while current_day <= end_day:
        if _is_trading_day_cached(current_day):
            total += _expected_intraday_rows_for_day(current_day, first_utc, latest_utc, timeframe, step)
        current_day += timedelta(days=1)
    return total


def _expected_intraday_rows_for_day(
    trading_day: date,
    first_utc: datetime,
    latest_utc: datetime,
    timeframe: str,
    step: timedelta,
) -> int:
    open_dt = datetime.combine(trading_day, time(9, 30), tzinfo=_ET).astimezone(timezone.utc)
    close_dt = datetime.combine(trading_day, _session_close_time_cached(trading_day), tzinfo=_ET).astimezone(timezone.utc)
    if timeframe == "1h":
        points = [open_dt]
        current = datetime.combine(trading_day, time(10, 0), tzinfo=_ET).astimezone(timezone.utc)
        while current + step <= close_dt:
            points.append(current)
            current += step
        return sum(1 for ts in points if first_utc <= ts <= latest_utc)

    first_point = max(open_dt, first_utc)
    last_point = min(close_dt - step, latest_utc)
    if last_point < first_point:
        return 0
    offset = first_point - open_dt
    steps_from_open = max(0, _ceil_div_timedelta(offset, step))
    aligned = open_dt + (step * steps_from_open)
    if aligned > last_point:
        return 0
    return int((last_point - aligned) // step) + 1


def _ceil_div_timedelta(value: timedelta, step: timedelta) -> int:
    value_us = int(value.total_seconds() * 1_000_000)
    step_us = int(step.total_seconds() * 1_000_000)
    return (value_us + step_us - 1) // step_us


@lru_cache(maxsize=None)
def _is_expected_daily(asset_class: str, day: date) -> bool:
    if asset_class in {"futures", "rates"}:
        return day.weekday() < 5
    return _is_trading_day_cached(day)


@lru_cache(maxsize=None)
def _is_trading_day_cached(day: date) -> bool:
    return is_trading_day(day)


@lru_cache(maxsize=None)
def _session_close_time_cached(day: date) -> time:
    return session_close_time(day)


def _coverage(rows: int, expected_rows: int) -> float:
    if expected_rows <= 0:
        return 1.0 if rows == 0 else 1.0
    return min(rows / expected_rows, 1.0)


def _status(
    rows: int,
    coverage_ratio: float,
    stale_days: int | None,
    timeframe: str,
    has_daily_snapshot: bool,
) -> str:
    if rows == 0:
        return "error"
    if timeframe in INTRADAY_TIMEFRAMES and not has_daily_snapshot:
        return "thin" if coverage_ratio < 0.95 or (stale_days is not None and stale_days > 3) else "ok"
    if stale_days is not None and stale_days > 7:
        return "error"
    if stale_days is not None and stale_days > 3:
        return "warn" if has_daily_snapshot else "thin"
    if coverage_ratio < 0.95:
        return "thin"
    return "ok"


def _reason(
    *,
    rows: int,
    coverage_ratio: float,
    stale_days: int | None,
    timeframe: str,
    has_daily_snapshot: bool,
    status: str,
) -> str:
    if rows == 0:
        return "Empty parquet snapshot; rerun the source backfill for this symbol/timeframe."
    if status in {"warn", "error"}:
        if stale_days is not None and stale_days > 0:
            return (
                f"Latest bar is {stale_days} calendar days behind the report target; "
                "this is an actionable repair candidate."
            )
        return "Actionable health issue detected by the warehouse scanner."
    if status == "thin":
        base = (
            "Low density means the vendor emitted bars for fewer than 95% of possible RTH slots; "
            "for trade bars this usually means no trades occurred in many buckets, not an actionable repair. "
            "Fill synthetic zero-volume bars in silver if a dense grid is required."
        )
        if timeframe in INTRADAY_TIMEFRAMES and not has_daily_snapshot:
            return "This is an intraday-only snapshot with no matching daily bronze file. " + base
        if coverage_ratio < 0.95:
            return base
        return "Fresh enough but classified thin by the warehouse policy."
    return "Fresh enough and dense enough for the warehouse health policy."


def _has_daily_snapshot(path: Path, timeframe: str) -> bool:
    if timeframe == "1d":
        return True
    return (path.parent / "1d.parquet").exists()


def _default_target_date() -> date:
    return previous_trading_day(date.today())


def _run_repair_action(action: RepairAction) -> int:
    cmd = [sys.executable if part == "python" else part for part in action.command]
    return subprocess.run(cmd, check=False, env=_build_repair_env()).returncode


def _build_repair_env() -> dict[str, str]:
    env = dict(os.environ)
    warehouse = Path(env.get("MDW_WAREHOUSE_DIR", str(Path.home() / "market-warehouse")))
    for env_file in (PROJECT_ROOT / ".env", warehouse / ".env"):
        _merge_env_file(env, env_file.expanduser())
    return env


def _merge_env_file(env: dict[str, str], path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        try:
            parsed = shlex.split(value, comments=False, posix=True)
        except ValueError:
            parsed = [value.strip()]
        env[key] = _expand_env_value(parsed[0] if parsed else "")


def _expand_env_value(value: str) -> str:
    return os.path.expanduser(os.path.expandvars(value))


def _summarize(
    snapshots: list[SnapshotHealth],
    key_fn,
) -> dict[str, GroupSummary]:
    grouped: dict[str, list[SnapshotHealth]] = {}
    for item in snapshots:
        grouped.setdefault(str(key_fn(item)), []).append(item)

    summaries: dict[str, GroupSummary] = {}
    for name, items in sorted(grouped.items()):
        rows = sum(item.rows for item in items)
        expected = sum(item.expected_rows for item in items)
        summaries[name] = GroupSummary(
            snapshots=len(items),
            symbols=len({item.symbol for item in items}),
            rows=rows,
            ok=sum(1 for item in items if item.status == "ok"),
            thin=sum(1 for item in items if item.status == "thin"),
            warn=sum(1 for item in items if item.status == "warn"),
            error=sum(1 for item in items if item.status == "error"),
            coverage_ratio=_coverage(rows, expected),
        )
    return summaries


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    return None


def _coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return None


def _value_date(value: object | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    return None


def _format_value(value: object | None) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _render_snapshot_row(bronze_root: Path, item: SnapshotHealth) -> str:
    rel_path = _safe_relative(item.path, bronze_root)
    status_class = "" if item.status == "ok" else f" {item.status}"
    stale = "" if item.stale_days is None else str(item.stale_days)
    return f"""<tr data-asset="{_a(item.asset_class)}" data-symbol="{_a(item.symbol)}" data-timeframe="{_a(item.timeframe)}" data-status="{_a(item.status)}" data-reason="{_a(item.reason)}" data-rows="{item.rows}" data-expected="{item.expected_rows}" data-coverage="{item.coverage_ratio:.8f}" data-missing="{item.missing_estimate}" data-first="{_a(item.first)}" data-latest="{_a(item.latest)}" data-stale="{stale or 0}" data-size="{item.size_bytes}" data-path="{_a(rel_path)}">
  <td>{_e(item.asset_class)}</td>
  <td><strong>{_e(item.symbol)}</strong></td>
  <td>{_e(item.timeframe)}</td>
  <td><span class="badge{status_class}">{_e(item.status.upper())}</span></td>
  <td>{_e(item.reason)}</td>
  <td class="num">{item.rows:,}</td>
  <td class="num">{item.expected_rows:,}</td>
  <td class="num">{_pct(item.coverage_ratio)}<div class="coverage-bar"><div class="coverage-fill" style="width: {_pct(item.coverage_ratio)}"></div></div></td>
  <td class="num">{item.missing_estimate:,}</td>
  <td>{_e(item.first)}</td>
  <td>{_e(item.latest)}</td>
  <td class="num">{_e(stale)}</td>
  <td class="num">{_format_bytes(item.size_bytes)}</td>
  <td><code>{_e(rel_path)}</code></td>
</tr>"""


def _render_asset_summary_rows(groups: dict[str, GroupSummary]) -> str:
    return "\n".join(
        f"""<tr>
  <td>{_e(asset)}</td>
  <td class="num">{summary.symbols:,}</td>
  <td class="num">{summary.snapshots:,}</td>
  <td class="num">{summary.rows:,}</td>
  <td class="num">{_pct(summary.coverage_ratio)}</td>
  <td class="num status-ok">{summary.ok:,}</td>
  <td class="num status-thin">{summary.thin:,}</td>
  <td class="num status-warn">{summary.warn:,}</td>
  <td class="num status-error">{summary.error:,}</td>
</tr>"""
        for asset, summary in groups.items()
    )


def _render_ticker_summary_sections(snapshots: list[SnapshotHealth]) -> str:
    grouped: dict[tuple[str, str], list[SnapshotHealth]] = {}
    for item in snapshots:
        grouped.setdefault((item.asset_class, item.symbol), []).append(item)

    by_asset: dict[str, list[str]] = {}
    asset_counts: dict[str, int] = {}
    asset_rows: dict[str, int] = {}
    for (asset_class, symbol), items in sorted(grouped.items()):
        status = _worst_status(item.status for item in items)
        status_class = "" if status == "ok" else f" {status}"
        total_rows = sum(item.rows for item in items)
        expected = sum(item.expected_rows for item in items)
        latest = max((item.latest for item in items if item.latest), default="")
        timeframes = ", ".join(sorted((item.timeframe for item in items), key=_timeframe_sort_key))
        reason = _ticker_reason(items, status)
        asset_counts[asset_class] = asset_counts.get(asset_class, 0) + 1
        asset_rows[asset_class] = asset_rows.get(asset_class, 0) + total_rows
        by_asset.setdefault(asset_class, []).append(
            f"""<tr>
  <td><strong>{_e(symbol)}</strong></td>
  <td><span class="badge{status_class}">{_e(status.upper())}</span></td>
  <td>{_e(timeframes)}</td>
  <td class="num">{len(items):,}</td>
  <td class="num">{total_rows:,}</td>
  <td class="num">{_pct(_coverage(total_rows, expected))}</td>
  <td>{_e(latest)}</td>
  <td>{_e(reason)}</td>
</tr>"""
        )

    sections: list[str] = []
    for asset_class, rows in sorted(by_asset.items()):
        sections.append(
            f"""<details class="asset-ticker-group" data-asset-group="{_a(asset_class)}">
  <summary>{_e(asset_class)} · {asset_counts[asset_class]:,} tickers · {asset_rows[asset_class]:,} rows</summary>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Symbol</th>
          <th>Worst Status</th>
          <th>Timeframes</th>
          <th class="num">Snapshots</th>
          <th class="num">Rows</th>
          <th class="num">Density</th>
          <th>Latest</th>
          <th>Reason</th>
        </tr>
      </thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
  </div>
</details>"""
        )
    return "\n".join(sections) or _empty_card("No tickers found")


def _worst_status(statuses: Iterable[str]) -> str:
    rank = {"error": 0, "warn": 1, "thin": 2, "ok": 3}
    return min(statuses, key=lambda status: rank.get(status, 99), default="ok")


def _ticker_reason(items: list[SnapshotHealth], status: str) -> str:
    if status == "ok":
        return "All grouped snapshots are fresh enough and dense enough."
    matching = [item for item in items if item.status == status]
    if status == "thin":
        return f"{len(matching)} thin snapshot(s); low-density trade bars are visible but not repair actions."
    reasons = sorted({item.reason for item in matching})
    return reasons[0] if reasons else "See per-file details."


def _timeframe_sort_key(timeframe: str) -> tuple[int, str]:
    order = {"1d": 0, "1m": 1, "5m": 2, "1h": 3}
    return order.get(timeframe, 99), timeframe


def _render_group_card(name: str, summary: GroupSummary) -> str:
    return f"""<article class="card">
  <div class="label">{_e(name)}</div>
  <div class="value">{_pct(summary.coverage_ratio)}</div>
  <div>{summary.symbols:,} symbols · {summary.snapshots:,} snapshots</div>
  <div>{summary.rows:,} rows</div>
  <div class="status-ok">{summary.ok:,} ok</div>
  <div class="status-thin">{summary.thin:,} thin</div>
  <div class="status-warn">{summary.warn:,} warn</div>
  <div class="status-error">{summary.error:,} error</div>
</article>"""


def _summary_card(label: str, value: str, css_class: str = "") -> str:
    klass = f" {css_class}" if css_class else ""
    return f"""<article class="card">
  <div class="label">{_e(label)}</div>
  <div class="value{klass}">{_e(value)}</div>
</article>"""


def _empty_card(message: str) -> str:
    return f"""<article class="card"><div class="label">{_e(message)}</div></article>"""


def _pct(value: float) -> str:
    return f"{value:.2%}"


def _format_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{value} B"


def _safe_relative(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _e(value: object) -> str:
    return html.escape(str(value), quote=False)


def _a(value: object) -> str:
    return html.escape(str(value), quote=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

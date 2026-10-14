#!/usr/bin/env python3
"""IB MIDPOINT 1-minute bars for the IB spot universe (`cmdty` + `fx_ib` presets).

One writer, two IB sources:

* live IB requests — the nightly path, run by the ib-evening job after the 1d lanes;
* signal-lab's IB 1m archive (``--archive``) — a one-time history import. Same
  request shape (reqHistoricalData, 1 min, MIDPOINT, useRTH=False, UTC bar start);
  every file is sha256-checked against the archive's MANIFEST.csv before it is read,
  and ``.partial`` years are refused.

Bars are stored at UTC bar start in ``<asset_class>/symbol=<S>/1m.parquet``. Only
bars of completed IB sessions are stored: the FX/metals day closes 17:00 New York,
so nothing at or after the most recent 17:00 NY instant is written — a session
still trading is never stored (pm:2026-10-01-seed-stored-the-session-still-trading).
Existing bars are never overwritten; a completed session does not change.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import socket
import sys
import time as clock
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:  # pragma: no cover
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients import constants, ledger
from clients.ingestion_common import FX_ASSET_CLASSES, is_inverted_fx_pair, load_preset, make_contract
from clients.intraday_bronze_client import IntradayBronzeClient
from livewire_scripts.paths import data_lake_dir

#: The IB midpoint 1d presets; the 1m universe is the same symbols.
PRESETS = {"cmdty": _PROJECT_ROOT / "presets" / "cmdty-metals.json", "fx_ib": _PROJECT_ROOT / "presets" / "fx-ib.json"}
_NY = ZoneInfo("America/New_York")
SESSION_CLOSE_NY = time(17, 0)
#: 1m MIDPOINT: a 2-week request returned 13,884 EURUSD bars; 1 month timed out
#: (mini, 2026-10-14).
CHUNK = timedelta(days=14)
#: A symbol with no 1m file is seeded this far back; deeper history comes from the
#: archive or an explicit --since.
SEED = timedelta(days=14)
#: A hole longer than this between consecutive bars is reported (a weekend is ~2 days).
HOLE = timedelta(days=6)
REQUEST_INTERVAL_S = constants.declared("ib_request_interval_s/1m")


def universe(symbols: list[str] | None = None) -> dict[str, str]:
    """``{symbol: asset_class}`` for the IB midpoint presets, optionally narrowed."""
    owned = {ticker: asset_class for asset_class, path in PRESETS.items() for ticker in load_preset(path)[1]}
    if symbols is None:
        return owned
    unknown = sorted(set(symbols) - set(owned))
    if unknown:
        raise ValueError(f"not in the IB midpoint presets: {unknown}")
    return {symbol: owned[symbol] for symbol in symbols}


def session_cutoff(now: datetime) -> datetime:
    """The most recent 17:00 New York instant at or before *now*, in UTC."""
    local = now.astimezone(_NY)
    day = local.date() if local.time() >= SESSION_CLOSE_NY else local.date() - timedelta(days=1)
    return datetime.combine(day, SESSION_CLOSE_NY, tzinfo=_NY).astimezone(UTC)


def session_of(ts: datetime) -> date:
    """The IB FX/metals session a bar belongs to: 17:15 NY D-1 -> 17:00 NY D is day D."""
    return (ts.astimezone(_NY) + timedelta(hours=7)).date()


def validate(table: pa.Table, symbol: str) -> None:
    """Fail closed on unsorted/duplicate timestamps or an impossible OHLC row."""
    ts = table.column("bar_timestamp")
    if table.num_rows > 1:
        prev, cur = pc.cast(ts.slice(0, table.num_rows - 1), pa.int64()), pc.cast(ts.slice(1), pa.int64())
        if not pc.all(pc.greater(cur, prev)).as_py():
            raise ValueError(f"{symbol}: bar timestamps are not strictly increasing")
    low, high = table.column("low"), table.column("high")
    for col in ("open", "close"):
        bad = pc.or_(pc.less(table.column(col), low), pc.greater(table.column(col), high))
        if pc.any(bad).as_py():
            raise ValueError(f"{symbol}: {col} outside [low, high]")


def holes(ts: list[datetime]) -> list[tuple[datetime, datetime]]:
    """Gaps longer than HOLE between consecutive bar times."""
    return [(a, b) for a, b in zip(ts, ts[1:], strict=False) if b - a > HOLE]


def _table(times: list[datetime], o, h, lo, c, symbol_id: int, invert: bool) -> pa.Table:
    if invert:
        o, h, lo, c = [1 / x for x in o], [1 / x for x in lo], [1 / x for x in h], [1 / x for x in c]
    return pa.table(
        {
            "bar_timestamp": pa.array(times, type=pa.timestamp("us", tz="UTC")),
            "symbol_id": pa.array([symbol_id] * len(times), type=pa.int64()),
            "open": pa.array(o, type=pa.float64()),
            "high": pa.array(h, type=pa.float64()),
            "low": pa.array(lo, type=pa.float64()),
            "close": pa.array(c, type=pa.float64()),
            # MIDPOINT has no volume; IB sends -1/0. Same convention as bars_to_midpoint_rows.
            "volume": pa.array([0] * len(times), type=pa.int64()),
        }
    )


def _write(client: IntradayBronzeClient, symbol: str, table: pa.Table, cutoff: datetime) -> int:
    table = table.filter(pc.less(table.column("bar_timestamp"), pa.scalar(cutoff, type=pa.timestamp("us", tz="UTC"))))
    if table.num_rows == 0:
        return 0
    validate(table, symbol)
    return client.merge_ticker_rows(symbol, table.to_pylist(), overwrite_existing=False)


def _invert(symbol: str, asset_class: str) -> bool:
    return asset_class in FX_ASSET_CLASSES and is_inverted_fx_pair(symbol)


def read_manifest(archive: Path) -> list[dict[str, str]]:
    with (archive / "MANIFEST.csv").open(newline="") as handle:
        return list(csv.DictReader(handle))


def verified_files(archive: Path, symbol: str) -> tuple[list[Path], list[str]]:
    """Complete-year files for *symbol* whose sha256 matches the manifest, oldest first.

    Returns ``(files, skipped_partial)``. A hash mismatch or a missing file raises:
    the archive is the evidence, so a file that is not what was recorded is not read.
    """
    files, partial = [], []
    for row in sorted((r for r in read_manifest(archive) if r["symbol"] == symbol), key=lambda r: r["file"]):
        if row["partial"].strip().lower() == "true":
            partial.append(row["file"])
            continue
        path = archive / symbol / row["file"]
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != row["sha256"]:
            raise ValueError(f"{path}: sha256 {digest} != manifest {row['sha256']}")
        files.append(path)
    return files, partial


def import_archive(archive: Path, symbol: str, asset_class: str, bronze_root: Path, cutoff: datetime) -> dict:
    files, partial = verified_files(archive, symbol)
    client = IntradayBronzeClient(bronze_dir=bronze_root / f"asset_class={asset_class}", timeframe="1m")
    symbol_id = client.get_symbol_id(symbol)
    added = 0
    for path in files:
        raw = pq.read_table(path, columns=["time", "open", "high", "low", "close"])
        table = _table(
            raw.column("time").to_pylist(),
            *(raw.column(c).to_pylist() for c in ("open", "high", "low", "close")),
            symbol_id,
            _invert(symbol, asset_class),
        )
        added += _write(client, symbol, table, cutoff)
    # Across files too: a missing year sits between two complete ones.
    path = client.bronze_dir / f"symbol={symbol}" / "1m.parquet"
    stored = pq.read_table(path, columns=["bar_timestamp"]).column("bar_timestamp").to_pylist() if path.exists() else []
    return {"added": added, "files": len(files), "partial": partial, "holes": holes(stored)}


def fetch_ib(ib, symbol: str, asset_class: str, start: datetime, cutoff: datetime) -> list:
    """IB 1m MIDPOINT bars in [start, cutoff), walked back from cutoff in CHUNK steps."""
    contract = make_contract(symbol, asset_class)
    ib.qualify_contract(contract)
    bars, end = [], cutoff
    while end > start:
        if end < cutoff:
            clock.sleep(REQUEST_INTERVAL_S)
        chunk = ib.get_historical_data(
            contract, duration=f"{CHUNK.days} D", bar_size="1 min", what_to_show="MIDPOINT",
            use_rth=False, end_date=end,
        )  # fmt: skip
        for b in chunk or []:
            # A naive bar would be read in the host's zone (HKT on the mini): refuse it.
            if not isinstance(b.date, datetime) or b.date.tzinfo is None:
                raise ValueError(f"{symbol}: IB returned a naive bar time {b.date!r}")
        bars += [b for b in chunk or [] if start <= b.date.astimezone(UTC) < cutoff]
        end -= CHUNK
    return sorted({b.date.astimezone(UTC): b for b in bars}.values(), key=lambda b: b.date.astimezone(UTC))


def sync_ib(ib, symbol: str, asset_class: str, bronze_root: Path, cutoff: datetime, since: datetime | None) -> dict:
    client = IntradayBronzeClient(bronze_dir=bronze_root / f"asset_class={asset_class}", timeframe="1m")
    latest = client.get_latest_timestamps().get(symbol)
    start = since or (latest + timedelta(minutes=1) if latest else cutoff - SEED)
    bars = fetch_ib(ib, symbol, asset_class, start, cutoff)
    table = _table(
        [b.date.astimezone(UTC) for b in bars],
        [b.open for b in bars], [b.high for b in bars], [b.low for b in bars], [b.close for b in bars],
        client.get_symbol_id(symbol),
        _invert(symbol, asset_class),
    )  # fmt: skip
    # The seam with what was already stored counts: a missed week sits between the two.
    seam = ([latest] if latest else []) + table.column("bar_timestamp").to_pylist()
    return {"added": _write(client, symbol, table, cutoff), "fetched": len(bars), "holes": holes(seam)}


def _last_session(bronze_root: Path, symbol: str, asset_class: str) -> date | None:
    path = bronze_root / f"asset_class={asset_class}" / f"symbol={symbol}" / "1m.parquet"
    if not path.exists():
        return None
    latest = pc.max(pq.read_table(path, columns=["bar_timestamp"]).column("bar_timestamp")).as_py()
    return session_of(latest) if latest else None


def _measure(name: str, scope: str, value: float, unit: str, run_id: str) -> dict:
    return {
        "name": name,
        "scope": scope,
        "measured_at": datetime.now(UTC),
        "value": float(value),
        "unit": unit,
        "source": "measured",
        "run_id": run_id,
    }


def main(argv: list[str] | None = None, *, ib_factory=None, now: datetime | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", type=Path, help="Import signal-lab's IB 1m archive instead of calling IB")
    parser.add_argument("--symbols", nargs="+", help="Limit to these symbols (default: both presets)")
    parser.add_argument("--since", type=date.fromisoformat, help="IB mode: fetch from this UTC date (history seed)")
    parser.add_argument("--host", default=os.getenv("MDW_IB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MDW_IB_PORT", "4001")))
    args = parser.parse_args(argv)

    targets = universe(args.symbols)
    bronze_root = data_lake_dir() / "bronze"
    cutoff = session_cutoff(now or datetime.now(UTC))
    since = datetime.combine(args.since, time(0), tzinfo=UTC) if args.since else None

    inherited = os.environ.get("LW_RUN_ID")
    run_id = inherited or ledger.new_run_id("ib-1m")
    run_row = {
        "run_id": run_id,
        "job": "ib-1m",
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": datetime.now(UTC),
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    if not inherited:
        ledger.open_run(run_row)

    rows, failures, ib = [], 0, None
    try:
        if args.archive is None:
            from clients.ib_client import IBClient

            ib = (ib_factory or IBClient)()
            ib.connect(host=args.host, port=args.port)
        for symbol, asset_class in targets.items():
            try:
                if args.archive is not None:
                    result = import_archive(args.archive, symbol, asset_class, bronze_root, cutoff)
                else:
                    result = sync_ib(ib, symbol, asset_class, bronze_root, cutoff, since)
            except Exception as exc:  # one symbol never costs the others
                failures += 1
                print(f"{symbol}: FAILED {type(exc).__name__}: {exc}")
                rows.append(_measure("ib_1m_failed", symbol, 1, "count", run_id))
                continue
            for a, b in result["holes"]:
                print(f"{symbol}: hole {a:%Y-%m-%d %H:%M} -> {b:%Y-%m-%d %H:%M}Z ({(b - a).days}d)")
            print(
                f"{symbol}: added={result['added']} "
                + " ".join(f"{k}={v}" for k, v in result.items() if k in ("files", "fetched", "partial"))
            )
            rows.append(_measure("ib_1m_bars_added", symbol, result["added"], "bars", run_id))
            rows.append(_measure("ib_1m_holes", symbol, len(result["holes"]), "count", run_id))
        sessions = [_last_session(bronze_root, s, a) for s, a in targets.items()]
        if all(sessions):
            # The laggard decides: status grades `last_session` scope ib_1m like the IB-only lanes.
            rows.append(
                _measure("last_session", "ib_1m", (min(sessions) - date(1970, 1, 1)).days, "epoch_days", run_id)
            )
    except BaseException:
        if not inherited:
            ledger.emit(
                "runs", [run_row | {"ended": datetime.now(UTC), "exit_code": 1, "verdict": "FAILED"}], run_id=run_id
            )
        raise
    finally:
        if ib is not None:
            ib.disconnect()
    if rows:
        ledger.emit("measurements", rows, run_id=run_id)
    code = 1 if failures else 0
    if not inherited:
        verdict = "FAILED" if code else "OK"
        ledger.emit(
            "runs", [run_row | {"ended": datetime.now(UTC), "exit_code": code, "verdict": verdict}], run_id=run_id
        )
    return code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

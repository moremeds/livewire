#!/usr/bin/env python3
"""Capture index option chains from Futu OpenD into bronze, append-only.

Layout: bronze/asset_class=option/underlying=<SPX|VIX|NDX|RUT>/date=<YYYY-MM-DD>/
    contracts.parquet     the day's discovered chain: the expected set for every capture
    snap_<HHMM>.parquet   one capture per New York slot, written once, never rewritten

Cadence (plan docs/plans/2026-10-16-futu-market-data-uplift-plan.md, W1): SPX and VIX
every 10 minutes from 09:40 to 10 minutes after the close, NDX and RUT on the hour and
half hour, and all four at the official slot 15 minutes before the close (15:45 on a
full day). launchd fires the job every 10 minutes and at :45; the job decides from the
New York clock what, if anything, is due. A tick with nothing due writes no run row. A
tick that lands after the official slot and before the close takes any official capture
still missing (a late or skipped :45 wake). The 15:30 tick re-discovers the chain, so
strikes listed during the day are in the official capture's expected set.

An existing snapshot is never rewritten; a rerun grades it again instead.

Futu's IV and greeks are stored as given (0.0 means "not modelled", not zero) and are
advisory: a consumer recomputes IV from the bid/ask mid. Open interest is the previous
session's OCC figure, so `oi_asof` is the previous trading day.

Exit 0 when every due underlying captured at least `futu_min_capture_ratio/option` of its
expected contracts, 1 otherwise, 86 when OpenD is unreachable or lacks the US option
quote right (a skipped run, like a down IB Gateway).
"""

from __future__ import annotations

import argparse
import os
import re
import signal
import socket
import sys
import time as clock
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from clients import constants, ledger
from clients.futu_client import FutuUnavailable, QuoteSession, us_time
from clients.parquet_io import path_lock, publish_parquet
from clients.trading_calendar import is_trading_day, previous_trading_day, session_close_time
from livewire_scripts.paths import data_lake_dir

NEW_YORK = ZoneInfo("America/New_York")
UNDERLYINGS = {"SPX": "US..SPX", "VIX": "US..VIX", "NDX": "US..NDX", "RUT": "US..RUT"}
EVERY_10_MIN = ("SPX", "VIX")
EVERY_30_MIN = ("NDX", "RUT")
FIRST_SLOT = time(9, 40)
#: US indices have no Futu quote, so each capture carries its ETF's quote. VIX has no ETF that
#: tracks the index; its forward needs VIX futures, which this feed does not carry.
PROXIES = {"SPX": "US.SPY", "NDX": "US.QQQ", "RUT": "US.IWM", "VIX": None}
EXIT_UNAVAILABLE = 86
#: get_option_chain spans at most 30 days per call.
CHAIN_SPAN = timedelta(days=29)
RIGHTS_OK = {"LV1", "LV2", "LV3"}
_CODE = re.compile(r"^US\.([A-Z]+)\d{6}[CP]\d+$")
_TS = pa.timestamp("us", tz="UTC")

CONTRACT_SCHEMA = pa.schema(
    [
        ("code", pa.string()),
        ("root", pa.string()),
        ("expiry", pa.date32()),
        ("strike", pa.float64()),
        ("right", pa.string()),
        ("settlement", pa.string()),
        ("expiration_cycle", pa.string()),
        ("lot_size", pa.int64()),
        ("discovered_at", _TS),
    ]
)

SNAPSHOT_SCHEMA = pa.schema(
    [
        ("code", pa.string()),
        ("root", pa.string()),
        ("expiry", pa.date32()),
        ("strike", pa.float64()),
        ("right", pa.string()),
        ("settlement", pa.string()),
        ("capture_ts_utc", _TS),
        # OpenD rejected this code by name: a row with no quote, so the file says what was asked.
        ("refused", pa.bool_()),
        ("vendor_update_ts_utc", _TS),
        ("bid", pa.float64()),
        ("ask", pa.float64()),
        ("bid_size", pa.int64()),
        ("ask_size", pa.int64()),
        ("last", pa.float64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("prev_close", pa.float64()),
        ("volume", pa.int64()),
        ("turnover", pa.float64()),
        ("open_interest", pa.int64()),
        ("oi_asof", pa.date32()),
        ("iv_vendor_pct", pa.float64()),
        ("delta", pa.float64()),
        ("gamma", pa.float64()),
        ("vega", pa.float64()),
        ("theta", pa.float64()),
        ("rho", pa.float64()),
        ("contract_multiplier", pa.float64()),
        ("sec_status", pa.string()),
        ("proxy_code", pa.string()),
        ("proxy_bid", pa.float64()),
        ("proxy_ask", pa.float64()),
        ("proxy_last", pa.float64()),
        ("proxy_ts_utc", _TS),
    ]
)


class RunBudgetExceeded(BaseException):
    """A hung OpenD call never raises on its own; this closes the run FAILED instead."""


def _expire(signum, frame):
    raise RunBudgetExceeded(f"futu-options outlived its {constants.declared('run_budget_s/futu-options'):.0f}s budget")


def slot_of(now: datetime) -> datetime:
    """The New York capture slot `now` belongs to: wall clock floored to 5 minutes."""
    local = now.astimezone(NEW_YORK).replace(second=0, microsecond=0)
    return local - timedelta(minutes=local.minute % 5)


def official_slot(day: date) -> time:
    close = datetime.combine(day, session_close_time(day))
    return (close - timedelta(minutes=15)).time()


def due(slot: datetime) -> list[str]:
    """Underlyings to capture at `slot` (New York)."""
    day, at = slot.date(), slot.time()
    if not is_trading_day(day):
        return []
    last = (datetime.combine(day, session_close_time(day)) + timedelta(minutes=10)).time()
    if at < FIRST_SLOT or at > last:
        return []
    if at == official_slot(day):
        return list(UNDERLYINGS)
    if at.minute % 10:
        return []
    return [*EVERY_10_MIN, *(EVERY_30_MIN if at.minute % 30 == 0 else ())]


class PastClose(Exception):
    """An official capture that could only start after `official_deadline`: another market by then."""


def official_deadline(day: date) -> datetime:
    """The last instant an official capture may start: the end of the close minute (16:00:59)."""
    return datetime.combine(day, session_close_time(day), tzinfo=NEW_YORK) + timedelta(minutes=1)


def targets(now: datetime, root: Path) -> list[tuple[str, datetime]]:
    """(underlying, slot) pairs to capture now: what is due, plus any official capture still missing."""
    slot = slot_of(now)
    day = slot.date()
    out = [(u, slot) for u in due(slot)]
    if not is_trading_day(day):
        return out
    official = datetime.combine(day, official_slot(day), tzinfo=NEW_YORK)
    # The real instant, not the floored slot (16:04 floors to 16:00): up to the close minute, so the
    # 16:00 wake still recovers a :45 run that outlived the :50 wake, and 16:04 does not.
    if official < slot and now < official_deadline(day):
        late = [(u, official) for u in UNDERLYINGS if not _snap_path(root, u, official).exists()]
        out = late + out
    return out


def _snap_path(root: Path, underlying: str, slot: datetime) -> Path:
    return _day_dir(root, underlying, slot.date()) / f"snap_{slot:%H%M}.parquet"


def _day_dir(root: Path, underlying: str, day: date) -> Path:
    return root / "asset_class=option" / f"underlying={underlying}" / f"date={day.isoformat()}"


def _root(code: str) -> str | None:
    match = _CODE.match(code)
    return match.group(1) if match else None


def discover(session: QuoteSession, underlying: str, day: date, now: datetime) -> pa.Table:
    """Every listed contract of `underlying`, one get_option_chain call per 30-day window."""
    expiries = session.expiries(UNDERLYINGS[underlying])
    windows: list[tuple[date, date]] = []
    for expiry in expiries:
        if not windows or expiry > windows[-1][1]:
            windows.append((expiry, expiry + CHAIN_SPAN))
    found: dict[str, dict] = {}
    for start, end in windows:
        for row in session.chain(UNDERLYINGS[underlying], start, end):
            found[row["code"]] = row
    rows = [
        {
            "code": code,
            "root": _root(code),
            "expiry": date.fromisoformat(row["strike_time"]),
            "strike": row["strike_price"],
            "right": row["option_type"],
            "settlement": row.get("option_settlement_mode"),
            "expiration_cycle": row.get("expiration_cycle"),
            "lot_size": row.get("lot_size"),
            "discovered_at": now,
        }
        for code, row in sorted(found.items())
        if _trading(date.fromisoformat(row["strike_time"]), row.get("option_settlement_mode"), day)
    ]
    return pa.Table.from_pylist(rows, schema=CONTRACT_SCHEMA)


def _trading(expiry: date, settlement: str | None, day: date) -> bool:
    """Still trading on `day`. An AM-settled option (SPX, NDX, RUT monthlies; VIX) stops the
    session before its expiry date and settles at that morning's open."""
    return expiry > day or (expiry == day and settlement != "AM")


def contracts_for(
    session: QuoteSession, root: Path, underlying: str, day: date, now: datetime, *, refresh: bool = False
) -> tuple[pa.Table, int]:
    """The day's expected set and how many codes this call added to it.

    Discovered at the first tick; `refresh` adds codes listed since (the set only grows, and
    each row keeps the time it was first seen).
    """
    path = _day_dir(root, underlying, day) / "contracts.parquet"
    held = pq.read_table(path, schema=CONTRACT_SCHEMA) if path.exists() else None
    if held is not None and not refresh:
        return held, 0
    try:
        found = discover(session, underlying, day, now)
    except Exception as exc:
        if held is None:
            raise
        print(f"{underlying}: re-discovery failed ({type(exc).__name__}: {exc}); capturing the held set")
        return held, -1
    if held is None and found.num_rows == 0:
        raise ValueError(f"{underlying}: Futu listed no contracts expiring on or after {day}")
    known = set(held.column("code").to_pylist()) if held is not None else set()
    new = found.filter(pa.array([c not in known for c in found.column("code").to_pylist()], pa.bool_()))
    if new.num_rows == 0:
        return held, 0
    table = (pa.concat_tables([held, new]) if held is not None else new).sort_by("code")
    publish_parquet(path, table, "code")
    return table, new.num_rows if held is not None else 0


def snapshot_table(
    contracts: pa.Table, rows: list[dict], refused: list[str], proxy_code: str | None, proxy: dict, day: date
) -> pa.Table:
    identity = {c["code"]: c for c in contracts.to_pylist()}
    oi_asof = previous_trading_day(day)
    keys = ("code", "root", "expiry", "strike", "right", "settlement")
    now = datetime.now(UTC)
    out = [
        {**{k: identity[code][k] for k in keys}, "capture_ts_utc": now, "refused": True}
        for code in sorted(set(refused) & identity.keys())
    ]
    for row in {r["code"]: r for r in rows if r.get("code") in identity}.values():
        c = identity[row["code"]]
        out.append(
            {
                **{k: c[k] for k in keys},
                "capture_ts_utc": row["received_at"],
                "refused": False,
                "vendor_update_ts_utc": us_time(row.get("update_time")),
                "bid": row.get("bid_price"),
                "ask": row.get("ask_price"),
                "bid_size": row.get("bid_vol"),
                "ask_size": row.get("ask_vol"),
                "last": row.get("last_price"),
                "open": row.get("open_price"),
                "high": row.get("high_price"),
                "low": row.get("low_price"),
                "prev_close": row.get("prev_close_price"),
                "volume": row.get("volume"),
                "turnover": row.get("turnover"),
                "open_interest": row.get("option_open_interest"),
                "oi_asof": oi_asof,
                "iv_vendor_pct": row.get("option_implied_volatility"),
                "delta": row.get("option_delta"),
                "gamma": row.get("option_gamma"),
                "vega": row.get("option_vega"),
                "theta": row.get("option_theta"),
                "rho": row.get("option_rho"),
                "contract_multiplier": row.get("option_contract_multiplier"),
                "sec_status": row.get("sec_status"),
                "proxy_code": proxy_code,
                "proxy_bid": proxy.get("bid_price"),
                "proxy_ask": proxy.get("ask_price"),
                "proxy_last": proxy.get("last_price"),
                "proxy_ts_utc": us_time(proxy.get("update_time")),
            }
        )
    out.sort(key=lambda r: r["code"])
    return pa.Table.from_pylist(out, schema=SNAPSHOT_SCHEMA)


def quote_lag_s(table: pa.Table) -> float | None:
    """The freshest quote in the nearest expiry: its batch's receipt time minus its vendor time."""
    rows = [r for r in table.to_pylist() if r["vendor_update_ts_utc"] is not None]
    if not rows:
        return None
    nearest = min(r["expiry"] for r in rows)
    return min(
        (r["capture_ts_utc"] - r["vendor_update_ts_utc"]).total_seconds() for r in rows if r["expiry"] == nearest
    )


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


def capture(session: QuoteSession, root: Path, underlying: str, slot: datetime, run_id: str) -> tuple[bool, list]:
    """One underlying at one slot. Returns (ok, measurements).

    One lock per underlying-day covers check, discovery and publish, so a manual run and a
    scheduled tick cannot both find a slot empty and the second replace the first.
    """
    day = slot.date()
    path = _snap_path(root, underlying, slot)
    with path_lock(_day_dir(root, underlying, day) / ".capture.lock"):
        started = clock.monotonic()
        refused: list[str] = []
        added = 0
        if path.exists():
            # Append-only: grade what the slot holds against the set it was captured from.
            table = pq.read_table(path, schema=SNAPSHOT_SCHEMA)
            contracts = pq.read_table(_day_dir(root, underlying, day) / "contracts.parquet", schema=CONTRACT_SCHEMA)
            taken = min(table.column("capture_ts_utc").to_pylist(), default=None)
            contracts = contracts.filter(
                pa.array([taken is None or seen <= taken for seen in contracts.column("discovered_at").to_pylist()])
            )
            print(f"{underlying}: {path.name} already captured; append-only, graded as stored")
        else:
            now = datetime.now(UTC)
            refresh = slot.time() == (datetime.combine(day, official_slot(day)) - timedelta(minutes=15)).time()
            contracts, added = contracts_for(session, root, underlying, day, now, refresh=refresh)
            # Checked here, after the lock wait and discovery, right before the first quote request.
            if slot.time() == official_slot(day) and datetime.now(UTC) >= official_deadline(day):
                raise PastClose(f"official {slot:%H%M} not taken, past the close")
            rows, refused = session.snapshot(contracts.column("code").to_pylist())
            proxy_code = PROXIES[underlying]
            proxy_rows = session.snapshot([proxy_code])[0] if proxy_code else []
            table = snapshot_table(contracts, rows, refused, proxy_code, proxy_rows[0] if proxy_rows else {}, day)
            if table.num_rows:
                publish_parquet(path, table, "code")
    # A code OpenD refuses by name is one it no longer serves, not a row this run lost: it is
    # printed, measured and graded WARN on the official capture, but not counted as missing.
    refused = [r["code"] for r in table.select(["code", "refused"]).to_pylist() if r["refused"]]
    table = table.filter(pc.invert(table["refused"]))
    expected, captured = contracts.num_rows - len(refused), table.num_rows
    ratio = captured / expected if expected > 0 else 0.0
    iv = table.column("iv_vendor_pct").to_pylist()
    quoted = min(table["bid"].null_count, table["ask"].null_count) < captured
    lag = quote_lag_s(table)
    print(f"{underlying} {slot:%H:%M}: captured={captured}/{expected} refused={len(refused)} added={added} lag_s={lag}")
    for code in refused[:20]:
        print(f"  refused {code}")
    if captured and not quoted:
        print(f"{underlying}: no row carries a bid or an ask; the snapshot field names no longer match")
    measurements = [
        _measure("futu_contracts_expected", underlying, expected, "contracts", run_id),
        _measure("futu_contracts_captured", underlying, captured, "contracts", run_id),
        _measure("futu_contracts_added_intraday", underlying, max(added, 0), "contracts", run_id),
        _measure("futu_refused_codes", underlying, len(refused), "codes", run_id),
        _measure("futu_capture_seconds", underlying, clock.monotonic() - started, "s", run_id),
        _measure("futu_iv_null_share", underlying, sum(1 for v in iv if not v) / max(len(iv), 1), "ratio", run_id),
    ]
    if added < 0:
        measurements.append(_measure("futu_discovery_failed", underlying, 1, "count", run_id))
    if lag is not None:
        measurements.append(_measure("futu_quote_lag_s", underlying, lag, "s", run_id))
    if slot.time() == official_slot(day):
        # Its own names, so a run that also takes a regular slot cannot lend the official one its lag.
        if lag is not None:
            measurements.append(_measure("futu_official_quote_lag_s", underlying, lag, "s", run_id))
        measurements.append(_measure("futu_official_refused_codes", underlying, len(refused), "codes", run_id))
        # A file with no quotes in it is not an official capture, however many rows it has.
        measurements.append(
            _measure("futu_official_capture_ratio", underlying, ratio if quoted else 0.0, "ratio", run_id)
        )
    return ratio >= constants.declared("futu_min_capture_ratio/option") and quoted, measurements


def main(argv: list[str] | None = None, *, session_factory=QuoteSession, now: datetime | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--underlying", nargs="+", choices=sorted(UNDERLYINGS), help="Capture these now, due or not")
    parser.add_argument("--host", default=os.getenv("LW_FUTU_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("LW_FUTU_PORT", "11111")))
    args = parser.parse_args(argv)

    root = data_lake_dir() / "bronze"
    now = now or datetime.now(UTC)
    todo = [(u, slot_of(now)) for u in args.underlying] if args.underlying else targets(now, root)
    if not todo:
        return 0

    run_id = ledger.new_run_id("futu-options")
    run_row = {
        "run_id": run_id,
        "job": "futu-options",
        "host": socket.gethostname(),
        "release_sha": os.environ.get("LW_RELEASE_SHA"),
        "presets_sha": None,
        "registry_sha": None,
        "started": datetime.now(UTC),
        "ended": None,
        "exit_code": None,
        "verdict": None,
    }
    ledger.open_run(run_row)
    rows: list[dict] = []
    failures = 0
    code = 0
    signal.signal(signal.SIGALRM, _expire)
    signal.alarm(int(constants.declared("run_budget_s/futu-options")))
    try:
        with session_factory(args.host, args.port) as session:
            right = session.user_info().get("us_option_qot_right")
            rows.append(_measure("futu_us_option_right_ok", "futu", right in RIGHTS_OK, "bool", run_id))
            if right not in RIGHTS_OK:
                raise FutuUnavailable(f"US option quote right is {right!r}, not LV1+")
            for underlying, slot in todo:
                try:
                    ok, measured = capture(session, root, underlying, slot, run_id)
                except FutuUnavailable:
                    raise
                except PastClose as exc:
                    ok, measured = False, [_measure("futu_official_past_close", underlying, 1, "count", run_id)]
                    print(f"{underlying}: {exc}")
                except Exception as exc:  # one underlying never costs the others
                    ok, measured = False, [_measure("futu_capture_failed", underlying, 1, "count", run_id)]
                    print(f"{underlying}: FAILED {type(exc).__name__}: {exc}")
                rows += measured
                failures += not ok
            rows.append(_measure("futu_rate_limited_calls", "futu-options", session.throttled, "calls", run_id))
        code = 1 if failures else 0
    except FutuUnavailable as exc:
        print(f"futu-options: SKIPPED {exc}")
        code = EXIT_UNAVAILABLE
    except BaseException:
        ledger.emit(
            "runs", [run_row | {"ended": datetime.now(UTC), "exit_code": 1, "verdict": "FAILED"}], run_id=run_id
        )
        if rows:
            ledger.emit("measurements", rows, run_id=run_id)
        raise
    finally:
        signal.alarm(0)
    if rows:
        ledger.emit("measurements", rows, run_id=run_id)
    verdict = {0: "OK", EXIT_UNAVAILABLE: "DEGRADED"}.get(code, "FAILED")
    ledger.emit("runs", [run_row | {"ended": datetime.now(UTC), "exit_code": code, "verdict": verdict}], run_id=run_id)
    return code


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

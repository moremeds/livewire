#!/usr/bin/env python3
"""One-shot probe to determine IB intraday return type, timezone, and bar grid.

Run before implementing intraday support to lock in formatDate / useRTH choices.
Results are documented in commit message and as constants in intraday_bronze_client.py.

Usage:
    source ~/market-warehouse/.venv/bin/activate
    python scripts/probe_ib_intraday.py
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def main() -> None:
    import ib_async

    ib = ib_async.IB()
    ib.connect("127.0.0.1", 4001, clientId=99)

    contract = ib_async.Stock("AAPL", "SMART", "USD")
    ib.qualifyContracts(contract)

    print("=" * 60)
    print("IB INTRADAY PROBE — AAPL")
    print("=" * 60)

    for bar_size, label in [("5 mins", "5m"), ("1 hour", "1h")]:
        print(f"\n--- {label} bars (formatDate=1, useRTH=True) ---")
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )
        print(f"Got {len(bars)} bars")
        if bars:
            first, last = bars[0], bars[-1]
            print(f"First.date type: {type(first.date).__name__}")
            print(f"First.date repr: {first.date!r}")
            print(f"First.date tzinfo: {getattr(first.date, 'tzinfo', 'N/A')}")
            print(f"Last.date repr:  {last.date!r}")

        print(f"\n--- {label} bars (formatDate=2, useRTH=True) ---")
        bars2 = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting=bar_size,
            whatToShow="TRADES",
            useRTH=True,
            formatDate=2,
        )
        print(f"Got {len(bars2)} bars")
        if bars2:
            first, last = bars2[0], bars2[-1]
            print(f"First.date type: {type(first.date).__name__}")
            print(f"First.date repr: {first.date!r}")
            print(f"First.date tzinfo: {getattr(first.date, 'tzinfo', 'N/A')}")
            print(f"Last.date repr:  {last.date!r}")

    # DST transition probe
    print("\n--- DST spring forward (2026-03-08, 5m bars) ---")
    bars_dst = ib.reqHistoricalData(
        contract,
        endDateTime="20260309 21:00:00",
        durationStr="2 D",
        barSizeSetting="5 mins",
        whatToShow="TRADES",
        useRTH=True,
        formatDate=2,
    )
    print(f"Got {len(bars_dst)} bars across DST boundary")
    if bars_dst:
        for b in bars_dst[:3]:
            print(f"  {b.date!r}")
        print("  ...")
        for b in bars_dst[-3:]:
            print(f"  {b.date!r}")

    ib.disconnect()
    print("\n" + "=" * 60)
    print("PROBE COMPLETE — capture this output in commit message")
    print("=" * 60)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Manually verify the Livewire Silver writer contract against Apex."""

from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
from pathlib import Path
from textwrap import dedent

PRODUCER = dedent(
    """
    import sys
    from datetime import UTC, date, datetime
    from decimal import Decimal
    from pathlib import Path

    import pyarrow as pa
    import pyarrow.parquet as pq

    from clients.adjustment_engine import FactorInterval
    from clients.silver_client import SilverClient
    from clients.silver_revision import AffectedSymbol, SilverRevisionPublisher

    root = Path(sys.argv[1])
    bronze = root / "bronze/asset_class=equity/symbol=AAPL/1m.parquet"
    bronze.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [{
                "bar_timestamp": datetime(2026, 9, 4, 14, 30, tzinfo=UTC),
                "open": 10.0, "high": 12.0, "low": 8.0, "close": 10.0,
                "volume": 100,
            }]
        ),
        bronze,
    )

    silver_root = root / "silver"
    client = SilverClient(silver_root, generation_id="apex-interop")
    daily = client.publish_daily("AAPL", [{
        "trade_date": date(2026, 9, 4), "symbol_id": 1,
        "open": 5.0, "high": 6.0, "low": 4.0, "close": 5.0,
        "adj_close": 5.0, "volume": 200,
        "price_adjustment_factor": 0.5, "split_volume_factor": 2.0,
        "adjustment_revision": 1,
    }])
    factors = client.publish_factors("AAPL", [FactorInterval(
        date(2026, 9, 4), date(2026, 9, 4), Decimal("0.5"), Decimal("2"), 1,
    )])
    SilverRevisionPublisher(silver_root).publish(
        [daily, factors],
        [AffectedSymbol("AAPL", date(2026, 9, 4), ("1d", "1m"))],
        datetime(2026, 9, 8, tzinfo=UTC),
        published_at=datetime(2026, 9, 8, 2, 40, tzinfo=UTC),
        generation_id="apex-interop",
    )
    """
)

CONSUMER = dedent(
    """
    import asyncio
    import sys
    from datetime import UTC, datetime
    from pathlib import Path

    from src.infrastructure.adapters.livewire.ohlc_provider import LivewireOhlcProvider

    async def main():
        root = Path(sys.argv[1])
        provider = LivewireOhlcProvider(
            root / "bronze", root / "silver", "adjusted"
        ).pin_snapshot()
        start = datetime(2026, 9, 4, tzinfo=UTC)
        end = datetime(2026, 9, 4, 23, 59, tzinfo=UTC)
        daily, intraday = await asyncio.gather(
            provider.fetch_bars("AAPL", "1d", start, end),
            provider.fetch_bars("AAPL", "1m", start, end),
        )
        assert provider.snapshot is not None and provider.snapshot.revision == 1
        assert daily[0].bar_start == start
        assert daily[0].close == 5.0 and daily[0].volume == 200
        assert intraday[0].bar_start == datetime(2026, 9, 4, 14, 30, tzinfo=UTC)
        assert intraday[0].close == 5.0 and intraday[0].volume == 200

    asyncio.run(main())
    """
)


def run_payload(root: Path, payload: str, fixture: Path) -> None:
    python = root / ".venv/bin/python"
    if not python.is_file():
        raise SystemExit(f"missing repository Python: {python}")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root)
    subprocess.run(
        [str(python), "-c", payload, str(fixture)],
        cwd=root,
        env=env,
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--livewire-root", type=Path, required=True)
    parser.add_argument("--apex-root", type=Path, required=True)
    args = parser.parse_args()

    livewire_root = args.livewire_root.expanduser().resolve()
    apex_root = args.apex_root.expanduser().resolve()
    with tempfile.TemporaryDirectory(prefix="livewire-apex-contract-") as temp:
        fixture = Path(temp)
        run_payload(livewire_root, PRODUCER, fixture)
        run_payload(apex_root, CONSUMER, fixture)
    print("PASS: Livewire Silver writer -> Apex adjusted daily/intraday reader")


if __name__ == "__main__":
    main()

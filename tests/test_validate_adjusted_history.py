from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.massive_client import MassiveDailyBar
from clients.symbol_paths import encode_symbol
from livewire_scripts.validate_adjusted_history import run

DATES = (date(2024, 1, 2), date(2024, 1, 3))


def _write_history(root: Path, symbol: str = "TEST") -> tuple[Path, Path]:
    bronze_path = root / "bronze/asset_class=equity" / f"symbol={encode_symbol(symbol)}" / "1d.parquet"
    silver_path = root / "silver/asset_class=equity" / f"symbol={encode_symbol(symbol)}" / "1d.parquet"
    bronze_path.parent.mkdir(parents=True)
    silver_path.parent.mkdir(parents=True)
    bronze = [
        {
            "trade_date": trade_date,
            "symbol_id": 1,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "adj_close": close,
            "volume": 100,
            "source": "ib",
            "price_basis": "raw",
        }
        for trade_date, close in zip(DATES, (10.0, 11.0), strict=True)
    ]
    silver = [
        {
            **row,
            "price_adjustment_factor": 1.0,
            "split_volume_factor": 1.0,
            "adjustment_revision": 1,
        }
        for row in bronze
    ]
    for row in silver:
        row.pop("source")
        row.pop("price_basis")
    pq.write_table(pa.Table.from_pylist(bronze), bronze_path)
    pq.write_table(pa.Table.from_pylist(silver), silver_path)
    return bronze_path, silver_path


class _Massive:
    def __init__(self, *, dates=DATES, mutate: Path | None = None):
        self.dates = dates
        self.mutate = mutate
        self.calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def get_daily_bars(self, ticker, start, end, *, adjusted):
        self.calls += 1
        if self.mutate is not None:
            self.mutate.write_bytes(self.mutate.read_bytes() + b"changed")
            self.mutate = None
        closes = {DATES[0]: 10.0, DATES[1]: 11.0}
        return [
            MassiveDailyBar(item, closes[item], closes[item], closes[item], closes[item], 100) for item in self.dates
        ]

    def get_sma(self, ticker, window, start, end):
        return []

    def get_splits(self, ticker):
        return []

    def get_dividends(self, ticker):
        return []


def _args(root: Path, output: Path, *extra: str) -> list[str]:
    return [
        "--tickers",
        "TEST",
        "--data-lake-root",
        str(root),
        "--silver-root",
        str(root / "silver"),
        "--output-dir",
        str(output),
        "--as-of-date",
        "2024-01-03",
        "--no-ib-fallback",
        *extra,
    ]


def test_full_history_pass_writes_reports_without_changing_inputs(tmp_path) -> None:
    root = tmp_path / "lake"
    output = tmp_path / "validation"
    bronze, silver = _write_history(root)
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in (bronze, silver)}
    massive = _Massive()

    result = run(_args(root, output), massive_factory=lambda: massive)

    assert result == 0
    assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in (bronze, silver)} == before
    detail = (output / "symbols/TEST.json").read_text()
    assert '"outcome": "pass"' in detail
    assert '"price_evidence": "cross_provider"' in detail
    assert (output / "manifest.json").is_file()
    assert "PASS" in (output / "summary.md").read_text()
    assert (output / "cache/massive/TEST.json").is_file()
    assert '"rows"' not in detail


def test_partial_provider_history_is_unresolved_not_intersection_pass(tmp_path) -> None:
    root = tmp_path / "lake"
    output = tmp_path / "validation"
    _write_history(root)

    result = run(_args(root, output), massive_factory=lambda: _Massive(dates=(DATES[1],)))

    assert result == 1
    detail = (output / "symbols/TEST.json").read_text()
    assert '"outcome": "unresolved"' in detail
    assert DATES[0].isoformat() in detail


def test_partial_massive_history_uses_fresh_ib_fallback(tmp_path) -> None:
    root = tmp_path / "lake"
    output = tmp_path / "validation"
    _write_history(root)
    args = _args(root, output)
    args.remove("--no-ib-fallback")

    class IB:
        def connect(self, **kwargs):
            self.connected = kwargs

        def disconnect(self):
            self.disconnected = True

    def fetcher_factory(client):
        def fetcher(symbol, start, end):
            return [
                {
                    "trade_date": item,
                    "symbol_id": 1,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "adj_close": close,
                    "volume": 100,
                    "source": "ib",
                    "price_basis": "split_adjusted",
                }
                for item, close in zip(DATES, (10.0, 11.0), strict=True)
            ]

        return fetcher

    result = run(
        args,
        massive_factory=lambda: _Massive(dates=(DATES[1],)),
        ib_factory=IB,
        ib_fetcher_factory=fetcher_factory,
    )

    assert result == 0
    detail = (output / "symbols/TEST.json").read_text()
    assert '"price_evidence": "hybrid"' in detail
    assert '"2024-01-02": "ib"' in detail


def test_ib_connection_failure_is_reported_without_aborting_run(tmp_path) -> None:
    root = tmp_path / "lake"
    output = tmp_path / "validation"
    _write_history(root)
    args = _args(root, output)
    args.remove("--no-ib-fallback")

    class IB:
        def connect(self, **kwargs):
            raise RuntimeError("gateway unavailable")

        def disconnect(self):
            raise AssertionError("never connected")

    result = run(args, massive_factory=lambda: _Massive(dates=(DATES[1],)), ib_factory=IB)

    assert result == 1
    detail = (output / "symbols/TEST.json").read_text()
    assert '"outcome": "unresolved"' in detail
    assert "gateway unavailable" in detail


def test_massive_client_creation_failure_becomes_provider_error(tmp_path) -> None:
    root = tmp_path / "lake"
    output = tmp_path / "validation"
    _write_history(root)

    def unavailable():
        raise RuntimeError("missing Massive credentials")

    result = run(_args(root, output), massive_factory=unavailable)

    assert result == 1
    detail = (output / "symbols/TEST.json").read_text()
    assert '"outcome": "provider-error"' in detail
    assert "missing Massive credentials" in detail


def test_input_mutation_during_provider_call_cannot_pass(tmp_path) -> None:
    root = tmp_path / "lake"
    output = tmp_path / "validation"
    bronze, _ = _write_history(root)

    result = run(_args(root, output), massive_factory=lambda: _Massive(mutate=bronze))

    assert result == 1
    assert '"outcome": "input-changed"' in (output / "symbols/TEST.json").read_text()


def test_resume_reuses_hash_bound_terminal_checkpoint(tmp_path) -> None:
    root = tmp_path / "lake"
    output = tmp_path / "validation"
    _write_history(root)
    massive = _Massive()

    assert run(_args(root, output), massive_factory=lambda: massive) == 0
    assert run(_args(root, output, "--resume"), massive_factory=lambda: massive) == 0

    assert massive.calls == 1


def test_output_inside_canonical_bronze_is_rejected_before_provider_call(tmp_path) -> None:
    root = tmp_path / "lake"
    _write_history(root)
    massive = _Massive()

    with pytest.raises(ValueError, match="outside canonical Bronze and Silver"):
        run(_args(root, root / "bronze/validation"), massive_factory=lambda: massive)

    assert massive.calls == 0

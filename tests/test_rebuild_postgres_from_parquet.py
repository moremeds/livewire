from __future__ import annotations

from pathlib import Path

import pytest


class FakePostgresClient:
    instances: list[FakePostgresClient] = []

    def __init__(self, dsn=None, schema=None):
        self.dsn = dsn
        self.schema = schema
        self.calls = []
        FakePostgresClient.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def ensure_schema(self) -> None:
        self.calls.append(("ensure_schema",))

    def replace_equities_from_parquet(self, bronze_dir, asset_class="equity", venue="SMART"):
        self.calls.append(("replace_equities_from_parquet", Path(bronze_dir), asset_class, venue))
        return {"symbols": 1, "rows": 2}

    def replace_futures_from_parquet(self, bronze_dir):
        self.calls.append(("replace_futures_from_parquet", Path(bronze_dir)))
        return {"rows": 3}

    def replace_equities_intraday_from_parquet(self, bronze_dir, timeframe):
        self.calls.append(("replace_equities_intraday_from_parquet", Path(bronze_dir), timeframe))
        return {"symbols": 1, "rows": 4}

    def replace_telemetry_from_jsonl(self, path):
        self.calls.append(("replace_telemetry_from_jsonl", Path(path)))
        return {"rows": 5, "skipped": 0}

    def replace_quality_flags_from_jsonl(self, path):
        self.calls.append(("replace_quality_flags_from_jsonl", Path(path)))
        return {"rows": 6, "skipped": 1}


def touch_parquet(bronze_dir: Path, symbol: str, filename: str) -> None:
    path = bronze_dir / f"symbol={symbol}" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"placeholder")


def fake_client(monkeypatch: pytest.MonkeyPatch):
    import livewire_scripts.rebuild_postgres_from_parquet as script

    FakePostgresClient.instances.clear()
    monkeypatch.setattr(script, "PostgresClient", FakePostgresClient)
    return script


def test_default_bronze_path_derives_from_asset_class(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = fake_client(monkeypatch)
    data_lake = tmp_path / "data-lake"
    bronze = data_lake / "bronze" / "asset_class=equity"
    touch_parquet(bronze, "AAPL", "1d.parquet")

    monkeypatch.setattr(script, "DATA_LAKE", data_lake)

    assert script.main(["--dsn", "postgresql://example/livewire", "--timeframe", "1d"]) == 0

    assert FakePostgresClient.instances[0].calls == [
        ("ensure_schema",),
        ("replace_equities_from_parquet", bronze, "equity", "SMART"),
    ]


def test_equity_all_calls_daily_and_existing_intraday(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    script = fake_client(monkeypatch)
    touch_parquet(tmp_path, "AAPL", "1d.parquet")
    touch_parquet(tmp_path, "AAPL", "1m.parquet")
    touch_parquet(tmp_path, "AAPL", "1h.parquet")

    assert script.main(["--dsn", "postgresql://example/livewire", "--bronze-dir", str(tmp_path)]) == 0

    calls = FakePostgresClient.instances[0].calls
    assert ("replace_equities_from_parquet", tmp_path, "equity", "SMART") in calls
    assert ("replace_equities_intraday_from_parquet", tmp_path, "1m") in calls
    assert ("replace_equities_intraday_from_parquet", tmp_path, "1h") in calls
    assert ("replace_equities_intraday_from_parquet", tmp_path, "5m") not in calls
    assert "Skipping 5m" in capsys.readouterr().out


def test_equity_all_skips_missing_daily_when_intraday_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    script = fake_client(monkeypatch)
    touch_parquet(tmp_path, "AAPL", "1h.parquet")

    assert script.main(["--dsn", "postgresql://example/livewire", "--bronze-dir", str(tmp_path)]) == 0

    calls = FakePostgresClient.instances[0].calls
    assert ("replace_equities_from_parquet", tmp_path, "equity", "SMART") not in calls
    assert ("replace_equities_intraday_from_parquet", tmp_path, "1h") in calls
    assert "Skipping 1d" in capsys.readouterr().out


def test_volatility_calls_daily_loader_with_cboe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = fake_client(monkeypatch)
    touch_parquet(tmp_path, "VIX", "1d.parquet")

    script.main([
        "--dsn",
        "postgresql://example/livewire",
        "--asset-class",
        "volatility",
        "--bronze-dir",
        str(tmp_path),
    ])

    assert ("replace_equities_from_parquet", tmp_path, "volatility", "CBOE") in FakePostgresClient.instances[0].calls


def test_futures_calls_futures_loader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = fake_client(monkeypatch)
    touch_parquet(tmp_path, "ES_202606", "1d.parquet")

    script.main([
        "--dsn",
        "postgresql://example/livewire",
        "--asset-class",
        "futures",
        "--bronze-dir",
        str(tmp_path),
    ])

    assert ("replace_futures_from_parquet", tmp_path) in FakePostgresClient.instances[0].calls


def test_include_reliability_imports_jsonl_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = fake_client(monkeypatch)
    touch_parquet(tmp_path, "AAPL", "1d.parquet")
    telemetry = tmp_path / "telemetry.jsonl"
    quality = tmp_path / "quality.jsonl"

    script.main([
        "--dsn",
        "postgresql://example/livewire",
        "--bronze-dir",
        str(tmp_path),
        "--timeframe",
        "1d",
        "--include-reliability",
        "--telemetry-path",
        str(telemetry),
        "--quality-audit-path",
        str(quality),
    ])

    assert ("replace_telemetry_from_jsonl", telemetry) in FakePostgresClient.instances[0].calls
    assert ("replace_quality_flags_from_jsonl", quality) in FakePostgresClient.instances[0].calls


def test_missing_bronze_dir_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = fake_client(monkeypatch)

    with pytest.raises(FileNotFoundError, match="bronze directory does not exist"):
        script.main([
            "--dsn",
            "postgresql://example/livewire",
            "--bronze-dir",
            str(tmp_path / "missing"),
        ])


def test_missing_explicit_timeframe_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = fake_client(monkeypatch)
    touch_parquet(tmp_path, "AAPL", "1d.parquet")

    with pytest.raises(FileNotFoundError, match="no 5m parquet snapshots found"):
        script.main([
            "--dsn",
            "postgresql://example/livewire",
            "--bronze-dir",
            str(tmp_path),
            "--timeframe",
            "5m",
        ])


def test_missing_explicit_1m_timeframe_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = fake_client(monkeypatch)
    touch_parquet(tmp_path, "AAPL", "1d.parquet")

    with pytest.raises(FileNotFoundError, match="no 1m parquet snapshots found"):
        script.main([
            "--dsn",
            "postgresql://example/livewire",
            "--bronze-dir",
            str(tmp_path),
            "--timeframe",
            "1m",
        ])


def test_empty_bronze_dir_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = fake_client(monkeypatch)

    with pytest.raises(FileNotFoundError, match="no bronze parquet snapshots found"):
        script.main([
            "--dsn",
            "postgresql://example/livewire",
            "--bronze-dir",
            str(tmp_path),
        ])

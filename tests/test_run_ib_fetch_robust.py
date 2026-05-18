import json

from scripts.run_ib_fetch_robust import (
    _bronze_path_for,
    _is_already_done,
    load_tickers,
    parse_args,
)


def test_parse_args_defaults():
    args = parse_args(["--preset", "presets/sp500.json", "--mode", "seed"])
    assert args.timeout == 300
    assert args.max_attempts == 3
    assert args.cooldown == 60
    assert args.asset_class == "equity"
    assert args.mode == "seed"


def test_parse_args_env_overrides(monkeypatch):
    monkeypatch.setenv("MDW_ORCHESTRATOR_TIMEOUT_SECONDS", "120")
    monkeypatch.setenv("MDW_ORCHESTRATOR_MAX_ATTEMPTS", "5")
    monkeypatch.setenv("MDW_ORCHESTRATOR_COOLDOWN_SECONDS", "30")
    args = parse_args(["--preset", "presets/sp500.json", "--mode", "seed"])
    assert args.timeout == 120
    assert args.max_attempts == 5
    assert args.cooldown == 30


def test_parse_args_invalid_env_uses_defaults(monkeypatch):
    monkeypatch.setenv("MDW_ORCHESTRATOR_TIMEOUT_SECONDS", "not-int")
    args = parse_args(["--preset", "presets/sp500.json", "--mode", "seed"])
    assert args.timeout == 300


def test_load_tickers_from_preset(tmp_path):
    preset = tmp_path / "p.json"
    preset.write_text(json.dumps({"name": "test", "tickers": ["AAPL", "MSFT"]}))
    assert load_tickers(preset_path=preset, explicit=None) == ["AAPL", "MSFT"]


def test_load_tickers_explicit_wins():
    assert load_tickers(preset_path=None, explicit=["HOOD"]) == ["HOOD"]


def test_load_tickers_no_source_returns_empty():
    assert load_tickers(preset_path=None, explicit=None) == []


def test_bronze_path_for_daily_equity():
    assert _bronze_path_for("/tmp/bronze", "equity", "AAPL").as_posix().endswith(
        "/asset_class=equity/symbol=AAPL/1d.parquet"
    )


def test_is_already_done_seed(tmp_path):
    p = tmp_path / "asset_class=equity" / "symbol=AAPL" / "1d.parquet"
    p.parent.mkdir(parents=True)
    p.write_bytes(b"x")
    assert _is_already_done(p, mode="seed") is True
    assert _is_already_done(p, mode="backfill") is False


def test_is_already_done_backfill_missing(tmp_path):
    p = tmp_path / "asset_class=equity" / "symbol=GHOST" / "1d.parquet"
    assert _is_already_done(p, mode="seed") is False
    assert _is_already_done(p, mode="backfill") is True

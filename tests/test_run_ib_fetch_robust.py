import json
import subprocess
from unittest.mock import MagicMock

import pytest

from clients.ib_gateway_preflight import GATEWAY_DOWN_EXIT_CODE
from livewire_scripts.run_ib_fetch_robust import (
    OutcomeCategory,
    TickerOutcome,
    _bronze_path_for,
    _build_worker_cmd,
    _count_rows,
    _is_already_done,
    format_summary,
    load_tickers,
    main,
    parse_args,
    run_one_ticker,
)


def test_parse_args_defaults():
    args = parse_args(["--preset", "presets/sp500.json", "--mode", "seed"])
    assert args.timeout == 300
    assert args.max_attempts == 3
    assert args.cooldown == 60
    assert args.asset_class == "equity"
    assert args.mode == "seed"
    assert args.source == "auto"


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


def test_parse_args_paths_follow_warehouse_override(tmp_path, monkeypatch):
    warehouse = tmp_path / "warehouse"
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))
    monkeypatch.delenv("MDW_DATA_LAKE", raising=False)
    monkeypatch.delenv("MDW_LOG_DIR", raising=False)

    args = parse_args(["--preset", "presets/sp500.json", "--mode", "seed"])

    assert args.bronze_dir == warehouse / "data-lake" / "bronze"
    assert args.log_dir == warehouse / "logs"


def test_load_tickers_from_preset(tmp_path):
    preset = tmp_path / "p.json"
    preset.write_text(json.dumps({"name": "test", "tickers": ["AAPL", "MSFT"]}))
    assert load_tickers(preset_path=preset, explicit=None) == ["AAPL", "MSFT"]


def test_load_tickers_explicit_wins():
    assert load_tickers(preset_path=None, explicit=["HOOD"]) == ["HOOD"]


def test_load_tickers_no_source_returns_empty():
    assert load_tickers(preset_path=None, explicit=None) == []


def test_bronze_path_for_daily_equity():
    assert (
        _bronze_path_for("/tmp/bronze", "equity", "AAPL")
        .as_posix()
        .endswith("/asset_class=equity/symbol=AAPL/1d.parquet")
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


def test_build_worker_cmd_backfill():
    cmd = _build_worker_cmd("AAPL", "backfill", "equity")
    assert "--backfill" in cmd
    assert "--years" not in cmd
    assert cmd[cmd.index("--source") + 1] == "auto"


def test_build_worker_cmd_accepts_forced_source():
    cmd = _build_worker_cmd("AAPL", "backfill", "equity", source="massive")
    assert cmd[cmd.index("--source") + 1] == "massive"


def test_parse_args_include_expired_defaults_off():
    args = parse_args(["--preset", "presets/sp500.json", "--mode", "seed"])
    assert args.include_expired is False


def test_parse_args_include_expired_requires_futures():
    with pytest.raises(SystemExit):
        parse_args(["--tickers", "AAPL", "--mode", "seed", "--include-expired"])


def test_parse_args_include_expired_parses_for_futures():
    args = parse_args(
        [
            "--tickers",
            "CL_202610",
            "--mode",
            "backfill",
            "--asset-class",
            "futures",
            "--include-expired",
        ]
    )
    assert args.include_expired is True
    assert args.asset_class == "futures"


def test_build_worker_cmd_include_expired_off_by_default():
    cmd = _build_worker_cmd("CL_202610", "backfill", "futures")
    assert "--include-expired" not in cmd


def test_build_worker_cmd_include_expired_appends_flag():
    cmd = _build_worker_cmd("CL_202610", "backfill", "futures", include_expired=True)
    assert "--include-expired" in cmd
    assert cmd[cmd.index("--asset-class") + 1] == "futures"


def test_count_rows_reads_parquet(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    parquet = tmp_path / "rows.parquet"
    pq.write_table(pa.table({"x": [1, 2, 3]}), parquet)
    assert _count_rows(parquet) == 3


def test_run_one_ticker_skips_completed_seed(tmp_path, monkeypatch):
    parquet = tmp_path / "asset_class=equity" / "symbol=AAPL" / "1d.parquet"
    parquet.parent.mkdir(parents=True)
    parquet.write_bytes(b"data")

    def fake_run(*a, **kw):
        raise AssertionError("subprocess should not run for skipped ticker")

    monkeypatch.setattr("subprocess.run", fake_run)
    outcome = run_one_ticker(
        ticker="AAPL",
        mode="seed",
        asset_class="equity",
        bronze_dir=tmp_path,
        timeout=10,
        max_attempts=3,
        cooldown=0,
    )
    assert outcome.code == OutcomeCategory.SKIP
    assert outcome.attempts_used == 0


def test_success_first_attempt(tmp_path, monkeypatch):
    parquet = tmp_path / "asset_class=equity" / "symbol=AAPL" / "1d.parquet"
    parquet.parent.mkdir(parents=True)
    counts = [0, 2]

    def fake_run(*a, **kw):
        parquet.write_bytes(b"data")
        return MagicMock(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr(
        "livewire_scripts.run_ib_fetch_robust._count_rows",
        lambda p: counts.pop(0),
    )
    outcome = run_one_ticker(
        ticker="AAPL",
        mode="seed",
        asset_class="equity",
        bronze_dir=tmp_path,
        timeout=10,
        max_attempts=3,
        cooldown=0,
    )
    assert outcome.code == OutcomeCategory.OK
    assert outcome.attempts_used == 1
    assert outcome.note == "rows +2"


def test_timeout_then_success(tmp_path, monkeypatch):
    parquet = tmp_path / "asset_class=equity" / "symbol=MSFT" / "1d.parquet"
    parquet.parent.mkdir(parents=True)
    calls = [0]

    def fake_run(*a, **kw):
        calls[0] += 1
        if calls[0] == 1:
            raise subprocess.TimeoutExpired(cmd=a[0], timeout=kw.get("timeout", 10))
        parquet.write_bytes(b"data")
        return MagicMock(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    outcome = run_one_ticker(
        ticker="MSFT",
        mode="seed",
        asset_class="equity",
        bronze_dir=tmp_path,
        timeout=10,
        max_attempts=3,
        cooldown=0,
    )
    assert outcome.code == OutcomeCategory.OK
    assert outcome.attempts_used == 2


def test_all_attempts_timeout_is_fail(tmp_path, monkeypatch):
    def fake_run(*a, **kw):
        raise subprocess.TimeoutExpired(cmd=a[0], timeout=kw.get("timeout", 10))

    monkeypatch.setattr("subprocess.run", fake_run)
    outcome = run_one_ticker(
        ticker="HOOD",
        mode="seed",
        asset_class="equity",
        bronze_dir=tmp_path,
        timeout=1,
        max_attempts=2,
        cooldown=0,
    )
    assert outcome.code == OutcomeCategory.TIMEOUT
    assert outcome.attempts_used == 2


def test_non_zero_exit_retried(tmp_path, monkeypatch):
    parquet = tmp_path / "asset_class=equity" / "symbol=X" / "1d.parquet"
    parquet.parent.mkdir(parents=True)
    calls = [0]

    def fake_run(*a, **kw):
        calls[0] += 1
        if calls[0] < 3:
            return MagicMock(returncode=1)
        parquet.write_bytes(b"data")
        return MagicMock(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr("time.sleep", lambda s: None)
    outcome = run_one_ticker(
        ticker="X",
        mode="seed",
        asset_class="equity",
        bronze_dir=tmp_path,
        timeout=10,
        max_attempts=3,
        cooldown=0,
    )
    assert outcome.code == OutcomeCategory.OK


def test_gateway_unavailable_is_typed_and_never_retried(tmp_path, monkeypatch):
    calls = 0

    def fake_run(*args, **kwargs):
        nonlocal calls
        calls += 1
        return MagicMock(returncode=GATEWAY_DOWN_EXIT_CODE)

    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr(
        "time.sleep",
        lambda seconds: (_ for _ in ()).throw(AssertionError("gateway state must not cool down or retry")),
    )

    outcome = run_one_ticker(
        ticker="AAPL",
        mode="seed",
        asset_class="equity",
        bronze_dir=tmp_path,
        timeout=10,
        max_attempts=3,
        cooldown=60,
        source="ib",
    )

    assert calls == 1
    assert outcome.code == OutcomeCategory.TEMPORARY_UNAVAILABLE
    assert outcome.attempts_used == 1
    assert outcome.note == "ib-gateway-unavailable"


def test_cooldown_sleeps_between_attempts(tmp_path, monkeypatch):
    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr("time.sleep", fake_sleep)

    def fake_run(*a, **kw):
        return MagicMock(returncode=1)

    monkeypatch.setattr("subprocess.run", fake_run)
    run_one_ticker(
        ticker="X",
        mode="seed",
        asset_class="equity",
        bronze_dir=tmp_path,
        timeout=10,
        max_attempts=3,
        cooldown=30,
    )
    assert sleeps == [30, 30]


def test_backfill_ok_noop_when_no_rows_added(tmp_path, monkeypatch):
    parquet = tmp_path / "asset_class=equity" / "symbol=COIN" / "1d.parquet"
    parquet.parent.mkdir(parents=True)
    parquet.write_bytes(b"existing")

    def fake_run(*a, **kw):
        return MagicMock(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr(
        "livewire_scripts.run_ib_fetch_robust._count_rows",
        lambda p: 100,
    )
    outcome = run_one_ticker(
        ticker="COIN",
        mode="backfill",
        asset_class="equity",
        bronze_dir=tmp_path,
        timeout=10,
        max_attempts=3,
        cooldown=0,
    )
    assert outcome.code == OutcomeCategory.OK_NOOP
    assert outcome.attempts_used == 1


def test_seed_exit_zero_without_bronze_is_fail(tmp_path, monkeypatch):
    monkeypatch.setattr("subprocess.run", lambda *a, **kw: MagicMock(returncode=0))
    outcome = run_one_ticker(
        ticker="MISSING",
        mode="seed",
        asset_class="equity",
        bronze_dir=tmp_path,
        timeout=10,
        max_attempts=1,
        cooldown=0,
    )
    assert outcome.code == OutcomeCategory.FAIL
    assert outcome.note == "exit 0 but no bronze written"


def test_futures_seed_without_history_is_skipped_for_retry(tmp_path, monkeypatch):
    monkeypatch.setattr("subprocess.run", lambda *a, **kw: MagicMock(returncode=0))
    outcome = run_one_ticker(
        ticker="CL_202611",
        mode="seed",
        asset_class="futures",
        bronze_dir=tmp_path,
        timeout=10,
        max_attempts=1,
        cooldown=0,
    )
    assert outcome.code == OutcomeCategory.SKIP
    assert outcome.note == "no futures history yet"


def test_run_one_ticker_forwards_include_expired_to_worker_cmd(tmp_path, monkeypatch):
    parquet = tmp_path / "asset_class=futures" / "symbol=CL_202610" / "1d.parquet"
    parquet.parent.mkdir(parents=True)
    parquet.write_bytes(b"existing")
    captured: dict = {}

    def fake_run(cmd, *a, **kw):
        captured["cmd"] = cmd
        return MagicMock(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr(
        "livewire_scripts.run_ib_fetch_robust._count_rows",
        lambda p: 100,
    )
    outcome = run_one_ticker(
        ticker="CL_202610",
        mode="backfill",
        asset_class="futures",
        bronze_dir=tmp_path,
        timeout=10,
        max_attempts=1,
        cooldown=0,
        include_expired=True,
    )
    assert "--include-expired" in captured["cmd"]
    assert outcome.code == OutcomeCategory.OK_NOOP


def test_main_forwards_include_expired_to_run_one_ticker(tmp_path, monkeypatch):
    captured: dict = {}

    def fake_run_one_ticker(**kwargs):
        captured.update(kwargs)
        return TickerOutcome("CL_202610", OutcomeCategory.SKIP, 0, 0.0, 0, 0)

    monkeypatch.setattr(
        "livewire_scripts.run_ib_fetch_robust.run_one_ticker",
        fake_run_one_ticker,
    )
    rc = main(
        [
            "--tickers",
            "CL_202610",
            "--mode",
            "backfill",
            "--asset-class",
            "futures",
            "--include-expired",
            "--log-dir",
            str(tmp_path),
            "--bronze-dir",
            str(tmp_path / "bronze"),
        ]
    )
    assert rc == 0
    assert captured["include_expired"] is True


def test_summary_line_format():
    outcomes = [
        TickerOutcome("AAPL", OutcomeCategory.OK, 1, 12.0, 0, 6000),
        TickerOutcome("HOOD", OutcomeCategory.FAIL, 3, 900.0, 0, 0),
        TickerOutcome("COIN", OutcomeCategory.OK_NOOP, 1, 8.0, 100, 100),
    ]
    line = format_summary(outcomes, mode="seed", elapsed_minutes=15)
    assert "ok=1" in line
    assert "ok-noop=1" in line
    assert "fail=1" in line
    assert "elapsed=15m" in line


def test_main_returns_2_when_preset_has_no_tickers(tmp_path):
    preset = tmp_path / "empty.json"
    preset.write_text(json.dumps({"tickers": []}))
    rc = main(["--preset", str(preset), "--mode", "seed", "--log-dir", str(tmp_path)])
    assert rc == 2


def test_main_writes_summary_and_returns_fail_status(tmp_path, monkeypatch, capsys):
    outcomes = [
        TickerOutcome("AAPL", OutcomeCategory.OK, 1, 12.0, 0, 2, "rows +2"),
        TickerOutcome("HOOD", OutcomeCategory.FAIL, 3, 90.0, 0, 0, "boom"),
    ]

    def fake_run_one_ticker(**kwargs):
        return outcomes.pop(0)

    monkeypatch.setattr(
        "livewire_scripts.run_ib_fetch_robust.run_one_ticker",
        fake_run_one_ticker,
    )
    rc = main(
        [
            "--tickers",
            "AAPL",
            "HOOD",
            "--mode",
            "seed",
            "--log-dir",
            str(tmp_path),
            "--bronze-dir",
            str(tmp_path / "bronze"),
        ]
    )

    assert rc == 1
    out = capsys.readouterr().out
    assert "[1/2 ok] AAPL" in out
    assert "=== orch done mode=seed ok=1" in out
    summary_logs = list(tmp_path.glob("orch_seed_*/*_summary.log"))
    assert len(summary_logs) == 1
    assert "HOOD" in summary_logs[0].read_text()


def test_main_returns_fail_status_for_timeout(tmp_path, monkeypatch, capsys):
    outcomes = [
        TickerOutcome("HOOD", OutcomeCategory.TIMEOUT, 2, 90.0, 0, 0, ""),
    ]

    def fake_run_one_ticker(**kwargs):
        return outcomes.pop(0)

    monkeypatch.setattr(
        "livewire_scripts.run_ib_fetch_robust.run_one_ticker",
        fake_run_one_ticker,
    )

    rc = main(
        [
            "--tickers",
            "HOOD",
            "--mode",
            "seed",
            "--log-dir",
            str(tmp_path),
            "--bronze-dir",
            str(tmp_path / "bronze"),
        ]
    )

    assert rc == 1
    assert "[1/1 timeout] HOOD" in capsys.readouterr().out


def test_main_returns_gateway_down_when_seed_only_hits_temporary_unavailability(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        "livewire_scripts.run_ib_fetch_robust.run_one_ticker",
        lambda **kwargs: TickerOutcome(
            "CL_202611", OutcomeCategory.TEMPORARY_UNAVAILABLE, 1, 0.1, 0, 0, "ib-gateway-unavailable"
        ),
    )

    rc = main(
        [
            "--tickers",
            "CL_202611",
            "--mode",
            "seed",
            "--asset-class",
            "futures",
            "--log-dir",
            str(tmp_path),
            "--bronze-dir",
            str(tmp_path / "bronze"),
        ]
    )

    assert rc == GATEWAY_DOWN_EXIT_CODE
    assert "temporary-unavailable" in capsys.readouterr().out


def test_main_returns_failure_if_gateway_loss_coincides_with_hard_failure(tmp_path, monkeypatch):
    outcomes = iter(
        [
            TickerOutcome("CL_202611", OutcomeCategory.TEMPORARY_UNAVAILABLE, 1, 0.1, 0, 0, "ib-gateway-unavailable"),
            TickerOutcome("HO_202611", OutcomeCategory.FAIL, 3, 2.0, 0, 0, "invalid contract"),
        ]
    )
    monkeypatch.setattr("livewire_scripts.run_ib_fetch_robust.run_one_ticker", lambda **kwargs: next(outcomes))

    rc = main(
        [
            "--tickers",
            "CL_202611",
            "HO_202611",
            "--mode",
            "seed",
            "--asset-class",
            "futures",
            "--log-dir",
            str(tmp_path),
            "--bronze-dir",
            str(tmp_path / "bronze"),
        ]
    )

    assert rc == 1

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from clients import ib_gateway_preflight
from clients.ib_client import IBConnectionError
from scripts import livewire_ingest, livewire_ops, livewire_quality, livewire_store

REPO_ROOT = Path(__file__).resolve().parents[1]


def _fake_module(calls: list[tuple[str, list[str]]], name: str, *, accepts_argv: bool):
    if accepts_argv:

        def main(argv):
            calls.append((name, list(argv)))
            return 7
    else:

        def main():
            calls.append((name, []))
            return None

    return SimpleNamespace(main=main)


def test_ingest_dispatches_module_commands(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    preflight_calls: list[bool] = []
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=False),
    )
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: preflight_calls.append(True),
    )

    assert livewire_ingest.main(["daily", "--force"]) == 0
    assert calls == [("livewire_scripts.daily_update", [])]
    assert preflight_calls == [True]


def test_ingest_maps_midrun_ib_session_loss_to_typed_gateway_state(monkeypatch) -> None:
    monkeypatch.setattr(ib_gateway_preflight, "assert_gateway_up", lambda: None)

    def failed_module(name):
        def main():
            raise IBConnectionError("session lost")

        return SimpleNamespace(main=main)

    monkeypatch.setattr(livewire_ingest.importlib, "import_module", failed_module)

    assert livewire_ingest.main(["historical", "--tickers", "AAPL", "--source", "ib"]) == (
        ib_gateway_preflight.GATEWAY_DOWN_EXIT_CODE
    )


def test_quality_dispatches_warehouse_report(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_quality.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_quality.main(["warehouse", "--output", "report.html"]) == 7
    assert calls == [("livewire_scripts.warehouse_health_report", ["--output", "report.html"])]


def test_store_dispatches_rebuild_silver(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_store.main(["rebuild-silver", "--tickers", "NVDA"]) == 7
    assert calls == [("livewire_scripts.rebuild_silver", ["--tickers", "NVDA"])]


def test_store_dispatches_shepherd_daily(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    argv = ["plan", "--index", "sp500", "--membership-revision", "1", "--as-of", "2026-08-31"]
    assert livewire_store.main(["shepherd-daily", *argv]) == 7
    assert calls == [("livewire_scripts.shepherd_daily", argv)]


def test_store_dispatches_shepherd_actions(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    argv = ["export", "--symbols", "AAPL", "--as-of", "2026-08-31T01:00:00+00:00"]
    assert livewire_store.main(["shepherd-actions", *argv]) == 7
    assert calls == [("livewire_scripts.shepherd_actions", argv)]


def test_store_dispatches_shepherd_silver(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    argv = ["publish", "--index", "sp500", "--membership-revision", "1", "--as-of", "2026-08-31T23:59:00+00:00"]
    assert livewire_store.main(["shepherd-silver", *argv]) == 7
    assert calls == [("livewire_scripts.shepherd_silver", argv)]


def test_store_dispatches_shepherd_repair(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    argv = ["preflight", "--manifest", "/tmp/repair.json"]
    assert livewire_store.main(["shepherd-repair", *argv]) == 7
    assert calls == [("livewire_scripts.shepherd_repair", argv)]


def test_store_dispatches_price_basis_migration(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_store.main(["migrate-price-basis", "--tickers", "AAPL"]) == 7
    assert calls == [("livewire_scripts.migrate_equity_price_basis", ["--tickers", "AAPL"])]


def test_quality_dispatches_split_basis_audit(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_quality.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_quality.main(["audit-split-basis", "--tickers", "AAPL", "--output", "audit.json"]) == 7
    assert calls == [("livewire_scripts.audit_split_basis", ["--tickers", "AAPL", "--output", "audit.json"])]


def test_quality_dispatches_audit_legacy_basis(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_quality.importlib, "import_module", lambda name: _fake_module(calls, name, accepts_argv=True)
    )
    assert livewire_quality.main(["audit-legacy-basis", "--full", "--output", "x.json"]) == 7
    assert calls == [("livewire_scripts.audit_legacy_basis", ["--full", "--output", "x.json"])]


def test_quality_dispatches_split_basis_resolution(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_quality.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_quality.main(["resolve-split-basis", "--audit-manifest", "audit.json"]) == 7
    assert calls == [("livewire_scripts.resolve_split_basis", ["--audit-manifest", "audit.json"])]


def test_quality_dispatches_daily_basis_calibration(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_quality.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_quality.main(["calibrate-daily-basis", "--tickers", "AAPL"]) == 7
    assert calls == [("livewire_scripts.calibrate_daily_basis", ["--tickers", "AAPL"])]


def test_quality_dispatches_adjusted_history_validation(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_quality.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_quality.main(["validate-adjusted-history", "--tickers", "AAPL"]) == 7
    assert calls == [("livewire_scripts.validate_adjusted_history", ["--tickers", "AAPL"])]


def test_store_dispatches_split_basis_repair(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_store.main(["repair-split-basis", "--manifest", "audit.json"]) == 7
    assert calls == [("livewire_scripts.repair_split_basis", ["--manifest", "audit.json"])]


def test_store_dispatches_repair_legacy_basis(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib, "import_module", lambda name: _fake_module(calls, name, accepts_argv=True)
    )
    assert livewire_store.main(["repair-legacy-basis", "--audit-manifest", "a.json", "--output-dir", "out"]) == 7
    assert calls == [("livewire_scripts.repair_legacy_basis", ["--audit-manifest", "a.json", "--output-dir", "out"])]


def test_store_dispatches_resolve_yahoo_basis(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib, "import_module", lambda name: _fake_module(calls, name, accepts_argv=True)
    )
    assert livewire_store.main(["resolve-yahoo-basis", "--tickers", "AMC", "--output", "m.json"]) == 7
    assert calls == [("livewire_scripts.resolve_yahoo_basis", ["--tickers", "AMC", "--output", "m.json"])]


def test_ingest_daily_massive_bypasses_ib_preflight(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=False),
    )
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: (_ for _ in ()).throw(AssertionError("preflight should not run")),
    )

    assert livewire_ingest.main(["daily", "--source", "massive"]) == 0
    assert calls == [("livewire_scripts.daily_update", [])]


def test_ingest_daily_massive_equals_bypasses_ib_preflight(monkeypatch) -> None:
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: (_ for _ in ()).throw(AssertionError("preflight should not run")),
    )
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module([], name, accepts_argv=False),
    )

    assert livewire_ingest.main(["daily", "--source=massive"]) == 0


def test_removed_intraday_massive_route_keeps_ib_preflight(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    preflight_calls: list[bool] = []
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=False),
    )
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: preflight_calls.append(True),
    )

    assert (
        livewire_ingest.main(
            [
                "intraday-backfill",
                "--source",
                "massive",
                "--timeframe",
                "1m",
                "--asset-class",
                "equity",
                "--tickers",
                "AAPL",
            ]
        )
        == 0
    )
    assert calls == [("livewire_scripts.backfill_intraday", [])]
    assert preflight_calls == [True]


def test_ingest_historical_massive_equity_backfill_bypasses_ib_preflight(
    monkeypatch,
) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=False),
    )
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: (_ for _ in ()).throw(AssertionError("preflight should not run")),
    )

    assert (
        livewire_ingest.main(
            [
                "historical",
                "--source",
                "massive",
                "--backfill",
                "--tickers",
                "AAPL",
            ]
        )
        == 0
    )
    assert calls == [("livewire_scripts.fetch_ib_historical", [])]


def test_ingest_historical_auto_equity_backfill_keeps_preflight_with_massive_key(
    monkeypatch,
) -> None:
    preflight_calls: list[bool] = []
    monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module([], name, accepts_argv=False),
    )
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: preflight_calls.append(True),
    )

    assert livewire_ingest.main(["historical", "--backfill", "--tickers", "AAPL"]) == 0
    assert preflight_calls == [True]


def test_ingest_historical_auto_equity_backfill_keeps_preflight_without_massive_key(
    monkeypatch,
) -> None:
    preflight_calls: list[bool] = []
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module([], name, accepts_argv=False),
    )
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: preflight_calls.append(True),
    )

    assert livewire_ingest.main(["historical", "--backfill", "--tickers", "AAPL"]) == 0
    assert preflight_calls == [True]


def test_ingest_intraday_massive_non_equity_keeps_ib_preflight(monkeypatch) -> None:
    preflight_calls: list[bool] = []
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: preflight_calls.append(True),
    )
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module([], name, accepts_argv=False),
    )

    assert (
        livewire_ingest.main(
            [
                "intraday-backfill",
                "--source",
                "massive",
                "--timeframe",
                "1m",
                "--asset-class",
                "futures",
                "--tickers",
                "ES_202506",
            ]
        )
        == 0
    )
    assert preflight_calls == [True]


def test_ingest_universe_sync_bypasses_ib_preflight(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=False),
    )
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: (_ for _ in ()).throw(AssertionError("preflight should not run")),
    )

    assert livewire_ingest.main(["universe-sync", "--dry-run"]) == 0
    assert calls == [("livewire_scripts.universe_sync", [])]


def test_ingest_dispatches_corporate_actions_without_ib_preflight(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: (_ for _ in ()).throw(AssertionError("preflight should not run")),
    )

    assert livewire_ingest.main(["corporate-actions", "--tickers", "NVDA"]) == 7
    assert calls == [("livewire_scripts.sync_corporate_actions", ["--tickers", "NVDA"])]


def test_ingest_preserves_nonzero_system_exit(monkeypatch) -> None:
    def fake_module(name):
        def main():
            raise SystemExit(3)

        return SimpleNamespace(main=main)

    monkeypatch.setattr(livewire_ingest.importlib, "import_module", fake_module)
    monkeypatch.setattr(ib_gateway_preflight, "assert_gateway_up", lambda: None)

    with pytest.raises(SystemExit) as exc_info:
        livewire_ingest.main(["daily"])
    assert exc_info.value.code == 3


@pytest.mark.parametrize(
    "argv",
    [
        ["daily"],
        ["daily", "--source", "ib"],
        ["intraday-backfill"],
    ],
)
def test_ingest_ib_commands_keep_preflight(monkeypatch, argv) -> None:
    preflight_calls: list[bool] = []
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: preflight_calls.append(True),
    )
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module([], name, accepts_argv=False),
    )

    assert livewire_ingest.main(argv) == 0
    assert preflight_calls == [True]


@pytest.mark.parametrize("argv", [["backfill-all"], ["daily-backfill"]])
def test_ingest_orchestrators_do_not_preflight(monkeypatch, argv) -> None:
    """Each orchestrator runs nine phases and only two of them use IB.

    Preflighting here exited 86 before dispatch, so a down Gateway took out the
    Massive equity day_aggs lane, the flat-file intraday lane, FRED and CBOE —
    none of which have an IB dependency. Phase 5 shells out to
    `intraday-backfill`, which still preflights itself.
    """
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: (_ for _ in ()).throw(AssertionError("orchestrators must not preflight")),
    )
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module([], name, accepts_argv=False),
    )

    assert livewire_ingest.main(argv) == 0


def test_ingest_daily_help_does_not_preflight(monkeypatch) -> None:
    monkeypatch.setattr(
        ib_gateway_preflight,
        "assert_gateway_up",
        lambda: (_ for _ in ()).throw(AssertionError("preflight should not run")),
    )
    assert livewire_ingest.main(["daily", "--help"]) == 0


def test_entrypoints_render_top_level_help(capsys) -> None:
    assert livewire_ingest.main(["--help"]) == 0
    assert livewire_quality.main([]) == 0
    assert livewire_ops.main(["-h"]) == 0
    assert livewire_store.main(["--help"]) == 0

    out = capsys.readouterr().out
    assert "Livewire ingestion commands" in out
    assert "Livewire quality commands" in out
    assert "Livewire operational commands" in out
    assert "Livewire storage commands" in out


def test_ingest_backfill_all_dispatches_to_python(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []

    def zero_module(name):
        def main(argv):
            calls.append((name, list(argv)))
            return 0

        return SimpleNamespace(main=main)

    monkeypatch.setattr(livewire_ingest.importlib, "import_module", zero_module)
    monkeypatch.setattr(ib_gateway_preflight, "assert_gateway_up", lambda: None)

    assert livewire_ingest.main(["backfill-all"]) == 0
    assert calls[0][0] == "livewire_scripts.backfill_runner"


def test_ingest_daily_backfill_dispatches_to_python(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []

    def zero_module(name):
        def main(argv):
            calls.append((name, list(argv)))
            return 0

        return SimpleNamespace(main=main)

    monkeypatch.setattr(livewire_ingest.importlib, "import_module", zero_module)
    monkeypatch.setattr(ib_gateway_preflight, "assert_gateway_up", lambda: None)

    assert livewire_ingest.main(["daily-backfill"]) == 0
    assert calls[0][0] == "livewire_scripts.sync_runner"


def test_quality_dispatches_argv_aware_module(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_quality.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_quality.main(["report", "--view", "summary"]) == 7
    assert calls == [("livewire_scripts.data_quality_report", ["--view", "summary"])]


def test_quality_watchdog_loads_scheduled_env(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, list[str]]] = []
    loader_calls: list[Path] = []
    monkeypatch.setattr(livewire_quality, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        livewire_quality.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )
    monkeypatch.setattr(
        livewire_quality,
        "load_scheduled_env",
        lambda repo_root: loader_calls.append(repo_root),
    )

    assert livewire_quality.main(["watchdog", "--run-date", "2026-07-09"]) == 7
    assert loader_calls == [tmp_path]
    assert calls == [("livewire_scripts.check_daily_update_watchdog", ["--run-date", "2026-07-09"])]


def test_quality_health_loads_scheduled_env(monkeypatch, tmp_path) -> None:
    """The interior gap scan runs as its own launchd job now.

    It used to be spawned by the daily job and inherit that parent's env.
    launchd starts it cold, so without this MDW_DATA_LAKE_DIR / MDW_LOG_DIR
    resolve to defaults that may not be this warehouse — it would scan the
    wrong tree and write its artifact somewhere nothing reads.
    """
    calls: list[tuple[str, list[str]]] = []
    loader_calls: list[Path] = []
    monkeypatch.setattr(livewire_quality, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        livewire_quality.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )
    monkeypatch.setattr(
        livewire_quality,
        "load_scheduled_env",
        lambda repo_root: loader_calls.append(repo_root),
    )

    assert livewire_quality.main(["health", "--intraday", "--timeframe", "5m"]) == 7
    assert loader_calls == [tmp_path]
    assert calls == [("livewire_scripts.health_check", ["--intraday", "--timeframe", "5m"])]


def test_quality_other_commands_do_not_load_env(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_quality.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )
    monkeypatch.setattr(
        livewire_quality,
        "load_scheduled_env",
        lambda repo_root: (_ for _ in ()).throw(AssertionError("env should not load")),
    )

    # `weekly`, not `health`: health joined watchdog and coverage on the
    # env-loading list when the interior gap scan became its own launchd job.
    # It used to inherit a scheduled parent's env and now has no parent.
    assert livewire_quality.main(["weekly"]) == 7
    assert calls == [("livewire_scripts.weekly_quality_summary", [])]


def test_store_dispatches_storage_command(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_store.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_store.main(["sync-r2", "--upload"]) == 7
    assert calls == [("livewire_scripts.sync_to_r2", ["--upload"])]


def test_ops_send_alert_delegates_to_node(monkeypatch) -> None:
    seen = {}
    monkeypatch.setenv("MDW_NODE_BIN", "/custom/node")

    def fake_call(cmd):
        seen["cmd"] = cmd
        return 0

    monkeypatch.setattr(livewire_ops.subprocess, "call", fake_call)

    assert livewire_ops.main(["send-alert", "--mode", "failure"]) == 0
    assert seen["cmd"][0] == "/custom/node"
    assert seen["cmd"][1].endswith("livewire_node/send_daily_update_failure_email.mjs")
    assert seen["cmd"][2:] == ["--mode", "failure"]


def test_ops_run_daily_job_loads_env_files_and_dispatches(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, list[str]]] = []
    repo_env = tmp_path / ".env"
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    warehouse_env = warehouse / ".env"
    secrets = tmp_path / ".secrets"
    repo_env.write_text("export FROM_REPO='repo value'\n", encoding="utf-8")
    warehouse_env.write_text("FROM_WAREHOUSE=warehouse\n", encoding="utf-8")
    secrets.write_text("# comment\nFROM_SECRET=secret\nBROKEN_LINE\n", encoding="utf-8")

    monkeypatch.setattr(livewire_ops, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(livewire_ops.Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))
    monkeypatch.setattr(
        livewire_ops.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_ops.main(["run-daily-job", "--force"]) == 7
    assert calls == [("livewire_scripts.run_daily_update_job", ["--force"])]
    assert livewire_ops.os.environ["FROM_REPO"] == "repo value"
    assert livewire_ops.os.environ["FROM_WAREHOUSE"] == "warehouse"
    assert livewire_ops.os.environ["FROM_SECRET"] == "secret"


def test_ops_run_daily_job_uses_shared_scheduled_env_loader(monkeypatch, tmp_path) -> None:
    """The run-daily-job command must delegate env loading to
    livewire_scripts.scheduled_env so other scheduled wrappers reuse the same code path."""

    calls: list[Path] = []

    def _fake_loader(repo_root: Path) -> None:
        calls.append(repo_root)

    monkeypatch.setattr(livewire_ops, "load_scheduled_env", _fake_loader)
    monkeypatch.setattr(livewire_ops, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        livewire_ops.importlib,
        "import_module",
        lambda name: _fake_module([], name, accepts_argv=True),
    )

    livewire_ops.main(["run-daily-job"])

    assert calls == [tmp_path]


def test_ops_run_intraday_catchup_job_loads_env_files_and_dispatches(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, list[str]]] = []
    repo_env = tmp_path / ".env"
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    warehouse_env = warehouse / ".env"
    secrets = tmp_path / ".secrets"
    repo_env.write_text("export FROM_REPO='repo value'\n", encoding="utf-8")
    warehouse_env.write_text("FROM_WAREHOUSE=warehouse\n", encoding="utf-8")
    secrets.write_text("FROM_SECRET=secret\n", encoding="utf-8")

    monkeypatch.setattr(livewire_ops, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(livewire_ops.Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(warehouse))
    monkeypatch.setattr(
        livewire_ops.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_ops.main(["run-intraday-catchup-job"]) == 7
    assert calls == [("livewire_scripts.run_intraday_catchup_job", [])]
    assert livewire_ops.os.environ["FROM_REPO"] == "repo value"
    assert livewire_ops.os.environ["FROM_WAREHOUSE"] == "warehouse"
    assert livewire_ops.os.environ["FROM_SECRET"] == "secret"


def test_ops_help_lists_intraday_catchup_command(capsys) -> None:
    livewire_ops.main(["-h"])
    captured = capsys.readouterr().out
    assert "run-intraday-catchup-job" in captured


def test_ops_env_loader_ignores_missing_and_bad_quotes(tmp_path, monkeypatch) -> None:
    missing = tmp_path / "missing.env"
    livewire_ops._load_env_file(missing)

    env_file = tmp_path / "bad.env"
    env_file.write_text("=ignored\nBAD='unterminated\nEMPTY=\n", encoding="utf-8")
    monkeypatch.delenv("BAD", raising=False)
    monkeypatch.delenv("EMPTY", raising=False)

    livewire_ops._load_env_file(env_file)

    assert livewire_ops.os.environ["BAD"] == "'unterminated"
    assert livewire_ops.os.environ["EMPTY"] == ""

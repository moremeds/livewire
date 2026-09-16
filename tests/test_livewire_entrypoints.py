from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from clients import ib_gateway_preflight, ledger
from clients.ib_client import IBConnectionError
from clients.index_membership_store import IndexMembershipStore, MembershipEvent
from clients.security_master import SecurityIdentityEvent, SecurityMaster
from livewire_scripts import membership_sync, notify
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
    monkeypatch.setattr(livewire_ingest, "load_scheduled_env", lambda repo_root: None)

    assert livewire_ingest.main(["universe-sync", "--dry-run"]) == 0
    assert calls == [("livewire_scripts.universe_sync", [])]


@pytest.mark.parametrize(
    ("command", "module"),
    [
        ("universe-sync", "livewire_scripts.universe_sync"),
        ("shepherd-universe", "livewire_scripts.shepherd_universe"),
        ("membership-sync", "livewire_scripts.membership_sync"),
        ("security-master", "livewire_scripts.security_master_sync"),
    ],
)
def test_ingest_universe_refresh_commands_load_scheduled_env(monkeypatch, tmp_path, command, module) -> None:
    """`com.livewire.universe-refresh` and `com.livewire.membership-sync` invoke
    this entrypoint directly and launchd starts them cold.

    Every other job goes through `livewire_ops.py run-*-job`, which loads the
    same files first. Without the loader `universe_sync` logged
    `MASSIVE_API_KEY not set — skipping dead-ticker check` and exited 0, so
    the denominator gained new index members and never lost delisted ones —
    in the one job that exists to keep it honest.
    """
    calls: list[tuple[str, list[str]]] = []
    loader_calls: list[Path] = []
    monkeypatch.setattr(livewire_ingest, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=False),
    )
    monkeypatch.setattr(
        livewire_ingest,
        "load_scheduled_env",
        lambda repo_root: loader_calls.append(repo_root),
    )

    assert livewire_ingest.main([command]) == 0
    assert loader_calls == [tmp_path]
    assert calls == [(module, [])]


def test_ingest_knows_the_security_master_command() -> None:
    assert livewire_ingest.COMMANDS["security-master"] == "livewire_scripts.security_master_sync"
    # it is not an IB command: the whole path is Massive REST
    assert "security-master" not in livewire_ingest.IB_COMMANDS


def test_ingest_other_commands_do_not_load_env(monkeypatch) -> None:
    """Every other ingest command inherits a scheduled parent's env."""
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_ingest.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )
    monkeypatch.setattr(
        livewire_ingest,
        "load_scheduled_env",
        lambda repo_root: (_ for _ in ()).throw(AssertionError("env should not load")),
    )

    assert livewire_ingest.main(["daily", "--source", "massive"]) == 7
    assert calls == [("livewire_scripts.daily_update", ["--source", "massive"])]


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
    launchd starts it cold, so without this MDW_DATA_LAKE / MDW_LOG_DIR
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


def test_ops_removed_alert_command_is_rejected() -> None:
    removed = "send" + "-alert"  # kept grep-clean: the literal lives nowhere but history
    with pytest.raises(SystemExit) as excinfo:
        livewire_ops.main([removed, "--mode", "failure"])
    assert excinfo.value.code == 2


def test_ops_notify_sends_a_notice_and_records_it(monkeypatch, tmp_path) -> None:
    body = tmp_path / "page.txt"
    body.write_text("lane equity failed\n", encoding="utf-8")
    seen = {}

    def fake_child(command, *, timeout):
        seen["cmd"] = command
        return subprocess.CompletedProcess(command, 0, stdout='{"accepted":["t@x"],"messageId":"m1"}')

    monkeypatch.setattr(notify, "_run_child", fake_child)

    rc = livewire_ops.main(
        ["notify", "--kind", "page", "--subject", "PAGE 2026-09-12: x", "--body-file", str(body), "--force"]
    )
    assert rc == 0
    assert seen["cmd"][1].endswith("livewire_node/send_mail.mjs")
    assert "--subject=PAGE 2026-09-12: x" in seen["cmd"]
    rows = ledger.query("select exit_code, receipt_json from executions where script='notify'")
    assert len(rows) == 1 and rows[0]["exit_code"] == 0
    assert json.loads(rows[0]["receipt_json"])["kind"] == "page"


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


_MEMBERSHIP_IMPORT_NOW = datetime(2026, 9, 13, 1, 0, tzinfo=UTC)


def _verifies(ref: str, digest: str) -> bool:
    return ref == f"artifact://sha256/{digest}"


def _verified_identity(
    master: SecurityMaster,
    symbol: str,
    *,
    effective_from: datetime = datetime(2000, 1, 1, tzinfo=UTC),
    effective_to: datetime | None = None,
) -> str:
    security_id = master.new_security_id()
    master.append(
        SecurityIdentityEvent(
            event_id=f"identity-{symbol}-{security_id}",
            security_id=security_id,
            revision=1,
            symbol=symbol,
            provider="massive",
            exchange_mic="XNAS",
            currency="USD",
            effective_from=effective_from,
            effective_to=effective_to,
            known_at=effective_from,
            issuer_name=f"{symbol} issuer",
            cik="0000000001",
            composite_figi=None,
            share_class_figi=None,
            continuity_basis="provider_figi",
            relationship_type=None,
            related_security_id=None,
            source_refs=("artifact://sha256/" + "a" * 64,),
            source_hashes=("a" * 64,),
            status="verified",
            supersedes=None,
        )
    )
    return security_id


def _membership_lake(tmp_path: Path) -> Path:
    """A two-identity master plus a six-line grok panel, imported at _MEMBERSHIP_IMPORT_NOW."""
    lake = tmp_path / "lake"
    master = SecurityMaster(lake, evidence_verifier=_verifies)
    if not master.events():
        _verified_identity(master, "AAPL")
        _verified_identity(master, "MSFT")
    rows = [
        {"effective_date": "2004-01-01", "action": "add", "ticker": "AAPL", "kind": "bootstrap"},
        {"effective_date": "2004-01-01", "action": "add", "ticker": "AEOS", "kind": "bootstrap"},
        {"effective_date": "2004-01-01", "action": "add", "ticker": "MSFT", "kind": "bootstrap"},
        {"effective_date": "2007-01-01", "action": "remove", "ticker": "AEOS", "kind": "diff"},
        {"effective_date": "2010-01-01", "action": "remove", "ticker": "AAPL", "kind": "diff"},
        {"effective_date": "2015-01-01", "action": "add", "ticker": "AAPL", "kind": "diff"},
    ]
    events = tmp_path / "events.jsonl"
    events.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    source = tmp_path / "source.snapshot"
    source.write_bytes(b'{"note": "evidence"}\n')
    membership_sync.import_events(
        index_id="sp500",
        events_path=events,
        sources=[source],
        data_lake_root=lake,
        now=_MEMBERSHIP_IMPORT_NOW,
        confidence="B",
    )
    return lake


def test_ops_membership_prints_sorted_symbols_and_marks_unresolved(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("MDW_DATA_LAKE", str(_membership_lake(tmp_path)))

    assert livewire_ops.main(["membership", "--index", "sp500", "--effective-at", "2006-01-01"]) == 0
    out, err = capsys.readouterr()
    assert out.splitlines() == ["?unresolved:AEOS", "AAPL", "MSFT"]
    assert err.strip() == "3"


def test_ops_membership_effective_at_replays_adds_and_removes(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("MDW_DATA_LAKE", str(_membership_lake(tmp_path)))

    assert livewire_ops.main(["membership", "--index", "sp500", "--effective-at", "2012-01-01"]) == 0
    out, err = capsys.readouterr()
    assert out.splitlines() == ["MSFT"]  # AAPL removed 2010, AEOS removed 2007
    assert err.strip() == "1"

    assert livewire_ops.main(["membership", "--index", "sp500", "--effective-at", "2016-01-01"]) == 0
    out, err = capsys.readouterr()
    assert out.splitlines() == ["AAPL", "MSFT"]  # the 2015 re-add landed
    assert err.strip() == "2"


def test_ops_membership_as_of_before_the_import_returns_nothing(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("MDW_DATA_LAKE", str(_membership_lake(tmp_path)))

    rc = livewire_ops.main(["membership", "--index", "sp500", "--effective-at", "2016-01-01", "--as-of", "2026-09-12"])
    out, err = capsys.readouterr()
    assert rc == 0 and out == "" and err.strip() == "0"  # imported 2026-09-13; we did not know it on the 12th

    rc = livewire_ops.main(["membership", "--index", "sp500", "--effective-at", "2016-01-01", "--as-of", "2026-09-13"])
    out, err = capsys.readouterr()
    assert rc == 0 and out.splitlines() == ["AAPL", "MSFT"] and err.strip() == "2"


def test_ops_membership_reads_an_empty_index_and_exits_zero(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("MDW_DATA_LAKE", str(_membership_lake(tmp_path)))

    assert livewire_ops.main(["membership", "--index", "ndx100", "--effective-at", "2016-01-01"]) == 0
    out, err = capsys.readouterr()
    assert out == "" and err.strip() == "0"


def test_ops_membership_names_a_symbol_for_an_expired_identity(tmp_path, monkeypatch, capsys) -> None:
    """A member whose verified identity ended before the query date still names a symbol."""
    lake = tmp_path / "lake"
    master = SecurityMaster(lake, evidence_verifier=_verifies)
    csco_id = _verified_identity(master, "CSCO", effective_to=datetime(2010, 1, 1, tzinfo=UTC))
    store = IndexMembershipStore(lake, security_master=master, evidence_verifier=_verifies)
    store.append(
        MembershipEvent(
            event_id="m1",
            index_id="sp500",
            security_id=csco_id,
            action="add",
            announced_at=None,
            effective_at=datetime(2004, 1, 1, tzinfo=UTC),
            known_at=datetime(2004, 1, 1, tzinfo=UTC),
            source_refs=("artifact://sha256/" + "b" * 64,),
            source_hashes=("b" * 64,),
            revision=1,
            supersedes=None,
            status="candidate",
        )
    )
    monkeypatch.setenv("MDW_DATA_LAKE", str(lake))

    # effective_at past the identity's end: no interval contains it, so the
    # latest verified identity still names the symbol.
    assert livewire_ops.main(["membership", "--index", "sp500", "--effective-at", "2015-01-01"]) == 0
    out, err = capsys.readouterr()
    assert out.splitlines() == ["CSCO"] and err.strip() == "1"


def _membership_sync_argv() -> list[str]:
    """The argv the scheduled `com.livewire.membership-sync` plist produces."""
    import plistlib
    import shlex

    payload = plistlib.loads((REPO_ROOT / "launchd" / "com.livewire.membership-sync.plist.example").read_bytes())
    words = shlex.split(payload["ProgramArguments"][2])
    start = words.index("scripts/livewire_ingest.py")
    return words[start + 1 :]


def test_the_scheduled_membership_sync_argv_parses_and_covers_every_index(monkeypatch) -> None:
    """The exact argv from the plist, through the real argparse, not a mock.

    The template names the four panels as one space-separated list; the flag was
    declared `action="append"`, which takes one value, so the scheduled job died
    every weekday on `unrecognized arguments: ndx100 djia` and never synced a
    single membership event.
    """
    seen: dict[str, object] = {}
    monkeypatch.setattr(
        membership_sync,
        "sync",
        lambda **kwargs: seen.update(kwargs) or 0,
    )
    monkeypatch.setattr(membership_sync, "data_lake_dir", lambda: REPO_ROOT)
    monkeypatch.setattr(livewire_ingest, "load_scheduled_env", lambda repo_root: None)

    argv = _membership_sync_argv()
    assert argv[0] == "membership-sync"
    assert livewire_ingest.main(argv) == 0
    assert sorted(seen["indexes"]) == sorted(membership_sync.DEFAULT_INDEXES)


def test_membership_sync_index_flag_is_also_repeatable(monkeypatch) -> None:
    seen: dict[str, object] = {}
    monkeypatch.setattr(membership_sync, "sync", lambda **kwargs: seen.update(kwargs) or 0)
    monkeypatch.setattr(membership_sync, "data_lake_dir", lambda: REPO_ROOT)

    assert membership_sync.main(["--index", "sp500", "--index", "djia", "ndx100"]) == 0
    assert sorted(seen["indexes"]) == ["djia", "ndx100", "sp500"]


def test_cboe_vol_propagates_a_failure_exit_code_to_its_caller(tmp_path, monkeypatch) -> None:
    """The seam the bug actually lived on. `fetch_cboe_volatility.main()`
    returned None, `_dispatch_module` turned that into 0, and
    `sync_runner`'s `if rc != 0` could therefore never fire — the phase could
    not fail however many symbols were missing. Exercising the real entrypoint
    is the only thing that proves the exit code survives the dispatch.
    pm:2026-09-16-cboe-could-not-fail
    """
    import httpx

    monkeypatch.setattr("clients.http_retry.time.sleep", lambda _seconds: None)
    request = httpx.Request("GET", "https://cdn.cboe.com/api/global/delayed_quotes/charts/historical/_VXHYG.json")
    outage = httpx.Response(503, request=request)
    monkeypatch.setattr("livewire_scripts.fetch_cboe_volatility.httpx.get", lambda *a, **k: outage)

    assert livewire_ingest.main(["cboe-vol", "--symbols", "VXHYG", "--warehouse", str(tmp_path)]) == 1


def test_cboe_vol_still_exits_zero_when_cboe_has_retired_the_index(tmp_path, monkeypatch) -> None:
    """A 404 is CBOE's claim about the index, surfaced by `Stale non-equity`;
    failing the phase for it would page nightly for a retired symbol
    (pm:2026-09-07-retired-cboe-index-alerted-forever)."""
    import httpx

    monkeypatch.setattr("clients.http_retry.time.sleep", lambda _seconds: None)
    request = httpx.Request("GET", "https://cdn.cboe.com/api/global/delayed_quotes/charts/historical/_VIXTLT.json")
    gone = httpx.Response(404, request=request)
    monkeypatch.setattr("livewire_scripts.fetch_cboe_volatility.httpx.get", lambda *a, **k: gone)

    assert livewire_ingest.main(["cboe-vol", "--symbols", "VIXTLT", "--warehouse", str(tmp_path)]) == 0

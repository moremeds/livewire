from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from clients import ib_gateway_preflight
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


def test_ingest_intraday_massive_equity_bypasses_ib_preflight(monkeypatch) -> None:
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
        ["backfill-all"],
        ["daily-backfill"],
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


def test_backfill_all_runner_includes_fred_rates_phase() -> None:
    script_path = REPO_ROOT / "tools" / "run_backfill_all.sh"
    script = script_path.read_text(encoding="utf-8")

    assert "PHASE 3: FRED Treasury rates" in script
    assert "source .env" in script
    assert "fred-rates" in script
    assert "backfill_fred_rates.log" in script
    assert script.index("PHASE 2 COMPLETE") < script.index(
        "PHASE 3: FRED Treasury rates"
    )

    subprocess.run(["bash", "-n", str(script_path)], check=True)


def test_quality_dispatches_argv_aware_module(monkeypatch) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        livewire_quality.importlib,
        "import_module",
        lambda name: _fake_module(calls, name, accepts_argv=True),
    )

    assert livewire_quality.main(["report", "--view", "summary"]) == 7
    assert calls == [("livewire_scripts.data_quality_report", ["--view", "summary"])]


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


def test_ops_run_daily_job_loads_env_files_and_dispatches(
    monkeypatch, tmp_path
) -> None:
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

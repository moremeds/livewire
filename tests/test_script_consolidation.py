from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


EXPECTED_SCRIPT_FILES = {
    "livewire.py",
    "livewire_ingest.py",
    "livewire_ops.py",
    "livewire_quality.py",
    "livewire_store.py",
    "setup_market_warehouse.sh",
}


def test_scripts_directory_exposes_only_five_operator_entrypoints() -> None:
    script_files = {
        path.name for path in (REPO_ROOT / "scripts").iterdir() if path.is_file()
    }

    assert script_files == EXPECTED_SCRIPT_FILES


def test_operator_entrypoint_modules_are_importable() -> None:
    for module_name in (
        "scripts.livewire_ingest",
        "scripts.livewire_ops",
        "scripts.livewire_quality",
        "scripts.livewire_store",
    ):
        module = importlib.import_module(module_name)

        assert callable(module.main)


def test_operator_entrypoints_render_subcommand_help() -> None:
    expected_commands = {
        "livewire_ingest.py": [
            "daily",
            "historical",
            "robust",
            "cboe-vol",
            "fred-rates",
            "intraday-backfill",
            "flatfile-ingest",
            "daily-backfill",
        ],
        "livewire_quality.py": ["health", "coverage", "report", "weekly", "watchdog"],
        "livewire_ops.py": ["run-daily-job", "send-alert"],
        "livewire_store.py": [
            "rebuild-postgres",
            "smoke-postgres",
            "sync-r2",
            "migrate-parquet",
        ],
    }

    for script_name, commands in expected_commands.items():
        result = subprocess.run(
            [sys.executable, str(REPO_ROOT / "scripts" / script_name), "--help"],
            check=True,
            capture_output=True,
            text=True,
        )

        for command in commands:
            assert command in result.stdout


def test_operator_entrypoints_forward_subcommand_help() -> None:
    examples = {
        "livewire_ingest.py": ("daily", "Daily market data update"),
        "livewire_quality.py": ("report", "Livewire data quality report"),
        "livewire_store.py": ("rebuild-postgres", "Rebuild Postgres analytical tables"),
    }

    for script_name, (command, expected) in examples.items():
        result = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / script_name),
                command,
                "--help",
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        assert expected in result.stdout


def test_backfill_all_includes_default_full_warehouse_phases() -> None:
    script = (REPO_ROOT / "tools" / "run_backfill_all.sh").read_text()

    assert "Massive ${timeframe} intraday" in script
    assert "for timeframe in 1m 5m 1h" in script
    assert '--timeframe "$timeframe"' in script
    assert "--source massive" in script
    assert "--years 5" in script

    assert "PHASE 7: CBOE volatility daily" in script
    assert "cboe-vol --preset presets/volatility.json" in script

    assert "PHASE 8: IB volatility intraday" in script
    assert 'VOL_PRESET="presets/volatility-intraday.json"' in script
    assert "--asset-class volatility" in script
    assert "--source ib" in script
    assert "run_equity_intraday &" in script
    assert "run_volatility_intraday &" in script
    assert 'wait "$equity_intraday_pid"' in script
    assert 'wait "$volatility_intraday_pid"' in script
    assert "trap cleanup_children INT TERM EXIT" in script
    assert "kill_tree" in script
    assert "max_mtime" in script
    assert "MDW_BACKFILL_SUCCESS_COOLDOWN" in script
    assert "MDW_BACKFILL_NO_PROGRESS_COOLDOWN" in script

    assert "PHASE 9: Postgres analytical rebuild" in script
    assert (
        "rebuild-postgres --asset-class equity --timeframe all --include-reliability"
        in script
    )
    assert "rebuild-postgres --asset-class volatility --timeframe 1d" in script
    assert "MDW_POSTGRES_DSN" in script


def test_backfill_all_smoke_runs_every_phase_with_fake_python(tmp_path: Path) -> None:
    home = tmp_path / "home"
    activate = home / "market-warehouse" / ".venv" / "bin" / "activate"
    activate.parent.mkdir(parents=True)
    activate.write_text("", encoding="utf-8")

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python"
    fake_python.write_text(
        """#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

args = sys.argv[1:]
log = Path(os.environ["LW_FAKE_PYTHON_LOG"])
log.parent.mkdir(parents=True, exist_ok=True)
with log.open("a", encoding="utf-8") as fh:
    fh.write(" ".join(args) + "\\n")

home = Path(os.environ["HOME"])

def preset_info(path):
    with open(path, encoding="utf-8") as fh:
        payload = json.load(fh)
    return payload["name"], len(payload["tickers"])

if len(args) >= 2 and args[0] == "scripts/livewire_ingest.py":
    command = args[1]
    if "--preset" in args:
        name, total = preset_info(args[args.index("--preset") + 1])
    else:
        name, total = "custom", 1
    if command == "historical":
        filename = f"cursor_backfill_{name}.json" if "--backfill" in args else f"cursor_{name}.json"
        cursor = home / "market-warehouse" / "logs" / filename
    elif command == "intraday-backfill":
        timeframe = args[args.index("--timeframe") + 1]
        cursor = home / "market-warehouse" / "cursors" / f"cursor_intraday_{timeframe}_{name}.json"
    else:
        sys.exit(0)
    cursor.parent.mkdir(parents=True, exist_ok=True)
    cursor.write_text(json.dumps({"completed": [str(i) for i in range(total)]}), encoding="utf-8")
sys.exit(0)
""",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    env = os.environ.copy()
    env["HOME"] = str(home)
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    env["LW_FAKE_PYTHON_LOG"] = str(tmp_path / "calls.log")
    env["MDW_POSTGRES_DSN"] = "postgresql://example/livewire"
    env["MDW_BACKFILL_POLL_INTERVAL"] = "0.01"

    result = subprocess.run(
        ["bash", "tools/run_backfill_all.sh"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )

    calls = (tmp_path / "calls.log").read_text(encoding="utf-8")
    assert (
        "scripts/livewire_ingest.py historical --preset presets/sp500.json --years 0 --skip-existing"
        in calls
    )
    assert (
        "scripts/livewire_ingest.py historical --preset presets/r2k.json --backfill --source auto"
        in calls
    )
    assert (
        "scripts/livewire_ingest.py intraday-backfill --preset presets/sp500.json --timeframe 1m --source massive"
        in calls
    )
    assert (
        "scripts/livewire_ingest.py intraday-backfill --preset presets/r2k.json --timeframe 5m --source massive"
        in calls
    )
    assert (
        "scripts/livewire_ingest.py intraday-backfill --preset presets/volatility-intraday.json --timeframe 1h --source ib"
        in calls
    )
    assert (
        "scripts/livewire_store.py rebuild-postgres --asset-class equity --timeframe all --include-reliability"
        in calls
    )
    assert (
        "scripts/livewire_store.py rebuild-postgres --asset-class volatility --timeframe 1d"
        in calls
    )
    assert "ALL DONE" in result.stdout


def test_daily_backfill_includes_massive_recent_equity_and_same_side_lanes() -> None:
    script = (REPO_ROOT / "tools" / "run_daily_backfill.sh").read_text()

    assert "Daily backfill runner" in script
    assert "MDW_DAILY_BACKFILL_INTRADAY_DAYS" in script
    assert "MDW_DAILY_BACKFILL_INTRADAY_CONCURRENT" in script
    assert "MDW_DAILY_BACKFILL_TARGET_DATE" in script
    assert "RUN_FAILURES=()" in script
    assert "format_command" in script
    assert "... [%d more args]" in script
    assert "--allow-completed-summary" in script
    assert 'grep -q "Daily Update Complete"' in script
    assert "equity_ticker_union" in script
    assert "preset_tickers" in script
    assert 'VOL_TICKERS=($(preset_tickers "$VOL_PRESET"))' in script
    assert "daily --asset-class equity --source massive" in script
    assert '--tickers "${EQUITY_TICKERS[@]}"' in script
    assert '--target-date "$TARGET_DATE"' in script
    assert (
        'intraday-backfill --tickers "${EQUITY_TICKERS[@]}" --timeframe "$timeframe"'
        in script
    )
    assert '--source massive --asset-class equity --days "$INTRADAY_DAYS"' in script
    assert '--max-concurrent "$INTRADAY_CONCURRENT"' in script
    assert "--existing-only" not in script
    assert "fred-rates" in script
    assert "cboe-vol --preset presets/volatility.json" in script
    assert (
        'intraday-backfill --tickers "${VOL_TICKERS[@]}" --timeframe "$timeframe"'
        in script
    )
    assert '--source ib --asset-class volatility --days "$INTRADAY_DAYS"' in script
    assert (
        "rebuild-postgres --asset-class equity --timeframe all --include-reliability"
        in script
    )


def test_daily_backfill_smoke_runs_expected_phases_with_fake_python(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    activate = home / "market-warehouse" / ".venv" / "bin" / "activate"
    activate.parent.mkdir(parents=True)
    activate.write_text("", encoding="utf-8")

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python"
    fake_python.write_text(
        """#!/usr/bin/env python3
import os
import sys
from pathlib import Path

log = Path(os.environ["LW_FAKE_PYTHON_LOG"])
log.parent.mkdir(parents=True, exist_ok=True)
with log.open("a", encoding="utf-8") as fh:
    fh.write(" ".join(sys.argv[1:]) + "\\n")
sys.exit(0)
""",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    env = os.environ.copy()
    env["HOME"] = str(home)
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    env["LW_FAKE_PYTHON_LOG"] = str(tmp_path / "calls.log")
    env["MDW_POSTGRES_DSN"] = "postgresql://example/livewire"
    env["MDW_DAILY_BACKFILL_INTRADAY_DAYS"] = "3"
    env["MDW_DAILY_BACKFILL_INTRADAY_CONCURRENT"] = "7"
    env["MDW_DAILY_BACKFILL_TARGET_DATE"] = "2026-05-19"

    result = subprocess.run(
        ["bash", "tools/run_daily_backfill.sh"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )

    calls = (tmp_path / "calls.log").read_text(encoding="utf-8")
    assert (
        "scripts/livewire_ingest.py daily --asset-class equity --source massive --tickers"
        in calls
    )
    assert "--target-date 2026-05-19 --force" in calls
    assert "scripts/livewire_ingest.py intraday-backfill --tickers" in calls
    assert (
        "--timeframe 1m --source massive --asset-class equity --days 3 --max-concurrent 7"
        in calls
    )
    assert "scripts/livewire_ingest.py fred-rates" in calls
    assert (
        "scripts/livewire_ingest.py cboe-vol --preset presets/volatility.json" in calls
    )
    assert (
        "scripts/livewire_ingest.py intraday-backfill --tickers VIX SPX --timeframe 1h --source ib --asset-class volatility --days 3"
        in calls
    )
    assert (
        "scripts/livewire_store.py rebuild-postgres --asset-class equity --timeframe all --include-reliability"
        in calls
    )
    assert "DAILY BACKFILL COMPLETE" in result.stdout

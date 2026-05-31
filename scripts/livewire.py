#!/usr/bin/env python3
"""Livewire — unified CLI for the market data warehouse.

Commands:
    sync        Daily catch-up: make all asset classes current
    backfill    Deep historical fill to maximum provider depth
    check       Quality, health, and coverage reporting
    publish     Push bronze data to Postgres or R2
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import os
import sys
from pathlib import Path
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

SYNC_MODULES = {
    "equity": "livewire_scripts.daily_update",
    "volatility": "livewire_scripts.fetch_cboe_volatility",
    "rates": "livewire_scripts.fetch_fred_rates",
}

BACKFILL_MODULES = {
    "daily": "livewire_scripts.fetch_ib_historical",
    "intraday": "livewire_scripts.backfill_intraday",
}

CHECK_MODULES = {
    "health": "livewire_scripts.health_check",
    "coverage": "livewire_scripts.coverage_report",
    "report": "livewire_scripts.data_quality_report",
    "weekly": "livewire_scripts.weekly_quality_summary",
    "watchdog": "livewire_scripts.check_daily_update_watchdog",
    "universe": "livewire_scripts.universe_screener",
}

PUBLISH_MODULES = {
    "postgres": "livewire_scripts.rebuild_postgres_from_parquet",
    "r2": "livewire_scripts.sync_to_r2",
}


def _has_massive_key() -> bool:
    return bool(os.environ.get("MASSIVE_API_KEY"))


def _ib_reachable() -> bool:
    import socket

    try:
        with socket.create_connection(("127.0.0.1", 4001), timeout=2):
            return True
    except OSError:
        return False


def _needs_ib(asset_class: str) -> bool:
    if asset_class in ("futures", "cmdty", "fx"):
        return True
    if asset_class == "volatility":
        return True
    if asset_class == "equity" and not _has_massive_key():
        return True
    return False


def _dispatch_module(module_name: str, argv: list[str], display: str) -> int:
    module = importlib.import_module(module_name)
    original = sys.argv
    sys.argv = [display, *argv]
    try:
        sig = inspect.signature(module.main)
        try:
            result = module.main(list(argv)) if sig.parameters else module.main()
        except SystemExit as exc:
            if exc.code in (0, None):
                return 0
            raise
    finally:
        sys.argv = original
    return int(result or 0)


def _dispatch_sync(argv: list[str]) -> int:
    """Daily catch-up: equity + volatility + rates, auto source selection."""
    parser = argparse.ArgumentParser(prog="livewire sync")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--scheduled",
        action="store_true",
        help="Run as scheduled job with retry + alerting",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run daily-backfill orchestrator (Massive equity + FRED + CBOE + IB vol + Postgres)",
    )
    parser.add_argument(
        "--asset-class",
        choices=["equity", "volatility", "futures", "rates", "all"],
        default="all",
    )
    args, rest = parser.parse_known_args(argv)

    if args.scheduled:
        return _dispatch_module(
            "livewire_scripts.run_daily_update_job", rest, "livewire sync --scheduled"
        )

    if args.full:
        return _dispatch_module(
            "livewire_scripts.sync_runner", rest, "livewire sync --full"
        )

    if _needs_ib(args.asset_class if args.asset_class != "all" else "equity"):
        if args.asset_class in ("futures", "all"):
            from clients.ib_gateway_preflight import assert_gateway_up

            assert_gateway_up()

    results = []
    classes = (
        ["equity", "volatility", "futures", "rates"]
        if args.asset_class == "all"
        else [args.asset_class]
    )

    for ac in classes:
        cmd_argv = list(rest)
        if args.dry_run:
            cmd_argv.append("--dry-run")
        if args.force:
            cmd_argv.append("--force")

        if ac == "equity":
            if _has_massive_key():
                cmd_argv.extend(["--source", "massive"])
            cmd_argv.extend(["--asset-class", "equity"])
            results.append(
                _dispatch_module(
                    SYNC_MODULES["equity"], cmd_argv, "livewire sync equity"
                )
            )

        elif ac == "volatility":
            results.append(
                _dispatch_module(
                    SYNC_MODULES["volatility"], cmd_argv, "livewire sync volatility"
                )
            )

        elif ac == "futures":
            cmd_argv.extend(["--asset-class", "futures"])
            results.append(
                _dispatch_module(
                    SYNC_MODULES["equity"], cmd_argv, "livewire sync futures"
                )
            )

        elif ac == "rates":
            results.append(
                _dispatch_module(SYNC_MODULES["rates"], cmd_argv, "livewire sync rates")
            )

    return max(results) if results else 0


def _dispatch_backfill(argv: list[str]) -> int:
    """Deep historical fill with auto source selection."""
    parser = argparse.ArgumentParser(prog="livewire backfill")
    parser.add_argument(
        "--timeframe",
        nargs="+",
        choices=["1d", "1h", "30m", "5m", "1m", "all"],
        default=["all"],
    )
    parser.add_argument(
        "--asset-class",
        choices=["equity", "volatility", "futures", "all"],
        default="all",
    )
    parser.add_argument("--years", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--preset", type=str, default=None)
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full warehouse backfill orchestrator (all presets, all phases)",
    )
    args, rest = parser.parse_known_args(argv)

    if args.full:
        return _dispatch_module(
            "livewire_scripts.backfill_runner", rest, "livewire backfill --full"
        )

    timeframes = (
        ["1d", "1h", "30m", "5m", "1m"] if "all" in args.timeframe else args.timeframe
    )

    results = []
    for tf in timeframes:
        cmd_argv = list(rest)
        if args.dry_run:
            cmd_argv.append("--dry-run")
        if args.preset:
            cmd_argv.extend(["--preset", args.preset])
        if args.skip_existing:
            cmd_argv.append("--skip-existing")

        if tf == "1d":
            if _has_massive_key() and args.asset_class in ("equity", "all"):
                cmd_argv.extend(["--source", "massive"])
            if args.years is not None:
                cmd_argv.extend(["--years", str(args.years)])
            results.append(
                _dispatch_module(
                    BACKFILL_MODULES["daily"], cmd_argv, f"livewire backfill {tf}"
                )
            )
        else:
            cmd_argv.extend(["--timeframe", tf])
            if _has_massive_key() and args.asset_class in ("equity", "all"):
                cmd_argv.extend(["--source", "massive"])
            if args.years is not None:
                cmd_argv.extend(["--years", str(args.years)])
            results.append(
                _dispatch_module(
                    BACKFILL_MODULES["intraday"], cmd_argv, f"livewire backfill {tf}"
                )
            )

    return max(results) if results else 0


def _dispatch_check(argv: list[str]) -> int:
    """Quality, health, and coverage reporting."""
    parser = argparse.ArgumentParser(prog="livewire check")
    parser.add_argument(
        "--mode", choices=list(CHECK_MODULES.keys()), default="coverage"
    )
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--weekly", action="store_true")
    parser.add_argument("--universe", action="store_true")
    parser.add_argument("--health", action="store_true")
    args, rest = parser.parse_known_args(argv)

    if args.report:
        mode = "report"
    elif args.weekly:
        mode = "weekly"
    elif args.universe:
        mode = "universe"
    elif args.health:
        mode = "health"
    else:
        mode = args.mode

    return _dispatch_module(CHECK_MODULES[mode], rest, f"livewire check {mode}")


def _dispatch_publish(argv: list[str]) -> int:
    """Push bronze data to Postgres or R2."""
    parser = argparse.ArgumentParser(prog="livewire publish")
    parser.add_argument("target", choices=["postgres", "r2"], nargs="?", default=None)
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Run smoke test instead of full rebuild (postgres only)",
    )
    parser.add_argument(
        "--migrate", action="store_true", help="Run parquet schema migration"
    )
    args, rest = parser.parse_known_args(argv)

    if args.migrate:
        return _dispatch_module(
            "livewire_scripts.migrate_parquet_filename",
            rest,
            "livewire publish --migrate",
        )

    if args.target is None:
        parser.error("target is required (postgres or r2) unless --migrate is set")

    if args.target == "postgres":
        if args.smoke:
            return _dispatch_module(
                "livewire_scripts.smoke_postgres_analytical",
                rest,
                "livewire publish postgres --smoke",
            )
        return _dispatch_module(
            PUBLISH_MODULES["postgres"], rest, "livewire publish postgres"
        )

    return _dispatch_module(PUBLISH_MODULES["r2"], rest, "livewire publish r2")


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)

    parser = argparse.ArgumentParser(
        prog="livewire",
        description="Livewire market data warehouse CLI",
    )
    parser.add_argument(
        "command",
        choices=["sync", "backfill", "check", "publish"],
        help="Command to run",
    )

    if not argv or argv[0] in {"-h", "--help"}:
        parser.print_help()
        return 0

    args = parser.parse_args(argv[:1])
    rest = argv[1:]

    dispatch = {
        "sync": _dispatch_sync,
        "backfill": _dispatch_backfill,
        "check": _dispatch_check,
        "publish": _dispatch_publish,
    }

    return dispatch[args.command](rest)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

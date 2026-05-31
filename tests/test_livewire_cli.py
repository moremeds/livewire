"""Tests for the unified livewire CLI dispatcher (scripts/livewire.py)."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

from scripts.livewire import (
    _dispatch_backfill,
    _dispatch_check,
    _dispatch_module,
    _dispatch_publish,
    _dispatch_sync,
    _has_massive_key,
    _has_s3_keys,
    _ib_reachable,
    _needs_ib,
    main,
)


class TestMain:
    def test_help_exits_zero(self):
        assert main(["--help"]) == 0

    def test_empty_argv_prints_help(self):
        assert main([]) == 0

    def test_unknown_command_exits_nonzero(self):
        with pytest.raises(SystemExit) as exc_info:
            main(["nonexistent"])
        assert exc_info.value.code != 0

    def test_sync_dispatches(self):
        with patch("scripts.livewire._dispatch_sync", return_value=0) as mock:
            main(["sync", "--dry-run"])
        mock.assert_called_once_with(["--dry-run"])

    def test_backfill_dispatches(self):
        with patch("scripts.livewire._dispatch_backfill", return_value=0) as mock:
            main(["backfill", "--dry-run"])
        mock.assert_called_once_with(["--dry-run"])

    def test_check_dispatches(self):
        with patch("scripts.livewire._dispatch_check", return_value=0) as mock:
            main(["check"])
        mock.assert_called_once_with([])

    def test_publish_dispatches(self):
        with patch("scripts.livewire._dispatch_publish", return_value=0) as mock:
            main(["publish", "r2"])
        mock.assert_called_once_with(["r2"])


class TestHasMassiveKey:
    def test_returns_true_when_set(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "test-key")
        assert _has_massive_key() is True

    def test_returns_false_when_unset(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        assert _has_massive_key() is False

    def test_returns_false_when_empty(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "")
        assert _has_massive_key() is False


class TestNeedsIB:
    def test_futures_always_needs_ib(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "key")
        assert _needs_ib("futures") is True

    def test_fx_always_needs_ib(self):
        assert _needs_ib("fx") is True

    def test_cmdty_always_needs_ib(self):
        assert _needs_ib("cmdty") is True

    def test_volatility_needs_ib(self):
        assert _needs_ib("volatility") is True

    def test_equity_needs_ib_without_massive(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        assert _needs_ib("equity") is True

    def test_equity_skips_ib_with_massive(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "key")
        assert _needs_ib("equity") is False


class TestDispatchModule:
    def test_dispatches_with_argv(self):
        mock_module = MagicMock()
        mock_module.main.return_value = 0
        with patch(
            "scripts.livewire.importlib.import_module", return_value=mock_module
        ):
            result = _dispatch_module("fake.module", ["--flag"], "test")
        assert result == 0
        mock_module.main.assert_called_once_with(["--flag"])

    def test_handles_system_exit_zero(self):
        mock_module = MagicMock()
        mock_module.main.side_effect = SystemExit(0)
        with patch(
            "scripts.livewire.importlib.import_module", return_value=mock_module
        ):
            result = _dispatch_module("fake.module", [], "test")
        assert result == 0

    def test_propagates_nonzero_system_exit(self):
        mock_module = MagicMock()
        mock_module.main.side_effect = SystemExit(1)
        with patch(
            "scripts.livewire.importlib.import_module", return_value=mock_module
        ):
            with pytest.raises(SystemExit):
                _dispatch_module("fake.module", [], "test")

    def test_handles_none_return(self):
        mock_module = MagicMock()
        mock_module.main.return_value = None
        with patch(
            "scripts.livewire.importlib.import_module", return_value=mock_module
        ):
            result = _dispatch_module("fake.module", [], "test")
        assert result == 0


class TestDispatchSync:
    def test_equity_with_massive_adds_source_flag(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "key")
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append((mod, argv, display))
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_sync(["--asset-class", "equity"])

        assert len(dispatched) == 1
        mod, argv, _ = dispatched[0]
        assert mod == "livewire_scripts.daily_update"
        assert "--source" in argv
        assert "massive" in argv

    def test_equity_without_massive_no_source_flag(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append((mod, argv))
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        monkeypatch.setattr("scripts.livewire._needs_ib", lambda ac: False)
        _dispatch_sync(["--asset-class", "equity"])

        mod, argv = dispatched[0]
        assert "--source" not in argv

    def test_volatility_dispatches_cboe(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        monkeypatch.setattr("scripts.livewire._needs_ib", lambda ac: False)
        _dispatch_sync(["--asset-class", "volatility"])

        assert "livewire_scripts.fetch_cboe_volatility" in dispatched

    def test_rates_dispatches_fred(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_sync(["--asset-class", "rates"])

        assert dispatched == ["livewire_scripts.fetch_fred_rates"]

    def test_scheduled_dispatches_job_runner(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_sync(["--scheduled"])

        assert dispatched == ["livewire_scripts.run_daily_update_job"]

    def test_dry_run_passed_through(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append((mod, argv))
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        monkeypatch.setattr("scripts.livewire._needs_ib", lambda ac: False)
        _dispatch_sync(["--asset-class", "rates", "--dry-run"])

        _, argv = dispatched[0]
        assert "--dry-run" in argv

    def test_all_asset_classes_dispatches_four(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "key")
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        monkeypatch.setattr("scripts.livewire._needs_ib", lambda ac: False)
        _dispatch_sync([])

        assert len(dispatched) == 4


class TestDispatchBackfill:
    def test_daily_with_massive_adds_source(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "key")
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append((mod, argv))
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--timeframe", "1d", "--dry-run"])

        mod, argv = dispatched[0]
        assert mod == "livewire_scripts.fetch_ib_historical"
        assert "--source" in argv
        assert "massive" in argv
        assert "--dry-run" in argv

    def test_intraday_dispatches_backfill_intraday(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append((mod, argv))
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--timeframe", "5m", "--dry-run"])

        mod, argv = dispatched[0]
        assert mod == "livewire_scripts.backfill_intraday"
        assert "--timeframe" in argv
        assert "5m" in argv

    def test_all_timeframes_dispatches_five(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(display)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--timeframe", "all", "--dry-run"])

        assert len(dispatched) == 5
        assert "livewire backfill 1d" in dispatched
        assert "livewire backfill 30m" in dispatched

    def test_years_passed_through(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(argv)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--timeframe", "1d", "--years", "3"])

        assert "--years" in dispatched[0]
        assert "3" in dispatched[0]

    def test_preset_passed_through(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(argv)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--timeframe", "1h", "--preset", "presets/sp500.json"])

        assert "--preset" in dispatched[0]
        assert "presets/sp500.json" in dispatched[0]


class TestDispatchCheck:
    def test_default_mode_is_coverage(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_check([])

        assert dispatched == ["livewire_scripts.coverage_report"]

    def test_report_flag(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_check(["--report"])

        assert dispatched == ["livewire_scripts.data_quality_report"]

    def test_weekly_flag(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_check(["--weekly"])

        assert dispatched == ["livewire_scripts.weekly_quality_summary"]

    def test_health_flag(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_check(["--health"])

        assert dispatched == ["livewire_scripts.health_check"]

    def test_universe_flag(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_check(["--universe"])

        assert dispatched == ["livewire_scripts.universe_screener"]

    def test_mode_flag(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_check(["--mode", "watchdog"])

        assert dispatched == ["livewire_scripts.check_daily_update_watchdog"]


class TestDispatchPublish:
    def test_postgres(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_publish(["postgres"])

        assert dispatched == ["livewire_scripts.rebuild_postgres_from_parquet"]

    def test_r2(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_publish(["r2"])

        assert dispatched == ["livewire_scripts.sync_to_r2"]

    def test_postgres_smoke(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_publish(["postgres", "--smoke"])

        assert dispatched == ["livewire_scripts.smoke_postgres_analytical"]

    def test_migrate(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_publish(["--migrate"])

        assert dispatched == ["livewire_scripts.migrate_parquet_filename"]

    def test_no_target_no_migrate_errors(self):
        with pytest.raises(SystemExit):
            _dispatch_publish([])


class TestIBReachable:
    def test_returns_true_when_port_open(self):
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        with patch("socket.create_connection", return_value=conn):
            assert _ib_reachable() is True

    def test_returns_false_when_port_closed(self):
        with patch("socket.create_connection", side_effect=OSError("refused")):
            assert _ib_reachable() is False


class TestSyncIBPreflight:
    def test_futures_triggers_preflight(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        preflight = MagicMock()
        monkeypatch.setattr("clients.ib_gateway_preflight.assert_gateway_up", preflight)
        monkeypatch.setattr("scripts.livewire._dispatch_module", lambda *a, **kw: 0)
        _dispatch_sync(["--asset-class", "futures"])
        preflight.assert_called_once()


class TestSyncForceFlag:
    def test_force_passed_through(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(argv)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        monkeypatch.setattr("scripts.livewire._needs_ib", lambda ac: False)
        _dispatch_sync(["--asset-class", "rates", "--force"])

        assert "--force" in dispatched[0]


class TestSyncFull:
    def test_full_dispatches_sync_runner(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_sync(["--full"])

        assert dispatched == ["livewire_scripts.sync_runner"]


class TestBackfillFull:
    def test_full_dispatches_backfill_runner(self, monkeypatch):
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--full"])

        assert dispatched == ["livewire_scripts.backfill_runner"]


class TestBackfillSkipExisting:
    def test_skip_existing_passed_through(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(argv)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--timeframe", "5m", "--skip-existing"])

        assert "--skip-existing" in dispatched[0]


class TestBackfillIntradayMassive:
    def test_intraday_with_massive_adds_source(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_API_KEY", "key")
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(argv)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--timeframe", "5m", "--years", "2"])

        argv = dispatched[0]
        assert "--source" in argv
        assert "massive" in argv
        assert "--years" in argv
        assert "2" in argv


class TestHasS3Keys:
    def test_returns_true_when_both_set(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY", "ak")
        monkeypatch.setenv("MASSIVE_S3_SECRET_KEY", "sk")
        assert _has_s3_keys() is True

    def test_returns_false_when_access_key_missing(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY", raising=False)
        monkeypatch.setenv("MASSIVE_S3_SECRET_KEY", "sk")
        assert _has_s3_keys() is False

    def test_returns_false_when_secret_key_missing(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY", "ak")
        monkeypatch.delenv("MASSIVE_S3_SECRET_KEY", raising=False)
        assert _has_s3_keys() is False

    def test_returns_false_when_both_missing(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY", raising=False)
        monkeypatch.delenv("MASSIVE_S3_SECRET_KEY", raising=False)
        assert _has_s3_keys() is False


class TestBackfillSourceS3:
    def test_s3_dispatches_flatfile_ingest(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY", raising=False)
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append((mod, display))
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--source", "s3", "--preset", "presets/sp500.json"])

        assert len(dispatched) == 2  # 1d (daily) + s3 flat file
        assert dispatched[1][0] == "livewire_scripts.ingest_flatfiles"
        assert "s3" in dispatched[1][1]

    def test_s3_auto_detected_with_keys(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY", "ak")
        monkeypatch.setenv("MASSIVE_S3_SECRET_KEY", "sk")
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append((mod, display))
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--timeframe", "1m", "--preset", "presets/sp500.json"])

        assert dispatched[0][0] == "livewire_scripts.ingest_flatfiles"

    def test_s3_passes_years_through(self, monkeypatch):
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        monkeypatch.delenv("MASSIVE_S3_ACCESS_KEY", raising=False)
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append((mod, argv))
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(
            ["--source", "s3", "--preset", "presets/sp500.json", "--years", "3"]
        )

        s3_call = [d for d in dispatched if d[0] == "livewire_scripts.ingest_flatfiles"]
        assert len(s3_call) == 1
        assert "--years" in s3_call[0][1]
        assert "3" in s3_call[0][1]

    def test_s3_not_used_for_non_equity(self, monkeypatch):
        monkeypatch.setenv("MASSIVE_S3_ACCESS_KEY", "ak")
        monkeypatch.setenv("MASSIVE_S3_SECRET_KEY", "sk")
        dispatched = []

        def capture(mod, argv, display):
            dispatched.append(mod)
            return 0

        monkeypatch.setattr("scripts.livewire._dispatch_module", capture)
        _dispatch_backfill(["--timeframe", "5m", "--asset-class", "volatility"])

        assert dispatched[0] == "livewire_scripts.backfill_intraday"

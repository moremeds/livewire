"""The preflight belongs to the phases that use IB, not to the orchestrators.

`daily-backfill` has nine phases and two of them use IB. Gating the whole
orchestrator on the Gateway meant that on 2026-08-08 and 2026-08-09 the Massive
equity day_aggs lane, the Massive flat-file intraday lane, FRED rates and CBOE
all failed to run because of a dependency none of them have. Friday 2026-08-07
is absent from bronze warehouse-wide as a result.
"""

from scripts.livewire_ingest import _requires_ib_preflight, main


class TestOrchestratorsDoNotGateOnIB:
    def test_daily_backfill_does_not_require_preflight(self):
        assert _requires_ib_preflight("daily-backfill", []) is False

    def test_backfill_all_does_not_require_preflight(self):
        assert _requires_ib_preflight("backfill-all", []) is False

    def test_shepherd_universe_does_not_require_preflight(self):
        assert _requires_ib_preflight("shepherd-universe", ["scan", "--index", "sp500"]) is False


def test_shepherd_universe_dispatches_without_gateway(monkeypatch):
    captured = {}

    def fake_dispatch(module_name, argv, display_name):
        captured.update(module=module_name, argv=argv, display=display_name)
        return 0

    monkeypatch.setattr("scripts.livewire_ingest._dispatch_module", fake_dispatch)
    assert main(["shepherd-universe", "scan", "--index", "sp500"]) == 0
    assert captured == {
        "module": "livewire_scripts.shepherd_universe",
        "argv": ["scan", "--index", "sp500"],
        "display": "livewire_ingest.py shepherd-universe",
    }


class TestTheIBPhasesStillGate:
    def test_intraday_backfill_still_requires_preflight(self):
        # This is what sync_runner Phase 5 actually invokes.
        assert (
            _requires_ib_preflight(
                "intraday-backfill",
                ["--source", "ib", "--asset-class", "volatility", "--timeframe", "30m"],
            )
            is True
        )

    def test_daily_still_requires_preflight_by_default(self):
        assert _requires_ib_preflight("daily", ["--asset-class", "equity"]) is True

    def test_daily_with_massive_source_does_not(self):
        assert _requires_ib_preflight("daily", ["--asset-class", "equity", "--source", "massive"]) is False

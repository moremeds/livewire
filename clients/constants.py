"""The one place every operating constant in this repo lives.

Each key is scope-bound (spec docs/superpowers/specs/2026-09-02-livewire-ledger-design.md
section 4): the 5 req/min FX limit is FX-only, the lane budgets are one value
per lane. The scope is the part of the key after '/'; a key with no '/' is
genuinely global. A scope-bound number declared without its scope is a bug in
this file, not an exception to the rule.

`run_daily_update_job.main()` emits this whole dict as
`measurements(source='declared')` once per run, splitting each key into
(name, scope) at emit time. `status.py` compares those rows against the
14-day p95 of `source='measured'` rows with the same (name, scope) and WARNs
on a >2x drift.
"""

from __future__ import annotations

import os

#: The nightly daily-update lanes in run order. No-fallback lanes first: the
#: IB-only lanes take minutes and cannot be back-sourced, so they never queue
#: behind a 3-8h Massive lane (pm:2026-07-28-daily-job-deadline-is-a-total).
#: `run_daily_update_job` runs this list and `status` grades it; a lane that is
#: not in this tuple exists in neither.
LANE_ORDER: tuple[str, ...] = (
    "futures",
    "cmdty",
    "cboe",
    "fx",
    "corporate-actions",
    "equity",
    "silver",
    "catalog",
)

#: Lanes with no provider fallback. A down Gateway degrades these rather than
#: manufacturing a success (pm:2026-07-22-ib-not-a-single-point-of-failure).
IB_ONLY_LANES: tuple[str, ...] = ("futures", "cmdty")

# key -> (value, unit). Scope is the segment after '/', absent when global.
DECLARED: dict[str, tuple[float, str]] = {
    # Per-lane wall-clock budgets. One per lane in run_daily_update_job.LANE_ORDER,
    # plus the fallback used when a scope is not a known lane.
    "lane_budget_s/futures": (30 * 60, "s"),
    "lane_budget_s/cmdty": (30 * 60, "s"),
    "lane_budget_s/cboe": (30 * 60, "s"),
    "lane_budget_s/fx": (30 * 60, "s"),
    "lane_budget_s/corporate-actions": (3 * 60 * 60, "s"),
    "lane_budget_s/equity": (2 * 60 * 60, "s"),
    # Measured full rebuilds on the mini: 2026-08-27 2h12m, 08-28 2h15m,
    # 08-30 2h51m, 08-31 1h18m. 7200 was a guess and it killed the first
    # budgeted run at 7201s on 2026-09-07; 4h clears the longest by 1h09m.
    "lane_budget_s/silver": (4 * 60 * 60, "s"),
    # Reuse the established default until daily-run measurements are recorded;
    # the 527s intraday catalog observation is not a daily cold-run budget.
    "lane_budget_s/catalog": (30 * 60, "s"),
    "lane_budget_s/default": (30 * 60, "s"),
    # How long a lane is expected to wait for the lake-io lock. Global on
    # purpose: this is a property of the lock, not of a lane. 2340s is the
    # measured lake-alone corporate-actions pass on the mini, 2026-09-04 14:09Z
    # (14,839 symbols, 39 minutes) -- the longest single hold an intraday phase
    # can queue behind under the 05:00Z / 10:00Z order. `status` grades it
    # against the 14-day p95 of the per-lane measured rows and reads UNKNOWN
    # until those rows exist.
    "lake_lock_wait_s": (2340, "s"),
    # Priority, expressed as how often each job looks. The daily job takes a
    # freed lock within a second; the intraday job looks once a minute, so a
    # daily lane waiting at the moment of a release wins by 59s of margin. No
    # queue, no fairness logic (spec 2026-09-06 section 3).
    "lake_lock_poll_s/daily": (1, "s"),
    "lake_lock_poll_s/intraday": (60, "s"),
    # Share of attempted symbols that may fail before a run counts as systemic.
    "failure_rate_tolerance": (0.05, "ratio"),
    # Massive flat-file GET floor, rolling. Derived from the scan date, never
    # hardcoded as a date (pm:2026-07-29-massive-floor-derived-from-scan-date).
    "massive_window_days": (1827, "days"),
    # Massive REST FX plan: 5 succeed, the 6th 429s, no Retry-After. FX-scoped.
    "massive_requests_per_minute/fx": (5, "per_min"),
    # Massive REST /v3/reference/tickers. Massive's own knowledge base says paid
    # plans have no request-per-minute cap and asks callers to stay under 100
    # req/s; the 5/min above is the FREE Currencies tier and must never be
    # copied to a Stocks-plan endpoint. This scope was seeded from it anyway and
    # paced the first backfill at 12 s/request — 40+ hours of pure sleep for a
    # job whose requests cost 0.6 s each
    # (pm:2026-09-16-reference-rate-inherited-the-free-fx-tier). 600/min is a
    # tenth of the documented ceiling and the rate the 2026-09-16 backfill ran
    # at; at that pace the sleep (0.1 s) is below the round-trip, so the run is
    # latency-bound and raising this further buys nothing.
    "massive_requests_per_minute/reference": (600, "per_min"),
    # The single 429 backoff for that endpoint; a second 429 is a fetch failure,
    # not a longer wait.
    "massive_backoff_s/reference": (60, "s"),
    # Minimum share of a raw flat file's ticker set a publish must cover.
    "flatfile_min_publish_ratio": (0.9, "ratio"),
    # Coverage ratio below which the surface and the digest complain.
    "coverage_alert_threshold": (0.95, "ratio"),
    # Free space a flat-file plan requires before it starts.
    "flatfile_min_free_gb": (25, "GB"),
    # FRED transport retry. api.stlouisfed.org answered one DGS5 request with a
    # 502 on 2026-09-14 and read-timed-out twice in July; each cost a page and
    # the three series after the failing one
    # (pm:2026-09-14-fred-502-aborted-remaining-series). Bounded on purpose:
    # rates are four requests, so a real outage must still fail the phase fast:
    # worst case 4 series x 3 attempts x 30s httpx timeout + 4 x (2+4)s backoff
    # = 384s, against the 6h sync_runner.phase_timeout_seconds budget.
    "fred_retry_attempts": (3, "count"),
    "fred_retry_backoff_s": (2, "s"),
    # CBOE transport retry, the same shape and the same reasoning as FRED's
    # above. The index list is short (a preset of ~10 symbols), so the worst
    # case is bounded the same way: symbols x 3 attempts x 30s timeout.
    "cboe_retry_attempts": (3, "count"),
    "cboe_retry_backoff_s": (2, "s"),
    # EIA API v2. Published limit (eia.gov/opendata/faqs.php, read 2026-09-23):
    # sustained < ~9,000 requests/hour, burst < 5/second; a throttled key is
    # suspended "between a few seconds and a number of minutes" and lifts on
    # its own, so 429 is retried (and only for EIA). 0.5 s between requests
    # caps the rate at 2/s and 7,200/h. The retry waits 30+60+90 s.
    "eia_min_request_interval_s": (0.5, "s"),
    "eia_retry_attempts": (4, "count"),
    "eia_retry_backoff_s": (30, "s"),
    # Window the daily electricity run re-fetches. Observed publication lag on
    # 2026-09-23 was 1-3 days (endPeriod 09-20..09-22); 14 days is margin.
    # Whether EIA revises days older than that has not been measured.
    "eia_electricity_lookback_days": (14, "days"),
}


def _env_key(key: str) -> str:
    """`lane_budget_s/corporate-actions` -> `LW_DECLARED_LANE_BUDGET_S_CORPORATE_ACTIONS`."""
    return "LW_DECLARED_" + key.upper().replace("/", "_").replace("-", "_")


def split_scope(key: str) -> tuple[str, str]:
    """`key` -> `(name, scope)`; scope is "" when the key carries none.

    Used only by the emitter, which writes the ledger's two columns.
    """
    name, _, scope = key.partition("/")
    return name, scope


def declared(key: str) -> float:
    """Return the declared value for `key`, env override applied.

    Raises KeyError if the key is not declared — a call site with a typo fails
    loudly at call time, not silently with a made-up default. A non-numeric
    override raises ValueError for the same reason: a typo'd env var must not
    silently resolve to the declared value.
    """
    value, _unit = DECLARED[key]
    override = os.environ.get(_env_key(key))
    return float(override) if override is not None else float(value)

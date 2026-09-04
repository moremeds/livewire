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
    "lane_budget_s/silver": (2 * 60 * 60, "s"),
    "lane_budget_s/default": (30 * 60, "s"),
    # Share of attempted symbols that may fail before a run counts as systemic.
    "failure_rate_tolerance": (0.05, "ratio"),
    # Massive flat-file GET floor, rolling. Derived from the scan date, never
    # hardcoded as a date (pm:2026-07-29-massive-floor-derived-from-scan-date).
    "massive_window_days": (1827, "days"),
    # Massive REST FX plan: 5 succeed, the 6th 429s, no Retry-After. FX-scoped.
    "massive_requests_per_minute/fx": (5, "per_min"),
    # Minimum share of a raw flat file's ticker set a publish must cover.
    "flatfile_min_publish_ratio": (0.9, "ratio"),
    # Coverage ratio below which the surface and the digest complain.
    "coverage_alert_threshold": (0.95, "ratio"),
    # Free space a flat-file plan requires before it starts.
    "flatfile_min_free_gb": (25, "GB"),
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

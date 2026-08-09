"""Retention must never be able to eat something unrecoverable.

Four categories are protected by name, and the tests assert each survives a run
with retention set aggressively enough to delete everything else:

  raw/            older than the rolling 5-year GET floor cannot be refetched
  repairs/triage/ a verdict obtainable today may be unobtainable next year
  repairs/*/backup/ the only basis for rollback-legacy-basis
  the release `current` points at — deleting it leaves current dangling and
                  promote then refuses to rebuild
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from livewire_scripts import housekeeping
from livewire_scripts.housekeeping import plan_appledouble, plan_housekeeping

_NOW = datetime(2026, 8, 9, 12, 0, 0)  # the fixed "today" every test below passes as `now`


def _touch(path: Path, *, days_old: int = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")
    if days_old:
        # A POSIX timestamp, not ordinal*86400 — ordinals count from year 1, so
        # that arithmetic lands the mtime somewhere around the year 4000 and the
        # age comparison silently inverts.
        stamp = (_NOW - timedelta(days=days_old)).timestamp()
        os.utime(path, (stamp, stamp))
    return path


# Checked against BOTH planners. `plan_housekeeping` no longer walks the lake at
# all, so on its own these would pass vacuously — and a protection test that
# cannot fail is worse than none, because it reads as coverage.
# `plan_appledouble` is the function that does the recursive walk, so it is the
# one that has to be proven not to enter these trees.
@pytest.mark.parametrize(
    "relative",
    [
        # older than the rolling 5-year GET floor; cannot be refetched, ever
        "raw/massive/us_stocks_sip/day_aggs_v1/date=2021-07-28/part.parquet",
        # a verdict obtainable today may be unobtainable next year
        "repairs/triage/current.json",
        # the only basis rollback-legacy-basis has
        "repairs/yahoo-relabel-batch1/backup/NVDA.parquet",
        # 12,636 verbatim .bak files from the 2026-07-15 cutover: operator call
        "repairs/adjusted-silver-cutover-20260715-production/A.abc.parquet.bak",
        # the protection must hold for the sidecars inside those trees too
        "raw/massive/us_stocks_sip/day_aggs_v1/date=2021-07-28/._part.parquet",
    ],
)
class TestProtectedPathsSurvive:
    def test_the_nightly_sweep_never_plans_it(self, tmp_path, relative):
        lake = tmp_path / "data-lake"
        protected = _touch(lake / relative)
        planned = plan_housekeeping(
            tmp_path / "logs",
            lake,
            log_retention_days=0,
            keep_evicted=0,
            now=date(2026, 8, 9),
        )
        assert protected not in [p for _, p in planned]

    def test_the_opt_in_lake_walk_never_plans_it(self, tmp_path, relative):
        lake = tmp_path / "data-lake"
        protected = _touch(lake / relative)
        assert protected not in [p for _, p in plan_appledouble(lake)]


class TestRetentionDoesItsJob:
    def test_old_logs_are_planned_and_recent_ones_are_not(self, tmp_path):
        logs = tmp_path / "logs"
        old = _touch(logs / "daily_update_2026-06-01.log", days_old=90)
        recent = _touch(logs / "daily_update_2026-08-08.log", days_old=1)
        planned = [
            p
            for _, p in plan_housekeeping(
                logs,
                tmp_path / "data-lake",
                log_retention_days=60,
                keep_evicted=2,
                now=date(2026, 8, 9),
            )
        ]
        assert old in planned
        assert recent not in planned

    def test_only_the_oldest_evicted_revisions_are_planned(self, tmp_path):
        lake = tmp_path / "data-lake"
        for rev in ("10", "12", "14", "19", "21"):
            _touch(lake / f"silver/evicted/{rev}/NVDA.parquet")
        planned = [
            str(p)
            for _, p in plan_housekeeping(
                tmp_path / "logs",
                lake,
                log_retention_days=60,
                keep_evicted=2,
                now=date(2026, 8, 9),
            )
        ]
        # Sorted numerically: lexical order would put "10" after "9" and keep
        # the wrong two.
        assert any("evicted/10" in p for p in planned)
        assert any("evicted/12" in p for p in planned)
        assert any("evicted/14" in p for p in planned)
        assert not any("evicted/19" in p for p in planned), "the 2 newest are kept"
        assert not any("evicted/21" in p for p in planned)

    def test_appledouble_is_opt_in_and_never_in_the_nightly_sweep(self, tmp_path):
        lake = tmp_path / "data-lake"
        sidecar = _touch(lake / "bronze/asset_class=equity/symbol=NVDA/._1d.parquet")
        real = _touch(lake / "bronze/asset_class=equity/symbol=NVDA/1d.parquet")

        nightly = [
            p
            for _, p in plan_housekeeping(
                tmp_path / "logs",
                lake,
                log_retention_days=60,
                keep_evicted=2,
                now=date(2026, 8, 9),
            )
        ]
        assert sidecar not in nightly, "an rglob over the whole lake is not a nightly job"

        opt_in = [p for _, p in plan_appledouble(lake)]
        assert sidecar in opt_in
        assert real not in opt_in

    def test_a_missing_log_dir_is_not_an_error(self, tmp_path):
        assert (
            plan_housekeeping(
                tmp_path / "nope",
                tmp_path / "also-nope",
                log_retention_days=60,
                keep_evicted=2,
                now=date(2026, 8, 9),
            )
            == []
        )


class TestDryRunIsTheDefault:
    def test_plan_never_mutates(self, tmp_path):
        logs = tmp_path / "logs"
        old = _touch(logs / "daily_update_2026-06-01.log", days_old=90)
        plan_housekeeping(
            logs,
            tmp_path / "data-lake",
            log_retention_days=60,
            keep_evicted=2,
            now=date(2026, 8, 9),
        )
        assert old.exists(), "planning is read-only"


class TestMainIsDryRunUnlessTold:
    """--apply is the only thing that deletes. Everything else previews."""

    @staticmethod
    def _lake_with_junk(tmp_path):
        logs = tmp_path / "logs"
        lake = tmp_path / "data-lake"
        old = _touch(logs / "daily_update_2026-01-01.log", days_old=200)
        protected = _touch(lake / "repairs/triage/current.json")
        raw = _touch(lake / "raw/massive/us_stocks_sip/day_aggs_v1/date=2021-07-28/part.parquet")
        for rev in ("10", "19", "21"):
            _touch(lake / f"silver/evicted/{rev}/NVDA.parquet")
        return logs, lake, old, protected, raw

    def test_default_run_deletes_nothing(self, tmp_path, monkeypatch):
        logs, lake, old, protected, raw = self._lake_with_junk(tmp_path)
        monkeypatch.setattr(housekeeping, "prune_releases", lambda keep, dry_run: [])

        assert housekeeping.main(["--log-dir", str(logs), "--data-lake", str(lake)]) == 0

        assert old.exists(), "dry run is the default"
        assert protected.exists()
        assert raw.exists()

    def test_apply_deletes_only_the_unprotected(self, tmp_path, monkeypatch):
        logs, lake, old, protected, raw = self._lake_with_junk(tmp_path)
        monkeypatch.setattr(housekeeping, "prune_releases", lambda keep, dry_run: [])

        rc = housekeeping.main(["--apply", "--log-dir", str(logs), "--data-lake", str(lake)])

        assert rc == 0
        assert not old.exists()
        assert not (lake / "silver/evicted/10").exists()
        assert (lake / "silver/evicted/19").exists(), "the 2 newest are kept"
        assert protected.exists(), "repairs/triage is protected by name"
        assert raw.exists(), "raw below the GET floor can never be refetched"

    def test_a_failed_delete_is_counted_not_swallowed(self, tmp_path, monkeypatch):
        """ignore_errors=True would report a clean sweep over surviving files —
        the exact 'green while wrong' shape this branch exists to remove."""
        logs, lake, _old, _protected, _raw = self._lake_with_junk(tmp_path)
        monkeypatch.setattr(housekeeping, "prune_releases", lambda keep, dry_run: [])
        monkeypatch.setattr(
            housekeeping.Path,
            "unlink",
            lambda self, missing_ok=False: (_ for _ in ()).throw(OSError("read-only volume")),
        )

        rc = housekeeping.main(["--apply", "--log-dir", str(logs), "--data-lake", str(lake)])

        assert rc == 1, "a failed delete must not report success"

    def test_appledouble_is_opt_in_at_the_cli_too(self, tmp_path, monkeypatch):
        logs = tmp_path / "logs"
        lake = tmp_path / "data-lake"
        sidecar = _touch(lake / "bronze/asset_class=equity/symbol=NVDA/._1d.parquet")
        monkeypatch.setattr(housekeeping, "prune_releases", lambda keep, dry_run: [])

        housekeeping.main(["--apply", "--log-dir", str(logs), "--data-lake", str(lake)])
        assert sidecar.exists(), "an rglob over 13 TiB is not part of the default sweep"

        housekeeping.main(
            ["--apply", "--appledouble", "--log-dir", str(logs), "--data-lake", str(lake)]
        )
        assert not sidecar.exists()

    def test_releases_are_previewed_in_the_dry_run(self, tmp_path, monkeypatch, caplog):
        logs = tmp_path / "logs"
        lake = tmp_path / "data-lake"
        seen: dict = {}

        def fake_prune(keep, dry_run):
            seen.update(keep=keep, dry_run=dry_run)
            return ["deadbeef"]

        monkeypatch.setattr(housekeeping, "prune_releases", fake_prune)

        with caplog.at_level("INFO"):
            housekeeping.main(["--log-dir", str(logs), "--data-lake", str(lake)])

        assert seen == {"keep": housekeeping.KEEP_RELEASES, "dry_run": True}
        assert "would prune release deadbeef" in caplog.text

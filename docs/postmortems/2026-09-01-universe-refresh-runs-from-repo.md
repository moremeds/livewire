# com.livewire.universe-refresh runs from the REPO, not from the release

**Rule:** Run universe-refresh from the repo (a frozen release forbids writing presets/) and keep the denominator refreshed weekly.

**Incident / measurement:**

⚠️ **`com.livewire.universe-refresh` runs from the REPO, not from the release.**
It is the one warehouse job that cannot use `current`: `release.freeze()` does
`chmod -R a-w` over the release tree and `universe_sync` writes
`presets/*.json` on every run, so from `current` it fails with `PermissionError`
every week. It also needs `shepherd-universe`'s required subcommand and index —
without them the job exited argparse status 2 every week.

**It exists because nothing refreshed the denominator.** `livewire_scripts/universe_sync.py` and
`livewire_scripts/shepherd_universe.py` both existed and **neither was
scheduled**, so index membership changes never reached `presets/*`. An
unrefreshed preset is wrong in both directions: a delisted name is expected
forever, and a new index member is never expected at all. Detection built on a
stale denominator measures the past. The job chains the two through the existing
`scripts/livewire_ingest.py` router (`universe-sync` then `shepherd-universe`,
with `&&` so a failed sync cannot let the shepherd act on stale input), weekly on
Sunday after that day's scan.


**It also had no env, and that failure was silent.** Installed on the mini
2026-09-02; the first launchd-triggered run exited 0 and logged
`MASSIVE_API_KEY not set — skipping dead-ticker check`. The key is present in
`~/market-warehouse/.env` — the job simply never read it, because launchd starts
it cold and it is the only plist that invokes `scripts/livewire_ingest.py`
directly (every other job goes through `livewire_ops.py run-*-job`, which loads
`~/.secrets` → repo `.env` → warehouse `.env` first). So the weekly refresh
would have added new index members and never removed delisted ones: the
denominator drifting in exactly one direction, in the job that exists to stop it
drifting. `universe_sync` degrades rather than failing, so nothing would have
paged. `livewire_ingest.py` now calls `load_scheduled_env` for `universe-sync`
and `shepherd-universe`, the same way `livewire_quality.py` already did for
`watchdog`/`coverage`/`health` — one loader, one key name (`MASSIVE_API_KEY` is
the only Massive REST key in the codebase), no plist-local `source`.

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)

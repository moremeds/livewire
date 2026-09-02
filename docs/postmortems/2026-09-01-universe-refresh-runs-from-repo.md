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


**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)

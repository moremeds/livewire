# The nightly disk line measured the wrong volume

**Rule:** Report both filesystems in `_disk_section`, deduplicated on the `(total, used, free)` triple.

**Incident / measurement:**

- ⚠️ **The nightly disk line measured the wrong volume.** `data-lake` is a
  symlink to `/Volumes/DATA_LAKE`, so `shutil.disk_usage` reported 6.6 TiB free
  every night while the internal volume holding `releases/`, `logs/`, `cursors/`
  and the venv sat at 93% / 14.7 GiB — below the 25 GiB reserve, unreported.
  livewire's own footprint there is only ~2.5 GB, so this is a monitoring gap
  rather than livewire filling the disk; each `release promote` still takes
  another 422 MB. `_disk_section` now reports both, deduplicated on the
  `(total, used, free)` triple read field by field, so a single-filesystem
  deployment still prints one plain `Disk:` line.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)

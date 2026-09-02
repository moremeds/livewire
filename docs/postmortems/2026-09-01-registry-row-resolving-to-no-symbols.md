# The rates registry row pointed at an empty preset and reported all-green

**Rule:** Raise on any zero denominator: a registry row that resolves to no symbols fails the run.

**Incident / measurement:**

- ⚠️ **A row that resolves to no symbols fails the run.** The rates row
  originally pointed at `presets/interests.json`, which is empty, so it reported
  all-green for a reason that had nothing to do with the data — the disk-glob
  failure this engine replaces, reintroduced from the registry side. It now
  points at `presets/rates.json` (`DGS3/DGS5/DGS10/DGS30`, the documented FRED
  series), and `scan` raises on any zero denominator.

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)

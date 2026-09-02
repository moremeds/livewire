# The AppleDouble sweep costs 2047s and must stay opt-in

**Rule:** Keep `--appledouble` opt-in and out of the nightly job, and protect `raw/` and `repairs/` by name rather than by an age rule.

**Incident / measurement:**

- **`housekeeping` prunes logs (60d), releases (keep 3) and superseded evicted
  silver revisions (keep 2).** `raw/` and `repairs/` are protected **by name**,
  never by an age rule: raw below the rolling GET floor cannot be refetched, and
  repairs holds the triage verdict store plus every rollback backup. The 26 GB
  of 2026-07-15 cutover `.parquet.bak` files are out of scope by design. Dry run
  is the default and `release.prune` previews in it too — the review is
  worthless if the 422 MB-per-item category is invisible until `--apply`.
- ⚠️ **The AppleDouble sweep is `--appledouble`, opt-in, and must never go in the
  nightly job.** Finding `._*` means `rglob` over the whole 13 TiB exFAT volume —
  the operation measured at 281s cold for a *single* timeframe glob. Under a
  nightly budget the failure is worse than surviving sidecars: planning finishes
  before anything is deleted, so a traversal that blows the budget deletes
  **nothing**, logs and evicted revisions included, while reporting one warning.
  Measured 2026-08-10, first real run: **2047s (34 min) for 324,121 sidecars**,
  `user+sys` 51s — 97.5% I/O wait, so this cannot be threaded out either. It is
  **3.4× the retired 600s housekeeping budget on its own**, which is the whole
  argument. The dry run named no path under `raw/` or `repairs/` and never named
  the release `current` points at.

Silver artifacts are published beneath `MDW_SILVER_DIR` (default
`data-lake/silver`). Daily files preserve Apex-required OHLCV names and add
`price_adjustment_factor`, `split_volume_factor`, and `adjustment_revision`;
factor files contain exhaustive date intervals. Immutable revision manifests are
written before `current.json`, which is the final cross-file commit record.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)

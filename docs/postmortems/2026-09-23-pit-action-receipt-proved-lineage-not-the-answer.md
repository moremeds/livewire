# PIT action receipts proved the lineage, not the answer, and could not replay

**Rule:** A PIT corporate-action receipt proves each event's head row at as-of
against the latest verified provider page — by provider id and payload hash, or
for a split by date and ratio — and records only what is fixed at as-of. It never
copies the store's live `status` column, which `reconcile` rewrites in place.

**Incident / measurement:** 2026-09-23, host **macmini**, first PIT Silver
publish (sp500 rev 1, ndx100 rev 2, as-of 2026-09-23T00:00Z, Silver rev 77).

Both published `PARTIAL`: sp500 43/458 verified, ndx100 12/87. 413 of the 415
sp500 failures were `missing-event-evidence`. Rows ingested before per-event
provenance landed (2026-08-31, `a9497a7`) carry no `source_ref`, and
`reconcile` counts a same-payload event as `unchanged` without back-filling it,
so no nightly fetch could ever heal them. The export demanded provenance on
every stored row, superseded ones included. Measured the same day: every one of
the 27,294 null-provenance active heads for sp500 sat in the latest verified page
with an identical payload hash. The data was right; the proof asked the wrong
question.

The same day both manifests stopped verifying. At 06:01Z Massive revised AXON
2004-02-11 from 2:3 to 1:3; `reconcile` rewrote the July row's `status` to
`corrected` in place, and the receipt carried that column as `storageStatus`,
so the replay no longer matched a receipt written two hours earlier. Any
provider revision would have broken every published manifest touching it.

Proving heads by id alone then left 33 splits unresolved: Massive returns the
same split under two ids on alternate days (CSX 2006-08-16 flipped six times
between 09-16 and 09-23).

**What remains unprovable, by design:** heads from providers with no page of
their own — yahoo repairs, `eod_fx` conversions, `legacy` rows — are
`non-massive-head-without-proof`. After the change: ndx100 83/87, sp500 440/458,
all remaining blockers of that kind or FLT/JEC (pre-rename tickers).

**Test:** `tests/test_shepherd_actions.py::test_a_provider_revision_after_as_of_does_not_change_the_replay`,
`::test_a_null_provenance_july_lineage_row_does_not_block_a_head_proven_by_the_latest_page`,
`::test_a_split_the_provider_lists_under_another_id_is_proven_by_its_content`,
`tests/test_pit_silver_revision.py::test_a_v1_receipt_cannot_be_replayed`.

**The same afternoon, twice more.** The first v2 republish (release `a2e065c`) failed
before writing anything:
- sp500: its fresh receipt no longer matched its own replay seconds later. The
  corporate-actions lane was writing the store at the same moment, and `reconcile`
  still rewrote superseded rows to `corrected` in place.
- ndx100: it would have been blocked by `_recover_orphans`, which verifies the
  current revision (a v1 receipt) before allocating the next one, and v2 refuses to
  replay v1. (Flagged by apex-ea's spec review.)

Now the store is append-only: neither `reconcile` nor the dividend conversion
rewrites a superseded row. Every reader goes through `latest_active()`, which picks
the max revision. A manual DuckDB query over the raw `corporate_actions` view that
filters `status = 'active'` will now also see superseded rows. Heads rewritten
before the change are dated by their successor: same payload means a revived
cancellation, a different payload means a revised active row. A legacy-version
current revision is checked for integrity only and then superseded. A
current-version revision that fails replay still blocks.

**Test:** `tests/test_shepherd_actions.py::test_a_head_rewritten_in_place_before_the_store_went_append_only_reads_its_as_of_status`,
`tests/test_pit_silver_revision.py::test_a_legacy_current_revision_is_superseded_after_an_integrity_check`,
`::test_a_current_version_revision_that_no_longer_replays_still_blocks_the_next_publish`.

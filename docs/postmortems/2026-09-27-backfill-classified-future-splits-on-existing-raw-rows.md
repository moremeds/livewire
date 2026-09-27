# Backfill classified a future split on existing raw rows, not on IB's own seam

**Rule:** `prepare_ib_rows_for_publish` widens `as_of_date` to cover whatever
existing rows it was given (`max(as_of_date, *existing_dates)`) before deciding
which corporate actions are "effective" — a backfill caller's `as_of_date` is
often the last date in the incoming batch, not real calendar-today, and a
split dated after that batch but on or before the existing data's own latest
date has provably already happened. Once effective, a split whose `ex_date`
falls after the last incoming IB row is classified from the **seam** between
that last IB row and the first existing row after it, never at the split's
own real ex_date, where both adjacent rows are existing data and only
reproduce the split's already-correct raw jump. `last_ib_date` is passed
explicitly from the *incoming* batch (`prepare_ib_rows_for_publish` computes
it from `staged`, not from every `source == 'ib'` row in the combined set) —
recent bronze rows are commonly `source='ib'` too, and inferring it from the
combined set let an existing IB-sourced row dated after the split hide the
seam entirely. Seam splits are further partitioned: one whose `ex_date` falls
*in* the gap (on or before the first existing row past the seam) has already
happened and shows its real jump there, same as an in-window split (raw
target = its own factor, adjusted target = 1); one dated *later still* (the
existing series has already resumed before it) only shows IB's inverted
pre-adjustment (raw target = 1, adjusted target = 1/factor) — the two
groups' targets multiply independently, and a seam spanning more than 5
trading days is refused as ambiguous rather than read as a single boundary
step.

**Incident / measurement:** 2026-09-27, host **macmini**,
`fetch_ib_historical.py --tickers SVXY VXX QLD UVXY --backfill --source ib
--years 20` (`logs/volETF-ib-backfill-20260927T1414Z.log`).

SVXY (2,437 rows, 2011-10-04..2021-06-10) and VXX (855 rows, 2018-01-18..
2021-06-10) were inserted into bronze as `price_basis='raw'` on a mixed basis.
Splits with `ex_date` inside the incoming window reversed correctly. Splits
after it did not: SVXY's 2024-04-11 1:2 split and VXX's two 2023-03-07 /
2024-07-24 4:1 reverse splits were IB-adjusted throughout the incoming block
but never divided out, because `classify_split_events` measured each at its
own `ex_date` against **existing** rows on both sides (the incoming block
never reaches that date) — a real raw jump on the existing side, so it read as
already raw and was left alone. Evidence: SVXY IB close 2021-06-10 = 27.045 vs
existing raw close 2021-06-11 = 54.83 (fold 2.0, matching
`seed_boundary.predict_boundary_fold`); VXX 507.68 vs 30.82 (fold 16.0). A
lake-wide seam scan found the same shape on XLK, XLY, XLB, XLU, XLE (all a
2:1 split on 2025-12-05 for the sector-ETF group, or SVXY/VXX's own dates) —
7 symbols, ~3,300 rows total. Two lookalikes on the same scan, OUST and NCMI,
were checked and ruled out: their seams sit on real 10:1 reverse-split ex-
dates with correct raw jumps, not this bug.

The same run failed closed on QLD (`ValueError: ambiguous split
classifications: 40a5b16c...`, 3,265 fetched bars, 0 written) for its
2015-05-20 1:2 split — an **in-window** split, not this bug (the log carries
no `SplitClassification` fields, only the action id). A read-only replay of
the *exact* backfill chunk call for the window covering this date
(`duration='1 Y', end_date='20150613-00:00:00'`, matching
`compute_date_windows` for QLD's real range) reconstructs the row pair the
classifier compared: 2015-05-19 close 4.8078, 2015-05-20 close 4.8131 — an
ordinary daily move that classifies cleanly as `adjusted`. This rules out
both the seam bug above and a window-chunk-boundary artifact (2015-05-20 is
mid-window, 11 months and 3+ weeks from either boundary). The real cause is
unresolved: production's fetch ran all 15 windows concurrently via
`asyncio.gather`, which a sequential read-only replay cannot reproduce, and
without executing (mutating) the real path there is no further evidence to
collect — reported, not guessed at, and not papered over with a looser
tolerance.

**What remains unproven:** QLD's actual failure mechanism under concurrent
fetch (chunk-boundary is ruled out; a pacing/dedup artifact is the only
untested candidate). XLF has a separate, unrelated published break (a
2016-09-19 spin-off double-counted in corporate actions) — out of scope here.

**Test:** `tests/test_price_basis.py::test_svxy_post_window_split_classified_from_seam`,
`::test_vxx_two_post_window_reverse_splits_classified_from_seam`,
`::test_xlk_post_window_split_classified_from_seam`,
`::test_post_window_split_out_of_scope_without_existing_rows_past_it`,
`::test_qld_in_window_split_with_real_ib_values_classifies_cleanly`,
`::test_gap_split_classified_like_in_window_not_purely_future`,
`::test_existing_rows_sourced_ib_after_seam_still_widen_and_classify`,
`::test_mixed_gap_and_later_splits_partition_independently`,
`::test_long_gap_seam_is_ambiguous`,
`::test_health_check_shaped_gap_fill_reverses_post_gap_split`,
`tests/test_fetch_ib_historical.py::TestBackfillTickerSplitBasis::test_backfill_reverses_split_after_incoming_window`.

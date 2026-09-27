# Backfill classified a future split on existing raw rows, not on IB's own seam

**Rule:** `prepare_ib_rows_for_publish` widens `as_of_date` to cover whatever
existing rows it was given (`max(as_of_date, *existing_dates)`) before deciding
which corporate actions are "effective" — a backfill caller's `as_of_date` is
often the last date in the incoming batch, not real calendar-today, and a
split dated after that batch but on or before the existing data's own latest
date has provably already happened. Once effective, a split whose `ex_date`
falls after the last incoming IB row is classified from the **seam** between
that last IB row and the first existing row after it (`observed = 1.0` raw
hypothesis, `1.0 / combined_factor` adjusted hypothesis — the seam ratio is the
inverse of the in-window one, because the factor already baked into the IB
side is being undone by the existing side, not appearing as a fresh jump
between two homogeneous rows), never at the split's own real ex_date, where
both adjacent rows are existing data and only reproduce the split's already-
correct raw jump.

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
2015-05-20 1:2 split — an **in-window** split, not this bug. A read-only
re-fetch of the same two IB bars (2015-05-19 4.8078, 2015-05-20 4.8131, no
bronze write) classifies cleanly as `adjusted`; the real values IB serves for
that pair are not ambiguous. The production ambiguity most likely came from a
discontinuity at a boundary of QLD's 15-window chunked historical fetch
(2006-06-21..2021-06-11) landing near this split, not from the classifier —
unfixed, tracked as a follow-up, not papered over with a looser tolerance.

**What remains unproven:** whether QLD's 15-window chunk boundaries are
introducing artificial adjustment discontinuities generally (only this one
instance was checked). XLF has a separate, unrelated published break (a
2016-09-19 spin-off double-counted in corporate actions) — out of scope here.

**Test:** `tests/test_price_basis.py::test_svxy_post_window_split_classified_from_seam`,
`::test_vxx_two_post_window_reverse_splits_classified_from_seam`,
`::test_xlk_post_window_split_classified_from_seam`,
`::test_post_window_split_out_of_scope_without_existing_rows_past_it`,
`::test_qld_in_window_split_with_real_ib_values_classifies_cleanly`,
`tests/test_fetch_ib_historical.py::TestBackfillTickerSplitBasis::test_backfill_reverses_split_after_incoming_window`.

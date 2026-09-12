# Coverage 1d: the due gate outran the schedule

Rule: a denominator gate keyed to a delivery instant is only as honest as the
schedule relative to that instant — coverage at 11:00Z cannot measure a session
the rule calls due at 15:00Z, and "0/0" must print UNKNOWN, never 100%.

Observed on `moremeds-Mini`, 2026-09-12 (diagnosis over logs + ledger).

Since 2026-09-09 every weekday coverage run printed `1d=0/0 (100.00%)`.
Cause: `compute_coverage` returns `CoverageResult(0,0)` when
`session_due_at(target) > as_of`, and `session_due_at` (PR #95) sets a session
due at next-day 06:00Z + 9h delivery allowance = 15:00Z. The job fires at 19:00
HKT = 11:00Z, four hours early, so every weekday target is gated to zero. The
first masked day is exactly 09-09: over Labor Day weekend every run targeted
Fri 09-04, already due, so 09-06/07/08 ledger rows still showed ~13,548. The
`_symbols.parquet` hypothesis was rejected on evidence — raw partitions existed
for 09-09/10/11 and intraday scopes (which do use the traded set) reported real
denominators on the same runs. The 1d bars themselves were on disk: the daily
job finishes publishing before the 10:00Z deadline.

Two dispositions, decision left to the operator (not this branch): shrink
`DELIVERY_ALLOWANCE_SECONDS` toward the daily job's 4h deadline — shared with
`build_denominator`, a spec-level change — or reschedule coverage to ≥15:30Z
(the stale 23:30 HKT example worked precisely because it postdated the due
instant). Until either lands, the honest surface is UNKNOWN: the rewrite's
0/0 → UNKNOWN rendering and per-scope status grading already ship that.

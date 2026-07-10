# Massive Trade-Date Audit

Date: 2026-07-10

## Provider Evidence

Raw AAPL timestamps were read from Massive REST grouped-daily responses and
original S3 `day_aggs_v1` CSV files. No staged parquet was used as timestamp
evidence.

| Trade date | REST `t` (ms) | REST UTC | S3 `window_start` (ns) | S3 UTC |
| --- | ---: | --- | ---: | --- |
| 2024-01-03 | 1704315600000 | 21:00 | 1704258000000000000 | 05:00 |
| 2024-03-08 | 1709931600000 | 21:00 | 1709874000000000000 | 05:00 |
| 2024-03-11 | 1710187200000 | 20:00 | 1710129600000000000 | 04:00 |
| 2024-06-03 | 1717444800000 | 20:00 | 1717387200000000000 | 04:00 |
| 2024-11-01 | 1730491200000 | 20:00 | 1730433600000000000 | 04:00 |
| 2024-11-04 | 1730754000000 | 21:00 | 1730696400000000000 | 05:00 |

REST encodes the regular-session close. S3 encodes midnight in
`America/New_York`. The instants differ, but both map to the same Eastern trade
date across standard time and both DST transitions.

## Bronze Audit

A read-only scan compared consecutive daily bars for exact duplicate OHLCV and
also checked every staged day-aggs bucket's `trade_date` statistics against its
`date=YYYY-MM-DD` provider partition.

- Bronze files scanned: 13,080
- Bronze rows scanned: 18,945,253
- Bronze read errors: 0
- Adjacent exact-OHLCV candidates: 24,948
- Zero-volume candidates: 21,178
- Positive-volume candidates: 3,770
- Candidates during the S3 overlap period: 3,608
- Raw S3 partitions scanned: 1,273
- Raw bucket files scanned: 40,736
- Raw staged rows checked: 13,996,124
- Raw `trade_date` versus partition mismatches: 0
- Raw read errors: 0

The adjacent duplicate signature is not specific to timestamp corruption: most
candidates are zero-volume rows, and 21,340 candidates predate the S3 overlap
period. The direct staged-row check found no neighboring-date shift.

## Conclusion

No existing parquet repair is required. The code change centralizes the shared
Eastern-calendar rule and corrects fixtures/comments so future REST and S3
changes cannot introduce convention drift.

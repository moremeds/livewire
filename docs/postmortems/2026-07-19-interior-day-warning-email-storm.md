# A single missing interior day was a warning, and warning is the email threshold

**Rule:** Score a single missing interior day `info`, never `warning` — a universe sweep cannot be damped by the rate limiter.

**Incident / measurement:**

- ⚠️ **A single missing interior day was a `warning`, and `warning` is the email
  threshold.** `_INTERIOR_GAPS_WARNING_DAYS = 1` sat in `quality_detector.py`
  declared and never read, so one absent day scored `warning` and mailed. On
  2026-07-19 that sent ~150 emails in 20 minutes (SAAQW, SBCWW, SLND.WS,
  WENC.U, TDACU, XRPNU …), all `missing_days_count` 1–2, and left **4,408**
  undelivered. The rate limiter cannot damp a universe sweep: its key is
  `(source, ticker, category)` and 13K tickers never repeat one. One absent day
  on an illiquid warrant is a no-trade day — coverage already refuses to count
  "absent from the day's raw traded set" as missing. It is now `info`: still
  detected, still in the sidecar and the audit JSONL, no longer paged.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)

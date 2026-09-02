# no_trade must never make a run look failed, including in the watchdog

**Rule:** Gate `stale_equity_summary` on `updated == 0 and errors` — never on `no_trade`.

**Incident / measurement:**

- ⚠️ **`no_trade` must never make a run look failed, including in the watchdog.**
  `stale_equity_summary` fired on `updated == 0 and (errors or no_trade)`. The
  job runs at 06:00 UTC *daily*, so the UTC-Sunday and UTC-Monday runs target
  the same Friday the UTC-Saturday run already published: `updated=0` with a
  full `no_trade` sweep and `errors=0` is the **correct** outcome. It paged on
  2026-07-26, 2026-07-27 and 2026-08-03 and would page every quiet weekend. The
  condition is now `updated == 0 and errors`. The case it was written for — a
  day genuinely absent from the lake — is measured by the coverage job against
  real parquet, not guessed from a counter.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)

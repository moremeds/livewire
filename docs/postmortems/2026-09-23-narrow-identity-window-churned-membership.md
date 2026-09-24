# A narrow identity window churned index membership

**Rule:** The membership live diff compares tickers, never `security_id`s. A
researched identity covers its whole membership stretch, not `[add-1d, add+1d)`.
A churn repair rejects only rows the live diff wrote (`effective_at == known_at`).

**Incident / measurement:** host **macmini**, measured 2026-09-23.

`import_researched_identities` (2026-09-16) wrote each Wikipedia/EDGAR identity
as `[add-1d, add+1d)`. `members_effective_at` keeps a member only while its
identity is verified, so sp500 on 2025-01-02 read 237 members instead of 483.

At 2026-09-17T01:00:05Z the live diff resolved every ticker at `now`, which no
narrow window covered: it wrote `remove(old id)` + `add(placeholder)` for 523
sp500 and 27 ndx100 rows, and at 05:17Z the placeholders resolved to new
Massive identities. One company, two ids (A: `sec_ce56…` → `sec_8989…`). PIT
sp500 rev 3 dated 204 of 461 members 2026-09-17.

The repair was nearly wrong twice, both caught on a scratch copy of the mini
lake, not by unit tests:

- R4 first rejected any same-date remove+add of one ticker. The backfill had
  recorded real renames in that shape (TMK → GL 2019-08-08, BHGE → BKR
  2019-10-18, known 2026-09-16), so GL and BKR would have left sp500.
- Ticker diffing alone removed and re-added a renamed member (FLT → CPAY:
  the add is covered by the FLT claim, the live list says CPAY, both resolve
  to one id).
- A rename across the two merged ids (FISV → FI → FISV), re-pointed onto one
  id, became a same-date remove+add of one id; the store orders ties by
  `known_at`, the re-pointed remove won, and Fiserv left sp500 after
  2025-11-11. The second apply then appended 3 more identity rows.
- The GOOGL churn pair only pairs through the R1 merge: its researched id's
  latest claim reads GOOG, not GOOGL. The first unit test lacked that claim and
  passed with the fix removed.

**Cost:** member counts below truth for every date before 2026-09-22 since
2026-09-16; PIT revisions 1–4 were published on them.

→ test: `tests/test_membership_repair_identity.py` · spec
`docs/superpowers/specs/2026-09-23-membership-identity-continuity-design.md`

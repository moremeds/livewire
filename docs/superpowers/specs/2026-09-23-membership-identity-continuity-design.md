# Membership identity continuity — design

Date: 2026-09-23. Status: approved direction (user, 2026-09-23: "身份按成员区间延长";
membership-sync compares tickers; a ledgered repair rejects the 2026-09-17 churn;
PIT republished afterwards). Silver-wide identity coverage (AAC) is a separate PR.

## 1. What is broken (measured on the mini, 2026-09-23)

`IndexMembershipStore.members_effective_at` keeps a member only while
`SecurityMaster.is_verified(security_id, effective_at)` holds.
`import_researched_identities` (2026-09-16) writes each Wikipedia/EDGAR identity
as `[add − 1d, add + 1d)`, so a member is valid on its add date and gone the
next day.

| sp500 `as_of` | store.members | same events, identity gate off |
| ------------- | ------------- | ------------------------------ |
| 2005-01-03    | 5             | 299                            |
| 2025-01-02    | 237           | 483                            |
| 2026-09-01    | 244           | 489                            |
| 2026-09-22    | 454           | 454                            |

The narrow windows then caused churn. `membership_sync` resolves every live
ticker at `now`. A narrow identity does not cover `now`, so at
2026-09-17T01:00:05Z it appended `remove(old id)` plus `add(unresolved:<T>)`,
both with `effective_at = now` (`membership_sync.py:463`). At 05:17Z the
placeholders resolved to new Massive identities starting 2026-09-17 (157 of
them). The result is two `security_id`s for one company: A was `sec_ce56…`
(valid 2000-06-04..06), then `sec_8989…` from 2026-09-17. sp500 has 523 live-diff
rows and ndx100 has 27. PIT sp500 rev 3 has 204 of 461 members with
`effective_from` 2026-09-17.

A CIK joins the two identities: 269 of the 270 identities created on
2026-09-17 share a CIK with a researched identity for the same ticker. HOLX is
the only one whose CIK differs.

## 2. Rules

**R1 — one company, one `security_id`.** For each CIK that has a
`wikipedia_sec_research` identity, the researched `security_id` is canonical.
A `massive` identity with the same CIK and symbol is a duplicate:

- its active claims are superseded as `rejected`;
- the same claim (same provider, FIGIs, MIC and window) is appended under the
  canonical id.

The CIK is the join key and the only one. If the CIKs differ (HOLX), or either
side lacks a CIK, nothing is merged and the pair is reported.

**R1b — history under a merged duplicate moves with it.** An active,
non-churn membership event under a merged Massive duplicate is the same
company's history, joined by CIK. The first dry run on the mini found 105 such
verified events; 100 were written on 2026-09-16 with real historical dates (for
example NVDA in ndx100: add 2004-01-01, remove 2004-12-20, add 2005-12-19).
Each one is superseded as `rejected` under the duplicate and appended
unchanged (action, dates, evidence, status) under the canonical id.

**R2 — a researched identity covers its membership interval.** A researched
claim `[d − 1d, d + 1d)` is superseded by a claim for the same
`(provider, symbol, mic, cik)` covering `[d − 1d, end)`:

- `end` is where the security stops being a member of every index: the end
  of the continuous stretch, starting inside the claim, during which it is in
  at least one index. Leaving one index while staying in another does not end
  it (NVDA left ndx100 on 2004-12-20 and stayed in sp500). While it is still a
  member somewhere, `end` is open (`None`). Events under a merged duplicate
  (R1b) count as the security's own.
- The claim cites the original claim's evidence, the adds inside the claim,
  and the remove that ends the stretch, by their `source_refs`, which are
  already in the source-evidence CAS. The index's constituent record is the
  evidence that ticker T was this constituent from add to remove.

If the extension would collide under `SecurityMaster._validate_append`
(symbol interval, `share_class_figi`, `composite_figi`), `end` is capped at
the start of the colliding claim and the cap is reported. It is never forced.

**R3 — membership-sync diffs tickers, not ids.** The live diff maps each
current member to its ticker:

- a placeholder carries its own ticker;
- a resolved id takes the symbol of its identity claim covering the member's
  last add.

A ticker present on both sides produces no event, even when it now resolves to
a different id. A new live ticker that resolves to a current member is a rename
(FLT → CPAY): neither the add nor that member's remove is written. Only a ticker that enters or leaves the live list produces an
add or remove. The live source has no dates, so `effective_at = now` stays for
genuine changes.

**R4 — reject the churn.** Churn is a same-timestamp pair in one index: a
`remove` written by the live diff (`effective_at == known_at`) and an `add` of
the same security — the same ticker, or two ids R1 merges — plus any later
event that resolves that placeholder. A backfilled rename has the same shape
(TMK → GL on 2019-08-08, known 2026-09-16) and is history, not churn. The
exception is a same-date remove+add across two ids R1 merges (FISV → FI on
2023-06-07 and back on 2025-11-11): the security never left the index, so the
pair is rejected, not re-pointed. Re-pointed onto one id, the store's
`known_at` order would let the remove win the tie. Each churn row that is still
active gets a `rejected` row that supersedes it (revision + 1). A verified
membership event that still references a rejected Massive duplicate after R1b
fails the run and is reported; it is never guessed.

## 3. Entrypoint

`livewire_ingest.py membership-sync repair-identity [--index sp500 ndx100 djia] [--apply]`:

- Dry run by default: computes R1, R1b, R2 and R4, and writes a JSON manifest
  (merges, repoints, extensions, caps, rejections, conflicts, before/after member counts
  at fixed dates) plus one ledger `runs` row and its `measurements`.
- `--apply` appends security-master rows first (R1, then R2), then membership
  rejections and re-points (R4, R1b), in one run. It is idempotent: event ids derive from the
  superseded id, so a rerun appends nothing.
- It lives in `membership_sync.py`, next to `reresolve`. No new script.

## 4. Acceptance (checked on a scratch copy of the mini lake before `--apply`)

1. sp500 `members_effective_at` for 2015-01-02, 2025-01-02 and 2026-09-01 are
   each within 5 of the gate-off count of the same events after R4, and none is
   below 480.
2. After R4, no sp500 or ndx100 member has `effective_from` 2026-09-17
   unless the ticker was genuinely added then (no paired remove).
3. A `membership-sync --dry-run` right after the repair reports zero adds and
   zero removes for tickers already in the index.
4. Re-running `repair-identity --apply` appends nothing.
5. `pit_silver_revision verify` still passes for existing revisions 1–4.
   After a republish, member counts match (1).

## 5. Out of scope

- Real historical dates for genuine live-diff adds (the live source has none).
- Identities for Silver symbols outside any index (AAC): separate PR.
- The ~1216 membership placeholders the security master could never resolve.

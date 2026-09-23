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
| --- | --- | --- |
| 2005-01-03 | 5 | 299 |
| 2025-01-02 | 237 | 483 |
| 2026-09-01 | 244 | 489 |
| 2026-09-22 | 454 | 454 |

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

**R2 — a researched identity covers its membership interval.** A researched
claim `[d − 1d, d + 1d)` is superseded by a claim for the same
`(provider, symbol, mic, cik)` covering `[d − 1d, end)`:
- `end` is the `effective_at` of the next non-churn `remove` of that security
  in any index. If no such remove exists, `end` is open (`None`).
- The claim cites the original claim's evidence and the add and remove events'
  `source_refs`, which are already in the source-evidence CAS. The index's
  constituent record is the evidence that ticker T was this constituent from
  add to remove.

If the extension would collide under `SecurityMaster._validate_append`
(symbol interval, `share_class_figi`, `composite_figi`), `end` is capped at
the start of the colliding claim and the cap is reported. It is never forced.

**R3 — membership-sync diffs tickers, not ids.** The live diff maps each
current member to its ticker:
- a placeholder carries its own ticker;
- a resolved id takes the symbol of its identity claim covering the member's
  last add.

A ticker present on both sides produces no event, even when it now resolves to
a different id. Only a ticker that enters or leaves the live list produces an
add or remove. The live source has no dates, so `effective_at = now` stays for
genuine changes.

**R4 — reject the churn.** Churn is a same-timestamp pair in one index: a
`remove` of ticker T and an `add` of T (placeholder or resolved), plus any
later event that resolves that placeholder. Each churn row that is still
active gets a `rejected` row that supersedes it (revision + 1). A verified
membership event that still references a rejected Massive duplicate after R1
fails the run and is reported; it is never guessed.

## 3. Entrypoint

`livewire_ingest.py membership-sync repair-identity [--index sp500 ndx100 djia] [--apply]`:
- Dry run by default: computes R1, R2 and R4, and writes a JSON manifest
  (merges, extensions, caps, rejections, conflicts, before/after member counts
  at fixed dates) plus one ledger `runs` row and its `measurements`.
- `--apply` appends security-master rows first (R1, then R2), then membership
  rejections (R4), in one run. It is idempotent: event ids derive from the
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

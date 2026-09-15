# Tribunal review record — PR #134 (security master backfill)

Review-cycle run 2026-09-15 on `feat/security-master-backfill`, base `7f2791b`.
Reports are verbatim pane captures; each carries its `BEGIN_REVIEW <round> <snapshot>` markers.

| Round | Snapshot (sha256 of `gh pr diff 134`, 12 chars) | HEAD | Seats |
| --- | --- | --- | --- |
| r1 | eee28ad7c929 | 7abf183 | lead (Fable), Codex `gpt-6-astra` (pane w2:p6, `astra-reviewer`), Cursor `cursor-grok-4.6-high` (pane w2:p7, `reviewer-sm`) |
| r2 | ca534d7634dd | 2b1982a | Astra, Grok |
| r3 | 59dc17b16aac | 6039c6b | Astra, Grok |
| r4 | eb466258767f | 19274bd | Astra, Grok |
| r5 | 2498be4d41aa | a88215a | Astra, Grok |

Model identity: read from each pane's status line at start (`gpt-6-astra medium`; `Cursor Grok 4.6 High`).
Both seats ran read-only (`--sandbox read-only`; `--mode ask`); the only approval granted was a read-only
`git rev-parse HEAD && git log -1` in the Cursor pane, once per round.

## Findings and dispositions

| # | Raised by | Finding | Disposition | Commit |
| --- | --- | --- | --- | --- |
| P1 | lead | pacing per ticker against a per-request constant | fixed | 7abf183 |
| A1 | Astra | `known_at` = run start, spec §4 says fetch time | fixed: per-ticker `clock()` | 2b1982a |
| A2 | Astra | FIGI-less probe row stamps another issuer's listing (cik ignored) | fixed: probe must share FIGI or cik | 2b1982a |
| A3 | Astra | rename halves never reach one derive call; second half opens a new id | fixed: `_rename_match` joins the earlier id | 2b1982a |
| A4 / C1a | Astra, Grok | `probe_date=min(dates)` loses later memberships when the earliest is pre-coverage | fixed: walk `probe_dates` ascending | 2b1982a |
| C1b | Grok | placeholders whose only dates are pre-coverage never resolve; current members invisible in verified replay | **escalated to the user** (scope: spec §8 accepted pre-2003 stays unresolved; measured on the mini: sp500 411 of 1209 unresolved tickers have only pre-2003-09-11 dates, 311 more are mixed) | — |
| C2 / L1 | Grok, lead | one evidence commit per run; crash at ticker 2000 discards a day of fetches | fixed: commit+append per 50-ticker chunk | 2b1982a |
| A5 (r2) | Astra | cross-fetch rename joins overlapping intervals (master skips same-id collisions) | fixed: overlap → conflict on own id | 6039c6b |
| A6 (r2) / lead P3 | Astra, lead | revision from matched row collides on multi-symbol ids | fixed: `_next_revision` over the id | 6039c6b |
| C3 (r2) | Grok | mixed pre/post date sets refetched every run | first fix (r3, "before earliest interval") rejected by Astra as unsound; final: `identity_probe_empty` ledger record + `_open_dates` | 19274bd, a88215a |
| A7 (r3) | Astra | widening across a renamed sibling bypasses the overlap guard | fixed | 19274bd |
| A8 (r3) | Astra | interval start treated as proof an earlier date was probed | fixed (see C3) | 19274bd |
| A9 / C4 (r4) | Astra, Grok | recorded-empty dates re-probed when another date is open | fixed: `_open_dates` | a88215a |
| L2 | lead | KeyboardInterrupt leaves the run row open | not fixed; same convention as membership_sync; disclosed | — |

Debate/rebuttal rounds: not run. Every material finding was either verified by the lead against source and
applied, or (C1b) escalated as a scope decision a debate cannot settle.

## Per-worker handoff

- `astra-reviewer` (Codex, requested/observed `gpt-6-astra`, pane w2:p6, cwd = this worktree): five review rounds, read-only, no edits; 9 findings, all verified by the lead and applied. Closed after r5.
- `reviewer-sm` (Cursor, requested/observed `cursor-grok-4.6-high`, pane w2:p7, cwd = this worktree): five review rounds, read-only, no edits; 4 findings, 3 applied, 1 escalated. Closed after r5.
- lead (Claude Fable): own findings P1/L1/L2/P3; all fixes and tests written by the lead.

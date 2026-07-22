# Fix: documentation drift sweep

**Item:** L2 (+ I5: docs/architecture decision) · Severity: low · Status: proposed

## Problem / inventory of drift

All verified against current code during the 2026-07-04 audit:

1. `README.md:640` — claims "100% coverage enforced (fail_under = 100)"; actual gate
   is 95 (`pyproject.toml [tool.coverage.report]`, CI `--cov-fail-under=95`,
   CLAUDE.md agrees).
2. `CLAUDE.md:159` — says `report --email` writes `quality_summary_*.marker`; the
   writer moved to `livewire_scripts/nightly_digest.py` (its own comment at
   `:141-142` says so). Rewrite the `MDW_LOG_DIR` description to name the digest.
   (If `fix-digest-marker-after-send` lands first, describe the new
   marker-after-send semantics.)
3. `.env.example` — still documents removed Cerebras enrichment vars
   (`CEREBRAS_API_KEY*`, `MDW_CEREBRAS_*`) and a `~/.zshrc` fallback the mailer does
   not implement. Delete the section. Also fix `.codex/project-memory.md:29` (stale
   AI-enrichment description).
4. `launchd/com.livewire.daily-update-watchdog.plist.example:18-19` — header says
   "~5.5h after the daily-update window starts at 05:05 UTC"; daily-update moved to
   06:00 UTC, so the buffer is 4.5h. Fix the comment.
5. `launchd/com.livewire.intraday-catchup.plist.example:24` — PDT example
   self-contradicts (`Hour=21 … adjust to Hour=22`). Rewrite as a plain two-row
   table (PDT → Hour=22, PST → Hour=21).
6. **docs/architecture/ (I5, needs a user decision):** five untracked drafts dated
   2026-06-14 (`{livewire,apex,argon,xenon}-adaptation.md`,
   `livewire-adaptation-plan.md`). Untracked = lost on fresh clone, never
   PR-reviewed. Options: (a) commit as-is under `docs/architecture/` marked Draft,
   (b) delete as superseded, (c) move to a personal notes location outside the repo.
   **Recommendation: (a) commit them** — cheapest way to stop the rot; they can be
   deleted later with history.
   EXECUTOR: item 6 requires an owner decision. If there is no `## Decision` section
   in this file recording an explicit choice among (a) commit / (b) delete / (c)
   move-out-of-repo, do NOT `git add`, delete, or move anything under
   `docs/architecture/`. Execute items 1–5 (which need no decision) and SKIP item 6,
   reporting it as blocked-on-decision. Do not default to the recommendation.

   ## Decision

   APPROVED: option (a) — commit the five `docs/architecture/` drafts as-is, marked
   Draft (owner approval, 2026-07-05). Item 6 is unblocked.

## Preconditions (STOP if any differ)

- `README.md:640` contains `fail_under = 100`. (README:641's "ib_client.py and
  historical_provider.py excluded" is ACCURATE — pyproject omits both — leave it.)
- `CLAUDE.md:159` describes `MDW_LOG_DIR` as where `report --email` writes the marker.
- `.env.example:65-71` contains the CEREBRAS block incl. the `~/.zshrc` fallback at :66.
- `.codex/project-memory.md:29` mentions a "Cerebras-generated summary".
- `launchd/com.livewire.daily-update-watchdog.plist.example:18-19` says "~5.5h ... 05:05 UTC".
- `launchd/com.livewire.intraday-catchup.plist.example:24` has the `Hour=21 … adjust
  to Hour=22` PDT row.

If any anchor's content/line differs, STOP and re-locate — do not blind-edit.

## Files to change

- `README.md`, `CLAUDE.md`, `.env.example`, `.codex/project-memory.md`,
  two `launchd/*.plist.example` headers, `docs/architecture/*` (git add, pending
  decision)

## Tests / verification

None — prose only. Per-item verification (run each; all must hold):

1. `grep -rn "fail_under = 100\|100% coverage" README.md` → 0 hits;
   `grep -n "fail_under = 95\|95%" README.md` → ≥1 hit.
2. `grep -n "report --email.*marker" CLAUDE.md` → 0 hits;
   `grep -ni "nightly_digest\|digest.*marker" CLAUDE.md` → ≥1 hit near the
   MDW_LOG_DIR entry.
3. `grep -rn "CEREBRAS\|MDW_CEREBRAS\|\.zshrc" .env.example .codex/project-memory.md` → 0 hits.
4. `grep -n "05:05\|5.5h" launchd/com.livewire.daily-update-watchdog.plist.example` → 0 hits;
   `grep -n "4.5h\|06:00" launchd/com.livewire.daily-update-watchdog.plist.example` → ≥1 hit.
5. `grep -n "adjust to Hour" launchd/com.livewire.intraday-catchup.plist.example` → 0 hits.
6. (only if decision = commit) `git status --porcelain docs/architecture/` shows the
   five files staged, not untracked.

Whole-sweep guard (CHANGELOG.md may legitimately retain historical mentions):
`grep -rn "fail_under = 100\|CEREBRAS\|05:05\|adjust to Hour" README.md CLAUDE.md .env.example launchd/ .codex/` → 0 hits.
After edits, `plutil -lint launchd/com.livewire.daily-update-watchdog.plist.example launchd/com.livewire.intraday-catchup.plist.example`
→ both "OK" (comment-only edits must not break the plists).

## Risks / notes

- Item 2 wording depends on whether `fix-digest-marker-after-send` has merged —
  sequence this PR after it, or write the post-fix wording directly.
- One PR for the whole sweep (single topic: docs truthfulness), including the
  `docs/architecture` commit if approved.

## Acceptance criteria

- Every numbered claim above matches the code on main at merge time.

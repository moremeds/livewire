# seesaw-review: security-master backfill spec

review id: seesaw-secmaster-spec; target class: plan/prose (design spec)
base: origin/main 7f2791b; HEAD 7f2791b; in-scope untracked: docs/superpowers/specs/2026-09-15-security-master-backfill-design.md
snapshot (sha256 prefix of the spec): 1cafca27e4db; workspace: .worktrees/security-master-backfill
round: r1. herd.toml gained [[worker]] astra-reviewer (kind codex, gpt-6-astra, --sandbox read-only) — roster had no Astra seat; uncommitted.
peer seat: astra-reviewer pane w1:p6, requested model gpt-6-astra; observed: pane status line "gpt-6-astra medium" at 2026-09-15 (herdr agent read w1:p6).
Devin mini-runner is a separate task (daily-update monitor), not part of this review.

## Original failure / intended behavior
Production mini: all 4599 membership events unresolved (master has 1 row) -> members_effective_at() empty -> apex 503.
Intended: fill master from Massive reference, reresolve placeholders; PIT stays honest (known_at = reresolve time).

## Must preserve (lead's list, written before reading peer output)
- SecurityMaster.append collision + verified/candidate rules unchanged; spec only appends through it.
- IndexMembershipStore supersede rule (same index_id+security_id); revision sequential.
- PIT: as_of before the reresolve sees the placeholder, never the resolved member.
- corporate-actions currency lookup keeps reading master currency; a wrong currency changes dividend FX.
- Status checks "Unresolved memberships" / "Membership sync ran today" keep grading correctly with the new runs rows.
- Evidence committed once per run; nothing appended without evidence.

## Lead initial assessment (before peer)
Open questions I already hold:
1. Rename rule "two rows rev 1 and 2, no supersedes" — does the master allow two symbol rows under one security_id without supersedes, and is revision per security_id sequential across symbols? Unverified from memory.
2. Double-count risk: after reresolve the placeholder is rejected (excluded from _current_members) and a fresh event appended; but if the SAME ticker later maps to a DIFFERENT security_id (ticker reuse), _resolve at effective_at picks the interval — fine — yet the r2k-proxy candidate + a verified sp500 event for the same security are separate indexes, fine.
3. `provider_reference` as a candidate basis: does append accept an unknown basis string or is there an enum?
4. `identity_fetch_failed > 0 -> exit 1` while ledger row closes normally — consistent with fred-rates pattern? need runs.exit_code semantics.
5. Ledger §5: extra runs row job="membership-sync" from reresolve — the "ran today" check reads latest run per weekday; a reresolve OK row is harmless only if the check does not also require scope/index coverage.
6. Massive `date=` probe semantic: earliest_effective_at may be before 2003 -> 0 rows -> counted unknown even though the ticker exists today under a different name. Spec's step 3 calls date= only when no list_date; acceptable, but a pre-2003 ticker that Massive knows only as active (no list_date) becomes effective_from=probe date? No: probe returns 0 rows -> effective_from falls to... unspecified. Ambiguity to flag.

## Round 1 results
Peer report: /private/tmp/claude-501/-Users-chenxi-projects-livewire/ac09ab71-eb97-4911-81d3-53c6a1b2b795/scratchpad/seesaw-1cafca27e4db/seesaw-peer-1cafca27e4db-r1.txt (verdict FIX, 7 findings, 8 counterexamples). Opus verification worker (model opus) returned file:line checks; used as lead evidence, not a vote.
Lead disposition (each verified by reading the cited lines myself):
- F1 restart safety: ACCEPT (store.append is one row; two appends not transactional; reject-first loses membership on retry). Fix: resolved append first, deterministic event_id, retry completes rejection.
- F2 backlog counts superseded unresolved rows: ACCEPT (membership_sync.py:156,408 count every status=="unresolved" row). Fix: one helper, current rows only, in import/sync/reresolve.
- F3 latest-run masking: ACCEPT (status.py:323-335 order by started desc limit 1). Fix: job="membership-reresolve".
- F4 composite-FIGI-only continuity: ACCEPT (contract priority 3; security_master.py:174 skips same-id rows). Fix: rename requires share-class FIGI match; conflict -> two candidates + identity_conflict; partial existing interval -> new revision on existing id; empty probe cannot backdate.
- F5 confidence promotion: ACCEPT (membership_sync.py:50,147: import maps C/D->candidate; placeholder carries no confidence). Fix: reresolve --confidence required, same contract as import.
- F6 record() not buffered / verifier bytes-only: ACCEPT (source_evidence.py:208-211). Fix: record_many once before appends; raise aborts appends.
- F7 _equity_currency_resolver name + no interval filter: ACCEPT (sync_corporate_actions.py:674-701). Fix: name corrected; test asserts resolver choice; resolver change out of scope.
- Counterexample 7 (remove at interval end): ACCEPT (_contains end-exclusive). Fix: remove takes id of ticker's latest resolved add.
- Counterexample 8 (sync adds resolved id before reresolve): ACCEPT (membership_sync.py:422-450; pit_silver_revision.py:263-265 raises). Fix: fail closed + ordering.
- Opus C3 revision counter across collapsed placeholders: ACCEPT, spec now states the carried counter.
- Opus C4 master needs its own evidence verifier: ACCEPT (membership_sync builds masters with None).
- Opus "pit_silver_revision positional prefix shifts meaning": REJECT — the log is append-only, an earlier prefix N still denotes the same events; only new pins are longer.
- Peer "Silver lane reads index_membership": ACCEPT as wrong claim; replaced with the actual consumers (shepherd_daily, pit_silver_revision, shepherd_repair).
- Citations fixed: PIT rule lives in 2026-09-13 dividend-fx-and-pit-membership spec, contract lines 12-15, exchange_mic param name, import-decision does append.
- Unverified by either reviewer and still so: the production counts in §1/§2 (measured by me on the mini earlier this session, ledger + parquet reads; not re-run for this review).
Round 2 snapshot: 8bb783ccf568; delta /private/tmp/claude-501/-Users-chenxi-projects-livewire/ac09ab71-eb97-4911-81d3-53c6a1b2b795/scratchpad/seesaw-1cafca27e4db/seat-astra/delta-r1-to-r2.diff

## Round 2 results (snapshot 8bb783ccf568)
Peer report: /private/tmp/claude-501/-Users-chenxi-projects-livewire/ac09ab71-eb97-4911-81d3-53c6a1b2b795/scratchpad/seesaw-1cafca27e4db/seesaw-peer-8bb783ccf568-r2.txt — verdict FIX. Closed: F2 F3 F5 F6, CE2 CE3 CE4 CE5 CE6. Open: F1 F4 F7(narrow), CE1 CE7 CE8; new N1-N8.
Lead disposition:
- N1 guard blocks valid removes / N2 recovery after conflict check / N3 guard at effective_at misses later add: ACCEPT all three — my own step 2 was wrong. Fix: recovery first by deterministic event_id; then balanced-replay simulation over the full sequence (same rule pit_silver_revision.py:263-271 enforces).
- N4 known_at start + delisted end rejected by master: ACCEPT (security_master.py:200-202 effective_to <= effective_from raises). Fix: no provable start -> nothing appended, identity_no_start.
- N5 overlap closure inferred / candidate vs unresolved: ACCEPT the overlap part (contract rule 7: time conflict -> not resolvable) -> two candidates + identity_conflict. Keep  for share-class conflicts with the reason written into the table (master's unresolved is for no-record claims).
- N6 earliest-date-only coverage skip: ACCEPT. Fix: check every needed date.
- N7 rows vs distinct ids: ACCEPT (membership_sync.py:156 is a set of ids). Fix: distinct ids among current rows.
- N8 strict-greater tie: ACCEPT (sync_corporate_actions.py:692). Wording + equal-known_at test.
- Constants initial values: ACCEPT — 5 per_min initial (the fx measurement, constants.py:78), 60 s backoff; sample run measures.
- "none scheduled tonight": removed; consumers pin prefixes explicitly (accepted from peer's unverified list).
- Peer withdrew its r1 import-idempotency claim; removed from §9.
Round 3 snapshot: 2ee8a0adc21c (second and last correction round); delta /private/tmp/claude-501/-Users-chenxi-projects-livewire/ac09ab71-eb97-4911-81d3-53c6a1b2b795/scratchpad/seesaw-1cafca27e4db/seat-astra/delta-r2-to-r3.diff

## Round 3 results (snapshot 2ee8a0adc21c) — last correction round
Peer report: seesaw-peer-2ee8a0adc21c-r3.txt — verdict FIX. Closed: F1/CE1/N2, F7/N8, CE7/N1, CE8/N3, N4, N6, N7, constants, "none scheduled". Open: R3-1 (MEDIUM), R3-2 (HIGH), R3-3 (LOW), provider fixtures (prerequisite).
Lead disposition (verified):
- R3-2 replay guard must be verified-only like PIT Silver: ACCEPT (pit_silver_revision.py:262 filters status != verified). Fixed: guard replays events of the status the proposed event will carry.
- R3-1 material conflict is `unresolved` per contract rule 7, not `candidate`; my "reserved for no-record" sentence was invented: ACCEPT (contract lines 76-77). Fixed: overlapping dates and differing share-class FIGIs -> unresolved; missing share-class FIGI -> candidate.
- R3-3 shepherd_repair pins a master prefix, not membership: ACCEPT (grep membership clients/shepherd_repair.py = 0). Fixed wording.
- Provider fixtures: not a spec defect; a prerequisite already stated in §2/§7. Stays open until the implementation PR captures them.
Final snapshot d439f845260f applied by the lead AFTER the peer's last round; the peer has not verified it. Round budget exhausted.

## Report
| Intended change or preserved behavior | Related path | Check / evidence / snapshot | Result or gap |
| --- | --- | --- | --- |
| Reresolve restart-safe, no lost or duplicated membership | index_membership_store.py:73-138, revisioned_parquet.py:56-60 | peer r1 F1, r2 N1-N3, r3 closed; lead read | closed at 2ee8a0adc21c |
| Backlog measure drains and stays drained; status checks not masked | membership_sync.py:156,408; status.py:323-347 | peer r1 F2/F3, r2 closed | closed at 8bb783ccf568 |
| Identity continuity per contract (FIGI, intervals, conflicts) | security_master.py:148-226; contract 43-86 | peer r1 F4, r2 N4/N5, r3 R3-1 | fixed at d439f845260f, unverified by peer |
| Replay guard matches PIT Silver verified-only replay | pit_silver_revision.py:256-271 | peer r3 R3-2 | fixed at d439f845260f, unverified by peer |
| Confidence not promoted by identity resolution | membership_sync.py:50,147,185 | peer r1 F5, r2 closed | closed |
| Evidence committed once, before appends; master wired with a verifier | source_evidence.py:208-219; membership_sync.py:65-72,112 | peer r1 F6, Opus C4, r2 closed | closed |
| Currency resolver behavior stated and tested | sync_corporate_actions.py:674-701 | peer F7/N8, r3 closed | closed |
| Production counts in §1/§2 | mini ledger + parquet | measured earlier this session, not re-run | unverified in this review |
| Massive probe raw bodies as fixtures | tests/fixtures/massive_reference/ | none exist yet | prerequisite, open |
Verdict: FIX — all peer findings have lead-applied fixes, but the final snapshot d439f845260f was not seen by the peer and the fixture prerequisite is open. Not PASS.
Workers: astra-reviewer (codex, requested gpt-6-astra, observed "gpt-6-astra medium" on pane status line; pane w1:p6, tab w1:t2 created by this review; 3 rounds, read-only, still open and idle). Opus verification subagent (model opus): file:line confirmation of spec claims, one finding rejected by lead (positional prefix). Cursor/Grok not used. Devin not used for review.

## Round 4 (user-authorized verification round, snapshot d439f845260f)
Peer report: seesaw-peer-d439f845260f-r4.txt — PASS. R3-1, R3-2, R3-3 and the mixed-status counterexample CLOSED. Lead accepts: each closure cites the same lines I verified in round 3.
Final verdict: PASS for the bounded spec review. Open prerequisites carried into the implementation plan: capture Massive probe raw bodies as fixtures; measure the reference-endpoint rate on a --tickers sample; production counts re-measured at acceptance.
Teardown: astra-reviewer pane w1:p6 / tab w1:t2 (created by this review) closed after this note.

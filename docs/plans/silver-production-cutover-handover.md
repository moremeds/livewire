# Handover — Bronze→Silver production cutover

> ## ⛔ RETIRED 2026-07-22 — superseded by reality. Do NOT follow this runbook.
>
> This doc (as-of 2026-07-15) says "Production Silver has **never been published**".
> **That is false now:** the data lake `silver/revisions/current.json` = **rev 10,
> published 2026-07-19, full universe**. The cutover it describes already happened,
> via a different path than this runbook (the seed/window/triage machinery of PR #57
> and the `resolve-yahoo-basis` reconstruction of PR #63 — none of which this doc
> mentions; its `migrate-price-basis → resolve-split-basis → repair-split-basis`
> sequence is the *superseded* split-basis path).
>
> **The only thing still pending is the Apex-side flag flip** (`APEX_LIVEWIRE_SILVER_ROOT`
> / `APEX_LIVEWIRE_PRICE_MODE=raw→adjusted`), which lives in the **separate `apex`
> repo**, gated on shadow acceptance — not in this repo and not this runbook.
>
> Current status of everything: **[CONSOLIDATED-STATUS-2026-07-22.md](CONSOLIDATED-STATUS-2026-07-22.md)**.
> Kept below only as a historical record of the 2026-07-15 plan.

**As of:** 2026-07-15
**Author:** takeover session (verified the prior "Codex" handover, shipped the partial-publish fix, ran the full-universe disposable dry-run)
**For:** whoever executes the production cutover next (Codex or operator)
**Repo:** `/Users/moremeds/projects/livewire` · **Data lake:** `~/market-warehouse/data-lake` (on-disk `/Volumes/DATA_LAKE/livewire/data-lake`)

---

## 0. TL;DR

The Bronze→Silver adjusted-price pipeline is **code-complete and mechanically proven end-to-end at full 13k-symbol scale** on a disposable copy. Production Silver has **never been published** (`~/market-warehouse/data-lake/silver/` is empty). What remains is not engineering — it is **running the cutover on the warehouse box (Mac mini) against current Bronze, in a writer-freeze window, gated on explicit human approval at the destructive Bronze-mutation step.**

Two findings changed the plan versus the original Codex handover:

1. **The rescued 2026-07-13 resolver evidence is STALE** (30/31 sampled source hashes changed — nightly syncs rewrote `1d.parquet`). It is **unusable as-is**. The resolver must be **re-run against current Bronze** before repair. The audit only honours evidence whose `status=="resolved"` *and* whose embedded `data_lake_root`/source hash still matches; stale evidence silently drops ~442 symbols back to ineligible.
2. **The full cutover must run on the Mac mini, not this MacBook.** Full-scale repair+rebuild is ~4 h here, exFAT-bound, and repeatedly risks a memory-SIGKILL. The mini owns the data volume and the IB Gateway.

**Do not run the destructive production Bronze migrate/repair without explicit user approval.** Everything up to and including the disposable rehearsal is safe; the production mutation is the gate.

---

## 1. What is already merged (no action needed)

All on `origin/main` — verified 2026-07-15:

| PR | Commit | What |
|----|--------|------|
| #47–#52 | (various) | Bronze `price_basis`/`source` columns, corporate-action pipeline, adjusted-history validation, resolver + evidence model |
| #53 | `0d1e94a` | Resumable corporate-action reconciliation |
| **#54** | **`af8e3f3`** | **`rebuild-silver` partial-publish** — publishes the healthy staged subset instead of aborting the whole revision on any per-symbol failure; exit code is threshold-based via `resolve_exit_code` (non-zero only on *systemic* breakage). This is what decouples the 71 unresolved blockers from the other ~13,028 healthy symbols so they no longer block the universe or trigger a nightly alert storm. Per-symbol artifact atomicity preserved. |

> **Local `main` lag:** local `main` is ~67 commits behind `origin/main` because of *uncommitted* CHANGELOG/presets/tasks edits in the working tree that block a fast-forward. **Do not clobber those.** Branch any new work off `origin/main`, not local `main`.

Apex side: PRs #150/#151 merged on `master`. The running container `apex-deploy-api-1` predates them and has `APEX_LIVEWIRE_SILVER_ROOT` unset — it is Bronze-only by design until we deliberately shadow Silver on a fresh image.

---

## 2. The pipeline — corrected command sequence

The **corrected** order (the original handover under-specified the repair step). All commands take `--data-lake-root` so they can target a disposable root; omit it to hit production `~/market-warehouse/data-lake`.

```
migrate-price-basis  →  resolve-split-basis (IB)  →  audit-split-basis (w/ evidence)  →  repair-split-basis --approve  →  rebuild-silver --full
```

Exact signatures (verified from argparse on 2026-07-15):

```bash
# 1. Blanket tag: sets source=legacy, price_basis=unknown on ALL rows. Does NOT resolve basis.
python scripts/livewire_store.py migrate-price-basis --full [--data-lake-root ROOT]

# 2. Re-resolve ambiguous split basis via IB ADJUSTED_LAST. PRODUCES the evidence dir.
#    Input is an audit manifest (run a first audit to get one); output-dir gets symbols/*.json with status=resolved.
python scripts/livewire_quality.py resolve-split-basis \
  --audit-manifest AUDIT.json --output-dir EVIDENCE_DIR \
  --host macmini --port 4001 [--data-lake-root ROOT] [--resume]

# 3. Audit, THIS TIME feeding the fresh evidence so resolved symbols become eligible.
python scripts/livewire_quality.py audit-split-basis --full \
  --output AUDIT2.json [--data-lake-root ROOT] [--evidence-dir EVIDENCE_DIR]

# 4. Filter AUDIT2 to eligible-only, then flip eligible rows to price_basis=raw.
#    (repair consumes the manifest; feed it the eligible-filtered one — see filter snippet below.)
python scripts/livewire_store.py repair-split-basis \
  --manifest AUDIT2_ELIGIBLE.json --approve [--data-lake-root ROOT]
#    Rollback: same command with --rollback instead of --approve.

# 5. Build Silver. Publishes healthy, quarantines blocked, exit 0 unless systemic.
python scripts/livewire_store.py rebuild-silver --full [--dry-run]
```

Eligible-filter snippet (used in the rehearsal):

```python
import json, sys
d = json.load(open(sys.argv[1]))
elig = [s for s in d.get("symbols", []) if s.get("eligible")]
out = dict(d); out["symbols"] = elig
json.dump(out, open(sys.argv[2], "w"))
print(f"total={len(d['symbols'])} eligible={len(elig)} ineligible={len(d['symbols'])-len(elig)}")
```

### Key semantics (learned the hard way — don't relitigate)

- `migrate-price-basis --full` is a **blanket tag**, not a resolver. Every row → `source=legacy, price_basis=unknown`.
- `repair-split-basis --approve` is what actually flips eligible rows to `price_basis=raw`. **Required before `rebuild-silver`** or every split-affected symbol fails staging.
- `audit-split-basis` upgrades an ambiguous symbol to `eligible` **only** when `--evidence-dir` contains `symbols/<enc>.json` with `status=="resolved"`, matching `symbol`, and a matching `data_lake_root`. Root/hash mismatch → silently ineligible. **This is why stale evidence is dangerous, not just wasteful.**
- `rebuild-silver` (post-#54): partial-publish. If ineligible/failed ≤ `max(50, 5%)`, it publishes the healthy subset and **exits 0**; `current.json` advances atomically after all healthy symbols validate. Idempotent — a rerun with no changes leaves `current.json` untouched.

---

## 3. Disposable dry-run — proven facts (production untouched)

Ran on `/Volumes/WD2/livewire-rehearsal/data-lake` (a 1d+corporate_action-only copy of production Bronze). **Left intact on WD2 for reference** — delete when you no longer need it.

| Step | Result |
|------|--------|
| `migrate-price-basis --full` | 13,109 symbols tagged, clean, ~19 min |
| `audit-split-basis --full` (no evidence) | **12,596 eligible / 513 ineligible (3.9%)** → under the 5% systemic threshold → `rebuild-silver` partial-publishes, exit 0 |
| 9-symbol full sequence | migrate→audit→repair→rebuild→canary→idempotent-noop all pass; 5 clean published, 4 blocked (INTC/HWBK/CBIO/BKTI) quarantined, exit 0, `current.json` advanced |
| Evidence staleness check | 2026-07-13 durable evidence = **30/31 sampled hashes stale** vs current production Bronze |

The 513 ineligible = 71 truly-blocked + ~442 evidence-resolvable (which the fresh resolver run in §4 will recover). Canary control symbol **must be split/action-free** — AMZN was a bad control (it has a real split); pick a symbol present in `bronze/asset_class=equity` but absent from `bronze/asset_class=corporate_action`.

---

## 4. Production cutover runbook (execute on the Mac mini)

> **Prereqs:** run on the warehouse box (owns `/Volumes/DATA_LAKE`, has RAM headroom). IB Gateway reachable — verified `nc -z macmini 4001` **OPEN** on 2026-07-15. Use `--host macmini` (the `.env` hostname `ib-gateway` is stale and does not resolve).

**A. Freeze writers.** Disable the launchd jobs so no daily/intraday sync rewrites Bronze mid-cutover:
```bash
launchctl unload ~/Library/LaunchAgents/com.livewire.daily-update.plist
launchctl unload ~/Library/LaunchAgents/com.livewire.intraday-catchup.plist
launchctl unload ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist
```

**B. Back up Bronze** (migrate + repair mutate `1d.parquet` bytes in place). Snapshot `bronze/asset_class=equity` and `bronze/asset_class=corporate_action` to a repairs dir, e.g. `data-lake/repairs/adjusted-silver-cutover-<date>/bronze-backup/`. Verify a sample of sha256s.

**C. Migrate (blanket tag).**
```bash
python scripts/livewire_store.py migrate-price-basis --full --dry-run   # inspect
python scripts/livewire_store.py migrate-price-basis --full
```

**D. First audit → resolve (IB) → second audit.** This is the step the stale evidence forced:
```bash
python scripts/livewire_quality.py audit-split-basis --full --output audit1.json
python scripts/livewire_quality.py resolve-split-basis \
  --audit-manifest audit1.json \
  --output-dir data-lake/repairs/adjusted-silver-cutover-<date>/evidence-<date> \
  --host macmini --port 4001 --resume
python scripts/livewire_quality.py audit-split-basis --full --output audit2.json \
  --evidence-dir data-lake/repairs/adjusted-silver-cutover-<date>/evidence-<date>
```
Confirm `audit2` eligible count jumped (the ~442 recovered). Expect ~71 still ineligible — that's the known blocker set (§5).

> ⛔ **DESTRUCTIVE GATE — get explicit human approval before step E.** Everything above is reversible/non-committal; E writes adjusted basis into Bronze.

**E. Repair (flip eligible → raw).**
```bash
# filter audit2 → eligible-only manifest (snippet in §2), then:
python scripts/livewire_store.py repair-split-basis --manifest audit2_eligible.json --approve
```

**F. Build Silver.**
```bash
python scripts/livewire_store.py rebuild-silver --full --dry-run
python scripts/livewire_store.py rebuild-silver --full        # expect exit 0, healthy published, ~71 quarantined
```

**G. Validate.** Canary a handful of clean tickers + one action-free control against known adjusted values; confirm `current.json` exists and points at the new revision; spot-check a split name (e.g. NVDA) shows corrected adjusted history.

**H. Apex shadow.** Deploy a **fresh** Apex image (with PRs #150/#151) with `APEX_LIVEWIRE_SILVER_ROOT` pointed at the Silver root; run in shadow before promoting.

**I. Unfreeze** — reload the three launchd jobs.

**Rollback at any point:** `repair-split-basis --rollback`, restore the Bronze backup from step B, and drop `data-lake/silver/`. Silver is a replayable publish target — nothing canonical is lost.

---

## 5. The 71 blockers + the one genuinely-new PR

71 symbols stay evidence-blocked (58 IB-basis-ambiguous). All actively trading, all have pre-ex-date history (none free to drop). Materiality is mostly low: most ambiguous events are ratio≈1.0 annual stock dividends (small $ impact). Genuine attention: the handful of real old splits, 5 weird-ratio cases (CBIO/BTX/ELOX/MDRR), and 4 zero-OHLC rows (BKTI/CBRE/CNC/NEOG). 13 liquid names blocked: **AIG AXON CBRE CNC ECL GOLD INTC LEN PLUG ROL SLG VLO XRX**.

**Deferred PR — ADJUSTED_LAST explicit-basis follow-up** (the only new engineering task): use IB `ADJUSTED_LAST` history to give these an explicit basis. Massive 403s on pre-recent history and Stooq is anti-bot-blocked, so IB is the only automated path.

- **INTC is the regression fixture.** The user flagged the INTC blocker as "dubious" — the ~20→~140 rise is genuine, and boundary math on the 1987 3:2 split strongly suggests **IB already applied it**, so INTC likely needs **NO repair**. Use it as the test that the resolver correctly emits "no repair needed" rather than double-adjusting.

This PR is **not required** for the cutover — post-#54, the 71 simply quarantine and the other ~13,028 publish. Do it as a follow-up to shrink the quarantine set.

---

## 6. Artifact & path reference

- **Disposable rehearsal root:** `/Volumes/WD2/livewire-rehearsal/data-lake/` — intact, safe to delete.
- **Durable (STALE) evidence + audit manifests:** `~/market-warehouse/data-lake/repairs/adjusted-silver-cutover-20260713/` — contains `split-basis-ib-evidence-20260713{,-v2,-v3}/` and large `split-audit-*.json`. Treat as reference only; regenerate. **Strip `._*` AppleDouble sidecars** before any pyarrow dataset read (`find <dir> -name '._*' -delete`) — exFAT scatters them and they break dataset scans.
- **Rehearsal driver scripts** (session scratchpad, if still present): `build_disposable.sh`, `run_pipeline.sh`, `resume_pipeline.sh` under `.../scratchpad/`. Good templates; they hard-guard `MDW_DATA_LAKE` against non-disposable roots.
- **Path lever:** `MDW_DATA_LAKE` overrides the Bronze root, `MDW_SILVER_DIR` the Silver root (`livewire_scripts/paths.py:data_lake_dir()`). This is how you retarget any command at a disposable copy.
- **Memory:** `~/.claude/projects/-Users-moremeds-projects-livewire/memory/project-silver-cutover-status.md` has the running state.

## 7. Environment gotchas

- IB Gateway is on the Mac mini at `macmini:4001` (Tailscale) — **not** this MacBook. `.env` hostname `ib-gateway` is stale.
- This MacBook: internal disk ~97% full, tight RAM — **not** a cutover host. Data lake is external 13 TB exFAT (`/Volumes/DATA_LAKE`, ~6.7 TB free); exFAT is slow for `du`/many-small-files.
- Any daily/intraday sync rewrites `1d.parquet` and **re-stales resolver evidence** — hence the freeze in step A and the re-resolve in step D. Never trust evidence generated before the last sync.

---

## 8. Open decisions for the operator

1. **When to run the cutover** — needs a maintenance window with writers frozen (§4A). Suggest right after a daily sync so Bronze is freshest and the freeze window is shortest.
2. **Whether to land the ADJUSTED_LAST PR first** (shrinks quarantine from ~71) or cut over now and follow up. Post-#54 either order is safe.
3. **Apex promotion criteria** — how long to shadow Silver before pointing production reads at it.

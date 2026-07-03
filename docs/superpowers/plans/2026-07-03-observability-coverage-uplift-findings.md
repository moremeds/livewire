# Livewire uplift plan — observability, alerting, coverage (2026-07-03)

Status: PROPOSED (not started). Findings verified against the live warehouse and repo on 2026-07-03.

## 1. Findings

### 1.1 The data lake is mostly healthy — the *monitoring* is what's broken

Freshness audit (parquet footer stats, 2026-07-03; reference trading day 2026-07-02):

| Lane | State |
|---|---|
| Equity 1d (11,840 symbols) | 97.6% current through 2026-07-02; small tail of thinly-traded names |
| Equity intraday 1m/5m/30m/1h (12,931 symbols) | Current through the full 07-02 extended session (spot-checked 20 large caps + file counts) |
| Volatility 1d (15) + intraday (14) | Current through 07-02 |
| Rates (DGS3/5/10/30) | 2026-07-01 = normal FRED 1-day lag |
| Futures | **EMPTY** — `asset_class=futures/` has zero symbols despite 4 presets and a daily-job lane |
| cmdty (XAUUSD), fx (USDEUR) | Frozen at 2026-05-20; no sync lane owns them |
| option_chain_snapshot (AAPL, AMZN) | One-off snapshots from 2026-05-21/22; abandoned experiment |
| Raw day_aggs_v1 | Stale since **2026-06-11** (cursor mtime Jun 13) — full-universe daily flatfile lane stopped |
| Raw minute_aggs_v1 | Current (07-02) |
| Disk | 46 Gi free / 228 Gi (78% used); raw = 87 G and growing daily |

Structural inconsistencies:
- **1,091 symbols have intraday parquet but no `1d.parquet`** (12,931 vs 11,840) — the intraday flatfile universe is wider than the daily universe.
- 8,620 symbols already archived to `bronze-delisted/` (the OTC archive path works); active equity = 12,943 dirs.

### 1.2 The daily_update "total failure" email is a counting/classification defect chain

Evidence from `daily_update_2026-07-03.log`: attempt 1 actually **updated 9,091 tickers** and "failed" 372; attempts 2–3 retried the residue and ended 0 updated / 277 failed → email says "277/277 tickers failed".

Root-cause chain (file:line verified):
1. **"No trade" is counted as failure.** `get_missing_trading_dates` (`livewire_scripts/daily_update.py:232-250`) assumes every NYSE session must have a bar for every ticker. The 277 perpetual failures are mostly illiquid SPAC units/warrants/rights (`*U`, `*W`, `.WS`, `*R`) that simply didn't print that day; Massive REST correctly returns no bars → `tickers_failed += 1` (`daily_update.py:737-740`). A few are real names (VSCO, BLD) with older structural gaps the target-day REST query can't fill.
2. **Partial success counted as failure.** 95 tickers got the target-day bar but had older unresolved gaps → "…published from Massive, still missing …" → failed (`daily_update.py:756-766`).
3. **Boolean exit code, no tolerance.** `if tickers_failed > 0: return 1` (`daily_update.py:785-787`, `:941-943`). One no-trade warrant fails the whole 9,463-ticker job, every day, forever.
4. **Wrapper retries a job that can never succeed** (3 attempts, 300 s apart — `run_daily_update_job.py:262-373`) then fires the failure alert.
5. **Success lines classified as the "dominant error".** `extract_error_summary` (`run_daily_update_job.py:219-239`) regex `\s+\S+:\s+(.+)` matches *every* `TICKER: message` line including the green success line "1 bar published from Massive" (`daily_update.py:768-771`); `Counter.most_common(1)` then reports the success message 9,091× as the dominant error.
6. **Cerebras enrichment is boilerplate.** Under launchd the API key resolution (`livewire_node/cerebras_client.mjs:52-79`) often fails → static fallback text (`send_daily_update_failure_email.mjs:210-229`). Even when it works it reasons over the corrupted error summary + 40-line tail → generic output.

### 1.3 The daily-summary email noise has the same root cause

`range_shortfall` flags (`clients/quality_detector.py:36-66`) compare `expected_start = last_bar + 1 trading day` against the first returned bar. Illiquid units that don't trade for a week get flagged (e.g. SPKLU: expected 2026-06-25, actual 2026-07-02 → warning). On the Massive path `ib_head_timestamp` is never populated (`daily_update.py:338-347`) so the suppression branch (`quality_detector.py:49-50`) can't fire. The "Top tickers" table is therefore a list of warrants nobody cares about.

### 1.4 The intraday catch-up job is invisible by design

- `run_intraday_catchup_job.py` writes a 35-line orchestrator log; all detail is in 6 per-phase logs; phase failures are swallowed ("exited 1 after completed summary; continuing").
- Completion marker is literally `=== Done <ts> ===` — no counts, no per-phase outcomes (`run_intraday_catchup_job.py:198-200`).
- **No success summary email, no telemetry, no watchdog** (the watchdog only covers daily_update).
- Its failure "error summary" is just the last non-blank log line (`:111-121`).

### 1.5 The coverage/quality tooling is dead

- Last `coverage_*.log` is **2026-06-17** and reports `1d=0/20396 (0.00%)` for every timeframe — an obviously broken result (denominator = full SIP universe incl. delisted; numerator matching broken), and the job hasn't run since (it was tied to a container entrypoint that no longer exists in the launchd world).
- **No `quality_weekly_*.md` has ever been written.**
- So: the two tools whose whole job is answering "what is my coverage?" are (a) wrong and (b) unscheduled. This is the core of "totally in the dark".

### 1.6 Job architecture overlap

- 05:00 UTC intraday-catchup runs an equity daily sync over the 2,388-ticker preset union, then 06:00 UTC daily_update re-runs equity daily over ~9,463 bronze-discovered tickers. Two lanes, one hour apart, doing overlapping work with different universes.
- The daily job also loops `--asset-class futures` against an empty tree every day.

### 1.7 Uncommitted in-flight work

Working tree has `livewire_scripts/archive_otc_symbols.py` + tests (untracked) and modifications to `flatfile_publisher.py`, `intraday_bronze_client.py`, `tasks/archive.md`. This is the OTC-archive workstream, partially executed (8,620 symbols already moved). It must be landed or reverted before other changes stack on it.

---

## 2. Plan

Each phase = one PR (branch → PR → CI green → merge). Tests required for all new code (100% coverage gate). Order matters: fix semantics before fixing emails, fix coverage truth before acting on coverage.

### Phase 0 — Land the in-flight OTC-archive work (small)
- Review, test, and commit `archive_otc_symbols.py` + `flatfile_publisher.py` / `intraday_bronze_client.py` changes on a branch; open PR.
- Run it (dry-run first) to archive the remaining non-SIP junk so the daily universe stops including instruments no provider can serve.

### Phase 1 — Daily update outcome semantics (the false-alarm killer)
1. Introduce per-ticker outcome classes in `daily_update.py`: `updated`, `no_trade` (no bars returned, no error — benign), `partial` (target day filled, older gaps remain), `error` (exception/HTTP failure). Replace the binary updated/failed counters.
2. Optional strengthening: consult the target-day raw `_symbols.parquet` (already on disk from the flatfile lane) — a ticker absent from the day's SIP traded set is definitively `no_trade`, present-but-missing is `error`.
3. Exit-code policy: exit 1 only for systemic failure — e.g. `error` count > max(5% of processed, 50), or zero updates on a trading day. `no_trade`/`partial` never fail the run.
4. Emit a **structured machine-readable summary** (one JSON line, e.g. `SUMMARY_JSON {"updated":9091,"no_trade":277,...}`) at end of run; keep the human table.
5. `extract_error_summary` parses the JSON line instead of regexing prose; delete the `TICKER: message` counter. Failure emails then say what's true: "9,091 updated, 277 no-trade, 95 partial, 0 errors".
6. Wrapper: skip retries when residue is all `no_trade`/`partial`.
7. `range_shortfall`: suppress when the instrument didn't trade (trade-count/SIP-set check), and stop passing mechanical `expected_start` for illiquid instruments; aggregate remaining flags by severity in the summary email instead of per-warrant rows.

### Phase 2 — Coverage truth restored
1. Debug + fix `livewire_quality.py coverage` (the 0/20396 result): scope the denominator to the **active bronze universe** (or the day's SIP traded set), fix the numerator matching, add `30m` to tracked timeframes.
2. Schedule it: new launchd plist (or append to the daily-update wrapper after success) so it runs every day again; keep the auto-recovery safety cap.
3. Schedule `livewire_quality.py weekly` (Sunday self-skip already exists — it just never runs).
4. Reconcile the 1,091 intraday-only symbols: publish their `1d` from day_aggs raw, or archive them — one-off script + decision.
5. Decide the fate of the **day_aggs full-universe daily lane** (stale since 06-11): either re-enable `flatfile-ingest-daily catch-up` in the nightly orchestrator, or formally retire it and document that daily universe = active bronze (~12.9K). (Recommended: re-enable — it's the natural authority for `no_trade` classification and for the 1d side of the universe.)

### Phase 3 — One trustworthy daily digest (replace noise with signal)
1. Single "Livewire nightly digest" email after the 06:00 job: per-job status table (intraday_catchup, daily_update per asset class, coverage), per-lane counts (updated/no_trade/partial/error), freshness snapshot (max dates per asset class), coverage %, disk headroom, and only *actionable* quality flags.
2. Intraday catch-up: emit the same structured per-phase summary (phase, exit, duration, rows/symbols published) into its log + a marker JSON; feed it into the digest; extend the watchdog to cover it (alert if no completion marker by 08:00 UTC).
3. Failure alerts become rare and real: only systemic failures (per Phase 1 policy) send immediate mail.
4. Cerebras enrichment: fix key loading under launchd (source from `~/market-warehouse/.env` like other secrets) **or drop it** — with a structured summary the AI paragraph adds little. (Recommended: drop from the failure path, keep the plumbing.)

### Phase 4 — Datalake hygiene
1. **Futures**: decide — seed via `presets/futures-*.json` (`historical --asset-class futures`) and let the daily lane maintain them, or remove futures from the daily-job loop until wanted. Currently the lane runs against nothing every night.
2. **cmdty/fx**: no owner, stale 6 weeks — archive out of bronze or add an explicit lane. **option_chain_snapshot**: archive (dead experiment).
3. **Disk retention**: raw = 87 G of 228 Gi disk at 78%. Add a retention/compaction policy for `minute_aggs_v1` raw partitions older than N days (bronze is the system of record; raw is re-downloadable), or push to R2 via the existing sync path.

### Phase 5 — Job architecture consolidation (after 1–3 are stable)
- Collapse the duplicated equity daily sync: one lane owns equity 1d (recommended: the flatfile/day_aggs path for the whole universe + Massive REST only as target-day gap recovery), the 06:00 job keeps IB-only asset classes.
- Re-examine schedules afterwards (05:00 + 06:00 + watchdog 10:30 UTC) — pin all documentation in UTC/ET.

## 3. Decisions needed from the operator

1. Daily equity universe: full active bronze (~12.9K, recommended) vs presets-only (~2.4K)?
2. Futures: seed and maintain, or drop the lane for now?
3. Keep Cerebras incident enrichment (fix env) or remove from the alert path?
4. Raw minute-file retention: delete raw older than N days / offload to R2 / keep all?
5. cmdty/fx/option_chain_snapshot: archive or own?

## 4. Verification criteria (per phase)

- P1: a normal trading day produces exit 0 with residual no-trade tickers; a simulated Massive outage (mocked) produces exit 1 with a truthful error summary; unit tests cover the classifier and threshold policy.
- P2: `coverage` reports ≥97% for 1d on the active universe on a current day; runs appear daily in logs; weekly report renders on Sunday.
- P3: exactly one digest email per day on success; failure email only on injected systemic failure; intraday job visible in digest.
- P4/P5: no daily lane runs against an empty or unowned asset class; disk trend flat or bounded.

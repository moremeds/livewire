# Gold coverage evidence — GLD / XAUUSD — 2026-09-22 (rev 2)

Worker `gold-coverage-scout` (roster role implementer, kind Devin). LEAD_PANE=wD:p1.
Task file: `GOLD_COVERAGE_TASK.md` beside this report. Read-only investigation; no backfill, ingest, production write, restart, commit, or paid-provider call was executed or authorized.

- **Model actually running:** SWE-2 Max (Devin CLI 3000.11.1)
- **cwd / worktree:** `/Users/chenxi/projects/livewire/.worktrees/price-discovery` (branch `feat/price-discovery`, dirty files preserved untouched)
- **Rule paths loaded:** `~/.claude/CLAUDE.md`, `~/.config/devin/AGENTS.md`, repo `AGENTS.md`, repo `CLAUDE.md`, c-memory `INDEX.md` → `wiki/context.md`, `wiki/gotchas.md`, `wiki/projects/livewire.md`, `wiki/projects/apex.md`; argon worktree `AGENTS.md` and `MPD00-MPD02-EVIDENCE.md` on lead correction
- **Method:** SSH `macmini` + mini venv PyArrow 24.0.0 direct reads of named symbol folders only (`/Users/moremeds/market-warehouse/data-lake/bronze/…`); live read-only GETs against Apex :8322 on the mini; local repo grep/read; read-only JSON metadata read of argon's frozen macro export. No whole-lake scan.
- **Two capture instants:** A = 2026-09-22 ~08:40–08:44 UTC; B = 2026-09-23 ~02:05 UTC refresh (same narrow checks).

## 1. Observed lake state (mini, actual parquet — not catalog)

| File | Rows | Key (type) | Min | Max | Dup keys | Capture |
|---|---:|---|---|---|---:|---|
| `equity/symbol=GLD/1d.parquet` | 5,491 | `trade_date` date32 | 2004-11-18 | 2026-09-21 | 0 | A |
| `equity/symbol=GLD/1m.parquet` | 756,562 → **757,374** | `bar_timestamp` ts[us, UTC] | 2021-06-11 08:16Z | 2026-09-18 23:59Z → **2026-09-21 23:59Z** | 0 | A → B |
| `equity/symbol=GLD/5m.parquet` | 195,900 | same | 2021-06-11 08:15Z | 2026-09-18 23:55Z | 0 | A |
| `equity/symbol=GLD/30m.parquet` | 39,813 | same | 2021-06-11 08:00Z | 2026-09-18 23:30Z | 0 | A |
| `equity/symbol=GLD/1h.parquet` | 20,673 | same | 2021-06-11 08:00Z | 2026-09-18 23:00Z | 0 | A |
| `cmdty/symbol=XAUUSD/1d.parquet` | 2,656 | `trade_date` date32 | 2016-05-24 | 2026-09-21 | 0 | A |

GLD 1d: Mon–Fri only; source {ib 4149, legacy 1293, massive 49}; price_basis {raw 4198, unknown 1293}. XAUUSD 1d: Mon–Fri only; **no `source`/`price_basis`/`available_at` columns**; volume=0 every row. No files other than 1d under `symbol=XAUUSD/`. GLD silver rev 76 covers 2004-11-18→2026-09-18 (MPD00).

**GLD intraday session regularity (observed, not denominator-reconciled).** Last 12 sessions observed at capture A (2026-09-02→09-18) plus 09-21 observed at capture B each carry 16 1h / 32 30m / ~181–192 5m / ~712–844 1m bars, spanning 08:00→23:00/24:00 UTC (04:00→19:00/20:00 ET) — i.e. extended hours are included, consistent with Massive SIP flatfiles rather than IB RTH. Session *dates* reconcile to the XNYS calendar: every weekday Sep 2→21 is present except Sep 7 (Labor Day), which is correctly absent. Per-day bar counts are **uniform within the observed band only** — no reconciliation against an expected per-minute/session denominator was performed, so "no missing minutes inside sessions" is NOT claimed.

**2026-09-21 intraday: absent at capture A, present at capture B — report as observed timing, lag hypothesis corroborated but not proven.** At capture A (2026-09-22 08:43Z) the newest raw partition was `raw/massive/us_stocks_sip/minute_aggs_v1/date=2026-09-18` and GLD intraday ended 09-18 while 1d already held 09-21. At capture B (2026-09-23 02:05Z) `date=2026-09-21` exists (`_SUCCESS`, `_symbols.parquet`, 256 buckets), `date=2026-09-22` does not yet (its publication window is still open), and GLD 1m/1h parquet were rewritten Sep 22 20:21 mini-local with the 09-21 session present (812 1m bars). This is *consistent with* provider-publication-plus-next-catch-up cadence, but the upstream publish instant was not independently measured — the alternative (a delayed or retried ingestion) is not excluded.

**Sidecar flags.** GLD `1d.parquet.meta.json` carries a critical `range_shortfall` (actual 2004-11-18 vs `expected_start` 1993-01-29) — the expectation is not GLD's inception (GLD listed 2004-11-18), so the flag is an expectation artifact, not a data gap; the GLD daily series starts at the instrument's inception date, but interior completeness is not proven from the min date alone. XAUUSD meta flags `interior_gaps`: 6 missing days within its detection window (first 2017-05-23, last 2024-05-21).

**XAUUSD missing weekdays — recomputed full-range, grouped by holiday coincidence.** Recomputing weekday dates absent between 2016-05-24 and 2026-09-21 yields **39 missing weekdays** (the sidecar's "6" reflects its narrower scan window, not the full series). No contract calendar for this instrument was verified, so **all 39 dates remain unreconciled against the actual instrument calendar**; the grouping below records calendar coincidence only, not confirmed closures:

- **34 holiday-coinciding candidates**: Christmas-window dates (×10: 2016-12-26, 2017-12-25, 2018-12-25, 2019-12-25, 2020-12-25, 2021-12-24, 2022-12-26, 2023-12-25, 2024-12-25, 2025-12-25), New-Year-window dates (×10: 2017-01-02, 2018-01-01, 2019-01-01, 2020-01-01, 2021-01-01, 2022-01-03, 2023-01-02, 2024-01-01, 2025-01-01, 2026-01-01), Good Friday dates (×10: 2017→2026), UK Spring Bank Holiday 2026-05-25, Juneteenth 2026-06-19, July-4-observed 2026-07-03, Labor Day 2026-09-07.
- **5 other dates, cause not determined**: 2017-05-23 Tue, 2018-05-23 Wed, 2019-05-23 Thu, 2023-05-22 Mon, 2024-05-21 Tue — a recurring mid/late-May cluster with no matching widely observed holiday. The sidecar detector reported 6 misses in-window vs my 5 non-holiday-coinciding dates in the same range — the exact reconciliation (which 6th date the detector counted) was not performed; both numbers are reported as-is.

## 2. XAUUSD identity — partially verified, not fully established

Code: `clients/ingestion_common.py:100` builds `Contract(secType="CMDTY", symbol="XAUUSD", exchange="SMART", currency="USD")`; the cmdty/fx daily lane fetches `what_to_show="MIDPOINT"` (`fetch_ib_historical.py:407`).

Observed: USD-denominated price levels consistent with spot gold (1,227.23 on 2016-05-24 → 4,343.02 on 2026-09-21), volume=0 every row, Mon–Fri `trade_date` calendar with 39 weekday gaps (§1), no settlement/open_interest/contract-expiry fields.

**Verified:** the *currently configured* production path for this symbol is the CMDTY/SMART/USD contract fetched as IB MIDPOINT daily bars. **Not verified:** the provenance of the stored historic rows — the cmdty schema carries no `source`/`price_basis` columns, so which path produced each historical row is not established; the continuous-spot-proxy / no-roll characterization is inference from the contract type and observed fields, not a verified property of every stored row; full IB contract details (exchange routing, contract size/settlement), the day-boundary convention composing one `trade_date`, and entitled historical depth are likewise unverified — no IB session was used, `ib_head_timestamp` is null.

## 3. Apex support (repo-verified + live API on mini)

`apex/src/infrastructure/adapters/livewire/asset_classes.py`: equity + fx get `(1m,5m,30m,1h,1d)`; volatility `(5m,30m,1h,1d)`; **cmdty, futures, rates are daily-only**; `supports_adjusted` is equity-only. Live on mini (:8322), capture A:

| Request | Result |
|---|---|
| `GET /v1/equity/GLD/bars?timeframe=1h&start=…&end=…&price_mode=adjusted` | 200, real bars (rev 76; factor join is identity for GLD — factors 1.0) |
| same `price_mode=raw` | 200, identical values |
| `GET /v1/equity/GLD/bars?timeframe=1d&price_mode=adjusted` | 200 through 2026-09-18 = rev 76 boundary (bronze 1d already at 09-21) |
| `GET /v1/cmdty/XAUUSD/bars?timeframe=1d` | 200, raw/unadjusted, through 2026-09-21 |
| `GET /v1/cmdty/XAUUSD/bars?timeframe=1h` | **400 `unsupported_timeframe` ("have ['1d']")** |

Caveats: bare `limit=N` without `start`/`end` returned 0 bars on 1h — window params are effectively required for intraday reads (params quirk, not investigated further). Deployed apex defaults equity reads to `price_mode=adjusted`.

**Lake vs API:** GLD intraday exists in the lake AND is served. XAUUSD intraday exists in neither — and even if ingested, Apex could not serve it without extending the cmdty ladder. Two separate gaps.

## 4. Macro overlay — CORRECTED (lead rejection)

My rev-1 claim "DFII10 absent" was wrong: it cited the Apex `/v1/rates` 404s recorded in PRICE_DISCOVERY_MASTER — that is the livewire `rates` partition (DGS3/DGS5/DGS10/DGS30 only), a different surface. Argon's `option_wizard` DB DOES hold the macro overlay (`docs/research/price-discovery/MPD00-MPD02-EVIDENCE.md`, capture 2026-09-22T07:58Z; export metadata `macro-2026-09-22.json`, schema `mpd00-macro-evidence-v1`):

| Series | Rows | Distinct periods | Availability instants | Coverage | Unit |
|---|---:|---:|---:|---|---|
| DFII10 (real yield) | 1,429 | 1,429 | 1,418 | 2021-01-04 → 2026-09-18 | percent |
| DTWEXBGS (broad USD) | 4,265 | 1,429 | 298 | 2021-01-04 → 2026-09-18 | index (Jan-2006=100) |

Both `source=fred`, daily, quality `valid`. Caveats that bound the study: `published_at` NULL on all rows; `available_at` is midnight-UTC precision (`availability_precision=day`, `eligibility_policy=next_utc_day`); Argon first *observed* these series 2026-08-20/23 — coverage is public-vintage reconstruction, not proof of historical observation; DTWEXBGS's 298 availability instants mean distinct daily periods are not independent releases.

## 5. Feasible backfill/repair options (entrypoints as they exist today)

| Want | Entrypoint | Status |
|---|---|---|
| GLD intraday 1m/5m/30m/1h | `flatfile-ingest` (`backfill`/`catch-up`/`repair --dates`) | **Already populated** 2021-06-11→ (max-entitled Massive depth at build time) and accruing on schedule; `repair` is the supported fix for any future single-day gap. |
| GLD daily longer than 2004-11-18 | none | **Impossible for this instrument** — GLD did not exist earlier. |
| XAUUSD daily deeper than 2016-05-24 | `livewire_ingest.py historical --asset-class cmdty --tickers XAUUSD --years 0 --backfill` (IB MIDPOINT) | **Supported entrypoint, depth unverified** — whether IB serves CMDTY XAUUSD before 2016-05-24 is unknown (`ib_head_timestamp` null); needs a Gateway session (2FA-gated). No Massive/Stooq/Yahoo lane exists for cmdty; the Yahoo lane is fx-scoped and XAUUSD is registered cmdty. |
| XAUUSD intraday | `intraday-backfill --asset-class cmdty --tickers XAUUSD --timeframe <tf>` | **Nominally accepted, functionally unsupported.** `backfill_intraday.py:178` hardcodes `what_to_show="TRADES"` while every other cmdty path uses `MIDPOINT` (spot metal has no TRADES tape — likely empty/error 162/200 → skip). `validate_intraday_bar(require_rth=True for cmdty)` rejects every bar outside 09:30–16:00 ET and on non-US-equity trading days — wrong session model for ~24h OTC metal. Needs a code change (MIDPOINT + cmdty session rules) plus the Apex cmdty-ladder change; whether IB serves intraday MIDPOINT for CMDTY XAUUSD is unverified. |
| Lineage columns on cmdty schema | — | cmdty daily schema has no `source`/`price_basis`/`available_at`; improving lineage is a schema/publisher change, not more rows. |

## 6. Recommendation — scoped to the current study, not a global verdict

**`gold_gld_broadusd_eod_v1` (primary horizon 5 sessions, aux 1 session): existing data is sufficient for this study; no gold-leg backfill is needed for it.** GLD 1d starts at the instrument's 2004-11-18 inception and is current (bronze 09-21 raw / rev 76 adjusted through 09-18 — pin a common cutoff per PRICE_DISCOVERY_MASTER §7); interior completeness was not proven from the min date alone. The macro overlay exists in Argon (DFII10 + DTWEXBGS, 2021-01-04→2026-09-18) with documented vintage caveats — the study's real bound is the **overlap window and day-precision vintage** (macro floor 2021-01-04; `published_at` NULL; eligibility next-UTC-day), not gold row counts. XAUUSD daily (2016+) remains a distinct instrument (OTC spot midpoint vs ETF, different session calendar, no lineage columns) — usable for cross-checks, not a GLD substitute.

**A separate intraday price-discovery / event-reaction study is feasible today on GLD only**: ~5.3y of minute bars (2021-06-11→, extended hours, zero dup keys, uniform observed sessions) already served by Apex raw + adjusted. XAUUSD intraday would be a two-repo change (livewire MIDPOINT + non-RTH cmdty validation; Apex cmdty ladder) with IB depth unverified — defer until that study is registered.

**Scope guard:** "sufficient for the current GLD daily study" is not "no backfill is ever needed". Deeper XAUUSD daily (pre-2016, unverified via IB), XAUUSD intraday (unsupported today), the 5 unexplained XAUUSD missing days, and cmdty lineage columns are all open extensions with distinct costs.

**Four asks kept separate:** longer daily history (only XAUUSD-pre-2016 conceivable, unverified), repairing missing periods (nothing actionable for GLD — 09-21 landed via normal cadence; XAUUSD's 34 holiday-coinciding dates are unreconciled to the instrument calendar, 5 May dates cause-undetermined), improving lineage (schema work), intraday coverage (GLD done; XAUUSD unsupported).

## 7. Probe commands / evidence

- `ssh macmini ls -la …/symbol={GLD,XAUUSD}/` + `cat *.meta.json`; `ls raw/massive/us_stocks_sip/minute_aggs_v1/` + named `date=` partitions (captures A and B).
- PyArrow (mini `/Users/moremeds/market-warehouse/.venv/bin/python`, pyarrow 24.0.0) `pq.read_table` on the six files — all counts/min/max/dup/session numbers direct reads; XAUUSD missing-weekday recompute enumerated all 39 dates.
- `curl` GETs to `127.0.0.1:8322` on mini — verbatim in §3.
- Argon evidence: `MPD00-MPD02-EVIDENCE.md` + top-level metadata of `evidence/macro-2026-09-22.json` (fields: `captured_at`, `history_mode=public_vintage_reconstruction`, `availability_precision=day`, `eligibility_policy=next_utc_day`, 5,694 observations / 29 artifacts / 0 invalidations). Raw observation rows not bulk-read.
- Repo reads: `backfill_intraday.py:178,199`, `clients/ingestion_common.py:94-113`, `daily_update.py:327-383`, `fetch_ib_historical.py:407,641-697`, `clients/intraday_bronze_client.py:30-61`, `scripts/livewire_ingest.py:18-52`, `presets/cmdty-metals.json`, apex `asset_classes.py:44-102`, `chart.py:235-249`.

## 8. Unknowns / not established

- IB CMDTY XAUUSD history depth (daily or intraday MIDPOINT) earlier than 2016-05-24 — needs a 2FA-gated Gateway head-timestamp probe, not run.
- IB's day-boundary convention for XAUUSD daily bars; full contract identity beyond secType/exchange/currency.
- Upstream Massive flatfile publish instant (the lag-vs-ingestion-gap alternative for 09-21 is not excluded, only less consistent with the observed next-run landing).
- Per-minute/per-session expected denominator for GLD extended-hours intraday — session dates reconcile to XNYS, in-session completeness not established.
- The 5 unexplained XAUUSD mid-May missing days; sidecar's windowed count (6) vs recomputed full-range (39) not reconciled line-by-line.
- GLD 1h `limit=N` with no start/end returning empty — params quirk, out of scope.

## Deviations

- One `find` over `raw/massive/us_stocks_sip` was killed after ~10s (too broad for exFAT); replaced with targeted `ls` on named date partitions.
- Rev 2 (lead corrections): added argon macro evidence, refreshed the same narrow mini partition checks at capture B, recomputed + classified XAUUSD missing weekdays, relabeled session-completeness and instrument-identity certainty, rescoped the recommendation. No commits; no writes outside this report.

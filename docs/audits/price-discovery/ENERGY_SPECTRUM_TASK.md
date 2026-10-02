# Energy product spectrum — Task 2 scope amendment

User steering, 2026-09-23: expand the gasoline-only investigation to the entire energy product spectrum, explicitly including heating oil, RBOB and natural gas. This authorizes investigation, not wholesale ingestion, backfill, new services, paid subscriptions or futures implementation. Gold closure continues separately.

Worker: gold-coverage-scout, Devin SWE-2 Max, existing Livewire price-discovery worktree. Preserve all gasoline artifacts already captured. Load applicable shared/project rules. No further delegation.

## Deliverable

Write `ENERGY_SPECTRUM.md` beside this assignment. Reuse the gasoline findings; do not repeat successful downloads. Map these families, with unavailable evidence marked explicitly:

- Crude: WTI, Brent, other relevant benchmarks discovered from existing catalogs.
- Gasoline: retail regular/all grades, wholesale/spot, RBOB futures.
- Distillates: heating oil, diesel/ULSD; explicitly verify whether labels designate the same contract or distinct series.
- Jet fuel/kerosene, propane/NGLs, residual fuel oil where authoritative catalogs expose them.
- Natural gas: Henry Hub spot/futures, regional benchmarks and LNG where accessible; separate physical prices from futures and retail utility CPI.
- Electricity and coal: catalog-level coverage only, to identify adjacent scope without an exhaustive regional crawl.

For each candidate, record economic product, geography/hub, instrument type, verified identifier/source URL, currency/unit, frequency, observed coverage, release clock/vintage support, current repo/lake/API support, and access/backfill constraints. Separate existing local coverage, verified accessible source, and metadata-only candidate. Futures need explicit contract/expiry, roll/adjustment and historical-depth evidence; never treat a continuous ticker as a verified term structure.

Group use cases: consumer inflation pass-through, spot/futures price discovery, and refinery/crack-spread research. Do not compute spreads before unit, contract and timing alignment. Rank a small next batch using actual coverage and cost, and state what remains unknown. No model fitting or claim of alpha.

## Explicit user priority: crack spreads

User additionally requested crack as upstream information for products. Give crude-to-product relationships their own first-class section, not a footnote. Inventory gasoline/RBOB cracks, heating-oil/ULSD cracks, and a 3-2-1 refinery basket; include other published product cracks only when readily evidenced. Separate exchange-listed spread instruments, source-published assessments and locally derived spreads. Identify each leg's benchmark (WTI/Brent/etc.), geography, quality, currency/unit, delivery month and observation/settlement timestamp. Explain whether actual contract-level data exists; independently rolled continuous series are not automatically aligned legs. Verify conversion factors and basket normalization against authoritative specifications before any calculation. A crack spread is a gross refining-margin proxy, not net realized refinery profitability, and upstream predictive value remains a hypothesis. Natural-gas processing/spark spreads are different economics and must not be mislabeled as petroleum cracks. No numerical spread computation, new ingestion, strategy or trading implementation in this investigation.

## Bounds and evidence

Own only this investigation's reports under `docs/audits/price-discovery/` and `evidence/energy-access/` (existing `evidence/gasoline-access/` retained). No edits to this lead-owned assignment or other project source files. Repo inspection and narrowly targeted read-only mini checks allowed. Small public official GET requests only; no credentials, account creation, VPN changes, paid API calls, production writes, lake changes, backfills, installs or service actions. Discover download URLs from authoritative pages rather than guessing. Cap total new downloads at 20 MB, one retry per failing route, and at most one representative sample per family; catalog metadata suffices for the rest.

Preserve response bytes, URL, retrieval timestamp, HTTP status and SHA-256 for samples. Keep transformed/extracted data separate from raw responses; retrieval time does not establish historical availability. Do not present source-page metadata as proof of local ingestion.

Rule 5: no staging, commits, push, PR, merge or deployment; report `commit none`.
Rule 6: stop after this task. Return `herd-report gold-coverage-scout task 2: commit none, evidence <path>, deviations: <text|none>`. Reverse IPC is blocked; leave the report on disk and a concise terminal summary.

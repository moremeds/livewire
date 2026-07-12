# Livewire Corporate Actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest auditable, revision-aware Massive split and cash-dividend events into canonical bronze Parquet.

**Architecture:** Extend the existing authenticated Massive client with paginated reference endpoints, normalize provider payloads into strict immutable event models, and publish per-symbol event histories through a focused atomic store. Expose targeted and universe reconciliation through the ingestion CLI.

**Tech Stack:** Python 3.13, requests, dataclasses, Decimal, PyArrow/Parquet, pytest responses mocks.

## Global Constraints

- Work in `/Users/moremeds/projects/livewire` on branch `feat/corporate-actions` after the design/spec PR lands.
- Depends on R0 only; it does not alter equity OHLCV bronze.
- Massive access uses `MASSIVE_API_KEY`; pagination follows provider `next_url` without leaking the token.
- Canonical path: `data-lake/bronze/asset_class=corporate_action/symbol={encoded_symbol}/events.parquet`.
- Provider revisions and cancellations remain auditable; only latest active versions feed Silver.
- All code in `clients/` and `scripts/` receives tests and the 95% coverage gate.

---

### Task 1: Massive reference endpoint models and pagination

**Files:**
- Modify: `clients/massive_client.py`
- Modify: `clients/__init__.py`
- Test: `tests/test_massive_client.py`

**Interfaces:**
- Produces: `MassiveSplit`, `MassiveDividend`, `MassiveClient.get_splits(ticker)`, and `get_dividends(ticker)`.

- [ ] Write failing tests for two-page responses, auth preservation, malformed ratios, malformed dates, and dividend currency/amount validation.
- [ ] Run `uv run pytest tests/test_massive_client.py -q -k 'split or dividend or pagination'`; expect failure for missing APIs.
- [ ] Implement `_get_paginated(endpoint, params) -> list[dict]`, following only HTTPS `api.massive.com` `next_url` values and reusing `_get` error mapping.
- [ ] Normalize ratios with `Decimal`, require `split_from > 0`, `split_to > 0`, `cash_amount >= 0`, ISO dates, provider IDs, and uppercase ticker.
- [ ] Run the focused tests; expect PASS.
- [ ] Commit with `git commit -m "feat: fetch Massive corporate actions"`.

Key model shape:

```python
@dataclass(frozen=True)
class MassiveSplit:
    provider_event_id: str
    ticker: str
    execution_date: date
    split_from: Decimal
    split_to: Decimal
    payload_hash: str

@dataclass(frozen=True)
class MassiveDividend:
    provider_event_id: str
    ticker: str
    ex_dividend_date: date
    cash_amount: Decimal
    currency: str
    declaration_date: date | None
    record_date: date | None
    pay_date: date | None
    payload_hash: str
```

### Task 2: Revision-aware corporate-action store

**Files:**
- Create: `clients/corporate_action_store.py`
- Test: `tests/test_corporate_action_store.py`

**Interfaces:**
- Produces: `CorporateActionStore.reconcile(symbol, events, fetched_at) -> ReconcileResult`.
- Produces: `latest_active(symbol) -> list[CorporateAction]`.

- [ ] Write failing tests for first insert, unchanged no-op, corrected payload lineage, cancellation, duplicate provider ID rejection, sort order, and atomic-publish failure.
- [ ] Run `uv run pytest tests/test_corporate_action_store.py -q`; expect import failure.
- [ ] Define the exact PyArrow schema from the approved design, adding `event_revision` and `supersedes_action_id`.
- [ ] Implement stable `action_id = blake2b(provider|provider_event_id|event_revision|payload_hash)` and append-only logical revisions; use `publish_parquet(..., sort_column="action_id")` after deterministic ordering.
- [ ] Implement cancellation only when a previously known logical event disappears from a completed unfiltered provider reconciliation; targeted/date-filtered fetches may not infer cancellation.
- [ ] Run focused tests; expect PASS.
- [ ] Commit with `git commit -m "feat: store canonical corporate actions"`.

### Task 3: Reconciliation CLI

**Files:**
- Create: `livewire_scripts/sync_corporate_actions.py`
- Modify: `scripts/livewire_ingest.py`
- Test: `tests/test_sync_corporate_actions.py`
- Modify: `tests/test_livewire_entrypoints.py`

**Interfaces:**
- Produces CLI: `scripts/livewire_ingest.py corporate-actions [--tickers ... | --preset ...] [--full-reconcile] [--dry-run]`.

- [ ] Write parser/dispatch and mocked reconciliation tests, including the rule that cancellations require `--full-reconcile`.
- [ ] Run focused tests; expect failure because command is absent.
- [ ] Implement ticker resolution through existing preset and bronze-symbol patterns; batch sequential provider calls with telemetry counters `inserted`, `revised`, `cancelled`, `unchanged`, and `failed`.
- [ ] Ensure dry-run performs provider comparison but writes nothing.
- [ ] Run `uv run pytest tests/test_sync_corporate_actions.py tests/test_livewire_entrypoints.py -q`; expect PASS.
- [ ] Commit with `git commit -m "feat: reconcile corporate actions"`.

### Task 4: Documentation and verification

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `.codex/project-memory.md`

- [ ] Document operator commands, paths, revision lineage, cancellation semantics,
  and the fact that scheduling lands with the Silver engine so ingestion and
  publication ordering are introduced together.
- [ ] Run `uv run pytest tests/test_massive_client.py tests/test_corporate_action_store.py tests/test_sync_corporate_actions.py -q`.
- [ ] Run CI-equivalent coverage: `uv run pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing` and `uv run pytest tests -q -W error::RuntimeWarning`.
- [ ] Commit with `git commit -m "docs: operate corporate action reconciliation"`.

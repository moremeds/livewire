# Silver Causal Canary Hardening Design

## Problem

The Silver canary currently proves that adjusted daily rows agree with published
factor intervals. That is an internal consistency check, not an independent
correctness check: bars and factors can both contain the same premature future
corporate-action adjustment and still pass.

The local warehouse sweep found 289 active actions across five reconciled
symbols. One action is not yet effective: MSFT's 2026-08-20 cash dividend. The
engine cutoff prevents new contamination, but the canary must also detect stale
artifacts created before that fix.

## Design

### Independent causal comparison

For each canary symbol, the validator will load canonical Bronze daily rows and
the latest active corporate actions, establish one New York `as_of_date` for
the validation run, and recompute expected factor intervals through the pure
adjustment engine. It will compare the semantic interval fields—effective date
range, price factor, and volume factor—with the published Silver factor
artifact. Revision identifiers are intentionally excluded from this comparison.

A mismatch will fail that symbol with a causal-factor error even when the
published daily rows remain internally consistent with the contaminated factor
artifact. This makes the validator capable of detecting existing stale Silver
output.

The validator entry point will accept an injected `as_of_date` for deterministic
tests and default once per invocation to the current `America/New_York` date.

### Rebuild and validation observability

Silver rebuild summaries and the top-level canary report will expose:

- `as_of_date`;
- `action_count` for all latest active actions in scope;
- `effective_action_count` for actions with `ex_date <= as_of_date`;
- `future_action_count` for actions excluded because their ex-date is later.

Each canary symbol result will also expose its effective and future action
counts. Future actions are normal state and do not fail validation by
themselves.

### Scope boundaries

This change will not modify Bronze records, Silver Parquet schemas, revision
manifest schemas, corporate-action reconciliation, or Apex. It will not create
a second factor algorithm; the pure adjustment engine remains the single
definition of factor semantics, while the validator compares that fresh causal
result against persisted artifacts.

## Edge Cases

- Future cash dividends and future splits must remain identity contributions.
- An action becomes effective when the New York as-of date equals its ex-date.
- The action adjusts only existing bars strictly before its ex-date, including
  when no Bronze bar exists on the ex-date because of a market-calendar gap.
- One validation or rebuild invocation uses one cutoff for every symbol.
- Cancelled and superseded revisions remain absent through
  `CorporateActionStore.latest_active`.
- Existing contaminated factor artifacts must cause the canary to fail rather
  than being silently accepted because daily bars match them.

## Tests and Verification

Tests will cover future dividend and split exclusion, exact ex-date activation,
an ex-date without a matching trading bar, one shared batch cutoff, rebuild
counter output, canary counter output, and rejection of a deliberately
contaminated but internally consistent Silver daily/factor pair.

Verification will run focused adjustment, rebuild, and canary tests; Ruff and
Pyright; the CI-equivalent test and coverage command; and a read-only/disposable
canary sweep across all five locally reconciled symbols. The real MSFT result
must report its future dividend as ignored and pass only with causal factors.

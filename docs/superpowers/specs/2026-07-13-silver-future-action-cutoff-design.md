# Silver Future Corporate-Action Cutoff Design

## Problem

Silver currently builds adjustment factors from every active corporate action.
That includes announced dividends and splits whose ex-date has not occurred.
Consequently, a future dividend can modify historical adjusted prices before it
becomes effective, introducing look-ahead information into Apex and other
Silver consumers.

Bronze must continue retaining future announcements. They are valid provider
records and are required for audit and later publication. The causal cutoff is
a Silver derivation concern.

## Design

`build_factor_intervals` will require an explicit `as_of_date`. It will compute
factors only from active actions whose `ex_date <= as_of_date`. Cancelled and
superseded actions remain excluded by the existing active-action selection.

The Silver rebuild command will derive its default cutoff from the current
calendar date in `America/New_York`, matching the U.S. equity session calendar
and avoiding UTC date rollover before the local ex-date begins. The rebuild
entry point will accept an injected as-of date for deterministic tests and
replayable programmatic runs.

Once an action reaches its ex-date, the existing fully back-adjusted semantics
remain unchanged: it adjusts bars strictly before the ex-date, while bars on
and after the ex-date do not receive that action's factor.

No Apex changes are required. Apex will continue reading the committed Silver
revision and factor artifacts.

## Data Flow

1. Corporate-action reconciliation stores active future and completed actions
   in canonical Bronze.
2. A Silver rebuild establishes one New York `as_of_date` for the entire batch.
3. Each symbol's factor calculation excludes actions after that cutoff.
4. Silver compares the causal candidate artifacts with the current revision.
5. Changed artifacts are committed under one revision, with `current.json`
   replaced last under the existing transaction protocol.

## Error Handling and Observability

Future actions are intentionally ignored rather than treated as errors. They
remain counted in the reconciled Bronze action set, but cannot affect factor or
daily-bar artifacts until effective. Existing validation errors for malformed
effective actions remain unchanged.

The implementation will avoid per-symbol clock reads so all symbols in one
revision share the same cutoff.

## Tests and Verification

Regression tests will prove that:

- an active announced dividend after `as_of_date` leaves every factor at 1.0;
- advancing `as_of_date` to the dividend's ex-date adjusts only earlier bars;
- the ex-date and subsequent bars remain unadjusted by that dividend;
- the rebuild path passes one explicit cutoff into factor construction.

Verification will include focused adjustment/rebuild tests, the repository's
CI-equivalent coverage command, RuntimeWarning enforcement, and a disposable
MSFT Silver rebuild using real local Bronze and corporate-action data. The MSFT
result must show no contribution from the 2026-08-20 dividend before that date.

## Scope

This change does not alter corporate-action ingestion, Bronze schemas, Silver
artifact schemas, revision manifest schemas, Apex behavior, or the definition
of fully back-adjusted prices for already-effective actions.

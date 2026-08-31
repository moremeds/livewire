# Shepherd stable-security identity contract

Status: frozen for LS-02.2a on 2026-08-31. Runtime storage begins in LS-02.2b.

## Purpose

Shepherd must join membership, corporate actions, and bars to a security, not
to whatever issuer currently owns a ticker. A ticker is a time-bounded provider
locator. It is never a stable identity and is never sufficient to publish PIT
Silver.

The internal `security_id` is an opaque, immutable generated identifier. It is
not derived from ticker, CIK, FIGI, issuer name, or a hash of mutable provider
metadata. External identifiers and symbols are append-only, versioned
intervals linked to that internal ID with their own evidence and `known_at`.

## Evidence inventory and authority

The executable inventory is frozen under
`tests/fixtures/shepherd/identity/sources.json`. The August 30 capture verified
that the following are actually obtainable:

1. A revision-bound Wikipedia HTML response with current symbols and CIKs.
   This is secondary discovery evidence, not final identity authority.
2. SEC `company_tickers.json`, which maps current SEC ticker spelling to an
   issuer CIK but supplies neither effective intervals nor share-class IDs.
3. SEC submissions, which identify the registrant, current ticker list, and
   exchange list but not a complete historical security mapping.
4. Massive ticker details. In the sampled entitlement it returned CIK,
   exchange MIC, composite FIGI, and share-class FIGI for META, BRK.A, BRK.B,
   GE, and GEHC. The policy does not assume those fields exist for every row.
5. Responsible-issuer and regulator-filed corporate-action material for a
   rename, ticker reuse boundary, and spinoff.

Every admitted observation must retain its source ref, exact raw response hash,
retrieval time, publication time when available, effective interval, and the
fields the source did not provide. Search snippets and model statements are
discovery hints only.

## Identifier meaning and priority

The following order governs continuity decisions. “Higher” evidence may still
be defeated by an explicit responsible-publisher event or a material conflict.

| Priority | Evidence | What it can prove | What it cannot prove alone |
| --- | --- | --- | --- |
| 1 | Responsible issuer, exchange, regulator filing, merger/spinoff terms, unchanged CUSIP statement | Rename continuity, terminal event, successor/child relationship, effective boundary | Unstated historical intervals |
| 2 | Matching share-class FIGI across adjacent intervals, corroborated by issuer/exchange history | Same share-class security across a symbol or venue change | Continuity through a contrary corporate action |
| 3 | Matching composite FIGI plus compatible share-class, issuer, exchange, and interval evidence | Same country composite security | Global share-class continuity or issuer continuity by itself |
| 4 | CIK plus compatible security-level evidence | Same SEC registrant/issuer | Same share class or same security; one CIK may have multiple classes |
| 5 | Exchange MIC, currency, security type, issuer name | Corroboration and candidate narrowing | Stable identity |
| 6 | Ticker or normalized ticker spelling | Locate an observation within a bounded interval | Any identity or continuity decision |

FIGI values are provider observations, not magical truth. A changed or missing
FIGI becomes a versioned claim. It is never silently replaced. CIK is issuer
evidence: the frozen Berkshire fixture proves one CIK can correspond to two
distinct listed securities.

## Decision rules

1. **Rename:** retain an identity only when a responsible source or compatible
   high-priority identifiers establish continuity. Close the old symbol
   interval and append the new interval.
2. **Ticker reuse:** different issuer/security evidence creates distinct
   identities even when symbol intervals do not overlap. Historical bars are
   never spliced.
3. **Share classes:** distinct share-class or composite FIGIs create distinct
   identities. A shared CIK does not merge them.
4. **Merger:** terminate the acquired security at the effective boundary. The
   surviving buyer remains its own identity; consideration records a
   relationship, not bar continuity.
5. **Spinoff:** retain the parent identity and create a distinct child identity.
   Distribution terms link them without merging history.
6. **Missing identifiers:** ticker, exchange, and issuer label may create a
   candidate, never a verified identity.
7. **Conflict:** preserve every source claim. A material CIK, FIGI, issuer, or
   effective-time conflict is `unresolved` until independent adjudication.
8. **Retrospective correction:** append a new revision with its later
   `known_at`; never rewrite what an earlier as-of query could know.

## Publication states

- `verified`: the disposition is independently reproducible from locally
  hashed evidence and all effective/knowledge boundaries are present.
- `candidate`: evidence is incomplete but not materially contradictory.
- `unresolved`: material sources disagree or a transition cannot be bounded.
- `rejected`: the claim is disproven; its evidence remains append-only.

Only `verified` identity and symbol intervals may enter a PIT Silver revision.
The system may continue researching or ingesting raw evidence for candidate and
unresolved cases, but those cases cannot widen a repair manifest or mutate
Silver.

## Collision handling

Uniqueness is temporal and evidence-aware:

- The same `(provider, symbol, exchange)` may map to only one verified security
  at an effective instant, but it may map to different identities in disjoint
  intervals.
- A security may have multiple provider spellings (for example `BRK-B` and
  `BRK.B`) in the same interval. Spellings remain provider-specific aliases.
- The same external identifier cannot silently attach to multiple verified
  identities in overlapping intervals. Such an observation creates a conflict.
- A newly discovered collision changes this policy and its fixture manifest in
  a reviewed commit before any affected membership is published.

## LS-02.2b acceptance boundary

The storage implementation must import `cases.json` unchanged as acceptance
inputs. It must generate opaque IDs, append identifier and symbol intervals,
preserve every evidence ref/hash, enforce the dispositions above, and exclude
candidate/unresolved identities from PIT queries. Passing unit tests with a
ticker-derived ID does not satisfy this contract.

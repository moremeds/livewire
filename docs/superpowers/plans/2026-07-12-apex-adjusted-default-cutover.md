# Apex Adjusted-By-Default Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make adjusted Livewire bars the Apex production default after the shadow canary passes, while retaining an explicit raw diagnostic rollback.

**Architecture:** Change configuration defaults and deployment mounts only after verifying current Silver freshness and revision application. Add startup/readiness guards and operator-visible rollback instructions; do not change adjustment mathematics in this PR.

**Tech Stack:** Python 3.13, FastAPI health/readiness, Docker Compose, pytest, shell preflight.

## Global Constraints

- Work in `/Users/moremeds/projects/apex` on branch `feat/adjusted-bars-default` only after the canary acceptance record is approved.
- No silent raw fallback.
- Rollback is `APEX_LIVEWIRE_PRICE_MODE=raw`; it does not delete Silver.
- Production Compose changes require explicit user approval and a separate deploy verification.
- No PR merge without explicit approval.

---

### Task 1: Adjusted default and readiness guard

**Files:**
- Modify: `src/api/server.py`
- Modify: `src/api/routes/health.py`
- Modify: `scripts/serve.sh`
- Test: `tests/unit/api/test_server_lifespan.py`
- Test: `tests/unit/api/test_health.py`

- [ ] Write failing tests that unset `APEX_LIVEWIRE_PRICE_MODE` and expect `adjusted`, require readable Silver current manifest, and report readiness false for stale/invalid revisions while liveness stays true.
- [ ] Change the default to adjusted and add configurable `APEX_LIVEWIRE_MAX_REVISION_AGE_SECONDS` defaulting to `93600` (26 hours).
- [ ] Keep `/health` live but include `ready`, `price_mode`, observed/applied revisions, and revision age.
- [ ] Update `scripts/serve.sh` preflight to verify both bronze and Silver roots in adjusted mode without printing secrets.
- [ ] Run focused tests; expect PASS.
- [ ] Commit with `git commit -m "feat: default Apex to adjusted bars"`.

### Task 2: Production configuration and rollback documentation

**Files:**
- Operational edit after explicit approval: `/Users/moremeds/apex-deploy/compose.yml`
- Modify: `/Users/moremeds/projects/apex/README.md`
- Modify: `/Users/moremeds/projects/apex/docs/livewire-apex-integration.md`
- Modify: `/Users/moremeds/projects/apex/docs/argon-apex-api.md`

- [ ] Add read-only Silver bind mount and explicit adjusted mode:

```yaml
environment:
  APEX_LIVEWIRE_ROOT: /data/livewire/bronze
  APEX_LIVEWIRE_SILVER_ROOT: /data/livewire/silver
  APEX_LIVEWIRE_PRICE_MODE: adjusted
volumes:
  - /Volumes/DATA_LAKE/livewire/data-lake/bronze:/data/livewire/bronze:ro
  - /Volumes/DATA_LAKE/livewire/data-lake/silver:/data/livewire/silver:ro
```

- [ ] Document exact rollback: set mode to raw, recreate only Apex, verify health, and leave all artifacts intact.
- [ ] Document adjusted semantics for charts, indicators, warmup, volume, and revision fields.
- [ ] Run `docker compose -f /Users/moremeds/apex-deploy/compose.yml config`; expect exit 0 and both mounts.
- [ ] Commit the Apex repository documentation. Record the non-repository
  `apex-deploy/compose.yml` change in the cutover checklist rather than claiming
  it as a sixth PR.

### Task 3: Production cutover verification

**Files:**
- Create: `docs/operations/silver-cutover-checklist.md`
- Test: operational commands only; no code changes.

- [ ] Before deploy, verify Livewire `current.json` checksum, age under 26 hours, and successful NVDA/AAPL/SPY canary.
- [ ] Deploy through the existing Apex Compose workflow after explicit approval; do not restart Livewire or Xenon.
- [ ] Verify `/health`, `/bars/NVDA`, `/indicators/NVDA`, active subscription applied revision, and raw diagnostic mode in a separate non-production invocation.
- [ ] Observe one full polling interval and one live tick/bar cycle; require no reseed failures or dropped-buffer alerts.
- [ ] Record revision IDs, timestamps, commands, and results in the checklist.
- [ ] Run full Apex gates: `uv run pytest tests -q` and `uv run mypy src/ --ignore-missing-imports`; expect PASS.
- [ ] Commit with `git commit -m "docs: record adjusted bars production cutover"`.

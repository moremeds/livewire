# Per-response source evidence would cost 41 min a night

**Rule:** Commit the source-evidence manifest once per run under one global lock, never once per response; keep persist_raw inline and flush in a finally.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

- `MDW_SOURCE_EVIDENCE` (default `on`): set to `off`/`0`/`false`/`no` to stop
  `corporate-actions` collecting exact provider response bytes. The manifest is
  rewritten whole under one global lock, so evidence is committed **once per
  run**, never once per response — the per-response shape measured at
  2.8 us/row/call, i.e. **41 min a night** for ~29.6k responses against a
  ~29.6k-row manifest, against a nightly budget with only 76 min of headroom.
  `persist_raw` stays inline because a provider response that is not written
  before the run dies cannot be refetched; the flush runs in a `finally` for
  the same reason.

**Source:** CLAUDE.md section "Reliability foundation environment variables" (moved 2026-09-02)

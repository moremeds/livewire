# IB is not a single point of failure

**Rule:** Gate Silver on its own inputs only, and let an unreachable Gateway degrade a lane instead of failing or retrying the run.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

### IB is not a single point of failure

`rebuild-silver` reads equity bronze and the corporate-action store — both
Massive-backed. It never reads IB. So the Silver gate depends on exactly those
two lanes, **not** on futures/cmdty (IB daily), CBOE, or fx. Gating on every lane
meant one stale FX contract blocked the adjusted rebuild for the whole ~13K
equity universe.

IB legitimately owns futures/cmdty daily and volatility intraday
(VIX/SPX/NDX/RUT/VXN/RVX). It no longer owns fx — see "FX and DXY" below. The
rule is that IB *failure* must not cascade:

- An unreachable Gateway exits `GATEWAY_DOWN_EXIT_CODE` (86, distinct from 1
  and argparse's 2). The lane is **skipped, not retried** — 2FA and IBKR
  maintenance are not something livewire recovers, and retrying burns
  3×`retry_delay_seconds` against a dead port. It logs `=== Skipped <scope> ===`
  and the run is DEGRADED, not failed.
- `fetch_batch` maps a raised fetch to the exception, never to `[]`. Collapsing
  both meant a total IB outage classified every ticker `no_trade`, held
  `errors` at 0, and `resolve_exit_code` reported success for a run that
  ingested nothing.

**Source:** CLAUDE.md section "IB is not a single point of failure" (moved 2026-09-02)

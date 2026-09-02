# Tier follows the repair SOURCE, not the severity of the gap

**Rule:** Derive tier from the repair source per asset class — IB-sourced lanes are always Tier B — and raise on an unmapped asset class.

**Incident / measurement:**

- ⚠️ **Tier follows the repair SOURCE, not the severity of the gap.** Tier A
  means *repairable unattended*, which is a property of where the bar comes from:

  | Asset class | Source | Tier | Floor |
  |---|---|---|---|
  | equity | Massive | A inside the window, B below | rolling |
  | fx | Yahoo | A | none (deep history) |
  | volatility | CBOE | A | none |
  | rates | FRED | A | none |
  | futures, cmdty | **IB** | **always B** | n/a |

  IB-sourced lanes are **never** Tier A no matter how recent the gap: IB is
  2FA-gated and never auto-retries. Deriving tier from the equity Massive floor
  for every asset class put 76 non-equity findings in the Tier A manifest
  claiming an unattended Massive repair that does not exist for them.
  An unmapped asset class raises rather than defaulting — `repair_source` fails
  closed.

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)

# G14 is exculpatory-only, and a symbol absent from every session is not a terminus

**Rule:** Withhold G14 unless all three gates pass through clients.terminus.confirmed_terminus, and never call a symbol absent from every session of the window a terminus.

**Incident / measurement:**

- ⚠️ **`G14` is exculpatory-only and fails closed three ways.** It is withheld
  unless (1) the raw tape reaches the target session — checked on the
  `_symbols.parquet` file, not the directory, because an interrupted fetch
  leaves the latter; (2) the corporate-action store was reconciled *after* the
  terminus; and (3) no active split sits within ±10 days of it. Any gate failing
  yields an ordinary repairable gap **at Tier B**: "we could not check" must
  never render as a delisting, and must not be silently absorbed by the no-trade
  exemption either — that absorption is what hid EA/AVB/EQR for a month.
  `clients.terminus.confirmed_terminus` is the only entry point; composing the
  three parts by hand at a call site is how the two surfaces diverged once.
  ⚠️ **And a symbol absent from EVERY session of the window is not a terminus.**
  Leaving the tape is a transition; a symbol never observed on it has not been
  seen making one. Measured on the real tape 2026-09-01: BK is absent from all
  30 sessions while BNY prints in all 30 — a rename, not a delisting. The action
  store carries only splits and cash dividends, so no event can ever explain a
  ticker change and all three gates pass on **silence rather than evidence**.
  Without this precondition a live S&P 500 member renders as delisted. The stale
  `BK` preset row is a separate fix.
  A withheld terminus counts as **missing but is never auto-recovered**: a fetch
  for an instrument we could not prove still prints cannot succeed, so it stays
  in the ratio and in `still_missing` where the alert names it, and no Massive
  batch is queued for it.

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)

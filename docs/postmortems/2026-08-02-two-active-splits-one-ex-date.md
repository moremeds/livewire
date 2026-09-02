# Two active splits on one ex-date double-adjusted the factor intervals

**Rule:** Collapse equal duplicate ratios (removing the entry from action_factors) and fail closed on unequal ones; count duplicate records inside stored bronze, not affected symbols.

**Incident / measurement:**

#### Two active splits on one ex-date

`latest_active()` dedupes on the **provider-scoped** `provider_event_id`, so one
logical event recorded under two ids survives twice — and `build_factor_intervals`
multiplied every active action, both into `splits_by_date` and in the per-bar
loop. The store has always assumed one active split per ex-date
(`corporate_action_store.py`, `# ponytail: one active split per ex-date is
assumed`); nothing enforced it.

Measured 2026-08-02: 18 such ex-dates across 16 symbols. They are not two events
— they are one event disagreeing with itself: exact inverses (LIME `300:1` and
`1:300`, TTSH `3000:1` and `1:3000`), ratios that migrated between dates across
revisions (TSM 2007 and 2009 swapped), or the same ratio restated at another
scale (PGC `10:11` and `100:110`, CZFS `1:1.01` and `100:101`).

- **Equal ratios collapse.** One event written twice applies once. Dropping the
  duplicate from `action_factors` is the half that matters — checking the ratio
  without removing the entry still double-adjusts in the per-bar loop.
- **Unequal ratios fail closed**, quarantining the symbol. Nothing in the store
  says which is right, so publishing either one would be a guess.

⚠️ **Count duplicate records, not affected symbols.** Only **5** of the 16 have
their duplicate ex-date *inside* stored bronze (FTLF, LADR, MDRR, OUT, SLG); for
the rest it is prehistory and touches no stored row, exactly as
`first_trade_date < action.ex_date` already required. All 5 were independently
absent from Silver, so the production impact at discovery was **zero** — the bug
was latent, not active. Reading blast radius off the action store alone
overstates it every time.


**Source:** CLAUDE.md section "Two active splits on one ex-date" (moved 2026-09-02)

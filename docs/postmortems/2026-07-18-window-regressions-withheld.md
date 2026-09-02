# A window start that moves later is withheld from republication

**Rule:** Withhold any symbol whose window start regresses and keep serving its previous window; --allow-window-regression is for the rev-3 bootstrap only.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

#### Window regressions — the prevention invariant

A symbol whose window start moves **later** than the revision currently serving it
is **withheld from republication** and keeps serving its previous window. This is
the fail-closed half of the contract: the suffix rule trusts the newer side of a
break, which is right for the 2021-06 seed artifact and wrong for a bad new bar —
a corrupt close arriving tonight would otherwise collapse the window onto itself
and publish one garbage row. The nightly digest reports the count under
**Silver rebuild**; the run still exits 0, so the digest is the only alert.

`--allow-window-regression` overrides it. **Required exactly once, for the rev-3
bootstrap**, because rev-2 published untrimmed history and every intentional trim
looks like a regression on that first run.


**Source:** CLAUDE.md section "Window regressions — the prevention invariant" (moved 2026-09-02)

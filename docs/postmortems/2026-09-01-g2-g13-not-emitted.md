# G2 (interior) and G13 (head) produced zero true findings out of 501

**Rule:** Do not emit G2 or G13, and do not let a registry row declare them, until a measurement asks for it.

**Incident / measurement:**

  ⚠️ **`G2` (interior) and `G13` (head) are named in the taxonomy and NOT
  emitted.** They produced zero true findings out of 501 on the first production
  run, and interior absence judged from bar files alone is the circular question
  that made the 5m scan flag 96.6% of the universe. `registry/gaps.json` rows no
  longer declare `G2`, and `tests/test_gap_registry_contract.py` fails if one
  does. Reinstating either needs a measurement asking for it; `classify()`'s
  signature does not change either way.

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)

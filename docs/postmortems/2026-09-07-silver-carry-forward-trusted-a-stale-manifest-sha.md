# _carry_forward trusted the previous manifest's sha and wedged Silver for eight days

**Rule:** A carried-forward artifact takes its sha256 from the file on disk (`_sha256(path)`), never from the previous manifest. The manifest describes the bytes being served; a stale sha only makes the publish fail.

**Date:** 2026-09-07. Host: macmini (production).

**Incident / measurement:**

`livewire_scripts/rebuild_silver.py::_carry_forward` re-lists artifacts for symbols a
run did not republish and built `PublishedArtifact(path, artifact.sha256, ...)` from
the sha recorded in the **previous** manifest. Its twin `_carry_orphans` already did
the right thing — `_sha256(path)`, skip the symbol when the file vanished. The twin
was never fixed with it (`CLAUDE.md` "How to work in this repo" §5).

`clients/silver_revision.py::_validate_artifacts` re-hashes every artifact at publish
time, so any drift between disk and the recorded sha raises
`ValueError: artifact checksum mismatch: <relative>` and aborts the whole revision.

What happened on macmini:

- The 2026-09-06 silver lane ran 16:31:57Z→18:31:57Z, hit its 7200s `LANE_BUDGET_S`
  and was SIGKILLed by process group (`lane_results` outcome=timeout, exit_code=124)
  **after it had entered the publish transaction**. It had written 25,318 of the
  manifest's 26,506 artifact files and never committed revision 39.
  `silver/revisions/current.json` is still revision 38, published 2026-08-30T11:12:51Z.
- The 2026-09-07 run re-staged everything, found those on-disk files byte-identical to
  what it would write, so they were not in `changed`, went through `_carry_forward`
  with revision 38's stale sha, and `_validate_artifacts` raised on the alphabetically
  first symbol: `artifact checksum mismatch: adjustments/asset_class=equity/symbol=A/factors.parquet`.
  Symbol A's `factors.parquet` (mtime 2026-09-06 17:44:54Z) and `1d.parquet` both
  differ from the revision-38 sha.

It is self-perpetuating: the files equal staging output, so they are never `changed`,
never rewritten, and the manifest sha is never refreshed. Every subsequent run fails
identically.

**Cost:** Silver stuck at revision 38 since 2026-08-30 — apex served 8-day-old adjusted
data — plus one failed nightly run every night from 2026-09-07 until the fix ships.

**Fix:** `_carry_forward` computes the sha from disk and drops the symbol when
`_sha256(path)` is `None`, mirroring `_carry_orphans`. The
`pq.ParquetFile(path).metadata.num_rows` read stays: it is the corruption guard, since
a truncated file raises there rather than being silently manifested.
→ test: `tests/test_rebuild_silver.py::test_carried_symbol_takes_its_sha_from_disk_not_the_stale_manifest`

**Follow-up (named, not fixed here):** an interrupted publish leaves the lake ahead of
the manifest and there is no reconciliation step. A killed publish transaction abandons
partially written artifact files that no later run reconciles against the committed
revision; the publish transaction itself was deliberately not redesigned in this fix.

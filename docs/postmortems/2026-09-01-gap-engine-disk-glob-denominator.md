# The disk-glob denominator could not count a symbol that never landed

**Rule:** Derive the coverage denominator from registry/gaps.json x presets/*.json x the trading calendar, and keep exactly one detector.

**Incident / measurement:**

⚠️ **This is the defect it replaced.** `coverage_report.py` used to build its
denominator by globbing `bronze/asset_class=*/symbol=*/`, so numerator and
denominator were drawn from the same set and **a symbol that never landed could
not be counted missing**. Measured 2026-09-01 on the local lake: the registry
denominator returns 11 `G3` findings for `presets/futures-active.json` because
`bronze/asset_class=futures/` **does not exist at all** — there is no directory
for a glob to enumerate, so the disk-glob detector reported nothing wrong. The
old hardcoded non-equity tuple also omitted `fx` and `cmdty` outright.

⚠️ **A second detector is forbidden, and there is a test for it.**
`tests/test_coverage_orchestration.py` asserts that `gap_scan.py` does not come
back and that `classify()` keeps exactly one production caller. Two detectors
answering one question with two denominators is what spec §11 criterion 7
forbids — the retired `gap-scan` carried neither the no-trade exemption nor the
ingestion deadline, so it and `coverage` disagreed about the same symbol nightly.

```bash

**Source:** CLAUDE.md section "Gap engine — the denominator is not the disk" (moved 2026-09-02)

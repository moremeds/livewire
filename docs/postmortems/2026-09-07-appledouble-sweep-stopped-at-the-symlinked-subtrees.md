# The AppleDouble sweep stopped at the symlinked subtrees and called it a clean run

**Rule:** A sweep of the lake walks it with `os.walk(..., followlinks=True)`, never `Path.rglob` — `rglob` does not descend into a symlinked directory. And a sweep that entered none of the lake's large subtrees says so at WARNING; it never renders as a clean summary.

**Incident / measurement:** 2026-09-07, host **macmini**.

The lake layout changed on **2026-09-06**: `~/market-warehouse/data-lake` is now a
real directory on the internal APFS disk, and its big subtrees are individually
symlinked onto the exFAT volume — `bronze`, `bronze-archived`, `bronze-delisted`,
`gold`, `quarantine`, `repairs`, `security_master` all point at
`/Volumes/DATA_LAKE/livewire/data-lake/<name>`, while `cursors`, `ledger`,
`operations` and `raw` are real directories.

`plan_appledouble` did `data_lake.rglob("._*")`. `pathlib` does not follow
symlinked directories when recursing, so from 2026-09-06 the sweep walked only
the small real internal directories:

- `housekeeping --apply --appledouble` reported **`71 item(s) deleted, 0 failed`** —
  a success, exit 0, no warning.
- From the internal root, `sum(1 for _ in root.rglob('._*'))` returns **88**.
- The sidecars the command exists to remove were untouched:
  `bronze/asset_class=equity/symbol=RJF/` still held `._1d.parquet`, `._1h.parquet`,
  `._1m.parquet`, `._30m.parquet`, `._5m.parquet` plus their `.lock` companions.
- The baseline is **324,121 sidecars in 2047s**
  ([pm:2026-08-10-appledouble-sweep-cost](2026-08-10-appledouble-sweep-cost.md)),
  measured when the whole `data-lake` was itself one symlink to the volume — so
  the sweep crossed it as its own *root* rather than as a *child*, and `rglob`
  never had to descend through a link. The 2026-09-06 layout moved the link one
  level down and the same code silently covered ~0.03% of the lake.
- Operator workaround, verified the same day:
  `MDW_DATA_LAKE=/Volumes/DATA_LAKE/livewire/data-lake` makes `data_lake_dir()`
  resolve to the real tree and the walk covers everything.

**What it cost:** a day of believing the sidecars were gone. They are not inert —
they pollute every `*.parquet` glob (one reader, `warehouse_health_report`, was
reading them as fabricated timeframes), and 324k extra directory entries slow
every walk of an exFAT lake that already measures 1400–2860s cold.

**Why the loud signal, not just the walk fix:** the walk bug is one line; the
reason nobody noticed for a day is that the sweep's only output was a clean
summary. `os.walk` yields a dirpath for every directory it enters, empty ones
included, so a first-level subtree missing from the visited set was not walked at
all — a dangling symlink (the volume is unmounted), a target off the lake, or an
unreadable directory. Each one now logs
`appledouble sweep never entered <path> — it swept nothing there`. Rule 9: a
detector with no output is dead, not healthy.

**Containment:** the walk may enter the lake root and whatever the lake's own
first-level entries resolve to — that is exactly how the volume is reached now.
A deeper symlink resolving outside all of those is off the lake and is not
followed, and every followed symlink target is recorded by `(st_dev, st_ino)` so
a loop terminates. `_is_protected` is unchanged and still sees unresolved paths
under the lake root it was handed.

**Source:** new incident, 2026-09-07. Fix and tests in
`tests/test_housekeeping.py::TestTheSweepCrossesTheSymlinkedSubtrees`.

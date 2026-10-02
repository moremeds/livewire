# Livewire price-discovery evidence — 2026-09-22

Base/current checkout: `f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2`. Initial dirty state: only the three untracked PRICE_DISCOVERY Markdown documents. They were preserved. No production mutations, restarts, commit, merge, or deployment.

## Read-only runtime capture

At 07:57:20 UTC, read actual mini files via SSH and PyArrow, not the catalog. Subsequently copied those four artifacts with scp; local SHA256 values agree with the read-only probe. The saved directory `2026-09-22-capture/` contains exact bytes; source was `~/market-warehouse/data-lake` on `macmini`.

| Artifact | Rows / coverage | SHA256 |
|---|---|---|
| silver-revision-76.json | committed revision 76 | 2fdc32ac718a739da439e96494e37521d937465238c6567bc4ed01d5035647df |
| GLD-bronze-1d.parquet | 5,491 / 2004-11-18–2026-09-21 | ab557a88c9f669b298734244e00c57f983b45cb439ec0a449f65a6345d233a59 |
| GLD-silver-1d.parquet | 5,490 / 2004-11-18–2026-09-18 | 6919b932606a09bb833c178999f765d3a6d1effd4e841d3887d6d8c1a48b1227 |
| GLD-factors.parquet | one interval, both factors 1.0 | 206356987a115cd7435cc8bc233f79992d068569544bfe55e5035630ca0b4bb6 |

Manifest generation: `20260920T063649.335749Z-c2009d879b044c7c8777b0b93fea955d`; lake publication `2026-09-20T07:09:52.039975Z`; corporate-actions-as-of `2026-09-20T06:03:52.588833Z`. These are producer clocks, not historical upstream availability.

GLD Bronze observed contiguous source/basis segments:

| Dates | Rows | Source | Input basis |
|---|---:|---|---|
| 2004-11-18–2021-05-17 | 4149 | ib | raw |
| 2021-05-18–2026-07-13 | 1293 | legacy | unknown |
| 2026-07-14–2026-09-21 | 49 | massive | raw |

Silver daily schema omits source and input basis. Its producer applies split and dividend back-adjustment, but current Bronze is not an immutable historical input reference for revision 76. Do not assign Bronze source to Silver as proven lineage. Original provider response bytes and historical receipt/available-at/vintage were not established. Revision identity supports replay of today's reconstruction, not a historical-PIT claim.

DXY actual Bronze: 14,149 daily rows from 1971-01-04 through 2026-09-22; no source or timing columns; SHA256 `885c1966b1765242bf101df62c4c2e99221bc3de5052a60b613fb6d4ca769b52`. Last row was already present at 07:57 UTC on Sep22; current daily session is incomplete. Source code `fetch_fx.sync_daily` uses Yahoo, but historic row-level provenance is absent. This series is not DTWEXBGS and is not required for the default gold study.

Bounded EIA inventory: tracked clients/scripts/presets search found no EIA/gasoline integration; mini raw top-level directories were `massive`, `shepherd`; rates partitions DGS3/DGS5/DGS10/DGS30; cmdty XAUUSD. This is not an exhaustive whole-lake absence claim. No EIA collector or production backfill was started.

## EIA no-key access evidence

The [official gasoline update](https://www.eia.gov/petroleum/gasdiesel/) links a [full-history XLS](https://www.eia.gov/petroleum/gasdiesel/xls/pswrgvwall.xls). At approximately 08:01 UTC, a read-only `curl --fail --location --max-time 25` through `ssh macmini` fetched its response directly into this worktree. A second request returned HTTP **200**, **6,792,704 bytes**. The preserved `2026-09-22-capture/eia-pswrgvwall.xls` hash is `5af9f7facd07ada639200cfe773756ab30a1ba46d988349a804d1c4df6f0c4cc`. Egress label: mini-current; Singapore egress remains untested. No API key was supplied or needed for this URL.

The [official US weekly table](https://www.eia.gov/dnav/pet/pet_pri_gnd_dcus_nus_w.htm) distinguishes all-grades, regular, midgrade, premium gasoline and diesel. Frequency **weekly**, geography **United States**, unit **dollars per gallon including taxes**. The downloaded link is labeled **regular gasoline**, which must not silently stand in for all-grades gasoline or the NSA gasoline CPI target. The table reports Sep15 publication and Sep22 next publication for observations through Sep14; no intraday precision or historical vintage guarantee was established. It also documents a May14,2018 gasoline methodology discontinuity. Access succeeds; historical release vintages and CPI target vintage remain unverified. No installed XLS reader was found, so workbook sheet/row coverage is not claimed; no dependency was added.

The same mini egress captured that source HTML as `2026-09-22-capture/eia-weekly-us.html` (22,524 bytes; SHA256 `74c0f1864db409d2b4bbe2e14c2fae389e69e62869f224b380c0d682643c8a28`). The raw table is durable evidence for identity/unit/release-date claims; it is not historical vintage evidence.

## Minimal implementation after lead contract freeze

`clients.research_export.export_gld_research(silver_root, bronze_path, destination)` uses existing `SilverSnapshot`, verifies selected GLD immutable references, retains full manifest/Parquet bytes, and records separate Bronze segments. Unknown Silver upstream identity/basis and historical availability remain explicit nulls. A content-addressed ZIP is atomically published with an exclusive hard link, fsync, and idempotent verification; failures cannot overwrite prior captures. No canonical Parquet schema change, service, scheduler, dependency, or second lake write path.

Real local export: `2026-09-22-capture/exports/0daabdd5851870c5a72e864f3463a8538e4e5064968564ccce0553f64bb76b7f.zip`; SHA256 `ea39849aecbece59fec299cc15c92ac3cc8c8ff72fcb6f9f8155aaca2b5927b4`. The archive contains raw artifact bytes and metadata, not just URLs. Its receipt is local capture time, not original ingest receipt. Apex independently reads pinned Silver; no adapter dependency on this ZIP is required.

## Checks

- Baseline `uv run pytest tests/test_silver_snapshot.py tests/test_silver_revision.py tests/test_fetch_fx.py tests/test_fetch_fred_rates.py tests/test_fred_client.py -q`: **90 passed**, 4.42s.
- New capture plus Silver checks: `uv run pytest tests/test_research_export.py tests/test_silver_snapshot.py tests/test_silver_revision.py -q`: **38 passed**, 3.75s. Test uses the above real capture and checks repeated export identity, unknown lineage, original manifest retention, corrupt-source rejection, and preservation of old archive.
- `uv run ruff check clients/research_export.py tests/test_research_export.py`: passed.
- Final `uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q`: **3038 passed**, 2 warnings, **95.04% coverage**, 156.49s. Initial full run exposed an unreachable-module gate and 94.96% coverage; adding the public client export and concrete fault/retry checks resolved both. No gate was weakened.
- Final Ruff check/format and `git diff --check`: passed. Self-review added failure-publication, concurrent-publication, empty-input, missing-artifact, and corrupted-existing-capture checks. The export is local and uncommitted; no production execution claim.

Open work: Yahoo finished-session capture policy; select gasoline grade/series and parse full-history capture only after identity contract is agreed. Historical input lineage/timing remains unknown. These gaps do not prevent current-revision gold research, but prohibit claiming historical PIT validity or a deploy/run acceptance from unit tests.

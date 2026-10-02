# Expired futures daily backfill run — 2026-09-23

## Scope and result

Executed the bounded daily-only IB task for `OJ_202609` (seed) and
`CL_202610` (backfill) with the candidate `--include-expired` implementation.
No intraday requests, other symbols, shared cursors, deployment, or Gateway
restart were performed.

| Contract | Mode | Robust result | Before | After / observed |
|---|---|---:|---|---|
| `OJ_202609` | `seed` | `ok`, +746 rows, 8 s | No target parquet | 746 unique daily rows, 2023-10-03 through 2026-09-22; readable Parquet, 20,170 bytes |
| `CL_202610` | `backfill` | `ok-noop`, 7 s | 2,137 rows, 2018-01-24 through 2026-09-22 | Unchanged: 2,137 unique dates, same bounds, 48,493 bytes; IB reported no older history |

The exact-path Parquet read used the production release Python with PyArrow's
`ParquetFile(...).read()`. The schema was readable for both files and every
stored date was unique. OJ's output contains 746 rows; the prior evidence says
that the contract's last-trade date was 2026-09-10. The returned bars extend
through 2026-09-22; this run records provider output and does not reinterpret
the expiry metadata.

## Runtime and write boundary

- `current` resolved to release `65a0d96df1e35937ac99b2c042bb6fddaeb40592`.
- IB Gateway loopback `127.0.0.1:4001` was reachable immediately before the
  fetches.
- The only Bronze target files were
  `asset_class=futures/symbol=OJ_202609/1d.parquet` and
  `asset_class=futures/symbol=CL_202610/1d.parquet`.
- OJ's output and its lock/meta sidecars were created at 16:55 on the Mini.
  CL's parquet remained at 48,493 bytes with its earlier 13:00 modification
  time. No futures ingestion process remained in the post-run process check;
  the observed scheduled `daily` child was explicitly `--asset-class equity`.
- Run cursors, telemetry and orchestrator summaries were isolated under
  `~/market-warehouse/logs/expired-futures-backfill-20260923/`. Summaries:
  `orch/orch_seed_20260923_085458Z/_summary.log` and
  `orch/orch_backfill_20260923_085516Z/_summary.log`.
- Seed telemetry recorded IB connected and disconnected normally. Robust
  outcomes were `ok=1` for OJ and `ok-noop=1` for CL; no failed/timed-out
  ticker was reported.

## Deviations and limits

- The task-specific log directory was present and empty at the final preflight
  snapshot. The runner proceeded to use that empty directory instead of
  selecting a new unique path as its contract required. It contained only
  this run's custom cursors, telemetry and summaries afterward; no shared
  cursor path was used. This is a process deviation, not an observed data
  collision.
- During preflight, the runner listed up to 50 names from the futures Bronze
  directory, beyond the exact-target-only read scope. This was read-only and
  did not trigger additional requests or writes.
- CL returned no older history. No claim is made that IB can serve history
  older than the window observed for this contract, or that other expired
  futures were backfilled. Intraday remains untested and out of scope.

## Verification

- Robust summary: OJ `ok`, +746; CL `ok-noop`, no older history.
- Independent exact-file PyArrow read: both Parquets readable; OJ 746/746
  unique dates; CL 2,137/2,137 unique dates; expected date bounds above.
- Independent exact-process check: no expired-futures task or futures fetch
  remained active; scheduled daily child was equity-only.
- No code commit, deployment, push, PR or merge was made.

herd-report expired-futures-backfill-runner task 3: commit none, evidence docs/audits/price-discovery/EXPIRED_FUTURES_RUN_2026-09-23.md, deviations: task log directory already existed but empty and was reused; preflight listed up to 50 futures directory names

# Expired futures backfill preflight — 2026-09-23

Lead-run, read-only production preflight. As of 2026-09-23 08:25 UTC, the
bounded target is only `CL_202610` and `OJ_202609`, the two qualified contracts
omitted from the live strip because their last-trade dates had passed. IB's
TWS API allows `includeExpired` for futures, with history limited to two years
after expiry; both contracts are inside that window.

## Exact target state

Command run from the Mac mini:

```sh
CURRENT=$(readlink /Users/moremeds/market-warehouse/current)
printf 'current_release=%s\n' "$CURRENT"
/Users/moremeds/market-warehouse/current/.venv/bin/python -c 'from pathlib import Path; import pyarrow.parquet as pq
base=Path.home()/"market-warehouse/data-lake/bronze/asset_class=futures"
for s in ("CL_202610","OJ_202609"):
 p=base/f"symbol={s}"/"1d.parquet"
 if not p.exists(): print(f"{s}: missing {p}"); continue
 pf=pq.ParquetFile(p); a=pf.read(columns=["trade_date"]).column("trade_date").to_pylist()
 print(f"{s}: rows={pf.metadata.num_rows} min={min(a) if a else None} max={max(a) if a else None} path={p}")'
```

The Python command read only `trade_date` and Parquet metadata at:

- `CL_202610`: `asset_class=futures/symbol=CL_202610/1d.parquet`, exists,
  2,137 rows, date range `2018-01-24`–`2026-09-22`.
- `OJ_202609`: `asset_class=futures/symbol=OJ_202609/1d.parquet`, absent.

Output: release symlink resolved to
`/Users/moremeds/market-warehouse/releases/65a0d96df1e35937ac99b2c042bb6fddaeb40592`.
The process already active is PID 18448/18449 with a child using release
`f83e3df0c4c9ba1ba54d6e5e9abd49be50b278e2`; it has been active for about 3h23m.
The current release pointer has moved but this process remains on its initial
release. `lsof` showed no open target Parquet path and no holder of the existing
`CL_202610` symbol lock; the `OJ_202609` lock file does not exist. Check again
immediately before the write because the daily process is still active.

Other preflight commands and results:

| Command | Exit | Result |
|---|---:|---|
| `launchctl list | awk '/livewire|market-warehouse|mdw/ {print $1, $3}'` | 0 | Livewire daily-update is active; other listed Livewire scheduled jobs showed no running PID. |
| `pgrep -f '[l]ivewire_ingest.py|[l]ivewire_ops.py|[l]ivewire_store.py|[l]ivewire_quality.py'` followed by `ps -p ... -o pid=,comm=` | 0 | Active daily-update shell/Python and its child are visible. |
| `nc -z -G 5 127.0.0.1 4001` | 0 | Loopback Gateway port is reachable. No IB historical request was made. |
| `df -h /Users/moremeds/market-warehouse/data-lake` | 0 | 35 GiB available at inspection. |
| `lsof -p 18449,75177 | grep -E 'CL_202610|OJ_202609'` | 0 (no matches) | No target Parquet path was open at inspection. |
| Loop over `ls -l` and `lsof -F pfn` for `symbol=CL_202610.lock` and `symbol=OJ_202609.lock` | Output captured; exit not recorded separately | CL lock file exists with no `lsof` holder; OJ lock file is absent. |
| `test -e ~/market-warehouse/logs/expired-futures-backfill-20260923` | not run | Recheck before creating a task-specific log directory. |

## Limits and next check

- No production bars, cursors, logs, or data were written during preflight.
- The active daily-update process is still running. Before backfill, verify it
  has exited or recheck that it is not approaching either target; repeat the
  exact-path lock check and confirm the isolated task log directory is absent.
- `OJ_202609` will be seeded; `CL_202610` needs backfill mode so existing rows
  are preserved while older bars are merged.
- No `includeExpired` historical request has been made yet. Contract
  qualification/history availability must be observed during the authorized
  bounded run.

## Herd scout disposition

`expired-futures-scout` (Devin SWE-2 Max, Mac mini pane `w1:p4`) was assigned a
read-only preflight first. It confirmed the loopback port and listed Livewire
processes but did not return its required report/evidence. A request to inspect
the daily log and a broader agent-roster query were denied as outside the
bounded evidence need. The lead independently ran and recorded the checks
above; this report is lead evidence, not an accepted worker report.

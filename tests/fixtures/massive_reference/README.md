# Frozen Massive reference responses

Exact bodies of `GET https://api.polygon.io/v3/reference/tickers`, recorded from
the Mac mini with the production key on 2026-09-15 (spec
`docs/superpowers/specs/2026-09-15-security-master-backfill-design.md` §2, §7).
Real tickers, real values, captured once — tests never hit the network.
Re-record only by rerunning the capture in Task 0 of the plan and updating this
table; never hand-edit a body.

| file                                              | request                                             | as-of      | sha256                                                             |
| ------------------------------------------------- | --------------------------------------------------- | ---------- | ------------------------------------------------------------------ |
| aapl-active-2026-09-15.json                       | `?ticker=AAPL`                                      | 2026-09-15 | `5b1e89eaf77f714a04148d640b75d97aba509f4eb41d2f0f2f65fb83fe789fbe` |
| aapl-date-2010-01-04-2026-09-15.json              | `?ticker=AAPL&date=2010-01-04`                      | 2026-09-15 | `0a166f2c09db363bfe1bac9ec611414d6623f02576171535a12165c1f5b9139a` |
| aapl-date-1998-01-05-2026-09-15.json              | `?ticker=AAPL&date=1998-01-05` (empty)              | 2026-09-15 | `11e4693b89cb3df7b3e1e7fffa53b97b2de8df48fa473103bdaa66ba0f8c9d6f` |
| yhoo-active-false-2026-09-15.json                 | `?ticker=YHOO&active=false`                         | 2026-09-15 | `d74951c5931be9ed8b078e79b6be32a37ac05d195dca1c79732d88dc982bb922` |
| yhoo-date-2015-06-01-2026-09-15.json              | `?ticker=YHOO&date=2015-06-01`                      | 2026-09-15 | `5b0f6e06ed9af04fca61f79b30d93ab28dd8073306995caa11e45a1a3b8d6300` |
| yhoo-date-2015-06-01-active-false-2026-09-15.json | `?ticker=YHOO&date=2015-06-01&active=false` (empty) | 2026-09-15 | `71b2d2fc4364fc217520ba7c4ddc7fac6555f20722dca6a0e0479285fb8b2148` |
| aaba-active-false-2026-09-15.json                 | `?ticker=AABA&active=false`                         | 2026-09-15 | `57f49956e8efbef3daa687054684579ba5176a1688f01444ca4539b169f49754` |
| aaba-date-2018-06-01-2026-09-15.json              | `?ticker=AABA&date=2018-06-01`                      | 2026-09-15 | `9cd81115fde049f83cbf7fbcfd2a09953c9de04bbb31135f694e55fd2c2f3fa5` |
| aaba-date-2018-06-01-active-false-2026-09-15.json | `?ticker=AABA&date=2018-06-01&active=false` (empty) | 2026-09-15 | `8817938e503702a93666877a0d8357f888cba674cc9d99b5e426d16a105354e3` |
| wlp-active-false-2026-09-15.json                  | `?ticker=WLP&active=false`                          | 2026-09-15 | `bdb4c060d186aa033a0bf3d6661983b2dafe967ace9d1aa565a4ee678565045b` |
| dell-active-false-2026-09-15.json                 | `?ticker=DELL&active=false` (empty)                 | 2026-09-15 | `2d1ee42d8b42c64c3a096002a1db6dca9e76c2ef4565912a967c4849132e82f5` |
| aamrq-active-false-2026-09-15.json                | `?ticker=AAMRQ&active=false` (empty)                | 2026-09-15 | `e7d2142541451d6018ea84bbbd096d9d77997f4f92d1af60c3191818e58c2ac5` |
| imo-active-2026-09-15.json                        | `?ticker=IMO` (XASE listing)                        | 2026-09-15 | `462b50fb0056c7088314711df4f5c80a053e4b3526ce27670626de191ad26ccb` |
| delisted-earliest-2026-09-15.json                 | `?active=false&sort=delisted_utc&order=asc&limit=1` | 2026-09-15 | `ec4678c57853e629939b757433f312b183f9347b71318bb527f7e419772eb612` |

## Observed on capture

Seven facts measured on the mini on 2026-09-15 against the production key. They
contradict assumptions the plan and spec were written on, so the tests that load
these bodies are written to the facts, not to the assumptions.

- **F1.** No body carries `list_date`. This list endpoint never returns it, so
  the `date=` probe always runs for every ticker and `effective_from` always
  comes from `existed_at` (the probe date) or from nothing.
- **F2.** An empty envelope has no `count` key at all:
  `{"results":[],"status":"OK","request_id":"..."}`. A parser must not read
  `count` to decide emptiness.
- **F3.** `date=` combined with `active=false` returns an empty envelope even
  for a ticker that existed on that date. The date probe must send only
  `ticker` and `date`.
- **F4.** A `date=` body may omit `primary_exchange`:
  `aapl-date-2010-01-04` has none, `yhoo-date-2015-06-01` has `XNAS`.
- **F5.** Delisted records may carry no FIGI. `yhoo-active-false` and
  `wlp-active-false` carry `cik` only; `aaba-active-false` carries
  `composite_figi` `BBG000KB2D74`, `share_class_figi` `BBG001S8V781` and `cik`
  `0001011006`. YHOO's `cik` is `0000316736`. So YHOO → AABA is **not** a
  FIGI-matched rename in this data: YHOO derives to a `candidate` row with
  identity basis `provider_reference` and AABA to a `verified` `provider_figi`
  row — two security_ids, no conflict.
- **F6.** All 13,181 active US stock listings (14 pages of
  `?market=stocks&active=true&limit=1000`) are `currency_name: usd`. No real
  non-USD fixture exists on this endpoint, so the planned `nonusd-<TICKER>`
  fixture was not created.
- **F7.** `?ticker=AABA` without `active=false` is empty (AABA delisted
  2019-10-07). The plan's `aaba-active` file was replaced by
  `aaba-active-false`.

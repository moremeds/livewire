# Futu OpenD 行情接口探测（免费权限）— 2026-10-16

只读探测；后半部分（A–C）是同日的独立查证，把前半部分「未确定」的地方实测定了下来。主机：MacBook（OpenD GUI 10.11.7108，`127.0.0.1:11111`，`futu-api` 10.03，xenon 的 venv）。
mini 装了 `Futu_OpenD.app`，探测时**没有运行**。没有写 lake，没有改代码。

## 账号实际权限（`get_user_info`，观测值）

| 市场 | 权限 |
|---|---|
| 美股 | LV3（官方文档：推广期免费） |
| 美股期权 | LV1（文档：总资产 ≥ HKD 20,000 免费） |
| 港股 / 港股期权 / 港股期货 | LV1 |
| 美股期货（CME/CBOT/NYMEX/COMEX/CBOE） | 无 — `US.CLmain` 快照返回 "Insufficient quote permission" |
| A 股 | 无（国际用户）— `SH.600519` 返回无权限，且**一个无权限代码会让整批快照失败** |

额度：订阅 1000，历史 K 线 1000 只 / 7 天滚动（第三档：资产 ≥ HKD 50 万，或月成交 > 200 笔，或月成交额 > HKD 200 万）。
期权另有独立额度：该档订阅 200、历史 K 线 200。官方原文按**期权链**（同一到期日的所有期权）计数："每请求 1 条期权链的历史 K 线，将会占用 1 个历史 K 线额度"。`get_history_kl_quota(get_detail=True)` 能列出已占用的期权链，但不显示 200 条还剩多少（B7）。额度随资产/交易量自动调整，不是永久。

## 历史深度（观测值，2026-10-15 ~12:13 ET 测）

- 日 K：AAPL、00700 都从 2006-10-02/03 开始，大约 20 年（比 now−20y 早 13 天，不精确）。
- 1 分 K：滚动窗口，精确到分钟，第一根 bar 始终是「现在 − 2,928 天」（A1）。`time_key` 标的是 bar 结束时刻（A3）。按天滚动没有跨日测过。
- 退市股票不提供：5/5 回 `Unknown stock`，所以历史只有现存股票（A2）。
- 盘前盘后：`extended_time=True` 给 04:01–20:00 ET，960 根/天；文档说 ≤60 分钟级别，不足 2 年。
- 期权：日/1/5/15/60 分 K。`US.SPY261120P730000` 日 K 有成交量。
- `time_key` 是交易所本地时间，不是 UTC。`autype=AuType.NONE` 给未复权价。

## 吞吐（观测值）

AAPL 1 分 K 一年 = 97,693 根，98 页，7.1 秒（每页 1000 根）。
限频 60 次 / 30 秒只算每只股票的**首页**，后续翻页不限（文档原话，和实测相符）。
一只股票 8 年 1 分 K 实测 48.9 秒（A5）；额度 1000 只/周是真正的瓶颈，不是速度。

## 与 livewire 对数（mini 生产 lake，`bronze/asset_class=equity/symbol=AAPL/1d.parquet`，source=massive，raw）

2026-10-08..14 五个交易日：OHLC 完全一致；volume 差 0–4,305 股（< 0.02%，合并成交量口径差）。
mini 上 AAPL 1m/5m 最早 `2021-05-21 13:30Z`（Massive 平面文件 5 年下限）。

## 对 livewire 的补充价值

1. **2018-10 → 2021-05 的美股 1 分 K**：Massive 拿不到的约 2.6 年，只有现存股票（A2）；减 1 分钟后和 Massive 对齐（A3）；改名股票要按 security_id 映射（A4）。
2. **不需要每次手动 2FA 的 Tier B 来源**（但 OpenD 登录会触发短信验证，SDK 有 `NeedPhoneVerifyCode`、`KickedOut` 事件 — 能否无人值守未验证）：Massive 窗口以下目前只有 IB（手动 2FA）。Futu 覆盖日 K 2006 起、1 分 K 2018 起。
3. **独立复权因子**（`get_rehab`：拆股/分红 + 前后复权因子）— 可交叉核对 Silver 的 corporate actions。
4. 新资产：港股 + 港股期货 LV1、美股期权 K 线（livewire 现在都没有）。

不能补的：美股期货（要期货账户 + 付费卡）、A 股、OTC。

## SPX 期权（2026-10-15 ~12:20 ET 测）

- 标的代码 `US..SPX`（两个点）。指数本身**没有报价**：snapshot/quote 都返回 "US stock indices are not supported"。
- 期权链可用：62 个到期日，到 2031-12。代码 `US.SPX<yymmdd><C|P><strike×1000>`（AM 月度）和 `US.SPXW…`（PM 周度），10-16 那天 SPX 1090 个、SPXW 970 个合约。
- 快照字段：bid/ask/size、成交量、`option_open_interest`、`option_implied_volatility`（单位 %）、delta/gamma/vega/theta/rho。延迟按本机时钟是秒级（C9），没有和第二来源对比。
- 日 K 从合约上市开始：Dec-2026 6000C 最早 2021-11-08（单页 1000 根，未翻完）。
- 过期合约：2026-09-18 和 2026-10-09 到期的还能拉，最后一根落在到期日之后的一个交易日（09-21、10-12），复查确认是 volume=0 的补齐 bar（SPX AM 合约在到期日当天 volume 也是 0，因为按开盘价结算）。2025-12-19 到期的返回 "Unknown stock"：过期合约约一个日历月后被清掉（B6）。
- 1 分 K：没成交的分钟会补成 volume=0 的平线，用之前要过滤 `volume > 0`。SPX/SPXW 有 Cboe GTH 夜盘，所以夜间非零成交的 bar 可能是真实成交（抽查的 7695C 两天只有 10 点有 2 根非零 bar）。
- 期权 K 线不占股票历史额度（股票额度测试前后都是 2/1000）。
- 到期日：`get_option_expiration_date` 返回 62 行，但只有 56 个不同日期 — 有 6 天同时有 SPX（AM）和 SPXW（PM）。
- `get_user_info` 里没有美股指数权限字段（futu-api 10.03）；"US stock indices are not supported" 是错误文本，不是权限字段。指数权限只在 QotRight 系统推送里有：`futu/quote/quote_query.py:1618` 的 `us_index_qot_right`。

### 全链快照成本（2026-10-15 12:33–12:40 ET 测）

- 范围：≤120 DTE 全部 + 1 年内月度，共 43 个到期日，行权价 spot×[0.70,1.10] → 17,868 个合约（SPXW 12,690 / SPX 5,178）。这是下限：有 5 个近月到期日被限流漏掉了，而且**限流丢掉的部分没有任何报错之外的提示**。采集程序必须主动限速、重试被限流的批次，并断言行数 == 预期数。不加行权价过滤时是 22,290 个。
- `get_market_snapshot`：每次 400 个代码，17,868 行用 5.9 秒；**不占订阅额度**（C10）。
- `get_option_chain`：**每 30 秒 10 次**，每次时间跨度 ≤30 天。一个 30 天区间返回 22 个到期日、11,056 个合约。合约发现约 1 分钟。
- 质量：6.4% 的行 IV 为空或 0，4.3% 的行 bid=0。

### 其他标的 + 10 分钟频率（2026-10-15 ~12:50 ET 测）

文档写明的快照限制：每 30 秒 60 次，每次 400 个代码，所以每 30 秒最多 24,000 个合约。实测超过就报 "Get Market Snapshot request failed due to high frequency. Maximum 60 times per 30 seconds."（查链是 "Get Option Chain request failed due to high frequency. Maximum 10 times per 30 seconds."）。

| 标的 | 到期日数 | 最远 | 前 30 天合约数 |
|---|---|---|---|
| `US..SPX` | 62 | 2031-12 | 11,056 |
| `US..NDX` | 48 | 2029-12 | 10,926 |
| `US..RUT` | 34 | 2028-12 | 6,004 |
| `US..XSP` | 45 | 2028-12 | 7,436 |
| `US..VIX` | 13 | 2027-06 | 580 |
| `US.SPY` / `QQQ` / `IWM` | 34 / 32 / 34 | 2029-01 | 4,430 / 4,462 / 2,648 |
| `US.GLD` / `TLT` | 30 / 31 | 2029-01 | 3,312 / 936 |
| `US.NVDA` / `TSLA` | 25 / 22 | 2029-01 | 1,288 / 1,550 |
| `HK.00700` / `HK.800000`（恒指） | 9 / 15 | 2027-09 / 2031-12 | 312 / 1,064 |

SPX 全链（不过滤）：30,470 个合约，56 个到期日 = 全部 56 个不同的到期日期，没有缺失。合约发现需要 16 次 30 天区间调用，用了 65 秒。快照需要 77 次调用，约 40 秒（中间有 5 批被限流，所以只拿到 28,470 行 = 正好少了 5×400）。
142 列 zstd parquet = 2.80 MB / 次（98 B/行）。
10 分钟一次 × RTH 40 次（09:40–16:10）≈ 112 MB/天 ≈ 28 GB/年（计算值，未裁剪列）。每天只拍 1 次时约 0.7 GB/年。mini `/Volumes/DATA_LAKE` 剩 5.7 TiB；内置盘只剩 30 GiB，不能放在那里。

### 各会话意见（2026-10-15，argon1 / signal-lab-cf / signal-lab-4b）

- argon：现在 SPX 期权数据靠 UW。SPX 每天只有 2–5 个到期日（2026-05-14 起），`option_surface_grid_daily` 里 SPX 行数为 0。UW 日配额 9 月里 10 天有 6 天用完。最大收益是 25–50 DTE 的 SPX 带偏斜 put 曲面，用来给 VRP macro credit 重新定价：用 flat vol 算，这个 credit 被高估约 65%。其次是期限结构/偏斜做 regime、IB 宕机时的备用报价、用 OI 算 GEX 与 UW 交叉核对。0DTE 现在没有使用方。
- signal-lab：价值在多到期日的深度，不在历史。只能往后积累，1–2 年内不能做回测，也不能推翻已有的 KILL 结论。会受影响的研究线：SPX density/cone、VRP 期限结构、flat-vol butterfly、SPX put-spread VRP。
- 共识的采集规则：15:45 ET 每天一次，必做；10:00 ET 可选。只追加，不覆盖，不回填。存原始 bid/ask/size、last、volume、OI（标为 t−1，即 OCC 前一日结算值）、Futu 的 IV/greeks（只作参考）、合约代码 + AM/PM root、vendor 报价时间 + 我们的采集 UTC 时间、同一时刻的 SPY 报价。IV 用中间价 + put-call parity 隐含远期**自己重算**。SPX 和 SPXW 不能混用：结算时间不同（SOQ 开盘 vs 收盘）。
- 有一处分歧未定：argon 还要 16:15 后的收盘报价，signal-lab-4b 认为 16:00–16:15 应该避开。
- argon 采信之前：先用现有的 IB IV canary（`option_surface_iv_canary`）和 Futu IV 并行对比几周。

## 和现有 UW / Postgres 数据的重复（mini Postgres 17，2026-10-16 查）

期权数据在 `option_wizard.uw_scan`，全部来自 UW；`core.xenon.uw_analyze_*` 是 UW 的 flow/GEX 分析结果；`option_chain.archive` 是 xenon 的 IB 全链归档；livewire lake 在 mini 上没有任何期权数据。

| 表 | 内容 | 覆盖（实测） | 和 Futu 的关系 |
|---|---|---|---|
| `option_contract_snapshots`（1.04 亿行，29 GB） | 逐合约 NBBO、IV、OI、volume + UW 独有的 ask/bid/sweep/multi-leg 成交量、premium | 约 20 分钟一次，**每次 500 个合约封顶**；SPY/QQQ/IWM/TSLA/NVDA/GOOGL/AAPL/AMZN/NET 7 天各 415 次，其余个股 70 次；**无 SPX/VIX/NDX/RUT** | 报价部分重复，但 Futu 是全链（SPY 约 1 万个 vs 500 个）；成交方向/sweep 只有 UW 有 |
| `option_surface_grid_daily`（3,520 万行） | 每日逐行权价 IV + delta/gamma/vega/theta/vanna/charm | SPY/QQQ/IWM/GLD/TLT/NVDA 自 2025-12-26 起 202 天，每天 26–33 个到期日；**SPX/SPXW/VIX/NDX/RUT 为 0 行** | 这些 ETF 和 NVDA **完全重复**（Futu 没有 vanna/charm，但可以算出来） |
| `greeks_by_expiry_strike` | 逐行权价 greeks | SPX 自 2026-05-14 起 74 天，最近每天只有 **1 个到期日**（约 93 DTE） | Futu 完全覆盖并超过 |
| `option_chain_per_strike` | 逐行权价 volume + OI | SPX 自 2026-05-15 起 97 天，10-14 只有 6 个到期日 / 266 行 | Futu 完全覆盖 |
| `oi_by_strike` | 逐行权价 OI | SPY 自 2025-01-02 起 448 天；SPX 只有 5 天（6 月） | SPX 是空白，SPY 重复 |
| `iv_smile_snapshots` | 逐行权价 IV | SPX 只有 4 天（2026-06-05..08） | 已停 |
| `exposures_*`、`gex_snapshots`、`iv_term`、`interpolated_iv`、`max_pain` | UW 算好的衍生指标 | — | Futu 只给原始数据，要自己算；不算就不重复 |
| `flow_events`、`uw_dark_lit_flow_prints`、`uw_intraday_option_flow_bars`、`oi_change_events`、`uw_analyze_flow_alerts` | 成交流、暗池、flow alerts | — | **无重复**：Futu 没有这类数据 |
| `option_chain.archive.*`（xenon IB 归档） | 设计为 SPX/NDX/RUT/VIX 全链每 10 分钟一次 | 只在 2026-06-04 跑过 34 次，全部 `partial`，之后停掉 | 设计目标和 Futu 方案相同；IB 线数上限让它做不成 |

## 运维风险（xenon-68 审查补充，2026-10-16）

- SDK 遇到 OpenD 拒绝连接时会**无限重试**：xenon PR #179 里 xenon-api 因此卡死约 4 小时。修法是先做 3 秒 TCP 探测，再用 30 秒有上限的 connect。
- 一个坏代码会让整批 400 个失败。期权合约如果在合约发现和快照之间到期或被清掉，就会拖垮它所在的整批。失败时要把批次拆开重试。
- Futu 返回的数据里会出现 `'N/A'` 字符串和 list 类型的单元格，直接写成 parquet，各次快照的 schema 会漂移。要固定列和 dtype。
- SDK 会推送 `NotifyType_QotRight`，行情权限在运行中可能变化。每次运行前都要检查 `get_user_info`。
- OpenD 如果放到 mini 上，xenon 和 livewire 会共用一个登录和一套限频。两边要一起规划。xenon 的 CHANGELOG 记录了容器内连不到 OpenD。

## 历史 K 线和快照查证（2026-10-15 13:07–13:40 ET）

独立查证上文的说法（SWE agent，英文原文）。MacBook，OpenD 127.0.0.1:11111，futu-api 10.03。mini 上只读（parquet、一次 `psql`，`statement_timeout=60s`）。脚本和原始输出在 `/tmp/futu-probe2/`（不在 repo 里）。

Quota used by this probe:
- Stock history-K: 4 new symbols (US.SPY, US.GOGO, US.META, US.ELV). `US.AAPL` and `HK.00700` were already counted. `get_history_kl_quota` shows used 6 / remain 994.
- Option history-K: 3 new chains (SPX-index 2026-09-15, SPX-index 2026-09-17, SPY 2026-09-18). The chains SPX 2026-12-18 and SPY 2026-11-20 were already in the list and were reused. The option list now has 8 entries.
- Failed lookups ("Unknown stock") added nothing to either list.

### A. Stock history K

#### A1. Floors

| Item | Result | Numbers | Call |
|---|---|---|---|
| 1m floor moves with the clock | VERIFIED (rolling) | At 13:12:37 ET the first AAPL, SPY and GOGO 1m bar was `2018-10-09 13:13`. At 13:25:51 ET the first AAPL and SPY bar was `2018-10-09 13:26`. The offset to "now" stayed 2,928 days (8 years + 6 days, computed). The floor follows the minute of the day. | `request_history_kline(code, '2018-01-01', '2026-10-15', K_1M, AuType.NONE, extended_time=False)`, first page |
| 1m floor is month-aligned or fixed | REFUTED | A fixed or month-aligned floor would not move 13 minutes in 13 minutes. | same |
| 5m floor | VERIFIED | AAPL first 5m bar `2018-10-09 13:15` at 13:12 ET. | `K_5M`, start 2016-01-01 |
| Daily floor | VERIFIED value, rolling UNVERIFIED | AAPL and SPY: `2006-10-02`. GOGO: `2013-06-21` (its own first bar). Same on both measurements. Both runs were on the same ET date, so a day-level move is not tested. | `K_DAY`, start 2000-01-01 |
| Day offset stable across days | UNVERIFIED | Needs a re-run on another date. | |

#### A2. Survivorship (delisted stocks)

| Item | Result | Numbers | Call |
|---|---|---|---|
| Mini delisted candidates | VERIFIED | `bronze-delisted` has 8,622 symbols. 3,214 have a last `trade_date` from 2019-01-01 to 2023-06-30. Every delisted file starts at `2021-06-11`. | pyarrow footer statistics of `1d.parquet`, column `trade_date` |
| Futu serves delisted stocks | REFUTED | 5 of 5 failed for daily and 1m: TWTR (last 2022-10-27), XLNX (2022-02-11), SIVB (2023-03-09), CERN (2022-06-07), FRC (2023-04-28). Error: `Unknown stock. <SYM>`. 10 requests, 0 quota used. | `request_history_kline('US.<SYM>', ...)`, `K_DAY` and `K_1M`. Only the plain `US.<SYM>` form was tried. |
| OHLC match on 3 dates | UNVERIFIED (no Futu data) | Not possible. | |

#### A3. 1m label and accuracy (RTH, `AuType.NONE`, `extended_time=False`)

Sessions: 2022-03-15 (EDT), 2023-01-18 (EST), 2026-10-08 (EDT).

| Item | Result | Numbers | Call |
|---|---|---|---|
| Label convention | VERIFIED | Futu `time_key` is the bar end. A session has 390 bars labeled 09:31 to 16:00 ET. Subtract 1 minute, then convert ET to UTC, to match Massive `bar_timestamp` (bar start). Shift -1 wins on all 9 symbol-sessions. Shifts 0, +1, -2, +2: 0 exact matches for AAPL and SPY. For GOGO they give at most 19.5% (2026-10-08) of Futu's 390 bars. | `request_history_kline(code, d, d, K_1M, NONE)`; join on UTC minute |
| Exact OHLC match at shift -1 | VERIFIED | AAPL: 333/390 (85.4%), 341/390 (87.4%), 390/390 (100%). SPY: 331/390 (84.9%), 325/390 (83.3%), 388/390 (99.5%). GOGO (share of bars on both sides): 333/338, 222/222, 349/350. | |
| Size of the OHLC differences | VERIFIED | AAPL 2022-03-15: 57 bars differ. Max abs difference: open 0.025, high 0.01, low 0.005, close 0.07. | |
| Bar counts per session | VERIFIED | Futu: 390 every session, including no-trade minutes padded with volume 0 (GOGO: 9, 32, 4 zero-volume bars). Mini RTH minutes (09:30 to 15:59): AAPL 390, 390, 390. SPY 390, 390, 390. GOGO 338, 222, 350. Mini total with extended hours: AAPL 864, 760, 836. SPY 884, 797, 916. GOGO 341, 226, 369. | |
| Extra small bars in Futu for GOGO | VERIFIED | Futu bars absent from mini with volume > 0: 43, 136, 36. They hold 1.07%, 4.47%, 0.32% of the day volume. AAPL and SPY: none. | |
| Volume ratio (Futu / mini, day total, shift -1) | VERIFIED | AAPL 1.131, 1.115, 1.228. SPY 1.018, 1.047, 1.031. GOGO 1.060, 1.212, 1.359. The per-bar median ratio is 1.0000 (AAPL 2022-03-15 and 2026-10-08, GOGO 2023-01-18). | |
| Cause of the day-total gap | VERIFIED | The last bar (label 16:00) holds the closing auction in Futu. AAPL 2026-10-08: 6,380,238 vs 868,506. SPY 2,072,281 vs 889,501. GOGO 601,490 vs 80,893. Without the last bar the ratios are 0.9945 (AAPL 2022-03-15), 1.0001 (AAPL 2026-10-08) and 1.0000 (GOGO 2026-10-08). Computed from the printed sums. | |
| Mini 1d source field | REFUTED (for the task brief) | AAPL `1d.parquet`: 11,486 rows `source='legacy'`, 65 rows `source='massive'` (2026 only). | |

#### A4. Ticker changes

| Item | Result | Numbers | Call |
|---|---|---|---|
| `US.META` daily before 2022-06-09 | VERIFIED | First bar `2012-05-18`, 3,623 bars. Mini `META` 1d (3,622 rows, from 2012-05-18) is Facebook history. On 5 dates, high, low and close match on 4 of 5. Only 2022-06-08 matches all four. Differences: open 79.3 vs 79.4 (2015-06-01), 343.015 vs 343.0, 201.165 vs 201.16, and close 38.2318 vs 38.18 (2012-05-18). | `request_history_kline('US.META', '2000-01-01', '2026-10-15', K_DAY, NONE)` |
| `US.META` 1m before 2022-06-09 vs mini `META` 1m | REFUTED as a like-for-like test | Mini `META` 1m before 2022-06-09 is a different security: monthly median close 13.8 to 16.2 from 2021-06 to 2022-01. It also has no bars from 2022-01-31 to 2022-06-08. | pyarrow read of mini `symbol=META/1m.parquet` |
| `US.META` 1m vs mini `FB` 1m | VERIFIED | Mini `FB` 1m holds the Facebook prices (about 333 on 2021-12-15). Exact OHLC at shift -1: 367/390 on 2021-12-15 and 362/390 on 2022-04-04. Close-only: 377/390 and 376/390. | `K_1M` for the single day |
| Second rename ANTM -> ELV | VERIFIED | `US.ELV` daily starts `2006-10-02`. Exact OHLC on 3 of 4 dates (2021-06-11, 2022-01-04, 2022-03-25). On 2022-03-28 only the low differs: 477.0201 vs 477.02. Mini `ELV` starts 2001-10-30 and mini delisted `ANTM` covers 2021-06-11 to 2022-06-27. | `K_DAY` |
| Daily volume vs mini 1d | VERIFIED difference, cause UNVERIFIED | Futu/mini ratio: ELV 2.58, 2.30, 2.36, 2.57. META 1.08 (2012), 1.12 (2015), 1.74, 1.31, 1.39 (2021-22). AAPL 2022-03-15: mini 1d 92.2M (legacy), Futu 1m sum 86.7M, Massive 1m sum 76.6M. Mini `legacy` 1d volume is not a reliable reference. | |
| Mini `FB` symbol | VERIFIED | `FB` 1d: 250 rows in 2021-22 (price 175 to 382). It also has 328 rows in 2025-26 at price 40 to 46, which is another security. Ticker reuse affects both `FB` and `META`. | |

#### A5. Throughput

| Item | Result | Numbers | Call |
|---|---|---|---|
| Full 8-year 1m pull, one symbol | VERIFIED | AAPL, 2018-10-01 to 2026-10-15: 783 pages, 782,401 bars, 48.9 s, 0 errors. First bar `2018-10-09 13:13`. The last bar was the minute still open (`2026-10-15 13:13`). | `request_history_kline(..., max_count=1000, page_req_key=...)` in a loop |
| Time for 1,000 symbols (serial) | Computed | 1,000 x 48.9 s = 13.6 h. | |

### B. Option history K

#### B6. Retention of expired contracts (probe date 2026-10-15)

| Item | Result | Numbers | Call |
|---|---|---|---|
| SPX monthly (AM) | VERIFIED | 2026-09-18 served (27 days after expiry). 2026-08-21 not served (`Unknown stock`, tried strikes 6000 and 6500). No SPX monthly exists between them. | `US.SPX260821C6000000`, daily K |
| SPXW | VERIFIED | Served: 2026-09-17 and 2026-09-15 (30 days after expiry). Not served: 2026-09-14, 09-11, 09-04. | `US.SPXW<yymmdd>C7500000` |
| SPY | VERIFIED | Served: 2026-09-18. Not served: 2026-09-11 and 09-04 (strikes 700, 650, 750). | `US.SPY<yymmdd>C700000` |
| Rule | Inference | Served if expiry >= one calendar month before the probe date (2026-09-15 on 2026-10-15). Three roots agree. A single probe date; re-test on another day. | |
| Last bar of an expired contract | VERIFIED | SPXW 2026-09-15 and 09-17: last bar one trading day after expiry (09-16 for the first). | |

#### B7. Full-chain daily K, SPX expiry 2026-12-18

| Item | Result | Numbers | Call |
|---|---|---|---|
| Chain size | VERIFIED | 1,744 contracts: SPX 972 and SPXW 772. 872 calls and 872 puts. Strikes 200 to 13,400. | `get_option_chain('US..SPX', start='2026-12-18', end='2026-12-18')` |
| Cost | VERIFIED | 1,838 requests (1,744 first pages + 94 follow-up pages for contracts with more than 1,000 bars). 1,026 s (17.1 min) at 0.55 s pacing. 0 rate-limit errors, 0 other errors. 301,771 rows. | `request_history_kline(code, '2021-01-01', '2026-10-15', K_DAY, NONE)` per contract |
| Option usage in `get_history_kl_quota` | The existing audit claim "no API can read option usage" is REFUTED in part | `get_history_kl_quota(get_detail=True)` lists each option chain as an entry named like `S&P 500 Index [opt exp 2026-12-18]`. The list had 14 entries before and after the 1,744-contract pull (6 stock + 8 option). The `used` number (6) counts stock symbols only. No number for the 200-chain option limit is shown. A repeat request updates `request_time` and does not add an entry. SPX and SPXW share one entry (inferred: the SPXW contracts added no entry). | `c.get_history_kl_quota(get_detail=True)` |

#### B8. Data quality

| Item | Result | Numbers | Call |
|---|---|---|---|
| Rows with volume = 0 | VERIFIED | 69.5% of 301,771 rows. SPX 69.8%, SPXW 67.5%. 256 of 1,744 contracts (14.7%) never traded. | pandas on the B7 pull |
| Close on zero-volume days | VERIFIED | 100% of zero-volume rows are flat (O=H=L=C). Close equals the previous close on 60.6%. On the other 39.4% (82,205 rows) the close changes. Example: `SPX261218C200000` on 2025-12-24 has close 6701.3963 (previous 6646.0). Origin of those values: UNVERIFIED. Filter `volume > 0` before using any close. | |
| Non-zero-volume rows with O=H=L=C | VERIFIED | 34.6%. | |
| UW cross-check, same date | REFUTED (0 of 10 match) | 5 SPY puts (expiry 2026-11-20, strikes 700, 730, 740, 750, 760) on 2026-10-09 and 2026-10-12. UW last snapshot of the day (16:55 ET) vs Futu close and volume of the same date: no match. | `request_history_kline('US.SPY261120P<k>000', K_DAY)`; psql on `uw_scan.option_contract_snapshots` with `inserted_at` bounds |
| UW cross-check, prior session | VERIFIED (10 of 10 exact) | UW value of day D equals the Futu bar of the previous trading day: close within 0.005 and volume exact. Example P750: UW 10-09 = 6.20 / 13,516 = Futu 10-08; UW 10-12 = 5.04 / 1,868 = Futu 10-09. | same |
| UW value static intraday | VERIFIED (one contract, SPY only) | `SPY261120P00750000` on 2026-10-15, snapshots 12:40 to 13:15 ET: last 6.81, bid 6.78, ask 6.84, volume 13,868 in every run. Futu 10-14 bar = 6.81 / 13,868. Futu live snapshot at 13:19 ET: last 6.25, volume 6,952, equal to the Futu 10-15 daily bar. | `get_market_snapshot` |
| Futu date labels are correct | VERIFIED | SPY fell 778.01 -> 769.60 on 2026-10-14 (mini bronze). Futu P750 rose 4.73 -> 6.81 on the 10-14 bar. | |
| Futu option 1m vs daily | VERIFIED | P750: 405 1m bars per day (09:31 to 16:15). Volume sum 1,868 (10-09) and 6,992 (10-12), equal to the daily bars. 228 and 203 bars have volume > 0. | `K_1M` |

### C. Snapshot claims

#### C9. Quote lag, 0DTE SPXW (2026-10-15 expiry), during RTH

8 codes: strikes 7695, 7700, 7705, 7710, call and put. The strike is chosen from SPY x 10 = 7701.4. Lag = local ET after the call minus `update_time`. `update_time` has a 1-second resolution.

| Sample (ET) | Result | Min / median / max lag (s) |
|---|---|---|
| 13:12:03 | VERIFIED | 0.6 / 1.6 / 9.6 |
| 13:23:03 | VERIFIED | 1.8 / 3.8 / 56.8 |
| 13:34:03 | VERIFIED | 3.0 / 15.0 / 30.0 |

No 15-minute delay. The lag measures the age of the last update, so a quiet strike shows more. Across all 400 codes in the batch the median was about 6,060 to 6,470 s and the max about 595,000 s (strikes that did not trade for days). `update_time` is not a staleness test for far strikes. The clock source of the Mac was not checked.

#### C10. Subscription quota

| Item | Result | Numbers | Call |
|---|---|---|---|
| `get_market_snapshot` on 400 option codes leaves `total_used` at 0 | VERIFIED | 3 of 3 samples, 400 rows each, `total_used` 0 before and 0 after. Also 0 after all history requests. | `query_subscription()['total_used']` around `get_market_snapshot` |

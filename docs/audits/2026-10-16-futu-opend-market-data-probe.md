# Futu OpenD 行情接口探测（免费权限）— 2026-10-16

只读探测。主机：MacBook（OpenD GUI 10.11.7108，`127.0.0.1:11111`，`futu-api` 10.03，xenon 的 venv）。
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
期权另有独立额度：该档订阅 200、历史 K 线 200。官方原文按**期权链**（同一到期日的所有期权）计数："每请求 1 条期权链的历史 K 线，将会占用 1 个历史 K 线额度"。没有 API 能读出期权额度的用量，所以这一点在运行中**未验证**（xenon-68 复查原文后同意）。额度随资产/交易量自动调整，不是永久。

## 历史深度（观测值，2026-10-15 ~12:13 ET 测）

- 日 K：AAPL、00700 都从 2006-10-02/03 开始，大约 20 年（比 now−20y 早 13 天，不精确）。
- 1 分 K：AAPL 最早 `2018-10-09 12:14`（交易所本地时间），大约 8 年。now−8y = 2018-10-15，实际早了 6 天，只有时刻和测试时间相同。下限是滚动、按月取整还是固定的，**未确定** — 换一天重测之前，不要用"每天滚掉一天"来制造紧迫感（xenon-68 指出）。
- 盘前盘后：`extended_time=True` 给 04:01–20:00 ET，960 根/天；文档说 ≤60 分钟级别，不足 2 年。
- 期权：日/1/5/15/60 分 K。`US.SPY261120P730000` 日 K 有成交量。
- `time_key` 是交易所本地时间，不是 UTC。`autype=AuType.NONE` 给未复权价。

## 吞吐（观测值）

AAPL 1 分 K 一年 = 97,693 根，98 页，7.1 秒（每页 1000 根）。
限频 60 次 / 30 秒只算每只股票的**首页**，后续翻页不限（文档原话，和实测相符）。
推算：一只股票 8 年 1 分 K ≈ 1 分钟；额度 1000 只/周是真正的瓶颈，不是速度。

## 与 livewire 对数（mini 生产 lake，`bronze/asset_class=equity/symbol=AAPL/1d.parquet`，source=massive，raw）

2026-10-08..14 五个交易日：OHLC 完全一致；volume 差 0–4,305 股（< 0.02%，合并成交量口径差）。
mini 上 AAPL 1m/5m 最早 `2021-05-21 13:30Z`（Massive 平面文件 5 年下限）。

## 对 livewire 的补充价值

1. **2018-10 → 2021-05 的美股 1 分 K**：Massive 拿不到的约 2.6 年。下限是否每天滚动未确定（见上）。回填前要先验证：(a) 已退市股票是否还在（过期期权会返回 "Unknown stock"，所以退市股票可能也被清掉了 — 用 security master 里 2019–2020 退市的股票测）；(b) 1m 和 Massive 在 2021-05 之后的重叠区间逐分钟对数（盘前盘后 04:01–20:00 的 960 根说明 Futu 可能按 bar 结束时间标记，而 Massive 按开始时间，这样两边会差 1 分钟，**未验证**）；(c) 改名股票的代码映射。
2. **不需要每次手动 2FA 的 Tier B 来源**（但 OpenD 登录会触发短信验证，SDK 有 `NeedPhoneVerifyCode`、`KickedOut` 事件 — 能否无人值守未验证）：Massive 窗口以下目前只有 IB（手动 2FA）。Futu 覆盖日 K 2006 起、1 分 K 2018 起。
3. **独立复权因子**（`get_rehab`：拆股/分红 + 前后复权因子）— 可交叉核对 Silver 的 corporate actions。
4. 新资产：港股 + 港股期货 LV1、美股期权 K 线（livewire 现在都没有）。

不能补的：美股期货（要期货账户 + 付费卡）、A 股、OTC。

## SPX 期权（2026-10-15 ~12:20 ET 测）

- 标的代码 `US..SPX`（两个点）。指数本身**没有报价**：snapshot/quote 都返回 "US stock indices are not supported"。
- 期权链可用：62 个到期日，到 2031-12。代码 `US.SPX<yymmdd><C|P><strike×1000>`（AM 月度）和 `US.SPXW…`（PM 周度），10-16 那天 SPX 1090 个、SPXW 970 个合约。
- 快照字段：bid/ask/size、成交量、`option_open_interest`、`option_implied_volatility`（单位 %）、delta/gamma/vega/theta/rho。是否实时没有和第二来源对比 — **未验证**。
- 日 K 从合约上市开始：Dec-2026 6000C 最早 2021-11-08（单页 1000 根，未翻完）。
- 过期合约：2026-09-18 和 2026-10-09 到期的还能拉，最后一根落在到期日之后的一个交易日（09-21、10-12），复查确认是 volume=0 的补齐 bar（SPX AM 合约在到期日当天 volume 也是 0，因为按开盘价结算）。2025-12-19 到期的返回 "Unknown stock"：过期合约会被清掉，保留多久未测。
- 1 分 K：没成交的分钟会补成 volume=0 的平线，用之前要过滤 `volume > 0`。SPX/SPXW 有 Cboe GTH 夜盘，所以夜间非零成交的 bar 可能是真实成交（抽查的 7695C 两天只有 10 点有 2 根非零 bar）。
- 期权 K 线不占股票历史额度（股票额度测试前后都是 2/1000）。期权额度的用量没有 API 可查。
- 到期日：`get_option_expiration_date` 返回 62 行，但只有 56 个不同日期 — 有 6 天同时有 SPX（AM）和 SPXW（PM）。
- `get_user_info` 里没有美股指数权限字段（futu-api 10.03）；"US stock indices are not supported" 是错误文本，不是权限字段。指数权限只在 QotRight 系统推送里有：`futu/quote/quote_query.py:1618` 的 `us_index_qot_right`。

### 全链快照成本（2026-10-15 12:33–12:40 ET 测）

- 范围：≤120 DTE 全部 + 1 年内月度，共 43 个到期日，行权价 spot×[0.70,1.10] → 17,868 个合约（SPXW 12,690 / SPX 5,178）。这是下限：有 5 个近月到期日被限流漏掉了，而且**限流丢掉的部分没有任何报错之外的提示**。采集程序必须主动限速、重试被限流的批次，并断言行数 == 预期数。不加行权价过滤时是 22,290 个。
- `get_market_snapshot`：每次 400 个代码，17,868 行用 5.9 秒；**不占订阅额度**（前后 `total_used` 都是 0）。
- `get_option_chain`：**每 30 秒 10 次**，每次时间跨度 ≤30 天。一个 30 天区间返回 22 个到期日、11,056 个合约。合约发现约 1 分钟。
- 实时性：0DTE 平值 SPXW `update_time` 12:33:53，当时本地 ET 12:33:56，相差 3 秒，不是 15 分钟延迟（单次观测）。
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

## 接入前要决定的事（未做）

- OpenD 必须常驻登录。现在只在 MacBook 上跑；放到 mini 需要登录态和偶发的短信验证 — 能否无人值守**未验证**。
- `futu-api` 依赖 pandas，livewire 现在不装 pandas — 这是新依赖，需批准。
- 新 provider 意味着新 source 值（现有闭集 `ib`/`uw`/`massive`）、一行 registry、ledger run — 按 CLAUDE.md 不能做成旁路脚本。
- 额度会掉档：资产或交易量下降后可能回到 300 或 100 只/周。

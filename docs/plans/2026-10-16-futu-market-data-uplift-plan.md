# Plan：Futu market data uplift — 2026-10-16

状态：**草案 v3，未批准。** v3 = v2 + tribunal 审查（Opus、Astra、Fable、Grok）第一轮的修正，见 §10。
- v2 改动：并入历史 K 线查证结果（§4.1），按结果改写 W2/W3/W4；W1 已在分支 `feat/futu-options` 实现，并在 MacBook 上实测（§3.1），未提交、未上 mini。
取代：`docs/plans/2026-10-16-futu-option-chain-capture-proposal.md`。那份提案的内容成为本计划的 W1，原文件保留作为记录。
证据：
- `docs/audits/2026-10-16-futu-opend-market-data-probe.md`：快照、期权链、和 UW/Postgres 的重复情况。
- `docs/audits/2026-10-16-futu-history-kline-verification.md`：历史 K 线查证。
参与意见：argon1、signal-lab-cf、signal-lab-4b、xenon-68。

## 0. 说人话

1. **问题**：
   - argon 拿不到 SPX/VIX/NDX/RUT 的多期限期权曲面。UW 在 Postgres 里这几个标的是 0 行，SPX 每天只有 1 个到期日。
   - livewire 的美股 1m 线从 2021-05-21 开始，这是 Massive 的下限，更早的拿不到。
   - Massive 窗口以外的缺口只能靠手动 2FA 的 IB 来补。
2. **Futu 免费权限能补的**：
   - 期权全链快照：4 个标的共约 59,800 个合约，一轮 68 秒（MacBook 实测），不占订阅额度，报价实时。
   - 美股 1m 线：滚动窗口，约为当前时刻往前 2,928 天（现在约 2018-10）；日线从 2006 开始。
   - **只有现存股票**：退市股票 Futu 一律回 `Unknown stock`（5/5），所以历史回填有幸存者偏差。
3. **成本**：
   - 一个新依赖：`futu-api`，它会带进 pandas。
   - OpenD 要常驻 mini，并和 xenon 共用一个登录。
   - 约 33.5 GB/年的 lake 存储（按 MacBook 实测文件大小算：每天约 133 MB）。
4. **需要决定**：§8 的 D1–D8。

## 1. 工作流

| ID | 工作流 | 价值 | 状态 |
|---|---|---|---|
| W0 | 公共基础：OpenD 上 mini、`clients/futu_client.py` | 其他工作流都依赖它 | client 已实现；OpenD 上 mini 的 P0 实测未做 |
| W1 | 指数期权全链快照（SPX/SPXW、VIX 每 10 分钟；NDX、RUT 每 30 分钟；15:45 ET 正式快照） | argon 的 VRP 曲面、期限结构 / 偏斜、GEX 交叉核对；signal-lab 往后积累的存档 | **已实现**，MacBook 实测通过（§3.1） |
| W2 | 期权日 K 一次性抢救（SPX/SPXW 在市合约，从上市日开始） | 合约的历史成交量 / 最后成交价，过期后会被 Futu 清掉 | **价值低，建议不做**（§4.2） |
| W3 | 美股 1m 回填 2018-10 → 2021-05 | Massive 下限以前约 2.6 年的分钟线（只有现存股票） | 门槛：G-a/G-c/G-d 有结论，G-b 不通过 → 只能做幸存者版本（§4.3） |
| W4 | Futu 作为 Tier B 日线补洞来源（2006 起） | 减少只能靠 2FA IB 补的 Tier B 队列 | 有门槛：日线重叠吻合率未测；退市股票补不了 |
| W5 | `get_rehab` 复权因子交叉核对 Silver 的 corporate actions | 第二个独立来源核对拆股 / 分红 | 推迟：没有使用方要求 |

**不做**：
- SPY/QQQ/IWM/GLD/TLT/NVDA 的期权曲面：UW 从 2025-12-26 起已经有了，再存就是重复（见 D5）。
- 个股期权、港股期权：现在没有使用方。
- 美股期货、A 股：没有权限。
- livewire 内部重算 IV：由使用方做。
- 期权 1m K 线：没成交的分钟补成 volume=0，而且只有最后成交价。

## 2. W0 公共基础

### 2.1 OpenD 部署（P0 实测，不写代码）

OpenD 常驻 mini，只听 `127.0.0.1:11111`。P0 要实测以下 5 项：

1. 无人值守登录能否连续 3 个交易日不掉线。SDK 有 `NeedPhoneVerifyCode` 和 `KickedOut` 事件。
2. 同一账号同时开 MacBook 和 mini 两个 OpenD，会不会互踢。
3. 限频按账号算还是按 OpenD 实例算。这决定 livewire 和 xenon 怎么分配额度。
4. xenon 容器怎么连到 mini 的 OpenD。xenon 的 CHANGELOG 记录了"容器内连不到 OpenD"。
5. 报价时间戳：在 mini 上对 5 个合约同时取 Futu 和 IB 的报价，一天测 3 个时点。

### 2.2 `clients/futu_client.py`（唯一 import `futu` 的模块）

职责（每一条都对应一个已经发生过的问题）：

| 职责 | 原因 |
|---|---|
| 先做 3 秒 TCP 探测，再在 daemon 线程里构造 context，最多等 `futu_connect_timeout_s`（30 秒）；SDK 线程全部设成 daemon — **已实现** | SDK 构造函数在握手失败时 `while True` 每 6 秒重试，xenon-api 因此卡死约 4 小时（PR #179）；非 daemon 线程会让被中断的进程退不出去，launchd 就不会再启动下一次 |
| 每次运行前读 `get_user_info`，权限不够就 86；status 对最近一次 run 不是 OK 报 WARN — **已实现**。~~订阅 `NotifyType_QotRight` 推送~~：一次运行约 70 秒，每次开头已经检查，不做 | 权限可能变化，不能写入降级的数据 |
| 主动限速，速率按端点区分：快照 ≤ 55/30s，查链 ≤ 9/30s — **已实现**；历史 K 线首页 ≤ 55/30s 属于 W3，在 T8 里加 | 限流丢掉的部分没有任何报错之外的提示；每个限频数字都要注明端点 |
| 批次失败时二分拆开重试，坏代码单独记录；限流错误不拆，拆之前先确认行情仍然登录（`get_global_state`），掉线就 86 — **已实现** | 一个坏代码会让整批 400 个失败；掉线时不加判断地拆，会变成约 3 万次单代码调用 |
| 拿到 DataFrame 立即转成 pyarrow，按固定 schema；`'N/A'` 转 null，list 类型的单元格不存 | 防止 schema 漂移（xenon 踩过） |
| 把交易所本地的 `time_key` / `update_time` 转成 UTC | Futu 给的是 ET/HKT 本地时间 |
| 连不上或未登录时抛 `FutuUnavailable` → 退出码 86 | 沿用 IB 规则：DEGRADED，不 page，不重试 OpenD |

再加一个 containment 测试，照 `tests/test_duckdb_containment.py` 写：`futu` 和 `pandas` 只能在 `clients/futu_client.py` 里出现。

### 2.3 横切改动

- ~~`_VALID_SOURCES` 加 `futu`~~：W1 不写 telemetry / quality flags，没有调用方，不加。W3 写 bronze 时再加。
- `clients/constants.DECLARED` 新增所有 `futu_*` 限频和阈值 key，每一个都写明对应的端点。
- `pyproject.toml` + `uv.lock` 加 `futu-api`。

## 3. W1 指数期权全链快照

设计照搬原提案的 §4（`docs/plans/2026-10-16-futu-option-chain-capture-proposal.md`），这里只列关键点：

- **入口**：`scripts/livewire_ingest.py futu-options`，实现放在 `livewire_scripts/fetch_futu_options.py`。
- **调度**：新的 launchd 任务 `com.livewire.futu-options`，`StartCalendarInterval` 只写分钟（:00/:10/:20/:30/:40/:45/:50），每小时都触发，与时区无关；指向 `<warehouse>/current`。没有到期的快照时退出 0，不写 run 行。
  - 程序自己用 `clients/trading_calendar.py` 判断是否在交易时段，半日市按 `session_close_time` 处理。
  - 每天第一次触发时做合约发现，生成 `contracts.parquet`，作为当天的 expected。
  - 15:30 那次（半日市 12:30）再做一次发现，把盘中新挂的行权价加进 expected（只增不减），记 `futu_contracts_added_intraday`。
  - 正式时段之后、到收盘那一分钟结束（16:00:59）之前的任何一次触发，会补拍还没有的正式快照（:45 那次晚了或被跳过时）。每个正式快照在发出第一个报价请求前（等锁和发现之后）再查一次这个截止时间。收盘后不补，因为那已经是另一个市场。
- **lake 布局**：`bronze/asset_class=option/underlying=<U>/date=<D>/{contracts,snap_<HHMM>}.parquet`。
  - 写入用 `publish_parquet`。只追加由 `capture()` 保证：发布前检查文件是否存在，检查、发现、发布都在同一个标的-日期锁（`path_lock`）里，所以手动运行和定时运行不会互相覆盖。已存在的快照重跑时重新评分，不重写。
- **列**：原始 bid/ask/size、last、volume、OI（另加 `oi_asof` = 上一个交易日）、Futu 的 IV/greeks（只作参考）、root（SPX/SPXW）、`settlement`（AM/PM）、`capture_ts_utc`、`vendor_update_ts_utc`。另附对应 ETF 的报价作为标的代理价（`proxy_code`：SPX 用 SPY、NDX 用 QQQ、RUT 用 IWM，VIX 没有）；它在整条链之后单独取，时间记在 `proxy_ts_utc`。`capture_ts_utc` 是每一行所在批次返回的时刻。
- **ledger 和 status**：
  - ledger run 的 job 名是 `futu-options`。
  - 每个标的每次快照写 measurements：expected、captured、seconds、quote lag、IV 空值比例、被限流的调用次数。
  - status 新增一个检查 "Futu option snapshots"，每个标的一行，评的是「收盘 + 30 分钟已经过去的最近一个纽约交易日」。所以第二天 10:30Z 的 watchdog 和 15:45Z 的 digest 看到的是前一天的正式快照，周末看到的是周五的。
    - BAD：正式快照没有，或不完整（< `futu_min_capture_ratio/option` = 1.0）。watchdog 会 page。
    - WARN：正式快照的报价延迟 > `futu_max_quote_lag_s/option`（300 秒），正式快照里有被 OpenD 按名拒绝的代码，或最近一次 run 不是 OK（OpenD 断线、权限掉了）。
    - UNKNOWN：没有延迟数据；或本机 7 天内没有 `futu-options` run（还没部署在这台机器上）。
  - 每一行 bid 和 ask 都是空时，这个标的失败（字段名变了，行数照样对得上）。
  - OpenD 按名拒绝的代码存成 `refused = true` 的行，不算缺失（它已经不再提供），但会打印、计数，并让 status WARN。没有报错却少了的行才算缺失。
  - AM 结算的期权（SPX/NDX/RUT 月度、VIX）在到期日当天不算 expected：它们在前一个交易日就停止交易，到期日早上按开盘价结算。Futu 在到期日是否还列出、还报价这些代码，**未实测**（下一个这样的日子是 2026-10-16）。
  - 15:30 再发现失败时，用已有的合约集照常拍（记 `futu_discovery_failed`）。
- **预算**：
  - 4 个标的一次触发 68 秒（实测）；第一次加合约发现 168 秒；15:30 那次要再加约 100 秒的发现。运行上限 `run_budget_s/futu-options` 480 秒。
  - 存储约 33.5 GB/年（实测）。mini `/Volumes/DATA_LAKE` 剩 5.7 TiB。

### 3.0 W1 验收（从原提案 §2 搬过来，在 mini 生产环境上验证）

1. 连续 5 个交易日，每个标的都有正式快照，`captured / expected` = 1.0。expected 来自当天的合约发现清单，不来自 glob lake。查法：`ledger query "select scope, measured_at, value from measurements where name = 'futu_official_capture_ratio' order by measured_at"`。
2. SPX 每 10 分钟一次的快照覆盖 09:40–16:10 ET（半日市截止到 `session_close_time` + 10 分钟），缺失 ≤ 1 次/天。查法：数 `bronze/asset_class=option/underlying=SPX/date=<D>/snap_*.parquet`。
3. `status` 和 digest 上能看到 "Futu option snapshots"，是 OK；OpenD 断线时是 DEGRADED run → WARN，不是 OK；正式快照缺失时第二天的 watchdog page。
4. CI 命令全过：`uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning`。

### 3.1 MacBook 实测（2026-10-15 13:48–13:52 ET，OpenD 127.0.0.1:11111，临时 lake 和 ledger）

| 项 | 结果 |
|---|---|
| 合约数 | SPX 30,470（SPXW PM 20,180 + SPX AM 10,290）、NDX 16,298、RUT 11,558、VIX 1,520 |
| 完整度 | 4 个标的都是 100%，refused 0，被限流 0 次 |
| 耗时 | 第一次（含当天合约发现）168 秒；之后每次 4 个标的共 68 秒 |
| 报价延迟（最近到期日里最新的一条） | SPX 0.9 秒、NDX 2.1 秒、VIX 10 秒、RUT 38.5 秒 |
| IV 空值 / 0 的比例 | SPX 7.6%、NDX 9.3%、RUT 9.5%、VIX 39.7% |
| 调度入口（不带参数） | 同一时段已拍过的文件不重写，退出 0 |
| CI | 3,325 个测试通过，覆盖率 95.17% |

实测发现并修好的问题：第一版每次快照只记一个 `capture_ts_utc`（开始时刻），而 SPX 3 万个代码要分约 77 次调用、40 多秒，报价延迟算出来是负数（NDX −58 秒）。现在每一行记它所在批次返回的时刻。

未实测：正式快照（15:45 ET）的实际运行、半日市、OpenD 掉线时的 86、在 mini 上运行。

## 4. W2–W4 历史 K 线

### 4.1 门槛检查（只读；结果见 `docs/audits/2026-10-16-futu-history-kline-verification.md`）

| # | 检查 | 不通过时 |
|---|---|---|
| G-a | 1m 和日线的下限是滚动、按月取整还是固定的 | W3 的窗口和紧迫性按实测结果重写 |
| G-b | 退市股票是否还在（用 bronze-delisted 里 2019–2023 退市的股票测） | W3/W4 只能补现存的股票，并且要写明有幸存者偏差；一份没有退市股票的历史不能当作回测数据 |
| G-c | 1m 的时间标记方式和精度：在 2022–2026 的重叠区间，和 Massive 逐分钟对比 | 时间平移 ≠ 0 时 adapter 要做平移；OHLC 吻合率低于阈值则 W3 取消 |
| G-d | 改名股票（FB→META）的历史能否对上 | 用 security master 映射，否则 W3 跳过改名的股票 |
| G-e | 过期期权保留多久 | 决定 W2 是否值得做、要多快做 |
| G-f | 拉一个 SPX 到期日全部合约日 K 的成本，以及 `get_history_kl_quota` 是否显示期权用量 | 决定 W2 的耗时和额度 |

结果：

| # | 结论 | 实测 |
|---|---|---|
| G-a | 滚动，精确到分钟 | 1m 第一根 bar 始终是"现在 − 2,928 天"；13 分钟内下限移动了 13 分钟 |
| G-b | **不通过** | 5/5 只退市股票回 `Unknown stock`（另抽查 TWTR 同样）。lake 里 2021-06-11 以前也没有退市股票的 bar |
| G-c | 通过，需要平移 | Futu 1m 标的是 bar 结束时刻；减 1 分钟后，流动性好的股票 OHLC 完全一致 83–100%（价差 ≤ 0.07）。16:00 那根含收盘集合竞价量；无成交的分钟补成 volume=0 的平 bar（每天 390 根），Massive 不补 |
| G-d | 通过，需要 security_id 映射 | Futu 的 META、ELV 跨改名连续。mini 上 FB 的 1m 在 2022-06-09 以前存在 `FB` 下，`META` 下是另一只证券 |
| G-e | 约 1 个月 | 过期合约约一个日历月后消失（一个探测日期） |
| G-f | 一个到期日 17 分钟 | SPX 2026-12-18：1,744 个合约、1,838 次请求、1,026 秒、0 次限流；占 1 条链额度。期权额度清单能读到，剩余数读不到 |

### 4.2 W2 期权日 K 一次性抢救

**v2 结论：建议不做。** 原因：
- 过期合约约一个月后消失，抢救只能拿到在市合约和最近一个月过期的合约，补不了更早的历史。
- 期权日 K 有 69.5% 是 volume=0 的行，其中 39.4% 的收盘价在动，不是成交价。
- 一个到期日 17 分钟、1 条链额度；SPX 56 个到期日约 16 小时，每周额度 200 条链。
- Futu 期权日 K 和 UW 错开一个交易日后完全一致（SPY），所以对 SPY 是重复的。
- W1 从现在开始每天存全链快照，以后的数据已经覆盖。

如果仍然要做，以下是原设计：

- **做法**：一次性 ledger run（job `futu-option-history`）。对 W1 合约发现清单里的 SPX/SPXW 合约，从上市日拉日 K 到昨天。
- **存储**：`bronze/asset_class=option/underlying=SPX/history/expiry=<E>/1d.parquet`。
- **数据标记**：在 schema 里写明这是最后成交价、不是中间价、不能用来算 IV，并带上 `volume=0` 的补齐标志。
- **额度**：期权历史 K 线按链计数（同一到期日算 1 条），SPX 共 56 个到期日，在 200 的额度以内。耗时按 G-f 的实测值。
- **不做定期更新**：W1 的每日快照已经覆盖了以后的数据。

### 4.3 W3 美股 1m 回填 2018-10 → 2021-05（幸存者版本）

v2 结论：G-b 不通过，所以这份数据**只能**作为现存股票的分钟线补充，不能单独当回测 universe。要做的话，按以下规则：
- **最老的先拉**：窗口每天往后滚约 1 天；全分母 13,311 只、每周 1,000 只额度，要 14 周以上，这期间最老的约 98 天会滚出窗口。
- **时间**：`utc = ET(time_key) − 1 分钟`。
- **补零的 bar**：volume=0 的平 bar 不存（和 Massive 一致）。
- **收盘集合竞价**：16:00 那根的量是否保留要定规则；不含它时日成交量和 Massive 相差 ≤ 0.7%。
- **映射**：按 security_id，不按 ticker。
- **吞吐**：一只 49 秒、约 78 万根 bar；1,000 只约 13.6 小时。限制是额度，不是速度。

- **范围**：`registry/gaps.json` 里现有的股票 1m 分母，不另外造一个 universe；退市股票按 G-b 的结论处理。
- **额度和吞吐**：每 7 天 1000 只股票。8 年 1m 一只约 1 分钟。sp500 + ndx100 约一周拉完；全分母要按额度分多周。
- **写入**：只补 Massive 下限以前的部分，和现有的 Massive 行不重叠，并在 `source` 里写明 `futu`。
- **和 Massive 的边界**：Massive 平面文件的下限每天往后滚，所以以后 Futu 和 Massive 之间的接缝也会移动。adapter 必须按 bar 的 `source` 区分，不能混用。
- **验收**：T8 开始前先把数字定下来写进本节，不在拿到数据以后再定：
  - 比较范围：重叠区间里双方都有的 RTH bar（Futu 减 1 分钟后对齐；Futu 的 volume=0 补齐 bar 不算）。
  - 吻合：OHLC 每个值相差 ≤ 0.01 美元或 0.05%（取大者）；20 只里每只的吻合率 ≥ 阈值，阈值在 T8 开始前定。
  - 只有一边有的 bar：分别计数并报告，不算吻合也不算不吻合。
  - 空洞：一个交易日内 Futu 有成交的分钟数为 0，而 Massive 那天有 bar，才算空洞。

### 4.4 W4 Futu 作为 Tier B 日线补洞来源

- **v3 更正**：v2 写的「一行 registry 加一个测试」做不到。tier 不是 registry 决定的：`clients/gap_engine.py` 按 asset class 选修复来源（`REPAIR_SOURCE["equity"] = "massive"`），再按 Massive 下限判 A/B；`RegistryRow` 没有修复来源字段；`shepherd_repair` 只接受 ib/massive/nasdaq/stooq/legacy 的血缘。而且 CLAUDE.md 写明 Tier A 清单还没有喂给 `shepherd_repair`。
- **要做的范围**（如果做）：
  - `gap_engine` 按 symbol 选来源：Massive 下限以下、Futu 能报价的现存股票 → `futu`、Tier A；退市股票 → 仍是 `ib`、Tier B。
  - Futu 日线的抓取、`shepherd_repair` 接受 `futu` 血缘、以及修复执行的测试。
  - 「自动修复」要等 Tier A 清单真正接到修复执行器之后才成立。
- **规则**：依照 CLAUDE.md "Tier follows the repair source"。不另写检测器。
- **门槛**：G-d 已通过（需要 security_id 映射）；G-b 不通过，所以退市股票的缺口仍然只能 IB 补。日线在重叠区间的吻合率**还没测**，测完才做。

## 5. 任务和依赖

```
T0 OpenD@mini 实测 ──┐
T1 历史 K 线查证 ────┼──────────────┐
                    ▼              ▼
T2 futu_client + containment   T6 G-a..G-f 结论 → 改写 W2/W3/W4
        │                          │
        ▼                          ▼
T3 W1 fetcher + status ─→ T4 W1 上线 ─→ T5 argon IV canary
        │
T6 + T2 ─→ T8 W3 试点 20 只 ─→ T9 W3 全量
T8 ─→ T10 W4 来源路由 + 修复路径
```

| 任务 | 内容 | depends_on | 验收 |
|---|---|---|---|
| T0 | OpenD 部署到 mini 并实测 §2.1 的 5 项 | [] | 3 个交易日不掉线；报价延迟中位数 ≤ 5 秒；限频归属有结论 |
| T1 | 历史 K 线查证 — **完成** | [] | 查证报告落地，G-a..G-f 每项都有结论 |
| T2 | `clients/futu_client.py`、containment 测试、constants — **代码完成（MacBook），验收待 T0** | [T0] | 单测覆盖 §2.2 里标了「已实现」的每一条；CI 覆盖率 ≥ 95%；T0 的限频归属和每个端点的限流原文写进 constants |
| T3 | W1 fetcher、launchd 模板、status 检查、runbook — **代码完成，MacBook 实测通过，验收待 T0** | [T2] | 在 mini 上 dry-run 一个交易日（未做，依赖 T0） |
| T4 | W1 上线：PR 的 CI 绿 → 你授权后合并 → main 上该 SHA 的 CI 绿 → `release promote` → 在 mini 上 `readlink current`；同一个变更里把 `com.livewire.futu-options` 加进 `status._LAUNCHD_JOBS`，并在 mini 上加载 plist | [T3] | §3.0 的验收 1–3；卸载 plist 后 status 的 launchd 检查是 BAD |
| T5 | argon/apex 读取 + IV canary（由 argon 负责） | [T4] | argon 确认能读到数据 |
| T6 | 根据 G-a..G-f 改写 W2/W3/W4 — **完成（本 v2）** | [T1] | 本计划升到 v2 |
| T8 | W3 试点 20 只股票：先定 §4.3 的数字，再加历史 K 线的限速和抓取方法 | [T2, T6] | §4.3 的验收 |
| T9 | W3 全量回填，按额度分周 | [T8] | 每周的 ledger measurements；status 能看到进度 |
| T10 | W4：`gap_engine` 按 symbol 选来源、Futu 日线抓取、`shepherd_repair` 接受 `futu` 血缘（§4.4） | [T8] | 现存股票在 Massive 下限以下的缺口是 `futu`、Tier A；退市股票仍是 `ib`、Tier B；有测试 |

## 6. 运维规则（继承 CLAUDE.md）

- 写 lake 的操作一律通过 entrypoint，并写入 `runs` + `measurements`。
- 一个标的失败不影响其他标的，但只要有任何内容没拉到，这一阶段就不能退出 0。
- 不在 mini 上打断正在跑的补数任务。T0 部署 OpenD 时，要避开 IB 驱动正在跑的时段。
- 合并不等于上线：要 `release promote`，并在 mini 上核对 `readlink current`。

## 7. 风险

| 风险 | 影响 | 应对 |
|---|---|---|
| OpenD 无人值守失败 | W1 断档，快照补不回来 | T0 先验证；status 显示 BAD |
| 和 xenon 共用登录 / 限频 | 互相挤占 | T0 实测后在 constants 里分配额度 |
| 推广期结束，LV3/LV1 变成收费 | 数据源中断 | 每次运行记录权限；权限一变 status 就报 WARN |
| 退市股票缺失（已证实） | W3 带幸存者偏差 | 只做幸存者版本，写进数据的使用说明 |
| 1m 时间标记方式不同（已证实：标结束时刻） | W3 数据错位 1 分钟 | adapter 减 1 分钟，并有测试 |
| 远端合约的 `update_time` 是几天前 | 不能用它判断整条链是否过期 | 延迟只看最近到期日里最新的一条 |
| Futu IV 模型未知 | IV 不能直接用 | 存原始 bid/ask，由使用方重算 |
| 额度掉档 | W3 变慢；W1 不受影响 | 每次运行记录 `get_user_info` |

## 8. 需要决定的事

| # | 决定 | 建议 |
|---|---|---|
| D1 | 新依赖 `futu-api`（会带进 pandas） | 同意，并用 containment 测试限制在一个模块里 |
| D2 | OpenD 常驻 mini，和 xenon 共用登录和限频 | 同意，前提是 T0 通过 |
| D3 | 16:15 之后要不要再拍一次收盘价 | 15:45 必拍；16:20 可选，默认关闭 |
| D4 | 存 lake 还是 Postgres | 存 lake |
| D5 | SPY/QQQ 等是否改用 Futu、以节省 UW 配额 | 先不做；T5 之后由 argon 决定 |
| D6 | 退役 xenon 的 IB 期权归档 `option_chain.archive` | T4 之后再退役；删表需要你明确确认 |
| D7 | W2 期权日 K 抢救 | 不做（§4.2） |
| D8 | W3/W4 是否进入实现 | W3：只做幸存者版本，等 W1 上线后再定；W4：先测日线吻合率 |
| D9 | W1 在哪台机器跑 | OpenD 现在在 MacBook；上线前先做 T0，把 OpenD 放到 mini。2026-10-19 你确认 OpenD 可以常驻在线（§2.1 第 1 项）；第 2–5 项仍在 T0 里实测 |
| D10 | VIX 每 10 分钟、NDX/RUT 每 30 分钟的盘中快照要不要 | **已决定（2026-10-19）：保留。** |

## 9. 查证中顺带发现的 lake 问题（不在 Futu 范围内，另行处理）

- mini 上 `META` 的 1m 在 2022-06-09 以前是另一只证券，并且 2022 年有 4 个月的空洞；Facebook 的 1m 存在 `FB` 下。
- mini 上 `AAPL` 1d 有 11,486 / 11,551 行是 `source='legacy'`。
- UW 的 `option_contract_snapshots`（SPY）整天显示上一交易日的 last/bid/ask/volume，不是盘中报价。

## 10. 审查记录（tribunal，2026-10-16）

座位：Opus 5.5（主持兼投票）、Astra（`GPT-6-Astra medium`）、Fable（`claude-fable-5-1`）、Grok（`Grok 4.7 High`），各 1.0。代码和计划各审一次，再辩论 3 个有争议的点。

| 结论 | 内容 | 处理 |
|---|---|---|
| 代码已改 | status 只看今天，定时检查永远看不到正式快照缺失（4 个座位都提出） | 改成评最近一个已过截止时间的交易日 |
| 代码已改 | 掉线时二分拆批次 → 约 3 万次调用、FAILED 而不是 86；握手不结束时构造函数无限重试，非 daemon 线程让进程退不出 | 拆之前查 `get_global_state`；限流不拆；构造放 daemon 线程，30 秒上限 |
| 代码已改 | 部分快照重跑时返回成功；手动和定时并发会覆盖；:45 晚了正式快照就没了；盘中新挂的行权价永远不在 expected 里 | 重跑重新评分；标的-日期锁；收盘前补拍；15:30 再发现 |
| 代码已改 | VIX/NDX/RUT 也挂 SPY 当代理价；延迟为空时评 OK；后续 run 失败不影响 status；bid/ask 全空时照样 OK | 按标的选 ETF；空 → UNKNOWN；最近 run 非 OK → WARN；全空 → 失败 |
| 计划已改 | W4 一行 registry 做不到 Tier A；W3 门槛没有数字；T4 先合并后 CI；T2/T3 的依赖不成立；W1 验收只在被取代的文件里；T7 已决定不做却还在任务表；0.98 的理由不成立 | 见 §4.4、§4.3、§5、§3.0；`futu_min_capture_ratio/option` 改为 1.0 |
| 驳回 | 「OpenD 不发 high frequency」（Fable） | 实测原文是 "...request failed due to high frequency. Maximum 60 times per 30 seconds."，审计只引了后半句（已补全）；三个座位在辩论中都同意 |
| 推迟到 T4 | 现在就把任务加进 `_LAUNCHD_JOBS`（Fable） | 现在加会让 mini 每天 BAD；三个座位都同意放到 T4，已写进 T4 |
| 交给你 | 盘中频率（Fable 部分成立） | D10 |
| 代码已改（第 3 轮） | 正式快照补拍按取整后的时段判断，16:04 也会补（Astra）；延迟取自别的快照（Astra）；服务整体出错时仍逐个拆（Astra）；空延迟排在 WARN 前面（Grok）；无报价的正式文件记 1.0（Grok）；1.0 下限会让 Futu 不再提供的代码整天失败（Fable）；15:30 发现失败丢掉快照（Fable） | 用真实时刻；只取正式快照那次 run 的延迟；两半都失败就停；WARN 优先；无报价记 0；按名拒绝的代码另存、不算缺失、WARN；AM 结算到期日不算 expected；发现失败用已有集合 |
| 代码已改（第 4 轮，复核） | 二分时已救回的半边仍向上报失败，一批里两个被拒代码就让整个标的失败（Astra、Fable）；16:00 那次唤醒不再补拍正式快照（Grok）；同一 run 里晚到的正式快照和普通时段混用延迟（Astra 部分成立） | 子批次返回「是否救回任何行」，只有 ≥3 个代码且一个都没救回才算整体故障；补拍截到收盘那一分钟结束；正式快照的延迟和拒绝数用 `futu_official_*` 单独记录，status 只读这些。每条都有回归测试，全量 CI 3354 通过、覆盖率 95.19% |
| 代码已改（第 5 轮，复核） | 顺序补拍时没有在每个标的前再查一次收盘，和 §3「收盘后不补」矛盾（Astra）；第 4 轮的延迟测试没有调用 status，把 SQL 改回旧名测试仍然通过（Grok） | 每个正式快照开拍前再查一次截止时间，过了就不拍，记 `futu_official_past_close`，run 退出 1；测试改为经过 status 评分。两处都做了消融：去掉修复后测试失败 |
| 实测关闭（第 5 轮，Fable） | 二分规则下，连续 ≥7 个被拒代码（例如收盘后整个到期日被清掉）会让整个标的失败 | 2026-10-15（周四，SPXW 0DTE 到期日）MacBook 实测：16:02 和 16:12 ET 时 OpenD 仍提供全部 498 个当日到期代码，报价停在 16:00:00，拒绝 0；16:00 和 16:10 两次 tick 的 SPX 都是 30470/30470。最后一次 tick 是 16:10，第二天 `discover` 不再包含已到期代码，所以这个触发条件在采集窗口内不出现。规则保留 |
| 代码已改（第 6 轮，Astra） | 截止检查在等锁和发现之前，等锁 20 秒后 16:01:10 仍会拍 SPX 正式快照（Astra 用离线假会话复现） | 检查移进 `capture()`：等锁、发现之后，第一个报价请求之前；已存文件的重新评分不受影响。测试分两种变慢方式（前面的快照、本标的的发现），去掉检查两种都失败 |
| 测试补强 | 构造函数挂住时进程能退出（真 SDK 子进程）；锁；重评只看当时已发现的合约 | 去掉 daemon 设置 → 子进程 60 秒超时；去掉锁 → 测试失败（都实际跑过） |

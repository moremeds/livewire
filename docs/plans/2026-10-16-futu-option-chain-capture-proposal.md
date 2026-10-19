# Proposal：用 Futu OpenD 采集指数期权全链快照 — 2026-10-16

状态：**提案，未批准，没有写代码。**
证据：`docs/audits/2026-10-16-futu-opend-market-data-probe.md`（实测数字都在那里，下文不再重复来源）。
参与意见：argon1、signal-lab-cf、signal-lab-4b（2026-10-15 跨会话问答）；xenon-68 审查（2026-10-16）。

## 1. 问题

argon 的 SPX 期权数据只有 UW 一个来源，而且很薄。mini Postgres 实测：

- `option_surface_grid_daily` 里 SPX/SPXW、VIX、NDX、RUT 都是 0 行。
- `greeks_by_expiry_strike` 的 SPX 每天只有 1 个到期日。
- UW 日配额 9 月里 10 天有 6 天用完。

所以 argon 拟合不出 SPX 多期限曲面。VRP macro credit 只能按 flat vol 定价，按 argon 的估算被高估约 65%。
xenon 在 6 月做过 IB 全链归档（`option_chain.archive`），只跑了 34 次就停了，原因是 IB 约 100 条行情线的上限。

Futu 免费权限（OPRA LV1）可以补这个缺口：

- 快照每次 400 个代码，**不占订阅额度**，报价实测是实时的（延迟 3 秒）。
- SPX 全链 30,470 个合约，一轮约 40 秒。

## 2. 目标和验收

目标：从现在起每个交易日，把 SPX/SPXW、VIX、NDX、RUT 的全链报价 + Greeks 原样存进 lake。只追加，不覆盖。

验收（在 mini 生产环境上验证）：

1. 连续 5 个交易日，每个标的都有 15:45 ET 的快照，`captured / expected ≥ 0.98`。expected 来自当天的合约发现清单，不来自 glob lake。
2. SPX 每 10 分钟一次的快照覆盖 09:40–16:10 ET（半日市截止到 `session_close_time` + 10 分钟），缺失 ≤ 1 次/天。
3. `status` 和 digest 上能看到新的检查项，且是绿的。OpenD 断线时显示 DEGRADED，不显示成 OK。
4. CI 命令全过：`uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning`。

## 3. 范围

**做：**

| 层级 | 标的 | 频率 |
|---|---|---|
| T1 | SPX + SPXW（`US..SPX` 链里两个 root 都有）、VIX | 每 10 分钟，RTH 内 |
| T2 | NDX、RUT | 每 30 分钟 + 15:45 |

15:45 ET 那一次对所有标的都做，并标记为当天的"正式"快照（三方共识的时间点）。

**不做（以及原因）：**

- SPY/QQQ/IWM/GLD/TLT/NVDA：UW 从 2025-12-26 起已经有每日曲面，再存就是重复。是否用 Futu 替换 UW 由 argon 决定（见 §9 D5）。
- 个股、港股期权：现在没有使用方。
- XSP：和 SPX 重复。
- 期权 1 分钟 K 线、过期合约历史回填：只有 last-trade OHLCV，没有 IV/OI；1m 里没成交的分钟会补成 volume=0 的平线（GTH 夜盘的非零 bar 可能是真实成交）。
- livewire 内部重算 IV：只存原始数据。IV 用中间价 + put-call parity 重算放在使用方（apex/argon），以后需要时再考虑加 derived 层。
- 美股 1 分钟线 2018-10 → 2021-05 回填：价值另算，单独提案。

## 4. 设计

### 4.1 进程和调度

- OpenD 常驻在 **mini**，只听 `127.0.0.1:11111`。现在只在 MacBook 上跑。
- 新的入口是 `scripts/livewire_ingest.py futu-options`，实现放在 `livewire_scripts/fetch_futu_options.py`。
- 新增第 10 个 launchd 任务 `com.livewire.futu-options`：`StartInterval 600`，程序指向 `<warehouse>/current`。
- 每次触发时，程序先用 `clients/trading_calendar.py` 判断是否在交易时段内。不在时段内就直接退出 0，不写 run 行。
- 每个交易日的第一次触发（≥ 09:15 ET）先做合约发现，结果写成当天的合约清单。后面的触发只拉快照。

### 4.2 限频预算（每 30 秒：快照 60 次 × 400 码；查链 10 次，每次跨度 ≤ 30 天）

| 步骤 | 调用次数 | 耗时 | 依据 |
|---|---|---|---|
| 每日合约发现（4 个标的） | 约 50 次查链 | 约 2.5 分钟 | SPX 实测 16 次 / 65 秒；其他标的按到期日数估算 |
| 10 分钟触发：SPX + VIX | 约 82 次快照 | 约 41 秒 | SPX 实测 77 次；VIX 估算 ≤ 5 次 |
| 整点 / 半点触发：再加 NDX + RUT | 再加约 93 次 | 合计约 90 秒 | 估算：NDX 约 2.5 万、RUT 约 1.2 万个合约 |

最忙的一次触发也只用 600 秒窗口里快照额度的约 15%。
客户端按每 30 秒 ≤ 55 次主动限速，不靠撞到限频错误再重试（参考 Massive FX 的教训）。

### 4.3 lake 布局

与 energy 一样不按 symbol 分键：

```
bronze/asset_class=option/underlying=<SPX|VIX|NDX|RUT>/date=<YYYY-MM-DD>/
    contracts.parquet          # 当天合约发现清单 = expected
    snap_<HHMM>.parquet        # 每次快照一个文件，写一次后不再改
```

- 写入用 `clients/parquet_io.publish_parquet`（temp → validate → os.replace）。目标文件已存在就拒绝写，保证只追加。
- 每天约 108 个文件（SPX 和 VIX 各 40 次，NDX 和 RUT 各 14 次）。按计算约 160 MB/天、约 40 GB/年，这是 142 列全存的上限，裁剪列后会更小。mini `/Volumes/DATA_LAKE` 剩 5.7 TiB。
- 内置盘只剩 30 GiB，所以数据不能落在本地的 ledger/raw 目录里。

**列**（从 Futu 快照里挑出来的原始字段，不做变换）：

- 时间：`capture_ts_utc`、`vendor_update_ts_utc`（Futu 给的是 ET 本地时间，要转成 UTC）。
- 合约：`code`、`root`（SPX/SPXW）、`underlying`、`expiry`、`settlement`（AM/PM）、`strike`、`right`、`multiplier`。
- 报价：`bid`、`ask`、`bid_size`、`ask_size`、`last`、`volume`。
- OI：`open_interest`，另加 `oi_asof`（= 上一个交易日，即 OCC 结算值，按 PIT 规则标记）。
- Futu 计算值：`iv_vendor`（%）、`delta`、`gamma`、`vega`、`theta`、`rho`（只作参考）。
- 每个快照文件还附一行同一时刻的 SPY bid/ask 作为标的代理价，因为 Futu 没有 SPX 指数报价。

### 4.4 ledger、status 和 constants

- **ledger**：用 `ledger.open_run` 开 run，job 名为 `futu-options`。整个运行体包在 `BaseException` 里，以保证中断也会写关闭行。
- **measurements**（每个标的每次快照写一条）：
  - `futu_contracts_expected`、`futu_contracts_captured`
  - `futu_capture_seconds`
  - `futu_quote_lag_s`：采集时间减去最近到期日平值合约的 `vendor_update_ts`
  - `futu_iv_null_share`
  - `futu_rate_limited_calls`
- **status 的 CHECKS 新增两行**：
  - "Futu option snapshots"：今天 15:45 快照每个标的的 captured/expected 是否 ≥ 阈值。16:30 ET 后还没有就是 BAD。
  - "Futu quote lag"：中位数超过阈值是 WARN。
  - digest 会自动带上这两项，因为 digest 渲染的就是 `status.collect()`。
- **constants**：在 `clients/constants.DECLARED` 里新增以下 key：
  - `futu_snapshot_calls_per_30s`（55）、`futu_snapshot_codes_per_call`（400）
  - `futu_chain_calls_per_30s`（9）、`futu_chain_span_days`（30）
  - `futu_min_capture_ratio`（0.98）、`futu_max_quote_lag_s`（60）
  - 每次运行都按现有机制把它们写成 `source='declared'`。
- **source 闭集**：在 `clients/telemetry.py` 和 `clients/quality_flags.py` 的 `_VALID_SOURCES` 里加 `futu`。

### 4.5 失败语义（沿用 IB 规则）

- OpenD 连不上或未登录：抛出带类型的错误，退出码 86，记为 DEGRADED，不立刻 page，也不重试 OpenD。livewire 不管理 OpenD，和 IB Gateway 一样。
- 快照永远补不回来，所以 15:45 这一次缺失会由 status 判 BAD，并通过 digest 让人看到。
- 单个批次失败：只隔离这一批，其他批次继续拉；最后 captured/expected 低于阈值，这次运行就算失败。不能出现"拉到 0 行、退出 0"的结果。
- 撞到限频错误：按声明的限速退避，然后重试这一批一次。限流丢掉的部分没有任何报错之外的提示，所以每次快照都要断言 `rows == expected`；不相等时把差额写入 measurement。
- 一个坏代码（比如合约在发现之后到期或被清掉）会让整批 400 个失败。批次失败时二分拆开重试，把坏代码单独记下来，不能让它拖垮整批。
- 连接 OpenD：先做 3 秒 TCP 探测，再用 30 秒有上限的 connect。SDK 在 OpenD 拒绝连接时会无限重试（xenon PR #179，xenon-api 因此卡死约 4 小时）。
- 每次运行前调用 `get_user_info`。美股期权权限不是 LV1 时直接 DEGRADED，不写入降级的数据。SDK 推送的 `KickedOut`、`NeedPhoneVerifyCode`、`NotifyType_QotRight` 事件都记成 measurement。
- 列和 dtype 固定在一个 pyarrow schema 里。Futu 的 `'N/A'` 字符串转成 null；list 类型的单元格不存。schema 不匹配时整次快照失败，不能漂移。

### 4.6 依赖控制

- 新增 `futu-api`。它会带进 pandas、simplejson、protobuf、pycryptodome。
- `futu` 只允许在 `fetch_futu_options.py` 里 import，拿到的 DataFrame 立即转成 pyarrow。
- 加一个 containment 测试（照 `tests/test_duckdb_containment.py` 写），禁止其他模块 import futu 或 pandas。

## 5. 使用方（不在本提案的代码范围内）

- **argon**：通过 apex 读 lake（由 argon/apex 各自提 PR）。正式采信之前，先用 `option_surface_iv_canary` 把 IB IV 和 Futu IV 并行对比几周。
- **signal-lab**：数据只能往后积累，1–2 年内不能做回测，也不能推翻已有的 KILL 结论。研究照常走实验室的预注册流程。
- 使用方要遵守：SPX（AM，按开盘 SOQ 结算）和 SPXW（PM，按收盘结算）不能混用；不能把 last 当作价格去算 IV；IV 用中间价自己重算。

## 6. 阶段

| 阶段 | 内容 | 验收 | depends_on |
|---|---|---|---|
| P0 实测（不写代码） | 在 mini 上配置 OpenD 无人值守登录；测同一账号同时开 MacBook 和 mini 两个 OpenD 会不会互踢；测限频是按账号算还是按 OpenD 算；在 mini 上用 IB 和 Futu 对比 5 个合约的报价时间戳，一天测三个时点 | OpenD 无人值守连续 3 个交易日不掉线；报价延迟中位数 ≤ 5 秒；限频归属有结论 | [] |
| P1 实现 | fetcher、launchd 模板（加进 `test_launchd_templates` 的 `JOB_TEMPLATES`）、ledger、status、constants、containment 测试、runbook 条目 | 本地 CI 全过；在 mini 上 dry-run 一天 | [P0] |
| P2 上线 | 合并 → CI 绿 → `release promote` → 检查 `readlink current` | 达到 §2 的验收 1–3 | [P1] |
| P3 交接 | 告诉 argon/apex 读取路径；argon 开始跑 IV canary | argon 确认能读到数据 | [P2] |

## 7. 预计改动的文件

新增：

- `livewire_scripts/fetch_futu_options.py`
- `launchd/com.livewire.futu-options.plist.example`
- `tests/test_fetch_futu_options.py`
- `tests/test_futu_containment.py`

修改：

- `scripts/livewire_ingest.py`（子命令）
- `clients/constants.py`、`clients/telemetry.py`、`clients/quality_flags.py`
- `livewire_scripts/status.py`
- `tests/test_launchd_templates.py`、`tests/test_status.py`、`tests/test_constants.py`
- `pyproject.toml` + `uv.lock`
- `docs/runbook.md`、`CLAUDE.md`（一行规则）

## 8. 风险

| 风险 | 影响 | 应对 |
|---|---|---|
| mini 上的 OpenD 需要人工登录或短信验证（SDK 有 `NeedPhoneVerifyCode` / `KickedOut` 事件） | 采集中断，而且缺失的快照补不回来 | P0 先验证；断线由 status 显示 DEGRADED / BAD |
| 两个 OpenD 互踢，或限频按账号共享（xenon 也在用 Futu） | MacBook 或 xenon 和采集任务互相挤占 | P0 实测；必要时只在 mini 上保留一个 OpenD |
| 额度掉档（资产或交易量下降） | 只影响历史 K 线和订阅；快照不占额度，基本不受影响 | 每天记录 `get_user_info` 的权限，权限一变 status 就报 WARN |
| Futu 的 IV 模型未知 | IV 不能直接用 | 存 bid/ask 原始值，由使用方重算 IV |
| Futu 清理过期合约 | 晚一天开始，就永久少一天的数据 | 只能尽早上线 |
| 推广期结束，LV3/LV1 变成收费 | 数据源中断 | `get_user_info` 每天记录权限变化，status 报 WARN |

## 9. 需要决定的事

| # | 决定 | 建议 |
|---|---|---|
| D1 | 允许新依赖 `futu-api`（会带进 pandas） | 同意，并用 containment 测试限制在一个模块里 |
| D2 | OpenD 常驻 mini，和 xenon 共用一个登录和一套限频（xenon 现在在容器里连不到 OpenD） | 同意，前提是 P0 通过；xenon 一侧的连接方式和限频分配一起规划 |
| D3 | 16:15 之后要不要再拍一次收盘价（argon 想要；signal-lab-4b 认为 16:00–16:15 要避开） | 15:45 必拍；16:20 作为可选标记的快照，默认关闭 |
| D4 | 存在 livewire lake，还是存在 Postgres、和 UW 表放一起 | 存 lake（livewire 的规则是 lake 为唯一记录来源，并且 argon1 也是这样提议的） |
| D5 | SPY/QQQ/IWM/GLD/TLT/NVDA 是否改用 Futu、以节省 UW 配额 | 先不做；P3 之后由 argon 决定 |
| D6 | 退役 `option_chain.archive`（xenon 的 IB 归档，34 次 partial 运行） | P2 之后再退役；删表需要你明确确认 |

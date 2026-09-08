# STEP3 — 数据与容量判断（待人工批准）

交接步骤 3「完成数据/容量判断」的评审件。范围：`docs/codex-handoffs/2026-09-08-integrity-recovery-inventory.md` 编目的 **269 个失败 symbol + 53 个窗口缩短 + 1 个窗口延长（MUNJ）**，加上已测容量数字的合并与缺口清单。

**本文件不修数据、不改代码、不做切换。** 它只回答三件事：这些条目当前被隔离成什么样、消费者看到什么、下一步需要谁给什么证据。凡是判定「哪边数据正确」的地方一律留空——本轮没有做过 provider 侧核对。

写作时间 2026-09-08。候选 `38c90bb`（本 worktree HEAD 的父提交链内），容量样本固定在 `c11caa8`，Apex 候选 `ae57f93`。

---

## 0. 三行结论

1. **269 个失败 symbol 已经不在生产 Silver 里了**——生产 rev39 与 fixture rev1 都是 13,279 成员、additions=0/withdrawals=0，而输入是 13,548 个 symbol（13,548 − 269 = 13,279）。切换不改变消费者能看到的 symbol 集合。
2. **53 个窗口缩短在真实生产血统里不会被发布**：它们会被 `rebuild_silver` 的 window-regression 闸门挡住、旧 rev39 窗口被 carry-forward 保留，运行仍 exit 0 但 `window_regressions=53` 常驻告警。fixture 里该计数为 0 **只是因为 fixture 从空血统起步**（代码可证，见 §2.1）。
3. **容量数字齐全且与 Mini receipt 逐个对上**（本轮抽查 3 份 receipt，全部匹配，含两个 SHA256）；真正没有测的是冷缓存整轮、coverage 竞争、锁等待预算、读者影响、断电、单盘故障、备份恢复——一律保持公开未知，不给估计值。

---

## 1. 269 个失败：按类别的隔离、消费者影响与解除条件

### 1.0 计数自检（本轮重算）

| 类别 | 数量 | 复核方式 |
|---|---:|---|
| U 价格基准未知 | 198 | `awk` 统计 inventory 附录 A 的类别列 |
| C 股息/报价币种矛盾 | 61 | 同上 |
| S 有效拆股冲突 | 5 | 同上 |
| D 现金股息约束失败 | 4 | 同上 |
| R RJF parquet footer 不可读 | 1 | 同上 |
| **合计** | **269** | 269 个唯一 symbol；与 53 个窗口 symbol **交集 = 0**（`comm -12` 实算，与 inventory 声明一致） |

来源：`docs/codex-handoffs/2026-09-08-integrity-recovery-inventory.md` 附录 A/B。原始 receipt `/Volumes/DATA_LAKE/livewire/disposable/se08-20260908T1122-c11caa8/logs/full-migration-failures.json`，本轮 `ssh macmini shasum -a 256` 实测 `7e5b2c4760307f7a10dd826bb698e57b0c771e7fed73a5d58424304f9d9f26ba`，与 inventory 记录**一致**。

### 1.1 共同的隔离机制（五类走同一条路）

五类失败全部是 `build_factor_intervals` / staging 抛出的异常，被同一个 per-symbol 捕获点吞掉：

| 环节 | 代码位置 | 行为 |
|---|---|---|
| 抛出 | `clients/adjustment_engine.py:91`（S）、`:115`（C）、`:119`（D）、`:141`（U） | `ValueError`，fail closed，不猜测 |
| 抛出（R） | pyarrow 读 footer 失败，经 `livewire_scripts/rebuild_silver.py:502`(`OSError`)/`:507`(`Exception`) | 同上 |
| 捕获 + 记账 | `livewire_scripts/rebuild_silver.py:502-509` → `_failure()` `:354-384` | 写 symbol、error_type、error、bronze_path、source_sha256、首/末交易日、active_actions 身份；stderr 打一行；**循环继续** |
| 逐出 manifest | `livewire_scripts/rebuild_silver.py:236-262`（`_carry_forward` docstring 与 `if symbol in scope and symbol not in staged_ok: continue`） | 「in-scope 但 staging 失败」的 symbol **不被 carry-forward** —— 这就是隔离/quarantine |
| 全灭保护 | `livewire_scripts/rebuild_silver.py:636-638` | 若一个 in-scope symbol 都没成功，`SystemExit`，拒绝发布空 revision，旧 commit 原样保留 |

一句话：**单 symbol 失败只影响该 symbol，其余 13,279 个照常提交，退出码 0。** 这一点在 Mini 上被 fixture-corruption 场景实测过：故意损坏一个 symbol 后 `revision 3 / failed 270 / unchanged 13278 / exit 0`（`logs/fixture-corruption.stdout.log`，本轮 ssh 读取原文）。

### 1.2 消费者（Apex）看到什么

**关键事实：这 269 个 symbol 现在就已经不在生产 Silver manifest 里。** 生产 `current.json` 本轮实读 `revision 39 / members 13279 / artifacts 26558`；fixture rev1 同为 13,279 成员，比较结果 `additions=0, withdrawals=0`（`logs/window-manifest-comparison.json`，本轮 ssh 实读）。所以候选切换**不会新增任何一个被排除的 symbol**。

候选 Apex（`ae57f93`）对一个不在 manifest 里的 symbol 的应答：

| 情形 | 代码位置 | 结果 |
|---|---|---|
| 无 Silver artifact，但 Bronze 存在（= 被隔离，269 全属此类） | `src/infrastructure/adapters/livewire/ohlc_provider.py:229` → `src/api/routes/chart.py:103` → `src/api/errors.py:55` | `AdjustedDataUnavailable` → `ApiError(ADJUSTED_UNAVAILABLE)` → **HTTP 503**，body 带 `symbol`/`asset_class` |
| 无 Silver 也无 Bronze（不存在的 ticker） | `ohlc_provider.py:222-228` 返回 `[]` → 路由探测 artifact 后 | **HTTP 404 `unknown_symbol`** |
| intraday adjusted 缺 factor artifact | `ohlc_provider.py:236` | 同样 **503** |
| Bronze/raw 模式 | 不经 Silver | 不受影响 |

**与仓库 `CLAUDE.md` 的偏差**：根 `CLAUDE.md`「Architecture in six lines」写的是「a missing one fails closed (HTTP 500)」。候选 Apex 已把这条改成 503（`src/api/errors.py:42` 的注释明写「503 for ADJUSTED_UNAVAILABLE is deliberate」）+ 404（未知 ticker）。切换后需要更新那一行描述，否则文档与行为不符。

### 1.3 逐类：证据、恢复动作、解除/升级条件

下表的「什么证据能定分」是本文件唯一允许的判断——**不判定哪边数据对**，只说需要拿到什么才能判。

| 类 | 数量 | 触发条件（代码语义） | 恢复动作（未执行） | 什么证据能定分 | 升级条件 |
|---|---:|---|---|---|---|
| **U** 价格基准未知 | 198 | `adjustment_engine.py:139-141`：某个 split 生效日之前的 bar 其 `price_basis == "unknown"`，无法判断该行是 raw 还是已 split-adjusted，故拒绝构造 factor | 逐 symbol 定位「首个受影响日期」（inventory 已记，如 `1980-03-17` … `2026-05-29`，共 143 个不同日期），用同期 provider 原始行反推基准；候选走 `resolve_yahoo_basis` 且必须 `--ib-verify`（根 `CLAUDE.md` 硬规则） | 同一 symbol、split 前后各 N 日的 **第三方原始 OHLCV**（IB 窗口作为 gate，非 source），能证明存储行是 raw 还是 split_adjusted | 若该 symbol 被 Apex 图表/信号实际请求且用户要求可用 → 升为单 symbol 候选审批 |
| **C** 股息/报价币种矛盾 | 61 | `adjustment_engine.py:113-115`：action 的 `currency` 与前一根 bar 的 `currency` 不等（bar 缺字段时默认 `USD`） | 取出该 symbol 的 cash_dividend action 币种与 Bronze 报价币种做对照表；不自动换汇、不改币种 | provider 原始股息记录的币种字段 + 证券上市地/报价币种（security master 或交易所）；**receipt 未记录冲突的具体币种对**，必须回 `/failures/<i>/active_actions` 取 | 若对照表显示是 livewire 侧 ingest 的元数据缺省（如 bar `currency` 字段缺失被默认成 USD）→ 变成代码/ingest 问题，升级 |
| **S** 有效拆股冲突 | 5 | `adjustment_engine.py:87-93`：同一 ex_date 上两个 active split 的 price factor 不等（相等则按 `superseded` 折叠） | 事件级对照：两个 factor、action_id、provider 与 revision/supersession | provider 侧该事件的权威记录（含撤销/重述），或交易所公告 | 任一 symbol 是 S&P500/常用标的 → 升级（本组 5 个：见下表） |
| **D** 现金股息约束失败 | 4 | `adjustment_engine.py:117-119`：`cash >= previous_close × split_factor` 或 `previous_close <= 0` | 核对除息日、金额单位、币种、前收盘及其 basis；区分特别股息/单位错误/基准错误 | 该次分派的原始公告（金额+单位+币种）与同日收盘 | 若前收盘为 0/负 → 是 Bronze 数据问题，升级为 Bronze 修复 |
| **R** RJF footer 不可读 | 1 | parquet magic bytes 缺失 | 找可验证替代来源（时间覆盖、SHA、完整解码、OHLCV 校验），保留损坏原件 | canonical 与 fixture 字节 SHA256 相同（`54e59feb…4051`，两份文档一致，**本轮未复读**[unverified]），说明不是复制过程损坏；**原始损坏原因未知，不给因果结论** | RJF 是 S&P500 成分股 → 已经是升级项；需用户批准替换 canonical |

**S 类 5 个的完整明细**（inventory 附录 A 原文，本轮 grep 抽出）：

| ID | Symbol | ex_date | 冲突 price factor | action_id | 输入覆盖 | 指针 |
|---|---|---|---|---|---|---|
| F096 | FTLF | 2019-04-16 | 0.00125 vs 8000 | `8ecc168c…` | 2013-10-01 → 2026-09-04 | `/failures/95` |
| F131 | LADR | 2015-12-08 | 0.91109365… vs 0.88742719… | `dbf3096a…` | 2014-02-06 → 2026-09-04 | `/failures/130` |
| F144 | MDRR | 2024-07-03 | 10 vs 0.2 | `e24ba650…` | 2021-06-11 → 2026-09-04 | `/failures/143` |
| F174 | OUT | 2014-11-18 | 0.89158345… vs 0.85309674… | `69a3b1ca…` | 2014-03-28 → 2026-09-04 | `/failures/173` |
| F210 | SLG | 2020-12-14 | 0.96683747… vs 0.97285728… | `e9e1c8a6…` | 1997-08-15 → 2026-09-04 | `/failures/209` |

观察（非结论）：FTLF 与 MDRR 是**互为倒数**的一对（0.00125↔8000 即 1:8000 与 8000:1；10↔0.2 非精确倒数），LADR/OUT/SLG 是**接近但不等**的两个比率，形态上更像同一事件在两次 revision 里被重述。这与 `adjustment_engine.py:75-86` 注释里记录的 2026-08-02 观测同型。要定分仍需 provider 侧事件记录。

**D 类 4 个**：F009 ARKD、F073 DBRG、F086 ELA、F143 MCHB（`/failures/8, 72, 85, 142`）。receipt 未记录触发的具体除息日与金额，必须从 `active_actions` 取。

### 1.4 「不必全修才能发布」的依据

- 隔离是 per-symbol 的，且全灭有保护（§1.1）。
- 消费者可见集合不变（§1.2）——269 已经不在 rev39 里。
- 因此 **269 项不是切换的阻塞项**；它们是切换之后的长期数据债，按类推进。
- 但有一条必须写进切换方案：切换后 Silver 的 `failed=269` 会继续每晚出现在 SUMMARY_JSON 里；这不是新故障，需要在告警基线里标成已知量，否则每晚误报。

---

## 2. 53 个窗口缩短：逐项表与决策

### 2.1 先决事实：在真实生产血统里，这 53 项默认**不会**生效

fixture 的 `window_regressions=0` 是构造性的，不是「没有回归」：

```
livewire_scripts/rebuild_silver.py:529-542
previous_start = {item.symbol: item.earliest_date for item in (current.affected if current else ())}
regressions   = [... for item in staged if item.symbol in previous_start and item.window["start"] > previous_start[...]]
regressed     = set() if args.allow_window_regression else {…}
publishable   = [item for item in staged if item.symbol not in regressed]
```

fixture 首次全量发布时 `current is None` → `previous_start` 为空 → regressions 恒为 `[]`。生产 rev39 存在时，同样的 53 个 symbol 会命中 `window["start"] > previous_start`，被移出 `publishable`，再由 `_carry_forward`（`:236-276`，regressed symbol 仍在 `staged_ok` 里，故**被 carry**）沿用 rev39 的旧 artifact 与旧 `earliest_date`。

后果（推断，基于代码 + receipt，未在生产上实跑 [unverified]）：

| | fixture 里发生的 | 生产血统里会发生的 |
|---|---|---|
| 53 个 symbol 的窗口 | 按新（短）首日发布 | **保留 rev39 旧（长）窗口** |
| `window_regressions` | 0 | **53** |
| 退出码 | 0 | 0（该警告不改退出码，见 `:320-331` 注释） |
| 需要额外动作 | — | 除非有人显式加 `--allow-window-regression`（根 `CLAUDE.md`：该 flag 只为 rev-3 bootstrap 用过一次），否则**不需要**在切换前逐项定案 |

**注意 carry-forward 的副作用**：rev39 的 artifact 是 legacy 路径（非 `generations/`），carry 时走 `_copy_verified_legacy_artifact`（`:214`/`:265-271`），逐个校验 sha256；任何一个不匹配就 `ValueError` 并**整次发布失败**（不是 per-symbol 隔离）。这条应放进切换方案的 stop condition。

### 2.2 逐项表（symbol / 旧首日 / 新首日 / delta 天）

数据源：inventory 附录 B（本轮解析 53 行，全部有 `/shortened/<i>` 指针，指向 `logs/window-manifest-comparison.json`，本轮实测 SHA256 `b9b5af2e…bc39`，与文档一致）。delta 为本文件按日历日计算（`new − old`）。**「最后有效数据」全部未知**，故「决策」列一律是需要什么证据。

| ID | Symbol | 旧首日(rev39) | 新首日(fixture rev1) | Δ天 | 形态分组 | 决策 / 需要的证据 | 证据指针 |
|---|---|---|---|---:|---|---|---|
| W001 | AACBR | 2025-04-07 | 2026-08-14 | 494 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/0` |
| W002 | ABEO | 2012-12-28 | 2015-05-22 | 875 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/1` |
| W003 | AIM | 2021-06-11 | 2025-06-17 | 1467 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/2` |
| W004 | ALPS | 2022-09-28 | 2025-10-31 | 1129 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/3` |
| W005 | AMCR | 2019-06-11 | 2021-06-11 | 731 | seed 边界（新首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：seed 判定输入与 2021-06 前后的原始 bronze 行 | `/shortened/4` |
| W006 | ANSCW | 2024-01-03 | 2026-08-03 | 943 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/5` |
| W007 | APUS | 2025-05-09 | 2026-03-26 | 321 | 缩短<1年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/6` |
| W008 | BNY | 2021-06-11 | 2026-05-21 | 1805 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/7` |
| W009 | BNZI | 2023-12-15 | 2026-05-08 | 875 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/8` |
| W010 | CCIXW | 2024-06-21 | 2026-07-15 | 754 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/9` |
| W011 | CELUW | 2021-07-19 | 2026-07-17 | 1824 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/10` |
| W012 | CPHI | 2021-06-11 | 2026-07-21 | 1866 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/11` |
| W013 | CSX | 2015-12-22 | 2021-06-11 | 1998 | seed 边界（新首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：seed 判定输入与 2021-06 前后的原始 bronze 行 | `/shortened/12` |
| W014 | CTO | 2021-02-03 | 2021-06-11 | 128 | seed 边界（新首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：seed 判定输入与 2021-06 前后的原始 bronze 行 | `/shortened/13` |
| W015 | DELL | 2018-12-21 | 2021-06-11 | 903 | seed 边界（新首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：seed 判定输入与 2021-06 前后的原始 bronze 行 | `/shortened/14` |
| W016 | DKI | 2025-08-08 | 2025-09-30 | 53 | 缩短<1年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/15` |
| W017 | FCUV | 2021-08-31 | 2026-07-31 | 1795 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/16` |
| W018 | FGIWW | 2022-01-25 | 2026-07-28 | 1645 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/17` |
| W019 | GDEVW | 2023-03-20 | 2026-08-25 | 1254 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/18` |
| W020 | GFAIW | 2021-09-29 | 2026-08-27 | 1793 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/19` |
| W021 | HHH | 2021-10-20 | 2023-08-14 | 663 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/20` |
| W022 | LBGJ | 2026-02-27 | 2026-07-20 | 143 | 缩短<1年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/21` |
| W023 | MZTI | 1980-03-17 | 2010-05-24 | 11025 | 缩短≥10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/22` |
| W024 | NCL | 2023-10-19 | 2023-12-19 | 61 | 缩短<1年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/23` |
| W025 | NCT | 2025-03-28 | 2026-09-03 | 524 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/24` |
| W026 | OII | 1980-03-17 | 2026-09-04 | 16972 | 缩短≥10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/25` |
| W027 | OPI | 2021-06-11 | 2026-06-22 | 1837 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/26` |
| W028 | PAMT | 1994-05-27 | 2005-10-18 | 4162 | 缩短≥10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/27` |
| W029 | PFSA | 2025-07-14 | 2026-08-18 | 400 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/28` |
| W030 | PLAG | 2021-06-11 | 2026-08-11 | 1887 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/29` |
| W031 | POM | 2025-10-08 | 2026-05-08 | 212 | 缩短<1年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/30` |
| W032 | PSTV | 2021-06-11 | 2026-04-02 | 1756 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/31` |
| W033 | RCKTW | 2023-02-27 | 2026-09-02 | 1283 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/32` |
| W034 | RCON | 2021-06-11 | 2026-08-05 | 1881 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/33` |
| W035 | RITR | 2024-08-23 | 2026-08-03 | 710 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/34` |
| W036 | RNWWW | 2021-08-24 | 2026-08-17 | 1819 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/35` |
| W037 | RVI | 2021-06-11 | 2026-03-06 | 1729 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/36` |
| W038 | SION | 2025-02-07 | 2026-08-10 | 549 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/37` |
| W039 | SKK | 2024-10-08 | 2026-05-04 | 573 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/38` |
| W040 | SMH | 2011-12-21 | 2021-06-11 | 3460 | seed 边界（新首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：seed 判定输入与 2021-06 前后的原始 bronze 行 | `/shortened/39` |
| W041 | SMJF | 2025-12-04 | 2026-08-27 | 266 | 缩短<1年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/40` |
| W042 | SRE | 2006-05-24 | 2021-06-11 | 5497 | seed 边界（新首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：seed 判定输入与 2021-06 前后的原始 bronze 行 | `/shortened/41` |
| W043 | STAK | 2025-02-26 | 2026-07-24 | 513 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/42` |
| W044 | TENX | 2021-06-11 | 2026-08-10 | 1886 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/43` |
| W045 | VIVO | 2021-06-11 | 2026-03-16 | 1739 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/44` |
| W046 | VSEEW | 2024-06-25 | 2026-08-12 | 778 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/45` |
| W047 | VSTD | 2025-09-03 | 2026-08-24 | 355 | 缩短<1年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/46` |
| W048 | WRB | 2006-11-16 | 2021-06-11 | 5321 | seed 边界（新首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：seed 判定输入与 2021-06 前后的原始 bronze 行 | `/shortened/47` |
| W049 | WW | 2021-06-11 | 2025-07-07 | 1487 | seed 起点被再裁（旧首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：为何 2021-06-11 起点被再次裁掉——continuity>6.0 触发日与 triage verdict | `/shortened/48` |
| W050 | XLE | 2021-05-18 | 2021-06-11 | 24 | seed 边界（新首日=2021-06-11） | 保留旧窗口（默认已发生）；需证据：seed 判定输入与 2021-06 前后的原始 bronze 行 | `/shortened/49` |
| W051 | XOSWW | 2026-06-03 | 2026-08-18 | 76 | 缩短<1年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/50` |
| W052 | YXT | 2024-08-16 | 2026-08-05 | 719 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/51` |
| W053 | ZYBT | 2025-01-07 | 2026-07-20 | 559 | 缩短1–10年 | 保留旧窗口（默认已发生）；需证据：触发裁剪的具体 break 日期 + triage verdict + 该段原始 OHLCV | `/shortened/52` |

### 2.3 形态分组汇总

| 分组 | 项数 | 说明 |
|---|---:|---|
| seed 边界（新首日=2021-06-11） | 8 | 新首日正好落在 2021-06 seed floor：与 `rebuild_silver.py:454-469` 的 trim 1（`classify_seed_boundary` 判 corrupt 后按 seed 日截断）形态一致。**这是最可能有系统性解释的一组。** |
| seed 起点被再裁（旧首日=2021-06-11） | 11 | 旧首日本身就是 seed floor，新首日更晚：说明是 trim 2（>6.0 连续性盲扫）在 seed 之后又切了一刀。需要 triage verdict 才能判。 |
| 缩短≥10年 | 3 | 跨度极大（MZTI 1980→2010、OII 1980→2026、PAMT 1994→2005），影响面最大，优先取证。 |
| 缩短1–10年 | 23 | 多数项；逐项需要 break 日期与 triage verdict。 |
| 缩短<1年 | 8 | 影响最小，可最后处理。 |
| **合计** | **53** | — |

### 2.4 一条线索：`trimmed=261` vs `shortened=53`

fixture 全量运行的 SUMMARY_JSON（`logs/full-migration.stdout.log`，本轮 ssh 实读原文）里 `"trimmed": 261`，而与 rev39 比较只有 53 个 symbol 变短。也就是说：**261 个 symbol 在本次运行中被裁剪过，其中 208 个裁到的位置不比 rev39 更晚**（rev39 自己多半也裁过同样的位置）。53 只是「这次比上次裁得更狠」的子集。

这给出一条更省事的取证路径：不必逐个 symbol 从零查，先取 fixture 这一轮每个 symbol 的 `window["reason"]`（staging 阶段就有，`rebuild_silver.py:535-538` 的 regression 记录里也带 `reason` 字段），按 reason 分桶，很可能 53 项会塌缩成 2–3 个原因。**本轮没有跑这个查询**（需要在 Mini 上读 fixture 的 staged window 或重跑一次带 `--failure-output` 的比较），列为待办。

### 2.5 MUNJ（唯一延长项）与交集核对

| 项 | 值 | 来源 |
|---|---|---|
| MUNJ 旧首日 → 新首日 | 2026-08-27 → 2026-08-26（延长 1 天） | `window-manifest-comparison.json` `lengthened[0]`，本轮 ssh 实读 |
| additions / withdrawals | 0 / 0 | 同上 `counts` |
| shortened / lengthened | 53 / 1 | 同上 `counts` |
| 269 与 53 的 symbol 交集 | **0**（本轮 `comm -12` 实算，269 与 53 各自唯一） | inventory 附录 A/B |

MUNJ 延长 1 天不需要决策；记录即可。**不要**把它当作「新逻辑更好」的证据——单日差异同样可能来自输入 as-of 差异。

---

## 3. 容量：已测数字合并表

全部数字来自 `c11caa8` 的 Mini receipt。**本轮抽查了 3 份 receipt 的原始内容，逐个字段与文档对上（含 2 个 SHA256），无不符。**

### 3.1 时间（外盘输出 + 内盘输入，非冷缓存、非 idle host）

| 阶段 | 耗时(s) | 结果（SUMMARY_JSON 原文） | 文档来源 | receipt 抽查 |
|---|---:|---|---|---|
| 复制 13,548 symbols 输入 | 773.162 | `equity_bytes=541,545,366`，`action_bytes=92,119,730`，`triage_copied=false` | EXEC-SUMMARY「全市场性能样本」 | ✅ `logs/input-copy-receipt.json` 本轮实读，字段全同 |
| 首次全量发布 | 1,971.265 | `revision=1, rebuilt=13279, failed=269, trimmed=261, window_regressions=0, unchanged=0` | 同上 / verification L86-88 | ✅ `logs/scenario-receipt.json` + `full-migration.stdout.log` |
| full no-op | 743.799 | `revision=1, rebuilt=0, unchanged=13279, failed=269` | 同上 | ✅ `continuation-receipt.json` + `full-noop.stdout.log` |
| AAPL targeted increment | 624.424 | `revision=2, rebuilt=1, failed=0` | 同上 | ✅ 同上 |
| 损坏 fixture A 后 full | 1,319.281 | `revision=3, rebuilt=0, unchanged=13278, failed=270` | 同上 | ✅ 同上 |
| 恢复 A 后 targeted retry | 652.258 | `revision=4, rebuilt=1, failed=0` | 同上 | ✅ 同上（该段与 13:00 生产 daily 竞争，非 idle） |
| 5 个 SIGKILL 发布边界 | 128.36 | 全通过 | verification L122-125 | ⬜ 未抽查 [unverified]（receipt 在 `se08-architecture-_ab1q7_c/receipt.json`） |
| 最终候选 `38c90bb` 外盘恢复/identity 回归 | 21.84 | 14/14 passed | EXEC-SUMMARY | ⬜ 未抽查 [unverified] |

抽查命令（本轮实际执行）：`ssh macmini 'cat …/logs/{input-copy,scenario,c11-retained-capacity}-receipt.json; grep -o "\"duration_seconds\": [0-9.]*" …/continuation-receipt.json; shasum -a 256 …/full-migration-failures.json …/window-manifest-comparison.json'`。

**这 6 个阶段合计约 6,084s ≈ 1h41m**，但它们是分段测量、内盘输入 + 外盘输出，**不是一次冷缓存生产整轮，也不是 SLA**。

### 3.2 空间

| 指标 | 值 | 来源 | 抽查 |
|---|---:|---|---|
| 最终 current 引用的 artifact 字节 | 775,354,298 B（26,558 个） | `logs/c11-retained-capacity.json` `current_referenced_*` | ✅ 本轮实读，完全一致 |
| rev1–4 去重保留 | 776,044,796 B（26,562 个） | 同上 `retained_union_*` | ✅ |
| 观测时间 | 2026-09-08T05:07:23.720675Z | 同上 `observed_at_utc` | ✅ |
| 内盘可用（05:07:23Z） | 65,011,445,760 B ≈ 60.5 GiB | 同上 `free_bytes.internal_tmp` | ✅ |
| 外盘可用（05:07:23Z） | 7,216,270,278,656 B ≈ 6.56 TiB | 同上 `free_bytes.external_fixture` | ✅ |
| 内盘可用（本轮 13:5x HKT `df -k /`） | 63,181,632 KiB ≈ 60.3 GiB | `ssh macmini df -k /` | ✅ 与 05:07Z 一致量级 |
| 外盘可用（本轮 `df -k /Volumes/DATA_LAKE`） | 7,046,952,064 KiB ≈ 6.56 TiB，容量 49% | 同上 | ✅ |
| receipt 口径 | 「manifest artifact path stat only; no generations traversal」 | `c11-retained-capacity.json` `scope` | ✅ **不含 orphan/scratch/文件系统分配开销** |

**内存**：首轮采样 RSS 峰值 691,488 KiB（进程启动后才开始采样，`run-scenarios-rss.tsv`）；continuation 的 native lifetime physical-footprint peak 为 861.4 MiB。两个指标口径不同，**不可混用**，也不能当作上界。参照：被否掉的全内存方案在 16 GiB 的 Mini 上仅行对象就要 16.005 GiB（verification L221-223）。

### 3.3 容量结论（可写进切换方案的部分）

- 一次全量 Silver 重建的输出足迹 ~0.78 GB / 26.5k 文件；即使保留 4 个 revision，去重后也只多 0.69 MB（因为 rev2/3/4 只改 1 个 symbol）。**外盘 6.56 TiB 可用，空间不是约束。**
- 内盘 60 GiB 可用；输入复制需要 ~0.63 GB scratch（541 MB equity + 92 MB action），`_require_snapshot_capacity`（`rebuild_silver.py:174`）会先做预检。**内盘也不是约束。**
- 真正的约束是**时间与竞争**：全量 ~33 分钟（热、分段、内盘输入），而生产 daily 13:00 HKT、intraday 18:00、coverage 19:00，且三者共用同一把 `locks/lake-io.lock`。见下方未测项。

### 3.4 明确**没有**测量的场景（不给估计值）

| # | 未测场景 | 为什么重要 | 拿到它需要做什么 |
|---|---|---|---|
| 1 | **冷缓存下的真实湖整轮** | 上表全部是热缓存 + 内盘输入。根 `CLAUDE.md` 记录 coverage 冷 exFAT 扫描是 1400–2860s 区间，波动 2x | 在真实 lake 上、冷缓存、维护窗口内跑一次全量，记录 elapsed/RSS/锁持有时长 |
| 2 | **与 coverage repair 的竞争** | coverage 无 timeout，19:00 启动；Silver 与它抢同一把 lake lock | 让两者按真实时刻表并发跑一次，记录 `outcome='blocked', blocker='lake_lock'` 是否出现 |
| 3 | **锁等待预算** | 已知 lane budget 机制存在，但 Silver 在新流程下持锁多久没有测 | 记录 Silver lane 的实际持锁时长 p50/p95，与 `LANE_BUDGET_S` 比 |
| 4 | **读者（Apex）在发布期间的影响** | 发布是 `os.replace` 提交，但 carry-forward 会 copy+verify 26.5k 文件；这期间 Apex 的 30s 轮询会做什么没有测 | 发布进行中对 Apex 打一组 `/bars` 请求，记录延迟与错误码 |
| 5 | **断电持久性** | 只做过 5 个 SIGKILL 边界（进程级），**不是掉电** | 需要真实断电/强制关机演练，或明确接受不测 |
| 6 | **单盘故障持续可用** | 外盘是单个 exFAT 卷，无冗余 | 需要明确「盘挂了怎么办」的答案；当前没有 |
| 7 | **备份恢复演练** | 从未演练过从备份恢复 Silver/Bronze | 需要一次端到端 restore 计时 |
| 8 | **候选代码的生产正常运行** | 13:00 跑的是旧生产代码 `23e1de2`（本轮 `readlink ~/market-warehouse/current` 实测），不是候选 | 切换后至少观察一次完整定时运行 |

以上 8 项**全部保持开放**，不写估计数字。

---

## 4. Open decisions for the user（≤5，需要你拍板）

1. **269 个失败是否作为已知基线放行切换？** 证据：它们已经不在生产 rev39 里（membership 完全一致，additions=0/withdrawals=0），隔离是 per-symbol 且有全灭保护。放行意味着切换后每晚 SUMMARY_JSON 稳定出现 `failed=269`，需要把它写进告警基线。**建议：放行 + 基线化。**
2. **53 个窗口缩短是否接受「默认保留旧窗口 + 常驻 `window_regressions=53` 告警」？** 代码路径决定了在生产血统里它们会被 carry-forward 挡回去（§2.1）。接受的话，切换前不需要逐项定案；**明确不接受**的话，需要先做 §2.4 的 reason 分桶取证，会额外花时间。**建议：接受，逐项定案排到切换之后。**
3. **是否批准在 Mini 维护窗口跑一次冷缓存全量真实湖 Silver 重建（只写 disposable 输出，不切 current）？** 这是关闭 §3.4 第 1/3 项的唯一办法，代价是一个维护窗口 + 约 1–2 小时（冷缓存下可能更长，**不给保证**）。
4. **断电、单盘故障、备份恢复（§3.4 第 5/6/7 项）是接受为已知未测风险，还是切换的前置条件？** 目前三项都没有任何证据；把它们当前置条件会显著推迟切换。
5. **RJF 是否授权寻找替代来源并准备替换候选？**（仅准备，不执行替换。）RJF 是 S&P500 成分股，是 269 里唯一的字节级损坏；canonical 与 fixture 字节 SHA 相同，说明不是本次复制造成的，但**原始损坏原因未知**。

---

## 5. Unknowns kept open（不填、不猜）

- **322 项的「最后有效数据」全部未知。** 输入覆盖日期、旧 manifest 记录、fixture 发布成功，三者都不能替代对文件/语义/reader 的验证。
- **269 里没有任何一项判定了「哪边数据正确」。** 本文件只给了「什么证据能定分」。
- **C 类 61 项的具体冲突币种对未知**——receipt 未记录，必须回 `/failures/<i>/active_actions` 取。
- **D 类 4 项的具体除息日与金额未知**——同上。
- **RJF 原始损坏原因未知**，不给历史因果结论。
- **53 项的 `window["reason"]` 未取**（§2.4），因此「seed trim vs continuity trim」的分组是按日期形态推断，不是代码输出的原因字段 [unverified]。
- **§2.1 的生产血统行为是代码推断**，没有在真实 rev39 上实跑验证 [unverified]。
- **`se08-architecture-_ab1q7_c`（SIGKILL 128.36s）与 `se08-20260908T1315-38c90bb`（14 PASS / 21.84s）两份 receipt 本轮未抽查** [unverified]。
- **RJF 的 SHA256 `54e59feb…4051` 本轮未复读** [unverified]，仅沿用两份既有文档的一致记录。
- **§3.4 全部 8 个场景没有任何测量值**，不接受用类比或经验值填充。
- **容量口径缺口**：receipt 明写只 stat 了 manifest 引用的 artifact，**不含 generations 目录里的 orphan、scratch 残留与文件系统分配开销**；真实磁盘占用高于 776 MB，高多少未知。
- **Apex 生产镜像 digest 是否仍是 `sha256:3d3613d3…ee6d` 未在本轮复核** [unverified]（EXEC-SUMMARY 记录为 03:38Z 观测）。

---

## 附：本轮实际执行的核验命令

| 目的 | 命令 | 结果 |
|---|---|---|
| 类别计数 | `awk -F'\|' '/^\| F[0-9]{3} \|/{...}' inventory.md \| sort \| uniq -c` | 198 U / 61 C / 5 S / 4 D / 1 R = 269 ✅ |
| 交集核对 | `comm -12 <(F symbols) <(W symbols)` | 0 ✅（与 inventory 声明一致） |
| receipt 哈希 | `ssh macmini shasum -a 256 …/full-migration-failures.json …/window-manifest-comparison.json` | `7e5b2c47…f26ba` / `b9b5af2e…4bc39`，均与 inventory 一致 ✅ |
| 容量 receipt | `ssh macmini cat …/c11-retained-capacity.json` | 775,354,298 / 776,044,796 / 26,562 / free 65,011,445,760 + 7,216,270,278,656 ✅ |
| 阶段耗时 | `ssh macmini grep -o '"duration_seconds": [0-9.]*' …/continuation-receipt.json` | 1971.265 / 743.799 / 624.424 / 1319.281 / 652.258 ✅ |
| 各阶段语义 | `ssh macmini cat …/{full-migration,full-noop,aapl-targeted-increment,fixture-corruption,fixture-corruption-restored-retry}.stdout.log` | 5 条 SUMMARY_JSON，与文档全部一致 ✅ |
| 生产现状 | `ssh macmini readlink ~/market-warehouse/current` | `releases/23e1de284f343e357f97100cfe83fb48ac4be9ec`（旧生产代码，非候选） |
| 生产 Silver | `ssh macmini python3 -c "json.load(current.json)"` | `revision 39 / members 13279 / artifacts 26558` |
| 生产磁盘 | `ssh macmini df -k /Volumes/DATA_LAKE /` | 外盘 6.56 TiB 可用 / 49%；内盘 60.3 GiB 可用 |

本文件只读、未修改任何生产数据、未 promote、未触碰 IB Gateway。

# Livewire 完整性改造与发布交接

接手者：Claude Code。整理时间：2026-09-08 13:32 HKT。

## 执行结论

**候选实现已经交付，生产发布尚未完成。请接手剩余发布条件、受控切换和正常运行验收，不要重做整个项目。**

目标是：同类损坏、中断、并发写入再次发生时，系统保留最后有效发布、准确报告影响范围，通过正常流程恢复；局部失败不拖停无关工作，不再靠补哈希、收编孤儿文件、删除重跑或临时线上补丁。

用户明确修订的边界：稳定的是 **Livewire → Apex 数据交付契约**。Apex 只实现必要的 snapshot 读取适配和明确结果/错误；不是冻结 Apex 对外 API，也不是完成其订阅、指标、缓存、信号与事件队列内部重写。后者留给后续重构。这项修订优先于旧文档中可能扩大 watcher/reseed 验收范围的措辞。

用户本轮要求打包交接，由 Claude Code 继续；Codex 到此停止实施。没有执行合并、release、promote 或 canonical 数据修复。用户询问过能否 promote/release，但尚未批准一份具体生产切换和数据替换清单。

## 当前工作位置与精确版本

| 项目 | 执行工作区 | 分支 / PR | 当前版本与状态 |
|---|---|---|---|
| Livewire | `/Users/chenxi/projects/livewire/.worktrees/systematic-integrity` | `fix/systematic-integrity` / https://github.com/moremeds/livewire/pull/125 | `a4003d3dc39d177db01d5e4e84ba83db32a65416`，草稿、未合并、工作区干净 |
| Apex | `/Users/chenxi/projects/apex/.worktrees/livewire-snapshots` | `fix/livewire-snapshots` / https://github.com/moremeds/apex/pull/161 | `ae57f93cfb6f772277c6a309f5ae428dd1ce3298`，草稿、未合并、工作区干净 |

Livewire 最终实现/测试版本是 `38c90bba46f668cf2d615c90f867a3e1e9220a09`；`a4003d3` 仅追加三个文档文件。性能实验固定在更早的 `c11caa8d96b8416c1a5256f71179639618461692`，不能冒充最终版本完整性能复测。

原工作区有用户未提交内容，必须保留：

- Livewire `/Users/chenxi/projects/livewire`：`tasks/lessons.md`、`tasks/todo.md` 修改，以及两个未跟踪的完整性计划。
- Apex `/Users/chenxi/projects/apex`：`.serena/project.yml`、`uv.lock` 修改，以及若干已经暂存的文档删除。完整状态在包内 `evidence/current-state.json`。
- 不得用 restore/reset/clean 或分支切换丢弃上述内容。继续使用现有执行 worktree；Apex 默认分支是 `master`，Livewire 是 `main`。

## 已完成的行为

1. Canonical 写入入口统一协作边界，读改写锁、候选验证和原子替换；失效候选不能覆盖有效旧文件。
2. Silver 发布不可变 generation，完整 manifest 的 current 替换是提交点；旧读者固定的有效版本继续可读，过期构建不能提交。
3. 按真实依赖隔离 symbol/provider/bucket/catalog/通知故障；保留明确失败、最后有效数据和恢复条件。
4. DuckDB catalog 的 staging/发布互斥，失败保留旧目录；读取固定已提交 Silver manifest。
5. 手工修复具有写前备份、候选/源/action 哈希、可恢复逐项状态；回滚比较当前与已记录 applied 哈希，拒绝覆盖后来更新的数据。
6. Apex 请求级 adapter 固定 snapshot，验证成员、文件路径、哈希及覆盖范围；daily/intraday 实际 producer→consumer 互操作通过。
7. 外盘测试发现并修正 rollback 误读 AppleDouble `._*.json` 元数据。普通损坏 JSON 仍显式失败。新测试不要求 macOS 管理的元数据字节保持不变。
8. 269 个数据失败和 53 个窗口差异已逐项编目，列明证据指针、核验动作与解除条件；这不是数据已经修复。

## 不可降低的需求

- Bronze Parquet 是系统记录源；DuckDB 是查询/目录层，不引入第二份 canonical 写入路径。
- 单 symbol 故障隔离该 symbol；IB 故障不阻断 CBOE/Massive 等无关来源；catalog/邮件失败不阻断 ingestion。
- 全局提交完整性或被选入 manifest 的 artifact 校验失败，整次提交必须拒绝。不可验证的旧 snapshot 不可继续冒充有效数据。
- 任务结束、提交成功、数据健康、新鲜度、消费者读到什么，是不同结论。
- warning 必须说明：发生时间与动作、影响的 symbol/timeframe/session、证据、最后有效数据、正在执行的有限动作、人工下一步、解除/升级条件；同一故障聚合，影响变化或恢复时通知。
- 恢复走正常发布与相同验证规则；不得刷新旧 manifest 哈希来“认可”现有字节，不收编未经验证的孤儿文件。
- 无相关修改不反复跑全市场实验，不靠增加 timeout/并发来掩盖成本，也不引入新服务或通用框架。
- 单机/单盘故障持续可用、断电持久性、备份恢复演练都未被证明；不可写成已解决。
- 生产只通过 `ssh macmini` 验证，不能拿 MacBook 的本地湖替代。禁止管理或重启 IB Gateway、下单、无授权删除数据。
- 用户数据删除/覆盖、丢弃未提交内容等须遵守 AGENTS 的确认规则；尚无本次具体数据变更的确认口令。先把候选、差异与回退材料准备到可评审，再请求最终批准。

## 已有验证证据

| 证据 | 结果及边界 |
|---|---|
| Livewire 最终本地完整套件 | `uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning`：2,688 passed，95.01%，44.60s；源集是配置中的 `clients` 与 `livewire_scripts`，不能用薄 `scripts` wrapper 替代 |
| 最终 PR-head Linux CI | `a4003d3`，runs `34189986710` / `34190023718` 均成功；后者 2,688 passed、95.01%；Node alerts 25 passed/0 failed |
| 静态检查 | Ruff/format、lock 检查通过；Pyright 0 errors、25 warnings |
| Mini 真实文件系统崩溃边界 | c11 发布代码的 fsync/flock 与 5 个实际 SIGKILL 边界通过，128.36s；不是断电测试 |
| Mini 最终恢复/identity 回归 | 精确 `38c90bb` 外盘 14/14 passed，21.84s；生产 daily 并行，仅作为正确性证据 |
| Apex | focused 114 passed；CI integration 129 passed/1 skipped；lint/type 失败，unit CI 被跳过，不能称全绿 |
| Apex 本地更广测试 | unit 2,091 passed/79 skipped/2 Yahoo 网络基线失败；integration 129 passed/1 skipped；本地 mypy 729 文件通过。与 CI 工具版本不同 |
| 真实互操作 | Livewire `062caa6` → Apex `ae57f93`：daily 与 14:30 UTC intraday 均 close=5、volume=200、日期/revision 正确；后续变更只涉及 rollback 元数据与测试，不涉及 adapter/producer |

Apex 红门禁的已核实位置：isort 9.0.1 拒绝未改动的 `backtest/execution/parallel.py`、`order_matching.py`；CI mypy 2.3.1 / Optuna 5 在未改动 `backtest/optimization/bayesian.py:73` 报 direction 类型错误。本地版本不同。接手后做最小必要门禁修复，不能趁机重写 Apex 内部或私自新增/升级依赖；若确需改依赖/锁文件，先明确范围。GitHub Claude review job 的 success 不代表它完成了独立代码审查，本轮已知该 job 有权限拒绝。

### 全市场性能样本（c11，测试副本）

| 阶段 | 耗时 | 结果 |
|---|---:|---|
| 复制 13,548 symbols 的 daily/CA 输入 | 773.162s | 内盘测试输入；复制持有新输入锁，但旧生产 writer 不遵守新锁，不能称一致生产快照 |
| 首次全量发布 | 1,971.265s | rev1，13,279 members / 26,558 artifacts，269 failures |
| full no-op | 743.799s | rev1，rebuilt0，同样269 failures |
| AAPL targeted increment | 624.424s | rev2，rebuilt1 |
| 只损坏 fixture A 后 full | 1,319.281s | rev3，只有 A 被移除；13,278 members，270 failures，exit0 |
| 按备份字节恢复 A 后 targeted retry | 652.258s | rev4，rebuilt1；rev1/2/4 成员完全相同 |

所有阶段成功完成其预期语义；exit0 不是全市场健康。增量仍校验完整保留 artifact 集，不能承诺常数时间。仅这组阶段总计约1小时41分，避免无意义重复。13:00 HKT 生产 daily 真正启动，恢复重试后半段有竞争；不是 idle-host 性能。

首轮采样 RSS 峰值 691,488 KiB（启动后才采样）；continuation native sample 的 lifetime physical-footprint peak 是861.4 MiB，两种指标不可混用。最终 current 引用775,354,298B；rev1–4去重保留776,044,796B /26,562 artifacts；不含 orphan/scratch/文件系统分配开销。05:07:23Z 空闲内盘65,011,445,760B、外盘7,216,270,278,656B。冷缓存正常整轮、coverage repair 竞争、完整锁等待/读者影响预算仍未闭环；13:00/18:00/19:00触发间隔不是用户承诺的 SLA。

## 未完成事项与 release 风险

1. **Apex CI 红门禁**：先处理上述具体失败并验证兼容组合，不扩大内部重构。
2. **数据决策**：198 unknown price basis、61 股息/报价币种矛盾、5 split冲突、4 dividend约束失败、1 RJF坏footer；53 shorter windows是旧rev39与fixture rev1的manifest元数据差异。不要先验认定哪边正确。MUNJ另有1项窗口延长。269和53无symbol重叠。
3. **269不必全部修复才能发布**：但必须解释每类隔离策略、消费者受影响范围、恢复动作；53项窗口变化需要逐项证据和保留/修正决策。RJF原始损坏原因未知，不得强行给历史因果结论。
4. **容量与切换**：补最少必要的未验证场景，形成具体版本/维护范围/空间/中断/回退清单，不直接 promote。`promote` 会实际切换 `current`，不是只打包。
5. **正常生产验收**：新候选未部署，13:00启动的是原生产代码，不能拿来证明候选正常运行。

### 已观察的生产布局（历史快照，行动前重新核对）

- Livewire 初始部署/主干 `23e1de284f343e357f97100cfe83fb48ac4be9ec`；生产 `~/market-warehouse/current`，自动 promoter 12:30 HKT；daily13:00、intraday18:00、coverage19:00。
- Apex 03:38Z观察：`apex-deploy-api-1`，Compose `/Users/moremeds/apex-deploy/compose.yml`；OCI revision `905cab6b562af1af87ad821e109e91f24958465e`；镜像 digest `ghcr.io/moremeds/apex-api@sha256:3d3613d38f10a8fcc3e8934c60d9c040218b462711bfabac20a09bdb3656ee6d`。本轮未验证重新拉取该digest。
- Bronze/Silver/delisted只读挂载；adjusted读取轮询30秒。shared `xenon-watchtower-1` 每60秒检查，Apex opt-in；stable tag发布会更新latest。不能停共享updater来影响其他应用；应只控制Apex的image/update资格。
- 先验证初始immutable snapshot并部署兼容reader，再启用writer；根据维护窗口协调旧reader在这期间的行为。不能靠“先合并但暂不手动部署”阻止自动promoter激活。
- 数据回退必须发布新的单调递增manifest，引用保留且校验通过的历史artifacts；不能直接回退旧mutable writer或倒退revision。不得prune旧进程仍使用的release。

## 接手执行顺序（停在验收条件，不重复已完成工作）

1. **刷新状态**：读项目AGENTS/CLAUDE和本包计划，核对worktree、PR heads/CI、Mini current/容器/活动writer。验收：差异明确，不以本快照当实时状态；保留全部无关dirty内容。
2. **关闭Apex发布门禁**：最小修复lint/type并跑相关与必需CI；核对真实producer→reader兼容。验收：精确候选组合全绿，无引入新的内部重构。
3. **完成数据/容量判断**：沿322项清单做有针对性的证据核验，确定53窗口差异和269隔离策略；只补缺失容量/竞争场景。验收：有清晰影响、恢复条件和可行窗口，未知仍显式保留。
4. **准备并评审具体切换方案**：列明精确SHAs/image、调度与自动更新保护、旧writer退出、初始snapshot、部署顺序、停止条件、数据回退及用户数据操作。验收：方案可直接执行且可回退；需要的最终批准在这一刻集中请求，不能在准备工作之前停下来泛问。
5. **获批后合并与发布**：通过PR合并，核对合并SHA CI，保护旧release与输入，按兼容顺序release/promote并验证真实adapter与manifest/hash。验收：代码、运行进程、数据版本和消费者一致；不擅自停止Gateway或删除数据。
6. **正常运行关闭**：观察至少一次新版本正常定时运行，按lane/symbol/date确认质量、新鲜度、catalog及告警恢复。分别给出代码/CI、外盘故障行为、部署、生产结果四个结论；未闭环项不能改成通过。

权威依赖图：SE00 → {SE01,SE02} → SE03 → SE04 → {SE05,SE06} → SE07 → SE08 → SE09 → SE10 → SE11。当前候选SE00–07已完成，SE08部分、SE09未全绿、SE10/11未执行。

## 包内阅读顺序与原始证据

以下路径均相对于本 worktree 根目录；接手入口是根目录 `CLAUDE_HANDOFF.md`。

1. 本文件、根目录 `CLAUDE_HANDOFF.md`。
2. `docs/plans/2026-09-08-datalake-integrity-implementation.md`：完整需求、故障矩阵、入口责任、SE00–11验收。
3. 同目录 `2026-09-08-silver-atomic-publication.md`：稳定的数据/发布契约；`2026-09-08-datalake-integrity-end-state.md`：目标与时间线。
4. `docs/codex-handoffs/2026-09-08-systematic-integrity-verification.md`：精确验证与切换要求。
5. 同目录 `2026-09-08-integrity-recovery-inventory.md`：全部269+53逐项清单。
6. `tasks/todo.md` 与 `docs/codex-handoffs/claude-release/evidence/`。计划历史数字以本执行摘要和更新后的验证文档为准；源仓库实际状态仍须刷新。

Mini详细receipt保留原位，不复制全湖：

- `/Volumes/DATA_LAKE/livewire/disposable/se08-20260908T1122-c11caa8/logs/`：输入复制、全量、continuation、269failures、53comparison、容量记录。
- `/Volumes/DATA_LAKE/livewire/disposable/se08-architecture-_ab1q7_c/`：fsync/flock和5SIGKILL记录。
- `/Volumes/DATA_LAKE/livewire/disposable/se08-20260908T1315-38c90bb/`：最终14PASS，`receipt.txt`、`pytest-output.txt`、`command.txt`。archive SHA256 `b7058cf59e204328948dacd3dafd36c14557bd870c7be03fd8fa8f7b7e8c7f69`。
- 旧的062/52失败fixture保留，不能清掉来隐藏失败历史。

当前交接 worktree 包含 Livewire 完整代码、需求、状态与证据摘要，不含生产数据或凭据；Apex 代码在上列独立工作区。所有引用观察均带日期或版本；未执行额外生产刷新，不假定主机目前仍空闲。

# Livewire 系统性数据完整性实施计划

状态：执行中。用户已批准本计划实施，包括所列 Livewire 存储与 Apex 消费者改造；
具体生产切换和数据替换仍在候选版本可评审后按 SE-10 单独批准。

2026-09-08 暂停后恢复的范围修订（用户明确批准）：稳定的是 Livewire → Apex
的数据交付契约，而不是冻结 Apex 对外 API 或完成 Apex 内部重构。
Livewire 负责可验证的原子发布、manifest/snapshot、数据及失败语义；Apex 数据
适配层负责固定 snapshot、按契约读取并传递明确结果/错误。适配层之后的指标、
缓存、订阅刷新、信号状态与事件队列属于 Apex 内部，留给后续重写。
SE-06/07/09 的 Apex 工作仅保留契约接入必需的最小实现与契约测试；不得把
当前布尔可用性开关写成原子 reseed 或旧排队信号隔离已完成的证明。
Livewire 完整性与故障隔离验收仍保留。Apex 内部端到端信号验收明确延期，
不以此宣称新组合可直接无条件上线；SE-10 仍需具体兼容与切换方案。

执行基线（2026-09-08）：Livewire `origin/main` / mini release 均为
`23e1de284f343e357f97100cfe83fb48ac4be9ec`；工作区
`.worktrees/systematic-integrity`，分支 `fix/systematic-integrity`。
原 checkout 的两个未提交计划及 `tasks/todo.md`、`tasks/lessons.md` 修改已复制
至执行工作区，原件保留。Apex `origin/master` 为
`905cab6b562af1af87ad821e109e91f24958465e`，独立工作区
`/Users/chenxi/projects/apex/.worktrees/livewire-snapshots`。
Apex 原 checkout 的 `.serena/project.yml`、`uv.lock` 和已暂存文档删除均未改动。
mini Apex checkout 为 `54b2676`（存在 `.serena` 修改），实际容器标签为 `905cab6b`，
镜像 ID `sha256:3d3613d38f10a8fcc3e8934c60d9c040218b462711bfabac20a09bdb3656ee6d`。
运行容器将外置盘的 bronze、bronze-delisted、silver 以只读方式分别挂载到
`/data/livewire`、`/data/livewire-delisted`、`/data/livewire-silver`；metadata 路径
可达性继续核对。01:05Z 检查未发现活动 Livewire worker。

## 目标与范围

同类损坏、中断、并发写入再次发生时，系统保留最后有效发布、准确报告受影响范围，
并通过既定流程恢复，不再靠临时改代码、补哈希、收编孤儿文件或删文件重跑。

本计划是唯一执行清单；[最终形态与实际时间线](2026-09-08-datalake-integrity-end-state.md)
提供目标及证据，[Silver 发布契约](2026-09-08-silver-atomic-publication.md)提供存储与
消费者设计。后续发现不一致时先修改相应契约，不在实现里另建一套行为。

包括：写入控制、发布完整性、消费者快照、可信验收、运行容量与安全切换。
不包括：新增数据库/服务/依赖、通用事务框架、自动垃圾回收、Gateway 管理、
不相关性能改造。生产数据只检查 Mac mini，不用 MacBook 的湖替代。

基线：本对话已核对 PR #124 / release
`23e1de284f343e357f97100cfe83fb48ac4be9ec`；实施开始时必须重新核对。
既有“CI success”不能作为验收完成：Linux 原始覆盖率 94.89%，低于要求的 95%。
RJF 初始损坏原因仍未知；不能把一项防护修复写成历史因果已证实。

## 必须成立的运行规则

1. 所有 canonical 写入入口遵守同一协作边界，包括定时、coverage repair 和手工命令。
2. 临时文件验证失败不覆盖有效旧文件；首次写入失败不留下可用目标。
3. Silver artifact 不可变，完整 manifest 的 current 替换是唯一提交点。
4. 一次 adapter 读取固定一个 manifest；不存在固定路径补读旁路。
   Apex 内部 reseed 事务边界在其后续重构中实现，不作为本候选的保证。
5. 单 symbol 质量问题按证据隔离；只有仍然有效的旧数据才能沿用，并显示真实日期。
   被选入 manifest 的 artifact 校验失败或事务不完整，则中止整次提交。
6. 提交成功、任务完成、数据健康与新鲜度是不同结论，不能互相代替。
7. 所有恢复必须通过正常发布与同一套验收规则，不设“修复模式”绕过验证。
8. 故障按真实数据依赖传播；无关 provider、symbol、lane 和已验证的读取继续工作。
9. warning 必须包含实际影响、证据、下一步和解除条件；不能只重复计数或“任务失败”。

## 故障隔离与告警契约（硬验收项）

### 避免把局部故障升级为系统单点故障

唯一数据权威和唯一提交点用于保证一致性；不能因此让每个异常阻断全部功能。
先在 SE-03 写出故障域/依赖矩阵，再实现与测试。必须覆盖：

| 故障 | 应限制的影响范围 | 必须继续或保留的能力 |
|---|---|---|
| IB 不可用 | 依赖 IB 且无已验证替代来源的 lane | CBOE、Massive 等无关来源继续；已验证历史可读 |
| 单 symbol 损坏/语义冲突 | 该 symbol 及实际依赖它的派生结果 | 其他 symbol 正常处理；受影响项不伪造成功 |
| Silver 新 candidate/事务失败 | 拒绝这次提交 | 有效旧 snapshot 保持可读；不改其哈希来适配新文件 |
| catalog 构建失败 | 本次目录更新及依赖其新鲜度的查询 | 保留旧目录并标时点；canonical 数据不因此被删除或修复 |
| Apex watcher/reseed 失败 | 对应订阅或新 revision 接受过程 | Livewire ingestion 不受影响；旧读者按明确的新鲜度要求处理 |
| 邮件发送失败 | 该通知渠道 | 数据处理和本地 ledger 继续；投递失败可从现有状态入口查到 |
| 某个 writer 卡住/崩溃 | 受其锁与数据依赖影响的工作 | 可取消或安全退出，OS 释放锁；无关读取保持可用 |

共享湖锁可能形成全局等待点。必须区分需要一致性保护的读改写与不必持锁的
网络等待/准备阶段，测量真实持锁时间。若无关 lane 因一个卡住的 writer 一直
无法推进，则隔离验收不通过：修正锁范围或安全取消流程，不能仅提高 timeout，
也不能为继续运行绕过一致性保护。协调机制复用现有实现，不先建设调度服务。

不可验证的旧 snapshot 不能继续服务；此时阻断确实依赖它的读取，并指出范围。
“继续运行”不能以悄悄使用错误数据为代价。

单台 Mac mini、单外置盘仍是物理单点。当前单机架构不能承诺硬件故障下持续服务。
SE-01 必须记录现有备份、恢复来源及其新鲜度；没有演练的恢复能力不能算已解决。
若要求主机/磁盘故障时仍在线，需要另行确定恢复时间/数据损失目标和冗余方案，
本计划不私自新增第二台机器、复制服务或第二份 canonical 写入系统。

### 每条 warning 必须言之有物

使用现有 ledger、status、digest 和通知路径。先复用已有字段，缺失证据明确写未知；
需要扩展协议时列明具体变化。每条 warning 至少回答：

- **发生了什么、何时发生**：具体失败动作、首次/最近发生时间、run/release/revision。
- **影响什么**：命名 symbol/timeframe/目标 session 或明确受影响范围；最后有效数据时点。
- **依据是什么**：实际错误、验证结果和可追溯证据位置；原因未证实时不写成确定根因。
- **系统正在做什么**：等待哪个依赖、已执行何种有限重试、是否沿用有效旧数据。
- **需要谁做什么**：下一条可执行动作或明确说明当前无需人工行动及何时重新检查。
- **何时解除/升级**：恢复需通过的同一验收条件；升级基于影响扩大或截止时间受威胁。

同一故障持续存在时聚合首次/最近时间和次数；重大影响变化、需要行动或恢复时通知。
不能每次运行都发一封内容相同的邮件，也不能因为未重复发送就从状态里消失。
正常无变化、已解释的无交易和仍在预期范围内的锁等待，不单独制造 warning。
具体新鲜度与等待阈值从业务截止时间和测量确定，不能任意写一个天数。

格式示例（不是新的实时观测）：

> Catalog 本次构建未提交：RJF/1d 校验失败，旧目录保留，最后构建时间为 <时间>。
> 影响：本次目录新鲜度未更新；RJF 数据不可验证。证据：<run / 错误 / 路径>。
> 自动处理：已拒绝发布，未修改源文件。下一步：核对该文件来源并按已批准恢复流程处理。
> 解除条件：源文件验证通过且完整 catalog 重建成功；恢复后更新本条事件。

## 依赖图

```text
SE-00 → {SE-01, SE-02} → SE-03 → SE-04 → {SE-05, SE-06}
                                                 ↓
                          SE-07 ←───────────────┘
                            ↓
                          SE-08 → SE-09 → SE-10 → SE-11
```

SE-05/06 的存储与 Apex 范围已随本计划实施批准；SE-10 开始前仍需具体切换方案批准。
这些是执行条件，不需要为了编写或评审本计划重复确认。

## 任务与验收

### 实施中的入口与锁责任（2026-09-08）

| 入口 / 最终写入者 | 一致性边界 | 失败与恢复 |
|---|---|---|
| daily、robust、手工 Bronze ingest → `BronzeClient` / `IntradayBronzeClient` | 现有 `symbol_lock` 覆盖读改写；日线取得 equity input 共享锁，再取得稳定目录锁、文件锁 | 完整解码、fsync 后替换；坏临时文件不能覆盖旧文件 |
| corporate-actions → `CorporateActionStore` | corporate_action input 共享锁、目录锁、文件锁 | 与 Silver 输入复制互斥；源网络等待不持有全湖锁 |
| CBOE → `fetch_cboe_volatility` | 同一 symbol 锁覆盖读取、合并和 `publish_parquet` | 保留有效旧文件，其他来源不受 IB 故障影响 |
| Massive daily/minute ingest | 各自 cursor 独占整个操作；raw 每日期独占发布，共享锁仅保护读者打开文件描述符 | 同流重入明确失败；raw 交换可恢复；桶或 symbol 失败仍继续无关桶，并返回不完整结果 |
| coverage repair → 正常 `flatfile-ingest repair` | 复用 minute cursor 所有权；活动写入时报告 deferred | 同一日期只 repair 一次；子进程取消会清理整个进程组 |
| `rebuild_silver` | 按 corporate_action → equity 顺序独占输入锁复制到临时磁盘；释放后逐 symbol 计算；Silver revision 锁保护提交 | baseline 前进则拒绝过期构建；新文件不可变，current 最后替换；保留旧读者与失败残留 |
| DuckDB coverage build | 目标数据库 `.publish.lock` 覆盖 staging 清理、构建及替换；每连接固定 Silver manifest | 失败保留旧目录；并发构建不能移除另一个构建的 staging |
| OTC / universe archive | 统一 `archive_symbol`；equity input 独占锁、稳定 symbol 目录独占锁 | 同文件系统整体 rename；已有归档不覆盖；不执行跨盘 copy/delete |
| filename migration | input 共享锁、目录独占锁、新旧文件锁 | 在锁内复核双文件冲突；旧 writer 完成后才 rename |
| 手工 price-basis migrate / repair / rollback | migration 在 symbol 锁内读改写；repair/resolve 按 action → equity 顺序取得与复核哈希；写前持久保存原始备份、候选哈希和逐项状态；两类 rollback 复用精确字节验证/fsync/替换 | 输入变化拒绝旧候选；中断重试保留原始备份；回退比较当前与已记录的 applied 哈希；旧 sidecar 缺少该哈希时拒绝覆盖，已恢复到原始字节时仅作幂等确认 |
| Shepherd repair | 已有 symbol 锁、候选完整验证、当前/源/备份哈希比较、fsync 与替换 | 沿用既有实现；`shepherd_universe` 的 copy 是临时 preflight，不是 canonical 写入 |

调度器不再持有跨来源全湖锁。Silver 的输入复制不覆盖网络等待、调整计算或
Silver 输出；intraday 和其他资产也不受 equity 日线输入锁影响。进程超时、取消
和主进程非零退出清理同一子进程组，防止后台后代继续持有写锁。

本轮已运行真实子进程检查：Silver 五个发布阶段 SIGKILL 后的旧/新完整快照与
重试、取消/非零退出后代锁释放、catalog staging 互斥、旧 filename writer 与
迁移互斥。它们使用临时数据；不能替代 Mac mini 文件系统与正常调度验收。

消融结果：删除无运行调用者的全湖锁 helper；Apex 的订阅/信号改动已从候选
撤出并保留可复原备份。保留的共享原语各有具体作用：input 锁保护成对冻结输入，
目录锁保护归档与文件写入，manifest resolver 阻断旧固定路径补读，raw 日期交换
helper 被 daily/minute 两个真实 store 共用，精确 Parquet 恢复 helper 被两类
rollback 共用以保留原始字节并在替换前验证。没有新增服务或依赖。

### SE-00：隔离工作、固定证据

`depends_on: []`

- 重新核对 Livewire/Apex 的 main、工作区改动、部署目录、运行进程与消费者根路径。
- 在独立 worktree 开发，记录初始 SHA 和已有改动；保存当前运行证据，不触碰活动湖。
- 复核实际 launchd 和全部写入程序，不把 plist example 当作已安装配置。

交付：本计划中的基线记录、工作区与文件所有权。
验收：代码、部署、进程和数据路径分别可定位；未知项明确记录。

### SE-01：完整追踪读写入口与故障链

`depends_on: [SE-00]`

- 从 daily、intraday、coverage、手工 ingest/repair/rebuild、迁移/回退命令追到实际
  文件替换；搜索共同函数的全部 caller，不按命令名推断读写性质。
- 为每个入口记录：输入、输出、锁持有者、子进程、发布步骤、退出/恢复行为。
- 特别覆盖 coverage 的自动 repair 和手工 rebuild-silver 的湖锁旁路；
  检查 universe-refresh 对运行中输入集合的影响。
- 保存 RJF 损坏证据并关联可能写入者；不能从时间重叠直接推断唯一原因。
- 同时登记 provider、writer、catalog、consumer、通知与物理存储的故障域，
  记录实际备份/恢复证据；标出会把局部失败扩散为全局失败的位置。

主要范围：`scripts/livewire_ingest.py`、`scripts/livewire_store.py`、
`livewire_scripts/run_daily_update_job.py`、`sync_runner.py`、`coverage_report.py`、
`rebuild_silver.py`、`clients/parquet_io.py` 及查实的各 publisher。

交付：追加到本计划的入口矩阵和已证实缺口，不另建永久登记系统。
验收：每条实际写入路径有归属；剩余旁路或未知入口会阻止后续安全结论。

### SE-02：恢复可信的跨平台验收门禁

`depends_on: [SE-00]`

- 复现覆盖率数值、舍入、退出码不一致；统一门禁判定，不降低阈值或扩大排除范围。
- 去掉 quality_flags 的 Linux 跳过，并按实际复现修复：进程 monotonic
  时钟小于去重窗口时，默认零时间戳错误地抑制首次告警；缺失记录必须独立判断。
- 用隔离的低覆盖率样例证明实际 CI 命令退出非零；用真实测试确认正常路径通过。

主要范围：`pyproject.toml`、`.github/workflows/ci.yml`、`tests/test_quality_flags.py`
和查实的导入接缝。只修改复现所需文件，不顺带重构测试系统。

验收：Linux 与 macOS 的必需测试可执行；低于 95% 确实失败；不依赖供应商网络。

### SE-03：固定统一写入契约

`depends_on: [SE-01, SE-02]`

- 根据入口矩阵确定现有湖锁的唯一获取责任：谁保护输入读取直到提交，谁只负责
  调度与等待。明确父子进程关系，避免父进程持锁等待再次取锁的子进程。
- 复用现有文件锁与湖锁，不加第二套并发协议。不得仅凭环境变量自称“已经持锁”。
- 明确等待、超时、取消、重试和 blocked 的语义；等待时间与执行时间分别记录。
- coverage 对尚在写入的目标范围应等待/报告未完成，不立即启动竞争性修复。
- 给出锁顺序及真实入口的回归场景，再进入实现；若需要新的公开参数或数据协议，
  在这里列出具体差异，不偷偷扩展。
- 固定上述故障域/依赖矩阵及 warning 契约；对单个依赖失败明确哪些工作必须继续。

交付：本计划中的唯一锁责任表、失败语义及拟修改函数清单。
验收：能逐项解释所有入口及嵌套调用，没有无限等待、双重取锁或安全绕行。

### SE-04：落实共同写入边界与原子单文件写入

`depends_on: [SE-03]`

- 在共同边界修复已证实的控制缺口，将定时与手工入口接入同一实现；移除被替代代码。
- 核对同文件系统临时文件、关闭、验证、同步、替换顺序；正确的现有实现不重写。
- 使用真实子进程同时触发定时与 repair/rebuild 入口，检查串行边界与嵌套调用。
- 在临时数据上注入截断、SIGKILL、权限失败、空间不足；保留失败前后文件字节证据。

主要范围：`clients/parquet_io.py`、实际 publisher、`job_runner_common.py`、
daily/intraday/coverage/rebuild 调用链及对应现有测试；最终范围以 SE-03 为准。

验收：所有写入入口都遵守契约；失败保留有效旧文件；重试不依赖人工清理。
退出码或 blocked 不得让未完成数据变成健康状态。

### SE-05：实现不可变 Silver 发布

`depends_on: [SE-04]`；前置：存储契约批准。

- 按已有 Silver 设计生成唯一 attempt，写新 daily/factor 文件并验证。
- 复用已提交且仍有效的未变化 artifacts；构建输入处于 SE-04 的保护范围。
- 以完整 manifest 的 current 替换作为提交点；未提交残留不能自动成为 served 数据。
- 替换现有覆写、补哈希、孤儿收编及移动旧读者所需文件的逻辑，避免新旧机制并存。
- 无变化不发布新 revision；单 symbol 隔离不伪装为完整健康；空间不足安全停止。

主要文件：`clients/silver_client.py`、`clients/silver_revision.py`、
`livewire_scripts/rebuild_silver.py` 及对应现有测试。

验收：各提交阶段真实 SIGKILL 后，current 与其引用文件仍一致；重试使用新 attempt。

### SE-06：统一消费者的 snapshot 读取

`depends_on: [SE-04]`；前置：与 SE-05 相同的已批准契约及 Apex 范围批准。

- Livewire DuckDB、验证脚本、PIT 和 Apex 从固定 manifest 获取路径。
- 一次 adapter 请求及 daily/factor 联读共享同一 snapshot；Apex 内部
  watcher reseed 的事务一致性不属于本次范围。
- 缺项明确拒绝；删除固定路径 fallback，不通过 glob 或 newest directory 补数据。
- 用 SE-01 清单证明所有消费者都已覆盖，不只修改 Apex 一个 adapter。

Livewire 主要文件：`clients/duckdb_catalog.py`、`clients/pit_silver_revision.py`、
`livewire_scripts/validate_adjusted_history.py`、`shepherd_silver.py`（按需）。
Apex 主要范围：`src/infrastructure/adapters/livewire/` 及对应契约测试，
必要的现有调用接缝只做最小适配。watcher 通知只表示数据发布，不能表示
内部重算或信号切换完成。内部 reseed/信号状态机制延期至 Apex 重构。

验收：发布期间老读者仍读旧 snapshot，新读者读完整新 snapshot；历史 PIT 保持正确。
两端共享一份交付契约，明确 provider、adapter 与 Apex 内部责任；内部信号一致性
不是本阶段已实现保证，后续重构可用同一契约测试验证接入兼容性。

### SE-07：集成发布、目录、状态与恢复

`depends_on: [SE-05, SE-06]`

- DuckDB 的 Silver 统计绑定已提交 manifest；失败保留旧 catalog 并显示其实际时点。
- 按现有资格集合核对 symbol/timeframe，不以视图非空或最新日期代替完整性。
- 用现有 ledger 表达运行、提交、质量与新鲜度；确需新增协议时先列差异再批准。
- 对 269/53 旧基线重新实测、分类：合法状态、暂时故障、数据矛盾、损坏、未知。
  形成逐项恢复清单；未授权数据替换只做准备，不执行。
- 把分类转成可行动 warning：范围、证据、最后有效数据、自动动作、人工动作与
  解除条件完整；验证去重、影响升级和恢复通知，不能只有累计失败数。
- 在真实 reader/writer 集成测试中覆盖并发、缺项、隔离、重试和递增 revision 回退。
- 故障隔离实验必须验证“无关工作继续”：IB 断开时其他来源继续、单 symbol 损坏
  不拖停全市场、catalog/邮件失败不阻断 ingestion、writer 崩溃不遗留永久锁。

验收：报告和消费者引用同一可解释结果；局部失败不隐藏，无关分支继续，
全局事务失败不提交；每条 warning 能指导判断与行动。

### SE-08：按真实时间线验证容量

`depends_on: [SE-07]`

- 分别测冷缓存正常增量、无变化、首次迁移、失败重试；首次迁移不塞进日常预算。
- 记录总耗时、锁等待/持有、写入量、校验和读者影响；覆盖 coverage repair 竞争场景。
- 以实际 13:00 daily、18:00 intraday、19:00 coverage 为起点评估；业务截止时间
  尚未指定，不能把五小时触发间隔写成已承诺 SLA。
- 没有容量证据不调大 timeout 或增加并发。超出窗口时先定位重复扫描/重建，
  再形成明确的非关键工作延后方案，不能静默丢工作。

验收：用完整成功样本及明确余量支持目标窗口；小样本不包装成可靠 p95。
真实文件系统上的实验只使用可丢弃目录，不对生产湖做破坏性测试。

### SE-09：独立审查与完整发布候选

`depends_on: [SE-08]`

- 独立审核完整累计 diff、契约、故障测试和时间线；检查是否仍有旁路及重复实现。
- 消融新抽象：移除后仍满足验收则删除；必要设计写明它防住的具体故障。
- 执行仓库全部必需检查，不以工具返回 success 代替日志中的实际结果。
- 分别交付 Livewire/Apex PR，记录兼容组合和部署顺序；合并与切换遵守已有授权。

Livewire 必需检查：

```sh
uv lock --check
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning
npm ci
npm run test:alerts
```

`--cov` 使用配置中的 `clients` 与 `livewire_scripts`；不以 wrapper 目录替代覆盖源。
Apex 按其实施时的仓库规则执行。测试和依赖安装在隔离 checkout，不修改 release。

验收：精确合并 SHA 的检查通过，无阻断发现；不可用的独立审查明确写未验证。

### SE-10：受控生产切换

`depends_on: [SE-09]`；前置：具体生产切换批准。

- 提交可评审的清单：确切 SHAs、维护范围、空间预算、允许的中断、停止与回退条件。
- 控制自动 promotion、全部 writer 和 repair 的进入；等待旧 writer 退出，保护
  其仍使用的 release。不能只看空进程列表或依赖 release 默认 prune。
- 验证基线数据；旧 manifest 不一致时不得刷新哈希伪造有效基线。
- 在保护输入的窗口中准备初始 snapshot，按契约部署消费者与 writer，验证后恢复调度。
- 保留旧文件；新格式上线后禁止直接回退到旧 mutable writer。数据回退使用新的
  递增 revision 引用保留的有效 artifacts。

验收：运行进程、发布目录、数据 snapshot 和消费者完全匹配；无活动 writer 被误杀，
无未经批准的数据替换/删除。进程中断验证不等于断电持久性验证。

### SE-11：Claude 独立验证与关闭任务

`depends_on: [SE-10]`

- 给 Claude 精确 release SHAs、最终验证命令和故障矩阵，不交付预设“通过”的结论。
- 独立核对 released code、真实 manifest/hash、Apex 请求和 watcher/reseed。
- 至少验证一次完整正常定时运行；无变化/重试若未自然发生，在隔离数据验证。
- 逐项报告代码、部署、运行、数据完整性、新鲜度、消费结果与剩余未知。

验收：同类故障再现时无需改代码或人工补提交即可安全处理。原始损坏原因未知、
未解决数据质量或文件系统持久性限制仍明确保留；不能为了关闭任务将其改为通过。

## 实施分工与提交纪律

- 主负责人：契约、锁责任、共享文件、集成、验收与发布。
- 入口调查和测试门禁可并行；SE-03 后统一写入改动由单一负责人集成。
- SE-05 writer 与 SE-06 reader 可并行，但各自拥有明确文件；共享 DuckDB/状态文件
  由主负责人负责。审核者只读，不与实现者同时修改同一文件。
- 每份 PR 是一块可验证的完整行为改动，不能留下需要临时旁路才能运行的半套协议。
- 不按测试数、PR 数或消除多少条报错衡量进度；按上述验收条件逐项关闭。

## 立即执行顺序（实施授权后的第一轮）

1. SE-00 固定当前状态并建立独立 worktree。
2. 并行完成 SE-01 入口矩阵与 SE-02 可信门禁。
3. 固定 SE-03 契约及失败测试，实施 SE-04 共同控制边界。
4. 继续 SE-05 至 SE-11，按依赖及批准条件推进；不将 SE-04 的阶段交付称作根治完成。

# Price Discovery 总纲 v2 — 2026-09-22

## 状态与授权
本次只准备规格与 handover；实现、研究运行、测试、提交、部署均未开始。各项目由用户在新 session 启动。四个 worktree 均为 .worktrees/price-discovery，分支 feat/price-discovery。保护主 checkout 的现有改动。无生产写入、服务重启、下单、外发、merge 或部署授权。

原 /mnt/data 的 IMPLEMENTATION_PLAN.md、ASTRA_START_HERE.txt 和 zip 未恢复。本文是根据引用会话与用户后续决策编写的新规格，不是原附件复刻。恢复原件后对照差异；不能声称覆盖原件未见的 40 个测试。以下 MPD 编号源于已恢复的会话摘要，具体实施以本修订的明确边界推进。

## 已确定的职责
Livewire → Apex → Argon：所有本研究的 lake 输入经过 Apex。Argon 原有宏观 evidence/FRED ingestion 继续复用，不要求为了这条链路迁移到 Livewire。Helium 最后接入。

Yahoo：用户明确授权本研究使用 Yahoo 宏观数据。保留真实 upstream_provider、数据质量、时钟与历史版本限制；不能把 provider 改名为 Apex/Livewire，也不全局解除其他功能的 source policy。旧 Gold Compass 行为保持。
黄金默认仍为 gold_gld_broadusd_eod_v1；DXY 不等于 DTWEXBGS。若开展 Yahoo-DXY 研究，单独注册 study/version，独立冻结配置与验证，不替换原研究数据后沿用名称/成绩。

## 审查发现、证据与修正
1. HIGH / 原建议 invalid：DTWEXBGS 不是 yield，不能放进 yield_pct/tenor_years。Apex src/api/payload/chart.py:105 定义 yield-only 输出；general series 应保留 index 单位。
2. HIGH / 原建议 partial：Argon 已有 macro/contracts.py、macro/evidence_store.py、sources/fred_macro.py。先查真实 DFII10/DTWEXBGS evidence 覆盖；不预先要求 Livewire 重建宏观账本。
3. HIGH / calendar partial：Argon storage/macro_release_calendar.py:40 的 upsert 覆盖 forecast/prior/captured_at；追加 immutable captures，保留 latest projection；actual 精确匹配 reported period，不能取最新日期替代。
4. HIGH / replay partial：web/app/macro/[tab]/goldTab.tsx 的现有回放是 obs_date。新研究回放按 immutable run/bundle + decision_as_of，与旧 posture date 分开。
5. HIGH / source policy partial：用户的 Yahoo 授权不补齐历史 available_at。今日抓到的修订数据只能明确标为重建/当前版本研究；hash 证明内容身份，不证明历史可得。
6. MEDIUM / 数据调查 partial：此前 EIA 全湖查找未完成。已检查的顶层分区和 tracked ingestion 未见 EIA；不能声称全盘绝无 EIA 文件。
7. MEDIUM：GLD API adjusted 最新 Sep 18，而 bronze 到 Sep 21；需确定价格模式与共同截止点，不能静默 raw fallback。
8. MEDIUM：worktree 就绪不代表依赖/测试就绪；这些仍待 MPD-00。

## 2026-09-22 Mac mini 实测记录
约 07:28–07:31 UTC，Apex :8322 /health 返回 0.1.9，adjusted mode，revision 76。
GLD bars 200；抽查 bronze source=massive / price_basis=raw。DFII10 rates 404。DTWEXBGS rates 404（该 route 本身也不适合 index）。
DXY FX bars 200；fetch_fx.py 明确 Yahoo。DXY parquet 无 source/timing；DGS10 有 source=fred，无 available_at/vintage；GLD 有 source/price_basis，无 available_at。Apex 输出未传递这些 source 字段。
以上是抽查结果，不代表整个历史 provider 已审计。mini repo checkout SHA 不等于运行容器 SHA。后续复核禁止以 catalog 快照替代实际文件覆盖。

## EIA 网络与 key
用户无 key，报告申请时的网络限制，可自行 VPN 申请。
官方 https://www.eia.gov/opendata/register.php 说明 API 需 key、bulk download 不需 key；https://www.eia.gov/opendata/documentation.php 未声明必须美国 IP。没有证据保证新加坡出口一定成功。
https://www.eia.gov/opendata/faqs.php 明确 API v2 不再有旧 update 字段，不能把请求时间当发布时间。
先检查一个官方 no-key bulk/CSV/XLS 是否含目标 gasoline 系列及可用元数据，避免把 key 当全部工作的前置条件。不能编造 endpoint。
拿到 key 后从实际 mini 出口及用户配置的新加坡出口各做一次小请求，记录时间/HTTP/行数/hash/出口标签，隐藏 key。没有配置新加坡出口时记 EIA_SG_EGRESS_UNTESTED；不擅改 VPN。
Gasoline 数据必须对齐产品、地域、周/月频率和单位；获取成功不等于有历史 vintage。
待查：EIA_ACCESS_UNVERIFIED、EIA_RELEASE_VINTAGE_UNVERIFIED、GASOLINE_CPI_TARGET_VINTAGE_UNVERIFIED。黄金不依赖 EIA。

## MPD-01 共享契约（待冻结的新设计，不是已有接口）
Argon lead 协调三个 owner，一次性冻结：
- series/instrument、economic meaning、unit、frequency、source provider、transport、adjustment basis、period/session/timezone。
- published_at 可空并携带精度/证据；received_at、available_at、vintage、raw/normalized hashes、schema/source-policy version。
- unknown 时间不能填 observation date；历史公开信息重建、当时实际记录和当前修订研究必须区分。
- bundle 保存确切输入行与源引用、cutoff、study/config/code 身份及 manifest hash；可断网重放，不仅保存会变化的 URL。
- card 保存 run ID、decision_as_of、produced_at、bundle hash、attribution、forecast、baseline validation、独立 forecast/pricing/tradability gates 及原因。
- 规范排序/数值精度/UTC，拒绝 NaN/Infinity。replay 对比确定性数值/hash；produced_at 属于原记录，不能把重算当前时钟混入数值 hash。
公共 schema/registry/migrations/router/scheduler/generated types 每个 repo 由 lead 串行集成。worker 只写独占文件，不 checkout/restore/stash 其他人的工作。

## 执行 DAG
0. MPD-00：Livewire/Apex/Argon 同步盘点各自代码、真实覆盖、源/时间语义、baseline tests。Argon lead 汇总缺口与 source ownership。
1. MPD-01：冻结共享契约与 study；未经此 gate 不并行发明接口。
2. 可并行：Argon MPD-02 calendar；Livewire MPD-03 export 后交 Apex adapter；Argon MPD-04 离线 runner。
3. MPD-05：Apex 的真实 GLD 输入 + Argon 合格宏观 evidence → gold run。
4. MPD-06：原子/幂等持久化 → read-only API → default-off worker。
5. MPD-07：Gold UI + run replay，先闭合黄金。
6. MPD-08：Livewire gasoline capture → Apex series → Argon gasoline CPI forecast/validation/UI。访问调查可提前，真实研究等待输入。
7. MPD-10A：黄金/汽油确定性闭环及故障演练。
8. MPD-09：Helium 最后只读接入；MPD-10B 验解释集成。
9. MPD-11/12：原油曲线/通胀合约条件扩展暂不实施。
10A/10B 是本次显式修订，原始缺失 DAG 不作虚构还原。

## 研究协议
黄金 attribution 为共同已可用窗口上的 GLD returns、real-yield change、USD returns；residual 不称 fair value gap。
Forward：B0 历史均值；B1 黄金自有 lag/vol；P1 加已可用宏观；P2 加只用历史训练得到的 residual。主 horizon 5 sessions，辅助 1。
查看 holdout 前落盘：split dates、训练最小量、features/transforms、primary metric、baseline、uncertainty/block unit、purge、seed 和 gate thresholds。根据 MPD-00 覆盖制定并冻结；不能假造原计划阈值。训练不得包含 cutoff 后才完成的 labels；看过的探索区间不能改称 untouched holdout。
汽油只预测 NSA gasoline CPI 分项，observed/unobserved month 分开，与 seasonal baseline 比较，分子/前月分母须同 release vintage。无完整分项模型不称 headline CPI；无匹配通胀报价则 pricing BLOCKED；无 executable quotes/cost/timing 则 tradability BLOCKED。
样本不足为 BLOCKED；null/无增量价值是有效研究结论，全部持久化。

## 验收/交付
每项提供 changed paths、base/current SHA、准确测试命令与结果、real capture/bundle/run hashes、失败/null/blockers。单元测试不联网，用已有测试基础设施；金融样本来自注明日期的真实 capture，人工数据显式标 synthetic。
必验：未来数据不改变过去预测；consensus 修订不能改历史 cutoff；错 reported period 拒绝；index/yield 单位不混；Yahoo identity 穿透 Apex；原输入变化不改 frozen replay；原子发布/幂等重试；invalidated evidence 保留历史并停止 promotion；UI missing/error/blocked/replay；LLM 篡改/错引用被拒绝。
工程 tests、真实研究、可交易性和部署分别报告。可以用只读真实 capture 和本地 Apex/Argon 完成集成，生产不部署。

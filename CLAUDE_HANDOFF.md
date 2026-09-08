# Claude Code 接手入口

你正在用户要求的交接 worktree：

`/Users/chenxi/projects/livewire/.worktrees/claude-release-handoff`

分支：`handoff/claude-release-20260908`，从已验证候选 `a4003d3` 创建，包含完整 Livewire 源码与需求文档。先读 [Executive Summary](docs/codex-handoffs/claude-release/EXECUTIVE-SUMMARY.md)，再按其中六步接手。

本 worktree 是新的接手入口；摘要中 `systematic-integrity` 路径表示原实施/验证工作区。后续 Livewire 工作在这里继续，保留原工作区。Apex 是独立仓库，继续使用 `/Users/chenxi/projects/apex/.worktrees/livewire-snapshots`；不要复制第二套 Apex 实现到本仓库。

目标：关闭最小必要的 Apex CI 阻断、数据/容量决策和发布条件，准备具体切换方案；获批后通过 PR 合并、release/promote，再验证新版本一次正常定时运行。固定 Livewire→Apex 数据契约，Apex 内部重写明确延期。

已有草稿 PR：Livewire #125、Apex #161。当前交接分支尚未推送，不是 #125 的源分支；后续通过正常分支/PR流程交付改动，不能直接推 main/master，也不能假定本分支提交会自动进入 #125。

先刷新实际 PR/CI、Mini 部署和进程状态。生产切换与数据替换的具体批准尚未取得；准备到可评审后集中请求必要批准。保留用户无关 dirty 内容，不重启 Gateway，不补哈希认可坏 manifest，不删除数据或旧 release。

验证资料已经在仓库中，无需 ZIP：完整计划在 `docs/plans/`，故障验证与269/53逐项清单在 `docs/codex-handoffs/`，本次状态快照在 `docs/codex-handoffs/claude-release/evidence/`。

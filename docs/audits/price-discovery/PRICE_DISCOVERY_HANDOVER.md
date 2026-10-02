# livewire handover

worktree: /Users/chenxi/projects/livewire/.worktrees/price-discovery
branch: feat/price-discovery
base SHA prefix: f83e3df0
状态：仅三份文档，未实现、未测试、未提交、未部署。

先读本目录 PRICE_DISCOVERY_MASTER.md、PRICE_DISCOVERY_SPEC.md 与 repo/layer instructions。完整 SHA 用 git rev-parse HEAD 复核。主 checkout 有用户改动，禁止带入、restore/stash/discard。本 worktree 三份 markdown 是预期未提交文件。

下一步：
1. 用户在本 session 启动后核对 branch/HEAD/diff。
2. 完成自己 MPD-00 与环境/baseline checks；保存命令/真实结果。
3. 等待 MPD-01 契约冻结后按 spec 推进；下游等待真实上游样本。
4. worker 独占文件，lead 串行 registry/migrations/router/generated types；禁止跨 repo 改动。
5. 交付 changed paths、SHA、tests、artifact/hash、研究 null/失败、blocker 和下游 next step。提交/PR遵循明确授权，生产不动。

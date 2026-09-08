# STEP4 — 具体切换方案（待批准后逐步执行）

交接步骤 4「准备并评审具体切换方案」的交付件。范围：把 Livewire 候选（PR #125）与 Apex 候选（PR #161）按可回退顺序切进生产，并在一次正常定时运行后给出四个独立结论。

**本文件不执行任何动作。** 每一步都写成「命令 + 期望输出 + 中止条件」。所有命令都能在 `docs/runbook.md` 或本仓库 `scripts/` / `livewire_scripts/` 里找到出处（逐条标注）；找不到出处的一律标 `[unverified]`，并在 §9 汇总。

- 写作时间：2026-09-08。现场核对时间：2026-09-08 05:45Z 与 06:0xZ（`ssh macmini`，只读）。
- Livewire 候选 PR #125 head `a4003d3`（CI 绿、draft、**未合并**）。合并后会产生**新的 squash SHA**，全文写作 `<MERGE_SHA>`。
- Apex 候选 PR #161 head `74c29db`（CI 13 项全绿，run 34192607375；镜像 digest 待合并后构建产出）。
- 生产现状（2026-09-08 实测）：`current -> releases/23e1de284f343e357f97100cfe83fb48ac4be9ec`；`releases/` 另有 `66d36dc`、`c295c37`、`dfd7710`；Silver `revision 39 / members 13279 / artifacts 26558`。

---

## 0. 三行结论

1. **Livewire 侧没有「部署但不启用」这一档**：`promote` 翻转 `current` 之后，下一次 13:00 HKT `daily-update` 的 silver lane 就是新 writer。所以 Apex reader 必须先于 `promote` 上线。
2. **两个自动化会在无人值守时激活半成品组合**：Livewire 的 `com.livewire.release-promote`（04:30Z / 12:30 HKT，读仓库、拿 `origin/main`）和 Apex 的 watchtower opt-in（60s 轮询 `ghcr.io/moremeds/apex-api:latest`）。两者都必须在合并**之前**用可逆方式关掉。
3. **本轮新发现的两个硬风险**（不在任何交接文档里）：`promote --keep` 默认 3，且 prune 在 `flip_current` **之后**跑，旧的 `23e1de2` 不再受 `active` 保护 → 必须 `--keep 6`；`rebuild_silver.py` **不取** `lake-io.lock` → 任何手工 `rebuild-silver` 都不与定时 lane 串行，只能在空窗期跑。

---

## 1. 前置条件（Preconditions）

### 1.1 精确版本

| 项 | 值 | 如何取得 |
|---|---|---|
| Livewire 合并前 head | `a4003d3` | `gh pr view 125 --json headRefOid` `[unverified]` |
| Livewire 合并后 SHA | `<MERGE_SHA>` | 合并后 `git -C ~/projects/livewire rev-parse origin/main` |
| Livewire 旧生产 SHA | `23e1de284f343e357f97100cfe83fb48ac4be9ec` | `ssh macmini readlink ~/market-warehouse/current` |
| Apex 候选 head | `74c29dbe2613597b3040ff8c7a25424278d0c50f` | 另一 session 修完 CI 后填 |
| Apex 生产镜像 digest（旧） | `sha256:3d3613d38f10a8fcc3e8934c60d9c040218b462711bfabac20a09bdb3656ee6d` | EXEC-SUMMARY 03:38Z 观测，行动前需复核 `[unverified]` |
| Apex 新镜像 digest | `<APEX_IMAGE_DIGEST>` | 构建产出后填 |

### 1.2 CI 证据

`a4003d3` 的两个成功 run：`34189986710` / `34190023718`（EXEC-SUMMARY 记录）。**这不是发布门禁。** `promote` 的门禁只认合并后那个 SHA：

```bash
# livewire_scripts/release.py:84-96 里 ci_is_green() 的原样命令
gh run list --commit <MERGE_SHA> --workflow ci.yml --limit 20 --json status,conclusion
```

期望：至少一条 `{"status":"completed","conclusion":"success"}`。
中止条件：无 completed+success → 不 promote，不加 `--allow-unverified`（runbook §9：该 flag 只为 bootstrap 第一次 release 用过）。

### 1.3 Mini 空窗期

时刻表（`docs/runbook.md` §9 Schedule 表，UTC；括号内 HKT = UTC+8）：

| Job | UTC | HKT |
|---|---|---|
| `com.livewire.release-promote` | 04:30 | 12:30 |
| `com.livewire.daily-update` | 05:00 | 13:00 |
| `com.livewire.intraday-catchup` | 10:00 | 18:00 |
| `com.livewire.daily-update-watchdog` | 10:30 | 18:30 |
| `com.livewire.coverage` | 11:00 | 19:00 |
| `com.livewire.universe-refresh` | 周日 13:00 | 周日 21:00 |

`coverage` 无 timeout，冷缓存单轮 1400–2860s（根 `CLAUDE.md`）。**建议维护窗口：20:30 HKT — 12:00 HKT（次日）。**

```bash
# 1) 没有 livewire 定时任务在跑：第一列全是 "-"，不能有 PID
ssh macmini 'launchctl list | grep livewire'

# 2) 湖锁没人持有：无输出即空闲（docs/runbook.md:77）
ssh macmini 'lsof ~/market-warehouse/locks/lake-io.lock'

# 3) 上一轮 lane 的收尾状态（docs/runbook.md:716）
ssh macmini 'source ~/market-warehouse/.venv/bin/activate && python ~/market-warehouse/current/scripts/livewire_ops.py ledger query "select lane, outcome, elapsed_s from lane_results order by started desc limit 20"'

# 4) 一张打分表（docs/runbook.md:706；退出码恒为 0，看内容不看 rc）
ssh macmini 'source ~/market-warehouse/.venv/bin/activate && python ~/market-warehouse/current/scripts/livewire_ops.py status'
```

**中止条件**：`launchctl list` 第一列出现 PID（当前 2026-09-08 05:45Z 实测 `com.livewire.daily-update` PID 83252 在跑）；或 `lsof` 有输出；或 `status` 出现与本次切换相关的 `BAD`。等到空窗，不要抢锁。

---

## 2. 旧 release 与输入的保护

### 2.1 必须原样存活的路径

| 路径 | 为什么 |
|---|---|
| `~/market-warehouse/releases/23e1de284f343e357f97100cfe83fb48ac4be9ec` | 唯一的代码回退目标 |
| `~/market-warehouse/releases/{66d36dc*,c295c37*,dfd7710*}` | 更早的回退深度 |
| `<lake>/silver/revisions/current.json` 与 rev39 manifest + 它引用的 26,558 个 artifact | 唯一的数据回退底座；carry-forward 也要逐个校验它们 |
| `<lake>/bronze/`、`<lake>/raw/`、`<lake>/repairs/` | canonical 输入；`raw/`、`repairs/` 由 housekeeping 按名保护（runbook §10） |
| `<lake>/ledger/` | 四个结论的证据来源 |

**规则**：切换全程不跑 `housekeeping --apply`（它 keep-releases 3 / keep-evicted 2）；不 `rm` `current` 指向的 release（根 `CLAUDE.md`：用 `release rollback` 恢复）。

### 2.2 `promote` 自己会 prune —— 必须显式加 `--keep`

`livewire_scripts/release.py:270-276`：`flip_current(target_sha)` **之后**才 `prune(keep)`，而 `prune` 只跳过 `current_sha()`（此时已是新 SHA）。`DEFAULT_KEEP = 3`（`release.py:37`）。releases 目录现有 4 个，promote 后 5 个 → 默认会删掉最旧的两个之一，且旧 current `23e1de2` 已失去保护。

```bash
# 唯一正确的 promote 形式（--keep 见 release.py:315）
python scripts/livewire_ops.py release promote --keep 6
```

### 2.3 开工前的可读性验证（只读）

```bash
# a) 当前 Silver 提交记录
ssh macmini 'python3 -c "import json;d=json.load(open(\"/Volumes/DATA_LAKE/silver/revisions/current.json\"));print(d.get(\"revision\"), len(d.get(\"members\",[])), len(d.get(\"artifacts\",[])))"'
# 期望：39 13279 26558（与 STEP3 附表一致）；路径若不同，先 readlink ~/market-warehouse/data-lake 确认 [unverified]

# b) rev39 引用的 artifact 抽样可读（抽 20 个算 sha256，与 manifest 记录比对）
#    —— 没有现成 CLI，见 §9；按 manifest 里的相对路径 shasum -a 256 逐个比 [unverified]

# c) release 清单，* 标记正在服务的（docs/runbook.md:883）
ssh macmini 'source ~/market-warehouse/.venv/bin/activate && python ~/market-warehouse/current/scripts/livewire_ops.py release list'
# 期望：5 行内含 23e1de28…，且 * 在它上面
```

**中止条件**：(a) 打不开或 revision ≠ 39 → 停，先解释；(b) 任何一个抽样 sha256 不匹配 → **停**，这正是 §6 里 carry-forward 会整次失败的那条路径，绝不能靠刷新哈希「认可」现有字节（EXEC-SUMMARY「不可降低的需求」）。

---

## 3. 自动更新器的可逆封锁

### 3.1 Livewire 自动 promoter（04:30Z / 12:30 HKT）

合并到 main 之后，下一次 12:30 HKT 会自动 `promote origin/main`，把新 writer 装进生产而无人在场。**必须在合并前卸载。**

```bash
# 关（docs/runbook.md §9 launchd install 的 unload 形式）
ssh macmini 'launchctl unload ~/Library/LaunchAgents/com.livewire.release-promote.plist'
ssh macmini 'launchctl list | grep release-promote'     # 期望：无输出

# 记录可逆配置：plist 原样保留在 ~/Library/LaunchAgents/，不删不改
ssh macmini 'ls -l ~/Library/LaunchAgents/com.livewire.release-promote.plist'
```

```bash
# 开（切换验收通过后）
ssh macmini 'launchctl load ~/Library/LaunchAgents/com.livewire.release-promote.plist'
ssh macmini 'launchctl list | grep release-promote'     # 期望：一行，第一列 "-"
```

### 3.2 Apex watchtower（60s 轮询）—— 只动 Apex 的资格，不停共享服务

实测（2026-09-08，`/Users/moremeds/apex-deploy/compose.yml`）：`apex-api` 用 `image: ghcr.io/moremeds/apex-api:latest`（第 25 行），并在第 68-69 行携带 opt-in 标签 `com.centurylinklabs.watchtower.enable: "true"`；watchtower 本体是 engine-wide 的 `xenon-watchtower-1`，定义在 `/opt/xenon/compose.yml`，`WATCHTOWER_LABEL_ENABLE` 模式。

**唯一允许的做法**：改 `apex-deploy/compose.yml` 里 apex-api 的 opt-in 标签（或把 `image:` 钉到 digest），然后 `docker compose up -d apex-api` 重建该服务。**不得** `docker stop xenon-watchtower-1`（会影响其他应用）。

```bash
# 关：把 apex-api 的 opt-in 改为 "false"，只重建这一个服务  [unverified：docker 命令不在 livewire runbook 里]
ssh macmini 'cp /Users/moremeds/apex-deploy/compose.yml /Users/moremeds/apex-deploy/compose.yml.pre-cutover-20260908'
#   编辑 compose.yml：com.centurylinklabs.watchtower.enable: "false"
ssh macmini 'cd /Users/moremeds/apex-deploy && docker compose up -d apex-api'
ssh macmini 'docker inspect -f "{{index .Config.Labels \"com.centurylinklabs.watchtower.enable\"}} {{.Image}}" apex-deploy-api-1'
# 期望：false <旧 digest>
ssh macmini 'docker ps --filter name=xenon-watchtower-1'   # 期望：仍在 Up —— 共享服务没被动过
```

```bash
# 开（验收通过后）：恢复备份的 compose.yml，重建 apex-api
ssh macmini 'cp /Users/moremeds/apex-deploy/compose.yml.pre-cutover-20260908 /Users/moremeds/apex-deploy/compose.yml && cd /Users/moremeds/apex-deploy && docker compose up -d apex-api'
```

> 备份文件写入与 compose.yml 编辑属于「改用户数据」，列在 §8 审批清单。

---

## 4. 部署顺序与中间态

顺序来自 `docs/codex-handoffs/2026-09-08-systematic-integrity-verification.md` §5：**验证初始不可变 snapshot → 部署兼容 reader → 启用 writer**。

### 4.1 先验证「初始不可变 snapshot」能被候选代码产出且不发布

合并后、promote 前，在 mini 的仓库 checkout（`~/projects/livewire`，就是 promoter 读的那个）上跑一次 dry-run。dry-run 只比较不发布（`docs/runbook.md:491`）。

```bash
ssh macmini 'cd ~/projects/livewire && git checkout main && git pull'      # docs/runbook.md:888
ssh macmini 'cd ~/projects/livewire && git rev-parse HEAD'                  # 期望 = <MERGE_SHA>
ssh macmini 'source ~/market-warehouse/.venv/bin/activate && cd ~/projects/livewire && \
  python scripts/livewire_store.py rebuild-silver --full --dry-run \
    --failure-output ~/market-warehouse/logs/cutover-dry-20260908.json'     # docs/runbook.md:492
```

期望输出：一行 `SUMMARY_JSON`，`failed=269`（STEP3 §1 的基线）、`window_regressions=53`（STEP3 §2.1 的预测，生产血统首次实测）、exit 0，`revision` 不变（dry-run 不发布）。

> **`rebuild_silver.py` 不取 `lake-io.lock`**（本轮 grep：文件内无 `flock`/`lake-io`）。这一步必须在 §1.3 的空窗期内跑，否则会与定时 lane 并发读写同一个湖。

**中止条件**：
- 出现 `ValueError` 且信息指向 `_copy_verified_legacy_artifact`（`rebuild_silver.py:214, 265-271`）→ **立即停**。这是 legacy artifact sha256 不匹配导致的**整次发布失败**（不是 per-symbol 隔离），STEP3 §2.1 明确列为 stop condition。不得刷新哈希绕过。
- `failed` 显著 ≠ 269，或 `window_regressions` 既不是 53 也不是 0 → 停，先解释差异再谈发布。

### 4.2 再部署 Apex reader（兼容旧数据）

Apex 候选是 request 级 adapter，读固定 snapshot；对**旧格式 rev39**必须仍然可读（EXEC-SUMMARY「已完成的行为」6：daily/intraday producer→consumer 互操作已通过）。

```bash
# 按 <APEX_IMAGE_DIGEST> 拉起（把 image 钉到 digest，而不是 latest） [unverified]
ssh macmini 'cd /Users/moremeds/apex-deploy && docker compose up -d apex-api'
ssh macmini 'docker inspect -f "{{.Image}} {{index .Config.Labels \"org.opencontainers.image.revision\"}}" apex-deploy-api-1'
# 期望：<APEX_IMAGE_DIGEST> 74c29dbe2613597b3040ff8c7a25424278d0c50f
```

**此时旧 reader 已被替换，新 reader 读的仍是 rev39。** 中间态（新 reader + 旧 writer）预期表现：Apex 继续按 30s 轮询读 `revisions/current.json` 指向的 rev39，legacy 路径 artifact 正常返回；Bronze/Silver/delisted 仍是只读挂载。
**中止条件**：新 reader 对 rev39 返回 5xx 或空数据 → 立刻按 §6.2 回滚 Apex 镜像，**不要**继续 promote。

### 4.3 最后启用 writer（promote）

见 §5。**注意：Livewire 没有「装了但不启用」这一档** —— `promote` 之后的第一次 13:00 HKT `daily-update` 的 silver lane 就是新 writer；`rebuild-silver` 本身没有开关 flag。

**中间态里旧 reader 看到什么**（如果 §4.2 因故未做而 §4.3 已做）：旧 reader 会遇到新 writer 发布的 `generations/` 布局 —— 这是被明确禁止的组合，不允许出现。顺序不可交换。

---

## 5. 合并与 promote

```bash
# 5.1 合并（squash → 新 SHA）  [unverified：gh pr merge 不在 runbook 里]
gh pr ready 125 && gh pr merge 125 --squash

# 5.2 等 main 上的 ci.yml（ci.yml 对 push to main 触发，正是为 squash merge 而设；根 CLAUDE.md）
gh run list --commit <MERGE_SHA> --workflow ci.yml --limit 20 --json status,conclusion
#   期望：completed / success；未 completed 就等，不要用 --allow-unverified

# 5.3 mini checkout 同步（promote 用 checkout 自己的 builder；docs/runbook.md:888）
ssh macmini 'cd ~/projects/livewire && git checkout main && git pull && git rev-parse HEAD'
#   期望：<MERGE_SHA>

# 5.4 dry-run（docs/runbook.md:882）
ssh macmini 'source ~/market-warehouse/.venv/bin/activate && cd ~/projects/livewire && \
  python scripts/livewire_ops.py release promote --dry-run --keep 6'
#   期望日志：would build .../releases/<MERGE_SHA>
#   ⚠ promote 在 CI 不绿时打印 "CI is not green for … — keeping …" 并 **return 0**
#     （release.py:247-253）。退出码不是信号，必须读这一行。

# 5.5 真跑
ssh macmini 'source ~/market-warehouse/.venv/bin/activate && cd ~/projects/livewire && \
  python scripts/livewire_ops.py release promote --keep 6'
#   期望日志：current -> <MERGE_SHA>；不出现 "pruned 23e1de28…"

# 5.6 确认
ssh macmini 'readlink ~/market-warehouse/current'
#   期望：releases/<MERGE_SHA>
ssh macmini 'source ~/market-warehouse/.venv/bin/activate && python ~/market-warehouse/current/scripts/livewire_ops.py release list'
#   期望：* 在 <MERGE_SHA>，且 23e1de28… 仍在列表里
```

**中止条件**：5.4/5.5 出现 `CI is not green` → 停；5.5 日志出现 `pruned 23e1de28…` → **立刻停**并按 §6.1 处理（回退目标已被删，需要重建）；5.6 的 `readlink` 不是新 SHA → 停。

---

## 6. 停止条件与回退

### 6.1 代码回退（Livewire release）

```bash
ssh macmini 'source ~/market-warehouse/.venv/bin/activate && python ~/market-warehouse/current/scripts/livewire_ops.py release rollback --to 23e1de284f343e357f97100cfe83fb48ac4be9ec'
ssh macmini 'readlink ~/market-warehouse/current'   # 期望 releases/23e1de28…
```

`--to` 见 `release.py:324`；不带 `--to` 时默认选「最近的非 current」（`release.py:284-287`），promote 之后那通常正是 `23e1de2`，但**显式写全 SHA**。
回退后如需再上：修好问题 → 新 PR → 新 SHA → 重走 §5。**永远不要** `rm -rf` `current` 指向的目录（runbook §9）。

### 6.2 Apex 回退

```bash
# 把 image 钉回旧 digest 后重建（compose.yml 备份见 §3.2） [unverified]
ssh macmini 'cd /Users/moremeds/apex-deploy && docker compose up -d apex-api'
ssh macmini 'docker inspect -f "{{.Image}}" apex-deploy-api-1'
# 期望：sha256:3d3613d38f10a8fcc3e8934c60d9c040218b462711bfabac20a09bdb3656ee6d
```

### 6.3 数据回退（Silver）

规则（verification doc §5，不可违反）：**发布一个新的、单调递增的 manifest，引用保留且校验通过的历史 artifact**；不倒退 revision、不改 `current.json` 指回旧 revision、不退回可变 writer。

**本候选没有对应的操作员 CLI**（本轮核对 `rebuild_silver.py:63-96` 的全部 flag，没有「选定历史 manifest 发布」的入口；该路径只在库层 `SilverRevisionPublisher.publish` 与回归测试 `test_rebuild_preserves_bytes_pinned_by_prior_revision` 里被走过）。因此：

- 现实可用的路径是**重跑 `rebuild-silver --full`**，让 carry-forward 依据受保护的 canonical 输入重新引用被保留的 artifact，产出 rev N+1。`[unverified]` —— 未在生产血统实跑。
- 任何真正的数据回退都需要**用户的具体数据变更批准**（verification doc §5 末段），列在 §8。

### 6.4 逐步中止条件汇总

| 步骤 | 中止信号 | 动作 |
|---|---|---|
| §1.3 | `launchctl list` 有 PID / `lsof` 有输出 | 等空窗，不抢锁 |
| §2.3 | `current.json` 打不开、revision ≠ 39、抽样 sha256 不匹配 | 停；**不刷新哈希** |
| §4.1 | `_copy_verified_legacy_artifact` `ValueError` | 停；legacy 校验失败会让整次发布失败，非 per-symbol |
| §4.1 | `failed` ≠ ~269 或 `window_regressions` ∉ {0,53} | 停，先解释 |
| §4.2 | 新 reader 读 rev39 报错 | §6.2 回滚 Apex，不 promote |
| §5.4/5.5 | 日志 `CI is not green` | 停；禁用 `--allow-unverified` |
| §5.5 | 日志 `pruned 23e1de28…` | 停；回退目标已丢 |
| §5.6 | `readlink` 非 `<MERGE_SHA>` | 停 |
| §7 | 新 writer 首夜 silver lane `outcome='failed'` | §6.1 代码回退；数据按 §6.3 且需另行批准 |

---

## 7. 首次正常运行验收（交接第 6 步）

**观察对象**：promote 之后的第一次 `com.livewire.daily-update`（05:00Z / 13:00 HKT），随后的 `intraday-catchup`（10:00Z）、`watchdog`（10:30Z）、`coverage`（11:00Z）。**手工迁移不算这一次运行**（verification doc §6）。

```bash
# 7.1 这一夜确实跑的是候选代码
ssh macmini 'readlink ~/market-warehouse/current'                    # <MERGE_SHA>

# 7.2 逐 lane 结果（docs/runbook.md:716）
ssh macmini 'source ~/market-warehouse/.venv/bin/activate && python ~/market-warehouse/current/scripts/livewire_ops.py ledger query "select lane, outcome, elapsed_s from lane_results where date(started) = current_date order by started"'
#   期望：LANE_ORDER 全员出现；silver outcome='ok'；无 outcome='blocked'/'timeout'

# 7.3 湖锁等待（docs/runbook.md:80,84）
ssh macmini '… ledger query "select scope as lane, round(value) as waited_s from measurements where name = '"'"'lake_lock_wait_s'"'"' and measured_at >= current_date order by value desc"'
ssh macmini '… ledger query "select lane, outcome, blocker from lane_results where blocker = '"'"'lake_lock'"'"' and date(started) = current_date"'
#   期望：第二条零行

# 7.4 每个 scope 的最新 session（逐 date 检查；docs/runbook.md:717）
ssh macmini '… ledger query "select scope, date '"'"'1970-01-01'"'"' + cast(value as int) as last_session from measurements where name = '"'"'last_session'"'"'"'

# 7.5 Silver 提交点前进了一格且成员数没塌
ssh macmini 'python3 -c "import json;d=json.load(open(\"/Volumes/DATA_LAKE/silver/revisions/current.json\"));print(d.get(\"revision\"), len(d.get(\"members\",[])))"'
#   期望：revision 40（= 39+1），members 13279（additions=0/withdrawals=0 的基线）

# 7.6 catalog 新鲜度（docs/runbook.md:852-855）
ssh macmini '… python ~/market-warehouse/current/scripts/livewire_store.py duckdb views'
ssh macmini '… duckdb freshness'
ssh macmini '… duckdb lag'
#   期望：views 齐全（缺视图 status 判 BAD）；freshness/lag 无 silver 落后

# 7.7 逐 symbol 抽查（真实 adapter 读，而不是只看 manifest）
ssh macmini '… python ~/market-warehouse/current/scripts/livewire_store.py duckdb bars --symbols NVDA AAPL SPY'   # docs/runbook.md:857
#   期望：三者都有当日 session；RJF 仍不可读（269 基线内，见 STEP3 §1）

# 7.8 一张打分表 + 告警投递
ssh macmini '… python ~/market-warehouse/current/scripts/livewire_ops.py status'
#   期望：无 BAD；`window_regressions=53` 与 `failed=269` 作为已知基线出现，不是新问题
#   告警未投递会被 status 判 WARN（根 CLAUDE.md：executions(script='send_alert', exit_code<>0)）
```

### 7.9 四个独立结论（不得合并成一句「全好了」）

| # | 结论 | 依据 | 允许的措辞边界 |
|---|---|---|---|
| 1 | 代码 / CI | `<MERGE_SHA>` 的 ci.yml run，加 Apex `74c29dbe2613597b3040ff8c7a25424278d0c50f` 的全绿 | 只说 CI；不代表数据健康 |
| 2 | 外盘故障行为（disposable 崩溃/文件系统） | 既有 receipt（`se08-*`），不因本次切换而更新 | SIGKILL ≠ 断电；单盘故障、备份恢复仍未证 |
| 3 | 部署 / 切换 | §5.6 `readlink`、§4.2 `docker inspect`、§7.5 manifest | 只说「运行的是这个版本、读的是这个 manifest」 |
| 4 | 生产正常运行结果 | §7.2–7.8 | 269/53 是已知基线不是「修好了」；STEP3 §3.4 的 8 项未测场景保持公开未知 |

---

## 8. 请求批准（Approvals requested）

以下动作**不可逆**或**改动用户/生产数据**，需要你在一条消息里点头。逐条编号，回一句「§8 全部批准」或指明哪几条批准即可。

1. **合并 Livewire PR #125（squash）到 `main`** —— 产生新 SHA，且 `main` 从此带候选 writer。
2. **卸载 `com.livewire.release-promote`**（`launchctl unload`），验收后再 load。期间**没有自动发布**。
3. **修改 `/Users/moremeds/apex-deploy/compose.yml`**（先备份为 `compose.yml.pre-cutover-20260908`）：把 apex-api 的 watchtower opt-in 改为 `false` / 把 `image` 钉到 digest，并 `docker compose up -d apex-api` 重建**仅此一个**容器。**不停** `xenon-watchtower-1`。
4. **在 mini 上跑一次 `rebuild-silver --full --dry-run`**（不发布，但会全量读湖、写一个 `--failure-output` JSON 到 `~/market-warehouse/logs/`，且**不持 lake 锁**）。
5. **`release promote --keep 6`** —— 真正翻转 `~/market-warehouse/current`，并使下一次 13:00 HKT daily 用新 writer 发布 Silver rev40（**这是本次唯一的生产数据写入授权**）。
6. **接受 269 failures 与 53 window regressions 作为切换后的告警基线**（STEP3 §4 决策 1、2）。
7. **合并 Apex PR #161（head `74c29db`）到 `master`**，等 stable/latest 镜像构建出 digest，再按第 3 条把 apex-api 钉到该 digest 拉起 —— 这是 reader 先于 writer 部署的前提。

8. **接受 STEP3 §3.4 的 8 项未测场景为已知未测风险**（冷缓存整轮、coverage 竞争、锁等待预算、reader 影响、断电、单盘故障、备份恢复、候选生产运行）——最后一项由本方案 §7 关闭，其余 7 项保持开放。

**明确不在本次批准范围**：任何 269/53 的数据修复或替换、RJF 替代来源的实际替换、`housekeeping --apply`、删除任何 release 或 lake 数据、`--allow-window-regression`、`--allow-unverified`、触碰 IB Gateway。

---

## 9. 命令出处与缺口

### 9.1 有出处

`release promote/list/rollback/gc`（`docs/runbook.md:881-884`，flag 见 `livewire_scripts/release.py:313-329`）· `git checkout main && git pull`（`docs/runbook.md:888`）· `launchctl load/unload`（`docs/runbook.md` §9 launchd install）· `lsof …/lake-io.lock`（`docs/runbook.md:77`）· `ledger query`（`docs/runbook.md:80,84,716,717`）· `status`（`docs/runbook.md:706`）· `rebuild-silver --full --dry-run --failure-output`（`docs/runbook.md:491-492`）· `duckdb views/freshness/lag/bars`（`docs/runbook.md:852-857`）· `gh run list --commit … --workflow ci.yml`（`livewire_scripts/release.py:84-96`）· `housekeeping`（`docs/runbook.md` §10）。

### 9.2 找不到出处 → 标 `[unverified]`

| 命令 | 情况 |
|---|---|
| `gh pr ready / gh pr merge --squash / gh pr view` | runbook 无；`gh` 只在 `release.py` 的 `ci_is_green` 里出现（且只有 `gh run list`） |
| 全部 `docker` / `docker compose` 命令 | Livewire runbook 不覆盖 Apex 部署；只以 2026-09-08 实读的 `/Users/moremeds/apex-deploy/compose.yml`（image 第 25 行、opt-in 标签第 68-69 行）为依据 |
| Silver「按历史 manifest 发布新 revision」的数据回退 | `rebuild_silver.py` 无此 flag；只有库层 `SilverRevisionPublisher.publish` + 回归测试。§6.3 给的是推断路径 |
| rev39 artifact 抽样校验 | 没有现成子命令；需按 manifest 相对路径手工 `shasum -a 256` |
| `current.json` 的绝对路径 `/Volumes/DATA_LAKE/silver/revisions/current.json` | 沿用 STEP3 附表；执行前用 `readlink ~/market-warehouse/data-lake` 复核 |

### 9.3 本轮发现的文档与代码冲突

1. **`promote` 会删掉旧 current** —— runbook §9 与根 `CLAUDE.md` 都只说「不要 `rm -rf` current 指向的 release」，没有提 `promote` 自己的 `prune(keep=3)` 在 `flip_current` 之后运行、旧 current 因此失去 `active` 保护。见 `release.py:270-276, 203-215`。本方案用 `--keep 6` 规避。
2. **`promote` 拒绝时退出码是 0** —— `release.py:247-253` 在 CI 不绿时只 `LOGGER.warning` 并 `return 0`。runbook 没有提醒「退出码不是信号」。
3. **`rebuild-silver` 不取 lake 锁** —— runbook §9 的注解「两个写者由代码而非时刻表串行，每个 lane 都持 `lake-io.lock`」只对 lane runner 成立；手工 `livewire_store.py rebuild-silver` 走的是另一条路，文件内无任何 flock。手工全量因此可以与定时 lane 并发。
4. **lane 顺序表述** —— 根 `CLAUDE.md` 六行架构把 `release-promote` 写在 coverage 之后，runbook §9 的时刻表把它排在最前（04:30Z，早于 daily 05:00Z）。以 runbook 时刻表为准。

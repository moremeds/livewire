# Evidence — 2026-09-12 notify rewrite

Audit trail per plan contract rule 3: task number, every command run, its exit
code, last 5 lines of output. Commands on the MacBook unless marked `[mini]`.

## T0 — production facts for the notify rewrite

### Setup (worktree + plan copy; correction (b) — plan file is untracked on main)

```
$ git worktree add .worktrees/notify-rewrite -b notify-rewrite origin/main \
    && mkdir -p .worktrees/notify-rewrite/docs/superpowers/plans \
    && cp docs/superpowers/plans/2026-09-12-notify-rewrite.md .worktrees/notify-rewrite/docs/superpowers/plans/
exit 0
Preparing worktree (new branch 'notify-rewrite')
branch 'notify-rewrite' set up to track 'origin/main'.
HEAD is now at 4cffb2c fix(raw): skip AppleDouble sidecars when validating a staged Massive raw date (#126)
```

### Step 1 — coverage schedule as loaded `[mini]`

```
$ ssh macmini 'launchctl print gui/$(id -u)/com.livewire.coverage | grep -A3 -i "calendar\|hour\|minute"'
exit 0
		descriptor = {
			"Minute" => 0
			"Hour" => 19
		}
	}
--
		"com.apple.launchd.calendarinterval" = {
			port = 0xb51db
			active = 0
			managed = 1
```

**Answer: coverage runs at Hour 19 / Minute 0 local (HKT) = 11:00Z.** Matches the
plan's expected value, not the plist example's 15:30Z comment. Task 8's digest
at 12:15Z (HKT 20:15) stands; runbook's 11:00Z is confirmed correct.

### Step 2 — mail env present in the warehouse env `[mini]`

```
$ ssh macmini 'grep -c "^MDW_ALERT_" ~/market-warehouse/.env; grep -o "^MDW_ALERT_[A-Z_]*" ~/market-warehouse/.env'
exit 0
7
MDW_ALERT_EMAIL_FROM
MDW_ALERT_EMAIL_TO
MDW_ALERT_SMTP_HOST
MDW_ALERT_SMTP_PORT
MDW_ALERT_SMTP_SECURE
MDW_ALERT_SMTP_USER
MDW_ALERT_SMTP_PASS
```

**Answer: 7 `MDW_ALERT_*` keys present (≥4 ✔).** Key names only, for the Task 11
runbook table: `MDW_ALERT_EMAIL_FROM`, `MDW_ALERT_EMAIL_TO`,
`MDW_ALERT_SMTP_HOST`, `MDW_ALERT_SMTP_PORT`, `MDW_ALERT_SMTP_SECURE`,
`MDW_ALERT_SMTP_USER`, `MDW_ALERT_SMTP_PASS`. Absent from `.env`:
`MDW_ALERT_EMAIL_CC`, `MDW_ALERT_EMAIL_BCC`, `MDW_ALERT_EMAIL_REPLY_TO`,
`MDW_ALERT_EMAIL_SUBJECT_PREFIX`, `MDW_ALERT_SMTP_URL`, `MDW_ALERT_TRANSPORT`
— send_mail.mjs must work without them.

### Step 3 — node path in the release environment `[mini]`

```
$ ssh macmini 'grep -n "MDW_NODE_BIN\|PATH" ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist; which node; ls /opt/homebrew/bin/node'
exit 0
41:        <key>PATH</key>
node not found
/opt/homebrew/bin/node
```

Follow-up (same step, captures the PATH value the first grep truncated):

```
$ ssh macmini 'grep -n -A2 "<key>PATH</key>" ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist; grep -c "MDW_NODE_BIN" ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist'
exit 1  (second grep found 0 matches of MDW_NODE_BIN — count output "0", grep exits 1 on no match; first grep exited 0)
41:        <key>PATH</key>
42-        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
43-    </dict>
0
```

**Answer: resolved node path under launchd is `/opt/homebrew/bin/node`.** The
watchdog plist sets `PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin` (so
`shutil.which("node")` succeeds in that environment), sets no `MDW_NODE_BIN`,
and a bare ssh PATH has no node at all ("node not found"). Task 2's resolver
(`MDW_NODE_BIN` → `which` → `/opt/homebrew/bin/node`) reaches the right binary
in all three environments.

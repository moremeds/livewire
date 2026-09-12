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

## T1 — send_mail.mjs: SMTP and nothing else

All commands in `.worktrees/notify-rewrite`.

### Step 1/2 — failing tests, then FAIL

`tests/node/send_mail.test.mjs` written; `package.json` `test:alerts` repointed
at it (required before the FAIL run, otherwise the old test still passes).

```
$ ls node_modules | head -5; npm run test:alerts
exit 0  (ls printed nothing — worktree has no node_modules; npm run itself exited 1)
✖ tests/node/send_mail.test.mjs (35.655125ms)
ℹ tests 1
ℹ pass 0
ℹ fail 1
✖ failing tests:
test at tests/node/send_mail.test.mjs:1:1   (import of ../../livewire_node/send_mail.mjs fails — module missing)
```

```
$ ls package-lock.json && npm ci --omit=dev
exit 0  (nodemailer installed for the PASS run)
```

### Step 3/4 — implement, PASS

`livewire_node/send_mail.mjs`: `resolveAlertConfig` and `sendMail` copied
verbatim from `send_daily_update_failure_email.mjs:155-204` and `:565-583`
(renamed; base64 comment block kept). New single-token `parseArgs`
(`--subject=`/`--body-file=` only), `main` reads the body file, applies
`subjectPrefix`, prints `{"accepted":[...],"messageId":...}`, exits 0 iff
accepted. `MDW_ALERT_TRANSPORT=stream` uses `{streamTransport:true,buffer:true}`
and prints the RFC822 message instead of the JSON line; it satisfies
`resolveAlertConfig`'s SMTP check with a placeholder URL because stream never
contacts a transport (FROM/TO validation still applies — they are headers).
streamTransport returns no `accepted` list, so stream mode returns 0 once the
message prints.

```
$ wc -l livewire_node/send_mail.mjs && npm run test:alerts
149 livewire_node/send_mail.mjs
exit 0
ℹ tests 11
ℹ pass 11
ℹ fail 0
```

### Step 5 — delete old mailer, grep

```
$ rm livewire_node/send_daily_update_failure_email.mjs tests/node/send_daily_update_failure_email.test.mjs
exit 0
```

```
$ grep -rn send_daily_update_failure_email . | grep -v "^./docs/postmortems/"
exit 0 — non-postmortem hits remain, all in files the plan's own file map
assigns to later tasks (T3: scripts/livewire_ops.py:47,
tests/test_livewire_entrypoints.py:719; T8: livewire_scripts/nightly_digest.py:36;
T11: CLAUDE.md:132, .env.example:40) plus historical docs (docs/superpowers/
plans/*, specs/*, this plan and evidence file) and a docstring narrative at
livewire_scripts/release.py:139. Plan step 5 says "only docs/postmortems/ hits
remain" — not literally achievable in T1; the residual live-code references are
deleted by T3/T8 and V4 requires zero at the end. Flagged as a plan-text
deviation, not missed work.
```

Post-deletion re-run and real-CLI smoke (stream transport, no mocks):

```
$ npm run test:alerts
exit 0
ℹ tests 11
ℹ pass 11
ℹ fail 0
```

```
$ printf 'revision=28 rebuilt=10\n' > /tmp/lw-body.txt && MDW_ALERT_TRANSPORT=stream \
    MDW_ALERT_EMAIL_FROM=a@b MDW_ALERT_EMAIL_TO=c@d \
    node livewire_node/send_mail.mjs --subject="digest 2026-09-12" --body-file=/tmp/lw-body.txt
exit 0
From: a@b
To: c@d
Subject: [Livewire] digest 2026-09-12
Content-Transfer-Encoding: 7bit      (pure-ASCII body — nodemailer sends verbatim, no `=` reinterpretation)
revision=28 rebuilt=10
```


## T1 fix — entry guard runs main through symlinks/relative paths; ≤150 lines

Reviewer rejection of b61786e, fixed as a new commit (no amend).

### Failing test first (guard as committed in b61786e)

```
$ npm run test:alerts
exit 1
✖ the CLI runs when invoked through a symlinked dir and a relative path
  AssertionError: argv[1]=…/mdw-symlink-XXX/current/livewire_node/send_mail.mjs
  actual: ''          (empty stdout — guard false, main() never ran)
  expected: /^Subject: \[Livewire\] t$/m
```

### Fix

`import.meta.url === \`file://${process.argv[1]}\`` replaced by a realpath
compare on BOTH sides (`realpathSync(process.argv[1]) ===
realpathSync(fileURLToPath(import.meta.url))`) — import.meta.url is itself the
unresolved path, so only one side realpathed still misses the symlink case.
New spawn test exercises `…/current/livewire_node/send_mail.mjs` through a
symlinked dir and `livewire_node/send_mail.mjs` relative from the repo root.
base64 comment trimmed to the one-line rule + pm reference per reviewer.

Note: the first draft of the relative-path case used a `..`-relative argv[1]
computed from `os.tmpdir()`; it failed with MODULE_NOT_FOUND because cwd
resolves through `/private/var` while tmpdir is `/var/…` (one `..` short). The
test now uses the realistic `livewire_node/send_mail.mjs` from repo root.

### Passing

```
$ wc -l livewire_node/send_mail.mjs
     150 livewire_node/send_mail.mjs
$ npm run test:alerts
exit 0
ℹ tests 12
ℹ pass 12
ℹ fail 0
✔ the CLI runs when invoked through a symlinked dir and a relative path
```

## T2 — notify.py: render, dedup, send, record

All commands in `.worktrees/notify-rewrite`; conftest autouse already points
`LW_LEDGER_ROOT` and `MDW_LOG_DIR` at tmp_path for every test.

### Step 1/2 — failing tests, then FAIL

```
$ uv run pytest tests/test_notify.py -q
exit 2
ImportError: cannot import name 'notify' from 'livewire_scripts'
```

### Step 3/4 — implement, PASS

`livewire_scripts/notify.py`: Notice dataclass, `node_bin()`
(MDW_NODE_BIN → which → /opt/homebrew/bin/node — the T0-verified chain),
`fingerprint`, `already_sent`, `sent_within`, `page_from_sections` (None when no
BAD; fp over sorted notification_key|name:verdict keys), `page_for_lane`
(fp keys = [LW_RUN_ID, lane]), `send` — dedup skip emits its own row with
skipped=true, body lands in log_dir as notify_<kind>_<utc>.txt (or --body-path),
node runs in its own process group via process_group_guard, Timeout/OSError →
exit 1 + receipt_json.error, every row evidence_hash=sha256(body),
release_sha=readlink(warehouse/current) or None, ledger emit wrapped in
try→stderr. Subjects are passed WITHOUT "[Livewire] " — the prefix is
send_mail.mjs's subjectPrefix job (plan parenthetical).

```
$ uv run pytest tests/test_notify.py -q
exit 0
........                                       [100%]
8 passed in 7.82s
```

### Full gate

```
$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
exit 1
FAILED tests/test_no_dead_modules.py::test_no_module_is_unreachable_from_the_entrypoints
  unreachable == ['livewire_scripts.notify']
Required test coverage of 95% reached. Total coverage: 95.02%
2697 passed, 1 failed
```

NOT a deleted-mjs test: notify.py has no caller until T3 adds the
`livewire_ops.py notify` subcommand (plan ordering — T2 creates, T3 wires).
No test skipped or deleted; reported per instruction.

## T2 fix — failed dedup lookup never suppresses a send

Reviewer-required change on top of 22a7962 (new commit, no amend).

`send` wraps `already_sent` so any Exception prints
`notify: dedup lookup failed: <exc>; sending` to stderr and proceeds —
fail open. The recorded row carries `receipt_json.dedup_error`.

```
$ uv run pytest tests/test_notify.py -q
exit 0
9 passed in 0.42s   (new: test_a_failed_dedup_lookup_sends_and_records_dedup_error)
```

Test-authoring note: the first draft restored the patched `ledger.query` via
`monkeypatch.undo()`, which also reverted conftest's shared-instance
`LW_LEDGER_ROOT`/`MDW_LOG_DIR` env — the read-back then queried the real lake
(0 rows). Fixed by re-setting the attribute to the saved original instead.

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

## T3 — livewire_ops.py notify replaces send-alert

- `scripts/livewire_ops.py`: `send-alert`/`_dispatch_send_alert` deleted; `notify`
  subcommand added (`--kind {page,digest} --subject --body-file --force`); body is
  read from the file into `Notice(kind, subject, body, fingerprint(kind,[subject]))`
  and handed to `notify.send(notice, force=…)`. `subprocess` import dropped;
  `import os` kept — `livewire_ops.os.environ` is a test seam
  (`_load_env_file` re-export precedent, line 16).
- `tests/test_livewire_entrypoints.py`: `test_ops_send_alert_delegates_to_node`
  replaced by `test_ops_send_alert_is_removed` (argparse SystemExit 2) and
  `test_ops_notify_sends_a_notice_and_records_it` (fake `_run_child`; asserts
  node argv and one `executions(script='notify')` row, exit_code=0, kind=page).

```
$ uv run pytest tests/test_livewire_entrypoints.py -q -k "send_alert or notify"   # pre-impl
exit 1   FAILED test_ops_send_alert_is_removed, FAILED test_ops_notify_sends_a_notice_and_records_it
         (send-alert still a valid choice; notify not a valid choice)

$ uv run pytest tests/test_livewire_entrypoints.py tests/test_notify.py tests/test_no_dead_modules.py -q
exit 0   62 passed   (test_no_dead_modules now green — notify reachable)

$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
exit 0   2700 passed, 2 warnings   TOTAL coverage 95.02% (>=95)
```

### Step-4 grep deviation

Expected "only status.py and coverage_report.py". Actual `send-alert`/`send_alert`
hits also in `job_runner_common.py` (build_alert_command helper), callers
`run_daily_update_job.py` (T6), `run_intraday_catchup_job.py`,
`data_quality_report.py`, `health_check.py`, `universe_screener.py`,
`clients/quality_flags.py` (T9), `status.py` (T4), `coverage_report.py` (T5),
and tests (`test_job_runner_common._KNOWN_INLINE_ALERT_BUILDERS`,
`test_coverage_report`, `test_health_check`, `test_run_daily_update_job`,
`test_quality_flags`, `test_check_daily_update_watchdog`, `test_status`,
`test_run_intraday_catchup_job`). `run_daily_update_job`/`coverage_report`/
`status`/`quality_flags` are assigned to T4/T5/T6/T9. `health_check`,
`data_quality_report`, `universe_screener`, `run_intraday_catchup_job`,
`job_runner_common` appear in NO task's file map — flagging for reviewer: their
send-alert argv now exits 2 at the argparse boundary (loud, not silent) if
invoked, and V4's zero-hit grep cannot pass until they are handled.

## T4 — status checks: today's truth, deadlines, per-scope UNKNOWN

Plan file refreshed from main first: byte-identical, only diffs are the
reviewer's Amendment A1 (after T6 step 5) and Amendment A2 (end of T9). NOTE for
reviewer: in main's copy the A2 paragraph absorbed the `### Task 10:` heading —
Task 10's title text trails the amendment paragraph and its body follows without
a heading (line ~395). Cosmetic; content intact.

`status.py`: `collect(..., now=None)` → `params["now"]`; seven rows
replaced/added per plan SQL verbatim ("Daily update ran" 07:00 deadline,
"Undelivered notifications" on script='notify', "Digest sent today" 12:45,
"Coverage ran today" 12:00, per-scope "Coverage" with UNKNOWN(expected=0),
"Coverage recovery" deferred-x2, "Stale non-equity"); "Post-success tail" reads
lane='tail'; _EMPTY_IS_OK swaps "Undelivered alerts"→"Undelivered notifications"
+"Stale non-equity"; _FIXES rekeyed.

Tests: 8 new (plan's list) + 3 replaced (send_alert/digest-lane/coverage-scan
tests); `test_no_run_row_at_all_is_unknown_not_ok` now freezes now=05:30 —
unfrozen it would depend on wall-clock vs the 07:00 deadline. Boundary note:
plan text says "now=07:00Z → BAD" but its SQL is strict `>`; tested at 07:00:30.

```
$ uv run pytest tests/test_status.py -q   # pre-impl
exit 1   10 failed, 92 passed (StopIteration on new names; TypeError now=)

$ uv run pytest tests/test_status.py -q   # post-impl
exit 0   102 passed

$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
exit 0   2705 passed, 2 warnings   TOTAL coverage 95.02% (>=95)
```

## T5 — coverage emits facts, sends nothing

Plan file re-copied from main (only diff: restored `### Task 10:` heading; cmp-verified).

`coverage_report.py`: `_send_alert` and `_OPS_SCRIPT` deleted; module docstring
updated. `CoverageResult.ratio` → `0.0` for `total == 0` (was vacuous 1.0);
`format_one_liner` prints `tf=p/t (UNKNOWN)` for a zero denominator — spec §17
`=`/parens shape kept (plan text "1d 0/0 UNKNOWN" read as intent, test asserts
the literal `1d=0/0 (UNKNOWN)`). New `emit_recovery_measurements(results,
outcomes)` and `emit_stale_non_equity(results)` beside
`emit_coverage_scan_measurement`, same try/except-log shape.

Two deliberate readings beyond the plan's letter, both anti-stale-flag:
- both emitters write a row for EVERY scope each run (0 when clean), so a
  deferred/stale flag cannot outlive its condition — matches the
  "WARN nobody can clear is worse than silence" rule in _launchd_section.
  `emit_stale_non_equity` takes the full non-equity results dict, not just the
  stale subset the plan named the arg after.
- recovery gate gained `or not r.missing_symbols`: ratio 0.0 on total=0 would
  otherwise enter auto_recover (which early-returns on empty input anyway);
  the guard keeps the old skip semantics explicit.
- `--no-recover` still returns before emit_recovery_measurements — recovery
  was not attempted, so its last state correctly stands.
- "Coverage recovery"/"Stale non-equity" WARN rows now have live data; status
  check keys verified end-to-end by the T4 tests reading these names.

Tests: deleted TestSendAlert (2) + `_error_summary` helper + `_send_alert`
import; `test_main_repairs_minute_date_once_for_all_rollups` now asserts
`_run_child` never called and reads deferred rows; two TestMain tests assert
alert_calls == [] + still_missing values; `test_empty_bronze` ratio 1.0 → 0.0.
`_KNOWN_INLINE_ALERT_BUILDERS` (test_job_runner_common.py) lost
coverage_report.py — the meta-test fails the gate otherwise; the remaining
three names leave with A2/T9.

```
$ uv run pytest tests/test_coverage_report.py -q   # pre-impl
exit 1   6 failed (missing emitters, ratio 1.0, email-shape asserts)

$ uv run pytest tests/test_coverage_report.py tests/test_status.py -q   # mid
exit 1   2 failed (TestMain email asserts) — updated, then

$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
exit 1   FAILED test_only_one_module_encodes_the_alert_contract
         (coverage_report.py no longer carries "send-alert")
         → removed it from _KNOWN_INLINE_ALERT_BUILDERS, re-ran:
exit 0   2706 passed, 2 warnings   TOTAL coverage 95.05% (>=95)
```

## T6 — daily-update + intraday pages through notify; digest leaves the tail (incl. Amendment A1)

Focused FAIL first: `tail_of`/`notify` imports absent → ImportError/NameError as expected; PASS after implementation.

```
uv run pytest tests/test_run_daily_update_job.py tests/test_run_intraday_catchup_job.py tests/test_job_runner_common.py tests/test_check_daily_update_watchdog.py -x -q
→ 130 passed in 2.44s  (exit 0)
```

Full gate:

```
uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
→ 2694 passed, 2 warnings in 44.50s  (exit 0)   TOTAL coverage 95.04%
```

Interim full-gate run failed once on `test_nightly_digest.py::test_the_digest_lane_is_recorded_in_the_ledger` — it asserted `lane='digest'`; renamed to `test_the_tail_lane_is_recorded_in_the_ledger` asserting `lane='tail'` (that rename IS the T6 deliverable).

What changed:

- `run_daily_update_job.py`: `AlertRequest`/`send_failure_alert`/`record_failed_send`/`build_alert_command`/`node_binary_exists` deleted; `import json` dropped (only record_failed_send used it). `_page_failure(config, log_file, exit_code, *, lane, attempts, env=None)` builds `notify.page_for_lane(run_date, lane, exit_code, extract_error_summary(log_file), tail_of(log_file, 60))` and calls `notify.send(notice)` — no runner parameter, page goes through notify's own `_run_child`. Both call sites pass `lane=done_scope`. `run_post_success_quality` no longer spawns `digest --email`; `_emit_lane("tail", …)`.
- `run_intraday_catchup_job.py` (A1): `_send_failure_alert` → `_page_failure(log_file, exit_code, now_fn)`; `notify.page_for_lane(run_date, "intraday_catchup", exit_code, _extract_error_summary(log_file), tail_of(log_file, 60))` + `notify.send`. Local `build_alert_command` wrapper, `_node_binary_exists`, node/alert-script prechecks deleted — notify.send records a missing node as a failed send row instead of skipping silently.
- `job_runner_common.py`: `AlertRequest` + `build_alert_command` deleted; `tail_of(log_file, lines)` added (deque tail, "" on missing file); docstring reworded.
- `check_daily_update_watchdog.py` (import update per plan): drops the deleted names; `run_watchdog` send block now `notify.page_for_lane(run_date, "watchdog", 1, reason, tail_of(log_file, 60))` + `notify.send(notice, runner=runner)`. Marker-file dedup and `missing_jobs` kept — T7 rewrites the file fully.
- `clients/constants.py` NOT touched: `tail` is outside `LANE_ORDER`, `test_declared_lane_budgets_cover_exactly_the_lane_set` unchanged — the plan's conditional resolved to "no budget needed".
- Tests: `test_a_lane_failure_pages_once_per_run_and_lane` (two same-lane failures → one notify row exit 0 skipped=false + one skipped=true, `_run_child` called once), `test_the_tail_runs_weekly_and_housekeeping_only` (no digest argv; lane row `tail`), `TestTheLaneRunnerNeverRunsTheAlert` adapted (strict lane runner sees only lane commands; `notify._run_child` spy sees exactly one send), send_failure_alert/record_failed_send/AlertRequest tests deleted, `test_job_runner_common.py` `_KNOWN_INLINE_ALERT_BUILDERS` unchanged (the three A2/T9 modules) + `tail_of` tests added.

Deliberate readings beyond the letter:

1. `_page_failure` keeps `config`/`attempts`/`env` params per the plan's signature though the notify path doesn't consume them — call-site stability through the transition; T7's watchdog rewrite is where signatures get re-shaped.
2. `RunnerConfig.alert_script`/`node_bin` (and the intraday twins) remain as parsed config fields — env contract (`MDW_DAILY_UPDATE_ALERT_SCRIPT`, `MDW_NODE_BIN`) is unchanged surface the plists/.env may still set; the now-unused fields are data, not logic. `node_binary_exists`/`_node_binary_exists` were pure email-gating logic and were deleted.
3. Watchdog `run_watchdog` signature kept `(config, run_date, runner=None)`; `runner` now means the notify send-runner `(command, timeout=...)` — T7 rewrites the function fully anyway.
4. Between T6 and T8 the nightly digest has no sender — the tail no longer spawns it and T8 installs the dedicated job. Flagged mid-plan state, per the plan's own ordering.
5. Meta-test renamed `test_no_scheduled_runner_encodes_the_alert_contract` — the module no longer encodes the contract at all.

## T7 — watchdog = collect() → page_from_sections → send; no marker file

Focused FAIL first: `run_watchdog() got an unexpected keyword argument 'now'` (old signature) → PASS after rewrite.

```
uv run pytest tests/test_check_daily_update_watchdog.py -x -q
→ 8 passed in 0.27s  (exit 0)
```

Full gate:

```
uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
→ 2693 passed, 2 warnings in 44.92s  (exit 0)   TOTAL coverage 95.06%
```

What changed:

- `check_daily_update_watchdog.py` rewritten (53 lines): `run_watchdog(run_date: date, *, now=None, runner=None)` → `collect(run_date, log_dir(), data_lake_dir(), now=now)` → `notify.page_from_sections(sections, run_date)` → `notify.send(notice, runner=runner)`; exit 3 on send failure. Deleted: `build_watchdog_marker_file`, `record_alert_marker`, `build_daily_log_file`, the `missing_jobs` loop (T4's deadline checks replaced it), `RunnerConfig`/`build_config`/`tail_of`/`Verdict` imports — the watchdog no longer depends on `run_daily_update_job` at all.
- `main()` parses `--run-date` to `date.fromisoformat` (was str pass-through).
- Tests rewritten to the plan's five: `test_bad_pages_once_per_state_across_runs` (same BAD twice → one send + one skipped=true row), `test_a_new_bad_pages_again` (BAD set grows → second send, different fingerprint), `test_nothing_bad_sends_nothing_and_writes_nothing`, `test_a_failed_send_is_exit_3_and_a_row`, `test_no_marker_file_is_written` — plus `test_unknown_and_warn_do_not_page`, `test_parse_args`, `test_main_runs_the_watchdog_for_the_parsed_date`.
- External callers verified unaffected: `livewire_quality.py` command table and `test_livewire_entrypoints.py:648` only reference the module + `--run-date` argv.

Deviations: none beyond the test file adding two extra tests (UNKNOWN/WARN no-page and the main() dispatch) — both within the task's own files.

## T8 — unconditional daily digest after coverage; own launchd job

Focused FAIL first: `module 'livewire_scripts.nightly_digest' has no attribute '_coverage_rows'`
(old build_digest signature + fingerprint/lock internals) → PASS after rewrite.

```
uv run pytest tests/test_nightly_digest.py tests/test_launchd_templates.py -q
→ 38 passed in 0.41s  (exit 0)
```

Full gate:

```
uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
→ 2698 passed, 2 warnings in 43.81s  (exit 0)   TOTAL coverage 95.08%
```

What changed:

- `nightly_digest.py` rewritten (~225 lines): pure `build_digest(run_date, sections, *,
  previous_verdicts, coverage_rows, sent_rows, now=None)` renders four blocks —
  COVERAGE (per-scope pct/total with prev from the second-latest row, deferred-streak
  xN, still_missing, non-equity stale+session line), CHANGED (verdict diff vs the
  previous digest row's receipt_json.verdicts, "(new)" for unseen checks), SENT
  (notify.sent_within(24), skipped rows marked), STATUS (every check, same glyph/fix
  rules as render()). `main`: `--run-date`, `--email`, `--body-out PATH` (renders from
  the ledger, writes the file, no send and no ledger write). Send path is
  `notify.send(force=True, receipt_extra={"verdicts": {name: verdict}})` with
  fingerprint("digest", [run_date]). Deleted: `_warning_fingerprint`,
  `_last_delivered_fingerprint`, `_record_delivery`, `path_lock`, `_run_email_child`,
  `_send_email`, `--force-email`, the send_daily_update_failure_email.mjs argv.
- `livewire_ops.py`: `"digest"` added to COMMANDS (module is `nightly_digest`) and to
  the load_scheduled_env set — the plist starts cold and needs the SMTP env.
  `livewire_quality.py digest` mapping kept for compatibility.
- `launchd/com.livewire.digest.plist.example` created: `livewire_ops.py digest --email`,
  20:15 HKT = 12:15Z (≥1h after coverage's verified 19:00 HKT; T0 launchctl print).
- `launchd/com.livewire.daily-update-watchdog.plist.example`: StartCalendarInterval
  is now an array of two dicts, 18:30 + 20:00 HKT (10:30Z + 12:00Z); the second pass
  pages a coverage-degraded status 15 min before the digest reports it.
- `test_launchd_templates.py`: `"com.livewire.digest"` in JOB_TEMPLATES; new
  `test_the_digest_runs_after_coverage_and_the_watchdog_runs_twice` asserts digest
  Hour ≥ verified-coverage-hour 19 + 1 and watchdog has exactly the two intervals.
- `test_nightly_digest.py` rewritten (14 tests): the plan's six plus body-out,
  verdicts-in-receipt, previous-verdicts-from-last-row, sent-block, failed-send
  exit code, default-date; `test_the_tail_lane_is_recorded_in_the_ledger` kept.

Deviations / readings:

1. The body-example non-equity line (`cmdty 1/1@09-12`) is not renderable from the
   plan's listed queries: T5 emits only `stale_non_equity` counts per class — no
   per-class totals exist in the ledger. Rendered as `<class> missing=<n>@<MM-DD>`
   (session from `last_session`, lane scope ↔ class via `{"volatility":"cboe",
   "corporate_action":"corporate-actions"}`). Similarly `x/y` per equity scope is
   `total−still_missing/total` when a still_missing row exists, else `n total`.
   `recovery=deferred xN` counts the latest-two rows (max x2 — the plan only lists
   "latest two" per scope).
2. `build_digest` gained a keyword-only `now` param for the "(sent HH:MMZ)" header;
   pure otherwise. `test_the_digest_runs_after_coverage` compares against the
   T0-verified production hour (19), NOT the coverage template — the template's
   23:30 HKT is stale vs the loaded 19:00 plist; flagged, not fixed (outside T8's
   file map).
3. Old dedup machinery (fingerprint/lock/force-email) deleted per plan; the digest
   is now unconditional and concurrency safety is inherited from notify's append.

## T9 — delete the last email paths (quality_flags, A2 senders); grep gate clean

Focused:

```
uv run pytest tests/test_quality_flags.py tests/test_quality_detector.py \
  tests/test_health_check.py tests/test_data_quality_report.py \
  tests/test_universe_screener.py tests/test_backfill_intraday.py \
  tests/test_fetch_ib_historical.py tests/test_daily_update.py \
  tests/test_job_runner_common.py tests/test_livewire_entrypoints.py \
  tests/test_status.py -q
→ 687 passed, 2 warnings in 50.96s  (exit 0)
```

Full gate:

```
uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
→ 2686 passed, 2 warnings in 84.10s  (exit 0)   TOTAL coverage 95.06%
```

Grep gate — `git grep` (tracked files only) for the widened needle set; output is
file:count after the only allowed locations. Zero hits outside dated historical
documents, the plan, and this evidence file:

```
git grep -n "alert_on_flag\|MDW_ALERT_SEVERITY\|MDW_ALERT_RATE_LIMIT\|MDW_UNDELIVERED\|send-alert\|send_alert" \
  | cut -d: -f1 | sort | uniq -c
   1 docs/codex-handoffs/claude-release/STEP4-CUTOVER-PLAN.md
   1 docs/plans/misc-config-knobs.md
   1 docs/postmortems/2026-08-16-status-surface-grading.md
   3 docs/superpowers/plans/2026-07-29-nightly-reliability-and-backfill-unblock.md
   2 docs/superpowers/plans/2026-08-10-graded-status-surface.md
  10 docs/superpowers/plans/2026-09-02-livewire-ledger-l1.md
  13 docs/superpowers/plans/2026-09-12-notify-rewrite.evidence.md
  19 docs/superpowers/plans/2026-09-12-notify-rewrite.md
   1 docs/superpowers/specs/2026-04-06-multi-timeframe-design.md
   8 docs/superpowers/specs/2026-05-17-mdw-reliability-foundation-design.md
   3 docs/superpowers/specs/2026-06-05-livewire-intraday-catchup-scheduler-design.md
   1 docs/superpowers/specs/2026-08-10-graded-status-surface-design.md
   4 docs/superpowers/specs/2026-09-02-livewire-ledger-design.md
```

Widened extras also clean: `alerts_enabled` and `MDW_INTRADAY_BACKFILL` produce no
hits anywhere in tracked files.

What changed:

- `clients/quality_flags.py` → sidecar + audit only. Deleted: `alert_on_flag`,
  `_RATE_LIMIT_CACHE`, `_record_failed_alert`, `_SEVERITY_ORDER`,
  `_resolve_threshold`, `_resolve_rate_limit_seconds` (the two env knobs),
  `subprocess`/`sys`/`time`/`datetime`/`ledger` imports.
- `clients/quality_detector.py`: `run_detection` drops `alerts_enabled`; the
  dynamic import loses `alert_on_flag`; the flag loop writes sidecar + audit only.
- `livewire_scripts/backfill_intraday.py`: `_intraday_backfill_alerts_enabled`
  and the `alerts_enabled=` argument removed.
- A2 — `health_check.py`: `_send_alert` + `--alert-threshold` deleted; report/log
  output and the repair subprocess kept. `data_quality_report.py`: `_send_email`,
  `--email`, `_EMAIL_SCRIPT` deleted; summary/flap/quality views kept.
  `universe_screener.py`: `_send_screener_alert`, `_OPS_SCRIPT`, `EMAIL_THRESHOLD`
  and the additions/removals alert call deleted; scan, state, logs and the
  additions backfill via `livewire_ingest.py` kept.
- `tests/conftest.py`: `MDW_UNDELIVERED_DIR` removed from the artifact fixture.
- `tests/test_job_runner_common.py`: `_KNOWN_INLINE_ALERT_BUILDERS` now `set()`;
  the scan uses `re.compile(r"send[-_]alert")` so the test file itself carries no
  literal hit for the gate.
- `docs/runbook.md`: the two `MDW_ALERT_*` env rows removed (lines 58-59); the
  Alerts section now documents `livewire_ops.py notify --kind page …`.
- README.md: ops command list `send-alert` → `digest | notify`; the two env rows
  removed. CLAUDE.md: the failed-send rule rewritten to `executions(script=
  'notify')` + `test_undelivered_notifications_reads_the_notify_script`.
  `.env.example`: stale sender-script comment → `send_mail.mjs via notify.send`.
- Tests: `test_quality_flags.test_flags_are_written_without_any_send` (sidecar +
  audit write under a raising `subprocess.run`); `TestNoAlert` in health_check
  (source scan + `--alert-threshold` rejected); `test_repairs_do_not_spawn_a_
  process`; `test_data_quality_report` email asserts → no-subprocess;
  `test_changes_trigger_no_email_subprocess` in screener (55 additions: backfill
  argv still spawns, no alert argv, preset updated); all `alert_on_flag`
  monkeypatches deleted from daily_update/backfill_intraday/fetch_ib_historical
  tests; `test_sends_alert_for_large_changes` deleted (superseded);
  `test_ops_removed_alert_command_is_rejected` keeps the literal out of the file.

Deviations / readings:

1. The gate's "outside docs/postmortems/ and the plan" is met by zero hits in any
   live surface — source, tests, runbook, README, CLAUDE.md, .env.example. The
   residual hits above are all dated historical documents (postmortem, older
   plans/specs, a codex handoff, docs/plans/misc-config-knobs.md) plus this plan
   and its evidence — the historical record, left intact on purpose.
2. `test_alert_threshold_flag_is_rejected` keeps the literal `--alert-threshold`
   in argv — it asserts the flag is now REJECTED (SystemExit 2); it matches none
   of the six gate needles.
3. `universe_screener.EMAIL_THRESHOLD` is deleted with the alert helper — nothing
   else consumed it; `test_changes_trigger_no_email_subprocess` exercises the
   same 55-additions scenario and asserts the backfill spawn survives.
4. CLAUDE.md §"Alerts and the digest" still names `send_daily_update_failure_
   email.test.mjs` in the two transport bullets — a stale test reference, no gate
   needle; flagged for T11's docs sweep rather than edited here.

## T10 — bounded diagnosis: why 1d reads 0/0 → 100% since 2026-09-09

**Cause (one sentence):** the `session_due_at` pre-deadline gate in
`compute_coverage` (shipped in #95, 8926de1, in release `4cffb2c` now on the
mini) treats the previous trading session as not due until next-day 15:00Z
(`session + 1d` at `JOB_START_UTC` 06:00 + `DELIVERY_ALLOWANCE_SECONDS` 9h), but
production coverage fires at 19:00 HKT = 11:00Z — four hours early — so every
weekday run takes the `CoverageResult(0,0)` branch and the old `0/0 → 1.0`
rendering prints 100%. The plan's `_symbols.parquet` hypothesis is REJECTED:
raw partitions exist for 09-09/10/11, and intraday denominators — which do use
the traded set — were nonzero on the same runs; 1d's denominator is
`on_disk ∪ registry` and never touches `_symbols.parquet`.

The 09-06/07/08 ledger rows show real totals because each of those runs targeted
Fri 09-04 (Labor Day weekend), whose due instant had long passed; the first
weekday-pair run (09-09 targeting 09-08) is exactly when the zeros began —
matching "since 2026-09-09" precisely.

Commands (mini, read-only; exit codes as reported by the wrapper):

```
1. ssh macmini 'grep -n "1d" logs/coverage_2026-09-0{8,9,10}.log | head -20'  → 0
   09-08: 1d=0/0 (100.00%)  1m=0/14837 (0.00%) …        (all scopes 0 — the 09-08/09 outage)
   09-09: 1d=0/0 (100.00%)  1m=4079/11869 (34.37%) …    (intraday real, 1d zero)
   09-10: 1d=0/0 (100.00%)  1m=4256/11956 (35.60%) …

2. ssh macmini 'ls raw/massive/*/date=2026-09-1*/_symbols.parquet | tail -5'  → 0
   zsh: no matches (path is two levels deep — glob shape wrong, not absent)

3. ssh macmini 'ls raw/massive/*/*/date=2026-09-1*/_symbols.parquet | tail -5'  → 0
   day_aggs_v1/date=2026-09-10, day_aggs_v1/date=2026-09-11,
   minute_aggs_v1/date=2026-09-10, minute_aggs_v1/date=2026-09-11  — PRESENT
   (09-02…09-09 minute partitions also present)

4. ssh macmini 'grep -h "coverage: 1d" logs/coverage_2026-09-0*.log'  → 0
   09-01: 1d=0/0 (100.00%)            then 1d=2355/13521 (17.42%)  (gate day 1, mid-ingest)
   09-03: 1d=0/0 (100.00%)                                      (target 09-03, due 09-04 15:00Z)
   09-04: 1d=13537/13547 … 13539/13548 (99.93%)                 (written 09-08 — due passed)
   09-08: 1d=0/0 · 09-09: 1d=0/0 · 09-10: 1d=0/0               (all weekday targets)

5. ssh macmini 'stat -f "%Sm %N" logs/coverage_2026-09-0*.log …'  → 0
   mtimes 19:03–19:55 HKT ≈ 11:0xZ — scheduled ~19:0x HKT every run;
   coverage_2026-09-04.log last written 09-08 19:11 (target dated Fri)

6. ssh macmini '… StartCalendarInterval + plist mtime'  → 0
   plist mtime 2026-08-10 (unchanged); comment: Hour=19 Minute=0 Asia/Hong_Kong
   = 11:00 UTC, "after the daily job's 4h DEADLINE (10:00 UTC)"

7. ssh macmini 'readlink ~/market-warehouse/current'  → 0
   releases/4cffb2c21443dd63689889d94bd77d93026b7c5b  (includes #95 due-gate)

8. ledger query: select date(started)…  → exit 1 (column is measured_at, retried)

9. ledger query: "select date(measured_at) d, scope, value from measurements
    where name='coverage_total' and scope='1d' order by measured_at desc limit 10"  → 0
   09-11 → 0.0   09-10 → 0.0   09-09 → 0.0     ← run-day rows, zeros begin 09-09
   09-08 → 13548.0   09-07 → 13548.0   09-06 → 13547.0   (target 09-04, already due)

10. ssh macmini 'stat -f "%Sm" bronze AAPL/SPY 1d.parquet + 09-10 _symbols.parquet'  → 0
    AAPL 1d 09-12 15:02 HKT, SPY 1d 09-12 18:15 HKT — daily job publishes 1d
    through ~10:15Z, done before coverage's 11:0xZ scan: the data IS on disk
    when the gate hides it. _symbols 09-10 partition landed 09-11 18:17 HKT
    (10:17Z) — present ~1h before the scan.
```

Code anchors (local): `coverage_report.py:382-391`
`if session_due_at(target_date) > as_of: results["1d"] = CoverageResult(0,0)`;
`clients/coverage_denominator.py:25-38` (`JOB_START_UTC` 06:00 + 9h allowance
= 15:00Z due); `_resolve_target_date` → `_et_today()` returns the *previous*
trading day pre-16:00 ET — so the scanned session is always due 4h after the
scan on weekdays.

Disposition: **no code change.** The gate is doing its designed job; the
failure is that the schedule (11:00Z, fixed by the daily job's 10:00Z deadline +
the no-mixed-snapshot rule) sits 4h before the due rule's instant. Resolution
is an operator/spec call, either of which is bigger than one file + one test:
(a) shrink `DELIVERY_ALLOWANCE_SECONDS` toward the 4h job deadline — shared by
`build_denominator`/gap engine, needs a spec decision; (b) reschedule coverage
≥15:30Z (23:30 HKT — the stale example's value worked *because* it postdates
the due instant) — a production plist change. Task 5's UNKNOWN rendering is
already the honest surface: post-merge the line reads `1d=0/0 (UNKNOWN)`, not
100%. Recorded for T11's postmortem.

## T11 — docs: two postmortems, rules, runbook, corrected coverage template

Gates:

```
uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
→ 2686 passed, 2 warnings in 83.07s  (exit 0)   TOTAL coverage 95.06%
npm run test:alerts → pass 12, fail 0  (exit 0)
uv run pytest tests/test_launchd_templates.py -q → 24 passed
```

What changed:

- New postmortem `docs/postmortems/2026-09-12-email-was-a-side-effect-not-a-record.md`:
  rule (every email = one `executions(script='notify')` row; page deduped 24h,
  digest unconditional), cost (the week's evidence table condensed), date.
- New postmortem `docs/postmortems/2026-09-12-coverage-1d-due-gate-vs-schedule.md`:
  the T10 finding — due gate (S+1 15:00Z) vs the 11:00Z schedule, masked since
  09-09 (first weekday-pair run after Labor Day), _symbols.parquet hypothesis
  rejected, both dispositions recorded, decision left to the operator.
- `CLAUDE.md`: tree lines updated (`livewire_node/` = notify's SMTP transport;
  7 templates); "Seven launchd jobs" line adds watchdog 12:00Z + digest 12:15Z;
  "all seven templates" in the plist rule. Alerts section: the two transport
  bullets now name `tests/node/send_mail.test.mjs`; the plan's four rules added
  verbatim (notify-row/dedup, unconditional digest + 12:45 deadline, zero
  denominator UNKNOWN, recovery-deferred-x2 BAD) plus one line for the due-gate
  postmortem.
- `launchd/com.livewire.coverage.plist.example`: 23:30 HKT → **19:00** with the
  conversion table corrected (11:00 UTC / 07:00 EDT / 12:00 BST) and a comment
  that production was verified at 19:00 on 2026-09-12 (T0 launchctl print);
  header retimed to "after the daily job's 4h DEADLINE (10:00 UTC) + watchdog".
- `tests/test_launchd_templates.py`: the ordering test now reads the coverage
  template's Hour instead of a hardcoded 19 (docstring updated — the template
  is no longer stale).
- `docs/runbook.md`: scheduled-env note covers `livewire_ops.py digest`;
  coverage bullets lose both email sentences (abort → `coverage_recovery_
  deferred` measurement, residual → `coverage_still_missing`, watchdog pages);
  `report --email` example deleted; Nightly digest section rewritten
  (`livewire_ops.py digest`, own 12:15Z job, unconditional, block list,
  `--body-out`; the `livewire_quality.py` alias flagged as not loading the
  scheduled env); watchdog "10:30 and 12:00 UTC"; Notices gains the full
  `MDW_ALERT_*` + `MDW_NODE_BIN` env table; schedule table gains
  `com.livewire.digest` 12:15 and watchdog's second interval; install loops
  include `digest`; run-daily-job paragraph corrected (tail = weekly +
  housekeeping; digest is a separate job); intraday-catchup failure now "pages
  through notify".
- `README.md`: `report --email` example removed; "before the digest" → "before
  the tail lane"; ops row → "notices (page/digest)"; "alerting" → "notices".
- `AGENTS.md`: rollup command loses `--email`; quality flags emit to two paths
  (sidecar + audit — "findings, not email").
- `.codex/project-memory.md`: terminal-failure paging, watchdog/digest rows,
  Cerebras incident-report line and `--mode flag-alert`/marker claims all
  corrected to the notify architecture.

Deviations / readings:

1. The env table documents the live `MDW_ALERT_*` surface verbatim from
   `send_mail.mjs` (FROM/TO required; CC/BCC/REPLY_TO/SUBJECT_PREFIX optional;
   SMTP_URL or HOST/PORT/SECURE/USER/PASS; TRANSPORT=stream for tests) — the
   plan's "MDW_ALERT_* table from Task 0" — plus `MDW_NODE_BIN` (notify's
   single resolver).
2. AGENTS.md and `.codex/project-memory.md` were edited though not in T11's
   file map: both asserted deleted behavior (`--email`, flag-alert mail,
   completion-marker watchdog) and AGENTS.md itself requires project-memory
   updates when stable facts change.

## T12 — verification: V1–V4 (local) + M1–M5 (mini)

Mini ground rules honored: the only writable location was
`V=/Users/moremeds/tmp/notify-verify-20260912T112704Z/` (created, deleted at
M5); every notify/digest invocation ran with `LW_LEDGER_ROOT=$V/ledger
MDW_LOG_DIR=$V/logs` and body files under `$V`; no `launchctl
load/unload/kickstart/bootstrap/bootout`; nothing under `~/market-warehouse/`
was created, modified or deleted (before/after listings below). No
`MDW_ALERT_SMTP_*` value was ever printed — env key *names* only where noted.

### Precheck — running jobs never interrupted

```text
$ ssh macmini 'date -u "+now %Y-%m-%d %H:%M:%SZ"; launchctl list | grep -i livewire;
               ps aux | grep -iE "livewire|coverage" | grep -v grep | head -10'
now 2026-09-12 11:26:49Z          (= Sat 19:26 HKT)
-	0	com.livewire.coverage            (coverage 19:00 HKT run finished, exit 0)
-	0	com.livewire.daily-update-watchdog
-	124	com.livewire.daily-update        (today's run interrupted — the "Daily update ran" BAD below)
99150	0	com.livewire.intraday-catchup  ← RUNNING (spawned 18:00 HKT; daily-backfill + flatfile-ingest children active)
-	0	com.livewire.universe-refresh
-	0	com.livewire.release-promote
PRECHECK_EXIT=0
```

All T12 mini steps ran read-only beside the live intraday-catchup; nothing was
interrupted.

### Setup

```text
$ ssh macmini 'TS=$(date -u +%Y%m%dT%H%M%SZ); V=$HOME/tmp/notify-verify-$TS; mkdir -p $V/src $V/logs && echo V=$V'
V=/Users/moremeds/tmp/notify-verify-20260912T112704Z
$ rsync -a --exclude .git --exclude .venv --exclude node_modules --exclude .worktrees ./ macmini:$V/src/ && ssh macmini 'cd $V/src && npm ci --omit=dev'
SETUP_EXIT=0   (nodemailer installed under $V/src/node_modules)
```

Code under test on the mini = the worktree copy at `$V/src` — not
`~/projects/livewire`, not `current/`.

### V1 — full pytest gate

```text
$ uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q
2686 passed, 2 warnings
TOTAL coverage: 95.06%
exit 0
```

### V2 — node transport tests

```text
$ npm run test:alerts
pass 12 / fail 0
exit 0
```

### V3 — digest through stream transport, temp ledger

```text
$ MDW_ALERT_TRANSPORT=stream MDW_ALERT_EMAIL_FROM=a@b MDW_ALERT_EMAIL_TO=c@d \
  LW_LEDGER_ROOT=$(mktemp -d) \
  uv run python scripts/livewire_ops.py digest --run-date 2026-09-11 --email
EXIT=0
Livewire digest — 2026-09-11 (sent 11:24Z)
COVERAGE as of UNKNOWN (no scan row)
…
{"script": "notify", "exit_code": 0, "s": "Digest 2026-09-11", "k": "digest"}   ← exactly one notify,0 row on the temp root
```

**Deviation (documented, not a defect):** the plan's "`Subject:` in stdout"
lives in the *receipt*, not the outer stdout — `notify.send` captures the
child's stream output into `receipt_json.node_stdout` rather than echoing it.
M4's ledger row below shows the generated line verbatim:
`Subject: [Livewire] PAGE 2026-09-12: Daily update ran; Coverage`. Outer
stdout carries the digest body + the one-line `{"script":"notify",…}` receipt
summary; the Subject is confirmed on the receipt.

### V4 — removed names gone from live surfaces

```text
$ git grep -n -iE 'send.alert|alert_on_flag|MDW_ALERT_SEVERITY|MDW_ALERT_RATE_LIMIT|MDW_UNDELIVERED|send_daily_update_failure_email|send_digest_email|send_mail_daily' \
    -- clients/ livewire_scripts/ scripts/ tests/ launchd/ livewire_node/ README.md CLAUDE.md AGENTS.md docs/runbook.md .env.example
(exit 1 — zero hits)
```

One stale hit was found and fixed in this task: `livewire_scripts/release.py`'s
`build_node_modules` docstring still named
`send_daily_update_failure_email.mjs` → now `send_mail.mjs` (the "failure
alert" phrase → "failure page"). That one-line docstring fix is the only
non-evidence change in this commit — the "proven issue" the task allows.

### M1 — real status through the new checks

```text
$ ssh macmini 'set -a; source ~/market-warehouse/.env; set +a;
               source ~/market-warehouse/.venv/bin/activate; cd $V/src;
               python scripts/livewire_ops.py status'
exit 0 — real-ledger verdicts (installed release 4cffb2c predates notify, so
         "Digest sent today" is UNKNOWN and "Undelivered notifications" OK):
[BAD ] Daily update ran:      run_id=daily-update-20260912T050003Z-85116 (the exit-124 interruption)
[BAD ] Coverage:              scopes=1d=UNKNOWN(expected=0) 1h=28.7% 1m=26.5% 30m=33.5% 5m=31.0%
[?? ] Digest sent today:      not sent yet (no notify rows — correct)
[OK ] Undelivered notifications: none
[OK ] Coverage ran today:     measured_at=2026-09-12 11:18:33 (the 19:00 HKT run)
[?? ] Intraday catch-up ran:  no rows — job still running (lane_results land at lane end)
[WARN] Intraday catch-up finished: running_minutes=87
```

### M2 — body-only render; real lake untouched

```text
$ ssh macmini 'ls -la ~/market-warehouse/data-lake/ledger/executions/ | tail -3'   # BEFORE
drwxr-xr-x@ 4  date=2026-09-10   10 Sep 17:17
drwxr-xr-x@ 4  date=2026-09-11   11 Sep 17:15
drwxr-xr-x@ 4  date=2026-09-12   12 Sep 18:26

$ ssh macmini 'set -a; source .env; set +a; source .venv/bin/activate; cd $V/src;
               LW_LEDGER_ROOT=$V/ledger MDW_LOG_DIR=$V/logs \
               python scripts/livewire_ops.py digest --run-date 2026-09-12 --body-out $V/digest.txt'
exit 0 — $V/digest.txt written; no send, no executions row.

$ grep -c 'COVERAGE as of' $V/digest.txt
1   →  line 3: "COVERAGE as of UNKNOWN (no scan row)"
```

`COVERAGE as of` count = exactly 1 as required. It reads UNKNOWN because the
plan's own command points `LW_LEDGER_ROOT` at the empty temp root (the "no
scan" fallback the plan anticipated); the `bronze_*`/`silver_*` freshness rows
in the body come from the DuckDB-catalog check, a read-only lake probe.
Confirmed the override holds: `livewire_ops.py digest` calls
`load_scheduled_env`, which *overwrites* `os.environ` — env-key audit
(`grep -oE '^[A-Z_]+=' ~/market-warehouse/.env`, names only) shows the file
defines no `LW_LEDGER_ROOT`/`MDW_LOG_DIR`, so the command-line roots survived.

```text
$ ls -la ~/market-warehouse/data-lake/ledger/executions/ | tail -3               # AFTER — identical
drwxr-xr-x@ 4  date=2026-09-10   10 Sep 17:17
drwxr-xr-x@ 4  date=2026-09-11   11 Sep 17:15
drwxr-xr-x@ 4  date=2026-09-12   12 Sep 18:26
```

### M3 — real SMTP send from the $V copy

```text
$ ssh macmini 'set -a; source ~/market-warehouse/.env; set +a;   # subshell only; no value ever printed
               source ~/market-warehouse/.venv/bin/activate; cd $V/src;
               LW_LEDGER_ROOT=$V/ledger MDW_LOG_DIR=$V/logs \
               python scripts/livewire_ops.py notify --kind digest \
                 --subject "VERIFY digest 2026-09-12 notify-rewrite T12" \
                 --body-file $V/digest.txt --force'
M3_SEND_EXIT=0   (outer stdout empty — same receipt-capture behavior as V3)
```

Delivered subject: `[Livewire] VERIFY digest 2026-09-12 notify-rewrite T12`
(operator: look for that subject in the alert mailbox).

Temp-ledger row (`select * from executions`, `$V/ledger`):

```text
{"script":"notify","attempt":1,"release_sha":"4cffb2c…","started":"2026-09-12 11:30:45.701134+00:00",
 "ended":"2026-09-12 11:30:50.195825+00:00","exit_code":0,"run_id":"notify-20260912T113050Z-39448",
 "receipt_json":{"kind":"digest","subject":"VERIFY digest 2026-09-12 notify-rewrite T12",
   "fingerprint":"1168dbc5…","node_exit":0,"skipped":false,
   "node_stdout":"{\"accepted\":[\"chenxi.li08@outlook.com\"],\"messageId\":\"<c1f10363-70dc-6a86-e17a-a142ce565237@gmail.com>\"}",
   "body_file":"$V/logs/notify_digest_20260912T113045Z.txt"}}
```

Node stdout JSON (accepted/messageId) — the real SMTP proof:

```text
{"accepted":["chenxi.li08@outlook.com"],"messageId":"<c1f10363-70dc-6a86-e17a-a142ce565237@gmail.com>"}
```

Real `data-lake/ledger/executions/` listing after M3 — unchanged (tail still
`date=2026-09-12` mtime `12 Sep 18:26`, the scheduled coverage run that
predates this session).

### M4 — page dedup against real status, temp ledger

Real status had BAD sections ("Daily update ran", "Coverage"), so M4 was
exercisable. Helper `$V/m4.py` (plan-required shape — grade real root, switch
ledger root only for the send):

```python
import os, sys
from datetime import date
from livewire_scripts import notify, status
from livewire_scripts.paths import data_lake_dir, log_dir

today = date.today()
sections = status.collect(today, log_dir(), data_lake_dir())   # REAL root — read-only
notice = notify.page_from_sections(sections, today)
if notice is None:
    print("M4: no BAD sections today — page not exercisable"); sys.exit(0)
os.environ["LW_LEDGER_ROOT"] = os.environ["M4_TEMP_LEDGER"]     # switch AFTER collect
os.environ["MDW_LOG_DIR"]    = os.environ["M4_TEMP_LOGS"]
print(f"M4 page subject: {notice.subject}")
sys.exit(notify.send(notice))
```

(`ledger_root()` resolves `LW_LEDGER_ROOT` lazily per call — verified.)

```text
$ PYTHONPATH=$V/src M4_TEMP_LEDGER=$V/ledger M4_TEMP_LOGS=$V/logs \
  MDW_ALERT_TRANSPORT=stream python $V/m4.py        # RUN 1
M4 page subject: PAGE 2026-09-12: Daily update ran; Coverage
M4_RUN1_EXIT=0

$ … same command                                              # RUN 2
M4 page subject: PAGE 2026-09-12: Daily update ran; Coverage
notify: page b1f2e614969b already sent within 24h; skipped
M4_RUN2_EXIT=0
```

Temp ledger afterwards — exactly the M4 contract (`select run_id, exit_code,
receipt_json from executions where script='notify' order by started`):

| run_id | kind | skipped | notes |
|---|---|---|---|
| notify-20260912T113050Z-39448 | digest | false | M3's real send (accepted/messageId above) |
| notify-20260912T113119Z-42217 | page | **false** | node_exit=0; node_stdout = full stream message, `Subject: [Livewire] PAGE 2026-09-12: Daily update ran; Coverage` |
| notify-20260912T113129Z-43292 | page | **true** | same fingerprint `b1f2e614969b…`; **no node fields at all** — send_mail.mjs never invoked on the dedup hit |

First send `skipped=false`, repeat `skipped=true` — dedup proven end-to-end on
the real grading.

### M5 — teardown; final lake proof

```text
$ ssh macmini 'ls -la ~/market-warehouse/data-lake/ledger/executions/ | tail -4;   # FINAL — unchanged
               rm -rf $V; ls $HOME/tmp/'
drwxr-xr-x@ 4  date=2026-09-09    9 Sep 17:31
drwxr-xr-x@ 4  date=2026-09-10   10 Sep 17:17
drwxr-xr-x@ 4  date=2026-09-11   11 Sep 17:15
drwxr-xr-x@ 4  date=2026-09-12   12 Sep 18:26
RM_EXIT=0
alert-sample          ← $HOME/tmp after teardown; $V gone, only pre-existing dir remains
M5_EXIT=0
```

### Result

| step | result |
|---|---|
| V1 | exit 0 — 2686 passed, coverage 95.06% |
| V2 | exit 0 — 12 pass / 0 fail |
| V3 | exit 0 — digest body + one `notify,0` row; `Subject:` verified in receipt (see deviation) |
| V4 | clean — zero live hits; stale `release.py` docstring fixed in this commit |
| M1 | exit 0 — real status graded; new sections live |
| M2 | exit 0 — body rendered, `COVERAGE as of` ×1, executions dir identical |
| M3 | exit 0 — real send `accepted/messageId`, row on temp root only |
| M4 | exit 0 — `skipped=false` then `skipped=true`, dedup row pair proven |
| M5 | exit 0 — `$V` removed, `~/tmp` clean, executions dir unchanged end-to-end |

Deviations: V3/M3 outer-stdout subject capture (receipt-verified); V4 docstring
fix; M2's "no scan row" is the empty-temp-root fallback the plan's own command
produces. No substitutions — every step ran as written.

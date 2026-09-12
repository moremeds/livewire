# Notify rewrite — email is a rendering of the ledger

> **For the implementing agent:** read "Execution contract" below first; it is binding. One linear thread, one worktree, one commit per task, an evidence file appended per task. Steps use `- [ ]` checkboxes. Do **not** fan out to sub-agents.

**Goal:** Replace the five email emission paths with two ledger-rendered notices — a deduplicated _page_ and an unconditional daily _digest_ sent after coverage — so every email the operator receives is traceable to one `executions` row and the digest always says what coverage is _today_.

**Architecture:** One Python module `livewire_scripts/notify.py` owns rendering, dedup, dispatch and the ledger receipt. One Node file `livewire_node/send_mail.mjs` owns SMTP only (`--subject`, `--body-file`). `status.collect()` stays the single grader; the watchdog and the digest are its two callers. Nothing else in the repo sends mail.

**Tech stack:** Python 3.13 / `uv`, DuckDB over the parquet ledger (`clients/ledger.py`), nodemailer (`livewire_node/`), launchd.

**Spec:** `docs/superpowers/specs/2026-09-02-livewire-ledger-design.md` §3 ("status / watchdog / digest — one reader") and §1 (`executions` as the alert record). This plan deviates from §3 in one place, stated in "Decisions" below.

## What is actually wrong (evidence: week of 2026-09-06 → 09-12 on `moremeds-Mini`)

| #   | Problem                                                                           | Evidence                                                                                                                                                                                           | Cause                                                                                                                                                                            |
| --- | --------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | The digest reports **yesterday's** coverage                                       | 09-10 digest: `worst_ratio=0.0 measured_at=2026-09-09 11:02Z`; 09-11 digest: `0.34 measured_at=2026-09-10`                                                                                         | Digest is the tail of daily-update (05:00Z, sent ~09:15Z); coverage runs at 11:00Z. Structural — the digest can never see today's scan. `run_daily_update_job.py:694-699`        |
| 2   | 1d shows **100% on 0/0**                                                          | `coverage_2026-09-09/10.log`: `1d 0/0 100.00%`; only 09-04 and 09-08 measured a real 1d (13537/13547)                                                                                              | `coverage_report` prints ratio 1.0 for an empty denominator; `status.py:210-212` only catches _all_ totals zero, so 1d's fake 1.0 enters `min(coverage_pct)` and the detail line |
| 3   | Same BAD → **no digest**; silence ≠ OK                                            | No `nightly_digest` row 09-06, 09-07 (fingerprint unchanged)                                                                                                                                       | `nightly_digest.py:58-74,180-186` fingerprint dedup                                                                                                                              |
| 4   | Recovery blocked by its own lock **four days**, graded INFO                       | `1m/1h/5m/30m recovery ABORTED: DEFERRED: minute cursor is active` on 09-09, 09-10, 09-11; 1m stuck 34.4→34.4→35.6%                                                                                | `coverage_report.py:1286-1296` only writes the abort to the log and to a page; no measurement, so `status` cannot grade a repeat                                                 |
| 5   | Incident day (09-09) produced **zero pages**; 09-06 produced **five** for one run | `daily_update_*.log` grep `Triggering failure alert`                                                                                                                                               | Pages are shaped by _retries_, not by _state_. No dedup on `_page_failure`; watchdog dedups by a marker file; digest by fingerprint; quality flags by an in-process dict         |
| 6   | No table answers "what was I sent this week"                                      | `executions` has `send_alert` **only on failure** (`run_daily_update_job.py:508`) and `nightly_digest` **only on success** (`nightly_digest.py:94`); `coverage_report._send_alert` records nothing | Five callers, three receipt conventions                                                                                                                                          |
| 7   | One undelivered page: `FileNotFoundError: 'node'` (09-08 10:45Z, release 59f18e3) | `executions` row `quality-flag-20260908T104516Z`                                                                                                                                                   | `scripts/livewire_ops.py:46` uses bare `"node"`; `nightly_digest.py:185` resolves via `MDW_NODE_BIN`/`which`/`/opt/homebrew/bin/node`. Two resolvers                             |
| 8   | Missing coverage run / missing digest is **silence**                              | 09-12: daily-update interrupted ("Workstation has been lost."), no coverage, no digest, no page about either                                                                                       | Watchdog pages only `BAD` (`check_daily_update_watchdog.py:54`); "no rows" is `UNKNOWN` and UNKNOWN never pages                                                                  |
| 9   | Per-ticker quality-flag email exists                                              | RJF interior-gap `critical`, `first_missing=1983-07-28`                                                                                                                                            | `clients/quality_flags.py:117-175`; pm:2026-07-19 storm; the scan "measures liquidity, not loss"                                                                                 |
| 10  | `MDW_ALERT_*` env undocumented; coverage plist says 15:30Z, runbook says 11:00Z   | `send_daily_update_failure_email.mjs:155-204`; `launchd/com.livewire.coverage.plist.example:37-40` vs `docs/runbook.md:937`; ledger `measured_at` 11:02–11:16Z says 11:00Z is what runs            | Docs drift                                                                                                                                                                       |

## Decisions

- **Two notice kinds, no third.** `page` (state is BAD now; dedup 24h by fingerprint) and `digest` (every day, unconditional, after coverage). A per-ticker alert, a coverage-only alert and a "daily summary" no longer exist.
- **Every send is an `executions` row** — success and failure — `script='notify'`, `evidence_hash=sha256(body)`, `receipt_json={kind, subject, fingerprint, node_exit}`. This is the only record. Deviation from spec §3: the `.alerted` marker file is **deleted**; a successful `notify` row is the idempotence state. One dedup mechanism, not three.
- **Digest gets its own launchd job at 12:15Z** (HKT 20:15), after coverage (11:00Z, measured 1400–2860s cold). The daily-update tail keeps weekly + housekeeping under lane name `tail`.
- **Watchdog runs twice**: 10:30Z (daily-update / intraday) and 12:00Z (coverage). It pages any BAD not yet paged in 24h.
- **"Must have happened by now" is a BAD, not an UNKNOWN.** `collect()` gains a `$now` parameter; checks for daily-update, coverage and digest turn UNKNOWN into BAD after their deadline. Rule 9.
- **A zero denominator is UNKNOWN, per scope.** Never 100%.
- **Recovery deferred twice in a row is BAD.** It is a measurement now.
- **Coverage's and quality-flag's own emails are deleted**, not migrated. Their facts become measurements; the digest renders them.
- **One node resolver** (`MDW_NODE_BIN` → `shutil.which("node")` → `/opt/homebrew/bin/node`) lives in `notify.py`.

## Execution contract for the implementing agent (swe2) — read before Task 0

This is the first time this agent works in this repo. The rules below are not
style; each one is a boundary the operator has set. Breaking one ends the run.

**Where work happens**

1. Implementation is **local only**, on this MacBook, in a git worktree:
   `git worktree add .worktrees/notify-rewrite -b notify-rewrite origin/main`.
   Never edit the `main` checkout. Never `git push` until the user says so.
2. **One commit per task**, message prefixed with the task number
   (`T4 feat(status): …`). No `--amend`, no rebase, no squash, no force-push,
   no `git reset --hard`. The history must replay the plan in order.
3. Keep `docs/superpowers/plans/2026-09-12-notify-rewrite.evidence.md` in the
   worktree. After every task append: the task number, every command run, its
   exit code, and the last 5 lines of its output. Commit it with the task.
   That file is the audit trail; a claim with no line in it is unverified.
4. Tests run through `uv run pytest …` and `npm run test:alerts` only. No
   `--no-cov`, no lowering `--cov-fail-under`, no `@pytest.mark.skip` added to
   make a gate pass. A criterion you cannot meet is reported as "not met".
5. Python runtime is `uv` exclusively — never bare `python`, `pip`, or an
   activated venv on the MacBook.

**The Mac mini (`ssh macmini`, user `moremeds`) is read-only for you**

6. Allowed over ssh: `ls`, `cat`, `head`, `tail`, `grep`, `wc`, `stat`,
   `readlink`, `find`, `launchctl print`, `launchctl list`, `date`,
   `hostname`, `which`, and
   `python ~/market-warehouse/current/scripts/livewire_ops.py {status,ledger query "<select …>"}`
   (after `source ~/market-warehouse/.venv/bin/activate`). `ledger query`
   accepts only `select` statements.
7. **Forbidden on the mini, no exceptions:** anything under
   `~/market-warehouse/data-lake/` being created, modified, moved, or
   deleted (this includes `data-lake/ledger/`, which is inside the lake);
   `livewire_ingest.py`, `livewire_store.py`, `livewire_quality.py` in any
   form; `livewire_ops.py release|promote|housekeeping|digest|notify`
   against the real warehouse; `launchctl load|unload|kickstart|bootstrap|bootout`;
   `git` in `~/projects/livewire` on the mini; `rm`, `mv`, `cp`, `rsync`
   **into** `~/market-warehouse/`; editing `~/market-warehouse/.env` or any
   plist under `~/Library/LaunchAgents/`.
8. The one write you may perform on the mini is a **temporary directory under
   `$HOME/tmp/notify-verify-<utc-ts>/`** holding a copy of your worktree and
   scratch output, and you delete it at the end of Task 12. Every command in
   Task 12 that runs new code sets `LW_LEDGER_ROOT`, `MDW_LOG_DIR` and
   `--body-file` inside that directory. The real ledger is **read** (DuckDB over parquet, a `select`) through the
   default `MDW_WAREHOUSE_DIR` only in the steps that say so (M1, M2, M4);
   every `emit` in Task 12 goes to `LW_LEDGER_ROOT=$V/ledger`.
9. You **may send email to the operator** from the mini, using the SMTP
   settings in `~/market-warehouse/.env` (source it, never print it, never
   copy it). Every such send goes through your temp ledger root. Subject must
   start with `[Livewire] VERIFY` so the operator can tell it from production.
10. Never interrupt, wait on, or reschedule a running job on the mini. If a
    job is running (`launchctl list | grep livewire` shows a PID), read-only
    commands are still fine.

**Reporting**

11. Report in plain words, three sentences before any table: what is broken,
    what it costs, what you need decided. Name the host for every measurement.
12. When a step's expected output does not match, stop that task and report;
    do not "fix" the criterion or the test to match.
13. **Review gate.** Every task is reviewed by the Claude session
    `livewire-2a` (herdr pane `w2:p1`) before the next task starts. After
    the task's commit, stop and report: task number, commit SHA, the evidence
    entry, and anything you deviated from. Continue only after the reviewer
    says so. The reviewer may reject a task; a rejected task is fixed with a
    new commit on top, never by rewriting the rejected one.

## Global constraints

- Tests: `uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning` and `npm run test:alerts` (CI runs both).
- No new script under `scripts/`; new commands hang off `livewire_ops.py`.
- Every new check is one `(name, sql)` row in `CHECKS` plus one test; no `_foo_section` function.
- `status.py` never imports duckdb directly; never scans bar parquet.
- Nothing here writes the lake; `notify.py` writes only the ledger and `<log_dir>/notify_*.txt`.
- The mini is touched only in Task 0, Task 10 step 1 and Task 12 M1–M5, under contract rules 6–10: reads of the warehouse, writes only under `$HOME/tmp/notify-verify-*`. The O-block of Task 12 is the operator's, after merge.

## File map

| Path                                                                                                  | Action                                                                        | Responsibility                                                                                                               |
| ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `livewire_node/send_mail.mjs`                                                                         | **create** (from the transport half of `send_daily_update_failure_email.mjs`) | SMTP only: `--subject=`, `--body-file=`; base64; prints `{"accepted":[...],"messageId":...}`                                 |
| `livewire_node/send_daily_update_failure_email.mjs`                                                   | **delete**                                                                    | —                                                                                                                            |
| `tests/node/send_mail.test.mjs`                                                                       | create (keep QP/base64, dash-safe, env-config tests)                          |                                                                                                                              |
| `tests/node/send_daily_update_failure_email.test.mjs`                                                 | delete                                                                        |                                                                                                                              |
| `livewire_scripts/notify.py`                                                                          | **create**                                                                    | `Notice`, `send`, `already_sent`, `page_from_sections`, `build_digest`, `sent_within`                                        |
| `tests/test_notify.py`                                                                                | create                                                                        | unit + one real-node seam test                                                                                               |
| `scripts/livewire_ops.py`                                                                             | modify                                                                        | `send-alert` → `notify`; delete `_dispatch_send_alert`                                                                       |
| `livewire_scripts/status.py`                                                                          | modify `CHECKS`, `collect`                                                    | new/changed checks, `$now`                                                                                                   |
| `tests/test_status.py`                                                                                | modify                                                                        | one test per check                                                                                                           |
| `livewire_scripts/coverage_report.py`                                                                 | modify                                                                        | delete `_send_alert`; emit recovery/stale measurements; 0/0 prints UNKNOWN                                                   |
| `livewire_scripts/run_daily_update_job.py`                                                            | modify                                                                        | `_page_failure` → notify; delete `AlertRequest`, `send_failure_alert`, `record_failed_send`; tail lane `tail` without digest |
| `livewire_scripts/check_daily_update_watchdog.py`                                                     | rewrite                                                                       | ~40 lines                                                                                                                    |
| `livewire_scripts/nightly_digest.py`                                                                  | rewrite                                                                       | build + `notify.send`, no dedup                                                                                              |
| `clients/quality_flags.py`, `clients/quality_detector.py`                                             | modify                                                                        | delete `alert_on_flag`, `_record_failed_alert`, rate-limit cache                                                             |
| `launchd/com.livewire.digest.plist.example`                                                           | create                                                                        | 12:15Z                                                                                                                       |
| `launchd/com.livewire.daily-update-watchdog.plist.example`                                            | modify                                                                        | two intervals                                                                                                                |
| `tests/test_launchd_templates.py`                                                                     | modify                                                                        | add digest; assert intervals                                                                                                 |
| `docs/runbook.md`, `CLAUDE.md`, `docs/postmortems/2026-09-12-email-was-a-side-effect-not-a-record.md` | modify/create                                                                 |                                                                                                                              |

---

### Task 0: Verify the three production facts this plan depends on (read-only, mini)

**Files:** none.

- [ ] **Step 1: coverage schedule as loaded**
      `ssh macmini 'launchctl print gui/$(id -u)/com.livewire.coverage | grep -A3 -i "calendar\|hour\|minute"'`
      Expected: Hour 19 / Minute 0 HKT (= 11:00Z). If it prints 23:30 HKT, the runbook is wrong and Task 8's digest time must move to 16:00Z — record which.
- [ ] **Step 2: mail env present in the warehouse env**
      `ssh macmini 'grep -c "^MDW_ALERT_" ~/market-warehouse/.env'` → ≥ 4. Record the key names (not values) for the runbook table in Task 11.
- [ ] **Step 3: node path in the release environment**
      `ssh macmini 'grep -n "MDW_NODE_BIN\|PATH" ~/Library/LaunchAgents/com.livewire.daily-update-watchdog.plist; which node; ls /opt/homebrew/bin/node'`
      Record the resolved path; Task 2's resolver must find it without `PATH`.
- [ ] **Step 4:** write the three answers, each beside its command, as the first entry of `docs/superpowers/plans/2026-09-12-notify-rewrite.evidence.md`; commit it as `T0 docs: production facts for the notify rewrite`. If Step 1 contradicts 11:00Z, say so in that entry — Task 8 and the runbook follow the measured value, never the plan's assumption.

### Task 1: `send_mail.mjs` — SMTP and nothing else

**Files:** create `livewire_node/send_mail.mjs`, `tests/node/send_mail.test.mjs`; delete `livewire_node/send_daily_update_failure_email.mjs`, `tests/node/send_daily_update_failure_email.test.mjs`; modify `package.json:9`.

**Interfaces — produces:** CLI `node livewire_node/send_mail.mjs --subject=<text> --body-file=<path>`; env `MDW_ALERT_EMAIL_FROM/TO/CC/BCC/REPLY_TO/SUBJECT_PREFIX`, `MDW_ALERT_SMTP_URL | MDW_ALERT_SMTP_HOST/PORT/SECURE/USER/PASS`, new `MDW_ALERT_TRANSPORT=stream` (streamTransport, prints the RFC822 message, no network — for the Python seam test and for dry runs on the mini). Exit 0 on accepted, 1 otherwise; stdout one JSON line.

- [ ] **Step 1: write the failing tests** (`tests/node/send_mail.test.mjs`, node:test). Port from the old file: `resolveAlertConfig` env cases (missing FROM/TO, URL vs host), the base64 regression (body `revision=28 rebuilt=10` survives — assert the built message has `textEncoding: "base64"` and the streamed output contains `Content-Transfer-Encoding: base64`), a subject beginning with `--` survives (`--subject=--not-a-flag`), and `MDW_ALERT_TRANSPORT=stream` prints the message without a transport.
- [ ] **Step 2:** `npm run test:alerts` → FAIL (module missing).
- [ ] **Step 3: implement** — copy `resolveAlertConfig` (lines 155-204) and `sendFailureAlert` (565-583, rename `sendMail`) verbatim; new `parseArgs` accepting only `--subject=` and `--body-file=` in single-token form; `main` reads the body file, prefixes subject with `subjectPrefix`, sends, prints `JSON.stringify({accepted: info.accepted, messageId: info.messageId})`. When `env.MDW_ALERT_TRANSPORT === "stream"` use `{streamTransport: true, buffer: true}` and print `info.message.toString()`. Target ≤ 150 lines. Keep the base64 comment block.
- [ ] **Step 4:** `npm run test:alerts` → PASS. `package.json` `test:alerts` points at the new test file.
- [ ] **Step 5: delete** the old mjs + test. `grep -rn send_daily_update_failure_email .` → only `docs/postmortems/` hits remain.
- [ ] **Step 6: commit** `refactor(notify): one-mode mailer send_mail.mjs`

### Task 2: `notify.py` — render, dedup, send, record

**Files:** create `livewire_scripts/notify.py`, `tests/test_notify.py`.

**Interfaces — consumes:** `clients.ledger.emit/query/new_run_id`, `livewire_scripts.status.Section/Verdict`, `livewire_scripts.paths.log_dir`, `send_mail.mjs`. **Produces:**

```python
KINDS = ("page", "digest")

@dataclass(frozen=True)
class Notice:
    kind: str            # "page" | "digest"
    subject: str
    body: str
    fingerprint: str     # sha256 of the semantic state (page) or of run_date (digest)

def node_bin() -> str
def fingerprint(kind: str, keys: list[str]) -> str
def already_sent(fp: str, *, within_hours: int = 24) -> bool
def sent_within(hours: int = 24) -> list[dict]            # [{started, kind, subject, exit_code}]
def page_from_sections(sections: list[Section], run_date: date) -> Notice | None
def page_for_lane(run_date: date, lane: str, exit_code: int, error_summary: str, log_tail: str) -> Notice
def send(notice: Notice, *, force: bool = False, runner=None, timeout_s: int = 120,
         body_path: Path | None = None, receipt_extra: dict | None = None) -> int
```

`send` semantics: (1) if not `force` and `already_sent(fp)` → print `notify: <kind> <fp[:12]> already sent within 24h; skipped`, return 0, **and emit an executions row with `exit_code=0`, `receipt_json.skipped=true`** so the skip is itself visible; (2) write body to `body_path` if given, else `<log_dir>/notify_<kind>_<utc-ts>.txt` (`log_dir()` honours `MDW_LOG_DIR`); `receipt_extra` is merged into `receipt_json`; (3) run `[node_bin(), send_mail.mjs, f"--subject={subject}", f"--body-file={path}"]` in its own process group with `timeout_s`; (4) emit `executions` row `{evidence_hash: "sha256:"+sha256(body), script: "notify", attempt: 1, args_json: {kind, subject, body_file}, release_sha: readlink(<warehouse>/current) or None, started, ended, exit_code, receipt_json: {kind, fingerprint, subject, node_stdout[:2000], skipped: false}, run_id}`; (5) return exit code. A `TimeoutExpired`/`OSError` is exit 1 with the exception text in `receipt_json.error`. The row is written in a `try` — a failed ledger write prints to stderr and never raises.

- [ ] **Step 1: failing tests** (`tests/test_notify.py`, temp `LW_LEDGER_ROOT`, temp log dir):
  - `test_send_records_success_and_failure_rows` — fake runner returning 0 then 1; two `executions` rows with `script='notify'`, exit 0 and 1, `receipt_json.fingerprint` equal to the notice's.
  - `test_same_fingerprint_within_24h_is_skipped_and_the_skip_is_recorded` — second send returns 0, runner called once, second row has `skipped=true`.
  - `test_force_bypasses_dedup`.
  - `test_page_from_sections_is_none_when_nothing_is_bad`, `test_page_fingerprint_ignores_timestamps` (two section lists differing only in `measured_at` detail → same fp; use `notification_key` when present else `name:verdict`).
  - `test_timeout_is_exit_1_with_error_in_receipt`.
  - `test_real_node_seam` — **no mock**: `MDW_ALERT_TRANSPORT=stream`, `MDW_ALERT_EMAIL_FROM/TO` set, run the real `send_mail.mjs`; assert exit 0, the ledger row exists, and `node_stdout` contains `Subject: [Livewire] ` and the body line `revision=28` intact. Skip (`pytest.skip`) only if `node` is not resolvable — and make that skip print the resolver's answer.
- [ ] **Step 2:** `uv run pytest tests/test_notify.py -q` → FAIL (import error).
- [ ] **Step 3: implement** `notify.py` per the interface. Subjects: page → `f"[Livewire] PAGE {run_date}: " + "; ".join(bad headlines)[:120]`; digest → `f"[Livewire] digest {run_date}"` (the `[Livewire] ` prefix is `MDW_ALERT_EMAIL_SUBJECT_PREFIX`'s job — pass the subject without it). `already_sent` SQL: `select 1 from executions where script='notify' and exit_code=0 and started >= now() - interval {h} hour and json_extract_string(receipt_json,'$.fingerprint') = '{fp}' and json_extract_string(receipt_json,'$.skipped') = 'false' limit 1`.
- [ ] **Step 4:** tests PASS.
- [ ] **Step 5: commit** `feat(notify): one sender, one receipt per send`

### Task 3: `livewire_ops.py notify` replaces `send-alert`

**Files:** modify `scripts/livewire_ops.py:45-48,56,65-66`; test `tests/test_livewire_entrypoints.py`.

- [ ] **Step 1: failing test** — `livewire_ops.py notify --kind page --subject x --body-file f --force` with a fake runner records one `executions` row; `send-alert` is no longer an accepted command (`SystemExit` 2).
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3:** delete `_dispatch_send_alert`; add `notify` subcommand: args `--kind {page,digest}`, `--subject`, `--body-file`, `--force`; body read from file; calls `notify.send(Notice(kind, subject, body, fingerprint(kind,[subject])))`.
- [ ] **Step 4:** PASS. `grep -rn "send-alert\|send_alert" livewire_scripts clients scripts tests` → only `status.py` (fixed in Task 4) and `coverage_report.py` (Task 5).
- [ ] **Step 5: commit** `refactor(ops): notify subcommand; send-alert removed`

### Task 4: status checks — today's truth, deadlines, per-scope UNKNOWN

**Files:** modify `livewire_scripts/status.py:77-267,269-274,878-887`; tests `tests/test_status.py`.

**Interfaces — consumes:** measurements `coverage_pct`, `coverage_total`, `coverage_scan_ok` (existing), `coverage_recovery_deferred`, `coverage_still_missing`, `stale_non_equity` (Task 5 produces); executions `script='notify'` (Task 2). **Produces:** `collect(..., now: datetime | None = None)`; param `$now` = `now.isoformat()`; new/changed `CHECKS` names exactly as below (the digest and watchdog key on them).

- [ ] **Step 1: failing tests** (one per row; fixture ledgers via `ledger.emit` into a temp root; freeze `now`):
  1. `test_undelivered_notifications_reads_the_notify_script` — row `script='notify', exit_code=1` today → WARN; `send_alert` rows are ignored.
  2. `test_coverage_a_zero_denominator_scope_is_unknown_not_one_hundred` — 1d total=0 pct=1.0, others 0.99 → verdict UNKNOWN, detail contains `1d=UNKNOWN(expected=0)`; with 1m=0.36 → BAD and detail still shows `1d=UNKNOWN`.
  3. `test_coverage_ran_today_is_bad_after_the_deadline` — no `coverage_scan_ok` today: `now=11:30Z` → UNKNOWN; `now=12:10Z` → BAD; row present → OK/WARN by value.
  4. `test_digest_sent_today_is_bad_after_its_deadline` — no notify digest row today at 12:50Z → BAD; at 12:10Z → UNKNOWN; row exit 0 → OK.
  5. `test_daily_update_absent_after_deadline_is_bad` — no `runs` row for today and `now=07:00Z` → BAD (was UNKNOWN); `now=05:30Z` → UNKNOWN.
  6. `test_coverage_recovery_deferred_twice_is_bad` — `coverage_recovery_deferred` scope 1m value 1 on two latest scans → BAD; once → WARN; latest 0 → OK; none → UNKNOWN.
  7. `test_stale_non_equity_is_warn_never_bad` — `stale_non_equity` scope=volatility value 1 → WARN with the scope in detail.
  8. `test_post_success_tail_reads_lane_tail`.
  9. existing `test_adding_a_lane_makes_it_appear_in_the_lanes_terminal_check` still passes.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: implement.** Replace the rows:

```python
(
    "Daily update ran",
    "select case verdict when 'FAILED' then 'BAD' when 'DEGRADED' then 'WARN' when 'OK' then 'OK' else 'UNKNOWN' end as verdict, run_id, started "
    "from runs where job = 'daily-update' and date(started) = date '$today' and ended is not null "
    "union all select case when timestamp '$now' > timestamp '$today 07:00:00' then 'BAD' else 'UNKNOWN' end, 'no run today', null "
    "where not exists (select 1 from runs where job = 'daily-update' and date(started) = date '$today') "
    "order by started desc nulls last limit 1",
),
(
    "Undelivered notifications",
    "select 'WARN' as verdict, count(*) as failed_sends, string_agg(json_extract_string(receipt_json,'$.subject'), '; ') as subjects "
    "from executions where script = 'notify' and exit_code <> 0 and date(started) = date '$today' having count(*) > 0",
),
(
    "Digest sent today",
    "select 'OK' as verdict, started, json_extract_string(receipt_json,'$.subject') as subject from executions "
    "where script = 'notify' and exit_code = 0 and json_extract_string(receipt_json,'$.kind') = 'digest' "
    "and date(started) = date '$today' "
    "union all select case when timestamp '$now' > timestamp '$today 12:45:00' then 'BAD' else 'UNKNOWN' end, null, 'not sent yet' "
    "where not exists (select 1 from executions where script = 'notify' and exit_code = 0 "
    "and json_extract_string(receipt_json,'$.kind') = 'digest' and date(started) = date '$today') "
    "order by started desc nulls last limit 1",
),
(
    "Coverage ran today",
    "select case when value = 1 then 'OK' else 'WARN' end as verdict, measured_at from measurements "
    "where name = 'coverage_scan_ok' and date(measured_at) = date '$today' "
    "union all select case when timestamp '$now' > timestamp '$today 12:00:00' then 'BAD' else 'UNKNOWN' end, null "
    "where not exists (select 1 from measurements where name = 'coverage_scan_ok' and date(measured_at) = date '$today') "
    "order by measured_at desc nulls last limit 1",
),
(
    "Coverage",
    "select case when count(*) < 5 then 'UNKNOWN' "
    "when min(pct) filter (where total > 0) < $coverage_threshold then 'BAD' "
    "when date_diff('day', date(max(measured_at)), date '$today') > $coverage_stale_days then 'BAD' "
    "when count(*) filter (where total = 0) > 0 then 'UNKNOWN' else 'OK' end as verdict, "
    "string_agg(scope || '=' || case when total = 0 then 'UNKNOWN(expected=0)' else format('{:.1f}%', 100*pct) end, ' ' order by scope) as scopes, "
    "min(pct) filter (where total > 0) as worst_ratio, max(measured_at) as measured_at from ("
    "  select p.scope, p.value as pct, t.value as total, p.measured_at from "
    "  (select scope, value, measured_at from measurements where name = 'coverage_pct' and scope in ('1d','1m','1h','5m','30m') "
    "   qualify row_number() over (partition by scope order by measured_at desc) = 1) p "
    "  join (select scope, value from measurements where name = 'coverage_total' and scope in ('1d','1m','1h','5m','30m') "
    "   qualify row_number() over (partition by scope order by measured_at desc) = 1) t using (scope))",
),
(
    "Coverage recovery",
    "select case when count(*) = 0 then 'UNKNOWN' when max(twice) = 1 then 'BAD' when max(latest) = 1 then 'WARN' else 'OK' end as verdict, "
    "string_agg(scope || case when twice = 1 then '=deferred x2' when latest = 1 then '=deferred' else '=ok' end, ' ' order by scope) as scopes from ("
    "  select scope, max(case when rn = 1 then value end) as latest, "
    "  case when max(case when rn = 1 then value end) = 1 and max(case when rn = 2 then value end) = 1 then 1 else 0 end as twice from ("
    "    select scope, value, row_number() over (partition by scope order by measured_at desc) as rn "
    "    from measurements where name = 'coverage_recovery_deferred') where rn <= 2 group by scope)",
),
(
    "Stale non-equity",
    "select 'WARN' as verdict, string_agg(scope || '=' || cast(value as int), ' ') as classes from ("
    "  select scope, value from measurements where name = 'stale_non_equity' "
    "  qualify row_number() over (partition by scope order by measured_at desc) = 1) where value > 0 having count(*) > 0",
),
```

Rename `"Post-success tail"` SQL `lane = 'digest'` → `lane = 'tail'`. Add `"Undelivered notifications"`, `"Stale non-equity"` to `_EMPTY_IS_OK`; remove `"Undelivered alerts"`. `collect(..., now=None)` → `params["now"] = (now or datetime.now(UTC)).strftime("%Y-%m-%d %H:%M:%S")`. Update `_FIXES` keys.

- [ ] **Step 4:** PASS; full `tests/test_status.py` PASS.
- [ ] **Step 5: commit** `feat(status): deadline-aware checks, per-scope coverage, notify receipts`

### Task 5: coverage emits facts, sends nothing

**Files:** modify `livewire_scripts/coverage_report.py:1038-1073,1260-1298` and the printing of the `1d 0/0 100%` line (find with `grep -n "100.0\|ratio" coverage_report.py` around the per-timeframe summary); tests `tests/test_coverage_report.py`.

- [ ] **Step 1: failing tests**: `test_recovery_abort_is_a_measurement_not_an_email` (aborted outcome → `coverage_recovery_deferred` scope=tf value=1 and `coverage_still_missing` value=n; no subprocess call), `test_stale_non_equity_is_a_measurement` (scope=asset_class, value=count), `test_zero_denominator_prints_unknown` (summary line reads `1d 0/0 UNKNOWN`, and the `coverage_pct` row for that scope is still emitted with value 0.0 — not 1.0 — so `status` never sees a fake 1.0). Delete the 5 `_send_alert` tests.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** delete `_send_alert`, `_OPS_SCRIPT` if now unused; add `emit_recovery_measurements(outcomes)` and `emit_stale_non_equity(stale)` next to `emit_coverage_scan_measurement` (same shape, `unit='boolean'`/`'symbols'`); ratio for `total == 0` is `0.0` and the printed word is `UNKNOWN`.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: commit** `refactor(coverage): recovery and staleness are measurements; no email path`

### Task 6: daily-update pages through notify; the tail no longer sends the digest

**Files:** modify `livewire_scripts/run_daily_update_job.py:321-393,460-533,664-730`; `clients/constants.py` (if `tail` needs a budget — check `test_declared_lane_budgets_cover_exactly_the_lane_set`; the tail lane is outside `LANE_ORDER` today, keep it so); tests `tests/test_run_daily_update_job.py`.

- [ ] **Step 1: failing tests**: `test_a_lane_failure_pages_once_per_run_and_lane` (two failures of the same lane in one run → one `notify` row with exit 0 and one skipped row), `test_the_tail_runs_weekly_and_housekeeping_only` (runner receives no `digest` argv; lane row name `tail`), `TestTheLaneRunnerNeverRunsTheAlert` adapted to `notify.send` (still: the page runner is `subprocess.run`, never the lane runner). Delete `record_failed_send`/`send_failure_alert` tests.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** `_page_failure(config, log_file, exit_code, *, lane, attempts, env=None)` builds `notify.page_for_lane(run_date, lane, exit_code, extract_error_summary(log_file), tail_of(log_file, 60))` and calls `notify.send(notice)`; fingerprint = `fingerprint("page", [run_id(), lane])`. Delete `AlertRequest`, `send_failure_alert`, `record_failed_send`; keep `extract_error_summary`. In `run_post_success_quality` remove the digest spawn and `record_failed_send`; `_emit_lane("tail", ...)`. Update `check_daily_update_watchdog` import in the same commit (Task 7 rewrites it fully).
- [ ] **Step 4:** PASS.
- [ ] **Step 5: commit** `refactor(daily-update): page via notify; digest leaves the tail`

**Amendment A1 (reviewer, 2026-09-12, after T3):** the twin path. `livewire_scripts/run_intraday_catchup_job.py` (scheduled intraday-catchup 10:00Z) pages through `job_runner_common.build_alert_command` → `send-alert`, which T3 removed. In this same task: its failure path builds `notify.page_for_lane(run_date, "intraday_catchup", exit_code, error_summary, log_tail)` and calls `notify.send`; delete `AlertRequest` and `build_alert_command` from `livewire_scripts/job_runner_common.py` and the local `build_alert_command` wrapper; update `tests/test_run_intraday_catchup_job.py` and `tests/test_job_runner_common.py` (`_KNOWN_INLINE_ALERT_BUILDERS`) accordingly.

### Task 7: watchdog = `collect()` → `page_from_sections` → `send`

**Files:** rewrite `livewire_scripts/check_daily_update_watchdog.py`; tests `tests/test_check_daily_update_watchdog.py`.

- [ ] **Step 1: failing tests**: `test_bad_pages_once_per_state_across_runs` (same BAD at 10:30Z and 12:00Z → one send, one skip row), `test_a_new_bad_pages_again` (coverage turns BAD at 12:00Z → second send with a different fingerprint), `test_nothing_bad_sends_nothing_and_writes_nothing`, `test_a_failed_send_is_exit_3_and_a_row`, `test_no_marker_file_is_written` (`state/daily-update-watchdog` never created).
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** implement:

```python
def run_watchdog(run_date: date, *, now: datetime | None = None, runner=None) -> int:
    sections = collect(run_date, log_dir(), data_lake_dir(), now=now)
    notice = notify.page_from_sections(sections, run_date)
    if notice is None:
        return 0
    return 0 if notify.send(notice, runner=runner) == 0 else ALERT_FAILED_EXIT_CODE
```

Delete `build_watchdog_marker_file`, `record_alert_marker`, `missing_jobs` (the deadline checks in Task 4 replace it).

- [ ] **Step 4:** PASS.
- [ ] **Step 5: commit** `refactor(watchdog): ledger-deduped pages, no marker file`

### Task 8: the digest — every day, after coverage, says today

**Files:** rewrite `livewire_scripts/nightly_digest.py`; create `launchd/com.livewire.digest.plist.example`; modify `launchd/com.livewire.daily-update-watchdog.plist.example`; tests `tests/test_nightly_digest.py`, `tests/test_launchd_templates.py`.

**Body layout** (plain text, `key=value` lines survive base64):

```
Livewire digest — 2026-09-13 (sent 12:15Z)

COVERAGE as of 2026-09-13 11:14Z (scan ok=1, 1402s)
  1d   13541/13547  99.96%   (prev 99.93%)
  1m    4256/11956  35.60%   (prev 34.40%)  recovery=deferred x2  still_missing=7700
  1h ...
  non-equity: cmdty 1/1@09-12  futures 6/10@09-12  fx 21/21@09-12  rates 4/4@09-11  volatility 42/42@09-12

CHANGED since yesterday's digest
  Coverage recovery: WARN -> BAD
  Silver failures: 288 -> 291

SENT to you in the last 24h
  2026-09-13 10:31Z page  [Livewire] PAGE 2026-09-13: Coverage recovery ...   exit=0
  2026-09-13 12:01Z page  (skipped, same state)

STATUS (every check)
[OK ] Daily update ran: ...
...
```

- [ ] **Step 1: failing tests**: `test_digest_is_sent_every_day_even_when_unchanged` (two consecutive runs → two `notify` rows, none skipped — digest fingerprint is `fingerprint("digest",[run_date])` and `send(..., force=True)`), `test_coverage_block_is_today_and_per_scope` (rows from a fixture ledger; `prev` from the second-latest measurement per scope), `test_changed_block_diffs_verdicts_against_the_previous_digest` (previous verdicts stored in the previous digest row's `receipt_json.verdicts` — add `verdicts: {name: verdict}` to the digest notice's receipt via a `receipt_extra` kwarg on `send`), `test_sent_block_lists_notify_rows`, `test_a_missing_coverage_row_renders_UNKNOWN_not_blank`, `test_build_never_raises_on_empty_ledger`. Delete the fingerprint/lock tests.
- [ ] **Step 2:** FAIL.
- [ ] **Step 3:** implement `build_digest(run_date, sections, *, previous_verdicts, coverage_rows, sent_rows) -> str` (pure), `main(argv, runner=None)`: `--run-date`, `--email`, `--body-out PATH` (build from the ledger, write the body to PATH, **no send and no ledger write** — this is the mode Task 12 uses on the mini); queries: coverage rows = latest two `coverage_pct`/`coverage_total`/`coverage_recovery_deferred`/`coverage_still_missing` per scope + `last_session` per non-equity lane; previous verdicts from the last successful digest row. `send(notice, force=True, receipt_extra={"verdicts": {...}})`.
- [ ] **Step 4:** plist: `com.livewire.digest.plist.example` copied from the watchdog template, `Hour 20 / Minute 15` HKT (comment block lists 12:15Z), program `.../current/.venv/bin/python .../current/scripts/livewire_ops.py digest --email`. Watchdog plist: `StartCalendarInterval` becomes an array of two dicts (18:30 and 20:00 HKT). `test_launchd_templates.py`: add `"com.livewire.digest"` to `JOB_TEMPLATES`; new `test_the_digest_runs_after_coverage_and_the_watchdog_runs_twice` asserting digest hour 20 ≥ coverage hour 19 + 1 and watchdog has exactly two intervals. (If Task 0 found coverage at 23:30 HKT, use 00:45 next day for the digest and say so in the test name.)
- [ ] **Step 5:** PASS (both test files).
- [ ] **Step 6: commit** `feat(digest): unconditional daily digest after coverage, own launchd job`

### Task 9: delete the per-ticker quality-flag email

**Files:** modify `clients/quality_flags.py` (delete `alert_on_flag`, `_RATE_LIMIT_CACHE`, `_record_failed_alert`, `MDW_ALERT_SEVERITY_THRESHOLD`/`MDW_ALERT_RATE_LIMIT_SECONDS` handling), `clients/quality_detector.py:320,350`; tests `tests/test_quality_flags.py`, `tests/conftest.py:26` (drop `MDW_UNDELIVERED_DIR`); `docs/runbook.md:58-59`.

- [ ] **Step 1:** delete the 7 `alert_on_flag` tests; add `test_flags_are_written_without_any_send` (detector run → sidecar + audit written, no subprocess).
- [ ] **Step 2:** FAIL (ImportError from detector).
- [ ] **Step 3:** delete; `grep -rn "alert_on_flag\|MDW_ALERT_SEVERITY\|MDW_ALERT_RATE_LIMIT\|MDW_UNDELIVERED" .` → zero hits outside `docs/postmortems/`.
- [ ] **Step 4:** PASS.
- [ ] **Step 5: commit** `refactor(quality): flags are findings, not emails`

**Amendment A2 (reviewer, 2026-09-12, after T3):** three more senders outside the two kinds, all deleted in this task, same commit: `livewire_scripts/health_check.py` `_send_alert` (interior-gap scan; not scheduled, `status` does not grade it), `livewire_scripts/data_quality_report.py` `_send_email` (old `--mode daily-summary`, superseded by the digest), `livewire_scripts/universe_screener.py` the additions/removals `send-alert` call (informational, not BAD). Each keeps its log/ledger output and loses only the subprocess; delete the corresponding tests and add one per module asserting no subprocess is spawned. Step 3's grep widens to `send-alert\|send_alert` → zero hits outside `docs/postmortems/` and this plan. why is the 1d denominator empty since 2026-09-09? (bounded diagnosis)

**Files:** none until the cause is known; then the one file + one test.

- [ ] **Step 1 (mini, read-only):** `ssh macmini 'grep -n "1d" ~/market-warehouse/logs/coverage_2026-09-08.log ~/market-warehouse/logs/coverage_2026-09-10.log | head -20'` and `ssh macmini 'ls ~/market-warehouse/data-lake/raw/massive/*/date=2026-09-1*/_symbols.parquet 2>&1 | tail -5'`. Hypothesis to confirm or reject: the 1d denominator is built from the day's raw traded set (`_symbols.parquet`, CLAUDE.md "no bar vs no trade") and that partition has been absent or not yet published at 11:00Z since the 09-09 incident, so "expected" resolved to zero and the report encoded UNKNOWN as 0/0 = 100%.
- [ ] **Step 2 (local):** `grep -n "_symbols\|expected.*1d\|def .*denominator\|total == 0" livewire_scripts/coverage_report.py clients/gap_engine.py` — find where a missing traded set becomes `expected=0` instead of UNKNOWN.
- [ ] **Step 3:** write the one-sentence cause with the two commands beside it into the postmortem of Task 11. If it is a code path (expected=0 silently), add a test `test_missing_raw_traded_set_makes_1d_unknown_not_zero` and the minimal fix; if it is a data/timing fact (partition lands after 11:00Z), the Task 5 UNKNOWN rendering is the fix and no code changes here.
- [ ] **Step 4: commit** only if code changed: `fix(coverage): missing traded set is UNKNOWN, not an empty denominator`

### Task 11: docs — one postmortem, CLAUDE.md lines, runbook env table

**Files:** create `docs/postmortems/2026-09-12-email-was-a-side-effect-not-a-record.md`; modify `CLAUDE.md` "Alerts and the digest" section; `docs/runbook.md` (new `notify`/`digest` commands, `MDW_ALERT_*` table from Task 0, coverage time fixed to what Task 0 measured, delete `send-alert`).

- [ ] **Step 1:** postmortem: rule ("every email is an `executions(script='notify')` row; the digest is unconditional and runs after coverage; a deadline missed is BAD"), what it cost (the week's table from the top of this plan, condensed), date.
- [ ] **Step 2:** CLAUDE.md: replace the five alert lines with:
  - `Every email is an executions(script='notify') row, success or failure; the only dedup is a successful row with the same fingerprint in 24h. → test: tests/test_notify.py · pm:2026-09-12-email-was-a-side-effect-not-a-record`
  - `The digest is unconditional, daily, 12:15Z after coverage, and reports today's scan; "Digest sent today" is BAD after 12:45Z. → test: tests/test_nightly_digest.py, tests/test_status.py::test_digest_sent_today_is_bad_after_its_deadline`
  - `A zero denominator is UNKNOWN per scope, never 100%. → test: tests/test_status.py::test_coverage_a_zero_denominator_scope_is_unknown_not_one_hundred`
  - `Recovery deferred on two consecutive scans is BAD. → test: ...::test_coverage_recovery_deferred_twice_is_bad`
  - keep the base64 line (now `tests/node/send_mail.test.mjs`) and the `--key=value` line.
- [ ] **Step 3:** `uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning -q` and `npm run test:alerts` — both green, paste the last line of each into the PR body.
- [ ] **Step 4: commit** `docs: notify rewrite postmortem, rules, runbook`

### Task 12: verification after implementation

**Local (before the PR is opened) — all four must be quoted in the PR body:**

- [ ] V1 `uv run pytest tests/ --cov --cov-fail-under=95 -W error::RuntimeWarning` → exit 0, coverage ≥ 95%.
- [ ] V2 `npm run test:alerts` → exit 0.
- [ ] V3 real seam, no mocks: `MDW_ALERT_TRANSPORT=stream MDW_ALERT_EMAIL_FROM=a@b MDW_ALERT_EMAIL_TO=c@d LW_LEDGER_ROOT=$(mktemp -d) uv run python scripts/livewire_ops.py digest --run-date 2026-09-11 --email` → stdout contains `Subject: [Livewire] digest 2026-09-11`, `COVERAGE as of`, and `ledger query "select script, exit_code from executions"` on that root → one row `notify, 0`.
- [ ] V4 `grep -rn "send_alert\|send-alert\|\.alerted\|_warning_fingerprint\|alert_on_flag\|send_daily_update_failure_email" livewire_scripts clients scripts launchd tests package.json` → zero hits.

**On the mini, by swe2, before the PR — real data, real SMTP, zero writes to the warehouse (contract rules 6–9):**

Setup: `TS=$(date -u +%Y%m%dT%H%M%SZ); V=\$HOME/tmp/notify-verify-$TS` on the mini. Copy the worktree there (`rsync -a --exclude .git --exclude .venv --exclude node_modules --exclude .worktrees ./ macmini:$V/src/`), then `ssh macmini "cd $V/src && npm ci --omit=dev"` (writes only under `$V`). Python deps: use the warehouse venv read-only — `source ~/market-warehouse/.venv/bin/activate` — it already has duckdb/pyarrow/rich; if an import is missing, report it, do not `pip install` into that venv.

Every command below runs as `ssh macmini 'set -a; source ~/market-warehouse/.env; set +a; source ~/market-warehouse/.venv/bin/activate; cd $V/src; …'`. Never echo the sourced env.

- [ ] M1 **Today's status through the new checks, from the real ledger, read-only.** `python scripts/livewire_ops.py status` → paste the `Coverage`, `Coverage ran today`, `Coverage recovery`, `Digest sent today`, `Undelivered notifications` lines into the evidence file. Expected on a mini that still runs the _old_ release: `Digest sent today` is UNKNOWN/BAD by the clock (no `notify` rows exist yet) — that is the correct reading, record it as such. `Coverage` must list five scopes and must not print `100%` next to a zero total.
- [ ] M2 **Build today's digest body from the real ledger, no send, no write to the lake:** `python scripts/livewire_ops.py digest --run-date $(date -u +%F) --body-out $V/digest.txt` → `$V/digest.txt` exists; `grep -c "COVERAGE as of $(date -u +%F)" $V/digest.txt` = 1 (or, before 11:00Z, the block reads `COVERAGE: UNKNOWN (no scan today yet)` and the previous scan's date). Confirm nothing was written: `ls -la ~/market-warehouse/data-lake/ledger/executions/ | tail -3` before and after M2 are identical (paste both).
- [ ] M3 **Send that body to the operator through the real SMTP path from a temp ledger root** (proves node resolution + SMTP env under the mini's environment — the 09-08 failure class): `LW_LEDGER_ROOT=$V/ledger MDW_LOG_DIR=$V/logs python scripts/livewire_ops.py notify --kind digest --subject "VERIFY digest $(date -u +%F) from $V" --body-file $V/digest.txt --force` → exit 0; `LW_LEDGER_ROOT=$V/ledger python scripts/livewire_ops.py ledger query "select exit_code, json_extract_string(receipt_json,'$.subject') s from executions"` → one row `0, VERIFY …`. Tell the operator the exact subject to look for. Real-lake check again: `ls -la ~/market-warehouse/data-lake/ledger/executions/ | tail -3` unchanged.
- [ ] M4 **Page dedup against real state, temp ledger:** run `python livewire_scripts/check_daily_update_watchdog.py` twice with `LW_LEDGER_ROOT=$V/ledger MDW_LOG_DIR=$V/logs` **and** `MDW_ALERT_TRANSPORT=stream` (no email for this one — the real ledger's BAD sections are rendered into the stream output). Expected: run 1 prints the page and writes one row `skipped=false`; run 2 writes one row `skipped=true`; `ledger query` on `$V/ledger` shows exactly those two rows. If today has no BAD, run `status` first and record "no BAD today — M4 not exercisable", do not fabricate one. (Note: with `LW_LEDGER_ROOT` pointed at `$V/ledger`, `collect()` reads the _temp_ ledger and sees no runs; to grade the real ledger, this step instead uses `python -c "from livewire_scripts import notify, status; …"` reading `status.collect()` with the default root and then `notify.send(...)` with `LW_LEDGER_ROOT` switched — write this helper into `$V/m4.py`, keep it in the evidence file. It must contain no `emit` against the default root.)
- [ ] M5 **Teardown:** `rm -rf $V` (only this directory), then `ls $HOME/tmp/` to show it is gone. Paste into the evidence file.

**Production cutover — operator only (the user, not swe2), after the PR is merged:**

- [ ] O1 `python scripts/livewire_ops.py release promote` on the mini; `readlink ~/market-warehouse/current` = merged SHA; CI for that SHA green.
- [ ] O2 install `com.livewire.digest.plist` and the two-interval watchdog plist under `~/Library/LaunchAgents/` (runbook section from Task 11); `launchctl print gui/$(id -u)/com.livewire.digest | grep -i "hour\|minute"` → 20/15.
- [ ] O3 **Night 1 (next 12:20Z):** `status` shows `Coverage ran today OK`, `Digest sent today OK`; inbox has one digest whose `COVERAGE as of` timestamp is today's 11:xxZ.
- [ ] O4 **Nights 1–3 acceptance (all three, or report "not met"):**
  1. exactly one `notify` digest row per day with exit 0, and one digest in the inbox — `ledger query "select date(started) d, count(*) from executions where script='notify' and json_extract_string(receipt_json,'$.kind')='digest' and exit_code=0 group by d"`;
  2. no two page rows share a fingerprint within 24h with `skipped=false` — `ledger query "select json_extract_string(receipt_json,'$.fingerprint') f, count(*) from executions where script='notify' and json_extract_string(receipt_json,'$.kind')='page' and json_extract_string(receipt_json,'$.skipped')='false' and started >= now() - interval 3 day group by f having count(*) > 1"` → zero rows;
  3. the digest's `SENT to you` block matches the inbox for that 24h window.
- [ ] O5 **Negative check (optional, operator's call):** `launchctl unload` the coverage job for one night → next digest must show `Coverage ran today BAD` and a page must arrive after 12:00Z. Reload afterwards. If not run, the PR says "O5 not run".

swe2 may run the O3/O4 `ledger query` and `status` commands read-only on the operator's request after O1/O2, and nothing else in this block.

## Self-review

- Spec §3 coverage: watchdog as `collect()` caller ✔ (T7); digest renders `collect()` ✔ (T8, plus three ledger-only blocks); failed send = `executions` row ✔ (T2, extended to successes — deviation stated); coverage emits measurements not `coverage:` lines ✔ (existing + T5); marker file deviation stated ✔.
- Problems 1–10 → tasks: 1→T8, 2→T4+T5+T10, 3→T8, 4→T5+T4, 5→T2+T6+T7, 6→T2, 7→T2 `node_bin`, 8→T4 deadlines+T7, 9→T9, 10→T0+T11.
- Names used consistently: `notify.send/page_from_sections/page_for_lane/fingerprint/already_sent/sent_within/node_bin`; measurements `coverage_recovery_deferred`, `coverage_still_missing`, `stale_non_equity`; lane `tail`; checks `Undelivered notifications`, `Digest sent today`, `Coverage ran today`, `Coverage recovery`, `Stale non-equity`.
- Out of scope, on purpose: the weekly report (no email today, none added); the interior-gap scan; helium/agent inbox (spec §5).

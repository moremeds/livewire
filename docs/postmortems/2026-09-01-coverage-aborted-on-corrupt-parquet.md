# One unreadable parquet aborted the whole coverage scan, nine times

**Rule:** A corrupt per-symbol parquet is quarantined by the publisher and counted **missing** by every reader. A read-only detector never raises on one file, and never repairs one either.

**Incident / measurement:**

`livewire_scripts/coverage_report.py` read parquet footers inside `pool.map` with
no guard, so one torn file raised out of the thread pool and killed the entire
`com.livewire.coverage` run. Roughly 70,000 files measured nothing because one
of them was truncated.

Measured on the mini, `/tmp/com.livewire.coverage.stderr.log`:

- **9 aborts**, the last 2026-09-01 19:40 HKT; `launchctl list` showed the job at
  exit 1.
- Both files failed with `pyarrow.lib.ArrowInvalid: Parquet magic bytes not found
in footer` — `symbol=IGA/5m.parquet` and `symbol=VSLU/1m.parquet`.
- Coverage logs are absent for days around each abort: `08-14`, `08-17`..`08-21`,
  `08-25`, `08-28`, and nothing after, against a job that runs daily.

The lake is on an external exFAT volume and bronze publishes by `os.replace()`,
so a torn file is a normal operating condition rather than an exceptional one.
Both symbols were later healed by the flat-file lane on its own
(`2026-08-30 18:38 WARNING IGA: rebuilding corrupt …`), which is why the symbol
looked fine by the time anyone looked — and why the last abort, at 19:40 on
2026-09-01, was on neither of them. **The bug is a file class, not a symbol.**

The write path had been fixed for exactly this in 2026-07-14
([pm:2026-07-14-corrupt-parquet-aborted-publish](2026-07-14-corrupt-parquet-aborted-publish.md)):
`flatfile_publisher.quarantine_corrupt_parquet` moves the file aside, reports the
symbol and publishes the rest of the market. The read path never got the same
treatment, so the same class of file kept taking the detector down for another
seven weeks.

**Why unreadable → `None` and not a new failure channel:** `present_symbols`
requires `latest >= target_date`, so `None` already reads as MISSING. The symbol
stays in the coverage ratio, the `missing:` log line and the alert, and the path
is named at ERROR. This is the same argument the stale-cache-entry case rests on
— it over-reports gaps, it cannot hide one. Nothing is downgraded to silence.

**Why the detector does not quarantine:** moving a file aside belongs to the
write path that owns the data. A read-only scan that mutates bronze is a worse
bug than the one it fixes.

**The generalisation worth keeping:** a writer and a reader over the same corrupt
file must fail in opposite directions — the writer makes the loss explicit by
moving it, the reader makes it visible by counting it absent. Neither aborts.
Fixing one and not the other is what turned a known 2026-07 incident into a
2026-09 outage.

**Source:** new incident, 2026-09-02. Fix and test in PR #97.

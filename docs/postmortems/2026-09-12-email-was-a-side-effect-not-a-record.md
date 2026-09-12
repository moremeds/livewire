# Email was a side effect, not a record

Rule: every email the operator receives is one `executions(script='notify')`
row — success, failure, and dedup skip alike — and the only email paths are the
deduplicated _page_ and the unconditional daily _digest_, both rendered from
the ledger. A send that leaves no row never happened.

Observed on `moremeds-Mini`, week of 2026-09-06 → 2026-09-12.

Five emitters existed, each with its own receipt convention — or none. The
digest ran as the tail of the 05:00Z daily-update job, so it could only ever
report yesterday's coverage; coverage itself ran at 11:00Z. A fingerprint
dedup made an unchanged BAD produce _no_ digest at all — silence read as OK
(09-06, 09-07). A page storm had no dedup (five pages for one run on 09-06)
while an incident day produced zero (09-09), because pages were shaped by
retries, not by state. One undelivered page (`FileNotFoundError: 'node'`,
09-08 10:45Z) existed because two different node resolvers lived in two
different senders. And no table answered "what was I sent this week": failure
alerts wrote `executions` only on failure, the digest only on success, and the
coverage alert wrote nothing.

Cost: one week where the operator could not tell a healthy silent system from
a broken one, one undelivered page, and a per-ticker quality-flag email path
(the 2026-07-19 storm lineage) still alive four months after its incident.

Now: `livewire_scripts/notify.py` owns rendering, 24h fingerprint dedup for
pages, dispatch and the receipt; `livewire_node/send_mail.mjs` owns SMTP only.
Coverage and quality flags emit measurements and sidecars — facts the digest
renders, never mail. The watchdog and the digest both grade through
`status.collect()`, so a check reaches both surfaces or neither.

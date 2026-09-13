# Corporate actions mutated outside the ledger

Rule: an agent-driven repair that writes the lake goes through an entrypoint
subcommand that emits `runs` + `measurements`, or it did not happen. The
ledger is what tells us when Sunday's reconcile reverts it.

Observed on `moremeds-Mini`, 2026-09-13.

`grok_index/gaps/dividend_fx_conversion_applied.json` records 39 dividend rows
superseded in the corporate-action store — foreign-currency amounts converted
to the equity's currency via FX EOD bars. No script under `grok_index/`
performs that write: `executions` has no row for it, `runs` has no job for
it, `measurements` has no count of it. The same shape recurred in
`grok_index/pit_membership/`: ~30 years of add/remove events keyed by ticker
with no `security_id`, no evidence hash, no `known_at`, under a README
claiming weekday 09:00–09:15 HKT auto-updates that no launchd job or crontab
performs.

The write was also revertable. The converted rows' `source_ref` recorded the
FX provenance, but nothing recorded whether `provider` changed; had it stayed
`massive`, the next provider-scoped `--full-reconcile` would see the same
`provider_event_id` with a different `payload_hash` and supersede the
converted row back to CAD — silently, because a reconcile reports only its
own counts.

Cost: an invisible mutation is invisible twice — `status` and the digest
could not see that it happened, and nobody could see when it stopped or was
reverted. The membership panels read "auto-updated" over a static snapshot.

Now: `corporate-actions convert-dividend-currency` writes the repair through
the store as `provider='eod_fx'` superseding rows with FX-bar evidence in the
CAS, emits a `dividend-fx` run and `dividend_currency_mismatch` /
`dividend_fx_converted` / `dividend_fx_skipped` measurements, and reconcile
treats an active non-`massive` head as the current answer
(`tests/test_corporate_action_store.py::test_full_reconcile_after_conversion_leaves_the_eod_fx_row_active`).
The `Foreign-currency dividends` status check surfaces any remaining count in
`status` and the digest.

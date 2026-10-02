# Loaded launchd jobs and maintenance isolation

On 2026-09-30 eight participating launchers were disabled without unloading
their jobs. Fresh process checks found no writers before the two approved CBOE
batches; those batches ended at 09:34:31Z and 09:45:32Z. All 42 output hashes
still matched their receipts at 10:10:35Z.

The later manual catalog scan ran 09:52:27Z–10:00:58Z. The loaded
`com.livewire.intraday-catchup` job started at its 10:00:03Z schedule despite
its disabled flag. Its equity writer overlapped the scan. Identical published
catalog hashes and stable volatility files establish successful publication
and the 42-symbol volatility projection; they do not establish a coherent
all-source snapshot. That acceptance awaits the normal job's final publisher.
The prior enabled states were restored; no normal job was interrupted.

Do not use disable plus a point-in-time process check as an exclusive window.
Before the next manual writer, qualify a reversible mechanism that excludes
loaded scheduled launches after natural drainage, and restores the captured
loaded and enabled states. Test scheduled-launch exclusion on an isolated test
service before using it in production. This note introduces no service,
scheduler, runtime code, or new data operation.

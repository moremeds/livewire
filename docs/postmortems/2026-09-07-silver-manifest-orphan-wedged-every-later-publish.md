# A manifest orphaned by SIGKILL wedged every later Silver publish

**Rule:** The Silver publisher reconciles `revisions/` before it computes a revision number — a manifest at exactly `current + 1` that parses as schema 1 and self-identifies as that revision is adopted, anything else is quarantined, never deleted.

**Date:** 2026-09-07. Host: macmini (production) is where the kill happened; the fix and its tests are MacBook-verified.

**Incident / measurement:**

`clients/silver_revision.py::_publish_locked` committed in two steps:
`_write_immutable(revision=N.json)` with `open("xb")`, then `_replace_current(payload)`,
with a `try/except` that unlinks the immutable file if the pointer swap fails.

`except` does not run under SIGKILL. A kill in that window leaves `revision=N.json`
on disk while `current.json` still names N-1. Every later publish reads current,
computes the same N, and dies in `open("xb")` with `FileExistsError` — not once, but
on every run from then on, with nothing on any recovery path to clear it. The silver
lane was SIGKILLed by its 7200s lane budget on 2026-09-06 (`outcome='timeout'`,
exit 124); that kill landed during artifact writing rather than during the manifest
write (PR #117), so this window was the remaining un-recovered one in the same
transaction.

**The twin asymmetry** (`CLAUDE.md` "How to work in this repo" §5): the recovery
already existed, in the module that does not run at 03:00.
`clients/pit_silver_revision.py::_recover_orphans` — added for the point-in-time
publisher, which nothing schedules — has adopted-or-quarantined orphans since it was
written, and its `_write_immutable` is idempotent on identical bytes as well. The
scheduled publisher, the one a lane budget actually kills, had neither.

Both entry points needed it. `publish()` recovers inside `_publish_locked`;
`transaction()` recovers *before* it reserves a revision number, because the
reservation is taken at `__enter__` and `commit()` treats a revision other than the
reserved one as fatal ("reserved Silver revision was not committed") — recovering
only in `_publish_locked` turns the wedge into that error instead.

**Deliberately not copied from the PIT sibling:** its `verify()` re-hashes every
artifact the manifest names, and its precondition raises when `current.json` and its
immutable file disagree. Re-hashing the Silver tree on every publish means a full
cold read of an exFAT lake (cf. `2026-08-02-coverage-budget-expired-silently`), and
the publisher hashed those artifacts already, immediately before writing the file
being recovered. The validation is the honest minimum: the JSON parses, the schema is
1, and `revision` equals the number in the filename.

**Cost:** every Silver publish after the kill fails identically until someone moves
the file by hand; Apex keeps serving revision N-1 while bronze advances.

**Test:** `tests/test_silver_revision.py::test_an_orphaned_manifest_no_longer_wedges_every_later_publish`
and the four cases around it (adoption before the reservation, out-of-sequence,
non-schema-1, filename/body disagreement, chained orphans).

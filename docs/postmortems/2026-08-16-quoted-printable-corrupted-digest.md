# Never send a body as quoted-printable — = is the payload here

**Rule:** Never let a mailed body be encoded quoted-printable; `sendFailureAlert` sets `textEncoding: "base64"` for every mode.

**Incident / measurement:**

- ⚠️ **Never send a body as quoted-printable — `=` is the payload here.** Every
  body this repo mails is `key=value` telemetry, and QP reads `=NN` as the byte
  `0xNN`. Audited 2026-08-16 against the delivered mail: the on-disk digest read
  `revision=28 rebuilt=10 unchanged=13135 trimmed=256 failed=240 no_trade=972`
  and what arrived read `revision( rebuilt\x10 unchanged\x13135 trimmed%6
  failed$0 no_trade\x972`, in **both** the text and html parts. `last=2026-08-14`
  became `last 26-08-14` (`=20` is a space). A value survived only when its
  first two characters were not valid hex, so `updated=9` looked fine and
  `revision=28` did not — selective corruption of exactly the numbers the digest
  exists to report, and it never looked like an error. `sendFailureAlert` now
  sets `textEncoding: "base64"` for every mode. The invariant the test asserts
  is **"never quoted-printable"**, not "always base64": nodemailer sends a
  pure-ASCII body as `7bit` and only consults `textEncoding` when the content
  forces a choice, which for a real digest is the `—`/`⚠` it always carries.

**Source:** CLAUDE.md section "Scheduled-job invariants worth not re-breaking" (moved 2026-09-02)

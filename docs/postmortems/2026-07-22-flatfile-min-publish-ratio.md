# A raw file holding 12,000 symbols could publish 40 and exit 0

**Rule:** Fail a flat-file publish that covers less than MDW_FLATFILE_MIN_PUBLISH_RATIO of the raw file's ticker set, except on a resumed run.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

- `MDW_FLATFILE_MIN_PUBLISH_RATIO` (default `0.9`): minimum share of the raw
  file's ticker set a publish must cover before the run fails. Nothing checked
  this before — a raw file holding 12,000 symbols could publish 40 and exit 0.
  Skipped on a resumed run, where a low published count is legitimate.

**Source:** CLAUDE.md section "Massive S3 flat-file environment variables" (moved 2026-09-02)

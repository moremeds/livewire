# A wedged IB call blocked its phase forever with no timeout on the path

**Rule:** Bound every daily-backfill phase with MDW_SYNC_PHASE_TIMEOUT_SECONDS (default 21600, 6h).

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

- `MDW_SYNC_PHASE_TIMEOUT_SECONDS` (default `21600`, 6h): hard per-phase budget
  in `daily-backfill`. There was no timeout on this path at all, so a wedged IB
  call blocked its phase forever and launchd would not start another instance.

**Source:** CLAUDE.md section "Massive S3 flat-file environment variables" (moved 2026-09-02)

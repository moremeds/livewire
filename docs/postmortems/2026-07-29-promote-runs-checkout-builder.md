# promote exports origin/main but runs the checkout's own builder

**Rule:** Run `git checkout main && git pull` before promoting anything that changes the promoter.

**Incident / measurement:**

- ⚠️ **`promote` exports `origin/main` but RUNS the checkout's own builder.**
  The two come from different commits. A fix to `release.py` itself does not
  take effect until the checkout you run `promote` from contains it — exporting
  the fixed SHA is not enough. Measured 2026-07-29: promoting from a feature
  branch produced a release whose *source* had `build_node_modules` but whose
  *build* never ran it, so `node_modules/` was silently absent again.
  **`git checkout main && git pull` before promoting anything that changes the
  promoter.**

**Source:** CLAUDE.md section "Immutable release artifacts — production does not run from the checkout" (moved 2026-09-02)

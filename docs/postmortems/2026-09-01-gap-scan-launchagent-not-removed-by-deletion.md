# Retiring com.livewire.gap-scan does not remove an already-rendered LaunchAgent

**Rule:** After retiring a scheduled job, unload and delete its rendered LaunchAgent on every host that installed it — deleting the plist template and subcommand from the repo does not.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

⚠️ **`com.livewire.gap-scan` was retired**; its plist template and its `livewire_quality.py gap-scan` subcommand are both deleted. Deleting them from the repo does **not** remove an already-rendered LaunchAgent, which would keep invoking a subcommand that no longer parses — on any host that installed it, run `launchctl unload ~/Library/LaunchAgents/com.livewire.gap-scan.plist && rm ~/Library/LaunchAgents/com.livewire.gap-scan.plist` once.

**Source:** CLAUDE.md section "Daily updates" (moved 2026-09-02)

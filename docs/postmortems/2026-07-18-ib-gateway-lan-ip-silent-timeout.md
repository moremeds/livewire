# IB Gateway: connect to 127.0.0.1, never the LAN IP

**Rule:** Always connect to `127.0.0.1:4001`; never point MDW_IB_HOST at the mini's LAN address.

_Date from git (`git log -S` on CLAUDE.md); the bullet itself states no date._

**Incident / measurement:**

⚠️ **Connect to `127.0.0.1:4001`, never the LAN IP.** The mini's LAN address is TCP-open, so `nc -z` succeeds against it — but `TrustedTwsApiClientIPs` is empty, so an API connection there silently times out after ~4 minutes with no error. A "hanging" IB run is almost always this. An earlier version of this file framed the Gateway as remote from the working host; it is not.

**Source:** CLAUDE.md section "IB Gateway / IBC" (moved 2026-09-02)

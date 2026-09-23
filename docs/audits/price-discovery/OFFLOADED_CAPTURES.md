# Offloaded captures

Files over 1 MB were kept out of git (repo docs blobs stay under ~300 KB).
Stored on the MacBook under `~/backups/2026-09-23/price-discovery/offloaded/` (same relative path), and inside `untracked.tgz` next to it.

| path | bytes | sha256 |
|---|---|---|
| `2026-09-22-capture/eia-pswrgvwall.xls` | 6792704 | `5af9f7facd07ada639200cfe773756ab30a1ba46d988349a804d1c4df6f0c4cc` |
| `2026-09-22-capture/exports/0daabdd5851870c5a72e864f3463a8538e4e5064968564ccce0553f64bb76b7f.zip` | 1539597 | `ea39849aecbece59fec299cc15c92ac3cc8c8ff72fcb6f9f8155aaca2b5927b4` |
| `2026-09-22-capture/silver-revision-76.json` | 6825243 | `2fdc32ac718a739da439e96494e37521d937465238c6567bc4ed01d5035647df` |
| `COMMODITY_CONTRACT_INVENTORY_2026-09-23.json` | 2636476 | `b3f50a3a474aad3711221654b1d25771ce2b94e9ac124892b184fe36d26df20c` |

`silver-revision-76.json` is also committed as `2026-09-22-capture/silver-revision-76.json.gz`
(gzip -9 -n; decompresses to the sha256 above) because `tests/test_research_export.py` reads it.

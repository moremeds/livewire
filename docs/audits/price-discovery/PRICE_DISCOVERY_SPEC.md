# livewire spec — 2026-09-22

职责：数据采集与版本化导出。
先读同目录 PRICE_DISCOVERY_MASTER.md，此文件补充本 repo 的 ownership/执行顺序。

1. LW-00：检查 GLD 全历史 source segments、raw/adjusted basis、Silver generation/ledger/raw artifact，定点核对 EIA，交付 coverage inventory。
2. LW-01：复用 GLD ingest/immutable generations，补最小导出 envelope/sidecar，让 Apex 能冻结确切 bytes、provider、basis、revision、receipt；能复用则不改原 parquet schema。
3. LW-02：已有 Yahoo DXY 保留 source lineage，过滤未完成 daily session，记录 capture 时钟。不要把今日抓取的历史数据伪装成历史 receipt。
4. LW-03：先证明 EIA 官方 no-key 下载或 key/API 可用，选定 gasoline 产品/地理/单位；保存真实原始响应及发布证据；复用现有 ledger/原子发布。
5. LW-04：只有 Argon evidence 实测不够时才补 DFII10/DTWEXBGS collector；指数用 general series，不用 yield schema。

锚点：livewire_scripts/fetch_fx.py、fetch_fred_rates.py；clients/fred_client.py；presets/rates.json；tests/test_fetch_fx.py、test_fetch_fred_rates.py、test_fred_client.py。
验收：更新不破坏旧 capture、hash 可复算、mixed provider 不丢身份、未知时钟明确、失败发布不覆盖完整版本、空响应不能报成功覆盖。用 repo 规定的现有测试工具跑窄测。交 Apex 一个真实冻结样本和契约；不启动生产回填。

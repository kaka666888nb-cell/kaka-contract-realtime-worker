# Step781.2.7 / Render 650.8.15.58

Production root-cause repair after the Step781.2.6 gate exposed a blocked Bybit realtime child and one false Binance window failure.

- Bybit public-trade one-second history no longer scans, filters, normalizes, and sorts the full 3600-row ring for every incoming trade. Each exact market+symbol now keeps a millisecond bucket index; an existing second is updated in O(1), and only a genuinely new out-of-order second uses binary insertion.
- All four pinned Bybit hot targets (spot/contract BTCUSDT and ETHUSDT) are created immediately and warmed in parallel. A slow restore, seed, or handshake for one target cannot prevent the following contract target from existing.
- The child remains responsive while high-activity spot and contract trades are ingested, so health/history reads no longer wait behind full-history rebuild work.
- The one-second history gate now validates the actual goal: bounded requests must cover at least 45 seconds of older time with real distinct seconds. It no longer incorrectly requires two pages when a single Binance page already covers the App's approximately 90-second target.
- Step781.2.6 Coinbase official per-match realtime, App-owned empty-second wall clock, and portrait/fullscreen bounded 90-second backfill remain unchanged.

Existing protections remain unchanged: exact provider/market/symbol identity, strict older-time progress, bounded requests, empty results never overwrite verified data, Render never fabricates empty seconds, and Binance contract direct Render REST remains permanently disabled.

# Step781.2.6 / Render 650.8.15.57

Phone-validation repair after Step781.2.5 passed the all-platform one-second history gate.

- Coinbase spot one-second realtime now uses the official Coinbase Exchange `ticker` channel, which updates per match, plus `heartbeat`. If that upstream connection closes, the worker falls back to the official Advanced Trade `market_trades` + `heartbeats` feed.
- Coinbase public market-channel `USDC` identity is mapped to the corresponding `USD` product only for the upstream subscription; the App/provider/symbol identity remains exact and unchanged.
- The visible App page owns empty natural seconds. Its zero-volume wall-clock carry is independent from WebSocket reconnects in portrait and fullscreen, so reconnecting cannot stop horizontal time advancement.
- One left-edge gesture on a one-second chart automatically continues through up to four strict older pages, targeting about 90 seconds of genuine time coverage. Each page is published incrementally; the user does not need to drag four separate times.
- Render still never fabricates empty seconds. Gap rows are generated only inside the App between verified same-provider trade seconds, with zero volume, and real trades replace the same-second carry.

Existing protections remain unchanged: exact provider/market/symbol identity, strict older-time progress, bounded requests, empty results never overwrite verified data, and Binance contract direct Render REST remains permanently disabled.

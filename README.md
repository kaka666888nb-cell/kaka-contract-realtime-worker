# Kaka Contract Realtime Worker 650.8.15.43 / Step769

This release exposes a shared current derivatives-field snapshot backed by the existing Supabase OI/ratio and funding-current caches. The Data page no longer needs its per-user 5×8 exact exchange hydration loop.

- `GET /api/contract-flow/current-snapshot?max_age_minutes=30` reads Supabase only.
- One-minute Render cache, inflight merge and ten-minute verified stale fallback.
- No new exchange REST/WebSocket connection is opened by snapshot reads.
- Existing backend full-universe rotation remains the single collection owner.
- Binance contract direct REST stays disabled.
- Existing flow/funding/liquidation persistence and retention remain unchanged.

# Kaka Contract Realtime Worker 650.8.15.44 / Step769

This release exposes a shared current derivatives-field snapshot backed by the existing Supabase OI/ratio and funding-current caches. The Data page no longer needs its per-user 5×8 exact exchange hydration loop.

- `GET /api/contract-flow/current-snapshot?max_age_minutes=30` reads Supabase only.
- One-minute Render cache, inflight merge and ten-minute verified stale fallback.
- No new exchange REST/WebSocket connection is opened by snapshot reads.
- Existing backend full-universe rotation remains the single collection owner.
- Binance contract direct REST stays disabled.
- Existing flow/funding/liquidation persistence and retention remain unchanged.


## Step769.2 backend metric rotation fix

The full-universe trade scanner now also owns one bounded OI/ratio refresh cycle. It refreshes BTC/ETH plus six rotating USDT contracts per provider every four scan cycles (about 26 minutes), with global concurrency 2 and existing provider governors/cooldowns. Shared snapshot reads still start zero exchange requests.

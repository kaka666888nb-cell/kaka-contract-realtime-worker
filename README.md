# Kaka Contract Realtime Worker 650.8.15.45 / Step770

Step770 completes the shared current derivatives snapshot. The existing bounded backend OI/ratio rotation now collects and persists current funding, next settlement, mark price and index price for the same BTC/ETH plus rotating USDT targets.

- `GET /api/contract-flow/current-snapshot?max_age_minutes=30` remains Supabase-only and starts zero exchange requests.
- Current metadata uses the same roughly 26-minute bounded rotation, global concurrency 2 and existing provider governors/cooldowns.
- Rows are bulk-upserted into `app_funding_rate_current_cache`.
- Stale metadata is never rewritten with a fresh cache timestamp.
- Binance prefers the existing official mark-price WebSocket and keeps direct Render Binance contract REST disabled.
- Fixes the Bybit current-meta symbol scope and uses Gate contract detail for next funding time.
- No new SQL, Cron, Edge function, environment variable or App file is required.

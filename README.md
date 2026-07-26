# Kaka Contract Realtime Worker 650.8.15.39 / Step767

This release unifies five-provider contract funding history under one bounded backend owner.

## Shared funding history

- Existing `GET /api/contract-funding` current/history compatibility remains.
- New read-only `GET /api/contract-funding/history?provider=okx&symbol=BTCUSDT&limit=24` reads only persisted Supabase history and starts zero exchange requests.
- Successful Binance, OKX, Bybit, Bitget and Gate history reads are upserted by exact `provider + market_type + symbol + funding_time`.
- Render merges identical in-flight requests, caches persisted exact-key reads for five minutes and may retain a verified stale response for at most thirty minutes.
- A single backend rotation runs once per hour. Each provider keeps BTC/ETH when available plus four persistent directory-rotation symbols. This is at most thirty small history requests per hour across all five venues, independent of App user count.
- Rotation cursors are persisted in `app_contract_funding_rotation_state`, so a Render restart does not reset every provider to the same first symbols.
- The old Binance core-four Cron remains temporarily for parallel comparison; exact upsert keys prevent duplicate history rows.

## Retention

- Current funding cache stale identities: 7 days.
- Funding history: 31 days.
- Cleanup runs after startup and then at most once every six hours.
- No new Cron, Edge Function or environment variable is required.

## Existing safety retained

- Binance contract REST on Render remains hard disabled.
- Binance history continues through the authenticated Edge relay background lane.
- Non-Binance REST continues through the existing provider governor with exact-key in-flight merge, Retry-After handling, hard cooldown and negative cache.
- App detail history is served from the shared history endpoint first; direct Supabase read is only a compatibility fallback.

## Deployment

1. Run `supabase/STEP767_五平台资金费率共享历史持久化与清理.sql` once.
2. Deploy this complete Render repository.
3. Confirm `/api/contract-funding/health` reports `650.8.15.39`, persistence enabled and background rotation status.
4. Confirm `/api/contract-funding/history?provider=binance&symbol=BTCUSDT&limit=24` returns `ok: true` and `exchange_requests_started: 0`.
5. Install Step767 App main.dart.

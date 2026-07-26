# Kaka Contract Realtime Worker 650.8.15.38 / Step764.1

This release replaces the first Step764 raw-history implementation with a bounded shared-bucket store.

## Shared fund-flow history

- Existing full-universe public-trade WebSocket scanner continues writing verified 5-minute rows to `app_contract_flow_5m_cache`.
- Supabase RPC `kaka_refresh_contract_flow_15m_cache` aggregates only recent raw rows into private table `app_contract_flow_15m_cache`.
- `GET /api/contract-flow/history?period=15m&hours=168` reads at most about 672 persisted 15-minute rows instead of repeatedly downloading tens of thousands of raw rows.
- Render refreshes the shared 15-minute table at most once per five-minute boundary and merges concurrent callers.
- The endpoint response is cached for five minutes; a verified stale response may be retained for up to thirty minutes if Supabase is temporarily unavailable.
- App users all read the same backend buckets. The history endpoint never opens an exchange REST or WebSocket connection.

## Retention and cleanup

- Raw flow and position 5-minute rows: 8 days.
- Shared 15-minute rows: 31 days.
- Cleanup RPC runs once after startup and then at most once every six hours.
- Render memory history cache is capped at 8 keys and expired after 30 minutes.
- Existing active stream limits, idle eviction, persistence queue coalescing and Binance REST prohibition remain unchanged.

## Exchange request protection retained

- Main flow collection uses public trade WebSockets, not repeated REST polling.
- Scanner rotation defaults to 390 seconds.
- Per-cycle batches: Binance 18, OKX 8, Bybit 8, Bitget 8, Gate 8.
- Maximum active flow states: 80; Binance maximum: 24.
- Binance WebSocket connect gap: 2.5 seconds; maximum 40 attempts per five minutes.
- Repeated App reads of `/market-snapshot` do not rotate the scanner.
- Provider REST governor still merges identical in-flight requests and applies Retry-After, cooldowns and negative cache.

## Deployment

1. Run `supabase/STEP764_1_后台共享资金桶存储与清理.sql` once in Supabase SQL Editor.
2. Deploy this complete Render repository.
3. Confirm `/api/contract-flow/health` reports version `650.8.15.38`, table `app_contract_flow_15m_cache`, raw retention 8 days and aggregate retention 31 days.
4. Confirm `/api/contract-flow/history?period=15m&hours=168` returns `ok: true`.
5. Install the Step764.1 App main.dart.

No new exchange source, Cron, Edge Function or environment variable is required.

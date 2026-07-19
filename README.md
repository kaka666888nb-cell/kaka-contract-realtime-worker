# Kaka Web3 Contract Realtime Worker — Step650.8.15.2

Step650.8.15.2 is a Render-only auxiliary-data first-paint repair built on the validated Step650.8.13 Binance USDⓈ-M WebSocket migration.


## Step651.2D.2 bandwidth containment

- Throttles the full Binance contract last-known-good snapshot upload from every 30 seconds to every 15 minutes.
- Stops ticker/book/mark-price events from falsely marking the market universe dirty when symbol identity did not change.
- Adds read-only byte/message counters for Binance market WebSocket streams, contract-flow WebSockets, and snapshot persistence.
- Does not change App APIs, contract-flow sampling, funding, depth, liquidation, Kline, Edge relay, SQL, environment variables, or direct Binance REST protections.

## Current architecture

- Binance contract universe and 24h ticker: official production `/market` WebSocket plus last-known-good Supabase snapshot.
- Binance best bid/ask and depth: official production `/public` WebSocket.
- Binance mark price, index price, current funding rate and next funding time: official global mark-price `/market` WebSocket.
- Binance completed Kline history: official `data.binance.vision` USD-M archive.
- Exact near-current Kline bridge and allowlisted auxiliary HTTP: authenticated Supabase Edge relay.
- Binance current candle, aggregate trades, flow and liquidation: official production WebSocket.
- Render direct Binance REST: hard-disabled before network in both parent and child processes.
- No synthetic candles, interpolation, cross-exchange substitution or client-controlled validation end time.

The already deployed Edge relay remains unchanged. It is a separately isolated egress path, not a represented fixed/dedicated IP.

## Step650.8.15.2 changes

- Funding first paint reads current funding, mark price and index price from the official mark-price WebSocket and does not wait for history.
- Funding history is stale-while-revalidate and refreshes in the background through the existing authenticated Edge allowlist.
- Contract meta first paint reuses the same exact mark-price snapshot and adds current open interest from stale cache or a critical-priority Edge refresh.
- Contract flow returns a valid HTTP 200 partial snapshot immediately; it no longer waits for full ratio/OI history before first paint.
- Binance critical auxiliary requests use a 2500 ms lane, Kline remains the highest priority 3000 ms lane, and slow auxiliary history remains on the 12000 ms lane while preserving one active Edge request globally.
- Quiet Binance aggTrade windows return HTTP 200 with an empty list and `empty_reason=no_recent_trade_event` instead of leaving the App spinner on a 502/timeout.
- Existing `/market` and `/public` WebSocket migration, arbitrary-symbol 240-row Kline first paint, route ownership fix, persistent validation state and zero direct Render Binance REST are preserved.

## Health endpoints

- `/health`
- `/ws-health`
- `/api/binance-contract-market-health`
- `/api/binance-contract-kline-seed-health`
- `/api/binance-contract-kline-relay-health`
- `/api/contract-funding/health`
- `/api/contract-flow/health`
- `/api/contract-depth/health`

## Deployment

1. Completely close Kaka Web3 and quit `flutter run`.
2. Overwrite the existing Render worker repository with this package.
3. Deploy the same Render service: `kaka-contract-realtime-worker`.
4. Preserve all existing environment variables.
5. Wait at least three minutes after Render reports the service live.
6. Run the Step650.8.15.2 health-only audit.
7. Only after it reports READY, run the one-time 2Z auxiliary validation.

Do not redeploy Supabase Edge, change environment variables, modify App `main.dart`, run SQL/Cron, change `pubspec.yaml`, or run `flutter clean` for this step.

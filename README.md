# Kaka Web3 Step413.2 Contract Realtime Worker

A small Node.js WebSocket relay for Binance USD-M contract Kline streams.

- HTTP health: `/health`
- Upstream diagnosis: `/diagnose?market=contract&symbol=BTCUSDT&interval=1m`
- Browser test: `/browser-test`
- App WebSocket: `/ws?protocol=kaka.market.realtime.v1&channel=kline&provider=binance&market=contract&symbol=BTCUSDT&interval=1m`

The service listens on `process.env.PORT` and defaults to `8080`.

## Step650 Binance contract market snapshot

Step650 moves Binance USDS-M perpetual **universe and 24h ticker** traffic away from per-request REST calls:

- official all-market ticker WebSocket: `!ticker@arr`
- official all-market book ticker WebSocket: `!bookTicker`
- official contract info WebSocket: `!contractInfo`
- Supabase last-known-good snapshots in `app_market_backend_snapshots`
- REST is retained only as a low-frequency metadata refresh and is guarded by cooldown/single-flight logic
- empty or incomplete snapshots never overwrite a validated non-empty snapshot

Health endpoint:

```text
GET /api/binance-contract-market-health
```

Required Render environment variables already used by the existing worker persistence layer:

```text
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
```

Run `sql/step650_app_market_backend_snapshots.sql` in Supabase before deploying Step650.

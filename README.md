# Step781.2.1 / Render 650.8.15.52

Production audit: latest one-second history passed 11/11, while true older-page pagination passed 5/11. This version adds bounded historical pagination for OKX, Bybit and Bitget and keeps verified Binance, Gate and Coinbase paths unchanged.

# Step780 / Render 650.8.15.50

Adds a backend-shared exact spot ticker endpoint for coin-detail first paint. The App no longer calls the dedicated spot ticker Edge function or device-direct ticker fallbacks for that first paint.

# Kaka Contract Realtime Worker

Version **650.8.15.49 / Step778**.

Step777 adds a shared exact-key spot taker-flow history endpoint:

- `GET /api/spot-flow/history?provider=binance&symbol=BTCUSDT`
- `GET /api/spot-flow/history-health`
- One backend build reads the existing normalized `1m × 300`, `5m × 320`, and `1h × 180` spot Kline windows.
- Same provider+symbol reads share a 2-minute cache and in-flight request; verified stale results may be used for at most 15 minutes.
- Builds are globally bounded to 2 active and 32 queued.
- Empty or failed builds never overwrite a verified non-empty stale payload.
- This removes the App's three direct per-user Kline history reads on the spot capital page.

Unchanged boundaries:

- Binance contract direct REST remains hard disabled.
- No new SQL, Edge, Cron, environment variables, or persistent raw trade history.
- Current spot snapshot, contract flow, funding, liquidation, depth, trades, Klines and WebSocket routes remain intact.


## Step778

Adds `/api/spot-flow/snapshot` and `/api/spot-flow/snapshot-health`. The endpoint activates and reads the existing spot trade-flow periods, size-periods, and five-day daily RPCs once per exact provider+symbol key, then shares a 40-second cache with inflight merge and a five-minute verified stale fallback. App clients no longer call those four Supabase RPCs directly.

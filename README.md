# Kaka Contract Realtime Worker

Version **650.8.15.48 / Step777**.

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

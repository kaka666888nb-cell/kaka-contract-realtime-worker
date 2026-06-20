# Kaka Web3 Step413.2 Contract Realtime Worker

A small Node.js WebSocket relay for Binance USD-M contract Kline streams.

- HTTP health: `/health`
- Upstream diagnosis: `/diagnose?market=contract&symbol=BTCUSDT&interval=1m`
- Browser test: `/browser-test`
- App WebSocket: `/ws?protocol=kaka.market.realtime.v1&channel=kline&provider=binance&market=contract&symbol=BTCUSDT&interval=1m`

The service listens on `process.env.PORT` and defaults to `8080`.

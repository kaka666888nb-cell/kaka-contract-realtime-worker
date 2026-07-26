# Kaka Contract Realtime Worker 650.8.15.47 / Step773

Step773 adds a bounded backend-shared current USDT spot snapshot for the Data page.

- Endpoint: `GET /api/spot-market/current-snapshot`
- Health: `GET /api/spot-market/health`
- Five providers: Binance, OKX, Bybit, Bitget, Gate
- Per provider: 4 high-activity pairs + 16 rotating directory pairs
- Background scan interval: 5 minutes, one provider at a time
- Snapshot reads start zero exchange requests and zero exchange connections
- Empty or failed provider scans never overwrite the last verified rows
- Existing contract flow, funding, liquidation, depth, market, Kline, and Binance contract REST guards remain unchanged

No new SQL, Edge function, Cron job, environment variable, or persistent raw spot event storage is required.

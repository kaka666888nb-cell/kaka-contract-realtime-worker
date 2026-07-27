# Step781.2.4 / Render 650.8.15.55

Fixes the three remaining Step781.2.3 all-platform 1-second history gate failures:

- OKX contract history uses a strictly decreasing type=2 timestamp cursor and rejects rows newer than the requested `end_time`.
- Bybit shared 1-second history serves already-cached child rows immediately; seed/WS repair runs in the background instead of blocking the read path.
- Bitget contract history starts directly from `/api/v2/mix/market/fills-history` with `startTime`/`endTime`, then pages older data with the response tail `tradeId` as `idLessThan`.

Existing protections remain unchanged: no synthetic empty seconds in Render, exact provider/market/symbol identity, bounded pagination, empty results never overwrite verified data, and Binance contract direct Render REST remains permanently disabled.

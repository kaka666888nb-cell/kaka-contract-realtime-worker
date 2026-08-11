# Step788.1.2.1 / Render 650.8.15.70

本次只补齐`risk_controls`中的4个Coinbase声明：

- coinbase_exact_ticker_official_directory_preflight = true
- coinbase_nonexistent_product_returns_honest_empty = true
- coinbase_nonexistent_product_writes_negative_cache = true
- coinbase_mixed_batch_absent_symbol_does_not_fail_valid_neighbors = true

Step788.1.2已经通过的业务逻辑全部保持不变：

- Coinbase不存在产品不访问ticker/stats；
- exact ticker返回HTTP 404精确空；
- 60秒负缓存正常；
- 混合BTC/USD + BTC/BTC只保留真实BTC/USD；
- 11个不存在交易对K线全部HTTP 200精确空；
- K线身份预检、跨quote、跨market、资产数量0保持；
- Binance Render直连REST继续永久禁用。

App继续Step781.2.10，不覆盖main.dart。

## Step980.6.3 / Render 650.8.15.76

Side-by-side backend-only rollout. Existing App endpoints remain unchanged.

New shared endpoints:

- `GET /api/market-light/health`
- `GET /api/market-light/current-snapshot?market_type=spot|contract`
- Optional read controls: `provider=...`, `include_rows=0`, `offset=...`, `limit=...`

Scope: primary quote full-directory light data. Spot uses Binance/OKX/Bybit/Bitget/Gate USDT plus Coinbase USD; contracts use Binance/OKX/Bybit/Bitget/Gate USDT perpetual directories. Batch/all-market sources are preferred. Coinbase uses one shared public `ticker_batch` WebSocket rather than per-symbol REST. OKX contracts add batch mark-price and open-interest enrichment. Bitget prefers the public v3 product-level tickers and falls back to the existing v2 path.

Safety: snapshot reads start zero exchange requests/connections, user count does not scale upstream, failed refreshes keep the last verified rows, and missing fields stay missing rather than becoming zero. Old bounded Data Hub endpoints remain active until a later App cutover.


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

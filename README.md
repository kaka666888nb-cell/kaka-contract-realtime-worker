# Step787.1.1 / Render 650.8.15.67

本次仅补齐健康页`risk_controls`声明：

- mixed_quote_ticker_requests_grouped_by_exact_quote = true
- mixed_quote_ticker_max_quote_groups = 8
- mixed_quote_ticker_merge_identity = provider_market_symbol
- okx_usdc_contract_identity_retired_after_official_delisting = true
- okx_current_contract_quotes = [USDT, USD]

Step787.1已经通过的业务逻辑保持不变：

- 混合报价币ticker按精确quote分组；
- 最多8组、2组并发；
- provider governor/cache/inflight继续复用；
- 按provider+market+symbol精确合并；
- OKX USDC合约旧身份已从市场、资金流、深度、资金费率、清算移除；
- OKX USDT/USD保持；
- Binance Render直连REST继续永久禁用。

App继续Step781.2.10，不覆盖main.dart。

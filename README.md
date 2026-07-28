# Step787.1 / Render 650.8.15.66

本次修正：

1. `/api/tickers`混合报价币请求
- 按每个symbol的精确quote分组；
- 每次最多8个quote组；
- 最多2组并发；
- 继续复用原provider governor、cache和inflight；
- 最后按provider+market+symbol精确合并；
- 不再只看第一个symbol的quote并静默丢弃后续symbol。

2. OKX合约USDC旧身份退役
- 市场目录：OKX只保留USDT、USD；
- 资金流、深度、资金费率、清算同步拒绝OKX USDC；
- 资产报价汇总不再统计不存在的OKX USDC合约；
- OKX USD合约保持不变。

3. 安全
- App继续Step781.2.10，不覆盖main.dart；
- Binance Render直连REST继续永久禁用；
- 不改SQL、Edge、Cron或环境变量。

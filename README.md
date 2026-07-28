# Step788.1.2 / Render 650.8.15.69

Coinbase不存在产品Ticker真实空态修正：

- Coinbase ticker请求先查询当前官方产品目录；
- 精确symbol不存在时直接返回空，不访问`/ticker`或`/stats`；
- `/api/spot-market/exact-ticker`将其写入60秒负缓存并返回HTTP 404真实空；
- 不再把Coinbase官方404转换成Render 503；
- 混合批量ticker中，不存在的quote组返回空，不影响真实邻居symbol；
- 已有K线身份预检、正负缓存、跨quote和跨market隔离保持不变；
- 官方目录读取失败仍抛错，不写负缓存，不伪装为空；
- App继续Step781.2.10，不覆盖main.dart；
- Binance Render直连REST继续永久禁用。

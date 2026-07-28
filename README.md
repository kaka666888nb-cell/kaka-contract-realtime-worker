# Step788.1 / Render 650.8.15.68

- 所有K线请求先按provider+market+symbol核对官方目录。
- 目录确认不存在时，本机返回HTTP 200精确空数组。
- 不再请求交易所K线、成交历史或Binance Edge Relay。
- 不再把unknown-symbol错误转换成Render 502。
- 正身份缓存300秒，负身份缓存60秒，最多512个key。
- 同一精确key并发合并。
- 官方目录读取失败时抛错，不写负缓存，不伪装为空。
- 不跨provider、market或quote补数据。
- App继续Step781.2.10。
- Binance Render直连REST继续永久禁用。

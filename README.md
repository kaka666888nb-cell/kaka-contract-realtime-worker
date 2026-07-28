# Step786.2 / Render 650.8.15.64

- Gate现货更旧成交使用官方`last_id + reverse=true`游标。
- 严格接受`trade.time < exact end_time`。
- 最多8页，每页1000条。
- Step786.1会触发502的30天`from/to`范围扫描已退役。
- 游标异常且没有取得任何更旧成交时，仅回退Gate官方1秒K线。
- 不生成假秒线，不跨平台、市场或报价币补数据。
- App继续Step781.2.10，不修改main.dart。
- Binance Render直连REST继续永久禁用。

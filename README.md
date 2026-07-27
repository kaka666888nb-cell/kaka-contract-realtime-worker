# Step781.2.9 / Render 650.8.15.60

- Coinbase all real spot quote identities share the same directory/ticker/WebSocket/1-second-history parser.
- Coinbase trade history uses official `CB-AFTER` cursor checkpoints, 1000 trades per page, at most 12 pages per request, and no longer restarts from latest on every left drag.
- Coinbase USD/USDT/USDC/EUR/GBP/BTC/ETH and any other actual directory quote are handled by exact native product identity.
- Six spot and five contract providers retain strict provider/market/symbol/quote isolation.
- Render direct Binance contract REST remains hard disabled.
- Asset quote discovery now uses the same expanded exact quote set as directory/ticker/realtime/history, and Coinbase USDT is never cross-aliased to USD.

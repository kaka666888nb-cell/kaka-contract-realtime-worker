# Kaka Contract Realtime Worker

## 650.8.15.42 / Step768

This release adds bounded, shared five-provider liquidation hour buckets.

### Shared liquidation history

- `GET /api/contract-liquidation/history?hours=6&limit=2500`
- `GET /api/contract-liquidation/health`
- Storage: `app_contract_liquidation_1h_cache`
- Exact identity: `provider + market_type + symbol + bucket_start`
- Aggregate retention: 15 days
- Raw liquidation events are **not** persisted
- History reads use Supabase only and start zero exchange requests
- Five-minute Render response cache with in-flight coalescing
- Verified stale response retained for up to 30 minutes on Supabase failure
- Queue-coalesced writes flushed once per minute
- Cleanup on startup and at most once every six hours

Existing single-venue live liquidation, funding, flow, depth, Kline and Binance REST guards remain unchanged.

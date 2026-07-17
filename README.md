# Kaka Web3 Contract Realtime Worker — Step650.8.11

Step650.8.11 isolates Binance USDⓈ-M historical Kline HTTP from the Render process after the Render outbound IP received an upstream HTTP 418 ban.

## Architecture

- Binance contract universe/ticker/book ticker/contract info: official production WebSocket + last-known-good Supabase snapshot.
- Binance contract completed history: official `data.binance.vision` USD-M archive.
- Exact near-current historical bridge: authenticated Supabase Edge Function `kaka-binance-contract-kline-relay`.
- Existing Binance public HTTP needed by funding, contract meta/position metrics, aggregate trades, and guarded Spot fallbacks is also routed through the same strict Edge allowlist after staged validation completes.
- Current candle: official production Binance Kline WebSocket.
- Render direct Binance REST: hard-disabled before network in the parent and child processes.
- No synthetic candles, cross-exchange substitution, interpolation, or client-controlled validation end time.

The Edge relay is a separately isolated egress path; it is not represented as a dedicated/fixed IP. It permits one allowlisted `/fapi/v1/klines` call per Render relay request, performs no retry, and reports upstream restriction telemetry back to the durable Render guard.

## Safety controls

- Existing `KAKA_BINANCE_VALIDATION_KEY` authorizes validation start/reset; it is never printed or stored in the package.
- Edge Function requires the Supabase service-role bearer token and should be deployed with normal JWT verification.
- Before BANANAS31/BCH validation completes, ordinary Binance public HTTP relay calls are blocked; only the exact token-locked validation Kline request can use the Edge path. After completion, existing funding/meta/metrics/Spot fallback calls resume through the allowlist rather than the Render IP.
- Authenticated GET preflight verifies the deployed Edge version without contacting Binance.
- Relay FIFO is bounded to six pending requests and starts requests at least 12 seconds apart.
- 403/418/429/451 immediately open a durable relay cooldown using the official ban time when available plus 90 seconds.
- Relay guard state is stored in the existing `app_market_backend_snapshots` table; no SQL migration is required.
- Validation preserves the already passed `1000SHIBUSDT` and `ARCUSDT` results and continues only with `BANANAS31USDT`, then `BCHUSDT`, both 15m/240.
- Successful Kline telemetry is serialized before validation completion persistence, preventing an older write from overwriting `validation_next_index`.
- Reset refuses to clear a token while a real relay request still owns the network slot.

## Health endpoints

- `/health`
- `/api/binance-contract-market-health`
- `/api/binance-contract-kline-seed-health`
- `/api/binance-contract-kline-relay-health`

## Validation endpoints

- `POST /api/binance-contract-kline-relay-validation-start`
- `POST /api/binance-contract-kline-relay-validation-reset`

Legacy direct-REST probe/reset routes return HTTP 410.

## Deployment

1. Deploy `supabase/functions/kaka-binance-contract-kline-relay` normally. Do not use `--no-verify-jwt`.
2. Deploy this Render Worker while preserving all existing environment variables, including `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and `KAKA_BINANCE_VALIDATION_KEY`.
3. Wait for deployment stabilization, run the health-only audit, then run the continuation validator only when it reports READY.

No App file, SQL, Cron, `pubspec.yaml`, or `flutter clean` change is required.

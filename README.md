# Kaka Web3 Contract Realtime Worker

Current backend version: **Step650.4**. The service keeps the legacy realtime Kline relay while also providing multi-platform contract flow/depth/liquidation/funding and persistent Binance contract market/Kline snapshots.

- HTTP health: `/health`
- Upstream diagnosis: `/diagnose?market=contract&symbol=BTCUSDT&interval=1m`
- Browser test: `/browser-test`
- App WebSocket: `/ws?protocol=kaka.market.realtime.v1&channel=kline&provider=binance&market=contract&symbol=BTCUSDT&interval=1m`

The service listens on `process.env.PORT` and defaults to `8080`.

## Step650 Binance contract market snapshot

Step650 moves Binance USDS-M perpetual **universe and 24h ticker** traffic away from per-request REST calls:

- official all-market ticker WebSocket: `!ticker@arr`
- official all-market book ticker WebSocket: `!bookTicker`
- official contract info WebSocket: `!contractInfo`
- Supabase last-known-good snapshots in `app_market_backend_snapshots`
- REST is retained only as a low-frequency metadata refresh and is guarded by cooldown/single-flight logic
- empty or incomplete snapshots never overwrite a validated non-empty snapshot

Health endpoint:

```text
GET /api/binance-contract-market-health
```

Required Render environment variables already used by the existing worker persistence layer:

```text
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
```

Run `sql/step650_app_market_backend_snapshots.sql` in Supabase before deploying Step650.

## Step650.2 ticker isolation and Binance contract Kline seed

Step650.2 fixes two remaining failures found during real-device validation:

- Binance contract `/api/universe`, `/api/tickers`, and `/api/klines` bypass the old provider-wide REST circuit because they now use independent WebSocket snapshots or the official public data archive.
- A missing/delisted requested ticker returns an empty row for that symbol instead of opening a provider-wide circuit that hides valid BTC/BNB/BCH tickers.
- Historical Binance contract candles are seeded from official `data.binance.vision` USD-M daily/monthly ZIP archives and persisted in the existing `app_market_backend_snapshots` table (`snapshot_type=klines`).
- The current live candle continues to use the existing official Binance Kline WebSocket.

Additional health endpoint:

```text
GET /api/binance-contract-kline-seed-health
```

No new environment variable, SQL table, dependency, or Supabase Edge deployment is required after Step650.

## Step650.3 current-day Kline continuity bridge

Step650.3 closes the gap between the last completed Binance public archive day and the current live candle without mixing another exchange or inventing candles:

- completed historical candles continue to come from the official USD-M daily/monthly archive;
- the missing current-day range is requested from official Binance USD-M Kline/continuous-Kline HTTP candidates, each with its own cooldown so one restricted endpoint cannot disable the others;
- after an on-demand symbol/interval request, the official Binance Kline WebSocket keeps the current candle and later closed candles fresh;
- archive, current-day bridge, persisted snapshot, and live rows are merged strictly by `open_time`;
- `/api/klines` now returns a `coverage` object with row count, internal gap count, missing intervals, lag to the requested end, and `continuous_to_current`;
- failed bridge candidates never trigger the old provider-wide REST circuit and never replace a non-empty last-known-good snapshot.

Additional diagnostics remain available at:

```text
GET /api/binance-contract-kline-seed-health
```

No new SQL table, environment variable, Supabase Edge deployment, Cron task, or Flutter dependency is required. Deploy and validate Render first; only install the Step650.2 App candidate after `coverage.continuous_to_current` is true.


## Step650.4 internal-gap-aware current-day repair

Step650.4 fixes a logic defect found by real Render coverage validation. A persisted snapshot could contain completed archive candles and a newly received live candle with a large gap between them. Step650.3 looked only at the newest candle, so it incorrectly started the bridge after that live candle and skipped the internal gap.

Step650.4 now:

- scans the most recent requested candle window for internal missing intervals;
- bypasses the memory fast path whenever that window is not continuous to the current candle;
- starts the official current-day HTTP bridge at the first missing interval, not merely after the newest row;
- rechecks continuity after merging archive, persisted, HTTP bridge, and live WebSocket rows;
- exposes gap-repair counters and timestamps through `/api/binance-contract-kline-seed-health`.

No synthetic candles or cross-exchange fallback are used. Acceptance remains:

```text
coverage.gap_count = 0
coverage.missing_intervals = 0
coverage.lag_intervals_to_end <= 1
coverage.continuous_to_current = true
```

No new SQL, environment variable, Supabase Edge deployment, Cron task, Flutter file, or dependency is required.

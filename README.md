# Kaka Web3 Contract Realtime Worker

Current backend version: **Step650.8**. The service keeps the legacy realtime Kline relay while also providing multi-platform contract flow/depth/liquidation/funding and persistent Binance contract market/Kline snapshots.

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
- as of Step650.8, automatic Binance contract REST metadata refresh is disabled; WebSocket plus the persistent last-known-good snapshot is the only universe/ticker path
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


## Step650.5 cold-symbol Kline isolation and non-empty retention

Step650.5 addresses real-device cases where ARC/BANANAS31 or another cold symbol could return HTTP 502, open a provider-wide App cooldown, and later replace an already displayed chart with an empty result.

- Flutter Kline cooldown keys now include provider, market, symbol, and interval.
- A failed symbol no longer disables BTC/BCH/1000SHIB or other Binance contract charts.
- Portrait and fullscreen chart refreshes never replace non-empty real rows with an empty response.
- Render cold starts try the official continuous-Kline latest page before archive downloads.
- Continuous-Kline candidates are tried first with bounded timeouts.
- transient bridge cooldown is symbol-scoped; only explicit rate/region restrictions remain candidate-global.
- intraday archive fallback uses bounded daily/monthly searches so new symbols cannot scan 24 empty months and exceed the proxy timeout.

No new SQL, environment variable, Edge Function, Cron task, or dependency is required.


## Step650.6 partial-bridge completeness validation

Step650.6 fixes the remaining Render result found in Step650.5 validation: ARC, BANANAS31, and 1000SHIB could return 240 rows but still contain one 71-candle internal gap. The first official bridge candidate returned a non-empty partial result (often only the newest live candle), and the worker incorrectly stopped before trying the exact-symbol Kline candidate.

Step650.6 now:

- validates each candidate against the full requested start/end window;
- continues to later official candidates when a response is non-empty but partial;
- merges continuous-Kline and exact-symbol Kline rows by `open_time`;
- caches a current-day bridge only when it covers the requested start, has no internal gaps, and is at most one interval behind;
- allows archive fallback when a cold-start bridge is partial;
- refuses to persist a near-current snapshot unless its recent requested window is continuous.

No synthetic candles, cross-exchange fallback, new SQL, environment variable, Edge Function, Cron task, Flutter dependency, or App file is introduced by Step650.6.


## Step650.7 shared Binance IP-ban guard and exact-symbol-first bridge

Step650.7 is based on the real Render diagnosis where all four HTTP candidates returned Binance `418 / -1003` and the response included an exact `banned until <epoch-ms>` value.

- the exact-symbol `/fapi/v1/klines` route is attempted before continuous-contract Klines, reducing partial-first duplicate calls;
- current-day bridge requests are globally serialized with at least 1200 ms between starts, so multiple App prefetches cannot burst from the same Render IP;
- the first 418/429/451 or explicit IP-ban response opens one shared bridge-wide cooldown and immediately stops the remaining fapi/www candidates;
- `banned until` is parsed from Binance's response and respected with a safety margin instead of retrying after a fixed 30 minutes;
- ordinary 5xx/network failures remain isolated to the current candidate and symbol;
- archive snapshots, non-empty App Klines, and live WebSocket candles continue to be served while the HTTP bridge is cooling down;
- `/api/binance-contract-kline-seed-health` exposes `bridge_wide_cooldown`, request pacing counters, and parsed-ban diagnostics.

No new SQL, environment variable, Supabase Edge deployment, Cron task, or Flutter dependency is required.


## Step650.8 persistent all-caller Binance REST quarantine

Step650.8 addresses the second real Render validation failure: after Step650.7 was deployed, the Kline bridge correctly stopped after the first 418, but the process still had other Binance REST callers and the in-memory Kline ban state was lost across deployment. The shared Render egress IP was therefore banned again before the next Kline validation.

Step650.8 now:

- adds one persistent Binance contract REST guard shared by Kline, funding, contract metadata, position metrics, and legacy contract aggregate-trade callers;
- stores the exact ban deadline in the existing `app_market_backend_snapshots` table using the allowed `snapshot_type=klines` namespace and a reserved `REST_GUARD:BINANCE_CONTRACT` key so a Render restart/deploy cannot forget it;
- seeds a one-time migration quarantine through `2026-07-17T20:39:46.570Z`, fifteen minutes beyond the last observed official ban deadline;
- disables all automatic Binance contract REST universe/ticker refreshes; those datasets continue through official WebSocket streams and the persistent Supabase snapshot;
- reduces the current-day Kline bridge to one documented exact-symbol endpoint (`/fapi/v1/klines`), with no `www` or continuous-Kline multi-host retry burst;
- serializes every guarded Binance contract REST request and keeps at least five seconds between request starts;
- parses both Binance's `banned until` payload and `Retry-After`, then persists the later deadline plus a safety margin;
- prevents funding/contract-meta/position-metric requests from bypassing the Kline ban guard;
- continues serving official archive rows, persistent snapshots, and WebSocket data during quarantine; no synthetic or cross-exchange candles are used.

No new SQL table, environment variable, Supabase Edge deployment, Cron task, Flutter dependency, or App file is required. The existing Step650 snapshot table is reused.

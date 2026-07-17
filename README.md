# Kaka Web3 Contract Realtime Worker

Current backend version: **Step650.8.10**. The service keeps the legacy realtime Kline relay while also providing multi-platform contract flow/depth/liquidation/funding and persistent Binance contract market/Kline snapshots.

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


## Step650.8.3 post-ban probe gate and bounded Binance REST queue

A full static audit of Step650.8 found two remaining production risks before the next real Binance validation:

1. once the time-based quarantine expired, any normal page request (funding, contract metadata, position metrics, legacy aggregate trades, or Kline) could become the first post-ban REST caller before the controlled validation script;
2. the shared FIFO was serialized but unbounded, so many different users/symbols could leave stale requests queued after their HTTP clients had already gone away.

Step650.8.3 closes both gaps:

- after the quarantine expires, **all normal Binance USD-M REST callers remain blocked** until one explicit low-weight `GET /api/binance-contract-rest-probe` succeeds against official `/fapi/v1/ping`;
- the probe is the only caller allowed through the post-ban gate; a new 418/429/451 immediately persists the exact deadline and no Kline or secondary symbol is requested;
- successful probing clears `probe_required` and persists that authorization across a Render restart;
- normal Binance REST starts are spaced by at least 10 seconds;
- the queue is bounded to six pending requests and each queued request expires after at most 25 seconds, preventing an abandoned multi-user backlog from running minutes later;
- explicit queue-full, queue-timeout, probe-required, and active-ban states are visible through `/health`;
- all restricted responses flush guard persistence before the caller returns whenever the module can await it;
- `npm run check` now syntax-checks every shipped `.mjs` module, including depth and liquidation.

No App file, SQL migration, Supabase Edge Function, Cron job, environment variable, or new dependency is required.


### Step650.8.3 validation-only gate

The audit was extended to Binance spot REST calls in the same Render process. All current Binance spot and contract REST call sites now enter the same persistent guard. After the low-weight probe succeeds, the guard remains in `validation_only` mode: only the exact-symbol Kline bridge source is allowed. Spot universe/ticker/Kline/trades, funding, contract metadata, position metrics, and legacy contract REST remain blocked until staged Kline validation has passed and a later reviewed release explicitly enables them. This prevents background users or open pages from competing with the controlled post-ban test.


## Step650.8.3 validation session lock

Step650.8.3 hardens the post-ban validation window so an ordinary App request or another caller cannot consume the first post-probe Binance REST slot. The explicit probe now returns a random validation token. Only a request carrying that token can authorize a Binance contract Kline REST bridge while the worker is in `validation_only` mode. Each validation API request is limited to one outbound Binance REST call, and the whole validation session has a four-call budget: one for 1000SHIB in phase 1 and three for ARC, BANANAS31, and BCH in phase 2. Tokenless App traffic can still read archive, WebSocket, memory, and persistent snapshots, but it cannot start Binance REST during validation.

Health output exposes only a short hash prefix and the remaining budget; it never exposes the raw validation token.


Step650.8.3 also removes the former parent/child split-brain risk. Market HTTP endpoints now execute in the parent process together with contract funding/flow/metrics, while the legacy child is restricted to WebSocket relay duties. `/health`, the explicit probe, Binance Spot REST, Binance Contract REST, and Kline validation therefore observe the same in-memory queue and restriction state immediately. Internal gate errors use a non-418 status and cannot be mistaken for a new Binance IP ban.


## Step650.8.3 Binance validation hardening

- `KAKA_BINANCE_VALIDATION_KEY` is required before the public validation probe route can send any Binance REST request.
- The child WebSocket worker runs with `KAKA_DISABLE_BINANCE_REST=1`; it cannot restore/persist the Binance guard or send Binance REST.
- Binance 1-second streams seed only from the official aggTrade WebSocket in the child.
- Staged validation is server-side locked to `1000SHIBUSDT -> ARCUSDT -> BANANAS31USDT -> BCHUSDT`, all at `15m`.
- A validation request forces exactly one direct `/fapi/v1/klines` page and cannot reuse a memory bridge cache.

- Validation budget and next-symbol state are persisted successfully before each real Binance validation request leaves the process.

### Step650.8.3 final Binance safety audit additions

- The post-ban probe requires working Supabase guard persistence before it can touch Binance.
- A successful probe state is durably persisted before the validation token is returned.
- Each validation budget decrement and next-symbol transition is durably persisted before the corresponding Binance Kline request leaves the process.
- The probe must include a numeric `x-mbx-used-weight-1m` response header no greater than the conservative threshold `100`; a missing or higher value issues no validation token, sends no Kline request, and enters a local ten-minute safety cooldown.
- Persistence queue failures are surfaced by `flushBinanceRestGuardPersistence()` instead of being silently treated as success.


## Step650.8.10 completed Binance guard, shared streams, and staged release

Step650.8.10 is the first candidate that closes the full Binance safety loop rather than only delaying the next request.

- The persisted guard has three explicit modes: `probe_required`, `validation_only`, and `normal_guarded`. Four successful 15-minute exact-symbol validations move the worker into bounded normal operation; any restriction, unsafe weight, missing weight header, persistence failure, restart with an uncertain in-flight call, or administrator-key rotation fails closed and returns to `probe_required`.
- Binance `403` WAF responses are handled together with `418`, `429`, and `451`, using the official ban deadline or `Retry-After` plus a safety margin.
- Every successful Binance REST response is checked for `x-mbx-used-weight-1m`. The conservative limits are 100 for the probe, 150 during validation, and 600 during guarded normal operation.
- Probe and validation transitions must be durable in the existing Supabase snapshot before the next Binance request is allowed. An unhealthy persistence path blocks Binance network access.
- The private validation administrator key is never stored in a script or delivery package. The PowerShell validator reads it as hidden input; the temporary validation token is stored locally with Windows DPAPI and is never printed.
- Binance Spot public REST uses `data-api.binance.vision`, shares one cache/in-flight map, and still passes through the same parent-process guard.
- Binance contract validation is server-locked to `1000SHIBUSDT -> ARCUSDT -> BANANAS31USDT -> BCHUSDT`, all at `15m`, with one exact `/fapi/v1/klines` request per validation API call.
- Contract archive downloads have a process-wide active/pending limit, URL-level in-flight deduplication, and a bounded parsed-file cache.
- App Binance Kline WebSockets are shared by `market + symbol + interval`, with 64 upstream streams, 1000 total downstream clients, 250 clients per stream, connection pacing, reconnect limits, reference counting, and idle cleanup.
- Binance depth/trade WebSockets are capped at 32 symbols and now have a separate conservative connection-start governor (1500 ms gap, 60 attempts per five minutes).
- One-second candles are emitted only for seconds that contain official trades. Empty seconds are not fabricated as zero-volume OHLC candles.
- The WebSocket-only child process cannot issue Binance REST, and the parent health endpoint reports the same guard that actually sends Binance REST.

Step650.8.10 reuses the existing `app_market_backend_snapshots` table and the existing Render environment variables. It requires no SQL migration, Supabase Edge deployment, Cron change, App file, Flutter dependency, or `flutter clean`.


## Step650.8.10 validation recovery and Kline WS pacing

- Validation sessions expire after two hours and require a fresh probe.
- An authenticated local reset endpoint clears stranded validation state without calling Binance.
- Probe timeout/network/5xx outcomes enter a durable ten-minute local cooldown before retry.
- On-demand Binance Kline WebSocket connection attempts are globally paced and capped at 60 per five minutes.


## Step650.8.10 restore fail-closed and reset-route correction

- Binance REST guard startup now fails closed when the Supabase guard snapshot cannot be restored; it does not overwrite a possibly newer remote ban/session state with a local fallback.
- Guard health exposes restore attempts/success/errors and the last restore error.
- The authenticated validation reset endpoint now correctly accepts POST, matching the recovery scripts; GET is rejected.
- Phase-1 recovery attempts a remote reset whenever the probe may have reached Render, including a lost client response after a successful server-side probe.


## Step650.8.10 final Binance queue and aggregate WebSocket audit

This release fixes FIFO release on post-queue persistence failures, removes production fallback to Binance Futures testnet/undocumented WebSocket paths, and caps the designed aggregate Binance WebSocket connection-attempt budget at 185 per five minutes across all modules. Validation scripts preserve existing sessions and perform authenticated no-Binance recovery on failure.


## Step650.8.10 validation integrity, cancellation, and durable Kline audit

- Admin validation reset increments a persisted control epoch and aborts an in-flight probe. A late probe result cannot recreate a cleared session.
- Concurrent probe calls are rejected; one validation token is never shared between two scripts.
- Server-side validation is fixed to Binance contract, the four-symbol sequence, 15m, exactly 240 rows, and a server-controlled current end time. Client `end_time` is rejected during validation.
- The exact validation Kline snapshot must be persisted successfully before the guard advances. Snapshot I/O has an 8-second timeout.
- Client disconnects cancel queued and paced Binance Kline REST work before an upstream request starts.
- One `/api/klines` request may start at most one Binance REST bridge call.
- Health reports both official Futures and Spot WebSocket hosts, and slow downstream WebSocket clients are disconnected before their send buffer can grow without bound.

## Step650.8.10 repeated final audit: deployment overlap, exact validation, and downstream isolation

This revision was rebuilt after another end-to-end Binance audit instead of treating a health-only pass as proof that the real egress path was safe.

- The validation contract is server-fixed to Binance USD-M contract, the four reviewed symbols, `15m`, exactly `240` rows, and a server-controlled current end time. Client-controlled `end_time`, a different limit, a different symbol order, or a second simultaneous probe is rejected before Binance network access.
- Probe/reset state uses a persisted control epoch. An administrator reset invalidates and aborts the active probe first; a late successful probe cannot recreate a cleared validation session.
- A validation Kline snapshot must be written successfully within eight seconds before the persistent REST guard advances to the next symbol.
- One API request can consume at most one Binance Kline REST call. A client disconnect can remove queued/pre-start work, but never aborts an already-started Binance fetch in a way that could hide a real restricted response.
- The deployed Render Blueprint is explicitly `plan: free`. Render Free web services cannot scale beyond one instance and cannot receive private-network traffic, so their `*-discovery` hostname can legitimately return `ENOTFOUND`. DNS discovery is used when available, but it is not treated as a prerequisite on this plan.
- New Render instances have a 180-second Binance REST startup grace. Render sends `SIGTERM` to the old zero-downtime instance after 60 seconds and then applies the shutdown delay; the longer grace ensures the new instance cannot overlap Binance REST with the draining old instance.
- `SIGTERM` immediately blocks all new Binance REST work before HTTP shutdown begins.
- Guard health exposes the selected instance-safety strategy, expected plan, optional DNS discovery result, startup-grace remainder, and shutdown state. Paid multi-instance Binance REST remains intentionally unsupported until a distributed leader/lease is introduced.
- Binance App WebSocket clients are bounded by downstream IP, stream count, connection rate, and send-buffer size. The Render-normalized first `X-Forwarded-For` address is used for these per-IP caps.
- The validation administrator secret must be exactly 64 hexadecimal characters. No secret value or validation token is shipped in the source or delivery package.

The existing `app_market_backend_snapshots` table and existing Render environment variables are reused. No SQL, Supabase Edge, Cron, Flutter, App, or dependency migration is required.

## Step650.8.10 probe observability and official-limit safety margins

Step650.8.10 fixes the validation blocker found during the first real post-ban probes. Binance returned HTTP 200, but the previous hard stop of 100 request-weight/min was only a very small fraction of the current published USD-M Futures IP limit. The worker now keeps conservative staged ceilings of 1200 for the probe, 1500 during four-symbol validation, and 1800 in normal guarded mode, while continuing to stop on every 403/418/429/451 and to inspect the weight header after every successful REST response.

The exact probe HTTP status, raw weight header, parsed weight, threshold, timestamp, and safe/unsafe decision are persisted in the REST guard snapshot and exposed through health. Internal 409 responses also include the safe diagnostic fields. Validation scripts use baseline counter increments instead of hard-coded lifetime totals.

import { tickers as loadMarketTickers } from './market-rest.mjs';
import { getMarketLightInternalSnapshot } from './market-light-bridge.mjs';

const STEP_VERSION = '650.8.15.164';
const PROVIDERS = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const HIGH_ACTIVITY_PER_PROVIDER = 4;
const ROTATING_PER_PROVIDER = 16;
const TARGETS_PER_PROVIDER = HIGH_ACTIVITY_PER_PROVIDER + ROTATING_PER_PROVIDER;
const SCAN_INTERVAL_MS = Math.max(
  60_000,
  Number(process.env.KAKA_SPOT_CURRENT_SCAN_INTERVAL_MS || 5 * 60_000),
);
const START_DELAY_MS = Math.max(
  1_000,
  Number(process.env.KAKA_SPOT_CURRENT_START_DELAY_MS || 5_000),
);
const STALE_MS = Math.max(
  SCAN_INTERVAL_MS,
  Number(process.env.KAKA_SPOT_CURRENT_STALE_MS || 30 * 60_000),
);

const rowsByProvider = new Map();
const cursorByProvider = new Map(PROVIDERS.map((provider) => [provider, 0]));
const attemptsByProvider = Object.fromEntries(PROVIDERS.map((provider) => [provider, 0]));
const successByProvider = Object.fromEntries(PROVIDERS.map((provider) => [provider, 0]));
const errorsByProvider = Object.fromEntries(PROVIDERS.map((provider) => [provider, '']));
const updatedAtByProvider = Object.fromEntries(PROVIDERS.map((provider) => [provider, null]));

let started = false;
let running = false;
let cycle = 0;
let lastStartedAt = null;
let lastCompletedAt = null;
let lastError = '';
let totalUpstreamTickerLoads = 0;
let totalBinanceMarketLightReuses = 0;
let totalBinanceBridgeWaitRetries = 0;
let totalSnapshotReads = 0;
let timer = null;
let interval = null;

function compact(value) {
  return String(value ?? '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function finiteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function quoteAssetFor(row, symbol) {
  const explicit = compact(row?.quote_asset ?? row?.quoteAsset ?? row?.settle_asset);
  if (explicit) return explicit;
  for (const quote of ['FDUSD', 'PYUSD', 'USDT', 'USDC', 'USD1', 'TUSD', 'BUSD', 'EURC', 'DAI', 'USD', 'BTC', 'BNB', 'ETH', 'EUR', 'GBP', 'JPY', 'KRW', 'TRY', 'BRL', 'AUD', 'CAD', 'SGD', 'HKD', 'CHF', 'MXN', 'PLN']) {
    if (symbol.endsWith(quote) && symbol.length > quote.length) return quote;
  }
  return '';
}

function rowTimeMs(row) {
  for (const value of [
    row?.source_time,
    row?.cached_at,
    row?.updated_at,
    row?.requested_at,
  ]) {
    const parsed = Date.parse(String(value ?? ''));
    if (Number.isFinite(parsed)) return parsed;
  }
  return 0;
}

function normalizeRow(provider, raw, observedAt) {
  if (!raw || typeof raw !== 'object') return null;
  const symbol = compact(raw.symbol ?? raw.native_symbol);
  if (!symbol) return null;
  const quote = quoteAssetFor(raw, symbol);
  if (quote !== 'USDT') return null;
  const lastPrice = finiteNumber(raw.last_price ?? raw.price ?? raw.lastPrice ?? raw.close);
  if (!(lastPrice > 0)) return null;
  const marketType = String(raw.market_type ?? raw.market ?? 'spot').trim().toLowerCase();
  if (marketType && marketType !== 'spot') return null;
  const cachedAt = String(
    raw.cached_at ?? raw.source_time ?? raw.updated_at ?? raw.requested_at ?? observedAt,
  );
  return {
    ...raw,
    provider,
    market_type: 'spot',
    symbol,
    quote_asset: 'USDT',
    quote_symbol: 'USDT',
    last_price: lastPrice,
    price: lastPrice,
    source_time: raw.source_time ?? cachedAt,
    cached_at: cachedAt,
    backend_shared: true,
  };
}

function dedupeRows(provider, rawRows, observedAt) {
  const bySymbol = new Map();
  for (const raw of Array.isArray(rawRows) ? rawRows : []) {
    const row = normalizeRow(provider, raw, observedAt);
    if (!row) continue;
    const existing = bySymbol.get(row.symbol);
    if (!existing || rowTimeMs(row) >= rowTimeMs(existing)) bySymbol.set(row.symbol, row);
  }
  return [...bySymbol.values()];
}

function turnover(row) {
  return finiteNumber(
    row?.quote_volume_24h ??
    row?.turnover_24h ??
    row?.quoteVolume24h ??
    row?.quoteVolume ??
    row?.quote_volume,
  ) ?? 0;
}

function selectRows(provider, rawRows, observedAt, round) {
  const normalized = dedupeRows(provider, rawRows, observedAt);
  normalized.sort((a, b) => {
    const byTurnover = turnover(b) - turnover(a);
    if (byTurnover !== 0) return byTurnover;
    return a.symbol.localeCompare(b.symbol);
  });

  const selected = [];
  const selectedSymbols = new Set();
  for (const row of normalized.slice(0, HIGH_ACTIVITY_PER_PROVIDER)) {
    if (!selectedSymbols.add(row.symbol)) continue;
    selected.push({ ...row, selection_role: 'high_activity' });
  }

  const rotationPool = normalized
    .filter((row) => !selectedSymbols.has(row.symbol))
    .sort((a, b) => a.symbol.localeCompare(b.symbol));
  if (rotationPool.length) {
    const cursor = (cursorByProvider.get(provider) || 0) % rotationPool.length;
    for (let offset = 0; offset < rotationPool.length && selected.length < TARGETS_PER_PROVIDER; offset += 1) {
      const row = rotationPool[(cursor + offset) % rotationPool.length];
      if (!selectedSymbols.add(row.symbol)) continue;
      selected.push({ ...row, selection_role: 'rotated_directory' });
    }
    cursorByProvider.set(provider, (cursor + ROTATING_PER_PROVIDER) % rotationPool.length);
  }

  return selected.map((row, index) => ({
    ...row,
    shared_round: round,
    shared_slot: index + 1,
    shared_observed_at: observedAt,
  }));
}

async function scanProvider(provider, round) {
  attemptsByProvider[provider] += 1;
  const observedAt = new Date().toISOString();
  try {
    let rawRows;
    if (provider === 'binance') {
      // Step1031.2: Binance USDT current snapshot must never start the former
      // heavy all-symbol REST ticker request. Reuse the already-running shared
      // market-light WebSocket snapshot instead; users still read only cache.
      let shared = getMarketLightInternalSnapshot({
        market: 'spot',
        provider: 'binance',
      });
      rawRows = Array.isArray(shared?.rows) ? shared.rows : [];
      totalBinanceMarketLightReuses += 1;
      // Cold deploy: the isolated market-light child starts independently.
      // Give the localhost bridge a few short read-only retries so Binance does
      // not miss the entire next 5-minute spot-current cycle.
      for (let retry = 0; rawRows.length === 0 && retry < 3; retry += 1) {
        await new Promise((resolve) => setTimeout(resolve, 1_500));
        totalBinanceBridgeWaitRetries += 1;
        shared = getMarketLightInternalSnapshot({
          market: 'spot',
          provider: 'binance',
        });
        rawRows = Array.isArray(shared?.rows) ? shared.rows : [];
      }
    } else {
      totalUpstreamTickerLoads += 1;
      rawRows = await loadMarketTickers(provider, 'spot', []);
    }
    const selected = selectRows(provider, rawRows, observedAt, round);
    if (!selected.length) throw new Error('spot_ticker_rows_empty');
    rowsByProvider.set(provider, selected);
    successByProvider[provider] += 1;
    errorsByProvider[provider] = '';
    updatedAtByProvider[provider] = observedAt;
    return selected.length;
  } catch (error) {
    errorsByProvider[provider] = String(error?.message || error);
    return 0;
  }
}

export async function runSpotCurrentSnapshotCycle({ reason = 'scheduled' } = {}) {
  if (running) return false;
  running = true;
  lastStartedAt = new Date().toISOString();
  lastError = '';
  const round = cycle + 1;
  try {
    await Promise.allSettled(PROVIDERS.map((provider) => scanProvider(provider, round)));
    cycle = round;
    lastCompletedAt = new Date().toISOString();
    const successfulProviders = PROVIDERS.filter((provider) => (rowsByProvider.get(provider) || []).length > 0);
    if (!successfulProviders.length) {
      lastError = 'all_provider_spot_snapshot_rows_empty';
    }
    return successfulProviders.length > 0;
  } catch (error) {
    lastError = `${reason}:${String(error?.message || error)}`;
    return false;
  } finally {
    running = false;
  }
}

export function startSpotCurrentSnapshotScanner() {
  if (started || process.env.KAKA_DISABLE_SPOT_CURRENT_SCANNER === '1') return;
  started = true;
  timer = setTimeout(() => {
    runSpotCurrentSnapshotCycle({ reason: 'startup' }).catch(() => {});
  }, START_DELAY_MS);
  timer.unref?.();
  interval = setInterval(() => {
    runSpotCurrentSnapshotCycle({ reason: 'interval' }).catch(() => {});
  }, SCAN_INTERVAL_MS);
  interval.unref?.();
}

function providerCoverage(nowMs) {
  return Object.fromEntries(PROVIDERS.map((provider) => {
    const rows = rowsByProvider.get(provider) || [];
    const updatedAt = updatedAtByProvider[provider];
    const updatedMs = Date.parse(String(updatedAt || ''));
    const stale = !Number.isFinite(updatedMs) || nowMs - updatedMs > STALE_MS;
    return [provider, {
      rows: rows.length,
      high_activity: rows.filter((row) => row.selection_role === 'high_activity').length,
      rotated_directory: rows.filter((row) => row.selection_role === 'rotated_directory').length,
      stale,
      updated_at: updatedAt,
      error: errorsByProvider[provider],
    }];
  }));
}

export function getSpotCurrentSnapshotHealth() {
  const now = Date.now();
  const coverage = providerCoverage(now);
  return {
    ok: true,
    version: STEP_VERSION,
    enabled: started || process.env.KAKA_DISABLE_SPOT_CURRENT_SCANNER !== '1',
    mode: 'backend_shared_bounded_five_provider_spot_ticker_rotation_parallel_provider_fault_isolation_binance_market_light_reuse',
    endpoint: '/api/spot-market/current-snapshot',
    health_endpoint: '/api/spot-market/health',
    providers: PROVIDERS,
    targets_per_provider: TARGETS_PER_PROVIDER,
    high_activity_per_provider: HIGH_ACTIVITY_PER_PROVIDER,
    rotating_per_provider: ROTATING_PER_PROVIDER,
    scan_interval_minutes: SCAN_INTERVAL_MS / 60_000,
    stale_minutes: STALE_MS / 60_000,
    global_concurrency: PROVIDERS.length,
    provider_refresh_isolated: true,
    provider_refresh_scheduling: 'parallel_all_settled_provider_governor_bounded',
    binance_current_ticker_source: 'shared_market_light_websocket_snapshot',
    binance_current_ticker_rest_requests: 0,
    binance_current_ticker_user_scaled_requests: 0,
    running,
    cycle,
    last_started_at: lastStartedAt,
    last_completed_at: lastCompletedAt,
    last_error: lastError,
    attempts_by_provider: { ...attemptsByProvider },
    success_by_provider: { ...successByProvider },
    errors_by_provider: { ...errorsByProvider },
    cursor_by_provider: Object.fromEntries(cursorByProvider),
    rows_by_provider: Object.fromEntries(PROVIDERS.map((provider) => [provider, (rowsByProvider.get(provider) || []).length])),
    provider_coverage: coverage,
    total_upstream_ticker_loads: totalUpstreamTickerLoads,
    total_binance_market_light_reuses: totalBinanceMarketLightReuses,
    total_binance_bridge_wait_retries: totalBinanceBridgeWaitRetries,
    total_snapshot_reads: totalSnapshotReads,
    snapshot_reads_start_exchange_requests: false,
    snapshot_reads_start_exchange_connections: false,
    snapshot_reads_scale_with_users: false,
    empty_or_failed_provider_never_overwrites_last_verified_rows: true,
    time: new Date(now).toISOString(),
  };
}

function sendJson(res, status, payload) {
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
  });
  res.end(body);
}

export async function handleSpotCurrentSnapshot(req, res, url) {
  if (url.pathname === '/api/spot-market/health') {
    sendJson(res, 200, getSpotCurrentSnapshotHealth());
    return true;
  }
  if (url.pathname !== '/api/spot-market/current-snapshot') return false;

  totalSnapshotReads += 1;
  const now = Date.now();
  const rows = PROVIDERS.flatMap((provider) => rowsByProvider.get(provider) || []);
  const coverage = providerCoverage(now);
  const providerCount = PROVIDERS.filter((provider) => (rowsByProvider.get(provider) || []).length > 0).length;
  const currentProviderCount = PROVIDERS.filter((provider) => coverage[provider]?.rows > 0 && coverage[provider]?.stale === false).length;
  sendJson(res, 200, {
    ok: true,
    version: STEP_VERSION,
    source: 'render_shared_bounded_five_provider_spot_current_snapshot',
    market_type: 'spot',
    quote_asset: 'USDT',
    rows,
    row_count: rows.length,
    provider_coverage: coverage,
    provider_count: providerCount,
    current_provider_count: currentProviderCount,
    shared_round: cycle,
    targets_per_provider: TARGETS_PER_PROVIDER,
    high_activity_per_provider: HIGH_ACTIVITY_PER_PROVIDER,
    rotating_per_provider: ROTATING_PER_PROVIDER,
    scan_interval_minutes: SCAN_INTERVAL_MS / 60_000,
    generated_at: new Date(now).toISOString(),
    warming: rows.length === 0 || running,
    backend_running: running,
    last_completed_at: lastCompletedAt,
    aggregation_scope: 'backend_shared_five_provider_four_high_activity_plus_sixteen_directory_rotation',
    exchange_requests_started: 0,
    exchange_connections_started: 0,
    shared_reads_do_not_scale_with_users: true,
    empty_or_failed_provider_never_overwrites_last_verified_rows: true,
  });
  return true;
}

export const _test = {
  normalizeRow,
  selectRows,
  seed(rowsByProviderInput = {}) {
    for (const provider of PROVIDERS) {
      const rows = Array.isArray(rowsByProviderInput[provider]) ? rowsByProviderInput[provider] : [];
      if (rows.length) {
        rowsByProvider.set(provider, rows.map((row, index) => ({
          ...row,
          provider,
          market_type: 'spot',
          quote_asset: 'USDT',
          quote_symbol: 'USDT',
          backend_shared: true,
          shared_round: 1,
          shared_slot: index + 1,
        })));
        updatedAtByProvider[provider] = new Date().toISOString();
      }
    }
    cycle = 1;
    lastCompletedAt = new Date().toISOString();
  },
};

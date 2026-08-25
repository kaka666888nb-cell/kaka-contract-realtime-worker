import { tickers } from './market-rest.mjs';
import { getMarketLightInternalSnapshot } from './market-light-bridge.mjs';
import { requestIsolatedJson } from './collector-isolation.mjs';

const VERSION = '650.8.15.168';
const PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);

const CACHE_TTL_MS = 20_000;
const STALE_MS = 3 * 60_000;
const NEGATIVE_TTL_MS = 60_000;
const CACHE_MAX = 256;
const BUILD_MAX_ACTIVE = 6;
const BUILD_MAX_QUEUE = 96;
const BUILD_PROVIDER_MAX_ACTIVE = 2;
const BUILD_PROVIDER_MAX_QUEUE = 16;

const cache = new Map();
const negativeCache = new Map();
const inflight = new Map();
const providerBuildState = new Map([...PROVIDERS].map((provider) => [provider, { provider, active: 0, queue: [], started: 0, completed: 0, rejected: 0, max_queue_seen: 0 }]));
const providerBuildOrder = [...PROVIDERS];
let providerBuildCursor = 0;
let activeBuilds = 0;

const stats = {
  reads: 0,
  fresh_hits: 0,
  stale_hits: 0,
  negative_hits: 0,
  inflight_hits: 0,
  builds_started: 0,
  builds_succeeded: 0,
  builds_empty: 0,
  builds_failed: 0,
  queue_rejections: 0,
  cache_evictions: 0,
  upstream_ticker_builds: 0,
  market_light_reuse_builds: 0,
  market_light_bridge_wait_retries: 0,
  isolated_market_light_fallback_reads: 0,
  isolated_market_light_fallback_hits: 0,
  isolated_market_light_fallback_failures: 0,
  binance_market_light_reuse_builds: 0,
  binance_bridge_wait_retries: 0,
  binance_isolated_market_light_fallback_reads: 0,
  binance_isolated_market_light_fallback_hits: 0,
  binance_isolated_market_light_fallback_failures: 0,
};

function providerKey(raw) {
  let value = String(raw || '').trim().toLowerCase();
  if (value === 'gate.io') value = 'gate';
  if (value === 'okex') value = 'okx';
  return PROVIDERS.has(value) ? value : '';
}

function symbolKey(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/-SWAP$/i, '')
    .replace(/_UMCBL$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}

function positiveNumber(value) {
  const number = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(number) && number > 0 ? number : null;
}

function sendJson(res, statusCode, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
  });
  res.end(body);
}

function pruneMaps() {
  const now = Date.now();
  for (const [key, entry] of cache.entries()) {
    if (Number(entry?.staleUntil || 0) <= now) cache.delete(key);
  }
  for (const [key, until] of negativeCache.entries()) {
    if (Number(until || 0) <= now) negativeCache.delete(key);
  }
  while (cache.size > CACHE_MAX) {
    const oldestKey = [...cache.entries()]
      .sort((a, b) => Number(a[1]?.storedAt || 0) - Number(b[1]?.storedAt || 0))[0]?.[0];
    if (!oldestKey) break;
    cache.delete(oldestKey);
    stats.cache_evictions += 1;
  }
  while (negativeCache.size > CACHE_MAX) {
    const oldestKey = negativeCache.keys().next().value;
    if (!oldestKey) break;
    negativeCache.delete(oldestKey);
  }
}

function cachedPayload(entry, state) {
  return {
    ...entry.payload,
    cache_state: state,
    cache_age_seconds: Math.max(0, Math.floor((Date.now() - entry.storedAt) / 1000)),
  };
}

function buildQueueTotal() {
  let total = 0;
  for (const state of providerBuildState.values()) total += state.queue.length;
  return total;
}

function buildBulkheadHealth() {
  return Object.fromEntries([...providerBuildState.entries()].map(([provider, state]) => [provider, {
    active: state.active,
    queue: state.queue.length,
    max_active: BUILD_PROVIDER_MAX_ACTIVE,
    max_queue: BUILD_PROVIDER_MAX_QUEUE,
    started: state.started,
    completed: state.completed,
    rejected: state.rejected,
    max_queue_seen: state.max_queue_seen,
  }]));
}

function pumpBuildQueue() {
  if (!providerBuildOrder.length) return;
  let progress = true;
  while (progress && activeBuilds < BUILD_MAX_ACTIVE) {
    progress = false;
    for (let offset = 0; offset < providerBuildOrder.length; offset += 1) {
      const index = (providerBuildCursor + offset) % providerBuildOrder.length;
      const provider = providerBuildOrder[index];
      const state = providerBuildState.get(provider);
      if (!state || state.active >= BUILD_PROVIDER_MAX_ACTIVE) continue;
      while (state.queue.length && state.queue[0]?.signal?.aborted) state.queue.shift();
      const item = state.queue.shift();
      if (!item) continue;
      if (item.signal && item.onAbort) item.signal.removeEventListener('abort', item.onAbort);
      state.active += 1;
      activeBuilds += 1;
      state.started += 1;
      providerBuildCursor = (index + 1) % providerBuildOrder.length;
      item.resolve(() => releaseBuildSlot(provider));
      progress = true;
      break;
    }
  }
}

function acquireBuildSlot(provider, signal) {
  if (signal?.aborted) return Promise.reject(new Error('request_aborted_before_queue'));
  const state = providerBuildState.get(provider);
  if (!state) return Promise.reject(new Error('unsupported_provider_bulkhead'));
  if (buildQueueTotal() === 0 && state.active < BUILD_PROVIDER_MAX_ACTIVE && activeBuilds < BUILD_MAX_ACTIVE) {
    state.active += 1;
    activeBuilds += 1;
    state.started += 1;
    return Promise.resolve(() => releaseBuildSlot(provider));
  }
  if (state.queue.length >= BUILD_PROVIDER_MAX_QUEUE || buildQueueTotal() >= BUILD_MAX_QUEUE) {
    stats.queue_rejections += 1;
    state.rejected += 1;
    return Promise.reject(new Error(state.queue.length >= BUILD_PROVIDER_MAX_QUEUE ? 'spot_exact_ticker_provider_queue_full' : 'spot_exact_ticker_global_queue_full'));
  }
  return new Promise((resolve, reject) => {
    const item = { resolve, reject, signal, onAbort: null };
    if (signal) {
      item.onAbort = () => {
        const index = state.queue.indexOf(item);
        if (index >= 0) state.queue.splice(index, 1);
        reject(new Error('request_aborted_while_queued'));
      };
      signal.addEventListener('abort', item.onAbort, { once: true });
    }
    state.queue.push(item);
    state.max_queue_seen = Math.max(state.max_queue_seen, state.queue.length);
    pumpBuildQueue();
  });
}

function releaseBuildSlot(provider) {
  const state = providerBuildState.get(provider);
  if (state) {
    state.active = Math.max(0, state.active - 1);
    state.completed += 1;
  }
  activeBuilds = Math.max(0, activeBuilds - 1);
  pumpBuildQueue();
}

async function buildExactTicker(provider, symbol) {
  let rows;
  if (provider !== 'coinbase') {
    // Step1042.1.5: all five crypto spot providers reuse the fixed background
    // market-light snapshot. Exact user reads no longer start provider REST.
    let shared = getMarketLightInternalSnapshot({ market: 'spot', provider });
    rows = Array.isArray(shared?.rows)
      ? shared.rows.filter((row) => symbolKey(row?.symbol) === symbol)
      : [];
    stats.market_light_reuse_builds += 1;
    if (provider === 'binance') stats.binance_market_light_reuse_builds += 1;

    for (let retry = 0; rows.length === 0 && retry < 2; retry += 1) {
      await new Promise((resolve) => setTimeout(resolve, 300));
      stats.market_light_bridge_wait_retries += 1;
      if (provider === 'binance') stats.binance_bridge_wait_retries += 1;
      shared = getMarketLightInternalSnapshot({ market: 'spot', provider });
      rows = Array.isArray(shared?.rows)
        ? shared.rows.filter((row) => symbolKey(row?.symbol) === symbol)
        : [];
    }

    if (rows.length === 0) {
      stats.isolated_market_light_fallback_reads += 1;
      if (provider === 'binance') stats.binance_isolated_market_light_fallback_reads += 1;
      try {
        const isolated = await requestIsolatedJson(
          'market-light',
          `/api/market-light/current-snapshot?market_type=spot&provider=${encodeURIComponent(provider)}&include_rows=1&limit=5000`,
          5_000,
        );
        rows = Array.isArray(isolated?.rows)
          ? isolated.rows.filter((row) => symbolKey(row?.symbol) === symbol)
          : [];
        if (rows.length > 0) {
          stats.isolated_market_light_fallback_hits += 1;
          if (provider === 'binance') stats.binance_isolated_market_light_fallback_hits += 1;
        }
      } catch (_) {
        stats.isolated_market_light_fallback_failures += 1;
        if (provider === 'binance') stats.binance_isolated_market_light_fallback_failures += 1;
      }
    }
  } else {
    // Coinbase remains an exact official product read because the parent bridge
    // intentionally does not copy the several-thousand-product Coinbase set.
    stats.upstream_ticker_builds += 1;
    rows = await tickers(provider, 'spot', [symbol]);
  }
  if (!Array.isArray(rows)) return null;
  for (const raw of rows) {
    if (!raw || typeof raw !== 'object') continue;
    const rowSymbol = symbolKey(raw.symbol || raw.native_symbol);
    const price = positiveNumber(
      raw.price ?? raw.last_price ?? raw.lastPrice ?? raw.last ?? raw.close,
    );
    if (rowSymbol !== symbol || price == null) continue;
    return {
      ...raw,
      provider,
      market_type: 'spot',
      symbol,
      price,
      read_only_shared: provider !== 'coinbase' ? true : raw?.read_only_shared,
      user_upstream_requests: provider !== 'coinbase' ? 0 : raw?.user_upstream_requests,
      reads_scale_with_users: provider !== 'coinbase' ? false : raw?.reads_scale_with_users,
    };
  }
  return null;
}

async function getSharedExactTicker(provider, symbol, signal) {
  stats.reads += 1;
  pruneMaps();
  const key = `${provider}:${symbol}`;
  const now = Date.now();
  const existing = cache.get(key);

  if (existing && existing.freshUntil > now) {
    stats.fresh_hits += 1;
    return cachedPayload(existing, 'fresh');
  }
  if (Number(negativeCache.get(key) || 0) > now) {
    stats.negative_hits += 1;
    return {
      ok: false,
      version: VERSION,
      provider,
      market_type: 'spot',
      symbol,
      status: 'temporarily_unavailable',
      ticker: null,
      cache_state: 'negative',
      source: 'render_shared_exact_spot_ticker',
      generated_at: new Date().toISOString(),
    };
  }
  const running = inflight.get(key);
  if (running) {
    stats.inflight_hits += 1;
    return await running;
  }

  const task = (async () => {
    let release = null;
    try {
      release = await acquireBuildSlot(provider, signal);
      stats.builds_started += 1;
      const ticker = await buildExactTicker(provider, symbol);
      if (!ticker) {
        stats.builds_empty += 1;
        negativeCache.set(key, Date.now() + NEGATIVE_TTL_MS);
        if (existing && existing.staleUntil > Date.now()) {
          stats.stale_hits += 1;
          return cachedPayload(existing, 'stale_build_empty');
        }
        return {
          ok: false,
          version: VERSION,
          provider,
          market_type: 'spot',
          symbol,
          status: 'not_ready',
          ticker: null,
          cache_state: 'miss_empty',
          source: 'render_shared_exact_spot_ticker',
          generated_at: new Date().toISOString(),
        };
      }

      negativeCache.delete(key);
      const payload = {
        ok: true,
        version: VERSION,
        provider,
        market_type: 'spot',
        symbol,
        status: 'ready',
        ticker,
        source: 'render_shared_exact_spot_ticker',
        generated_at: new Date().toISOString(),
      };
      const entry = {
        payload,
        storedAt: Date.now(),
        freshUntil: Date.now() + CACHE_TTL_MS,
        staleUntil: Date.now() + STALE_MS,
      };
      cache.set(key, entry);
      pruneMaps();
      stats.builds_succeeded += 1;
      return cachedPayload(entry, 'miss');
    } catch (error) {
      stats.builds_failed += 1;
      if (existing && existing.staleUntil > Date.now()) {
        stats.stale_hits += 1;
        return cachedPayload(existing, 'stale_error');
      }
      throw error;
    } finally {
      if (release) release();
    }
  })();

  inflight.set(key, task);
  try {
    return await task;
  } finally {
    if (inflight.get(key) === task) inflight.delete(key);
  }
}

export function getSpotExactTickerHealth() {
  pruneMaps();
  return {
    ok: true,
    version: VERSION,
    endpoint: '/api/spot-market/exact-ticker',
    health_endpoint: '/api/spot-market/exact-ticker-health',
    providers: [...PROVIDERS],
    mode: 'backend_shared_exact_provider_spot_symbol_ticker_five_crypto_venues_market_light_reuse_coinbase_exact_official',
    cache_ttl_seconds: Math.round(CACHE_TTL_MS / 1000),
    stale_seconds: Math.round(STALE_MS / 1000),
    negative_ttl_seconds: Math.round(NEGATIVE_TTL_MS / 1000),
    cache_entries: cache.size,
    negative_entries: negativeCache.size,
    inflight_entries: inflight.size,
    cache_max: CACHE_MAX,
    build_active: activeBuilds,
    build_max_active: BUILD_MAX_ACTIVE,
    build_queue: buildQueueTotal(),
    build_max_queue: BUILD_MAX_QUEUE,
    build_provider_max_active: BUILD_PROVIDER_MAX_ACTIVE,
    build_provider_max_queue: BUILD_PROVIDER_MAX_QUEUE,
    build_bulkhead_mode: 'per_provider_round_robin_with_global_emergency_ceiling',
    provider_bulkheads: buildBulkheadHealth(),
    same_exact_key_reads_share_cache_and_inflight: true,
    fresh_snapshot_reads_start_exchange_requests: false,
    step1042_1_5_five_crypto_spot_exact_market_light_shared: true,
    non_coinbase_exact_ticker_user_exchange_requests: 0,
    non_coinbase_exact_ticker_user_exchange_connections: 0,
    non_coinbase_exact_ticker_reads_scale_with_users: false,
    non_coinbase_exact_ticker_direct_provider_fallback: false,
    coinbase_exact_ticker_parent_bridge_intentionally_disabled_for_memory_bound: true,
    binance_exact_ticker_source: 'shared_market_light_websocket_snapshot',
    binance_exact_ticker_rest_requests: 0,
    binance_exact_ticker_user_scaled_requests: 0,
    binance_exact_ticker_isolated_market_light_fallback_exchange_requests: 0,
    binance_exact_ticker_parent_bridge_miss_uses_localhost_snapshot_once: true,
    empty_or_failed_build_never_overwrites_verified_stale_payload: true,
    app_no_longer_calls_spot_detail_ticker_edge: true,
    app_no_longer_uses_device_direct_spot_ticker_fallback_for_detail_first_paint: true,
    detail_first_paint_ticker_builds_scale_by_exact_key_not_user_count: true,
    coinbase_exact_symbol_official_directory_preflight: true,
    coinbase_nonexistent_product_writes_negative_cache: true,
    coinbase_nonexistent_product_returns_http_404_honest_empty: true,
    coinbase_upstream_404_is_not_a_build_failure: true,
    ...stats,
    time: new Date().toISOString(),
  };
}

export async function handleSpotExactTicker(req, res, url, signal = null) {
  if (url.pathname === '/api/spot-market/exact-ticker-health') {
    sendJson(res, 200, getSpotExactTickerHealth());
    return true;
  }
  if (url.pathname !== '/api/spot-market/exact-ticker') return false;

  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, OPTIONS',
      'access-control-allow-headers': 'content-type',
    });
    res.end();
    return true;
  }
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, error: 'GET required' });
    return true;
  }

  const provider = providerKey(url.searchParams.get('provider'));
  const symbol = symbolKey(url.searchParams.get('symbol'));
  if (!provider || !symbol) {
    sendJson(res, 400, {
      ok: false,
      error: !provider ? 'unsupported provider' : 'symbol required',
      provider,
      market_type: 'spot',
      symbol,
    });
    return true;
  }

  try {
    const payload = await getSharedExactTicker(provider, symbol, signal);
    sendJson(res, payload.ok === true ? 200 : 404, payload);
  } catch (error) {
    sendJson(res, 503, {
      ok: false,
      version: VERSION,
      provider,
      market_type: 'spot',
      symbol,
      error: String(error?.message || error),
    });
  }
  return true;
}

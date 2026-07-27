import { fetchMarketKlines } from './market-rest.mjs';

const VERSION = '650.8.15.48';
const PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
const CACHE_TTL_MS = 2 * 60_000;
const STALE_MS = 15 * 60_000;
const NEGATIVE_TTL_MS = 15 * 60_000;
const CACHE_MAX = 64;
const BUILD_MAX_ACTIVE = 2;
const BUILD_MAX_QUEUE = 32;

const cache = new Map();
const inflight = new Map();
const queue = [];
let activeBuilds = 0;

const stats = {
  reads: 0,
  fresh_hits: 0,
  stale_hits: 0,
  inflight_hits: 0,
  builds_started: 0,
  builds_succeeded: 0,
  builds_partial: 0,
  builds_failed: 0,
  queue_rejections: 0,
  cache_evictions: 0,
  upstream_kline_calls: 0,
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

function pruneCache() {
  const now = Date.now();
  for (const [key, entry] of cache.entries()) {
    if (Number(entry?.staleUntil || 0) <= now) cache.delete(key);
  }
  while (cache.size > CACHE_MAX) {
    const oldest = cache.keys().next().value;
    if (oldest == null) break;
    cache.delete(oldest);
    stats.cache_evictions += 1;
  }
}

function rowsWithTimes(raw) {
  const rows = Array.isArray(raw) ? raw.filter((row) => row && typeof row === 'object') : [];
  rows.sort((a, b) => Number(a.open_time ?? a.openTime ?? a.time ?? 0) - Number(b.open_time ?? b.openTime ?? b.time ?? 0));
  const deduped = [];
  const seen = new Set();
  for (const row of rows) {
    const time = Number(row.open_time ?? row.openTime ?? row.time ?? 0);
    if (!Number.isFinite(time) || time <= 0 || seen.has(time)) continue;
    seen.add(time);
    deduped.push(row);
  }
  return deduped;
}

function takerCoverage(rows) {
  let totalRows = 0;
  let takerRows = 0;
  for (const row of rows) {
    const total = Number(row.quote_volume ?? row.quoteVolume ?? 0);
    const buy = Number(row.taker_buy_quote_volume ?? row.takerBuyQuoteVolume);
    if (Number.isFinite(total) && total > 0) totalRows += 1;
    if (Number.isFinite(total) && total > 0 && Number.isFinite(buy) && buy >= 0) takerRows += 1;
  }
  return { total_rows: totalRows, taker_rows: takerRows };
}

function cachedPayload(entry, cacheState) {
  return {
    ...entry.payload,
    cache_state: cacheState,
    cache_age_seconds: Math.max(0, Math.floor((Date.now() - entry.storedAt) / 1000)),
  };
}

function acquireBuildSlot(signal) {
  if (signal?.aborted) return Promise.reject(new Error('request_aborted_before_queue'));
  if (activeBuilds < BUILD_MAX_ACTIVE) {
    activeBuilds += 1;
    return Promise.resolve(() => releaseBuildSlot());
  }
  if (queue.length >= BUILD_MAX_QUEUE) {
    stats.queue_rejections += 1;
    return Promise.reject(new Error('spot_flow_history_queue_full'));
  }
  return new Promise((resolve, reject) => {
    const item = { resolve, reject, signal, onAbort: null };
    if (signal) {
      item.onAbort = () => {
        const index = queue.indexOf(item);
        if (index >= 0) queue.splice(index, 1);
        reject(new Error('request_aborted_while_queued'));
      };
      signal.addEventListener('abort', item.onAbort, { once: true });
    }
    queue.push(item);
  });
}

function releaseBuildSlot() {
  activeBuilds = Math.max(0, activeBuilds - 1);
  while (queue.length > 0 && activeBuilds < BUILD_MAX_ACTIVE) {
    const item = queue.shift();
    if (!item || item.signal?.aborted) continue;
    if (item.signal && item.onAbort) item.signal.removeEventListener('abort', item.onAbort);
    activeBuilds += 1;
    item.resolve(() => releaseBuildSlot());
    break;
  }
}

async function readInterval(provider, symbol, interval, limit, signal) {
  stats.upstream_kline_calls += 1;
  const spanMs = interval === '1h' ? 60 * 60_000 : (interval === '5m' ? 5 * 60_000 : 60_000);
  const end = Date.now() + spanMs * 2;
  const rows = await fetchMarketKlines(provider, 'spot', symbol, interval, end, limit, { signal });
  return rowsWithTimes(rows);
}

async function buildPayload(provider, symbol, signal) {
  const settled = await Promise.allSettled([
    readInterval(provider, symbol, '1m', 300, signal),
    readInterval(provider, symbol, '5m', 320, signal),
    readInterval(provider, symbol, '1h', 180, signal),
  ]);
  const oneMinute = settled[0].status === 'fulfilled' ? settled[0].value : [];
  const fiveMinute = settled[1].status === 'fulfilled' ? settled[1].value : [];
  const oneHour = settled[2].status === 'fulfilled' ? settled[2].value : [];
  const errors = settled
    .map((item, index) => item.status === 'rejected'
      ? `${['1m', '5m', '1h'][index]}:${String(item.reason?.message || item.reason)}`
      : '')
    .filter(Boolean);

  const coverage = {
    one_minute: takerCoverage(oneMinute),
    five_minute: takerCoverage(fiveMinute),
    one_hour: takerCoverage(oneHour),
  };
  const takerRows = coverage.one_minute.taker_rows + coverage.five_minute.taker_rows + coverage.one_hour.taker_rows;
  const totalRows = coverage.one_minute.total_rows + coverage.five_minute.total_rows + coverage.one_hour.total_rows;
  const ready = takerRows > 0;
  const status = ready
    ? (errors.length ? 'partial_ready' : 'ready')
    : (totalRows > 0 ? 'taker_fields_unavailable' : 'history_unavailable');

  return {
    ok: true,
    version: VERSION,
    provider,
    market_type: 'spot',
    symbol,
    ready,
    status,
    taker_supported: ready,
    one_minute_rows: oneMinute,
    five_minute_rows: fiveMinute,
    one_hour_rows: oneHour,
    coverage,
    interval_errors: errors,
    source: 'render_shared_exact_key_spot_kline_history_for_taker_flow',
    generated_at: new Date().toISOString(),
  };
}

async function getSharedHistory(provider, symbol, signal) {
  stats.reads += 1;
  pruneCache();
  const key = `${provider}:${symbol}`;
  const now = Date.now();
  const existing = cache.get(key);
  if (existing && existing.freshUntil > now) {
    stats.fresh_hits += 1;
    return cachedPayload(existing, 'fresh');
  }
  const running = inflight.get(key);
  if (running) {
    stats.inflight_hits += 1;
    return await running;
  }

  const task = (async () => {
    let release = null;
    try {
      release = await acquireBuildSlot(signal);
      stats.builds_started += 1;
      const payload = await buildPayload(provider, symbol, signal);
      const hasAnyRows = payload.one_minute_rows.length > 0 || payload.five_minute_rows.length > 0 || payload.one_hour_rows.length > 0;
      if (!hasAnyRows && existing && existing.staleUntil > Date.now()) {
        stats.stale_hits += 1;
        return cachedPayload(existing, 'stale_build_empty');
      }
      const ttl = payload.ready ? CACHE_TTL_MS : NEGATIVE_TTL_MS;
      const entry = {
        payload,
        storedAt: Date.now(),
        freshUntil: Date.now() + ttl,
        staleUntil: Date.now() + Math.max(ttl, STALE_MS),
      };
      cache.set(key, entry);
      pruneCache();
      if (payload.ready && payload.interval_errors.length === 0) stats.builds_succeeded += 1;
      else if (hasAnyRows) stats.builds_partial += 1;
      else stats.builds_failed += 1;
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

export function getSpotFlowHistoryHealth() {
  pruneCache();
  return {
    ok: true,
    version: VERSION,
    endpoint: '/api/spot-flow/history',
    health_endpoint: '/api/spot-flow/history-health',
    providers: [...PROVIDERS],
    mode: 'shared_exact_key_three_interval_spot_kline_history_cache',
    intervals: {
      '1m': 300,
      '5m': 320,
      '1h': 180,
    },
    cache_ttl_seconds: Math.round(CACHE_TTL_MS / 1000),
    stale_seconds: Math.round(STALE_MS / 1000),
    negative_ttl_seconds: Math.round(NEGATIVE_TTL_MS / 1000),
    cache_entries: cache.size,
    inflight_entries: inflight.size,
    cache_max: CACHE_MAX,
    build_active: activeBuilds,
    build_max_active: BUILD_MAX_ACTIVE,
    build_queue: queue.length,
    build_max_queue: BUILD_MAX_QUEUE,
    same_exact_key_reads_share_cache_and_inflight: true,
    app_no_longer_starts_three_direct_kline_history_reads: true,
    empty_or_failed_build_never_overwrites_verified_stale_payload: true,
    ...stats,
    time: new Date().toISOString(),
  };
}

export async function handleSpotFlowHistory(req, res, url) {
  if (url.pathname === '/api/spot-flow/history-health') {
    sendJson(res, 200, getSpotFlowHistoryHealth());
    return true;
  }
  if (url.pathname !== '/api/spot-flow/history') return false;
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, error: 'method_not_allowed' });
    return true;
  }
  const provider = providerKey(url.searchParams.get('provider'));
  const symbol = symbolKey(url.searchParams.get('symbol'));
  if (!provider || !symbol) {
    sendJson(res, 400, { ok: false, error: 'invalid_provider_or_symbol' });
    return true;
  }
  try {
    const payload = await getSharedHistory(provider, symbol, null);
    sendJson(res, 200, payload);
  } catch (error) {
    const message = String(error?.message || error);
    const status = message.includes('queue_full') ? 503 : 502;
    sendJson(res, status, {
      ok: false,
      version: VERSION,
      provider,
      market_type: 'spot',
      symbol,
      error: message,
      one_minute_rows: [],
      five_minute_rows: [],
      one_hour_rows: [],
      time: new Date().toISOString(),
    });
  }
  return true;
}

export const _test = {
  providerKey,
  symbolKey,
  rowsWithTimes,
  takerCoverage,
};

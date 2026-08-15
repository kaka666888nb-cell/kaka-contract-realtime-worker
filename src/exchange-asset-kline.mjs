// Step1026: exact-identity official Kline bridge for the non-crypto assets
// already verified by the Step1025 shared product catalog.
// This module never substitutes venue/product/ticker identities and never touches
// the protected Binance contract REST path.

const VERSION = '650.8.15.145';
const DATA_VERSION = 10260;
const SCHEMA_VERSION = 'step1026_all_asset_kline_v1';
const ENDPOINT = '/api/asset-klines';
const HEALTH_ENDPOINT = '/api/asset-klines/health';
const SELF_TEST_ENDPOINT = '/api/asset-klines/self-test';

const PROVIDERS = new Set(['okx', 'bybit', 'bitget', 'gate']);
const INTERVALS = new Set(['1m', '5m', '15m', '1h', '4h', '1d']);
const CACHE_MAX = 512;
const NEGATIVE_CACHE_MAX = 256;
const BUILD_MAX_ACTIVE = 4;
const BUILD_MAX_QUEUE = 80;
const STALE_MS = 15 * 60_000;
const NEGATIVE_TTL_MS = 45_000;
const FETCH_TIMEOUT_MS = 15_000;

const cache = new Map();
const negativeCache = new Map();
const inflight = new Map();
const queue = [];
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
  upstream_requests_started: 0,
  upstream_by_provider: { okx: 0, bybit: 0, bitget: 0, gate: 0 },
};

function text(raw) {
  return String(raw ?? '').trim();
}

function providerKey(raw) {
  let value = text(raw).toLowerCase();
  if (value === 'okex') value = 'okx';
  if (value === 'gate.io') value = 'gate';
  return PROVIDERS.has(value) ? value : '';
}

function marketTypeKey(raw) {
  return text(raw).toLowerCase().replace(/\s+/g, '_').slice(0, 64);
}

function assetClassKey(raw) {
  return text(raw).toLowerCase().replace(/\s+/g, '_').slice(0, 96);
}

function assetIdKey(raw) {
  const value = text(raw);
  if (!value || value.length > 240 || /[\u0000-\u001f\u007f]/.test(value)) return '';
  return value;
}

function nativeSymbolKey(raw) {
  const value = text(raw).toUpperCase();
  // Preserve official exchange identity. Only reject unsafe/control characters;
  // do not remove '-'/'_' because OKX and Gate rely on them.
  if (!value || value.length > 160) return '';
  if (!/^[A-Z0-9._:\-/]+$/.test(value)) return '';
  return value;
}

function intervalKey(raw) {
  const value = text(raw);
  return INTERVALS.has(value) ? value : '';
}

function safeLimit(raw) {
  const number = Number(raw);
  if (!Number.isFinite(number)) return 240;
  return Math.max(20, Math.min(300, Math.trunc(number)));
}

function finite(raw) {
  const value = typeof raw === 'number' ? raw : Number(raw);
  return Number.isFinite(value) ? value : null;
}

function nonNegative(raw) {
  const value = finite(raw);
  return value !== null && value >= 0 ? value : null;
}

function positive(raw) {
  const value = finite(raw);
  return value !== null && value > 0 ? value : null;
}

function isSpotMarket(marketType) {
  const value = marketTypeKey(marketType);
  return value === 'spot' || value.includes('spot');
}

function isSparseMarket({ provider, marketType, assetClass }) {
  if (provider !== 'okx') return false;
  const combined = `${marketTypeKey(marketType)}|${assetClassKey(assetClass)}`;
  return combined.includes('event') || combined.includes('prediction');
}

function exactScopeSupported({ provider, marketType, assetClass }) {
  const p = providerKey(provider);
  const market = marketTypeKey(marketType);
  const cls = assetClassKey(assetClass);
  if (!p || !market || !cls) return false;

  if (p === 'bybit') {
    return /(equity|stock|commodity|metal)/.test(cls);
  }
  if (p === 'bitget') {
    return /(equity|stock|rwa|commodity|metal)/.test(cls);
  }
  if (p === 'gate') {
    // Gate cash equities intentionally remain locked pending the commercial
    // second source. Step1026 only opens official derivative bars.
    if (isSpotMarket(market) || cls.includes('equity_cash')) return false;
    return /(equity_derivative|fx|forex|commodity|metal)/.test(cls);
  }
  if (p === 'okx') {
    return /(rwa|premarket|pre_market|event|prediction)/.test(cls) ||
      /(event|prediction)/.test(market);
  }
  return false;
}

function freshTtlMs(interval) {
  switch (interval) {
    case '1m': return 20_000;
    case '5m': return 30_000;
    case '15m': return 45_000;
    case '1h': return 60_000;
    case '4h': return 120_000;
    case '1d': return 300_000;
    default: return 30_000;
  }
}

function sendJson(res, statusCode, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
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
    let oldestKey = '';
    let oldestAt = Infinity;
    for (const [key, entry] of cache.entries()) {
      const storedAt = Number(entry?.storedAt || 0);
      if (storedAt < oldestAt) {
        oldestAt = storedAt;
        oldestKey = key;
      }
    }
    if (!oldestKey) break;
    cache.delete(oldestKey);
    stats.cache_evictions += 1;
  }
  while (negativeCache.size > NEGATIVE_CACHE_MAX) {
    const oldestKey = negativeCache.keys().next().value;
    if (!oldestKey) break;
    negativeCache.delete(oldestKey);
  }
}

function cachedPayload(entry, state) {
  return {
    ...entry.payload,
    cache_status: state,
    cache_age_seconds: Math.max(0, Math.floor((Date.now() - entry.storedAt) / 1000)),
  };
}

function acquireBuildSlot(signal) {
  if (signal?.aborted) return Promise.reject(new Error('asset_kline_request_aborted_before_queue'));
  if (activeBuilds < BUILD_MAX_ACTIVE) {
    activeBuilds += 1;
    return Promise.resolve(() => releaseBuildSlot());
  }
  if (queue.length >= BUILD_MAX_QUEUE) {
    stats.queue_rejections += 1;
    return Promise.reject(new Error('asset_kline_queue_full'));
  }
  return new Promise((resolve, reject) => {
    const item = { resolve, reject, signal, onAbort: null };
    if (signal) {
      item.onAbort = () => {
        const index = queue.indexOf(item);
        if (index >= 0) queue.splice(index, 1);
        reject(new Error('asset_kline_request_aborted_while_queued'));
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
    if (item.signal && item.onAbort) {
      item.signal.removeEventListener('abort', item.onAbort);
    }
    activeBuilds += 1;
    item.resolve(() => releaseBuildSlot());
    break;
  }
}

async function jsonFetch(url, provider, timeoutMs = FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('asset_kline_upstream_timeout')), timeoutMs);
  stats.upstream_requests_started += 1;
  if (stats.upstream_by_provider[provider] !== undefined) {
    stats.upstream_by_provider[provider] += 1;
  }
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3/Step1026-AssetKline',
      },
      signal: controller.signal,
    });
    const raw = await response.text();
    let payload = null;
    try {
      payload = raw ? JSON.parse(raw) : null;
    } catch {
      throw new Error(`asset_kline_invalid_json status=${response.status}`);
    }
    if (!response.ok) {
      throw new Error(`asset_kline_upstream_http_${response.status}`);
    }
    return payload;
  } finally {
    clearTimeout(timer);
  }
}

function normalizeRow({
  provider,
  assetId,
  nativeSymbol,
  interval,
  sparse,
  openTimeMs,
  open,
  high,
  low,
  close,
  volume = null,
  quoteVolume = null,
  confirm = null,
  source,
}) {
  const ts = finite(openTimeMs);
  const numberFn = sparse ? nonNegative : positive;
  const o = numberFn(open);
  const h = numberFn(high);
  const l = numberFn(low);
  const c = numberFn(close);
  if (ts === null || ts <= 0 || o === null || h === null || l === null || c === null || h < l) {
    return null;
  }
  const v = nonNegative(volume);
  const qv = nonNegative(quoteVolume);
  return {
    provider,
    asset_id: assetId,
    native_symbol: nativeSymbol,
    interval,
    open_time_ms: Math.trunc(ts),
    open: o,
    high: h,
    low: l,
    close: c,
    volume: v,
    quote_volume: qv,
    confirm: confirm == null ? null : String(confirm),
    source,
  };
}

function uniqueSortedRows(rows, limit) {
  const byTime = new Map();
  for (const row of rows) {
    if (!row) continue;
    byTime.set(Number(row.open_time_ms), row);
  }
  return [...byTime.values()]
    .sort((a, b) => Number(a.open_time_ms) - Number(b.open_time_ms))
    .slice(-limit);
}

function bybitInterval(interval) {
  return ({ '1m': '1', '5m': '5', '15m': '15', '1h': '60', '4h': '240', '1d': 'D' })[interval] || '';
}

function bitgetInterval(interval) {
  return ({ '1m': '1m', '5m': '5m', '15m': '15m', '1h': '1H', '4h': '4H', '1d': '1D' })[interval] || '';
}

function gateInterval(interval) {
  return ({ '1m': '1m', '5m': '5m', '15m': '15m', '1h': '1h', '4h': '4h', '1d': '1d' })[interval] || '';
}

function okxInterval(interval) {
  return ({ '1m': '1m', '5m': '5m', '15m': '15m', '1h': '1H', '4h': '4H', '1d': '1D' })[interval] || '';
}

function bybitCategory(marketType) {
  const market = marketTypeKey(marketType);
  if (market.includes('spot')) return 'spot';
  if (market.includes('inverse')) return 'inverse';
  return 'linear';
}

function bitgetCategory(marketType, nativeSymbol) {
  const market = marketTypeKey(marketType);
  if (market.includes('spot')) return 'SPOT';
  if (market.includes('usdc') || nativeSymbol.endsWith('USDC')) return 'USDC-FUTURES';
  if (market.includes('coin') || (!nativeSymbol.endsWith('USDT') && !nativeSymbol.endsWith('USDC'))) {
    return 'COIN-FUTURES';
  }
  return 'USDT-FUTURES';
}

function gateSettle(nativeSymbol, marketType) {
  const market = marketTypeKey(marketType);
  if (market.includes('btc') || /_BTC$/.test(nativeSymbol)) return 'btc';
  if (market.includes('usd') && !market.includes('usdt') && /_USD$/.test(nativeSymbol)) return 'usd';
  return 'usdt';
}

export function parseBybitRows(payload, identity) {
  if (Number(payload?.retCode ?? -1) !== 0) return [];
  const list = Array.isArray(payload?.result?.list) ? payload.result.list : [];
  return uniqueSortedRows(list.map((a) => Array.isArray(a) ? normalizeRow({
    ...identity,
    openTimeMs: a[0], open: a[1], high: a[2], low: a[3], close: a[4],
    volume: a[5], quoteVolume: a[6], source: 'bybit_official_public_asset_kline',
  }) : null), identity.limit);
}

export function parseBitgetRows(payload, identity) {
  if (String(payload?.code ?? '') !== '00000') return [];
  const list = Array.isArray(payload?.data) ? payload.data : [];
  return uniqueSortedRows(list.map((a) => Array.isArray(a) ? normalizeRow({
    ...identity,
    openTimeMs: a[0], open: a[1], high: a[2], low: a[3], close: a[4],
    volume: a[5], quoteVolume: a[6], source: 'bitget_official_public_asset_kline',
  }) : null), identity.limit);
}

export function parseOkxRows(payload, identity) {
  if (String(payload?.code ?? '') !== '0') return [];
  const list = Array.isArray(payload?.data) ? payload.data : [];
  return uniqueSortedRows(list.map((a) => Array.isArray(a) ? normalizeRow({
    ...identity,
    openTimeMs: a[0], open: a[1], high: a[2], low: a[3], close: a[4],
    volume: a[5], quoteVolume: a[7] ?? a[6], confirm: a[8],
    source: identity.sparse
      ? 'okx_official_public_event_asset_kline'
      : 'okx_official_public_asset_kline',
  }) : null), identity.limit);
}

export function parseGateRows(payload, identity) {
  if (!Array.isArray(payload)) return [];
  const rows = payload.map((a) => {
    if (Array.isArray(a)) {
      // Gate futures array compatibility: [t, v, c, h, l, o, sum].
      return normalizeRow({
        ...identity,
        openTimeMs: finite(a[0]) !== null ? Number(a[0]) * 1000 : null,
        open: a[5], high: a[3], low: a[4], close: a[2],
        volume: a[1], quoteVolume: a[6], source: 'gate_official_public_asset_futures_kline',
      });
    }
    if (!a || typeof a !== 'object') return null;
    return normalizeRow({
      ...identity,
      openTimeMs: finite(a.t) !== null ? Number(a.t) * 1000 : null,
      open: a.o, high: a.h, low: a.l, close: a.c,
      volume: a.v, quoteVolume: a.sum ?? a.a, source: 'gate_official_public_asset_futures_kline',
    });
  });
  return uniqueSortedRows(rows, identity.limit);
}

async function fetchBybit(identity) {
  const category = bybitCategory(identity.marketType);
  const bar = bybitInterval(identity.interval);
  const url = new URL('https://api.bybit.com/v5/market/kline');
  url.searchParams.set('category', category);
  url.searchParams.set('symbol', identity.nativeSymbol);
  url.searchParams.set('interval', bar);
  url.searchParams.set('limit', String(Math.min(1000, identity.limit)));
  const payload = await jsonFetch(url.toString(), 'bybit');
  return parseBybitRows(payload, identity);
}

async function fetchBitget(identity) {
  const category = bitgetCategory(identity.marketType, identity.nativeSymbol);
  const bar = bitgetInterval(identity.interval);
  const rows = [];
  let endTime = Date.now();
  for (let page = 0; page < 3 && rows.length < identity.limit; page += 1) {
    const url = new URL('https://api.bitget.com/api/v3/market/history-candles');
    url.searchParams.set('category', category);
    url.searchParams.set('symbol', identity.nativeSymbol);
    url.searchParams.set('interval', bar);
    url.searchParams.set('type', 'market');
    url.searchParams.set('endTime', String(endTime));
    url.searchParams.set('limit', String(Math.min(100, identity.limit - rows.length)));
    const payload = await jsonFetch(url.toString(), 'bitget');
    const pageRows = parseBitgetRows(payload, { ...identity, limit: 1000 });
    if (!pageRows.length) break;
    rows.push(...pageRows);
    const oldest = Math.min(...pageRows.map((row) => Number(row.open_time_ms)));
    if (!Number.isFinite(oldest) || oldest <= 1) break;
    const nextEnd = oldest - 1;
    if (nextEnd >= endTime) break;
    endTime = nextEnd;
    if (pageRows.length < 100) break;
  }
  return uniqueSortedRows(rows, identity.limit);
}

async function fetchOkx(identity) {
  const bar = okxInterval(identity.interval);
  const rows = [];
  let after = '';
  for (let page = 0; page < 3 && rows.length < identity.limit; page += 1) {
    const url = new URL('https://www.okx.com/api/v5/market/history-candles');
    url.searchParams.set('instId', identity.nativeSymbol);
    url.searchParams.set('bar', bar);
    url.searchParams.set('limit', String(Math.min(100, identity.limit - rows.length)));
    if (after) url.searchParams.set('after', after);
    const payload = await jsonFetch(url.toString(), 'okx');
    const pageRows = parseOkxRows(payload, { ...identity, limit: 1000 });
    if (!pageRows.length) break;
    rows.push(...pageRows);
    const oldest = Math.min(...pageRows.map((row) => Number(row.open_time_ms)));
    if (!Number.isFinite(oldest) || oldest <= 1) break;
    const nextAfter = String(oldest);
    if (nextAfter === after) break;
    after = nextAfter;
    if (pageRows.length < 100) break;
  }
  return uniqueSortedRows(rows, identity.limit);
}

async function fetchGate(identity) {
  const bar = gateInterval(identity.interval);
  const settle = gateSettle(identity.nativeSymbol, identity.marketType);
  const stepSeconds = ({ '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 })[identity.interval];
  const nowSeconds = Math.floor(Date.now() / 1000);
  const fromSeconds = Math.max(0, nowSeconds - (identity.limit + 8) * stepSeconds);
  const bases = [
    `https://api.gateio.ws/api/v4/futures/${settle}/candlesticks`,
    `https://fx-api.gateio.ws/api/v4/futures/${settle}/candlesticks`,
  ];
  let lastError = null;
  for (const base of bases) {
    try {
      const url = new URL(base);
      url.searchParams.set('contract', identity.nativeSymbol);
      url.searchParams.set('interval', bar);
      url.searchParams.set('from', String(fromSeconds));
      url.searchParams.set('to', String(nowSeconds));
      const payload = await jsonFetch(url.toString(), 'gate');
      const rows = parseGateRows(payload, identity);
      if (rows.length) return rows;
      lastError = new Error('gate_asset_kline_empty');
    } catch (error) {
      lastError = error;
    }
  }
  if (lastError) throw lastError;
  return [];
}

async function buildRows(identity) {
  if (identity.provider === 'bybit') return fetchBybit(identity);
  if (identity.provider === 'bitget') return fetchBitget(identity);
  if (identity.provider === 'gate') return fetchGate(identity);
  if (identity.provider === 'okx') return fetchOkx(identity);
  return [];
}

function responseBase(identity) {
  return {
    ok: true,
    version: VERSION,
    data_version: DATA_VERSION,
    schema_version: SCHEMA_VERSION,
    read_only_shared: true,
    user_direct_exchange_requests: 0,
    same_exact_key_reads_share_cache_and_inflight: true,
    cross_provider_substitution: false,
    cross_product_substitution: false,
    cross_ticker_substitution: false,
    provider: identity.provider,
    market_type: identity.marketType,
    asset_class: identity.assetClass,
    asset_id: identity.assetId,
    native_symbol: identity.nativeSymbol,
    resolved_native_symbol: identity.nativeSymbol,
    interval: identity.interval,
    sparse_market_bars: identity.sparse,
  };
}

function cacheKey(identity) {
  return [
    identity.provider,
    identity.marketType,
    identity.assetClass,
    identity.assetId,
    identity.nativeSymbol,
    identity.interval,
    identity.limit,
  ].join('|');
}

async function getSharedRows(identity, signal) {
  stats.reads += 1;
  pruneMaps();
  const key = cacheKey(identity);
  const now = Date.now();
  const existing = cache.get(key);
  if (existing && existing.freshUntil > now) {
    stats.fresh_hits += 1;
    return cachedPayload(existing, 'fresh_hit');
  }
  if (Number(negativeCache.get(key) || 0) > now) {
    stats.negative_hits += 1;
    if (identity.sparse) {
      return {
        ...responseBase(identity),
        rows: [],
        row_count: 0,
        cache_status: 'negative_sparse_empty',
        generated_at: new Date().toISOString(),
      };
    }
    return {
      ...responseBase(identity),
      ok: false,
      error: 'official_exact_asset_kline_temporarily_empty',
      rows: [],
      row_count: 0,
      cache_status: 'negative_empty',
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
      release = await acquireBuildSlot(signal);
      stats.builds_started += 1;
      const rows = await buildRows(identity);
      if (!rows.length && !identity.sparse) {
        stats.builds_empty += 1;
        negativeCache.set(key, Date.now() + NEGATIVE_TTL_MS);
        if (existing && existing.staleUntil > Date.now()) {
          stats.stale_hits += 1;
          return cachedPayload(existing, 'stale_build_empty');
        }
        return {
          ...responseBase(identity),
          ok: false,
          error: 'official_exact_asset_kline_empty',
          rows: [],
          row_count: 0,
          cache_status: 'miss_empty',
          generated_at: new Date().toISOString(),
        };
      }
      if (!rows.length && identity.sparse) {
        negativeCache.set(key, Date.now() + NEGATIVE_TTL_MS);
      } else {
        negativeCache.delete(key);
      }
      const payload = {
        ...responseBase(identity),
        rows,
        row_count: rows.length,
        cache_status: 'miss',
        generated_at: new Date().toISOString(),
      };
      const entry = {
        payload,
        storedAt: Date.now(),
        freshUntil: Date.now() + freshTtlMs(identity.interval),
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

function parseIdentity(url) {
  const provider = providerKey(url.searchParams.get('provider'));
  const marketType = marketTypeKey(url.searchParams.get('market_type'));
  const assetClass = assetClassKey(url.searchParams.get('asset_class'));
  const assetId = assetIdKey(url.searchParams.get('asset_id'));
  const nativeSymbol = nativeSymbolKey(url.searchParams.get('native_symbol'));
  const interval = intervalKey(url.searchParams.get('interval'));
  const limit = safeLimit(url.searchParams.get('limit'));
  const sparse = isSparseMarket({ provider, marketType, assetClass });
  return { provider, marketType, assetClass, assetId, nativeSymbol, interval, limit, sparse };
}

export function runAssetKlineSelfTest() {
  const sampleIdentity = {
    provider: 'bybit', marketType: 'spot', assetClass: 'equity_token',
    assetId: 'bybit|spot|AAPLXUSDT', nativeSymbol: 'AAPLXUSDT',
    interval: '15m', limit: 20, sparse: false,
  };
  const bybit = parseBybitRows({
    retCode: 0,
    result: { list: [['1786800000000','334.0','335.0','333.0','334.5','10','3345']] },
  }, sampleIdentity);
  const bitget = parseBitgetRows({
    code: '00000', data: [['1786800000000','10','11','9','10.5','20','210']],
  }, { ...sampleIdentity, provider: 'bitget', assetId: 'bitget|spot|RAAPLUSDT', nativeSymbol: 'RAAPLUSDT' });
  const okx = parseOkxRows({
    code: '0', data: [['1786800000000','0','0','0','0','0','0','0','1']],
  }, { ...sampleIdentity, provider: 'okx', marketType: 'event', assetClass: 'prediction_event', assetId: 'okx|event|demo', nativeSymbol: 'DEMO-EVENT', sparse: true });
  const gate = parseGateRows([
    { t: 1786800000, o: '70', h: '71', l: '69', c: '70.5', v: '100', sum: '7050' },
  ], { ...sampleIdentity, provider: 'gate', marketType: 'contract', assetClass: 'commodity', assetId: 'gate|contract|CL_USDT', nativeSymbol: 'CL_USDT' });

  const checks = {
    bybit_exact_symbol: bybit.length === 1 && bybit[0].native_symbol === 'AAPLXUSDT',
    bitget_exact_symbol: bitget.length === 1 && bitget[0].native_symbol === 'RAAPLUSDT',
    okx_sparse_zero_preserved: okx.length === 1 && okx[0].close === 0,
    gate_exact_symbol: gate.length === 1 && gate[0].native_symbol === 'CL_USDT',
    gate_cash_stock_blocked: exactScopeSupported({ provider: 'gate', marketType: 'spot', assetClass: 'equity_cash' }) === false,
    coinbase_not_supported: providerKey('coinbase') === '',
    bybit_equity_supported: exactScopeSupported({ provider: 'bybit', marketType: 'spot', assetClass: 'equity_token' }) === true,
    okx_event_supported: exactScopeSupported({ provider: 'okx', marketType: 'event', assetClass: 'prediction_event' }) === true,
    no_symbol_rewrite: nativeSymbolKey('EURUSD_USDT') === 'EURUSD_USDT' && nativeSymbolKey('ABC-USD-SWAP') === 'ABC-USD-SWAP',
  };
  return {
    ok: Object.values(checks).every(Boolean),
    version: VERSION,
    data_version: DATA_VERSION,
    schema_version: SCHEMA_VERSION,
    checks,
  };
}

export function getAssetKlineHealth() {
  pruneMaps();
  return {
    ok: true,
    version: VERSION,
    data_version: DATA_VERSION,
    schema_version: SCHEMA_VERSION,
    endpoint: ENDPOINT,
    health_endpoint: HEALTH_ENDPOINT,
    self_test_endpoint: SELF_TEST_ENDPOINT,
    providers: [...PROVIDERS],
    intervals: [...INTERVALS],
    mode: 'exact_identity_shared_cache_singleflight_official_asset_klines',
    read_only_shared: true,
    app_direct_exchange_requests: 0,
    same_exact_key_reads_share_cache_and_inflight: true,
    cross_provider_substitution: false,
    cross_product_substitution: false,
    cross_ticker_substitution: false,
    gate_cash_equity_secondary_source_still_locked: true,
    coinbase_equity_secondary_source_still_locked: true,
    okx_event_sparse_bars_allowed: true,
    binance_contract_rest_touched: false,
    cache_entries: cache.size,
    negative_entries: negativeCache.size,
    inflight_entries: inflight.size,
    cache_max: CACHE_MAX,
    build_active: activeBuilds,
    build_max_active: BUILD_MAX_ACTIVE,
    build_queue: queue.length,
    build_max_queue: BUILD_MAX_QUEUE,
    stale_seconds: Math.round(STALE_MS / 1000),
    negative_ttl_seconds: Math.round(NEGATIVE_TTL_MS / 1000),
    ...stats,
    self_test: runAssetKlineSelfTest(),
    time: new Date().toISOString(),
  };
}

export async function handleAssetKline(req, res, url, signal = null) {
  if (url.pathname === HEALTH_ENDPOINT) {
    sendJson(res, 200, getAssetKlineHealth());
    return true;
  }
  if (url.pathname === SELF_TEST_ENDPOINT) {
    sendJson(res, 200, runAssetKlineSelfTest());
    return true;
  }
  if (url.pathname !== ENDPOINT) return false;

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
    sendJson(res, 405, { ok: false, version: VERSION, error: 'GET required' });
    return true;
  }

  const identity = parseIdentity(url);
  const missing = [];
  if (!identity.provider) missing.push('provider');
  if (!identity.marketType) missing.push('market_type');
  if (!identity.assetClass) missing.push('asset_class');
  if (!identity.assetId) missing.push('asset_id');
  if (!identity.nativeSymbol) missing.push('native_symbol');
  if (!identity.interval) missing.push('interval');
  if (missing.length) {
    sendJson(res, 400, {
      ok: false,
      version: VERSION,
      data_version: DATA_VERSION,
      schema_version: SCHEMA_VERSION,
      error: 'invalid_or_missing_exact_asset_identity',
      missing,
      user_direct_exchange_requests: 0,
    });
    return true;
  }

  if (!exactScopeSupported(identity)) {
    sendJson(res, 403, {
      ...responseBase(identity),
      ok: false,
      error: 'asset_kline_scope_not_opened_or_secondary_source_required',
      rows: [],
      row_count: 0,
      cache_status: 'blocked_scope',
    });
    return true;
  }

  try {
    const payload = await getSharedRows(identity, signal);
    const status = payload.ok === true ? 200 : 404;
    sendJson(res, status, payload);
  } catch (error) {
    sendJson(res, 503, {
      ...responseBase(identity),
      ok: false,
      error: String(error?.message || error),
      rows: [],
      row_count: 0,
      cache_status: 'build_error',
      generated_at: new Date().toISOString(),
    });
  }
  return true;
}

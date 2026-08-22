// Step1035.19.6: Coinbase cash-equity candles were runtime-proven unavailable and are closed.
 // Cash-equity chart fallback now uses an EXPLICITLY LABELLED Bitget Reality rToken
 // reference product, never a hidden cross-product substitution.
 // All currently-online Reality SPOT instruments are discovered by one background
 // public instruments request per hour. User catalog reads never start exchange work.
 // Exact rToken Klines continue through the existing per-key shared cache/singleflight.
 // This module never touches the protected Binance contract REST path.

import { createPrivateKey, randomBytes, sign as cryptoSign } from 'node:crypto';
import { resolveCoinbaseEquityCandleRoute, getCoinbaseCoreKlineReplicaHealth } from './stock-catalog-v2.mjs';

const VERSION = '650.8.15.196.10.2';
const DATA_VERSION = 1041010;
const SCHEMA_VERSION = 'step1026_all_asset_kline_v1';
const ENDPOINT = '/api/asset-klines';
const HEALTH_ENDPOINT = '/api/asset-klines/health';
const SELF_TEST_ENDPOINT = '/api/asset-klines/self-test';

const PROVIDERS = new Set(['okx', 'bybit', 'bitget', 'gate']);
const INTERVALS = new Set(['1m', '5m', '15m', '1h', '4h', '1d']);
const CACHE_MAX = 512;
const NEGATIVE_CACHE_MAX = 256;
const BUILD_MAX_ACTIVE = 4; // global emergency ceiling retained from .161
const BUILD_MAX_QUEUE = 80; // global emergency queue ceiling retained from .161
const BUILD_PROVIDER_MAX_ACTIVE = 1;
const BUILD_PROVIDER_MAX_QUEUE = 20;
const BUILD_PROVIDER_ORDER = Object.freeze(['okx', 'bybit', 'bitget', 'gate']);
const STALE_MS = 15 * 60_000;
const NEGATIVE_TTL_MS = 45_000;
const FETCH_TIMEOUT_MS = 15_000;

const COINBASE_HOST = 'api.coinbase.com';
const COINBASE_CDP_KEY_NAME = String(process.env.KAKA_COINBASE_CDP_KEY_NAME || process.env.COINBASE_CDP_API_KEY_NAME || '').trim();
const COINBASE_CDP_KEY_SECRET = String(process.env.KAKA_COINBASE_CDP_KEY_SECRET || process.env.COINBASE_CDP_API_KEY_SECRET || '').replace(/\\n/g, '\n').trim();
const COINBASE_CDP_CONFIGURED = Boolean(COINBASE_CDP_KEY_NAME && COINBASE_CDP_KEY_SECRET);
let coinbasePrivateKey = null;
const COINBASE_CORE_EQUITY_TICKERS = new Set([
  'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'GOOG', 'META', 'TSLA',
  'AVGO', 'AMD', 'NFLX', 'ORCL', 'CRM', 'INTC', 'QCOM', 'ADBE',
  'COST', 'JPM', 'BAC', 'V', 'MA', 'WMT', 'XOM', 'CVX', 'UNH',
  'JNJ', 'PG', 'HD', 'LLY', 'ABBV', 'COIN', 'PLTR', 'SPY', 'QQQ',
]);

const BITGET_REALITY_MAP_ENDPOINT = '/api/asset-klines/reality-map';
const BITGET_REALITY_CATALOG_URL =
  'https://api.bitget.com/api/v3/market/instruments?category=SPOT';
const BITGET_REALITY_CATALOG_REFRESH_MS = 60 * 60_000;
const BITGET_REALITY_CATALOG_RETAIN_MS = 24 * 60 * 60_000;
const BITGET_REALITY_MAP_MAX_LIMIT = 1000;

let bitgetRealityCatalogRows = [];
let bitgetRealityByUnderlying = new Map();
let bitgetRealityCatalogInflight = null;
let bitgetRealityCatalogStarted = false;
const bitgetRealityCatalogStats = {
  refreshes_started: 0,
  refreshes_succeeded: 0,
  refreshes_failed: 0,
  last_started_at: null,
  last_success_at: null,
  last_error: '',
  last_row_count: 0,
  last_online_count: 0,
};

// Explicit aliases are only used to identify the SAME underlying reference asset.
// A candidate is accepted only if that Reality instrument actually exists in the
// live Bitget catalog. Unknown names never guess a product.
const BITGET_REALITY_CASH_TICKER_ALIASES = Object.freeze({
  '000660': ['SKHY'],       // SK hynix (KRX 000660)
  '000660.KS': ['SKHY'],
  '005930': ['SMSN', 'SAMSUNG'], // Samsung candidates, only accepted if live catalog contains one.
  '005930.KS': ['SMSN', 'SAMSUNG'],
});

const cache = new Map();
const negativeCache = new Map();
const inflight = new Map();
const buildBulkheads = new Map(BUILD_PROVIDER_ORDER.map((provider) => [provider, {
  provider,
  active: 0,
  queue: [],
  started: 0,
  completed: 0,
  rejected: 0,
  max_queue_seen: 0,
}]));
let activeBuilds = 0;
let buildRoundRobinCursor = 0;

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
  upstream_by_provider: { okx: 0, bybit: 0, bitget: 0, gate: 0, coinbase: 0 },
};

function text(raw) {
  return String(raw ?? '').trim();
}

function lower(raw) {
  return text(raw).toLowerCase();
}

function upper(raw) {
  return text(raw).toUpperCase();
}

function realityUnderlyingKey(raw) {
  const value = upper(raw).replace(/\s+/g, '');
  return value.replace(/[^A-Z0-9._-]/g, '').slice(0, 64);
}

function parseBitgetRealityInstrument(row) {
  if (!row || typeof row !== 'object') return null;
  if (lower(row.isReality) !== 'yes') return null;
  if (upper(row.category || 'SPOT') !== 'SPOT') return null;
  const status = lower(row.status);
  if (status && status !== 'online' && status !== 'listed') return null;

  const symbol = nativeSymbolKey(row.symbol);
  const quote = upper(row.quoteCoin || 'USDT');
  let base = text(row.baseCoin);
  if (!symbol || !quote) return null;

  let underlying = '';
  if (/^r/i.test(base)) underlying = realityUnderlyingKey(base.slice(1));
  if (!underlying && symbol.startsWith('R') && symbol.endsWith(quote) &&
      symbol.length > quote.length + 1) {
    underlying = realityUnderlyingKey(symbol.slice(1, -quote.length));
  }
  if (!underlying) return null;

  return {
    security_key: `bitget_reality:${underlying}:${quote}`,
    asset_group: 'stocks',
    security_type: 'equity_token',
    asset_class: 'equity_token',
    asset_class_zh: '股票代币',
    asset_class_en: 'Equity token',
    product_kind: 'reality_stock_token',
    provider: 'bitget',
    market_type: 'spot',
    asset_id: `bitget|spot|${symbol}`,
    exchange_symbol: symbol,
    native_symbol: symbol,
    display_symbol: underlying,
    security_ticker: underlying,
    display_name: underlying,
    display_name_zh: '',
    quote_asset: quote,
    base_asset: text(row.baseCoin) || `r${underlying}`,
    status: text(row.status),
    is_reality: true,
    reference_only_for_cash_equity: true,
    underlying_identity_kind: 'reality_protocol_reference',
    official_kline_capability: 'supported',
    official_kline_source: 'bitget_public_reality_candlestick',
    kline_intervals: ['1m', '5m', '15m', '1h', '4h', '1d'],
    source: 'bitget_official_v3_market_instruments_isReality_yes',
  };
}

function commitBitgetRealityCatalog(rows) {
  const sorted = [...rows].sort((a, b) =>
    `${a.security_ticker}|${a.quote_asset}|${a.exchange_symbol}`
      .localeCompare(`${b.security_ticker}|${b.quote_asset}|${b.exchange_symbol}`));
  const byUnderlying = new Map();
  for (const row of sorted) {
    const key = realityUnderlyingKey(row.security_ticker);
    if (!key) continue;
    const current = byUnderlying.get(key) || [];
    current.push(row);
    current.sort((a, b) => {
      const rank = (x) => x.quote_asset === 'USDT' ? 0 : x.quote_asset === 'USDC' ? 1 : 2;
      return rank(a) - rank(b) || a.exchange_symbol.localeCompare(b.exchange_symbol);
    });
    byUnderlying.set(key, current);
  }
  bitgetRealityCatalogRows = sorted;
  bitgetRealityByUnderlying = byUnderlying;
}

async function refreshBitgetRealityCatalog() {
  if (bitgetRealityCatalogInflight) return bitgetRealityCatalogInflight;
  bitgetRealityCatalogInflight = (async () => {
    bitgetRealityCatalogStats.refreshes_started += 1;
    bitgetRealityCatalogStats.last_started_at = new Date().toISOString();
    try {
      const payload = await jsonFetch(BITGET_REALITY_CATALOG_URL, 'bitget');
      if (String(payload?.code ?? '') !== '00000' || !Array.isArray(payload?.data)) {
        throw new Error('bitget_reality_catalog_invalid_payload');
      }
      const parsed = payload.data.map(parseBitgetRealityInstrument).filter(Boolean);
      if (!parsed.length) throw new Error('bitget_reality_catalog_empty');
      commitBitgetRealityCatalog(parsed);
      bitgetRealityCatalogStats.refreshes_succeeded += 1;
      bitgetRealityCatalogStats.last_success_at = new Date().toISOString();
      bitgetRealityCatalogStats.last_error = '';
      bitgetRealityCatalogStats.last_row_count = parsed.length;
      bitgetRealityCatalogStats.last_online_count = parsed.filter((row) =>
        lower(row.status) === 'online' || lower(row.status) === 'listed').length;
      return parsed.length;
    } catch (error) {
      bitgetRealityCatalogStats.refreshes_failed += 1;
      bitgetRealityCatalogStats.last_error =
        text(error?.message || error).replace(/\s+/g, ' ').slice(0, 240);
      throw error;
    } finally {
      bitgetRealityCatalogInflight = null;
    }
  })();
  return bitgetRealityCatalogInflight;
}

function startBitgetRealityCatalogCollector() {
  if (bitgetRealityCatalogStarted) return;
  bitgetRealityCatalogStarted = true;
  const first = setTimeout(() => {
    refreshBitgetRealityCatalog().catch(() => {});
  }, 1_500);
  first.unref?.();
  const timer = setInterval(() => {
    refreshBitgetRealityCatalog().catch(() => {});
  }, BITGET_REALITY_CATALOG_REFRESH_MS);
  timer.unref?.();
}

function realityNameCandidates(rawName) {
  const name = text(rawName).toLowerCase();
  if (!name) return [];
  const out = [];
  if (name.includes('sk hynix') || name.includes('sk hynix') || name.includes('海力士')) out.push('SKHY');
  if (name.includes('sandisk') || name.includes('san disk') || name.includes('闪迪')) out.push('SNDK');
  if (name.includes('samsung') || name.includes('三星')) out.push('SMSN', 'SAMSUNG');
  if (name.includes('micron') || name.includes('美光')) out.push('MU');
  return out;
}

function resolveBitgetRealityReference(securityTicker, displayName = '') {
  const ticker = realityUnderlyingKey(securityTicker);
  const candidates = [];
  const seen = new Set();
  const add = (value, mode) => {
    const key = realityUnderlyingKey(value);
    if (!key || seen.has(key)) return;
    seen.add(key);
    candidates.push({ key, mode });
  };
  add(ticker, 'exact_security_ticker');
  for (const value of BITGET_REALITY_CASH_TICKER_ALIASES[ticker] || []) {
    add(value, 'curated_underlying_alias');
  }
  for (const value of realityNameCandidates(displayName)) {
    add(value, 'curated_underlying_name_alias');
  }
  for (const candidate of candidates) {
    const rows = bitgetRealityByUnderlying.get(candidate.key);
    if (Array.isArray(rows) && rows.length) {
      return {
        ...rows[0],
        match_mode: candidate.mode,
        requested_security_ticker: ticker,
      };
    }
  }
  return null;
}

function bitgetRealityCatalogHealth() {
  const successMs = Date.parse(bitgetRealityCatalogStats.last_success_at || '');
  const ageMs = Number.isFinite(successMs) ? Math.max(0, Date.now() - successMs) : null;
  return {
    mode: 'background_public_spot_instruments_isReality_dynamic_all',
    endpoint: BITGET_REALITY_MAP_ENDPOINT,
    source_endpoint: '/api/v3/market/instruments?category=SPOT',
    ready: bitgetRealityCatalogRows.length > 0 &&
      (ageMs === null || ageMs <= BITGET_REALITY_CATALOG_RETAIN_MS),
    rows: bitgetRealityCatalogRows.length,
    distinct_underlyings: bitgetRealityByUnderlying.size,
    refresh_interval_minutes: Math.round(BITGET_REALITY_CATALOG_REFRESH_MS / 60_000),
    user_map_reads_start_exchange_requests: false,
    reads_scale_with_users: false,
    one_catalog_request_per_refresh: true,
    kline_build_mode: 'exact_rtoken_shared_cache_singleflight_on_demand',
    supported_intervals: ['1m', '5m', '15m', '1h', '4h', '1d'],
    ...bitgetRealityCatalogStats,
  };
}

function base64Url(value) {
  return Buffer.from(value).toString('base64url');
}

function coinbaseJwt(path) {
  if (!COINBASE_CDP_CONFIGURED) throw new Error('coinbase_cdp_credentials_not_configured');
  if (!coinbasePrivateKey) coinbasePrivateKey = createPrivateKey(COINBASE_CDP_KEY_SECRET);
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: 'ES256', typ: 'JWT', kid: COINBASE_CDP_KEY_NAME, nonce: randomBytes(16).toString('hex') };
  const payload = { iss: 'cdp', nbf: now, exp: now + 120, sub: COINBASE_CDP_KEY_NAME, uri: `GET ${COINBASE_HOST}${path}` };
  const input = `${base64Url(JSON.stringify(header))}.${base64Url(JSON.stringify(payload))}`;
  const sig = cryptoSign('sha256', Buffer.from(input), { key: coinbasePrivateKey, dsaEncoding: 'ieee-p1363' });
  return `${input}.${base64Url(sig)}`;
}

function coinbaseCoreIdentityAllowed(identity) {
  if (!COINBASE_CORE_EQUITY_TICKERS.has(identity.securityTicker)) return false;
  return identity.assetId === `coinbase:equity:${identity.nativeSymbol}`;
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

function securityTickerKey(raw) {
  const value = text(raw).toUpperCase().replace(/[^A-Z0-9._-]/g, '');
  return value.slice(0, 48);
}

function assetIdKey(raw) {
  const value = text(raw);
  if (!value || value.length > 240 || /[\u0000-\u001f\u007f]/.test(value)) return '';
  return value;
}

function nativeSymbolKey(raw, { preserveCase = false } = {}) {
  const original = text(raw);
  if (!original || original.length > 240 || /[\u0000-\u001f\u007f]/.test(original)) return '';
  if (preserveCase) return original;
  const value = original.toUpperCase();
  // Preserve official exchange identity. Only reject unsafe/control characters;
  // do not remove '-'/'_' because OKX and Gate rely on them.
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

  if (p === 'coinbase') {
    // Runtime Step1035.19.5 proved Coinbase EQUITY candles unavailable:
    // public alias/pair returned invalid product_id and canonical auth returned empty.
    // Keep cash-equity candles closed; the App may explicitly open a labelled
    // Bitget Reality reference product instead.
    return false;
  }
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

function buildQueueTotal() {
  let total = 0;
  for (const state of buildBulkheads.values()) total += state.queue.length;
  return total;
}

function buildBulkheadHealth() {
  const providers = {};
  for (const provider of BUILD_PROVIDER_ORDER) {
    const state = buildBulkheads.get(provider);
    providers[provider] = {
      active: state.active,
      queue: state.queue.length,
      max_active: BUILD_PROVIDER_MAX_ACTIVE,
      max_queue: BUILD_PROVIDER_MAX_QUEUE,
      started: state.started,
      completed: state.completed,
      rejected: state.rejected,
      max_queue_seen: state.max_queue_seen,
    };
  }
  return providers;
}

function startQueuedBuild(provider, item) {
  const state = buildBulkheads.get(provider);
  if (!state || !item || item.signal?.aborted) return false;
  if (item.signal && item.onAbort) item.signal.removeEventListener('abort', item.onAbort);
  state.active += 1;
  state.started += 1;
  activeBuilds += 1;
  item.resolve(() => releaseBuildSlot(provider));
  return true;
}

function drainBuildQueues() {
  while (activeBuilds < BUILD_MAX_ACTIVE) {
    let startedOne = false;
    for (let offset = 0; offset < BUILD_PROVIDER_ORDER.length; offset += 1) {
      const index = (buildRoundRobinCursor + offset) % BUILD_PROVIDER_ORDER.length;
      const provider = BUILD_PROVIDER_ORDER[index];
      const state = buildBulkheads.get(provider);
      if (!state || state.active >= BUILD_PROVIDER_MAX_ACTIVE) continue;
      while (state.queue.length > 0) {
        const item = state.queue.shift();
        if (!item || item.signal?.aborted) continue;
        buildRoundRobinCursor = (index + 1) % BUILD_PROVIDER_ORDER.length;
        startQueuedBuild(provider, item);
        startedOne = true;
        break;
      }
      if (startedOne || activeBuilds >= BUILD_MAX_ACTIVE) break;
    }
    if (!startedOne) break;
  }
}

function acquireBuildSlot(provider, signal) {
  const state = buildBulkheads.get(provider);
  if (!state) return Promise.reject(new Error('asset_kline_invalid_provider_bulkhead'));
  if (signal?.aborted) return Promise.reject(new Error('asset_kline_request_aborted_before_queue'));
  if (buildQueueTotal() === 0 && state.active < BUILD_PROVIDER_MAX_ACTIVE && activeBuilds < BUILD_MAX_ACTIVE) {
    state.active += 1;
    state.started += 1;
    activeBuilds += 1;
    return Promise.resolve(() => releaseBuildSlot(provider));
  }
  if (state.queue.length >= BUILD_PROVIDER_MAX_QUEUE || buildQueueTotal() >= BUILD_MAX_QUEUE) {
    stats.queue_rejections += 1;
    state.rejected += 1;
    return Promise.reject(new Error(state.queue.length >= BUILD_PROVIDER_MAX_QUEUE ? 'asset_kline_provider_queue_full' : 'asset_kline_global_queue_full'));
  }
  return new Promise((resolve, reject) => {
    const item = { provider, resolve, reject, signal, onAbort: null };
    if (signal) {
      item.onAbort = () => {
        const index = state.queue.indexOf(item);
        if (index >= 0) state.queue.splice(index, 1);
        reject(new Error('asset_kline_request_aborted_while_queued'));
        drainBuildQueues();
      };
      signal.addEventListener('abort', item.onAbort, { once: true });
    }
    state.queue.push(item);
    state.max_queue_seen = Math.max(state.max_queue_seen, state.queue.length);
    drainBuildQueues();
  });
}

function releaseBuildSlot(provider) {
  const state = buildBulkheads.get(provider);
  if (state) {
    state.active = Math.max(0, state.active - 1);
    state.completed += 1;
  }
  activeBuilds = Math.max(0, activeBuilds - 1);
  drainBuildQueues();
}

async function jsonFetch(url, provider, timeoutMs = FETCH_TIMEOUT_MS, headers = {}) {
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
        'user-agent': 'KakaWeb3/Step1035.19.6-AssetKline',
        ...headers,
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
      const detail = provider === 'coinbase'
        ? text(payload?.message || payload?.error || raw).replace(/\s+/g, ' ').slice(0, 180)
        : '';
      throw new Error(`asset_kline_upstream_http_${response.status}${detail ? `:${detail}` : ''}`);
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


function coinbaseGranularity(interval) {
  return ({
    '1m': 'ONE_MINUTE',
    '5m': 'FIVE_MINUTE',
    '15m': 'FIFTEEN_MINUTE',
    '1h': 'ONE_HOUR',
    '4h': 'FOUR_HOUR',
    '1d': 'ONE_DAY',
  })[interval] || '';
}

function intervalSeconds(interval) {
  return ({ '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 })[interval] || 60;
}

export function parseCoinbaseRows(payload, identity, source = 'coinbase_advanced_trade_public_exact_equity_candle') {
  const list = Array.isArray(payload?.candles) ? payload.candles : [];
  return uniqueSortedRows(list.map((a) => a && typeof a === 'object' ? normalizeRow({
    ...identity,
    openTimeMs: finite(a.start) !== null ? Number(a.start) * 1000 : null,
    open: a.open,
    high: a.high,
    low: a.low,
    close: a.close,
    volume: a.volume,
    quoteVolume: null,
    source,
  }) : null), identity.limit);
}

async function fetchCoinbase(_identity) {
  throw new Error('coinbase_cash_equity_candles_closed_runtime_proven_unavailable');
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
  // The Step1025 capability catalog identifies the verified trade-price path as
  // /api/v5/market/candles. The app requests at most 240 latest bars, while the
  // official current-candles endpoint accepts up to 300 in one response. Do not
  // route this latest-window request through history-candles (which caused the
  // real-device X-Perp failure seen in Step1026 productization).
  const url = new URL('https://www.okx.com/api/v5/market/candles');
  url.searchParams.set('instId', identity.nativeSymbol);
  url.searchParams.set('bar', bar);
  url.searchParams.set('limit', String(Math.min(300, identity.limit)));
  const payload = await jsonFetch(url.toString(), 'okx');
  return parseOkxRows(payload, { ...identity, limit: identity.limit });
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
  if (identity.provider === 'coinbase') return fetchCoinbase(identity);
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
    security_ticker: identity.securityTicker || null,
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
    identity.securityTicker || '',
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
      release = await acquireBuildSlot(identity.provider, signal);
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
  const nativeSymbol = nativeSymbolKey(url.searchParams.get('native_symbol'), { preserveCase: provider === 'coinbase' });
  const securityTicker = securityTickerKey(url.searchParams.get('security_ticker'));
  const interval = intervalKey(url.searchParams.get('interval'));
  const limit = safeLimit(url.searchParams.get('limit'));
  const sparse = isSparseMarket({ provider, marketType, assetClass });
  return { provider, marketType, assetClass, assetId, nativeSymbol, securityTicker, interval, limit, sparse };
}

function realityMapPayload(url) {
  const query = lower(url.searchParams.get('query'));
  const requestedTicker = text(url.searchParams.get('security_ticker'));
  const displayName = text(url.searchParams.get('display_name'));
  const offsetRaw = Number(url.searchParams.get('offset') || 0);
  const limitRaw = Number(url.searchParams.get('limit') || 50);
  const offset = Number.isFinite(offsetRaw) ? Math.max(0, Math.trunc(offsetRaw)) : 0;
  const limit = Number.isFinite(limitRaw)
    ? Math.max(1, Math.min(BITGET_REALITY_MAP_MAX_LIMIT, Math.trunc(limitRaw)))
    : 50;

  const filtered = query
    ? bitgetRealityCatalogRows.filter((row) => {
        const haystack = [
          row.security_ticker,
          row.exchange_symbol,
          row.base_asset,
          row.quote_asset,
        ].map((v) => lower(v)).join('|');
        return haystack.includes(query);
      })
    : bitgetRealityCatalogRows;
  const match = requestedTicker || displayName
    ? resolveBitgetRealityReference(requestedTicker, displayName)
    : null;

  return {
    ok: true,
    version: VERSION,
    data_version: DATA_VERSION,
    schema_version: 'step1035_19_6_bitget_reality_reference_v1',
    read_only_shared: true,
    background_catalog: true,
    user_read_upstream_requests: 0,
    user_read_upstream_connections: 0,
    user_map_reads_start_exchange_requests: false,
    reads_scale_with_users: false,
    cross_provider_substitution: false,
    hidden_cash_equity_substitution: false,
    explicit_reference_product: true,
    reference_provider: 'bitget',
    reference_product_kind: 'reality_stock_token',
    source: 'bitget_official_public_spot_instruments_isReality_yes',
    total: filtered.length,
    offset,
    limit,
    rows: filtered.slice(offset, offset + limit),
    match,
    catalog: bitgetRealityCatalogHealth(),
    generated_at: new Date().toISOString(),
  };
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
  const coinbase = parseCoinbaseRows({ candles: [{ start: '1786800000', low: '199', high: '202', open: '200', close: '201', volume: '12345' }] }, { ...sampleIdentity, provider: 'coinbase', marketType: 'equity', assetClass: 'equity_cash', assetId: 'coinbase:equity:opaqueCaseId', nativeSymbol: 'opaqueCaseId', securityTicker: 'AAPL' });

  const checks = {
    bybit_exact_symbol: bybit.length === 1 && bybit[0].native_symbol === 'AAPLXUSDT',
    bitget_exact_symbol: bitget.length === 1 && bitget[0].native_symbol === 'RAAPLUSDT',
    okx_sparse_zero_preserved: okx.length === 1 && okx[0].close === 0,
    gate_exact_symbol: gate.length === 1 && gate[0].native_symbol === 'CL_USDT',
    gate_cash_stock_blocked: exactScopeSupported({ provider: 'gate', marketType: 'spot', assetClass: 'equity_cash' }) === false,
    coinbase_provider_closed_for_cash_kline: providerKey('coinbase') === '',
    coinbase_parser_supported: coinbase.length === 1 && coinbase[0].close === 201,
    coinbase_opaque_product_id_case_preserved: nativeSymbolKey('opaqueCaseId', { preserveCase: true }) === 'opaqueCaseId',
    coinbase_cash_equity_scope_closed: exactScopeSupported({ provider: 'coinbase', marketType: 'equity', assetClass: 'equity_cash' }) === false,
    coinbase_core_exact_identity_guard: coinbaseCoreIdentityAllowed({ assetId: 'coinbase:equity:opaqueCaseId', nativeSymbol: 'opaqueCaseId', securityTicker: 'AAPL' }) === true && coinbaseCoreIdentityAllowed({ assetId: 'coinbase:equity:otherId', nativeSymbol: 'opaqueCaseId', securityTicker: 'AAPL' }) === false,
    coinbase_failed_candle_routes_removed_from_runtime: String(fetchCoinbase).includes('closed_runtime_proven_unavailable'),
    bitget_reality_catalog_parser: parseBitgetRealityInstrument({
      symbol: 'RAAPLUSDT', category: 'SPOT', baseCoin: 'rAAPL', quoteCoin: 'USDT',
      isReality: 'yes', status: 'online',
    })?.security_ticker === 'AAPL',
    bitget_reality_catalog_rejects_non_reality: parseBitgetRealityInstrument({
      symbol: 'BTCUSDT', category: 'SPOT', baseCoin: 'BTC', quoteCoin: 'USDT',
      isReality: 'no', status: 'online',
    }) === null,
    bitget_reality_map_is_background_only: String(realityMapPayload).includes('user_read_upstream_requests: 0'),
    bitget_reality_kline_uses_exact_native_symbol: String(fetchBitget).includes("url.searchParams.set('symbol', identity.nativeSymbol)"),
    coinbase_non_equity_scope_blocked: exactScopeSupported({ provider: 'coinbase', marketType: 'spot', assetClass: 'equity_cash' }) === false,
    bybit_equity_supported: exactScopeSupported({ provider: 'bybit', marketType: 'spot', assetClass: 'equity_token' }) === true,
    okx_event_supported: exactScopeSupported({ provider: 'okx', marketType: 'event', assetClass: 'prediction_event' }) === true,
    okx_latest_asset_window_uses_current_candles: true,
    okx_xperp_symbol_preserved: nativeSymbolKey('AAOI-USD_UM_XPERP-310711') === 'AAOI-USD_UM_XPERP-310711',
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
    coinbase_core_equity_official_candles_opened: false,
    coinbase_cash_equity_candles_closed_after_runtime_probe: true,
    coinbase_runtime_probe_result:
      'public_alias_and_pair_invalid_product_id; canonical_authenticated_empty',
    cash_equity_reference_policy:
      'explicit_labelled_reality_rtoken_reference_only_never_hidden_substitution',
    bitget_reality_reference_map_endpoint: BITGET_REALITY_MAP_ENDPOINT,
    bitget_reality_catalog: bitgetRealityCatalogHealth(),
    okx_event_sparse_bars_allowed: true,
    okx_latest_asset_window_endpoint: '/api/v5/market/candles',
    okx_history_candles_used_for_latest_asset_window: false,
    binance_contract_rest_touched: false,
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
    stale_seconds: Math.round(STALE_MS / 1000),
    negative_ttl_seconds: Math.round(NEGATIVE_TTL_MS / 1000),
    ...stats,
    self_test: runAssetKlineSelfTest(),
    time: new Date().toISOString(),
  };
}

startBitgetRealityCatalogCollector();

export async function handleAssetKline(req, res, url, signal = null) {
  if (url.pathname === HEALTH_ENDPOINT) {
    sendJson(res, 200, getAssetKlineHealth());
    return true;
  }
  if (url.pathname === SELF_TEST_ENDPOINT) {
    sendJson(res, 200, runAssetKlineSelfTest());
    return true;
  }
  if (url.pathname === BITGET_REALITY_MAP_ENDPOINT) {
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
    const health = bitgetRealityCatalogHealth();
    if (!health.ready) {
      sendJson(res, 503, {
        ok: false,
        version: VERSION,
        data_version: DATA_VERSION,
        schema_version: 'step1035_19_6_bitget_reality_reference_v1',
        read_only_shared: true,
        user_read_upstream_requests: 0,
        error: 'bitget_reality_background_catalog_pending',
        catalog: health,
      });
      return true;
    }
    sendJson(res, 200, realityMapPayload(url));
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
  if (identity.provider === 'coinbase' && !identity.securityTicker) missing.push('security_ticker');
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

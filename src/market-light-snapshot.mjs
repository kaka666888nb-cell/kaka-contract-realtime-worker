import { getMarketUniverseRows, tickers as loadMarketTickers } from './market-rest.mjs';

const STEP_VERSION = '650.8.15.86';
const SNAPSHOT_ROUTE = '/api/market-light/current-snapshot';
const HEALTH_ROUTE = '/api/market-light/health';

const SPOT_PROVIDERS = Object.freeze(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
const CONTRACT_PROVIDERS = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const PRIMARY_QUOTE = Object.freeze({
  spot: Object.freeze({
    binance: 'USDT',
    coinbase: 'USD',
    okx: 'USDT',
    bybit: 'USDT',
    bitget: 'USDT',
    gate: 'USDT',
  }),
  contract: Object.freeze({
    binance: 'USDT',
    okx: 'USDT',
    bybit: 'USDT',
    bitget: 'USDT',
    gate: 'USDT',
  }),
});

const SCAN_INTERVAL_MS = Math.max(15_000, Number(process.env.KAKA_MARKET_LIGHT_SCAN_INTERVAL_MS || 30_000));
const START_DELAY_MS = Math.max(1_000, Number(process.env.KAKA_MARKET_LIGHT_START_DELAY_MS || 7_000));
const STALE_MS = Math.max(60_000, Number(process.env.KAKA_MARKET_LIGHT_STALE_MS || 3 * 60_000));
const DIRECTORY_INTERVAL_MS = Math.max(2 * 60_000, Number(process.env.KAKA_MARKET_LIGHT_DIRECTORY_INTERVAL_MS || 10 * 60_000));
const SNAPSHOT_CACHE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_MARKET_LIGHT_RESPONSE_CACHE_TTL_MS || 20_000));
const BUILD_CONCURRENCY = Math.max(1, Math.min(3, Number(process.env.KAKA_MARKET_LIGHT_BUILD_CONCURRENCY || 2)));
const PARTIAL_RETAIN_RATIO = Math.max(0.4, Math.min(0.95, Number(process.env.KAKA_MARKET_LIGHT_PARTIAL_RETAIN_RATIO || 0.65)));
const DIRECTORY_MIN_RATIO_AFTER_WARM = Math.max(0.2, Math.min(0.9, Number(process.env.KAKA_MARKET_LIGHT_DIRECTORY_MIN_RATIO_AFTER_WARM || 0.35)));

// Coinbase Advanced Trade public ticker_batch. It gives a market-wide light
// price/24h layer without N per-symbol REST calls. Product IDs are subscribed
// in bounded chunks on one shared backend connection. BBO remains unavailable
// in ticker_batch and is intentionally left null rather than inferred.
const COINBASE_WS_URL = 'wss://advanced-trade-ws.coinbase.com';
const COINBASE_SUBSCRIBE_CHUNK = Math.max(20, Math.min(100, Number(process.env.KAKA_MARKET_LIGHT_COINBASE_SUBSCRIBE_CHUNK || 80)));
const COINBASE_SUBSCRIBE_GAP_MS = Math.max(130, Number(process.env.KAKA_MARKET_LIGHT_COINBASE_SUBSCRIBE_GAP_MS || 160));
const COINBASE_RECONNECT_MIN_MS = Math.max(1_000, Number(process.env.KAKA_MARKET_LIGHT_COINBASE_RECONNECT_MIN_MS || 2_000));
const COINBASE_RECONNECT_MAX_MS = Math.max(COINBASE_RECONNECT_MIN_MS, Number(process.env.KAKA_MARKET_LIGHT_COINBASE_RECONNECT_MAX_MS || 30_000));
const COINBASE_MAX_PRODUCT_IDS = Math.max(50, Math.min(2_000, Number(process.env.KAKA_MARKET_LIGHT_COINBASE_MAX_PRODUCT_IDS || 1_200)));

const rowsByKey = new Map();
const metaByKey = new Map();
const directoryCountByKey = new Map();
const directoryRowsByKey = new Map();
const directoryUpdatedAtByKey = new Map();
const responseCache = new Map();

let started = false;
let running = false;
let directoryRunning = false;
let round = 0;
let lastStartedAt = null;
let lastCompletedAt = null;
let lastError = '';
let scanTimer = null;
let scanInterval = null;
let directoryInterval = null;
let totalSnapshotReads = 0;
let totalBuilds = 0;
let totalBuildFailures = 0;
let responseCacheHits = 0;
let responseCacheMisses = 0;

const okxContractBatchEnrichment = {
  attempts: 0,
  successes: 0,
  failures: 0,
  last_started_at: null,
  last_completed_at: null,
  last_error: '',
  mark_rows: 0,
  index_rows: 0,
  open_interest_rows: 0,
  patch_rows: 0,
  mark_host: '',
  index_host: '',
  open_interest_host: '',
};

let wsCtorPromise = null;
const coinbase = {
  socket: null,
  connecting: null,
  reconnectTimer: null,
  reconnectAttempt: 0,
  ready: false,
  openedAt: 0,
  lastMessageAt: 0,
  lastHeartbeatAt: 0,
  lastError: '',
  universeUpdatedAt: 0,
  productIds: [],
  rows: new Map(),
  connectAttempts: 0,
  subscribeMessages: 0,
  messages: 0,
  tickerUpdates: 0,
};

function keyFor(market, provider) {
  return `${market}:${provider}`;
}

function compact(value) {
  return String(value ?? '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function finite(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function positive(value) {
  const number = finite(value);
  return number != null && number > 0 ? number : null;
}

function integer(value) {
  const number = finite(value);
  return number == null ? 0 : Math.trunc(number);
}

function isoMs(value) {
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric > 0) {
    const ms = numeric < 10_000_000_000 ? numeric * 1000 : numeric;
    const date = new Date(ms);
    if (Number.isFinite(date.getTime())) return date.toISOString();
  }
  const parsed = Date.parse(String(value ?? ''));
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}

function quoteAssetFor(row, fallback = '') {
  const explicit = compact(row?.quote_asset ?? row?.quoteAsset ?? row?.settle_asset ?? row?.quote_symbol);
  if (explicit) return explicit;
  const symbol = compact(row?.symbol ?? row?.native_symbol ?? row?.raw_symbol);
  for (const quote of ['FDUSD', 'PYUSD', 'USDT', 'USDC', 'USD1', 'TUSD', 'BUSD', 'EURC', 'DAI', 'USD', 'BTC', 'BNB', 'ETH', 'EUR', 'GBP', 'JPY', 'KRW', 'TRY', 'BRL', 'AUD', 'CAD', 'SGD', 'HKD', 'CHF', 'MXN', 'PLN']) {
    if (symbol.endsWith(quote) && symbol.length > quote.length) return quote;
  }
  return compact(fallback);
}

function baseAssetFor(row, quote) {
  const explicit = compact(row?.base_asset ?? row?.baseAsset);
  if (explicit) return explicit;
  const symbol = compact(row?.symbol ?? row?.native_symbol ?? row?.raw_symbol);
  return quote && symbol.endsWith(quote) ? symbol.slice(0, -quote.length) : '';
}

function rowTimeMs(row) {
  for (const value of [row?.source_time, row?.cached_at, row?.updated_at, row?.requested_at]) {
    const parsed = Date.parse(String(value ?? ''));
    if (Number.isFinite(parsed)) return parsed;
  }
  return 0;
}

function directoryIdentityMaps(provider, market) {
  const rows = directoryRowsByKey.get(keyFor(market, provider)) || [];
  const byDisplay = new Map();
  const byNative = new Map();
  for (const row of rows) {
    const display = compact(row?.symbol);
    const native = compact(row?.native_symbol ?? row?.raw_symbol ?? row?.symbol);
    if (display) byDisplay.set(display, row);
    if (native) byNative.set(native, row);
  }
  return { byDisplay, byNative };
}

function normalizeRow(provider, market, raw, observedAt, primaryQuote, identities = null) {
  if (!raw || typeof raw !== 'object') return null;
  const rawNative = compact(raw.native_symbol ?? raw.raw_symbol ?? raw.symbol);
  const rawDisplay = compact(raw.symbol);
  const identity = identities?.byNative?.get(rawNative) || identities?.byDisplay?.get(rawDisplay) || null;
  const hasDirectoryIdentity = Boolean((identities?.byNative?.size || 0) + (identities?.byDisplay?.size || 0));
  if (hasDirectoryIdentity && !identity) return null;
  const symbol = compact(identity?.symbol ?? rawDisplay ?? rawNative);
  if (!symbol) return null;
  const quote = compact(identity?.quote_asset) || quoteAssetFor(raw, primaryQuote);
  if (quote !== primaryQuote) return null;
  const lastPrice = positive(raw.last_price ?? raw.price ?? raw.lastPrice ?? raw.last ?? raw.close);
  if (lastPrice == null) return null;
  const base = compact(identity?.base_asset) || baseAssetFor(raw, quote);
  const sourceTime = raw.source_time ?? raw.cached_at ?? observedAt;
  return {
    ...raw,
    provider,
    market_type: market,
    symbol,
    raw_symbol: identity?.raw_symbol ?? raw.raw_symbol ?? raw.native_symbol ?? raw.symbol ?? symbol,
    native_symbol: identity?.native_symbol ?? raw.native_symbol ?? raw.raw_symbol ?? raw.symbol ?? symbol,
    base_asset: base || raw.base_asset || null,
    quote_asset: quote,
    quote_symbol: quote,
    settle_asset: identity?.settle_asset ?? raw.settle_asset ?? null,
    contract_type: identity?.contract_type ?? raw.contract_type ?? null,
    last_price: lastPrice,
    price: lastPrice,
    source_time: sourceTime,
    cached_at: raw.cached_at ?? sourceTime ?? observedAt,
    backend_shared: true,
    market_light_scope: 'primary_quote_full_directory',
  };
}

function dedupeRows(provider, market, rawRows, observedAt, primaryQuote) {
  const bySymbol = new Map();
  const identities = directoryIdentityMaps(provider, market);
  for (const raw of Array.isArray(rawRows) ? rawRows : []) {
    const row = normalizeRow(provider, market, raw, observedAt, primaryQuote, identities);
    if (!row) continue;
    const existing = bySymbol.get(row.symbol);
    if (!existing || rowTimeMs(row) >= rowTimeMs(existing)) bySymbol.set(row.symbol, row);
  }
  return [...bySymbol.values()].sort((a, b) => a.symbol.localeCompare(b.symbol));
}

function fieldCoverage(rows) {
  const fields = {
    price: 'last_price',
    change_24h: 'price_change_percent_24h',
    base_volume_24h: 'base_volume_24h',
    quote_volume_24h: 'quote_volume_24h',
    best_bid: 'best_bid',
    best_ask: 'best_ask',
    mark_price: 'mark_price',
    index_price: 'index_price',
    funding_rate: 'funding_rate',
    next_funding_time: 'next_funding_time',
    funding_interval_hours: 'funding_interval_hours',
    open_interest: 'open_interest',
    open_interest_value: 'open_interest_value',
    basis_rate: 'basis_rate',
  };
  const result = { rows: rows.length };
  for (const [name, field] of Object.entries(fields)) {
    result[name] = rows.filter((row) => row?.[field] != null && row?.[field] !== '').length;
  }
  return result;
}

function providerMeta(provider, market) {
  const key = keyFor(market, provider);
  const rows = rowsByKey.get(key) || [];
  const meta = metaByKey.get(key) || {};
  const updatedAt = String(meta.updated_at || '');
  const updatedMs = Date.parse(updatedAt);
  const stale = !Number.isFinite(updatedMs) || Date.now() - updatedMs > STALE_MS;
  const directoryRows = Number(directoryCountByKey.get(key) || 0);
  return {
    provider,
    market_type: market,
    primary_quote: PRIMARY_QUOTE[market]?.[provider] || null,
    row_count: rows.length,
    directory_count: directoryRows,
    directory_coverage_percent: directoryRows > 0 ? Math.min(100, (rows.length / directoryRows) * 100) : null,
    stale,
    updated_at: meta.updated_at || null,
    last_error: meta.last_error || '',
    successful_builds: Number(meta.successful_builds || 0),
    failed_builds: Number(meta.failed_builds || 0),
    build_calls: Number(meta.build_calls || 0),
    field_coverage: fieldCoverage(rows),
  };
}

async function mapLimit(values, concurrency, mapper) {
  const results = new Array(values.length);
  let cursor = 0;
  async function worker() {
    while (true) {
      const index = cursor++;
      if (index >= values.length) return;
      try {
        results[index] = await mapper(values[index], index);
      } catch (error) {
        results[index] = { ok: false, error: String(error?.message || error) };
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, Math.max(1, values.length)) }, () => worker()));
  return results;
}

async function fetchJson(url, { timeoutMs = 15_000 } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  timer.unref?.();
  try {
    const response = await fetch(url, {
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3/650.8.15.86 market-light',
      },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`market_light_http_${response.status}:${new URL(url).hostname}`);
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchFirstNonEmptyJson(urls, { timeoutMs = 15_000 } = {}) {
  let lastFailure = null;
  for (const url of urls) {
    try {
      const payload = await fetchJson(url, { timeoutMs });
      if (payload?.code != null && String(payload.code) !== '0') {
        throw new Error(`okx_code_${payload.code}:${String(payload.msg || '')}`);
      }
      const data = Array.isArray(payload?.data) ? payload.data : [];
      if (!data.length) throw new Error(`okx_empty_batch:${new URL(url).hostname}`);
      return { payload, url, host: new URL(url).hostname };
    } catch (error) {
      lastFailure = error;
    }
  }
  throw lastFailure || new Error('okx_batch_all_hosts_failed');
}

function bitgetV3Row(item, market, observedAt) {
  const symbol = compact(item?.symbol);
  if (!symbol || !symbol.endsWith('USDT')) return null;
  const last = positive(item?.lastPrice);
  if (last == null) return null;
  let change = finite(item?.price24hPcnt);
  if (change != null && Math.abs(change) <= 2) change *= 100;
  const bid = positive(item?.bid1Price);
  const ask = positive(item?.ask1Price);
  const baseVolume = finite(item?.volume24h);
  const quoteVolume = finite(item?.turnover24h);
  const oi = market === 'contract' ? finite(item?.openInterest) : null;
  return {
    provider: 'bitget',
    market_type: market,
    symbol,
    raw_symbol: symbol,
    native_symbol: symbol,
    base_asset: symbol.slice(0, -4),
    quote_asset: 'USDT',
    quote_symbol: 'USDT',
    last_price: last,
    price: last,
    price_change_percent_24h: change,
    volume_24h: baseVolume,
    base_volume_24h: baseVolume,
    quote_volume_24h: quoteVolume,
    high_24h: finite(item?.highPrice24h),
    low_24h: finite(item?.lowPrice24h),
    best_bid: bid,
    best_ask: ask,
    bid_price: bid,
    ask_price: ask,
    spread_percent: bid != null && ask != null && ask > 0 && ask >= bid ? ((ask - bid) / ask) * 100 : null,
    mark_price: market === 'contract' ? finite(item?.markPrice) : null,
    index_price: market === 'contract' ? finite(item?.indexPrice) : null,
    funding_rate: market === 'contract' ? finite(item?.fundingRate) : null,
    open_interest: oi,
    open_interest_value: oi != null ? oi * last : null,
    open_interest_unit: oi != null ? 'base_asset' : null,
    open_interest_value_unit: oi != null ? 'quote_asset' : null,
    source: 'bitget_official_public_v3_market_tickers_full_product',
    transport: 'rest_product_batch',
    source_time: isoMs(item?.ts) || observedAt,
    cached_at: observedAt,
  };
}

async function loadBitgetV3Rows(market, observedAt) {
  const category = market === 'contract' ? 'USDT-FUTURES' : 'SPOT';
  const payload = await fetchJson(`https://api.bitget.com/api/v3/market/tickers?category=${encodeURIComponent(category)}`);
  if (String(payload?.code ?? '00000') !== '00000') {
    throw new Error(`bitget_v3_tickers_code_${payload?.code ?? 'unknown'}`);
  }
  const data = Array.isArray(payload?.data) ? payload.data : Array.isArray(payload?.data?.list) ? payload.data.list : [];
  const rows = data.map((item) => bitgetV3Row(item, market, observedAt)).filter(Boolean);
  if (!rows.length) throw new Error('bitget_v3_tickers_rows_empty');
  return rows;
}

function mergeRowsByNative(baseRows, patchRows, provider, market, observedAt, primaryQuote) {
  const identities = directoryIdentityMaps(provider, market);
  const base = new Map();
  for (const raw of Array.isArray(baseRows) ? baseRows : []) {
    const row = normalizeRow(provider, market, raw, observedAt, primaryQuote, identities);
    if (row) base.set(row.symbol, row);
  }
  for (const raw of Array.isArray(patchRows) ? patchRows : []) {
    const native = compact(raw?.native_symbol ?? raw?.raw_symbol ?? raw?.symbol);
    const display = compact(raw?.symbol);
    const identity = identities.byNative.get(native) || identities.byDisplay.get(display) || null;
    const symbol = compact(identity?.symbol ?? display);
    if (!symbol || !base.has(symbol)) continue;
    const current = base.get(symbol) || {};
    const merged = { ...current };
    for (const [field, value] of Object.entries(raw || {})) {
      if (value !== null && value !== undefined && value !== '') merged[field] = value;
    }
    merged.provider = provider;
    merged.market_type = market;
    merged.symbol = symbol;
    merged.base_asset = compact(identity?.base_asset) || current.base_asset || null;
    merged.quote_asset = compact(identity?.quote_asset) || current.quote_asset || primaryQuote;
    merged.quote_symbol = merged.quote_asset;
    merged.raw_symbol = identity?.raw_symbol ?? current.raw_symbol ?? raw.raw_symbol ?? raw.native_symbol ?? symbol;
    merged.native_symbol = identity?.native_symbol ?? current.native_symbol ?? raw.native_symbol ?? raw.raw_symbol ?? symbol;
    base.set(symbol, merged);
  }
  return [...base.values()].sort((a, b) => a.symbol.localeCompare(b.symbol));
}

async function loadOkxContractBatchEnrichment(observedAt) {
  okxContractBatchEnrichment.attempts += 1;
  okxContractBatchEnrichment.last_started_at = new Date().toISOString();
  const [markResult, indexResult, oiResult] = await Promise.allSettled([
    fetchFirstNonEmptyJson([
      'https://www.okx.com/api/v5/public/mark-price?instType=SWAP',
      'https://aws.okx.com/api/v5/public/mark-price?instType=SWAP',
    ]),
    fetchFirstNonEmptyJson([
      'https://www.okx.com/api/v5/market/index-tickers?quoteCcy=USDT',
      'https://aws.okx.com/api/v5/market/index-tickers?quoteCcy=USDT',
    ]),
    fetchFirstNonEmptyJson([
      'https://www.okx.com/api/v5/public/open-interest?instType=SWAP',
      'https://aws.okx.com/api/v5/public/open-interest?instType=SWAP',
    ]),
  ]);

  const patches = new Map();
  function ensureSwap(instId) {
    const raw = String(instId || '').trim().toUpperCase();
    const key = compact(raw);
    if (!raw || !key) return null;
    if (!patches.has(key)) {
      patches.set(key, {
        provider: 'okx',
        market_type: 'contract',
        raw_symbol: raw,
        native_symbol: raw,
        source_time: observedAt,
        cached_at: observedAt,
      });
    }
    return patches.get(key);
  }
  function swapIdForIndex(indexId) {
    const raw = String(indexId || '').trim().toUpperCase();
    if (!raw) return '';
    return raw.endsWith('-SWAP') ? raw : `${raw}-SWAP`;
  }

  let markRows = 0;
  let indexRows = 0;
  let oiRows = 0;
  const errors = [];

  if (markResult.status === 'fulfilled') {
    okxContractBatchEnrichment.mark_host = markResult.value.host;
    for (const item of markResult.value.payload.data) {
      const row = ensureSwap(item?.instId);
      const value = finite(item?.markPx);
      if (!row || value == null || value <= 0) continue;
      row.mark_price = value;
      row.source_time = isoMs(item?.ts) || row.source_time;
      row.mark_price_source = 'okx_public_mark_price_batch';
      markRows += 1;
    }
  } else {
    errors.push(`mark:${String(markResult.reason?.message || markResult.reason || 'failed')}`);
  }

  if (indexResult.status === 'fulfilled') {
    okxContractBatchEnrichment.index_host = indexResult.value.host;
    for (const item of indexResult.value.payload.data) {
      const row = ensureSwap(swapIdForIndex(item?.instId));
      const value = finite(item?.idxPx);
      if (!row || value == null || value <= 0) continue;
      row.index_price = value;
      row.source_time = isoMs(item?.ts) || row.source_time;
      row.index_price_source = 'okx_public_index_tickers_usdt_batch';
      indexRows += 1;
    }
  } else {
    errors.push(`index:${String(indexResult.reason?.message || indexResult.reason || 'failed')}`);
  }

  if (oiResult.status === 'fulfilled') {
    okxContractBatchEnrichment.open_interest_host = oiResult.value.host;
    for (const item of oiResult.value.payload.data) {
      const row = ensureSwap(item?.instId);
      if (!row) continue;
      row.open_interest = finite(item?.oiCcy ?? item?.oi);
      row.open_interest_value = finite(item?.oiUsd);
      row.open_interest_unit = row.open_interest != null ? (item?.oiCcy != null ? 'base_asset' : 'contracts') : null;
      row.open_interest_value_unit = row.open_interest_value != null ? 'usd' : null;
      row.source_time = isoMs(item?.ts) || row.source_time;
      row.open_interest_source = 'okx_public_open_interest_batch';
      oiRows += 1;
    }
  } else {
    errors.push(`oi:${String(oiResult.reason?.message || oiResult.reason || 'failed')}`);
  }

  okxContractBatchEnrichment.mark_rows = markRows;
  okxContractBatchEnrichment.index_rows = indexRows;
  okxContractBatchEnrichment.open_interest_rows = oiRows;
  okxContractBatchEnrichment.patch_rows = patches.size;
  okxContractBatchEnrichment.last_completed_at = new Date().toISOString();
  okxContractBatchEnrichment.last_error = errors.join('|');
  if (markRows > 0 && indexRows > 0) okxContractBatchEnrichment.successes += 1;
  else okxContractBatchEnrichment.failures += 1;

  return [...patches.values()];
}

async function loadProviderRows(provider, market, observedAt) {
  if (provider === 'bitget') {
    try {
      return await loadBitgetV3Rows(market, observedAt);
    } catch (_) {
      return await loadMarketTickers(provider, market, []);
    }
  }
  const baseRows = await loadMarketTickers(provider, market, []);
  if (provider === 'okx' && market === 'contract') {
    const patches = await loadOkxContractBatchEnrichment(observedAt).catch(() => []);
    return mergeRowsByNative(baseRows, patches, provider, market, observedAt, 'USDT');
  }
  return baseRows;
}

function assertNoSeverePartialOverwrite(key, rows) {
  const previousRows = rowsByKey.get(key) || [];
  const directoryCount = Number(directoryCountByKey.get(key) || 0);
  const directoryFloor = directoryCount > 0 ? Math.floor(directoryCount * DIRECTORY_MIN_RATIO_AFTER_WARM) : 0;
  if (!previousRows.length) {
    // A side-by-side rollout must not publish a tiny first snapshot merely
    // because it has no earlier in-memory baseline yet. Once a verified
    // directory exists, require a meaningful fraction of that directory
    // before the first snapshot becomes visible.
    if (directoryCount >= 20 && rows.length < Math.max(1, directoryFloor)) {
      throw new Error(`market_light_initial_snapshot_too_partial:${rows.length}<${Math.max(1, directoryFloor)}`);
    }
    return;
  }
  const previousFloor = Math.floor(previousRows.length * PARTIAL_RETAIN_RATIO);
  const required = Math.max(1, previousFloor, directoryFloor);
  if (rows.length < required) {
    throw new Error(`market_light_partial_snapshot_rejected:${rows.length}<${required}`);
  }
}

async function ensureDirectory(provider, market) {
  const key = keyFor(market, provider);
  const rows = directoryRowsByKey.get(key);
  // Step980.6.3.4: Binance contract universe is already an in-process shared
  // WebSocket snapshot, so re-reading it here starts zero exchange requests.
  // Refresh this one cached directory every market-light cycle so a confirmed
  // stale-identity prune becomes visible within ~30s instead of waiting for the
  // generic 10-minute external-directory cadence used by other providers.
  const localBinanceContractDirectory = provider === 'binance' && market === 'contract';
  if (!localBinanceContractDirectory && Array.isArray(rows) && rows.length) return rows;
  const quote = PRIMARY_QUOTE[market]?.[provider];
  if (!quote) return [];
  try {
    const loaded = await getMarketUniverseRows(provider, market, quote);
    if (Array.isArray(loaded) && loaded.length) {
      directoryRowsByKey.set(key, loaded);
      directoryCountByKey.set(key, loaded.length);
      directoryUpdatedAtByKey.set(key, new Date().toISOString());
      return loaded;
    }
  } catch (_) {}
  return [];
}

async function buildProvider(provider, market, cycleRound) {
  totalBuilds += 1;
  if (provider === 'coinbase' && market === 'spot') {
    const rows = [...coinbase.rows.values()]
      .filter((row) => quoteAssetFor(row, 'USD') === 'USD')
      .sort((a, b) => a.symbol.localeCompare(b.symbol));
    if (!rows.length) {
      const current = metaByKey.get(keyFor(market, provider)) || {};
      metaByKey.set(keyFor(market, provider), {
        ...current,
        build_calls: Number(current.build_calls || 0) + 1,
        failed_builds: Number(current.failed_builds || 0) + 1,
        last_error: coinbase.lastError || 'coinbase_ticker_batch_rows_empty',
      });
      totalBuildFailures += 1;
      return false;
    }
    const observedAt = new Date().toISOString();
    const providerKey = keyFor(market, provider);
    try {
      assertNoSeverePartialOverwrite(providerKey, rows);
    } catch (error) {
      const current = metaByKey.get(providerKey) || {};
      metaByKey.set(providerKey, {
        ...current,
        build_calls: Number(current.build_calls || 0) + 1,
        failed_builds: Number(current.failed_builds || 0) + 1,
        last_error: String(error?.message || error).slice(0, 320),
      });
      totalBuildFailures += 1;
      return false;
    }
    rowsByKey.set(providerKey, rows.map((row) => ({
      ...row,
      shared_round: cycleRound,
      shared_observed_at: observedAt,
    })));
    const current = metaByKey.get(providerKey) || {};
    metaByKey.set(providerKey, {
      ...current,
      updated_at: observedAt,
      last_error: '',
      build_calls: Number(current.build_calls || 0) + 1,
      successful_builds: Number(current.successful_builds || 0) + 1,
    });
    return true;
  }

  const primaryQuote = PRIMARY_QUOTE[market]?.[provider];
  if (!primaryQuote) return false;
  await ensureDirectory(provider, market);
  const observedAt = new Date().toISOString();
  const key = keyFor(market, provider);
  const current = metaByKey.get(key) || {};
  try {
    const rawRows = await loadProviderRows(provider, market, observedAt);
    const rows = dedupeRows(provider, market, rawRows, observedAt, primaryQuote);
    if (!rows.length) throw new Error('market_light_rows_empty');
    assertNoSeverePartialOverwrite(key, rows);
    rowsByKey.set(key, rows.map((row) => ({
      ...row,
      shared_round: cycleRound,
      shared_observed_at: observedAt,
    })));
    metaByKey.set(key, {
      ...current,
      updated_at: observedAt,
      last_error: '',
      build_calls: Number(current.build_calls || 0) + 1,
      successful_builds: Number(current.successful_builds || 0) + 1,
    });
    return true;
  } catch (error) {
    totalBuildFailures += 1;
    metaByKey.set(key, {
      ...current,
      build_calls: Number(current.build_calls || 0) + 1,
      failed_builds: Number(current.failed_builds || 0) + 1,
      last_error: String(error?.message || error).slice(0, 320),
    });
    return false;
  }
}

async function refreshDirectoryCounts() {
  if (directoryRunning) return false;
  directoryRunning = true;
  try {
    const targets = [
      ...SPOT_PROVIDERS.map((provider) => ({ provider, market: 'spot' })),
      ...CONTRACT_PROVIDERS.map((provider) => ({ provider, market: 'contract' })),
    ];
    await mapLimit(targets, 2, async ({ provider, market }) => {
      const quote = PRIMARY_QUOTE[market]?.[provider];
      if (!quote) return;
      try {
        const rows = await getMarketUniverseRows(provider, market, quote);
        if (Array.isArray(rows) && rows.length) {
          directoryRowsByKey.set(keyFor(market, provider), rows);
          directoryCountByKey.set(keyFor(market, provider), rows.length);
          directoryUpdatedAtByKey.set(keyFor(market, provider), new Date().toISOString());
        }
      } catch (_) {
        // Directory failure must not clear last verified count.
      }
    });
    return true;
  } finally {
    directoryRunning = false;
  }
}

async function resolveWebSocketCtor() {
  if (!wsCtorPromise) {
    wsCtorPromise = (async () => {
      if (typeof globalThis.WebSocket === 'function') return globalThis.WebSocket;
      const imported = await import('ws');
      return imported.WebSocket || imported.default;
    })();
  }
  return await wsCtorPromise;
}

function wsListen(socket, eventName, handler) {
  if (typeof socket?.addEventListener === 'function') {
    socket.addEventListener(eventName, handler);
    return;
  }
  if (typeof socket?.on === 'function') {
    socket.on(eventName, handler);
    return;
  }
  socket[`on${eventName}`] = handler;
}

function wsReady(socket) {
  return socket && Number(socket.readyState) === 1;
}

function closeWs(socket) {
  try {
    if (typeof socket?.terminate === 'function') socket.terminate();
    else socket?.close?.();
  } catch (_) {}
}

function sendWs(socket, payload) {
  if (!wsReady(socket)) return false;
  try {
    socket.send(JSON.stringify(payload));
    return true;
  } catch (_) {
    return false;
  }
}

async function wsMessageText(eventOrData) {
  const value = eventOrData && typeof eventOrData === 'object' && 'data' in eventOrData
    ? eventOrData.data
    : eventOrData;
  if (typeof value === 'string') return value;
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  if (value instanceof ArrayBuffer) return Buffer.from(value).toString('utf8');
  if (ArrayBuffer.isView(value)) return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString('utf8');
  if (value && typeof value.text === 'function') return await value.text();
  return String(value ?? '');
}

function coinbaseProductIdFromUniverse(row) {
  const raw = String(row?.raw_symbol || '').trim().toUpperCase();
  if (raw.includes('-')) return raw;
  const base = compact(row?.base_asset);
  const quote = compact(row?.quote_asset);
  return base && quote ? `${base}-${quote}` : '';
}

async function refreshCoinbaseUniverse() {
  const rows = await getMarketUniverseRows('coinbase', 'spot', 'USD');
  const ids = [...new Set((Array.isArray(rows) ? rows : [])
    .map(coinbaseProductIdFromUniverse)
    .filter(Boolean))]
    .slice(0, COINBASE_MAX_PRODUCT_IDS);
  if (!ids.length) throw new Error('coinbase_usd_universe_empty');
  const changed = ids.length !== coinbase.productIds.length || ids.some((id, index) => coinbase.productIds[index] !== id);
  coinbase.productIds = ids;
  coinbase.universeUpdatedAt = Date.now();
  directoryCountByKey.set(keyFor('spot', 'coinbase'), ids.length);
  directoryUpdatedAtByKey.set(keyFor('spot', 'coinbase'), new Date().toISOString());
  return changed;
}

function coinbaseTickerRow(ticker, messageTime) {
  const productId = String(ticker?.product_id || '').trim().toUpperCase();
  const parts = productId.split('-');
  if (parts.length < 2) return null;
  const quote = compact(parts.at(-1));
  if (quote !== 'USD') return null;
  const base = compact(parts.slice(0, -1).join(''));
  const symbol = compact(`${base}${quote}`);
  const last = positive(ticker?.price);
  if (!symbol || !base || last == null) return null;
  const volume = finite(ticker?.volume_24_h ?? ticker?.volume_24h);
  const sourceTime = isoMs(messageTime) || new Date().toISOString();
  return {
    provider: 'coinbase',
    market_type: 'spot',
    symbol,
    raw_symbol: productId,
    native_symbol: productId,
    base_asset: base,
    quote_asset: 'USD',
    quote_symbol: 'USD',
    last_price: last,
    price: last,
    price_change_percent_24h: finite(ticker?.price_percent_chg_24_h ?? ticker?.price_percent_chg_24h),
    base_volume_24h: volume,
    volume_24h: volume,
    quote_volume_24h: volume != null ? volume * last : null,
    high_24h: finite(ticker?.high_24_h ?? ticker?.high_24h),
    low_24h: finite(ticker?.low_24_h ?? ticker?.low_24h),
    best_bid: null,
    best_ask: null,
    bid_price: null,
    ask_price: null,
    spread_percent: null,
    source: 'coinbase_advanced_trade_public_ticker_batch_websocket',
    transport: 'websocket_ticker_batch_5s',
    source_time: sourceTime,
    cached_at: sourceTime,
    backend_shared: true,
    market_light_scope: 'primary_quote_full_directory',
    bbo_available_in_source: false,
  };
}

function scheduleCoinbaseReconnect() {
  if (coinbase.reconnectTimer) return;
  const delay = Math.min(
    COINBASE_RECONNECT_MAX_MS,
    COINBASE_RECONNECT_MIN_MS * (2 ** Math.min(coinbase.reconnectAttempt, 5)),
  );
  coinbase.reconnectAttempt += 1;
  coinbase.reconnectTimer = setTimeout(() => {
    coinbase.reconnectTimer = null;
    ensureCoinbaseTickerBatch().catch(() => {});
  }, delay);
  coinbase.reconnectTimer.unref?.();
}

async function subscribeCoinbase(socket) {
  sendWs(socket, { type: 'subscribe', channel: 'heartbeats' });
  coinbase.subscribeMessages += 1;
  for (let start = 0; start < coinbase.productIds.length; start += COINBASE_SUBSCRIBE_CHUNK) {
    const productIds = coinbase.productIds.slice(start, start + COINBASE_SUBSCRIBE_CHUNK);
    if (!sendWs(socket, { type: 'subscribe', product_ids: productIds, channel: 'ticker_batch' })) {
      throw new Error('coinbase_ticker_batch_subscribe_failed');
    }
    coinbase.subscribeMessages += 1;
    if (start + COINBASE_SUBSCRIBE_CHUNK < coinbase.productIds.length) {
      await new Promise((resolve) => {
        const timer = setTimeout(resolve, COINBASE_SUBSCRIBE_GAP_MS);
        timer.unref?.();
      });
    }
  }
}

async function openCoinbaseTickerBatch() {
  const WebSocketCtor = await resolveWebSocketCtor();
  const changed = await refreshCoinbaseUniverse();
  if (!coinbase.productIds.length) throw new Error('coinbase_ticker_batch_no_products');
  if (changed) {
    const allowed = new Set(coinbase.productIds.map((id) => compact(id)));
    for (const symbol of [...coinbase.rows.keys()]) {
      if (!allowed.has(symbol)) coinbase.rows.delete(symbol);
    }
  }
  coinbase.connectAttempts += 1;
  const socket = new WebSocketCtor(COINBASE_WS_URL);
  coinbase.socket = socket;
  coinbase.ready = false;
  coinbase.lastError = '';
  return await new Promise((resolve, reject) => {
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      closeWs(socket);
      reject(new Error('coinbase_ticker_batch_connect_timeout'));
    }, 10_000);
    timeout.unref?.();

    wsListen(socket, 'open', async () => {
      if (settled) return;
      try {
        coinbase.openedAt = Date.now();
        coinbase.reconnectAttempt = 0;
        await subscribeCoinbase(socket);
        coinbase.ready = true;
        settled = true;
        clearTimeout(timeout);
        resolve(true);
      } catch (error) {
        settled = true;
        clearTimeout(timeout);
        coinbase.lastError = String(error?.message || error).slice(0, 320);
        closeWs(socket);
        reject(error);
      }
    });

    wsListen(socket, 'message', async (event) => {
      try {
        const text = await wsMessageText(event);
        const decoded = JSON.parse(text);
        coinbase.messages += 1;
        coinbase.lastMessageAt = Date.now();
        if (decoded?.channel === 'heartbeats') {
          coinbase.lastHeartbeatAt = Date.now();
          return;
        }
        if (decoded?.channel !== 'ticker_batch' && decoded?.channel !== 'ticker') return;
        const messageTime = decoded?.timestamp || Date.now();
        for (const eventRow of Array.isArray(decoded?.events) ? decoded.events : []) {
          for (const ticker of Array.isArray(eventRow?.tickers) ? eventRow.tickers : []) {
            const row = coinbaseTickerRow(ticker, messageTime);
            if (!row) continue;
            coinbase.rows.set(row.symbol, row);
            coinbase.tickerUpdates += 1;
          }
        }
      } catch (error) {
        coinbase.lastError = String(error?.message || error).slice(0, 320);
      }
    });

    wsListen(socket, 'error', (error) => {
      coinbase.lastError = String(error?.message || error || 'coinbase_ticker_batch_socket_error').slice(0, 320);
      if (!settled) {
        settled = true;
        clearTimeout(timeout);
        reject(error instanceof Error ? error : new Error('coinbase_ticker_batch_socket_error'));
      }
    });

    wsListen(socket, 'close', () => {
      coinbase.ready = false;
      if (coinbase.socket === socket) coinbase.socket = null;
      if (!settled) {
        settled = true;
        clearTimeout(timeout);
        reject(new Error('coinbase_ticker_batch_closed_before_open'));
      }
      scheduleCoinbaseReconnect();
    });
  });
}

async function ensureCoinbaseTickerBatch() {
  if (wsReady(coinbase.socket) && coinbase.ready) {
    if (Date.now() - coinbase.universeUpdatedAt > DIRECTORY_INTERVAL_MS) {
      const changed = await refreshCoinbaseUniverse().catch(() => false);
      if (changed) {
        closeWs(coinbase.socket);
        return false;
      }
    }
    return true;
  }
  if (coinbase.connecting) return await coinbase.connecting;
  coinbase.connecting = openCoinbaseTickerBatch()
    .catch((error) => {
      coinbase.lastError = String(error?.message || error).slice(0, 320);
      scheduleCoinbaseReconnect();
      return false;
    })
    .finally(() => {
      coinbase.connecting = null;
    });
  return await coinbase.connecting;
}

export async function runMarketLightSnapshotCycle({ reason = 'scheduled' } = {}) {
  if (running) return false;
  running = true;
  lastStartedAt = new Date().toISOString();
  lastError = '';
  const cycleRound = round + 1;
  try {
    ensureCoinbaseTickerBatch().catch(() => {});
    const targets = [
      ...SPOT_PROVIDERS.map((provider) => ({ provider, market: 'spot' })),
      ...CONTRACT_PROVIDERS.map((provider) => ({ provider, market: 'contract' })),
    ];
    const results = await mapLimit(targets, BUILD_CONCURRENCY, async ({ provider, market }) => ({
      provider,
      market,
      ok: await buildProvider(provider, market, cycleRound),
    }));
    const successful = results.filter((item) => item?.ok).length;
    if (!successful) lastError = `market_light_all_builds_failed:${reason}`;
    round = cycleRound;
    lastCompletedAt = new Date().toISOString();
    responseCache.clear();
    return successful > 0;
  } catch (error) {
    lastError = `${reason}:${String(error?.message || error)}`.slice(0, 320);
    return false;
  } finally {
    running = false;
  }
}

export function startMarketLightSnapshotScanner() {
  if (started || process.env.KAKA_DISABLE_MARKET_LIGHT_SCANNER === '1') return;
  started = true;
  ensureCoinbaseTickerBatch().catch(() => {});
  refreshDirectoryCounts().catch(() => {});
  scanTimer = setTimeout(() => {
    runMarketLightSnapshotCycle({ reason: 'startup' }).catch(() => {});
  }, START_DELAY_MS);
  scanTimer.unref?.();
  scanInterval = setInterval(() => {
    runMarketLightSnapshotCycle({ reason: 'interval' }).catch(() => {});
  }, SCAN_INTERVAL_MS);
  scanInterval.unref?.();
  directoryInterval = setInterval(() => {
    refreshDirectoryCounts().catch(() => {});
    ensureCoinbaseTickerBatch().catch(() => {});
  }, DIRECTORY_INTERVAL_MS);
  directoryInterval.unref?.();
}

function snapshotPayload({ market = '', provider = '', includeRows = true, offset = 0, limit = null } = {}) {
  const marketFilter = String(market || '').trim().toLowerCase();
  const providerFilter = String(provider || '').trim().toLowerCase();
  const safeOffset = Math.max(0, Math.trunc(Number(offset) || 0));
  const safeLimit = limit == null || limit === '' ? null : Math.max(1, Math.min(5000, Math.trunc(Number(limit) || 0)));
  const cacheKey = `${marketFilter || 'all'}|${providerFilter || 'all'}|${includeRows ? 1 : 0}|${safeOffset}|${safeLimit ?? 'all'}`;
  const cached = responseCache.get(cacheKey);
  if (cached && Date.now() - cached.at <= SNAPSHOT_CACHE_TTL_MS) {
    responseCacheHits += 1;
    return { ...cached.payload, cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }
  responseCacheMisses += 1;

  const markets = marketFilter === 'spot'
    ? ['spot']
    : marketFilter === 'contract'
      ? ['contract']
      : ['spot', 'contract'];
  const rows = [];
  const coverage = {};
  for (const marketName of markets) {
    const providers = marketName === 'spot' ? SPOT_PROVIDERS : CONTRACT_PROVIDERS;
    for (const providerName of providers) {
      if (providerFilter && providerFilter !== providerName) continue;
      const key = keyFor(marketName, providerName);
      rows.push(...(rowsByKey.get(key) || []).map((row) => ({ ...row })));
      coverage[key] = providerMeta(providerName, marketName);
    }
  }
  rows.sort((a, b) => {
    const marketOrder = String(a.market_type).localeCompare(String(b.market_type));
    if (marketOrder !== 0) return marketOrder;
    const providerOrder = String(a.provider).localeCompare(String(b.provider));
    if (providerOrder !== 0) return providerOrder;
    return String(a.symbol).localeCompare(String(b.symbol));
  });
  const totalRowCount = rows.length;
  const selectedRows = includeRows
    ? rows.slice(safeOffset, safeLimit == null ? undefined : safeOffset + safeLimit)
    : [];
  const payload = {
    ok: true,
    version: STEP_VERSION,
    source: 'render_shared_primary_quote_full_directory_market_light_snapshot',
    scope: 'primary_quote_full_directory',
    primary_quote_by_market_provider: PRIMARY_QUOTE,
    market_type: marketFilter || 'all',
    provider: providerFilter || null,
    row_count: totalRowCount,
    total_row_count: totalRowCount,
    returned_row_count: selectedRows.length,
    rows_included: Boolean(includeRows),
    offset: safeOffset,
    limit: safeLimit,
    has_more: includeRows && safeOffset + selectedRows.length < totalRowCount,
    provider_coverage: coverage,
    shared_round: round,
    scan_interval_seconds: Math.round(SCAN_INTERVAL_MS / 1000),
    stale_seconds: Math.round(STALE_MS / 1000),
    exchange_requests_started: 0,
    exchange_connections_started: 0,
    reads_scale_with_users: false,
    failed_refresh_never_overwrites_last_verified_rows: true,
    severe_partial_refresh_never_overwrites_last_verified_rows: true,
    rows: selectedRows,
    timestamp_ms: Date.now(),
  };
  responseCache.set(cacheKey, { at: Date.now(), payload });
  return { ...payload, cache_hit: false, cache_age_ms: 0 };
}


export function getMarketLightInternalSnapshot({ market = '', provider = '' } = {}) {
  const normalizedMarket = String(market || '').trim().toLowerCase();
  const normalizedProvider = String(provider || '').trim().toLowerCase();
  if (!['spot', 'contract'].includes(normalizedMarket)) {
    return { ok: false, version: STEP_VERSION, error: 'unsupported_market_type', market_type: normalizedMarket, provider: normalizedProvider, rows: [] };
  }
  const allowed = normalizedMarket === 'spot' ? SPOT_PROVIDERS : CONTRACT_PROVIDERS;
  if (!allowed.includes(normalizedProvider)) {
    return { ok: false, version: STEP_VERSION, error: 'unsupported_provider', market_type: normalizedMarket, provider: normalizedProvider, rows: [] };
  }
  const key = keyFor(normalizedMarket, normalizedProvider);
  const meta = providerMeta(normalizedProvider, normalizedMarket);
  const rows = rowsByKey.get(key) || [];
  return {
    ok: true,
    version: STEP_VERSION,
    market_type: normalizedMarket,
    provider: normalizedProvider,
    primary_quote: PRIMARY_QUOTE[normalizedMarket]?.[normalizedProvider] || null,
    row_count: rows.length,
    directory_count: Number(directoryCountByKey.get(key) || 0),
    stale: Boolean(meta.stale),
    updated_at: meta.updated_at || null,
    last_error: meta.last_error || '',
    shared_round: round,
    exchange_requests_started: 0,
    exchange_connections_started: 0,
    reads_scale_with_users: false,
    rows: rows.map((row) => ({ ...row })),
  };
}

export function getMarketLightSnapshotHealth() {
  const providerCoverage = {};
  for (const provider of SPOT_PROVIDERS) providerCoverage[keyFor('spot', provider)] = providerMeta(provider, 'spot');
  for (const provider of CONTRACT_PROVIDERS) providerCoverage[keyFor('contract', provider)] = providerMeta(provider, 'contract');
  return {
    ok: true,
    version: STEP_VERSION,
    enabled: started || process.env.KAKA_DISABLE_MARKET_LIGHT_SCANNER !== '1',
    mode: 'shared_primary_quote_full_directory_light_snapshot',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    spot_providers: SPOT_PROVIDERS,
    contract_providers: CONTRACT_PROVIDERS,
    primary_quote_by_market_provider: PRIMARY_QUOTE,
    scan_interval_seconds: Math.round(SCAN_INTERVAL_MS / 1000),
    directory_refresh_minutes: DIRECTORY_INTERVAL_MS / 60_000,
    stale_seconds: Math.round(STALE_MS / 1000),
    build_concurrency: BUILD_CONCURRENCY,
    running,
    round,
    last_started_at: lastStartedAt,
    last_completed_at: lastCompletedAt,
    last_error: lastError,
    total_builds: totalBuilds,
    total_build_failures: totalBuildFailures,
    total_snapshot_reads: totalSnapshotReads,
    response_cache_ttl_seconds: Math.round(SNAPSHOT_CACHE_TTL_MS / 1000),
    response_cache_entries: responseCache.size,
    response_cache_hits: responseCacheHits,
    response_cache_misses: responseCacheMisses,
    snapshot_reads_start_exchange_requests: false,
    snapshot_reads_start_exchange_connections: false,
    snapshot_reads_scale_with_users: false,
    failed_refresh_never_overwrites_last_verified_rows: true,
    severe_partial_refresh_never_overwrites_last_verified_rows: true,
    partial_retain_ratio: PARTIAL_RETAIN_RATIO,
    directory_min_ratio_after_warm: DIRECTORY_MIN_RATIO_AFTER_WARM,
    designed_upstream_budget: {
      scan_interval_seconds: Math.round(SCAN_INTERVAL_MS / 1000),
      approximate_batch_attempts_per_cycle: 11,
      approximate_batch_attempts_per_minute_at_default_interval: Math.round((60_000 / SCAN_INTERVAL_MS) * 11 * 10) / 10,
      coinbase_shared_market_ws_connections: 1,
      per_user_upstream_requests: 0,
      per_user_upstream_connections: 0,
      note: 'collector budget only; shared caches/governors may reduce physical upstream calls further',
    },
    full_market_light_source_notes: {
      binance_spot: 'official_all_24h_tickers_batch',
      binance_contract: 'existing_all_market_ticker_plus_mark_price_shared_snapshot',
      coinbase_spot: 'public_ticker_batch_shared_websocket; BBO intentionally unavailable in ticker_batch',
      okx_spot: 'official_SPOT_tickers_batch',
      okx_contract: 'official_SWAP_tickers_batch + dual-host public mark-price batch + USDT index-tickers batch + public open-interest batch; funding remains missing unless officially supplied by a batch source',
      bybit_spot: 'official_spot_tickers_batch',
      bybit_contract: 'official_linear_tickers_batch including mark/index/OI/funding/BBO',
      bitget_spot: 'official_v3_SPOT_tickers_product_batch with v2 fallback',
      bitget_contract: 'official_v3_USDT-FUTURES_tickers product batch including mark/index/OI/funding/BBO with v2 fallback',
      gate_spot: 'official_spot_tickers_batch',
      gate_contract: 'official_USDT_futures_tickers_batch including mark/index/funding/BBO; total_size preserved separately and not relabeled as OI',
    },
    provider_coverage: providerCoverage,
    okx_contract_batch_enrichment: {
      ...okxContractBatchEnrichment,
      mark_ready: okxContractBatchEnrichment.mark_rows > 0,
      index_ready: okxContractBatchEnrichment.index_rows > 0,
      open_interest_ready: okxContractBatchEnrichment.open_interest_rows > 0,
      dual_host_fallback_enabled: true,
      index_batch_mode: 'quoteCcy=USDT',
      mark_batch_mode: 'instType=SWAP',
      additional_user_scaled_requests: 0,
      additional_user_scaled_connections: 0,
    },
    coinbase_ticker_batch: {
      source: 'coinbase_advanced_trade_public_ticker_batch_websocket',
      url: COINBASE_WS_URL,
      connected: wsReady(coinbase.socket) && coinbase.ready,
      product_ids: coinbase.productIds.length,
      cached_rows: coinbase.rows.size,
      subscribe_chunk: COINBASE_SUBSCRIBE_CHUNK,
      subscribe_gap_ms: COINBASE_SUBSCRIBE_GAP_MS,
      connect_attempts: coinbase.connectAttempts,
      subscribe_messages: coinbase.subscribeMessages,
      messages: coinbase.messages,
      ticker_updates: coinbase.tickerUpdates,
      opened_at: coinbase.openedAt ? new Date(coinbase.openedAt).toISOString() : null,
      last_message_at: coinbase.lastMessageAt ? new Date(coinbase.lastMessageAt).toISOString() : null,
      last_heartbeat_at: coinbase.lastHeartbeatAt ? new Date(coinbase.lastHeartbeatAt).toISOString() : null,
      last_error: coinbase.lastError,
      best_bid_ask_in_ticker_batch: false,
      per_user_connections: 0,
    },
    time: new Date().toISOString(),
  };
}

function sendJson(res, status, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, OPTIONS',
    'content-length': String(body.length),
  });
  res.end(body);
}

export async function handleMarketLightSnapshot(req, res, url) {
  if (![SNAPSHOT_ROUTE, HEALTH_ROUTE].includes(url.pathname)) return false;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, OPTIONS',
      'cache-control': 'no-store',
    });
    res.end();
    return true;
  }
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: STEP_VERSION, error: 'method_not_allowed' });
    return true;
  }
  if (url.pathname === HEALTH_ROUTE) {
    sendJson(res, 200, getMarketLightSnapshotHealth());
    return true;
  }
  const market = String(url.searchParams.get('market_type') || url.searchParams.get('market') || '').trim().toLowerCase();
  const provider = String(url.searchParams.get('provider') || '').trim().toLowerCase();
  if (market && !['spot', 'contract'].includes(market)) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'unsupported_market_type' });
    return true;
  }
  const allowedProviders = market === 'contract' ? CONTRACT_PROVIDERS : market === 'spot' ? SPOT_PROVIDERS : [...new Set([...SPOT_PROVIDERS, ...CONTRACT_PROVIDERS])];
  if (provider && !allowedProviders.includes(provider)) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'unsupported_provider' });
    return true;
  }
  const includeRowsRaw = String(url.searchParams.get('include_rows') ?? '1').trim().toLowerCase();
  const includeRows = !['0', 'false', 'no', 'off'].includes(includeRowsRaw);
  const offset = Math.max(0, Math.trunc(Number(url.searchParams.get('offset') || 0) || 0));
  const limitRaw = url.searchParams.get('limit');
  const limit = limitRaw == null || limitRaw === '' ? null : Math.max(1, Math.min(5000, Math.trunc(Number(limitRaw) || 0)));
  totalSnapshotReads += 1;
  sendJson(res, 200, snapshotPayload({ market, provider, includeRows, offset, limit }));
  return true;
}

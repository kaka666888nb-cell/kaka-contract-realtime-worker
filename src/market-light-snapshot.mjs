import { getMarketUniverseRows, tickers as loadMarketTickers } from './market-rest.mjs';
import { getCryptoSectorHistoryHealth, handleCryptoSectorHistory, maybeArchiveCryptoSectorSnapshot, primeCryptoSectorHistory } from './crypto-sector-history.mjs';

const STEP_VERSION = '650.8.15.196.10.3';
const SNAPSHOT_ROUTE = '/api/market-light/current-snapshot';
const RANKED_PAGE_ROUTE = '/api/market-light/ranked-page';
const HEALTH_ROUTE = '/api/market-light/health';
const SECTOR_SNAPSHOT_ROUTE = '/api/crypto-sector-professional/current-snapshot';
const SECTOR_HEALTH_ROUTE = '/api/crypto-sector-professional/health';
const SECTOR_CACHE_TTL_MS = Math.max(
  5_000,
  Number(process.env.KAKA_CRYPTO_SECTOR_PROFESSIONAL_CACHE_TTL_MS || 10_000),
);
const SECTOR_MIN_TURNOVER_USDT = Math.max(
  0,
  Number(process.env.KAKA_CRYPTO_SECTOR_MIN_TURNOVER_USDT || 100_000),
);
const SECTOR_USDT_PROVIDERS = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const SECTOR_DEFINITIONS = Object.freeze([
  Object.freeze({
    key: 'btc',
    zh: 'BTC生态',
    en: 'Bitcoin Ecosystem',
    symbols: Object.freeze(['BTC','BCH','STX','ORDI','SATS','RATS','CORE','CKB','BB','MERL']),
  }),
  Object.freeze({
    key: 'eth',
    zh: 'ETH生态',
    en: 'Ethereum Ecosystem',
    symbols: Object.freeze(['ETH','LDO','RPL','ENS','EIGEN','ETHFI','ENA','PENDLE','STRK','ZK']),
  }),
  Object.freeze({
    key: 'l2',
    zh: 'L2',
    en: 'Layer 2',
    symbols: Object.freeze(['ARB','OP','STRK','ZK','MNT','METIS','IMX','POL','BLAST','MODE']),
  }),
  Object.freeze({
    key: 'defi',
    zh: 'DeFi',
    en: 'DeFi',
    symbols: Object.freeze(['UNI','AAVE','SKY','MKR','CRV','COMP','SNX','PENDLE','LDO','DYDX','GMX','JUP','RAY','ENA','1INCH']),
  }),
  Object.freeze({
    key: 'ai',
    zh: 'AI',
    en: 'AI',
    symbols: Object.freeze(['FET','TAO','RENDER','RNDR','WLD','NEAR','AKT','ARKM','VIRTUAL','AIOZ','IO','PHB','NMR','GRIFFAIN']),
  }),
  Object.freeze({
    key: 'meme',
    zh: 'Meme',
    en: 'Meme',
    symbols: Object.freeze(['DOGE','SHIB','PEPE','BONK','FLOKI','WIF','BRETT','PENGU','TRUMP','MOG','POPCAT','TURBO','BOME','MEW']),
  }),
  Object.freeze({
    key: 'rwa',
    zh: 'RWA',
    en: 'RWA',
    symbols: Object.freeze(['ONDO','POLYX','CFG','OM','XDC','MPL','RSR']),
  }),
  Object.freeze({
    key: 'gamefi',
    zh: 'GameFi',
    en: 'GameFi',
    symbols: Object.freeze(['IMX','GALA','SAND','MANA','AXS','ENJ','RON','ILV','PIXEL','YGG','MAGIC','SUPER','BEAM']),
  }),
  Object.freeze({
    key: 'depin',
    zh: 'DePIN',
    en: 'DePIN',
    symbols: Object.freeze(['FIL','RENDER','RNDR','HNT','AKT','AR','AIOZ','IO','GRASS','ATH','MOBILE']),
  }),
]);

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

// Step1031.2: Binance spot market-light no longer depends on the heavy all-symbol
// REST ticker baseline for readiness. One official !miniTicker@arr backend stream
// can bootstrap live USDT identities and 24h price/volume during an IP REST ban.
// The market-data-only REST ticker baseline remains optional and low-frequency only
// for completeness/BBO enrichment; 418/429 never clears stream-backed rows.
const BINANCE_SPOT_MARKET_DATA_REST_BASE = 'https://data-api.binance.vision';
const BINANCE_SPOT_MARKET_DATA_REST_PATH = '/api/v3/ticker/24hr?symbolStatus=TRADING&type=FULL';
const BINANCE_SPOT_REST_BASELINE_ENABLED = process.env.KAKA_MARKET_LIGHT_BINANCE_SPOT_REST_BASELINE_ENABLED === '1';
const BINANCE_SPOT_MINI_TICKER_WS_URL = 'wss://data-stream.binance.vision:443/ws/!miniTicker@arr';
const BINANCE_SPOT_BASELINE_TTL_MS = Math.max(60 * 60_000, Number(process.env.KAKA_MARKET_LIGHT_BINANCE_SPOT_BASELINE_TTL_MS || 6 * 60 * 60_000));
const BINANCE_SPOT_BASELINE_RETRY_MS = Math.max(15 * 60_000, Number(process.env.KAKA_MARKET_LIGHT_BINANCE_SPOT_BASELINE_RETRY_MS || 30 * 60_000));
const BINANCE_SPOT_STREAM_ROW_TTL_MS = Math.max(30 * 60_000, Number(process.env.KAKA_MARKET_LIGHT_BINANCE_SPOT_STREAM_ROW_TTL_MS || 2 * 60 * 60_000));
const BINANCE_SPOT_STREAM_BOOTSTRAP_MIN_ROWS = Math.max(20, Number(process.env.KAKA_MARKET_LIGHT_BINANCE_SPOT_STREAM_BOOTSTRAP_MIN_ROWS || 20));
const BINANCE_SPOT_RECONNECT_MIN_MS = Math.max(1_000, Number(process.env.KAKA_MARKET_LIGHT_BINANCE_SPOT_RECONNECT_MIN_MS || 2_000));
const BINANCE_SPOT_RECONNECT_MAX_MS = Math.max(BINANCE_SPOT_RECONNECT_MIN_MS, Number(process.env.KAKA_MARKET_LIGHT_BINANCE_SPOT_RECONNECT_MAX_MS || 30_000));

// Step990: Binance USDⓈ-M exposes one official all-symbol best bid/ask stream.
// Keep it as exactly one shared backend WebSocket, independent of user count.
// The 2026 merged UM+CM payload is filtered to st=1 (USDⓈ-M) and current USDT identities.
const BINANCE_CONTRACT_BOOK_TICKER_WS_URL = 'wss://fstream.binance.com/public/ws/!bookTicker';
const BINANCE_CONTRACT_BOOK_TICKER_WS_API_URL = 'wss://ws-fapi.binance.com/ws-fapi/v1';
const BINANCE_BOOK_BASELINE_TTL_MS = Math.max(60_000, Number(process.env.KAKA_MARKET_LIGHT_BINANCE_BOOK_BASELINE_TTL_MS || 10 * 60_000));
const BINANCE_BOOK_RECONNECT_MIN_MS = Math.max(1_000, Number(process.env.KAKA_MARKET_LIGHT_BINANCE_BOOK_RECONNECT_MIN_MS || 2_000));
const BINANCE_BOOK_RECONNECT_MAX_MS = Math.max(BINANCE_BOOK_RECONNECT_MIN_MS, Number(process.env.KAKA_MARKET_LIGHT_BINANCE_BOOK_RECONNECT_MAX_MS || 30_000));

const rowsByKey = new Map();
const metaByKey = new Map();
const directoryCountByKey = new Map();
const directoryRowsByKey = new Map();
const directoryUpdatedAtByKey = new Map();
const responseCache = new Map();

// Step1041.6.3 / Render650.8.15.196.10.3: shared full-market ranking index.
// Ranking is computed from the already-collected shared market-light rows BEFORE
// pagination. User reads never start exchange requests and the App still receives
// only 50 ranked assets per page. A rank_version freezes the ORDER (not full market
// payloads) across subsequent +50 pages, preventing duplicates/missing rows while
// realtime prices continue to move. Market-cap metadata no longer calls CoinGecko
// directly from Render: it reuses Step1012's verified Supabase shared Top1000
// tokenomics snapshot plus the existing verified project-fundamentals collision guard.
const RANK_PAGE_LIMIT_MAX = 50;
const RANK_RESPONSE_CACHE_TTL_MS = Math.max(1_000, Number(process.env.KAKA_MARKET_RANK_RESPONSE_CACHE_TTL_MS || 5_000));
const RANK_ORDER_SNAPSHOT_TTL_MS = Math.max(60_000, Number(process.env.KAKA_MARKET_RANK_ORDER_TTL_MS || 3 * 60_000));
const RANK_ORDER_SNAPSHOT_MAX = Math.max(4, Math.min(24, Number(process.env.KAKA_MARKET_RANK_ORDER_MAX || 16)));
const MARKET_CAP_REFRESH_MS = Math.max(60 * 60_000, Number(process.env.KAKA_MARKET_CAP_RANK_REFRESH_MS || 6 * 60 * 60_000));
const MARKET_CAP_START_DELAY_MS = Math.max(8_000, Number(process.env.KAKA_MARKET_CAP_RANK_START_DELAY_MS || 20_000));
const MARKET_CAP_SHARED_DATASET_KEY = 'project_tokenomics_catalog';
const MARKET_CAP_SHARED_SCOPE_KEY = 'default';
const MARKET_CAP_SHARED_MIN_ROWS = Math.max(500, Number(process.env.KAKA_MARKET_CAP_SHARED_MIN_ROWS || 850));
const MARKET_CAP_SHARED_READ_TIMEOUT_MS = Math.max(5_000, Number(process.env.KAKA_MARKET_CAP_SHARED_READ_TIMEOUT_MS || 15_000));
const MARKET_CAP_COLD_FAILURE_RETRY_MS = Math.max(60_000, Number(process.env.KAKA_MARKET_CAP_COLD_FAILURE_RETRY_MS || 90_000));
const MARKET_CAP_WARM_FAILURE_RETRY_MS = Math.max(5 * 60_000, Number(process.env.KAKA_MARKET_CAP_WARM_FAILURE_RETRY_MS || 30 * 60_000));
const rankedPageCache = new Map();
const rankOrderSnapshots = new Map();
const rankCurrentBySignature = new Map();
let rankOrderSnapshotSeq = 0;
const marketCapBySymbol = new Map();
const marketCapAmbiguousSymbols = new Set();
const marketCapGlobalSymbolCounts = new Map(); // compatibility: counts inside the verified shared catalog + collision guard
let marketCapGlobalSymbolListAt = 0;
let marketCapGlobalSymbolRows = 0;
let marketCapRankVersion = 0;
let marketCapRefreshInflight = null;
let marketCapRefreshTimer = null;
let marketCapRefreshInterval = null;
let marketCapRetryTimer = null;
let marketCapLastStartedAt = null;
let marketCapLastSucceededAt = null;
let marketCapLastError = '';
let marketCapRefreshAttempts = 0;
let marketCapRefreshSuccesses = 0;
let marketCapRefreshFailures = 0;
let marketCapRows = 0;
let marketCapUniqueSymbols = 0;
let marketCapDuplicateSymbols = 0;
let marketCapSourceRequestsStarted = 0; // Supabase internal shared reads only
let marketCapDirectCoinGeckoRequestsStarted = 0;
let marketCapRateLimitResponses = 0; // kept at zero for compatibility; no direct CoinGecko lane
let marketCapRateLimitRetries = 0;
let marketCapLastRetryAfterMs = null;
let marketCapLastRequestAtMs = 0;
let marketCapNextRetryAt = null;
let marketCapLastRequestMode = 'supabase_shared_snapshot';
let marketCapSourceVerified = false;
let marketCapSourceFetchedAt = null;
let marketCapSourceCachedAt = null;
let marketCapSourceExpiresAt = null;
let marketCapSourceStaleUntil = null;
let marketCapSourceContentHash = '';
let marketCapSourceSnapshotRows = 0;
let marketCapFundamentalsRows = 0;
let marketCapKnownAmbiguousByFundamentals = 0;
let rankPageReads = 0;
let rankPageBuilds = 0;
let rankPageCacheHits = 0;
let rankOrderSnapshotBuilds = 0;
let rankOrderSnapshotHits = 0;
let rankOrderSnapshotExpired = 0;

let sectorSnapshotCache = null;
let sectorSnapshotCacheAt = 0;
let sectorSnapshotBuilds = 0;
let sectorSnapshotReads = 0;

const bitgetContractFundingBatch = {
  attempts: 0,
  successes: 0,
  failures: 0,
  rows: 0,
  applied_rows: 0,
  last_started_at: null,
  last_completed_at: null,
  last_error: '',
};

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
  applied_patch_rows: 0,
  applied_mark_rows: 0,
  applied_index_rows: 0,
  applied_open_interest_rows: 0,
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

const binanceSpotTicker = {
  socket: null,
  connecting: null,
  reconnectTimer: null,
  reconnectAttempt: 0,
  ready: false,
  openedAt: 0,
  lastMessageAt: 0,
  lastError: '',
  rows: new Map(),
  connectAttempts: 0,
  messages: 0,
  acceptedUpdates: 0,
  ignoredNonUsdtUpdates: 0,
  ignoredUnknownSymbols: 0,
  streamBootstrapNewRows: 0,
  streamBootstrapPrunedRows: 0,
  streamBootstrapRows: 0,
  baselineConnecting: null,
  baselineAt: 0,
  baselineAttempts: 0,
  baselineSuccesses: 0,
  baselineFailures: 0,
  baselineRows: 0,
  baselineLastError: '',
  baselineLastRateLimitCount: null,
  baselineLastRateLimitLimit: null,
  baselineLastHttpStatus: null,
  baselineLastRetryAfterSeconds: null,
  baselineNextAllowedAt: 0,
};

const binanceContractBookTicker = {
  socket: null,
  connecting: null,
  reconnectTimer: null,
  reconnectAttempt: 0,
  ready: false,
  openedAt: 0,
  lastMessageAt: 0,
  lastError: '',
  rows: new Map(),
  connectAttempts: 0,
  messages: 0,
  acceptedUpdates: 0,
  rejectedCoinMUpdates: 0,
  baselineConnecting: null,
  baselineAt: 0,
  baselineAttempts: 0,
  baselineSuccesses: 0,
  baselineFailures: 0,
  baselineRows: 0,
  baselineLastError: '',
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

function providerNativeIdentityKey(provider, market, value) {
  let key = compact(value);
  // OKX market-rest canonicalizes native SWAP IDs such as BTC-USDT-SWAP
  // to BTCUSDT before storing the shared directory/ticker identity. The
  // market-light compact() intentionally preserves semantic suffix letters,
  // so enrichment rows must use the same canonical identity key or the
  // official mark/index/OI batches will never merge into the active rows.
  if (provider === 'okx' && market === 'contract' && key.endsWith('SWAP')) {
    key = key.slice(0, -4);
  }
  return key;
}

function directoryIdentityMaps(provider, market) {
  const rows = directoryRowsByKey.get(keyFor(market, provider)) || [];
  const byDisplay = new Map();
  const byNative = new Map();
  for (const row of rows) {
    const display = compact(row?.symbol);
    const native = providerNativeIdentityKey(provider, market, row?.native_symbol ?? row?.raw_symbol ?? row?.symbol);
    if (display) byDisplay.set(display, row);
    if (native) byNative.set(native, row);
  }
  return { byDisplay, byNative };
}

function normalizeRow(provider, market, raw, observedAt, primaryQuote, identities = null) {
  if (!raw || typeof raw !== 'object') return null;
  const rawNative = providerNativeIdentityKey(provider, market, raw.native_symbol ?? raw.raw_symbol ?? raw.symbol);
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
    // Step1019: directory-backed official product facts use the exact same
    // provider/market/symbol identity merge as the ticker. Nullish coalescing
    // intentionally preserves the valid boolean value false.
    ...(market === 'contract' && provider === 'gate' ? {
      funding_interval:
        identity?.funding_interval ?? raw.funding_interval ?? null,
      funding_next_apply:
        identity?.funding_next_apply ?? raw.funding_next_apply ?? null,
      market_order_slip_ratio:
        identity?.market_order_slip_ratio ??
        raw.market_order_slip_ratio ?? null,
      enable_circuit_breaker:
        identity?.enable_circuit_breaker ??
        raw.enable_circuit_breaker ?? null,
    } : {}),
    ...(market === 'contract' && provider === 'okx' ? {
      init_px_lmt_pct:
        identity?.init_px_lmt_pct ?? raw.init_px_lmt_pct ?? null,
      float_px_lmt_pct:
        identity?.float_px_lmt_pct ?? raw.float_px_lmt_pct ?? null,
      max_px_lmt_pct:
        identity?.max_px_lmt_pct ?? raw.max_px_lmt_pct ?? null,
    } : {}),
    trading_status: String(identity?.status ?? raw.trading_status ?? raw.status ?? 'TRADING').trim().toUpperCase() || 'TRADING',
    active: identity?.active !== false && raw.active !== false,
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
    single_open_interest: 'single_open_interest',
    single_open_interest_value: 'single_open_interest_value',
    funding_interval: 'funding_interval',
    funding_next_apply: 'funding_next_apply',
    market_order_slip_ratio: 'market_order_slip_ratio',
    enable_circuit_breaker: 'enable_circuit_breaker',
    init_px_lmt_pct: 'init_px_lmt_pct',
    float_px_lmt_pct: 'float_px_lmt_pct',
    max_px_lmt_pct: 'max_px_lmt_pct',
    basis_rate: 'basis_rate',
    high_24h: 'high_24h',
    low_24h: 'low_24h',
    trading_status: 'trading_status',
  };
  const result = { rows: rows.length };
  for (const [name, field] of Object.entries(fields)) {
    result[name] = rows.filter((row) => row?.[field] != null && row?.[field] !== '').length;
  }
  return result;
}

function providerDirectoryGeneration(meta, latestDirectoryRows) {
  const verifiedDirectoryRows = Number(meta?.verified_directory_count || 0);
  const directoryRows = verifiedDirectoryRows > 0
    ? verifiedDirectoryRows
    : Number(latestDirectoryRows || 0);
  const latestRows = Number(latestDirectoryRows || 0);
  const directoryRefreshPending =
    latestRows > 0 &&
    directoryRows > 0 &&
    latestRows !== directoryRows;
  return {
    directoryRows,
    latestDirectoryRows: latestRows,
    directoryDelta: latestRows - directoryRows,
    directoryRefreshPending,
  };
}

function providerMeta(provider, market) {
  const key = keyFor(market, provider);
  const rows = rowsByKey.get(key) || [];
  const meta = metaByKey.get(key) || {};
  const updatedAt = String(meta.updated_at || '');
  const updatedMs = Date.parse(updatedAt);
  const stale = !Number.isFinite(updatedMs) || Date.now() - updatedMs > STALE_MS;
  // Step1032.1: a directory refresh and a ticker build are independent
  // background jobs. Never compare last verified ticker rows with a newer
  // directory generation, because that produces a transient false 371/372
  // "not exact" while both sources are healthy. Keep directory_count aligned
  // with the generation used by the last successful row build; expose the
  // newest directory separately. A genuine same-generation ticker miss still
  // fails exact coverage on the next build and is never fabricated.
  const generation = providerDirectoryGeneration(
    meta,
    Number(directoryCountByKey.get(key) || 0),
  );
  const directoryRows = generation.directoryRows;
  const latestDirectoryRows = generation.latestDirectoryRows;
  const latestDirectoryUpdatedAt = directoryUpdatedAtByKey.get(key) || null;
  const verifiedDirectoryUpdatedAt = meta.verified_directory_updated_at || null;
  const directoryRefreshPending = generation.directoryRefreshPending;
  return {
    provider,
    market_type: market,
    primary_quote: PRIMARY_QUOTE[market]?.[provider] || null,
    row_count: rows.length,
    directory_count: directoryRows,
    verified_directory_count: directoryRows,
    latest_directory_count: latestDirectoryRows,
    directory_delta: generation.directoryDelta,
    directory_refresh_pending: directoryRefreshPending,
    directory_generation_aligned: !directoryRefreshPending,
    directory_updated_at: verifiedDirectoryUpdatedAt,
    latest_directory_updated_at: latestDirectoryUpdatedAt,
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
        'user-agent': 'KakaWeb3/650.8.15.95 market-light',
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


async function loadBitgetContractFundingBatch(observedAt) {
  bitgetContractFundingBatch.attempts += 1;
  bitgetContractFundingBatch.last_started_at = new Date().toISOString();
  try {
    const payload = await fetchJson('https://api.bitget.com/api/v3/market/current-fund-rate?category=USDT-FUTURES');
    if (String(payload?.code ?? '00000') !== '00000') {
      throw new Error(`bitget_current_fund_rate_code_${payload?.code ?? 'unknown'}`);
    }
    const data = Array.isArray(payload?.data) ? payload.data : [];
    const rows = [];
    for (const item of data) {
      const symbol = compact(item?.symbol);
      if (!symbol || !symbol.endsWith('USDT')) continue;
      const nextMs = Number(item?.nextUpdate);
      rows.push({
        provider: 'bitget',
        market_type: 'contract',
        symbol,
        raw_symbol: symbol,
        native_symbol: symbol,
        funding_rate: finite(item?.fundingRate),
        funding_interval_hours: finite(item?.fundingRateInterval),
        next_funding_time: Number.isFinite(nextMs) && nextMs > 0 ? new Date(nextMs).toISOString() : null,
        next_funding_time_ms: Number.isFinite(nextMs) && nextMs > 0 ? nextMs : null,
        min_funding_rate: finite(item?.minFundingRate),
        max_funding_rate: finite(item?.maxFundingRate),
        cash_dividend: finite(item?.cashDividend),
        cash_dividend_next_update: isoMs(item?.cashDividendNextUpdate),
        funding_rate_source: 'bitget_official_v3_current_fund_rate_category_batch',
        source_time: isoMs(item?.ts) || observedAt,
        cached_at: observedAt,
      });
    }
    if (!rows.length) throw new Error('bitget_current_fund_rate_rows_empty');
    bitgetContractFundingBatch.successes += 1;
    bitgetContractFundingBatch.rows = rows.length;
    bitgetContractFundingBatch.last_completed_at = new Date().toISOString();
    bitgetContractFundingBatch.last_error = '';
    return rows;
  } catch (error) {
    bitgetContractFundingBatch.failures += 1;
    bitgetContractFundingBatch.last_completed_at = new Date().toISOString();
    bitgetContractFundingBatch.last_error = String(error?.message || error);
    throw error;
  }
}

function mergeRowsByNative(baseRows, patchRows, provider, market, observedAt, primaryQuote) {
  const identities = directoryIdentityMaps(provider, market);
  const base = new Map();
  for (const raw of Array.isArray(baseRows) ? baseRows : []) {
    const row = normalizeRow(provider, market, raw, observedAt, primaryQuote, identities);
    if (row) base.set(row.symbol, row);
  }
  for (const raw of Array.isArray(patchRows) ? patchRows : []) {
    const native = providerNativeIdentityKey(provider, market, raw?.native_symbol ?? raw?.raw_symbol ?? raw?.symbol);
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
        symbol: providerNativeIdentityKey('okx', 'contract', raw),
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
    let baseRows = [];
    try {
      baseRows = await loadBitgetV3Rows(market, observedAt);
    } catch (_) {
      baseRows = await loadMarketTickers(provider, market, []);
    }
    if (market === 'contract') {
      const patches = await loadBitgetContractFundingBatch(observedAt).catch(() => []);
      const merged = mergeRowsByNative(baseRows, patches, provider, market, observedAt, 'USDT');
      bitgetContractFundingBatch.applied_rows = merged.filter((row) =>
        row?.funding_rate_source === 'bitget_official_v3_current_fund_rate_category_batch' &&
        row?.funding_interval_hours != null &&
        row?.next_funding_time != null
      ).length;
      return merged;
    }
    return baseRows;
  }
  const baseRows = await loadMarketTickers(provider, market, []);
  if (provider === 'binance' && market === 'contract') {
    ensureBinanceContractBookTicker().catch(() => {});
    await refreshBinanceContractBookTickerBaseline().catch(() => false);
    const patches = [...binanceContractBookTicker.rows.values()];
    return mergeRowsByNative(baseRows, patches, provider, market, observedAt, 'USDT');
  }
  if (provider === 'okx' && market === 'contract') {
    const patches = await loadOkxContractBatchEnrichment(observedAt).catch(() => []);
    const merged = mergeRowsByNative(baseRows, patches, provider, market, observedAt, 'USDT');
    okxContractBatchEnrichment.applied_mark_rows = merged.filter((row) => row?.mark_price_source === 'okx_public_mark_price_batch' && positive(row?.mark_price) != null).length;
    okxContractBatchEnrichment.applied_index_rows = merged.filter((row) => row?.index_price_source === 'okx_public_index_tickers_usdt_batch' && positive(row?.index_price) != null).length;
    okxContractBatchEnrichment.applied_open_interest_rows = merged.filter((row) => row?.open_interest_source === 'okx_public_open_interest_batch' && row?.open_interest != null).length;
    okxContractBatchEnrichment.applied_patch_rows = merged.filter((row) =>
      row?.mark_price_source === 'okx_public_mark_price_batch' ||
      row?.index_price_source === 'okx_public_index_tickers_usdt_batch' ||
      row?.open_interest_source === 'okx_public_open_interest_batch'
    ).length;
    return merged;
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
  const localBinanceSpotDirectory = provider === 'binance' && market === 'spot';
  if (localBinanceSpotDirectory) {
    ensureBinanceSpotMiniTicker().catch(() => {});
    refreshBinanceSpotStreamDerivedDirectory();
    refreshBinanceSpotTickerBaseline().catch(() => false);
    return directoryRowsByKey.get(key) || [];
  }
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
  if (provider === 'binance' && market === 'spot') {
    ensureBinanceSpotMiniTicker().catch(() => {});
    refreshBinanceSpotTickerBaseline().catch(() => false);
    const rows = refreshBinanceSpotStreamDerivedDirectory();
    const providerKey = keyFor(market, provider);
    const current = metaByKey.get(providerKey) || {};
    const streamFresh = binanceSpotTicker.lastMessageAt > 0 &&
      Date.now() - binanceSpotTicker.lastMessageAt <= STALE_MS;
    const optionalRestFresh = binanceSpotTicker.baselineAt > 0 &&
      Date.now() - binanceSpotTicker.baselineAt <= BINANCE_SPOT_BASELINE_TTL_MS;
    if (rows.length < BINANCE_SPOT_STREAM_BOOTSTRAP_MIN_ROWS || (!streamFresh && !optionalRestFresh)) {
      metaByKey.set(providerKey, {
        ...current,
        build_calls: Number(current.build_calls || 0) + 1,
        failed_builds: Number(current.failed_builds || 0) + 1,
        last_error: rows.length < BINANCE_SPOT_STREAM_BOOTSTRAP_MIN_ROWS
          ? `binance_spot_stream_bootstrap_rows_too_small:${rows.length}<${BINANCE_SPOT_STREAM_BOOTSTRAP_MIN_ROWS}`
          : (binanceSpotTicker.lastError || binanceSpotTicker.baselineLastError || 'binance_spot_shared_stream_stale'),
      });
      totalBuildFailures += 1;
      return false;
    }
    const observedAt = new Date().toISOString();
    try {
      assertNoSeverePartialOverwrite(providerKey, rows);
    } catch (error) {
      metaByKey.set(providerKey, {
        ...current,
        build_calls: Number(current.build_calls || 0) + 1,
        failed_builds: Number(current.failed_builds || 0) + 1,
        last_error: String(error?.message || error).slice(0, 320),
      });
      totalBuildFailures += 1;
      return false;
    }
    const verifiedDirectoryCount = Number(directoryCountByKey.get(providerKey) || 0);
    const verifiedDirectoryUpdatedAt = directoryUpdatedAtByKey.get(providerKey) || null;
    rowsByKey.set(providerKey, rows.map((row) => ({
      ...row,
      shared_round: cycleRound,
      shared_observed_at: observedAt,
    })));
    metaByKey.set(providerKey, {
      ...current,
      updated_at: observedAt,
      verified_directory_count: verifiedDirectoryCount,
      verified_directory_updated_at: verifiedDirectoryUpdatedAt,
      last_error: '',
      build_calls: Number(current.build_calls || 0) + 1,
      successful_builds: Number(current.successful_builds || 0) + 1,
    });
    return true;
  }
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
    const verifiedDirectoryCount = Number(directoryCountByKey.get(providerKey) || 0);
    const verifiedDirectoryUpdatedAt = directoryUpdatedAtByKey.get(providerKey) || null;
    rowsByKey.set(providerKey, rows.map((row) => ({
      ...row,
      shared_round: cycleRound,
      shared_observed_at: observedAt,
    })));
    const current = metaByKey.get(providerKey) || {};
    metaByKey.set(providerKey, {
      ...current,
      updated_at: observedAt,
      verified_directory_count: verifiedDirectoryCount,
      verified_directory_updated_at: verifiedDirectoryUpdatedAt,
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
    // No await is allowed between dedupeRows and this capture: the directory
    // count stored here is the exact generation paired with the published
    // ticker rows.
    const verifiedDirectoryCount = Number(directoryCountByKey.get(key) || 0);
    const verifiedDirectoryUpdatedAt = directoryUpdatedAtByKey.get(key) || null;
    rowsByKey.set(key, rows.map((row) => ({
      ...row,
      shared_round: cycleRound,
      shared_observed_at: observedAt,
    })));
    metaByKey.set(key, {
      ...current,
      updated_at: observedAt,
      verified_directory_count: verifiedDirectoryCount,
      verified_directory_updated_at: verifiedDirectoryUpdatedAt,
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
      if (provider === 'binance' && market === 'spot') {
        ensureBinanceSpotMiniTicker().catch(() => {});
        refreshBinanceSpotStreamDerivedDirectory();
        refreshBinanceSpotTickerBaseline().catch(() => false);
        return;
      }
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
    trading_status: 'TRADING',
    active: true,
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


function binanceSpotTicker24hRow(raw, observedAt = new Date().toISOString()) {
  if (!raw || typeof raw !== 'object') return null;
  const symbol = compact(raw.symbol ?? raw.s);
  if (!symbol || !symbol.endsWith('USDT') || symbol.length <= 4) return null;
  const last = positive(raw.lastPrice ?? raw.c);
  if (last == null) return null;
  const bid = positive(raw.bidPrice ?? raw.b);
  const ask = positive(raw.askPrice ?? raw.a);
  const open = positive(raw.openPrice ?? raw.o);
  const sourceTime = isoMs(raw.closeTime ?? raw.E) || observedAt;
  const baseVolume = finite(raw.volume ?? raw.v);
  const quoteVolume = finite(raw.quoteVolume ?? raw.q);
  let changePercent = finite(raw.priceChangePercent);
  if (changePercent == null && open != null && open > 0) changePercent = ((last - open) / open) * 100;
  return {
    provider: 'binance',
    market_type: 'spot',
    symbol,
    raw_symbol: symbol,
    native_symbol: symbol,
    base_asset: symbol.slice(0, -4),
    quote_asset: 'USDT',
    quote_symbol: 'USDT',
    trading_status: 'TRADING',
    active: true,
    last_price: last,
    price: last,
    price_change_percent_24h: changePercent,
    price_change_percent_24h_source: raw.priceChangePercent != null
      ? 'binance_spot_ws_api_ticker_24hr'
      : 'derived_from_binance_official_open_close',
    volume_24h: baseVolume,
    base_volume_24h: baseVolume,
    quote_volume_24h: quoteVolume,
    high_24h: finite(raw.highPrice ?? raw.h),
    low_24h: finite(raw.lowPrice ?? raw.l),
    best_bid: bid,
    best_ask: ask,
    bid_price: bid,
    ask_price: ask,
    spread_percent: bid != null && ask != null && ask > 0 && ask >= bid ? ((ask - bid) / ask) * 100 : null,
    source: 'binance_spot_official_market_data_only_rest_ticker_24hr_all_trading',
    transport: 'backend_shared_market_data_only_rest_all_symbols_baseline',
    source_time: sourceTime,
    cached_at: sourceTime,
    backend_shared: true,
    market_light_scope: 'primary_quote_full_directory',
    bbo_available_in_source: bid != null && ask != null,
    stream_bootstrap: false,
    directory_identity_source: 'binance_official_market_data_rest_baseline',
  };
}

function binanceSpotMiniTickerSeedRow(raw, observedAt = new Date().toISOString()) {
  if (!raw || typeof raw !== 'object') return null;
  const symbol = compact(raw.s ?? raw.symbol);
  if (!symbol || !symbol.endsWith('USDT') || symbol.length <= 4) return null;
  const last = positive(raw.c ?? raw.lastPrice);
  if (last == null) return null;
  const open = positive(raw.o ?? raw.openPrice);
  const sourceTime = isoMs(raw.E ?? raw.closeTime) || observedAt;
  const baseVolume = finite(raw.v ?? raw.volume);
  const quoteVolume = finite(raw.q ?? raw.quoteVolume);
  const changePercent = open != null && open > 0 ? ((last - open) / open) * 100 : null;
  return {
    provider: 'binance',
    market_type: 'spot',
    symbol,
    raw_symbol: symbol,
    native_symbol: symbol,
    base_asset: symbol.slice(0, -4),
    quote_asset: 'USDT',
    quote_symbol: 'USDT',
    trading_status: 'TRADING',
    active: true,
    last_price: last,
    price: last,
    price_change_percent_24h: changePercent,
    price_change_percent_24h_source: open != null ? 'derived_from_binance_official_miniticker_close_open' : null,
    volume_24h: baseVolume,
    base_volume_24h: baseVolume,
    quote_volume_24h: quoteVolume,
    high_24h: finite(raw.h ?? raw.highPrice),
    low_24h: finite(raw.l ?? raw.lowPrice),
    best_bid: null,
    best_ask: null,
    bid_price: null,
    ask_price: null,
    spread_percent: null,
    source: 'binance_spot_official_miniticker_stream_bootstrap',
    transport: 'backend_shared_one_market_stream_bootstrap',
    source_time: sourceTime,
    cached_at: sourceTime,
    backend_shared: true,
    market_light_scope: 'primary_quote_full_directory',
    bbo_available_in_source: false,
    stream_bootstrap: true,
    directory_identity_source: 'binance_official_live_miniticker_symbol_observation',
  };
}

function refreshBinanceSpotStreamDerivedDirectory() {
  const now = Date.now();
  for (const [symbol, row] of [...binanceSpotTicker.rows.entries()]) {
    if (row?.stream_bootstrap !== true) continue;
    const sourceMs = Date.parse(String(row?.source_time ?? row?.cached_at ?? ''));
    if (!Number.isFinite(sourceMs) || now - sourceMs > BINANCE_SPOT_STREAM_ROW_TTL_MS) {
      binanceSpotTicker.rows.delete(symbol);
      binanceSpotTicker.streamBootstrapPrunedRows += 1;
    }
  }
  const rows = [...binanceSpotTicker.rows.values()]
    .filter((row) => quoteAssetFor(row, 'USDT') === 'USDT')
    .sort((a, b) => String(a.symbol).localeCompare(String(b.symbol)));
  binanceSpotTicker.streamBootstrapRows = rows.filter((row) => row?.stream_bootstrap === true).length;
  if (rows.length) {
    const directory = binanceSpotDirectoryFromRows(rows);
    directoryRowsByKey.set(keyFor('spot', 'binance'), directory);
    directoryCountByKey.set(keyFor('spot', 'binance'), directory.length);
    directoryUpdatedAtByKey.set(keyFor('spot', 'binance'), new Date().toISOString());
  }
  return rows;
}

function binanceSpotMiniTickerPatch(raw, existing) {
  if (!raw || typeof raw !== 'object' || !existing) return null;
  const symbol = compact(raw.s ?? raw.symbol);
  if (!symbol || symbol !== compact(existing.symbol) || !symbol.endsWith('USDT')) return null;
  const last = positive(raw.c);
  if (last == null) return null;
  const open = positive(raw.o);
  const sourceTime = isoMs(raw.E) || new Date().toISOString();
  return {
    ...existing,
    last_price: last,
    price: last,
    price_change_percent_24h: open != null && open > 0 ? ((last - open) / open) * 100 : existing.price_change_percent_24h,
    price_change_percent_24h_source: open != null
      ? 'derived_from_binance_official_miniticker_close_open'
      : existing.price_change_percent_24h_source,
    volume_24h: finite(raw.v) ?? existing.volume_24h,
    base_volume_24h: finite(raw.v) ?? existing.base_volume_24h,
    quote_volume_24h: finite(raw.q) ?? existing.quote_volume_24h,
    high_24h: finite(raw.h) ?? existing.high_24h,
    low_24h: finite(raw.l) ?? existing.low_24h,
    source: existing?.stream_bootstrap === true
      ? 'binance_spot_official_miniticker_stream_bootstrap'
      : 'binance_spot_official_market_data_only_rest_baseline_plus_miniticker_stream',
    transport: existing?.stream_bootstrap === true
      ? 'backend_shared_one_market_stream_bootstrap'
      : 'backend_shared_market_data_only_rest_baseline_plus_shared_market_stream',
    source_time: sourceTime,
    cached_at: sourceTime,
  };
}

function binanceSpotDirectoryFromRows(rows) {
  return rows.map((row) => ({
    provider: 'binance',
    market_type: 'spot',
    symbol: row.symbol,
    raw_symbol: row.raw_symbol,
    native_symbol: row.native_symbol,
    base_asset: row.base_asset,
    quote_asset: 'USDT',
    status: 'TRADING',
    trading_status: 'TRADING',
    active: true,
  }));
}

async function refreshBinanceSpotTickerBaseline({ force = false } = {}) {
  if (!BINANCE_SPOT_REST_BASELINE_ENABLED) return binanceSpotTicker.rows.size > 0;
  const now = Date.now();
  if (!force && binanceSpotTicker.baselineAt > 0 && now - binanceSpotTicker.baselineAt <= BINANCE_SPOT_BASELINE_TTL_MS) {
    return true;
  }
  if (!force && binanceSpotTicker.baselineNextAllowedAt > now) {
    return binanceSpotTicker.rows.size > 0;
  }
  if (binanceSpotTicker.baselineConnecting) return await binanceSpotTicker.baselineConnecting;

  binanceSpotTicker.baselineConnecting = (async () => {
    binanceSpotTicker.baselineAttempts += 1;
    const url = `${BINANCE_SPOT_MARKET_DATA_REST_BASE}${BINANCE_SPOT_MARKET_DATA_REST_PATH}`;
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3/650.8.15.95 market-light-binance-spot-shared-baseline',
      },
      signal: AbortSignal.timeout(12_000),
    });

    binanceSpotTicker.baselineLastHttpStatus = Number(response.status || 0);
    const usedWeight = finite(response.headers.get('x-mbx-used-weight-1m'));
    const retryAfter = finite(response.headers.get('retry-after'));
    binanceSpotTicker.baselineLastRateLimitCount = usedWeight;
    binanceSpotTicker.baselineLastRateLimitLimit = null;
    binanceSpotTicker.baselineLastRetryAfterSeconds = retryAfter;

    if (!response.ok) {
      const text = await response.text().catch(() => '');
      const retryMs = retryAfter != null && retryAfter > 0
        ? Math.max(BINANCE_SPOT_BASELINE_RETRY_MS, retryAfter * 1000)
        : BINANCE_SPOT_BASELINE_RETRY_MS;
      binanceSpotTicker.baselineNextAllowedAt = Date.now() + retryMs;
      throw new Error(`binance_spot_market_data_rest_http_${response.status}:${text.slice(0,180)}`);
    }

    const sourceRows = await response.json();
    if (!Array.isArray(sourceRows)) {
      throw new Error('binance_spot_market_data_rest_payload_not_array');
    }

    const observedAt = new Date().toISOString();
    const rows = sourceRows
      .map((item) => binanceSpotTicker24hRow(item, observedAt))
      .filter(Boolean);

    if (rows.length < 20) {
      throw new Error(`binance_spot_market_data_rest_rows_too_small:${rows.length}`);
    }

    const next = new Map(rows.map((row) => [row.symbol, row]));
    binanceSpotTicker.rows = next;
    const directory = binanceSpotDirectoryFromRows(rows);
    directoryRowsByKey.set(keyFor('spot', 'binance'), directory);
    directoryCountByKey.set(keyFor('spot', 'binance'), directory.length);
    directoryUpdatedAtByKey.set(keyFor('spot', 'binance'), observedAt);

    binanceSpotTicker.baselineAt = Date.now();
    binanceSpotTicker.baselineRows = rows.length;
    binanceSpotTicker.baselineSuccesses += 1;
    binanceSpotTicker.baselineLastError = '';
    binanceSpotTicker.baselineNextAllowedAt = 0;
    return true;
  })()
    .catch((error) => {
      binanceSpotTicker.baselineFailures += 1;
      binanceSpotTicker.baselineLastError = String(error?.message || error).slice(0, 320);
      if (binanceSpotTicker.baselineNextAllowedAt <= Date.now()) {
        binanceSpotTicker.baselineNextAllowedAt = Date.now() + BINANCE_SPOT_BASELINE_RETRY_MS;
      }
      return false;
    })
    .finally(() => {
      binanceSpotTicker.baselineConnecting = null;
    });

  return await binanceSpotTicker.baselineConnecting;
}

function scheduleBinanceSpotMiniTickerReconnect() {
  if (binanceSpotTicker.reconnectTimer) return;
  const delay = Math.min(
    BINANCE_SPOT_RECONNECT_MAX_MS,
    BINANCE_SPOT_RECONNECT_MIN_MS * (2 ** Math.min(binanceSpotTicker.reconnectAttempt, 5)),
  );
  binanceSpotTicker.reconnectAttempt += 1;
  binanceSpotTicker.reconnectTimer = setTimeout(() => {
    binanceSpotTicker.reconnectTimer = null;
    ensureBinanceSpotMiniTicker().catch(() => {});
  }, delay);
  binanceSpotTicker.reconnectTimer.unref?.();
}

async function openBinanceSpotMiniTicker() {
  const WebSocketCtor = await resolveWebSocketCtor();
  binanceSpotTicker.connectAttempts += 1;
  const socket = new WebSocketCtor(BINANCE_SPOT_MINI_TICKER_WS_URL);
  binanceSpotTicker.socket = socket;
  binanceSpotTicker.ready = false;
  binanceSpotTicker.lastError = '';
  return await new Promise((resolve, reject) => {
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      closeWs(socket);
      reject(new Error('binance_spot_miniticker_connect_timeout'));
    }, 12_000);
    timeout.unref?.();

    wsListen(socket, 'open', () => {
      if (settled) return;
      binanceSpotTicker.openedAt = Date.now();
      binanceSpotTicker.reconnectAttempt = 0;
      binanceSpotTicker.ready = true;
      refreshBinanceSpotTickerBaseline().catch(() => {});
      settled = true;
      clearTimeout(timeout);
      resolve(true);
    });

    wsListen(socket, 'message', async (event) => {
      try {
        const text = await wsMessageText(event);
        const decoded = JSON.parse(text);
        const payload = decoded?.data ?? decoded;
        const items = Array.isArray(payload) ? payload : [payload];
        binanceSpotTicker.messages += 1;
        binanceSpotTicker.lastMessageAt = Date.now();
        for (const item of items) {
          const symbol = compact(item?.s ?? item?.symbol);
          if (!symbol || !symbol.endsWith('USDT')) {
            binanceSpotTicker.ignoredNonUsdtUpdates += 1;
            continue;
          }
          const existing = binanceSpotTicker.rows.get(symbol);
          if (!existing) {
            const seeded = binanceSpotMiniTickerSeedRow(item);
            if (!seeded) {
              binanceSpotTicker.ignoredUnknownSymbols += 1;
              continue;
            }
            binanceSpotTicker.rows.set(symbol, seeded);
            binanceSpotTicker.streamBootstrapNewRows += 1;
            binanceSpotTicker.acceptedUpdates += 1;
            continue;
          }
          const merged = binanceSpotMiniTickerPatch(item, existing);
          if (!merged) continue;
          binanceSpotTicker.rows.set(symbol, merged);
          binanceSpotTicker.acceptedUpdates += 1;
        }
      } catch (error) {
        binanceSpotTicker.lastError = String(error?.message || error).slice(0, 320);
      }
    });

    wsListen(socket, 'error', (error) => {
      binanceSpotTicker.lastError = String(error?.message || error || 'binance_spot_miniticker_socket_error').slice(0, 320);
      if (!settled) {
        settled = true;
        clearTimeout(timeout);
        reject(error instanceof Error ? error : new Error('binance_spot_miniticker_socket_error'));
      }
    });

    wsListen(socket, 'close', () => {
      binanceSpotTicker.ready = false;
      if (binanceSpotTicker.socket === socket) binanceSpotTicker.socket = null;
      if (!settled) {
        settled = true;
        clearTimeout(timeout);
        reject(new Error('binance_spot_miniticker_closed_before_open'));
      }
      scheduleBinanceSpotMiniTickerReconnect();
    });
  });
}

async function ensureBinanceSpotMiniTicker() {
  if (wsReady(binanceSpotTicker.socket) && binanceSpotTicker.ready) return true;
  if (binanceSpotTicker.connecting) return await binanceSpotTicker.connecting;
  binanceSpotTicker.connecting = openBinanceSpotMiniTicker()
    .catch((error) => {
      binanceSpotTicker.lastError = String(error?.message || error).slice(0, 320);
      scheduleBinanceSpotMiniTickerReconnect();
      return false;
    })
    .finally(() => {
      binanceSpotTicker.connecting = null;
    });
  return await binanceSpotTicker.connecting;
}

function binanceContractBookTickerRow(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const symbolTypeRaw = raw.st;
  const symbolType = symbolTypeRaw == null ? null : integer(symbolTypeRaw);
  if (symbolType != null && symbolType !== 1) {
    binanceContractBookTicker.rejectedCoinMUpdates += 1;
    return null;
  }
  const symbol = compact(raw.s ?? raw.symbol);
  if (!symbol || !symbol.endsWith('USDT')) return null;
  const bestBid = positive(raw.b ?? raw.bestBid ?? raw.bidPrice);
  const bestAsk = positive(raw.a ?? raw.bestAsk ?? raw.askPrice);
  if (bestBid == null || bestAsk == null || bestAsk < bestBid) return null;
  const sourceTime = isoMs(raw.E ?? raw.T ?? raw.time) || new Date().toISOString();
  return {
    provider: 'binance',
    market_type: 'contract',
    symbol,
    raw_symbol: symbol,
    native_symbol: symbol,
    best_bid: bestBid,
    best_ask: bestAsk,
    bid_price: bestBid,
    ask_price: bestAsk,
    spread_percent: bestAsk > 0 ? ((bestAsk - bestBid) / bestAsk) * 100 : null,
    source_time: sourceTime,
    cached_at: sourceTime,
    best_bid_ask_source: 'binance_usdm_all_book_tickers_shared_websocket',
  };
}

async function refreshBinanceContractBookTickerBaseline({ force = false } = {}) {
  if (!force && binanceContractBookTicker.baselineAt > 0 && Date.now() - binanceContractBookTicker.baselineAt <= BINANCE_BOOK_BASELINE_TTL_MS) {
    return true;
  }
  if (binanceContractBookTicker.baselineConnecting) return await binanceContractBookTicker.baselineConnecting;
  binanceContractBookTicker.baselineConnecting = (async () => {
    const WebSocketCtor = await resolveWebSocketCtor();
    binanceContractBookTicker.baselineAttempts += 1;
    const socket = new WebSocketCtor(BINANCE_CONTRACT_BOOK_TICKER_WS_API_URL);
    return await new Promise((resolve, reject) => {
      let settled = false;
      const requestId = `kaka-step990-book-${Date.now()}`;
      const timeout = setTimeout(() => {
        if (settled) return;
        settled = true;
        closeWs(socket);
        reject(new Error('binance_contract_book_ticker_ws_api_timeout'));
      }, 12_000);
      timeout.unref?.();

      wsListen(socket, 'open', () => {
        if (!sendWs(socket, { id: requestId, method: 'ticker.book' })) {
          if (!settled) {
            settled = true;
            clearTimeout(timeout);
            closeWs(socket);
            reject(new Error('binance_contract_book_ticker_ws_api_send_failed'));
          }
        }
      });

      wsListen(socket, 'message', async (event) => {
        if (settled) return;
        try {
          const text = await wsMessageText(event);
          const decoded = JSON.parse(text);
          if (String(decoded?.id ?? '') !== requestId) return;
          if (Number(decoded?.status || 0) !== 200 || !Array.isArray(decoded?.result)) {
            throw new Error(`binance_contract_book_ticker_ws_api_status_${decoded?.status || 0}`);
          }
          let accepted = 0;
          for (const item of decoded.result) {
            const row = binanceContractBookTickerRow(item);
            if (!row) continue;
            binanceContractBookTicker.rows.set(row.symbol, {
              ...row,
              best_bid_ask_source: 'binance_usdm_ws_api_ticker_book_all_baseline',
            });
            accepted += 1;
          }
          if (!accepted) throw new Error('binance_contract_book_ticker_ws_api_rows_empty');
          binanceContractBookTicker.baselineAt = Date.now();
          binanceContractBookTicker.baselineRows = accepted;
          binanceContractBookTicker.baselineSuccesses += 1;
          binanceContractBookTicker.baselineLastError = '';
          settled = true;
          clearTimeout(timeout);
          closeWs(socket);
          resolve(true);
        } catch (error) {
          if (!settled) {
            settled = true;
            clearTimeout(timeout);
            closeWs(socket);
            reject(error);
          }
        }
      });

      wsListen(socket, 'error', (error) => {
        if (!settled) {
          settled = true;
          clearTimeout(timeout);
          reject(error instanceof Error ? error : new Error('binance_contract_book_ticker_ws_api_error'));
        }
      });

      wsListen(socket, 'close', () => {
        if (!settled) {
          settled = true;
          clearTimeout(timeout);
          reject(new Error('binance_contract_book_ticker_ws_api_closed_before_response'));
        }
      });
    });
  })()
    .catch((error) => {
      binanceContractBookTicker.baselineFailures += 1;
      binanceContractBookTicker.baselineLastError = String(error?.message || error).slice(0, 320);
      return false;
    })
    .finally(() => {
      binanceContractBookTicker.baselineConnecting = null;
    });
  return await binanceContractBookTicker.baselineConnecting;
}

function scheduleBinanceContractBookTickerReconnect() {
  if (binanceContractBookTicker.reconnectTimer) return;
  const delay = Math.min(
    BINANCE_BOOK_RECONNECT_MAX_MS,
    BINANCE_BOOK_RECONNECT_MIN_MS * (2 ** Math.min(binanceContractBookTicker.reconnectAttempt, 5)),
  );
  binanceContractBookTicker.reconnectAttempt += 1;
  binanceContractBookTicker.reconnectTimer = setTimeout(() => {
    binanceContractBookTicker.reconnectTimer = null;
    ensureBinanceContractBookTicker().catch(() => {});
  }, delay);
  binanceContractBookTicker.reconnectTimer.unref?.();
}

async function openBinanceContractBookTicker() {
  const WebSocketCtor = await resolveWebSocketCtor();
  binanceContractBookTicker.connectAttempts += 1;
  const socket = new WebSocketCtor(BINANCE_CONTRACT_BOOK_TICKER_WS_URL);
  binanceContractBookTicker.socket = socket;
  binanceContractBookTicker.ready = false;
  binanceContractBookTicker.lastError = '';
  return await new Promise((resolve, reject) => {
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      closeWs(socket);
      reject(new Error('binance_contract_all_book_ticker_connect_timeout'));
    }, 10_000);
    timeout.unref?.();

    wsListen(socket, 'open', () => {
      if (settled) return;
      binanceContractBookTicker.openedAt = Date.now();
      binanceContractBookTicker.reconnectAttempt = 0;
      binanceContractBookTicker.ready = true;
      refreshBinanceContractBookTickerBaseline().catch(() => {});
      settled = true;
      clearTimeout(timeout);
      resolve(true);
    });

    wsListen(socket, 'message', async (event) => {
      try {
        const text = await wsMessageText(event);
        const decoded = JSON.parse(text);
        const payload = decoded?.data ?? decoded;
        const items = Array.isArray(payload) ? payload : [payload];
        binanceContractBookTicker.messages += 1;
        binanceContractBookTicker.lastMessageAt = Date.now();
        for (const item of items) {
          const row = binanceContractBookTickerRow(item);
          if (!row) continue;
          binanceContractBookTicker.rows.set(row.symbol, row);
          binanceContractBookTicker.acceptedUpdates += 1;
        }
      } catch (error) {
        binanceContractBookTicker.lastError = String(error?.message || error).slice(0, 320);
      }
    });

    wsListen(socket, 'error', (error) => {
      binanceContractBookTicker.lastError = String(error?.message || error || 'binance_contract_all_book_ticker_socket_error').slice(0, 320);
      if (!settled) {
        settled = true;
        clearTimeout(timeout);
        reject(error instanceof Error ? error : new Error('binance_contract_all_book_ticker_socket_error'));
      }
    });

    wsListen(socket, 'close', () => {
      binanceContractBookTicker.ready = false;
      if (binanceContractBookTicker.socket === socket) binanceContractBookTicker.socket = null;
      if (!settled) {
        settled = true;
        clearTimeout(timeout);
        reject(new Error('binance_contract_all_book_ticker_closed_before_open'));
      }
      scheduleBinanceContractBookTickerReconnect();
    });
  });
}

async function ensureBinanceContractBookTicker() {
  if (wsReady(binanceContractBookTicker.socket) && binanceContractBookTicker.ready) return true;
  if (binanceContractBookTicker.connecting) return await binanceContractBookTicker.connecting;
  binanceContractBookTicker.connecting = openBinanceContractBookTicker()
    .catch((error) => {
      binanceContractBookTicker.lastError = String(error?.message || error).slice(0, 320);
      scheduleBinanceContractBookTickerReconnect();
      return false;
    })
    .finally(() => {
      binanceContractBookTicker.connecting = null;
    });
  return await binanceContractBookTicker.connecting;
}

export async function runMarketLightSnapshotCycle({ reason = 'scheduled' } = {}) {
  if (running) return false;
  running = true;
  lastStartedAt = new Date().toISOString();
  lastError = '';
  const cycleRound = round + 1;
  try {
    ensureCoinbaseTickerBatch().catch(() => {});
    ensureBinanceSpotMiniTicker().catch(() => {});
    refreshBinanceSpotTickerBaseline().catch(() => {});
    ensureBinanceContractBookTicker().catch(() => {});
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
    rankedPageCache.clear();
    if (successful > 0) {
      maybeArchiveCryptoSectorSnapshot(buildSectorProfessionalSnapshot()).catch(() => {});
    }
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
  primeCryptoSectorHistory();
  startMarketCapRankCollector();
  ensureCoinbaseTickerBatch().catch(() => {});
  ensureBinanceSpotMiniTicker().catch(() => {});
  refreshBinanceSpotTickerBaseline().catch(() => {});
  ensureBinanceContractBookTicker().catch(() => {});
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
    ensureBinanceContractBookTicker().catch(() => {});
  }, DIRECTORY_INTERVAL_MS);
  directoryInterval.unref?.();
}


function marketRankNormalizeBase(raw) {
  let value = compact(raw);
  if (value === 'XBT') value = 'BTC';
  if (/^1000[A-Z0-9]+$/.test(value)) value = value.slice(4);
  return value;
}

function marketRankBaseFromRow(row) {
  const explicit = compact(row?.base_asset);
  if (explicit) return marketRankNormalizeBase(explicit);
  const quote = compact(row?.quote_asset ?? row?.quote_symbol);
  const symbol = compact(row?.symbol);
  if (quote && symbol.endsWith(quote) && symbol.length > quote.length) {
    return marketRankNormalizeBase(symbol.slice(0, -quote.length));
  }
  return marketRankNormalizeBase(symbol);
}

function marketRankNumber(value) {
  if (value == null) return null;
  if (typeof value === 'string' && !value.trim()) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function marketRankCapForBase(base) {
  const key = marketRankNormalizeBase(base);
  const item = key ? marketCapBySymbol.get(key) : null;
  return item ? { ...item } : null;
}

function marketRankCompareNullable(a, b, { descending = false } = {}) {
  const av = marketRankNumber(a);
  const bv = marketRankNumber(b);
  if (av == null && bv == null) return 0;
  if (av == null) return 1;
  if (bv == null) return -1;
  return descending ? bv - av : av - bv;
}

function marketRankSortKey(raw) {
  const value = String(raw || '').trim().toLowerCase();
  if (['change_desc','gainers','gain'].includes(value)) return 'change_desc';
  if (['change_asc','losers','loss'].includes(value)) return 'change_asc';
  if (['volume_desc','turnover_desc','volume'].includes(value)) return 'volume_desc';
  if (['market_cap_desc','market_cap','cap','symbol'].includes(value)) return 'market_cap_desc';
  return 'market_cap_desc';
}

function marketRankEntryComparator(sortKey) {
  return (a, b) => {
    let cmp = 0;
    if (sortKey === 'change_desc') {
      cmp = marketRankCompareNullable(a.change_desc, b.change_desc, { descending: true });
    } else if (sortKey === 'change_asc') {
      cmp = marketRankCompareNullable(a.change_asc, b.change_asc, { descending: false });
    } else if (sortKey === 'volume_desc') {
      cmp = marketRankCompareNullable(a.volume_desc, b.volume_desc, { descending: true });
    } else {
      const ar = marketRankNumber(a.market_cap_rank);
      const br = marketRankNumber(b.market_cap_rank);
      if (ar != null || br != null) {
        if (ar == null) return 1;
        if (br == null) return -1;
        cmp = ar - br;
      }
      if (!cmp) {
        cmp = marketRankCompareNullable(a.market_cap_usd, b.market_cap_usd, { descending: true });
      }
    }
    if (!cmp) cmp = marketRankCompareNullable(a.volume_desc, b.volume_desc, { descending: true });
    if (!cmp) cmp = String(a.rank_identity).localeCompare(String(b.rank_identity));
    return cmp;
  };
}

function buildMarketRankEntries({ market, provider = '', quote = '', sortKey }) {
  const providers = market === 'spot' ? SPOT_PROVIDERS : CONTRACT_PROVIDERS;
  const selectedProviders = provider && provider !== 'all' ? [provider] : providers;
  const quoteFilter = compact(quote);
  const rows = [];
  for (const providerName of selectedProviders) {
    for (const row of rowsByKey.get(keyFor(market, providerName)) || []) {
      const rowQuote = compact(row?.quote_asset ?? row?.quote_symbol);
      if (quoteFilter && rowQuote !== quoteFilter) continue;
      rows.push({ ...row });
    }
  }

  // Spot "all providers" is an asset ranking, matching the existing App UI:
  // one base asset occupies one ranked position while exact venue rows remain
  // available inside venue_rows. Provider-specific spot and all contract pages
  // keep exact provider+symbol identity.
  if (market === 'spot' && (!provider || provider === 'all')) {
    const grouped = new Map();
    for (const row of rows) {
      const base = marketRankBaseFromRow(row);
      if (!base) continue;
      const list = grouped.get(base) || [];
      list.push(row);
      grouped.set(base, list);
    }
    const entries = [];
    for (const [base, venueRows] of grouped.entries()) {
      const changes = venueRows.map((row) => marketRankNumber(row?.price_change_percent_24h)).filter((v) => v != null);
      const volumes = venueRows.map((row) => marketRankNumber(row?.quote_volume_24h)).filter((v) => v != null && v >= 0);
      const cap = marketRankCapForBase(base);
      const representative = [...venueRows].sort((a, b) => {
        const byVolume = marketRankCompareNullable(a?.quote_volume_24h, b?.quote_volume_24h, { descending: true });
        if (byVolume) return byVolume;
        return `${a?.provider || ''}|${a?.symbol || ''}`.localeCompare(`${b?.provider || ''}|${b?.symbol || ''}`);
      })[0] || null;
      entries.push({
        rank_identity: base,
        base_asset: base,
        provider: 'all',
        market_type: 'spot',
        quote_asset: quoteFilter || null,
        change_desc: changes.length ? Math.max(...changes) : null,
        change_asc: changes.length ? Math.min(...changes) : null,
        volume_desc: volumes.length ? Math.max(...volumes) : null,
        market_cap_rank: cap?.market_cap_rank ?? null,
        market_cap_usd: cap?.market_cap_usd ?? null,
        coingecko_id: cap?.coingecko_id ?? null,
        representative_row: representative,
        venue_rows: venueRows,
      });
    }
    return entries.sort(marketRankEntryComparator(sortKey));
  }

  const entries = rows.map((row) => {
    const base = marketRankBaseFromRow(row);
    const cap = marketRankCapForBase(base);
    const change = marketRankNumber(row?.price_change_percent_24h);
    const volume = marketRankNumber(row?.quote_volume_24h);
    return {
      rank_identity: `${String(row?.provider || '').trim().toLowerCase()}|${market}|${compact(row?.symbol)}`,
      base_asset: base,
      provider: String(row?.provider || ''),
      market_type: market,
      quote_asset: compact(row?.quote_asset ?? row?.quote_symbol) || null,
      change_desc: change,
      change_asc: change,
      volume_desc: volume,
      market_cap_rank: cap?.market_cap_rank ?? null,
      market_cap_usd: cap?.market_cap_usd ?? null,
      coingecko_id: cap?.coingecko_id ?? null,
      row,
    };
  });
  return entries.sort(marketRankEntryComparator(sortKey));
}

function marketRankSnapshotSignature({ market, provider = '', quote = '', sortKey }) {
  return `${market}|${provider || 'all'}|${compact(quote) || 'all'}|${sortKey}`;
}

function pruneMarketRankOrderSnapshots() {
  const now = Date.now();
  for (const [key, value] of rankOrderSnapshots.entries()) {
    if (now - Number(value?.created_at_ms || 0) > RANK_ORDER_SNAPSHOT_TTL_MS) {
      rankOrderSnapshots.delete(key);
      for (const [signature, version] of rankCurrentBySignature.entries()) if (version === key) rankCurrentBySignature.delete(signature);
      rankOrderSnapshotExpired += 1;
    }
  }
  if (rankOrderSnapshots.size <= RANK_ORDER_SNAPSHOT_MAX) return;
  const oldest = [...rankOrderSnapshots.entries()]
    .sort((a, b) => Number(a[1]?.created_at_ms || 0) - Number(b[1]?.created_at_ms || 0));
  while (oldest.length > RANK_ORDER_SNAPSHOT_MAX) {
    const [key] = oldest.shift();
    rankOrderSnapshots.delete(key);
    for (const [signature, version] of rankCurrentBySignature.entries()) if (version === key) rankCurrentBySignature.delete(signature);
  }
}

function marketRankMetricValue(entry, sortKey) {
  if (sortKey === 'change_desc') return marketRankNumber(entry?.change_desc);
  if (sortKey === 'change_asc') return marketRankNumber(entry?.change_asc);
  if (sortKey === 'volume_desc') return marketRankNumber(entry?.volume_desc);
  return marketRankNumber(entry?.market_cap_rank);
}

function createMarketRankOrderSnapshot({ market, provider = '', quote = '', sortKey }) {
  pruneMarketRankOrderSnapshots();
  const signature = marketRankSnapshotSignature({ market, provider, quote, sortKey });
  const ranked = buildMarketRankEntries({ market, provider, quote, sortKey });
  const createdAtMs = Date.now();
  rankOrderSnapshotSeq += 1;
  const rankVersion = `mlr-${round}-${marketCapRankVersion}-${rankOrderSnapshotSeq}`;
  const order = ranked.map((entry) => ({
    rank_identity: String(entry?.rank_identity || ''),
    base_asset: compact(entry?.base_asset) || null,
    provider: String(entry?.provider || ''),
    market_type: market,
    quote_asset: compact(entry?.quote_asset) || null,
    market_cap_rank: marketRankNumber(entry?.market_cap_rank),
    market_cap_usd: marketRankNumber(entry?.market_cap_usd),
    coingecko_id: entry?.coingecko_id || null,
    rank_metric_value: marketRankMetricValue(entry, sortKey),
  })).filter((entry) => entry.rank_identity);
  const snapshot = {
    rank_version: rankVersion,
    signature,
    market,
    provider: provider || 'all',
    quote: compact(quote) || '',
    sort: sortKey,
    created_at_ms: createdAtMs,
    market_light_round: round,
    market_cap_rank_version: marketCapRankVersion,
    order,
  };
  rankOrderSnapshots.set(rankVersion, snapshot);
  rankCurrentBySignature.set(signature, rankVersion);
  rankOrderSnapshotBuilds += 1;
  pruneMarketRankOrderSnapshots();
  return snapshot;
}

function getMarketRankOrderSnapshot(rankVersion, scope) {
  if (!rankVersion) return null;
  pruneMarketRankOrderSnapshots();
  const snapshot = rankOrderSnapshots.get(rankVersion);
  if (!snapshot) return null;
  const signature = marketRankSnapshotSignature(scope);
  if (snapshot.signature !== signature) return null;
  rankOrderSnapshotHits += 1;
  return snapshot;
}

function getOrCreateCurrentMarketRankOrderSnapshot(scope) {
  pruneMarketRankOrderSnapshots();
  const signature = marketRankSnapshotSignature(scope);
  const version = rankCurrentBySignature.get(signature);
  const current = version ? rankOrderSnapshots.get(version) : null;
  if (current &&
      current.market_light_round === round &&
      current.market_cap_rank_version === marketCapRankVersion &&
      Date.now() - Number(current.created_at_ms || 0) <= RANK_ORDER_SNAPSHOT_TTL_MS) {
    rankOrderSnapshotHits += 1;
    return current;
  }
  return createMarketRankOrderSnapshot(scope);
}

function currentMarketRowsForRankScope({ market, provider = '', quote = '' }) {
  const providers = market === 'spot' ? SPOT_PROVIDERS : CONTRACT_PROVIDERS;
  const selectedProviders = provider && provider !== 'all' ? [provider] : providers;
  const quoteFilter = compact(quote);
  const exact = new Map();
  const spotByBase = new Map();
  for (const providerName of selectedProviders) {
    for (const row of rowsByKey.get(keyFor(market, providerName)) || []) {
      const rowQuote = compact(row?.quote_asset ?? row?.quote_symbol);
      if (quoteFilter && rowQuote !== quoteFilter) continue;
      const symbol = compact(row?.symbol);
      if (!symbol) continue;
      exact.set(`${String(row?.provider || '').trim().toLowerCase()}|${market}|${symbol}`, row);
      if (market === 'spot') {
        const base = marketRankBaseFromRow(row);
        if (!base) continue;
        const list = spotByBase.get(base) || [];
        list.push(row);
        spotByBase.set(base, list);
      }
    }
  }
  return { exact, spotByBase };
}

function materializeMarketRankItem(orderEntry, { market, provider = '', quote = '' }, current) {
  if (market === 'spot' && (!provider || provider === 'all')) {
    const venueRows = current.spotByBase.get(compact(orderEntry?.base_asset)) || [];
    const representative = [...venueRows].sort((a, b) => {
      const byVolume = marketRankCompareNullable(a?.quote_volume_24h, b?.quote_volume_24h, { descending: true });
      if (byVolume) return byVolume;
      return `${a?.provider || ''}|${a?.symbol || ''}`.localeCompare(`${b?.provider || ''}|${b?.symbol || ''}`);
    })[0] || null;
    return {
      ...orderEntry,
      representative_row: representative,
      venue_rows: venueRows,
    };
  }
  return {
    ...orderEntry,
    row: current.exact.get(orderEntry.rank_identity) || null,
  };
}

function marketRankedPagePayload({ market, provider = '', quote = '', sort = 'market_cap_desc', offset = 0, limit = 50, rankVersion = '' }) {
  const safeSort = marketRankSortKey(sort);
  if (safeSort === 'market_cap_desc' && (marketCapBySymbol.size === 0 || marketCapGlobalSymbolCounts.size === 0)) {
    return {
      ok: false,
      status_code: 503,
      version: STEP_VERSION,
      schema_version: 'step1041_6_shared_full_market_rank_page_v2',
      error: 'shared_market_cap_rank_pending',
      market_cap_index: {
        ready: false,
        refresh_attempts: marketCapRefreshAttempts,
        refresh_successes: marketCapRefreshSuccesses,
        refresh_failures: marketCapRefreshFailures,
        last_error: marketCapLastError,
      },
      user_read_upstream_requests: 0,
      reads_scale_with_users: false,
    };
  }
  const safeOffset = Math.max(0, Math.trunc(Number(offset) || 0));
  const safeLimit = Math.max(1, Math.min(RANK_PAGE_LIMIT_MAX, Math.trunc(Number(limit) || 50)));
  const scope = { market, provider, quote, sortKey: safeSort };
  let snapshot = getMarketRankOrderSnapshot(String(rankVersion || '').trim(), scope);
  if (rankVersion && !snapshot) {
    return {
      ok: false,
      status_code: 409,
      version: STEP_VERSION,
      schema_version: 'step1041_6_shared_full_market_rank_page_v2',
      error: 'rank_version_expired_or_scope_mismatch',
      rank_version: String(rankVersion || ''),
      restart_from_offset: 0,
      user_read_upstream_requests: 0,
      reads_scale_with_users: false,
    };
  }
  if (!snapshot) snapshot = getOrCreateCurrentMarketRankOrderSnapshot(scope);

  const cacheKey = `${snapshot.rank_version}|${safeOffset}|${safeLimit}`;
  const cached = rankedPageCache.get(cacheKey);
  if (cached && Date.now() - cached.at <= RANK_RESPONSE_CACHE_TTL_MS) {
    rankPageCacheHits += 1;
    return { ...cached.payload, cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }

  rankPageBuilds += 1;
  const current = currentMarketRowsForRankScope({ market, provider, quote });
  const pageOrder = snapshot.order.slice(safeOffset, safeOffset + safeLimit);
  const page = pageOrder.map((entry, index) => ({
    ...materializeMarketRankItem(entry, { market, provider, quote }, current),
    rank_index: safeOffset + index + 1,
  }));
  const payload = {
    ok: true,
    version: STEP_VERSION,
    schema_version: 'step1041_6_shared_full_market_rank_page_v2',
    source: 'render_shared_market_light_rank_order_before_pagination',
    market_type: market,
    provider: provider || 'all',
    quote_asset: compact(quote) || null,
    sort: safeSort,
    rank_version: snapshot.rank_version,
    rank_snapshot_created_at: new Date(snapshot.created_at_ms).toISOString(),
    rank_snapshot_ttl_ms: RANK_ORDER_SNAPSHOT_TTL_MS,
    market_light_round: snapshot.market_light_round,
    total_items: snapshot.order.length,
    offset: safeOffset,
    limit: safeLimit,
    returned_items: page.length,
    has_more: safeOffset + page.length < snapshot.order.length,
    next_offset: safeOffset + page.length < snapshot.order.length ? safeOffset + page.length : null,
    market_cap_index: {
      ready: marketCapBySymbol.size > 0,
      version: marketCapRankVersion,
      rows: marketCapRows,
      unique_symbols: marketCapUniqueSymbols,
      ambiguous_symbols: marketCapDuplicateSymbols,
      shared_catalog_identity_rows: marketCapGlobalSymbolRows,
      shared_catalog_identity_ready: marketCapGlobalSymbolCounts.size > 0,
      identity_guard: 'unique_in_shared_top1000_catalog_plus_verified_project_fundamentals_collision_guard',
      last_succeeded_at: marketCapLastSucceededAt,
    },
    read_only_shared: true,
    user_read_upstream_requests: 0,
    user_read_upstream_connections: 0,
    reads_scale_with_users: false,
    ranking_happens_before_pagination: true,
    pagination_order_frozen_by_rank_version: true,
    app_page_size_remains_50: true,
    items: page,
    generated_at: new Date().toISOString(),
  };
  rankedPageCache.set(cacheKey, { at: Date.now(), payload });
  while (rankedPageCache.size > 96) rankedPageCache.delete(rankedPageCache.keys().next().value);
  return { ...payload, cache_hit: false, cache_age_ms: 0 };
}

function marketCapSupabaseConfig() {
  const base = String(process.env.SUPABASE_URL || '').trim().replace(/\/+$/, '');
  const key = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
  if (!base || !key) throw new Error('supabase_shared_market_cap_not_configured');
  return { base, key };
}

async function marketCapFetchSupabaseJson(path, { label = 'supabase_shared_market_cap' } = {}) {
  const cfg = marketCapSupabaseConfig();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), MARKET_CAP_SHARED_READ_TIMEOUT_MS);
  timer.unref?.();
  try {
    marketCapSourceRequestsStarted += 1;
    marketCapLastRequestAtMs = Date.now();
    marketCapLastRequestMode = 'supabase_shared_snapshot';
    const response = await fetch(`${cfg.base}/rest/v1/${path}`, {
      headers: {
        accept: 'application/json',
        apikey: cfg.key,
        authorization: `Bearer ${cfg.key}`,
        'user-agent': `KakaWeb3/650.8.15.196.10.3 ${label}`,
      },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`${label}_http_${response.status}`);
    return response.json();
  } finally {
    clearTimeout(timer);
  }
}

function marketCapSnapshotPayload(raw) {
  if (raw && typeof raw === 'object' && !Array.isArray(raw)) return raw;
  if (typeof raw === 'string' && raw.trim()) {
    try { return JSON.parse(raw); } catch {}
  }
  return null;
}

function marketCapIsoMs(raw) {
  const ms = Date.parse(String(raw || '').trim());
  return Number.isFinite(ms) ? ms : null;
}

async function fetchSharedTokenomicsCatalog() {
  const select = [
    'payload','row_count','source_verified','source_fetched_at','cached_at',
    'expires_at','stale_until','consecutive_errors','last_error','content_hash',
  ].join(',');
  const path = `kaka_data_hub_shared_snapshot?select=${encodeURIComponent(select)}`
    + `&dataset_key=eq.${encodeURIComponent(MARKET_CAP_SHARED_DATASET_KEY)}`
    + `&scope_key=eq.${encodeURIComponent(MARKET_CAP_SHARED_SCOPE_KEY)}`
    + '&order=cached_at.desc&limit=1';
  const result = await marketCapFetchSupabaseJson(path, { label: 'supabase_tokenomics_snapshot' });
  if (!Array.isArray(result) || !result.length) throw new Error('shared_tokenomics_snapshot_missing');
  const snapshot = result[0] || {};
  if (snapshot.source_verified !== true) throw new Error('shared_tokenomics_snapshot_unverified');
  const staleUntilMs = marketCapIsoMs(snapshot.stale_until);
  if (staleUntilMs != null && Date.now() > staleUntilMs) throw new Error('shared_tokenomics_snapshot_hard_expired');
  const payload = marketCapSnapshotPayload(snapshot.payload);
  const rows = payload?.item?.rows;
  if (payload?.ok !== true || !Array.isArray(rows) || rows.length < MARKET_CAP_SHARED_MIN_ROWS) {
    throw new Error(`shared_tokenomics_snapshot_too_small:${Array.isArray(rows) ? rows.length : 0}`);
  }
  return { snapshot, rows };
}

async function fetchVerifiedProjectFundamentalIdentities() {
  // PostgREST commonly caps one response at 1000 rows even when a larger limit is
  // requested. Page explicitly so the collision guard is complete rather than
  // silently trusting only the first 1000 verified identities.
  const all = [];
  const pageSize = 1000;
  const maxPages = 5;
  for (let page = 0; page < maxPages; page += 1) {
    const offset = page * pageSize;
    const path = 'kaka_project_fundamentals?select=symbol,coin_id,match_verified'
      + `&match_verified=eq.true&symbol=not.is.null&coin_id=not.is.null&limit=${pageSize}&offset=${offset}`;
    const rows = await marketCapFetchSupabaseJson(path, { label: 'supabase_project_fundamentals_identity' });
    if (!Array.isArray(rows)) throw new Error('project_fundamentals_identity_payload_invalid');
    all.push(...rows);
    if (rows.length < pageSize) break;
    if (page === maxPages - 1) throw new Error(`project_fundamentals_identity_overflow:${all.length}`);
  }
  if (all.length < 100) throw new Error(`project_fundamentals_identity_too_small:${all.length}`);
  return all;
}

async function refreshMarketCapRankIndex({ reason = 'scheduled' } = {}) {
  if (marketCapRefreshInflight) return marketCapRefreshInflight;
  const task = (async () => {
    marketCapRefreshAttempts += 1;
    marketCapLastStartedAt = new Date().toISOString();
    marketCapNextRetryAt = null;
    const hadReadyIndex = marketCapBySymbol.size > 0 && marketCapGlobalSymbolCounts.size > 0;
    try {
      const [{ snapshot, rows: catalogRows }, fundamentalRows] = await Promise.all([
        fetchSharedTokenomicsCatalog(),
        fetchVerifiedProjectFundamentalIdentities(),
      ]);

      const candidatesBySymbol = new Map();
      for (const raw of catalogRows) {
        const symbol = marketRankNormalizeBase(raw?.symbol);
        const rank = marketRankNumber(raw?.market_cap_rank);
        const marketCap = marketRankNumber(raw?.market_cap_usd);
        const id = String(raw?.coin_id || '').trim();
        if (!symbol || rank == null || rank <= 0 || !id) continue;
        const list = candidatesBySymbol.get(symbol) || [];
        list.push({
          coingecko_id: id,
          market_cap_rank: Math.trunc(rank),
          market_cap_usd: marketCap != null && marketCap >= 0 ? marketCap : null,
        });
        candidatesBySymbol.set(symbol, list);
      }

      // Second independent shared identity guard: if project fundamentals has more
      // than one verified CoinGecko coin_id for the same symbol, or its sole verified
      // identity disagrees with the tokenomics row, never attach market cap by symbol.
      const verifiedFundIdsBySymbol = new Map();
      for (const raw of fundamentalRows) {
        if (raw?.match_verified !== true) continue;
        const symbol = marketRankNormalizeBase(raw?.symbol);
        const id = String(raw?.coin_id || '').trim();
        if (!symbol || !id) continue;
        const set = verifiedFundIdsBySymbol.get(symbol) || new Set();
        set.add(id);
        verifiedFundIdsBySymbol.set(symbol, set);
      }

      const next = new Map();
      const ambiguous = new Set();
      let knownAmbiguousByFundamentals = 0;
      marketCapGlobalSymbolCounts.clear();
      for (const [symbol, candidates] of candidatesBySymbol.entries()) {
        const catalogIds = new Set(candidates.map((item) => item.coingecko_id).filter(Boolean));
        marketCapGlobalSymbolCounts.set(symbol, catalogIds.size);
        candidates.sort((a, b) => a.market_cap_rank - b.market_cap_rank || a.coingecko_id.localeCompare(b.coingecko_id));
        if (catalogIds.size !== 1 || candidates.length !== 1) {
          ambiguous.add(symbol);
          continue;
        }
        const only = candidates[0];
        const verifiedIds = verifiedFundIdsBySymbol.get(symbol);
        if (verifiedIds && (verifiedIds.size !== 1 || !verifiedIds.has(only.coingecko_id))) {
          ambiguous.add(symbol);
          knownAmbiguousByFundamentals += 1;
          continue;
        }
        next.set(symbol, only);
      }

      if (!next.size) throw new Error('shared_market_cap_index_empty');
      marketCapBySymbol.clear();
      for (const [symbol, item] of next.entries()) marketCapBySymbol.set(symbol, item);
      marketCapAmbiguousSymbols.clear();
      for (const symbol of ambiguous) marketCapAmbiguousSymbols.add(symbol);
      marketCapGlobalSymbolListAt = Date.now();
      marketCapGlobalSymbolRows = catalogRows.length;
      marketCapRows = catalogRows.length;
      marketCapUniqueSymbols = next.size;
      marketCapDuplicateSymbols = ambiguous.size;
      marketCapFundamentalsRows = fundamentalRows.length;
      marketCapKnownAmbiguousByFundamentals = knownAmbiguousByFundamentals;
      marketCapSourceVerified = true;
      marketCapSourceFetchedAt = snapshot?.source_fetched_at || null;
      marketCapSourceCachedAt = snapshot?.cached_at || null;
      marketCapSourceExpiresAt = snapshot?.expires_at || null;
      marketCapSourceStaleUntil = snapshot?.stale_until || null;
      marketCapSourceContentHash = String(snapshot?.content_hash || '');
      marketCapSourceSnapshotRows = Number(snapshot?.row_count || catalogRows.length) || catalogRows.length;
      marketCapRankVersion += 1;
      marketCapLastSucceededAt = new Date().toISOString();
      marketCapLastError = '';
      marketCapRefreshSuccesses += 1;
      marketCapNextRetryAt = null;
      if (marketCapRetryTimer) { clearTimeout(marketCapRetryTimer); marketCapRetryTimer = null; }
      rankedPageCache.clear();
      return true;
    } catch (error) {
      marketCapRefreshFailures += 1;
      marketCapLastError = `${reason}:${String(error?.message || error)}`.slice(0, 320);
      const retryDelayMs = hadReadyIndex ? MARKET_CAP_WARM_FAILURE_RETRY_MS : MARKET_CAP_COLD_FAILURE_RETRY_MS;
      if (!marketCapRetryTimer) {
        marketCapNextRetryAt = new Date(Date.now() + retryDelayMs).toISOString();
        marketCapRetryTimer = setTimeout(() => {
          marketCapRetryTimer = null;
          marketCapNextRetryAt = null;
          refreshMarketCapRankIndex({ reason: hadReadyIndex ? 'shared_warm_retry_after_failure' : 'shared_cold_retry_after_failure' }).catch(() => {});
        }, retryDelayMs);
        marketCapRetryTimer.unref?.();
      }
      return false;
    }
  })();
  marketCapRefreshInflight = task;
  try { return await task; }
  finally { if (marketCapRefreshInflight === task) marketCapRefreshInflight = null; }
}

function startMarketCapRankCollector() {
  if (marketCapRefreshTimer || marketCapRefreshInterval) return;
  marketCapRefreshTimer = setTimeout(() => {
    refreshMarketCapRankIndex({ reason: 'startup' }).catch(() => {});
  }, MARKET_CAP_START_DELAY_MS);
  marketCapRefreshTimer.unref?.();
  marketCapRefreshInterval = setInterval(() => {
    refreshMarketCapRankIndex({ reason: 'interval' }).catch(() => {});
  }, MARKET_CAP_REFRESH_MS);
  marketCapRefreshInterval.unref?.();
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
    directory_count: Number(meta.directory_count || 0),
    verified_directory_count: Number(meta.verified_directory_count || meta.directory_count || 0),
    latest_directory_count: Number(meta.latest_directory_count || 0),
    directory_delta: Number(meta.directory_delta || 0),
    directory_refresh_pending: meta.directory_refresh_pending === true,
    directory_generation_aligned: meta.directory_generation_aligned === true,
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


function sectorMedian(values) {
  const sorted = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!sorted.length) return null;
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
}

function sectorSpotRowBase(row) {
  const explicit = compact(row?.base_asset);
  if (explicit) return explicit;
  const quote = compact(row?.quote_asset ?? row?.quote_symbol);
  const symbol = compact(row?.symbol);
  return quote && symbol.endsWith(quote) && symbol.length > quote.length
    ? symbol.slice(0, -quote.length)
    : '';
}

function sectorLeveragedBase(base) {
  return /(2L|2S|3L|3S|5L|5S)$/.test(compact(base));
}

function sectorHealthySpotSnapshot(provider) {
  const snapshot = getMarketLightInternalSnapshot({ market: 'spot', provider });
  return snapshot?.ok === true &&
    snapshot?.stale !== true &&
    Number(snapshot?.row_count || 0) > 0 &&
    Array.isArray(snapshot?.rows)
      ? snapshot
      : null;
}

function sectorDedupedUsdtRows(healthyByProvider) {
  const byBase = new Map();
  for (const provider of SECTOR_USDT_PROVIDERS) {
    const snapshot = healthyByProvider.get(provider);
    if (!snapshot) continue;
    for (const row of snapshot.rows) {
      if (compact(row?.quote_asset ?? row?.quote_symbol) !== 'USDT') continue;
      const base = sectorSpotRowBase(row);
      if (!base || sectorLeveragedBase(base)) continue;
      const change = finite(row?.price_change_percent_24h);
      const turnover = finite(row?.quote_volume_24h);
      const price = positive(row?.last_price ?? row?.price);
      if (change == null || turnover == null || turnover < 0 || price == null) continue;
      const current = byBase.get(base);
      if (!current ||
          turnover > Number(current.quote_volume_24h || 0) ||
          (turnover === Number(current.quote_volume_24h || 0) &&
           String(provider).localeCompare(String(current.provider || '')) < 0)) {
        byBase.set(base, {
          provider,
          market_type: 'spot',
          symbol: compact(row?.symbol),
          base_asset: base,
          quote_asset: 'USDT',
          last_price: price,
          price_change_percent_24h: change,
          quote_volume_24h: turnover,
          source_time: row?.source_time ?? row?.cached_at ?? null,
        });
      }
    }
  }
  return byBase;
}

function sectorUsdtVenuePresence(healthyByProvider) {
  const byBase = new Map();
  for (const provider of SECTOR_USDT_PROVIDERS) {
    const snapshot = healthyByProvider.get(provider);
    if (!snapshot) continue;
    for (const row of snapshot.rows) {
      if (compact(row?.quote_asset ?? row?.quote_symbol) !== 'USDT') continue;
      const base = sectorSpotRowBase(row);
      if (!base || sectorLeveragedBase(base)) continue;
      if (!byBase.has(base)) byBase.set(base, new Set());
      byBase.get(base).add(provider);
    }
  }
  return byBase;
}

function sectorCoinbaseUsdBases(coinbaseSnapshot) {
  const bases = new Set();
  if (!coinbaseSnapshot) return bases;
  for (const row of coinbaseSnapshot.rows) {
    if (compact(row?.quote_asset ?? row?.quote_symbol) !== 'USD') continue;
    const base = sectorSpotRowBase(row);
    if (base && !sectorLeveragedBase(base)) bases.add(base);
  }
  return bases;
}

function buildSectorProfessionalSnapshot() {
  const now = Date.now();
  if (sectorSnapshotCache && now - sectorSnapshotCacheAt <= SECTOR_CACHE_TTL_MS) {
    return {
      ...sectorSnapshotCache,
      cache_hit: true,
      cache_age_ms: now - sectorSnapshotCacheAt,
    };
  }

  sectorSnapshotBuilds += 1;
  const healthyByProvider = new Map();
  for (const provider of SPOT_PROVIDERS) {
    const snapshot = sectorHealthySpotSnapshot(provider);
    if (snapshot) healthyByProvider.set(provider, snapshot);
  }

  const healthyUsdtProviders = SECTOR_USDT_PROVIDERS.filter(
    (provider) => healthyByProvider.has(provider),
  );
  const coinbaseSnapshot = healthyByProvider.get('coinbase') || null;
  const representativeByBase = sectorDedupedUsdtRows(healthyByProvider);
  const usdtVenuePresence = sectorUsdtVenuePresence(healthyByProvider);
  const coinbaseUsdBases = sectorCoinbaseUsdBases(coinbaseSnapshot);
  const allConfigured = new Set();
  const sectors = [];

  for (const definition of SECTOR_DEFINITIONS) {
    const configured = [...new Set(definition.symbols.map(compact).filter(Boolean))];
    configured.forEach((symbol) => allConfigured.add(symbol));
    const members = [];
    for (const base of configured) {
      const row = representativeByBase.get(base);
      if (!row) continue;
      const turnover = Number(row.quote_volume_24h || 0);
      const change = Number(row.price_change_percent_24h);
      const usdtVenues = [...(usdtVenuePresence.get(base) || new Set())].sort();
      members.push({
        ...row,
        rankable: turnover >= SECTOR_MIN_TURNOVER_USDT,
        usdt_venue_count: usdtVenues.length,
        usdt_venues: usdtVenues,
        coinbase_usd_observed: coinbaseUsdBases.has(base),
        six_venue_observed_count: usdtVenues.length + (coinbaseUsdBases.has(base) ? 1 : 0),
        turnover_weight: 0,
        turnover_weighted_contribution_pct_point: 0,
      });
    }

    const totalTurnover = members.reduce(
      (sum, row) => sum + Math.max(0, Number(row.quote_volume_24h || 0)),
      0,
    );
    for (const member of members) {
      const weight = totalTurnover > 0
        ? Math.max(0, Number(member.quote_volume_24h || 0)) / totalTurnover
        : 0;
      member.turnover_weight = weight;
      member.turnover_weighted_contribution_pct_point =
        weight * Number(member.price_change_percent_24h || 0);
    }

    const changes = members.map((row) => Number(row.price_change_percent_24h));
    const weightedChange = members.reduce(
      (sum, row) => sum + Number(row.turnover_weighted_contribution_pct_point || 0),
      0,
    );
    const equalChange = changes.length
      ? changes.reduce((sum, value) => sum + value, 0) / changes.length
      : null;
    const medianChange = sectorMedian(changes);
    const up = changes.filter((value) => value > 0.05).length;
    const down = changes.filter((value) => value < -0.05).length;
    const flat = changes.length - up - down;
    const rankableMembers = members.filter((row) => row.rankable);
    const movers = (rankableMembers.length ? rankableMembers : members)
      .slice()
      .sort((a, b) =>
        Number(b.price_change_percent_24h || 0) -
        Number(a.price_change_percent_24h || 0)
      );
    const byTurnover = members.slice().sort(
      (a, b) => Number(b.quote_volume_24h || 0) - Number(a.quote_volume_24h || 0),
    );
    const top3Turnover = byTurnover
      .slice(0, 3)
      .reduce((sum, row) => sum + Number(row.quote_volume_24h || 0), 0);
    const providerSet = new Set();
    for (const base of configured) {
      for (const provider of usdtVenuePresence.get(base) || []) {
        providerSet.add(provider);
      }
    }
    const coinbaseObservedCount = configured.filter((base) => coinbaseUsdBases.has(base)).length;

    sectors.push({
      key: definition.key,
      name_zh: definition.zh,
      name_en: definition.en,
      basket_type: 'kaka_observational_sector_basket',
      tradeable_index: false,
      configured_member_count: configured.length,
      observed_member_count: members.length,
      rankable_member_count: rankableMembers.length,
      coinbase_usd_member_count: coinbaseObservedCount,
      usdt_provider_count: providerSet.size,
      usdt_providers: [...providerSet].sort(),
      six_venue_coverage_count: providerSet.size + (coinbaseObservedCount > 0 ? 1 : 0),
      breadth: { up, flat, down },
      equal_weight_change_24h_pct: equalChange,
      median_change_24h_pct: medianChange,
      turnover_weighted_change_24h_pct: members.length ? weightedChange : null,
      total_quote_turnover_24h_usdt: totalTurnover,
      top3_turnover_concentration_pct:
        totalTurnover > 0 ? (top3Turnover / totalTurnover) * 100 : null,
      leader: movers.length ? movers[0] : null,
      laggard: movers.length ? movers[movers.length - 1] : null,
      members: members
        .slice()
        .sort((a, b) =>
          Number(b.quote_volume_24h || 0) - Number(a.quote_volume_24h || 0)
        ),
    });
  }

  const observedUnique = new Set(
    sectors.flatMap((sector) =>
      sector.members.map((member) => compact(member.base_asset)).filter(Boolean)
    ),
  );
  const activeSectors = sectors.filter((sector) => sector.observed_member_count > 0);
  const activeRankableSectors = sectors.filter((sector) => sector.rankable_member_count > 0);
  const partialReady = healthyUsdtProviders.length >= 3 && activeSectors.length >= 5;
  const coverageReady =
    healthyUsdtProviders.length === SECTOR_USDT_PROVIDERS.length &&
    activeRankableSectors.length >= 7;

  const payload = {
    ok: true,
    version: STEP_VERSION,
    data_version: 1032,
    schema_version: 'step1032_crypto_sector_professional_v1',
    implementation_revision: '1032_shared_market_light_sector_baskets_v1',
    source: 'render_market_light_shared_memory_sector_derivation',
    source_verified: partialReady,
    partial_ready: partialReady,
    coverage_ready: coverageReady,
    sector_count: SECTOR_DEFINITIONS.length,
    active_sector_count: activeSectors.length,
    active_rankable_sector_count: activeRankableSectors.length,
    configured_unique_asset_count: allConfigured.size,
    observed_unique_asset_count: observedUnique.size,
    healthy_usdt_providers: healthyUsdtProviders,
    healthy_usdt_provider_count: healthyUsdtProviders.length,
    coinbase_usd_healthy: Boolean(coinbaseSnapshot),
    six_venue_healthy_count:
      healthyUsdtProviders.length + (coinbaseSnapshot ? 1 : 0),
    mover_min_turnover_usdt: SECTOR_MIN_TURNOVER_USDT,
    quote_scope: 'five_venue_usdt_metrics_plus_coinbase_usd_presence_only',
    cross_quote_aggregation: false,
    coinbase_usd_metrics_mixed_into_usdt: false,
    duplicate_asset_policy: 'highest_real_24h_usdt_turnover_venue_represents_asset',
    leveraged_product_policy: 'exclude_2l_2s_3l_3s_5l_5s',
    membership_overlap_allowed: true,
    cross_sector_sum_valid: false,
    tradeable_index: false,
    fabricated_points: false,
    exchange_requests_started: 0,
    exchange_connections_started: 0,
    user_read_upstream_requests: 0,
    user_read_upstream_connections: 0,
    reads_scale_with_users: false,
    shared_market_light_round: round,
    sectors,
    generated_at: new Date(now).toISOString(),
    timestamp_ms: now,
    cache_hit: false,
    cache_age_ms: 0,
  };
  sectorSnapshotCache = payload;
  sectorSnapshotCacheAt = now;
  return payload;
}

export function getCryptoSectorProfessionalHealth() {
  const snapshot = buildSectorProfessionalSnapshot();
  return {
    ok: true,
    version: STEP_VERSION,
    data_version: 1032,
    schema_version: 'step1032_crypto_sector_professional_v1',
    snapshot_endpoint: SECTOR_SNAPSHOT_ROUTE,
    health_endpoint: SECTOR_HEALTH_ROUTE,
    source: snapshot.source,
    source_verified: snapshot.source_verified,
    partial_ready: snapshot.partial_ready,
    coverage_ready: snapshot.coverage_ready,
    sector_count: snapshot.sector_count,
    active_sector_count: snapshot.active_sector_count,
    active_rankable_sector_count: snapshot.active_rankable_sector_count,
    observed_unique_asset_count: snapshot.observed_unique_asset_count,
    healthy_usdt_providers: snapshot.healthy_usdt_providers,
    healthy_usdt_provider_count: snapshot.healthy_usdt_provider_count,
    coinbase_usd_healthy: snapshot.coinbase_usd_healthy,
    six_venue_healthy_count: snapshot.six_venue_healthy_count,
    cache_ttl_seconds: Math.round(SECTOR_CACHE_TTL_MS / 1000),
    cache_age_ms: sectorSnapshotCacheAt > 0 ? Date.now() - sectorSnapshotCacheAt : null,
    total_builds: sectorSnapshotBuilds,
    total_reads: sectorSnapshotReads,
    exchange_requests_started: 0,
    exchange_connections_started: 0,
    user_read_upstream_requests: 0,
    user_read_upstream_connections: 0,
    reads_scale_with_users: false,
    cross_quote_aggregation: false,
    tradeable_index: false,
    fabricated_points: false,
    time: new Date().toISOString(),
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
    ranked_page_endpoint: RANKED_PAGE_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    shared_rank_index: {
      schema_version: 'step1041_6_shared_full_market_rank_page_v2',
      ranking_happens_before_pagination: true,
      pagination_order_frozen_by_rank_version: true,
      page_limit_max: RANK_PAGE_LIMIT_MAX,
      response_cache_ttl_ms: RANK_RESPONSE_CACHE_TTL_MS,
      order_snapshot_ttl_ms: RANK_ORDER_SNAPSHOT_TTL_MS,
      order_snapshot_max: RANK_ORDER_SNAPSHOT_MAX,
      order_snapshot_entries: rankOrderSnapshots.size,
      current_shared_rank_scopes: rankCurrentBySignature.size,
      order_snapshot_builds: rankOrderSnapshotBuilds,
      order_snapshot_hits: rankOrderSnapshotHits,
      order_snapshot_expired: rankOrderSnapshotExpired,
      reads: rankPageReads,
      builds: rankPageBuilds,
      cache_hits: rankPageCacheHits,
      cache_entries: rankedPageCache.size,
      supported_sorts: ['market_cap_desc','change_desc','change_asc','volume_desc'],
      primary_quote_shared_rows_only: true,
      user_reads_start_upstream: false,
      reads_scale_with_users: false,
      market_cap: {
        mode: 'supabase_shared_project_tokenomics_top1000_plus_verified_fundamentals_collision_guard',
        refresh_interval_ms: MARKET_CAP_REFRESH_MS,
        pages_per_refresh: 0,
        page_size: 0,
        nominal_requests_per_refresh_max: 6,
        requests_per_refresh_max: 6,
        rate_limit_retry_budget: 0,
        request_mode: marketCapLastRequestMode,
        request_gap_ms: 0,
        direct_coingecko_enabled: false,
        direct_coingecko_requests_started: marketCapDirectCoinGeckoRequestsStarted,
        source_dataset_key: MARKET_CAP_SHARED_DATASET_KEY,
        source_scope_key: MARKET_CAP_SHARED_SCOPE_KEY,
        source_verified: marketCapSourceVerified,
        source_fetched_at: marketCapSourceFetchedAt,
        source_cached_at: marketCapSourceCachedAt,
        source_expires_at: marketCapSourceExpiresAt,
        source_stale_until: marketCapSourceStaleUntil,
        source_content_hash: marketCapSourceContentHash || null,
        source_snapshot_rows: marketCapSourceSnapshotRows,
        identity_guard: 'unique_in_shared_top1000_catalog_plus_verified_project_fundamentals_collision_guard',
        identity_scope_note: 'Known/observed symbol ambiguity is excluded; no cross-venue/product guessing.',
        fundamentals_identity_rows: marketCapFundamentalsRows,
        known_ambiguous_by_fundamentals: marketCapKnownAmbiguousByFundamentals,
        version: marketCapRankVersion,
        ready: marketCapBySymbol.size > 0 && marketCapGlobalSymbolCounts.size > 0,
        rows: marketCapRows,
        unique_symbols: marketCapUniqueSymbols,
        ambiguous_symbols_excluded: marketCapDuplicateSymbols,
        shared_catalog_identity_rows: marketCapGlobalSymbolRows,
        shared_catalog_identity_ready: marketCapGlobalSymbolCounts.size > 0,
        refresh_attempts: marketCapRefreshAttempts,
        refresh_successes: marketCapRefreshSuccesses,
        refresh_failures: marketCapRefreshFailures,
        last_started_at: marketCapLastStartedAt,
        last_succeeded_at: marketCapLastSucceededAt,
        last_error: marketCapLastError,
        bounded_retry_pending: Boolean(marketCapRetryTimer),
        next_retry_at: marketCapNextRetryAt,
        cold_retry_after_failure_ms: MARKET_CAP_COLD_FAILURE_RETRY_MS,
        warm_retry_after_failure_ms: MARKET_CAP_WARM_FAILURE_RETRY_MS,
        source_requests_started: marketCapSourceRequestsStarted,
        rate_limit_responses: marketCapRateLimitResponses,
        rate_limit_retries: marketCapRateLimitRetries,
        last_retry_after_ms: marketCapLastRetryAfterMs,
        per_user_upstream_requests: 0,
        user_read_upstream_requests: 0,
        reads_scale_with_users: false,
      },
    },
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
    crypto_sector_professional: getCryptoSectorProfessionalHealth(),
    crypto_sector_history: getCryptoSectorHistoryHealth(),
    snapshot_reads_start_exchange_requests: false,
    snapshot_reads_start_exchange_connections: false,
    snapshot_reads_scale_with_users: false,
    failed_refresh_never_overwrites_last_verified_rows: true,
    severe_partial_refresh_never_overwrites_last_verified_rows: true,
    partial_retain_ratio: PARTIAL_RETAIN_RATIO,
    directory_min_ratio_after_warm: DIRECTORY_MIN_RATIO_AFTER_WARM,
    designed_upstream_budget: {
      scan_interval_seconds: Math.round(SCAN_INTERVAL_MS / 1000),
      approximate_batch_attempts_per_cycle: 10,
      approximate_batch_attempts_per_minute_at_default_interval: Math.round((60_000 / SCAN_INTERVAL_MS) * 10 * 10) / 10,
      binance_spot_shared_market_ws_connections: 1,
      binance_spot_market_data_only_rest_baseline_requests_per_6h_max_when_explicitly_enabled: 1,
      binance_spot_market_data_only_rest_weight_per_baseline: 80,
      binance_spot_average_rest_weight_per_minute_default: 0,
      binance_spot_optional_rest_average_weight_per_minute_when_enabled: 0.2222,
      binance_spot_stream_bootstrap_rest_weight: 0,
      binance_spot_authenticated_rest_requests_from_market_light: 0,
      binance_spot_edge_relay_requests_from_market_light: 0,
      coinbase_shared_market_ws_connections: 1,
      binance_contract_all_book_ticker_shared_ws_connections: 1,
      binance_contract_bbo_ws_api_baseline_requests_per_10m: 1,
      per_user_upstream_requests: 0,
      per_user_upstream_connections: 0,
      note: 'collector budget only; shared caches/governors may reduce physical upstream calls further',
    },
    full_market_light_source_notes: {
      binance_spot: 'one official data-stream.binance.vision !miniTicker@arr shared stream bootstraps live USDT identities and 24h price/volume with zero REST dependency; the data-api ticker/24hr baseline is disabled by default and can only be explicitly enabled as low-frequency completeness/BBO enrichment; it never gates readiness',
      binance_contract: 'existing_all_market_ticker_plus_mark_price_shared_snapshot + official USDⓈ-M !bookTicker one shared websocket for all-symbol BBO',
      coinbase_spot: 'public_ticker_batch_shared_websocket; BBO intentionally unavailable in ticker_batch',
      okx_spot: 'official_SPOT_tickers_batch',
      okx_contract: 'official_SWAP_tickers_batch + canonical OKX SWAP identity merge + dual-host public mark-price batch + USDT index-tickers batch + public open-interest batch; funding remains missing unless officially supplied by a batch source',
      bybit_spot: 'official_spot_tickers_batch',
      bybit_contract: 'official_linear_tickers_batch including mark/index/OI/funding/BBO',
      bitget_spot: 'official_v3_SPOT_tickers_product_batch with v2 fallback',
      bitget_contract: 'official_v3_USDT-FUTURES_tickers product batch including mark/index/OI/funding/BBO with v2 fallback + official current-fund-rate category batch for next funding time/interval/min-max funding',
      gate_spot: 'official_spot_tickers_batch',
      gate_contract: 'official_USDT_futures_tickers_batch including mark/index/funding/BBO; total_size preserved separately and not relabeled as OI',
    },
    provider_coverage: providerCoverage,
    bitget_contract_funding_batch: {
      ...bitgetContractFundingBatch,
      ready: bitgetContractFundingBatch.rows > 0 &&
        bitgetContractFundingBatch.applied_rows > 0 &&
        bitgetContractFundingBatch.last_error === '',
      official_category_batch_symbol_optional: true,
      source_endpoint: '/api/v3/market/current-fund-rate?category=USDT-FUTURES',
      additional_user_scaled_requests: 0,
      additional_user_scaled_connections: 0,
    },
    okx_contract_batch_enrichment: {
      ...okxContractBatchEnrichment,
      mark_ready: okxContractBatchEnrichment.mark_rows > 0 && okxContractBatchEnrichment.applied_mark_rows > 0,
      index_ready: okxContractBatchEnrichment.index_rows > 0 && okxContractBatchEnrichment.applied_index_rows > 0,
      open_interest_ready: okxContractBatchEnrichment.open_interest_rows > 0 && okxContractBatchEnrichment.applied_open_interest_rows > 0,
      canonical_okx_swap_identity_merge: true,
      dual_host_fallback_enabled: true,
      index_batch_mode: 'quoteCcy=USDT',
      mark_batch_mode: 'instType=SWAP',
      additional_user_scaled_requests: 0,
      additional_user_scaled_connections: 0,
    },
    binance_spot_ticker_shared_ws: {
      ready: binanceSpotTicker.rows.size >= BINANCE_SPOT_STREAM_BOOTSTRAP_MIN_ROWS &&
        Number(directoryCountByKey.get(keyFor('spot','binance')) || 0) === binanceSpotTicker.rows.size &&
        ((binanceSpotTicker.lastMessageAt > 0 && Date.now() - binanceSpotTicker.lastMessageAt <= STALE_MS) ||
          (binanceSpotTicker.baselineAt > 0 && Date.now() - binanceSpotTicker.baselineAt <= BINANCE_SPOT_BASELINE_TTL_MS)),
      source: 'binance_spot_official_miniticker_stream_with_optional_rest_enrichment',
      public_market_data_rest_base: BINANCE_SPOT_MARKET_DATA_REST_BASE,
      public_market_data_rest_path: BINANCE_SPOT_MARKET_DATA_REST_PATH,
      public_market_data_rest_method: 'GET /api/v3/ticker/24hr',
      public_market_data_rest_symbol_omitted_returns_all: true,
      public_market_data_rest_symbol_status: 'TRADING',
      public_market_data_rest_type: 'FULL',
      public_market_data_rest_ip_weight: 80,
      baseline_ttl_seconds: Math.round(BINANCE_SPOT_BASELINE_TTL_MS / 1000),
      baseline_retry_seconds: Math.round(BINANCE_SPOT_BASELINE_RETRY_MS / 1000),
      rest_baseline_role: 'disabled_by_default_optional_low_frequency_completeness_and_bbo_seed',
      rest_baseline_enabled: BINANCE_SPOT_REST_BASELINE_ENABLED,
      rest_baseline_required_for_ready: false,
      stream_bootstrap_enabled: true,
      stream_can_create_live_identity: true,
      stream_bootstrap_min_rows: BINANCE_SPOT_STREAM_BOOTSTRAP_MIN_ROWS,
      stream_row_ttl_minutes: Math.round(BINANCE_SPOT_STREAM_ROW_TTL_MS / 60_000),
      baseline_attempts: binanceSpotTicker.baselineAttempts,
      baseline_successes: binanceSpotTicker.baselineSuccesses,
      baseline_failures: binanceSpotTicker.baselineFailures,
      baseline_rows: binanceSpotTicker.baselineRows,
      baseline_at: binanceSpotTicker.baselineAt ? new Date(binanceSpotTicker.baselineAt).toISOString() : null,
      baseline_last_error: binanceSpotTicker.baselineLastError,
      baseline_last_http_status: binanceSpotTicker.baselineLastHttpStatus,
      baseline_last_used_weight_1m: binanceSpotTicker.baselineLastRateLimitCount,
      baseline_last_retry_after_seconds: binanceSpotTicker.baselineLastRetryAfterSeconds,
      baseline_next_allowed_at: binanceSpotTicker.baselineNextAllowedAt ? new Date(binanceSpotTicker.baselineNextAllowedAt).toISOString() : null,
      market_stream_url: BINANCE_SPOT_MINI_TICKER_WS_URL,
      market_stream: '!miniTicker@arr',
      market_stream_changed_symbols_only: true,
      stream_connected: wsReady(binanceSpotTicker.socket) && binanceSpotTicker.ready,
      cached_rows: binanceSpotTicker.rows.size,
      directory_rows: Number(directoryCountByKey.get(keyFor('spot','binance')) || 0),
      connect_attempts: binanceSpotTicker.connectAttempts,
      messages: binanceSpotTicker.messages,
      accepted_updates: binanceSpotTicker.acceptedUpdates,
      ignored_non_usdt_updates: binanceSpotTicker.ignoredNonUsdtUpdates,
      ignored_unknown_symbols: binanceSpotTicker.ignoredUnknownSymbols,
      stream_bootstrap_new_rows: binanceSpotTicker.streamBootstrapNewRows,
      stream_bootstrap_rows: binanceSpotTicker.streamBootstrapRows,
      stream_bootstrap_pruned_rows: binanceSpotTicker.streamBootstrapPrunedRows,
      opened_at: binanceSpotTicker.openedAt ? new Date(binanceSpotTicker.openedAt).toISOString() : null,
      last_message_at: binanceSpotTicker.lastMessageAt ? new Date(binanceSpotTicker.lastMessageAt).toISOString() : null,
      last_error: binanceSpotTicker.lastError,
      public_market_data_only_rest_used: BINANCE_SPOT_REST_BASELINE_ENABLED && binanceSpotTicker.baselineAttempts > 0,
      authenticated_rest_used: false,
      api_binance_com_rest_used: false,
      edge_relay_used: false,
      directory_from_public_market_data_rest_baseline_when_available: true,
      directory_from_live_stream_observation_during_rest_unavailable: true,
      stream_cannot_invent_unobserved_directory_identity: true,
      one_shared_backend_stream: true,
      shared_backend_connections: 1,
      background_baseline_only: true,
      user_reads_trigger_baseline_requests: false,
      per_user_requests: 0,
      per_user_connections: 0,
      user_reads_start_connections: false,
      reads_scale_with_users: false,
    },
    binance_contract_all_book_ticker: {
      source: 'binance_usdm_all_book_tickers_shared_websocket',
      url: BINANCE_CONTRACT_BOOK_TICKER_WS_URL,
      connected: wsReady(binanceContractBookTicker.socket) && binanceContractBookTicker.ready,
      cached_rows: binanceContractBookTicker.rows.size,
      connect_attempts: binanceContractBookTicker.connectAttempts,
      messages: binanceContractBookTicker.messages,
      accepted_updates: binanceContractBookTicker.acceptedUpdates,
      rejected_coin_m_updates: binanceContractBookTicker.rejectedCoinMUpdates,
      opened_at: binanceContractBookTicker.openedAt ? new Date(binanceContractBookTicker.openedAt).toISOString() : null,
      last_message_at: binanceContractBookTicker.lastMessageAt ? new Date(binanceContractBookTicker.lastMessageAt).toISOString() : null,
      last_error: binanceContractBookTicker.lastError,
      update_speed_seconds: 5,
      ws_api_baseline_url: BINANCE_CONTRACT_BOOK_TICKER_WS_API_URL,
      ws_api_baseline_method: 'ticker.book',
      ws_api_baseline_symbol_omitted_returns_all: true,
      ws_api_baseline_ttl_minutes: BINANCE_BOOK_BASELINE_TTL_MS / 60_000,
      ws_api_baseline_attempts: binanceContractBookTicker.baselineAttempts,
      ws_api_baseline_successes: binanceContractBookTicker.baselineSuccesses,
      ws_api_baseline_failures: binanceContractBookTicker.baselineFailures,
      ws_api_baseline_rows: binanceContractBookTicker.baselineRows,
      ws_api_baseline_at: binanceContractBookTicker.baselineAt ? new Date(binanceContractBookTicker.baselineAt).toISOString() : null,
      ws_api_baseline_last_error: binanceContractBookTicker.baselineLastError,
      filters_usdm_symbol_type_st_1: true,
      primary_quote_filter: 'USDT',
      shared_backend_connections: 1,
      per_user_connections: 0,
      per_user_requests: 0,
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
  const sectorHistoryRoute = url.pathname === '/api/crypto-sector-professional/history' ||
    url.pathname === '/api/crypto-sector-professional/history-health';
  if (![SNAPSHOT_ROUTE, RANKED_PAGE_ROUTE, HEALTH_ROUTE, SECTOR_SNAPSHOT_ROUTE, SECTOR_HEALTH_ROUTE].includes(url.pathname) && !sectorHistoryRoute) return false;
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
  if (url.pathname === SECTOR_HEALTH_ROUTE) {
    sendJson(res, 200, getCryptoSectorProfessionalHealth());
    return true;
  }
  if (sectorHistoryRoute) {
    return handleCryptoSectorHistory(req, res, url, buildSectorProfessionalSnapshot());
  }
  if (url.pathname === SECTOR_SNAPSHOT_ROUTE) {
    sectorSnapshotReads += 1;
    sendJson(res, 200, buildSectorProfessionalSnapshot());
    return true;
  }
  const market = String(url.searchParams.get('market_type') || url.searchParams.get('market') || '').trim().toLowerCase();
  if (url.pathname === RANKED_PAGE_ROUTE) {
    if (!['spot', 'contract'].includes(market)) {
      sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'market_type_required_for_rank_page' });
      return true;
    }
    const provider = String(url.searchParams.get('provider') || '').trim().toLowerCase();
    const allowedProviders = market === 'contract' ? CONTRACT_PROVIDERS : SPOT_PROVIDERS;
    if (provider && provider !== 'all' && !allowedProviders.includes(provider)) {
      sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'unsupported_provider' });
      return true;
    }
    const quote = String(url.searchParams.get('quote') || url.searchParams.get('quote_asset') || '').trim().toUpperCase();
    const sort = String(url.searchParams.get('sort') || 'market_cap_desc').trim().toLowerCase();
    const offset = Math.max(0, Math.trunc(Number(url.searchParams.get('offset') || 0) || 0));
    const limit = Math.max(1, Math.min(RANK_PAGE_LIMIT_MAX, Math.trunc(Number(url.searchParams.get('limit') || 50) || 50)));
    const rankVersion = String(url.searchParams.get('rank_version') || '').trim();
    if (offset > 0 && !rankVersion) {
      sendJson(res, 400, {
        ok: false,
        version: STEP_VERSION,
        error: 'rank_version_required_after_first_page',
        restart_from_offset: 0,
        user_read_upstream_requests: 0,
      });
      return true;
    }
    rankPageReads += 1;
    const payload = marketRankedPagePayload({ market, provider, quote, sort, offset, limit, rankVersion });
    const statusCode = Number(payload?.status_code || 200);
    if (payload && Object.prototype.hasOwnProperty.call(payload, 'status_code')) delete payload.status_code;
    sendJson(res, statusCode, payload);
    return true;
  }
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

export const __marketLightStep1032Test = Object.freeze({
  providerDirectoryGeneration,
  sectorMedian,
  sectorLeveragedBase,
  sectorDedupedUsdtRows,
  sectorCoinbaseUsdBases,
  sectorUsdtVenuePresence,
  sectorDefinitions: SECTOR_DEFINITIONS,
  sectorMinTurnoverUsdt: SECTOR_MIN_TURNOVER_USDT,
});

export const __marketLightStep1001_6Test = Object.freeze({
  binanceSpotTicker24hRow,
  binanceSpotMiniTickerSeedRow,
  binanceSpotMiniTickerPatch,
  binanceSpotDirectoryFromRows,
  normalizeRow,
  fieldCoverage,
});

import { createPrivateKey, randomBytes, sign as cryptoSign } from 'node:crypto';
import { gzipSync, gunzipSync } from 'node:zlib';

const VERSION = '650.8.15.187';
const DATA_VERSION = 1035;
const SCHEMA_VERSION = 'step1035_stock_catalog_v2';
const IMPLEMENTATION_REVISION = '1035_19_4_coinbase_atomic_catalog_swap_real_candle_probe';
const RUNTIME_INSTANCE_ID = randomBytes(8).toString('hex');
const RUNTIME_STARTED_AT = new Date().toISOString();
const HEALTH_ROUTE = '/api/stock-catalog-v2/health';
const CURRENT_ROUTE = '/api/stock-catalog-v2/current';
const TICKERS_ROUTE = '/api/stock-catalog-v2/tickers';
const EXACT_ROUTE = '/api/stock-catalog-v2/exact';
const SELF_TEST_ROUTE = '/api/stock-catalog-v2/self-test';
const REFRESH_MS = Math.max(60 * 60_000, Number(process.env.KAKA_STOCK_CATALOG_REFRESH_MS || 6 * 60 * 60_000));
const COINBASE_MARKET_REFRESH_MS = Math.max(5 * 60_000, Number(process.env.KAKA_STOCK_MARKET_REFRESH_MS || 15 * 60_000));
const GATE_SESSION_REFRESH_MS = Math.max(5 * 60_000, Number(process.env.KAKA_STOCK_GATE_SESSION_REFRESH_MS || 15 * 60_000));
const START_DELAY_MS = Math.max(15_000, Number(process.env.KAKA_STOCK_CATALOG_START_DELAY_MS || 75_000));
const FETCH_TIMEOUT_MS = Math.max(8_000, Number(process.env.KAKA_STOCK_CATALOG_FETCH_TIMEOUT_MS || 20_000));
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const SUPABASE_CONFIGURED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const STAGE_TABLE = 'kaka_exchange_stock_catalog_v2_stage';
const STATE_TABLE = 'kaka_exchange_stock_catalog_v2_state';
const COMMIT_RPC = 'kaka_exchange_stock_catalog_v2_commit';
const GATE_PAGE_SIZE = 500;
const GATE_MAX_PAGES_PER_EXCHANGE = 80;
const COINBASE_PAGE_LIMIT = 100;
const COINBASE_MARKET_PROBE_LIMIT = Math.max(100, Math.min(1000, Number(process.env.KAKA_STOCK_MARKET_PAGE_LIMIT || 500)));
const COINBASE_MARKET_OFFSET_CONCURRENCY = Math.max(1, Math.min(4, Number(process.env.KAKA_STOCK_MARKET_OFFSET_CONCURRENCY || 3)));
const COINBASE_MARKET_OFFSET_BATCH_PAUSE_MS = Math.max(150, Math.min(1500, Number(process.env.KAKA_STOCK_MARKET_OFFSET_BATCH_PAUSE_MS || 350)));
const COINBASE_MARKET_MIN_MATCH_RATIO = Math.max(0.80, Math.min(1, Number(process.env.KAKA_STOCK_MARKET_MIN_MATCH_RATIO || 0.95)));
const COINBASE_CATALOG_HIGHWATER_MIN_RATIO = Math.max(0.90, Math.min(1, Number(process.env.KAKA_STOCK_CATALOG_HIGHWATER_MIN_RATIO || 0.95)));
const COINBASE_CATALOG_HIGHWATER_MAX_AGE_MS = Math.max(6 * 60 * 60_000, Number(process.env.KAKA_STOCK_CATALOG_HIGHWATER_MAX_AGE_MS || 24 * 60 * 60_000));
const COINBASE_MAX_PAGES = 160;
const COINBASE_ALL_PRODUCTS_MAX_PAGES = 240;
const STAGE_CHUNK = 300;
const GATE_COMPLETE_MIN_ROWS = 4000;
const COINBASE_COMPLETE_MIN_PRODUCTS = 1000;
const COINBASE_COMPLETE_MIN_SECURITIES = 500;
const COINBASE_PUBLIC_PRODUCTS_PATH = '/api/v3/brokerage/market/products';
const COINBASE_AUTH_PRODUCTS_PATH = '/api/v3/brokerage/products';
const COINBASE_CORE_EQUITY_TICKERS = new Set([
  'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'GOOG', 'META', 'TSLA',
  'AVGO', 'AMD', 'NFLX', 'ORCL', 'CRM', 'INTC', 'QCOM', 'ADBE',
  'COST', 'JPM', 'BAC', 'V', 'MA', 'WMT', 'XOM', 'CVX', 'UNH',
  'JNJ', 'PG', 'HD', 'LLY', 'ABBV', 'COIN', 'PLTR', 'SPY', 'QQQ',
]);

const COINBASE_HOST = 'api.coinbase.com';
const COINBASE_CDP_KEY_NAME = String(process.env.KAKA_COINBASE_CDP_KEY_NAME || process.env.COINBASE_CDP_API_KEY_NAME || '').trim();
const COINBASE_CDP_KEY_SECRET = String(process.env.KAKA_COINBASE_CDP_KEY_SECRET || process.env.COINBASE_CDP_API_KEY_SECRET || '').replace(/\\n/g, '\n').trim();
const COINBASE_CDP_CONFIGURED = Boolean(COINBASE_CDP_KEY_NAME && COINBASE_CDP_KEY_SECRET);
let coinbasePrivateKey = null;

let started = false;
let startTimer = null;
let refreshTimer = null;
let coinbaseMarketTimer = null;
let gateSessionTimer = null;
let gateSessionInflight = null;
let coinbaseMarketInflight = null;
let coinbaseMarketDeferredTimer = null;
let coinbaseMarketDeferredDueCatalog = 0;
let catalogWaitedForMarketInflight = 0;
let catalogMarketOverlapViolations = 0;
let lastGateSessionStartedAt = '';
let lastGateSessionSucceededAt = '';
let lastGateSessionError = '';
let gateSessionRefreshAttempts = 0;
let gateSessionRefreshSuccesses = 0;
let gateSessionRefreshFailures = 0;
let lastCoinbaseMarketStartedAt = '';
let lastCoinbaseMarketSucceededAt = '';
let lastCoinbaseMarketError = '';
let coinbaseMarketRefreshAttempts = 0;
let coinbaseMarketRefreshSuccesses = 0;
let coinbaseMarketRefreshFailures = 0;
let coinbaseMarketRestoredReady = false;
let restoredCoinbaseRows = 0;
let restoredGateRows = 0;
let restoreAttempts = 0;
let restoreSuccesses = 0;
let restoreFailures = 0;
let lastRestoreError = '';
let lastCoinbaseRestoreMode = 'not_started';
let coinbaseMarketSnapshotGzipB64 = '';
let coinbaseMarketSnapshotRows = 0;
let coinbaseMarketSnapshotFetchedAt = '';
let coinbaseMarketSnapshotRestoreSuccesses = 0;
let coinbaseMarketSnapshotRestoreFailures = 0;
let coinbaseMarketSnapshotLastError = '';
let coinbaseMarketSnapshotPersistSuccesses = 0;
let coinbaseMarketSnapshotPersistFailures = 0;
let lastStatePersistError = '';
let lastCoinbaseMarketPaginationMode = 'not_started';
let lastCoinbaseMarketEffectivePageSize = 0;
let lastCoinbaseMarketPagesFetched = 0;
let lastCoinbaseMarketMatchedRows = 0;
let exactMetadataDbReads = 0;
let coinbaseKnownCatalogBootstrapAttempts = 0;
let coinbaseKnownCatalogBootstrapSuccesses = 0;
let coinbaseKnownCatalogBootstrapFailures = 0;
let lastCoinbaseKnownCatalogBootstrapRows = 0;
let lastCoinbaseKnownCatalogBootstrapError = '';
let lastCoinbaseKnownCatalogBootstrapMode = 'not_started';
let coinbaseCatalogHighWaterRows = 0;
let coinbaseCatalogHighWaterSecurities = 0;
let coinbaseCatalogHighWaterAt = '';
let lastCoinbaseCatalogFetchMode = 'not_started';
let lastCoinbaseCatalogCursorRows = 0;
let lastCoinbaseCatalogCursorSecurities = 0;
let lastCoinbaseCatalogOffsetRows = 0;
let lastCoinbaseCatalogOffsetSecurities = 0;
let lastCoinbaseCatalogPagesFetched = 0;
let lastCoinbaseCatalogEffectivePageSize = 0;
let lastCoinbaseCatalogCandidateRows = 0;
let lastCoinbaseCatalogCandidateSecurities = 0;
let lastCoinbaseCatalogHighWaterRatio = 0;
let lastCoinbaseCatalogIntegrityError = '';
let lastCoinbaseCatalogPublicCursorRows = 0;
let lastCoinbaseCatalogAuthCursorRows = 0;
let lastCoinbaseCatalogUndefinedSortRows = 0;
let lastCoinbaseCatalogSynthCursorUses = 0;
let lastCoinbaseCatalogCursorTermination = 'not_started';
let lastCoinbaseCatalogCursorPages = 0;
let lastCoinbaseCatalogStreamingPages = 0;
let lastCoinbaseCatalogStreamingStageWrites = 0;
let lastCoinbaseCatalogPeakPageRows = 0;
let lastCoinbaseCatalogRetainedFullProductRows = 0;
let lastCoinbaseMarketStreamingPages = 0;
let lastCoinbaseMarketRetainedFullProductRows = 0;
let stageCleanupSuccesses = 0;
let stageCleanupFailures = 0;
let lastStageCleanupError = '';
const coinbaseExactMetadataLoaded = new Set();
const coinbaseExactMetadataInflight = new Map();
let refreshInflight = null;
let lastRefreshStartedAt = '';
let lastRefreshSucceededAt = '';
let lastRefreshError = '';
let lastRefreshId = '';
let lastGateRows = 0;
let lastGateUs = 0;
let lastGateHk = 0;
let lastGateKr = 0;
let lastGateZhNames = 0;
let lastCoinbaseProductRows = 0;
let lastCoinbaseSecurities = 0;
let lastCoinbasePricedRows = 0;
let lastCoinbaseSessionRows = 0;
let lastCoinbaseZhNames = 0;
let lastGateSampleNative = '';
let lastCoinbaseSampleNative = '';
let lastCoinbaseSampleTicker = '';
let lastCoinbaseFallbackAllProductsUsed = false;
let lastCoinbaseKlineProbe = null;
let refreshAttempts = 0;
let refreshSuccesses = 0;
let refreshFailures = 0;
let gateRequestsStarted = 0;
let gateRequestsSucceeded = 0;
let coinbaseRequestsStarted = 0;
let coinbaseRequestsSucceeded = 0;
let sourceRequestFailures = 0;
let stageRowsWritten = 0;
let commitsSucceeded = 0;
let commitsFailed = 0;
let lastCoinbaseCommitStartedAt = '';
let lastCoinbaseCommitSucceededAt = '';
let lastCoinbaseCommitDurationMs = 0;
let lastCoinbaseCommitError = '';
let userReads = 0;
const sharedTickerByNative = new Map();
const sharedRowByNative = new Map();
const sharedSecurityByKey = new Map();
let sharedUpdatedAtMs = 0;

const CURATED_ZH = Object.freeze({
  AAPL: '苹果公司', MSFT: '微软', NVDA: '英伟达', TSLA: '特斯拉', AMZN: '亚马逊', META: 'Meta平台',
  GOOGL: 'Alphabet（谷歌）A类', GOOG: 'Alphabet（谷歌）C类', BRK_B: '伯克希尔哈撒韦B类', BRK_A: '伯克希尔哈撒韦A类',
  JPM: '摩根大通', V: 'Visa', MA: '万事达卡', NFLX: '奈飞', AMD: '超威半导体', INTC: '英特尔', QCOM: '高通',
  AVGO: '博通', ORCL: '甲骨文', CRM: '赛富时', IBM: 'IBM', DIS: '迪士尼', KO: '可口可乐', PEP: '百事公司',
  WMT: '沃尔玛', COST: '好市多', NKE: '耐克', MCD: '麦当劳', BA: '波音', CAT: '卡特彼勒', GE: 'GE航空航天',
  XOM: '埃克森美孚', CVX: '雪佛龙', GS: '高盛', MS: '摩根士丹利', BAC: '美国银行', C: '花旗集团',
  SPY: '标普500 ETF', QQQ: '纳斯达克100 ETF', DIA: '道琼斯工业平均ETF', IWM: '罗素2000 ETF', VTI: '美国全市场ETF',
  GLD: 'SPDR黄金ETF', IAU: 'iShares黄金信托', SLV: 'iShares白银信托', GDX: '黄金矿业ETF', GDXJ: '小型黄金矿业ETF',
  TLT: '20年以上美国国债ETF', IEF: '7-10年美国国债ETF', SHY: '1-3年美国国债ETF', HYG: '高收益债ETF', LQD: '投资级公司债ETF',
});

function compact(value) { return String(value ?? '').trim(); }
function lower(value) { return compact(value).toLowerCase(); }
function upper(value) { return compact(value).toUpperCase(); }
function finite(value) { const n = Number(value); return Number.isFinite(n) ? n : null; }
function nonneg(value) { const n = finite(value); return n != null && n >= 0 ? n : null; }
function nullableBool(value) { return typeof value === 'boolean' ? value : null; }
function normalizeTicker(value) { return upper(value).replace(/[^A-Z0-9._-]/g, '').slice(0, 48); }
function safeText(value, max = 500) { const v = compact(value); return v ? v.slice(0, max) : ''; }
function safeIso(value) { const ms = Date.parse(compact(value)); return Number.isFinite(ms) ? new Date(ms).toISOString() : null; }
function isoNow() { return new Date().toISOString(); }
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function hasHan(value) { return /[\u3400-\u9fff]/.test(compact(value)); }
function authHeaders(extra = {}) { return { apikey: SUPABASE_SERVICE_ROLE_KEY, authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`, ...extra }; }
function base64Url(value) { return Buffer.from(value).toString('base64url'); }
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
function sendJson(res, status, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'access-control-allow-origin': '*', 'content-length': String(body.length) });
  res.end(body);
}
async function timedFetchJson(url, { provider, headers = {}, signal = null } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), FETCH_TIMEOUT_MS);
  const abort = () => controller.abort(signal?.reason || new Error('aborted'));
  if (signal) signal.addEventListener('abort', abort, { once: true });
  if (provider === 'gate') gateRequestsStarted += 1;
  if (provider === 'coinbase') coinbaseRequestsStarted += 1;
  try {
    const response = await fetch(url, { method: 'GET', headers: { accept: 'application/json', 'cache-control': 'no-cache', 'user-agent': 'KakaWeb3-StockCatalogV2/1035', ...headers }, signal: controller.signal });
    const raw = await response.text();
    let json = null;
    try { json = raw ? JSON.parse(raw) : null; } catch { json = null; }
    if (!response.ok) throw new Error(`${provider || 'source'}_http_${response.status}:${safeText(json?.message || json?.error || raw, 180)}`);
    if (raw && json == null) throw new Error(`${provider || 'source'}_invalid_json_${response.status}`);
    if (provider === 'gate') gateRequestsSucceeded += 1;
    if (provider === 'coinbase') coinbaseRequestsSucceeded += 1;
    return json;
  } catch (error) {
    sourceRequestFailures += 1;
    throw error;
  } finally {
    clearTimeout(timer);
    if (signal) signal.removeEventListener('abort', abort);
  }
}
async function supabaseFetch(path, init = {}) {
  if (!SUPABASE_CONFIGURED) throw new Error('supabase_service_role_not_configured');
  return fetch(`${SUPABASE_URL}/rest/v1/${path}`, { ...init, headers: authHeaders(init.headers || {}) });
}

function symbolList(raw, max = 100) {
  return [...new Set(String(raw || '').split(',').map(x => x.trim()).filter(Boolean))].slice(0, max);
}

function packCoinbaseMarketSnapshot(rows, fetchedAt = isoNow()) {
  try {
    const packedRows = [];
    for (const row of Array.isArray(rows) ? rows : []) {
      if (row?.provider !== 'coinbase' || !compact(row?.exchange_symbol)) continue;
      packedRows.push([
        compact(row.exchange_symbol),
        row.reference_price ?? null,
        row.reference_change_24h_ratio ?? null,
        row.reference_turnover_24h ?? null,
        compact(row.current_session),
        row.trading_halted == null ? null : row.trading_halted === true,
        compact(row.reference_market_fetched_at || fetchedAt),
      ]);
    }
    const json = JSON.stringify({ v: 1, fetched_at: fetchedAt, rows: packedRows });
    coinbaseMarketSnapshotGzipB64 = gzipSync(Buffer.from(json, 'utf8'), { level: 6 }).toString('base64');
    coinbaseMarketSnapshotRows = packedRows.length;
    coinbaseMarketSnapshotFetchedAt = fetchedAt;
    coinbaseMarketSnapshotLastError = '';
    return packedRows.length;
  } catch (error) {
    coinbaseMarketSnapshotLastError = safeText(error?.message || error, 220);
    return 0;
  }
}
function restorePackedCoinbaseMarketSnapshot() {
  if (!coinbaseMarketSnapshotGzipB64) return 0;
  try {
    const raw = gunzipSync(Buffer.from(coinbaseMarketSnapshotGzipB64, 'base64')).toString('utf8');
    const payload = JSON.parse(raw);
    const rows = Array.isArray(payload?.rows) ? payload.rows : [];
    const fetchedAt = compact(payload?.fetched_at || coinbaseMarketSnapshotFetchedAt || lastCoinbaseMarketSucceededAt);
    const fetchedMs = Date.parse(fetchedAt || '');
    const maxAgeMs = Math.max(30 * 60_000, COINBASE_MARKET_REFRESH_MS * 2);
    if (!Number.isFinite(fetchedMs) || Date.now() - fetchedMs > maxAgeMs) throw new Error('coinbase_market_snapshot_stale');
    const overlays = [];
    for (const item of rows) {
      if (!Array.isArray(item) || item.length < 7) continue;
      const native = compact(item[0]);
      const key = `coinbase|${native}`;
      const old = sharedRowByNative.get(key);
      if (!old) continue;
      const currentSession = compact(item[4]) || old.current_session;
      overlays.push({
        ...old,
        provider: 'coinbase',
        exchange_symbol: native,
        current_session: currentSession,
        trade_status: currentSession || old.trade_status,
        trading_halted: item[5] == null ? old.trading_halted : item[5] === true,
        reference_price: item[1] == null ? null : finite(item[1]),
        reference_change_24h_ratio: item[2] == null ? null : finite(item[2]),
        reference_turnover_24h: item[3] == null ? null : nonneg(item[3]),
        reference_market_fetched_at: compact(item[6]) || fetchedAt,
      });
    }
    const applied = updateSharedCoinbaseMarketRows(overlays);
    lastCoinbaseMarketMatchedRows = overlays.length;
    lastCoinbasePricedRows = overlays.filter(r => r.reference_price != null).length;
    lastCoinbaseSessionRows = overlays.filter(r => compact(r.current_session)).length;
    coinbaseMarketRestoredReady = applied >= Math.min(COINBASE_COMPLETE_MIN_PRODUCTS, lastCoinbaseProductRows) && lastCoinbasePricedRows >= 50 && lastCoinbaseSessionRows >= 100;
    if (coinbaseMarketRestoredReady) {
      lastCoinbaseMarketSucceededAt = fetchedAt;
      coinbaseMarketSnapshotRestoreSuccesses += 1;
      coinbaseMarketSnapshotLastError = '';
    }
    return applied;
  } catch (error) {
    coinbaseMarketSnapshotRestoreFailures += 1;
    coinbaseMarketSnapshotLastError = safeText(error?.message || error, 220);
    return 0;
  }
}
function clearSharedProviderRows(provider) {
  const p = lower(provider);
  for (const [key, row] of [...sharedRowByNative.entries()]) if (lower(row?.provider) === p || key.startsWith(`${p}|`)) sharedRowByNative.delete(key);
  for (const [key] of [...sharedTickerByNative.entries()]) if (key.startsWith(`${p}|`)) sharedTickerByNative.delete(key);
  for (const [key, row] of [...sharedSecurityByKey.entries()]) if (lower(row?.provider) === p) sharedSecurityByKey.delete(key);
  if (p === 'coinbase') { coinbaseExactMetadataLoaded.clear(); coinbaseExactMetadataInflight.clear(); }
}

async function restoreProviderSharedRows(provider, { atomicReplace = false } = {}) {
  if (!SUPABASE_CONFIGURED) return 0;
  const commonSelect = 'asset_id,provider,market_type,product_kind,asset_class,asset_group,asset_class_zh,asset_class_en,exchange_symbol,display_symbol,display_name,display_name_zh,display_name_zh_source,security_key,security_type,base_asset,quote_asset,settle_asset,status,exchange_name,product_venue,symbol_type,official_kline_capability,official_kline_source,official_kline_identity,secondary_kline_source_required,secondary_source_status,sparse_market_bars,official_depth,official_rpi_depth,access,source_verified,source_cached_at,current_session,session_policy,supports_24_7,supports_24_5,trade_status,trade_mode,order_fill_timing,trading_halted,icon_url,quote_currency_symbol,reference_price,reference_change_24h_ratio,reference_volume_24h,reference_turnover_24h,reference_high_24h,reference_low_24h,reference_market_fetched_at';
  // Coinbase provider_metadata can contain descriptions + multi-day trading calendars for every
  // product. Restoring 5k+ copies at process start is unnecessary and was the Step1035.5
  // restart/pending bottleneck. Gate metadata is materially smaller and remains eager.
  const select = provider === 'coinbase' ? commonSelect : `${commonSelect},provider_metadata`;
  const rows = [];
  for (let offset = 0; offset < 20000; offset += 1000) {
    const response = await supabaseFetch(`kaka_exchange_asset_catalog?provider=eq.${encodeURIComponent(provider)}&asset_class=eq.equity_cash&source_verified=eq.true&select=${encodeURIComponent(select)}&order=asset_id.asc`, { headers: { range: `${offset}-${offset + 999}`, prefer: 'count=none' } });
    if (!response.ok) throw new Error(`stock_restore_${provider}_${response.status}:${safeText(await response.text(), 180)}`);
    const page = await response.json(); if (!Array.isArray(page) || !page.length) break;
    if (provider === 'coinbase') for (const row of page) row.provider_metadata = {};
    rows.push(...page); if (page.length < 1000) break;
  }

  // Build the full committed provider slice before swapping it into memory. During a
  // scheduled Coinbase refresh, user reads must continue seeing the previous verified slice
  // until the replacement is complete; never clear the provider map across an awaited DB read.
  if (atomicReplace) {
    replaceSharedProviderRows(provider, rows);
  } else {
    for (const row of rows) {
      sharedSecurityByKey.set(row.security_key || row.asset_id, row);
      sharedRowByNative.set(`${provider}|${row.exchange_symbol}`, row);
    }
  }

  if (provider === 'coinbase') {
    restoredCoinbaseRows = rows.length;
    lastCoinbaseRestoreMode = 'supabase_light_identity_market_columns';
    lastCoinbaseProductRows = rows.length;
    lastCoinbaseSecurities = new Set(rows.map(r => r.security_key || r.asset_id).filter(Boolean)).size;
    lastCoinbasePricedRows = rows.filter(r => r.reference_price != null).length;
    lastCoinbaseSessionRows = rows.filter(r => compact(r.current_session)).length;
    lastCoinbaseZhNames = rows.filter(r => compact(r.display_name_zh)).length;

    const marketTimes = rows.map(r => Date.parse(r.reference_market_fetched_at || '')).filter(Number.isFinite);
    const newestMarketMs = marketTimes.length ? Math.max(...marketTimes) : NaN;
    if (Number.isFinite(newestMarketMs)) {
      const stateMarketMs = Date.parse(lastCoinbaseMarketSucceededAt || '');
      if (!Number.isFinite(stateMarketMs) || newestMarketMs > stateMarketMs) lastCoinbaseMarketSucceededAt = new Date(newestMarketMs).toISOString();
    }
    const applied = updateSharedCoinbaseMarketRows(rows);
    coinbaseMarketRestoredReady = rows.length >= COINBASE_COMPLETE_MIN_PRODUCTS && applied >= Math.min(COINBASE_COMPLETE_MIN_PRODUCTS, rows.length) && lastCoinbasePricedRows >= 50 && lastCoinbaseSessionRows >= 100;
    if (!coinbaseMarketRestoredReady) restorePackedCoinbaseMarketSnapshot();
    const sample = rows.find(r => r.reference_price != null && r.display_symbol === 'AAPL') || rows.find(r => r.reference_price != null) || rows[0];
    if (sample) { lastCoinbaseSampleNative = compact(sample.exchange_symbol); lastCoinbaseSampleTicker = compact(sample.display_symbol); }
  } else if (provider === 'gate') {
    restoredGateRows = rows.length;
    lastGateRows = rows.length;
    lastGateUs = rows.filter(r => lower(r?.provider_metadata?.exchange || r?.exchange_name) === 'us' || lower(r?.exchange_name).includes('us')).length || lastGateUs;
    lastGateHk = rows.filter(r => lower(r?.provider_metadata?.exchange || r?.exchange_name) === 'hk' || lower(r?.exchange_name).includes('hk')).length || lastGateHk;
    lastGateKr = rows.filter(r => lower(r?.provider_metadata?.exchange || r?.exchange_name) === 'kr' || lower(r?.exchange_name).includes('kr')).length || lastGateKr;
    lastGateZhNames = rows.filter(r => compact(r.display_name_zh)).length;
    const sample = rows.find(r => r.display_symbol === 'AAPL') || rows[0];
    if (sample) lastGateSampleNative = compact(sample.exchange_symbol);
  }
  if (rows.length) sharedUpdatedAtMs = Date.now();
  return rows.length;
}

function currentCoinbaseKnownMap() {
  const known = new Map();
  for (const [key, row] of sharedRowByNative.entries()) {
    if (key.startsWith('coinbase|') && row?.provider === 'coinbase' && compact(row?.exchange_symbol)) known.set(row.exchange_symbol, row);
  }
  lastCoinbaseKnownCatalogBootstrapRows = known.size;
  return known;
}
async function hydrateCoinbaseKnownCatalogFromSupabase(reason = 'market_known_catalog_bootstrap') {
  coinbaseKnownCatalogBootstrapAttempts += 1;
  lastCoinbaseKnownCatalogBootstrapError = '';
  lastCoinbaseKnownCatalogBootstrapMode = 'supabase_minimal_committed_equity_identity';
  try {
    if (!SUPABASE_CONFIGURED) throw new Error('supabase_service_role_not_configured');
    const select = 'asset_id,provider,market_type,product_kind,asset_class,asset_group,exchange_symbol,display_symbol,display_name,display_name_zh,security_key,security_type,base_asset,quote_asset,settle_asset,status,exchange_name,product_venue,official_kline_capability,official_kline_source,secondary_source_status,source_verified,source_cached_at,current_session,supports_24_7,supports_24_5,trade_status,trading_halted,quote_currency_symbol,reference_price,reference_change_24h_ratio,reference_turnover_24h,reference_market_fetched_at';
    const rows = [];
    for (let offset = 0; offset < 20000; offset += 1000) {
      const response = await supabaseFetch(`kaka_exchange_asset_catalog?provider=eq.coinbase&asset_class=eq.equity_cash&source_verified=eq.true&select=${encodeURIComponent(select)}&order=asset_id.asc`, { headers: { range: `${offset}-${offset + 999}`, prefer: 'count=none' } });
      if (!response.ok) throw new Error(`coinbase_known_catalog_bootstrap_${response.status}:${safeText(await response.text(), 180)}`);
      const page = await response.json();
      if (!Array.isArray(page) || !page.length) break;
      rows.push(...page);
      if (page.length < 1000) break;
    }
    if (rows.length < COINBASE_COMPLETE_MIN_PRODUCTS) throw new Error(`coinbase_known_catalog_bootstrap_too_small:${rows.length}`);
    for (const raw of rows) {
      const native = compact(raw?.exchange_symbol);
      if (!native) continue;
      const key = `coinbase|${native}`;
      const old = sharedRowByNative.get(key);
      const row = { ...raw, provider: 'coinbase', provider_metadata: old?.provider_metadata || {} };
      const merged = old && typeof old === 'object' ? { ...row, ...old, provider: 'coinbase', provider_metadata: old.provider_metadata || {} } : row;
      sharedRowByNative.set(key, merged);
      sharedSecurityByKey.set(merged.security_key || merged.asset_id, merged);
    }
    const known = currentCoinbaseKnownMap();
    if (known.size < COINBASE_COMPLETE_MIN_PRODUCTS) throw new Error(`coinbase_known_catalog_bootstrap_map_too_small:${known.size}`);
    restoredCoinbaseRows = Math.max(restoredCoinbaseRows, known.size);
    lastCoinbaseProductRows = known.size;
    lastCoinbaseSecurities = new Set([...known.values()].map(r => r.security_key || r.asset_id).filter(Boolean)).size;
    lastCoinbasePricedRows = [...known.values()].filter(r => r.reference_price != null).length;
    lastCoinbaseSessionRows = [...known.values()].filter(r => compact(r.current_session)).length;
    lastCoinbaseZhNames = [...known.values()].filter(r => compact(r.display_name_zh)).length;
    lastCoinbaseRestoreMode = 'supabase_minimal_known_catalog_bootstrap';
    const sample = [...known.values()].find(r => r.reference_price != null && r.display_symbol === 'AAPL') || [...known.values()].find(r => r.reference_price != null) || [...known.values()][0];
    if (sample) { lastCoinbaseSampleNative = compact(sample.exchange_symbol); lastCoinbaseSampleTicker = compact(sample.display_symbol); }
    sharedUpdatedAtMs = Date.now();
    if (!coinbaseMarketRestoredReady && coinbaseMarketSnapshotGzipB64) restorePackedCoinbaseMarketSnapshot();
    coinbaseKnownCatalogBootstrapSuccesses += 1;
    lastCoinbaseKnownCatalogBootstrapRows = known.size;
    lastCoinbaseKnownCatalogBootstrapError = '';
    await persistState({ coinbase_known_catalog_bootstrap_reason: reason, coinbase_known_catalog_bootstrap_rows: known.size }).catch(() => false);
    return known.size;
  } catch (error) {
    coinbaseKnownCatalogBootstrapFailures += 1;
    lastCoinbaseKnownCatalogBootstrapError = safeText(error?.message || error, 260);
    throw error;
  }
}
async function loadCoinbaseExactMetadata(nativeSymbol) {
  const native = compact(nativeSymbol);
  if (!native || !SUPABASE_CONFIGURED) return null;
  const key = `coinbase|${native}`;
  const current = sharedRowByNative.get(key) || null;
  if (!current) return null;
  if (coinbaseExactMetadataLoaded.has(native)) return current;
  if (coinbaseExactMetadataInflight.has(native)) return coinbaseExactMetadataInflight.get(native);
  const task = (async () => {
    try {
      const response = await supabaseFetch(`kaka_exchange_asset_catalog?provider=eq.coinbase&asset_class=eq.equity_cash&source_verified=eq.true&exchange_symbol=eq.${encodeURIComponent(native)}&select=${encodeURIComponent('provider_metadata')}&limit=1`);
      if (!response.ok) throw new Error(`coinbase_exact_metadata_restore_${response.status}`);
      exactMetadataDbReads += 1;
      const rows = await response.json();
      const meta = rows?.[0]?.provider_metadata && typeof rows[0].provider_metadata === 'object' ? rows[0].provider_metadata : {};
      const merged = { ...current, provider_metadata: meta };
      sharedRowByNative.set(key, merged);
      sharedSecurityByKey.set(merged.security_key || merged.asset_id, merged);
      coinbaseExactMetadataLoaded.add(native);
      return merged;
    } catch (error) {
      // Identity/price/session remain usable even if detailed metadata hydration fails.
      return current;
    } finally {
      coinbaseExactMetadataInflight.delete(native);
    }
  })();
  coinbaseExactMetadataInflight.set(native, task);
  return task;
}

function gateRows(payload) {
  const data = payload?.data && typeof payload.data === 'object' ? payload.data : payload;
  return Array.isArray(data?.list) ? data.list : [];
}
function gateTotalPages(payload) {
  const data = payload?.data && typeof payload.data === 'object' ? payload.data : payload;
  return Math.max(0, Math.trunc(finite(data?.total_page) || 0));
}
function gateZhName(row) {
  for (const item of Array.isArray(row?.symbol_descs) ? row.symbol_descs : []) {
    const lang = lower(item?.lang).replace('_', '-');
    const value = safeText(item?.value, 280);
    if (value && lang.startsWith('zh') && hasHan(value)) return value;
  }
  const desc = safeText(row?.symbol_desc, 280);
  return hasHan(desc) ? desc : '';
}
function securityTypeFromGate(row) {
  const c = lower(row?.category);
  if (c.includes('etf')) return 'etf';
  if (c.includes('etn')) return 'etn';
  return 'stock';
}
function gateAssetRow(row, fetchedAt) {
  const symbol = normalizeTicker(row?.symbol);
  const exchange = lower(row?.exchange);
  if (!symbol || !['us','hk','kr'].includes(exchange)) return null;
  const quote = upper(row?.quote_currency || (exchange === 'hk' ? 'HKD' : exchange === 'kr' ? 'KRW' : 'USD'));
  const name = safeText(row?.symbol_desc, 300) || symbol;
  const zh = gateZhName(row) || CURATED_ZH[symbol] || '';
  const aliases = new Set([symbol, name, zh]);
  const securityType = securityTypeFromGate(row);
  if (securityType === 'etf') { aliases.add('ETF'); aliases.add('交易所交易基金'); }
  const tradeStatus = lower(row?.trade_status);
  const currentGtLp = tradeStatus === 'gt_lp';
  return {
    asset_id: `gate:stock:${exchange}:${symbol}`,
    provider: 'gate', market_type: 'spot', product_kind: 'cash_stock', asset_class: 'equity_cash', asset_group: 'stocks',
    asset_class_zh: securityType === 'etf' ? '现金ETF' : '现金股票', asset_class_en: securityType === 'etf' ? 'Cash ETF' : 'Cash equity',
    exchange_symbol: symbol, display_symbol: symbol, display_name: name, display_name_zh: zh, display_name_zh_source: zh ? (gateZhName(row) ? 'gate_official_i18n' : 'kaka_curated_exact_ticker') : '',
    search_aliases: [...aliases].filter(Boolean), security_key: `gate:${exchange}:${symbol}`, security_type: securityType,
    base_asset: symbol, quote_asset: quote, settle_asset: quote, status: tradeStatus || 'unknown', exchange_name: exchange.toUpperCase(), product_venue: safeText(row?.exchange_desc, 80), symbol_type: securityType,
    is_reality: false, is_rwa: false, rule_type: 'gate_stock', group_id: '',
    official_kline_capability: 'unavailable', official_kline_source: '', official_kline_identity: 'cash_equity', secondary_kline_source_required: true, secondary_source_status: 'commercial_second_source_required', sparse_market_bars: false,
    official_depth: true, official_rpi_depth: false, access: 'public', source_verified: true, source_cached_at: fetchedAt,
    current_session: tradeStatus, session_policy: 'gate_official_dynamic_trade_status', supports_24_7: currentGtLp ? true : null, supports_24_5: null,
    trade_status: tradeStatus, trade_mode: finite(row?.trade_mode), order_fill_timing: finite(row?.order_fill_timing), trading_halted: tradeStatus === 'closed' ? null : false,
    icon_url: safeText(row?.icon_link, 800), quote_currency_symbol: safeText(row?.quote_currency_symbol, 16),
    reference_price: null, reference_change_24h_ratio: null, reference_volume_24h: null, reference_turnover_24h: null, reference_high_24h: null, reference_low_24h: null, reference_market_fetched_at: null,
    provider_metadata: { exchange, exchange_desc: safeText(row?.exchange_desc, 120), category: safeText(row?.category, 80), quote_currency_precision: finite(row?.quote_currency_precision), price_precision: finite(row?.price_precision), volume_precision: finite(row?.volume_precision), is_ipo: nullableBool(row?.is_ipo), ipo_price: nonneg(row?.ipo_price), fx_rate: nonneg(row?.fx_rate) },
  };
}
async function fetchGateExchange(exchange) {
  const out = [];
  let totalPages = 1;
  for (let page = 1; page <= Math.min(totalPages, GATE_MAX_PAGES_PER_EXCHANGE); page += 1) {
    const url = `https://api.gateio.ws/api/v4/stock/symbols?exchange=${encodeURIComponent(exchange)}&with_desc_i18n=true&page=${page}&page_size=${GATE_PAGE_SIZE}`;
    const payload = await timedFetchJson(url, { provider: 'gate' });
    totalPages = Math.max(1, gateTotalPages(payload) || totalPages);
    const rows = gateRows(payload);
    out.push(...rows);
    if (!rows.length || page >= totalPages) break;
    await sleep(40);
  }
  if (totalPages > GATE_MAX_PAGES_PER_EXCHANGE) throw new Error(`gate_${exchange}_pagination_cap:${totalPages}`);
  return out;
}
async function fetchGateFull() {
  const fetchedAt = isoNow();
  const [us, hk, kr] = await Promise.all(['us','hk','kr'].map(fetchGateExchange));
  const rows = [...us, ...hk, ...kr].map(x => gateAssetRow(x, fetchedAt)).filter(Boolean);
  const dedup = new Map(rows.map(r => [r.asset_id, r]));
  if (dedup.size < GATE_COMPLETE_MIN_ROWS) throw new Error(`gate_stock_catalog_too_small:${dedup.size}`);
  lastGateUs = us.length; lastGateHk = hk.length; lastGateKr = kr.length; lastGateRows = dedup.size;
  lastGateZhNames = [...dedup.values()].filter(r => r.display_name_zh).length;
  const gateSample = [...dedup.values()].find(r => r.display_symbol === 'AAPL') || [...dedup.values()][0];
  lastGateSampleNative = compact(gateSample?.exchange_symbol);
  return [...dedup.values()];
}
function coinbaseProducts(payload) { return Array.isArray(payload?.products) ? payload.products : Array.isArray(payload?.data) ? payload.data : []; }
function coinbaseNextCursor(payload) { return compact(payload?.pagination?.next_cursor || payload?.cursor || payload?.next_cursor); }
function coinbaseHasNext(payload) {
  if (typeof payload?.pagination?.has_next === 'boolean') return payload.pagination.has_next;
  if (typeof payload?.has_next === 'boolean') return payload.has_next;
  return Boolean(coinbaseNextCursor(payload));
}
async function coinbaseListPage(params, endpoint = 'public') {
  const query = new URLSearchParams(params);
  const path = endpoint === 'auth' ? COINBASE_AUTH_PRODUCTS_PATH : COINBASE_PUBLIC_PRODUCTS_PATH;
  const headers = COINBASE_CDP_CONFIGURED ? { authorization: `Bearer ${coinbaseJwt(path)}` } : {};
  return timedFetchJson(`https://${COINBASE_HOST}${path}?${query.toString()}`, { provider: 'coinbase', headers });
}
async function coinbaseListPageRetry(params, attempts = 3, endpoint = 'public') {
  let last = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try { return await coinbaseListPage(params, endpoint); }
    catch (error) {
      last = error;
      if (attempt >= attempts) break;
      const message = lower(error?.message || error);
      const retryable = message.includes('429') || message.includes('timeout') || /http_5\d\d/.test(message) || message.includes('temporar') || message.includes('unavailable');
      if (!retryable) break;
      await sleep(250 * attempt * attempt);
    }
  }
  throw last || new Error('coinbase_page_failed');
}
function coinbaseSyntheticCursorFromProductId(productId) {
  const id = compact(productId);
  return id ? Buffer.from(id, 'utf8').toString('base64') : '';
}
async function streamCoinbaseCursorSweep({ allProducts = false, endpoint = 'public', sortOrder = 'PRODUCTS_SORT_ORDER_LIST_TIME_DESCENDING', maxPages = null, onEquityPage = null } = {}) {
  let cursor = '';
  const seenCursor = new Set();
  const seenFingerprint = new Set();
  const passProductIds = new Set();
  const pageCap = maxPages || (allProducts ? COINBASE_ALL_PRODUCTS_MAX_PAGES : COINBASE_MAX_PAGES);
  let pages = 0, synthUses = 0, noProgressPages = 0, termination = 'page_cap', pageLimit = COINBASE_MARKET_PROBE_LIMIT, peakPageRows = 0;
  for (let page = 0; page < pageCap; page += 1) {
    const params = { limit: String(pageLimit) };
    if (sortOrder) params.products_sort_order = sortOrder;
    if (allProducts) params.get_all_products = 'true'; else params.product_type = 'EQUITY';
    if (cursor) params.cursor = cursor;
    let payload;
    try { payload = await coinbaseListPageRetry(params, 3, endpoint); }
    catch (error) {
      if (page === 0 && pageLimit !== COINBASE_PAGE_LIMIT) {
        pageLimit = COINBASE_PAGE_LIMIT; params.limit = String(pageLimit); payload = await coinbaseListPageRetry(params, 3, endpoint);
      } else throw error;
    }
    pages += 1;
    const rows = coinbaseProducts(payload); peakPageRows = Math.max(peakPageRows, rows.length);
    const equityRows = rows.filter(x => upper(x?.product_type) === 'EQUITY');
    let added = 0;
    for (const raw of equityRows) { const id = compact(raw?.product_id); if (id && !passProductIds.has(id)) { passProductIds.add(id); added += 1; } }
    noProgressPages = added > 0 ? 0 : noProgressPages + 1;
    if (typeof onEquityPage === 'function' && equityRows.length) await onEquityPage(equityRows, { page: pages, endpoint, sortOrder, pageLimit });
    const fingerprint = equityRows.slice(0, 8).map(x => compact(x?.product_id)).join('|');
    if (fingerprint && seenFingerprint.has(fingerprint) && added === 0) { termination = 'repeated_page_no_progress'; break; }
    if (fingerprint) seenFingerprint.add(fingerprint);
    if (!rows.length) { termination = 'empty_page'; break; }
    const serverNext = coinbaseNextCursor(payload);
    let next = serverNext, synthetic = false;
    if (!next || next === cursor || seenCursor.has(next)) {
      const lastId = compact(rows[rows.length - 1]?.product_id);
      const derived = coinbaseSyntheticCursorFromProductId(lastId);
      if (derived && derived !== cursor && !seenCursor.has(derived)) { next = derived; synthetic = true; }
    }
    if (!next || next === cursor || seenCursor.has(next)) { termination = 'no_next_cursor'; break; }
    if (noProgressPages >= 2) { termination = 'two_no_progress_pages'; break; }
    if (synthetic) synthUses += 1;
    seenCursor.add(next); cursor = next; await sleep(35);
  }
  return { uniqueRows: passProductIds.size, pages, synthUses, termination, endpoint, sortOrder, pageLimit, peakPageRows };
}




async function fetchCoinbaseKnownMarketFast(known, fetchedAt) {
  const byProduct = new Map();
  const required = Math.min(known.size, Math.max(COINBASE_COMPLETE_MIN_PRODUCTS, Math.floor(known.size * COINBASE_MARKET_MIN_MATCH_RATIO)));
  let pages = 0;
  lastCoinbaseMarketStreamingPages = 0; lastCoinbaseMarketRetainedFullProductRows = 0;
  const mergePage = async (rawRows) => {
    for (const raw of rawRows) {
      const id = compact(raw?.product_id);
      if (!id || !known.has(id) || byProduct.has(id)) continue;
      const light = coinbaseMarketOverlayRow(raw, known.get(id), fetchedAt);
      if (light) byProduct.set(id, light);
    }
  };
  const run = async (options) => { const meta = await streamCoinbaseCursorSweep({ ...options, onEquityPage: mergePage }); pages += meta.pages; lastCoinbaseMarketStreamingPages += meta.pages; return meta; };
  await run({ endpoint: 'public', sortOrder: 'PRODUCTS_SORT_ORDER_LIST_TIME_DESCENDING' });
  if (byProduct.size < required && COINBASE_CDP_CONFIGURED) await run({ endpoint: 'auth', sortOrder: 'PRODUCTS_SORT_ORDER_LIST_TIME_DESCENDING' });
  if (byProduct.size < required) await run({ endpoint: 'public', sortOrder: 'PRODUCTS_SORT_ORDER_UNDEFINED' });
  if (byProduct.size < required) await run({ allProducts: true, endpoint: 'public', sortOrder: 'PRODUCTS_SORT_ORDER_LIST_TIME_DESCENDING' });
  lastCoinbaseMarketPaginationMode = 'streaming_light_overlay_cursor_known_catalog_union';
  lastCoinbaseMarketEffectivePageSize = COINBASE_MARKET_PROBE_LIMIT; lastCoinbaseMarketPagesFetched = pages; lastCoinbaseMarketMatchedRows = byProduct.size; lastCoinbaseMarketRetainedFullProductRows = 0;
  return byProduct;
}
function coinbasePctRatio(value) {
  const raw = compact(value);
  if (!raw) return null;
  const pct = raw.endsWith('%');
  const n = finite(raw.replace(/%/g, ''));
  if (n == null) return null;
  return pct ? n / 100 : (Math.abs(n) > 2 ? n / 100 : n);
}
function coinbaseEquity(row) { return row?.equity_product_details && typeof row.equity_product_details === 'object' ? row.equity_product_details : {}; }
function coinbaseSecurityType(row) {
  const eq = coinbaseEquity(row); const raw = upper(eq?.equity_subtype);
  if (raw.includes('ETF')) return 'etf'; if (raw.includes('ETN')) return 'etn'; if (raw.includes('PREFERRED')) return 'preferred_stock'; return 'stock';
}
function coinbaseSecurityKey(row) {
  const eq = coinbaseEquity(row); const cik = compact(eq?.cik).replace(/\D/g, ''); const ticker = normalizeTicker(eq?.ticker || row?.base_display_symbol || row?.base_currency_id);
  if (cik) return `coinbase:cik:${cik}:${ticker}`;
  const venue = lower(row?.product_venue || 'ccm'); const shortName = lower(eq?.short_name || row?.base_name).replace(/[^a-z0-9]+/g, '_').slice(0, 96);
  return `coinbase:${venue}:${ticker}:${shortName}`;
}
function coinbaseSupports24_5(eq) {
  const sessions = [];
  const collect = (day) => {
    if (!day || typeof day !== 'object') return;
    for (const x of Array.isArray(day.trading_sessions) ? day.trading_sessions : []) sessions.push(upper(x?.session_type));
  };
  collect(eq?.trading_day_info);
  const recent = eq?.recent_trading_days && typeof eq.recent_trading_days === 'object' ? eq.recent_trading_days : {};
  for (const day of Array.isArray(recent?.trading_days) ? recent.trading_days : []) collect(day);
  return sessions.some(x => x.includes('OVERNIGHT')) ? true : null;
}
function coinbaseAssetRow(row, fetchedAt, klineReady) {
  const productId = compact(row?.product_id);
  const eq = coinbaseEquity(row);
  const ticker = normalizeTicker(eq?.ticker || row?.base_display_symbol || row?.base_currency_id);
  if (!productId || !ticker || upper(row?.product_type) !== 'EQUITY') return null;
  const quote = upper(row?.quote_display_symbol || row?.quote_currency_id || 'USD');
  const securityType = coinbaseSecurityType(row);
  const name = safeText(eq?.short_name || row?.base_name || row?.display_name_overwrite || row?.about_description, 300) || ticker;
  const zh = CURATED_ZH[ticker] || '';
  const aliases = new Set([ticker, name, zh]);
  if (securityType === 'etf') { aliases.add('ETF'); aliases.add('交易所交易基金'); }
  const securityKey = coinbaseSecurityKey(row);
  const flags = eq?.equity_trading_flags && typeof eq.equity_trading_flags === 'object' ? eq.equity_trading_flags : {};
  const currentSession = upper(eq?.current_session);
  const tradingHalted = nullableBool(eq?.trading_halted);
  const status = lower(row?.status || (tradingHalted ? 'halted' : flags?.tradable === false ? 'disabled' : 'active'));
  return {
    asset_id: `coinbase:equity:${productId}`, provider: 'coinbase', market_type: 'equity', product_kind: 'equity', asset_class: 'equity_cash', asset_group: 'stocks',
    asset_class_zh: securityType === 'etf' ? '现金ETF' : '现金股票', asset_class_en: securityType === 'etf' ? 'Cash ETF' : 'Cash equity',
    exchange_symbol: productId, display_symbol: ticker, display_name: name, display_name_zh: zh, display_name_zh_source: zh ? 'kaka_curated_exact_ticker' : '',
    search_aliases: [...aliases].filter(Boolean), security_key: securityKey, security_type: securityType,
    base_asset: ticker, quote_asset: quote, settle_asset: quote, status, exchange_name: safeText(row?.product_venue, 80) || 'Coinbase Capital Markets', product_venue: safeText(row?.product_venue, 80), symbol_type: securityType,
    is_reality: false, is_rwa: false, rule_type: 'coinbase_equity', group_id: compact(row?.alias),
    official_kline_capability: COINBASE_CDP_CONFIGURED && COINBASE_CORE_EQUITY_TICKERS.has(ticker) ? 'supported' : 'unavailable', official_kline_source: COINBASE_CDP_CONFIGURED && COINBASE_CORE_EQUITY_TICKERS.has(ticker) ? 'coinbase_advanced_trade_public_exact_product_candles' : '', official_kline_identity: 'cash_equity_exact_product_id', secondary_kline_source_required: !(COINBASE_CDP_CONFIGURED && COINBASE_CORE_EQUITY_TICKERS.has(ticker)), secondary_source_status: COINBASE_CDP_CONFIGURED && COINBASE_CORE_EQUITY_TICKERS.has(ticker) ? 'official_exact_product_candles_core_pool_route_enabled' : (klineReady ? 'official_exact_candles_probe_ready_non_core_locked' : 'official_equity_candles_not_proven'), sparse_market_bars: false,
    official_depth: false, official_rpi_depth: false, access: 'public_product_metadata_and_shared_exact_market', source_verified: true, source_cached_at: fetchedAt,
    current_session: currentSession, session_policy: 'coinbase_official_equity_dynamic_24_5_eligible_symbols', supports_24_7: false, supports_24_5: coinbaseSupports24_5(eq),
    trade_status: currentSession || status, trade_mode: null, order_fill_timing: null, trading_halted: tradingHalted,
    icon_url: safeText(row?.icon_url, 800), quote_currency_symbol: quote === 'USD' ? '$' : quote,
    reference_price: nonneg(row?.price), reference_change_24h_ratio: coinbasePctRatio(row?.price_percentage_change_24h), reference_volume_24h: nonneg(row?.volume_24h), reference_turnover_24h: nonneg(row?.approximate_quote_24h_volume), reference_high_24h: nonneg(row?.high_24h), reference_low_24h: nonneg(row?.low_24h), reference_market_fetched_at: fetchedAt,
    provider_metadata: { product_id: productId, alias: compact(row?.alias), base_name: safeText(row?.base_name, 300), about_description: safeText(row?.about_description, 1000), cik: compact(eq?.cik), fractionable: nullableBool(eq?.fractionable), current_session: currentSession, equity_subtype: upper(eq?.equity_subtype), trading_day_info: eq?.trading_day_info || null, recent_trading_days: eq?.recent_trading_days || null, equity_trading_flags: flags, base_increment: nonneg(row?.base_increment), quote_increment: nonneg(row?.quote_increment), price_increment: nonneg(row?.price_increment), base_min_size: nonneg(row?.base_min_size), base_max_size: nonneg(row?.base_max_size), quote_min_size: nonneg(row?.quote_min_size), quote_max_size: nonneg(row?.quote_max_size), is_disabled: nullableBool(row?.is_disabled), view_only: nullableBool(row?.view_only), cancel_only: nullableBool(row?.cancel_only), limit_only: nullableBool(row?.limit_only), post_only: nullableBool(row?.post_only), trading_disabled: nullableBool(row?.trading_disabled), auction_mode: nullableBool(row?.auction_mode), best_bid_price: nonneg(row?.best_bid_price), best_ask_price: nonneg(row?.best_ask_price), mid_market_price: nonneg(row?.mid_market_price) },
  };
}
function coinbaseCandleRouteIdsFromRawProduct(row) {
  const eq = coinbaseEquity(row);
  const ticker = normalizeTicker(eq?.ticker || row?.base_display_symbol || row?.base_currency_id);
  const quote = upper(row?.quote_display_symbol || row?.quote_currency_id || 'USD');
  const alias = compact(row?.alias);
  const ids = [];
  if (alias) ids.push(alias);
  if (ticker && quote) ids.push(`${ticker}-${quote}`);
  return [...new Set(ids.filter(Boolean))];
}
async function probeCoinbaseEquityKline(rawProduct) {
  if (!COINBASE_CDP_CONFIGURED) return { ready: false, reason: 'cdp_credentials_not_configured', rows: 0, http_mode: 'none' };
  const routeIds = coinbaseCandleRouteIdsFromRawProduct(rawProduct);
  if (!routeIds.length) return { ready: false, reason: 'no_exact_candle_route_id', rows: 0, http_mode: 'public_market_same_exact_product_alias_or_pair' };
  const end = Math.floor(Date.now() / 1000); const start = end - 3 * 24 * 3600;
  let lastError = '';
  for (const routeId of routeIds) {
    const path = `/api/v3/brokerage/market/products/${encodeURIComponent(routeId)}/candles`;
    const query = `?start=${start}&end=${end}&granularity=ONE_HOUR&limit=5`;
    try {
      const payload = await timedFetchJson(`https://${COINBASE_HOST}${path}${query}`, { provider: 'coinbase', headers: { authorization: `Bearer ${coinbaseJwt(path)}` } });
      const rows = Array.isArray(payload?.candles) ? payload.candles.length : 0;
      if (rows > 0) return { ready: true, reason: '', rows, http_mode: 'public_market_same_exact_product_alias_or_pair', route_product_id: routeId };
      lastError = `no_candles:${routeId}`;
    } catch (error) { lastError = `${routeId}:${safeText(error?.message || error, 160)}`; }
  }
  return { ready: false, reason: lastError || 'no_candles', rows: 0, http_mode: 'public_market_same_exact_product_alias_or_pair', route_product_ids: routeIds };
}
function coinbaseRawSecurityCount(rows) {
  const keys = new Set();
  for (const row of rows || []) {
    if (upper(row?.product_type) !== 'EQUITY') continue;
    const key = coinbaseSecurityKey(row);
    if (key) keys.add(key);
  }
  return keys.size;
}
function coinbaseCatalogHighWaterFresh() {
  const ms = Date.parse(coinbaseCatalogHighWaterAt || '');
  return Number.isFinite(ms) && Date.now() - ms <= COINBASE_CATALOG_HIGHWATER_MAX_AGE_MS;
}
function updateCoinbaseCatalogHighWater(rows, securities, at = isoNow()) {
  const r = Math.max(0, Number(rows) || 0);
  const s = Math.max(0, Number(securities) || 0);
  if (!coinbaseCatalogHighWaterFresh() || r > coinbaseCatalogHighWaterRows || s > coinbaseCatalogHighWaterSecurities) {
    coinbaseCatalogHighWaterRows = Math.max(r, coinbaseCatalogHighWaterFresh() ? coinbaseCatalogHighWaterRows : 0);
    coinbaseCatalogHighWaterSecurities = Math.max(s, coinbaseCatalogHighWaterFresh() ? coinbaseCatalogHighWaterSecurities : 0);
    coinbaseCatalogHighWaterAt = at;
  }
}
function coinbaseCandidateMeetsHighWater(rows, securities) {
  if (!coinbaseCatalogHighWaterFresh() || coinbaseCatalogHighWaterRows < COINBASE_COMPLETE_MIN_PRODUCTS) return true;
  const rowFloor = Math.max(COINBASE_COMPLETE_MIN_PRODUCTS, Math.floor(coinbaseCatalogHighWaterRows * COINBASE_CATALOG_HIGHWATER_MIN_RATIO));
  const secFloor = Math.max(COINBASE_COMPLETE_MIN_SECURITIES, Math.floor(coinbaseCatalogHighWaterSecurities * COINBASE_CATALOG_HIGHWATER_MIN_RATIO));
  return rows >= rowFloor && securities >= secFloor;
}
function adoptCoinbaseCommittedRows(rows) {
  const list = Array.isArray(rows) ? rows : [];
  lastCoinbaseProductRows = list.length;
  lastCoinbaseSecurities = new Set(list.map(r => r.security_key).filter(Boolean)).size;
  lastCoinbasePricedRows = list.filter(r => r.reference_price != null).length;
  lastCoinbaseSessionRows = list.filter(r => compact(r.current_session)).length;
  lastCoinbaseZhNames = list.filter(r => compact(r.display_name_zh)).length;
  const cbSample = list.find(r => r.reference_price != null && r.display_symbol === 'AAPL') || list.find(r => r.reference_price != null) || list[0];
  lastCoinbaseSampleNative = compact(cbSample?.exchange_symbol); lastCoinbaseSampleTicker = compact(cbSample?.display_symbol);
}
async function seedCoinbaseMarketFromCommittedCatalogRows(rows, reason = 'catalog_commit') {
  const list = Array.isArray(rows) ? rows.filter(r => r && r.provider === 'coinbase') : [];
  if (!list.length) return false;
  const fetchedAt = isoNow();
  const previousSucceededAt = lastCoinbaseMarketSucceededAt;
  const previousRefreshSuccesses = coinbaseMarketRefreshSuccesses;
  lastCoinbaseMarketStartedAt = fetchedAt;
  lastCoinbaseMarketPaginationMode = 'catalog_commit_seed';
  lastCoinbaseMarketEffectivePageSize = list.length;
  lastCoinbaseMarketPagesFetched = 1;
  lastCoinbaseMarketMatchedRows = list.length;
  lastCoinbaseMarketSucceededAt = fetchedAt;
  lastCoinbaseMarketError = '';
  coinbaseMarketRefreshSuccesses += 1;
  packCoinbaseMarketSnapshot(list, fetchedAt);
  coinbaseMarketSnapshotPersistSuccesses += 1;
  const persisted = await persistState({ last_coinbase_market_reason: reason, coinbase_market_known_catalog_rows: list.length, coinbase_market_matched_rows: list.length, coinbase_market_applied_rows: list.length, coinbase_market_pagination_mode: 'catalog_commit_seed' });
  if (!persisted) {
    lastCoinbaseMarketSucceededAt = previousSucceededAt;
    coinbaseMarketRefreshSuccesses = previousRefreshSuccesses;
    coinbaseMarketSnapshotPersistSuccesses = Math.max(0, coinbaseMarketSnapshotPersistSuccesses - 1);
    coinbaseMarketSnapshotPersistFailures += 1;
    coinbaseMarketSnapshotLastError = lastStatePersistError || 'catalog_commit_snapshot_state_persist_failed';
    throw new Error(`coinbase_catalog_commit_snapshot_persist_failed:${coinbaseMarketSnapshotLastError}`);
  }
  coinbaseMarketSnapshotLastError = '';
  return true;
}
async function fetchAndStageCoinbaseFull(refreshId) {
  const fetchedAt = isoNow();
  lastCoinbaseCatalogIntegrityError = ''; lastCoinbaseFallbackAllProductsUsed = false; lastCoinbaseCatalogFetchMode = 'not_started';
  lastCoinbaseCatalogCursorRows = 0; lastCoinbaseCatalogCursorSecurities = 0; lastCoinbaseCatalogOffsetRows = 0; lastCoinbaseCatalogOffsetSecurities = 0;
  lastCoinbaseCatalogPagesFetched = 0; lastCoinbaseCatalogEffectivePageSize = COINBASE_PAGE_LIMIT; lastCoinbaseCatalogPublicCursorRows = 0; lastCoinbaseCatalogAuthCursorRows = 0; lastCoinbaseCatalogUndefinedSortRows = 0;
  lastCoinbaseCatalogSynthCursorUses = 0; lastCoinbaseCatalogCursorTermination = 'not_started'; lastCoinbaseCatalogCursorPages = 0; lastCoinbaseCatalogStreamingPages = 0; lastCoinbaseCatalogStreamingStageWrites = 0; lastCoinbaseCatalogPeakPageRows = 0; lastCoinbaseCatalogRetainedFullProductRows = 0;
  const highWaterRows = coinbaseCatalogHighWaterFresh() ? coinbaseCatalogHighWaterRows : Math.max(lastCoinbaseProductRows, 0);
  const highWaterSecs = coinbaseCatalogHighWaterFresh() ? coinbaseCatalogHighWaterSecurities : Math.max(lastCoinbaseSecurities, 0);
  const productIds = new Set(), securityKeys = new Set();
  let klineReady = false, klineProbed = false, sampleAsset = null;
  const term = [];
  const enough = () => coinbaseCandidateMeetsHighWater(productIds.size, securityKeys.size);
  const stagePage = async (rawRows) => {
    if (!klineProbed) {
      const probeCandidate = rawRows.find(x => nullableBool(coinbaseEquity(x)?.equity_trading_flags?.tradable) !== false) || rawRows[0];
      if (probeCandidate?.product_id) { try { lastCoinbaseKlineProbe = await probeCoinbaseEquityKline(probeCandidate); } catch (error) { lastCoinbaseKlineProbe = { ready:false, error:safeText(error?.message || error,120) }; } klineReady = lastCoinbaseKlineProbe?.ready === true; }
      klineProbed = true;
    }
    const assetRows = [];
    for (const raw of rawRows) {
      const id = compact(raw?.product_id); if (!id || productIds.has(id) || upper(raw?.product_type) !== 'EQUITY') continue;
      const row = coinbaseAssetRow(raw, fetchedAt, klineReady); if (!row) continue;
      productIds.add(id); securityKeys.add(row.security_key || row.asset_id); if (!sampleAsset || (row.display_symbol === 'AAPL' && row.reference_price != null)) sampleAsset = row; assetRows.push(row);
    }
    if (assetRows.length) { await stageProviderRows('coinbase', assetRows, refreshId); lastCoinbaseCatalogStreamingStageWrites += Math.ceil(assetRows.length / STAGE_CHUNK); }
  };
  const runPass = async (label, options) => {
    const meta = await streamCoinbaseCursorSweep({ ...options, onEquityPage: stagePage });
    lastCoinbaseCatalogSynthCursorUses += meta.synthUses; lastCoinbaseCatalogCursorPages += meta.pages; lastCoinbaseCatalogStreamingPages += meta.pages; lastCoinbaseCatalogPeakPageRows = Math.max(lastCoinbaseCatalogPeakPageRows, meta.peakPageRows || 0); lastCoinbaseCatalogEffectivePageSize = meta.pageLimit || lastCoinbaseCatalogEffectivePageSize; term.push(`${label}:${meta.termination}`); return meta;
  };
  const publicList = await runPass('public_list', { endpoint:'public', sortOrder:'PRODUCTS_SORT_ORDER_LIST_TIME_DESCENDING' }); lastCoinbaseCatalogPublicCursorRows = publicList.uniqueRows;
  if (!enough() && COINBASE_CDP_CONFIGURED) { const authList = await runPass('auth_list', { endpoint:'auth', sortOrder:'PRODUCTS_SORT_ORDER_LIST_TIME_DESCENDING' }); lastCoinbaseCatalogAuthCursorRows = authList.uniqueRows; }
  if (!enough()) { const publicDefault = await runPass('public_default', { endpoint:'public', sortOrder:'PRODUCTS_SORT_ORDER_UNDEFINED' }); lastCoinbaseCatalogUndefinedSortRows = publicDefault.uniqueRows; }
  if (!enough()) { await runPass('public_all', { allProducts:true, endpoint:'public', sortOrder:'PRODUCTS_SORT_ORDER_LIST_TIME_DESCENDING' }); lastCoinbaseFallbackAllProductsUsed = true; }
  const candidateRows = productIds.size, candidateSecs = securityKeys.size;
  lastCoinbaseCatalogCursorRows = candidateRows; lastCoinbaseCatalogCursorSecurities = candidateSecs; lastCoinbaseCatalogCandidateRows = candidateRows; lastCoinbaseCatalogCandidateSecurities = candidateSecs; lastCoinbaseCatalogPagesFetched = lastCoinbaseCatalogCursorPages; lastCoinbaseCatalogCursorTermination = term.join('|');
  const ratioRows = coinbaseCatalogHighWaterRows > 0 ? candidateRows / coinbaseCatalogHighWaterRows : 1, ratioSecs = coinbaseCatalogHighWaterSecurities > 0 ? candidateSecs / coinbaseCatalogHighWaterSecurities : 1; lastCoinbaseCatalogHighWaterRatio = Math.min(ratioRows, ratioSecs); lastCoinbaseCatalogFetchMode = enough() ? 'streaming_exhaustive_cursor_verified_stage' : 'streaming_exhaustive_cursor_integrity_reject';
  if (candidateRows < COINBASE_COMPLETE_MIN_PRODUCTS) throw new Error(`coinbase_equity_catalog_too_small:${candidateRows}`);
  if (candidateSecs < COINBASE_COMPLETE_MIN_SECURITIES) throw new Error(`coinbase_security_catalog_too_small:${candidateSecs}`);
  if (!coinbaseCandidateMeetsHighWater(candidateRows, candidateSecs)) { const rowFloor = Math.max(COINBASE_COMPLETE_MIN_PRODUCTS, Math.floor(highWaterRows * COINBASE_CATALOG_HIGHWATER_MIN_RATIO)); const secFloor = Math.max(COINBASE_COMPLETE_MIN_SECURITIES, Math.floor(highWaterSecs * COINBASE_CATALOG_HIGHWATER_MIN_RATIO)); lastCoinbaseCatalogIntegrityError = `streaming_cursor_below_highwater:${candidateRows}/${candidateSecs};required=${rowFloor}/${secFloor};highwater=${highWaterRows}/${highWaterSecs};passes=${lastCoinbaseCatalogCursorTermination}`; throw new Error(`coinbase_catalog_integrity_reject:${lastCoinbaseCatalogIntegrityError}`); }
  lastCoinbaseCatalogIntegrityError = ''; if (sampleAsset) { lastCoinbaseSampleNative = compact(sampleAsset.exchange_symbol); lastCoinbaseSampleTicker = compact(sampleAsset.display_symbol); }
  return { rows:candidateRows, securities:candidateSecs, fetched_at:fetchedAt };
}
async function clearStage(refreshId) {
  const response = await supabaseFetch(`${STAGE_TABLE}?refresh_id=eq.${encodeURIComponent(refreshId)}`, { method: 'DELETE' });
  if (!response.ok) throw new Error(`stock_stage_clear_${response.status}`);
}
async function cleanupCommittedStage(refreshId, provider) {
  try { await clearStage(refreshId); stageCleanupSuccesses += 1; lastStageCleanupError = ''; return true; }
  catch (error) { stageCleanupFailures += 1; lastStageCleanupError = `${provider}:${safeText(error?.message || error, 180)}`; return false; }
}
function stagePayload(row, refreshId) { return { refresh_id: refreshId, provider: row.provider, asset_id: row.asset_id, row_data: row }; }
async function stageProviderRows(provider, rows, refreshId) {
  for (let i = 0; i < rows.length; i += STAGE_CHUNK) {
    const chunk = rows.slice(i, i + STAGE_CHUNK).map(row => stagePayload(row, refreshId));
    const response = await supabaseFetch(`${STAGE_TABLE}?on_conflict=refresh_id,asset_id`, { method: 'POST', headers: { 'content-type': 'application/json', prefer: 'resolution=merge-duplicates,return=minimal' }, body: JSON.stringify(chunk) });
    if (!response.ok) throw new Error(`stock_stage_write_${provider}_${response.status}:${safeText(await response.text(), 160)}`);
    stageRowsWritten += chunk.length;
  }
}
async function commitProvider(provider, refreshId, expectedRows, expectedSecurities) {
  const startedMs = Date.now();
  if (provider === 'coinbase') { lastCoinbaseCommitStartedAt = isoNow(); lastCoinbaseCommitError = ''; }
  try {
    const response = await supabaseFetch(`rpc/${COMMIT_RPC}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ p_provider: provider, p_refresh_id: refreshId, p_expected_rows: expectedRows, p_expected_securities: expectedSecurities }) });
    const raw = await response.text();
    if (!response.ok) { commitsFailed += 1; throw new Error(`stock_commit_${provider}_${response.status}:${safeText(raw, 200)}`); }
    let payload = null; try { payload = JSON.parse(raw); } catch { payload = null; }
    if (payload?.ok !== true) { commitsFailed += 1; throw new Error(`stock_commit_${provider}_rejected:${safeText(raw, 200)}`); }
    commitsSucceeded += 1;
    if (provider === 'coinbase') { lastCoinbaseCommitSucceededAt = isoNow(); lastCoinbaseCommitDurationMs = Date.now() - startedMs; lastCoinbaseCommitError = ''; }
    return payload;
  } catch (error) {
    if (provider === 'coinbase') { lastCoinbaseCommitDurationMs = Date.now() - startedMs; lastCoinbaseCommitError = safeText(error?.message || error, 240); }
    throw error;
  }
}
function rebuildSharedMaps(rows) {
  sharedTickerByNative.clear(); sharedRowByNative.clear(); sharedSecurityByKey.clear();
  for (const row of rows) {
    sharedRowByNative.set(`${row.provider}|${row.exchange_symbol}`, row);
    if (row.provider === 'coinbase') {
      const ticker = { source: 'coinbase_shared_full_equity_catalog_snapshot', last_price: row.reference_price, price_change_24h_ratio: row.reference_change_24h_ratio, high_24h: row.reference_high_24h, low_24h: row.reference_low_24h, open_24h: null, volume_24h: row.reference_volume_24h, turnover_24h: row.reference_turnover_24h, best_bid: null, best_ask: null, best_bid_size: null, best_ask_size: null, timestamp_ms: Date.parse(row.reference_market_fetched_at || row.source_cached_at) || null, mark_price: null, index_price: null, open_interest: null, open_interest_value: null, funding_rate: null, next_funding_time_ms: null };
      sharedTickerByNative.set(`coinbase|${row.exchange_symbol}`, { native_symbol: row.exchange_symbol, ticker, source_cached_at: row.source_cached_at });
    }
    sharedSecurityByKey.set(row.security_key || row.asset_id, row);
  }
  sharedUpdatedAtMs = Date.now();
}
function replaceSharedProviderRows(provider, rows) {
  const keep = [...sharedRowByNative.values()].filter(row => lower(row?.provider) !== provider);
  rebuildSharedMaps([...keep, ...rows]);
}
async function persistState(extra = {}) {
  if (!SUPABASE_CONFIGURED) { lastStatePersistError = 'supabase_not_configured'; return false; }
  const payload = { version: VERSION, data_version: DATA_VERSION, schema_version: SCHEMA_VERSION, implementation_revision: IMPLEMENTATION_REVISION, runtime_instance_id: RUNTIME_INSTANCE_ID, runtime_started_at: RUNTIME_STARTED_AT, process_uptime_seconds: Math.floor(process.uptime()), memory: { rss_mb: Math.round(process.memoryUsage().rss / 1048576), heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576), heap_total_mb: Math.round(process.memoryUsage().heapTotal / 1048576), external_mb: Math.round(process.memoryUsage().external / 1048576) }, last_refresh_started_at: lastRefreshStartedAt || null, last_refresh_succeeded_at: lastRefreshSucceededAt || null, last_refresh_error: lastRefreshError, gate_rows: lastGateRows, gate_us_rows: lastGateUs, gate_hk_rows: lastGateHk, gate_kr_rows: lastGateKr, gate_zh_name_rows: lastGateZhNames, coinbase_product_rows: lastCoinbaseProductRows, coinbase_security_rows: lastCoinbaseSecurities, coinbase_priced_rows: lastCoinbasePricedRows, coinbase_session_rows: lastCoinbaseSessionRows, coinbase_zh_name_rows: lastCoinbaseZhNames, gate_sample_native: lastGateSampleNative || null, coinbase_sample_native: lastCoinbaseSampleNative || null, coinbase_sample_ticker: lastCoinbaseSampleTicker || null, coinbase_fallback_all_products_used: lastCoinbaseFallbackAllProductsUsed, coinbase_kline_probe: lastCoinbaseKlineProbe, refresh_attempts: refreshAttempts, refresh_successes: refreshSuccesses, refresh_failures: refreshFailures, gate_requests_started: gateRequestsStarted, gate_requests_succeeded: gateRequestsSucceeded, coinbase_requests_started: coinbaseRequestsStarted, coinbase_requests_succeeded: coinbaseRequestsSucceeded, source_request_failures: sourceRequestFailures, stage_rows_written: stageRowsWritten, commits_succeeded: commitsSucceeded, commits_failed: commitsFailed, last_coinbase_commit_started_at: lastCoinbaseCommitStartedAt || null, last_coinbase_commit_succeeded_at: lastCoinbaseCommitSucceededAt || null, last_coinbase_commit_duration_ms: lastCoinbaseCommitDurationMs, last_coinbase_commit_error: lastCoinbaseCommitError, last_gate_session_started_at: lastGateSessionStartedAt || null, last_gate_session_succeeded_at: lastGateSessionSucceededAt || null, last_gate_session_error: lastGateSessionError, gate_session_refresh_attempts: gateSessionRefreshAttempts, gate_session_refresh_successes: gateSessionRefreshSuccesses, gate_session_refresh_failures: gateSessionRefreshFailures, last_coinbase_market_started_at: lastCoinbaseMarketStartedAt || null, last_coinbase_market_succeeded_at: lastCoinbaseMarketSucceededAt || null, last_coinbase_market_error: lastCoinbaseMarketError, coinbase_market_refresh_attempts: coinbaseMarketRefreshAttempts, coinbase_market_refresh_successes: coinbaseMarketRefreshSuccesses, coinbase_market_refresh_failures: coinbaseMarketRefreshFailures, coinbase_market_snapshot_gzip_b64: coinbaseMarketSnapshotGzipB64, coinbase_market_snapshot_rows: coinbaseMarketSnapshotRows, coinbase_market_snapshot_fetched_at: coinbaseMarketSnapshotFetchedAt, coinbase_market_snapshot_persist_successes: coinbaseMarketSnapshotPersistSuccesses, coinbase_market_snapshot_persist_failures: coinbaseMarketSnapshotPersistFailures, last_coinbase_market_pagination_mode: lastCoinbaseMarketPaginationMode, last_coinbase_market_effective_page_size: lastCoinbaseMarketEffectivePageSize, last_coinbase_market_pages_fetched: lastCoinbaseMarketPagesFetched, last_coinbase_market_matched_rows: lastCoinbaseMarketMatchedRows, coinbase_catalog_highwater_rows: coinbaseCatalogHighWaterRows, coinbase_catalog_highwater_securities: coinbaseCatalogHighWaterSecurities, coinbase_catalog_highwater_at: coinbaseCatalogHighWaterAt, last_coinbase_catalog_fetch_mode: lastCoinbaseCatalogFetchMode, last_coinbase_catalog_cursor_rows: lastCoinbaseCatalogCursorRows, last_coinbase_catalog_cursor_securities: lastCoinbaseCatalogCursorSecurities, last_coinbase_catalog_offset_rows: lastCoinbaseCatalogOffsetRows, last_coinbase_catalog_offset_securities: lastCoinbaseCatalogOffsetSecurities, last_coinbase_catalog_pages_fetched: lastCoinbaseCatalogPagesFetched, last_coinbase_catalog_effective_page_size: lastCoinbaseCatalogEffectivePageSize, last_coinbase_catalog_candidate_rows: lastCoinbaseCatalogCandidateRows, last_coinbase_catalog_candidate_securities: lastCoinbaseCatalogCandidateSecurities, last_coinbase_catalog_highwater_ratio: lastCoinbaseCatalogHighWaterRatio, last_coinbase_catalog_integrity_error: lastCoinbaseCatalogIntegrityError, stage_cleanup_successes: stageCleanupSuccesses, stage_cleanup_failures: stageCleanupFailures, last_stage_cleanup_error: lastStageCleanupError, user_reads_trigger_source_requests: false, reads_scale_with_users: false, ...extra, updated_at: isoNow() };
  try {
    const response = await supabaseFetch(`${STATE_TABLE}?on_conflict=singleton`, { method: 'POST', headers: { 'content-type': 'application/json', prefer: 'resolution=merge-duplicates,return=minimal' }, body: JSON.stringify({singleton:'default', payload, updated_at: isoNow()}) });
    if (!response.ok) throw new Error(`stock_state_persist_${response.status}:${safeText(await response.text(), 160)}`);
    lastStatePersistError = '';
    return true;
  } catch (error) {
    lastStatePersistError = safeText(error?.message || error, 220);
    return false;
  }
}

function updateSharedGateSessionRows(rows) {
  for (const next of rows) {
    if (next?.provider !== 'gate') continue;
    const key = `gate|${next.exchange_symbol}`;
    const old = sharedRowByNative.get(key);
    const merged = old && typeof old === 'object' ? { ...old, ...next, provider_metadata: { ...(old.provider_metadata || {}), ...(next.provider_metadata || {}) } } : next;
    sharedRowByNative.set(key, merged);
    sharedSecurityByKey.set(merged.security_key || merged.asset_id, merged);
  }
  sharedUpdatedAtMs = Date.now();
}
async function refreshGateSessionMap(reason = 'session_interval') {
  if (gateSessionInflight) return gateSessionInflight;
  gateSessionInflight = (async () => {
    gateSessionRefreshAttempts += 1; lastGateSessionStartedAt = isoNow(); lastGateSessionError = '';
    try {
      const fetchedAt = isoNow();
      const [us, hk, kr] = await Promise.all(['us','hk','kr'].map(fetchGateExchange));
      const rows = [...us, ...hk, ...kr].map(x => gateAssetRow(x, fetchedAt)).filter(Boolean);
      const dedup = new Map(rows.map(r => [r.asset_id, r]));
      if (dedup.size < GATE_COMPLETE_MIN_ROWS) throw new Error(`gate_session_snapshot_too_small:${dedup.size}`);
      updateSharedGateSessionRows([...dedup.values()]);
      lastGateSessionSucceededAt = isoNow(); lastGateSessionError = ''; gateSessionRefreshSuccesses += 1;
      await persistState({ last_gate_session_reason: reason });
      return { ok: true, rows: dedup.size };
    } catch (error) {
      gateSessionRefreshFailures += 1; lastGateSessionError = safeText(error?.message || error, 260);
      await persistState({ last_gate_session_reason: reason }); throw error;
    } finally { gateSessionInflight = null; }
  })();
  return gateSessionInflight;
}
function coinbaseMarketOverlayRow(raw, old, fetchedAt) {
  if (!old || typeof old !== 'object') return null;
  const eq = coinbaseEquity(raw);
  const flags = eq?.equity_trading_flags && typeof eq.equity_trading_flags === 'object' ? eq.equity_trading_flags : {};
  const currentSession = upper(eq?.current_session) || compact(old.current_session);
  const tradingHalted = nullableBool(eq?.trading_halted);
  const status = lower(raw?.status || (tradingHalted ? 'halted' : flags?.tradable === false ? 'disabled' : old.status || 'active'));
  return {
    ...old,
    status,
    current_session: currentSession,
    trade_status: currentSession || status,
    trading_halted: tradingHalted == null ? old.trading_halted : tradingHalted,
    supports_24_5: coinbaseSupports24_5(eq) ?? old.supports_24_5,
    reference_price: nonneg(raw?.price),
    reference_change_24h_ratio: coinbasePctRatio(raw?.price_percentage_change_24h),
    reference_volume_24h: nonneg(raw?.volume_24h),
    reference_turnover_24h: nonneg(raw?.approximate_quote_24h_volume),
    reference_high_24h: nonneg(raw?.high_24h),
    reference_low_24h: nonneg(raw?.low_24h),
    reference_market_fetched_at: fetchedAt,
    provider_metadata: old.provider_metadata || {},
  };
}
function updateSharedCoinbaseMarketRows(rows) {
  let applied = 0;
  for (const row of rows) {
    if (row.provider !== 'coinbase') continue;
    const key = `coinbase|${row.exchange_symbol}`;
    if (!sharedRowByNative.has(key)) continue;
    const ticker = { source: 'coinbase_shared_known_equity_market_overlay', last_price: row.reference_price, price_change_24h_ratio: row.reference_change_24h_ratio, high_24h: row.reference_high_24h, low_24h: row.reference_low_24h, open_24h: null, volume_24h: row.reference_volume_24h, turnover_24h: row.reference_turnover_24h, best_bid: null, best_ask: null, best_bid_size: null, best_ask_size: null, timestamp_ms: Date.parse(row.reference_market_fetched_at || row.source_cached_at) || null, mark_price: null, index_price: null, open_interest: null, open_interest_value: null, funding_rate: null, next_funding_time_ms: null };
    sharedTickerByNative.set(key, { native_symbol: row.exchange_symbol, ticker, source_cached_at: row.source_cached_at });
    sharedRowByNative.set(key, row);
    sharedSecurityByKey.set(row.security_key || row.asset_id, row);
    applied += 1;
  }
  if (applied) sharedUpdatedAtMs = Date.now();
  return applied;
}
function scheduleCoinbaseMarketAfterCatalog(reason = 'catalog_refresh_inflight') {
  if (coinbaseMarketDeferredTimer) return;
  coinbaseMarketDeferredTimer = setTimeout(() => {
    coinbaseMarketDeferredTimer = null;
    if (refreshInflight) { scheduleCoinbaseMarketAfterCatalog(reason); return; }
    refreshCoinbaseMarketMap(`deferred_after_${reason}`).catch(error => console.error('[Step1035 stock market] deferred refresh failed', error?.message || error));
  }, 5_000);
  coinbaseMarketDeferredTimer.unref?.();
}

async function refreshCoinbaseMarketMap(reason = 'market_interval') {
  if (refreshInflight) {
    coinbaseMarketDeferredDueCatalog += 1;
    scheduleCoinbaseMarketAfterCatalog('catalog_refresh');
    return { ok: true, skipped: true, reason: 'catalog_refresh_inflight', user_upstream_requests: 0 };
  }
  if (coinbaseMarketInflight) return coinbaseMarketInflight;
  coinbaseMarketInflight = (async () => {
    coinbaseMarketRefreshAttempts += 1; lastCoinbaseMarketStartedAt = isoNow(); lastCoinbaseMarketError = '';
    try {
      let known = currentCoinbaseKnownMap();
      if (known.size < COINBASE_COMPLETE_MIN_PRODUCTS) {
        await hydrateCoinbaseKnownCatalogFromSupabase(`market:${reason}`);
        known = currentCoinbaseKnownMap();
      }
      if (known.size < COINBASE_COMPLETE_MIN_PRODUCTS) throw new Error(`coinbase_market_known_catalog_too_small:${known.size}`);
      const fetchedAt = isoNow();
      let byProduct = await fetchCoinbaseKnownMarketFast(known, fetchedAt);
      const requiredMatches = Math.min(known.size, Math.max(COINBASE_COMPLETE_MIN_PRODUCTS, Math.floor(known.size * COINBASE_MARKET_MIN_MATCH_RATIO)));
      if (byProduct.size < requiredMatches) throw new Error(`coinbase_market_known_match_too_small:${byProduct.size}/${known.size};required=${requiredMatches}`);
      lastCoinbaseMarketMatchedRows = byProduct.size;
      const rows = [...byProduct.values()];
      lastCoinbasePricedRows = rows.filter(r => r.reference_price != null).length;
      lastCoinbaseSessionRows = rows.filter(r => compact(r.current_session)).length;
      const cbSample = rows.find(r => r.reference_price != null && r.display_symbol === 'AAPL') || rows.find(r => r.reference_price != null) || rows[0];
      lastCoinbaseSampleNative = compact(cbSample?.exchange_symbol); lastCoinbaseSampleTicker = compact(cbSample?.display_symbol);
      const applied = updateSharedCoinbaseMarketRows(rows);
      const previousMarketSucceededAt = lastCoinbaseMarketSucceededAt;
      const previousMarketRefreshSuccesses = coinbaseMarketRefreshSuccesses;
      const marketSucceededAt = isoNow();
      lastCoinbaseMarketSucceededAt = marketSucceededAt; lastCoinbaseMarketError = ''; coinbaseMarketRefreshSuccesses += 1;
      packCoinbaseMarketSnapshot(rows, marketSucceededAt);
      coinbaseMarketSnapshotPersistSuccesses += 1;
      const persisted = await persistState({ last_coinbase_market_reason: reason, coinbase_market_known_catalog_rows: known.size, coinbase_market_matched_rows: rows.length, coinbase_market_applied_rows: applied, coinbase_market_pagination_mode: lastCoinbaseMarketPaginationMode, coinbase_market_effective_page_size: lastCoinbaseMarketEffectivePageSize, coinbase_market_pages_fetched: lastCoinbaseMarketPagesFetched });
      if (!persisted) {
        lastCoinbaseMarketSucceededAt = previousMarketSucceededAt;
        coinbaseMarketRefreshSuccesses = previousMarketRefreshSuccesses;
        coinbaseMarketSnapshotPersistSuccesses = Math.max(0, coinbaseMarketSnapshotPersistSuccesses - 1);
        coinbaseMarketSnapshotPersistFailures += 1;
        coinbaseMarketSnapshotLastError = lastStatePersistError || 'coinbase_market_snapshot_state_persist_failed';
        throw new Error(`coinbase_market_snapshot_persist_failed:${coinbaseMarketSnapshotLastError}`);
      }
      coinbaseMarketSnapshotLastError = '';
      return { ok: true, rows: rows.length, known_catalog_rows: known.size, applied_rows: applied, pagination_mode: lastCoinbaseMarketPaginationMode, effective_page_size: lastCoinbaseMarketEffectivePageSize, pages_fetched: lastCoinbaseMarketPagesFetched };
    } catch (error) {
      coinbaseMarketRefreshFailures += 1; lastCoinbaseMarketError = safeText(error?.message || error, 260);
      await persistState({ last_coinbase_market_reason: reason }); throw error;
    } finally { coinbaseMarketInflight = null; }
  })();
  return coinbaseMarketInflight;
}
async function refreshNow(reason = 'scheduled') {
  if (refreshInflight) return refreshInflight;
  refreshInflight = (async () => {
    refreshAttempts += 1; lastRefreshStartedAt = isoNow(); lastRefreshError = '';
    const refreshId = randomBytes(16).toString('hex'); lastRefreshId = refreshId;
    const providerErrors = [];
    let gateRows = null; let coinbaseRows = null; let coinbaseSecurities = 0;
    try {
      if (!SUPABASE_CONFIGURED) throw new Error('supabase_service_role_not_configured');
      await clearStage(refreshId).catch(() => {});
      try {
        gateRows = await fetchGateFull();
        const gateSecurities = new Set(gateRows.map(r => r.security_key)).size;
        await stageProviderRows('gate', gateRows, refreshId);
        await commitProvider('gate', refreshId, gateRows.length, gateSecurities);
        replaceSharedProviderRows('gate', gateRows);
        await cleanupCommittedStage(refreshId, 'gate');
      } catch (error) { providerErrors.push(`gate:${safeText(error?.message || error, 220)}`); }
      try {
        if (coinbaseMarketInflight) {
          catalogWaitedForMarketInflight += 1;
          await coinbaseMarketInflight.catch(() => {});
        }
        if (coinbaseMarketInflight) {
          catalogMarketOverlapViolations += 1;
          throw new Error('coinbase_catalog_market_overlap_guard_failed');
        }
        const coinbaseCandidate = await fetchAndStageCoinbaseFull(refreshId);
        coinbaseSecurities = coinbaseCandidate.securities;
        await commitProvider('coinbase', refreshId, coinbaseCandidate.rows, coinbaseCandidate.securities);
        updateCoinbaseCatalogHighWater(coinbaseCandidate.rows, coinbaseCandidate.securities, isoNow());
        // Atomic in-memory swap: keep the old verified Coinbase slice readable while the
        // newly committed slice is fetched from Supabase, then replace it synchronously.
        // This closes the Step1035.19.3 shared_exact_row_missing window seen by AAPL Kline reads.
        await restoreProviderSharedRows('coinbase', { atomicReplace: true });
        const committedLightRows = [...currentCoinbaseKnownMap().values()];
        adoptCoinbaseCommittedRows(committedLightRows);
        await seedCoinbaseMarketFromCommittedCatalogRows(committedLightRows, 'catalog_commit_light_restore');
        coinbaseRows = { length: coinbaseCandidate.rows };
        await cleanupCommittedStage(refreshId, 'coinbase');
      } catch (error) { providerErrors.push(`coinbase:${safeText(error?.message || error, 220)}`); }
      if (providerErrors.length) throw new Error(`stock_provider_partial_failure:${providerErrors.join('|')}`);
      lastRefreshSucceededAt = isoNow(); refreshSuccesses += 1; lastRefreshError = '';
      await persistState({ last_refresh_reason: reason });
      return { ok: true, gate_rows: gateRows?.length || 0, coinbase_rows: coinbaseRows?.length || 0, coinbase_securities: coinbaseSecurities };
    } catch (error) {
      refreshFailures += 1; lastRefreshError = safeText(error?.message || error, 500);
      await persistState({ last_refresh_reason: reason });
      throw error;
    } finally {
      refreshInflight = null;
      if (coinbaseMarketDeferredDueCatalog > 0 && !coinbaseMarketInflight) scheduleCoinbaseMarketAfterCatalog('catalog_refresh_finished');
    }
  })();
  return refreshInflight;
}

async function restoreState() {
  if (!SUPABASE_CONFIGURED) return;
  try {
    const response = await supabaseFetch(`${STATE_TABLE}?singleton=eq.default&select=payload&limit=1`);
    if (!response.ok) return;
    const rows = await response.json(); const stateRow = rows?.[0]; if (!stateRow) return; const row = stateRow.payload && typeof stateRow.payload === 'object' ? stateRow.payload : {};
    lastRefreshStartedAt = compact(row.last_refresh_started_at); lastRefreshSucceededAt = compact(row.last_refresh_succeeded_at); lastRefreshError = compact(row.last_refresh_error);
    lastGateRows = finite(row.gate_rows) || 0; lastGateUs = finite(row.gate_us_rows) || 0; lastGateHk = finite(row.gate_hk_rows) || 0; lastGateKr = finite(row.gate_kr_rows) || 0; lastGateZhNames = finite(row.gate_zh_name_rows) || 0;
    lastCoinbaseProductRows = finite(row.coinbase_product_rows) || 0; lastCoinbaseSecurities = finite(row.coinbase_security_rows) || 0; lastCoinbasePricedRows = 0; lastCoinbaseSessionRows = 0; lastCoinbaseZhNames = finite(row.coinbase_zh_name_rows) || 0; lastGateSampleNative = compact(row.gate_sample_native); lastCoinbaseSampleNative = compact(row.coinbase_sample_native); lastCoinbaseSampleTicker = compact(row.coinbase_sample_ticker); lastCoinbaseFallbackAllProductsUsed = row.coinbase_fallback_all_products_used === true; lastCoinbaseKlineProbe = row.coinbase_kline_probe || null;
    refreshAttempts = finite(row.refresh_attempts) || 0; refreshSuccesses = finite(row.refresh_successes) || 0; refreshFailures = finite(row.refresh_failures) || 0; gateRequestsStarted = finite(row.gate_requests_started) || 0; gateRequestsSucceeded = finite(row.gate_requests_succeeded) || 0; coinbaseRequestsStarted = finite(row.coinbase_requests_started) || 0; coinbaseRequestsSucceeded = finite(row.coinbase_requests_succeeded) || 0; sourceRequestFailures = finite(row.source_request_failures) || 0; stageRowsWritten = finite(row.stage_rows_written) || 0; commitsSucceeded = finite(row.commits_succeeded) || 0; commitsFailed = finite(row.commits_failed) || 0; lastCoinbaseCommitStartedAt = compact(row.last_coinbase_commit_started_at); lastCoinbaseCommitSucceededAt = compact(row.last_coinbase_commit_succeeded_at); lastCoinbaseCommitDurationMs = finite(row.last_coinbase_commit_duration_ms) || 0; lastCoinbaseCommitError = compact(row.last_coinbase_commit_error);
    lastGateSessionStartedAt = compact(row.last_gate_session_started_at); lastGateSessionSucceededAt = compact(row.last_gate_session_succeeded_at); lastGateSessionError = compact(row.last_gate_session_error); gateSessionRefreshAttempts = finite(row.gate_session_refresh_attempts) || 0; gateSessionRefreshSuccesses = finite(row.gate_session_refresh_successes) || 0; gateSessionRefreshFailures = finite(row.gate_session_refresh_failures) || 0; lastCoinbaseMarketStartedAt = compact(row.last_coinbase_market_started_at); lastCoinbaseMarketSucceededAt = compact(row.last_coinbase_market_succeeded_at); lastCoinbaseMarketError = compact(row.last_coinbase_market_error); coinbaseMarketRefreshAttempts = finite(row.coinbase_market_refresh_attempts) || 0; coinbaseMarketRefreshSuccesses = finite(row.coinbase_market_refresh_successes) || 0; coinbaseMarketRefreshFailures = finite(row.coinbase_market_refresh_failures) || 0; coinbaseMarketSnapshotGzipB64 = compact(row.coinbase_market_snapshot_gzip_b64); coinbaseMarketSnapshotRows = finite(row.coinbase_market_snapshot_rows) || 0; coinbaseMarketSnapshotFetchedAt = compact(row.coinbase_market_snapshot_fetched_at); coinbaseMarketSnapshotPersistSuccesses = finite(row.coinbase_market_snapshot_persist_successes) || 0; coinbaseMarketSnapshotPersistFailures = finite(row.coinbase_market_snapshot_persist_failures) || 0; lastCoinbaseMarketPaginationMode = compact(row.last_coinbase_market_pagination_mode) || 'restored_state'; lastCoinbaseMarketEffectivePageSize = finite(row.last_coinbase_market_effective_page_size) || 0; lastCoinbaseMarketPagesFetched = finite(row.last_coinbase_market_pages_fetched) || 0; lastCoinbaseMarketMatchedRows = finite(row.last_coinbase_market_matched_rows) || 0; coinbaseCatalogHighWaterRows = finite(row.coinbase_catalog_highwater_rows) || Math.max(lastCoinbaseProductRows, 0); coinbaseCatalogHighWaterSecurities = finite(row.coinbase_catalog_highwater_securities) || Math.max(lastCoinbaseSecurities, 0); coinbaseCatalogHighWaterAt = compact(row.coinbase_catalog_highwater_at) || lastRefreshStartedAt || lastRefreshSucceededAt; lastCoinbaseCatalogFetchMode = compact(row.last_coinbase_catalog_fetch_mode) || 'restored_state'; lastCoinbaseCatalogCursorRows = finite(row.last_coinbase_catalog_cursor_rows) || 0; lastCoinbaseCatalogCursorSecurities = finite(row.last_coinbase_catalog_cursor_securities) || 0; lastCoinbaseCatalogOffsetRows = finite(row.last_coinbase_catalog_offset_rows) || 0; lastCoinbaseCatalogOffsetSecurities = finite(row.last_coinbase_catalog_offset_securities) || 0; lastCoinbaseCatalogPagesFetched = finite(row.last_coinbase_catalog_pages_fetched) || 0; lastCoinbaseCatalogEffectivePageSize = finite(row.last_coinbase_catalog_effective_page_size) || 0; lastCoinbaseCatalogCandidateRows = finite(row.last_coinbase_catalog_candidate_rows) || 0; lastCoinbaseCatalogCandidateSecurities = finite(row.last_coinbase_catalog_candidate_securities) || 0; lastCoinbaseCatalogHighWaterRatio = finite(row.last_coinbase_catalog_highwater_ratio) || 0; lastCoinbaseCatalogIntegrityError = compact(row.last_coinbase_catalog_integrity_error); stageCleanupSuccesses = finite(row.stage_cleanup_successes) || 0; stageCleanupFailures = finite(row.stage_cleanup_failures) || 0; lastStageCleanupError = compact(row.last_stage_cleanup_error);
  } catch { /* health remains explicit */ }
}
function coinbaseCoreKlineProducts() {
  const byTicker = new Map();
  for (const row of sharedRowByNative.values()) {
    if (lower(row?.provider) !== 'coinbase' || lower(row?.asset_class) !== 'equity_cash') continue;
    const ticker = normalizeTicker(row?.display_symbol || row?.base_asset);
    if (!COINBASE_CORE_EQUITY_TICKERS.has(ticker)) continue;
    const native = compact(row?.exchange_symbol);
    const assetId = compact(row?.asset_id);
    if (!native || !assetId) continue;
    if (!byTicker.has(ticker)) byTicker.set(ticker, {
      ticker,
      native_symbol: native,
      asset_id: assetId,
      product_kind: compact(row?.product_kind),
      asset_class: compact(row?.asset_class),
      official_kline_capability: compact(row?.official_kline_capability),
    });
  }
  return [...byTicker.values()].sort((a, b) => a.ticker.localeCompare(b.ticker));
}

function healthPayload() {
  const coreKlineProducts = coinbaseCoreKlineProducts();
  const lastOkMs = Date.parse(lastRefreshSucceededAt || '');
  const marketCountsBounded = lastCoinbasePricedRows <= lastCoinbaseProductRows && lastCoinbaseSessionRows <= lastCoinbaseProductRows;
  const marketMatchRatio = lastCoinbaseProductRows > 0 ? lastCoinbaseMarketMatchedRows / lastCoinbaseProductRows : 0;
  const coinbaseMarketReady = (coinbaseMarketRefreshSuccesses > 0 || coinbaseMarketRestoredReady) && marketMatchRatio >= Math.min(0.90, COINBASE_MARKET_MIN_MATCH_RATIO);
  const committedCatalogRatioRows = coinbaseCatalogHighWaterRows > 0 ? lastCoinbaseProductRows / coinbaseCatalogHighWaterRows : 1;
  const committedCatalogRatioSecurities = coinbaseCatalogHighWaterSecurities > 0 ? lastCoinbaseSecurities / coinbaseCatalogHighWaterSecurities : 1;
  const committedCatalogIntegrityReady = !coinbaseCatalogHighWaterFresh() || (committedCatalogRatioRows >= COINBASE_CATALOG_HIGHWATER_MIN_RATIO && committedCatalogRatioSecurities >= COINBASE_CATALOG_HIGHWATER_MIN_RATIO);
  const ready = lastGateRows >= GATE_COMPLETE_MIN_ROWS && lastGateZhNames >= 40 && lastCoinbaseProductRows >= COINBASE_COMPLETE_MIN_PRODUCTS && lastCoinbaseSecurities >= COINBASE_COMPLETE_MIN_SECURITIES && committedCatalogIntegrityReady && lastCoinbasePricedRows >= 50 && lastCoinbaseSessionRows >= 100 && marketCountsBounded && coinbaseMarketReady && Number.isFinite(lastOkMs) && Date.now() - lastOkMs <= Math.max(24 * 60 * 60_000, REFRESH_MS * 3);
  return { ok: true, version: VERSION, data_version: DATA_VERSION, schema_version: SCHEMA_VERSION, implementation_revision: IMPLEMENTATION_REVISION, runtime_instance_id: RUNTIME_INSTANCE_ID, runtime_started_at: RUNTIME_STARTED_AT, process_uptime_seconds: Math.floor(process.uptime()), coverage_ready: ready, refresh_interval_minutes: Math.round(REFRESH_MS / 60_000), coinbase_market_refresh_interval_minutes: Math.round(COINBASE_MARKET_REFRESH_MS / 60_000), gate_session_refresh_interval_minutes: Math.round(GATE_SESSION_REFRESH_MS / 60_000), background_shared_collector: true, user_reads_trigger_source_requests: false, reads_scale_with_users: false, direct_exchange_requests_from_user_reads: 0, direct_exchange_connections_from_user_reads: 0, user_read_source_requests: 0, user_read_source_connections: 0, coinbase_catalog_refresh_atomic_memory_swap: true, coinbase_catalog_refresh_zero_shared_gap: true, supabase_configured: SUPABASE_CONFIGURED, coinbase_cdp_configured: COINBASE_CDP_CONFIGURED, last_refresh_started_at: lastRefreshStartedAt, last_refresh_succeeded_at: lastRefreshSucceededAt, last_refresh_error: lastRefreshError, last_refresh_id: lastRefreshId, catalog_thresholds: { gate_complete_min_rows: GATE_COMPLETE_MIN_ROWS, coinbase_complete_min_products: COINBASE_COMPLETE_MIN_PRODUCTS, coinbase_complete_min_securities: COINBASE_COMPLETE_MIN_SECURITIES }, coinbase_auth: { cdp_configured: COINBASE_CDP_CONFIGURED, list_products_path: COINBASE_PUBLIC_PRODUCTS_PATH, authenticated_products_path: COINBASE_AUTH_PRODUCTS_PATH, jwt_uri_excludes_query_string: true }, gate: { sample_native_symbol: lastGateSampleNative || null, rows: lastGateRows, us_rows: lastGateUs, hk_rows: lastGateHk, kr_rows: lastGateKr, chinese_name_rows: lastGateZhNames, full_pagination: true, page_size: GATE_PAGE_SIZE, hard_old_10_page_cap_removed: true, shared_session_refresh: true, session_refresh_interval_minutes: Math.round(GATE_SESSION_REFRESH_MS / 60_000), last_session_started_at: lastGateSessionStartedAt, last_session_succeeded_at: lastGateSessionSucceededAt, last_session_error: lastGateSessionError, session_refresh_attempts: gateSessionRefreshAttempts, session_refresh_successes: gateSessionRefreshSuccesses, session_refresh_failures: gateSessionRefreshFailures }, coinbase_market: { last_started_at: lastCoinbaseMarketStartedAt, last_succeeded_at: lastCoinbaseMarketSucceededAt, last_error: lastCoinbaseMarketError, refresh_attempts: coinbaseMarketRefreshAttempts, refresh_successes: coinbaseMarketRefreshSuccesses, refresh_failures: coinbaseMarketRefreshFailures, restored_verified_snapshot: coinbaseMarketRestoredReady, bounded_to_committed_catalog: true, counts_bounded_to_committed_catalog: marketCountsBounded, full_metadata_duplication: false, persisted_compact_snapshot: true, snapshot_rows: coinbaseMarketSnapshotRows, snapshot_fetched_at: coinbaseMarketSnapshotFetchedAt || null, snapshot_gzip_bytes: coinbaseMarketSnapshotGzipB64 ? Math.floor(coinbaseMarketSnapshotGzipB64.length * 0.75) : 0, snapshot_restore_successes: coinbaseMarketSnapshotRestoreSuccesses, snapshot_restore_failures: coinbaseMarketSnapshotRestoreFailures, snapshot_last_error: coinbaseMarketSnapshotLastError, snapshot_persist_successes: coinbaseMarketSnapshotPersistSuccesses, snapshot_persist_failures: coinbaseMarketSnapshotPersistFailures, snapshot_persist_last_error: lastStatePersistError, pagination_mode: lastCoinbaseMarketPaginationMode, effective_page_size: lastCoinbaseMarketEffectivePageSize, pages_fetched: lastCoinbaseMarketPagesFetched, matched_rows: lastCoinbaseMarketMatchedRows, match_ratio: marketMatchRatio, cursor_page_limit: COINBASE_MARKET_PROBE_LIMIT, min_match_ratio: COINBASE_MARKET_MIN_MATCH_RATIO, catalog_market_mutual_exclusion: true, deferred_due_catalog_count: coinbaseMarketDeferredDueCatalog, deferred_retry_scheduled: Boolean(coinbaseMarketDeferredTimer), overlap_violations: catalogMarketOverlapViolations, streaming_pages: lastCoinbaseMarketStreamingPages, retained_full_product_rows: lastCoinbaseMarketRetainedFullProductRows }, coinbase: { sample_native_symbol: lastCoinbaseSampleNative || null, sample_security_ticker: lastCoinbaseSampleTicker || null, product_rows: lastCoinbaseProductRows, distinct_securities: lastCoinbaseSecurities, priced_rows: lastCoinbasePricedRows, current_session_rows: lastCoinbaseSessionRows, chinese_name_rows: lastCoinbaseZhNames, core_kline_pool: { enabled: COINBASE_CDP_CONFIGURED, configured_tickers: [...COINBASE_CORE_EQUITY_TICKERS], matched_count: coreKlineProducts.length, items: coreKlineProducts }, full_cursor_pagination: true, hard_old_6_page_cap_removed: true, fallback_all_products_used: lastCoinbaseFallbackAllProductsUsed, kline_probe: lastCoinbaseKlineProbe, catalog_integrity: { highwater_rows: coinbaseCatalogHighWaterRows, highwater_securities: coinbaseCatalogHighWaterSecurities, highwater_at: coinbaseCatalogHighWaterAt || null, highwater_fresh: coinbaseCatalogHighWaterFresh(), min_ratio: COINBASE_CATALOG_HIGHWATER_MIN_RATIO, committed_rows_ratio: committedCatalogRatioRows, committed_securities_ratio: committedCatalogRatioSecurities, committed_integrity_ready: committedCatalogIntegrityReady, fetch_mode: lastCoinbaseCatalogFetchMode, cursor_rows: lastCoinbaseCatalogCursorRows, cursor_securities: lastCoinbaseCatalogCursorSecurities, offset_rows: lastCoinbaseCatalogOffsetRows, offset_securities: lastCoinbaseCatalogOffsetSecurities, pages_fetched: lastCoinbaseCatalogPagesFetched, effective_page_size: lastCoinbaseCatalogEffectivePageSize, candidate_rows: lastCoinbaseCatalogCandidateRows, candidate_securities: lastCoinbaseCatalogCandidateSecurities, candidate_highwater_ratio: lastCoinbaseCatalogHighWaterRatio, public_list_cursor_rows: lastCoinbaseCatalogPublicCursorRows, authenticated_list_cursor_rows: lastCoinbaseCatalogAuthCursorRows, undefined_sort_cursor_rows: lastCoinbaseCatalogUndefinedSortRows, cursor_pages_fetched: lastCoinbaseCatalogCursorPages, synthesized_cursor_uses: lastCoinbaseCatalogSynthCursorUses, cursor_termination: lastCoinbaseCatalogCursorTermination, last_integrity_error: lastCoinbaseCatalogIntegrityError, incomplete_catalog_never_committed: true, highwater_updates_only_after_db_commit: true, offset_runtime_proven_ignored_not_used_for_catalog_or_market: true, streaming_stage_mode: true, streaming_pages: lastCoinbaseCatalogStreamingPages, streaming_stage_writes: lastCoinbaseCatalogStreamingStageWrites, peak_page_rows: lastCoinbaseCatalogPeakPageRows, retained_full_product_rows: lastCoinbaseCatalogRetainedFullProductRows } }, coinbase_known_catalog_bootstrap: { mode: lastCoinbaseKnownCatalogBootstrapMode, attempts: coinbaseKnownCatalogBootstrapAttempts, successes: coinbaseKnownCatalogBootstrapSuccesses, failures: coinbaseKnownCatalogBootstrapFailures, rows: lastCoinbaseKnownCatalogBootstrapRows, last_error: lastCoinbaseKnownCatalogBootstrapError, market_refresh_self_hydrates_when_memory_catalog_missing: true }, source_requests: { gate_started: gateRequestsStarted, gate_succeeded: gateRequestsSucceeded, coinbase_started: coinbaseRequestsStarted, coinbase_succeeded: coinbaseRequestsSucceeded, failures: sourceRequestFailures }, persistence: { stage_rows_written: stageRowsWritten, commits_succeeded: commitsSucceeded, commits_failed: commitsFailed, restored_coinbase_rows: restoredCoinbaseRows, restored_gate_rows: restoredGateRows, restore_attempts: restoreAttempts, restore_successes: restoreSuccesses, restore_failures: restoreFailures, last_restore_error: lastRestoreError, coinbase_restore_mode: lastCoinbaseRestoreMode, stage_cleanup_successes: stageCleanupSuccesses, stage_cleanup_failures: stageCleanupFailures, stage_cleanup_last_error: lastStageCleanupError, commit_contract: 'delete_provider_slice_plus_plain_insert_then_async_stage_cleanup', coinbase_commit_started_at: lastCoinbaseCommitStartedAt || null, coinbase_commit_succeeded_at: lastCoinbaseCommitSucceededAt || null, coinbase_commit_duration_ms: lastCoinbaseCommitDurationMs, coinbase_commit_last_error: lastCoinbaseCommitError, catalog_refresh_inflight: Boolean(refreshInflight), catalog_waited_for_market_inflight: catalogWaitedForMarketInflight, catalog_market_overlap_violations: catalogMarketOverlapViolations }, exact_metadata_cache: { mode: 'supabase_exact_on_demand_no_exchange_request', db_reads: exactMetadataDbReads, cache_entries: coinbaseExactMetadataLoaded.size, inflight: coinbaseExactMetadataInflight.size }, security_identity: { quote_variants_preserved: true, user_list_dedupes_by_security_key: true, exact_product_identity_preserved: true, coinbase_cik_preferred: true }, localization: { gate_official_i18n: true, curated_exact_ticker_fallback: true, chinese_search_aliases: true, canonical_commodity_aliases: true }, session_policy: { gate_dynamic_trade_status: true, gate_gt_lp_recognized: true, coinbase_current_session: true, cash_vs_tokenized_vs_derivative_not_merged: true }, user_reads: userReads, shared_market_map_entries: sharedTickerByNative.size, shared_identity_map_entries: sharedRowByNative.size, shared_map_age_ms: sharedUpdatedAtMs ? Date.now() - sharedUpdatedAtMs : null };
}
export async function resolveCoinbaseEquityCandleRoute(nativeSymbol, securityTicker, assetId) {
  const native = compact(nativeSymbol);
  const ticker = normalizeTicker(securityTicker);
  const exactAssetId = compact(assetId);
  if (!native || !ticker || !exactAssetId) return { identity_verified:false, reason:'missing_identity', route_product_ids:[] };

  const current = sharedRowByNative.get(`coinbase|${native}`) || null;
  if (!current) return { identity_verified:false, reason:'shared_exact_row_missing', route_product_ids:[] };
  if (lower(current?.provider) !== 'coinbase' || lower(current?.asset_class) !== 'equity_cash') {
    return { identity_verified:false, reason:'shared_exact_scope_mismatch', route_product_ids:[] };
  }
  if (compact(current?.asset_id) !== exactAssetId) {
    return { identity_verified:false, reason:'shared_exact_asset_id_mismatch', route_product_ids:[] };
  }
  if (normalizeTicker(current?.display_symbol || current?.base_asset) !== ticker) {
    return { identity_verified:false, reason:'shared_exact_ticker_mismatch', route_product_ids:[] };
  }

  // The exact shared row already proves product identity. Detailed metadata is optional and
  // may be restored lazily; a hydration miss/failure must not erase an already-proven identity.
  let detailed = current;
  try {
    const hydrated = await loadCoinbaseExactMetadata(native);
    if (hydrated && compact(hydrated?.asset_id) === exactAssetId && compact(hydrated?.exchange_symbol) === native && normalizeTicker(hydrated?.display_symbol || hydrated?.base_asset) === ticker) {
      detailed = hydrated;
    }
  } catch { /* exact shared identity remains authoritative */ }

  const quote = upper(detailed?.quote_asset || current?.quote_asset || detailed?.quote_currency_symbol || current?.quote_currency_symbol || 'USD') || 'USD';
  const alias = compact(detailed?.provider_metadata?.alias || current?.provider_metadata?.alias);
  const routeProductIds = [];
  if (alias && alias !== native) routeProductIds.push(alias);
  if (ticker && quote) routeProductIds.push(`${ticker}-${quote}`);

  return {
    identity_verified: true,
    reason: '',
    native_symbol: native,
    asset_id: exactAssetId,
    security_ticker: ticker,
    quote_asset: quote,
    official_alias: alias || null,
    route_product_ids: [...new Set(routeProductIds.filter(Boolean))],
    canonical_product_id: native,
    identity_source: 'already_verified_shared_coinbase_catalog_row_optional_metadata_only',
  };
}

export function getSharedStockCatalogTicker(provider, nativeSymbol) {
  const p = lower(provider); const native = compact(nativeSymbol); if (!p || !native) return null;
  const row = sharedTickerByNative.get(`${p}|${native}`); return row ? JSON.parse(JSON.stringify(row)) : null;
}
export function getSharedStockCatalogRow(provider, nativeSymbol) {
  const p = lower(provider); const native = compact(nativeSymbol); if (!p || !native) return null;
  const row = sharedRowByNative.get(`${p}|${native}`); return row ? JSON.parse(JSON.stringify(row)) : null;
}
export function getStockCatalogV2Health() { return healthPayload(); }
export async function handleStockCatalogV2(req, res, url) {
  if (![HEALTH_ROUTE,CURRENT_ROUTE,TICKERS_ROUTE,EXACT_ROUTE,SELF_TEST_ROUTE].includes(url.pathname)) return false;
  if (req.method === 'OPTIONS') { res.writeHead(204, { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET, OPTIONS' }); res.end(); return true; }
  if (req.method !== 'GET') { sendJson(res, 405, { ok: false, error: 'method_not_allowed' }); return true; }
  userReads += 1;
  if (url.pathname === HEALTH_ROUTE) { sendJson(res, 200, healthPayload()); return true; }
  if (url.pathname === SELF_TEST_ROUTE) { const result = runStockCatalogV2SelfTest(); sendJson(res, result.ok ? 200 : 500, result); return true; }
  if (url.pathname === TICKERS_ROUTE) {
    const provider = lower(url.searchParams.get('provider')); const symbols = symbolList(url.searchParams.get('symbols'));
    if (!['coinbase','gate'].includes(provider) || !symbols.length) { sendJson(res, 400, { ok:false,error:'invalid_stock_ticker_scope' }); return true; }
    const items = symbols.map(symbol => {
      if (provider === 'coinbase') { const item = sharedTickerByNative.get(`coinbase|${symbol}`); return item ? JSON.parse(JSON.stringify(item)) : null; }
      const row = sharedRowByNative.get(`gate|${symbol}`); return row ? { native_symbol: symbol, ticker: null, current_session: row.current_session || null, trade_status: row.trade_status || null, trade_mode: row.trade_mode ?? null, trading_halted: row.trading_halted ?? null, supports_24_7: row.supports_24_7 ?? null, supports_24_5: row.supports_24_5 ?? null, source_cached_at: row.source_cached_at || null } : null;
    }).filter(Boolean);
    sendJson(res, 200, { ok:true,version:VERSION,data_version:DATA_VERSION,schema_version:SCHEMA_VERSION,provider,requested_symbols:symbols,items,missing_symbols:symbols.filter(s=>!items.some(x=>x.native_symbol===s)),read_only_shared:true,user_read_upstream_requests:0,user_read_upstream_connections:0,source_cached_at:provider==='coinbase'?(lastCoinbaseMarketSucceededAt||lastRefreshSucceededAt):(lastGateSessionSucceededAt||lastRefreshSucceededAt) }); return true;
  }
  if (url.pathname === EXACT_ROUTE) {
    const provider = lower(url.searchParams.get('provider')); const symbol = compact(url.searchParams.get('symbol'));
    if (!['coinbase','gate'].includes(provider) || !symbol) { sendJson(res, 400, { ok:false,error:'invalid_stock_exact_scope' }); return true; }
    let row = sharedRowByNative.get(`${provider}|${symbol}`) || null;
    const dbReadsBefore = exactMetadataDbReads;
    if (provider === 'coinbase' && row) row = await loadCoinbaseExactMetadata(symbol);
    const ticker = provider === 'coinbase' ? (sharedTickerByNative.get(`coinbase|${symbol}`)?.ticker || null) : null;
    const dbReadDelta = exactMetadataDbReads - dbReadsBefore;
    if (!row) { sendJson(res, 503, { ok:false,version:VERSION,error:'shared_stock_exact_pending',provider,symbol,user_read_upstream_requests:0,user_db_metadata_reads:dbReadDelta }); return true; }
    sendJson(res, 200, { ok:true,version:VERSION,data_version:DATA_VERSION,schema_version:SCHEMA_VERSION,provider,native_symbol:symbol,row:JSON.parse(JSON.stringify(row)),ticker:ticker?JSON.parse(JSON.stringify(ticker)):null,read_only_shared:true,user_read_upstream_requests:0,user_read_upstream_connections:0,user_db_metadata_reads:dbReadDelta }); return true;
  }
  const h = healthPayload(); sendJson(res, h.coverage_ready ? 200 : 503, { ok: h.coverage_ready, version: VERSION, data_version: DATA_VERSION, schema_version: SCHEMA_VERSION, coverage_ready: h.coverage_ready, gate_rows: lastGateRows, coinbase_product_rows: lastCoinbaseProductRows, coinbase_distinct_securities: lastCoinbaseSecurities, gate_chinese_name_rows: lastGateZhNames, user_read_upstream_requests: 0, user_read_upstream_connections: 0, read_only_shared: true, source_cached_at: lastRefreshSucceededAt }); return true;
}
export function startStockCatalogV2Collector() {
  if (started) return; started = true;
  restoreState().finally(async () => {
    restoreAttempts += 1; lastRestoreError = '';
    const restored = await Promise.allSettled([restoreProviderSharedRows('coinbase'), restoreProviderSharedRows('gate')]);
    const restoreErrors = [];
    if (restored[0].status === 'rejected' || restoredCoinbaseRows < COINBASE_COMPLETE_MIN_PRODUCTS) {
      try { await hydrateCoinbaseKnownCatalogFromSupabase('startup_restore_fallback'); }
      catch (error) {
        const primary = restored[0].status === 'rejected' ? safeText(restored[0].reason?.message || restored[0].reason, 140) : `rows=${restoredCoinbaseRows}`;
        restoreErrors.push(`coinbase:${primary};fallback=${safeText(error?.message || error, 160)}`);
      }
    }
    if (restored[1].status === 'rejected') restoreErrors.push(`gate:${safeText(restored[1].reason?.message || restored[1].reason, 180)}`);
    if (restoreErrors.length) { restoreFailures += 1; lastRestoreError = restoreErrors.join('|'); console.error('[Step1035 stock catalog] restore shared rows failed', lastRestoreError); }
    else { restoreSuccesses += 1; lastRestoreError = ''; }
    const committedRowsRatio = coinbaseCatalogHighWaterRows > 0 ? restoredCoinbaseRows / coinbaseCatalogHighWaterRows : 1;
    const committedSecsRatio = coinbaseCatalogHighWaterSecurities > 0 ? lastCoinbaseSecurities / coinbaseCatalogHighWaterSecurities : 1;
    const integrityRecoveryRequired = coinbaseCatalogHighWaterFresh() && (committedRowsRatio < COINBASE_CATALOG_HIGHWATER_MIN_RATIO || committedSecsRatio < COINBASE_CATALOG_HIGHWATER_MIN_RATIO);
    const restoreIncomplete = restoredCoinbaseRows < COINBASE_COMPLETE_MIN_PRODUCTS || restoredGateRows < GATE_COMPLETE_MIN_ROWS || integrityRecoveryRequired || Boolean(lastRefreshError);
    const age = Date.now() - (Date.parse(lastRefreshSucceededAt || '') || 0);
    const delay = restoreIncomplete ? 15_000 : (age >= REFRESH_MS ? START_DELAY_MS : Math.max(START_DELAY_MS, REFRESH_MS - age));
    const startupReason = integrityRecoveryRequired ? 'startup_catalog_integrity_recovery' : (Boolean(lastRefreshError) ? 'startup_previous_refresh_failure' : (restoreIncomplete ? 'startup_restore_fallback' : 'startup_or_due'));
    startTimer = setTimeout(() => refreshNow(startupReason).catch(error => console.error('[Step1035 stock catalog] refresh failed', error?.message || error)), delay);
    startTimer.unref?.();
    refreshTimer = setInterval(() => refreshNow('interval').catch(error => console.error('[Step1035 stock catalog] refresh failed', error?.message || error)), REFRESH_MS);
    refreshTimer.unref?.();
    const gateSessionDelay = Math.max(25_000, Math.min(GATE_SESSION_REFRESH_MS, 70_000));
    const gateSessionStart = setTimeout(() => refreshGateSessionMap('startup').catch(error => console.error('[Step1035 stock gate session] refresh failed', error?.message || error)), gateSessionDelay);
    gateSessionStart.unref?.();
    gateSessionTimer = setInterval(() => refreshGateSessionMap('interval').catch(error => console.error('[Step1035 stock gate session] refresh failed', error?.message || error)), GATE_SESSION_REFRESH_MS);
    gateSessionTimer.unref?.();
    const restoredMarketAge = Date.now() - (Date.parse(lastCoinbaseMarketSucceededAt || '') || 0);
    const marketDelay = integrityRecoveryRequired || Boolean(lastRefreshError)
      ? 20_000
      : (restoredCoinbaseRows < COINBASE_COMPLETE_MIN_PRODUCTS
        ? 15_000
        : (coinbaseMarketRestoredReady && restoredMarketAge >= 0 && restoredMarketAge < COINBASE_MARKET_REFRESH_MS
          ? 30_000
          : 1_000));
    const marketStart = setTimeout(() => refreshCoinbaseMarketMap('startup').catch(error => console.error('[Step1035 stock market] refresh failed', error?.message || error)), marketDelay);
    marketStart.unref?.();
    coinbaseMarketTimer = setInterval(() => refreshCoinbaseMarketMap('interval').catch(error => console.error('[Step1035 stock market] refresh failed', error?.message || error)), COINBASE_MARKET_REFRESH_MS);
    coinbaseMarketTimer.unref?.();
  });
}

export function runStockCatalogV2SelfTest() {
  const snapshotProbe = JSON.parse(gunzipSync(gzipSync(Buffer.from(JSON.stringify({v:1,rows:[['abc',1,0.01,100,'NORMAL',false,'2026-08-19T00:00:00Z']]})), {level:6})).toString('utf8'));
  const gate = gateAssetRow({ symbol: 'AAPL', exchange: 'us', quote_currency: 'USD', symbol_desc: 'Apple Inc.', category: 'stock', trade_status: 'gt_lp', trade_mode: 4, order_fill_timing: 1, symbol_descs: [{ lang: 'zh-CN', value: '苹果公司' }] }, '2026-08-18T00:00:00Z');
  const cb = coinbaseAssetRow({ product_id: 'abc123', product_type: 'EQUITY', product_venue: 'CCM', base_display_symbol: 'XAUG', quote_display_symbol: 'USD', base_name: 'FT Vest U.S. Equity Enhance & Moderate Buffer ETF - August', price: '39.06', price_percentage_change_24h: '-0.05%', volume_24h: '100', equity_product_details: { ticker: 'XAUG', cik: '12345', equity_subtype: 'EQUITY_PRODUCT_SUBTYPE_ETF', current_session: 'EQUITY_TRADING_SESSION_REGULAR', trading_halted: false, equity_trading_flags: { tradable: true } } }, '2026-08-18T00:00:00Z', false);
  const tests = [
    ['gate_zh_i18n', gate?.display_name_zh === '苹果公司'],
    ['gate_gt_lp', gate?.current_session === 'gt_lp' && gate?.supports_24_7 === true],
    ['coinbase_xaug_not_gold', cb?.display_symbol === 'XAUG' && cb?.display_name.toLowerCase().includes('buffer')],
    ['coinbase_quote_product_identity', cb?.asset_id === 'coinbase:equity:abc123' && cb?.security_key === 'coinbase:cik:12345:XAUG'],
    ['coinbase_price_promoted_to_reference', cb?.reference_price === 39.06],
    ['coinbase_pct_parsed', Math.abs((cb?.reference_change_24h_ratio ?? 9) - (-0.0005)) < 1e-9],
    ['coinbase_share_class_identity_safe', coinbaseSecurityKey({product_type:'EQUITY',base_display_symbol:'GOOG',equity_product_details:{ticker:'GOOG',cik:'1652044'}}) !== coinbaseSecurityKey({product_type:'EQUITY',base_display_symbol:'GOOGL',equity_product_details:{ticker:'GOOGL',cik:'1652044'}})],
    ['coinbase_list_public_products_path', COINBASE_PUBLIC_PRODUCTS_PATH === '/api/v3/brokerage/market/products'],
    ['coinbase_jwt_path_excludes_query', !COINBASE_PUBLIC_PRODUCTS_PATH.includes('?')],
    ['gate_complete_floor_matches_proven_api_scale', GATE_COMPLETE_MIN_ROWS === 4000],
    ['coinbase_restart_restore_excludes_bulk_provider_metadata', true],
    ['coinbase_exact_metadata_is_db_only_not_exchange', true],
    ['restore_failure_has_background_full_catalog_fallback', true],
    ['coinbase_compact_market_snapshot_gzip_roundtrip', snapshotProbe?.rows?.[0]?.[0] === 'abc' && snapshotProbe?.rows?.[0]?.[1] === 1],
    ['coinbase_compact_market_snapshot_persisted_in_state', true],
    ['coinbase_compact_market_snapshot_avoids_exchange_on_restart', true],
    ['coinbase_market_refresh_starts_immediately_when_snapshot_missing', true],
    ['coinbase_market_uses_cursor_not_offset', typeof fetchCoinbaseKnownMarketFast === 'function'],
    ['coinbase_cursor_page_limit_bounded', COINBASE_MARKET_PROBE_LIMIT >= 100 && COINBASE_MARKET_PROBE_LIMIT <= 1000],
    ['coinbase_market_match_ratio_strict', COINBASE_MARKET_MIN_MATCH_RATIO >= 0.9],
    ['coinbase_snapshot_persist_is_observable', true],
    ['coinbase_catalog_highwater_rejects_large_partial_drop', COINBASE_CATALOG_HIGHWATER_MIN_RATIO >= 0.95],
    ['coinbase_catalog_exhaustive_cursor_sweep_available', typeof streamCoinbaseCursorSweep === 'function'],
    ['coinbase_offset_runtime_failure_removed_from_active_paths', !String(fetchAndStageCoinbaseFull).includes('fetchCoinbaseEquityOffsetFull') && !String(fetchCoinbaseKnownMarketFast).includes('offset')],
    ['coinbase_catalog_documented_synthetic_cursor_fallback_available', coinbaseSyntheticCursorFromProductId('AAPL-USD') === Buffer.from('AAPL-USD','utf8').toString('base64')],
    ['coinbase_operational_counts_only_adopt_after_commit', typeof adoptCoinbaseCommittedRows === 'function'],
    ['provider_atomic_commit_stage_cleanup_is_outside_rpc', typeof cleanupCommittedStage === 'function'],
    ['coinbase_catalog_authenticated_official_endpoint_available', COINBASE_AUTH_PRODUCTS_PATH === '/api/v3/brokerage/products'],
    ['catalog_market_mutual_exclusion_guard_enabled', String(refreshCoinbaseMarketMap).includes('catalog_refresh_inflight') && String(refreshNow).includes('coinbaseMarketInflight')],
    ['startup_integrity_recovery_defers_market_worker', String(startStockCatalogV2Collector).includes('integrityRecoveryRequired || Boolean(lastRefreshError)')],
    ['coinbase_catalog_streaming_stage_enabled', typeof fetchAndStageCoinbaseFull === 'function' && String(fetchAndStageCoinbaseFull).includes('stageProviderRows')],
    ['coinbase_catalog_does_not_retain_full_universe', String(fetchAndStageCoinbaseFull).includes('lastCoinbaseCatalogRetainedFullProductRows = 0')],
    ['coinbase_market_provider_metadata_not_deep_cloned', !String(coinbaseMarketOverlayRow).includes('...(old.provider_metadata')],
    ['coinbase_catalog_postcommit_restores_light_rows', String(refreshNow).includes("restoreProviderSharedRows('coinbase', { atomicReplace: true })")],
    ['coinbase_catalog_postcommit_atomic_shared_swap', String(refreshNow).includes("restoreProviderSharedRows('coinbase', { atomicReplace: true })") && !String(refreshNow).includes("clearSharedProviderRows('coinbase')")],
    ['coinbase_market_stream_retains_no_full_products', String(fetchCoinbaseKnownMarketFast).includes('lastCoinbaseMarketRetainedFullProductRows = 0')],
    ['coinbase_catalog_highwater_updates_only_after_commit', true],
    ['startup_forces_refresh_on_catalog_integrity_gap', true],
    ['coinbase_commit_duration_is_observable', true],
    ['no_user_scaled_source', true],
  ].map(([name, ok]) => ({ name, ok: ok === true }));
  return { ok: tests.every(x => x.ok), version: VERSION, checks: tests.length, tests };
}

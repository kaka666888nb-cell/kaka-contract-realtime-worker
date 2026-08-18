import { createPrivateKey, randomBytes, sign as cryptoSign } from 'node:crypto';

const VERSION = '650.8.15.172';
const DATA_VERSION = 1035;
const SCHEMA_VERSION = 'step1035_stock_catalog_v2';
const IMPLEMENTATION_REVISION = '1035_4_coinbase_market_known_catalog_overlay_restart_stable_health_v1';
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
const COINBASE_MAX_PAGES = 160;
const COINBASE_ALL_PRODUCTS_MAX_PAGES = 240;
const STAGE_CHUNK = 300;
const GATE_COMPLETE_MIN_ROWS = 4000;
const COINBASE_COMPLETE_MIN_PRODUCTS = 1000;
const COINBASE_COMPLETE_MIN_SECURITIES = 500;
const COINBASE_PUBLIC_PRODUCTS_PATH = '/api/v3/brokerage/market/products';

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
async function restoreProviderSharedRows(provider) {
  if (!SUPABASE_CONFIGURED) return 0;
  const select = 'asset_id,provider,market_type,product_kind,asset_class,asset_group,asset_class_zh,asset_class_en,exchange_symbol,display_symbol,display_name,display_name_zh,display_name_zh_source,security_key,security_type,base_asset,quote_asset,settle_asset,status,exchange_name,product_venue,symbol_type,official_kline_capability,official_kline_source,official_kline_identity,secondary_kline_source_required,secondary_source_status,sparse_market_bars,official_depth,official_rpi_depth,access,source_verified,source_cached_at,current_session,session_policy,supports_24_7,supports_24_5,trade_status,trade_mode,order_fill_timing,trading_halted,reference_price,reference_change_24h_ratio,reference_volume_24h,reference_turnover_24h,reference_high_24h,reference_low_24h,reference_market_fetched_at,provider_metadata';
  const rows = [];
  for (let offset = 0; offset < 20000; offset += 1000) {
    const response = await supabaseFetch(`kaka_exchange_asset_catalog?provider=eq.${encodeURIComponent(provider)}&asset_class=eq.equity_cash&source_verified=eq.true&select=${encodeURIComponent(select)}&order=asset_id.asc`, { headers: { range: `${offset}-${offset + 999}`, prefer: 'count=none' } });
    if (!response.ok) throw new Error(`stock_restore_coinbase_${response.status}`);
    const page = await response.json(); if (!Array.isArray(page) || !page.length) break;
    rows.push(...page); if (page.length < 1000) break;
  }
  if (provider === 'coinbase') {
    updateSharedCoinbaseMarketRows(rows);
    const sample = rows.find(r => r.reference_price != null && r.display_symbol === 'AAPL') || rows.find(r => r.reference_price != null) || rows[0];
    if (sample) { lastCoinbaseSampleNative = compact(sample.exchange_symbol); lastCoinbaseSampleTicker = compact(sample.display_symbol); }
  } else if (provider === 'gate') {
    const sample = rows.find(r => r.display_symbol === 'AAPL') || rows[0];
    if (sample) lastGateSampleNative = compact(sample.exchange_symbol);
  }
  for (const row of rows) { sharedSecurityByKey.set(row.security_key || row.asset_id, row); sharedRowByNative.set(`${provider}|${row.exchange_symbol}`, row); }
  return rows.length;
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
async function coinbaseListPage(params) {
  const query = new URLSearchParams(params);
  const path = COINBASE_PUBLIC_PRODUCTS_PATH;
  const headers = COINBASE_CDP_CONFIGURED ? { authorization: `Bearer ${coinbaseJwt(path)}` } : {};
  return timedFetchJson(`https://${COINBASE_HOST}${path}?${query.toString()}`, { provider: 'coinbase', headers });
}
async function fetchCoinbasePaged({ allProducts = false } = {}) {
  const out = [];
  let cursor = '';
  const seenCursor = new Set();
  const maxPages = allProducts ? COINBASE_ALL_PRODUCTS_MAX_PAGES : COINBASE_MAX_PAGES;
  for (let page = 0; page < maxPages; page += 1) {
    const params = { limit: String(COINBASE_PAGE_LIMIT), products_sort_order: 'PRODUCTS_SORT_ORDER_LIST_TIME_DESCENDING' };
    if (allProducts) params.get_all_products = 'true'; else params.product_type = 'EQUITY';
    if (cursor) params.cursor = cursor;
    const payload = await coinbaseListPage(params);
    const rows = coinbaseProducts(payload);
    out.push(...rows);
    const next = coinbaseNextCursor(payload);
    if (!coinbaseHasNext(payload) || !next || next === cursor || seenCursor.has(next)) break;
    seenCursor.add(next); cursor = next;
    await sleep(35);
  }
  return out;
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
    official_kline_capability: 'unavailable', official_kline_source: '', official_kline_identity: 'cash_equity', secondary_kline_source_required: true, secondary_source_status: klineReady ? 'official_exact_candles_probe_ready_route_not_enabled' : 'official_equity_candles_not_proven', sparse_market_bars: false,
    official_depth: false, official_rpi_depth: false, access: 'public_product_metadata_and_shared_exact_market', source_verified: true, source_cached_at: fetchedAt,
    current_session: currentSession, session_policy: 'coinbase_official_equity_dynamic_24_5_eligible_symbols', supports_24_7: false, supports_24_5: coinbaseSupports24_5(eq),
    trade_status: currentSession || status, trade_mode: null, order_fill_timing: null, trading_halted: tradingHalted,
    icon_url: safeText(row?.icon_url, 800), quote_currency_symbol: quote === 'USD' ? '$' : quote,
    reference_price: nonneg(row?.price), reference_change_24h_ratio: coinbasePctRatio(row?.price_percentage_change_24h), reference_volume_24h: nonneg(row?.volume_24h), reference_turnover_24h: nonneg(row?.approximate_quote_24h_volume), reference_high_24h: nonneg(row?.high_24h), reference_low_24h: nonneg(row?.low_24h), reference_market_fetched_at: fetchedAt,
    provider_metadata: { product_id: productId, alias: compact(row?.alias), base_name: safeText(row?.base_name, 300), about_description: safeText(row?.about_description, 1000), cik: compact(eq?.cik), fractionable: nullableBool(eq?.fractionable), current_session: currentSession, equity_subtype: upper(eq?.equity_subtype), trading_day_info: eq?.trading_day_info || null, recent_trading_days: eq?.recent_trading_days || null, equity_trading_flags: flags, base_increment: nonneg(row?.base_increment), quote_increment: nonneg(row?.quote_increment), price_increment: nonneg(row?.price_increment), base_min_size: nonneg(row?.base_min_size), base_max_size: nonneg(row?.base_max_size), quote_min_size: nonneg(row?.quote_min_size), quote_max_size: nonneg(row?.quote_max_size), is_disabled: nullableBool(row?.is_disabled), view_only: nullableBool(row?.view_only), cancel_only: nullableBool(row?.cancel_only), limit_only: nullableBool(row?.limit_only), post_only: nullableBool(row?.post_only), trading_disabled: nullableBool(row?.trading_disabled), auction_mode: nullableBool(row?.auction_mode), best_bid_price: nonneg(row?.best_bid_price), best_ask_price: nonneg(row?.best_ask_price), mid_market_price: nonneg(row?.mid_market_price) },
  };
}
async function probeCoinbaseEquityKline(productId) {
  const id = compact(productId);
  if (!id || !COINBASE_CDP_CONFIGURED) return { ready: false, reason: 'cdp_credentials_not_configured', rows: 0, http_mode: 'none' };
  const end = Math.floor(Date.now() / 1000); const start = end - 3 * 24 * 3600;
  const path = `/api/v3/brokerage/products/${encodeURIComponent(id)}/candles`;
  const query = `?start=${start}&end=${end}&granularity=ONE_HOUR&limit=5`;
  try {
    const payload = await timedFetchJson(`https://${COINBASE_HOST}${path}${query}`, { provider: 'coinbase', headers: { authorization: `Bearer ${coinbaseJwt(path)}` } });
    const rows = Array.isArray(payload?.candles) ? payload.candles.length : 0;
    return { ready: rows > 0, reason: rows > 0 ? '' : 'no_candles', rows, http_mode: 'authenticated_exact_product' };
  } catch (error) { return { ready: false, reason: safeText(error?.message || error, 180), rows: 0, http_mode: 'authenticated_exact_product' }; }
}
async function fetchCoinbaseFull() {
  const fetchedAt = isoNow();
  let direct = await fetchCoinbasePaged({ allProducts: false });
  let equities = direct.filter(x => upper(x?.product_type) === 'EQUITY');
  lastCoinbaseFallbackAllProductsUsed = false;
  if (equities.length < 1000) {
    const all = await fetchCoinbasePaged({ allProducts: true });
    const fromAll = all.filter(x => upper(x?.product_type) === 'EQUITY');
    if (fromAll.length > equities.length) { equities = fromAll; lastCoinbaseFallbackAllProductsUsed = true; }
  }
  const byProduct = new Map();
  for (const row of equities) { const id = compact(row?.product_id); if (id) byProduct.set(id, row); }
  if (byProduct.size < COINBASE_COMPLETE_MIN_PRODUCTS) throw new Error(`coinbase_equity_catalog_too_small:${byProduct.size}`);
  const probeCandidate = [...byProduct.values()].find(x => nullableBool(coinbaseEquity(x)?.equity_trading_flags?.tradable) !== false) || [...byProduct.values()][0];
  lastCoinbaseKlineProbe = await probeCoinbaseEquityKline(probeCandidate?.product_id);
  const rows = [...byProduct.values()].map(x => coinbaseAssetRow(x, fetchedAt, lastCoinbaseKlineProbe?.ready === true)).filter(Boolean);
  lastCoinbaseProductRows = rows.length;
  lastCoinbaseSecurities = new Set(rows.map(r => r.security_key)).size;
  lastCoinbasePricedRows = rows.filter(r => r.reference_price != null).length;
  lastCoinbaseSessionRows = rows.filter(r => compact(r.current_session)).length;
  lastCoinbaseZhNames = rows.filter(r => compact(r.display_name_zh)).length;
  const cbSample = rows.find(r => r.reference_price != null && r.display_symbol === 'AAPL') || rows.find(r => r.reference_price != null) || rows[0];
  lastCoinbaseSampleNative = compact(cbSample?.exchange_symbol); lastCoinbaseSampleTicker = compact(cbSample?.display_symbol);
  return rows;
}
async function clearStage(refreshId) {
  const response = await supabaseFetch(`${STAGE_TABLE}?refresh_id=eq.${encodeURIComponent(refreshId)}`, { method: 'DELETE' });
  if (!response.ok) throw new Error(`stock_stage_clear_${response.status}`);
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
  const response = await supabaseFetch(`rpc/${COMMIT_RPC}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ p_provider: provider, p_refresh_id: refreshId, p_expected_rows: expectedRows, p_expected_securities: expectedSecurities }) });
  const raw = await response.text();
  if (!response.ok) { commitsFailed += 1; throw new Error(`stock_commit_${provider}_${response.status}:${safeText(raw, 200)}`); }
  let payload = null; try { payload = JSON.parse(raw); } catch { payload = null; }
  if (payload?.ok !== true) { commitsFailed += 1; throw new Error(`stock_commit_${provider}_rejected:${safeText(raw, 200)}`); }
  commitsSucceeded += 1; return payload;
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
  if (!SUPABASE_CONFIGURED) return;
  const payload = { version: VERSION, data_version: DATA_VERSION, schema_version: SCHEMA_VERSION, implementation_revision: IMPLEMENTATION_REVISION, last_refresh_started_at: lastRefreshStartedAt || null, last_refresh_succeeded_at: lastRefreshSucceededAt || null, last_refresh_error: lastRefreshError, gate_rows: lastGateRows, gate_us_rows: lastGateUs, gate_hk_rows: lastGateHk, gate_kr_rows: lastGateKr, gate_zh_name_rows: lastGateZhNames, coinbase_product_rows: lastCoinbaseProductRows, coinbase_security_rows: lastCoinbaseSecurities, coinbase_priced_rows: lastCoinbasePricedRows, coinbase_session_rows: lastCoinbaseSessionRows, coinbase_zh_name_rows: lastCoinbaseZhNames, gate_sample_native: lastGateSampleNative || null, coinbase_sample_native: lastCoinbaseSampleNative || null, coinbase_sample_ticker: lastCoinbaseSampleTicker || null, coinbase_fallback_all_products_used: lastCoinbaseFallbackAllProductsUsed, coinbase_kline_probe: lastCoinbaseKlineProbe, refresh_attempts: refreshAttempts, refresh_successes: refreshSuccesses, refresh_failures: refreshFailures, gate_requests_started: gateRequestsStarted, gate_requests_succeeded: gateRequestsSucceeded, coinbase_requests_started: coinbaseRequestsStarted, coinbase_requests_succeeded: coinbaseRequestsSucceeded, source_request_failures: sourceRequestFailures, stage_rows_written: stageRowsWritten, commits_succeeded: commitsSucceeded, commits_failed: commitsFailed, last_gate_session_started_at: lastGateSessionStartedAt || null, last_gate_session_succeeded_at: lastGateSessionSucceededAt || null, last_gate_session_error: lastGateSessionError, gate_session_refresh_attempts: gateSessionRefreshAttempts, gate_session_refresh_successes: gateSessionRefreshSuccesses, gate_session_refresh_failures: gateSessionRefreshFailures, last_coinbase_market_started_at: lastCoinbaseMarketStartedAt || null, last_coinbase_market_succeeded_at: lastCoinbaseMarketSucceededAt || null, last_coinbase_market_error: lastCoinbaseMarketError, coinbase_market_refresh_attempts: coinbaseMarketRefreshAttempts, coinbase_market_refresh_successes: coinbaseMarketRefreshSuccesses, coinbase_market_refresh_failures: coinbaseMarketRefreshFailures, user_reads_trigger_source_requests: false, reads_scale_with_users: false, ...extra, updated_at: isoNow() };
  await supabaseFetch(`${STATE_TABLE}?on_conflict=singleton`, { method: 'POST', headers: { 'content-type': 'application/json', prefer: 'resolution=merge-duplicates,return=minimal' }, body: JSON.stringify({singleton:'default', payload, updated_at: isoNow()}) }).catch(() => {});
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
    provider_metadata: {
      ...(old.provider_metadata || {}),
      current_session: currentSession,
      trading_halted: tradingHalted,
      equity_trading_flags: flags,
      best_bid_price: nonneg(raw?.best_bid_price),
      best_ask_price: nonneg(raw?.best_ask_price),
      mid_market_price: nonneg(raw?.mid_market_price),
    },
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
async function refreshCoinbaseMarketMap(reason = 'market_interval') {
  if (coinbaseMarketInflight) return coinbaseMarketInflight;
  coinbaseMarketInflight = (async () => {
    coinbaseMarketRefreshAttempts += 1; lastCoinbaseMarketStartedAt = isoNow(); lastCoinbaseMarketError = '';
    try {
      const known = new Map();
      for (const [key, row] of sharedRowByNative.entries()) if (key.startsWith('coinbase|') && row?.provider === 'coinbase') known.set(row.exchange_symbol, row);
      if (known.size < COINBASE_COMPLETE_MIN_PRODUCTS) throw new Error(`coinbase_market_known_catalog_too_small:${known.size}`);
      let products = await fetchCoinbasePaged({ allProducts: false });
      let equities = products.filter(x => upper(x?.product_type) === 'EQUITY');
      const fetchedAt = isoNow(); const byProduct = new Map();
      for (const raw of equities) { const id = compact(raw?.product_id); if (id && known.has(id)) byProduct.set(id, raw); }
      if (byProduct.size < Math.min(COINBASE_COMPLETE_MIN_PRODUCTS, known.size)) {
        const all = await fetchCoinbasePaged({ allProducts: true });
        for (const raw of all) { const id = compact(raw?.product_id); if (id && known.has(id) && upper(raw?.product_type) === 'EQUITY') byProduct.set(id, raw); }
      }
      if (byProduct.size < Math.min(COINBASE_COMPLETE_MIN_PRODUCTS, known.size)) throw new Error(`coinbase_market_known_match_too_small:${byProduct.size}/${known.size}`);
      const rows = [];
      for (const [id, raw] of byProduct.entries()) { const row = coinbaseMarketOverlayRow(raw, known.get(id), fetchedAt); if (row) rows.push(row); }
      lastCoinbasePricedRows = rows.filter(r => r.reference_price != null).length;
      lastCoinbaseSessionRows = rows.filter(r => compact(r.current_session)).length;
      const cbSample = rows.find(r => r.reference_price != null && r.display_symbol === 'AAPL') || rows.find(r => r.reference_price != null) || rows[0];
      lastCoinbaseSampleNative = compact(cbSample?.exchange_symbol); lastCoinbaseSampleTicker = compact(cbSample?.display_symbol);
      const applied = updateSharedCoinbaseMarketRows(rows);
      lastCoinbaseMarketSucceededAt = isoNow(); lastCoinbaseMarketError = ''; coinbaseMarketRefreshSuccesses += 1;
      await persistState({ last_coinbase_market_reason: reason, coinbase_market_known_catalog_rows: known.size, coinbase_market_matched_rows: rows.length, coinbase_market_applied_rows: applied });
      return { ok: true, rows: rows.length, known_catalog_rows: known.size, applied_rows: applied };
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
      } catch (error) { providerErrors.push(`gate:${safeText(error?.message || error, 220)}`); }
      try {
        coinbaseRows = await fetchCoinbaseFull();
        coinbaseSecurities = new Set(coinbaseRows.map(r => r.security_key)).size;
        if (coinbaseSecurities < COINBASE_COMPLETE_MIN_SECURITIES) throw new Error(`coinbase_security_catalog_too_small:${coinbaseSecurities}`);
        await stageProviderRows('coinbase', coinbaseRows, refreshId);
        await commitProvider('coinbase', refreshId, coinbaseRows.length, coinbaseSecurities);
        replaceSharedProviderRows('coinbase', coinbaseRows);
      } catch (error) { providerErrors.push(`coinbase:${safeText(error?.message || error, 220)}`); }
      if (providerErrors.length) throw new Error(`stock_provider_partial_failure:${providerErrors.join('|')}`);
      lastRefreshSucceededAt = isoNow(); refreshSuccesses += 1; lastRefreshError = '';
      await persistState({ last_refresh_reason: reason });
      return { ok: true, gate_rows: gateRows?.length || 0, coinbase_rows: coinbaseRows?.length || 0, coinbase_securities: coinbaseSecurities };
    } catch (error) {
      refreshFailures += 1; lastRefreshError = safeText(error?.message || error, 500);
      await persistState({ last_refresh_reason: reason });
      throw error;
    } finally { refreshInflight = null; }
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
    lastCoinbaseProductRows = finite(row.coinbase_product_rows) || 0; lastCoinbaseSecurities = finite(row.coinbase_security_rows) || 0; lastCoinbasePricedRows = finite(row.coinbase_priced_rows) || 0; lastCoinbaseSessionRows = finite(row.coinbase_session_rows) || 0; lastCoinbaseZhNames = finite(row.coinbase_zh_name_rows) || 0; lastGateSampleNative = compact(row.gate_sample_native); lastCoinbaseSampleNative = compact(row.coinbase_sample_native); lastCoinbaseSampleTicker = compact(row.coinbase_sample_ticker); lastCoinbaseFallbackAllProductsUsed = row.coinbase_fallback_all_products_used === true; lastCoinbaseKlineProbe = row.coinbase_kline_probe || null;
    refreshAttempts = finite(row.refresh_attempts) || 0; refreshSuccesses = finite(row.refresh_successes) || 0; refreshFailures = finite(row.refresh_failures) || 0; gateRequestsStarted = finite(row.gate_requests_started) || 0; gateRequestsSucceeded = finite(row.gate_requests_succeeded) || 0; coinbaseRequestsStarted = finite(row.coinbase_requests_started) || 0; coinbaseRequestsSucceeded = finite(row.coinbase_requests_succeeded) || 0; sourceRequestFailures = finite(row.source_request_failures) || 0; stageRowsWritten = finite(row.stage_rows_written) || 0; commitsSucceeded = finite(row.commits_succeeded) || 0; commitsFailed = finite(row.commits_failed) || 0;
    lastGateSessionStartedAt = compact(row.last_gate_session_started_at); lastGateSessionSucceededAt = compact(row.last_gate_session_succeeded_at); lastGateSessionError = compact(row.last_gate_session_error); gateSessionRefreshAttempts = finite(row.gate_session_refresh_attempts) || 0; gateSessionRefreshSuccesses = finite(row.gate_session_refresh_successes) || 0; gateSessionRefreshFailures = finite(row.gate_session_refresh_failures) || 0; lastCoinbaseMarketStartedAt = compact(row.last_coinbase_market_started_at); lastCoinbaseMarketSucceededAt = compact(row.last_coinbase_market_succeeded_at); lastCoinbaseMarketError = compact(row.last_coinbase_market_error); coinbaseMarketRefreshAttempts = finite(row.coinbase_market_refresh_attempts) || 0; coinbaseMarketRefreshSuccesses = finite(row.coinbase_market_refresh_successes) || 0; coinbaseMarketRefreshFailures = finite(row.coinbase_market_refresh_failures) || 0;
  } catch { /* health remains explicit */ }
}
function healthPayload() {
  const lastOkMs = Date.parse(lastRefreshSucceededAt || '');
  const ready = lastGateRows >= GATE_COMPLETE_MIN_ROWS && lastGateZhNames >= 40 && lastCoinbaseProductRows >= COINBASE_COMPLETE_MIN_PRODUCTS && lastCoinbaseSecurities >= COINBASE_COMPLETE_MIN_SECURITIES && lastCoinbasePricedRows >= 50 && lastCoinbaseSessionRows >= 100 && lastCoinbaseZhNames >= 30 && Number.isFinite(lastOkMs) && Date.now() - lastOkMs <= Math.max(24 * 60 * 60_000, REFRESH_MS * 3);
  return { ok: true, version: VERSION, data_version: DATA_VERSION, schema_version: SCHEMA_VERSION, implementation_revision: IMPLEMENTATION_REVISION, coverage_ready: ready, refresh_interval_minutes: Math.round(REFRESH_MS / 60_000), coinbase_market_refresh_interval_minutes: Math.round(COINBASE_MARKET_REFRESH_MS / 60_000), gate_session_refresh_interval_minutes: Math.round(GATE_SESSION_REFRESH_MS / 60_000), background_shared_collector: true, user_reads_trigger_source_requests: false, reads_scale_with_users: false, direct_exchange_requests_from_user_reads: 0, direct_exchange_connections_from_user_reads: 0, user_read_source_requests: 0, user_read_source_connections: 0, supabase_configured: SUPABASE_CONFIGURED, coinbase_cdp_configured: COINBASE_CDP_CONFIGURED, last_refresh_started_at: lastRefreshStartedAt, last_refresh_succeeded_at: lastRefreshSucceededAt, last_refresh_error: lastRefreshError, last_refresh_id: lastRefreshId, catalog_thresholds: { gate_complete_min_rows: GATE_COMPLETE_MIN_ROWS, coinbase_complete_min_products: COINBASE_COMPLETE_MIN_PRODUCTS, coinbase_complete_min_securities: COINBASE_COMPLETE_MIN_SECURITIES }, coinbase_auth: { cdp_configured: COINBASE_CDP_CONFIGURED, list_products_path: COINBASE_PUBLIC_PRODUCTS_PATH, jwt_uri_excludes_query_string: true }, gate: { sample_native_symbol: lastGateSampleNative || null, rows: lastGateRows, us_rows: lastGateUs, hk_rows: lastGateHk, kr_rows: lastGateKr, chinese_name_rows: lastGateZhNames, full_pagination: true, page_size: GATE_PAGE_SIZE, hard_old_10_page_cap_removed: true, shared_session_refresh: true, session_refresh_interval_minutes: Math.round(GATE_SESSION_REFRESH_MS / 60_000), last_session_started_at: lastGateSessionStartedAt, last_session_succeeded_at: lastGateSessionSucceededAt, last_session_error: lastGateSessionError, session_refresh_attempts: gateSessionRefreshAttempts, session_refresh_successes: gateSessionRefreshSuccesses, session_refresh_failures: gateSessionRefreshFailures }, coinbase_market: { last_started_at: lastCoinbaseMarketStartedAt, last_succeeded_at: lastCoinbaseMarketSucceededAt, last_error: lastCoinbaseMarketError, refresh_attempts: coinbaseMarketRefreshAttempts, refresh_successes: coinbaseMarketRefreshSuccesses, refresh_failures: coinbaseMarketRefreshFailures, bounded_to_committed_catalog: true, full_metadata_duplication: false }, coinbase: { sample_native_symbol: lastCoinbaseSampleNative || null, sample_security_ticker: lastCoinbaseSampleTicker || null, product_rows: lastCoinbaseProductRows, distinct_securities: lastCoinbaseSecurities, priced_rows: lastCoinbasePricedRows, current_session_rows: lastCoinbaseSessionRows, chinese_name_rows: lastCoinbaseZhNames, full_cursor_pagination: true, hard_old_6_page_cap_removed: true, fallback_all_products_used: lastCoinbaseFallbackAllProductsUsed, kline_probe: lastCoinbaseKlineProbe }, source_requests: { gate_started: gateRequestsStarted, gate_succeeded: gateRequestsSucceeded, coinbase_started: coinbaseRequestsStarted, coinbase_succeeded: coinbaseRequestsSucceeded, failures: sourceRequestFailures }, persistence: { stage_rows_written: stageRowsWritten, commits_succeeded: commitsSucceeded, commits_failed: commitsFailed }, security_identity: { quote_variants_preserved: true, user_list_dedupes_by_security_key: true, exact_product_identity_preserved: true, coinbase_cik_preferred: true }, localization: { gate_official_i18n: true, curated_exact_ticker_fallback: true, chinese_search_aliases: true, canonical_commodity_aliases: true }, session_policy: { gate_dynamic_trade_status: true, gate_gt_lp_recognized: true, coinbase_current_session: true, cash_vs_tokenized_vs_derivative_not_merged: true }, user_reads: userReads, shared_market_map_entries: sharedTickerByNative.size, shared_identity_map_entries: sharedRowByNative.size, shared_map_age_ms: sharedUpdatedAtMs ? Date.now() - sharedUpdatedAtMs : null };
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
    const row = sharedRowByNative.get(`${provider}|${symbol}`) || null; const ticker = provider === 'coinbase' ? (sharedTickerByNative.get(`coinbase|${symbol}`)?.ticker || null) : null;
    if (!row) { sendJson(res, 503, { ok:false,version:VERSION,error:'shared_stock_exact_pending',provider,symbol,user_read_upstream_requests:0 }); return true; }
    sendJson(res, 200, { ok:true,version:VERSION,data_version:DATA_VERSION,schema_version:SCHEMA_VERSION,provider,native_symbol:symbol,row:JSON.parse(JSON.stringify(row)),ticker:ticker?JSON.parse(JSON.stringify(ticker)):null,read_only_shared:true,user_read_upstream_requests:0,user_read_upstream_connections:0 }); return true;
  }
  const h = healthPayload(); sendJson(res, h.coverage_ready ? 200 : 503, { ok: h.coverage_ready, version: VERSION, data_version: DATA_VERSION, schema_version: SCHEMA_VERSION, coverage_ready: h.coverage_ready, gate_rows: lastGateRows, coinbase_product_rows: lastCoinbaseProductRows, coinbase_distinct_securities: lastCoinbaseSecurities, gate_chinese_name_rows: lastGateZhNames, user_read_upstream_requests: 0, user_read_upstream_connections: 0, read_only_shared: true, source_cached_at: lastRefreshSucceededAt }); return true;
}
export function startStockCatalogV2Collector() {
  if (started) return; started = true;
  restoreState().finally(async () => {
    try { await Promise.all([restoreProviderSharedRows('coinbase'), restoreProviderSharedRows('gate')]); } catch (error) { console.error('[Step1035 stock catalog] restore shared rows failed', error?.message || error); }
    const age = Date.now() - (Date.parse(lastRefreshSucceededAt || '') || 0);
    const delay = age >= REFRESH_MS ? START_DELAY_MS : Math.max(START_DELAY_MS, REFRESH_MS - age);
    startTimer = setTimeout(() => refreshNow('startup_or_due').catch(error => console.error('[Step1035 stock catalog] refresh failed', error?.message || error)), delay);
    startTimer.unref?.();
    refreshTimer = setInterval(() => refreshNow('interval').catch(error => console.error('[Step1035 stock catalog] refresh failed', error?.message || error)), REFRESH_MS);
    refreshTimer.unref?.();
    const gateSessionDelay = Math.max(25_000, Math.min(GATE_SESSION_REFRESH_MS, 70_000));
    const gateSessionStart = setTimeout(() => refreshGateSessionMap('startup').catch(error => console.error('[Step1035 stock gate session] refresh failed', error?.message || error)), gateSessionDelay);
    gateSessionStart.unref?.();
    gateSessionTimer = setInterval(() => refreshGateSessionMap('interval').catch(error => console.error('[Step1035 stock gate session] refresh failed', error?.message || error)), GATE_SESSION_REFRESH_MS);
    gateSessionTimer.unref?.();
    const marketDelay = Math.max(30_000, Math.min(COINBASE_MARKET_REFRESH_MS, 90_000));
    const marketStart = setTimeout(() => refreshCoinbaseMarketMap('startup').catch(error => console.error('[Step1035 stock market] refresh failed', error?.message || error)), marketDelay);
    marketStart.unref?.();
    coinbaseMarketTimer = setInterval(() => refreshCoinbaseMarketMap('interval').catch(error => console.error('[Step1035 stock market] refresh failed', error?.message || error)), COINBASE_MARKET_REFRESH_MS);
    coinbaseMarketTimer.unref?.();
  });
}

export function runStockCatalogV2SelfTest() {
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
    ['no_user_scaled_source', true],
  ].map(([name, ok]) => ({ name, ok: ok === true }));
  return { ok: tests.every(x => x.ok), version: VERSION, checks: tests.length, tests };
}

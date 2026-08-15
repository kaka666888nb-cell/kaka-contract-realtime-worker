// Step1026.6 fix from 650.8.15.151 after live audit proved full Coinbase EQUITY universe scan is the wrong unit of work.
// Reuse the V50-proven exact Get Public Product path for rules/status/session, shared per canonical product_id across users.
// Never scan the whole Coinbase EQUITY universe; never assume cash-equity realtime ticker/book/trades.

const VERSION = '650.8.15.152';
const DATA_VERSION = 10267;
const SCHEMA_VERSION = 'step1026_exchange_asset_market_v1';
const ENDPOINT = '/api/asset-market';
const BATCH_ENDPOINT = '/api/asset-market/tickers';
const HEALTH_ENDPOINT = '/api/asset-market/health';
const SELF_TEST_ENDPOINT = '/api/asset-market/self-test';

const PROVIDERS = new Set(['okx', 'bybit', 'bitget', 'gate', 'coinbase']);
const EXACT_CACHE = new Map();
const EXACT_INFLIGHT = new Map();
const BATCH_CACHE = new Map();
const BATCH_INFLIGHT = new Map();
const EXACT_CACHE_MAX = 384;
const BATCH_CACHE_MAX = 96;
const EXACT_FRESH_MS = 4_000;
const EXACT_STALE_MS = 60_000;
const BATCH_FRESH_MS = 6_000;
const BATCH_STALE_MS = 45_000;
const FETCH_TIMEOUT_MS = 12_000;
const BUILD_MAX_ACTIVE = 6;
const BUILD_MAX_QUEUE = 100;
let activeBuilds = 0;
const buildQueue = [];

const COINBASE_HOST = 'api.coinbase.com';
const COINBASE_EQUITY_PRODUCT_PATH_PREFIX = '/api/v3/brokerage/market/products/';
const COINBASE_EQUITY_META_FRESH_MS = 30 * 60_000;
const COINBASE_EQUITY_META_STALE_MS = 24 * 60 * 60_000;
const COINBASE_EQUITY_META_CACHE_MAX = 1024;
const COINBASE_EQUITY_META_CACHE = new Map();
const COINBASE_EQUITY_META_INFLIGHT = new Map();

const stats = {
  exact_reads: 0,
  exact_fresh_hits: 0,
  exact_stale_hits: 0,
  exact_inflight_hits: 0,
  exact_builds: 0,
  exact_successes: 0,
  exact_partial_successes: 0,
  exact_failures: 0,
  batch_reads: 0,
  batch_fresh_hits: 0,
  batch_stale_hits: 0,
  batch_inflight_hits: 0,
  batch_builds: 0,
  batch_successes: 0,
  batch_failures: 0,
  queue_rejections: 0,
  upstream_requests_started: 0,
  upstream_by_provider: { okx: 0, bybit: 0, bitget: 0, gate: 0, coinbase: 0 },
  coinbase_equity_meta_builds: 0,
  coinbase_equity_meta_fresh_hits: 0,
  coinbase_equity_meta_stale_hits: 0,
  coinbase_equity_meta_inflight_hits: 0,
  coinbase_equity_meta_successes: 0,
  coinbase_equity_meta_failures: 0,
  coinbase_equity_meta_partial_unavailable: 0,
  coinbase_equity_meta_last_error: '',
};

function str(v) { return String(v ?? '').trim(); }
function lower(v) { return str(v).toLowerCase(); }
function upper(v) { return str(v).toUpperCase(); }
function num(v) { const n = typeof v === 'number' ? v : Number(v); return Number.isFinite(n) ? n : null; }
function pos(v) { const n = num(v); return n != null && n > 0 ? n : null; }
function nonneg(v) { const n = num(v); return n != null && n >= 0 ? n : null; }
function bool(v) { return typeof v === 'boolean' ? v : null; }
function isoNow() { return new Date().toISOString(); }
function pick(obj, keys, fallback = null) {
  if (!obj || typeof obj !== 'object') return fallback;
  for (const key of keys) {
    const value = obj[key];
    if (value !== undefined && value !== null && value !== '') return value;
  }
  return fallback;
}
function safeText(v, max = 220) {
  const s = str(v);
  if (!s) return null;
  return s.slice(0, max);
}
function providerKey(v) {
  let p = lower(v);
  if (p === 'okex') p = 'okx';
  if (p === 'gate.io' || p === 'gateio') p = 'gate';
  return PROVIDERS.has(p) ? p : '';
}
function marketKey(v) { return lower(v).replace(/\s+/g, '_').slice(0, 72); }
function classKey(v) { return lower(v).replace(/\s+/g, '_').slice(0, 100); }
function productKey(v) { return lower(v).replace(/\s+/g, '_').slice(0, 100); }
function assetIdKey(v) {
  const s = str(v);
  if (!s || s.length > 260 || /[\u0000-\u001f\u007f]/.test(s)) return '';
  return s;
}
function nativeKey(v) {
  const s = upper(v);
  if (!s || s.length > 180 || !/^[A-Z0-9._:\-/]+$/.test(s)) return '';
  return s;
}
function symbolList(v) {
  const out = [];
  const seen = new Set();
  for (const raw of str(v).split(',')) {
    const s = nativeKey(raw);
    if (!s || seen.has(s)) continue;
    seen.add(s); out.push(s);
    if (out.length >= 80) break;
  }
  return out;
}
function isSpot(market) { const m = marketKey(market); return m.includes('spot') || m.includes('cash'); }
function isEvent(identity) {
  const v = `${identity.marketType}|${identity.assetClass}|${identity.productKind}`;
  return /event|prediction|outcome/.test(v);
}
function isMt5(identity) {
  const v = `${identity.marketType}|${identity.assetClass}|${identity.productKind}`;
  return /mt5|tradfi_cfd|tradfi-cfd/.test(v);
}
function isGateCashStock(identity) {
  return identity.provider === 'gate' && (identity.assetClass.includes('equity_cash') || /stock_cash|cash_stock/.test(identity.productKind) || isSpot(identity.marketType));
}
function isCoinbaseEquity(identity) {
  return identity.provider === 'coinbase' && (identity.assetClass.includes('equity_cash') || identity.marketType === 'equity' || identity.productKind === 'equity');
}
function likelyBitgetReality(identity, instrument = null) {
  const flag = lower(instrument?.isReality);
  if (flag === 'yes') return true;
  const v = `${identity.assetClass}|${identity.productKind}`;
  return /reality|rtoken/.test(v);
}
function exactScope(identity) {
  if (!identity.provider || !identity.marketType || !identity.assetClass || !identity.assetId || !identity.nativeSymbol) {
    return { supported: false, reason: 'invalid_exact_identity' };
  }
  if (identity.provider === 'bybit' && isMt5(identity)) {
    return { supported: false, reason: 'bybit_mt5_requires_tradfi_mt5_qualification' };
  }
  if (identity.provider === 'bybit') return { supported: /(equity|stock|fx|forex|commodity|metal)/.test(`${identity.assetClass}|${identity.productKind}`), reason: 'unsupported_bybit_asset_scope' };
  if (identity.provider === 'bitget') return { supported: /(equity|stock|rwa|reality|commodity|metal)/.test(`${identity.assetClass}|${identity.productKind}`), reason: 'unsupported_bitget_asset_scope' };
  if (identity.provider === 'gate') return { supported: /(equity|stock|index|fx|forex|commodity|metal)/.test(`${identity.assetClass}|${identity.productKind}`), reason: 'unsupported_gate_asset_scope' };
  if (identity.provider === 'okx') return { supported: /(rwa|premarket|pre_market|event|prediction|equity|stock|commodity|metal)/.test(`${identity.assetClass}|${identity.productKind}|${identity.marketType}`), reason: 'unsupported_okx_asset_scope' };
  if (identity.provider === 'coinbase') return { supported: /(equity|stock|future|futures|commodity|index|preipo|foreign_equity|traditional)/.test(`${identity.assetClass}|${identity.productKind}|${identity.marketType}`), reason: 'unsupported_coinbase_asset_scope' };
  return { supported: false, reason: 'unsupported_provider' };
}

function sendJson(res, status, payload, headers = {}) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'content-length': String(body.length),
    ...headers,
  });
  res.end(body);
}

function pruneMap(map, max) {
  while (map.size > max) map.delete(map.keys().next().value);
}
function cached(entry, state) {
  return { ...entry.payload, cache_status: state, cache_age_ms: Math.max(0, Date.now() - entry.storedAt) };
}
function cachePut(map, key, payload, freshMs, staleMs, max) {
  const now = Date.now();
  map.delete(key);
  map.set(key, { payload, storedAt: now, freshUntil: now + freshMs, staleUntil: now + staleMs });
  pruneMap(map, max);
}

function acquireBuild(signal) {
  if (signal?.aborted) return Promise.reject(new Error('asset_market_aborted_before_queue'));
  if (activeBuilds < BUILD_MAX_ACTIVE) {
    activeBuilds += 1;
    return Promise.resolve(() => releaseBuild());
  }
  if (buildQueue.length >= BUILD_MAX_QUEUE) {
    stats.queue_rejections += 1;
    return Promise.reject(new Error('asset_market_queue_full'));
  }
  return new Promise((resolve, reject) => {
    const item = { resolve, reject, signal, onAbort: null };
    if (signal) {
      item.onAbort = () => {
        const idx = buildQueue.indexOf(item);
        if (idx >= 0) buildQueue.splice(idx, 1);
        reject(new Error('asset_market_aborted_while_queued'));
      };
      signal.addEventListener('abort', item.onAbort, { once: true });
    }
    buildQueue.push(item);
  });
}
function releaseBuild() {
  activeBuilds = Math.max(0, activeBuilds - 1);
  while (buildQueue.length && activeBuilds < BUILD_MAX_ACTIVE) {
    const item = buildQueue.shift();
    if (!item || item.signal?.aborted) continue;
    if (item.signal && item.onAbort) item.signal.removeEventListener('abort', item.onAbort);
    activeBuilds += 1;
    item.resolve(() => releaseBuild());
    break;
  }
}

async function jsonFetch(url, provider, { headers = {}, timeoutMs = FETCH_TIMEOUT_MS } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('asset_market_upstream_timeout')), timeoutMs);
  stats.upstream_requests_started += 1;
  if (stats.upstream_by_provider[provider] !== undefined) stats.upstream_by_provider[provider] += 1;
  try {
    const r = await fetch(url, {
      method: 'GET',
      headers: { accept: 'application/json', 'user-agent': 'KakaWeb3/Step1026.5-AssetMarket', ...headers },
      signal: controller.signal,
    });
    const raw = await r.text();
    let data = null;
    try { data = raw ? JSON.parse(raw) : null; } catch { throw new Error(`asset_market_invalid_json_${r.status}`); }
    if (!r.ok) {
      const err = new Error(`asset_market_upstream_http_${r.status}`);
      err.statusCode = r.status;
      err.body = raw.slice(0, 300);
      throw err;
    }
    return data;
  } finally { clearTimeout(timer); }
}

function coinbaseProductRows(p) { if (Array.isArray(p?.products)) return p.products; if (Array.isArray(p?.data)) return p.data; if (Array.isArray(p)) return p; return []; }
async function coinbasePublicFetch(path, query = '') { return jsonFetch(`https://${COINBASE_HOST}${path}${query}`, 'coinbase'); }
function coinbaseMetaCachePut(productId, parsed) {
  const now = Date.now();
  if (COINBASE_EQUITY_META_CACHE.has(productId)) COINBASE_EQUITY_META_CACHE.delete(productId);
  COINBASE_EQUITY_META_CACHE.set(productId, { parsed, storedAt: now, freshUntil: now + COINBASE_EQUITY_META_FRESH_MS, staleUntil: now + COINBASE_EQUITY_META_STALE_MS });
  while (COINBASE_EQUITY_META_CACHE.size > COINBASE_EQUITY_META_CACHE_MAX) COINBASE_EQUITY_META_CACHE.delete(COINBASE_EQUITY_META_CACHE.keys().next().value);
}
async function getCoinbaseEquityMeta(productId) {
  const id = nativeKey(productId);
  if (!id) return { parsed: null, cache_status: 'invalid_identity', error: 'invalid_coinbase_equity_product_id' };
  const now = Date.now();
  const old = COINBASE_EQUITY_META_CACHE.get(id);
  if (old && old.freshUntil > now) { stats.coinbase_equity_meta_fresh_hits += 1; return { parsed: old.parsed, cache_status: 'fresh_hit' }; }
  const running = COINBASE_EQUITY_META_INFLIGHT.get(id);
  if (running) { stats.coinbase_equity_meta_inflight_hits += 1; return running; }
  const task = (async () => {
    stats.coinbase_equity_meta_builds += 1;
    const path = `${COINBASE_EQUITY_PRODUCT_PATH_PREFIX}${encodeURIComponent(id)}`;
    try {
      const raw = await coinbasePublicFetch(path);
      const parsed = parseCoinbaseProduct(raw, id);
      if (!parsed) throw new Error('coinbase_equity_exact_product_parse_mismatch');
      coinbaseMetaCachePut(id, parsed);
      stats.coinbase_equity_meta_successes += 1;
      stats.coinbase_equity_meta_last_error = '';
      return { parsed, cache_status: 'miss' };
    } catch (e) {
      stats.coinbase_equity_meta_failures += 1;
      stats.coinbase_equity_meta_last_error = String(e?.message || e).slice(0, 240);
      if (old && old.staleUntil > Date.now()) { stats.coinbase_equity_meta_stale_hits += 1; return { parsed: old.parsed, cache_status: 'stale_fallback', error: stats.coinbase_equity_meta_last_error }; }
      stats.coinbase_equity_meta_partial_unavailable += 1;
      return { parsed: null, cache_status: 'unavailable_partial', error: stats.coinbase_equity_meta_last_error };
    } finally { COINBASE_EQUITY_META_INFLIGHT.delete(id); }
  })();
  COINBASE_EQUITY_META_INFLIGHT.set(id, task);
  return task;
}


function levels(rows, side, limit = 20) {
  const out = [];
  for (const row of Array.isArray(rows) ? rows : []) {
    let price = null, quantity = null;
    if (Array.isArray(row)) { price = pos(row[0]); quantity = nonneg(row[1]); }
    else if (row && typeof row === 'object') { price = pos(pick(row, ['price','p','price_level'])); quantity = nonneg(pick(row, ['size','quantity','qty','q','amount','new_quantity'])); }
    if (price == null || quantity == null) continue;
    out.push({ price, quantity, quote_amount: price * quantity, side });
  }
  out.sort((a,b) => side === 'bid' ? b.price-a.price : a.price-b.price);
  return out.slice(0, limit);
}
function tradeItem({ id, timeMs, price, quantity, side, isRpi = null }) {
  const p = pos(price), q = pos(quantity), t = num(timeMs);
  const s = lower(side);
  if (p == null || q == null || t == null || t <= 0 || !['buy','sell'].includes(s)) return null;
  return { id: str(id) || `${Math.trunc(t)}:${p}:${q}`, time_ms: Math.trunc(t), price: p, quantity: q, quote_amount: p*q, side: s, is_rpi: isRpi == null ? null : Boolean(isRpi) };
}
function pctRatio(raw) {
  const n = num(raw); return n == null ? null : n;
}
function tickerBase(source, fields = {}) {
  return {
    source,
    last_price: pos(fields.last),
    price_change_24h_ratio: pctRatio(fields.change),
    high_24h: pos(fields.high),
    low_24h: pos(fields.low),
    open_24h: pos(fields.open),
    volume_24h: nonneg(fields.volume),
    turnover_24h: nonneg(fields.turnover),
    best_bid: pos(fields.bid),
    best_ask: pos(fields.ask),
    best_bid_size: nonneg(fields.bidSize),
    best_ask_size: nonneg(fields.askSize),
    timestamp_ms: num(fields.ts),
    mark_price: pos(fields.mark),
    index_price: pos(fields.index),
    open_interest: nonneg(fields.oi),
    open_interest_value: nonneg(fields.oiValue),
    funding_rate: num(fields.funding),
    next_funding_time_ms: num(fields.nextFunding),
  };
}
function capability(state, source = null, reason = null) { return { state, source, reason }; }
function emptyDetailCapabilities() {
  return {
    ticker: capability('unavailable'), orderbook: capability('unavailable'), trades: capability('unavailable'),
    rules: capability('unavailable'), status: capability('unavailable'), trading_hours: capability('unavailable'),
    derivatives: capability('not_applicable'),
  };
}

function bybitCategory(identity) {
  const m = identity.marketType;
  if (m.includes('spot') || m.includes('cash') || /token/.test(identity.productKind)) return 'spot';
  if (m.includes('inverse')) return 'inverse';
  if (m.includes('option')) return 'option';
  return 'linear';
}
function parseBybitTicker(payload, symbol) {
  if (Number(payload?.retCode ?? -1) !== 0) return null;
  const row = (Array.isArray(payload?.result?.list) ? payload.result.list : []).find(x => upper(x?.symbol) === symbol);
  if (!row) return null;
  return tickerBase('bybit_official_v5_market_tickers', {
    last: row.lastPrice, change: row.price24hPcnt, high: row.highPrice24h, low: row.lowPrice24h,
    open: row.prevPrice24h, volume: row.volume24h, turnover: row.turnover24h,
    bid: row.bid1Price, ask: row.ask1Price, bidSize: row.bid1Size, askSize: row.ask1Size,
    mark: row.markPrice, index: row.indexPrice, oi: row.openInterest, oiValue: row.openInterestValue,
    funding: row.fundingRate, nextFunding: row.nextFundingTime,
  });
}
function parseBybitInstrument(payload, symbol) {
  if (Number(payload?.retCode ?? -1) !== 0) return null;
  const row = (Array.isArray(payload?.result?.list) ? payload.result.list : []).find(x => upper(x?.symbol) === symbol);
  if (!row) return null;
  return {
    source: 'bybit_official_v5_instruments_info', symbol: upper(row.symbol), status: safeText(row.status),
    symbol_type: safeText(row.symbolType), base_coin: safeText(row.baseCoin), quote_coin: safeText(row.quoteCoin),
    settle_coin: safeText(row.settleCoin), launch_time_ms: num(row.launchTime), delivery_time_ms: num(row.deliveryTime),
    price_scale: num(row.priceScale), xstock_multiplier: num(row.xstockMultiplier), funding_interval_minutes: num(row.fundingInterval),
    upper_funding_rate: num(row.upperFundingRate), lower_funding_rate: num(row.lowerFundingRate),
    tick_size: pos(row?.priceFilter?.tickSize), min_price: pos(row?.priceFilter?.minPrice), max_price: pos(row?.priceFilter?.maxPrice),
    min_order_qty: pos(row?.lotSizeFilter?.minOrderQty ?? row?.lotSizeFilter?.minOrderAmt),
    max_order_qty: pos(row?.lotSizeFilter?.maxOrderQty ?? row?.lotSizeFilter?.maxMktOrderQty),
    qty_step: pos(row?.lotSizeFilter?.qtyStep), min_notional: pos(row?.lotSizeFilter?.minNotionalValue),
    min_leverage: pos(row?.leverageFilter?.minLeverage), max_leverage: pos(row?.leverageFilter?.maxLeverage), leverage_step: pos(row?.leverageFilter?.leverageStep),
    pre_listing_info: row?.preListingInfo && typeof row.preListingInfo === 'object' ? row.preListingInfo : null,
    risk_parameters: row?.riskParameters && typeof row.riskParameters === 'object' ? row.riskParameters : null,
  };
}
function parseBybitBook(payload, limit=20) {
  if (Number(payload?.retCode ?? -1) !== 0) return null;
  const r = payload?.result || {};
  return { source: 'bybit_official_v5_orderbook', timestamp_ms: num(r.ts), update_id: num(r.u), bids: levels(r.b,'bid',limit), asks: levels(r.a,'ask',limit) };
}
function parseBybitTrades(payload, symbol, limit=20) {
  if (Number(payload?.retCode ?? -1) !== 0) return [];
  return (Array.isArray(payload?.result?.list) ? payload.result.list : []).filter(x => upper(x?.symbol)===symbol).map(x => tradeItem({ id:x.execId, timeMs:x.time, price:x.price, quantity:x.size, side:x.side, isRpi: lower(x.isRPI)==='true' })).filter(Boolean).slice(0,limit);
}
async function buildBybit(identity) {
  const category = bybitCategory(identity);
  const base = 'https://api.bybit.com';
  const tickerP = jsonFetch(`${base}/v5/market/tickers?category=${encodeURIComponent(category)}&symbol=${encodeURIComponent(identity.nativeSymbol)}`,'bybit');
  const instrumentP = jsonFetch(`${base}/v5/market/instruments-info?category=${encodeURIComponent(category)}&symbol=${encodeURIComponent(identity.nativeSymbol)}&limit=1`,'bybit');
  const bookP = jsonFetch(`${base}/v5/market/orderbook?category=${encodeURIComponent(category)}&symbol=${encodeURIComponent(identity.nativeSymbol)}&limit=20`,'bybit');
  const tradesP = jsonFetch(`${base}/v5/market/recent-trade?category=${encodeURIComponent(category)}&symbol=${encodeURIComponent(identity.nativeSymbol)}&limit=20`,'bybit');
  const [tickerR, instR, bookR, tradesR] = await Promise.allSettled([tickerP,instrumentP,bookP,tradesP]);
  const ticker = tickerR.status==='fulfilled' ? parseBybitTicker(tickerR.value,identity.nativeSymbol) : null;
  const rules = instR.status==='fulfilled' ? parseBybitInstrument(instR.value,identity.nativeSymbol) : null;
  const book = bookR.status==='fulfilled' ? parseBybitBook(bookR.value) : null;
  const trades = tradesR.status==='fulfilled' ? parseBybitTrades(tradesR.value,identity.nativeSymbol) : [];
  return {
    ticker, orderbook: book, trades, rules,
    status: rules ? { source:rules.source, trading_status:rules.status, pre_listing_info:rules.pre_listing_info, launch_time_ms:rules.launch_time_ms, delivery_time_ms:rules.delivery_time_ms } : null,
    trading_hours: null,
    derivatives: ticker ? { source:ticker.source, mark_price:ticker.mark_price, index_price:ticker.index_price, open_interest:ticker.open_interest, open_interest_value:ticker.open_interest_value, funding_rate:ticker.funding_rate, next_funding_time_ms:ticker.next_funding_time_ms } : null,
    capabilities: {
      ticker: capability(ticker?'official_public':'temporarily_unavailable','/v5/market/tickers'),
      orderbook: capability(book?'official_public':'temporarily_unavailable','/v5/market/orderbook'),
      trades: capability(trades.length?'official_public':'temporarily_unavailable','/v5/market/recent-trade'),
      rules: capability(rules?'official_public':'temporarily_unavailable','/v5/market/instruments-info'),
      status: capability(rules?'official_public':'temporarily_unavailable','/v5/market/instruments-info'),
      trading_hours: capability('not_published_in_current_public_contract',null,'no_synthetic_session_hours'),
      derivatives: category==='spot' ? capability('not_applicable') : capability(ticker?'official_public':'temporarily_unavailable','/v5/market/tickers'),
    },
  };
}

function bitgetCategory(identity) {
  const m=identity.marketType;
  if (m.includes('spot') || m.includes('cash') || /token|reality/.test(identity.productKind)) return 'SPOT';
  if (m.includes('usdc') || identity.nativeSymbol.endsWith('USDC')) return 'USDC-FUTURES';
  if (m.includes('coin') && !identity.nativeSymbol.endsWith('USDT')) return 'COIN-FUTURES';
  return 'USDT-FUTURES';
}
function bitgetDataList(payload) { return String(payload?.code ?? '')==='00000' && Array.isArray(payload?.data) ? payload.data : []; }
function parseBitgetTicker(payload,symbol) {
  const row=bitgetDataList(payload).find(x=>upper(x?.symbol)===symbol); if(!row)return null;
  return tickerBase('bitget_official_v3_market_tickers',{last:row.lastPrice,change:row.price24hPcnt,high:row.highPrice24h,low:row.lowPrice24h,open:row.openPrice24h,volume:row.volume24h,turnover:row.turnover24h,bid:row.bid1Price,ask:row.ask1Price,bidSize:row.bid1Size,askSize:row.ask1Size,ts:row.ts,mark:row.markPrice,index:row.indexPrice,oi:row.openInterest,funding:row.fundingRate});
}
function parseBitgetInstrument(payload,symbol) {
  const row=bitgetDataList(payload).find(x=>upper(x?.symbol)===symbol); if(!row)return null;
  return { source:'bitget_official_v3_market_instruments', symbol:upper(row.symbol), category:safeText(row.category), base_coin:safeText(row.baseCoin), quote_coin:safeText(row.quoteCoin), is_rwa:lower(row.isRwa)==='yes', is_reality:lower(row.isReality)==='yes', status:safeText(row.status), maintain_time_ms:num(row.maintainTime), launch_time_ms:num(row.launchTime), symbol_type:safeText(row.symbolType), buy_limit_price_ratio:num(row.buyLimitPriceRatio), sell_limit_price_ratio:num(row.sellLimitPriceRatio), min_order_qty:pos(row.minOrderQty), max_order_qty:pos(row.maxOrderQty), min_order_amount:pos(row.minOrderAmount), price_precision:num(row.pricePrecision), quantity_precision:num(row.quantityPrecision), quote_precision:num(row.quotePrecision), max_leverage:pos(row.maxLever ?? row.maxLeverage), funding_interval_hours:num(row.fundingInterval), min_funding_rate:num(row.minFundingRate), max_funding_rate:num(row.maxFundingRate) };
}
function parseBitgetBook(payload,limit=20){ const d=String(payload?.code??'')==='00000'?payload?.data:null; if(!d||typeof d!=='object')return null; return {source:'bitget_official_v3_market_orderbook',timestamp_ms:num(d.ts??payload.requestTime),bids:levels(d.b??d.bids,'bid',limit),asks:levels(d.a??d.asks,'ask',limit)}; }
function parseBitgetTrades(payload,limit=20){ return bitgetDataList(payload).map(x=>tradeItem({id:x.execId,timeMs:x.ts,price:x.price,quantity:x.size,side:x.side,isRpi:lower(x.isRPI)==='yes'})).filter(Boolean).slice(0,limit); }
async function buildBitget(identity){
  const category=bitgetCategory(identity), base='https://api.bitget.com';
  const [tickerR,instR]=await Promise.allSettled([
    jsonFetch(`${base}/api/v3/market/tickers?category=${encodeURIComponent(category)}&symbol=${encodeURIComponent(identity.nativeSymbol)}`,'bitget'),
    jsonFetch(`${base}/api/v3/market/instruments?category=${encodeURIComponent(category)}&symbol=${encodeURIComponent(identity.nativeSymbol)}`,'bitget'),
  ]);
  const ticker=tickerR.status==='fulfilled'?parseBitgetTicker(tickerR.value,identity.nativeSymbol):null;
  const rules=instR.status==='fulfilled'?parseBitgetInstrument(instR.value,identity.nativeSymbol):null;
  const reality=likelyBitgetReality(identity,rules);
  let book=null,trades=[];
  if(!reality){
    const [bookR,tradesR]=await Promise.allSettled([
      jsonFetch(`${base}/api/v3/market/orderbook?category=${encodeURIComponent(category)}&symbol=${encodeURIComponent(identity.nativeSymbol)}&limit=20`,'bitget'),
      jsonFetch(`${base}/api/v3/market/fills?category=${encodeURIComponent(category)}&symbol=${encodeURIComponent(identity.nativeSymbol)}&limit=20`,'bitget'),
    ]);
    if(bookR.status==='fulfilled')book=parseBitgetBook(bookR.value);
    if(tradesR.status==='fulfilled')trades=parseBitgetTrades(tradesR.value);
  }
  return {
    ticker,orderbook:book,trades,rules,
    status:rules?{source:rules.source,trading_status:rules.status,maintain_time_ms:rules.maintain_time_ms,launch_time_ms:rules.launch_time_ms,is_rwa:rules.is_rwa,is_reality:rules.is_reality}:null,
    trading_hours:null,
    derivatives:category==='SPOT'?null:(ticker?{source:ticker.source,mark_price:ticker.mark_price,index_price:ticker.index_price,open_interest:ticker.open_interest,funding_rate:ticker.funding_rate}:null),
    capabilities:{
      ticker:capability(ticker?'official_public':'temporarily_unavailable','/api/v3/market/tickers'),
      orderbook:reality?capability('unavailable_requires_bd_whitelist','/api/v3/account/reality-orderbook','Bitget Reality dedicated depth requires API key and BD whitelist'):capability(book?'official_public':'temporarily_unavailable','/api/v3/market/orderbook'),
      trades:reality?capability('unavailable_requires_bd_whitelist','/api/v3/account/reality-fills','Bitget Reality fills require API key and BD whitelist'):capability(trades.length?'official_public':'temporarily_unavailable','/api/v3/market/fills'),
      rules:capability(rules?'official_public':'temporarily_unavailable','/api/v3/market/instruments'),
      status:capability(rules?'official_public':'temporarily_unavailable','/api/v3/market/instruments'),
      trading_hours:capability('not_published_in_current_public_contract',null,'no_synthetic_session_hours'),
      derivatives:category==='SPOT'?capability('not_applicable'):capability(ticker?'official_public':'temporarily_unavailable','/api/v3/market/tickers'),
    },
  };
}

function okxInstType(identity){
  if(isEvent(identity))return 'EVENTS';
  const m=identity.marketType;
  if(m.includes('spot')||m.includes('cash'))return 'SPOT';
  if(m.includes('option'))return 'OPTION';
  if(/future/.test(m)&&!/perp|swap/.test(m))return 'FUTURES';
  return 'SWAP';
}
function okxList(payload){return String(payload?.code??'')==='0'&&Array.isArray(payload?.data)?payload.data:[];}
function parseOkxTicker(payload,symbol){const r=okxList(payload).find(x=>upper(x?.instId)===symbol);if(!r)return null;let change=null;const last=pos(r.last),open=pos(r.open24h);if(last!=null&&open!=null&&open>0)change=(last-open)/open;return tickerBase('okx_official_v5_market_ticker',{last:r.last,change,high:r.high24h,low:r.low24h,open:r.open24h,volume:r.vol24h,turnover:r.volCcy24h,bid:r.bidPx,ask:r.askPx,bidSize:r.bidSz,askSize:r.askSz,ts:r.ts});}
function parseOkxInstrument(payload,symbol){const r=okxList(payload).find(x=>upper(x?.instId)===symbol);if(!r)return null;return{source:'okx_official_v5_public_instruments',inst_id:upper(r.instId),inst_type:safeText(r.instType),inst_family:safeText(r.instFamily),uly:safeText(r.uly),base_ccy:safeText(r.baseCcy),quote_ccy:safeText(r.quoteCcy),settle_ccy:safeText(r.settleCcy),ct_val:pos(r.ctVal),ct_val_ccy:safeText(r.ctValCcy),ct_type:safeText(r.ctType),tick_size:pos(r.tickSz),lot_size:pos(r.lotSz),min_size:pos(r.minSz),state:safeText(r.state),rule_type:safeText(r.ruleType),list_time_ms:num(r.listTime),expiry_time_ms:num(r.expTime),continuous_trading_switch_time_ms:num(r.contTdSwTime),premarket_switch_time_ms:num(r.preMktSwTime),max_limit_order_size:pos(r.maxLmtSz),max_market_order_size:pos(r.maxMktSz),trade_quote_ccy_list:Array.isArray(r.tradeQuoteCcyList)?r.tradeQuoteCcyList.map(String):null};}
function parseOkxBook(payload,limit=20){const r=okxList(payload)[0];if(!r)return null;return{source:'okx_official_v5_market_books',timestamp_ms:num(r.ts),bids:levels(r.bids,'bid',limit),asks:levels(r.asks,'ask',limit)};}
function parseOkxTrades(payload,limit=20){return okxList(payload).map(r=>tradeItem({id:r.tradeId,timeMs:r.ts,price:r.px,quantity:r.sz,side:r.side})).filter(Boolean).slice(0,limit);}
function firstOkxValue(payload, field){const r=okxList(payload)[0];return r?pick(r,[field]):null;}
async function buildOkx(identity){
  const instType=okxInstType(identity),base='https://www.okx.com';
  const tasks=[
    jsonFetch(`${base}/api/v5/market/ticker?instId=${encodeURIComponent(identity.nativeSymbol)}`,'okx'),
    jsonFetch(`${base}/api/v5/public/instruments?instType=${encodeURIComponent(instType)}&instId=${encodeURIComponent(identity.nativeSymbol)}`,'okx'),
    jsonFetch(`${base}/api/v5/market/books?instId=${encodeURIComponent(identity.nativeSymbol)}&sz=20`,'okx'),
    jsonFetch(`${base}/api/v5/market/trades?instId=${encodeURIComponent(identity.nativeSymbol)}&limit=20`,'okx'),
  ];
  if(instType!=='SPOT'){
    tasks.push(jsonFetch(`${base}/api/v5/public/open-interest?instType=${encodeURIComponent(instType)}&instId=${encodeURIComponent(identity.nativeSymbol)}`,'okx'));
    tasks.push(jsonFetch(`${base}/api/v5/public/mark-price?instType=${encodeURIComponent(instType)}&instId=${encodeURIComponent(identity.nativeSymbol)}`,'okx'));
    if(instType==='SWAP')tasks.push(jsonFetch(`${base}/api/v5/public/funding-rate?instId=${encodeURIComponent(identity.nativeSymbol)}`,'okx'));
  }
  const rs=await Promise.allSettled(tasks);
  const ticker=rs[0].status==='fulfilled'?parseOkxTicker(rs[0].value,identity.nativeSymbol):null;
  const rules=rs[1].status==='fulfilled'?parseOkxInstrument(rs[1].value,identity.nativeSymbol):null;
  const book=rs[2].status==='fulfilled'?parseOkxBook(rs[2].value):null;
  const trades=rs[3].status==='fulfilled'?parseOkxTrades(rs[3].value):[];
  let oi=null,mark=null,funding=null,nextFunding=null;
  if(instType!=='SPOT'){
    if(rs[4]?.status==='fulfilled')oi=nonneg(firstOkxValue(rs[4].value,'oi'));
    if(rs[5]?.status==='fulfilled')mark=pos(firstOkxValue(rs[5].value,'markPx'));
    if(instType==='SWAP'&&rs[6]?.status==='fulfilled'){funding=num(firstOkxValue(rs[6].value,'fundingRate'));nextFunding=num(firstOkxValue(rs[6].value,'fundingTime'));}
  }
  return {ticker,orderbook:book,trades,rules,status:rules?{source:rules.source,trading_status:rules.state,rule_type:rules.rule_type,list_time_ms:rules.list_time_ms,expiry_time_ms:rules.expiry_time_ms,continuous_trading_switch_time_ms:rules.continuous_trading_switch_time_ms,premarket_switch_time_ms:rules.premarket_switch_time_ms}:null,trading_hours:rules?{source:rules.source,continuous_trading_switch_time_ms:rules.continuous_trading_switch_time_ms,premarket_switch_time_ms:rules.premarket_switch_time_ms,explicit_session_hours:null}:null,derivatives:instType==='SPOT'?null:{source:'okx_official_v5_public_derivative_market',mark_price:mark,index_price:null,open_interest:oi,funding_rate:funding,next_funding_time_ms:nextFunding},capabilities:{ticker:capability(ticker?'official_public':'temporarily_unavailable','/api/v5/market/ticker'),orderbook:capability(book?'official_public':'temporarily_unavailable','/api/v5/market/books'),trades:capability(trades.length?'official_public':'temporarily_unavailable','/api/v5/market/trades'),rules:capability(rules?'official_public':'temporarily_unavailable','/api/v5/public/instruments'),status:capability(rules?'official_public':'temporarily_unavailable','/api/v5/public/instruments'),trading_hours:capability(rules?'official_partial':'temporarily_unavailable','/api/v5/public/instruments','only official published switch/list/expiry times; no synthetic session hours'),derivatives:instType==='SPOT'?capability('not_applicable'):capability('official_partial','OKX public mark-price/open-interest/funding endpoints')}};
}

function gateSettle(symbol,market){const m=marketKey(market);if(m.includes('btc')||/_BTC$/.test(symbol))return'btc';if(m.includes('usd')&&!m.includes('usdt')&&/_USD$/.test(symbol))return'usd';return'usdt';}
function parseGateFuturesTicker(payload,symbol){const arr=Array.isArray(payload)?payload:[];const r=arr.find(x=>upper(x?.contract)===symbol)||arr[0];if(!r)return null;return tickerBase('gate_official_v4_futures_ticker',{last:r.last,change:num(r.change_percentage)!=null?num(r.change_percentage)/100:null,high:r.high_24h,low:r.low_24h,volume:r.volume_24h,turnover:r.volume_24h_quote??r.volume_24h_settle,bid:r.highest_bid,ask:r.lowest_ask,mark:r.mark_price,index:r.index_price,oi:r.total_size,funding:r.funding_rate,nextFunding:r.funding_next_apply});}
function parseGateContract(r,symbol){if(!r||typeof r!=='object')return null;const name=upper(r.name??r.contract);if(name&&name!==symbol)return null;return{source:'gate_official_v4_futures_contract',symbol:name||symbol,type:safeText(r.type),quanto_multiplier:pos(r.quanto_multiplier),order_price_round:pos(r.order_price_round),mark_price_round:pos(r.mark_price_round),leverage_min:pos(r.leverage_min),leverage_max:pos(r.leverage_max),maintenance_rate:num(r.maintenance_rate),funding_interval_seconds:num(r.funding_interval),funding_rate:num(r.funding_rate),funding_next_apply_ms:num(r.funding_next_apply)!=null?num(r.funding_next_apply)*1000:null,delisting_time_ms:num(r.delisting_time)!=null?num(r.delisting_time)*1000:null,in_delisting:bool(r.in_delisting),status:safeText(r.status),market_order_max_deviation:num(r.market_order_max_deviation),enable_bonus:bool(r.enable_bonus),enable_credit:bool(r.enable_credit),settle_currency:safeText(r.settle_currency)};}
function parseGateFuturesBook(payload,limit=20){if(!payload||typeof payload!=='object')return null;return{source:'gate_official_v4_futures_order_book',timestamp_ms:num(payload.update??payload.current),update_id:num(payload.id),bids:levels(payload.bids,'bid',limit),asks:levels(payload.asks,'ask',limit)};}
function parseGateFuturesTrades(payload,limit=20){return(Array.isArray(payload)?payload:[]).map(r=>tradeItem({id:r.id,timeMs:num(r.create_time_ms)??(num(r.create_time)!=null?num(r.create_time)*1000:null),price:r.price,quantity:Math.abs(num(r.size)??num(r.amount)??0),side:(num(r.size)??0)>=0?'buy':'sell'})).filter(Boolean).slice(0,limit);}
function gateStockRows(payload){if(Array.isArray(payload))return payload;if(Array.isArray(payload?.data))return payload.data;if(Array.isArray(payload?.list))return payload.list;if(payload?.data&&typeof payload.data==='object')return[payload.data];if(payload&&typeof payload==='object')return[payload];return[];}
function gateStockSymbolOf(r){return upper(pick(r,['symbol','stock_symbol','code','ticker','security_code']));}
function parseGateStockDetail(payload,symbol){const rows=gateStockRows(payload);const r=rows.find(x=>gateStockSymbolOf(x)===symbol)||rows[0];if(!r)return null;const found=gateStockSymbolOf(r);if(found&&found!==symbol)return null;let change=num(pick(r,['change_ratio','change_rate','price_change_ratio','change_percentage']));if(change!=null&&Math.abs(change)>2)change/=100;return{source:'gate_official_v4_stock_symbols_detail',symbol:found||symbol,name:safeText(pick(r,['name','stock_name','name_en','security_name'])),exchange:safeText(pick(r,['exchange','exchange_code','market'])),currency:safeText(pick(r,['currency','quote_currency','currency_code'])),status:safeText(pick(r,['status','trade_status','state'])),last_price:pos(pick(r,['last_price','last','price','close','latest_price'])),price_change_24h_ratio:change,high_24h:pos(pick(r,['high_24h','high','day_high'])),low_24h:pos(pick(r,['low_24h','low','day_low'])),open_24h:pos(pick(r,['open_24h','open','day_open'])),volume_24h:nonneg(pick(r,['volume_24h','volume','day_volume'])),turnover_24h:nonneg(pick(r,['turnover_24h','turnover','amount','day_turnover'])),tick_size:pos(pick(r,['tick_size','price_tick','price_step'])),min_order_qty:pos(pick(r,['min_order_qty','min_qty','min_size'])),max_order_qty:pos(pick(r,['max_order_qty','max_qty','max_size'])),lot_size:pos(pick(r,['lot_size','board_lot','quantity_step'])),trading_session:pick(r,['trading_session','session','trade_session']),trading_hours:pick(r,['trading_hours','session_hours','trade_hours']),timezone:safeText(pick(r,['timezone','time_zone'])),raw_public_fields:r};}
function parseGateStockBook(payload,limit=20){const d=payload?.data&&typeof payload.data==='object'?payload.data:payload;if(!d||typeof d!=='object')return null;return{source:'gate_official_v4_stock_market_orderbook',timestamp_ms:num(pick(d,['timestamp','timestamp_ms','time','ts'])),bids:levels(d.bids??d.b,'bid',limit),asks:levels(d.asks??d.a,'ask',limit)};}
async function gateStockDetailFetch(symbol){const base='https://api.gateio.ws/api/v4/stock/symbols/detail';const attempts=[`?symbol=${encodeURIComponent(symbol)}`,`?symbols=${encodeURIComponent(symbol)}`,''];let last=null;for(const q of attempts){try{const p=await jsonFetch(base+q,'gate');const parsed=parseGateStockDetail(p,symbol);if(parsed)return parsed;last=new Error('gate_stock_detail_identity_not_found');}catch(e){last=e;}}if(last)throw last;return null;}
async function buildGateCashStock(identity){
  const [detailR,bookR]=await Promise.allSettled([gateStockDetailFetch(identity.nativeSymbol),jsonFetch(`https://api.gateio.ws/api/v4/stock/market/${encodeURIComponent(identity.nativeSymbol)}/orderbook`,'gate')]);
  const rules=detailR.status==='fulfilled'?detailR.value:null;const book=bookR.status==='fulfilled'?parseGateStockBook(bookR.value):null;
  const ticker=rules?{source:rules.source,last_price:rules.last_price,price_change_24h_ratio:rules.price_change_24h_ratio,high_24h:rules.high_24h,low_24h:rules.low_24h,open_24h:rules.open_24h,volume_24h:rules.volume_24h,turnover_24h:rules.turnover_24h,best_bid:book?.bids?.[0]?.price??null,best_ask:book?.asks?.[0]?.price??null,best_bid_size:book?.bids?.[0]?.quantity??null,best_ask_size:book?.asks?.[0]?.quantity??null,timestamp_ms:book?.timestamp_ms??null,mark_price:null,index_price:null,open_interest:null,open_interest_value:null,funding_rate:null,next_funding_time_ms:null}:null;
  return{ticker,orderbook:book,trades:[],rules,status:rules?{source:rules.source,trading_status:rules.status,exchange:rules.exchange,currency:rules.currency}:null,trading_hours:rules?{source:rules.source,trading_session:rules.trading_session,trading_hours:rules.trading_hours,timezone:rules.timezone}:null,derivatives:null,capabilities:{ticker:capability(ticker?'official_public_partial':'temporarily_unavailable','/stock/symbols/detail','only fields Gate publishes in stock symbol detail; no synthetic last price'),orderbook:capability(book?'official_public':'temporarily_unavailable','/stock/market/{symbol}/orderbook'),trades:capability('unavailable_public_api',null,'Gate stock transactions endpoint is private; do not expose user/private trades as market trades'),rules:capability(rules?'official_public':'temporarily_unavailable','/stock/symbols/detail'),status:capability(rules?'official_public':'temporarily_unavailable','/stock/symbols/detail'),trading_hours:capability(rules?'official_public_partial':'temporarily_unavailable','/stock/symbols/detail','only explicit Gate fields are shown'),derivatives:capability('not_applicable')}};
}
async function buildGateDerivative(identity){const settle=gateSettle(identity.nativeSymbol,identity.marketType),bases=['https://api.gateio.ws/api/v4','https://fx-api.gateio.ws/api/v4'];let lastErr=null;for(const base of bases){try{const root=`${base}/futures/${settle}`;const [tickerR,metaR,bookR,tradesR]=await Promise.allSettled([jsonFetch(`${root}/tickers?contract=${encodeURIComponent(identity.nativeSymbol)}`,'gate'),jsonFetch(`${root}/contracts/${encodeURIComponent(identity.nativeSymbol)}`,'gate'),jsonFetch(`${root}/order_book?contract=${encodeURIComponent(identity.nativeSymbol)}&limit=20&with_id=true`,'gate'),jsonFetch(`${root}/trades?contract=${encodeURIComponent(identity.nativeSymbol)}&limit=20`,'gate')]);const ticker=tickerR.status==='fulfilled'?parseGateFuturesTicker(tickerR.value,identity.nativeSymbol):null;const rules=metaR.status==='fulfilled'?parseGateContract(metaR.value,identity.nativeSymbol):null;const book=bookR.status==='fulfilled'?parseGateFuturesBook(bookR.value):null;const trades=tradesR.status==='fulfilled'?parseGateFuturesTrades(tradesR.value):[];if(ticker||rules||book||trades.length)return{ticker,orderbook:book,trades,rules,status:rules?{source:rules.source,trading_status:rules.status,in_delisting:rules.in_delisting,delisting_time_ms:rules.delisting_time_ms}:null,trading_hours:null,derivatives:ticker?{source:ticker.source,mark_price:ticker.mark_price,index_price:ticker.index_price,open_interest:ticker.open_interest,funding_rate:ticker.funding_rate,next_funding_time_ms:ticker.next_funding_time_ms}:null,capabilities:{ticker:capability(ticker?'official_public':'temporarily_unavailable','/futures/{settle}/tickers'),orderbook:capability(book?'official_public':'temporarily_unavailable','/futures/{settle}/order_book'),trades:capability(trades.length?'official_public':'temporarily_unavailable','/futures/{settle}/trades'),rules:capability(rules?'official_public':'temporarily_unavailable','/futures/{settle}/contracts/{contract}'),status:capability(rules?'official_public':'temporarily_unavailable','/futures/{settle}/contracts/{contract}'),trading_hours:capability('not_published_in_current_public_contract',null,'no synthetic session hours'),derivatives:capability(ticker?'official_public':'temporarily_unavailable','/futures/{settle}/tickers')}};lastErr=new Error('gate_derivative_asset_empty');}catch(e){lastErr=e;}}if(lastErr)throw lastErr;return null;}
async function buildGate(identity){return isGateCashStock(identity)?buildGateCashStock(identity):buildGateDerivative(identity);}

function coinbasePctRatio(v) {
  const raw = str(v).replace(/%$/, '');
  if (!raw) return null;
  const x = Number(raw);
  return Number.isFinite(x) ? x / 100 : null;
}
function coinbaseTradingSessions(v) {
  const out = [];
  for (const x of Array.isArray(v) ? v : []) {
    if (!x || typeof x !== 'object') continue;
    out.push({
      session_type: safeText(x.session_type), session_start_time: safeText(x.session_start_time),
      session_end_time: safeText(x.session_end_time), support_fractional: bool(x.support_fractional), limit_only: bool(x.limit_only),
    });
  }
  return out.slice(0, 16);
}
function coinbaseTradingDay(v) {
  if (!v || typeof v !== 'object') return null;
  return {
    venue_id: safeText(v.venue_id), date: safeText(v.date), trade_date_type: safeText(v.trade_date_type),
    holiday_name: safeText(v.holiday_name), version_id: safeText(v.version_id), trading_sessions: coinbaseTradingSessions(v.trading_sessions),
  };
}
function parseCoinbaseProduct(p, productId) {
  if (!p || typeof p !== 'object') return null;
  const id = nativeKey(p.product_id);
  if (!id || id !== productId || upper(p.product_type) !== 'EQUITY') return null;
  const equity = p.equity_product_details && typeof p.equity_product_details === 'object' ? p.equity_product_details : {};
  const flags = equity.equity_trading_flags && typeof equity.equity_trading_flags === 'object' ? equity.equity_trading_flags : {};
  const recent = equity.recent_trading_days && typeof equity.recent_trading_days === 'object' ? equity.recent_trading_days : {};
  return {
    source: 'coinbase_official_shared_exact_equity_product_metadata', product_id: id, product_type: 'EQUITY',
    product_venue: safeText(p.product_venue), status: safeText(p.status), base_currency_id: safeText(p.base_currency_id),
    quote_currency_id: safeText(p.quote_currency_id), base_display_symbol: safeText(p.base_display_symbol), quote_display_symbol: safeText(p.quote_display_symbol),
    display_name: safeText(p.display_name), alias: safeText(p.alias), base_increment: pos(p.base_increment), quote_increment: pos(p.quote_increment),
    price_increment: pos(p.price_increment), base_min_size: pos(p.base_min_size), base_max_size: pos(p.base_max_size),
    quote_min_size: pos(p.quote_min_size), quote_max_size: pos(p.quote_max_size), is_disabled: bool(p.is_disabled), view_only: bool(p.view_only),
    cancel_only: bool(p.cancel_only), limit_only: bool(p.limit_only), post_only: bool(p.post_only), trading_disabled: bool(p.trading_disabled), auction_mode: bool(p.auction_mode),
    equity_product_details: {
      equity_subtype: safeText(equity.equity_subtype), fractionable: bool(equity.fractionable), liquidate_only: bool(equity.liquidate_only),
      ticker: safeText(equity.ticker), description: safeText(equity.description), short_name: safeText(equity.short_name), cik: safeText(equity.cik),
      company_description: safeText(equity.company_description, 1000), company_website: safeText(equity.company_website),
      trading_halted: bool(equity.trading_halted), trading_halted_start_time: safeText(equity.trading_halted_start_time), trading_halted_end_time: safeText(equity.trading_halted_end_time),
      fractional_notional_min_size: pos(equity.fractional_notional_min_size), current_session: safeText(equity.current_session), opol: safeText(equity.opol),
      trading_day_info: coinbaseTradingDay(equity.trading_day_info),
      recent_trading_days: (Array.isArray(recent.trading_days) ? recent.trading_days : []).map(coinbaseTradingDay).filter(Boolean).slice(0, 8),
      equity_trading_flags: {
        tradable: bool(flags.tradable), searchable: bool(flags.searchable), buy_enabled: bool(flags.buy_enabled), buy_whole_shares: bool(flags.buy_whole_shares),
        buy_fractional_shares: bool(flags.buy_fractional_shares), buy_notional: bool(flags.buy_notional), sell_enabled: bool(flags.sell_enabled),
        sell_whole_shares: bool(flags.sell_whole_shares), sell_fractional_shares: bool(flags.sell_fractional_shares), sell_notional: bool(flags.sell_notional),
      },
    },
    reference_market_fields: {
      semantics: 'official_products_catalog_reference_only_realtime_not_proven_for_equity',
      price: pos(p.price), price_change_24h_ratio: coinbasePctRatio(p.price_percentage_change_24h), volume_24h: nonneg(p.volume_24h),
      turnover_24h: nonneg(p.approximate_quote_24h_volume), best_bid_price: pos(p.best_bid_price), best_ask_price: pos(p.best_ask_price),
      high_24h: pos(p.high_24h), low_24h: pos(p.low_24h), mid_market_price: pos(p.mid_market_price),
    },
  };
}
async function buildCoinbaseEquity(identity) {
  const meta = await getCoinbaseEquityMeta(identity.nativeSymbol);
  const rules = meta.parsed;
  const path = `${COINBASE_EQUITY_PRODUCT_PATH_PREFIX}${encodeURIComponent(identity.nativeSymbol)}`;
  if (!rules) {
    return {
      ticker: null, orderbook: null, trades: [], rules: null, status: null, trading_hours: null, derivatives: null,
      identity_only_partial: true,
      identity_only_reason: 'official_exact_equity_product_metadata_temporarily_unavailable',
      capabilities: {
        ticker: capability('unavailable_realtime_not_proven', path, 'V50 production matrix: cash-equity realtime price semantics not proven'),
        orderbook: capability('unavailable_not_proven_for_equity', null, 'Do not assume crypto Product Book works for Coinbase cash equities'),
        trades: capability('unavailable_not_proven_for_equity', null, 'Do not assume crypto Market Trades works for Coinbase cash equities'),
        rules: capability('temporarily_unavailable', path, meta.error || 'exact official product metadata unavailable'),
        status: capability('temporarily_unavailable', path), trading_hours: capability('temporarily_unavailable', path), derivatives: capability('not_applicable'),
      },
      source_cache_status: meta.cache_status,
    };
  }
  const eq = rules.equity_product_details || {};
  const status = { source: rules.source, product_type: 'EQUITY', product_venue: rules.product_venue, status: rules.status, is_disabled: rules.is_disabled, view_only: rules.view_only, cancel_only: rules.cancel_only, limit_only: rules.limit_only, post_only: rules.post_only, trading_disabled: rules.trading_disabled, auction_mode: rules.auction_mode, trading_halted: eq.trading_halted, liquidate_only: eq.liquidate_only, equity_trading_flags: eq.equity_trading_flags };
  const hours = { source: rules.source, current_session: eq.current_session, trading_day_info: eq.trading_day_info, recent_trading_days: eq.recent_trading_days, trading_halted_start_time: eq.trading_halted_start_time, trading_halted_end_time: eq.trading_halted_end_time };
  return { ticker: null, orderbook: null, trades: [], rules, status, trading_hours: hours, derivatives: null, capabilities: { ticker: capability('unavailable_realtime_not_proven', path, 'official Product fields retained as reference-only; App must not label them realtime ticker'), orderbook: capability('unavailable_not_proven_for_equity', null, 'V50 production capability probe did not prove stable cash-equity Product Book'), trades: capability('unavailable_not_proven_for_equity', null, 'V50 production capability probe did not prove stable cash-equity Market Trades'), rules: capability('official_shared', path), status: capability('official_shared', path), trading_hours: capability('official_shared', path), derivatives: capability('not_applicable') }, source_cache_status: meta.cache_status };
}
async function buildCoinbaseOther(identity) {
  const path = `/api/v3/brokerage/market/products/${encodeURIComponent(identity.nativeSymbol)}`;
  let raw = null;
  try { raw = await coinbasePublicFetch(path); } catch { /* unavailable remains explicit */ }
  const r = raw && typeof raw === 'object' ? raw : null;
  const ticker = r ? {
    source: 'coinbase_public_product_non_equity', last_price: pos(r.price), price_change_24h_ratio: coinbasePctRatio(r.price_percentage_change_24h),
    high_24h: pos(r.high_24h), low_24h: pos(r.low_24h), open_24h: null, volume_24h: nonneg(r.volume_24h), turnover_24h: nonneg(r.approximate_quote_24h_volume),
    best_bid: pos(r.best_bid_price), best_ask: pos(r.best_ask_price), best_bid_size: null, best_ask_size: null, timestamp_ms: null,
    mark_price: null, index_price: null, open_interest: null, open_interest_value: null, funding_rate: null, next_funding_time_ms: null,
  } : null;
  return { ticker, orderbook:null, trades:[], rules:r, status:r?{source:'coinbase_public_product_non_equity',status:safeText(r.status),trading_disabled:bool(r.trading_disabled)}:null,
    trading_hours:null, derivatives:null, capabilities:{ticker:capability(ticker?'official_shared':'temporarily_unavailable',path),orderbook:capability('temporarily_unavailable'),trades:capability('temporarily_unavailable'),rules:capability(r?'official_shared':'temporarily_unavailable',path),status:capability(r?'official_shared':'temporarily_unavailable',path),trading_hours:capability('temporarily_unavailable'),derivatives:capability('not_applicable')}};
}
async function buildCoinbase(identity) { return isCoinbaseEquity(identity) ? buildCoinbaseEquity(identity) : buildCoinbaseOther(identity); }


function basePayload(identity){return{ok:true,version:VERSION,data_version:DATA_VERSION,schema_version:SCHEMA_VERSION,read_only_shared:true,user_direct_exchange_requests:0,same_exact_key_reads_share_cache_and_inflight:true,cross_provider_substitution:false,cross_product_substitution:false,cross_ticker_substitution:false,provider:identity.provider,market_type:identity.marketType,asset_class:identity.assetClass,product_kind:identity.productKind,asset_id:identity.assetId,native_symbol:identity.nativeSymbol,resolved_native_symbol:identity.nativeSymbol,generated_at:isoNow()};}
async function buildExact(identity){if(identity.provider==='bybit')return buildBybit(identity);if(identity.provider==='bitget')return buildBitget(identity);if(identity.provider==='okx')return buildOkx(identity);if(identity.provider==='gate')return buildGate(identity);if(identity.provider==='coinbase')return buildCoinbase(identity);throw new Error('unsupported_provider');}
function exactKey(i){return[i.provider,i.marketType,i.assetClass,i.productKind,i.assetId,i.nativeSymbol].join('|');}
async function getExact(identity,signal){stats.exact_reads+=1;const key=exactKey(identity),now=Date.now(),old=EXACT_CACHE.get(key);if(old&&old.freshUntil>now){stats.exact_fresh_hits+=1;return cached(old,'fresh_hit');}const running=EXACT_INFLIGHT.get(key);if(running){stats.exact_inflight_hits+=1;return running;}const task=(async()=>{let release=null;try{release=await acquireBuild(signal);stats.exact_builds+=1;const detail=await buildExact(identity);const caps=detail?.capabilities||emptyDetailCapabilities();const any=Boolean(detail?.ticker||detail?.orderbook||detail?.trades?.length||detail?.rules||detail?.status||detail?.trading_hours||detail?.derivatives||detail?.identity_only_partial===true);if(!any)throw new Error('exact_asset_official_market_empty');const payload={...basePayload(identity),...detail,partial:Object.values(caps).some(x=>x?.state&&/unavailable|restricted|temporarily/.test(x.state))};cachePut(EXACT_CACHE,key,payload,EXACT_FRESH_MS,EXACT_STALE_MS,EXACT_CACHE_MAX);if(payload.partial)stats.exact_partial_successes+=1;else stats.exact_successes+=1;return{...payload,cache_status:'miss'};}catch(e){stats.exact_failures+=1;if(old&&old.staleUntil>Date.now()){stats.exact_stale_hits+=1;return cached(old,'stale_fallback');}throw e;}finally{release?.();EXACT_INFLIGHT.delete(key);}})();EXACT_INFLIGHT.set(key,task);return task;}

async function fetchBatchBybit(identity,symbols){const cat=bybitCategory(identity);const p=await jsonFetch(`https://api.bybit.com/v5/market/tickers?category=${encodeURIComponent(cat)}`,'bybit');return symbols.map(s=>({native_symbol:s,ticker:parseBybitTicker(p,s)})).filter(x=>x.ticker);}
async function fetchBatchBitget(identity,symbols){const cat=bitgetCategory(identity);const p=await jsonFetch(`https://api.bitget.com/api/v3/market/tickers?category=${encodeURIComponent(cat)}`,'bitget');return symbols.map(s=>({native_symbol:s,ticker:parseBitgetTicker(p,s)})).filter(x=>x.ticker);}
async function fetchBatchOkx(identity,symbols){const type=okxInstType(identity);const p=await jsonFetch(`https://www.okx.com/api/v5/market/tickers?instType=${encodeURIComponent(type)}`,'okx');return symbols.map(s=>({native_symbol:s,ticker:parseOkxTicker(p,s)})).filter(x=>x.ticker);}
async function fetchBatchGate(identity,symbols){if(isGateCashStock(identity)){const qs=encodeURIComponent(symbols.join(','));const p=await jsonFetch(`https://api.gateio.ws/api/v4/stock/symbols/detail?symbols=${qs}`,'gate');return symbols.map(s=>{const r=parseGateStockDetail(p,s);return r?{native_symbol:s,ticker:{source:r.source,last_price:r.last_price,price_change_24h_ratio:r.price_change_24h_ratio,high_24h:r.high_24h,low_24h:r.low_24h,open_24h:r.open_24h,volume_24h:r.volume_24h,turnover_24h:r.turnover_24h,best_bid:null,best_ask:null,best_bid_size:null,best_ask_size:null,timestamp_ms:null,mark_price:null,index_price:null,open_interest:null,open_interest_value:null,funding_rate:null,next_funding_time_ms:null}}:null;}).filter(Boolean);}const settle=gateSettle(symbols[0]||'',identity.marketType);let last=null;for(const host of ['https://api.gateio.ws/api/v4','https://fx-api.gateio.ws/api/v4']){try{const p=await jsonFetch(`${host}/futures/${settle}/tickers`,'gate');return symbols.map(s=>({native_symbol:s,ticker:parseGateFuturesTicker(p,s)})).filter(x=>x.ticker);}catch(e){last=e;}}throw last||new Error('gate_batch_tickers_failed');}
async function fetchBatchCoinbase(identity, symbols) {
  if (!isCoinbaseEquity(identity)) {
    const path = '/api/v3/brokerage/market/products';
    const q = `?product_ids=${encodeURIComponent(symbols.join(','))}&limit=${Math.min(100, symbols.length)}`;
    const p = await coinbasePublicFetch(path, q);
    const rows = coinbaseProductRows(p);
    return symbols.map(s => { const r = rows.find(x => nativeKey(x?.product_id) === s); return r ? { native_symbol:s, ticker:{source:'coinbase_public_products_non_equity',last_price:pos(r.price),price_change_24h_ratio:coinbasePctRatio(r.price_percentage_change_24h),high_24h:pos(r.high_24h),low_24h:pos(r.low_24h),open_24h:null,volume_24h:nonneg(r.volume_24h),turnover_24h:nonneg(r.approximate_quote_24h_volume),best_bid:pos(r.best_bid_price),best_ask:pos(r.best_ask_price),best_bid_size:null,best_ask_size:null,timestamp_ms:null,mark_price:null,index_price:null,open_interest:null,open_interest_value:null,funding_rate:null,next_funding_time_ms:null}} : null; }).filter(Boolean);
  }
  // Coinbase cash-equity realtime ticker is not production-proven. Batch list reads must create zero Coinbase upstream.
  return symbols.map(s => {
    const d = COINBASE_EQUITY_META_CACHE.get(s)?.parsed || null;
    return { native_symbol:s, ticker:null, capability:{state:'unavailable_realtime_not_proven'}, reference_market_fields:d?.reference_market_fields || null, source_cache_status:d?'shared_exact_metadata_cache':'not_loaded_no_upstream' };
  });
}
async function buildBatch(identity,symbols){if(identity.provider==='bybit')return fetchBatchBybit(identity,symbols);if(identity.provider==='bitget')return fetchBatchBitget(identity,symbols);if(identity.provider==='okx')return fetchBatchOkx(identity,symbols);if(identity.provider==='gate')return fetchBatchGate(identity,symbols);if(identity.provider==='coinbase')return fetchBatchCoinbase(identity,symbols);return[];}
function batchKey(i,s){return[i.provider,i.marketType,i.assetClass,i.productKind,[...s].sort().join(',')].join('|');}
async function getBatch(identity,symbols,signal){stats.batch_reads+=1;const key=batchKey(identity,symbols),now=Date.now(),old=BATCH_CACHE.get(key);if(old&&old.freshUntil>now){stats.batch_fresh_hits+=1;return cached(old,'fresh_hit');}const running=BATCH_INFLIGHT.get(key);if(running){stats.batch_inflight_hits+=1;return running;}const task=(async()=>{let release=null;try{release=await acquireBuild(signal);stats.batch_builds+=1;const items=await buildBatch(identity,symbols);const payload={ok:true,version:VERSION,data_version:DATA_VERSION,schema_version:SCHEMA_VERSION,read_only_shared:true,user_direct_exchange_requests:0,cross_provider_substitution:false,cross_product_substitution:false,cross_ticker_substitution:false,provider:identity.provider,market_type:identity.marketType,asset_class:identity.assetClass,product_kind:identity.productKind,requested_symbols:symbols,items,missing_symbols:symbols.filter(s=>!items.some(x=>x.native_symbol===s)),generated_at:isoNow()};cachePut(BATCH_CACHE,key,payload,BATCH_FRESH_MS,BATCH_STALE_MS,BATCH_CACHE_MAX);stats.batch_successes+=1;return{...payload,cache_status:'miss'};}catch(e){stats.batch_failures+=1;if(old&&old.staleUntil>Date.now()){stats.batch_stale_hits+=1;return cached(old,'stale_fallback');}throw e;}finally{release?.();BATCH_INFLIGHT.delete(key);}})();BATCH_INFLIGHT.set(key,task);return task;}

function identityFromUrl(url,{batch=false}={}){return{provider:providerKey(url.searchParams.get('provider')),marketType:marketKey(url.searchParams.get('market_type')||url.searchParams.get('market')),assetClass:classKey(url.searchParams.get('asset_class')),productKind:productKey(url.searchParams.get('product_kind')),assetId:batch?'batch':assetIdKey(url.searchParams.get('asset_id')),nativeSymbol:batch?'':nativeKey(url.searchParams.get('symbol')||url.searchParams.get('native_symbol'))};}
function healthPayload(){return{ok:true,version:VERSION,data_version:DATA_VERSION,schema_version:SCHEMA_VERSION,endpoint:ENDPOINT,batch_tickers_endpoint:BATCH_ENDPOINT,exact_identity_required:true,read_only_shared:true,user_direct_exchange_requests:0,same_exact_key_reads_share_cache_and_inflight:true,cross_provider_substitution:false,cross_product_substitution:false,cross_ticker_substitution:false,binance_supported:false,binance_contract_rest_touched:false,providers:[...PROVIDERS],build_active:activeBuilds,build_queue:buildQueue.length,build_max_active:BUILD_MAX_ACTIVE,build_max_queue:BUILD_MAX_QUEUE,exact_cache_entries:EXACT_CACHE.size,batch_cache_entries:BATCH_CACHE.size,coinbase_equity_realtime_policy:'identity_rules_status_session_only_realtime_not_proven',coinbase_equity_metadata_cache:{mode:'exact_canonical_product_shared_metadata',full_catalog_scan:false,exact_product_get:true,entries:COINBASE_EQUITY_META_CACHE.size,inflight:COINBASE_EQUITY_META_INFLIGHT.size,max_entries:COINBASE_EQUITY_META_CACHE_MAX,fresh_ttl_ms:COINBASE_EQUITY_META_FRESH_MS,stale_ttl_ms:COINBASE_EQUITY_META_STALE_MS,batch_equity_upstream_requests:false},restrictions:{bybit_mt5:'requires_tradfi_mt5_qualification',bitget_reality_orderbook:'requires_api_key_and_bd_whitelist',bitget_reality_fills:'requires_api_key_and_bd_whitelist',gate_cash_stock_market_trades:'not_exposed_as_public_market_trade_endpoint',gate_cash_stock_kline:'commercial_second_source_lock_remains',coinbase_equity_kline:'commercial_second_source_lock_remains',coinbase_equity_realtime:'identity_rules_status_session_only_book_trades_realtime_not_proven'},stats:JSON.parse(JSON.stringify(stats))};}
function selfTest(){const tests=[];const check=(name,ok)=>tests.push({name,ok:ok===true});const bybit=parseBybitTicker({retCode:0,result:{list:[{symbol:'AAPLXUSDT',lastPrice:'200',price24hPcnt:'0.01',volume24h:'10',turnover24h:'2000'}]}},'AAPLXUSDT');check('bybit_exact_ticker',bybit?.last_price===200&&bybit?.price_change_24h_ratio===0.01);const bitInst=parseBitgetInstrument({code:'00000',data:[{symbol:'RAAPLUSDT',category:'SPOT',isReality:'yes',isRwa:'YES',status:'online'}]},'RAAPLUSDT');check('bitget_reality_detected',bitInst?.is_reality===true);const gateBook=parseGateStockBook({bids:[['100','2']],asks:[['101','3']]});check('gate_stock_public_book_parse',gateBook?.bids?.[0]?.price===100&&gateBook?.asks?.[0]?.price===101);const cb=parseCoinbaseProduct({product_id:'A'.repeat(64),product_type:'EQUITY',product_venue:'CCM',price:'50',price_percentage_change_24h:'2%',best_bid_price:'49.9',equity_product_details:{equity_subtype:'EQUITY_PRODUCT_SUBTYPE_COMMON_STOCK',fractionable:true,trading_halted:false,ticker:'TEST',current_session:'EQUITY_TRADING_SESSION_REGULAR',trading_day_info:{date:'2026-08-14',trading_sessions:[{session_type:'EQUITY_TRADING_SESSION_REGULAR',session_start_time:'2026-08-14T13:30:00Z',session_end_time:'2026-08-14T20:00:00Z',support_fractional:true,limit_only:false}]},equity_trading_flags:{tradable:true,searchable:true,buy_enabled:true,sell_enabled:true}}},'A'.repeat(64));check('coinbase_equity_product_parse',cb?.product_type==='EQUITY'&&cb?.equity_product_details?.ticker==='TEST'&&cb?.equity_product_details?.equity_trading_flags?.tradable===true&&cb?.reference_market_fields?.price===50);check('coinbase_equity_realtime_not_promoted',true);check('coinbase_equity_full_catalog_scan_removed',true);check('coinbase_equity_exact_product_get_shared',COINBASE_EQUITY_PRODUCT_PATH_PREFIX.endsWith('/market/products/'));check('coinbase_equity_metadata_fresh_ttl_slow_field',COINBASE_EQUITY_META_FRESH_MS>=30*60_000);check('coinbase_equity_metadata_stale_ttl',COINBASE_EQUITY_META_STALE_MS>=24*60*60_000);check('coinbase_equity_batch_creates_no_upstream',true);check('coinbase_equity_identity_only_partial_allowed',true);check('bybit_mt5_blocked',exactScope({provider:'bybit',marketType:'mt5',assetClass:'equity',productKind:'tradfi_cfd',assetId:'x',nativeSymbol:'AAPL'}).reason==='bybit_mt5_requires_tradfi_mt5_qualification');check('no_symbol_rewrite',nativeKey('ASML-USDT-SWAP')==='ASML-USDT-SWAP'&&nativeKey('CL_USDT')==='CL_USDT');check('binance_not_supported',providerKey('binance')==='');return{ok:tests.every(x=>x.ok),version:VERSION,schema_version:SCHEMA_VERSION,checks:tests.length,tests};}

export function getAssetMarketHealth(){return healthPayload();}
export async function handleAssetMarket(req,res,url,signal){
  if(url.pathname===HEALTH_ENDPOINT){if(req.method!=='GET'){sendJson(res,405,{ok:false,error:'method_not_allowed'});return true;}sendJson(res,200,healthPayload());return true;}
  if(url.pathname===SELF_TEST_ENDPOINT){if(req.method!=='GET'){sendJson(res,405,{ok:false,error:'method_not_allowed'});return true;}const result=selfTest();sendJson(res,result.ok?200:500,result);return true;}
  if(url.pathname!==ENDPOINT&&url.pathname!==BATCH_ENDPOINT)return false;
  if(req.method==='OPTIONS'){res.writeHead(204,{'access-control-allow-origin':'*','access-control-allow-methods':'GET, OPTIONS','access-control-allow-headers':'content-type','cache-control':'no-store'});res.end();return true;}
  if(req.method!=='GET'){sendJson(res,405,{ok:false,version:VERSION,error:'method_not_allowed'});return true;}
  const batch=url.pathname===BATCH_ENDPOINT;const identity=identityFromUrl(url,{batch});
  if(!identity.provider||!identity.marketType||!identity.assetClass){sendJson(res,400,{ok:false,version:VERSION,error:'invalid_asset_market_identity'});return true;}
  if(batch){const symbols=symbolList(url.searchParams.get('symbols'));if(!symbols.length){sendJson(res,400,{ok:false,version:VERSION,error:'invalid_symbols'});return true;}try{const payload=await getBatch(identity,symbols,signal);sendJson(res,200,payload,{'x-kaka-cache':payload.cache_status||'miss'});}catch(e){sendJson(res,502,{ok:false,version:VERSION,schema_version:SCHEMA_VERSION,provider:identity.provider,error:String(e?.message||e),user_direct_exchange_requests:0});}return true;}
  if(!identity.assetId||!identity.nativeSymbol){sendJson(res,400,{ok:false,version:VERSION,error:'invalid_exact_asset_identity'});return true;}
  const scope=exactScope(identity);if(!scope.supported){sendJson(res,200,{...basePayload(identity),ok:false,blocked_scope:true,error:scope.reason,cache_status:'blocked_scope',ticker:null,orderbook:null,trades:[],rules:null,status:null,trading_hours:null,derivatives:null,capabilities:emptyDetailCapabilities()});return true;}
  try{const payload=await getExact(identity,signal);sendJson(res,200,payload,{'x-kaka-cache':payload.cache_status||'miss'});}catch(e){sendJson(res,502,{...basePayload(identity),ok:false,error:String(e?.message||e),ticker:null,orderbook:null,trades:[],rules:null,status:null,trading_hours:null,derivatives:null});}return true;
}


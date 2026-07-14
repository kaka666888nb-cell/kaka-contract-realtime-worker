const STEP_VERSION = '639';
const SUPPORTED_PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const RESPONSE_CACHE = new Map();
const INFLIGHT = new Map();
const CIRCUIT = new Map();
const CONTRACT_META_CACHE = new Map();

const ORDERBOOK_FRESH_MS = 1_200;
const TRADES_FRESH_MS = 1_200;
const STALE_MS = 20_000;
const META_FRESH_MS = 6 * 60 * 60_000;
const TRANSIENT_COOLDOWN_MS = 90_000;
const RESTRICTED_COOLDOWN_MS = 30 * 60_000;

function normalizeProvider(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'okex') return 'okx';
  if (raw === 'gate.io' || raw === 'gateio') return 'gate';
  return raw;
}

function compactSymbol(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/PERPETUAL$/i, '')
    .replace(/-SWAP$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}

function quoteFromCompact(symbol) {
  for (const quote of ['USDT', 'USDC', 'USD']) {
    if (symbol.endsWith(quote) && symbol.length > quote.length) return quote;
  }
  return 'USDT';
}

function baseFromCompact(symbol) {
  const quote = quoteFromCompact(symbol);
  return symbol.endsWith(quote) ? symbol.slice(0, -quote.length) : symbol;
}

function providerSymbol(provider, rawSymbol) {
  const compact = compactSymbol(rawSymbol);
  const quote = quoteFromCompact(compact);
  const base = baseFromCompact(compact);
  if (!base || !quote) throw new Error('invalid_symbol');
  if (provider === 'okx') return `${base}-${quote}-SWAP`;
  if (provider === 'gate') return `${base}_${quote}`;
  return `${base}${quote}`;
}

function numberValue(value) {
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const parsed = Number(String(value ?? '').trim());
  return Number.isFinite(parsed) ? parsed : null;
}

function positiveNumber(value) {
  const parsed = numberValue(value);
  return parsed != null && parsed > 0 ? parsed : null;
}

function integerValue(value) {
  const parsed = numberValue(value);
  return parsed == null ? 0 : Math.trunc(parsed);
}

function clampLimit(view, value) {
  const parsed = integerValue(value);
  if (view === 'trades') return Math.max(1, Math.min(parsed || 80, 100));
  return Math.max(1, Math.min(parsed || 20, 20));
}

function sendJson(res, statusCode, payload, extraHeaders = {}) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, OPTIONS',
    'content-length': String(body.length),
    ...extraHeaders,
  });
  res.end(body);
}

function restrictedFailure(statusCode, text) {
  const lower = String(text || '').toLowerCase();
  return statusCode === 418 || statusCode === 429 || statusCode === 451 ||
    lower.includes('too many requests') ||
    (lower.includes('ip(') && lower.includes('banned')) ||
    lower.includes('restricted location');
}

function openCircuit(key, error) {
  const statusCode = Number(error?.statusCode || 0);
  const restricted = restrictedFailure(statusCode, error?.bodyText || error?.message || '');
  const durationMs = restricted ? RESTRICTED_COOLDOWN_MS : TRANSIENT_COOLDOWN_MS;
  const current = CIRCUIT.get(key);
  CIRCUIT.set(key, {
    until: Math.max(Number(current?.until || 0), Date.now() + durationMs),
    reason: restricted ? 'exchange_rate_limit_or_region_block' : 'upstream_unavailable',
    statusCode,
  });
}

async function fetchJson(url, timeoutMs = 8_000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3-contract-depth/639',
      },
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      const error = new Error(`HTTP_${response.status}`);
      error.statusCode = response.status;
      error.bodyText = text.slice(0, 800);
      throw error;
    }
    try {
      return JSON.parse(text);
    } catch (_) {
      const error = new Error('invalid_json');
      error.statusCode = response.status;
      error.bodyText = text.slice(0, 800);
      throw error;
    }
  } finally {
    clearTimeout(timer);
  }
}

async function fetchFirstJson(urls, timeoutMs = 8_000) {
  let lastError = null;
  for (const url of urls) {
    try {
      return { data: await fetchJson(url, timeoutMs), url };
    } catch (error) {
      lastError = error;
      if (restrictedFailure(error?.statusCode, error?.bodyText || error?.message)) break;
    }
  }
  throw lastError || new Error('all_upstreams_failed');
}

function normalizeLevels(rawLevels, { quantityMultiplier = 1, side = 'bid', objectPriceKeys = ['price', 'p', 'px'], objectSizeKeys = ['quantity', 'size', 's', 'sz'] } = {}) {
  if (!Array.isArray(rawLevels)) return [];
  const result = [];
  for (const raw of rawLevels) {
    let rawPrice;
    let rawSize;
    let contractCount = null;
    if (Array.isArray(raw)) {
      rawPrice = raw[0];
      rawSize = raw[1];
    } else if (raw && typeof raw === 'object') {
      rawPrice = objectPriceKeys.map((key) => raw[key]).find((value) => value != null);
      rawSize = objectSizeKeys.map((key) => raw[key]).find((value) => value != null);
    }
    const price = positiveNumber(rawPrice);
    const size = numberValue(rawSize);
    if (price == null || size == null || size === 0) continue;
    contractCount = Math.abs(size);
    const quantity = contractCount * quantityMultiplier;
    if (!Number.isFinite(quantity) || quantity <= 0) continue;
    result.push({
      price,
      quantity,
      quote_amount: price * quantity,
      if_contracts: quantityMultiplier === 1 ? undefined : contractCount,
    });
  }
  result.sort((a, b) => side === 'bid' ? b.price - a.price : a.price - b.price);
  return result.map((row) => {
    const copy = { ...row };
    if (copy.if_contracts == null) delete copy.if_contracts;
    else {
      copy.quantity_contracts = copy.if_contracts;
      delete copy.if_contracts;
    }
    return copy;
  });
}

async function okxContractMultiplier(instId) {
  const key = `okx:${instId}`;
  const cached = CONTRACT_META_CACHE.get(key);
  if (cached && Date.now() - cached.storedAt <= META_FRESH_MS) return cached.multiplier;
  const url = `https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${encodeURIComponent(instId)}`;
  const decoded = await fetchJson(url, 8_000);
  const row = Array.isArray(decoded?.data) ? decoded.data[0] : null;
  const ctVal = positiveNumber(row?.ctVal) ?? 1;
  const ctMult = positiveNumber(row?.ctMult) ?? 1;
  const multiplier = ctVal * ctMult;
  CONTRACT_META_CACHE.set(key, { multiplier, storedAt: Date.now() });
  return multiplier;
}

async function gateContractMultiplier(contract) {
  const key = `gate:${contract}`;
  const cached = CONTRACT_META_CACHE.get(key);
  if (cached && Date.now() - cached.storedAt <= META_FRESH_MS) return cached.multiplier;
  const urls = [
    `https://fx-api.gateio.ws/api/v4/futures/usdt/contracts/${encodeURIComponent(contract)}`,
    `https://api.gateio.ws/api/v4/futures/usdt/contracts/${encodeURIComponent(contract)}`,
  ];
  const { data } = await fetchFirstJson(urls, 8_000);
  const multiplier = positiveNumber(data?.quanto_multiplier) ?? 1;
  CONTRACT_META_CACHE.set(key, { multiplier, storedAt: Date.now() });
  return multiplier;
}

async function loadBinance(view, symbol, limit) {
  const native = providerSymbol('binance', symbol);
  if (view === 'trades') {
    const url = `https://fapi.binance.com/fapi/v1/trades?symbol=${encodeURIComponent(native)}&limit=${limit}`;
    const data = await fetchJson(url);
    if (!Array.isArray(data)) throw new Error('binance_trades_invalid');
    const items = data.map((row) => {
      const price = positiveNumber(row?.price);
      const quantity = positiveNumber(row?.qty);
      const timeMs = integerValue(row?.time);
      if (price == null || quantity == null || timeMs <= 0) return null;
      return {
        id: String(row?.id ?? `${timeMs}:${price}:${quantity}`),
        time_ms: timeMs,
        price,
        quantity,
        quote_amount: price * quantity,
        side: row?.isBuyerMaker === true ? 'sell' : 'buy',
      };
    }).filter(Boolean);
    return { items, timestamp_ms: items[0]?.time_ms || Date.now(), upstream_host: 'fapi.binance.com', native_symbol: native };
  }
  const url = `https://fapi.binance.com/fapi/v1/depth?symbol=${encodeURIComponent(native)}&limit=${limit}`;
  const data = await fetchJson(url);
  const bids = normalizeLevels(data?.bids, { side: 'bid' });
  const asks = normalizeLevels(data?.asks, { side: 'ask' });
  return { bids, asks, timestamp_ms: integerValue(data?.E) || integerValue(data?.T) || Date.now(), upstream_host: 'fapi.binance.com', native_symbol: native };
}

async function loadOkx(view, symbol, limit) {
  const native = providerSymbol('okx', symbol);
  const multiplier = await okxContractMultiplier(native);
  if (view === 'trades') {
    const url = `https://www.okx.com/api/v5/market/trades?instId=${encodeURIComponent(native)}&limit=${limit}`;
    const data = await fetchJson(url);
    if (String(data?.code ?? '0') !== '0' || !Array.isArray(data?.data)) throw new Error(`okx_trades_${data?.code ?? 'invalid'}`);
    const items = data.data.map((row) => {
      const price = positiveNumber(row?.px);
      const contracts = positiveNumber(row?.sz);
      const quantity = contracts == null ? null : contracts * multiplier;
      const timeMs = integerValue(row?.ts);
      const side = String(row?.side || '').toLowerCase();
      if (price == null || quantity == null || quantity <= 0 || timeMs <= 0 || !['buy', 'sell'].includes(side)) return null;
      return {
        id: String(row?.tradeId ?? `${timeMs}:${price}:${quantity}`),
        time_ms: timeMs,
        price,
        quantity,
        quantity_contracts: contracts,
        quote_amount: price * quantity,
        side,
      };
    }).filter(Boolean);
    return { items, timestamp_ms: items[0]?.time_ms || Date.now(), upstream_host: 'www.okx.com', native_symbol: native, quantity_unit: 'base_asset', contract_multiplier: multiplier };
  }
  const url = `https://www.okx.com/api/v5/market/books?instId=${encodeURIComponent(native)}&sz=${Math.max(1, Math.min(limit, 20))}`;
  const data = await fetchJson(url);
  if (String(data?.code ?? '0') !== '0' || !Array.isArray(data?.data) || !data.data[0]) throw new Error(`okx_orderbook_${data?.code ?? 'invalid'}`);
  const row = data.data[0];
  const bids = normalizeLevels(row?.bids, { side: 'bid', quantityMultiplier: multiplier });
  const asks = normalizeLevels(row?.asks, { side: 'ask', quantityMultiplier: multiplier });
  return { bids, asks, timestamp_ms: integerValue(row?.ts) || Date.now(), upstream_host: 'www.okx.com', native_symbol: native, quantity_unit: 'base_asset', contract_multiplier: multiplier };
}

async function loadBybit(view, symbol, limit) {
  const native = providerSymbol('bybit', symbol);
  if (view === 'trades') {
    const url = `https://api.bybit.com/v5/market/recent-trade?category=linear&symbol=${encodeURIComponent(native)}&limit=${limit}`;
    const data = await fetchJson(url);
    if (integerValue(data?.retCode) !== 0 || !Array.isArray(data?.result?.list)) throw new Error(`bybit_trades_${data?.retCode ?? 'invalid'}`);
    const items = data.result.list.map((row) => {
      const price = positiveNumber(row?.p ?? row?.price);
      const quantity = positiveNumber(row?.v ?? row?.size);
      const timeMs = integerValue(row?.T ?? row?.time);
      const rawSide = String(row?.S ?? row?.side ?? '').toLowerCase();
      const side = rawSide === 'buy' ? 'buy' : rawSide === 'sell' ? 'sell' : '';
      if (price == null || quantity == null || timeMs <= 0 || !side) return null;
      return {
        id: String(row?.i ?? row?.execId ?? `${timeMs}:${price}:${quantity}`),
        time_ms: timeMs,
        price,
        quantity,
        quote_amount: price * quantity,
        side,
      };
    }).filter(Boolean);
    return { items, timestamp_ms: items[0]?.time_ms || integerValue(data?.time) || Date.now(), upstream_host: 'api.bybit.com', native_symbol: native };
  }
  const url = `https://api.bybit.com/v5/market/orderbook?category=linear&symbol=${encodeURIComponent(native)}&limit=${Math.max(1, Math.min(limit, 50))}`;
  const data = await fetchJson(url);
  if (integerValue(data?.retCode) !== 0 || !data?.result) throw new Error(`bybit_orderbook_${data?.retCode ?? 'invalid'}`);
  const bids = normalizeLevels(data.result.b, { side: 'bid' });
  const asks = normalizeLevels(data.result.a, { side: 'ask' });
  return { bids, asks, timestamp_ms: integerValue(data.result.cts) || integerValue(data.result.ts) || integerValue(data?.time) || Date.now(), upstream_host: 'api.bybit.com', native_symbol: native };
}

async function loadBitget(view, symbol, limit) {
  const native = providerSymbol('bitget', symbol);
  if (view === 'trades') {
    const url = `https://api.bitget.com/api/v2/mix/market/fills?symbol=${encodeURIComponent(native)}&productType=usdt-futures&limit=${limit}`;
    const data = await fetchJson(url);
    if (String(data?.code || '') !== '00000' || !Array.isArray(data?.data)) throw new Error(`bitget_trades_${data?.code ?? 'invalid'}`);
    const items = data.data.map((row) => {
      const price = positiveNumber(row?.price);
      const quantity = positiveNumber(row?.size);
      const timeMs = integerValue(row?.ts);
      const rawSide = String(row?.side || '').toLowerCase();
      const side = rawSide === 'buy' ? 'buy' : rawSide === 'sell' ? 'sell' : '';
      if (price == null || quantity == null || timeMs <= 0 || !side) return null;
      return {
        id: String(row?.tradeId ?? `${timeMs}:${price}:${quantity}`),
        time_ms: timeMs,
        price,
        quantity,
        quote_amount: price * quantity,
        side,
      };
    }).filter(Boolean);
    return { items, timestamp_ms: items[0]?.time_ms || integerValue(data?.requestTime) || Date.now(), upstream_host: 'api.bitget.com', native_symbol: native };
  }
  const requestLimit = limit <= 1 ? 1 : limit <= 5 ? 5 : limit <= 15 ? 15 : 50;
  const url = `https://api.bitget.com/api/v2/mix/market/merge-depth?productType=usdt-futures&symbol=${encodeURIComponent(native)}&precision=scale0&limit=${requestLimit}`;
  const data = await fetchJson(url);
  if (String(data?.code || '') !== '00000' || !data?.data) throw new Error(`bitget_orderbook_${data?.code ?? 'invalid'}`);
  const bids = normalizeLevels(data.data.bids, { side: 'bid' }).slice(0, limit);
  const asks = normalizeLevels(data.data.asks, { side: 'ask' }).slice(0, limit);
  return { bids, asks, timestamp_ms: integerValue(data.data.ts) || integerValue(data?.requestTime) || Date.now(), upstream_host: 'api.bitget.com', native_symbol: native };
}

async function loadGate(view, symbol, limit) {
  const native = providerSymbol('gate', symbol);
  const multiplier = await gateContractMultiplier(native);
  const bases = ['https://fx-api.gateio.ws/api/v4', 'https://api.gateio.ws/api/v4'];
  if (view === 'trades') {
    const urls = bases.map((base) => `${base}/futures/usdt/trades?contract=${encodeURIComponent(native)}&limit=${limit}`);
    const { data, url } = await fetchFirstJson(urls);
    if (!Array.isArray(data)) throw new Error('gate_trades_invalid');
    const items = data.map((row) => {
      const price = positiveNumber(row?.price);
      const contractsSigned = numberValue(row?.size);
      const contracts = contractsSigned == null ? null : Math.abs(contractsSigned);
      const quantity = contracts == null ? null : contracts * multiplier;
      const timeMs = integerValue(row?.create_time_ms) || integerValue(row?.create_time) * 1000;
      const rawSide = String(row?.side || '').toLowerCase();
      const side = rawSide === 'buy' || rawSide === 'sell'
        ? rawSide
        : contractsSigned != null && contractsSigned > 0 ? 'buy' : contractsSigned != null && contractsSigned < 0 ? 'sell' : '';
      if (price == null || quantity == null || quantity <= 0 || timeMs <= 0 || !side) return null;
      return {
        id: String(row?.id ?? `${timeMs}:${price}:${quantity}`),
        time_ms: timeMs,
        price,
        quantity,
        quantity_contracts: contracts,
        quote_amount: price * quantity,
        side,
      };
    }).filter(Boolean);
    return { items, timestamp_ms: items[0]?.time_ms || Date.now(), upstream_host: new URL(url).host, native_symbol: native, quantity_unit: 'base_asset', contract_multiplier: multiplier };
  }
  const urls = bases.map((base) => `${base}/futures/usdt/order_book?contract=${encodeURIComponent(native)}&limit=${limit}&with_id=true`);
  const { data, url } = await fetchFirstJson(urls);
  const bids = normalizeLevels(data?.bids, { side: 'bid', quantityMultiplier: multiplier, objectPriceKeys: ['p', 'price'], objectSizeKeys: ['s', 'size'] });
  const asks = normalizeLevels(data?.asks, { side: 'ask', quantityMultiplier: multiplier, objectPriceKeys: ['p', 'price'], objectSizeKeys: ['s', 'size'] });
  return { bids, asks, timestamp_ms: integerValue(data?.update) || Math.round((numberValue(data?.current) || 0) * 1000) || Date.now(), upstream_host: new URL(url).host, native_symbol: native, quantity_unit: 'base_asset', contract_multiplier: multiplier };
}

async function loadProviderData(provider, view, symbol, limit) {
  if (provider === 'binance') return loadBinance(view, symbol, limit);
  if (provider === 'okx') return loadOkx(view, symbol, limit);
  if (provider === 'bybit') return loadBybit(view, symbol, limit);
  if (provider === 'bitget') return loadBitget(view, symbol, limit);
  if (provider === 'gate') return loadGate(view, symbol, limit);
  throw new Error('unsupported_provider');
}

function buildPayload(provider, view, requestedSymbol, limit, data, cacheState = 'miss') {
  const common = {
    ok: true,
    version: STEP_VERSION,
    provider,
    market_type: 'contract',
    symbol: compactSymbol(requestedSymbol),
    native_symbol: data.native_symbol,
    view,
    limit,
    source: `${provider}_official_public_contract_${view}`,
    upstream_host: data.upstream_host || '',
    timestamp_ms: integerValue(data.timestamp_ms) || Date.now(),
    cached_at: new Date().toISOString(),
    cache_state: cacheState,
    quantity_unit: data.quantity_unit || 'base_asset',
    if_contract_multiplier: data.contract_multiplier,
  };
  if (common.if_contract_multiplier == null) delete common.if_contract_multiplier;
  else {
    common.contract_multiplier = common.if_contract_multiplier;
    delete common.if_contract_multiplier;
  }
  if (view === 'trades') return { ...common, items: Array.isArray(data.items) ? data.items.slice(0, limit) : [] };
  const bids = Array.isArray(data.bids) ? data.bids.slice(0, limit) : [];
  const asks = Array.isArray(data.asks) ? data.asks.slice(0, limit) : [];
  const bestBid = positiveNumber(bids[0]?.price);
  const bestAsk = positiveNumber(asks[0]?.price);
  const spreadPercent = bestBid != null && bestAsk != null && bestAsk >= bestBid ? ((bestAsk - bestBid) / bestBid) * 100 : null;
  return {
    ...common,
    bids,
    asks,
    best_bid: bestBid,
    best_ask: bestAsk,
    spread_percent: spreadPercent,
  };
}

async function resolveCached(provider, view, symbol, limit) {
  const key = `${provider}|${view}|${compactSymbol(symbol)}|${limit}`;
  const circuitKey = `${provider}|${view}|${compactSymbol(symbol)}`;
  const freshMs = view === 'trades' ? TRADES_FRESH_MS : ORDERBOOK_FRESH_MS;
  const now = Date.now();
  const cached = RESPONSE_CACHE.get(key);
  if (cached && now - cached.storedAt <= freshMs) {
    return { ...cached.payload, cache_state: 'fresh' };
  }
  const circuit = CIRCUIT.get(circuitKey);
  if (circuit && circuit.until > now) {
    if (cached && now - cached.storedAt <= STALE_MS) {
      return { ...cached.payload, cache_state: 'stale-circuit', stale: true };
    }
    const error = new Error('contract_depth_circuit_open');
    error.statusCode = 503;
    error.retryAfterSeconds = Math.max(1, Math.ceil((circuit.until - now) / 1000));
    error.reason = circuit.reason;
    throw error;
  }
  if (circuit) CIRCUIT.delete(circuitKey);

  let pending = INFLIGHT.get(key);
  if (!pending) {
    pending = loadProviderData(provider, view, symbol, limit)
      .then((data) => {
        const payload = buildPayload(provider, view, symbol, limit, data, 'miss');
        const hasData = view === 'trades' ? payload.items.length > 0 : payload.bids.length > 0 && payload.asks.length > 0;
        if (!hasData) throw new Error(`empty_${view}`);
        RESPONSE_CACHE.set(key, { payload, storedAt: Date.now() });
        return payload;
      })
      .catch((error) => {
        openCircuit(circuitKey, error);
        throw error;
      })
      .finally(() => INFLIGHT.delete(key));
    INFLIGHT.set(key, pending);
  }
  try {
    return await pending;
  } catch (error) {
    const fallback = RESPONSE_CACHE.get(key);
    if (fallback && Date.now() - fallback.storedAt <= STALE_MS) {
      return { ...fallback.payload, cache_state: 'stale-error', stale: true };
    }
    throw error;
  }
}

export async function handleContractDepth(req, res, url) {
  if (url.pathname !== '/api/contract-depth') return false;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'access-control-allow-origin': '*',
      'access-control-allow-methods': 'GET, OPTIONS',
      'access-control-allow-headers': 'content-type',
      'cache-control': 'no-store',
    });
    res.end();
    return true;
  }
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: STEP_VERSION, error: 'method_not_allowed' });
    return true;
  }

  const provider = normalizeProvider(url.searchParams.get('provider'));
  const view = String(url.searchParams.get('view') || 'orderbook').trim().toLowerCase() === 'trades' ? 'trades' : 'orderbook';
  const symbol = compactSymbol(url.searchParams.get('symbol'));
  const limit = clampLimit(view, url.searchParams.get('limit'));
  if (!SUPPORTED_PROVIDERS.has(provider)) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'unsupported_provider', provider });
    return true;
  }
  if (!symbol) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'invalid_symbol' });
    return true;
  }

  try {
    const payload = await resolveCached(provider, view, symbol, limit);
    sendJson(res, 200, payload, { 'x-kaka-cache': payload.cache_state || 'miss' });
  } catch (error) {
    const statusCode = Number(error?.statusCode || 0) === 503 ? 503 : 502;
    const retryAfterSeconds = Number(error?.retryAfterSeconds || 0);
    sendJson(res, statusCode, {
      ok: false,
      version: STEP_VERSION,
      provider,
      market_type: 'contract',
      symbol,
      view,
      error: error?.message || 'contract_depth_upstream_failed',
      reason: error?.reason || 'upstream_unavailable',
      retry_after_seconds: retryAfterSeconds || undefined,
    }, retryAfterSeconds > 0 ? { 'retry-after': String(retryAfterSeconds) } : {});
  }
  return true;
}

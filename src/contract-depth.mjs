// Step656.1: dynamic Binance real quote discovery; common spot quote identities only; Binance contract REST remains disabled.
const STEP_VERSION = '650.8.15.25';
const SUPPORTED_PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
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

const BINANCE_WS_STATES = new Map();
const BINANCE_WS_MAX_SYMBOLS = 24;
const BINANCE_WS_CONNECT_GAP_MS = 2_000;
const BINANCE_WS_MAX_CONNECT_ATTEMPTS_5M = 30;
const BINANCE_WS_CONNECT_ATTEMPTS = [];
let BINANCE_WS_CONNECT_CHAIN = Promise.resolve();
let BINANCE_WS_LAST_CONNECT_AT = 0;
const BINANCE_WS_STATS = {
  capacity_rejections: 0,
  evictions: 0,
  reconnects: 0,
  connect_rate_waits: 0,
  connect_rate_rejections: 0,
};
const BINANCE_WS_IDLE_MS = 75_000;
const BINANCE_WS_ORDERBOOK_STALE_MS = 8_000;
const BINANCE_WS_TRADES_STALE_MS = 12_000;
const BINANCE_WS_START_TIMEOUT_MS = 6_000;
const BINANCE_WS_HOSTS = ['fstream.binance.com'];
let BINANCE_WS_CTOR_PROMISE = null;

// Step652.1C.1.3: Coinbase BTC-USDC is exposed as a USD/USDC unified-book
// alias. The public Advanced Trade level2 feed can normalize the subscribed
// BTC-USDC id to BTC-USD in l2_data events. Accept that official alias and, if
// the direct alias subscription stays silent, reconnect once with BTC-USD.
const COINBASE_L2_STATES = new Map();
const COINBASE_L2_WS_URL = 'wss://advanced-trade-ws.coinbase.com';
const COINBASE_L2_IDLE_MS = 75_000;
const COINBASE_L2_STALE_MS = 15_000;
const COINBASE_L2_START_TIMEOUT_MS = 8_000;
const COINBASE_L2_MAX_SYMBOLS = 12;
const COINBASE_L2_STATS = {
  connections_started: 0,
  snapshots_received: 0,
  updates_received: 0,
  alias_events_accepted: 0,
  alias_fallback_connects: 0,
  heartbeats_subscribed: 0,
  idle_closes: 0,
  capacity_rejections: 0,
};

function coinbaseLevel2Route(native) {
  const requestedNative = String(native || '').trim().toUpperCase();
  const match = requestedNative.match(/^([A-Z0-9]+)-USDC$/);
  const aliasNative = match ? `${match[1]}-USD` : '';
  return {
    requestedNative,
    aliasNative,
    acceptedProductIds: new Set([requestedNative, aliasNative].filter(Boolean)),
    aliasMode: aliasNative ? 'coinbase_usd_usdc_unified' : '',
  };
}

async function resolveWebSocketCtor() {
  if (!BINANCE_WS_CTOR_PROMISE) {
    BINANCE_WS_CTOR_PROMISE = (async () => {
      if (typeof globalThis.WebSocket === 'function') return globalThis.WebSocket;
      try {
        const imported = await import('ws');
        return imported.WebSocket || imported.default;
      } catch (_) {
        throw new Error('binance_websocket_runtime_unavailable');
      }
    })();
  }
  return BINANCE_WS_CTOR_PROMISE;
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

function closeWsQuietly(socket) {
  try {
    if (typeof socket?.terminate === 'function') socket.terminate();
    else if (typeof socket?.close === 'function') socket.close();
  } catch (_) {}
}

async function wsMessageText(eventOrData) {
  const value = eventOrData && (typeof eventOrData === 'object' || typeof eventOrData === 'function') && 'data' in eventOrData
    ? eventOrData.data
    : eventOrData;
  if (typeof value === 'string') return value;
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  if (value instanceof ArrayBuffer) return Buffer.from(value).toString('utf8');
  if (ArrayBuffer.isView(value)) return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString('utf8');
  if (value && typeof value.text === 'function') return await value.text();
  return String(value ?? '');
}

function emptyBinanceConnection() {
  return { socket: null, connecting: null, reconnectTimer: null, reconnectAttempt: 0, hostIndex: 0 };
}

function binanceWsState(symbol) {
  const native = providerSymbol('binance', symbol);
  let state = BINANCE_WS_STATES.get(native);
  if (!state) {
    if (BINANCE_WS_STATES.size >= BINANCE_WS_MAX_SYMBOLS) {
      const now = Date.now();
      for (const [key, candidate] of BINANCE_WS_STATES.entries()) {
        const lastAccess = Math.max(
          Number(candidate.orderbookLastAccessAt || 0),
          Number(candidate.tradesLastAccessAt || 0),
        );
        if (lastAccess === 0 || now - lastAccess > BINANCE_WS_IDLE_MS) {
          candidate.manuallyClosing = true;
          closeBinanceView(candidate, 'orderbook');
          closeBinanceView(candidate, 'trades');
          BINANCE_WS_STATES.delete(key);
          BINANCE_WS_STATS.evictions += 1;
          break;
        }
      }
    }
    if (BINANCE_WS_STATES.size >= BINANCE_WS_MAX_SYMBOLS) {
      BINANCE_WS_STATS.capacity_rejections += 1;
      const error = new Error('binance_depth_ws_capacity_reached');
      error.cooldownMs = 5_000;
      throw error;
    }
    state = {
      native,
      orderbookConnection: emptyBinanceConnection(),
      tradesConnection: emptyBinanceConnection(),
      orderbookLastAccessAt: 0,
      tradesLastAccessAt: 0,
      lastMessageAt: 0,
      orderbook: null,
      trades: [],
      waiters: new Set(),
      manuallyClosing: false,
    };
    BINANCE_WS_STATES.set(native, state);
  }
  return state;
}

function binanceConnection(state, view) {
  return view === 'trades' ? state.tradesConnection : state.orderbookConnection;
}

function touchBinanceView(state, view) {
  if (view === 'trades') state.tradesLastAccessAt = Date.now();
  else state.orderbookLastAccessAt = Date.now();
}

function binanceViewReady(state, view) {
  if (view === 'trades') {
    return state.trades.length > 0 && Date.now() - Number(state.trades[0]?.time_ms || 0) <= BINANCE_WS_TRADES_STALE_MS;
  }
  return Boolean(state.orderbook && Date.now() - Number(state.orderbook.timestamp_ms || 0) <= BINANCE_WS_ORDERBOOK_STALE_MS);
}

function notifyBinanceWaiters(state) {
  for (const waiter of [...state.waiters]) {
    if (!binanceViewReady(state, waiter.view)) continue;
    state.waiters.delete(waiter);
    clearTimeout(waiter.timer);
    waiter.resolve();
  }
}

function rejectBinanceWaiters(state, view, error) {
  for (const waiter of [...state.waiters]) {
    if (waiter.view !== view) continue;
    state.waiters.delete(waiter);
    clearTimeout(waiter.timer);
    waiter.reject(error);
  }
}

function pruneBinanceDepthConnectAttempts() {
  const cutoff = Date.now() - 5 * 60_000;
  while (BINANCE_WS_CONNECT_ATTEMPTS.length && BINANCE_WS_CONNECT_ATTEMPTS[0] < cutoff) {
    BINANCE_WS_CONNECT_ATTEMPTS.shift();
  }
}

async function acquireBinanceDepthConnectSlot() {
  let release;
  const previous = BINANCE_WS_CONNECT_CHAIN;
  BINANCE_WS_CONNECT_CHAIN = new Promise((resolve) => { release = resolve; });
  await previous;
  try {
    pruneBinanceDepthConnectAttempts();
    if (BINANCE_WS_CONNECT_ATTEMPTS.length >= BINANCE_WS_MAX_CONNECT_ATTEMPTS_5M) {
      BINANCE_WS_STATS.connect_rate_rejections += 1;
      const error = new Error('binance_depth_ws_connect_rate_limited');
      error.cooldownMs = 10_000;
      throw error;
    }
    const waitMs = Math.max(0, BINANCE_WS_CONNECT_GAP_MS - (Date.now() - BINANCE_WS_LAST_CONNECT_AT));
    if (waitMs > 0) {
      BINANCE_WS_STATS.connect_rate_waits += 1;
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
    BINANCE_WS_LAST_CONNECT_AT = Date.now();
    BINANCE_WS_CONNECT_ATTEMPTS.push(BINANCE_WS_LAST_CONNECT_AT);
  } finally {
    release();
  }
}

function binanceStreamUrl(state, view, host) {
  const streamSymbol = state.native.toLowerCase();
  // Binance USDⓈ-M split legacy websocket traffic into dedicated categories.
  // Trades are regular market data; depth is high-frequency public data.
  if (view === 'trades') {
    return `wss://${host}/market/stream?streams=${streamSymbol}@aggTrade`;
  }
  return `wss://${host}/public/stream?streams=${streamSymbol}@depth20@100ms`;
}

function scheduleBinanceReconnect(state, view) {
  const connection = binanceConnection(state, view);
  const lastAccessAt = view === 'trades' ? state.tradesLastAccessAt : state.orderbookLastAccessAt;
  if (state.manuallyClosing || connection.reconnectTimer || Date.now() - lastAccessAt > BINANCE_WS_IDLE_MS) return;
  const delay = Math.min(15_000, 800 * (2 ** Math.min(connection.reconnectAttempt, 5)));
  connection.reconnectAttempt += 1;
  BINANCE_WS_STATS.reconnects += 1;
  connection.reconnectTimer = setTimeout(() => {
    connection.reconnectTimer = null;
    ensureBinanceWs(state, view).catch(() => {});
  }, delay);
  connection.reconnectTimer.unref?.();
}

async function handleBinanceWsPayload(state, rawPayload) {
  let decoded;
  try {
    const text = await wsMessageText(rawPayload);
    decoded = JSON.parse(text);
  } catch (_) {
    return;
  }
  const data = decoded?.data ?? decoded;
  const eventType = String(data?.e || '');
  if (eventType === 'depthUpdate' || (Array.isArray(data?.b) && Array.isArray(data?.a))) {
    const bids = normalizeLevels(data?.b ?? data?.bids, { side: 'bid' }).slice(0, 20);
    const asks = normalizeLevels(data?.a ?? data?.asks, { side: 'ask' }).slice(0, 20);
    if (bids.length && asks.length) {
      state.orderbook = {
        bids,
        asks,
        timestamp_ms: integerValue(data?.T) || integerValue(data?.E) || Date.now(),
      };
    }
  } else if (eventType === 'aggTrade') {
    const price = positiveNumber(data?.p);
    const quantity = positiveNumber(data?.q);
    const timeMs = integerValue(data?.T) || integerValue(data?.E);
    if (price != null && quantity != null && timeMs > 0) {
      const item = {
        id: String(data?.a ?? `${timeMs}:${price}:${quantity}`),
        time_ms: timeMs,
        price,
        quantity,
        quote_amount: price * quantity,
        side: data?.m === true ? 'sell' : 'buy',
      };
      if (!state.trades.length || state.trades[0].id !== item.id) {
        state.trades.unshift(item);
        if (state.trades.length > 120) state.trades.length = 120;
      }
    }
  }
  state.lastMessageAt = Date.now();
  notifyBinanceWaiters(state);
}

function openBinanceSocket(WebSocketCtor, state, view, host) {
  return new Promise((resolve, reject) => {
    const url = binanceStreamUrl(state, view, host);
    const socket = new WebSocketCtor(url);
    let settled = false;
    const startupTimer = setTimeout(() => {
      if (settled) return;
      settled = true;
      closeWsQuietly(socket);
      const error = new Error(`binance_websocket_${view}_open_timeout`);
      error.cooldownMs = 5_000;
      reject(error);
    }, BINANCE_WS_START_TIMEOUT_MS);
    startupTimer.unref?.();
    wsListen(socket, 'message', (payload) => {
      handleBinanceWsPayload(state, payload).catch(() => {});
    });
    wsListen(socket, 'open', () => {
      if (settled) return;
      settled = true;
      clearTimeout(startupTimer);
      resolve(socket);
    });
    wsListen(socket, 'error', () => {
      if (settled) return;
      settled = true;
      clearTimeout(startupTimer);
      closeWsQuietly(socket);
      const error = new Error(`binance_websocket_${view}_open_failed`);
      error.cooldownMs = 5_000;
      reject(error);
    });
  });
}

async function ensureBinanceWs(state, view) {
  touchBinanceView(state, view);
  const connection = binanceConnection(state, view);
  if (wsReady(connection.socket)) return;
  if (connection.connecting) return connection.connecting;
  state.manuallyClosing = false;
  connection.connecting = (async () => {
    const WebSocketCtor = await resolveWebSocketCtor();
    let lastError = null;
    for (let offset = 0; offset < BINANCE_WS_HOSTS.length; offset += 1) {
      const index = (connection.hostIndex + offset) % BINANCE_WS_HOSTS.length;
      const host = BINANCE_WS_HOSTS[index];
      try {
        await acquireBinanceDepthConnectSlot();
        const socket = await openBinanceSocket(WebSocketCtor, state, view, host);
        connection.socket = socket;
        connection.hostIndex = index;
        connection.reconnectAttempt = 0;
        wsListen(socket, 'close', () => {
          if (connection.socket === socket) connection.socket = null;
          if (!state.manuallyClosing) scheduleBinanceReconnect(state, view);
        });
        wsListen(socket, 'error', () => {});
        return;
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError || new Error(`binance_websocket_${view}_all_hosts_failed`);
  })().catch((error) => {
    closeWsQuietly(connection.socket);
    connection.socket = null;
    rejectBinanceWaiters(state, view, error);
    throw error;
  }).finally(() => {
    connection.connecting = null;
  });
  return connection.connecting;
}

async function waitForBinanceView(state, view, timeoutMs = BINANCE_WS_START_TIMEOUT_MS) {
  touchBinanceView(state, view);
  if (binanceViewReady(state, view)) return;
  await ensureBinanceWs(state, view);
  await new Promise((resolve, reject) => {
    const waiter = { view, resolve, reject, timer: null };
    waiter.timer = setTimeout(() => {
      state.waiters.delete(waiter);
      // A quiet symbol may legitimately have no aggTrade during the first few
      // seconds. Once the official socket is open, return an empty trade list
      // instead of converting quiet market activity into an App spinner/error.
      if (view === 'trades' && wsReady(binanceConnection(state, view).socket)) {
        resolve();
        return;
      }
      const error = new Error(`binance_websocket_${view}_data_timeout`);
      error.cooldownMs = 5_000;
      reject(error);
    }, view === 'trades' ? Math.min(timeoutMs, 2_800) : timeoutMs);
    waiter.timer.unref?.();
    state.waiters.add(waiter);
    notifyBinanceWaiters(state);
  });
}

function closeBinanceView(state, view) {
  const connection = binanceConnection(state, view);
  if (connection.reconnectTimer) clearTimeout(connection.reconnectTimer);
  connection.reconnectTimer = null;
  closeWsQuietly(connection.socket);
  connection.socket = null;
  rejectBinanceWaiters(state, view, new Error(`binance_websocket_${view}_idle_closed`));
}

const binanceWsCleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [symbol, state] of BINANCE_WS_STATES.entries()) {
    if (state.orderbookLastAccessAt > 0 && now - state.orderbookLastAccessAt > BINANCE_WS_IDLE_MS) {
      closeBinanceView(state, 'orderbook');
      state.orderbookLastAccessAt = 0;
      state.orderbook = null;
    }
    if (state.tradesLastAccessAt > 0 && now - state.tradesLastAccessAt > BINANCE_WS_IDLE_MS) {
      closeBinanceView(state, 'trades');
      state.tradesLastAccessAt = 0;
      state.trades = [];
    }
    if (state.orderbookLastAccessAt === 0 && state.tradesLastAccessAt === 0) {
      state.manuallyClosing = true;
      BINANCE_WS_STATES.delete(symbol);
    }
  }
}, 15_000);
binanceWsCleanupTimer.unref?.();

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
  // Longest quote first so BTCFDUSD is parsed as BTC / FDUSD, never BTCFD / USD.
  for (const quote of ['FDUSD', 'USDT', 'USDC', 'USD1', 'USD', 'BTC', 'BNB', 'ETH', 'EUR', 'GBP', 'JPY', 'TRY', 'BRL', 'AUD', 'CAD']) {
    if (symbol.endsWith(quote) && symbol.length > quote.length) return quote;
  }
  return 'USDT';
}

function baseFromCompact(symbol) {
  const quote = quoteFromCompact(symbol);
  return symbol.endsWith(quote) ? symbol.slice(0, -quote.length) : symbol;
}

function contractQuoteSupported(provider, quote) {
  if (quote === 'USDT') {
    return ['binance', 'okx', 'bybit', 'bitget', 'gate']
      .includes(provider);
  }
  if (quote === 'USDC') {
    return ['binance', 'okx', 'bybit', 'bitget']
      .includes(provider);
  }
  if (quote === 'USD') {
    return ['okx', 'bybit', 'bitget', 'gate']
      .includes(provider);
  }
  return false;
}
function bybitCategory(rawSymbol) {
  return quoteFromCompact(compactSymbol(rawSymbol)) === 'USD'
    ? 'inverse'
    : 'linear';
}
function gateSettle(rawSymbol) {
  return quoteFromCompact(compactSymbol(rawSymbol)) === 'USD'
    ? 'btc'
    : 'usdt';
}

function providerSymbol(provider, rawSymbol, marketType = 'contract') {
  const compact = compactSymbol(rawSymbol);
  const quote = quoteFromCompact(compact);
  const base = baseFromCompact(compact);
  if (!base || !quote) throw new Error('invalid_symbol');
  if (marketType === 'spot') {
    if (provider === 'okx' || provider === 'coinbase') return `${base}-${quote}`;
    if (provider === 'gate') return `${base}_${quote}`;
    return `${base}${quote}`;
  }
  if (!contractQuoteSupported(provider, quote)) {
    const error = new Error('unsupported_native_contract_quote');
    error.statusCode = 400;
    throw error;
  }
  if ((provider === 'bybit' || provider === 'bitget') &&
      quote === 'USDC') {
    return `${base}PERP`;
  }
  if ((provider === 'bybit' || provider === 'bitget') &&
      quote === 'USD') {
    return `${base}USD`;
  }
  if (provider === 'okx') return `${base}-${quote}-SWAP`;
  if (provider === 'gate') return `${base}_${quote}`;
  return `${base}${quote}`;
}

function bitgetProductType(rawSymbol) {
  const quote = quoteFromCompact(compactSymbol(rawSymbol));
  if (quote === 'USDC') return 'usdc-futures';
  if (quote === 'USD') return 'coin-futures';
  return 'usdt-futures';
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
  return statusCode === 403 || statusCode === 418 || statusCode === 429 || statusCode === 451 ||
    lower.includes('too many requests') ||
    (lower.includes('ip(') && lower.includes('banned')) ||
    lower.includes('restricted location') ||
    lower.includes('waf') ||
    lower.includes('cloudfront');
}

function openCircuit(key, error) {
  const statusCode = Number(error?.statusCode || 0);
  const restricted = restrictedFailure(statusCode, error?.bodyText || error?.message || '');
  const explicitCooldownMs = Number(error?.cooldownMs || 0);
  const durationMs = explicitCooldownMs > 0 ? explicitCooldownMs : restricted ? RESTRICTED_COOLDOWN_MS : TRANSIENT_COOLDOWN_MS;
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

function normalizeLevels(
  source,
  {
    side,
    quantityMultiplier = 1,
    quantityFromContracts = null,
    quantityUnit = 'base_asset',
  } = {},
) {
  const rows = Array.isArray(source) ? source : [];
  const result = [];
  for (const raw of rows) {
    let rawPrice;
    let rawSize;
    if (Array.isArray(raw)) {
      rawPrice = raw[0];
      rawSize = raw[1];
    } else if (raw && typeof raw === 'object') {
      rawPrice = ['price', 'p', 'px']
        .map((key) => raw[key])
        .find((value) => value != null);
      rawSize = ['quantity', 'size', 'amount', 'q', 'sz']
        .map((key) => raw[key])
        .find((value) => value != null);
    }
    const price = positiveNumber(rawPrice);
    const size = numberValue(rawSize);
    if (price == null || size == null || size === 0) continue;
    const contracts = Math.abs(size);
    const quantity = typeof quantityFromContracts === 'function'
      ? quantityFromContracts(contracts, price)
      : contracts * quantityMultiplier;
    if (!Number.isFinite(quantity) || quantity <= 0) continue;
    const quoteAmount = quantityUnit === 'base_asset'
      ? price * quantity
      : null;
    result.push({
      price,
      quantity,
      quote_amount: quoteAmount,
      quantity_contracts:
          quantityUnit === 'contracts' ||
          quantityMultiplier !== 1 ||
          typeof quantityFromContracts === 'function'
              ? contracts
              : undefined,
      quantity_unit: quantityUnit,
    });
  }
  result.sort((a, b) =>
    side === 'bid' ? b.price - a.price : a.price - b.price,
  );
  return result.map((row) => {
    const copy = { ...row };
    if (copy.quantity_contracts == null) {
      delete copy.quantity_contracts;
    }
    if (copy.quote_amount == null) delete copy.quote_amount;
    return copy;
  });
}

async function okxContractMeta(instId) {
  const key = `okx:${instId}`;
  const cached = CONTRACT_META_CACHE.get(key);
  if (cached && Date.now() - cached.storedAt <= META_FRESH_MS) {
    return cached.meta;
  }
  const url =
      `https://www.okx.com/api/v5/public/instruments` +
      `?instType=SWAP&instId=${encodeURIComponent(instId)}`;
  const decoded = await fetchJson(url, 8_000);
  const row =
      Array.isArray(decoded?.data) ? decoded.data[0] : null;
  const ctVal = positiveNumber(row?.ctVal);
  const ctMult = positiveNumber(row?.ctMult) ?? 1;
  const valueCurrency =
      String(row?.ctValCcy || '').toUpperCase();
  const parts = String(instId).toUpperCase().split('-');
  const base = parts[0] || '';
  const quote = parts[1] || '';
  const contractValue =
      ctVal == null ? null : ctVal * ctMult;
  const meta = {
    base,
    quote,
    contract_value: contractValue,
    contract_value_currency: valueCurrency,
    base_multiplier:
        contractValue != null && valueCurrency === base
            ? contractValue
            : null,
    quote_multiplier:
        contractValue != null && valueCurrency === quote
            ? contractValue
            : null,
  };
  CONTRACT_META_CACHE.set(key, {
    meta,
    storedAt: Date.now(),
  });
  return meta;
}

async function gateContractMeta(contract) {
  const key = `gate:${contract}`;
  const cached = CONTRACT_META_CACHE.get(key);
  if (cached && Date.now() - cached.storedAt <= META_FRESH_MS) {
    return cached.meta;
  }
  const settle = gateSettle(contract);
  const urls = [
    `https://fx-api.gateio.ws/api/v4/futures/${settle}/contracts/${encodeURIComponent(contract)}`,
    `https://api.gateio.ws/api/v4/futures/${settle}/contracts/${encodeURIComponent(contract)}`,
  ];
  const { data } = await fetchFirstJson(urls, 8_000);
  const meta = {
    type: String(data?.type || '').toLowerCase(),
    multiplier: positiveNumber(data?.quanto_multiplier),
    settle,
  };
  CONTRACT_META_CACHE.set(key, {
    meta,
    storedAt: Date.now(),
  });
  return meta;
}

async function loadBinance(view, symbol, limit) {
  const state = binanceWsState(symbol);
  await waitForBinanceView(state, view);
  if (view === 'trades') {
    const items = state.trades.slice(0, limit).map((row) => ({ ...row }));
    return {
      items,
      timestamp_ms: items[0]?.time_ms || state.lastMessageAt || Date.now(),
      upstream_host: BINANCE_WS_HOSTS[binanceConnection(state, 'trades').hostIndex] || 'fstream.binance.com',
      native_symbol: state.native,
      transport: 'websocket_market_aggTrade',
      connected: wsReady(binanceConnection(state, 'trades').socket),
    };
  }
  const snapshot = state.orderbook;
  return {
    bids: snapshot?.bids?.slice(0, limit).map((row) => ({ ...row })) || [],
    asks: snapshot?.asks?.slice(0, limit).map((row) => ({ ...row })) || [],
    timestamp_ms: snapshot?.timestamp_ms || state.lastMessageAt || Date.now(),
    upstream_host: BINANCE_WS_HOSTS[binanceConnection(state, 'orderbook').hostIndex] || 'fstream.binance.com',
    native_symbol: state.native,
    transport: 'websocket_public_depth20_100ms',
    connected: wsReady(binanceConnection(state, 'orderbook').socket),
  };
}

async function loadOkx(view, symbol, limit) {
  const native = providerSymbol('okx', symbol);
  const meta = await okxContractMeta(native);
  const convertContracts = (contracts, price) => {
    if (meta.base_multiplier != null) {
      return contracts * meta.base_multiplier;
    }
    if (meta.quote_multiplier != null && price > 0) {
      return contracts * meta.quote_multiplier / price;
    }
    return null;
  };
  if (view === 'trades') {
    const url =
        `https://www.okx.com/api/v5/market/trades` +
        `?instId=${encodeURIComponent(native)}&limit=${limit}`;
    const data = await fetchJson(url);
    if (String(data?.code ?? '0') !== '0' ||
        !Array.isArray(data?.data)) {
      throw new Error(`okx_trades_${data?.code ?? 'invalid'}`);
    }
    const items = data.data.map((row) => {
      const price = positiveNumber(row?.px);
      const contracts = positiveNumber(row?.sz);
      const quantity =
          price == null || contracts == null
              ? null
              : convertContracts(contracts, price);
      const timeMs = integerValue(row?.ts);
      const side = String(row?.side || '').toLowerCase();
      if (price == null ||
          contracts == null ||
          timeMs <= 0 ||
          !['buy', 'sell'].includes(side)) {
        return null;
      }
      if (quantity == null || quantity <= 0) {
        return {
          id: String(row?.tradeId ??
              `${timeMs}:${price}:${contracts}`),
          time_ms: timeMs,
          price,
          quantity: contracts,
          quantity_contracts: contracts,
          quantity_unit: 'contracts',
          side,
        };
      }
      return {
        id: String(row?.tradeId ??
            `${timeMs}:${price}:${quantity}`),
        time_ms: timeMs,
        price,
        quantity,
        quantity_contracts: contracts,
        quantity_unit: 'base_asset',
        quote_amount: price * quantity,
        side,
      };
    }).filter(Boolean);
    const quantityUnit = items.some(
      (row) => row.quantity_unit === 'base_asset',
    ) ? 'base_asset' : 'contracts';
    return {
      items,
      timestamp_ms: items[0]?.time_ms || Date.now(),
      upstream_host: 'www.okx.com',
      native_symbol: native,
      quantity_unit: quantityUnit,
      contract_value: meta.contract_value,
      contract_value_currency:
          meta.contract_value_currency,
    };
  }

  const url =
      `https://www.okx.com/api/v5/market/books` +
      `?instId=${encodeURIComponent(native)}` +
      `&sz=${Math.max(1, Math.min(limit, 20))}`;
  const data = await fetchJson(url);
  if (String(data?.code ?? '0') !== '0' ||
      !Array.isArray(data?.data) ||
      !data.data[0]) {
    throw new Error(`okx_orderbook_${data?.code ?? 'invalid'}`);
  }
  const row = data.data[0];
  const canConvert =
      meta.base_multiplier != null ||
      meta.quote_multiplier != null;
  const bids = normalizeLevels(row?.bids, {
    side: 'bid',
    quantityFromContracts:
        canConvert ? convertContracts : null,
    quantityUnit: canConvert ? 'base_asset' : 'contracts',
  });
  const asks = normalizeLevels(row?.asks, {
    side: 'ask',
    quantityFromContracts:
        canConvert ? convertContracts : null,
    quantityUnit: canConvert ? 'base_asset' : 'contracts',
  });
  return {
    bids,
    asks,
    timestamp_ms: integerValue(row?.ts) || Date.now(),
    upstream_host: 'www.okx.com',
    native_symbol: native,
    quantity_unit: canConvert ? 'base_asset' : 'contracts',
    contract_value: meta.contract_value,
    contract_value_currency:
        meta.contract_value_currency,
  };
}

async function loadBybit(view, symbol, limit) {
  const native = providerSymbol('bybit', symbol);
  const category = bybitCategory(symbol);
  if (view === 'trades') {
    const url =
        `https://api.bybit.com/v5/market/recent-trade` +
        `?category=${category}` +
        `&symbol=${encodeURIComponent(native)}` +
        `&limit=${limit}`;
    const data = await fetchJson(url);
    if (integerValue(data?.retCode) !== 0 ||
        !Array.isArray(data?.result?.list)) {
      throw new Error(
        `bybit_trades_${data?.retCode ?? 'invalid'}`,
      );
    }
    const items = data.result.list.map((row) => {
      const price = positiveNumber(row?.p ?? row?.price);
      const rawQuantity = positiveNumber(row?.v ?? row?.size);
      const quantity =
          category === 'inverse' &&
          price != null &&
          rawQuantity != null
              ? rawQuantity / price
              : rawQuantity;
      const timeMs = integerValue(row?.T ?? row?.time);
      const rawSide =
          String(row?.S ?? row?.side ?? '').toLowerCase();
      const side =
          rawSide === 'buy'
              ? 'buy'
              : rawSide === 'sell'
                  ? 'sell'
                  : '';
      if (price == null ||
          quantity == null ||
          timeMs <= 0 ||
          !side) {
        return null;
      }
      return {
        id: String(row?.i ?? row?.execId ??
            `${timeMs}:${price}:${quantity}`),
        time_ms: timeMs,
        price,
        quantity,
        quantity_unit: 'base_asset',
        quote_amount:
            category === 'inverse'
                ? rawQuantity
                : price * quantity,
        side,
      };
    }).filter(Boolean);
    return {
      items,
      timestamp_ms:
          items[0]?.time_ms ||
          integerValue(data?.time) ||
          Date.now(),
      upstream_host: 'api.bybit.com',
      native_symbol: native,
      quantity_unit: 'base_asset',
    };
  }

  const url =
      `https://api.bybit.com/v5/market/orderbook` +
      `?category=${category}` +
      `&symbol=${encodeURIComponent(native)}` +
      `&limit=${Math.max(1, Math.min(limit, 50))}`;
  const data = await fetchJson(url);
  if (integerValue(data?.retCode) !== 0 || !data?.result) {
    throw new Error(
      `bybit_orderbook_${data?.retCode ?? 'invalid'}`,
    );
  }
  const converter = category === 'inverse'
      ? (contracts, price) => contracts / price
      : null;
  const bids = normalizeLevels(data.result.b, {
    side: 'bid',
    quantityFromContracts: converter,
    quantityUnit: 'base_asset',
  });
  const asks = normalizeLevels(data.result.a, {
    side: 'ask',
    quantityFromContracts: converter,
    quantityUnit: 'base_asset',
  });
  return {
    bids,
    asks,
    timestamp_ms:
        integerValue(data.result.cts) ||
        integerValue(data.result.ts) ||
        integerValue(data?.time) ||
        Date.now(),
    upstream_host: 'api.bybit.com',
    native_symbol: native,
    quantity_unit: 'base_asset',
  };
}

async function loadBitget(view, symbol, limit) {
  const native = providerSymbol('bitget', symbol);
  if (view === 'trades') {
    const url = `https://api.bitget.com/api/v2/mix/market/fills?symbol=${encodeURIComponent(native)}&productType=${encodeURIComponent(bitgetProductType(symbol))}&limit=${limit}`;
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
  const url = `https://api.bitget.com/api/v2/mix/market/merge-depth?productType=${encodeURIComponent(bitgetProductType(symbol))}&symbol=${encodeURIComponent(native)}&precision=scale0&limit=${requestLimit}`;
  const data = await fetchJson(url);
  if (String(data?.code || '') !== '00000' || !data?.data) throw new Error(`bitget_orderbook_${data?.code ?? 'invalid'}`);
  const bids = normalizeLevels(data.data.bids, { side: 'bid' }).slice(0, limit);
  const asks = normalizeLevels(data.data.asks, { side: 'ask' }).slice(0, limit);
  return { bids, asks, timestamp_ms: integerValue(data.data.ts) || integerValue(data?.requestTime) || Date.now(), upstream_host: 'api.bitget.com', native_symbol: native };
}

async function loadGate(view, symbol, limit) {
  const native = providerSymbol('gate', symbol);
  const settle = gateSettle(symbol);
  const meta = await gateContractMeta(native);
  const bases = [
    'https://fx-api.gateio.ws/api/v4',
    'https://api.gateio.ws/api/v4',
  ];
  if (view === 'trades') {
    const urls = bases.map((base) =>
      `${base}/futures/${settle}/trades` +
      `?contract=${encodeURIComponent(native)}` +
      `&limit=${limit}`,
    );
    const { data, url } = await fetchFirstJson(urls);
    if (!Array.isArray(data)) throw new Error('gate_trades_invalid');
    const items = data.map((row) => {
      const price = positiveNumber(row?.price);
      const contracts = positiveNumber(
        row?.size ?? row?.amount,
      );
      const timeMs =
          integerValue(row?.create_time_ms) ||
          integerValue(row?.time_ms) ||
          integerValue(row?.create_time) * 1000 ||
          integerValue(row?.time) * 1000;
      const rawSize = numberValue(row?.size ?? row?.amount);
      const side =
          rawSize != null
              ? (rawSize >= 0 ? 'buy' : 'sell')
              : String(row?.side || '').toLowerCase();
      if (price == null ||
          contracts == null ||
          timeMs <= 0 ||
          !['buy', 'sell'].includes(side)) {
        return null;
      }
      if (settle === 'btc') {
        return {
          id: String(row?.id ??
              `${timeMs}:${price}:${contracts}`),
          time_ms: timeMs,
          price,
          quantity: contracts,
          quantity_contracts: contracts,
          quantity_unit: 'contracts',
          side,
        };
      }
      const multiplier = meta.multiplier ?? 1;
      const quantity = contracts * multiplier;
      return {
        id: String(row?.id ??
            `${timeMs}:${price}:${quantity}`),
        time_ms: timeMs,
        price,
        quantity,
        quantity_contracts: contracts,
        quantity_unit: 'base_asset',
        quote_amount: price * quantity,
        side,
      };
    }).filter(Boolean);
    return {
      items,
      timestamp_ms: items[0]?.time_ms || Date.now(),
      upstream_host: new URL(url).host,
      native_symbol: native,
      quantity_unit:
          settle === 'btc' ? 'contracts' : 'base_asset',
      contract_multiplier: meta.multiplier,
    };
  }

  const urls = bases.map((base) =>
    `${base}/futures/${settle}/order_book` +
    `?contract=${encodeURIComponent(native)}` +
    `&limit=${limit}&with_id=true`,
  );
  const { data, url } = await fetchFirstJson(urls);
  const quantityUnit =
      settle === 'btc' ? 'contracts' : 'base_asset';
  const multiplier =
      quantityUnit === 'base_asset'
          ? (meta.multiplier ?? 1)
          : 1;
  const bids = normalizeLevels(data?.bids, {
    side: 'bid',
    quantityMultiplier: multiplier,
    quantityUnit,
  });
  const asks = normalizeLevels(data?.asks, {
    side: 'ask',
    quantityMultiplier: multiplier,
    quantityUnit,
  });
  return {
    bids,
    asks,
    timestamp_ms:
        integerValue(data?.update) ||
        integerValue(data?.current) ||
        Date.now(),
    upstream_host: new URL(url).host,
    native_symbol: native,
    quantity_unit: quantityUnit,
    contract_multiplier: meta.multiplier,
  };
}

async function loadSpotOkx(view, symbol, limit) {
  const native = providerSymbol('okx', symbol, 'spot');
  if (view === 'trades') {
    const data = await fetchJson(`https://www.okx.com/api/v5/market/trades?instId=${encodeURIComponent(native)}&limit=${Math.min(limit, 100)}`);
    if (String(data?.code ?? '0') !== '0' || !Array.isArray(data?.data)) throw new Error(`okx_spot_trades_${data?.code ?? 'invalid'}`);
    const items = data.data.map((row) => {
      const price = positiveNumber(row?.px);
      const quantity = positiveNumber(row?.sz);
      const timeMs = integerValue(row?.ts);
      const side = String(row?.side || '').toLowerCase();
      if (price == null || quantity == null || timeMs <= 0 || !['buy', 'sell'].includes(side)) return null;
      return { id: String(row?.tradeId ?? `${timeMs}:${price}:${quantity}`), time_ms: timeMs, price, quantity, quote_amount: price * quantity, side };
    }).filter(Boolean);
    return { items, timestamp_ms: items[0]?.time_ms || Date.now(), upstream_host: 'www.okx.com', native_symbol: native };
  }
  const data = await fetchJson(`https://www.okx.com/api/v5/market/books?instId=${encodeURIComponent(native)}&sz=${Math.max(1, Math.min(limit, 20))}`);
  if (String(data?.code ?? '0') !== '0' || !Array.isArray(data?.data) || !data.data[0]) throw new Error(`okx_spot_orderbook_${data?.code ?? 'invalid'}`);
  const row = data.data[0];
  return { bids: normalizeLevels(row?.bids, { side: 'bid' }), asks: normalizeLevels(row?.asks, { side: 'ask' }), timestamp_ms: integerValue(row?.ts) || Date.now(), upstream_host: 'www.okx.com', native_symbol: native };
}

async function loadSpotBybit(view, symbol, limit) {
  const native = providerSymbol('bybit', symbol, 'spot');
  if (view === 'trades') {
    const data = await fetchJson(`https://api.bybit.com/v5/market/recent-trade?category=spot&symbol=${encodeURIComponent(native)}&limit=${Math.min(limit, 60)}`);
    if (integerValue(data?.retCode) !== 0 || !Array.isArray(data?.result?.list)) throw new Error(`bybit_spot_trades_${data?.retCode ?? 'invalid'}`);
    const items = data.result.list.map((row) => {
      const price = positiveNumber(row?.p ?? row?.price);
      const quantity = positiveNumber(row?.v ?? row?.size);
      const timeMs = integerValue(row?.T ?? row?.time);
      const rawSide = String(row?.S ?? row?.side ?? '').toLowerCase();
      const side = rawSide === 'buy' ? 'buy' : rawSide === 'sell' ? 'sell' : '';
      if (price == null || quantity == null || timeMs <= 0 || !side) return null;
      return { id: String(row?.i ?? row?.execId ?? `${timeMs}:${price}:${quantity}`), time_ms: timeMs, price, quantity, quote_amount: price * quantity, side };
    }).filter(Boolean);
    return { items, timestamp_ms: items[0]?.time_ms || integerValue(data?.time) || Date.now(), upstream_host: 'api.bybit.com', native_symbol: native };
  }
  const data = await fetchJson(`https://api.bybit.com/v5/market/orderbook?category=spot&symbol=${encodeURIComponent(native)}&limit=${Math.max(1, Math.min(limit, 50))}`);
  if (integerValue(data?.retCode) !== 0 || !data?.result) throw new Error(`bybit_spot_orderbook_${data?.retCode ?? 'invalid'}`);
  return { bids: normalizeLevels(data.result.b, { side: 'bid' }), asks: normalizeLevels(data.result.a, { side: 'ask' }), timestamp_ms: integerValue(data.result.cts) || integerValue(data.result.ts) || integerValue(data?.time) || Date.now(), upstream_host: 'api.bybit.com', native_symbol: native };
}

async function loadSpotBitget(view, symbol, limit) {
  const native = providerSymbol('bitget', symbol, 'spot');
  if (view === 'trades') {
    const data = await fetchJson(`https://api.bitget.com/api/v2/spot/market/fills?symbol=${encodeURIComponent(native)}&limit=${Math.min(limit, 100)}`);
    if (String(data?.code || '') !== '00000' || !Array.isArray(data?.data)) throw new Error(`bitget_spot_trades_${data?.code ?? 'invalid'}`);
    const items = data.data.map((row) => {
      const price = positiveNumber(row?.price);
      const quantity = positiveNumber(row?.size);
      const timeMs = integerValue(row?.ts);
      const rawSide = String(row?.side || '').toLowerCase();
      const side = rawSide === 'buy' ? 'buy' : rawSide === 'sell' ? 'sell' : '';
      if (price == null || quantity == null || timeMs <= 0 || !side) return null;
      return { id: String(row?.tradeId ?? `${timeMs}:${price}:${quantity}`), time_ms: timeMs, price, quantity, quote_amount: price * quantity, side };
    }).filter(Boolean);
    return { items, timestamp_ms: items[0]?.time_ms || integerValue(data?.requestTime) || Date.now(), upstream_host: 'api.bitget.com', native_symbol: native };
  }
  const data = await fetchJson(`https://api.bitget.com/api/v2/spot/market/orderbook?symbol=${encodeURIComponent(native)}&type=step0&limit=${Math.max(1, Math.min(limit, 50))}`);
  if (String(data?.code || '') !== '00000' || !data?.data) throw new Error(`bitget_spot_orderbook_${data?.code ?? 'invalid'}`);
  return { bids: normalizeLevels(data.data.bids, { side: 'bid' }).slice(0, limit), asks: normalizeLevels(data.data.asks, { side: 'ask' }).slice(0, limit), timestamp_ms: integerValue(data.data.ts) || integerValue(data?.requestTime) || Date.now(), upstream_host: 'api.bitget.com', native_symbol: native };
}

async function loadSpotGate(view, symbol, limit) {
  const native = providerSymbol('gate', symbol, 'spot');
  if (view === 'trades') {
    const data = await fetchJson(`https://api.gateio.ws/api/v4/spot/trades?currency_pair=${encodeURIComponent(native)}&limit=${Math.min(limit, 100)}`);
    if (!Array.isArray(data)) throw new Error('gate_spot_trades_invalid');
    const items = data.map((row) => {
      const price = positiveNumber(row?.price);
      const quantity = positiveNumber(row?.amount ?? row?.size);
      const timeMs = integerValue(row?.create_time_ms) || integerValue(row?.time_ms) || integerValue(row?.create_time) * 1000 || integerValue(row?.time) * 1000;
      const rawSide = String(row?.side || '').toLowerCase();
      const side = rawSide === 'buy' ? 'buy' : rawSide === 'sell' ? 'sell' : '';
      if (price == null || quantity == null || timeMs <= 0 || !side) return null;
      return { id: String(row?.id ?? `${timeMs}:${price}:${quantity}`), time_ms: timeMs, price, quantity, quote_amount: price * quantity, side };
    }).filter(Boolean);
    return { items, timestamp_ms: items[0]?.time_ms || Date.now(), upstream_host: 'api.gateio.ws', native_symbol: native };
  }
  const data = await fetchJson(`https://api.gateio.ws/api/v4/spot/order_book?currency_pair=${encodeURIComponent(native)}&limit=${Math.max(1, Math.min(limit, 50))}&with_id=true`);
  return { bids: normalizeLevels(data?.bids, { side: 'bid' }).slice(0, limit), asks: normalizeLevels(data?.asks, { side: 'ask' }).slice(0, limit), timestamp_ms: integerValue(data?.update) || integerValue(data?.current) || Date.now(), upstream_host: 'api.gateio.ws', native_symbol: native };
}


function coinbaseL2State(requestedNative) {
  const route = coinbaseLevel2Route(requestedNative);
  let state = COINBASE_L2_STATES.get(route.requestedNative);
  if (state) return state;
  if (COINBASE_L2_STATES.size >= COINBASE_L2_MAX_SYMBOLS) {
    const candidates = [...COINBASE_L2_STATES.values()].sort((a, b) => a.lastAccessAt - b.lastAccessAt);
    const victim = candidates[0];
    if (victim) {
      closeWsQuietly(victim.socket);
      COINBASE_L2_STATES.delete(victim.requestedNative);
    } else {
      COINBASE_L2_STATS.capacity_rejections += 1;
      throw new Error('coinbase_level2_capacity_reached');
    }
  }
  state = {
    requestedNative: route.requestedNative,
    aliasNative: route.aliasNative,
    aliasMode: route.aliasMode,
    acceptedProductIds: route.acceptedProductIds,
    subscribeNative: route.requestedNative,
    socket: null,
    connecting: null,
    bids: new Map(),
    asks: new Map(),
    timestamp_ms: 0,
    lastAccessAt: Date.now(),
    lastMessageAt: 0,
    lastChannel: '',
    lastProductId: '',
    lastError: '',
    aliasFallbackAttempted: false,
  };
  COINBASE_L2_STATES.set(route.requestedNative, state);
  return state;
}

function applyCoinbaseLevel2Message(state, decoded) {
  state.lastChannel = String(decoded?.channel || '');
  if (state.lastChannel !== 'l2_data' || !Array.isArray(decoded?.events)) return;
  let changed = false;
  for (const event of decoded.events) {
    const productId = String(event?.product_id || '').toUpperCase();
    state.lastProductId = productId;
    if (!state.acceptedProductIds.has(productId)) continue;
    if (productId !== state.requestedNative) COINBASE_L2_STATS.alias_events_accepted += 1;
    const eventType = String(event?.type || '').toLowerCase();
    if (eventType === 'snapshot') {
      state.bids.clear();
      state.asks.clear();
      COINBASE_L2_STATS.snapshots_received += 1;
    } else if (eventType === 'update') {
      COINBASE_L2_STATS.updates_received += 1;
    }
    for (const update of Array.isArray(event?.updates) ? event.updates : []) {
      const price = positiveNumber(update?.price_level);
      const quantity = numberValue(update?.new_quantity);
      const side = String(update?.side || '').toLowerCase();
      const target = side === 'bid' ? state.bids : (side === 'offer' || side === 'ask') ? state.asks : null;
      if (!target || price == null || quantity == null) continue;
      if (quantity <= 0) target.delete(price);
      else target.set(price, quantity);
      changed = true;
    }
  }
  if (changed) {
    state.timestamp_ms = Date.parse(String(decoded?.timestamp || '')) || Date.now();
    state.lastMessageAt = Date.now();
    state.lastError = '';
  }
}

async function ensureCoinbaseLevel2(state) {
  state.lastAccessAt = Date.now();
  if (wsReady(state.socket)) return;
  if (state.connecting) return state.connecting;
  state.connecting = (async () => {
    const WebSocketCtor = await resolveWebSocketCtor();
    await new Promise((resolve, reject) => {
      const socket = new WebSocketCtor(COINBASE_L2_WS_URL);
      state.socket = socket;
      let opened = false;
      let settled = false;
      const finish = (error = null) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (error) reject(error); else resolve();
      };
      const timer = setTimeout(() => {
        closeWsQuietly(socket);
        const error = new Error('coinbase_level2_connect_timeout');
        error.cooldownMs = 5_000;
        state.lastError = error.message;
        finish(error);
      }, 6_000);
      timer.unref?.();
      wsListen(socket, 'open', () => {
        opened = true;
        COINBASE_L2_STATS.connections_started += 1;
        try {
          socket.send(JSON.stringify({
            type: 'subscribe',
            product_ids: [state.subscribeNative],
            channel: 'level2',
          }));
          socket.send(JSON.stringify({
            type: 'subscribe',
            channel: 'heartbeats',
          }));
          COINBASE_L2_STATS.heartbeats_subscribed += 1;
          finish();
        } catch (error) {
          state.lastError = String(error?.message || error || 'coinbase_level2_subscribe_error');
          finish(error);
        }
      });
      wsListen(socket, 'message', async (event) => {
        try {
          const decoded = JSON.parse(await wsMessageText(event));
          if (String(decoded?.type || '').toLowerCase() === 'error' || String(decoded?.channel || '').toLowerCase() === 'error') {
            state.lastError = String(decoded?.message || decoded?.error || 'coinbase_level2_error');
            closeWsQuietly(socket);
            return;
          }
          applyCoinbaseLevel2Message(state, decoded);
        } catch (error) {
          state.lastError = String(error?.message || error || 'coinbase_level2_message_parse_error');
        }
      });
      wsListen(socket, 'error', (error) => {
        state.lastError = String(error?.message || error || 'coinbase_level2_socket_error');
        if (!opened) finish(error instanceof Error ? error : new Error('coinbase_level2_socket_error'));
      });
      wsListen(socket, 'close', () => {
        if (state.socket === socket) state.socket = null;
        if (!opened) finish(new Error('coinbase_level2_closed_before_open'));
      });
    });
  })().finally(() => {
    state.connecting = null;
  });
  return state.connecting;
}

async function waitForCoinbaseLevel2Book(state, timeoutMs) {
  const deadline = Date.now() + Math.max(250, timeoutMs);
  while (Date.now() < deadline) {
    if (state.bids.size > 0 && state.asks.size > 0 && Date.now() - state.lastMessageAt <= COINBASE_L2_STALE_MS) return true;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return state.bids.size > 0 && state.asks.size > 0;
}

async function loadCoinbaseLevel2Book(requestedNative, limit) {
  const state = coinbaseL2State(requestedNative);
  state.lastAccessAt = Date.now();
  await ensureCoinbaseLevel2(state);

  // First allow the requested BTC-USDC subscription to return either BTC-USDC
  // or the normalized BTC-USD alias. If Coinbase stays silent, reconnect once
  // using the official USD/USDC unified-book alias explicitly.
  let ready = await waitForCoinbaseLevel2Book(state, state.aliasNative ? 3_500 : COINBASE_L2_START_TIMEOUT_MS);
  if (!ready && state.aliasNative && !state.aliasFallbackAttempted) {
    state.aliasFallbackAttempted = true;
    state.subscribeNative = state.aliasNative;
    state.lastError = '';
    state.bids.clear();
    state.asks.clear();
    closeWsQuietly(state.socket);
    state.socket = null;
    COINBASE_L2_STATS.alias_fallback_connects += 1;
    await ensureCoinbaseLevel2(state);
    ready = await waitForCoinbaseLevel2Book(state, COINBASE_L2_START_TIMEOUT_MS);
  }

  if (!ready || state.bids.size === 0 || state.asks.size === 0) {
    const detail = [
      state.lastError,
      state.lastChannel ? `channel=${state.lastChannel}` : '',
      state.lastProductId ? `product=${state.lastProductId}` : '',
      state.subscribeNative ? `subscribed=${state.subscribeNative}` : '',
    ].filter(Boolean).join('|');
    const error = new Error(detail ? `coinbase_level2_snapshot_timeout:${detail}` : 'coinbase_level2_snapshot_timeout');
    error.cooldownMs = 5_000;
    throw error;
  }
  const bids = [...state.bids.entries()]
    .map(([price, quantity]) => ({ price, quantity, quote_amount: price * quantity }))
    .sort((a, b) => b.price - a.price)
    .slice(0, limit);
  const asks = [...state.asks.entries()]
    .map(([price, quantity]) => ({ price, quantity, quote_amount: price * quantity }))
    .sort((a, b) => a.price - b.price)
    .slice(0, limit);
  return {
    bids,
    asks,
    timestamp_ms: state.timestamp_ms || state.lastMessageAt || Date.now(),
    upstream_host: 'advanced-trade-ws.coinbase.com',
    native_symbol: state.requestedNative,
    upstream_symbol: state.lastProductId || state.subscribeNative,
    alias_mode: state.aliasMode,
    transport: 'public_level2_websocket',
    connected: wsReady(state.socket),
  };
}

const coinbaseL2CleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [native, state] of COINBASE_L2_STATES.entries()) {
    if (now - state.lastAccessAt <= COINBASE_L2_IDLE_MS) continue;
    closeWsQuietly(state.socket);
    state.socket = null;
    COINBASE_L2_STATES.delete(native);
    COINBASE_L2_STATS.idle_closes += 1;
  }
}, 10_000);
coinbaseL2CleanupTimer.unref?.();

async function loadSpotBinance(view, symbol, limit) {
  const native = providerSymbol('binance', symbol, 'spot');
  if (view === 'trades') {
    // Spot only. This does not touch fapi or any Binance contract REST route.
    const data = await fetchJson(
      `https://data-api.binance.vision/api/v3/aggTrades?symbol=${encodeURIComponent(native)}&limit=${Math.min(limit, 100)}`,
    );
    if (!Array.isArray(data)) throw new Error('binance_spot_trades_invalid');
    const items = data.map((row) => {
      const price = positiveNumber(row?.p);
      const quantity = positiveNumber(row?.q);
      const timeMs = integerValue(row?.T);
      // Binance m=true means buyer is maker, so the taker/aggressor side is sell.
      const side = row?.m === true ? 'sell' : 'buy';
      if (price == null || quantity == null || timeMs <= 0) return null;
      return {
        id: String(row?.a ?? `${timeMs}:${price}:${quantity}`),
        time_ms: timeMs,
        price,
        quantity,
        quote_amount: price * quantity,
        side,
      };
    }).filter(Boolean);
    return {
      items,
      timestamp_ms: items[0]?.time_ms || Date.now(),
      upstream_host: 'data-api.binance.vision',
      native_symbol: native,
      transport: 'rest_public_spot_aggTrades',
    };
  }
  const data = await fetchJson(
    `https://data-api.binance.vision/api/v3/depth?symbol=${encodeURIComponent(native)}&limit=${Math.max(5, Math.min(limit, 20))}`,
  );
  const bids = normalizeLevels(data?.bids, { side: 'bid' }).slice(0, limit);
  const asks = normalizeLevels(data?.asks, { side: 'ask' }).slice(0, limit);
  if (!bids.length || !asks.length) throw new Error('binance_spot_orderbook_empty');
  return {
    bids,
    asks,
    timestamp_ms: Date.now(),
    upstream_host: 'data-api.binance.vision',
    native_symbol: native,
    transport: 'rest_public_spot_depth',
  };
}

async function loadSpotCoinbase(view, symbol, limit) {
  const native = providerSymbol('coinbase', symbol, 'spot');
  if (view === 'trades') {
    const data = await fetchJson(`https://api.exchange.coinbase.com/products/${encodeURIComponent(native)}/trades`);
    if (!Array.isArray(data)) throw new Error('coinbase_spot_trades_invalid');
    const items = data.slice(0, limit).map((row) => {
      const price = positiveNumber(row?.price);
      const quantity = positiveNumber(row?.size);
      const timeMs = Date.parse(String(row?.time || ''));
      const makerSide = String(row?.side || '').toLowerCase();
      const side = makerSide === 'buy' ? 'sell' : makerSide === 'sell' ? 'buy' : '';
      if (price == null || quantity == null || !Number.isFinite(timeMs) || timeMs <= 0 || !side) return null;
      return { id: String(row?.trade_id ?? `${timeMs}:${price}:${quantity}`), time_ms: timeMs, price, quantity, quote_amount: price * quantity, side };
    }).filter(Boolean);
    return { items, timestamp_ms: items[0]?.time_ms || Date.now(), upstream_host: 'api.exchange.coinbase.com', native_symbol: native };
  }
  return await loadCoinbaseLevel2Book(native, limit);
}

async function loadProviderData(provider, marketType, view, symbol, limit) {
  if (marketType === 'spot') {
    if (provider === 'binance') return loadSpotBinance(view, symbol, limit);
    if (provider === 'coinbase') return loadSpotCoinbase(view, symbol, limit);
    if (provider === 'okx') return loadSpotOkx(view, symbol, limit);
    if (provider === 'bybit') return loadSpotBybit(view, symbol, limit);
    if (provider === 'bitget') return loadSpotBitget(view, symbol, limit);
    if (provider === 'gate') return loadSpotGate(view, symbol, limit);
    throw new Error('unsupported_spot_provider');
  }
  if (provider === 'binance') return loadBinance(view, symbol, limit);
  if (provider === 'okx') return loadOkx(view, symbol, limit);
  if (provider === 'bybit') return loadBybit(view, symbol, limit);
  if (provider === 'bitget') return loadBitget(view, symbol, limit);
  if (provider === 'gate') return loadGate(view, symbol, limit);
  throw new Error('unsupported_contract_provider');
}

function buildPayload(provider, marketType, view, requestedSymbol, limit, data, cacheState = 'miss') {
  const common = {
    ok: true,
    version: STEP_VERSION,
    provider,
    market_type: marketType,
    symbol: compactSymbol(requestedSymbol),
    native_symbol: data.native_symbol,
    view,
    limit,
    source: provider === 'binance'
      ? marketType === 'contract'
        ? `binance_official_public_contract_${view}_websocket`
        : `binance_official_public_spot_${view}`
      : `${provider}_official_public_${marketType}_${view}`,
    transport: data.transport || 'rest',
    upstream_host: data.upstream_host || '',
    timestamp_ms: integerValue(data.timestamp_ms) || Date.now(),
    cached_at: new Date().toISOString(),
    cache_state: cacheState,
    quantity_unit: data.quantity_unit || 'base_asset',
    connected: data.connected === true,
    if_contract_multiplier: data.contract_multiplier,
  };
  if (data.upstream_symbol) common.upstream_symbol = String(data.upstream_symbol);
  if (data.alias_mode) common.alias_mode = String(data.alias_mode);
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

async function resolveCached(provider, marketType, view, symbol, limit) {
  const key = `${provider}|${marketType}|${view}|${compactSymbol(symbol)}|${limit}`;
  const circuitKey = `${provider}|${marketType}|${view}|${compactSymbol(symbol)}`;
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
    pending = loadProviderData(provider, marketType, view, symbol, limit)
      .then((data) => {
        const payload = buildPayload(provider, marketType, view, symbol, limit, data, 'miss');
        const hasData = view === 'trades' ? payload.items.length > 0 : payload.bids.length > 0 && payload.asks.length > 0;
        const quietBinanceTradeStream = provider === 'binance' && view === 'trades' && payload.connected === true;
        if (!hasData && !quietBinanceTradeStream) throw new Error(`empty_${view}`);
        if (quietBinanceTradeStream && !hasData) {
          payload.empty_reason = 'no_recent_trade_event';
          payload.partial = true;
        }
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


export function getContractDepthHealth() {
  pruneBinanceDepthConnectAttempts();
  return {
    ok: true,
    version: STEP_VERSION,
    binance_contract_rest_disabled: true,
    binance_spot_depth_transport: 'official_data_api_rest_with_endpoint_cache_inflight_and_circuit',
    fdusd_spot_identity_enabled: true,
    tusd_spot_identity_enabled: true,
    tusd_spot_identity_examples: {
      binance: providerSymbol('binance', 'BTCTUSD', 'spot'),
      coinbase: providerSymbol('coinbase', 'BTCTUSD', 'spot'),
      okx: providerSymbol('okx', 'BTCTUSD', 'spot'),
      bybit: providerSymbol('bybit', 'BTCTUSD', 'spot'),
      bitget: providerSymbol('bitget', 'BTCTUSD', 'spot'),
      gate: providerSymbol('gate', 'BTCTUSD', 'spot'),
    },
    binance_ws_symbols: BINANCE_WS_STATES.size,
    binance_ws_max_symbols: BINANCE_WS_MAX_SYMBOLS,
    binance_ws_connect_gap_ms: BINANCE_WS_CONNECT_GAP_MS,
    binance_ws_connect_attempts_5m: BINANCE_WS_CONNECT_ATTEMPTS.length,
    binance_ws_max_connect_attempts_5m: BINANCE_WS_MAX_CONNECT_ATTEMPTS_5M,
    binance_ws_connections: [...BINANCE_WS_STATES.values()].reduce((sum, state) => {
      return sum +
        (wsReady(state.orderbookConnection?.socket) ? 1 : 0) +
        (wsReady(state.tradesConnection?.socket) ? 1 : 0);
    }, 0),
    coinbase_level2_mode: 'advanced_trade_public_websocket_alias_aware',
    coinbase_level2_symbols: COINBASE_L2_STATES.size,
    coinbase_level2_connections: [...COINBASE_L2_STATES.values()].filter((state) => wsReady(state.socket)).length,
    coinbase_level2_max_symbols: COINBASE_L2_MAX_SYMBOLS,
    coinbase_level2_active_routes: [...COINBASE_L2_STATES.values()].map((state) => ({
      requested_symbol: state.requestedNative,
      subscribed_symbol: state.subscribeNative,
      last_product_id: state.lastProductId,
      last_channel: state.lastChannel,
      last_error: state.lastError,
      bids: state.bids.size,
      asks: state.asks.size,
    })),
    ...COINBASE_L2_STATS,
    ...BINANCE_WS_STATS,
  };
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
  const marketType = String(url.searchParams.get('market_type') || 'contract').trim().toLowerCase() === 'spot' ? 'spot' : 'contract';
  const view = String(url.searchParams.get('view') || 'orderbook').trim().toLowerCase() === 'trades' ? 'trades' : 'orderbook';
  const symbol = compactSymbol(url.searchParams.get('symbol'));
  const limit = clampLimit(view, url.searchParams.get('limit'));
  if (!SUPPORTED_PROVIDERS.has(provider) || (marketType === 'contract' && provider === 'coinbase')) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'unsupported_provider', provider });
    return true;
  }
  if (!symbol) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'invalid_symbol' });
    return true;
  }

  try {
    const payload = await resolveCached(provider, marketType, view, symbol, limit);
    sendJson(res, 200, payload, { 'x-kaka-cache': payload.cache_state || 'miss' });
  } catch (error) {
    const rawStatus = Number(error?.statusCode || 0);
    const statusCode = rawStatus === 400 ? 400 : rawStatus === 503 ? 503 : 502;
    const retryAfterSeconds = Number(error?.retryAfterSeconds || 0);
    sendJson(res, statusCode, {
      ok: false,
      version: STEP_VERSION,
      provider,
      market_type: marketType,
      symbol,
      view,
      error: error?.message || 'contract_depth_upstream_failed',
      reason: error?.reason || 'upstream_unavailable',
      retry_after_seconds: retryAfterSeconds || undefined,
    }, retryAfterSeconds > 0 ? { 'retry-after': String(retryAfterSeconds) } : {});
  }
  return true;
}

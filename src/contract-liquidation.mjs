const STEP_VERSION = '640';
const SUPPORTED_PROVIDERS = new Set(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const FEEDS = new Map();
const META_CACHE = new Map();
const FEED_IDLE_MS = 75_000;
const READY_TIMEOUT_MS = 7_000;
const EVENT_RETENTION_MS = 15 * 60_000;
const MAX_EVENTS_PER_SYMBOL = 240;
const META_FRESH_MS = 6 * 60 * 60_000;
let WS_CTOR_PROMISE = null;

async function resolveWebSocketCtor() {
  if (!WS_CTOR_PROMISE) {
    WS_CTOR_PROMISE = (async () => {
      if (typeof globalThis.WebSocket === 'function') return globalThis.WebSocket;
      const imported = await import('ws');
      return imported.WebSocket || imported.default;
    })();
  }
  return WS_CTOR_PROMISE;
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

function sendWs(socket, payload) {
  if (!wsReady(socket)) return false;
  try {
    socket.send(typeof payload === 'string' ? payload : JSON.stringify(payload));
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

async function fetchJson(url, timeoutMs = 8_000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3-contract-liquidation/640',
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
    return JSON.parse(text);
  } finally {
    clearTimeout(timer);
  }
}

async function fetchFirstJson(urls, timeoutMs = 8_000) {
  let lastError = null;
  for (const url of urls) {
    try {
      return await fetchJson(url, timeoutMs);
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error('all_upstreams_failed');
}

async function okxContractMultiplier(instId) {
  const key = `okx:${instId}`;
  const cached = META_CACHE.get(key);
  if (cached && Date.now() - cached.storedAt <= META_FRESH_MS) return cached.multiplier;
  const decoded = await fetchJson(`https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${encodeURIComponent(instId)}`);
  const row = Array.isArray(decoded?.data) ? decoded.data[0] : null;
  const ctVal = positiveNumber(row?.ctVal) ?? 1;
  const ctMult = positiveNumber(row?.ctMult) ?? 1;
  const multiplier = ctVal * ctMult;
  META_CACHE.set(key, { multiplier, storedAt: Date.now() });
  return multiplier;
}

async function gateContractMultiplier(contract) {
  const key = `gate:${contract}`;
  const cached = META_CACHE.get(key);
  if (cached && Date.now() - cached.storedAt <= META_FRESH_MS) return cached.multiplier;
  const decoded = await fetchFirstJson([
    `https://fx-api.gateio.ws/api/v4/futures/usdt/contracts/${encodeURIComponent(contract)}`,
    `https://api.gateio.ws/api/v4/futures/usdt/contracts/${encodeURIComponent(contract)}`,
  ]);
  const multiplier = positiveNumber(decoded?.quanto_multiplier) ?? 1;
  META_CACHE.set(key, { multiplier, storedAt: Date.now() });
  return multiplier;
}

function feedKey(provider, symbol) {
  if (provider === 'okx' || provider === 'bitget') return `${provider}|all`;
  return `${provider}|${providerSymbol(provider, symbol)}`;
}

function sourceInfo(provider) {
  switch (provider) {
    case 'binance':
      return {
        source: 'binance_official_public_contract_liquidation_websocket',
        transport: 'websocket_market_forceOrder',
        upstream_host: 'fstream.binance.com',
        coverage: 'largest_liquidation_per_symbol_within_1000ms',
      };
    case 'okx':
      return {
        source: 'okx_official_public_contract_liquidation_websocket',
        transport: 'websocket_public_liquidation-orders',
        upstream_host: 'ws.okx.com',
        coverage: 'recent_liquidation_orders_not_total_market_count',
      };
    case 'bybit':
      return {
        source: 'bybit_official_public_contract_liquidation_websocket',
        transport: 'websocket_public_allLiquidation',
        upstream_host: 'stream.bybit.com',
        coverage: 'all_liquidation_stream_500ms',
      };
    case 'bitget':
      return {
        source: 'bitget_official_public_contract_liquidation_websocket',
        transport: 'websocket_public_liquidation',
        upstream_host: 'ws.bitget.com',
        coverage: 'largest_long_and_short_liquidation_per_pair_per_second',
      };
    case 'gate':
      return {
        source: 'gate_official_public_contract_liquidation_websocket',
        transport: 'websocket_public_liquidates',
        upstream_host: 'fx-ws.gateio.ws',
        coverage: 'up_to_one_liquidation_order_per_contract_per_second',
      };
    default:
      return { source: '', transport: '', upstream_host: '', coverage: '' };
  }
}

function createFeed(provider, symbol) {
  const key = feedKey(provider, symbol);
  const info = sourceInfo(provider);
  const feed = {
    key,
    provider,
    requestedNativeSymbol: providerSymbol(provider, symbol),
    socket: null,
    connecting: null,
    reconnectTimer: null,
    reconnectAttempt: 0,
    ready: false,
    manuallyClosing: false,
    openedAt: 0,
    lastMessageAt: 0,
    lastError: '',
    eventsBySymbol: new Map(),
    accessBySymbol: new Map(),
    waiters: new Set(),
    heartbeatTimer: null,
    ...info,
  };
  FEEDS.set(key, feed);
  return feed;
}

function getFeed(provider, symbol) {
  const key = feedKey(provider, symbol);
  return FEEDS.get(key) || createFeed(provider, symbol);
}

function touchFeed(feed, symbol) {
  feed.accessBySymbol.set(compactSymbol(symbol), Date.now());
}

function symbolIsActive(feed, symbol) {
  const compact = compactSymbol(symbol);
  const touchedAt = feed.accessBySymbol.get(compact);
  return touchedAt != null && Date.now() - touchedAt <= FEED_IDLE_MS;
}

function feedHasActiveSymbols(feed) {
  const now = Date.now();
  for (const [symbol, time] of [...feed.accessBySymbol.entries()]) {
    if (now - time <= FEED_IDLE_MS) return true;
    feed.accessBySymbol.delete(symbol);
  }
  return false;
}

function feedEvents(feed, symbol) {
  const compact = compactSymbol(symbol);
  const rows = feed.eventsBySymbol.get(compact);
  return Array.isArray(rows) ? rows : [];
}

function notifyReady(feed) {
  for (const waiter of [...feed.waiters]) {
    feed.waiters.delete(waiter);
    clearTimeout(waiter.timer);
    waiter.resolve();
  }
}

function rejectReady(feed, error) {
  for (const waiter of [...feed.waiters]) {
    feed.waiters.delete(waiter);
    clearTimeout(waiter.timer);
    waiter.reject(error);
  }
}

function trimEvents(rows) {
  const cutoff = Date.now() - EVENT_RETENTION_MS;
  const deduped = [];
  const seen = new Set();
  for (const row of rows) {
    if (!row || integerValue(row.time_ms) < cutoff) continue;
    const id = String(row.id || '');
    if (!id || seen.has(id)) continue;
    seen.add(id);
    deduped.push(row);
    if (deduped.length >= MAX_EVENTS_PER_SYMBOL) break;
  }
  return deduped;
}

function addEvent(feed, event) {
  const symbol = compactSymbol(event?.symbol);
  if ((feed.provider === 'okx' || feed.provider === 'bitget') && !symbolIsActive(feed, symbol)) return;
  const timeMs = integerValue(event?.time_ms);
  const price = positiveNumber(event?.price);
  const notional = positiveNumber(event?.notional);
  const liquidationSide = String(event?.liquidation_side || '').toLowerCase();
  if (!symbol || timeMs <= 0 || price == null || notional == null || !['long', 'short'].includes(liquidationSide)) return;
  const id = String(event.id || `${feed.provider}:${symbol}:${liquidationSide}:${timeMs}:${price}:${notional}`);
  const row = {
    id,
    provider: feed.provider,
    symbol,
    native_symbol: String(event.native_symbol || providerSymbol(feed.provider, symbol)),
    time_ms: timeMs,
    price,
    quantity: positiveNumber(event.quantity),
    quantity_contracts: positiveNumber(event.quantity_contracts),
    notional,
    liquidation_side: liquidationSide,
    order_side: String(event.order_side || '').toLowerCase(),
    price_type: String(event.price_type || ''),
  };
  for (const key of ['quantity', 'quantity_contracts']) {
    if (row[key] == null) delete row[key];
  }
  const rows = [row, ...feedEvents(feed, symbol)];
  rows.sort((a, b) => integerValue(b.time_ms) - integerValue(a.time_ms));
  feed.eventsBySymbol.set(symbol, trimEvents(rows));
  feed.lastMessageAt = Date.now();
}

function websocketUrl(feed) {
  const native = feed.requestedNativeSymbol;
  if (feed.provider === 'binance') {
    return `wss://fstream.binance.com/market/stream?streams=${native.toLowerCase()}@forceOrder`;
  }
  if (feed.provider === 'okx') return 'wss://ws.okx.com:8443/ws/v5/public';
  if (feed.provider === 'bybit') return 'wss://stream.bybit.com/v5/public/linear';
  if (feed.provider === 'bitget') return 'wss://ws.bitget.com/v3/ws/public';
  if (feed.provider === 'gate') return 'wss://fx-ws.gateio.ws/v4/ws/usdt';
  throw new Error('unsupported_provider');
}

function subscribeFeed(feed) {
  const socket = feed.socket;
  if (feed.provider === 'binance') return true;
  if (feed.provider === 'okx') {
    return sendWs(socket, { id: 'kaka640', op: 'subscribe', args: [{ channel: 'liquidation-orders', instType: 'SWAP' }] });
  }
  if (feed.provider === 'bybit') {
    return sendWs(socket, { op: 'subscribe', args: [`allLiquidation.${feed.requestedNativeSymbol}`] });
  }
  if (feed.provider === 'bitget') {
    return sendWs(socket, { op: 'subscribe', args: [{ instType: 'usdt-futures', topic: 'liquidation' }] });
  }
  if (feed.provider === 'gate') {
    return sendWs(socket, {
      time: Math.floor(Date.now() / 1000),
      channel: 'futures.public_liquidates',
      event: 'subscribe',
      payload: [feed.requestedNativeSymbol],
    });
  }
  return false;
}

function startHeartbeat(feed) {
  if (feed.heartbeatTimer) clearInterval(feed.heartbeatTimer);
  if (feed.provider === 'binance') return;
  feed.heartbeatTimer = setInterval(() => {
    if (!wsReady(feed.socket)) return;
    if (feed.provider === 'bybit') sendWs(feed.socket, { op: 'ping' });
    else if (feed.provider === 'gate') {
      sendWs(feed.socket, { time: Math.floor(Date.now() / 1000), channel: 'futures.ping' });
    } else {
      sendWs(feed.socket, 'ping');
    }
  }, 20_000);
  feed.heartbeatTimer.unref?.();
}

function stopHeartbeat(feed) {
  if (feed.heartbeatTimer) clearInterval(feed.heartbeatTimer);
  feed.heartbeatTimer = null;
}

function scheduleReconnect(feed) {
  if (feed.manuallyClosing || feed.reconnectTimer || !feedHasActiveSymbols(feed)) return;
  const delay = Math.min(15_000, 800 * (2 ** Math.min(feed.reconnectAttempt, 5)));
  feed.reconnectAttempt += 1;
  feed.reconnectTimer = setTimeout(() => {
    feed.reconnectTimer = null;
    ensureFeed(feed).catch(() => {});
  }, delay);
  feed.reconnectTimer.unref?.();
}

function closeFeed(feed) {
  feed.manuallyClosing = true;
  if (feed.reconnectTimer) clearTimeout(feed.reconnectTimer);
  feed.reconnectTimer = null;
  stopHeartbeat(feed);
  closeWsQuietly(feed.socket);
  feed.socket = null;
  feed.connecting = null;
  feed.ready = false;
  rejectReady(feed, new Error('feed_closed'));
}

async function handleBinance(feed, data) {
  const event = data?.data ?? data;
  if (String(event?.e || '') !== 'forceOrder' || !event?.o) return;
  if (integerValue(event?.st) === 2) return;
  const order = event.o;
  const symbol = compactSymbol(order?.s);
  const side = String(order?.S || '').toUpperCase();
  const price = positiveNumber(order?.ap) ?? positiveNumber(order?.L) ?? positiveNumber(order?.p);
  const quantity = positiveNumber(order?.z) ?? positiveNumber(order?.l) ?? positiveNumber(order?.q);
  const timeMs = integerValue(order?.T) || integerValue(event?.E) || Date.now();
  if (!symbol || !['BUY', 'SELL'].includes(side) || price == null || quantity == null) return;
  addEvent(feed, {
    id: `binance:${symbol}:${String(order?.i || '')}:${timeMs}:${side}`,
    symbol,
    native_symbol: String(order?.s || symbol),
    time_ms: timeMs,
    price,
    quantity,
    notional: price * quantity,
    liquidation_side: side === 'SELL' ? 'long' : 'short',
    order_side: side.toLowerCase(),
    price_type: positiveNumber(order?.ap) != null ? 'average_execution' : 'order_or_last_fill',
  });
}

async function handleOkx(feed, data) {
  if (String(data?.arg?.channel || '') !== 'liquidation-orders' || !Array.isArray(data?.data)) return;
  for (const group of data.data) {
    const native = String(group?.instId || '');
    const symbol = compactSymbol(native);
    if (!symbol || !Array.isArray(group?.details) || !symbolIsActive(feed, symbol)) continue;
    const multiplier = await okxContractMultiplier(native);
    for (const detail of group.details) {
      const price = positiveNumber(detail?.bkPx);
      const contracts = positiveNumber(detail?.sz);
      const timeMs = integerValue(detail?.ts) || Date.now();
      const posSide = String(detail?.posSide || '').toLowerCase();
      const orderSide = String(detail?.side || '').toLowerCase();
      const liquidationSide = ['long', 'short'].includes(posSide)
        ? posSide
        : orderSide === 'sell'
          ? 'long'
          : orderSide === 'buy'
            ? 'short'
            : '';
      const quantity = contracts == null ? null : contracts * multiplier;
      if (price == null || quantity == null || quantity <= 0 || !liquidationSide) continue;
      addEvent(feed, {
        id: `okx:${native}:${timeMs}:${liquidationSide}:${contracts}`,
        symbol,
        native_symbol: native,
        time_ms: timeMs,
        price,
        quantity,
        quantity_contracts: contracts,
        notional: price * quantity,
        liquidation_side: liquidationSide,
        order_side: orderSide,
        price_type: 'bankruptcy',
      });
    }
  }
}

async function handleBybit(feed, data) {
  if (!String(data?.topic || '').startsWith('allLiquidation.') || !Array.isArray(data?.data)) return;
  for (const row of data.data) {
    const symbol = compactSymbol(row?.s);
    const side = String(row?.S || '').toLowerCase();
    const price = positiveNumber(row?.p);
    const quantity = positiveNumber(row?.v);
    const timeMs = integerValue(row?.T) || integerValue(data?.ts) || Date.now();
    if (!symbol || !['buy', 'sell'].includes(side) || price == null || quantity == null) continue;
    addEvent(feed, {
      id: `bybit:${symbol}:${timeMs}:${side}:${quantity}`,
      symbol,
      native_symbol: String(row?.s || symbol),
      time_ms: timeMs,
      price,
      quantity,
      notional: price * quantity,
      liquidation_side: side === 'buy' ? 'long' : 'short',
      order_side: side,
      price_type: 'bankruptcy',
    });
  }
}

async function handleBitget(feed, data) {
  if (String(data?.arg?.topic || '') !== 'liquidation' || !Array.isArray(data?.data)) return;
  for (const row of data.data) {
    const symbol = compactSymbol(row?.symbol);
    if (!symbolIsActive(feed, symbol)) continue;
    const side = String(row?.side || '').toLowerCase();
    const price = positiveNumber(row?.price);
    const notional = positiveNumber(row?.amount);
    const timeMs = integerValue(row?.ts) || integerValue(data?.ts) || Date.now();
    const quantity = price != null && notional != null ? notional / price : null;
    if (!symbol || !['buy', 'sell'].includes(side) || price == null || notional == null || quantity == null || quantity <= 0) continue;
    addEvent(feed, {
      id: `bitget:${symbol}:${timeMs}:${side}:${notional}`,
      symbol,
      native_symbol: String(row?.symbol || symbol),
      time_ms: timeMs,
      price,
      quantity,
      notional,
      liquidation_side: side === 'buy' ? 'long' : 'short',
      order_side: side,
      price_type: 'liquidation',
    });
  }
}

async function handleGate(feed, data) {
  if (String(data?.channel || '') !== 'futures.public_liquidates' || String(data?.event || '') !== 'update' || !Array.isArray(data?.result)) return;
  for (const row of data.result) {
    const native = String(row?.contract || '');
    const symbol = compactSymbol(native);
    const signedContracts = numberValue(row?.size);
    const contracts = signedContracts == null ? null : Math.abs(signedContracts);
    const price = positiveNumber(row?.price);
    const timeMs = integerValue(row?.time_ms) || integerValue(row?.time) * 1000 || integerValue(data?.time_ms) || Date.now();
    if (!symbol || signedContracts == null || signedContracts === 0 || contracts == null || price == null) continue;
    const multiplier = await gateContractMultiplier(native);
    const quantity = contracts * multiplier;
    if (!Number.isFinite(quantity) || quantity <= 0) continue;
    addEvent(feed, {
      id: `gate:${native}:${timeMs}:${signedContracts}`,
      symbol,
      native_symbol: native,
      time_ms: timeMs,
      price,
      quantity,
      quantity_contracts: contracts,
      notional: price * quantity,
      liquidation_side: signedContracts < 0 ? 'long' : 'short',
      order_side: signedContracts < 0 ? 'sell' : 'buy',
      price_type: 'liquidation_order',
    });
  }
}

async function handlePayload(feed, raw) {
  let data;
  try {
    const text = await wsMessageText(raw);
    if (text === 'pong' || text === 'ping') return;
    data = JSON.parse(text);
  } catch (_) {
    return;
  }
  if (data?.event === 'error' || data?.code && String(data.code) !== '0') {
    feed.lastError = String(data?.msg || data?.ret_msg || data?.code || 'subscription_error');
    feed.ready = false;
    closeWsQuietly(feed.socket);
    return;
  }
  try {
    if (feed.provider === 'binance') await handleBinance(feed, data);
    else if (feed.provider === 'okx') await handleOkx(feed, data);
    else if (feed.provider === 'bybit') await handleBybit(feed, data);
    else if (feed.provider === 'bitget') await handleBitget(feed, data);
    else if (feed.provider === 'gate') await handleGate(feed, data);
  } catch (error) {
    feed.lastError = String(error?.message || error);
  }
  feed.lastMessageAt = Date.now();
}

async function openFeed(feed) {
  const WebSocketCtor = await resolveWebSocketCtor();
  return await new Promise((resolve, reject) => {
    const socket = new WebSocketCtor(websocketUrl(feed));
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      closeWsQuietly(socket);
      reject(new Error('liquidation_websocket_open_timeout'));
    }, READY_TIMEOUT_MS);
    timeout.unref?.();
    wsListen(socket, 'message', (payload) => {
      handlePayload(feed, payload).catch(() => {});
    });
    wsListen(socket, 'open', () => {
      if (settled) return;
      feed.socket = socket;
      const subscribed = subscribeFeed(feed);
      if (!subscribed) {
        settled = true;
        clearTimeout(timeout);
        closeWsQuietly(socket);
        reject(new Error('liquidation_websocket_subscribe_failed'));
        return;
      }
      settled = true;
      clearTimeout(timeout);
      feed.ready = true;
      feed.openedAt = Date.now();
      feed.lastError = '';
      feed.reconnectAttempt = 0;
      startHeartbeat(feed);
      notifyReady(feed);
      resolve();
    });
    wsListen(socket, 'close', () => {
      if (feed.socket === socket) feed.socket = null;
      feed.ready = false;
      stopHeartbeat(feed);
      if (!feed.manuallyClosing) scheduleReconnect(feed);
    });
    wsListen(socket, 'error', () => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      closeWsQuietly(socket);
      reject(new Error('liquidation_websocket_open_failed'));
    });
  });
}

async function ensureFeed(feed) {
  if (wsReady(feed.socket) && feed.ready) return;
  if (feed.connecting) return feed.connecting;
  feed.manuallyClosing = false;
  feed.connecting = openFeed(feed)
    .catch((error) => {
      feed.ready = false;
      feed.lastError = String(error?.message || error);
      closeWsQuietly(feed.socket);
      feed.socket = null;
      rejectReady(feed, error);
      scheduleReconnect(feed);
      throw error;
    })
    .finally(() => {
      feed.connecting = null;
    });
  return feed.connecting;
}

async function waitForReady(feed) {
  if (wsReady(feed.socket) && feed.ready) return;
  const promise = new Promise((resolve, reject) => {
    const waiter = {
      resolve,
      reject,
      timer: setTimeout(() => {
        feed.waiters.delete(waiter);
        reject(new Error('liquidation_feed_ready_timeout'));
      }, READY_TIMEOUT_MS),
    };
    waiter.timer.unref?.();
    feed.waiters.add(waiter);
  });
  ensureFeed(feed).catch(() => {});
  return promise;
}

const cleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [key, feed] of [...FEEDS.entries()]) {
    for (const [symbol, rows] of [...feed.eventsBySymbol.entries()]) {
      const trimmed = trimEvents(rows);
      if (trimmed.length) feed.eventsBySymbol.set(symbol, trimmed);
      else feed.eventsBySymbol.delete(symbol);
    }
    if (!feedHasActiveSymbols(feed)) {
      closeFeed(feed);
      FEEDS.delete(key);
      continue;
    }
    if (!wsReady(feed.socket) && !feed.connecting && !feed.reconnectTimer) {
      scheduleReconnect(feed);
    }
    if (feed.openedAt > 0 && now - feed.openedAt > 23 * 60 * 60_000) {
      closeWsQuietly(feed.socket);
    }
  }
}, 15_000);
cleanupTimer.unref?.();

function clampLimit(value) {
  const parsed = integerValue(value);
  return Math.max(1, Math.min(parsed || 80, 120));
}

export async function handleContractLiquidation(req, res, url) {
  if (url.pathname !== '/api/contract-liquidation') return false;
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
  const symbol = compactSymbol(url.searchParams.get('symbol'));
  const limit = clampLimit(url.searchParams.get('limit'));
  const sinceMs = Math.max(0, integerValue(url.searchParams.get('since_ms')));
  if (!SUPPORTED_PROVIDERS.has(provider)) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'unsupported_provider', provider });
    return true;
  }
  if (!symbol) {
    sendJson(res, 400, { ok: false, version: STEP_VERSION, error: 'invalid_symbol' });
    return true;
  }

  const feed = getFeed(provider, symbol);
  touchFeed(feed, symbol);
  try {
    await waitForReady(feed);
    const items = feedEvents(feed, symbol)
      .filter((row) => sinceMs <= 0 || integerValue(row.time_ms) >= sinceMs)
      .slice(0, limit)
      .map((row) => ({ ...row }));
    sendJson(res, 200, {
      ok: true,
      version: STEP_VERSION,
      provider,
      market_type: 'contract',
      symbol,
      native_symbol: providerSymbol(provider, symbol),
      connected: wsReady(feed.socket) && feed.ready,
      source: feed.source,
      transport: feed.transport,
      upstream_host: feed.upstream_host,
      coverage: feed.coverage,
      session_started_at_ms: feed.openedAt || Date.now(),
      timestamp_ms: items[0]?.time_ms || feed.lastMessageAt || Date.now(),
      last_event_at_ms: items[0]?.time_ms || null,
      items,
    });
  } catch (error) {
    sendJson(res, 502, {
      ok: false,
      version: STEP_VERSION,
      provider,
      market_type: 'contract',
      symbol,
      error: String(error?.message || error),
      reason: feed.lastError || 'upstream_unavailable',
    });
  }
  return true;
}

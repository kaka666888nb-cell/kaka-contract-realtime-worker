import { WebSocket } from 'ws';

const PROVIDER = 'binance';
const MARKET_TYPE = 'contract';
const DEFAULT_QUOTE = 'USDT';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const SNAPSHOT_TABLE = 'app_market_backend_snapshots';
const SNAPSHOT_MIN_UNIVERSE_ROWS = 50;
const SNAPSHOT_MIN_TICKER_ROWS = 50;
const SNAPSHOT_PERSIST_INTERVAL_MS = 30_000;
const REST_REFRESH_INTERVAL_MS = 6 * 60 * 60_000;
const REST_RESTRICTED_COOLDOWN_MS = 30 * 60_000;
const REST_TRANSIENT_COOLDOWN_MS = 90_000;
const WS_RECONNECT_MAX_MS = 60_000;
const WS_STALE_MS = 45_000;
const START_WAIT_MS = 6_500;

const universeBySymbol = new Map();
const tickerBySymbol = new Map();
const connectionState = new Map();
const waiters = new Set();

let started = false;
let restoredAt = 0;
let lastUniverseEventAt = 0;
let lastTickerEventAt = 0;
let lastContractInfoEventAt = 0;
let lastPersistAt = 0;
let dirtyUniverse = false;
let dirtyTickers = false;
let persistTimer = null;
let restRefreshPromise = null;
let restNextAllowedAt = 0;
let restLastSuccessAt = 0;
let restLastError = '';

function compact(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/-SWAP$/i, '')
    .replace(/_UMCBL$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}

function splitQuote(symbol) {
  const normalized = compact(symbol);
  for (const quote of ['FDUSD', 'USDT', 'USDC', 'USD']) {
    if (normalized.endsWith(quote) && normalized.length > quote.length) {
      return [normalized.slice(0, -quote.length), quote];
    }
  }
  return [normalized, DEFAULT_QUOTE];
}

function finite(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function iso(value = Date.now()) {
  return new Date(value).toISOString();
}

function isUsdmPayload(item) {
  const unifiedType = finite(item?.st);
  return unifiedType === null || unifiedType === 1;
}

function normalizedPerpetual(item) {
  if (!item || typeof item !== 'object' || !isUsdmPayload(item)) return null;
  const rawSymbol = String(item.s ?? item.symbol ?? '').trim().toUpperCase();
  const symbol = compact(rawSymbol);
  if (!symbol) return null;
  const rawPair = String(item.ps ?? item.pair ?? rawSymbol).trim().toUpperCase();
  const pair = compact(rawPair);
  // Quarterly/delivery contracts carry a symbol different from the underlying pair.
  if (pair && pair !== symbol) return null;
  const [base, quote] = splitQuote(symbol);
  if (!base || !quote) return null;
  return { symbol, rawSymbol: rawSymbol || symbol, base, quote };
}

function universeRow(identity, source, updatedAt = Date.now()) {
  return {
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    symbol: identity.symbol,
    raw_symbol: identity.rawSymbol,
    base_asset: identity.base,
    quote_asset: identity.quote,
    status: 'TRADING',
    active: true,
    source,
    cached_at: iso(updatedAt),
  };
}

function tickerRow(item, identity, source, updatedAt = Date.now()) {
  const last = finite(item.c ?? item.lastPrice ?? item.last_price ?? item.price);
  const open = finite(item.o ?? item.openPrice ?? item.open_24h);
  let percent = finite(item.P ?? item.priceChangePercent ?? item.price_change_percent_24h);
  if (percent === null && last !== null && open !== null && open !== 0) {
    percent = ((last - open) / open) * 100;
  }
  return {
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    symbol: identity.symbol,
    last_price: last,
    price: last,
    price_change_percent_24h: percent,
    quote_volume_24h: finite(item.q ?? item.quoteVolume ?? item.quote_volume_24h),
    base_volume_24h: finite(item.v ?? item.volume ?? item.base_volume_24h),
    high_24h: finite(item.h ?? item.highPrice ?? item.high_24h),
    low_24h: finite(item.l ?? item.lowPrice ?? item.low_24h),
    funding_rate: finite(item.fundingRate ?? item.funding_rate),
    open_interest: finite(item.openInterest ?? item.open_interest),
    open_interest_value: finite(item.openInterestValue ?? item.open_interest_value),
    source,
    cached_at: iso(updatedAt),
  };
}

function notifyWaiters() {
  if (!waiters.size) return;
  for (const check of [...waiters]) {
    try { check(); } catch (_) {}
  }
}

function upsertUniverse(identity, source, updatedAt = Date.now()) {
  const previous = universeBySymbol.get(identity.symbol);
  const next = universeRow(identity, source, updatedAt);
  universeBySymbol.set(identity.symbol, { ...previous, ...next });
  dirtyUniverse = true;
  notifyWaiters();
}

function upsertTicker(item, identity, source, updatedAt = Date.now()) {
  upsertUniverse(identity, source.replace('ticker', 'market'), updatedAt);
  const previous = tickerBySymbol.get(identity.symbol);
  const next = tickerRow(item, identity, source, updatedAt);
  tickerBySymbol.set(identity.symbol, { ...previous, ...next });
  dirtyTickers = true;
  notifyWaiters();
}

function removeSymbol(symbol) {
  const normalized = compact(symbol);
  if (!normalized) return;
  if (universeBySymbol.delete(normalized)) dirtyUniverse = true;
  if (tickerBySymbol.delete(normalized)) dirtyTickers = true;
}

function parsePayload(raw) {
  try {
    const decoded = JSON.parse(Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw));
    return decoded?.data ?? decoded;
  } catch (_) {
    return null;
  }
}

function handleTickerMessage(raw) {
  const payload = parsePayload(raw);
  const rows = Array.isArray(payload) ? payload : [];
  if (!rows.length) return;
  const now = Date.now();
  let accepted = 0;
  for (const item of rows) {
    const identity = normalizedPerpetual(item);
    if (!identity) continue;
    upsertTicker(item, identity, 'binance_official_public_ticker_websocket', now);
    accepted += 1;
  }
  if (accepted) {
    lastTickerEventAt = now;
    schedulePersist();
  }
}

function handleBookTickerMessage(raw) {
  const payload = parsePayload(raw);
  const rows = Array.isArray(payload) ? payload : [payload];
  const now = Date.now();
  let accepted = 0;
  for (const item of rows) {
    const identity = normalizedPerpetual(item);
    if (!identity) continue;
    upsertUniverse(identity, 'binance_official_public_market_bookticker_websocket', now);
    const bid = finite(item?.b ?? item?.bidPrice);
    const ask = finite(item?.a ?? item?.askPrice);
    if (!tickerBySymbol.has(identity.symbol) && bid !== null && ask !== null && bid > 0 && ask > 0) {
      const mid = (bid + ask) / 2;
      tickerBySymbol.set(identity.symbol, {
        provider: PROVIDER,
        market_type: MARKET_TYPE,
        symbol: identity.symbol,
        last_price: mid,
        price: mid,
        price_change_percent_24h: null,
        quote_volume_24h: null,
        base_volume_24h: null,
        high_24h: null,
        low_24h: null,
        funding_rate: null,
        open_interest: null,
        open_interest_value: null,
        source: 'binance_official_public_bookticker_websocket',
        cached_at: iso(now),
      });
      dirtyTickers = true;
    }
    accepted += 1;
  }
  if (accepted) {
    lastUniverseEventAt = now;
    schedulePersist();
  }
}

function handleContractInfoMessage(raw) {
  const payload = parsePayload(raw);
  const rows = Array.isArray(payload) ? payload : [payload];
  const now = Date.now();
  let accepted = 0;
  for (const item of rows) {
    if (!item || typeof item !== 'object' || !isUsdmPayload(item)) continue;
    const symbol = compact(item.s ?? item.symbol);
    if (!symbol) continue;
    const contractType = String(item.ct ?? item.contractType ?? '').toUpperCase();
    const contractStatus = String(item.cs ?? item.contractStatus ?? item.status ?? '').toUpperCase();
    if (contractType && contractType !== 'PERPETUAL') continue;
    if (contractStatus && !['TRADING', 'PRE_DELIVERING', 'PRE_SETTLE'].includes(contractStatus)) {
      removeSymbol(symbol);
      accepted += 1;
      continue;
    }
    const identity = normalizedPerpetual(item);
    if (!identity) continue;
    upsertUniverse(identity, 'binance_official_public_contract_info_websocket', now);
    accepted += 1;
  }
  if (accepted) {
    lastContractInfoEventAt = now;
    schedulePersist();
  }
}

const STREAMS = {
  ticker: {
    urls: [
      'wss://fstream.binance.com/market/ws/!ticker@arr',
      'wss://stream.binancefuture.com/market/ws/!ticker@arr',
      'wss://fstream.binance.com/ws/!ticker@arr',
      'wss://stream.binancefuture.com/ws/!ticker@arr',
    ],
    handler: handleTickerMessage,
  },
  bookTicker: {
    urls: [
      'wss://fstream.binance.com/public/ws/!bookTicker',
      'wss://stream.binancefuture.com/public/ws/!bookTicker',
      'wss://fstream.binance.com/market/ws/!bookTicker',
      'wss://stream.binancefuture.com/market/ws/!bookTicker',
      'wss://fstream.binance.com/ws/!bookTicker',
      'wss://stream.binancefuture.com/ws/!bookTicker',
    ],
    handler: handleBookTickerMessage,
  },
  contractInfo: {
    urls: [
      'wss://fstream.binance.com/market/ws/!contractInfo',
      'wss://stream.binancefuture.com/market/ws/!contractInfo',
      'wss://fstream.binance.com/ws/!contractInfo',
      'wss://stream.binancefuture.com/ws/!contractInfo',
    ],
    handler: handleContractInfoMessage,
  },
};

function streamStatus(name) {
  let state = connectionState.get(name);
  if (!state) {
    state = {
      connected: false,
      urlIndex: 0,
      attempts: 0,
      reconnectTimer: null,
      socket: null,
      openedAt: 0,
      lastMessageAt: 0,
      lastError: '',
    };
    connectionState.set(name, state);
  }
  return state;
}

function scheduleReconnect(name) {
  const state = streamStatus(name);
  if (state.reconnectTimer) return;
  state.connected = false;
  state.socket = null;
  state.attempts += 1;
  const delay = Math.min(WS_RECONNECT_MAX_MS, 1_000 * (2 ** Math.min(6, state.attempts - 1)));
  state.reconnectTimer = setTimeout(() => {
    state.reconnectTimer = null;
    connectStream(name);
  }, delay);
  state.reconnectTimer.unref?.();
}

function connectStream(name) {
  const spec = STREAMS[name];
  if (!spec) return;
  const state = streamStatus(name);
  if (state.socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(state.socket.readyState)) return;
  const url = spec.urls[state.urlIndex % spec.urls.length];
  state.urlIndex = (state.urlIndex + 1) % spec.urls.length;
  let socket;
  try {
    socket = new WebSocket(url, {
      handshakeTimeout: 15_000,
      perMessageDeflate: false,
      headers: { 'user-agent': 'KakaWeb3-Market-Worker/650' },
    });
  } catch (error) {
    state.lastError = String(error?.message || error);
    scheduleReconnect(name);
    return;
  }
  state.socket = socket;
  socket.on('open', () => {
    state.connected = true;
    state.attempts = 0;
    state.openedAt = Date.now();
    state.lastMessageAt = 0;
    state.lastError = '';
  });
  socket.on('message', (raw) => {
    state.lastMessageAt = Date.now();
    try {
      spec.handler(raw);
    } catch (error) {
      state.lastError = String(error?.message || error);
    }
  });
  socket.on('error', (error) => {
    state.lastError = String(error?.message || error);
  });
  socket.on('close', () => scheduleReconnect(name));
}

function supabaseEnabled() {
  return Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
}

function supabaseHeaders(prefer = '') {
  const headers = {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    'content-type': 'application/json',
    accept: 'application/json',
  };
  if (prefer) headers.prefer = prefer;
  return headers;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 15_000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function loadSnapshot(snapshotType, quoteAsset = DEFAULT_QUOTE) {
  if (!supabaseEnabled()) return [];
  const query = new URLSearchParams({
    provider: `eq.${PROVIDER}`,
    market_type: `eq.${MARKET_TYPE}`,
    snapshot_type: `eq.${snapshotType}`,
    quote_asset: `eq.${quoteAsset}`,
    select: 'payload,row_count,source,source_time,updated_at',
    limit: '1',
  });
  const response = await fetchWithTimeout(
    `${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}?${query.toString()}`,
    { headers: supabaseHeaders() },
    12_000,
  );
  if (!response.ok) throw new Error(`snapshot_restore_http_${response.status}`);
  const payload = await response.json();
  const record = Array.isArray(payload) ? payload[0] : null;
  const rows = Array.isArray(record?.payload?.rows) ? record.payload.rows : [];
  return rows;
}

async function restoreSnapshots() {
  if (!supabaseEnabled()) return;
  try {
    const [universeRows, tickerRows] = await Promise.all([
      loadSnapshot('universe'),
      loadSnapshot('tickers'),
    ]);
    for (const raw of universeRows) {
      const symbol = compact(raw?.symbol);
      const [fallbackBase, fallbackQuote] = splitQuote(symbol);
      const base = String(raw?.base_asset || fallbackBase).toUpperCase();
      const quote = String(raw?.quote_asset || fallbackQuote).toUpperCase();
      if (!symbol || !base || !quote) continue;
      universeBySymbol.set(symbol, {
        ...raw,
        provider: PROVIDER,
        market_type: MARKET_TYPE,
        symbol,
        base_asset: base,
        quote_asset: quote,
        status: 'TRADING',
        active: true,
        source: raw?.source || 'binance_contract_persistent_snapshot',
      });
    }
    for (const raw of tickerRows) {
      const symbol = compact(raw?.symbol);
      if (!symbol) continue;
      tickerBySymbol.set(symbol, {
        ...raw,
        provider: PROVIDER,
        market_type: MARKET_TYPE,
        symbol,
        source: raw?.source || 'binance_contract_persistent_snapshot',
      });
    }
    restoredAt = Date.now();
    notifyWaiters();
  } catch (error) {
    restLastError = `snapshot_restore:${String(error?.message || error)}`;
  }
}

async function persistSnapshot(snapshotType, rows, source) {
  if (!supabaseEnabled() || !rows.length) return;
  const body = [{
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    snapshot_type: snapshotType,
    quote_asset: DEFAULT_QUOTE,
    payload: { rows },
    row_count: rows.length,
    source,
    source_time: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }];
  const response = await fetchWithTimeout(
    `${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}?on_conflict=provider,market_type,snapshot_type,quote_asset`,
    {
      method: 'POST',
      headers: supabaseHeaders('resolution=merge-duplicates,return=minimal'),
      body: JSON.stringify(body),
    },
    15_000,
  );
  if (!response.ok) throw new Error(`snapshot_persist_http_${response.status}`);
}

function sortedUniverseRows(quote = DEFAULT_QUOTE) {
  const normalizedQuote = String(quote || DEFAULT_QUOTE).toUpperCase();
  return [...universeBySymbol.values()]
    .filter((row) => String(row.quote_asset || '').toUpperCase() === normalizedQuote && row.active !== false)
    .sort((a, b) => String(a.symbol).localeCompare(String(b.symbol)));
}

function sortedTickerRows(symbols = []) {
  const wanted = new Set((Array.isArray(symbols) ? symbols : []).map(compact).filter(Boolean));
  const rows = [...tickerBySymbol.values()]
    .filter((row) => !wanted.size || wanted.has(compact(row.symbol)))
    .sort((a, b) => String(a.symbol).localeCompare(String(b.symbol)));
  return rows;
}

async function persistDirtySnapshots() {
  persistTimer = null;
  const tasks = [];
  const universeRows = sortedUniverseRows(DEFAULT_QUOTE);
  const tickerRows = sortedTickerRows().filter((row) => compact(row.symbol).endsWith(DEFAULT_QUOTE));
  if (dirtyUniverse && universeRows.length >= SNAPSHOT_MIN_UNIVERSE_ROWS) {
    tasks.push(persistSnapshot('universe', universeRows, 'binance_contract_websocket_and_rest_snapshot')
      .then(() => { dirtyUniverse = false; }));
  }
  if (dirtyTickers && tickerRows.length >= SNAPSHOT_MIN_TICKER_ROWS) {
    tasks.push(persistSnapshot('tickers', tickerRows, 'binance_contract_websocket_and_rest_snapshot')
      .then(() => { dirtyTickers = false; }));
  }
  if (!tasks.length) return;
  try {
    await Promise.all(tasks);
    lastPersistAt = Date.now();
  } catch (error) {
    restLastError = `snapshot_persist:${String(error?.message || error)}`;
    schedulePersist();
  }
}

function schedulePersist() {
  if (!supabaseEnabled() || persistTimer) return;
  persistTimer = setTimeout(() => {
    persistDirtySnapshots().catch(() => {});
  }, SNAPSHOT_PERSIST_INTERVAL_MS);
  persistTimer.unref?.();
}

async function jsonFetchCandidates(urls, timeoutMs = 15_000) {
  let lastError;
  for (const url of urls) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: { accept: 'application/json', 'user-agent': 'KakaWeb3-Market-Worker/650' },
      });
      const text = await response.text();
      if (!response.ok) throw new Error(`${response.status} ${response.statusText} ${text.slice(0, 240)}`);
      return text ? JSON.parse(text) : null;
    } catch (error) {
      lastError = error;
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError || new Error('binance_rest_unavailable');
}

function restrictedRestError(error) {
  const text = String(error?.message || error).toLowerCase();
  return text.includes(' 418 ') || text.startsWith('418 ') ||
    text.includes(' 429 ') || text.startsWith('429 ') ||
    text.includes(' 451 ') || text.startsWith('451 ') ||
    text.includes('too many requests') || text.includes('banned until') || text.includes('restricted location');
}

async function refreshFromRestInternal() {
  const bases = [
    'https://fapi.binance.com',
    'https://fapi1.binance.com',
    'https://fapi2.binance.com',
    'https://fapi3.binance.com',
  ];
  const [exchangeInfo, tickerPayload] = await Promise.all([
    jsonFetchCandidates(bases.map((base) => `${base}/fapi/v1/exchangeInfo`)),
    jsonFetchCandidates(bases.map((base) => `${base}/fapi/v1/ticker/24hr`)),
  ]);
  const now = Date.now();
  let universeCount = 0;
  let tickerCount = 0;
  const activeSymbols = new Set();
  for (const item of Array.isArray(exchangeInfo?.symbols) ? exchangeInfo.symbols : []) {
    if (String(item?.status || '').toUpperCase() !== 'TRADING') continue;
    if (String(item?.contractType || '').toUpperCase() !== 'PERPETUAL') continue;
    const identity = normalizedPerpetual({
      s: item.symbol,
      ps: item.pair || item.symbol,
      st: 1,
    });
    if (!identity) continue;
    identity.base = String(item.baseAsset || identity.base).toUpperCase();
    identity.quote = String(item.quoteAsset || identity.quote).toUpperCase();
    upsertUniverse(identity, 'binance_official_public_market_rest_low_frequency', now);
    activeSymbols.add(identity.symbol);
    universeCount += 1;
  }
  if (universeCount >= SNAPSHOT_MIN_UNIVERSE_ROWS) {
    for (const symbol of [...universeBySymbol.keys()]) {
      if (symbol.endsWith(DEFAULT_QUOTE) && !activeSymbols.has(symbol)) removeSymbol(symbol);
    }
  }
  for (const item of Array.isArray(tickerPayload) ? tickerPayload : []) {
    const identity = normalizedPerpetual({ ...item, ps: item.pair || item.symbol, st: 1 });
    if (!identity || !universeBySymbol.has(identity.symbol)) continue;
    upsertTicker(item, identity, 'binance_official_public_ticker_rest_low_frequency', now);
    tickerCount += 1;
  }
  if (!universeCount) throw new Error('binance_rest_universe_empty');
  restLastSuccessAt = now;
  restLastError = '';
  restNextAllowedAt = now + REST_REFRESH_INTERVAL_MS;
  schedulePersist();
  return { universeCount, tickerCount };
}

export async function refreshBinanceContractMarketFromRest({ force = false } = {}) {
  const now = Date.now();
  if (!force && now < restNextAllowedAt) return null;
  if (restRefreshPromise) return restRefreshPromise;
  restRefreshPromise = refreshFromRestInternal()
    .catch((error) => {
      restLastError = String(error?.message || error);
      restNextAllowedAt = Date.now() + (restrictedRestError(error) ? REST_RESTRICTED_COOLDOWN_MS : REST_TRANSIENT_COOLDOWN_MS);
      throw error;
    })
    .finally(() => { restRefreshPromise = null; });
  return restRefreshPromise;
}

function waitForRows(predicate, timeoutMs = START_WAIT_MS) {
  if (predicate()) return Promise.resolve();
  return new Promise((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      waiters.delete(check);
      clearTimeout(timer);
      resolve();
    };
    const check = () => {
      if (predicate()) finish();
    };
    const timer = setTimeout(finish, timeoutMs);
    timer.unref?.();
    waiters.add(check);
  });
}

export function startBinanceContractMarket() {
  if (started) return;
  started = true;
  restoreSnapshots().finally(() => {
    for (const name of Object.keys(STREAMS)) connectStream(name);
    setTimeout(() => {
      refreshBinanceContractMarketFromRest().catch(() => {});
    }, 10_000).unref?.();
  });
  const watchdog = setInterval(() => {
    const now = Date.now();
    for (const name of Object.keys(STREAMS)) {
      const state = streamStatus(name);
      const expectsFrequentMessages = name === 'ticker' || name === 'bookTicker';
      const noFirstMessage = expectsFrequentMessages && state.connected && state.lastMessageAt === 0 &&
        state.openedAt > 0 && now - state.openedAt > WS_STALE_MS;
      const stale = expectsFrequentMessages && state.connected && state.lastMessageAt > 0 &&
        now - state.lastMessageAt > WS_STALE_MS;
      if (!state.connected || noFirstMessage || stale) {
        try { state.socket?.terminate(); } catch (_) {}
        scheduleReconnect(name);
      }
    }
    if (dirtyUniverse || dirtyTickers) schedulePersist();
    refreshBinanceContractMarketFromRest().catch(() => {});
  }, 30_000);
  watchdog.unref?.();
}

export async function getBinanceContractUniverse({ quote = DEFAULT_QUOTE, waitMs = START_WAIT_MS } = {}) {
  startBinanceContractMarket();
  const normalizedQuote = String(quote || DEFAULT_QUOTE).toUpperCase();
  const minimumRows = normalizedQuote === DEFAULT_QUOTE ? SNAPSHOT_MIN_UNIVERSE_ROWS : 1;
  let rows = sortedUniverseRows(normalizedQuote);
  if (rows.length < minimumRows && waitMs > 0) {
    await waitForRows(() => sortedUniverseRows(normalizedQuote).length >= minimumRows, waitMs);
    rows = sortedUniverseRows(normalizedQuote);
  }
  if (rows.length < minimumRows) {
    try { await refreshBinanceContractMarketFromRest(); } catch (_) {}
    rows = sortedUniverseRows(normalizedQuote);
  }
  if (rows.length < minimumRows) {
    throw new Error(`binance_contract_universe_incomplete:${rows.length}`);
  }
  return rows;
}

export async function getBinanceContractTickers({ symbols = [], waitMs = START_WAIT_MS } = {}) {
  startBinanceContractMarket();
  const wanted = (Array.isArray(symbols) ? symbols : []).map(compact).filter(Boolean);
  let rows = sortedTickerRows(wanted);
  // Step650.2：全市场快照已经完整时，某个旧/下架/拼写异常符号未命中就是正常空结果。
  // 不等待、不触发低频REST，也不把它升级成 provider 级故障。
  if (wanted.length && tickerBySymbol.size >= SNAPSHOT_MIN_TICKER_ROWS) return rows;
  const enough = () => wanted.length ? rows.length >= Math.min(wanted.length, 1) : rows.length >= SNAPSHOT_MIN_TICKER_ROWS;
  if (!enough() && waitMs > 0) {
    await waitForRows(() => {
      rows = sortedTickerRows(wanted);
      return wanted.length ? rows.length >= Math.min(wanted.length, 1) : rows.length >= SNAPSHOT_MIN_TICKER_ROWS;
    }, waitMs);
    rows = sortedTickerRows(wanted);
  }
  if (!rows.length) {
    try { await refreshBinanceContractMarketFromRest(); } catch (_) {}
    rows = sortedTickerRows(wanted);
  }
  return rows;
}

export function getBinanceContractMarketHealth() {
  const streams = {};
  for (const [name, state] of connectionState.entries()) {
    streams[name] = {
      connected: Boolean(state.connected),
      opened_at: state.openedAt ? iso(state.openedAt) : null,
      last_message_at: state.lastMessageAt ? iso(state.lastMessageAt) : null,
      last_error: state.lastError || null,
    };
  }
  return {
    ok: universeBySymbol.size > 0 || tickerBySymbol.size > 0,
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    universe_rows: universeBySymbol.size,
    ticker_rows: tickerBySymbol.size,
    usdt_universe_rows: sortedUniverseRows(DEFAULT_QUOTE).length,
    restored_at: restoredAt ? iso(restoredAt) : null,
    last_universe_event_at: lastUniverseEventAt ? iso(lastUniverseEventAt) : null,
    last_ticker_event_at: lastTickerEventAt ? iso(lastTickerEventAt) : null,
    last_contract_info_event_at: lastContractInfoEventAt ? iso(lastContractInfoEventAt) : null,
    last_persist_at: lastPersistAt ? iso(lastPersistAt) : null,
    rest_last_success_at: restLastSuccessAt ? iso(restLastSuccessAt) : null,
    rest_next_allowed_at: restNextAllowedAt ? iso(restNextAllowedAt) : null,
    rest_last_error: restLastError || null,
    persistence_enabled: supabaseEnabled(),
    streams,
    source: 'binance_official_public_websocket_with_persistent_snapshot',
    time: new Date().toISOString(),
  };
}

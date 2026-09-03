import { WebSocket } from 'ws';

const VERSION = '650.8.15.44.1';
const PROVIDER = 'binance';
const MARKET_TYPE = 'contract';
const DEFAULT_QUOTE = 'USDT';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const SNAPSHOT_TABLE = 'app_market_backend_snapshots';
const SNAPSHOT_MIN_UNIVERSE_ROWS = 50;
const SNAPSHOT_MIN_TICKER_ROWS = 50;
// Step651.2D.2: a full ~700-row market snapshot must not be uploaded every 30 seconds.
// Fifteen minutes is still fresh enough for last-known-good cold-start recovery because
// the official WebSocket immediately refreshes live rows after startup.
const SNAPSHOT_PERSIST_INTERVAL_MS = 15 * 60_000;
// Step980.6.3.5.1: identity mutations are rare and restart-critical. Persist the
// already-shared clean snapshot shortly after a real listing/delisting/prune instead
// of waiting for the ordinary 15-minute market-data persistence cadence.
const IDENTITY_MUTATION_PERSIST_DELAY_MS = 5_000;
const AUTOMATIC_REST_ENABLED = false;
const REST_REFRESH_INTERVAL_MS = 6 * 60 * 60_000;
const REST_RESTRICTED_COOLDOWN_MS = 30 * 60_000;
const REST_TRANSIENT_COOLDOWN_MS = 90_000;
const WS_RECONNECT_MAX_MS = 60_000;
const WS_STALE_MS = 45_000;
// Step651.2D.3: all-market ticker and mark-price payloads are large snapshots.
// The App calibrates large lists at low frequency and detail Klines have their own
// realtime WebSocket, so receiving the same 700-row arrays every second wastes
// several GiB per day. Capture one official snapshot per minute instead.
const MARKET_TICKER_SNAPSHOT_INTERVAL_MS = 60_000;
const MARKET_MARK_PRICE_SNAPSHOT_INTERVAL_MS = 60_000;
const MARKET_SNAPSHOT_RETRY_MS = 15_000;
const MARKET_SNAPSHOT_TIMEOUT_MS = 20_000;
const START_WAIT_MS = 6_500;
// Step980.6.3.4.2: Binance documents !ticker@arr as changed-only, while the
// USDⓈ-M WebSocket API `ticker.price` without symbol returns the current all-symbol
// latest-price array. Use two stable `ticker.price` arrays as the authoritative
// current priced-symbol identity baseline. Do NOT intersect it with !markPrice@arr:
// after the 2026 market-stream migration Binance may emit TradFi mark-price rows in
// a separate message, so a one-message mark snapshot is not a complete identity set.
// Once confirmed, changed ticker / mark-price traffic may enrich only confirmed
// identities; contractInfo remains the immediate source for listing/delisting changes.
const CURRENT_IDENTITY_MIN_ABSOLUTE_ROWS = 250;
const CURRENT_IDENTITY_MIN_RETAIN_RATIO = 0.70;
const CURRENT_IDENTITY_STABLE_OVERLAP_RATIO = 0.97;
const CURRENT_IDENTITY_CONFIRMATIONS_REQUIRED = 2;
const WS_API_URL = 'wss://ws-fapi.binance.com/ws-fapi/v1';
const CURRENT_PRICE_BASELINE_INTERVAL_MS = 10 * 60_000;
const CURRENT_PRICE_BASELINE_RETRY_MS = 15_000;
const CURRENT_PRICE_BASELINE_TIMEOUT_MS = 20_000;
const WS_CONNECT_GAP_MS = 3_000;
const WS_CONNECT_WINDOW_MS = 5 * 60_000;
const WS_MAX_CONNECT_ATTEMPTS_5M = 15;
const wsConnectAttempts = [];
let wsConnectChain = Promise.resolve();
let wsLastConnectAt = 0;
const wsConnectStats = { attempts: 0, waits: 0, window_blocks: 0 };

const universeBySymbol = new Map();
const tickerBySymbol = new Map();
const realtimeMetaBySymbol = new Map();
// Step1060.33.5: reuse the existing merged Binance futures public WebSocket streams
// for COIN-M dated delivery contracts. No new socket and no Binance REST request is
// introduced. st=2 rows are kept separate from the existing USDⓈ-M perpetual maps.
const deliveryBySymbol = new Map();
let lastDeliveryEventAt = 0;
const connectionState = new Map();
const waiters = new Set();

let started = false;
let restoredAt = 0;
let lastUniverseEventAt = 0;
let lastTickerEventAt = 0;
let lastContractInfoEventAt = 0;
let lastMarkPriceEventAt = 0;
let lastPersistAt = 0;
let dirtyUniverse = false;
let dirtyTickers = false;
let persistTimer = null;
let persistTimerDueAt = 0;
let identityMutationGeneration = 0;
let persistedIdentityGeneration = 0;
let lastIdentityMutationAt = 0;
let lastIdentityPersistAt = 0;
let lastIdentityReconcileAt = 0;
let restRefreshPromise = null;
let restNextAllowedAt = 0;
let restLastSuccessAt = 0;
let restLastError = '';
const restoredPendingSymbols = new Set();
// Step980.6.3.5.3: persisted universe/ticker snapshots are cache material only.
// They must never become the live identity authority during cold start.
// Stage them here, then merge only rows whose symbols are present in the
// official all-symbol live price baseline.
const restoredUniverseQuarantine = new Map();
const restoredTickerQuarantine = new Map();
const restoredSnapshotQuarantineStats = {
  loaded: false,
  active: false,
  loaded_universe_rows: 0,
  loaded_ticker_rows: 0,
  loaded_unique_symbols: 0,
  released_matching_symbols: 0,
  discarded_symbols: 0,
  released_at: 0,
};
let periodicEnrichmentStarted = false;
let currentIdentityCandidate = null;
let currentIdentityCandidateConfirmations = 0;
let currentIdentityLastCandidateRows = 0;
let currentIdentityLastCandidateAt = 0;
let currentIdentityLastPrunedRows = 0;
let currentIdentityTotalPrunedRows = 0;
let currentIdentityLastPrunedAt = 0;
let currentIdentityRejectedPartialSnapshots = 0;
let confirmedCurrentIdentitySymbols = new Set();
let currentIdentityConfirmedAt = 0;
let latestMarkPricePerpetualSymbols = new Set();
let latestMarkPriceIdentityAt = 0;
let currentPriceBaselineTimer = null;
let currentPriceBaselineRunning = false;
const currentPriceBaselineStats = {
  attempts: 0,
  succeeded: 0,
  failed: 0,
  last_rows: 0,
  last_intersection_rows: 0,
  last_valid_identity_rows: 0,
  last_at: 0,
  last_error: '',
};
const snapshotPersistStats = {
  attempts: 0,
  succeeded: 0,
  failed: 0,
  request_bytes: 0,
  last_request_bytes: 0,
  last_snapshot_type: '',
  last_attempt_at: 0,
};
const marketWsTraffic = Object.fromEntries(
  ['ticker', 'bookTicker', 'contractInfo', 'markPrice', 'priceBaselineApi'].map((name) => [name, { messages: 0, bytes: 0 }]),
);

function rawByteLength(raw) {
  if (Buffer.isBuffer(raw)) return raw.length;
  if (raw instanceof ArrayBuffer) return raw.byteLength;
  if (ArrayBuffer.isView(raw)) return raw.byteLength;
  return Buffer.byteLength(String(raw ?? ''), 'utf8');
}

function universeIdentityChanged(previous, next) {
  if (!previous) return true;
  for (const key of ['provider', 'market_type', 'symbol', 'raw_symbol', 'base_asset', 'quote_asset', 'status', 'active']) {
    if (previous[key] !== next[key]) return true;
  }
  return false;
}

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
  return [normalized, ''];
}

function symbolMatchesQuote(symbol, quote) {
  const normalizedSymbol = compact(symbol);
  const normalizedQuote = compact(quote);
  return Boolean(
    normalizedSymbol &&
    normalizedQuote &&
    normalizedSymbol.endsWith(normalizedQuote) &&
    normalizedSymbol.length > normalizedQuote.length
  );
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

function isCoinMPayload(item) {
  return finite(item?.st) === 2;
}

function deliveryExpiryFromSymbol(rawSymbol) {
  const match = String(rawSymbol || '').trim().toUpperCase().match(/_(\d{2})(\d{2})(\d{2})$/);
  if (!match) return 0;
  const year = 2000 + Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || month < 1 || month > 12 || day < 1 || day > 31) return 0;
  // Binance quarterly delivery symbols encode YYMMDD and expire at 08:00 UTC.
  // contractInfo.dt, when observed, always overrides this cold-start convention.
  const ms = Date.UTC(year, month - 1, day, 8, 0, 0, 0);
  const check = new Date(ms);
  if (check.getUTCFullYear() !== year || check.getUTCMonth() !== month - 1 || check.getUTCDate() !== day) return 0;
  return ms;
}

export function normalizeBinanceCoinMDeliveryPublicRow(item) {
  if (!item || typeof item !== 'object' || !isCoinMPayload(item)) return null;
  const rawSymbol = String(item.s ?? item.symbol ?? '').trim().toUpperCase();
  if (!/_\d{6}$/.test(rawSymbol)) return null;
  const rawPair = String(item.ps ?? item.pair ?? rawSymbol.replace(/_\d{6}$/, '')).trim().toUpperCase();
  const pair = compact(rawPair);
  const [base, quote] = splitQuote(pair);
  if (!rawSymbol || !pair || !base || !quote) return null;
  const expiryMs = finite(item.dt ?? item.deliveryDate ?? item.delivery_time_ms) || deliveryExpiryFromSymbol(rawSymbol);
  return { symbol: rawSymbol, rawSymbol, pair, base, quote, expiryMs };
}

function upsertCoinMDelivery(item, source, observedAt = Date.now()) {
  const identity = normalizeBinanceCoinMDeliveryPublicRow(item);
  if (!identity) return false;
  const status = String(item.cs ?? item.contractStatus ?? item.status ?? 'TRADING').trim().toUpperCase() || 'TRADING';
  if (!['TRADING', 'PRE_DELIVERING', 'PRE_SETTLE'].includes(status)) return deliveryBySymbol.delete(identity.symbol);
  const previous = deliveryBySymbol.get(identity.symbol) || {};
  const sourceTimeMs = finite(item.E ?? item.time ?? item.source_time) || observedAt;
  const expiryMs = finite(item.dt ?? item.deliveryDate) || previous.expiry_timestamp_ms || identity.expiryMs;
  const last = finite(item.c ?? item.lastPrice ?? item.last_price ?? item.price);
  const mark = finite(item.p ?? item.markPrice ?? item.mark_price);
  const index = finite(item.i ?? item.indexPrice ?? item.index_price);
  const contractType = String(item.ct ?? item.contractType ?? previous.contract_type ?? 'DELIVERY').trim().toUpperCase();
  const next = mergeNonNull(previous, {
    provider: PROVIDER, market_type: 'delivery', symbol: identity.symbol, raw_symbol: identity.rawSymbol,
    pair: identity.pair, base_asset: identity.base, quote_asset: identity.quote, quote_symbol: identity.quote,
    settle_asset: identity.base, contract_type: contractType, status, active: true,
    expiry_timestamp_ms: expiryMs && expiryMs > 0 ? expiryMs : null,
    expiry_at: expiryMs && expiryMs > 0 ? iso(expiryMs) : null,
    expiry_source: finite(item.dt ?? item.deliveryDate) ? 'binance_contract_info_dt' : (previous.expiry_source || 'binance_symbol_yymmdd_0800utc'),
    last_price: last, price: last, mark_price: mark, index_price: index,
    source_time: iso(sourceTimeMs), cached_at: iso(observedAt), source, symbol_type: 2,
  });
  if (!(finite(next.expiry_timestamp_ms) > Date.now())) {
    deliveryBySymbol.delete(identity.symbol);
    return false;
  }
  deliveryBySymbol.set(identity.symbol, next);
  lastDeliveryEventAt = Math.max(lastDeliveryEventAt, sourceTimeMs);
  return true;
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
  restoredPendingSymbols.delete(identity.symbol);
  const previous = universeBySymbol.get(identity.symbol);
  const next = universeRow(identity, source, updatedAt);
  // Step651.2D.2: ticker/book/mark-price events arrive continuously, but they do
  // not change the market universe. Do not mark the entire universe snapshot
  // dirty merely because source/cached_at changed on another market message.
  if (!universeIdentityChanged(previous, next)) return false;
  universeBySymbol.set(identity.symbol, { ...previous, ...next });
  dirtyUniverse = true;
  notifyWaiters();
  return true;
}

function mergeNonNull(previous, next) {
  const merged = { ...(previous || {}) };
  for (const [key, value] of Object.entries(next || {})) {
    if (value == null) continue;
    if (typeof value === 'string' && value.trim() === '') continue;
    merged[key] = value;
  }
  return merged;
}

function upsertTicker(item, identity, source, updatedAt = Date.now()) {
  upsertUniverse(identity, source.replace('ticker', 'market'), updatedAt);
  const previous = tickerBySymbol.get(identity.symbol);
  const next = tickerRow(item, identity, source, updatedAt);
  // Preserve mark-price/funding fields already supplied by the dedicated stream.
  // The 24h ticker payload legitimately omits those fields and must not erase them.
  tickerBySymbol.set(identity.symbol, mergeNonNull(previous, next));
  dirtyTickers = true;
  notifyWaiters();
}

function removeSymbol(symbol) {
  const normalized = compact(symbol);
  if (!normalized) return false;
  let removed = false;
  if (universeBySymbol.delete(normalized)) {
    dirtyUniverse = true;
    removed = true;
  }
  if (tickerBySymbol.delete(normalized)) {
    dirtyTickers = true;
    removed = true;
  }
  if (realtimeMetaBySymbol.delete(normalized)) {
    dirtyTickers = true;
    removed = true;
  }
  restoredPendingSymbols.delete(normalized);
  return removed;
}

function identitySetOverlapRatio(left, right) {
  if (!(left instanceof Set) || !(right instanceof Set) || !left.size || !right.size) return 0;
  let intersection = 0;
  const smaller = left.size <= right.size ? left : right;
  const larger = smaller === left ? right : left;
  for (const symbol of smaller) if (larger.has(symbol)) intersection += 1;
  return intersection / Math.max(left.size, right.size);
}

function currentIdentityMinimumRows() {
  const liveOrRestoredRows = Math.max(universeBySymbol.size, tickerBySymbol.size, realtimeMetaBySymbol.size);
  return Math.max(
    CURRENT_IDENTITY_MIN_ABSOLUTE_ROWS,
    Math.floor(liveOrRestoredRows * CURRENT_IDENTITY_MIN_RETAIN_RATIO),
  );
}

function currentIdentityConfirmed() {
  return currentIdentityCandidateConfirmations >= CURRENT_IDENTITY_CONFIRMATIONS_REQUIRED &&
    confirmedCurrentIdentitySymbols.size >= CURRENT_IDENTITY_MIN_ABSOLUTE_ROWS;
}

function symbolAllowedByConfirmedIdentity(symbol) {
  // Step980.6.3.5.4: enrichment is never allowed to establish identity.
  // Until the official all-symbol price baseline is confirmed 2/2, ticker/mark
  // enrichment must not write into the active maps at all. After confirmation
  // it may enrich only symbols already admitted by the authoritative baseline.
  if (!currentIdentityConfirmed()) return false;
  return confirmedCurrentIdentitySymbols.has(compact(symbol));
}

function markIdentityMutation(observedAt = Date.now()) {
  identityMutationGeneration += 1;
  lastIdentityMutationAt = observedAt;
  schedulePersist(IDENTITY_MUTATION_PERSIST_DELAY_MS);
}

function reconcileCurrentIdentitySnapshot(symbols, observedAt = Date.now()) {
  if (!(symbols instanceof Set) || !symbols.size) return { accepted: false, reason: 'empty' };
  const minimumRows = currentIdentityMinimumRows();
  currentIdentityLastCandidateRows = symbols.size;
  currentIdentityLastCandidateAt = observedAt;

  if (symbols.size < minimumRows) {
    currentIdentityRejectedPartialSnapshots += 1;
    currentIdentityCandidate = null;
    currentIdentityCandidateConfirmations = 0;
    return { accepted: false, reason: 'partial', rows: symbols.size, minimum_rows: minimumRows };
  }

  if (!currentIdentityCandidate) {
    currentIdentityCandidate = new Set(symbols);
    currentIdentityCandidateConfirmations = 1;
    return { accepted: true, confirmed: false, confirmations: 1, rows: symbols.size };
  }

  const overlapRatio = identitySetOverlapRatio(currentIdentityCandidate, symbols);
  if (overlapRatio < CURRENT_IDENTITY_STABLE_OVERLAP_RATIO) {
    currentIdentityCandidate = new Set(symbols);
    currentIdentityCandidateConfirmations = 1;
    return {
      accepted: true,
      confirmed: false,
      confirmations: 1,
      rows: symbols.size,
      overlap_ratio: overlapRatio,
      reason: 'candidate_changed',
    };
  }

  currentIdentityCandidate = new Set(symbols);
  currentIdentityCandidateConfirmations += 1;
  if (currentIdentityCandidateConfirmations < CURRENT_IDENTITY_CONFIRMATIONS_REQUIRED) {
    return {
      accepted: true,
      confirmed: false,
      confirmations: currentIdentityCandidateConfirmations,
      rows: symbols.size,
      overlap_ratio: overlapRatio,
    };
  }

  confirmedCurrentIdentitySymbols = new Set(symbols);
  currentIdentityConfirmedAt = observedAt;
  lastIdentityReconcileAt = observedAt;
  const allKnownSymbols = new Set([
    ...universeBySymbol.keys(),
    ...tickerBySymbol.keys(),
    ...realtimeMetaBySymbol.keys(),
  ]);
  let pruned = 0;
  for (const symbol of allKnownSymbols) {
    if (symbols.has(symbol)) continue;
    if (removeSymbol(symbol)) pruned += 1;
  }
  currentIdentityCandidateConfirmations = CURRENT_IDENTITY_CONFIRMATIONS_REQUIRED;
  if (pruned) {
    currentIdentityLastPrunedRows = pruned;
    currentIdentityTotalPrunedRows += pruned;
    currentIdentityLastPrunedAt = observedAt;
    markIdentityMutation(observedAt);
    notifyWaiters();
  }
  return {
    accepted: true,
    confirmed: true,
    confirmations: currentIdentityCandidateConfirmations,
    rows: symbols.size,
    overlap_ratio: overlapRatio,
    pruned_rows: pruned,
  };
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
  let deliveryAccepted = 0;
  for (const item of rows) {
    if (upsertCoinMDelivery(item, 'binance_official_public_merged_ticker_websocket', now)) deliveryAccepted += 1;
    const identity = normalizedPerpetual(item);
    if (!identity || !symbolAllowedByConfirmedIdentity(identity.symbol)) continue;
    upsertTicker(item, identity, 'binance_official_public_ticker_websocket', now);
    accepted += 1;
  }
  if (accepted) {
    lastTickerEventAt = now;
    schedulePersist();
  }
  if (deliveryAccepted) lastDeliveryEventAt = now;
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

function handleMarkPriceMessage(raw) {
  const payload = parsePayload(raw);
  const rows = Array.isArray(payload) ? payload : [payload];
  const now = Date.now();
  let accepted = 0;
  let deliveryAccepted = 0;
  const currentSymbols = new Set();
  for (const item of rows) {
    if (upsertCoinMDelivery(item, 'binance_official_public_merged_mark_price_websocket', now)) deliveryAccepted += 1;
    const identity = normalizedPerpetual(item);
    if (!identity || !symbolAllowedByConfirmedIdentity(identity.symbol)) continue;
    currentSymbols.add(identity.symbol);
    const fundingRate = finite(item?.r ?? item?.fundingRate ?? item?.funding_rate);
    const nextFundingTimeMs = finite(item?.T ?? item?.nextFundingTime ?? item?.next_funding_time);
    const sourceTimeMs = finite(item?.E ?? item?.time ?? item?.source_time) ?? now;
    const meta = {
      provider: PROVIDER,
      market_type: MARKET_TYPE,
      symbol: identity.symbol,
      mark_price: finite(item?.p ?? item?.markPrice ?? item?.mark_price),
      index_price: finite(item?.i ?? item?.indexPrice ?? item?.index_price),
      estimated_settle_price: finite(item?.P ?? item?.estimatedSettlePrice ?? item?.estimated_settle_price),
      last_funding_rate: fundingRate,
      funding_rate: fundingRate,
      last_funding_rate_percent: fundingRate == null ? null : fundingRate * 100,
      funding_rate_percent: fundingRate == null ? null : fundingRate * 100,
      next_funding_time: nextFundingTimeMs && nextFundingTimeMs > 0 ? iso(nextFundingTimeMs) : null,
      source_time: iso(sourceTimeMs),
      cached_at: iso(now),
      source: 'binance_official_public_mark_price_websocket',
    };
    realtimeMetaBySymbol.set(identity.symbol, mergeNonNull(realtimeMetaBySymbol.get(identity.symbol), meta));
    tickerBySymbol.set(identity.symbol, mergeNonNull(tickerBySymbol.get(identity.symbol), meta));
    upsertUniverse(identity, 'binance_official_public_mark_price_websocket', now);
    dirtyTickers = true;
    accepted += 1;
  }
  if (accepted) {
    lastMarkPriceEventAt = now;
    latestMarkPricePerpetualSymbols = currentSymbols;
    latestMarkPriceIdentityAt = now;
    notifyWaiters();
    schedulePersist();
    // A mark-price update may accelerate the shared startup baseline, but it is
    // enrichment only and is never used as the authoritative full identity set.
    if (!currentPriceBaselineRunning && !currentPriceBaselineTimer) {
      scheduleCurrentPriceBaseline(750);
    }
  }
  if (deliveryAccepted) lastDeliveryEventAt = now;
}

function handleContractInfoMessage(raw) {
  const payload = parsePayload(raw);
  const rows = Array.isArray(payload) ? payload : [payload];
  const now = Date.now();
  let accepted = 0;
  let identityMutated = false;
  let requiresIdentityRefresh = false;

  for (const item of rows) {
    if (!item || typeof item !== 'object') continue;
    if (isCoinMPayload(item)) {
      if (upsertCoinMDelivery(item, 'binance_official_public_contract_info_websocket', now)) accepted += 1;
      continue;
    }
    if (!isUsdmPayload(item)) continue;
    const symbol = compact(item.s ?? item.symbol);
    if (!symbol) continue;
    const contractType = String(item.ct ?? item.contractType ?? '').toUpperCase();
    const contractStatus = String(item.cs ?? item.contractStatus ?? item.status ?? '').toUpperCase();
    if (contractType && !['PERPETUAL', 'TRADIFI_PERPETUAL'].includes(contractType)) continue;

    const isRemovalState = Boolean(contractStatus) &&
      !['TRADING', 'PRE_DELIVERING', 'PRE_SETTLE'].includes(contractStatus);

    // Step980.6.3.5.4: before the authoritative all-symbol baseline reaches
    // 2/2 confirmation, contractInfo is advisory only. Never let it populate
    // active identity during cold start. Its event simply accelerates a fresh
    // all-symbol identity baseline.
    if (!currentIdentityConfirmed()) {
      requiresIdentityRefresh = true;
      accepted += 1;
      continue;
    }

    if (isRemovalState) {
      confirmedCurrentIdentitySymbols.delete(symbol);
      if (removeSymbol(symbol)) identityMutated = true;
      accepted += 1;
      continue;
    }

    const identity = normalizedPerpetual(item);
    if (!identity) continue;

    // A positive/listing contractInfo event cannot establish a new active
    // identity by itself. Existing confirmed symbols may update identity fields;
    // a genuinely new symbol triggers an immediate authoritative price baseline
    // and becomes active only if that baseline contains it.
    if (!confirmedCurrentIdentitySymbols.has(identity.symbol)) {
      requiresIdentityRefresh = true;
      accepted += 1;
      continue;
    }

    upsertUniverse(identity, 'binance_official_public_contract_info_websocket', now);
    accepted += 1;
  }

  if (accepted) {
    lastContractInfoEventAt = now;
    if (identityMutated) markIdentityMutation(now);
    else schedulePersist();
    if (requiresIdentityRefresh) scheduleCurrentPriceBaselineUrgent(250);
  }
}

const PERSISTENT_STREAMS = {
  contractInfo: {
    urls: ['wss://fstream.binance.com/market/ws/!contractInfo'],
    handler: handleContractInfoMessage,
  },
};

const PERIODIC_SNAPSHOT_STREAMS = {
  ticker: {
    urls: ['wss://fstream.binance.com/market/ws/!ticker@arr'],
    handler: handleTickerMessage,
    intervalMs: MARKET_TICKER_SNAPSHOT_INTERVAL_MS,
    initialDelayMs: 0,
  },
  markPrice: {
    urls: ['wss://fstream.binance.com/market/ws/!markPrice@arr@1s'],
    handler: handleMarkPriceMessage,
    intervalMs: MARKET_MARK_PRICE_SNAPSHOT_INTERVAL_MS,
    initialDelayMs: 5_000,
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
      connectingPromise: null,
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
    connectStream(name).catch(() => {});
  }, delay);
  state.reconnectTimer.unref?.();
}

function pruneWsConnectAttempts(now = Date.now()) {
  while (wsConnectAttempts.length && now - wsConnectAttempts[0] >= WS_CONNECT_WINDOW_MS) {
    wsConnectAttempts.shift();
  }
}

async function acquireMarketWsConnectSlot() {
  let release;
  const previous = wsConnectChain;
  wsConnectChain = new Promise((resolve) => { release = resolve; });
  await previous;
  try {
    const now = Date.now();
    pruneWsConnectAttempts(now);
    const gapWait = Math.max(0, WS_CONNECT_GAP_MS - (now - wsLastConnectAt));
    const windowWait = wsConnectAttempts.length >= WS_MAX_CONNECT_ATTEMPTS_5M
      ? Math.max(0, wsConnectAttempts[0] + WS_CONNECT_WINDOW_MS - now)
      : 0;
    const waitMs = Math.max(gapWait, windowWait);
    if (waitMs > 0) {
      wsConnectStats.waits += 1;
      if (windowWait > 0) wsConnectStats.window_blocks += 1;
      await new Promise((resolve) => {
        const timer = setTimeout(resolve, waitMs);
        timer.unref?.();
      });
    }
    wsLastConnectAt = Date.now();
    wsConnectAttempts.push(wsLastConnectAt);
    wsConnectStats.attempts += 1;
  } finally {
    release();
  }
}

async function connectStream(name) {
  const spec = PERSISTENT_STREAMS[name];
  if (!spec) return;
  const state = streamStatus(name);
  if (state.socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(state.socket.readyState)) return;
  if (state.connectingPromise) return state.connectingPromise;
  state.connectingPromise = (async () => {
    await acquireMarketWsConnectSlot();
    if (state.socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(state.socket.readyState)) return;
    const url = spec.urls[state.urlIndex % spec.urls.length];
    state.urlIndex = (state.urlIndex + 1) % spec.urls.length;
    let socket;
    try {
      socket = new WebSocket(url, {
        handshakeTimeout: 15_000,
        perMessageDeflate: false,
        headers: { 'user-agent': 'KakaWeb3-Market-Worker/650.8.15.43' },
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
      const traffic = marketWsTraffic[name] || (marketWsTraffic[name] = { messages: 0, bytes: 0 });
      traffic.messages += 1;
      traffic.bytes += rawByteLength(raw);
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
  })().finally(() => {
    state.connectingPromise = null;
  });
  return state.connectingPromise;
}

function schedulePeriodicSnapshot(name, delayMs) {
  const spec = PERIODIC_SNAPSHOT_STREAMS[name];
  if (!spec) return;
  const state = streamStatus(name);
  if (state.reconnectTimer || state.connectingPromise ||
      (state.socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(state.socket.readyState))) {
    return;
  }
  state.reconnectTimer = setTimeout(() => {
    state.reconnectTimer = null;
    capturePeriodicSnapshot(name).catch(() => {});
  }, Math.max(0, delayMs));
  state.reconnectTimer.unref?.();
}

async function capturePeriodicSnapshot(name) {
  const spec = PERIODIC_SNAPSHOT_STREAMS[name];
  if (!spec) return;
  const state = streamStatus(name);
  if (state.socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(state.socket.readyState)) return;
  if (state.connectingPromise) return state.connectingPromise;

  state.connectingPromise = (async () => {
    await acquireMarketWsConnectSlot();
    if (state.socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(state.socket.readyState)) return;
    const url = spec.urls[state.urlIndex % spec.urls.length];
    state.urlIndex = (state.urlIndex + 1) % spec.urls.length;
    let socket;
    let completed = false;
    let timeoutTimer = null;
    try {
      socket = new WebSocket(url, {
        handshakeTimeout: 15_000,
        perMessageDeflate: false,
        headers: { 'user-agent': 'KakaWeb3-Market-Worker/650.8.15.43' },
      });
    } catch (error) {
      state.lastError = String(error?.message || error);
      schedulePeriodicSnapshot(name, MARKET_SNAPSHOT_RETRY_MS);
      return;
    }
    state.socket = socket;

    const finish = (success) => {
      if (timeoutTimer) clearTimeout(timeoutTimer);
      timeoutTimer = null;
      state.connected = false;
      if (state.socket === socket) state.socket = null;
      if (success) state.attempts = 0;
      else state.attempts += 1;
      schedulePeriodicSnapshot(name, success ? spec.intervalMs : MARKET_SNAPSHOT_RETRY_MS);
    };

    socket.on('open', () => {
      state.connected = true;
      state.openedAt = Date.now();
      state.lastMessageAt = 0;
      state.lastError = '';
      timeoutTimer = setTimeout(() => {
        if (completed) return;
        completed = true;
        state.lastError = 'periodic_snapshot_timeout';
        try { socket.terminate(); } catch (_) {}
      }, MARKET_SNAPSHOT_TIMEOUT_MS);
      timeoutTimer.unref?.();
    });
    socket.on('message', (raw) => {
      if (completed) return;
      completed = true;
      state.lastMessageAt = Date.now();
      const traffic = marketWsTraffic[name] || (marketWsTraffic[name] = { messages: 0, bytes: 0 });
      traffic.messages += 1;
      traffic.bytes += rawByteLength(raw);
      try {
        spec.handler(raw);
      } catch (error) {
        state.lastError = String(error?.message || error);
      }
      try { socket.close(1000, 'periodic_snapshot_complete'); } catch (_) {
        try { socket.terminate(); } catch (_) {}
      }
    });
    socket.on('error', (error) => {
      state.lastError = String(error?.message || error);
    });
    socket.on('close', () => finish(completed && state.lastMessageAt > 0));
  })().finally(() => {
    state.connectingPromise = null;
  });
  return state.connectingPromise;
}


function startPeriodicEnrichmentStreams() {
  if (periodicEnrichmentStarted) return;
  periodicEnrichmentStarted = true;
  for (const [name, spec] of Object.entries(PERIODIC_SNAPSHOT_STREAMS)) {
    schedulePeriodicSnapshot(name, spec.initialDelayMs || 0);
  }
}

function mergeRestoredTickerForLiveIdentity(symbol) {
  const normalized = compact(symbol);
  if (!normalized) return null;
  const cached = restoredTickerQuarantine.get(normalized) || null;
  if (!cached) return null;
  restoredPendingSymbols.delete(normalized);
  return cached;
}

function releaseRestoredSnapshotQuarantine(liveSymbols, observedAt = Date.now()) {
  if (!restoredSnapshotQuarantineStats.active) return;
  const live = liveSymbols instanceof Set ? liveSymbols : new Set();
  const loaded = new Set([
    ...restoredUniverseQuarantine.keys(),
    ...restoredTickerQuarantine.keys(),
  ]);
  let matching = 0;
  for (const symbol of loaded) {
    if (live.has(symbol)) matching += 1;
  }
  restoredSnapshotQuarantineStats.released_matching_symbols = matching;
  restoredSnapshotQuarantineStats.discarded_symbols = Math.max(0, loaded.size - matching);
  restoredSnapshotQuarantineStats.active = false;
  restoredSnapshotQuarantineStats.released_at = observedAt;
  restoredUniverseQuarantine.clear();
  restoredTickerQuarantine.clear();
}

function scheduleCurrentPriceBaseline(delayMs) {
  if (currentPriceBaselineTimer || currentPriceBaselineRunning) return;
  currentPriceBaselineTimer = setTimeout(() => {
    currentPriceBaselineTimer = null;
    captureCurrentPriceBaseline().catch(() => {});
  }, Math.max(0, Number(delayMs) || 0));
  currentPriceBaselineTimer.unref?.();
}

function scheduleCurrentPriceBaselineUrgent(delayMs = 250) {
  if (currentPriceBaselineRunning) return;
  if (currentPriceBaselineTimer) {
    clearTimeout(currentPriceBaselineTimer);
    currentPriceBaselineTimer = null;
  }
  scheduleCurrentPriceBaseline(delayMs);
}

function applyCurrentPriceBaseline(resultRows, observedAt = Date.now()) {
  const rows = Array.isArray(resultRows) ? resultRows : [];
  currentPriceBaselineStats.last_rows = rows.length;
  currentPriceBaselineStats.last_at = observedAt;

  const currentSymbols = new Set();
  let seeded = 0;
  for (const item of rows) {
    upsertCoinMDelivery(item, 'binance_official_public_ws_api_ticker_price_all_symbols', observedAt);
    const identity = normalizedPerpetual(item);
    if (!identity) continue;
    const last = finite(item?.price ?? item?.c ?? item?.lastPrice);
    if (last === null || last <= 0) continue;
    currentSymbols.add(identity.symbol);
    upsertUniverse(identity, 'binance_official_public_ws_api_ticker_price_all_symbols', observedAt);
    const restoredTicker = mergeRestoredTickerForLiveIdentity(identity.symbol);
    const tickerSeed = mergeNonNull(restoredTicker, tickerBySymbol.get(identity.symbol));
    tickerBySymbol.set(identity.symbol, mergeNonNull(tickerSeed, {
      provider: PROVIDER,
      market_type: MARKET_TYPE,
      symbol: identity.symbol,
      last_price: last,
      price: last,
      source_time: item?.time ? iso(item.time) : iso(observedAt),
      cached_at: iso(observedAt),
      source: 'binance_official_public_ws_api_ticker_price_all_symbols',
    }));
    dirtyTickers = true;
    seeded += 1;
  }

  currentPriceBaselineStats.last_valid_identity_rows = currentSymbols.size;
  // Backward-compatible diagnostic name retained for one release; this is no
  // longer a mark-price intersection count.
  currentPriceBaselineStats.last_intersection_rows = currentSymbols.size;
  if (seeded < CURRENT_IDENTITY_MIN_ABSOLUTE_ROWS) {
    currentPriceBaselineStats.last_error = `price_baseline_identity_too_small:${seeded}`;
    currentIdentityRejectedPartialSnapshots += 1;
    return { accepted: false, reason: 'identity_too_small', rows: seeded };
  }

  const reconciliation = reconcileCurrentIdentitySnapshot(currentSymbols, observedAt);
  currentPriceBaselineStats.last_error = reconciliation.accepted ? '' : String(reconciliation.reason || 'reconciliation_rejected');
  // Step980.6.3.5.4: do not start changed-only ticker / mark-price enrichment
  // after the first 1/2 candidate. Those streams are not identity authorities
  // and may contain rows outside the final priced-symbol set. Start them only
  // after two stable all-symbol baselines have confirmed current identity.
  if (reconciliation.confirmed) {
    startPeriodicEnrichmentStreams();
    releaseRestoredSnapshotQuarantine(currentSymbols, observedAt);
  }
  notifyWaiters();
  schedulePersist();
  return { ...reconciliation, seeded_rows: seeded };
}

async function captureCurrentPriceBaseline() {
  if (currentPriceBaselineRunning) return false;
  currentPriceBaselineRunning = true;
  currentPriceBaselineStats.attempts += 1;
  let socket = null;
  let timeoutTimer = null;
  let settled = false;
  const requestId = `kaka-price-baseline-${Date.now()}`;
  try {
    await acquireMarketWsConnectSlot();
    const success = await new Promise((resolve) => {
      const finish = (ok, error = '') => {
        if (settled) return;
        settled = true;
        if (timeoutTimer) clearTimeout(timeoutTimer);
        timeoutTimer = null;
        if (error) currentPriceBaselineStats.last_error = String(error).slice(0, 320);
        try { socket?.close(1000, 'price_baseline_complete'); } catch (_) {
          try { socket?.terminate(); } catch (_) {}
        }
        resolve(Boolean(ok));
      };

      try {
        socket = new WebSocket(WS_API_URL, {
          handshakeTimeout: 15_000,
          perMessageDeflate: false,
          headers: { 'user-agent': 'KakaWeb3-Market-Worker/650.8.15.43' },
        });
      } catch (error) {
        finish(false, error?.message || error);
        return;
      }

      timeoutTimer = setTimeout(() => finish(false, 'price_baseline_timeout'), CURRENT_PRICE_BASELINE_TIMEOUT_MS);
      timeoutTimer.unref?.();

      socket.on('open', () => {
        try {
          socket.send(JSON.stringify({ id: requestId, method: 'ticker.price', params: {} }));
        } catch (error) {
          finish(false, error?.message || error);
        }
      });
      socket.on('message', (raw) => {
        const traffic = marketWsTraffic.priceBaselineApi || (marketWsTraffic.priceBaselineApi = { messages: 0, bytes: 0 });
        traffic.messages += 1;
        traffic.bytes += rawByteLength(raw);
        let decoded = null;
        try { decoded = JSON.parse(Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw)); } catch (_) {}
        if (!decoded || String(decoded.id || '') !== requestId) return;
        if (Number(decoded.status) !== 200 || !Array.isArray(decoded.result)) {
          finish(false, `price_baseline_bad_response:${decoded?.status ?? 'unknown'}`);
          return;
        }
        const outcome = applyCurrentPriceBaseline(decoded.result, Date.now());
        finish(Boolean(outcome?.accepted), outcome?.accepted ? '' : outcome?.reason || 'price_baseline_rejected');
      });
      socket.on('error', (error) => finish(false, error?.message || error));
      socket.on('close', () => {
        if (!settled) finish(false, 'price_baseline_closed_before_response');
      });
    });

    if (success) {
      currentPriceBaselineStats.succeeded += 1;
      return true;
    }
    currentPriceBaselineStats.failed += 1;
    return false;
  } finally {
    currentPriceBaselineRunning = false;
    const confirmed = currentIdentityCandidateConfirmations >= CURRENT_IDENTITY_CONFIRMATIONS_REQUIRED;
    scheduleCurrentPriceBaseline(confirmed ? CURRENT_PRICE_BASELINE_INTERVAL_MS : CURRENT_PRICE_BASELINE_RETRY_MS);
  }
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

    restoredUniverseQuarantine.clear();
    restoredTickerQuarantine.clear();
    restoredSnapshotQuarantineStats.loaded = true;
    restoredSnapshotQuarantineStats.active = true;
    restoredSnapshotQuarantineStats.loaded_universe_rows = Array.isArray(universeRows) ? universeRows.length : 0;
    restoredSnapshotQuarantineStats.loaded_ticker_rows = Array.isArray(tickerRows) ? tickerRows.length : 0;
    restoredSnapshotQuarantineStats.released_matching_symbols = 0;
    restoredSnapshotQuarantineStats.discarded_symbols = 0;
    restoredSnapshotQuarantineStats.released_at = 0;

    for (const raw of universeRows) {
      const symbol = compact(raw?.symbol);
      const [fallbackBase, fallbackQuote] = splitQuote(symbol);
      const base = String(raw?.base_asset || fallbackBase).toUpperCase();
      const quote = String(raw?.quote_asset || fallbackQuote).toUpperCase();
      if (!symbol || !base || !quote || !symbolMatchesQuote(symbol, quote)) continue;
      restoredUniverseQuarantine.set(symbol, {
        ...raw,
        provider: PROVIDER,
        market_type: MARKET_TYPE,
        symbol,
        base_asset: base,
        quote_asset: quote,
        status: String(raw?.status || 'TRADING').toUpperCase(),
        active: raw?.active !== false,
        source: raw?.source || 'binance_contract_persistent_snapshot',
      });
    }

    for (const raw of tickerRows) {
      const symbol = compact(raw?.symbol);
      if (!symbol) continue;
      if (!restoredUniverseQuarantine.has(symbol)) continue;
      restoredTickerQuarantine.set(symbol, {
        ...raw,
        provider: PROVIDER,
        market_type: MARKET_TYPE,
        symbol,
        source: raw?.source || 'binance_contract_persistent_snapshot',
      });
    }

    restoredSnapshotQuarantineStats.loaded_unique_symbols = new Set([
      ...restoredUniverseQuarantine.keys(),
      ...restoredTickerQuarantine.keys(),
    ]).size;

    // Do not populate the active maps here. The live all-symbol baseline must
    // establish current identity first; persisted rows are field cache only.
    restoredAt = Date.now();
    notifyWaiters();
  } catch (error) {
    restLastError = `snapshot_restore:${String(error?.message || error)}`;
  }
}

async function persistSnapshot(snapshotType, rows, source) {
  if (!supabaseEnabled() || !rows.length) return;
  const nowIso = new Date().toISOString();
  const body = [{
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    snapshot_type: snapshotType,
    quote_asset: DEFAULT_QUOTE,
    payload: { rows },
    row_count: rows.length,
    source,
    source_time: nowIso,
    updated_at: nowIso,
  }];
  const bodyText = JSON.stringify(body);
  const requestBytes = Buffer.byteLength(bodyText, 'utf8');
  snapshotPersistStats.attempts += 1;
  snapshotPersistStats.request_bytes += requestBytes;
  snapshotPersistStats.last_request_bytes = requestBytes;
  snapshotPersistStats.last_snapshot_type = snapshotType;
  snapshotPersistStats.last_attempt_at = Date.now();
  try {
    const response = await fetchWithTimeout(
      `${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}?on_conflict=provider,market_type,snapshot_type,quote_asset`,
      {
        method: 'POST',
        headers: supabaseHeaders('resolution=merge-duplicates,return=minimal'),
        body: bodyText,
      },
      15_000,
    );
    if (!response.ok) throw new Error(`snapshot_persist_http_${response.status}`);
    snapshotPersistStats.succeeded += 1;
  } catch (error) {
    snapshotPersistStats.failed += 1;
    throw error;
  }
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
  persistTimerDueAt = 0;
  const identityGenerationAtStart = identityMutationGeneration;
  const tasks = [];
  const universeRows = sortedUniverseRows(DEFAULT_QUOTE);
  const tickerRows = sortedTickerRows().filter((row) => compact(row.symbol).endsWith(DEFAULT_QUOTE));
  if (dirtyUniverse && universeRows.length >= SNAPSHOT_MIN_UNIVERSE_ROWS) {
    tasks.push(persistSnapshot('universe', universeRows, 'binance_contract_websocket_snapshot')
      .then(() => { dirtyUniverse = false; }));
  }
  if (dirtyTickers && tickerRows.length >= SNAPSHOT_MIN_TICKER_ROWS) {
    tasks.push(persistSnapshot('tickers', tickerRows, 'binance_contract_websocket_snapshot')
      .then(() => { dirtyTickers = false; }));
  }
  if (!tasks.length) return;
  try {
    await Promise.all(tasks);
    lastPersistAt = Date.now();
    if (identityGenerationAtStart > persistedIdentityGeneration) {
      persistedIdentityGeneration = identityGenerationAtStart;
      lastIdentityPersistAt = lastPersistAt;
    }
  } catch (error) {
    restLastError = `snapshot_persist:${String(error?.message || error)}`;
    schedulePersist();
  }
}

function schedulePersist(delayMs = SNAPSHOT_PERSIST_INTERVAL_MS) {
  if (!supabaseEnabled()) return;
  const safeDelay = Math.max(1_000, Number(delayMs) || SNAPSHOT_PERSIST_INTERVAL_MS);
  const dueAt = Date.now() + safeDelay;
  if (persistTimer) {
    // Keep an already-earlier persistence deadline; only replace a later timer
    // when a real identity mutation needs a prompt restart-safe checkpoint.
    if (persistTimerDueAt > 0 && persistTimerDueAt <= dueAt) return;
    clearTimeout(persistTimer);
    persistTimer = null;
  }
  persistTimerDueAt = dueAt;
  persistTimer = setTimeout(() => {
    persistDirtySnapshots().catch(() => {});
  }, safeDelay);
  persistTimer.unref?.();
}

export async function refreshBinanceContractMarketFromRest() {
  // Step650.8.15.3：目录与Ticker严格由官方WebSocket + Supabase最后正确快照提供。
  // 该导出仅保留旧调用兼容性，永远不会访问Binance REST。
  return null;
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
    for (const name of Object.keys(PERSISTENT_STREAMS)) connectStream(name).catch(() => {});
    // Establish live identity before changed-only / mark-price enrichment may
    // populate the active market maps.
    scheduleCurrentPriceBaseline(250);
  });
  const watchdog = setInterval(() => {
    for (const name of Object.keys(PERSISTENT_STREAMS)) {
      const state = streamStatus(name);
      if (!state.connected) {
        try { state.socket?.terminate(); } catch (_) {}
        scheduleReconnect(name);
      }
    }
    if (periodicEnrichmentStarted) {
      for (const name of Object.keys(PERIODIC_SNAPSHOT_STREAMS)) {
        const state = streamStatus(name);
        const socketActive = state.socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(state.socket.readyState);
        if (!state.reconnectTimer && !state.connectingPromise && !socketActive) {
          schedulePeriodicSnapshot(name, 0);
        }
      }
    }
    if (!currentPriceBaselineTimer && !currentPriceBaselineRunning) {
      scheduleCurrentPriceBaseline(currentIdentityCandidateConfirmations >= CURRENT_IDENTITY_CONFIRMATIONS_REQUIRED
        ? CURRENT_PRICE_BASELINE_INTERVAL_MS
        : 0);
    }
    if (dirtyUniverse || dirtyTickers) schedulePersist();
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
  return rows;
}

export function getBinanceDeliveryContractsSnapshot({ nowMs = Date.now() } = {}) {
  startBinanceContractMarket();
  const rows = [...deliveryBySymbol.values()]
    .filter((row) => {
      const expiry = finite(row?.expiry_timestamp_ms);
      const price = finite(row?.mark_price ?? row?.last_price ?? row?.price);
      return expiry != null && expiry > nowMs && price != null && price > 0 && row?.active !== false;
    })
    .sort((a, b) => Number(a.expiry_timestamp_ms || 0) - Number(b.expiry_timestamp_ms || 0) || String(a.symbol).localeCompare(String(b.symbol)));
  return {
    ok: true, version: VERSION, provider: PROVIDER, market_type: 'delivery',
    source: 'binance_existing_merged_public_futures_websocket_reuse', ready: rows.length > 0,
    row_count: rows.length, rows: rows.map((row) => ({ ...row })),
    last_delivery_event_at: lastDeliveryEventAt ? iso(lastDeliveryEventAt) : null,
    binance_contract_rest_requests: 0, additional_websocket_connections: 0,
    reuses_existing_contract_info_stream: true, reuses_existing_all_market_ticker_stream: true,
    reuses_existing_mark_price_stream: true, reads_scale_with_users: false,
  };
}

export function getBinanceContractRealtimeMeta(symbol) {
  startBinanceContractMarket();
  const normalized = compact(symbol);
  if (!normalized) return null;
  const ticker = tickerBySymbol.get(normalized) || null;
  const meta = realtimeMetaBySymbol.get(normalized) || null;
  if (!ticker && !meta) return null;
  return mergeNonNull(ticker, meta);
}

export async function ensureBinanceContractRealtimeMeta(
  symbol,
  { waitMs = 6500, requireFundingSchedule = false } = {},
) {
  startBinanceContractMarket();
  const normalized = compact(symbol);
  if (!normalized) return null;

  const current = () => {
    const ticker = tickerBySymbol.get(normalized) || null;
    const meta = realtimeMetaBySymbol.get(normalized) || null;
    if (!ticker && !meta) return null;
    return mergeNonNull(ticker, meta);
  };
  const complete = (row) => {
    if (!row) return false;
    const rate = finite(row.last_funding_rate ?? row.funding_rate);
    if (rate == null) return false;
    if (!requireFundingSchedule) return true;
    const nextMs = Date.parse(String(row.next_funding_time || ''));
    return Number.isFinite(nextMs) && nextMs > Date.now();
  };

  let row = current();
  if (complete(row)) return row;

  // Step650.8.15.38: funding/OI first paint must not wait for the next
  // one-minute periodic mark-price snapshot. Reuse the same official all-market
  // WebSocket, coalesce concurrent callers, and still close after one payload.
  const state = streamStatus('markPrice');
  if (state.reconnectTimer) {
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
  }
  const capture = capturePeriodicSnapshot('markPrice').catch(() => null);
  await Promise.race([
    capture,
    new Promise((resolve) => {
      const timer = setTimeout(resolve, Math.max(0, Number(waitMs) || 0));
      timer.unref?.();
    }),
  ]);
  row = current();
  return row;
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
    version: VERSION,
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    universe_rows: universeBySymbol.size,
    ticker_rows: tickerBySymbol.size,
    realtime_meta_rows: realtimeMetaBySymbol.size,
    usdt_universe_rows: sortedUniverseRows(DEFAULT_QUOTE).length,
    restored_pending_identity_rows: restoredPendingSymbols.size,
    restored_snapshot_quarantine: {
      mode: 'persistent_snapshot_fields_only_until_live_all_symbol_identity',
      persistent_snapshot_identity_authoritative: false,
      current_identity_authority: 'binance_official_ws_api_ticker_price_all_symbols',
      enrichment_streams_started_after_live_baseline: periodicEnrichmentStarted,
      loaded: restoredSnapshotQuarantineStats.loaded,
      active: restoredSnapshotQuarantineStats.active,
      loaded_universe_rows: restoredSnapshotQuarantineStats.loaded_universe_rows,
      loaded_ticker_rows: restoredSnapshotQuarantineStats.loaded_ticker_rows,
      loaded_unique_symbols: restoredSnapshotQuarantineStats.loaded_unique_symbols,
      released_matching_symbols: restoredSnapshotQuarantineStats.released_matching_symbols,
      discarded_symbols: restoredSnapshotQuarantineStats.discarded_symbols,
      released_at: restoredSnapshotQuarantineStats.released_at ? iso(restoredSnapshotQuarantineStats.released_at) : null,
    },
    current_identity_reconciliation: {
      mode: 'two_stable_ws_api_all_symbol_latest_price_snapshots_then_admit_enrichment_only',
      confirmations_required: CURRENT_IDENTITY_CONFIRMATIONS_REQUIRED,
      candidate_confirmations: currentIdentityCandidateConfirmations,
      last_candidate_rows: currentIdentityLastCandidateRows,
      last_candidate_at: currentIdentityLastCandidateAt ? iso(currentIdentityLastCandidateAt) : null,
      minimum_rows_now: currentIdentityMinimumRows(),
      stable_overlap_ratio: CURRENT_IDENTITY_STABLE_OVERLAP_RATIO,
      retain_ratio_guard: CURRENT_IDENTITY_MIN_RETAIN_RATIO,
      rejected_partial_snapshots: currentIdentityRejectedPartialSnapshots,
      last_pruned_rows: currentIdentityLastPrunedRows,
      total_pruned_rows: currentIdentityTotalPrunedRows,
      last_pruned_at: currentIdentityLastPrunedAt ? iso(currentIdentityLastPrunedAt) : null,
      last_reconcile_at: lastIdentityReconcileAt ? iso(lastIdentityReconcileAt) : null,
      changed_only_ticker_stream_not_used_as_full_identity: true,
      mark_price_stream_not_used_as_full_identity: true,
      enrichment_blocked_until_identity_confirmed: true,
      contract_info_additions_require_authoritative_baseline: true,
      confirmed_identity_rows: confirmedCurrentIdentitySymbols.size,
      confirmed_identity_at: currentIdentityConfirmedAt ? iso(currentIdentityConfirmedAt) : null,
      mark_price_perpetual_identity_rows: latestMarkPricePerpetualSymbols.size,
      mark_price_perpetual_identity_at: latestMarkPriceIdentityAt ? iso(latestMarkPriceIdentityAt) : null,
    },
    current_price_baseline: {
      transport: 'official_usds_m_websocket_api_ticker_price_all_symbols_authoritative_priced_identity',
      endpoint: 'ws-fapi.binance.com/ws-fapi/v1',
      interval_seconds_after_confirmed: Math.round(CURRENT_PRICE_BASELINE_INTERVAL_MS / 1000),
      retry_seconds_until_confirmed: Math.round(CURRENT_PRICE_BASELINE_RETRY_MS / 1000),
      running: currentPriceBaselineRunning,
      timer_scheduled: Boolean(currentPriceBaselineTimer),
      ...currentPriceBaselineStats,
      last_at: currentPriceBaselineStats.last_at ? iso(currentPriceBaselineStats.last_at) : null,
    },
    restored_at: restoredAt ? iso(restoredAt) : null,
    last_universe_event_at: lastUniverseEventAt ? iso(lastUniverseEventAt) : null,
    last_ticker_event_at: lastTickerEventAt ? iso(lastTickerEventAt) : null,
    last_contract_info_event_at: lastContractInfoEventAt ? iso(lastContractInfoEventAt) : null,
    last_mark_price_event_at: lastMarkPriceEventAt ? iso(lastMarkPriceEventAt) : null,
    last_persist_at: lastPersistAt ? iso(lastPersistAt) : null,
    automatic_rest_enabled: AUTOMATIC_REST_ENABLED,
    rest_last_success_at: restLastSuccessAt ? iso(restLastSuccessAt) : null,
    rest_next_allowed_at: restNextAllowedAt ? iso(restNextAllowedAt) : null,
    rest_last_error: restLastError || null,
    persistence_enabled: supabaseEnabled(),
    snapshot_persist_interval_seconds: Math.round(SNAPSHOT_PERSIST_INTERVAL_MS / 1000),
    identity_mutation_persist_delay_seconds: Math.round(IDENTITY_MUTATION_PERSIST_DELAY_MS / 1000),
    identity_persistence: {
      mutation_generation: identityMutationGeneration,
      persisted_generation: persistedIdentityGeneration,
      pending_generation: Math.max(0, identityMutationGeneration - persistedIdentityGeneration),
      last_mutation_at: lastIdentityMutationAt ? iso(lastIdentityMutationAt) : null,
      last_identity_persist_at: lastIdentityPersistAt ? iso(lastIdentityPersistAt) : null,
      restart_snapshot_ready: persistedIdentityGeneration >= identityMutationGeneration,
      persist_timer_scheduled: Boolean(persistTimer),
      persist_due_at: persistTimerDueAt ? iso(persistTimerDueAt) : null,
    },
    snapshot_persist_stats: {
      ...snapshotPersistStats,
      last_attempt_at: snapshotPersistStats.last_attempt_at ? iso(snapshotPersistStats.last_attempt_at) : null,
    },
    delivery_ws_reuse: {
      mode: 'coin_m_delivery_from_existing_merged_public_futures_websockets', symbol_type_filter: 2,
      rows: deliveryBySymbol.size, last_delivery_event_at: lastDeliveryEventAt ? iso(lastDeliveryEventAt) : null,
      additional_websocket_connections: 0, additional_rest_requests: 0, user_reads_start_connections: false,
      reuses_contract_info_stream: true, reuses_all_market_ticker_stream: true, reuses_mark_price_stream: true,
    },
    websocket_ingress: Object.fromEntries(
      Object.entries(marketWsTraffic).map(([name, value]) => [name, { ...value }]),
    ),
    streams,
    ws_connect_gap_ms: WS_CONNECT_GAP_MS,
    ws_max_connect_attempts_5m: WS_MAX_CONNECT_ATTEMPTS_5M,
    ws_connect_attempts_in_window: (pruneWsConnectAttempts(), wsConnectAttempts.length),
    ws_connect_attempts_total: wsConnectStats.attempts,
    ws_connect_waits: wsConnectStats.waits,
    ws_connect_window_blocks: wsConnectStats.window_blocks,
    production_ws_only: true,
    futures_ws_route_migration: 'market_public_split',
    futures_ws_legacy_root_disabled: true,
    websocket_routes: {
      ticker: 'market_periodic_snapshot',
      bookTicker: 'disabled_redundant',
      contractInfo: 'market_persistent',
      markPrice: 'market_periodic_snapshot',
    },
    websocket_modes: {
      ticker: 'periodic_one_shot',
      bookTicker: 'disabled_redundant_ticker_fallback',
      contractInfo: 'persistent_low_volume',
      markPrice: 'periodic_one_shot',
    },
    market_snapshot_intervals_seconds: {
      ticker: Math.round(MARKET_TICKER_SNAPSHOT_INTERVAL_MS / 1000),
      markPrice: Math.round(MARKET_MARK_PRICE_SNAPSHOT_INTERVAL_MS / 1000),
    },
    funding_current_source: 'binance_official_public_mark_price_websocket_periodic_snapshot',
    source: 'binance_official_ws_api_all_symbol_price_identity_plus_changed_market_enrichment_with_persistent_cold_start_no_automatic_rest',
    time: new Date().toISOString(),
  };
}

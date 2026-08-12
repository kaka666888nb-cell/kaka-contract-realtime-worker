import WebSocket from 'ws';
import { getContractFocusPoolInternalSnapshot } from './contract-focus-pool.mjs';
import { getMarketLightInternalSnapshot } from './market-light-snapshot.mjs';

const VERSION = '650.8.15.1';
const SNAPSHOT_ROUTE = '/api/okx-advanced/current-snapshot';
const HEALTH_ROUTE = '/api/okx-advanced/health';
const BASE = 'https://www.okx.com';
const WS_URL = 'wss://ws.okx.com:8443/ws/v5/public';

const START_DELAY_MS = Math.max(2_000, Number(process.env.KAKA_OKX_ADVANCED_START_DELAY_MS || 10_000));
const STARTUP_RETRY_MS = Math.max(10_000, Number(process.env.KAKA_OKX_ADVANCED_STARTUP_RETRY_MS || 15_000));
const FOCUS_REFRESH_MS = Math.max(2 * 60_000, Number(process.env.KAKA_OKX_ADVANCED_FOCUS_REFRESH_MS || 5 * 60_000));
const SECURITY_FUND_REFRESH_MS = Math.max(60 * 60_000, Number(process.env.KAKA_OKX_ADVANCED_SECURITY_FUND_REFRESH_MS || 6 * 60 * 60_000));
const RESPONSE_CACHE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_OKX_ADVANCED_RESPONSE_CACHE_TTL_MS || 20_000));
const FOCUS_STALE_MS = Math.max(5 * 60_000, Number(process.env.KAKA_OKX_ADVANCED_FOCUS_STALE_MS || 12 * 60_000));
const SECURITY_FUND_STALE_MS = Math.max(12 * 60 * 60_000, Number(process.env.KAKA_OKX_ADVANCED_SECURITY_FUND_STALE_MS || 36 * 60 * 60_000));
const PER_REQUEST_GAP_MS = Math.max(220, Number(process.env.KAKA_OKX_ADVANCED_PER_REQUEST_GAP_MS || 260));
const ADL_ACTIVE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_OKX_ADVANCED_ADL_ACTIVE_TTL_MS || 6_000));
const ADL_HEARTBEAT_MS = Math.max(10_000, Number(process.env.KAKA_OKX_ADVANCED_ADL_HEARTBEAT_MS || 20_000));
const ADL_RECONNECT_MIN_MS = Math.max(1_000, Number(process.env.KAKA_OKX_ADVANCED_ADL_RECONNECT_MIN_MS || 3_000));
const ADL_RECONNECT_MAX_MS = Math.max(10_000, Number(process.env.KAKA_OKX_ADVANCED_ADL_RECONNECT_MAX_MS || 60_000));
const ADL_SUBSCRIPTION_REFRESH_MS = Math.max(10_000, Number(process.env.KAKA_OKX_ADVANCED_ADL_SUBSCRIPTION_REFRESH_MS || 30_000));
const FOCUS_TARGET = 15;

let started = false;
let focusRunning = null;
let securityFundRunning = null;
let focusTimer = null;
let focusRecoveryTimer = null;
let focusInterval = null;
let securityFundTimer = null;
let securityFundRecoveryTimer = null;
let securityFundInterval = null;
let adlSubscriptionTimer = null;
let adlHeartbeatTimer = null;
let adlReconnectTimer = null;
let adlSocket = null;
let adlReconnectDelayMs = ADL_RECONNECT_MIN_MS;
let adlDesiredSignature = '';
let adlLastSubscribeAt = null;
let adlLastMessageAt = null;
let adlLastPongAt = null;
let adlLastError = '';
let adlConnectAttempts = 0;
let adlReconnects = 0;
let adlMessages = 0;
let adlWarningMessages = 0;
let adlSubscriptionAcks = 0;
let adlSubscriptionErrors = 0;
let totalReads = 0;
let responseCacheHits = 0;
let responseCacheMisses = 0;
let totalFocusBuilds = 0;
let totalFocusFailures = 0;
let totalSecurityFundBuilds = 0;
let totalSecurityFundFailures = 0;
let lastFocusStartedAt = null;
let lastFocusCompletedAt = null;
let lastFocusError = '';
let lastSecurityFundStartedAt = null;
let lastSecurityFundCompletedAt = null;
let lastSecurityFundError = '';
let round = 0;

const fundingRows = new Map();
const priceLimitRows = new Map();
const securityFundRows = new Map();
const adlWarningRows = new Map();
const adlSubscribedFamilies = new Set();
const laneStats = new Map();
const responseCache = new Map();

function finite(value) {
  if (value == null) return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function positive(value) {
  const n = finite(value);
  return n != null && n > 0 ? n : null;
}
function compact(raw) {
  return String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}
function isoMs(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? new Date(n).toISOString() : null;
}
function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}
function sleep(ms) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    timer.unref?.();
  });
}
function isFresh(updatedAt, maxAgeMs) {
  const ms = Date.parse(String(updatedAt || ''));
  return Number.isFinite(ms) && Date.now() - ms <= maxAgeMs;
}
function setLane(name, patch) {
  const current = laneStats.get(name) || {
    name,
    attempts: 0,
    successes: 0,
    failures: 0,
    last_started_at: null,
    last_completed_at: null,
    last_error: '',
    last_status: 0,
    last_rows: 0,
  };
  Object.assign(current, patch);
  laneStats.set(name, current);
}
function splitSymbol(symbol) {
  const normalized = compact(symbol);
  if (normalized.endsWith('USDT')) return { base: normalized.slice(0, -4), quote: 'USDT' };
  return { base: '', quote: '' };
}
function nativeSymbol(symbol) {
  const { base, quote } = splitSymbol(symbol);
  return base && quote ? `${base}-${quote}-SWAP` : '';
}
function instFamily(symbol) {
  const { base, quote } = splitSymbol(symbol);
  return base && quote ? `${base}-${quote}` : '';
}

function okxFocusTargets() {
  const focus = getContractFocusPoolInternalSnapshot();
  const rows = (Array.isArray(focus?.rows) ? focus.rows : [])
    .filter((row) => row?.provider === 'okx' && row?.market_type === 'contract')
    .map((row) => ({
      symbol: compact(row?.symbol),
      base_asset: compact(row?.base_asset),
      role: String(row?.role || ''),
      slot: Number(row?.slot || 0),
    }))
    .filter((row) => row.symbol.endsWith('USDT'));
  const unique = [];
  const seen = new Set();
  for (const row of rows) {
    if (!row.symbol || seen.has(row.symbol)) continue;
    seen.add(row.symbol);
    unique.push({
      ...row,
      native_symbol: nativeSymbol(row.symbol),
      inst_family: instFamily(row.symbol),
    });
  }
  return {
    focus_ready: focus?.ready === true,
    focus_round: Number(focus?.round || 0),
    rows: unique.slice(0, FOCUS_TARGET),
  };
}

function okxOpenInterestMap() {
  const market = getMarketLightInternalSnapshot({ market: 'contract', provider: 'okx' });
  const out = new Map();
  for (const row of Array.isArray(market?.rows) ? market.rows : []) {
    const symbol = compact(row?.symbol);
    if (!symbol || !symbol.endsWith('USDT')) continue;
    const oi = finite(row?.open_interest);
    const oiValue = finite(row?.open_interest_value);
    if (oi == null && oiValue == null) continue;
    out.set(symbol, {
      open_interest: oi,
      open_interest_value: oiValue,
      open_interest_unit: row?.open_interest_unit || null,
      open_interest_value_unit: row?.open_interest_value_unit || null,
      open_interest_source: row?.open_interest_source || 'okx_public_open_interest_batch',
      source_time: row?.source_time || row?.updated_at || null,
    });
  }
  return {
    ready: market?.ok === true && market?.stale !== true && !String(market?.last_error || '').trim(),
    shared_round: Number(market?.shared_round || 0),
    map: out,
  };
}

async function fetchJson(path, { lane = 'unknown', timeoutMs = 18_000 } = {}) {
  const startedAt = Date.now();
  setLane(lane, {
    attempts: Number(laneStats.get(lane)?.attempts || 0) + 1,
    last_started_at: new Date(startedAt).toISOString(),
  });
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  timer.unref?.();
  try {
    const response = await fetch(`${BASE}${path}`, {
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3/650.8.15.104 okx-advanced-shared',
      },
      signal: controller.signal,
    });
    const text = await response.text();
    let payload = null;
    try { payload = JSON.parse(text); } catch (_) {}
    if (!response.ok) throw new Error(`okx_http_${response.status}:${lane}`);
    if (String(payload?.code ?? '0') !== '0') {
      throw new Error(`okx_code_${payload?.code ?? 'unknown'}:${String(payload?.msg || '')}:${lane}`);
    }
    setLane(lane, {
      successes: Number(laneStats.get(lane)?.successes || 0) + 1,
      last_status: response.status,
      last_completed_at: new Date().toISOString(),
      last_error: '',
    });
    return payload;
  } catch (error) {
    setLane(lane, {
      failures: Number(laneStats.get(lane)?.failures || 0) + 1,
      last_completed_at: new Date().toISOString(),
      last_error: String(error?.message || error),
    });
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

function parseFunding(payload, target) {
  const row = Array.isArray(payload?.data) ? payload.data[0] : null;
  if (!row || compact(row?.instId) !== compact(target.native_symbol)) return null;
  const fundingRate = finite(row?.fundingRate);
  const fundingTimeMs = Number(row?.fundingTime || 0) || null;
  const sourceTimeMs = Number(row?.ts || 0) || null;
  if (fundingRate == null || fundingTimeMs == null) return null;
  return {
    provider: 'okx',
    market_type: 'contract',
    quote_asset: 'USDT',
    symbol: target.symbol,
    native_symbol: target.native_symbol,
    inst_family: target.inst_family,
    base_asset: target.base_asset,
    focus_role: target.role,
    focus_slot: target.slot,
    funding_rate: fundingRate,
    funding_time_ms: fundingTimeMs,
    funding_time: isoMs(fundingTimeMs),
    next_funding_rate: finite(row?.nextFundingRate),
    next_funding_time_ms: Number(row?.nextFundingTime || 0) || null,
    next_funding_time: isoMs(row?.nextFundingTime),
    settled_funding_rate: finite(row?.settFundingRate),
    settlement_state: String(row?.settState || '') || null,
    interest_rate: finite(row?.interestRate),
    premium: finite(row?.premium),
    min_funding_rate: finite(row?.minFundingRate),
    max_funding_rate: finite(row?.maxFundingRate),
    formula_type: String(row?.formulaType || '') || null,
    method: String(row?.method || '') || null,
    source_time_ms: sourceTimeMs,
    source_time: isoMs(sourceTimeMs),
    official_endpoint: '/api/v5/public/funding-rate',
    source: 'okx_official_public_funding_rate_focus15',
    updated_at: new Date().toISOString(),
  };
}

function parsePriceLimit(payload, target) {
  const row = Array.isArray(payload?.data) ? payload.data[0] : null;
  if (!row || compact(row?.instId) !== compact(target.native_symbol)) return null;
  const enabled = row?.enabled === true || String(row?.enabled).toLowerCase() === 'true';
  const buyLimit = finite(row?.buyLmt);
  const sellLimit = finite(row?.sellLmt);
  const sourceTimeMs = Number(row?.ts || 0) || null;
  if (enabled && (buyLimit == null || sellLimit == null)) return null;
  return {
    provider: 'okx',
    market_type: 'contract',
    quote_asset: 'USDT',
    symbol: target.symbol,
    native_symbol: target.native_symbol,
    inst_family: target.inst_family,
    base_asset: target.base_asset,
    focus_role: target.role,
    focus_slot: target.slot,
    enabled,
    highest_buy_limit: buyLimit,
    lowest_sell_limit: sellLimit,
    source_time_ms: sourceTimeMs,
    source_time: isoMs(sourceTimeMs),
    official_endpoint: '/api/v5/public/price-limit',
    source: 'okx_official_public_price_limit_focus15',
    updated_at: new Date().toISOString(),
  };
}

function parseSecurityFund(payload) {
  const rows = Array.isArray(payload?.data) ? payload.data : [];
  const out = new Map();
  for (const row of rows) {
    const type = String(row?.instType || '').toUpperCase();
    const family = String(row?.instFamily || '').trim().toUpperCase();
    if (type !== 'SWAP' || !family) continue;
    const details = (Array.isArray(row?.details) ? row.details : [])
      .map((item) => ({
        balance: finite(item?.balance),
        amount_change: finite(item?.amt),
        currency: String(item?.ccy || '') || null,
        type: String(item?.type || '') || null,
        ts: Number(item?.ts || 0) || null,
        source_time: isoMs(item?.ts),
      }))
      .filter((item) => item.balance != null || item.amount_change != null || item.ts != null)
      .sort((a, b) => Number(b.ts || 0) - Number(a.ts || 0));
    const latest = details[0] || null;
    out.set(family, {
      inst_type: type,
      inst_family: family,
      total_usd: finite(row?.total),
      latest_balance: latest?.balance ?? null,
      latest_amount_change: latest?.amount_change ?? null,
      latest_currency: latest?.currency ?? null,
      latest_type: latest?.type ?? null,
      latest_time_ms: latest?.ts ?? null,
      latest_time: latest?.source_time ?? null,
      detail_count: details.length,
      details,
      official_endpoint: '/api/v5/public/insurance-fund',
      source: 'okx_official_public_security_fund_daily',
      updated_at: new Date().toISOString(),
    });
  }
  return out;
}

function parseAdlWarningPush(payload) {
  const arg = payload?.arg || {};
  if (String(arg?.channel || '') !== 'adl-warning') return [];
  const rows = Array.isArray(payload?.data) ? payload.data : [];
  return rows.map((row) => {
    const family = String(row?.instFamily || arg?.instFamily || '').trim().toUpperCase();
    const state = String(row?.state || '').trim().toLowerCase();
    if (!family || !['warning', 'adl'].includes(state)) return null;
    const ts = Number(row?.ts || 0) || Date.now();
    return {
      inst_type: String(row?.instType || arg?.instType || 'SWAP').toUpperCase(),
      inst_family: family,
      state,
      security_fund_balance: finite(row?.bal),
      ts,
      source_time: isoMs(ts),
      official_channel: 'adl-warning',
      source: 'okx_official_public_adl_warning_channel',
      observed_at: new Date().toISOString(),
    };
  }).filter(Boolean);
}

function missingFocusTargets(targets) {
  return targets.filter((target) => {
    const funding = fundingRows.get(target.symbol);
    const limit = priceLimitRows.get(target.symbol);
    return !funding || !isFresh(funding.updated_at, FOCUS_STALE_MS) || !limit || !isFresh(limit.updated_at, FOCUS_STALE_MS);
  });
}

async function refreshFocusStats(reason = 'scheduled', { missingOnly = false } = {}) {
  if (focusRunning) return await focusRunning;
  const task = (async () => {
    lastFocusStartedAt = new Date().toISOString();
    lastFocusError = '';
    totalFocusBuilds += 1;
    const focus = okxFocusTargets();
    if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) {
      const error = new Error(`okx_focus_not_ready:${focus.rows.length}/${FOCUS_TARGET}`);
      lastFocusError = error.message;
      totalFocusFailures += 1;
      throw error;
    }
    const targets = missingOnly ? missingFocusTargets(focus.rows) : focus.rows;
    let successes = 0;
    let failures = 0;
    for (const target of targets) {
      try {
        const payload = await fetchJson(`/api/v5/public/funding-rate?instId=${encodeURIComponent(target.native_symbol)}`, { lane: 'funding_rate' });
        const parsed = parseFunding(payload, target);
        if (!parsed) throw new Error(`okx_funding_payload_invalid:${target.symbol}`);
        fundingRows.set(target.symbol, parsed);
        successes += 1;
      } catch (error) {
        failures += 1;
        lastFocusError = String(error?.message || error);
      }
      await sleep(PER_REQUEST_GAP_MS);
      try {
        const payload = await fetchJson(`/api/v5/public/price-limit?instId=${encodeURIComponent(target.native_symbol)}`, { lane: 'price_limit' });
        const parsed = parsePriceLimit(payload, target);
        if (!parsed) throw new Error(`okx_price_limit_payload_invalid:${target.symbol}`);
        priceLimitRows.set(target.symbol, parsed);
        successes += 1;
      } catch (error) {
        failures += 1;
        lastFocusError = String(error?.message || error);
      }
      await sleep(PER_REQUEST_GAP_MS);
    }
    setLane('funding_rate', { last_rows: focus.rows.filter((target) => isFresh(fundingRows.get(target.symbol)?.updated_at, FOCUS_STALE_MS)).length });
    setLane('price_limit', { last_rows: focus.rows.filter((target) => isFresh(priceLimitRows.get(target.symbol)?.updated_at, FOCUS_STALE_MS)).length });
    if (failures > 0 && successes === 0) totalFocusFailures += 1;
    lastFocusCompletedAt = new Date().toISOString();
    round += 1;
    responseCache.clear();
    return successes > 0 || targets.length === 0;
  })();
  focusRunning = task;
  try {
    return await task;
  } finally {
    if (focusRunning === task) focusRunning = null;
  }
}

async function refreshSecurityFund(reason = 'scheduled') {
  if (securityFundRunning) return await securityFundRunning;
  const task = (async () => {
    lastSecurityFundStartedAt = new Date().toISOString();
    lastSecurityFundError = '';
    totalSecurityFundBuilds += 1;
    try {
      const payload = await fetchJson('/api/v5/public/insurance-fund?instType=SWAP', { lane: 'security_fund', timeoutMs: 20_000 });
      const parsed = parseSecurityFund(payload);
      if (!parsed.size) throw new Error('okx_security_fund_rows_empty');
      securityFundRows.clear();
      for (const [family, row] of parsed) securityFundRows.set(family, row);
      setLane('security_fund', { last_rows: parsed.size });
      lastSecurityFundCompletedAt = new Date().toISOString();
      responseCache.clear();
      return true;
    } catch (error) {
      totalSecurityFundFailures += 1;
      lastSecurityFundError = `${reason}:${String(error?.message || error)}`.slice(0, 320);
      return false;
    }
  })();
  securityFundRunning = task;
  try {
    return await task;
  } finally {
    if (securityFundRunning === task) securityFundRunning = null;
  }
}

function adlDesiredFamilies() {
  return okxFocusTargets().rows.map((row) => row.inst_family).filter(Boolean);
}
function adlSignature(families) {
  return [...new Set(families)].sort().join('|');
}
function wsOpen() {
  return adlSocket && adlSocket.readyState === WebSocket.OPEN;
}
function clearAdlHeartbeat() {
  if (adlHeartbeatTimer) clearInterval(adlHeartbeatTimer);
  adlHeartbeatTimer = null;
}
function scheduleAdlReconnect() {
  if (!started || process.env.KAKA_DISABLE_OKX_ADVANCED_ADL_WS === '1' || adlReconnectTimer) return;
  const delay = adlReconnectDelayMs;
  adlReconnectTimer = setTimeout(() => {
    adlReconnectTimer = null;
    adlReconnects += 1;
    connectAdlWarningWs();
  }, delay);
  adlReconnectTimer.unref?.();
  adlReconnectDelayMs = Math.min(ADL_RECONNECT_MAX_MS, Math.max(ADL_RECONNECT_MIN_MS, delay * 2));
}
function sendWs(payload) {
  if (!wsOpen()) return false;
  try {
    adlSocket.send(JSON.stringify(payload));
    return true;
  } catch (error) {
    adlLastError = String(error?.message || error);
    return false;
  }
}
function syncAdlSubscriptions() {
  if (!wsOpen()) return false;
  const desiredFamilies = adlDesiredFamilies();
  if (desiredFamilies.length !== FOCUS_TARGET) return false;
  const desired = new Set(desiredFamilies);
  const remove = [...adlSubscribedFamilies].filter((family) => !desired.has(family));
  const add = [...desired].filter((family) => !adlSubscribedFamilies.has(family));
  if (remove.length) {
    sendWs({ op: 'unsubscribe', args: remove.map((family) => ({ channel: 'adl-warning', instType: 'SWAP', instFamily: family })) });
  }
  if (add.length) {
    sendWs({ op: 'subscribe', args: add.map((family) => ({ channel: 'adl-warning', instType: 'SWAP', instFamily: family })) });
    adlLastSubscribeAt = new Date().toISOString();
  }
  adlDesiredSignature = adlSignature(desiredFamilies);
  return true;
}
function connectAdlWarningWs() {
  if (!started || process.env.KAKA_DISABLE_OKX_ADVANCED_ADL_WS === '1') return;
  if (adlSocket && [WebSocket.OPEN, WebSocket.CONNECTING].includes(adlSocket.readyState)) return;
  adlConnectAttempts += 1;
  const ws = new WebSocket(WS_URL, { handshakeTimeout: 15_000 });
  adlSocket = ws;
  ws.on('open', () => {
    adlReconnectDelayMs = ADL_RECONNECT_MIN_MS;
    adlLastError = '';
    adlSubscribedFamilies.clear();
    syncAdlSubscriptions();
    clearAdlHeartbeat();
    adlHeartbeatTimer = setInterval(() => {
      if (!wsOpen()) return;
      try { adlSocket.send('ping'); } catch (_) {}
    }, ADL_HEARTBEAT_MS);
    adlHeartbeatTimer.unref?.();
  });
  ws.on('message', (buffer) => {
    const text = String(buffer || '');
    adlLastMessageAt = new Date().toISOString();
    adlMessages += 1;
    if (text === 'pong') {
      adlLastPongAt = adlLastMessageAt;
      return;
    }
    let payload = null;
    try { payload = JSON.parse(text); } catch (_) { return; }
    if (payload?.event === 'subscribe') {
      const family = String(payload?.arg?.instFamily || '').toUpperCase();
      if (payload?.arg?.channel === 'adl-warning' && family) {
        adlSubscribedFamilies.add(family);
        adlSubscriptionAcks += 1;
        responseCache.clear();
      }
      return;
    }
    if (payload?.event === 'unsubscribe') {
      const family = String(payload?.arg?.instFamily || '').toUpperCase();
      if (family) adlSubscribedFamilies.delete(family);
      responseCache.clear();
      return;
    }
    if (payload?.event === 'error') {
      adlSubscriptionErrors += 1;
      adlLastError = `okx_adl_ws_${payload?.code || 'error'}:${String(payload?.msg || '')}`;
      return;
    }
    const rows = parseAdlWarningPush(payload);
    for (const row of rows) {
      adlWarningRows.set(row.inst_family, row);
      adlWarningMessages += 1;
    }
    if (rows.length) responseCache.clear();
  });
  ws.on('error', (error) => {
    adlLastError = String(error?.message || error);
  });
  ws.on('close', () => {
    if (adlSocket === ws) adlSocket = null;
    adlSubscribedFamilies.clear();
    clearAdlHeartbeat();
    responseCache.clear();
    scheduleAdlReconnect();
  });
}

function focusStartupReady() {
  const focus = okxFocusTargets();
  if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) return false;
  return missingFocusTargets(focus.rows).length === 0;
}
function scheduleFocusStartupRecovery() {
  if (focusRecoveryTimer || focusStartupReady()) return;
  focusRecoveryTimer = setTimeout(async () => {
    focusRecoveryTimer = null;
    await refreshFocusStats('startup_recovery_missing_only', { missingOnly: true }).catch(() => false);
    if (!focusStartupReady()) scheduleFocusStartupRecovery();
  }, STARTUP_RETRY_MS);
  focusRecoveryTimer.unref?.();
}
function securityFundStartupReady() {
  return securityFundRows.size > 0 && isFresh(lastSecurityFundCompletedAt, SECURITY_FUND_STALE_MS);
}
function scheduleSecurityFundStartupRecovery() {
  if (securityFundRecoveryTimer || securityFundStartupReady()) return;
  securityFundRecoveryTimer = setTimeout(async () => {
    securityFundRecoveryTimer = null;
    await refreshSecurityFund('startup_recovery').catch(() => false);
    if (!securityFundStartupReady()) scheduleSecurityFundStartupRecovery();
  }, Math.max(30_000, STARTUP_RETRY_MS * 2));
  securityFundRecoveryTimer.unref?.();
}

export function startOkxAdvancedStatsScanner() {
  if (started || process.env.KAKA_DISABLE_OKX_ADVANCED_STATS === '1') return;
  started = true;

  focusTimer = setTimeout(async () => {
    await refreshFocusStats('startup').catch(() => false);
    if (!focusStartupReady()) scheduleFocusStartupRecovery();
  }, START_DELAY_MS);
  focusTimer.unref?.();

  securityFundTimer = setTimeout(async () => {
    await refreshSecurityFund('startup').catch(() => false);
    if (!securityFundStartupReady()) scheduleSecurityFundStartupRecovery();
  }, START_DELAY_MS + 1_500);
  securityFundTimer.unref?.();

  focusInterval = setInterval(() => refreshFocusStats('interval').catch(() => {}), FOCUS_REFRESH_MS);
  focusInterval.unref?.();
  securityFundInterval = setInterval(() => refreshSecurityFund('interval').catch(() => {}), SECURITY_FUND_REFRESH_MS);
  securityFundInterval.unref?.();

  const adlStart = setTimeout(() => connectAdlWarningWs(), START_DELAY_MS + 2_500);
  adlStart.unref?.();
  adlSubscriptionTimer = setInterval(() => {
    if (!wsOpen()) connectAdlWarningWs();
    else syncAdlSubscriptions();
  }, ADL_SUBSCRIPTION_REFRESH_MS);
  adlSubscriptionTimer.unref?.();
}

function currentAdlWarning(family) {
  const row = adlWarningRows.get(family);
  if (!row) return null;
  const ts = Number(row.ts || 0);
  return Number.isFinite(ts) && Date.now() - ts <= ADL_ACTIVE_TTL_MS ? row : null;
}

function snapshotPayload({ includeRows = true } = {}) {
  const cacheKey = includeRows ? 'full' : 'meta';
  const cached = responseCache.get(cacheKey);
  if (cached && Date.now() - cached.at <= RESPONSE_CACHE_TTL_MS) {
    responseCacheHits += 1;
    return { ...clone(cached.payload), cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }
  responseCacheMisses += 1;

  const focus = okxFocusTargets();
  const oiState = okxOpenInterestMap();
  const focusRows = focus.rows.map((target) => {
    const funding = fundingRows.get(target.symbol);
    const limit = priceLimitRows.get(target.symbol);
    const oi = oiState.map.get(target.symbol) || null;
    const fund = securityFundRows.get(target.inst_family) || null;
    const adl = currentAdlWarning(target.inst_family);
    return {
      provider: 'okx',
      market_type: 'contract',
      quote_asset: 'USDT',
      symbol: target.symbol,
      native_symbol: target.native_symbol,
      inst_family: target.inst_family,
      base_asset: target.base_asset,
      focus_role: target.role,
      focus_slot: target.slot,
      open_interest: oi?.open_interest ?? null,
      open_interest_value: oi?.open_interest_value ?? null,
      open_interest_unit: oi?.open_interest_unit ?? null,
      open_interest_value_unit: oi?.open_interest_value_unit ?? null,
      open_interest_source: oi?.open_interest_source ?? null,
      open_interest_source_time: oi?.source_time ?? null,
      funding_rate: isFresh(funding?.updated_at, FOCUS_STALE_MS) ? funding?.funding_rate ?? null : null,
      funding: isFresh(funding?.updated_at, FOCUS_STALE_MS) ? clone(funding) : null,
      price_limit: isFresh(limit?.updated_at, FOCUS_STALE_MS) ? clone(limit) : null,
      security_fund: securityFundStartupReady() && fund ? clone(fund) : null,
      adl_warning_state: adl?.state ?? null,
      adl_warning: adl ? clone(adl) : null,
    };
  });

  const coreRows = focusRows.filter((row) => row.focus_role === 'core');
  const oiRowsCount = focusRows.filter((row) => row.open_interest != null || row.open_interest_value != null).length;
  const fundingRowsCount = focusRows.filter((row) => row.funding != null).length;
  const priceLimitCoverageRows = focusRows.filter((row) => row.price_limit != null).length;
  const priceLimitEnabledRows = focusRows.filter((row) => row.price_limit?.enabled === true).length;
  const securityFundFocusRows = focusRows.filter((row) => row.security_fund != null).length;
  const activeAdlRows = focusRows.filter((row) => ['warning', 'adl'].includes(row.adl_warning_state)).length;
  const coreOiRows = coreRows.filter((row) => row.open_interest != null || row.open_interest_value != null).length;
  const coreFundingRows = coreRows.filter((row) => row.funding != null).length;
  const corePriceLimitRows = coreRows.filter((row) => row.price_limit != null).length;
  const desiredFamilies = focus.rows.map((row) => row.inst_family).filter(Boolean);
  const currentSignature = adlSignature(desiredFamilies);
  const subscribedCurrent = desiredFamilies.length === FOCUS_TARGET && desiredFamilies.every((family) => adlSubscribedFamilies.has(family));
  const adlChannelReady = wsOpen() && subscribedCurrent && adlDesiredSignature === currentSignature;
  const lane = Object.fromEntries([...laneStats.entries()].map(([key, value]) => [key, { ...value }]));
  const ready = focus.focus_ready &&
    focusRows.length === FOCUS_TARGET &&
    coreRows.length === 10 &&
    oiState.ready && oiRowsCount === FOCUS_TARGET && coreOiRows === 10 &&
    fundingRowsCount === FOCUS_TARGET && coreFundingRows === 10 &&
    priceLimitCoverageRows === FOCUS_TARGET && corePriceLimitRows === 10 &&
    securityFundStartupReady() && securityFundRows.size > 0 &&
    adlChannelReady;

  const payload = {
    ok: true,
    version: VERSION,
    source: 'render_shared_okx_official_public_advanced_statistics',
    ready,
    focus_target: FOCUS_TARGET,
    focus_round: focus.focus_round,
    focus_symbols: focus.rows.map((row) => row.symbol),
    row_count: focusRows.length,
    core_target_count: coreRows.length,
    open_interest_rows: oiRowsCount,
    core_open_interest_rows: coreOiRows,
    funding_rows: fundingRowsCount,
    core_funding_rows: coreFundingRows,
    price_limit_coverage_rows: priceLimitCoverageRows,
    price_limit_enabled_rows: priceLimitEnabledRows,
    core_price_limit_coverage_rows: corePriceLimitRows,
    security_fund_record_count: securityFundStartupReady() ? securityFundRows.size : 0,
    security_fund_focus_rows: securityFundFocusRows,
    adl_warning_channel_ready: adlChannelReady,
    adl_warning_active_rows: activeAdlRows,
    adl_warning_subscribed_families: adlSubscribedFamilies.size,
    adl_warning_desired_families: desiredFamilies.length,
    focus_refresh_seconds: Math.round(FOCUS_REFRESH_MS / 1000),
    security_fund_refresh_seconds: Math.round(SECURITY_FUND_REFRESH_MS / 1000),
    security_fund_official_update_policy: 'daily_after_settlement_around_08:00_UTC_per_OKX_2026_update',
    per_request_gap_ms: PER_REQUEST_GAP_MS,
    official_endpoint_rate_policy: {
      funding_rate: 'public REST; IP + instrument ID; shared focus15 every 5m',
      price_limit: 'public REST; shared focus15 every 5m',
      security_fund: '10 requests/2s/IP; collector intentionally uses one all-SWAP request every 6h',
      open_interest: 'reused from market-light official SWAP batch; zero duplicate Step994 requests',
      adl_warning: 'one shared public websocket connection; focus15 instFamily subscriptions; no push in normal state',
    },
    official_semantics: {
      open_interest: 'OKX official public open-interest SWAP batch already owned by market-light',
      funding_rate: 'OKX official current funding rate; nextFundingRate may be empty and stays null',
      price_limit: 'OKX official highest buy limit / lowest sell limit; enabled=false is official coverage and limits may be null',
      security_fund: 'OKX official security fund; regular_update removed in 2026; liquidation_balance_deposit/bankruptcy_loss change is daily around 08:00 UTC',
      adl_warning: 'OKX public adl-warning pushes only warning/adl once per second; silence is not fabricated as a per-symbol normal value',
    },
    separate_from_derived_data: true,
    no_cross_provider_substitution: true,
    no_cross_quote_substitution: true,
    missing_stays_null: true,
    collector_only_exchange_requests: true,
    provider_request_governor_reused: true,
    custom_provider_governor_created: false,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_collector: false,
    reads_scale_with_users: false,
    market_light_oi_reused_only: true,
    market_light_oi_additional_exchange_requests: 0,
    adl_warning_normal_state_not_fabricated: true,
    adl_warning_public_no_auth: true,
    adl_warning_one_shared_connection: true,
    last_focus_started_at: lastFocusStartedAt,
    last_focus_completed_at: lastFocusCompletedAt,
    last_focus_error: lastFocusError,
    last_security_fund_started_at: lastSecurityFundStartedAt,
    last_security_fund_completed_at: lastSecurityFundCompletedAt,
    last_security_fund_error: lastSecurityFundError,
    adl_ws: {
      url: WS_URL,
      connected: wsOpen(),
      desired_signature: currentSignature,
      subscribed_current_focus: subscribedCurrent,
      desired_families: desiredFamilies.length,
      subscribed_families: adlSubscribedFamilies.size,
      last_subscribe_at: adlLastSubscribeAt,
      last_message_at: adlLastMessageAt,
      last_pong_at: adlLastPongAt,
      last_error: adlLastError,
      connect_attempts: adlConnectAttempts,
      reconnects: adlReconnects,
      messages: adlMessages,
      warning_messages: adlWarningMessages,
      subscription_acks: adlSubscriptionAcks,
      subscription_errors: adlSubscriptionErrors,
      active_ttl_ms: ADL_ACTIVE_TTL_MS,
    },
    round,
    lane_stats: lane,
    rows: includeRows ? focusRows.map(clone) : [],
    security_fund_rows: includeRows && securityFundStartupReady() ? [...securityFundRows.values()].map(clone) : [],
    timestamp_ms: Date.now(),
  };
  responseCache.set(cacheKey, { at: Date.now(), payload });
  return { ...clone(payload), cache_hit: false, cache_age_ms: 0 };
}

export function getOkxAdvancedStatsHealth() {
  const snapshot = snapshotPayload({ includeRows: false });
  return {
    ...snapshot,
    enabled: started || process.env.KAKA_DISABLE_OKX_ADVANCED_STATS !== '1',
    mode: 'shared_okx_focus15_funding_price_limit_plus_batch_oi_security_fund_and_event_adl',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    total_reads: totalReads,
    total_focus_builds: totalFocusBuilds,
    total_focus_failures: totalFocusFailures,
    total_security_fund_builds: totalSecurityFundBuilds,
    total_security_fund_failures: totalSecurityFundFailures,
    response_cache_ttl_seconds: RESPONSE_CACHE_TTL_MS / 1000,
    response_cache_hits: responseCacheHits,
    response_cache_misses: responseCacheMisses,
    focus_running: Boolean(focusRunning),
    security_fund_running: Boolean(securityFundRunning),
    focus_lock_release_after_completion: true,
    security_fund_lock_release_after_completion: true,
    startup_recovery_until_focus_ready: true,
    startup_recovery_missing_only: true,
    security_fund_startup_recovery: true,
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

export async function handleOkxAdvancedStats(req, res, url) {
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
    sendJson(res, 405, { ok: false, version: VERSION, error: 'method_not_allowed' });
    return true;
  }
  totalReads += 1;
  if (url.pathname === HEALTH_ROUTE) {
    sendJson(res, 200, getOkxAdvancedStatsHealth());
    return true;
  }
  sendJson(res, 200, snapshotPayload({ includeRows: true }));
  return true;
}

export const __okxAdvancedTest = Object.freeze({
  parseFunding,
  parsePriceLimit,
  parseSecurityFund,
  parseAdlWarningPush,
  nativeSymbol,
  instFamily,
});

const VERSION = '650.8.15.163';
const SNAPSHOT_ROUTE = '/api/derivatives-public/current-snapshot';
const HEALTH_ROUTE = '/api/derivatives-public/health';

const REFRESH_MS = Math.max(2 * 60_000, Number(process.env.KAKA_DERIVATIVES_PUBLIC_REFRESH_MS || 5 * 60_000));
const DIRECTORY_REFRESH_MS = Math.max(10 * 60_000, Number(process.env.KAKA_DERIVATIVES_PUBLIC_DIRECTORY_REFRESH_MS || 30 * 60_000));
const START_DELAY_MS = Math.max(5_000, Number(process.env.KAKA_DERIVATIVES_PUBLIC_START_DELAY_MS || 20_000));
const STARTUP_RETRY_MS = Math.max(20_000, Number(process.env.KAKA_DERIVATIVES_PUBLIC_STARTUP_RETRY_MS || 60_000));
const REQUEST_TIMEOUT_MS = Math.max(5_000, Number(process.env.KAKA_DERIVATIVES_PUBLIC_REQUEST_TIMEOUT_MS || 18_000));
const MAX_ROWS_PER_PROVIDER = Math.max(500, Math.min(8_000, Number(process.env.KAKA_DERIVATIVES_PUBLIC_MAX_ROWS_PER_PROVIDER || 8_000)));
const MAX_BYBIT_PAGES = Math.max(1, Math.min(8, Number(process.env.KAKA_DERIVATIVES_PUBLIC_BYBIT_MAX_PAGES || 8)));
const MAX_BYBIT_BASES = Math.max(1, Math.min(24, Number(process.env.KAKA_DERIVATIVES_PUBLIC_BYBIT_MAX_BASES || 12)));
const MAX_GATE_UNDERLYINGS = Math.max(1, Math.min(24, Number(process.env.KAKA_DERIVATIVES_PUBLIC_GATE_MAX_UNDERLYINGS || 12)));
const MAX_OKX_FAMILIES = Math.max(1, Math.min(24, Number(process.env.KAKA_DERIVATIVES_PUBLIC_OKX_MAX_FAMILIES || 12)));
const BYBIT_HV_REFRESH_MS = Math.max(30 * 60_000, Number(process.env.KAKA_DERIVATIVES_PUBLIC_BYBIT_HV_REFRESH_MS || 60 * 60_000));
const BYBIT_HV_STALE_MS = Math.max(BYBIT_HV_REFRESH_MS, Number(process.env.KAKA_DERIVATIVES_PUBLIC_BYBIT_HV_STALE_MS || 2 * 60 * 60_000));

const OKX_BASE = 'https://www.okx.com';
const BYBIT_HOSTS = Object.freeze(['https://api.bybit.com', 'https://api.bytick.com']);
const GATE_BASES = Object.freeze(['https://api.gateio.ws/api/v4', 'https://fx-api.gateio.ws/api/v4']);

const PROVIDER_POLICY = Object.freeze({
  binance: Object.freeze({
    official_available: true,
    current_integration: 'not_collected_step996_existing_binance_safety_policy_preserved',
    reason: 'Binance Options has official public market data, but this Render keeps the established Binance direct-REST hard-disable and does not add a new /market WebSocket path in Step996.',
    direct_rest_used: false,
    direct_ws_added: false,
    source_url: 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/ws-streams/market',
  }),
  okx: Object.freeze({
    official_available: true,
    current_integration: 'shared_background_public_rest',
    source_url: 'https://www.okx.com/docs-v5/en/',
  }),
  bybit: Object.freeze({
    official_available: true,
    current_integration: 'shared_background_public_rest',
    source_url: 'https://bybit-exchange.github.io/docs/v5/market/instrument',
  }),
  gate: Object.freeze({
    official_available: true,
    current_integration: 'shared_background_public_rest',
    source_url: 'https://www.gate.com/docs/developers/apiv4/en/',
  }),
  bitget: Object.freeze({
    official_available: false,
    current_integration: 'crypto_options_not_in_current_public_crypto_product_family',
    reason: 'Bitget UTA public crypto products are spot and futures. Stock+ US stock options are a separate product family and are outside this crypto-derivatives layer.',
    source_url: 'https://www.bitget.com/api-doc/uta/intro',
  }),
});

let started = false;
let running = null;
let startTimer = null;
let refreshTimer = null;
let retryTimer = null;
let nextRefreshAt = 0;
let totalReads = 0;
let totalBuilds = 0;
let totalBuildFailures = 0;
let totalExchangeRequests = 0;
let totalExchangeSuccesses = 0;
let totalExchangeFailures = 0;
let lastStartedAt = null;
let lastCompletedAt = null;
let lastError = '';

const rowsByProvider = new Map();
const directoryByProvider = new Map();
const providerState = new Map();
const bybitHistoricalVolatilityByBase = new Map();

function nowIso() { return new Date().toISOString(); }
function clone(value) { return value == null ? value : JSON.parse(JSON.stringify(value)); }
function finite(value) {
  if (value == null || (typeof value === 'string' && value.trim() === '')) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function text(value) { return String(value ?? '').trim(); }
function upper(value) { return text(value).toUpperCase(); }
function compact(value) { return upper(value).replace(/[^A-Z0-9]/g, ''); }
function isoMs(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? new Date(n).toISOString() : null;
}
function isoSeconds(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? new Date(n * 1000).toISOString() : null;
}
function isFresh(value, ttlMs) {
  const ms = Date.parse(String(value || ''));
  return Number.isFinite(ms) && Date.now() - ms <= ttlMs;
}
function sortRows(rows) {
  return rows.sort((a, b) => {
    const provider = String(a.provider).localeCompare(String(b.provider));
    if (provider) return provider;
    const expiry = String(a.expiry_at || '').localeCompare(String(b.expiry_at || ''));
    if (expiry) return expiry;
    const base = String(a.base_asset || '').localeCompare(String(b.base_asset || ''));
    if (base) return base;
    const strike = Number(a.strike_price ?? Number.POSITIVE_INFINITY) - Number(b.strike_price ?? Number.POSITIVE_INFINITY);
    if (Number.isFinite(strike) && strike !== 0) return strike;
    return String(a.symbol || '').localeCompare(String(b.symbol || ''));
  });
}
function boundedRows(rows) {
  const dedup = new Map();
  for (const row of rows) {
    const key = `${row.provider}|${row.symbol}`;
    if (!row.provider || !row.symbol) continue;
    dedup.set(key, row);
  }
  return sortRows([...dedup.values()]).slice(0, MAX_ROWS_PER_PROVIDER);
}
function parseOptionSymbol(symbol) {
  const raw = upper(symbol);
  const parts = raw.split('-').filter(Boolean);
  if (parts.length < 4) return { option_type: null, strike_price: null, expiry_at: null };
  const sideRaw = parts[parts.length - 1];
  const strike = finite(parts[parts.length - 2]);
  const expiryRaw = parts[parts.length - 3];
  let expiryAt = null;
  if (/^\d{8}$/.test(expiryRaw)) {
    expiryAt = `${expiryRaw.slice(0, 4)}-${expiryRaw.slice(4, 6)}-${expiryRaw.slice(6, 8)}T08:00:00.000Z`;
  } else if (/^\d{6}$/.test(expiryRaw)) {
    const year = Number(expiryRaw.slice(0, 2)) + 2000;
    expiryAt = `${year}-${expiryRaw.slice(2, 4)}-${expiryRaw.slice(4, 6)}T08:00:00.000Z`;
  }
  const optionType = ['C', 'CALL'].includes(sideRaw) ? 'call' : ['P', 'PUT'].includes(sideRaw) ? 'put' : null;
  return { option_type: optionType, strike_price: strike, expiry_at: expiryAt };
}
function splitUnderlying(raw) {
  const value = upper(raw);
  for (const separator of ['-', '_', '/']) {
    if (value.includes(separator)) {
      const parts = value.split(separator).filter(Boolean);
      return { base: compact(parts[0]), quote: compact(parts[1] || '') };
    }
  }
  return { base: '', quote: '' };
}
function stateFor(provider) {
  if (!providerState.has(provider)) {
    providerState.set(provider, {
      provider,
      official_response: false,
      directory_ready: false,
      row_count: 0,
      attempts: 0,
      successes: 0,
      failures: 0,
      last_started_at: null,
      last_completed_at: null,
      last_error: '',
      last_http_status: 0,
      upstream_requests: 0,
      directory_updated_at: null,
      snapshot_updated_at: null,
    });
  }
  return providerState.get(provider);
}
function patchProvider(provider, patch) {
  Object.assign(stateFor(provider), patch);
}

async function fetchJsonUrl(url, provider, lane, timeoutMs = REQUEST_TIMEOUT_MS) {
  totalExchangeRequests += 1;
  const state = stateFor(provider);
  state.attempts += 1;
  state.upstream_requests += 1;
  state.last_started_at = nowIso();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  timer.unref?.();
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        accept: 'application/json',
        'user-agent': `KakaWeb3/${VERSION} derivatives-public-shared`,
        ...(provider === 'gate' ? { 'x-gate-size-decimal': '1' } : {}),
      },
      signal: controller.signal,
    });
    state.last_http_status = Number(response.status || 0);
    const body = await response.text();
    let payload = null;
    try { payload = body ? JSON.parse(body) : null; } catch {}
    if (!response.ok) throw new Error(`${provider}_http_${response.status}:${lane}:${body.slice(0, 160)}`);
    totalExchangeSuccesses += 1;
    state.successes += 1;
    state.last_completed_at = nowIso();
    state.last_error = '';
    return payload;
  } catch (error) {
    totalExchangeFailures += 1;
    state.failures += 1;
    state.last_error = String(error?.message || error).slice(0, 400);
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchBybit(path, lane) {
  let last = null;
  for (const host of BYBIT_HOSTS) {
    try {
      const payload = await fetchJsonUrl(`${host}${path}`, 'bybit', lane);
      if (Number(payload?.retCode ?? -1) !== 0) throw new Error(`bybit_retCode_${payload?.retCode}:${payload?.retMsg || lane}`);
      return payload;
    } catch (error) { last = error; }
  }
  throw last || new Error(`bybit_upstream_unavailable:${lane}`);
}
async function fetchGate(path, lane) {
  let last = null;
  for (const base of GATE_BASES) {
    try { return await fetchJsonUrl(`${base}${path}`, 'gate', lane); }
    catch (error) { last = error; }
  }
  throw last || new Error(`gate_upstream_unavailable:${lane}`);
}
async function fetchOkx(path, lane) {
  const payload = await fetchJsonUrl(`${OKX_BASE}${path}`, 'okx', lane);
  if (String(payload?.code ?? '') !== '0') throw new Error(`okx_code_${payload?.code}:${payload?.msg || lane}`);
  return payload;
}

function parseOkxRows(instrumentsPayload, tickerPayload, oiPayload, optionSummaryPayload = null) {
  const instruments = Array.isArray(instrumentsPayload?.data) ? instrumentsPayload.data : [];
  const tickers = new Map((Array.isArray(tickerPayload?.data) ? tickerPayload.data : []).map((row) => [upper(row?.instId), row]));
  const oiRows = new Map((Array.isArray(oiPayload?.data) ? oiPayload.data : []).map((row) => [upper(row?.instId), row]));
  const summaryRows = new Map((Array.isArray(optionSummaryPayload?.data) ? optionSummaryPayload.data : []).map((row) => [upper(row?.instId), row]));
  const updatedAt = nowIso();
  return instruments.map((item) => {
    const symbol = upper(item?.instId);
    if (!symbol) return null;
    const ticker = tickers.get(symbol) || {};
    const oi = oiRows.get(symbol) || {};
    const summary = summaryRows.get(symbol) || {};
    const parsed = parseOptionSymbol(symbol);
    const underlying = upper(item?.uly || item?.instFamily || '');
    const split = splitUnderlying(underlying);
    const ts = summary?.ts || ticker?.ts || oi?.ts || null;
    return {
      provider: 'okx', market_type: 'option', product_type: 'option', symbol, native_symbol: symbol,
      underlying: underlying || null,
      base_asset: upper(item?.baseCcy) || split.base || null,
      quote_asset: upper(item?.quoteCcy) || split.quote || null,
      settle_asset: upper(item?.settleCcy) || null,
      option_type: String(item?.optType || '').toUpperCase() === 'C' ? 'call' : String(item?.optType || '').toUpperCase() === 'P' ? 'put' : parsed.option_type,
      strike_price: finite(item?.stk) ?? parsed.strike_price,
      expiry_at: isoMs(item?.expTime) || parsed.expiry_at,
      listing_at: isoMs(item?.listTime),
      trading_status: text(item?.state) || null,
      last_price: finite(ticker?.last), mark_price: null, index_price: null, underlying_price: finite(summary?.fwdPx),
      best_bid: finite(ticker?.bidPx), best_ask: finite(ticker?.askPx), best_bid_size: finite(ticker?.bidSz), best_ask_size: finite(ticker?.askSz),
      open_interest: finite(oi?.oi), open_interest_value: finite(oi?.oiUsd),
      volume_24h: finite(ticker?.vol24h), turnover_24h: finite(ticker?.volCcy24h),
      bid_iv: finite(summary?.bidVol), ask_iv: finite(summary?.askVol), mark_iv: finite(summary?.markVol),
      delta: finite(summary?.delta), gamma: finite(summary?.gamma), vega: finite(summary?.vega), theta: finite(summary?.theta), rho: null,
      delta_bs: finite(summary?.deltaBS), gamma_bs: finite(summary?.gammaBS), vega_bs: finite(summary?.vegaBS), theta_bs: finite(summary?.thetaBS),
      leverage: finite(summary?.lever), realized_volatility: finite(summary?.realVol), volatility_level: finite(summary?.volLv), forward_price: finite(summary?.fwdPx),
      official: true, derived: false, identity_derived_from_official_symbol: parsed.option_type != null || parsed.strike_price != null,
      source: 'okx_official_public_option_instruments_tickers_open_interest_opt_summary',
      source_url: PROVIDER_POLICY.okx.source_url,
      source_time: isoMs(ts), updated_at: updatedAt,
    };
  }).filter(Boolean);
}

function parseBybitHistoricalVolatility(payload, baseCoin, quoteCoin = 'USD') {
  // Bybit's official V5 historical-volatility response returns `result` as
  // the row array. Keep the older nested-list shape as a defensive fallback,
  // but always prefer the current official array contract.
  const sourceRows = Array.isArray(payload?.result)
    ? payload.result
    : (Array.isArray(payload?.result?.list) ? payload.result.list : []);
  const rows = sourceRows
    .map((item) => {
      const timestampMs = Number(item?.time || 0);
      return {
        period: Number(item?.period || 0) || null,
        value: finite(item?.value),
        timestamp_ms: Number.isFinite(timestampMs) && timestampMs > 0 ? timestampMs : null,
        source_time: isoMs(timestampMs),
      };
    })
    .filter((item) => item.period != null && item.value != null && item.timestamp_ms != null)
    .sort((a, b) => Number(b.timestamp_ms || 0) - Number(a.timestamp_ms || 0));
  return {
    provider: 'bybit',
    market_type: 'option',
    base_asset: upper(baseCoin),
    quote_asset: upper(quoteCoin) || null,
    official_response: true,
    official_empty: rows.length === 0,
    latest: rows[0] || null,
    rows,
    row_count: rows.length,
    updated_at: nowIso(),
    source: 'bybit_official_public_option_historical_volatility',
    endpoint: '/v5/market/historical-volatility',
  };
}

function bybitOptionQuoteCoinsForBase(instruments, baseCoin) {
  const exactBase = upper(baseCoin);
  return [...new Set((Array.isArray(instruments) ? instruments : [])
    .filter((row) => upper(row?.baseCoin) === exactBase)
    .map((row) => upper(row?.quoteCoin))
    .filter(Boolean))];
}

function parseBybitRows(instrumentRows, tickerRows) {
  const tickers = new Map(tickerRows.map((row) => [upper(row?.symbol), row]));
  const updatedAt = nowIso();
  return instrumentRows.map((item) => {
    const symbol = upper(item?.symbol);
    if (!symbol) return null;
    const ticker = tickers.get(symbol) || {};
    const parsed = parseOptionSymbol(symbol);
    const base = upper(item?.baseCoin) || compact(symbol.split('-')[0]);
    const quote = upper(item?.quoteCoin) || 'USD';
    return {
      provider: 'bybit', market_type: 'option', product_type: 'option', symbol, native_symbol: symbol,
      underlying: base ? `${base}-${quote}` : null,
      base_asset: base || null, quote_asset: quote || null, settle_asset: upper(item?.settleCoin) || null,
      option_type: String(item?.optionsType || '').toLowerCase() === 'call' ? 'call' : String(item?.optionsType || '').toLowerCase() === 'put' ? 'put' : parsed.option_type,
      strike_price: finite(item?.strike) ?? parsed.strike_price,
      expiry_at: isoMs(item?.deliveryTime) || parsed.expiry_at,
      listing_at: isoMs(item?.launchTime), trading_status: text(item?.status) || null,
      last_price: finite(ticker?.lastPrice), mark_price: finite(ticker?.markPrice), index_price: finite(ticker?.indexPrice), underlying_price: finite(ticker?.underlyingPrice),
      best_bid: finite(ticker?.bid1Price), best_ask: finite(ticker?.ask1Price), best_bid_size: finite(ticker?.bid1Size), best_ask_size: finite(ticker?.ask1Size),
      open_interest: finite(ticker?.openInterest), open_interest_value: null,
      volume_24h: finite(ticker?.volume24h), turnover_24h: finite(ticker?.turnover24h),
      bid_iv: finite(ticker?.bid1Iv), ask_iv: finite(ticker?.ask1Iv), mark_iv: finite(ticker?.markIv),
      delta: finite(ticker?.delta), gamma: finite(ticker?.gamma), vega: finite(ticker?.vega), theta: finite(ticker?.theta), rho: null,
      official: true, derived: false, identity_derived_from_official_symbol: parsed.option_type != null || parsed.strike_price != null,
      source: 'bybit_official_public_option_instruments_tickers', source_url: PROVIDER_POLICY.bybit.source_url,
      source_time: null, updated_at: updatedAt,
    };
  }).filter(Boolean);
}

function parseGateRows(underlyings, tickerRows) {
  const underlyingSet = new Set(underlyings.map((value) => upper(typeof value === 'string' ? value : value?.name || value?.underlying)).filter(Boolean));
  const updatedAt = nowIso();
  return tickerRows.map((ticker) => {
    const symbol = upper(ticker?.name || ticker?.contract || ticker?.symbol);
    if (!symbol) return null;
    const parsed = parseOptionSymbol(symbol);
    let underlying = '';
    for (const candidate of underlyingSet) {
      if (symbol.startsWith(`${candidate}-`) || symbol.startsWith(candidate)) { underlying = candidate; break; }
    }
    if (!underlying) underlying = symbol.split('-').slice(0, -3).join('-');
    const split = splitUnderlying(underlying);
    return {
      provider: 'gate', market_type: 'option', product_type: 'option', symbol, native_symbol: symbol,
      underlying: underlying || null, base_asset: split.base || null, quote_asset: split.quote || null, settle_asset: split.quote || null,
      option_type: parsed.option_type, strike_price: parsed.strike_price, expiry_at: parsed.expiry_at,
      listing_at: null, trading_status: 'trading',
      last_price: finite(ticker?.last_price ?? ticker?.lastPrice), mark_price: finite(ticker?.mark_price ?? ticker?.markPrice), index_price: finite(ticker?.index_price ?? ticker?.indexPrice), underlying_price: finite(ticker?.underlying_price ?? ticker?.underlyingPrice),
      best_bid: finite(ticker?.bid1_price ?? ticker?.bid1Price), best_ask: finite(ticker?.ask1_price ?? ticker?.ask1Price), best_bid_size: finite(ticker?.bid1_size ?? ticker?.bid1Size), best_ask_size: finite(ticker?.ask1_size ?? ticker?.ask1Size),
      open_interest: finite(ticker?.position_size ?? ticker?.positionSize), open_interest_value: null,
      volume_24h: finite(ticker?.volume_24h ?? ticker?.volume24h), turnover_24h: finite(ticker?.turnover_24h ?? ticker?.turnover24h),
      bid_iv: finite(ticker?.bid_iv ?? ticker?.bidIv), ask_iv: finite(ticker?.ask_iv ?? ticker?.askIv), mark_iv: finite(ticker?.mark_iv ?? ticker?.markIv),
      delta: finite(ticker?.delta), gamma: finite(ticker?.gamma), vega: finite(ticker?.vega), theta: finite(ticker?.theta), rho: finite(ticker?.rho),
      official: true, derived: false, identity_derived_from_official_symbol: true,
      source: 'gate_official_public_options_tickers', source_url: PROVIDER_POLICY.gate.source_url,
      source_time: null, updated_at: updatedAt,
    };
  }).filter(Boolean);
}


function okxOptionFamilyFromInstId(value) {
  const raw = upper(value);
  const match = raw.match(/^([A-Z0-9]+-[A-Z0-9]+)-/);
  return match ? match[1] : '';
}

function slimOkxOptionInstruments(payload) {
  const data = Array.isArray(payload?.data) ? payload.data.map((row) => ({
    instId: row?.instId,
    uly: row?.uly,
    instFamily: row?.instFamily,
    baseCcy: row?.baseCcy,
    quoteCcy: row?.quoteCcy,
    settleCcy: row?.settleCcy,
    optType: row?.optType,
    stk: row?.stk,
    expTime: row?.expTime,
    listTime: row?.listTime,
    state: row?.state,
  })) : [];
  return { code: '0', data };
}

function slimBybitOptionInstruments(rows) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    symbol: row?.symbol,
    baseCoin: row?.baseCoin,
    quoteCoin: row?.quoteCoin,
    settleCoin: row?.settleCoin,
    optionsType: row?.optionsType,
    strike: row?.strike,
    deliveryTime: row?.deliveryTime,
    launchTime: row?.launchTime,
    status: row?.status,
  }));
}

async function refreshOkx() {
  // OKX currently requires instFamily when Get instruments is called with instType=OPTION.
  // The all-option Tickers endpoint does not require instFamily, so use its official instId set
  // to discover the live option families first, then read Instruments + OI per official family.
  const tickerPayload = await fetchOkx('/api/v5/market/tickers?instType=OPTION', 'option_tickers');
  const tickerRows = Array.isArray(tickerPayload?.data) ? tickerPayload.data : [];
  const tickerSymbols = new Set(tickerRows.map((row) => upper(row?.instId)).filter(Boolean));
  const allFamilies = [...new Set(tickerRows.map((row) => okxOptionFamilyFromInstId(row?.instId)).filter(Boolean))];
  if (tickerRows.length === 0 || allFamilies.length === 0) throw new Error('okx_option_tickers_or_families_empty');

  const families = allFamilies.slice(0, MAX_OKX_FAMILIES);
  const oldDirectory = directoryByProvider.get('okx');
  const oldPayload = oldDirectory?.payload || null;
  const oldRows = Array.isArray(oldPayload?.data) ? oldPayload.data : [];
  const oldFamilies = new Set(oldRows.map((row) => upper(row?.instFamily || row?.uly)).filter(Boolean));
  const directoryNeedsRefresh =
    oldRows.length === 0 ||
    !isFresh(oldDirectory?.updated_at, DIRECTORY_REFRESH_MS) ||
    families.some((family) => !oldFamilies.has(family));

  let instrumentsPayload = oldPayload;
  let directoryFamilySuccesses = 0;
  const partialErrors = [];
  if (directoryNeedsRefresh) {
    const instrumentRows = [];
    for (const instFamily of families) {
      try {
        const payload = await fetchOkx(
          `/api/v5/public/instruments?instType=OPTION&instFamily=${encodeURIComponent(instFamily)}`,
          `option_instruments_${instFamily}`,
        );
        if (Array.isArray(payload?.data)) instrumentRows.push(...payload.data);
        directoryFamilySuccesses += 1;
      } catch (error) {
        partialErrors.push(`directory:${instFamily}:${String(error?.message || error)}`);
      }
    }
    if (instrumentRows.length === 0) throw new Error(`okx_option_instruments_empty:${partialErrors.join('|').slice(0, 300)}`);
    instrumentsPayload = slimOkxOptionInstruments({ code: '0', data: instrumentRows });
    directoryByProvider.set('okx', { payload: instrumentsPayload, updated_at: nowIso() });
  } else {
    directoryFamilySuccesses = families.length;
  }

  const instrumentsAll = Array.isArray(instrumentsPayload?.data) ? instrumentsPayload.data : [];
  // Keep the current public market scope aligned with the official all-option ticker identity set.
  // This avoids retaining an expired directory row during the directory TTL window.
  const instruments = instrumentsAll.filter((row) => tickerSymbols.has(upper(row?.instId)));
  const currentInstrumentsPayload = { code: '0', data: instruments };

  const oiData = [];
  const optionSummaryData = [];
  let oiFamilySuccesses = 0;
  let optionSummaryFamilySuccesses = 0;
  for (const instFamily of families) {
    try {
      const payload = await fetchOkx(
        `/api/v5/public/open-interest?instType=OPTION&instFamily=${encodeURIComponent(instFamily)}`,
        `option_open_interest_${instFamily}`,
      );
      if (Array.isArray(payload?.data)) oiData.push(...payload.data);
      oiFamilySuccesses += 1;
    } catch (error) {
      partialErrors.push(`oi:${instFamily}:${String(error?.message || error)}`);
    }
    try {
      const payload = await fetchOkx(
        `/api/v5/public/opt-summary?instFamily=${encodeURIComponent(instFamily)}`,
        `option_summary_${instFamily}`,
      );
      if (Array.isArray(payload?.data)) optionSummaryData.push(...payload.data);
      optionSummaryFamilySuccesses += 1;
    } catch (error) {
      partialErrors.push(`opt-summary:${instFamily}:${String(error?.message || error)}`);
    }
  }

  const oiPayload = { code: '0', data: oiData };
  const optionSummaryPayload = { code: '0', data: optionSummaryData };
  const normalized = parseOkxRows(currentInstrumentsPayload, tickerPayload, oiPayload, optionSummaryPayload);
  const rowsTruncated = normalized.length > MAX_ROWS_PER_PROVIDER;
  const rows = boundedRows(normalized);
  if (rows.length === 0) throw new Error('okx_option_normalized_rows_empty');
  rowsByProvider.set('okx', rows);
  patchProvider('okx', {
    official_response: true,
    directory_ready: instruments.length > 0,
    row_count: rows.length,
    option_ticker_count: tickerRows.length,
    option_family_count: allFamilies.length,
    option_family_polled: families.length,
    directory_family_successes: directoryFamilySuccesses,
    directory_family_failures: families.length - directoryFamilySuccesses,
    scope_truncated: allFamilies.length > families.length,
    rows_truncated: rowsTruncated,
    oi_family_successes: oiFamilySuccesses,
    oi_family_failures: families.length - oiFamilySuccesses,
    option_summary_family_successes: optionSummaryFamilySuccesses,
    option_summary_family_failures: families.length - optionSummaryFamilySuccesses,
    option_summary_rows: optionSummaryData.length,
    option_summary_ready: optionSummaryFamilySuccesses === families.length && optionSummaryData.length > 0,
    partial_error: partialErrors.join(' | ').slice(0, 800),
    directory_updated_at: directoryByProvider.get('okx')?.updated_at || null,
    snapshot_updated_at: nowIso(),
    last_error: '',
  });
}

async function loadBybitInstruments() {
  const rows = [];
  let cursor = '';
  let truncated = false;
  let pages = 0;
  for (let page = 0; page < MAX_BYBIT_PAGES; page += 1) {
    const query = new URLSearchParams({ category: 'option', limit: '1000' });
    if (cursor) query.set('cursor', cursor);
    const payload = await fetchBybit(`/v5/market/instruments-info?${query}`, `option_instruments_page_${page + 1}`);
    pages += 1;
    const list = Array.isArray(payload?.result?.list) ? payload.result.list : [];
    rows.push(...list);
    const next = text(payload?.result?.nextPageCursor);
    if (!next || next === cursor || list.length === 0) { cursor = ''; break; }
    cursor = next;
  }
  if (cursor) truncated = true;
  return { rows, truncated, pages };
}

async function refreshBybit() {
  const oldDirectory = directoryByProvider.get('bybit');
  let instruments = oldDirectory?.rows || null;
  let directoryTruncated = oldDirectory?.truncated === true;
  let directoryPages = Number(oldDirectory?.pages || 0);
  if (!Array.isArray(instruments) || !isFresh(oldDirectory?.updated_at, DIRECTORY_REFRESH_MS)) {
    const loaded = await loadBybitInstruments();
    instruments = slimBybitOptionInstruments(loaded.rows);
    directoryTruncated = loaded.truncated === true;
    directoryPages = Number(loaded.pages || 0);
    directoryByProvider.set('bybit', { rows: instruments, truncated: directoryTruncated, pages: directoryPages, updated_at: nowIso() });
  }
  const allBases = [...new Set(instruments.map((row) => upper(row?.baseCoin)).filter(Boolean))];
  const bases = allBases.slice(0, MAX_BYBIT_BASES);
  const tickerRows = [];
  const partialErrors = [];
  let tickerBaseSuccesses = 0;
  let historicalVolatilityBaseSuccesses = 0;
  for (const baseCoin of bases) {
    try {
      const payload = await fetchBybit(`/v5/market/tickers?category=option&baseCoin=${encodeURIComponent(baseCoin)}`, `option_tickers_${baseCoin}`);
      tickerRows.push(...(Array.isArray(payload?.result?.list) ? payload.result.list : []));
      tickerBaseSuccesses += 1;
    } catch (error) {
      partialErrors.push(`ticker:${baseCoin}:${String(error?.message || error)}`);
    }
    const quoteCoins = bybitOptionQuoteCoinsForBase(instruments, baseCoin);
    const previousHv = bybitHistoricalVolatilityByBase.get(baseCoin);
    if (previousHv?.official_response === true && quoteCoins.includes(upper(previousHv.quote_asset)) && isFresh(previousHv.updated_at, BYBIT_HV_REFRESH_MS)) {
      historicalVolatilityBaseSuccesses += 1;
    } else {
      let verified = null;
      const quoteErrors = [];
      for (const quoteCoin of quoteCoins) {
        try {
          const payload = await fetchBybit(
            `/v5/market/historical-volatility?category=option&baseCoin=${encodeURIComponent(baseCoin)}&quoteCoin=${encodeURIComponent(quoteCoin)}&period=7`,
            `option_historical_volatility_${baseCoin}_${quoteCoin}`,
          );
          const parsed = parseBybitHistoricalVolatility(payload, baseCoin, quoteCoin);
          if (parsed.official_empty) throw new Error(`bybit_option_historical_volatility_empty:${baseCoin}:${quoteCoin}`);
          verified = parsed;
          break;
        } catch (error) {
          quoteErrors.push(`${quoteCoin}:${String(error?.message || error)}`);
        }
      }
      if (verified != null) {
        bybitHistoricalVolatilityByBase.set(baseCoin, verified);
        historicalVolatilityBaseSuccesses += 1;
      } else {
        const reason = quoteCoins.length === 0 ? 'official_option_directory_quote_empty' : quoteErrors.join(',');
        partialErrors.push(`historical-volatility:${baseCoin}:${reason}`);
      }
    }
  }
  const normalized = parseBybitRows(instruments, tickerRows);
  const rowsTruncated = normalized.length > MAX_ROWS_PER_PROVIDER;
  const rows = boundedRows(normalized);
  rowsByProvider.set('bybit', rows);
  patchProvider('bybit', {
    official_response: true,
    directory_ready: instruments.length > 0,
    row_count: rows.length,
    directory_pages: directoryPages,
    option_base_count: allBases.length,
    option_base_polled: bases.length,
    scope_truncated: directoryTruncated || allBases.length > bases.length,
    rows_truncated: rowsTruncated,
    ticker_base_successes: tickerBaseSuccesses,
    ticker_base_failures: bases.length - tickerBaseSuccesses,
    historical_volatility_base_successes: historicalVolatilityBaseSuccesses,
    historical_volatility_base_failures: bases.length - historicalVolatilityBaseSuccesses,
    historical_volatility_ready: bases.length > 0 && historicalVolatilityBaseSuccesses === bases.length,
    historical_volatility_refresh_seconds: Math.round(BYBIT_HV_REFRESH_MS / 1000),
    historical_volatility_stale_seconds: Math.round(BYBIT_HV_STALE_MS / 1000),
    partial_error: partialErrors.join(' | ').slice(0, 800),
    directory_updated_at: directoryByProvider.get('bybit')?.updated_at || null,
    snapshot_updated_at: nowIso(),
    last_error: '',
  });
}

async function loadGateUnderlyings() {
  const payload = await fetchGate('/options/underlyings', 'option_underlyings');
  if (!Array.isArray(payload)) throw new Error('gate_options_underlyings_not_array');
  return payload;
}
async function refreshGate() {
  const oldDirectory = directoryByProvider.get('gate');
  let underlyings = oldDirectory?.rows || null;
  if (!Array.isArray(underlyings) || !isFresh(oldDirectory?.updated_at, DIRECTORY_REFRESH_MS)) {
    underlyings = await loadGateUnderlyings();
    directoryByProvider.set('gate', { rows: underlyings, updated_at: nowIso() });
  }
  const allNames = underlyings.map((value) => upper(typeof value === 'string' ? value : value?.name || value?.underlying)).filter(Boolean);
  const names = allNames.slice(0, MAX_GATE_UNDERLYINGS);
  const tickerRows = [];
  const partialErrors = [];
  let tickerUnderlyingSuccesses = 0;
  if (names.length === 0) throw new Error('gate_options_underlyings_empty');
  for (const underlying of names) {
    try {
      const payload = await fetchGate(`/options/tickers?underlying=${encodeURIComponent(underlying)}`, `option_tickers_${underlying}`);
      if (Array.isArray(payload)) tickerRows.push(...payload);
      tickerUnderlyingSuccesses += 1;
    } catch (error) {
      partialErrors.push(`ticker:${underlying}:${String(error?.message || error)}`);
    }
  }
  const normalized = parseGateRows(names, tickerRows);
  const rowsTruncated = normalized.length > MAX_ROWS_PER_PROVIDER;
  const rows = boundedRows(normalized);
  rowsByProvider.set('gate', rows);
  patchProvider('gate', {
    official_response: true,
    directory_ready: allNames.length > 0,
    row_count: rows.length,
    option_underlying_count: allNames.length,
    option_underlying_polled: names.length,
    scope_truncated: allNames.length > names.length,
    rows_truncated: rowsTruncated,
    ticker_underlying_successes: tickerUnderlyingSuccesses,
    ticker_underlying_failures: names.length - tickerUnderlyingSuccesses,
    partial_error: partialErrors.join(' | ').slice(0, 800),
    directory_updated_at: directoryByProvider.get('gate')?.updated_at || null,
    snapshot_updated_at: nowIso(),
    last_error: '',
  });
}

async function refreshProvider(provider, fn) {
  try { await fn(); }
  catch (error) {
    patchProvider(provider, { official_response: false, last_error: String(error?.message || error).slice(0, 400) });
    throw error;
  }
}

async function buildCycle() {
  if (running) return running;
  running = (async () => {
    totalBuilds += 1;
    lastStartedAt = nowIso();
    const failures = [];
    const providerJobs = [['okx', refreshOkx], ['bybit', refreshBybit], ['gate', refreshGate]];
    const settled = await Promise.allSettled(providerJobs.map(async ([provider, fn]) => {
      try {
        await refreshProvider(provider, fn);
        return { provider, ok: true };
      } catch (error) {
        failures.push(`${provider}:${String(error?.message || error)}`);
        return { provider, ok: false };
      }
    }));
    void settled;
    lastCompletedAt = nowIso();
    if (failures.length) {
      totalBuildFailures += 1;
      lastError = failures.join(' | ').slice(0, 800);
    } else {
      lastError = '';
    }
    nextRefreshAt = Date.now() + REFRESH_MS;
  })().finally(() => { running = null; });
  return running;
}

export function startDerivativesPublicScanner() {
  if (started) return;
  started = true;
  startTimer = setTimeout(() => {
    buildCycle().catch(() => {
      retryTimer = setTimeout(() => buildCycle().catch(() => {}), STARTUP_RETRY_MS);
      retryTimer.unref?.();
    });
    refreshTimer = setInterval(() => buildCycle().catch(() => {}), REFRESH_MS);
    refreshTimer.unref?.();
  }, START_DELAY_MS);
  startTimer.unref?.();
}

function providerHealth(provider) {
  const state = clone(stateFor(provider));
  if (provider === 'binance' || provider === 'bitget') {
    return {
      provider,
      official_available: PROVIDER_POLICY[provider].official_available,
      current_integration: PROVIDER_POLICY[provider].current_integration,
      reason: PROVIDER_POLICY[provider].reason || null,
      row_count: 0,
      upstream_requests: 0,
      direct_rest_used: provider === 'binance' ? false : null,
      direct_ws_added: provider === 'binance' ? false : null,
      ready: true,
    };
  }
  return {
    ...state,
    official_available: true,
    current_integration: PROVIDER_POLICY[provider].current_integration,
    ready: state.official_response === true && state.directory_ready === true && state.row_count > 0 && state.scope_truncated !== true && state.rows_truncated !== true && !state.last_error &&
      (provider !== 'okx' || state.option_summary_ready === true) &&
      (provider !== 'bybit' || state.historical_volatility_ready === true),
  };
}

export function getDerivativesPublicHealth() {
  const okx = providerHealth('okx');
  const bybit = providerHealth('bybit');
  const gate = providerHealth('gate');
  const binance = providerHealth('binance');
  const bitget = providerHealth('bitget');
  return {
    ok: true,
    version: VERSION,
    step: 'Step1004.5.1 fixes OKX option family discovery and closes original Step996 public options/more-complete derivatives gap',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    collector_role: 'slow-stats',
    shared_background_collector: true,
    provider_refresh_isolated: true,
    provider_refresh_scheduling: 'parallel_all_settled_provider_governor_bounded',
    user_reads_trigger_collector: false,
    user_reads_trigger_exchange_requests: false,
    reads_scale_with_users: false,
    user_read_exchange_request_count: 0,
    total_reads: totalReads,
    total_builds: totalBuilds,
    total_build_failures: totalBuildFailures,
    total_exchange_requests: totalExchangeRequests,
    total_exchange_successes: totalExchangeSuccesses,
    total_exchange_failures: totalExchangeFailures,
    cycle_running: running != null,
    last_started_at: lastStartedAt,
    last_completed_at: lastCompletedAt,
    next_refresh_at: nextRefreshAt ? new Date(nextRefreshAt).toISOString() : null,
    last_error: lastError,
    max_rows_per_provider: MAX_ROWS_PER_PROVIDER,
    max_okx_families: MAX_OKX_FAMILIES,
    max_bybit_pages: MAX_BYBIT_PAGES,
    max_bybit_bases: MAX_BYBIT_BASES,
    max_gate_underlyings: MAX_GATE_UNDERLYINGS,
    current_scope_not_truncated: [okx, bybit, gate].every((item) => item.scope_truncated !== true && item.rows_truncated !== true),
    providers: { binance, okx, bybit, gate, bitget },
    supported_crypto_option_providers: ['okx', 'bybit', 'gate'],
    okx_option_summary_official_fields: ['bidVol', 'askVol', 'markVol', 'delta', 'gamma', 'vega', 'theta', 'deltaBS', 'gammaBS', 'vegaBS', 'thetaBS', 'lever', 'realVol', 'volLv', 'fwdPx'],
    bybit_option_historical_volatility_official_fields: ['period', 'value', 'time'],
    explicit_non_collected_or_unsupported: ['binance', 'bitget'],
    binance_direct_rest_added: false,
    binance_option_ws_added: false,
    bitget_stockplus_options_excluded_from_crypto_scope: true,
    official_only: true,
    derived_metrics_fabricated: false,
    cross_provider_substitution: false,
    cross_quote_substitution: false,
    missing_stays_null: true,
    ready: okx.ready === true && bybit.ready === true && gate.ready === true && binance.ready === true && bitget.ready === true,
    timestamp_ms: Date.now(),
  };
}

function snapshotRows(url) {
  const provider = String(url.searchParams.get('provider') || '').trim().toLowerCase();
  const base = compact(url.searchParams.get('base_asset') || '');
  const optionType = String(url.searchParams.get('option_type') || '').trim().toLowerCase();
  let rows = [];
  for (const key of ['okx', 'bybit', 'gate']) rows.push(...(rowsByProvider.get(key) || []));
  if (provider) rows = rows.filter((row) => row.provider === provider);
  if (base) rows = rows.filter((row) => compact(row.base_asset) === base);
  if (['call', 'put'].includes(optionType)) rows = rows.filter((row) => row.option_type === optionType);
  const offset = Math.max(0, Math.min(100_000, Number(url.searchParams.get('offset') || 0) || 0));
  const limit = Math.max(1, Math.min(1_000, Number(url.searchParams.get('limit') || 200) || 200));
  return { total: rows.length, offset, limit, rows: rows.slice(offset, offset + limit).map((row) => ({ ...row })) };
}

function snapshotBybitHistoricalVolatility(url) {
  const base = compact(url.searchParams.get('base_asset') || '');
  const rows = [...bybitHistoricalVolatilityByBase.values()]
    .filter((item) => item?.official_response === true && isFresh(item.updated_at, BYBIT_HV_STALE_MS))
    .filter((item) => !base || compact(item.base_asset) === base)
    .map(clone);
  return rows;
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

export async function handleDerivativesPublic(req, res, url) {
  if (![SNAPSHOT_ROUTE, HEALTH_ROUTE].includes(url.pathname)) return false;
  if (req.method === 'OPTIONS') { res.writeHead(204, { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET, OPTIONS', 'cache-control': 'no-store' }); res.end(); return true; }
  if (req.method !== 'GET') { sendJson(res, 405, { ok: false, error: 'method_not_allowed' }); return true; }
  totalReads += 1;
  if (url.pathname === HEALTH_ROUTE) { sendJson(res, 200, getDerivativesPublicHealth()); return true; }
  const page = snapshotRows(url);
  sendJson(res, 200, {
    ok: true,
    version: VERSION,
    source: 'kaka_shared_official_public_crypto_options',
    product_type: 'option',
    total_rows: page.total,
    offset: page.offset,
    limit: page.limit,
    rows: page.rows,
    option_historical_volatility: snapshotBybitHistoricalVolatility(url),
    provider_policy: PROVIDER_POLICY,
    user_read_triggered_exchange_requests: 0,
    reads_scale_with_users: false,
    timestamp_ms: Date.now(),
  });
  return true;
}

export const __testDerivativesPublic = Object.freeze({ parseOptionSymbol, okxOptionFamilyFromInstId, parseOkxRows, parseBybitRows, parseGateRows, parseBybitHistoricalVolatility, bybitOptionQuoteCoinsForBase });

// KakaWeb3 Step1058: delivery-futures expiry + current delivery price + annualized delivery basis.
// Extends the existing shared contract-basis scanner without changing its legacy perpetual contract.
// Legacy /api/contract-basis/current-snapshot fields remain backward-compatible.
// Delivery collection is backend-shared and runs on a bounded five-minute cadence; user reads never start exchange work.
// Binance COIN-M delivery reuses the already-running merged Binance public futures WebSocket streams; Binance REST remains permanently disabled.

import { getMarketLightInternalSnapshot } from './market-light-bridge.mjs';
import { getBinanceDeliveryContractsSnapshot } from './binance-contract-market.mjs';

const STEP_VERSION = '650.8.15.197.3.3.32.6.1-basis-partial-binance-warm';
const CURRENT_ROUTE = '/api/contract-basis/current-snapshot';
const HEALTH_ROUTE = '/api/contract-basis/health';
const PROVIDERS = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const DELIVERY_SOURCE_PROVIDERS = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const REFRESH_INTERVAL_MS = Math.max(15_000, Number(process.env.KAKA_CONTRACT_BASIS_REFRESH_INTERVAL_MS || 30_000));
const START_DELAY_MS = Math.max(5_000, Number(process.env.KAKA_CONTRACT_BASIS_START_DELAY_MS || 12_000));
const RESPONSE_CACHE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_CONTRACT_BASIS_RESPONSE_CACHE_TTL_MS || 20_000));
const INPUT_STALE_MS = Math.max(60_000, Number(process.env.KAKA_CONTRACT_BASIS_INPUT_STALE_MS || 3 * 60_000));
const BASIS_SIDE_FRESH_MS = Math.max(60_000, Number(process.env.KAKA_CONTRACT_BASIS_SIDE_FRESH_MS || 30 * 60_000));
const BASIS_MAX_SKEW_MS = Math.max(60_000, Number(process.env.KAKA_CONTRACT_BASIS_MAX_SKEW_MS || 15 * 60_000));
const DELIVERY_REFRESH_INTERVAL_MS = Math.max(
  60_000,
  Number(process.env.KAKA_CONTRACT_DELIVERY_BASIS_REFRESH_INTERVAL_MS || 5 * 60_000),
);
const DELIVERY_FETCH_TIMEOUT_MS = Math.max(
  3_000,
  Number(process.env.KAKA_CONTRACT_DELIVERY_BASIS_FETCH_TIMEOUT_MS || 8_000),
);
const DELIVERY_MAX_PROVIDER_PAGES = 4;
const MIN_ANNUALIZE_REMAINING_MS = 60 * 60_000;
const YEAR_MS = 365 * 24 * 60 * 60_000;
const MIN_READY_PROVIDERS = Math.max(1, Math.min(
  PROVIDERS.length,
  Number(process.env.KAKA_CONTRACT_BASIS_MIN_READY_PROVIDERS || 4),
));
const BINANCE_DELIVERY_WARM_RETRY_MS = Math.max(15_000, Number(
  process.env.KAKA_BINANCE_DELIVERY_WARM_RETRY_MS || 30_000,
));

let started = false;
let running = false;
let timer = null;
let interval = null;
let round = 0;
let lastStartedAt = null;
let lastCompletedAt = null;
let lastError = '';
let totalBuilds = 0;
let totalBuildFailures = 0;
let totalSnapshotReads = 0;
let responseCacheHits = 0;
let responseCacheMisses = 0;
let latestVerifiedSnapshot = null;
const responseCache = new Map();

let deliveryRunning = false;
let deliveryLastStartedAt = null;
let deliveryLastCompletedAt = null;
let deliveryLastError = '';
let deliveryTotalRefreshes = 0;
let deliveryTotalRefreshFailures = 0;
let deliveryTotalUpstreamRequests = 0;
let latestDeliverySnapshot = null;
let binanceDeliveryWarmLastAttemptAt = 0;
let binanceDeliveryWarmAttempts = 0;
let binanceDeliveryWarmSuccesses = 0;
let binanceDeliveryWarmLastError = '';

function compact(value) {
  return String(value ?? '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function finite(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function positive(value) {
  const number = finite(value);
  return number != null && number > 0 ? number : null;
}

function timeMs(value) {
  if (value == null || value === '') return 0;
  const number = Number(value);
  if (Number.isFinite(number) && number > 0) {
    return number < 10_000_000_000 ? number * 1000 : number;
  }
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

function rowTimeMs(row) {
  for (const value of [row?.source_time, row?.cached_at, row?.updated_at, row?.requested_at]) {
    const parsed = timeMs(value);
    if (parsed > 0) return parsed;
  }
  return 0;
}

function isoOrNull(ms) {
  return Number.isFinite(ms) && ms > 0 ? new Date(ms).toISOString() : null;
}

function percentage(numerator, denominator) {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) return null;
  const value = (numerator / denominator) * 100;
  return Number.isFinite(value) ? value : null;
}

function inputReady(snapshot, expectedQuote = 'USDT') {
  if (!snapshot?.ok) return false;
  if (String(snapshot.primary_quote || '').toUpperCase() !== expectedQuote) return false;
  if (snapshot.stale === true) return false;
  if (String(snapshot.last_error || '').trim()) return false;
  const rows = Number(snapshot.row_count || 0);
  const directory = Number(snapshot.directory_count || 0);
  if (rows <= 0 || directory <= 0 || rows !== directory) return false;
  const updatedMs = timeMs(snapshot.updated_at);
  if (updatedMs <= 0 || Date.now() - updatedMs > INPUT_STALE_MS) return false;
  return true;
}

function normalizeSpotMap(rows) {
  const byBase = new Map();
  for (const raw of Array.isArray(rows) ? rows : []) {
    const quote = compact(raw?.quote_asset ?? raw?.quote_symbol);
    if (quote !== 'USDT') continue;
    const base = compact(raw?.base_asset);
    const price = positive(raw?.last_price ?? raw?.price);
    if (!base || price == null) continue;
    const sourceMs = rowTimeMs(raw);
    const current = byBase.get(base);
    if (!current || sourceMs >= current.sourceMs) {
      byBase.set(base, { row: raw, price, sourceMs });
    }
  }
  return byBase;
}

export function buildContractBasisSnapshotFromInputs(inputByProvider, { nowMs = Date.now(), sharedRound = 0 } = {}) {
  const providerCoverage = {};
  const rows = [];
  let allInputsReady = true;
  let totalContractDirectory = 0;
  let totalSpotDirectory = 0;
  let markIndexRowCount = 0;
  let perpetualSpotRowCount = 0;

  for (const provider of PROVIDERS) {
    const spot = inputByProvider?.[provider]?.spot || null;
    const contract = inputByProvider?.[provider]?.contract || null;
    const spotReady = inputReady(spot);
    const contractReady = inputReady(contract);
    const ready = spotReady && contractReady;
    if (!ready) allInputsReady = false;

    const spotDirectory = Number(spot?.directory_count || 0);
    const contractDirectory = Number(contract?.directory_count || 0);
    totalSpotDirectory += spotDirectory;
    totalContractDirectory += contractDirectory;

    let providerMarkIndex = 0;
    let providerPerpetualSpot = 0;
    let missingSpot = 0;
    let missingMark = 0;
    let staleOrSkewed = 0;
    const spotByBase = normalizeSpotMap(spot?.rows || []);

    if (ready) {
      for (const contractRow of Array.isArray(contract?.rows) ? contract.rows : []) {
        const quote = compact(contractRow?.quote_asset ?? contractRow?.quote_symbol);
        if (quote !== 'USDT') continue;
        const symbol = compact(contractRow?.symbol);
        const base = compact(contractRow?.base_asset);
        if (!symbol || !base) continue;

        const markPrice = positive(contractRow?.mark_price);
        const indexPrice = positive(contractRow?.index_price);
        const contractSourceMs = rowTimeMs(contractRow);
        const markIndexPercent = markPrice != null && indexPrice != null
          ? percentage(markPrice - indexPrice, indexPrice)
          : null;
        if (markIndexPercent != null) providerMarkIndex += 1;

        let spotPrice = null;
        let spotSourceMs = 0;
        let skewMs = null;
        let perpetualSpotPercent = null;
        const spotMatch = spotByBase.get(base) || null;
        if (markPrice == null) {
          missingMark += 1;
        } else if (!spotMatch) {
          missingSpot += 1;
        } else {
          spotPrice = spotMatch.price;
          spotSourceMs = spotMatch.sourceMs;
          skewMs = contractSourceMs > 0 && spotSourceMs > 0 ? Math.abs(contractSourceMs - spotSourceMs) : null;
          const contractFresh = contractSourceMs > 0 && nowMs - contractSourceMs <= BASIS_SIDE_FRESH_MS;
          const spotFresh = spotSourceMs > 0 && nowMs - spotSourceMs <= BASIS_SIDE_FRESH_MS;
          const skewOk = skewMs != null && skewMs <= BASIS_MAX_SKEW_MS;
          if (contractFresh && spotFresh && skewOk) {
            perpetualSpotPercent = percentage(markPrice - spotPrice, spotPrice);
            if (perpetualSpotPercent != null) providerPerpetualSpot += 1;
          } else {
            staleOrSkewed += 1;
          }
        }

        if (markIndexPercent == null && perpetualSpotPercent == null) continue;
        rows.push({
          provider,
          market_type: 'contract',
          symbol,
          base_asset: base,
          quote_asset: 'USDT',
          quote_symbol: 'USDT',
          mark_price: markPrice,
          index_price: indexPrice,
          mark_index_basis_percent: markIndexPercent,
          spot_price: spotPrice,
          perpetual_spot_basis_percent: perpetualSpotPercent,
          contract_source_time: isoOrNull(contractSourceMs),
          spot_source_time: isoOrNull(spotSourceMs),
          source_time_skew_seconds: skewMs == null ? null : Math.round(skewMs / 1000),
          source_time: isoOrNull(Math.max(contractSourceMs, spotSourceMs)),
          derived_from_market_light_only: true,
          same_provider_spot_match: perpetualSpotPercent != null,
        });
      }
    }

    markIndexRowCount += providerMarkIndex;
    perpetualSpotRowCount += providerPerpetualSpot;
    providerCoverage[provider] = {
      provider,
      input_ready: ready,
      contract_input_ready: contractReady,
      spot_input_ready: spotReady,
      contract_directory_symbols: contractDirectory,
      contract_rows: Number(contract?.row_count || 0),
      spot_directory_symbols: spotDirectory,
      spot_rows: Number(spot?.row_count || 0),
      mark_index_pairs: providerMarkIndex,
      perpetual_spot_pairs: providerPerpetualSpot,
      missing_same_venue_spot: missingSpot,
      missing_mark_price: missingMark,
      stale_or_time_skewed_pairs: staleOrSkewed,
      spot_updated_at: spot?.updated_at || null,
      contract_updated_at: contract?.updated_at || null,
      spot_last_error: spot?.last_error || '',
      contract_last_error: contract?.last_error || '',
    };
  }

  rows.sort((a, b) => {
    if (a.provider !== b.provider) return a.provider.localeCompare(b.provider);
    return a.symbol.localeCompare(b.symbol);
  });

  const basesByProvider = new Map();
  for (const row of rows) {
    if (row.perpetual_spot_basis_percent == null) continue;
    if (!basesByProvider.has(row.base_asset)) basesByProvider.set(row.base_asset, new Set());
    basesByProvider.get(row.base_asset).add(row.provider);
  }
  const crossVenueAssetCount = [...basesByProvider.values()].filter((providers) => providers.size >= 2).length;

  return {
    ok: true,
    version: STEP_VERSION,
    mode: 'shared_derived_same_venue_basis_from_market_light',
    ready: allInputsReady,
    full_input_ready: allInputsReady,
    providers: PROVIDERS,
    provider_count: PROVIDERS.length,
    total_contract_directory_symbols: totalContractDirectory,
    total_spot_directory_symbols: totalSpotDirectory,
    row_count: rows.length,
    mark_index_row_count: markIndexRowCount,
    perpetual_spot_row_count: perpetualSpotRowCount,
    cross_venue_asset_count: crossVenueAssetCount,
    provider_coverage: providerCoverage,
    source_market_light_round: sharedRound,
    refresh_interval_seconds: Math.round(REFRESH_INTERVAL_MS / 1000),
    input_stale_seconds: Math.round(INPUT_STALE_MS / 1000),
    side_fresh_minutes: Math.round(BASIS_SIDE_FRESH_MS / 60_000),
    max_source_time_skew_minutes: Math.round(BASIS_MAX_SKEW_MS / 60_000),
    formula: {
      mark_index_basis_percent: '(mark_price-index_price)/index_price*100',
      perpetual_spot_basis_percent: '(mark_price-spot_price)/spot_price*100',
    },
    no_cross_provider_substitution: true,
    no_cross_quote_substitution: true,
    missing_values_remain_null: true,
    // Legacy contract kept intentionally: the core perpetual layer itself still uses only existing market-light.
    derived_from_existing_shared_market_light_only: true,
    additional_exchange_requests_per_build: 0,
    additional_exchange_connections_per_build: 0,
    exchange_requests_started: 0,
    exchange_connections_started: 0,
    reads_scale_with_users: false,
    rows,
    built_at: new Date(nowMs).toISOString(),
    timestamp_ms: nowMs,
  };
}

function collectInputs() {
  const inputByProvider = {};
  let sharedRound = 0;
  for (const provider of PROVIDERS) {
    const spot = getMarketLightInternalSnapshot({ market: 'spot', provider });
    const contract = getMarketLightInternalSnapshot({ market: 'contract', provider });
    inputByProvider[provider] = { spot, contract };
    sharedRound = Math.max(sharedRound, Number(spot?.shared_round || 0), Number(contract?.shared_round || 0));
  }
  return { inputByProvider, sharedRound };
}

async function fetchJson(url, provider) {
  deliveryTotalUpstreamRequests += 1;
  const response = await fetch(url, {
    method: 'GET',
    headers: {
      accept: 'application/json',
      'user-agent': `KakaWeb3-Step1058-DeliveryBasis/1.0 (${provider})`,
    },
    signal: AbortSignal.timeout(DELIVERY_FETCH_TIMEOUT_MS),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${provider}_delivery_http_${response.status}`);
  let payload;
  try {
    payload = JSON.parse(text);
  } catch (_) {
    throw new Error(`${provider}_delivery_invalid_json`);
  }
  return payload;
}

function buildDeliveryRow({
  provider,
  symbol,
  base,
  quote,
  settle,
  contractType,
  cycle,
  expiryMs,
  lastPrice,
  markPrice,
  indexPrice,
  sourceMs,
  sourceUrl,
  spotByBase,
  nowMs,
}) {
  const cleanProvider = String(provider || '').toLowerCase();
  const cleanSymbol = String(symbol || '').trim().toUpperCase();
  const cleanBase = compact(base);
  const cleanQuote = compact(quote);
  const cleanSettle = compact(settle);
  const expiry = timeMs(expiryMs);
  if (!cleanProvider || !cleanSymbol || !cleanBase || !cleanQuote || expiry <= nowMs) return null;

  const last = positive(lastPrice);
  const mark = positive(markPrice);
  const index = positive(indexPrice);
  const deliveryPrice = mark ?? last;
  const deliveryPriceKind = mark != null ? 'mark_price' : last != null ? 'last_price' : '';
  if (deliveryPrice == null) return null;

  const remainingMs = expiry - nowMs;
  const spotMatch = cleanQuote === 'USDT' ? (spotByBase?.get(cleanBase) || null) : null;
  const spotPrice = positive(spotMatch?.price);
  const spotSourceMs = Number(spotMatch?.sourceMs || 0);
  const sameQuoteSpot = cleanQuote === 'USDT' && spotPrice != null;
  const spotFresh = spotSourceMs > 0 && nowMs - spotSourceMs <= BASIS_SIDE_FRESH_MS;
  const deliverySourceMs = Number(sourceMs || 0) > 0 ? Number(sourceMs) : nowMs;
  const deliveryFresh = nowMs - deliverySourceMs <= BASIS_SIDE_FRESH_MS;
  const skewMs = spotSourceMs > 0 ? Math.abs(deliverySourceMs - spotSourceMs) : null;
  const skewOk = skewMs != null && skewMs <= BASIS_MAX_SKEW_MS;
  const basisComparable = sameQuoteSpot && spotFresh && deliveryFresh && skewOk && remainingMs >= MIN_ANNUALIZE_REMAINING_MS;

  let basisPercent = null;
  let annualizedBasisPercent = null;
  if (basisComparable) {
    basisPercent = percentage(deliveryPrice - spotPrice, spotPrice);
    const years = remainingMs / YEAR_MS;
    if (basisPercent != null && Number.isFinite(years) && years > 0) {
      annualizedBasisPercent = basisPercent / years;
      if (!Number.isFinite(annualizedBasisPercent)) annualizedBasisPercent = null;
    }
  }

  let comparisonReason = '';
  if (!basisComparable) {
    if (cleanQuote !== 'USDT') comparisonReason = 'quote_not_usdt_same_quote_required';
    else if (spotPrice == null) comparisonReason = 'same_venue_same_quote_spot_missing';
    else if (!spotFresh) comparisonReason = 'spot_stale';
    else if (!deliveryFresh) comparisonReason = 'delivery_price_stale';
    else if (!skewOk) comparisonReason = 'source_time_skew_too_large';
    else if (remainingMs < MIN_ANNUALIZE_REMAINING_MS) comparisonReason = 'too_close_to_expiry_for_annualization';
    else comparisonReason = 'not_comparable';
  }

  return {
    provider: cleanProvider,
    market_type: 'delivery',
    symbol: cleanSymbol,
    base_asset: cleanBase,
    quote_asset: cleanQuote,
    quote_symbol: cleanQuote,
    settle_asset: cleanSettle || null,
    contract_type: String(contractType || '').trim(),
    delivery_cycle: String(cycle || '').trim(),
    expiry_at: isoOrNull(expiry),
    expiry_timestamp_ms: expiry,
    remaining_seconds: Math.max(0, Math.floor(remainingMs / 1000)),
    last_price: last,
    mark_price: mark,
    index_price: index,
    delivery_price: deliveryPrice,
    delivery_price_kind: deliveryPriceKind,
    spot_price: basisComparable ? spotPrice : null,
    delivery_basis_percent: basisPercent,
    annualized_delivery_basis_percent: annualizedBasisPercent,
    basis_comparable: basisComparable,
    comparison_reason: comparisonReason,
    same_provider_spot_match: sameQuoteSpot,
    same_quote_spot_match: sameQuoteSpot,
    no_cross_provider_substitution: true,
    no_cross_quote_substitution: true,
    delivery_source_time: isoOrNull(deliverySourceMs),
    spot_source_time: basisComparable ? isoOrNull(spotSourceMs) : null,
    source_time_skew_seconds: basisComparable && skewMs != null ? Math.round(skewMs / 1000) : null,
    source_time: isoOrNull(deliverySourceMs),
    source_url: sourceUrl,
  };
}

async function collectBinanceDelivery({ spotByBase, nowMs }) {
  const snapshot = getBinanceDeliveryContractsSnapshot({ nowMs });
  if (!snapshot?.ok || snapshot?.ready !== true || !Array.isArray(snapshot?.rows) || !snapshot.rows.length) {
    throw new Error('binance_delivery_shared_ws_not_ready');
  }
  const rows = [];
  for (const item of snapshot.rows) {
    const row = buildDeliveryRow({
      provider: 'binance', symbol: item?.symbol, base: item?.base_asset,
      quote: item?.quote_asset || 'USD', settle: item?.settle_asset || item?.base_asset,
      contractType: item?.contract_type || 'COIN_M_DELIVERY', cycle: '', expiryMs: item?.expiry_timestamp_ms,
      lastPrice: item?.last_price ?? item?.price, markPrice: item?.mark_price, indexPrice: item?.index_price,
      sourceMs: timeMs(item?.source_time) || nowMs, sourceUrl: 'binance_existing_merged_public_futures_websocket',
      spotByBase, nowMs,
    });
    if (row) {
      row.delivery_identity_source = item?.expiry_source || 'binance_public_websocket';
      row.binance_contract_rest_requests = 0;
      row.binance_shared_ws_reuse = true;
      rows.push(row);
    }
  }
  return rows;
}

async function collectGateDelivery({ spotByBase, nowMs }) {
  const sourceUrl = 'https://api.gateio.ws/api/v4/delivery/usdt/contracts';
  const payload = await fetchJson(sourceUrl, 'gate');
  if (!Array.isArray(payload)) throw new Error('gate_delivery_payload_not_array');
  const rows = [];
  for (const item of payload) {
    const underlying = String(item?.underlying || '').toUpperCase();
    const [base, quote] = underlying.split('_');
    const row = buildDeliveryRow({
      provider: 'gate',
      symbol: item?.name,
      base,
      quote: quote || 'USDT',
      settle: 'USDT',
      contractType: item?.type,
      cycle: item?.cycle,
      expiryMs: item?.expire_time,
      lastPrice: item?.last_price,
      markPrice: item?.mark_price,
      indexPrice: item?.index_price,
      sourceMs: nowMs,
      sourceUrl,
      spotByBase,
      nowMs,
    });
    if (row) rows.push(row);
  }
  return rows;
}

async function collectOkxDelivery({ spotByBase, nowMs }) {
  const instrumentsUrl = 'https://www.okx.com/api/v5/public/instruments?instType=FUTURES';
  const tickersUrl = 'https://www.okx.com/api/v5/market/tickers?instType=FUTURES';
  const [instrumentsPayload, tickersPayload] = await Promise.all([
    fetchJson(instrumentsUrl, 'okx'),
    fetchJson(tickersUrl, 'okx'),
  ]);
  if (String(instrumentsPayload?.code ?? '') !== '0' || !Array.isArray(instrumentsPayload?.data)) {
    throw new Error('okx_delivery_instruments_not_ready');
  }
  if (String(tickersPayload?.code ?? '') !== '0' || !Array.isArray(tickersPayload?.data)) {
    throw new Error('okx_delivery_tickers_not_ready');
  }
  const tickerById = new Map(tickersPayload.data.map((item) => [String(item?.instId || ''), item]));
  const rows = [];
  for (const item of instrumentsPayload.data) {
    const state = String(item?.state || '').toLowerCase();
    const ruleType = String(item?.ruleType || 'normal').toLowerCase();
    const ctType = String(item?.ctType || '').toLowerCase();
    const expiry = timeMs(item?.expTime);
    if (state !== 'live' || expiry <= nowMs) continue;
    // OKX now also places X-Perps/pre-market X-Perps under FUTURES. They are not dated delivery basis instruments.
    if (ruleType === 'xperp' || ruleType === 'pre_market') continue;
    // Both OKX linear and inverse normal expiry futures are legitimate dated futures.
    // Do not discard inverse/USD rows; just keep strict same-quote basis comparability.
    // Step1058.1: OKX expiry FUTURES are not limited to linear/USDT.
    // Current production includes inverse BTC/ETH USD expiry futures and USD-margined
    // normal expiry futures (e.g. BTC-USD_UM-260925). Keep them as real display rows;
    // basis remains null unless the exact same venue + same base + same quote spot leg exists.
    const underlying = String(item?.uly || item?.instFamily || '').toUpperCase();
    const parts = underlying.split('-').filter(Boolean);
    const base = parts[0] || String(item?.ctValCcy || '').toUpperCase();
    const quote = parts[1] || String(item?.settleCcy || '').toUpperCase();
    if (!base || !quote) continue;
    const ticker = tickerById.get(String(item?.instId || '')) || null;
    const sourceMs = timeMs(ticker?.ts) || nowMs;
    const row = buildDeliveryRow({
      provider: 'okx',
      symbol: item?.instId,
      base,
      quote,
      settle: item?.settleCcy || quote,
      contractType: item?.ctType || 'linear',
      cycle: item?.alias || '',
      expiryMs: expiry,
      lastPrice: ticker?.last,
      markPrice: null,
      indexPrice: null,
      sourceMs,
      sourceUrl: instrumentsUrl,
      spotByBase,
      nowMs,
    });
    if (row) rows.push(row);
  }
  return rows;
}

async function collectBybitInstrumentPages(category) {
  const rows = [];
  let cursor = '';
  let rootTime = 0;
  for (let page = 0; page < DELIVERY_MAX_PROVIDER_PAGES; page += 1) {
    const url = new URL('https://api.bybit.com/v5/market/instruments-info');
    url.searchParams.set('category', category);
    url.searchParams.set('limit', '1000');
    if (cursor) url.searchParams.set('cursor', cursor);
    const payload = await fetchJson(url.toString(), 'bybit');
    if (Number(payload?.retCode) !== 0 || !Array.isArray(payload?.result?.list)) {
      throw new Error('bybit_delivery_instruments_not_ready');
    }
    rows.push(...payload.result.list);
    rootTime = Math.max(rootTime, timeMs(payload?.time));
    const next = String(payload?.result?.nextPageCursor || '').trim();
    if (!next || next === cursor) break;
    cursor = next;
  }
  return { rows, rootTime };
}

async function collectBybitDelivery({ spotByBase, nowMs }) {
  const instruments = await collectBybitInstrumentPages('linear');
  const tickersUrl = 'https://api.bybit.com/v5/market/tickers?category=linear';
  const tickersPayload = await fetchJson(tickersUrl, 'bybit');
  if (Number(tickersPayload?.retCode) !== 0 || !Array.isArray(tickersPayload?.result?.list)) {
    throw new Error('bybit_delivery_tickers_not_ready');
  }
  const tickerBySymbol = new Map(
    tickersPayload.result.list.map((item) => [String(item?.symbol || '').toUpperCase(), item]),
  );
  const rootTime = timeMs(tickersPayload?.time) || instruments.rootTime || nowMs;
  const rows = [];
  for (const item of instruments.rows) {
    const contractType = String(item?.contractType || '');
    const status = String(item?.status || '').toLowerCase();
    const quote = compact(item?.quoteCoin);
    const settle = compact(item?.settleCoin);
    const expiry = timeMs(item?.deliveryTime);
    if (!/futures/i.test(contractType) || /perpetual/i.test(contractType)) continue;
    if (status !== 'trading' || expiry <= nowMs || quote !== 'USDT') continue;
    if (settle && settle !== 'USDT') continue;
    const ticker = tickerBySymbol.get(String(item?.symbol || '').toUpperCase()) || null;
    const row = buildDeliveryRow({
      provider: 'bybit',
      symbol: item?.symbol,
      base: item?.baseCoin,
      quote,
      settle: settle || 'USDT',
      contractType,
      cycle: '',
      expiryMs: expiry,
      lastPrice: ticker?.lastPrice,
      markPrice: ticker?.markPrice,
      indexPrice: ticker?.indexPrice,
      sourceMs: rootTime,
      sourceUrl: tickersUrl,
      spotByBase,
      nowMs,
    });
    if (row) rows.push(row);
  }
  return rows;
}

async function collectBitgetDelivery({ spotByBase, nowMs }) {
  const contractsUrl = 'https://api.bitget.com/api/v2/mix/market/contracts?productType=COIN-FUTURES';
  const tickersUrl = 'https://api.bitget.com/api/v2/mix/market/tickers?productType=COIN-FUTURES';
  const [contractsPayload, tickersPayload] = await Promise.all([
    fetchJson(contractsUrl, 'bitget'),
    fetchJson(tickersUrl, 'bitget'),
  ]);
  if (String(contractsPayload?.code || '') !== '00000' || !Array.isArray(contractsPayload?.data)) {
    throw new Error('bitget_delivery_contracts_not_ready');
  }
  if (String(tickersPayload?.code || '') !== '00000' || !Array.isArray(tickersPayload?.data)) {
    throw new Error('bitget_delivery_tickers_not_ready');
  }
  const tickerBySymbol = new Map(
    tickersPayload.data.map((item) => [String(item?.symbol || '').toUpperCase(), item]),
  );
  const rows = [];
  for (const item of contractsPayload.data) {
    const symbolType = String(item?.symbolType || '').toLowerCase();
    const status = String(item?.symbolStatus || '').toLowerCase();
    const expiry = timeMs(item?.deliveryTime);
    if (symbolType !== 'delivery' || expiry <= nowMs) continue;
    if (status && !['normal', 'listed'].includes(status)) continue;
    const ticker = tickerBySymbol.get(String(item?.symbol || '').toUpperCase()) || null;
    const sourceMs = timeMs(ticker?.ts) || timeMs(tickersPayload?.requestTime) || nowMs;
    const row = buildDeliveryRow({
      provider: 'bitget',
      symbol: item?.symbol,
      base: item?.baseCoin,
      quote: item?.quoteCoin || 'USD',
      settle: item?.symbol?.toString()?.toUpperCase()?.startsWith(String(item?.baseCoin || '').toUpperCase())
        ? item?.baseCoin
        : '',
      contractType: 'coin_m_delivery',
      cycle: item?.deliveryPeriod,
      expiryMs: expiry,
      lastPrice: ticker?.lastPr,
      markPrice: ticker?.markPrice,
      indexPrice: ticker?.indexPrice,
      sourceMs,
      sourceUrl: contractsUrl,
      spotByBase,
      nowMs,
    });
    if (row) rows.push(row);
  }
  return rows;
}

async function refreshDeliverySnapshot(inputByProvider, { reason = 'scheduled', nowMs = Date.now() } = {}) {
  if (deliveryRunning) return latestDeliverySnapshot;
  deliveryRunning = true;
  deliveryTotalRefreshes += 1;
  deliveryLastStartedAt = new Date(nowMs).toISOString();

  const providerCoverage = {};
  const rows = [];
  const errors = [];
  const requestsBefore = deliveryTotalUpstreamRequests;

  const jobs = [
    ['binance', collectBinanceDelivery],
    ['okx', collectOkxDelivery],
    ['bybit', collectBybitDelivery],
    ['bitget', collectBitgetDelivery],
    ['gate', collectGateDelivery],
  ].map(async ([provider, collector]) => {
    const spotByBase = normalizeSpotMap(inputByProvider?.[provider]?.spot?.rows || []);
    try {
      const providerRows = await collector({ spotByBase, nowMs });
      return { provider, ok: true, rows: providerRows };
    } catch (error) {
      return {
        provider,
        ok: false,
        rows: [],
        error: String(error?.message || error),
      };
    }
  });

  try {
    const results = await Promise.all(jobs);
    for (const result of results) {
      rows.push(...result.rows);
      const comparable = result.rows.filter((row) => row.basis_comparable === true).length;
      providerCoverage[result.provider] = {
        provider: result.provider,
        source_supported: true,
        delivery_contracts: result.rows.length,
        comparable_pairs: comparable,
        status: result.ok ? 'ready' : 'error',
        reason: result.ok ? '' : result.error,
        upstream_requests: result.provider === 'binance' ? 0 : null,
        shared_ws_reuse: result.provider === 'binance',
      };
      if (!result.ok) errors.push(`${result.provider}:${result.error}`);
    }

    rows.sort((a, b) => {
      const ae = Number(a.expiry_timestamp_ms || 0);
      const be = Number(b.expiry_timestamp_ms || 0);
      if (ae !== be) return ae - be;
      if (a.provider !== b.provider) return a.provider.localeCompare(b.provider);
      return a.symbol.localeCompare(b.symbol);
    });

    const comparableRows = rows.filter((row) => row.basis_comparable === true);
    const providerReadyCount = DELIVERY_SOURCE_PROVIDERS.filter(
      (provider) => providerCoverage[provider]?.status === 'ready',
    ).length;
    const providerComparableCount = new Set(comparableRows.map((row) => row.provider)).size;
    const upstreamRequests = deliveryTotalUpstreamRequests - requestsBefore;

    if (providerReadyCount === 0 && latestDeliverySnapshot?.ready === true) {
      deliveryTotalRefreshFailures += 1;
      deliveryLastError = errors.join('|') || 'all_delivery_sources_unavailable';
      // Preserve the last verified delivery snapshot atomically. A temporary provider outage
      // must not erase already-visible real delivery data. The old built_at remains unchanged,
      // so consumers can still see that the preserved snapshot is older.
      return {
        ...latestDeliverySnapshot,
        stale_preserved: true,
        stale_preserve_reason: deliveryLastError,
        latest_refresh_failed_at: new Date(nowMs).toISOString(),
      };
    }

    const snapshot = {
      ok: true,
      schema: 'step1058_delivery_basis_v1',
      version: STEP_VERSION,
      ready: providerReadyCount > 0,
      partial: providerReadyCount < DELIVERY_SOURCE_PROVIDERS.length,
      providers: PROVIDERS,
      source_providers: DELIVERY_SOURCE_PROVIDERS,
      source_provider_count: DELIVERY_SOURCE_PROVIDERS.length,
      provider_ready_count: providerReadyCount,
      provider_comparable_count: providerComparableCount,
      row_count: rows.length,
      comparable_row_count: comparableRows.length,
      provider_coverage: providerCoverage,
      rows,
      formula: {
        delivery_basis_percent: '(delivery_price-spot_price)/spot_price*100',
        annualized_delivery_basis_percent: 'delivery_basis_percent/(remaining_milliseconds/(365*24h))',
      },
      delivery_price_policy: 'mark_price_when_available_else_last_price',
      same_provider_required: true,
      same_base_required: true,
      same_quote_required_for_basis: true,
      comparable_quote: 'USDT',
      no_cross_provider_substitution: true,
      no_cross_quote_substitution: true,
      missing_values_remain_null: true,
      min_remaining_seconds_for_annualization: Math.round(MIN_ANNUALIZE_REMAINING_MS / 1000),
      delivery_refresh_interval_seconds: Math.round(DELIVERY_REFRESH_INTERVAL_MS / 1000),
      delivery_upstream_requests_this_refresh: upstreamRequests,
      delivery_upstream_requests_scale_with_users: false,
      user_reads_start_delivery_upstream_requests: false,
      user_reads_scale_exchange_upstream: false,
      binance_contract_rest_requests: 0,
      binance_contract_rest_guard_preserved: true,
      binance_delivery_source: 'existing_merged_public_futures_websocket_st_2',
      binance_delivery_additional_websocket_connections: 0,
      binance_delivery_user_reads_start_connections: false,
      reason,
      errors,
      built_at: new Date(nowMs).toISOString(),
      timestamp_ms: nowMs,
    };

    latestDeliverySnapshot = snapshot;
    deliveryLastCompletedAt = new Date().toISOString();
    deliveryLastError = errors.join('|');
    if (providerReadyCount === 0) deliveryTotalRefreshFailures += 1;
    return snapshot;
  } catch (error) {
    deliveryTotalRefreshFailures += 1;
    deliveryLastError = String(error?.message || error);
    return latestDeliverySnapshot;
  } finally {
    deliveryRunning = false;
  }
}

async function maybeWarmBinanceDeliveryFromSharedWs(inputByProvider, nowMs = Date.now()) {
  const currentBinance = latestDeliverySnapshot?.provider_coverage?.binance || null;
  if (currentBinance?.status === 'ready' && Number(currentBinance?.delivery_contracts || 0) > 0) return latestDeliverySnapshot;
  if (nowMs - binanceDeliveryWarmLastAttemptAt < BINANCE_DELIVERY_WARM_RETRY_MS) return latestDeliverySnapshot;
  binanceDeliveryWarmLastAttemptAt = nowMs;
  binanceDeliveryWarmAttempts += 1;
  const spotByBase = normalizeSpotMap(inputByProvider?.binance?.spot?.rows || []);
  try {
    const binanceRows = await collectBinanceDelivery({ spotByBase, nowMs });
    if (!Array.isArray(binanceRows) || !binanceRows.length) {
      binanceDeliveryWarmLastError = 'binance_delivery_shared_ws_not_ready';
      return latestDeliverySnapshot;
    }
    const previous = latestDeliverySnapshot || {
      ok: true, schema: 'step1058_delivery_basis_v1', version: STEP_VERSION, provider_coverage: {}, rows: [],
    };
    const rows = [
      ...(Array.isArray(previous.rows) ? previous.rows.filter((row) => String(row?.provider || '').toLowerCase() !== 'binance') : []),
      ...binanceRows,
    ];
    rows.sort((a, b) => {
      const ae = Number(a.expiry_timestamp_ms || 0);
      const be = Number(b.expiry_timestamp_ms || 0);
      if (ae !== be) return ae - be;
      if (a.provider !== b.provider) return String(a.provider).localeCompare(String(b.provider));
      return String(a.symbol).localeCompare(String(b.symbol));
    });
    const providerCoverage = { ...(previous.provider_coverage || {}) };
    providerCoverage.binance = {
      provider: 'binance', source_supported: true, delivery_contracts: binanceRows.length,
      comparable_pairs: binanceRows.filter((row) => row?.basis_comparable === true).length,
      status: 'ready', reason: '', upstream_requests: 0, shared_ws_reuse: true,
    };
    const comparableRows = rows.filter((row) => row?.basis_comparable === true);
    const providerReadyCount = DELIVERY_SOURCE_PROVIDERS.filter((provider) => providerCoverage[provider]?.status === 'ready').length;
    const providerComparableCount = new Set(comparableRows.map((row) => row.provider)).size;
    latestDeliverySnapshot = {
      ...previous, version: STEP_VERSION, ready: providerReadyCount > 0,
      partial: providerReadyCount < DELIVERY_SOURCE_PROVIDERS.length,
      provider_ready_count: providerReadyCount, provider_comparable_count: providerComparableCount,
      row_count: rows.length, comparable_row_count: comparableRows.length, provider_coverage: providerCoverage, rows,
      binance_contract_rest_requests: 0, binance_contract_rest_guard_preserved: true,
      binance_delivery_source: 'existing_merged_public_futures_websocket_st_2',
      binance_delivery_additional_websocket_connections: 0,
      binance_shared_ws_warm_merged_at: new Date(nowMs).toISOString(),
    };
    binanceDeliveryWarmSuccesses += 1;
    binanceDeliveryWarmLastError = '';
    deliveryLastCompletedAt = new Date(nowMs).toISOString();
    if (latestVerifiedSnapshot) {
      latestVerifiedSnapshot = {
        ...latestVerifiedSnapshot,
        delivery: latestDeliverySnapshot,
        delivery_ready: latestDeliverySnapshot.ready === true,
        delivery_row_count: Number(latestDeliverySnapshot.row_count || 0),
        delivery_comparable_row_count: Number(latestDeliverySnapshot.comparable_row_count || 0),
        delivery_provider_ready_count: Number(latestDeliverySnapshot.provider_ready_count || 0),
        delivery_provider_comparable_count: Number(latestDeliverySnapshot.provider_comparable_count || 0),
      };
    }
    responseCache.clear();
    return latestDeliverySnapshot;
  } catch (error) {
    binanceDeliveryWarmLastError = String(error?.message || error);
    return latestDeliverySnapshot;
  }
}

async function maybeRefreshDeliverySnapshot(inputByProvider, { reason = 'scheduled' } = {}) {
  const builtMs = timeMs(latestDeliverySnapshot?.built_at);
  if (
    latestDeliverySnapshot &&
    builtMs > 0 &&
    Date.now() - builtMs < DELIVERY_REFRESH_INTERVAL_MS
  ) {
    return latestDeliverySnapshot;
  }
  return await refreshDeliverySnapshot(inputByProvider, { reason, nowMs: Date.now() });
}

function mergePartialProviderSnapshot(snapshot, previous, nowMs = Date.now()) {
  const healthyProviders = PROVIDERS.filter((provider) => snapshot?.provider_coverage?.[provider]?.input_ready === true);
  const unavailableProviders = PROVIDERS.filter((provider) => !healthyProviders.includes(provider));
  const providerReadyCount = healthyProviders.length;
  const partialReady = providerReadyCount >= MIN_READY_PROVIDERS;
  if (!partialReady) {
    return {
      ...snapshot,
      ready: false,
      partial_input_ready: false,
      minimum_ready_providers: MIN_READY_PROVIDERS,
      provider_ready_count: providerReadyCount,
      healthy_providers: healthyProviders,
      unavailable_providers: unavailableProviders,
      preserve_unavailable_cache: true,
      preserved_unavailable_provider_rows: 0,
    };
  }

  const rows = Array.isArray(snapshot?.rows) ? snapshot.rows.map((row) => ({ ...row })) : [];
  const seen = new Set(rows.map((row) => `${String(row?.provider || '').toLowerCase()}|${String(row?.symbol || '').toUpperCase()}`));
  let preservedRows = 0;
  if (previous && unavailableProviders.length) {
    for (const row of Array.isArray(previous?.rows) ? previous.rows : []) {
      const provider = String(row?.provider || '').toLowerCase();
      if (!unavailableProviders.includes(provider)) continue;
      const sourceMs = Math.max(timeMs(row?.contract_source_time), timeMs(row?.spot_source_time), timeMs(row?.source_time));
      if (!(sourceMs > 0) || nowMs - sourceMs > BASIS_SIDE_FRESH_MS) continue;
      const key = `${provider}|${String(row?.symbol || '').toUpperCase()}`;
      if (seen.has(key)) continue;
      rows.push({ ...row, stale_preserved_provider: true });
      seen.add(key);
      preservedRows += 1;
    }
  }

  rows.sort((a, b) => {
    if (a.provider !== b.provider) return String(a.provider).localeCompare(String(b.provider));
    return String(a.symbol).localeCompare(String(b.symbol));
  });
  const markIndexRowCount = rows.filter((row) => finite(row?.mark_index_basis_percent) != null).length;
  const perpetualSpotRowCount = rows.filter((row) => finite(row?.perpetual_spot_basis_percent) != null).length;
  const basesByProvider = new Map();
  for (const row of rows) {
    if (finite(row?.perpetual_spot_basis_percent) == null) continue;
    const base = compact(row?.base_asset);
    const provider = String(row?.provider || '').toLowerCase();
    if (!base || !provider) continue;
    if (!basesByProvider.has(base)) basesByProvider.set(base, new Set());
    basesByProvider.get(base).add(provider);
  }

  const providerCoverage = { ...(snapshot?.provider_coverage || {}) };
  for (const provider of unavailableProviders) {
    const preserved = rows.filter((row) => String(row?.provider || '').toLowerCase() === provider && row?.stale_preserved_provider === true).length;
    providerCoverage[provider] = {
      ...(providerCoverage[provider] || { provider }),
      stale_preserved_rows: preserved,
      stale_preserved: preserved > 0,
    };
  }

  return {
    ...snapshot,
    ready: true,
    full_input_ready: providerReadyCount === PROVIDERS.length,
    partial_input_ready: providerReadyCount < PROVIDERS.length,
    minimum_ready_providers: MIN_READY_PROVIDERS,
    provider_ready_count: providerReadyCount,
    healthy_providers: healthyProviders,
    unavailable_providers: unavailableProviders,
    preserve_unavailable_cache: true,
    preserved_unavailable_provider_rows: preservedRows,
    row_count: rows.length,
    mark_index_row_count: markIndexRowCount,
    perpetual_spot_row_count: perpetualSpotRowCount,
    cross_venue_asset_count: [...basesByProvider.values()].filter((providers) => providers.size >= 2).length,
    provider_coverage: providerCoverage,
    rows,
  };
}

export async function runContractBasisCycle({ reason = 'scheduled' } = {}) {
  if (running) return false;
  running = true;
  totalBuilds += 1;
  lastStartedAt = new Date().toISOString();
  try {
    const { inputByProvider, sharedRound } = collectInputs();

    // Step1060.33.5: delivery refresh is independent from the five-provider perpetual
    // completeness gate. A transient Gate/other market-light outage must not prevent
    // Binance COIN-M WS-only delivery (or another healthy delivery venue) from refreshing.
    // This remains background-shared and non-fatal; user reads still start zero upstream work.
    await maybeRefreshDeliverySnapshot(inputByProvider, { reason: `contract_basis_${reason}` });
    // Step1060.33.6.1: while Binance COIN-M delivery is warming after a Render restart,
    // retry ONLY the already-existing shared Binance WS snapshot. Do not refetch the
    // four REST-backed delivery venues and do not open any Binance REST/socket path.
    await maybeWarmBinanceDeliveryFromSharedWs(inputByProvider, Date.now());

    const rawSnapshot = buildContractBasisSnapshotFromInputs(inputByProvider, { nowMs: Date.now(), sharedRound });
    const snapshot = mergePartialProviderSnapshot(rawSnapshot, latestVerifiedSnapshot, Date.now());
    if (!snapshot.ready) {
      const reasons = PROVIDERS.flatMap((provider) => {
        const coverage = snapshot.provider_coverage?.[provider];
        return coverage?.input_ready ? [] : [`${provider}:spot=${coverage?.spot_input_ready},contract=${coverage?.contract_input_ready}`];
      });
      throw new Error(`market_light_inputs_below_minimum_ready:${snapshot.provider_ready_count}/${MIN_READY_PROVIDERS}:${reasons.join('|')}`);
    }

    latestVerifiedSnapshot = {
      ...snapshot,
      delivery: latestDeliverySnapshot,
      delivery_schema: latestDeliverySnapshot?.schema || 'step1058_delivery_basis_v1',
      delivery_ready: latestDeliverySnapshot?.ready === true,
      delivery_row_count: Number(latestDeliverySnapshot?.row_count || 0),
      delivery_comparable_row_count: Number(latestDeliverySnapshot?.comparable_row_count || 0),
      delivery_provider_ready_count: Number(latestDeliverySnapshot?.provider_ready_count || 0),
      delivery_provider_comparable_count: Number(latestDeliverySnapshot?.provider_comparable_count || 0),
      delivery_refresh_interval_seconds: Math.round(DELIVERY_REFRESH_INTERVAL_MS / 1000),
      delivery_upstream_requests_scale_with_users: false,
      binance_contract_rest_guard_preserved: true,
    };
    round += 1;
    lastCompletedAt = new Date().toISOString();
    lastError = '';
    responseCache.clear();
    return true;
  } catch (error) {
    totalBuildFailures += 1;
    lastError = String(error?.message || error);
    return false;
  } finally {
    running = false;
  }
}

export function startContractBasisScanner() {
  if (started || process.env.KAKA_DISABLE_CONTRACT_BASIS_SCANNER === '1') return;
  started = true;
  timer = setTimeout(() => {
    void runContractBasisCycle({ reason: 'startup' });
    interval = setInterval(() => void runContractBasisCycle({ reason: 'interval' }), REFRESH_INTERVAL_MS);
    interval.unref?.();
  }, START_DELAY_MS);
  timer.unref?.();
}

function responsePayload() {
  if (!latestVerifiedSnapshot) {
    return {
      ok: true,
      version: STEP_VERSION,
      ready: false,
      full_input_ready: false,
      mode: 'shared_derived_same_venue_basis_from_market_light',
      row_count: 0,
      mark_index_row_count: 0,
      perpetual_spot_row_count: 0,
      cross_venue_asset_count: 0,
      provider_count: PROVIDERS.length,
      providers: PROVIDERS,
      provider_coverage: {},
      rows: [],
      delivery: latestDeliverySnapshot,
      delivery_schema: 'step1058_delivery_basis_v1',
      delivery_ready: latestDeliverySnapshot?.ready === true,
      delivery_row_count: Number(latestDeliverySnapshot?.row_count || 0),
      delivery_comparable_row_count: Number(latestDeliverySnapshot?.comparable_row_count || 0),
      last_error: lastError,
      exchange_requests_started: 0,
      exchange_connections_started: 0,
      reads_scale_with_users: false,
      derived_from_existing_shared_market_light_only: true,
      binance_contract_rest_guard_preserved: true,
      timestamp_ms: Date.now(),
    };
  }
  return {
    ...latestVerifiedSnapshot,
    shared_basis_round: round,
    last_error: lastError,
    // Legacy zeroes mean a USER READ of this route starts no upstream work.
    // Delivery background collector accounting is exposed separately under delivery.*.
    exchange_requests_started: 0,
    exchange_connections_started: 0,
    reads_scale_with_users: false,
    timestamp_ms: Date.now(),
  };
}

function cachedResponse() {
  const key = 'all';
  const cached = responseCache.get(key);
  if (cached && Date.now() - cached.at <= RESPONSE_CACHE_TTL_MS) {
    responseCacheHits += 1;
    return { ...cached.payload, cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }
  responseCacheMisses += 1;
  const payload = responsePayload();
  responseCache.set(key, { at: Date.now(), payload });
  return { ...payload, cache_hit: false, cache_age_ms: 0 };
}

export function getContractBasisHealth() {
  return {
    ok: true,
    version: STEP_VERSION,
    enabled: started || process.env.KAKA_DISABLE_CONTRACT_BASIS_SCANNER !== '1',
    mode: 'shared_derived_same_venue_basis_from_market_light',
    current_snapshot_endpoint: CURRENT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    providers: PROVIDERS,
    running,
    round,
    last_started_at: lastStartedAt,
    last_completed_at: lastCompletedAt,
    last_error: lastError,
    total_builds: totalBuilds,
    total_build_failures: totalBuildFailures,
    total_snapshot_reads: totalSnapshotReads,
    response_cache_ttl_seconds: Math.round(RESPONSE_CACHE_TTL_MS / 1000),
    response_cache_entries: responseCache.size,
    response_cache_hits: responseCacheHits,
    response_cache_misses: responseCacheMisses,
    latest_ready: Boolean(latestVerifiedSnapshot?.ready),
    latest_full_input_ready: Boolean(latestVerifiedSnapshot?.full_input_ready),
    latest_partial_input_ready: Boolean(latestVerifiedSnapshot?.partial_input_ready),
    latest_provider_ready_count: Number(latestVerifiedSnapshot?.provider_ready_count || 0),
    latest_healthy_providers: latestVerifiedSnapshot?.healthy_providers || [],
    latest_unavailable_providers: latestVerifiedSnapshot?.unavailable_providers || [],
    latest_preserved_unavailable_provider_rows: Number(latestVerifiedSnapshot?.preserved_unavailable_provider_rows || 0),
    minimum_ready_providers: MIN_READY_PROVIDERS,
    latest_row_count: Number(latestVerifiedSnapshot?.row_count || 0),
    latest_mark_index_row_count: Number(latestVerifiedSnapshot?.mark_index_row_count || 0),
    latest_perpetual_spot_row_count: Number(latestVerifiedSnapshot?.perpetual_spot_row_count || 0),
    latest_cross_venue_asset_count: Number(latestVerifiedSnapshot?.cross_venue_asset_count || 0),
    latest_provider_coverage: latestVerifiedSnapshot?.provider_coverage || {},
    derived_from_existing_shared_market_light_only: true,
    additional_exchange_requests_per_build: 0,
    additional_exchange_connections_per_build: 0,
    snapshot_reads_start_exchange_requests: false,
    snapshot_reads_start_exchange_connections: false,
    snapshot_reads_scale_with_users: false,
    no_cross_provider_substitution: true,
    no_cross_quote_substitution: true,
    missing_values_remain_null: true,

    delivery: {
      schema: 'step1058_delivery_basis_v1',
      running: deliveryRunning,
      refresh_interval_seconds: Math.round(DELIVERY_REFRESH_INTERVAL_MS / 1000),
      last_started_at: deliveryLastStartedAt,
      last_completed_at: deliveryLastCompletedAt,
      last_error: deliveryLastError,
      total_refreshes: deliveryTotalRefreshes,
      total_refresh_failures: deliveryTotalRefreshFailures,
      total_upstream_requests: deliveryTotalUpstreamRequests,
      latest_ready: latestDeliverySnapshot?.ready === true,
      latest_partial: latestDeliverySnapshot?.partial === true,
      latest_row_count: Number(latestDeliverySnapshot?.row_count || 0),
      latest_comparable_row_count: Number(latestDeliverySnapshot?.comparable_row_count || 0),
      latest_provider_ready_count: Number(latestDeliverySnapshot?.provider_ready_count || 0),
      latest_provider_comparable_count: Number(latestDeliverySnapshot?.provider_comparable_count || 0),
      latest_provider_coverage: latestDeliverySnapshot?.provider_coverage || {},
      user_reads_start_exchange_requests: false,
      reads_scale_with_users: false,
      binance_contract_rest_requests: 0,
      binance_contract_rest_guard_preserved: true,
      binance_delivery_source: 'existing_merged_public_futures_websocket_st_2',
      binance_delivery_additional_websocket_connections: 0,
      binance_shared_ws_warm_retry_seconds: Math.round(BINANCE_DELIVERY_WARM_RETRY_MS / 1000),
      binance_shared_ws_warm_attempts: binanceDeliveryWarmAttempts,
      binance_shared_ws_warm_successes: binanceDeliveryWarmSuccesses,
      binance_shared_ws_warm_last_attempt_at: binanceDeliveryWarmLastAttemptAt ? new Date(binanceDeliveryWarmLastAttemptAt).toISOString() : null,
      binance_shared_ws_warm_last_error: binanceDeliveryWarmLastError || null,
    },
    time: new Date().toISOString(),
  };
}

function sendJson(res, status, payload, { cdnSMaxAgeSec = 0 } = {}) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  const headers = {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, OPTIONS',
    'content-length': String(body.length),
  };
  if (status >= 200 && status < 300 && Number(cdnSMaxAgeSec || 0) > 0) {
    headers['cdn-cache-control'] = `public, s-maxage=${Math.max(1, Number(cdnSMaxAgeSec))}, stale-while-revalidate=8, stale-if-error=30`;
    headers['x-kaka-edge-cache-policy'] = 'render_edge_cache_shared_snapshot_v1';
  }
  res.writeHead(status, headers);
  res.end(body);
}

export async function handleContractBasis(req, res, url) {
  if (![CURRENT_ROUTE, HEALTH_ROUTE].includes(url.pathname)) return false;
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
    sendJson(res, 405, { ok: false, version: STEP_VERSION, error: 'method_not_allowed' });
    return true;
  }
  if (url.pathname === HEALTH_ROUTE) {
    sendJson(res, 200, getContractBasisHealth());
    return true;
  }
  totalSnapshotReads += 1;
  sendJson(res, 200, cachedResponse(), { cdnSMaxAgeSec: 2 });
  return true;
}

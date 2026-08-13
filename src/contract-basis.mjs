import { getMarketLightInternalSnapshot } from './market-light-bridge.mjs';

const STEP_VERSION = '650.8.15.1';
const CURRENT_ROUTE = '/api/contract-basis/current-snapshot';
const HEALTH_ROUTE = '/api/contract-basis/health';
const PROVIDERS = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const REFRESH_INTERVAL_MS = Math.max(15_000, Number(process.env.KAKA_CONTRACT_BASIS_REFRESH_INTERVAL_MS || 30_000));
const START_DELAY_MS = Math.max(5_000, Number(process.env.KAKA_CONTRACT_BASIS_START_DELAY_MS || 12_000));
const RESPONSE_CACHE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_CONTRACT_BASIS_RESPONSE_CACHE_TTL_MS || 20_000));
const INPUT_STALE_MS = Math.max(60_000, Number(process.env.KAKA_CONTRACT_BASIS_INPUT_STALE_MS || 3 * 60_000));
const BASIS_SIDE_FRESH_MS = Math.max(60_000, Number(process.env.KAKA_CONTRACT_BASIS_SIDE_FRESH_MS || 30 * 60_000));
const BASIS_MAX_SKEW_MS = Math.max(60_000, Number(process.env.KAKA_CONTRACT_BASIS_MAX_SKEW_MS || 15 * 60_000));

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

export async function runContractBasisCycle({ reason = 'scheduled' } = {}) {
  if (running) return false;
  running = true;
  totalBuilds += 1;
  lastStartedAt = new Date().toISOString();
  try {
    const { inputByProvider, sharedRound } = collectInputs();
    const snapshot = buildContractBasisSnapshotFromInputs(inputByProvider, { nowMs: Date.now(), sharedRound });
    if (!snapshot.full_input_ready) {
      const reasons = PROVIDERS.flatMap((provider) => {
        const coverage = snapshot.provider_coverage?.[provider];
        return coverage?.input_ready ? [] : [`${provider}:spot=${coverage?.spot_input_ready},contract=${coverage?.contract_input_ready}`];
      });
      throw new Error(`market_light_inputs_not_ready:${reasons.join('|')}`);
    }
    latestVerifiedSnapshot = { ...snapshot, reason };
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
      last_error: lastError,
      exchange_requests_started: 0,
      exchange_connections_started: 0,
      reads_scale_with_users: false,
      derived_from_existing_shared_market_light_only: true,
      timestamp_ms: Date.now(),
    };
  }
  return {
    ...latestVerifiedSnapshot,
    shared_basis_round: round,
    last_error: lastError,
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
    latest_ready: Boolean(latestVerifiedSnapshot?.full_input_ready),
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

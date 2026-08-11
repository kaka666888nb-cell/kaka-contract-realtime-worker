import { getMarketLightInternalSnapshot } from './market-light-snapshot.mjs';

const VERSION = '650.8.15.3';
const SNAPSHOT_ROUTE = '/api/contract-focus-pool/current-snapshot';
const HEALTH_ROUTE = '/api/contract-focus-pool/health';
const PROVIDERS = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);

// Step982: V45 fixed-core priority anchor. The user-defined product rule is
// 10 fixed mainstream market-cap leaders + 5 provider-local hot contracts.
// This priority anchor is versioned/fixed; provider availability is checked
// against the live shared USDT-perpetual market-light directory before use.
// Stablecoins are intentionally not core observation assets.
const CORE_MARKET_CAP_PRIORITY = Object.freeze([
  'BTC', 'ETH', 'BNB', 'XRP', 'SOL', 'TRX', 'HYPE', 'DOGE',
  'LEO', 'ZEC', 'XMR', 'ADA', 'LINK', 'XLM', 'BCH',
]);
const CORE_SOURCE_AS_OF = '2026-08-09';
const CORE_TARGET = 10;
const HOT_TARGET = 5;
const POOL_TARGET = CORE_TARGET + HOT_TARGET;
const HOT_RANK_METRIC = 'quote_volume_24h';
const SCAN_INTERVAL_MS = 60_000;
const STARTUP_RETRY_MS = 15_000;
const HOT_REFRESH_MS = 5 * 60_000;
const RESPONSE_CACHE_TTL_MS = 20_000;
const STALE_MS = 3 * 60_000;

let started = false;
let running = null;
let timer = null;
let interval = null;
let round = 0;
let lastStartedAt = null;
let lastCompletedAt = null;
let lastError = '';
let totalBuilds = 0;
let totalBuildFailures = 0;
let totalReads = 0;
let responseCacheHits = 0;
let responseCacheMisses = 0;
const responseCache = new Map();
const providerState = new Map();

function finite(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function positive(value) {
  const number = finite(value);
  return number != null && number > 0 ? number : null;
}

function normalizeBase(raw) {
  return String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function normalizeSymbol(raw) {
  return String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}

function rowBase(row) {
  const explicit = normalizeBase(row?.base_asset);
  if (explicit) return explicit;
  const symbol = normalizeSymbol(row?.symbol);
  return symbol.endsWith('USDT') ? symbol.slice(0, -4) : '';
}

function usableContractRows(snapshot) {
  if (!snapshot?.ok) return [];
  if (snapshot.stale === true) return [];
  if (String(snapshot.last_error || '').trim()) return [];
  if (Number(snapshot.directory_count || 0) <= 0) return [];
  if (Number(snapshot.row_count || 0) !== Number(snapshot.directory_count || 0)) return [];
  const byBase = new Map();
  for (const raw of Array.isArray(snapshot.rows) ? snapshot.rows : []) {
    const symbol = normalizeSymbol(raw?.symbol);
    const base = rowBase(raw);
    const quote = normalizeBase(raw?.quote_asset || raw?.quote_symbol || '');
    if (!symbol || !base || quote !== 'USDT' || !symbol.endsWith('USDT')) continue;
    const current = byBase.get(base);
    const volume = positive(raw?.quote_volume_24h) || 0;
    const currentVolume = positive(current?.quote_volume_24h) || 0;
    if (!current || volume > currentVolume) byBase.set(base, { ...raw, symbol, base_asset: base, quote_asset: 'USDT' });
  }
  return [...byBase.values()];
}

function coreRowsFrom(rows) {
  const byBase = new Map(rows.map((row) => [rowBase(row), row]));
  const selected = [];
  for (let rank = 0; rank < CORE_MARKET_CAP_PRIORITY.length && selected.length < CORE_TARGET; rank += 1) {
    const base = CORE_MARKET_CAP_PRIORITY[rank];
    const row = byBase.get(base);
    if (!row) continue;
    selected.push({ row, marketCapPriority: rank + 1 });
  }
  return selected;
}

function hotRowsFrom(rows, coreBases) {
  return rows
    .filter((row) => !coreBases.has(rowBase(row)))
    .filter((row) => positive(row?.quote_volume_24h) != null)
    .sort((a, b) => {
      const volumeDelta = (positive(b?.quote_volume_24h) || 0) - (positive(a?.quote_volume_24h) || 0);
      if (volumeDelta !== 0) return volumeDelta;
      const moveDelta = Math.abs(finite(b?.price_change_percent_24h) || 0) - Math.abs(finite(a?.price_change_percent_24h) || 0);
      if (moveDelta !== 0) return moveDelta;
      return String(a.symbol).localeCompare(String(b.symbol));
    })
    .slice(0, HOT_TARGET);
}

function poolRow(provider, row, role, slot, extra = {}) {
  return {
    provider,
    market_type: 'contract',
    quote_asset: 'USDT',
    symbol: normalizeSymbol(row?.symbol),
    base_asset: rowBase(row),
    role,
    slot,
    selection_reason: role === 'core'
      ? 'fixed_market_cap_priority_available_on_provider'
      : 'provider_local_quote_volume_24h_top5_excluding_core',
    market_cap_priority: role === 'core' ? Number(extra.marketCapPriority || 0) || null : null,
    heat_rank: role === 'hot' ? Number(extra.heatRank || 0) || null : null,
    heat_metric: role === 'hot' ? HOT_RANK_METRIC : null,
    quote_volume_24h: finite(row?.quote_volume_24h),
    price_change_percent_24h: finite(row?.price_change_percent_24h),
    last_price: finite(row?.last_price ?? row?.price),
    source_time: row?.source_time || row?.cached_at || null,
  };
}

function sameSymbolSet(a = [], b = []) {
  const left = [...a].map((row) => String(row?.symbol || '')).sort().join('|');
  const right = [...b].map((row) => String(row?.symbol || '')).sort().join('|');
  return left === right;
}

function buildProvider(provider, now) {
  const input = getMarketLightInternalSnapshot({ market: 'contract', provider });
  const rows = usableContractRows(input);
  const previous = providerState.get(provider) || null;
  const coreSelected = coreRowsFrom(rows);
  const coreRows = coreSelected.map((entry, index) => poolRow(provider, entry.row, 'core', index + 1, entry));
  const coreBases = new Set(coreRows.map((row) => row.base_asset));

  let hotRows = [];
  let hotRefreshedAt = previous?.hot_refreshed_at_ms || 0;
  const previousHotStillValid = previous &&
    Array.isArray(previous.hot_rows) && previous.hot_rows.length === HOT_TARGET &&
    previous.hot_rows.every((row) => rows.some((candidate) => normalizeSymbol(candidate.symbol) === normalizeSymbol(row.symbol))) &&
    now - hotRefreshedAt < HOT_REFRESH_MS;
  if (previousHotStillValid) {
    const rowBySymbol = new Map(rows.map((row) => [normalizeSymbol(row.symbol), row]));
    hotRows = previous.hot_rows.map((old, index) => poolRow(
      provider,
      rowBySymbol.get(normalizeSymbol(old.symbol)) || old,
      'hot',
      CORE_TARGET + index + 1,
      { heatRank: index + 1 },
    ));
  } else {
    hotRows = hotRowsFrom(rows, coreBases).map((row, index) => poolRow(
      provider,
      row,
      'hot',
      CORE_TARGET + index + 1,
      { heatRank: index + 1 },
    ));
    hotRefreshedAt = now;
  }

  const poolRows = [...coreRows, ...hotRows];
  const ready = input?.ok === true &&
    input.stale !== true &&
    !String(input.last_error || '').trim() &&
    Number(input.row_count || 0) === Number(input.directory_count || 0) &&
    Number(input.directory_count || 0) > 0 &&
    coreRows.length === CORE_TARGET &&
    hotRows.length === HOT_TARGET &&
    new Set(poolRows.map((row) => row.symbol)).size === POOL_TARGET;

  const hotChanged = previous ? !sameSymbolSet(previous.hot_rows, hotRows) : false;
  const current = {
    provider,
    ready,
    input_ready: input?.ok === true && input.stale !== true && !String(input.last_error || '').trim() && Number(input.row_count || 0) === Number(input.directory_count || 0) && Number(input.directory_count || 0) > 0,
    directory_count: Number(input?.directory_count || 0),
    input_row_count: Number(input?.row_count || 0),
    input_shared_round: Number(input?.shared_round || 0),
    input_updated_at: input?.updated_at || null,
    input_last_error: String(input?.last_error || ''),
    core_count: coreRows.length,
    hot_count: hotRows.length,
    pool_count: poolRows.length,
    core_rows: coreRows,
    hot_rows: hotRows,
    rows: poolRows,
    hot_refreshed_at_ms: hotRefreshedAt,
    hot_refreshed_at: hotRefreshedAt ? new Date(hotRefreshedAt).toISOString() : null,
    hot_changed_this_build: hotChanged,
    built_at: new Date(now).toISOString(),
  };
  providerState.set(provider, current);
  return current;
}

async function buildAll(reason = 'interval') {
  if (running) return await running;
  const task = (async () => {
    const now = Date.now();
    lastStartedAt = new Date(now).toISOString();
    try {
      const providers = {};
      for (const provider of PROVIDERS) providers[provider] = buildProvider(provider, now);
      round += 1;
      totalBuilds += 1;
      lastCompletedAt = new Date().toISOString();
      lastError = '';
      responseCache.clear();
      return { reason, providers };
    } catch (error) {
      totalBuildFailures += 1;
      lastError = String(error?.message || error).slice(0, 300);
      throw error;
    }
  })();
  running = task;
  try {
    return await task;
  } finally {
    if (running === task) running = null;
  }
}

function allProvidersReady() {
  return PROVIDERS.every((provider) => providerState.get(provider)?.ready === true);
}

function scheduleStartupRecovery() {
  if (!started || allProvidersReady()) return;
  timer = setTimeout(async () => {
    try {
      await buildAll('startup_recovery');
    } catch {}
    scheduleStartupRecovery();
  }, STARTUP_RETRY_MS);
  timer.unref?.();
}

export function startContractFocusPoolScanner() {
  if (started || process.env.KAKA_DISABLE_CONTRACT_FOCUS_POOL === '1') return;
  started = true;
  timer = setTimeout(async () => {
    try {
      await buildAll('startup');
    } catch {}
    scheduleStartupRecovery();
  }, 8_000);
  timer.unref?.();
  interval = setInterval(() => buildAll('interval').catch(() => {}), SCAN_INTERVAL_MS);
  interval.unref?.();
}

function providerPayload(provider) {
  const state = providerState.get(provider);
  return state ? clone(state) : {
    provider,
    ready: false,
    input_ready: false,
    directory_count: 0,
    input_row_count: 0,
    core_count: 0,
    hot_count: 0,
    pool_count: 0,
    core_rows: [],
    hot_rows: [],
    rows: [],
    input_last_error: 'focus_pool_not_built_yet',
  };
}

function snapshotPayload() {
  const cached = responseCache.get('all');
  if (cached && Date.now() - cached.at <= RESPONSE_CACHE_TTL_MS) {
    responseCacheHits += 1;
    return { ...clone(cached.payload), cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }
  responseCacheMisses += 1;
  const providers = Object.fromEntries(PROVIDERS.map((provider) => [provider, providerPayload(provider)]));
  const rows = PROVIDERS.flatMap((provider) => providers[provider].rows || []);
  const readyProviders = PROVIDERS.filter((provider) => providers[provider].ready).length;
  const payload = {
    ok: true,
    version: VERSION,
    source: 'render_shared_contract_focus_pool_from_existing_market_light_only',
    ready: readyProviders === PROVIDERS.length && rows.length === PROVIDERS.length * POOL_TARGET,
    provider_count: PROVIDERS.length,
    ready_provider_count: readyProviders,
    target_per_provider: POOL_TARGET,
    core_target_per_provider: CORE_TARGET,
    hot_target_per_provider: HOT_TARGET,
    total_target_rows: PROVIDERS.length * POOL_TARGET,
    row_count: rows.length,
    core_market_cap_priority_source_as_of: CORE_SOURCE_AS_OF,
    core_market_cap_priority_candidates: CORE_MARKET_CAP_PRIORITY,
    stablecoins_excluded_from_core: true,
    hot_rank_metric: HOT_RANK_METRIC,
    hot_refresh_minutes: HOT_REFRESH_MS / 60_000,
    scanner_interval_seconds: SCAN_INTERVAL_MS / 1000,
    startup_retry_seconds: STARTUP_RETRY_MS / 1000,
    derived_from_existing_shared_market_light_only: true,
    exchange_requests_started: 0,
    exchange_connections_started: 0,
    reads_scale_with_users: false,
    providers,
    rows,
    round,
    generated_at: lastCompletedAt,
  };
  responseCache.set('all', { at: Date.now(), payload });
  return { ...clone(payload), cache_hit: false, cache_age_ms: 0 };
}


export function getContractFocusPoolInternalSnapshot() {
  return snapshotPayload();
}

export function getContractFocusPoolHealth() {
  const snapshot = snapshotPayload();
  return {
    ok: true,
    version: VERSION,
    enabled: started || process.env.KAKA_DISABLE_CONTRACT_FOCUS_POOL !== '1',
    mode: 'five_provider_fixed_10_core_plus_dynamic_5_hot_shared_registry',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    ready: snapshot.ready,
    provider_count: snapshot.provider_count,
    ready_provider_count: snapshot.ready_provider_count,
    row_count: snapshot.row_count,
    target_per_provider: POOL_TARGET,
    core_target_per_provider: CORE_TARGET,
    hot_target_per_provider: HOT_TARGET,
    core_market_cap_priority_source_as_of: CORE_SOURCE_AS_OF,
    core_market_cap_priority_candidates: CORE_MARKET_CAP_PRIORITY,
    hot_rank_metric: HOT_RANK_METRIC,
    hot_refresh_minutes: HOT_REFRESH_MS / 60_000,
    scanner_interval_seconds: SCAN_INTERVAL_MS / 1000,
    startup_retry_seconds: STARTUP_RETRY_MS / 1000,
    running: Boolean(running),
    round,
    last_started_at: lastStartedAt,
    last_completed_at: lastCompletedAt,
    last_error: lastError,
    total_builds: totalBuilds,
    total_build_failures: totalBuildFailures,
    total_reads: totalReads,
    response_cache_ttl_seconds: RESPONSE_CACHE_TTL_MS / 1000,
    response_cache_hits: responseCacheHits,
    response_cache_misses: responseCacheMisses,
    market_light_internal_reads_only: true,
    market_light_http_rereads_per_user: 0,
    build_lock_releases_after_completion: true,
    startup_recovery_until_all_providers_ready: true,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    reads_scale_with_users: false,
    provider_state: snapshot.providers,
  };
}

function sendJson(res, status, payload) {
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
  });
  res.end(body);
}

export async function handleContractFocusPool(req, res, url) {
  if (url.pathname === HEALTH_ROUTE) {
    sendJson(res, 200, getContractFocusPoolHealth());
    return true;
  }
  if (url.pathname !== SNAPSHOT_ROUTE) return false;
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: VERSION, error: 'method_not_allowed' });
    return true;
  }
  totalReads += 1;
  if (!lastCompletedAt) await buildAll('first_read').catch(() => {});
  sendJson(res, 200, snapshotPayload());
  return true;
}

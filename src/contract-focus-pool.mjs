import http from 'node:http';
import { getMarketLightInternalSnapshot } from './market-light-bridge.mjs';
import { getContractFlowHotScoreMetric, getHotScoreMetricsHealth } from './hot-score-metrics.mjs';

const VERSION = '650.8.15.5';
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
const HOT_RANK_METRIC = 'composite_6_factor_same_venue_shared';
const SCAN_INTERVAL_MS = 60_000;
const STARTUP_RETRY_MS = 15_000;
const HOT_REFRESH_MS = 5 * 60_000;
const RESPONSE_CACHE_TTL_MS = 20_000;
const LAST_GOOD_PRESERVE_MS = 3 * 60_000;
const HOT_SCORE_MIN_FACTORS = 4;
const HOT_SCORE_TOTAL_WEIGHT = 1.0;
const HOT_SCORE_WEIGHTS = Object.freeze({
  quote_turnover_24h: 0.22,
  trade_count_5m: 0.13,
  volatility_24h: 0.18,
  open_interest_change: 0.17,
  active_trade_change: 0.18,
  liquidation_activity_15m: 0.12,
});
const OI_SAMPLE_RETENTION_MS = 25 * 60_000;
const OI_REFERENCE_MIN_AGE_MS = 4 * 60_000;
const OI_SAMPLE_MIN_GAP_MS = 45_000;
const LIQUIDATION_LOCAL_PORT = Number(process.env.KAKA_LIQUIDATION_COLLECTOR_PORT || 10012);
const LIQUIDATION_ACTIVITY_TTL_MS = 70_000;

const oiSamples = new Map();
let liquidationActivityCache = { at: 0, rows: new Map(), providerCoverage: {}, error: '', reads: 0 };

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


function requestLocalJson(path, timeoutMs = 2500) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      hostname: '127.0.0.1',
      port: LIQUIDATION_LOCAL_PORT,
      method: 'GET',
      path,
      headers: { accept: 'application/json' },
    }, (res) => {
      const chunks = [];
      let total = 0;
      res.on('data', (chunk) => {
        total += chunk.length;
        if (total > 8 * 1024 * 1024) {
          res.destroy(new Error('focus_liquidation_local_response_too_large'));
          return;
        }
        chunks.push(chunk);
      });
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        if ((res.statusCode || 500) < 200 || (res.statusCode || 500) >= 300) {
          reject(new Error(`focus_liquidation_local_http_${res.statusCode}:${body.slice(0,180)}`));
          return;
        }
        try { resolve(JSON.parse(body)); }
        catch (error) { reject(error); }
      });
    });
    req.setTimeout(timeoutMs, () => req.destroy(new Error('focus_liquidation_local_timeout')));
    req.on('error', reject);
    req.end();
  });
}

function updateOiSample(provider, row, now = Date.now()) {
  const symbol = normalizeSymbol(row?.symbol);
  const value = positive(row?.open_interest_value ?? row?.open_interest);
  if (!provider || !symbol || value == null) return;
  const key = `${provider}:${symbol}`;
  const history = oiSamples.get(key) || [];
  const last = history.at(-1);
  if (!last || now - last.at >= OI_SAMPLE_MIN_GAP_MS || Math.abs(last.value - value) > Math.max(1e-12, Math.abs(value) * 1e-9)) {
    history.push({ at: now, value });
  }
  const cutoff = now - OI_SAMPLE_RETENTION_MS;
  while (history.length && history[0].at < cutoff) history.shift();
  while (history.length > 30) history.shift();
  oiSamples.set(key, history);
}

function oiChangeMetric(provider, symbol, now = Date.now()) {
  const history = oiSamples.get(`${provider}:${normalizeSymbol(symbol)}`) || [];
  if (history.length < 2) return null;
  const current = history.at(-1);
  let reference = null;
  for (let i = history.length - 2; i >= 0; i -= 1) {
    if (current.at - history[i].at >= OI_REFERENCE_MIN_AGE_MS) {
      reference = history[i];
      break;
    }
  }
  if (!reference || reference.value <= 0 || current.value <= 0) return null;
  return {
    value: Math.abs(Math.log(current.value / reference.value)),
    current: current.value,
    previous: reference.value,
    age_ms: current.at - reference.at,
  };
}

async function loadLiquidationActivity(now = Date.now()) {
  if (liquidationActivityCache.at > 0 && now - liquidationActivityCache.at <= LIQUIDATION_ACTIVITY_TTL_MS) return liquidationActivityCache;
  try {
    const payload = await requestLocalJson('/api/contract-liquidation/market-snapshot', 3500);
    if (payload?.ok !== true) throw new Error('focus_liquidation_market_snapshot_not_ready');
    const rows = new Map();
    for (const row of Array.isArray(payload.rows) ? payload.rows : []) {
      const provider = String(row?.provider || '').trim().toLowerCase().replace('gate.io', 'gate').replace('okex', 'okx');
      const symbol = normalizeSymbol(row?.symbol);
      if (!PROVIDERS.includes(provider) || !symbol || !symbol.endsWith('USDT')) continue;
      rows.set(`${provider}:${symbol}`, {
        total_notional: Math.max(0, finite(row?.total_notional) || 0),
        event_count: Math.max(0, finite(row?.event_count) || 0),
        coverage_complete: row?.coverage_complete === true,
      });
    }
    const providerCoverage = {};
    for (const provider of PROVIDERS) {
      const coverage = payload?.provider_coverage?.[provider] || {};
      providerCoverage[provider] = {
        coverage_complete_pct: finite(coverage?.coverage_complete_pct) || 0,
        connected_pct: finite(coverage?.connected_pct) || 0,
      };
    }
    liquidationActivityCache = {
      at: now,
      rows,
      providerCoverage,
      error: '',
      reads: Number(liquidationActivityCache.reads || 0) + 1,
    };
  } catch (error) {
    liquidationActivityCache = {
      ...liquidationActivityCache,
      error: String(error?.message || error).slice(0, 240),
      reads: Number(liquidationActivityCache.reads || 0) + 1,
    };
  }
  return liquidationActivityCache;
}

function liquidationMetric(provider, symbol, context) {
  const coverage = context?.providerCoverage?.[provider] || {};
  const row = context?.rows?.get(`${provider}:${normalizeSymbol(symbol)}`);
  if (row) {
    const notional = Math.max(0, finite(row.total_notional) || 0);
    const eventCount = Math.max(0, finite(row.event_count) || 0);
    return {
      value: Math.log1p(notional) + 0.25 * Math.log1p(eventCount),
      total_notional: notional,
      event_count: eventCount,
      coverage_complete: row.coverage_complete === true,
      observed_nonzero_event_activity: eventCount > 0,
    };
  }
  // A missing row can only be interpreted as a true zero after the provider's
  // entire 15m market window is complete. During warm-up it remains unavailable.
  if (Number(coverage.coverage_complete_pct || 0) < 99.5) return null;
  return { value: 0, total_notional: 0, event_count: 0, coverage_complete: true, observed_nonzero_event_activity: false };
}

function percentileMap(entries) {
  const valid = entries
    .filter((entry) => entry.value != null && Number.isFinite(entry.value))
    .sort((a, b) => a.value - b.value || a.symbol.localeCompare(b.symbol));
  const result = new Map();
  if (!valid.length) return result;
  if (valid.length === 1) {
    result.set(valid[0].symbol, 1);
    return result;
  }
  for (let i = 0; i < valid.length; i += 1) result.set(valid[i].symbol, i / (valid.length - 1));
  return result;
}

function compositeHotRows(provider, rows, coreBases, liquidationContext, now = Date.now()) {
  const candidates = rows
    .filter((row) => !coreBases.has(rowBase(row)))
    .filter((row) => positive(row?.quote_volume_24h) != null)
    .map((row) => {
      const flow = getContractFlowHotScoreMetric(provider, row.symbol, now);
      const oi = oiChangeMetric(provider, row.symbol, now);
      const liq = liquidationMetric(provider, row.symbol, liquidationContext);
      const factors = {
        quote_turnover_24h: positive(row?.quote_volume_24h),
        trade_count_5m: flow?.trade_count_5m != null ? Math.log1p(Math.max(0, Number(flow.trade_count_5m))) : null,
        volatility_24h: Math.abs(finite(row?.price_change_percent_24h) || 0),
        open_interest_change: oi?.value ?? null,
        active_trade_change: flow?.active_trade_change_log_abs ?? null,
        liquidation_activity_15m: liq?.value ?? null,
      };
      return { row, symbol: normalizeSymbol(row.symbol), factors, flow, oi, liq };
    });

  const rankByFactor = {};
  for (const factor of Object.keys(HOT_SCORE_WEIGHTS)) {
    rankByFactor[factor] = percentileMap(candidates.map((item) => ({ symbol: item.symbol, value: item.factors[factor] })));
  }

  for (const item of candidates) {
    let weighted = 0;
    let availableWeight = 0;
    let factorCount = 0;
    const factorScores = {};
    for (const [factor, weight] of Object.entries(HOT_SCORE_WEIGHTS)) {
      const raw = item.factors[factor];
      const rank = rankByFactor[factor].get(item.symbol);
      if (raw == null || rank == null) {
        factorScores[factor] = { available: false, raw: null, percentile: null, weight };
        continue;
      }
      factorCount += 1;
      availableWeight += weight;
      weighted += rank * weight;
      factorScores[factor] = { available: true, raw, percentile: rank, weight };
    }
    const normalized = availableWeight > 0 ? weighted / availableWeight : 0;
    const coverageRatio = HOT_SCORE_TOTAL_WEIGHT > 0 ? availableWeight / HOT_SCORE_TOTAL_WEIGHT : 0;
    item.score = normalized * (0.80 + 0.20 * coverageRatio);
    item.factorCount = factorCount;
    item.availableWeight = availableWeight;
    item.factorScores = factorScores;
  }

  candidates.sort((a, b) => {
    const aReady = a.factorCount >= HOT_SCORE_MIN_FACTORS ? 1 : 0;
    const bReady = b.factorCount >= HOT_SCORE_MIN_FACTORS ? 1 : 0;
    if (aReady !== bReady) return bReady - aReady;
    if (Math.abs(b.score - a.score) > 1e-12) return b.score - a.score;
    const volumeDelta = (positive(b.row?.quote_volume_24h) || 0) - (positive(a.row?.quote_volume_24h) || 0);
    if (volumeDelta !== 0) return volumeDelta;
    return a.symbol.localeCompare(b.symbol);
  });
  return candidates.slice(0, HOT_TARGET);
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

function hotRowsFrom(provider, rows, coreBases, liquidationContext, now) {
  return compositeHotRows(provider, rows, coreBases, liquidationContext, now);
}

function poolRow(provider, row, role, slot, extra = {}) {
  const score = role === 'hot' ? finite(extra.heatScore) : null;
  const factorScores = role === 'hot' && extra.factorScores && typeof extra.factorScores === 'object'
    ? clone(extra.factorScores)
    : null;
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
      : 'provider_local_composite_hot_score_top5_excluding_core',
    market_cap_priority: role === 'core' ? Number(extra.marketCapPriority || 0) || null : null,
    heat_rank: role === 'hot' ? Number(extra.heatRank || 0) || null : null,
    heat_metric: role === 'hot' ? HOT_RANK_METRIC : null,
    heat_score: score,
    heat_score_mode: role === 'hot' ? 'composite_same_venue_shared_percentile_weighted' : null,
    heat_factor_count: role === 'hot' ? Math.max(0, Number(extra.factorCount || 0)) : null,
    heat_available_weight: role === 'hot' ? finite(extra.availableWeight) : null,
    heat_factor_scores: factorScores,
    heat_source_semantics: role === 'hot'
      ? 'existing_shared_same_venue_market_light_plus_flow_plus_realized_liquidation_only'
      : null,
    flow_metric_source_time: role === 'hot' ? extra.flow?.source_time || null : null,
    liquidation_notional_15m: role === 'hot' ? finite(extra.liq?.total_notional) : null,
    liquidation_event_count_15m: role === 'hot' ? finite(extra.liq?.event_count) : null,
    open_interest_change_current: role === 'hot' ? finite(extra.oi?.current) : null,
    open_interest_change_previous: role === 'hot' ? finite(extra.oi?.previous) : null,
    open_interest_change_age_ms: role === 'hot' ? finite(extra.oi?.age_ms) : null,
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

function canPreservePreviousFocus(previous, now) {
  const previousBuiltMs = Date.parse(String(previous?.built_at || ''));
  return previous?.ready === true &&
    Array.isArray(previous?.rows) && previous.rows.length === POOL_TARGET &&
    Number.isFinite(previousBuiltMs) && now - previousBuiltMs <= LAST_GOOD_PRESERVE_MS;
}

function buildProvider(provider, now, liquidationContext) {
  const input = getMarketLightInternalSnapshot({ market: 'contract', provider });
  const rows = usableContractRows(input);
  const previous = providerState.get(provider) || null;
  for (const row of rows) updateOiSample(provider, row, now);
  const inputReady = input?.ok === true &&
    input.stale !== true &&
    !String(input.last_error || '').trim() &&
    Number(input.row_count || 0) === Number(input.directory_count || 0) &&
    Number(input.directory_count || 0) > 0;

  // Step993.3 stable-layer guard: a transient market-light not-ready window must
  // not erase an already verified 15-row focus pool. Preserve the last-good
  // provider composition for at most three minutes; expose the degraded input
  // separately and fail closed after the bounded preserve window expires.
  const previousBuiltMs = Date.parse(String(previous?.built_at || ''));
  const previousPreservable = canPreservePreviousFocus(previous, now);
  if (!inputReady && previousPreservable) {
    const preserved = {
      ...previous,
      ready: true,
      input_ready: false,
      directory_count: Number(input?.directory_count || previous.directory_count || 0),
      input_row_count: Number(input?.row_count || 0),
      input_shared_round: Number(input?.shared_round || previous.input_shared_round || 0),
      input_updated_at: input?.updated_at || previous.input_updated_at || null,
      input_last_error: String(input?.last_error || 'market_light_transient_not_ready'),
      preserved_last_good_due_to_transient_input: true,
      preserved_last_good_age_ms: Math.max(0, now - previousBuiltMs),
      preserved_last_good_max_ms: LAST_GOOD_PRESERVE_MS,
      hot_changed_this_build: false,
      built_at: previous.built_at,
      last_preserved_at: new Date(now).toISOString(),
    };
    providerState.set(provider, preserved);
    return preserved;
  }

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
      {
        heatRank: index + 1,
        heatScore: old.heat_score,
        factorCount: old.heat_factor_count,
        availableWeight: old.heat_available_weight,
        factorScores: old.heat_factor_scores,
        flow: { source_time: old.flow_metric_source_time },
        liq: { total_notional: old.liquidation_notional_15m, event_count: old.liquidation_event_count_15m },
        oi: { current: old.open_interest_change_current, previous: old.open_interest_change_previous, age_ms: old.open_interest_change_age_ms },
      },
    ));
  } else {
    hotRows = hotRowsFrom(provider, rows, coreBases, liquidationContext, now).map((scored, index) => poolRow(
      provider,
      scored.row,
      'hot',
      CORE_TARGET + index + 1,
      {
        heatRank: index + 1,
        heatScore: scored.score,
        factorCount: scored.factorCount,
        availableWeight: scored.availableWeight,
        factorScores: scored.factorScores,
        flow: scored.flow,
        liq: scored.liq,
        oi: scored.oi,
      },
    ));
    hotRefreshedAt = now;
  }

  const poolRows = [...coreRows, ...hotRows];
  const ready = inputReady &&
    coreRows.length === CORE_TARGET &&
    hotRows.length === HOT_TARGET &&
    new Set(poolRows.map((row) => row.symbol)).size === POOL_TARGET;

  const hotChanged = previous ? !sameSymbolSet(previous.hot_rows, hotRows) : false;
  const current = {
    provider,
    ready,
    input_ready: inputReady,
    preserved_last_good_due_to_transient_input: false,
    preserved_last_good_age_ms: 0,
    preserved_last_good_max_ms: LAST_GOOD_PRESERVE_MS,
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
    step999_composite_score_ready: hotRows.length === HOT_TARGET && hotRows.every((row) => row.heat_score != null && Number(row.heat_factor_count || 0) >= HOT_SCORE_MIN_FACTORS),
    hot_score_min_required_factors: HOT_SCORE_MIN_FACTORS,
    hot_score_min_observed_factors: hotRows.length ? Math.min(...hotRows.map((row) => Number(row.heat_factor_count || 0))) : 0,
    hot_score_max_observed_factors: hotRows.length ? Math.max(...hotRows.map((row) => Number(row.heat_factor_count || 0))) : 0,
    hot_score_factor_weights: HOT_SCORE_WEIGHTS,
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
      const liquidationContext = await loadLiquidationActivity(now);
      const providers = {};
      for (const provider of PROVIDERS) providers[provider] = buildProvider(provider, now, liquidationContext);
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
    step999_composite_hot_score_ready: PROVIDERS.every((provider) => providers[provider]?.step999_composite_score_ready === true),
    step999_hot_score_weights: HOT_SCORE_WEIGHTS,
    step999_hot_score_min_factors: HOT_SCORE_MIN_FACTORS,
    step999_hot_score_sources: ['market_light_quote_turnover_24h','existing_shared_flow_trade_count_5m','market_light_abs_price_change_24h','same_venue_market_light_open_interest_change','existing_shared_flow_active_trade_change','existing_shared_realized_liquidation_15m'],
    step999_no_additional_exchange_requests: true,
    step999_no_cross_provider_or_quote_substitution: true,
    step999_realized_liquidation_not_estimated_risk: true,
    hot_refresh_minutes: HOT_REFRESH_MS / 60_000,
    scanner_interval_seconds: SCAN_INTERVAL_MS / 1000,
    startup_retry_seconds: STARTUP_RETRY_MS / 1000,
    last_good_preserve_seconds: LAST_GOOD_PRESERVE_MS / 1000,
    transient_input_preserves_last_good_pool: true,
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
    step999_composite_hot_score_ready: snapshot.step999_composite_hot_score_ready === true,
    step999_hot_score_weights: HOT_SCORE_WEIGHTS,
    step999_hot_score_min_factors: HOT_SCORE_MIN_FACTORS,
    step999_hot_score_sources: snapshot.step999_hot_score_sources,
    step999_no_additional_exchange_requests: true,
    step999_no_cross_provider_or_quote_substitution: true,
    step999_realized_liquidation_not_estimated_risk: true,
    step999_flow_metric_health: getHotScoreMetricsHealth(),
    step999_liquidation_local_cache: {
      age_ms: liquidationActivityCache.at ? Math.max(0, Date.now() - liquidationActivityCache.at) : null,
      rows: liquidationActivityCache.rows?.size || 0,
      reads: Number(liquidationActivityCache.reads || 0),
      error: liquidationActivityCache.error || '',
      local_collector_port: LIQUIDATION_LOCAL_PORT,
      exchange_requests_started: 0,
    },
    hot_refresh_minutes: HOT_REFRESH_MS / 60_000,
    scanner_interval_seconds: SCAN_INTERVAL_MS / 1000,
    startup_retry_seconds: STARTUP_RETRY_MS / 1000,
    last_good_preserve_seconds: LAST_GOOD_PRESERVE_MS / 1000,
    transient_input_preserves_last_good_pool: true,
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

export const __contractFocusPoolTest = Object.freeze({ canPreservePreviousFocus, compositeHotRows, percentileMap });

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

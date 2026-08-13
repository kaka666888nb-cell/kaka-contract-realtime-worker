import { getContractFocusPoolInternalSnapshot } from './contract-focus-pool.mjs';
import { getMarketLightInternalSnapshot } from './market-light-snapshot.mjs';

const VERSION = '650.8.15.4';
const SNAPSHOT_ROUTE = '/api/bitget-advanced/current-snapshot';
const HEALTH_ROUTE = '/api/bitget-advanced/health';
const HISTORY_ROUTE = '/api/bitget-advanced/history-snapshot';
const BASE = 'https://api.bitget.com';

const START_DELAY_MS = Math.max(2_000, Number(process.env.KAKA_BITGET_ADVANCED_START_DELAY_MS || 8_000));
const STARTUP_RETRY_MS = Math.max(10_000, Number(process.env.KAKA_BITGET_ADVANCED_STARTUP_RETRY_MS || 15_000));
const FOCUS_REFRESH_MS = Math.max(2 * 60_000, Number(process.env.KAKA_BITGET_ADVANCED_FOCUS_REFRESH_MS || 5 * 60_000));
const BATCH_REFRESH_MS = Math.max(30_000, Number(process.env.KAKA_BITGET_ADVANCED_BATCH_REFRESH_MS || 60_000));
const RESPONSE_CACHE_TTL_MS = Math.max(3_000, Number(process.env.KAKA_BITGET_ADVANCED_RESPONSE_CACHE_TTL_MS || 20_000));
const STALE_MS = Math.max(5 * 60_000, Number(process.env.KAKA_BITGET_ADVANCED_STALE_MS || 12 * 60_000));
const ONE_PER_SECOND_GAP_MS = Math.max(1_020, Number(process.env.KAKA_BITGET_ADVANCED_1S_GAP_MS || 1_080));
const FOCUS_TARGET = 15;
const RISK_HISTORY_START_DELAY_MS = Math.max(12_000, Number(process.env.KAKA_BITGET_RISK_HISTORY_START_DELAY_MS || 18_000));
const RISK_HISTORY_REFRESH_MS = Math.max(60 * 60_000, Number(process.env.KAKA_BITGET_RISK_HISTORY_REFRESH_MS || 6 * 60 * 60_000));
const RISK_HISTORY_STALE_MS = Math.max(6 * 60 * 60_000, Number(process.env.KAKA_BITGET_RISK_HISTORY_STALE_MS || 12 * 60 * 60_000));
const RISK_HISTORY_WATCH_MS = Math.max(20_000, Number(process.env.KAKA_BITGET_RISK_HISTORY_WATCH_MS || 30_000));
const RISK_HISTORY_REQUEST_GAP_MS = Math.max(300, Number(process.env.KAKA_BITGET_RISK_HISTORY_REQUEST_GAP_MS || 380));
const OFFICIAL_5M_HISTORY_LIMIT = Math.max(36, Math.min(576, Number(process.env.KAKA_BITGET_OFFICIAL_5M_HISTORY_LIMIT || 288)));
const DERIVED_HISTORY_INTERVALS = Object.freeze({
  '15m': 15 * 60_000,
  '1h': 60 * 60_000,
  '4h': 4 * 60 * 60_000,
  '1d': 24 * 60 * 60_000,
});
const OFFICIAL_HISTORY_LANES = Object.freeze([
  'futures_active_buy_sell',
  'futures_long_short',
  'futures_position_long_short',
  'futures_account_long_short',
]);

let started = false;
let focusRunning = null;
let batchRunning = null;
let focusTimer = null;
let batchTimer = null;
let focusRecoveryTimer = null;
let fundingRecoveryTimer = null;
let focusInterval = null;
let batchInterval = null;
let riskHistoryRunning = null;
let riskHistoryTimer = null;
let riskHistoryInterval = null;
let riskHistoryWatchInterval = null;
let riskHistoryLastSignature = '';
let riskHistoryLastStartedAt = null;
let riskHistoryLastCompletedAt = null;
let riskHistoryLastError = '';
let riskHistoryBuilds = 0;
let riskHistoryFailures = 0;
let round = 0;
let totalReads = 0;
let responseCacheHits = 0;
let responseCacheMisses = 0;
let lastFocusStartedAt = null;
let lastFocusCompletedAt = null;
let lastBatchStartedAt = null;
let lastBatchCompletedAt = null;
let lastFocusError = '';
let lastBatchError = '';
let totalFocusBuilds = 0;
let totalFocusFailures = 0;
let totalBatchBuilds = 0;
let totalBatchFailures = 0;

let contractRows = new Map();
let spotRows = new Map();
let fundingRows = new Map();
let riskReservePools = [];
let oiLimitRows = new Map();
let riskReserveHistoryByPool = new Map();
const contractOfficial5mHistory = new Map(
  OFFICIAL_HISTORY_LANES.map((lane) => [lane, new Map()]),
);
let official5mHistoryCaptures = 0;
let official5mHistoryRowsCaptured = 0;
let official5mHistoryLastUpdatedAt = null;

const responseCache = new Map();
const laneStats = new Map();

function finite(value) {
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
function sleep(ms) {
  return new Promise((resolve) => {
    const t = setTimeout(resolve, ms);
    t.unref?.();
  });
}
function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}
function latestByTs(rows, keys = ['ts', 'date']) {
  const list = Array.isArray(rows) ? rows : [];
  let best = null;
  let bestTs = -1;
  for (const row of list) {
    const raw = keys.map((key) => row?.[key]).find((value) => value != null);
    const ts = Number(raw);
    if (!Number.isFinite(ts)) continue;
    if (ts > bestTs) {
      bestTs = ts;
      best = row;
    }
  }
  return best || list[0] || null;
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
async function fetchJson(path, { timeoutMs = 18_000, lane = 'unknown' } = {}) {
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
        'user-agent': 'KakaWeb3/650.8.15.96 bitget-advanced-shared',
      },
      signal: controller.signal,
    });
    const text = await response.text();
    let payload = null;
    try { payload = JSON.parse(text); } catch (_) {}
    if (!response.ok) throw new Error(`bitget_http_${response.status}:${lane}`);
    if (String(payload?.code ?? '00000') !== '00000') {
      throw new Error(`bitget_code_${payload?.code ?? 'unknown'}:${String(payload?.msg || '')}:${lane}`);
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

function bitgetFocusTargets() {
  const focus = getContractFocusPoolInternalSnapshot();
  const rows = (Array.isArray(focus?.rows) ? focus.rows : [])
    .filter((row) => row?.provider === 'bitget' && row?.market_type === 'contract')
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
    unique.push(row);
  }
  return {
    focus_ready: focus?.ready === true,
    focus_round: Number(focus?.round || 0),
    rows: unique.slice(0, FOCUS_TARGET),
  };
}

function bitgetSpotTargetSymbols(contractTargets) {
  const spot = getMarketLightInternalSnapshot({ market: 'spot', provider: 'bitget' });
  const symbols = new Set((Array.isArray(spot?.rows) ? spot.rows : []).map((row) => compact(row?.symbol)));
  return contractTargets
    .map((row) => row.symbol)
    .filter((symbol) => symbols.has(symbol))
    .slice(0, FOCUS_TARGET);
}

function normalizedHistoryRowForLane(lane, raw, symbol) {
  const ts = Number(raw?.ts || 0);
  if (!Number.isFinite(ts) || ts <= 0) return null;

  if (lane === 'futures_active_buy_sell') {
    return {
      symbol,
      ts,
      source_time: isoMs(ts),
      buy_volume: finite(raw?.buyVolume),
      sell_volume: finite(raw?.sellVolume),
    };
  }
  if (lane === 'futures_long_short') {
    return {
      symbol,
      ts,
      source_time: isoMs(ts),
      long_ratio: finite(raw?.longRatio),
      short_ratio: finite(raw?.shortRatio),
      long_short_ratio: finite(raw?.longShortRatio),
    };
  }
  if (lane === 'futures_position_long_short') {
    return {
      symbol,
      ts,
      source_time: isoMs(ts),
      long_position_ratio: finite(raw?.longPositionRatio),
      short_position_ratio: finite(raw?.shortPositionRatio),
      long_short_position_ratio: finite(raw?.longShortPositionRatio),
    };
  }
  if (lane === 'futures_account_long_short') {
    return {
      symbol,
      ts,
      source_time: isoMs(ts),
      long_account_ratio: finite(raw?.longAccountRatio),
      short_account_ratio: finite(raw?.shortAccountRatio),
      long_short_account_ratio: finite(raw?.longShortAccountRatio),
    };
  }
  return null;
}

function captureOfficial5mHistory(lane, symbol, payload) {
  const laneMap = contractOfficial5mHistory.get(lane);
  if (!laneMap) return 0;
  const rawRows = Array.isArray(payload?.data) ? payload.data : [];
  const rows = rawRows
    .map((raw) => normalizedHistoryRowForLane(lane, raw, symbol))
    .filter(Boolean)
    .sort((a, b) => a.ts - b.ts);

  const dedup = [];
  const seen = new Set();
  for (const row of rows) {
    if (seen.has(row.ts)) continue;
    seen.add(row.ts);
    dedup.push(row);
  }
  const limited = dedup.slice(-OFFICIAL_5M_HISTORY_LIMIT);
  laneMap.set(symbol, {
    provider: 'bitget',
    market_type: 'contract',
    symbol,
    lane,
    official: true,
    period: '5m',
    source: 'bitget_official_public_uta_advanced_statistics',
    row_count: limited.length,
    rows: limited,
    captured_at: new Date().toISOString(),
    additional_exchange_requests: 0,
    reused_existing_step991_response: true,
  });
  official5mHistoryCaptures += 1;
  official5mHistoryRowsCaptured += limited.length;
  official5mHistoryLastUpdatedAt = new Date().toISOString();
  return limited.length;
}

function averageFinite(rows, field) {
  const values = rows.map((row) => finite(row?.[field])).filter((value) => value != null);
  if (!values.length) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}
function sumFinite(rows, field) {
  const values = rows.map((row) => finite(row?.[field])).filter((value) => value != null);
  if (!values.length) return null;
  return values.reduce((sum, value) => sum + value, 0);
}

function derivedRollupRows(lane, baseRows, interval) {
  const intervalMs = DERIVED_HISTORY_INTERVALS[interval];
  if (!intervalMs) return [];
  const expectedSamples = Math.max(1, Math.round(intervalMs / (5 * 60_000)));
  const buckets = new Map();

  for (const row of baseRows) {
    const bucket = Math.floor(Number(row.ts || 0) / intervalMs) * intervalMs;
    if (!Number.isFinite(bucket) || bucket <= 0) continue;
    const group = buckets.get(bucket) || [];
    group.push(row);
    buckets.set(bucket, group);
  }

  return [...buckets.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([bucketStart, rows]) => {
      const base = {
        ts: bucketStart,
        source_time: isoMs(bucketStart),
        interval,
        official: false,
        derived_from_official_5m: true,
        sample_count: rows.length,
        expected_samples: expectedSamples,
        complete_bucket: rows.length >= expectedSamples,
      };

      if (lane === 'futures_active_buy_sell') {
        const buy = sumFinite(rows, 'buy_volume');
        const sell = sumFinite(rows, 'sell_volume');
        return {
          ...base,
          aggregation: 'sum_official_5m_volumes',
          buy_volume: buy,
          sell_volume: sell,
          buy_sell_ratio: buy != null && sell != null && sell !== 0 ? buy / sell : null,
        };
      }
      if (lane === 'futures_long_short') {
        return {
          ...base,
          aggregation: 'arithmetic_mean_official_5m_snapshots',
          long_ratio: averageFinite(rows, 'long_ratio'),
          short_ratio: averageFinite(rows, 'short_ratio'),
          long_short_ratio: averageFinite(rows, 'long_short_ratio'),
        };
      }
      if (lane === 'futures_position_long_short') {
        return {
          ...base,
          aggregation: 'arithmetic_mean_official_5m_snapshots',
          long_position_ratio: averageFinite(rows, 'long_position_ratio'),
          short_position_ratio: averageFinite(rows, 'short_position_ratio'),
          long_short_position_ratio: averageFinite(rows, 'long_short_position_ratio'),
        };
      }
      if (lane === 'futures_account_long_short') {
        return {
          ...base,
          aggregation: 'arithmetic_mean_official_5m_snapshots',
          long_account_ratio: averageFinite(rows, 'long_account_ratio'),
          short_account_ratio: averageFinite(rows, 'short_account_ratio'),
          long_short_account_ratio: averageFinite(rows, 'long_short_account_ratio'),
        };
      }
      return base;
    });
}

function contractHistoryCoverage() {
  const focus = bitgetFocusTargets();
  const focusSymbols = focus.rows.map((row) => row.symbol);
  const lanes = {};
  let allOfficialCoverage = true;
  for (const lane of OFFICIAL_HISTORY_LANES) {
    const laneMap = contractOfficial5mHistory.get(lane);
    const covered = focusSymbols.filter((symbol) => Number(laneMap?.get(symbol)?.row_count || 0) > 0).length;
    lanes[lane] = {
      focus_coverage: covered,
      focus_target: FOCUS_TARGET,
      official_5m_rows: focusSymbols.reduce((sum, symbol) => sum + Number(laneMap?.get(symbol)?.row_count || 0), 0),
    };
    if (covered !== FOCUS_TARGET) allOfficialCoverage = false;
  }
  return {
    ready: focus.focus_ready && focusSymbols.length === FOCUS_TARGET && allOfficialCoverage,
    focus_target: FOCUS_TARGET,
    official_lane_count: OFFICIAL_HISTORY_LANES.length,
    official_5m_history_limit: OFFICIAL_5M_HISTORY_LIMIT,
    lanes,
    captures: official5mHistoryCaptures,
    rows_captured_total: official5mHistoryRowsCaptured,
    last_updated_at: official5mHistoryLastUpdatedAt,
    additional_exchange_requests: 0,
    reused_existing_step991_response_arrays: true,
    shared_backend_memory: true,
    user_reads_trigger_exchange_requests: false,
    reads_scale_with_users: false,
    derived_intervals: Object.keys(DERIVED_HISTORY_INTERVALS),
    official_and_derived_kept_separate: true,
  };
}

function contractHistorySnapshot({ symbol, lane, interval = '5m' } = {}) {
  const cleanSymbol = compact(symbol);
  if (!cleanSymbol) return { ok: false, version: VERSION, error: 'symbol_required' };
  if (!OFFICIAL_HISTORY_LANES.includes(lane)) {
    return { ok: false, version: VERSION, error: 'unsupported_lane', supported_lanes: OFFICIAL_HISTORY_LANES };
  }
  const item = contractOfficial5mHistory.get(lane)?.get(cleanSymbol) || null;
  if (!item) {
    return {
      ok: true,
      version: VERSION,
      provider: 'bitget',
      market_type: 'contract',
      symbol: cleanSymbol,
      lane,
      interval,
      official: interval === '5m',
      ready: false,
      rows: [],
      row_count: 0,
      additional_exchange_requests: 0,
      user_read_triggered_exchange_requests: false,
      reads_scale_with_users: false,
    };
  }

  if (interval === '5m') {
    return {
      ok: true,
      version: VERSION,
      ready: true,
      ...clone(item),
      interval: '5m',
      official: true,
      derived_from_official_5m: false,
      additional_exchange_requests: 0,
      user_read_triggered_exchange_requests: false,
      reads_scale_with_users: false,
    };
  }

  if (!DERIVED_HISTORY_INTERVALS[interval]) {
    return {
      ok: false,
      version: VERSION,
      error: 'unsupported_interval',
      supported_intervals: ['5m', ...Object.keys(DERIVED_HISTORY_INTERVALS)],
    };
  }
  const rows = derivedRollupRows(lane, item.rows, interval);
  return {
    ok: true,
    version: VERSION,
    ready: rows.length > 0,
    provider: 'bitget',
    market_type: 'contract',
    symbol: cleanSymbol,
    lane,
    interval,
    official: false,
    source: 'derived_rollup_from_bitget_official_5m_history',
    derived_from_official_5m: true,
    official_base_period: '5m',
    official_base_row_count: item.row_count,
    row_count: rows.length,
    rows,
    additional_exchange_requests: 0,
    user_read_triggered_exchange_requests: false,
    reads_scale_with_users: false,
  };
}


async function runPerSymbolLane(name, symbols, pathFor, parse) {
  const results = new Map();
  for (let i = 0; i < symbols.length; i += 1) {
    const symbol = symbols[i];
    try {
      const payload = await fetchJson(pathFor(symbol), { lane: name });
      if (OFFICIAL_HISTORY_LANES.includes(name)) captureOfficial5mHistory(name, symbol, payload);
      const parsed = parse(payload, symbol);
      if (parsed) results.set(symbol, parsed);
    } catch (_) {}
    if (i < symbols.length - 1) await sleep(ONE_PER_SECOND_GAP_MS);
  }
  setLane(name, { last_rows: results.size });
  return results;
}

function parseActiveBuySell(payload, symbol) {
  const row = latestByTs(payload?.data);
  if (!row) return null;
  return {
    symbol,
    buy_volume: finite(row?.buyVolume),
    sell_volume: finite(row?.sellVolume),
    ts: Number(row?.ts || 0) || null,
    source_time: isoMs(row?.ts),
  };
}
function parseLongShort(payload, symbol) {
  const row = latestByTs(payload?.data);
  if (!row) return null;
  return {
    symbol,
    long_ratio: finite(row?.longRatio),
    short_ratio: finite(row?.shortRatio),
    long_short_ratio: finite(row?.longShortRatio),
    ts: Number(row?.ts || 0) || null,
    source_time: isoMs(row?.ts),
  };
}
function parsePositionLongShort(payload, symbol) {
  const row = latestByTs(payload?.data);
  if (!row) return null;
  return {
    symbol,
    long_position_ratio: finite(row?.longPositionRatio),
    short_position_ratio: finite(row?.shortPositionRatio),
    long_short_position_ratio: finite(row?.longShortPositionRatio),
    ts: Number(row?.ts || 0) || null,
    source_time: isoMs(row?.ts),
  };
}
function parseAccountLongShort(payload, symbol) {
  const row = latestByTs(payload?.data);
  if (!row) return null;
  return {
    symbol,
    long_account_ratio: finite(row?.longAccountRatio),
    short_account_ratio: finite(row?.shortAccountRatio),
    long_short_account_ratio: finite(row?.longShortAccountRatio),
    ts: Number(row?.ts || 0) || null,
    source_time: isoMs(row?.ts),
  };
}
function parsePositionTier(payload, symbol) {
  const rows = Array.isArray(payload?.data) ? payload.data : [];
  if (!rows.length) return null;
  return {
    symbol,
    tiers: rows.map((row) => ({
      tier: Number(row?.tier || 0) || null,
      min_tier_value: finite(row?.minTierValue),
      max_tier_value: finite(row?.maxTierValue),
      leverage: finite(row?.leverage),
      maintenance_margin_ratio: finite(row?.mmr),
    })),
  };
}
function parseIndexComponents(payload, symbol) {
  const data = payload?.data;
  const rows = Array.isArray(data?.componentList) ? data.componentList : [];
  if (!rows.length) return null;
  return {
    symbol: compact(data?.symbol) || symbol,
    components: rows.map((row) => ({
      exchange: String(row?.exchange || ''),
      spot_pair: String(row?.spotPair || ''),
      equivalent_price: finite(row?.equivalentPrice),
      weight: finite(row?.weight),
    })),
  };
}
function parseSpotWhale(payload, symbol) {
  const row = latestByTs(payload?.data, ['date', 'ts']);
  if (!row) return null;
  return {
    symbol,
    whale_net_volume: finite(row?.volume),
    ts: Number(row?.date ?? row?.ts ?? 0) || null,
    source_time: isoMs(row?.date ?? row?.ts),
  };
}
function parseSpotFund(payload, symbol) {
  const row = payload?.data;
  if (!row || Array.isArray(row)) return null;
  return {
    symbol,
    whale_buy_volume: finite(row?.whaleBuyVolume),
    dolphin_buy_volume: finite(row?.dolphinBuyVolume),
    fish_buy_volume: finite(row?.fishBuyVolume),
    whale_sell_volume: finite(row?.whaleSellVolume),
    dolphin_sell_volume: finite(row?.dolphinSellVolume),
    fish_sell_volume: finite(row?.fishSellVolume),
    whale_buy_ratio: finite(row?.whaleBuyRatio),
    dolphin_buy_ratio: finite(row?.dolphinBuyRatio),
    fish_buy_ratio: finite(row?.fishBuyRatio),
    whale_sell_ratio: finite(row?.whaleSellRatio),
    dolphin_sell_ratio: finite(row?.dolphinSellRatio),
    fish_sell_ratio: finite(row?.fishSellRatio),
  };
}
function parseSpotNet(payload, symbol) {
  const row = latestByTs(payload?.data);
  if (!row) return null;
  return {
    symbol,
    net_capital_inflow_24h: finite(row?.netFlow),
    ts: Number(row?.ts || 0) || null,
    source_time: isoMs(row?.ts),
  };
}

function mergeContractStats(targets, lanes) {
  const out = new Map();
  for (const target of targets) {
    const symbol = target.symbol;
    const row = {
      provider: 'bitget',
      market_type: 'contract',
      symbol,
      base_asset: target.base_asset || (symbol.endsWith('USDT') ? symbol.slice(0, -4) : null),
      quote_asset: 'USDT',
      focus_role: target.role || null,
      focus_slot: target.slot || null,
      period: '5m',
      active_buy_sell: lanes.active.get(symbol) || null,
      long_short: lanes.longShort.get(symbol) || null,
      position_long_short: lanes.position.get(symbol) || null,
      account_long_short: lanes.account.get(symbol) || null,
      position_tier: lanes.tier.get(symbol) || null,
      index_components: lanes.index.get(symbol) || null,
      source: 'bitget_official_public_uta_advanced_statistics',
      source_checked_at: '2026-08-12',
      updated_at: new Date().toISOString(),
    };
    const hasAny = row.active_buy_sell || row.long_short || row.position_long_short || row.account_long_short || row.position_tier || row.index_components;
    if (hasAny) out.set(symbol, row);
  }
  return out;
}
function mergeSpotStats(symbols, lanes) {
  const out = new Map();
  for (const symbol of symbols) {
    const row = {
      provider: 'bitget',
      market_type: 'spot',
      symbol,
      base_asset: symbol.endsWith('USDT') ? symbol.slice(0, -4) : null,
      quote_asset: 'USDT',
      whale_net_flow: lanes.whale.get(symbol) || null,
      fund_flow: lanes.fund.get(symbol) || null,
      net_capital_flow_24h: lanes.net.get(symbol) || null,
      source: 'bitget_official_public_uta_spot_trading_data',
      source_checked_at: '2026-08-12',
      updated_at: new Date().toISOString(),
    };
    if (row.whale_net_flow || row.fund_flow || row.net_capital_flow_24h) out.set(symbol, row);
  }
  return out;
}

async function refreshFocusStats(reason = 'interval') {
  if (focusRunning) return await focusRunning;
  const task = (async () => {
    lastFocusStartedAt = new Date().toISOString();
    totalFocusBuilds += 1;
    const targetState = bitgetFocusTargets();
    const targets = targetState.rows;
    if (!targetState.focus_ready || targets.length !== FOCUS_TARGET) {
      throw new Error(`bitget_focus_not_ready:${targets.length}/${FOCUS_TARGET}`);
    }
    const contractSymbols = targets.map((row) => row.symbol);
    const spotSymbols = bitgetSpotTargetSymbols(targets);

    const [
      active,
      longShort,
      position,
      account,
      tier,
      index,
      whale,
      fund,
      net,
    ] = await Promise.all([
      runPerSymbolLane(
        'futures_active_buy_sell',
        contractSymbols,
        (symbol) => `/api/v3/market/futures-active-buy-sell?symbol=${encodeURIComponent(symbol)}&period=5m`,
        parseActiveBuySell,
      ),
      runPerSymbolLane(
        'futures_long_short',
        contractSymbols,
        (symbol) => `/api/v3/market/futures-long-short?symbol=${encodeURIComponent(symbol)}&period=5m`,
        parseLongShort,
      ),
      runPerSymbolLane(
        'futures_position_long_short',
        contractSymbols,
        (symbol) => `/api/v3/market/futures-position-long-short?symbol=${encodeURIComponent(symbol)}&period=5m`,
        parsePositionLongShort,
      ),
      runPerSymbolLane(
        'futures_account_long_short',
        contractSymbols,
        (symbol) => `/api/v3/market/futures-account-long-short?symbol=${encodeURIComponent(symbol)}&period=5m`,
        parseAccountLongShort,
      ),
      runPerSymbolLane(
        'position_tier',
        contractSymbols,
        (symbol) => `/api/v3/market/position-tier?category=USDT-FUTURES&symbol=${encodeURIComponent(symbol)}`,
        parsePositionTier,
      ),
      runPerSymbolLane(
        'index_components',
        contractSymbols,
        (symbol) => `/api/v3/market/index-components?symbol=${encodeURIComponent(symbol)}`,
        parseIndexComponents,
      ),
      runPerSymbolLane(
        'spot_whale_flow',
        spotSymbols,
        (symbol) => `/api/v3/market/spot-whale-flow?symbol=${encodeURIComponent(symbol)}`,
        parseSpotWhale,
      ),
      runPerSymbolLane(
        'spot_fund_flow',
        spotSymbols,
        (symbol) => `/api/v3/market/spot-fund-flow?symbol=${encodeURIComponent(symbol)}&period=15m`,
        parseSpotFund,
      ),
      runPerSymbolLane(
        'spot_net_flow',
        spotSymbols,
        (symbol) => `/api/v3/market/spot-net-flow?symbol=${encodeURIComponent(symbol)}`,
        parseSpotNet,
      ),
    ]);

    const nextContractRows = mergeContractStats(targets, { active, longShort, position, account, tier, index });
    const nextSpotRows = mergeSpotStats(spotSymbols, { whale, fund, net });

    if (nextContractRows.size === FOCUS_TARGET) contractRows = nextContractRows;
    else if (!contractRows.size) contractRows = nextContractRows;

    if (nextSpotRows.size > 0) spotRows = nextSpotRows;

    round += 1;
    lastFocusCompletedAt = new Date().toISOString();
    lastFocusError = '';
    responseCache.clear();
    return true;
  })();

  focusRunning = task;
  try {
    return await task;
  } catch (error) {
    totalFocusFailures += 1;
    lastFocusCompletedAt = new Date().toISOString();
    lastFocusError = String(error?.message || error);
    return false;
  } finally {
    if (focusRunning === task) focusRunning = null;
  }
}


function riskReservePoolKey(pool) {
  const coin = compact(pool?.coin);
  const symbols = (Array.isArray(pool?.symbols) ? pool.symbols : []).map(compact).filter(Boolean).sort();
  return `${coin}:${symbols.join(',')}`;
}

function riskReserveHistoryTargets() {
  const focus = bitgetFocusTargets();
  const mapped = [];
  const pools = new Map();

  for (const target of focus.rows) {
    const pool = riskReservePools.find((item) => Array.isArray(item?.symbols) && item.symbols.includes(target.symbol));
    if (!pool) {
      mapped.push({ ...target, pool_key: '', representative_symbol: '', pool_found: false });
      continue;
    }
    const key = riskReservePoolKey(pool);
    let group = pools.get(key);
    if (!group) {
      group = {
        pool_key: key,
        coin: compact(pool.coin),
        symbols: [...pool.symbols].map(compact).filter(Boolean).sort(),
        representative_symbol: target.symbol,
        mapped_focus_symbols: [],
      };
      pools.set(key, group);
    }
    group.mapped_focus_symbols.push(target.symbol);
    mapped.push({
      ...target,
      pool_key: key,
      representative_symbol: group.representative_symbol,
      pool_found: true,
    });
  }

  return {
    focus_ready: focus.focus_ready,
    focus_round: focus.focus_round,
    mapped_focus_rows: mapped,
    pools: [...pools.values()].map((group) => ({
      ...group,
      mapped_focus_symbols: [...new Set(group.mapped_focus_symbols)].sort(),
    })),
  };
}

function riskHistorySignature(targets) {
  return targets.pools
    .map((pool) => `${pool.pool_key}:${pool.representative_symbol}:${pool.mapped_focus_symbols.join(',')}`)
    .sort()
    .join('|');
}

function parseRiskReserveHistory(payload, { lane, pool, granularity }) {
  const data = payload?.data && typeof payload.data === 'object' ? payload.data : {};
  const coin = compact(data?.coin);
  const list = Array.isArray(data?.riskReserveRecords) ? data.riskReserveRecords : [];
  const rows = list.map((raw) => {
    const ts = Number(raw?.ts || 0);
    if (!Number.isFinite(ts) || ts <= 0) return null;
    return {
      amount: finite(raw?.amount),
      balance: finite(raw?.balance),
      timestamp_ms: ts,
      source_time: isoMs(ts),
    };
  }).filter(Boolean).sort((a, b) => b.timestamp_ms - a.timestamp_ms);

  return {
    official_response: true,
    official_empty: rows.length === 0,
    granularity,
    coin: coin || pool.coin,
    representative_symbol: pool.representative_symbol,
    pool_key: pool.pool_key,
    mapped_focus_symbols: [...pool.mapped_focus_symbols],
    row_count: rows.length,
    rows,
    updated_at: new Date().toISOString(),
    lane,
    source: granularity === 'daily'
      ? 'bitget_official_public_risk_reserve_daily'
      : 'bitget_official_public_risk_reserve_hourly',
    endpoint: granularity === 'daily'
      ? '/api/v3/market/risk-reserve'
      : '/api/v3/market/risk-reserve-hour',
  };
}

function riskHistoryFresh(item) {
  const ms = Date.parse(String(item?.updated_at || ''));
  return item?.official_response === true &&
    Number.isFinite(ms) &&
    Date.now() - ms <= RISK_HISTORY_STALE_MS;
}

function riskHistoryPoolReady(poolKey) {
  const state = riskReserveHistoryByPool.get(poolKey);
  return riskHistoryFresh(state?.daily) && riskHistoryFresh(state?.hourly);
}

async function refreshRiskReserveHistory(reason = 'scheduled', { missingOnly = false } = {}) {
  if (riskHistoryRunning) return await riskHistoryRunning;
  const task = (async () => {
    riskHistoryLastStartedAt = new Date().toISOString();
    riskHistoryBuilds += 1;

    const targets = riskReserveHistoryTargets();
    if (!targets.focus_ready || targets.mapped_focus_rows.length !== FOCUS_TARGET) {
      riskHistoryLastError = `${reason}:focus_not_ready:${targets.mapped_focus_rows.length}/${FOCUS_TARGET}`;
      riskHistoryFailures += 1;
      return false;
    }
    const unmapped = targets.mapped_focus_rows.filter((row) => !row.pool_found);
    if (unmapped.length > 0 || targets.pools.length === 0) {
      riskHistoryLastError = `${reason}:risk_reserve_pool_mapping_missing:${unmapped.map((row) => row.symbol).join(',')}`;
      riskHistoryFailures += 1;
      return false;
    }

    for (const pool of targets.pools) {
      if (missingOnly && riskHistoryPoolReady(pool.pool_key)) continue;
      const previous = riskReserveHistoryByPool.get(pool.pool_key) || {};
      const next = { ...previous, pool: clone(pool) };

      if (!missingOnly || !riskHistoryFresh(previous.daily)) {
        try {
          const payload = await fetchJson(
            `/api/v3/market/risk-reserve?category=USDT-FUTURES&symbol=${encodeURIComponent(pool.representative_symbol)}`,
            { lane: 'risk_reserve_daily_history' },
          );
          next.daily = parseRiskReserveHistory(payload, {
            lane: 'risk_reserve_daily_history',
            pool,
            granularity: 'daily',
          });
        } catch (error) {
          if (!previous.daily?.official_response) {
            next.daily = {
              official_response: false,
              official_empty: false,
              granularity: 'daily',
              representative_symbol: pool.representative_symbol,
              pool_key: pool.pool_key,
              mapped_focus_symbols: [...pool.mapped_focus_symbols],
              row_count: 0,
              rows: [],
              updated_at: null,
              error: String(error?.message || error).slice(0, 240),
            };
          }
        }
        await sleep(RISK_HISTORY_REQUEST_GAP_MS);
      }

      if (!missingOnly || !riskHistoryFresh(previous.hourly)) {
        try {
          const payload = await fetchJson(
            `/api/v3/market/risk-reserve-hour?category=USDT-FUTURES&symbol=${encodeURIComponent(pool.representative_symbol)}`,
            { lane: 'risk_reserve_hourly_history' },
          );
          next.hourly = parseRiskReserveHistory(payload, {
            lane: 'risk_reserve_hourly_history',
            pool,
            granularity: 'hourly',
          });
        } catch (error) {
          if (!previous.hourly?.official_response) {
            next.hourly = {
              official_response: false,
              official_empty: false,
              granularity: 'hourly',
              representative_symbol: pool.representative_symbol,
              pool_key: pool.pool_key,
              mapped_focus_symbols: [...pool.mapped_focus_symbols],
              row_count: 0,
              rows: [],
              updated_at: null,
              error: String(error?.message || error).slice(0, 240),
            };
          }
        }
        await sleep(RISK_HISTORY_REQUEST_GAP_MS);
      }

      riskReserveHistoryByPool.set(pool.pool_key, next);
    }

    // Prune histories for pools no longer intersecting focus15.
    const activeKeys = new Set(targets.pools.map((pool) => pool.pool_key));
    for (const key of [...riskReserveHistoryByPool.keys()]) {
      if (!activeKeys.has(key)) riskReserveHistoryByPool.delete(key);
    }

    riskHistoryLastSignature = riskHistorySignature(targets);
    riskHistoryLastCompletedAt = new Date().toISOString();
    responseCache.clear();

    const ready = targets.pools.every((pool) => riskHistoryPoolReady(pool.pool_key));
    if (!ready) {
      riskHistoryLastError = `${reason}:risk_reserve_history_incomplete`;
      riskHistoryFailures += 1;
      return false;
    }
    riskHistoryLastError = '';
    return true;
  })();

  riskHistoryRunning = task;
  try {
    return await task;
  } finally {
    if (riskHistoryRunning === task) riskHistoryRunning = null;
  }
}

function riskReserveHistorySnapshot() {
  const targets = riskReserveHistoryTargets();
  const pools = targets.pools.map((pool) => {
    const state = riskReserveHistoryByPool.get(pool.pool_key) || {};
    return {
      ...clone(pool),
      daily: riskHistoryFresh(state.daily) ? clone(state.daily) : null,
      hourly: riskHistoryFresh(state.hourly) ? clone(state.hourly) : null,
    };
  });
  const dailyCoverage = pools.filter((pool) => pool.daily != null).length;
  const hourlyCoverage = pools.filter((pool) => pool.hourly != null).length;
  const mappedFocusCount = targets.mapped_focus_rows.filter((row) => row.pool_found).length;
  const dailyNonempty = pools.filter((pool) => Number(pool.daily?.row_count || 0) > 0).length;
  const hourlyNonempty = pools.filter((pool) => Number(pool.hourly?.row_count || 0) > 0).length;
  const requestCap = Math.max(0, targets.pools.length * 2);

  return {
    ready: targets.focus_ready &&
      targets.mapped_focus_rows.length === FOCUS_TARGET &&
      mappedFocusCount === FOCUS_TARGET &&
      targets.pools.length > 0 &&
      dailyCoverage === targets.pools.length &&
      hourlyCoverage === targets.pools.length,
    focus_target: FOCUS_TARGET,
    mapped_focus_rows: mappedFocusCount,
    pool_target_count: targets.pools.length,
    pool_dedup_saved_symbol_queries: Math.max(0, FOCUS_TARGET - targets.pools.length),
    daily_official_pool_coverage: dailyCoverage,
    hourly_official_pool_coverage: hourlyCoverage,
    daily_nonempty_pools: dailyNonempty,
    hourly_nonempty_pools: hourlyNonempty,
    full_cycle_request_cap: requestCap,
    full_cycle_symbol_naive_request_cap: FOCUS_TARGET * 2,
    representative_symbol_per_pool: true,
    current_risk_reserve_all_mapping_reused: true,
    official_daily_endpoint: '/api/v3/market/risk-reserve',
    official_hourly_endpoint: '/api/v3/market/risk-reserve-hour',
    shared_background_collector: true,
    user_reads_trigger_exchange_requests: false,
    exchange_requests_started_by_user_read: 0,
    reads_scale_with_users: false,
    provider_request_governor_reused: true,
    custom_provider_governor_created: false,
    refresh_seconds: Math.round(RISK_HISTORY_REFRESH_MS / 1000),
    stale_seconds: Math.round(RISK_HISTORY_STALE_MS / 1000),
    request_gap_ms: RISK_HISTORY_REQUEST_GAP_MS,
    last_started_at: riskHistoryLastStartedAt,
    last_completed_at: riskHistoryLastCompletedAt,
    last_error: riskHistoryLastError,
    builds: riskHistoryBuilds,
    failures: riskHistoryFailures,
    pools,
  };
}


function refreshFundingRowsFromMarketLight() {
  const snapshot = getMarketLightInternalSnapshot({ market: 'contract', provider: 'bitget' });
  const map = new Map();
  for (const raw of Array.isArray(snapshot?.rows) ? snapshot.rows : []) {
    const symbol = compact(raw?.symbol);
    if (!symbol.endsWith('USDT')) continue;
    if (raw?.funding_rate == null && raw?.funding_interval_hours == null && raw?.next_funding_time == null) continue;
    map.set(symbol, {
      provider: 'bitget',
      market_type: 'contract',
      symbol,
      funding_rate: finite(raw?.funding_rate),
      funding_interval_hours: finite(raw?.funding_interval_hours),
      next_funding_time: raw?.next_funding_time || null,
      next_funding_time_ms: Number(raw?.next_funding_time_ms || 0) || null,
      min_funding_rate: finite(raw?.min_funding_rate),
      max_funding_rate: finite(raw?.max_funding_rate),
      cash_dividend: finite(raw?.cash_dividend),
      cash_dividend_next_update: raw?.cash_dividend_next_update || null,
      source: raw?.funding_rate_source || raw?.source || 'bitget_market_light_shared_funding',
      updated_at: snapshot?.updated_at || new Date().toISOString(),
    });
  }
  if (map.size > 0) fundingRows = map;
  setLane('current_funding_all', {
    last_rows: map.size,
    last_completed_at: new Date().toISOString(),
    last_error: map.size > 0 ? '' : 'market_light_funding_not_ready',
  });
  return map.size;
}

async function refreshBatchStats(reason = 'interval') {
  if (batchRunning) return await batchRunning;
  const task = (async () => {
    lastBatchStartedAt = new Date().toISOString();
    totalBatchBuilds += 1;
    refreshFundingRowsFromMarketLight();
    const [reserveResult, oiLimitResult] = await Promise.allSettled([
      fetchJson('/api/v3/market/risk-reserve-all?category=USDT-FUTURES', { lane: 'risk_reserve_all' }),
      fetchJson('/api/v3/market/oi-limit?category=USDT-FUTURES', { lane: 'oi_limit_all' }),
    ]);

    const errors = [];
    if (fundingRows.size === 0) errors.push('funding:market_light_shared_funding_not_ready');

    if (reserveResult.status === 'fulfilled') {
      const list = Array.isArray(reserveResult.value?.data?.list) ? reserveResult.value.data.list : [];
      const rows = list.map((raw) => ({
        symbols: (Array.isArray(raw?.symbols) ? raw.symbols : []).map(compact).filter(Boolean),
        coin: compact(raw?.coin),
        balance: finite(raw?.balance),
      })).filter((row) => row.symbols.length && row.coin && row.balance != null);
      if (rows.length) riskReservePools = rows;
      setLane('risk_reserve_all', { last_rows: rows.length });
    } else errors.push(`reserve:${String(reserveResult.reason?.message || reserveResult.reason || 'failed')}`);

    if (oiLimitResult.status === 'fulfilled') {
      const list = Array.isArray(oiLimitResult.value?.data) ? oiLimitResult.value.data : [];
      const map = new Map();
      for (const raw of list) {
        const symbol = compact(raw?.symbol);
        if (!symbol.endsWith('USDT')) continue;
        map.set(symbol, {
          symbol,
          individual_position_notional_limit: finite(raw?.notionalValue),
          main_sub_total_notional_limit: finite(raw?.totalNotionalValue),
        });
      }
      if (map.size) oiLimitRows = map;
      setLane('oi_limit_all', { last_rows: map.size });
    } else errors.push(`oi_limit:${String(oiLimitResult.reason?.message || oiLimitResult.reason || 'failed')}`);

    lastBatchCompletedAt = new Date().toISOString();
    lastBatchError = errors.join('|');
    if (riskReservePools.length === 0 && oiLimitRows.size === 0 && fundingRows.size === 0) throw new Error(lastBatchError || 'bitget_batch_stats_empty');
    responseCache.clear();
    return errors.length === 0;
  })();

  batchRunning = task;
  try {
    return await task;
  } catch (error) {
    totalBatchFailures += 1;
    lastBatchCompletedAt = new Date().toISOString();
    lastBatchError = String(error?.message || error);
    return false;
  } finally {
    if (batchRunning === task) batchRunning = null;
  }
}

function focusStartupReady() {
  const focus = bitgetFocusTargets();
  if (!focus.focus_ready || focus.rows.length !== FOCUS_TARGET) return false;
  return focus.rows.every((target) => {
    const row = contractRows.get(target.symbol);
    return Boolean(
      row?.active_buy_sell &&
      row?.long_short &&
      row?.position_long_short &&
      row?.account_long_short &&
      row?.position_tier &&
      row?.index_components
    );
  });
}

function fundingStartupReady() {
  const snapshot = getMarketLightInternalSnapshot({ market: 'contract', provider: 'bitget' });
  const directoryCount = Number(snapshot?.directory_count || 0);
  const rowCount = Number(snapshot?.row_count || 0);
  if (snapshot?.ok !== true || snapshot?.stale === true || String(snapshot?.last_error || '').trim()) return false;
  if (directoryCount <= 0 || rowCount !== directoryCount) return false;
  let officialFundingRows = 0;
  for (const raw of Array.isArray(snapshot?.rows) ? snapshot.rows : []) {
    if (raw?.funding_rate != null || raw?.funding_interval_hours != null || raw?.next_funding_time != null) officialFundingRows += 1;
  }
  return officialFundingRows === directoryCount && fundingRows.size === directoryCount;
}

function scheduleFocusStartupRecovery() {
  if (!started || focusStartupReady() || focusRecoveryTimer) return;
  focusRecoveryTimer = setTimeout(async () => {
    focusRecoveryTimer = null;
    await refreshFocusStats('startup_recovery').catch(() => false);
    if (!focusStartupReady()) scheduleFocusStartupRecovery();
  }, STARTUP_RETRY_MS);
  focusRecoveryTimer.unref?.();
}

function scheduleFundingStartupRecovery() {
  if (!started || fundingStartupReady() || fundingRecoveryTimer) return;
  fundingRecoveryTimer = setTimeout(() => {
    fundingRecoveryTimer = null;
    refreshFundingRowsFromMarketLight();
    if (fundingStartupReady()) {
      if (lastBatchError.includes('funding:market_light_shared_funding_not_ready')) {
        lastBatchError = lastBatchError
          .split('|')
          .filter((part) => part && part !== 'funding:market_light_shared_funding_not_ready')
          .join('|');
      }
      lastBatchCompletedAt = new Date().toISOString();
      responseCache.clear();
      return;
    }
    scheduleFundingStartupRecovery();
  }, STARTUP_RETRY_MS);
  fundingRecoveryTimer.unref?.();
}

export function startBitgetAdvancedStatsScanner() {
  if (started || process.env.KAKA_DISABLE_BITGET_ADVANCED_STATS === '1') return;
  started = true;

  focusTimer = setTimeout(async () => {
    await refreshFocusStats('startup').catch(() => false);
    if (!focusStartupReady()) scheduleFocusStartupRecovery();
  }, START_DELAY_MS);
  focusTimer.unref?.();

  batchTimer = setTimeout(async () => {
    await refreshBatchStats('startup').catch(() => false);
    if (!fundingStartupReady()) scheduleFundingStartupRecovery();
  }, Math.min(START_DELAY_MS + 1_000, 12_000));
  batchTimer.unref?.();

  riskHistoryTimer = setTimeout(async () => {
    if (riskReservePools.length === 0) await refreshBatchStats('risk_history_startup_dependency').catch(() => false);
    await refreshRiskReserveHistory('startup', { missingOnly: false }).catch(() => false);
  }, RISK_HISTORY_START_DELAY_MS);
  riskHistoryTimer.unref?.();

  riskHistoryInterval = setInterval(() => refreshRiskReserveHistory('interval', { missingOnly: false }).catch(() => {}), RISK_HISTORY_REFRESH_MS);
  riskHistoryInterval.unref?.();

  riskHistoryWatchInterval = setInterval(async () => {
    const targets = riskReserveHistoryTargets();
    if (!targets.focus_ready || targets.mapped_focus_rows.length !== FOCUS_TARGET || targets.pools.length === 0) return;
    const signature = riskHistorySignature(targets);
    if (signature !== riskHistoryLastSignature) {
      await refreshRiskReserveHistory('focus_or_pool_change_missing_only', { missingOnly: true }).catch(() => false);
    }
  }, RISK_HISTORY_WATCH_MS);
  riskHistoryWatchInterval.unref?.();

  focusInterval = setInterval(() => refreshFocusStats('interval').catch(() => {}), FOCUS_REFRESH_MS);
  focusInterval.unref?.();
  batchInterval = setInterval(() => refreshBatchStats('interval').catch(() => {}), BATCH_REFRESH_MS);
  batchInterval.unref?.();
}

function statsFresh(updatedAt, maxAge = STALE_MS) {
  const ms = Date.parse(String(updatedAt || ''));
  return Number.isFinite(ms) && Date.now() - ms <= maxAge;
}

function snapshotPayload({ includeRows = true } = {}) {
  const cacheKey = includeRows ? 'full' : 'meta';
  const cached = responseCache.get(cacheKey);
  if (cached && Date.now() - cached.at <= RESPONSE_CACHE_TTL_MS) {
    responseCacheHits += 1;
    return { ...clone(cached.payload), cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }
  responseCacheMisses += 1;

  const focus = bitgetFocusTargets();
  const targetSymbols = focus.rows.map((row) => row.symbol);
  const contract = targetSymbols.map((symbol) => contractRows.get(symbol)).filter(Boolean);
  const spot = [...spotRows.values()];
  const funding = [...fundingRows.values()];
  const oiLimits = [...oiLimitRows.values()];
  const freshContract = contract.filter((row) => statsFresh(row.updated_at));
  const freshSpot = spot.filter((row) => statsFresh(row.updated_at));
  const lane = Object.fromEntries([...laneStats.entries()].map(([key, value]) => [key, { ...value }]));

  const officialStatsRows = freshContract.filter((row) =>
    row.active_buy_sell &&
    row.long_short &&
    row.position_long_short &&
    row.account_long_short
  );
  const coreTargetCount = focus.rows.filter((row) => row.role === 'core').length;
  const hotTargetCount = focus.rows.filter((row) => row.role === 'hot').length;
  const coreOfficialRows = officialStatsRows.filter((row) => row.focus_role === 'core').length;
  const hotOfficialRows = officialStatsRows.filter((row) => row.focus_role === 'hot').length;
  const riskFocusRows = freshContract.filter((row) => row.position_tier && row.index_components).length;
  const riskHistory = riskReserveHistorySnapshot();
  const contractHistory = contractHistoryCoverage();

  const payload = {
    ok: true,
    version: VERSION,
    source: 'render_shared_bitget_official_public_advanced_statistics',
    ready: focus.focus_ready &&
      targetSymbols.length === FOCUS_TARGET &&
      coreTargetCount === 10 &&
      coreOfficialRows === coreTargetCount &&
      officialStatsRows.length >= coreTargetCount &&
      funding.length > 0 &&
      riskReservePools.length > 0 &&
      oiLimits.length > 0,
    focus_target: FOCUS_TARGET,
    focus_round: focus.focus_round,
    focus_symbols: targetSymbols,
    contract_row_count: contract.length,
    contract_official_stats_rows: officialStatsRows.length,
    contract_core_target_count: coreTargetCount,
    contract_hot_target_count: hotTargetCount,
    contract_core_official_stats_rows: coreOfficialRows,
    contract_hot_official_stats_rows: hotOfficialRows,
    contract_risk_reference_rows: riskFocusRows,
    spot_target_count: bitgetSpotTargetSymbols(focus.rows).length,
    spot_row_count: freshSpot.length,
    funding_row_count: funding.length,
    risk_reserve_pool_count: riskReservePools.length,
    oi_limit_row_count: oiLimits.length,
    risk_reserve_history_ready: riskHistory.ready,
    risk_reserve_history_mapped_focus_rows: riskHistory.mapped_focus_rows,
    risk_reserve_history_pool_target_count: riskHistory.pool_target_count,
    risk_reserve_history_pool_dedup_saved_symbol_queries: riskHistory.pool_dedup_saved_symbol_queries,
    risk_reserve_history_daily_official_pool_coverage: riskHistory.daily_official_pool_coverage,
    risk_reserve_history_hourly_official_pool_coverage: riskHistory.hourly_official_pool_coverage,
    risk_reserve_history_daily_nonempty_pools: riskHistory.daily_nonempty_pools,
    risk_reserve_history_hourly_nonempty_pools: riskHistory.hourly_nonempty_pools,
    risk_reserve_history_full_cycle_request_cap: riskHistory.full_cycle_request_cap,
    risk_reserve_history_full_cycle_symbol_naive_request_cap: riskHistory.full_cycle_symbol_naive_request_cap,
    risk_reserve_history_representative_symbol_per_pool: riskHistory.representative_symbol_per_pool,
    risk_reserve_history_current_pool_mapping_reused: riskHistory.current_risk_reserve_all_mapping_reused,
    contract_official_5m_history_ready: contractHistory.ready,
    contract_official_5m_history_lane_count: contractHistory.official_lane_count,
    contract_official_5m_history_additional_exchange_requests: 0,
    contract_official_5m_history_reuses_existing_step991_response_arrays: true,
    contract_official_5m_history_shared_backend_memory: true,
    contract_history_user_reads_trigger_exchange_requests: false,
    contract_history_reads_scale_with_users: false,
    contract_history_derived_intervals: contractHistory.derived_intervals,
    focus_refresh_seconds: Math.round(FOCUS_REFRESH_MS / 1000),
    batch_refresh_seconds: Math.round(BATCH_REFRESH_MS / 1000),
    one_per_second_lane_gap_ms: ONE_PER_SECOND_GAP_MS,
    official_endpoint_rate_policy: {
      spot_whale_flow: '1/sec/IP',
      spot_fund_flow: '1/sec/IP',
      spot_net_flow: '1/sec/IP',
      futures_active_buy_sell: '1/sec/IP',
      futures_long_short: '1/sec/IP',
      futures_position_long_short: '1/sec/IP',
      futures_account_long_short: '1/sec/IP',
      position_tier: '20/sec/IP but collector keeps shared bounded schedule',
      index_components: '10/sec/IP but collector keeps shared bounded schedule',
      current_funding: 'owned by market-light official category batch; slow-stats reuses shared rows and opens no duplicate funding request',
      risk_reserve_all: '3/sec/IP category batch',
      risk_reserve_daily_history: '20/sec/IP official per-symbol endpoint; shared collector deduplicates by current risk-reserve pool and uses one representative focus symbol per pool',
      risk_reserve_hourly_history: '20/sec/IP official per-symbol endpoint; shared collector deduplicates by current risk-reserve pool and uses one representative focus symbol per pool',
      oi_limit: '10/sec/IP category batch',
    },
    official_semantics: {
      spot_whale_flow: 'Bitget official whale buy/sell net volume; never relabeled as our derived taker flow',
      spot_fund_flow: 'Bitget official whale/dolphin/fish buy/sell volume and ratio',
      spot_net_capital_24h: 'Bitget official 24h net capital inflow',
      futures_active_buy_sell: 'Bitget official 5m active buy/sell volume',
      futures_long_short: 'Bitget official 5m long/short ratio',
      futures_position_long_short: 'Bitget official 5m active long/short position ratio',
      futures_account_long_short: 'Bitget official 5m active long/short account ratio',
      futures_official_history: 'The existing Step991 5m response arrays are retained as official shared history with zero additional Bitget requests',
      futures_history_rollups: '15m/1h/4h/1d are backend-derived from the retained official 5m rows and are explicitly marked derived, never relabeled official',
      position_tier: 'Bitget official position tier/leverage/MMR',
      index_components: 'Bitget official index price components and weights',
      risk_reserve_all: 'Bitget official current insurance-fund pools and symbol membership',
      risk_reserve_history: 'Bitget official daily and hourly risk-reserve records; records are kept under their native endpoint granularity and are never derived from current balance',
      risk_reserve_history_pool_dedup: 'Current risk-reserve-all pool membership is reused to avoid repeated history calls for focus symbols that share the same reserve pool; one focus representative symbol is queried per pool',
      oi_limit: 'Bitget official individual and main/sub-account notional limits',
      funding: 'Bitget official current funding; market-light owns the optional-symbol category batch and slow-stats reuses it without duplicate upstream',
    },
    separate_from_derived_data: true,
    no_cross_provider_substitution: true,
    no_cross_quote_substitution: true,
    missing_stays_null: true,
    collector_only_exchange_requests: true,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_collector: false,
    reads_scale_with_users: false,
    last_focus_started_at: lastFocusStartedAt,
    last_focus_completed_at: lastFocusCompletedAt,
    last_focus_error: lastFocusError,
    last_batch_started_at: lastBatchStartedAt,
    last_batch_completed_at: lastBatchCompletedAt,
    last_batch_error: lastBatchError,
    round,
    lane_stats: lane,
    contract_rows: includeRows ? contract.map(clone) : [],
    spot_rows: includeRows ? freshSpot.map(clone) : [],
    funding_rows: includeRows ? funding.map(clone) : [],
    risk_reserve_pools: includeRows ? clone(riskReservePools) : [],
    risk_reserve_history: includeRows ? clone(riskHistory) : { ...riskHistory, pools: [] },
    contract_history_coverage: clone(contractHistory),
    oi_limit_rows: includeRows ? oiLimits.map(clone) : [],
    timestamp_ms: Date.now(),
  };
  responseCache.set(cacheKey, { at: Date.now(), payload });
  return { ...clone(payload), cache_hit: false, cache_age_ms: 0 };
}

export function getBitgetAdvancedStatsHealth() {
  const snapshot = snapshotPayload({ includeRows: false });
  return {
    ...snapshot,
    enabled: started || process.env.KAKA_DISABLE_BITGET_ADVANCED_STATS !== '1',
    mode: 'shared_slow_stats_focus15_plus_cheap_batch',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    total_reads: totalReads,
    total_focus_builds: totalFocusBuilds,
    total_focus_failures: totalFocusFailures,
    total_batch_builds: totalBatchBuilds,
    total_batch_failures: totalBatchFailures,
    response_cache_ttl_seconds: RESPONSE_CACHE_TTL_MS / 1000,
    response_cache_hits: responseCacheHits,
    response_cache_misses: responseCacheMisses,
    focus_running: Boolean(focusRunning),
    batch_running: Boolean(batchRunning),
    focus_lock_release_after_completion: true,
    startup_recovery_until_focus_ready: true,
    funding_startup_recovery_until_market_light_ready: true,
    funding_startup_recovery_reuses_market_light_only: true,
    funding_startup_recovery_additional_exchange_requests: 0,
    risk_reserve_history: riskReserveHistorySnapshot(),
    risk_reserve_history_running: Boolean(riskHistoryRunning),
    risk_reserve_history_startup_background_only: true,
    risk_reserve_history_focus_change_missing_only: true,
    risk_reserve_history_user_reads_trigger_exchange_requests: false,
    risk_reserve_history_reads_scale_with_users: false,
    contract_history: contractHistoryCoverage(),
    contract_history_route: HISTORY_ROUTE,
    contract_history_additional_exchange_requests: 0,
    contract_history_reuses_existing_step991_response_arrays: true,
    contract_history_shared_backend_memory: true,
    contract_history_user_reads_trigger_exchange_requests: false,
    contract_history_reads_scale_with_users: false,
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

export async function handleBitgetAdvancedStats(req, res, url) {
  if (![SNAPSHOT_ROUTE, HEALTH_ROUTE, HISTORY_ROUTE].includes(url.pathname)) return false;
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
    sendJson(res, 200, getBitgetAdvancedStatsHealth());
    return true;
  }
  if (url.pathname === HISTORY_ROUTE) {
    const payload = contractHistorySnapshot({
      symbol: url.searchParams.get('symbol') || '',
      lane: url.searchParams.get('lane') || '',
      interval: url.searchParams.get('interval') || '5m',
    });
    sendJson(res, payload.ok === false ? 400 : 200, payload);
    return true;
  }
  sendJson(res, 200, snapshotPayload({ includeRows: true }));
  return true;
}

export const __bitgetAdvancedTest = Object.freeze({
  parseActiveBuySell,
  parseLongShort,
  parsePositionLongShort,
  parseAccountLongShort,
  parsePositionTier,
  parseIndexComponents,
  parseSpotWhale,
  parseSpotFund,
  parseSpotNet,
});

export const __bitgetStep1000Test = Object.freeze({ parseRiskReserveHistory, riskReservePoolKey });

export const __bitgetStep1001Test = Object.freeze({ normalizedHistoryRowForLane, derivedRollupRows, contractHistorySnapshot });

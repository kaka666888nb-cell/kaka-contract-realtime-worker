import { getContractFocusPoolInternalSnapshot } from './contract-focus-pool.mjs';
import { getContractDepthSharedOrderbook } from './contract-depth.mjs';
import { getContractFlowInternalFocusSnapshot, reconcileContractFlowFocusPool } from './contract-flow.mjs';

const VERSION = '650.8.15.2';
const SNAPSHOT_ROUTE = '/api/contract-deep-shared/current-snapshot';
const HEALTH_ROUTE = '/api/contract-deep-shared/health';
const PROVIDERS = Object.freeze(['binance', 'okx', 'bybit', 'bitget', 'gate']);
const TARGET_PER_PROVIDER = 15;
const TARGET_ROWS = PROVIDERS.length * TARGET_PER_PROVIDER;
const DEPTH_LEVELS = 20;
const DEPTH_SAMPLE_PER_PROVIDER_PER_CYCLE = 4;
const SCAN_INTERVAL_MS = 15_000;
const STARTUP_DELAY_MS = 12_000;
const DEPTH_STALE_MS = 95_000;
const RESPONSE_CACHE_TTL_MS = 8_000;
const FLOW_RECONCILE_MIN_INTERVAL_MS = 60_000;

let started = false;
let running = null;
let interval = null;
let startupTimer = null;
let round = 0;
let totalScans = 0;
let totalScanFailures = 0;
let lastStartedAt = null;
let lastCompletedAt = null;
let lastError = '';
let totalReads = 0;
let responseCacheHits = 0;
let responseCacheMisses = 0;
let lastFocusSignature = '';
let lastFlowReconcileAt = 0;
let flowReconcileAttempts = 0;
let flowReconcileSuccesses = 0;
let flowReconcileFailures = 0;
let depthReadAttempts = 0;
let depthReadSuccesses = 0;
let depthReadFailures = 0;

const cursors = Object.fromEntries(PROVIDERS.map((provider) => [provider, 0]));
const depthState = new Map();
let responseCache = null;

function finite(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeProvider(raw) {
  const p = String(raw || '').trim().toLowerCase();
  return PROVIDERS.includes(p) ? p : '';
}

function normalizeSymbol(raw) {
  return String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function clone(value) {
  return value == null ? value : JSON.parse(JSON.stringify(value));
}

function focusRowsByProvider() {
  const snapshot = getContractFocusPoolInternalSnapshot();
  const byProvider = Object.fromEntries(PROVIDERS.map((provider) => [provider, []]));
  if (snapshot?.ready !== true) {
    return { ready: false, round: Number(snapshot?.round || 0), rows: [], byProvider };
  }
  for (const provider of PROVIDERS) {
    const rows = Array.isArray(snapshot?.providers?.[provider]?.rows) ? snapshot.providers[provider].rows : [];
    byProvider[provider] = rows
      .map((row) => ({
        provider,
        symbol: normalizeSymbol(row?.symbol),
        base_asset: String(row?.base_asset || '').trim().toUpperCase(),
        role: String(row?.role || ''),
        slot: Number(row?.slot || 0),
        quote_volume_24h: finite(row?.quote_volume_24h),
        price_change_percent_24h: finite(row?.price_change_percent_24h),
      }))
      .filter((row) => row.symbol && row.symbol.endsWith('USDT'))
      .slice(0, TARGET_PER_PROVIDER);
  }
  const rows = PROVIDERS.flatMap((provider) => byProvider[provider]);
  return {
    ready: PROVIDERS.every((provider) => byProvider[provider].length === TARGET_PER_PROVIDER) && rows.length === TARGET_ROWS,
    round: Number(snapshot?.round || 0),
    rows,
    byProvider,
  };
}

function focusSignature(focus) {
  return PROVIDERS.map((provider) => `${provider}:${(focus.byProvider[provider] || []).map((row) => row.symbol).join(',')}`).join('|');
}

function quoteAt(level) {
  const explicit = finite(level?.quote_amount);
  if (explicit != null && explicit >= 0) return explicit;
  const price = finite(level?.price);
  const quantity = finite(level?.quantity);
  if (price != null && quantity != null && price >= 0 && quantity >= 0) return price * quantity;
  return null;
}

function sumDepth(levels, count) {
  let total = 0;
  let seen = 0;
  for (const level of (Array.isArray(levels) ? levels : []).slice(0, count)) {
    const q = quoteAt(level);
    if (q == null) continue;
    total += q;
    seen += 1;
  }
  return seen ? total : null;
}

function compactLevels(levels) {
  return (Array.isArray(levels) ? levels : []).slice(0, DEPTH_LEVELS).map((row) => ({
    price: finite(row?.price),
    quantity: finite(row?.quantity),
    quote_amount: quoteAt(row),
  })).filter((row) => row.price != null && row.quantity != null);
}

function buildDepthRow(focusRow, payload, now) {
  const bids = compactLevels(payload?.bids);
  const asks = compactLevels(payload?.asks);
  const bestBid = finite(payload?.best_bid ?? bids[0]?.price);
  const bestAsk = finite(payload?.best_ask ?? asks[0]?.price);
  const spreadPercent = finite(payload?.spread_percent) ?? (
    bestBid != null && bestAsk != null && bestBid > 0 && bestAsk >= bestBid
      ? ((bestAsk - bestBid) / bestBid) * 100
      : null
  );
  return {
    provider: focusRow.provider,
    symbol: focusRow.symbol,
    base_asset: focusRow.base_asset,
    role: focusRow.role,
    slot: focusRow.slot,
    quote_asset: 'USDT',
    best_bid: bestBid,
    best_ask: bestAsk,
    spread_percent: spreadPercent,
    bid_depth_quote_5: sumDepth(bids, 5),
    ask_depth_quote_5: sumDepth(asks, 5),
    bid_depth_quote_10: sumDepth(bids, 10),
    ask_depth_quote_10: sumDepth(asks, 10),
    bid_depth_quote_20: sumDepth(bids, 20),
    ask_depth_quote_20: sumDepth(asks, 20),
    bids,
    asks,
    depth_levels: Math.min(DEPTH_LEVELS, Math.max(bids.length, asks.length)),
    depth_source: payload?.source || '',
    depth_transport: payload?.transport || '',
    depth_cache_state: payload?.cache_state || '',
    depth_timestamp_ms: Number(payload?.timestamp_ms || 0) || now,
    depth_sampled_at_ms: now,
    depth_sampled_at: new Date(now).toISOString(),
    depth_error: '',
  };
}

function nextDepthTargets(focus) {
  const targets = [];
  for (const provider of PROVIDERS) {
    const rows = focus.byProvider[provider] || [];
    if (!rows.length) continue;
    let cursor = Math.max(0, Number(cursors[provider] || 0)) % rows.length;
    const count = Math.min(DEPTH_SAMPLE_PER_PROVIDER_PER_CYCLE, rows.length);
    for (let i = 0; i < count; i += 1) targets.push(rows[(cursor + i) % rows.length]);
    cursors[provider] = (cursor + count) % rows.length;
  }
  return targets;
}

async function runBoundedByProvider(targets) {
  const grouped = Object.fromEntries(PROVIDERS.map((provider) => [provider, []]));
  for (const target of targets) grouped[target.provider].push(target);
  await Promise.all(PROVIDERS.map(async (provider) => {
    for (const target of grouped[provider]) {
      const key = `${provider}:${target.symbol}`;
      depthReadAttempts += 1;
      try {
        const payload = await getContractDepthSharedOrderbook(provider, target.symbol, DEPTH_LEVELS);
        const row = buildDepthRow(target, payload, Date.now());
        depthState.set(key, row);
        depthReadSuccesses += 1;
      } catch (error) {
        depthReadFailures += 1;
        const previous = depthState.get(key);
        if (previous) {
          depthState.set(key, {
            ...previous,
            depth_error: String(error?.message || error).slice(0, 220),
          });
        }
      }
    }
  }));
}

function pruneDepthState(focus) {
  const allowed = new Set(focus.rows.map((row) => `${row.provider}:${row.symbol}`));
  for (const key of [...depthState.keys()]) if (!allowed.has(key)) depthState.delete(key);
}

async function maybeReconcileFlow(focus, signature) {
  const flow = getContractFlowInternalFocusSnapshot();
  const focusChanged = signature !== lastFocusSignature;
  const notFullyActive = flow.target_rows === TARGET_ROWS && flow.active_rows < TARGET_ROWS;
  if (!focus.ready || (!focusChanged && !notFullyActive)) return flow;
  if (Date.now() - lastFlowReconcileAt < FLOW_RECONCILE_MIN_INTERVAL_MS) return flow;
  lastFlowReconcileAt = Date.now();
  flowReconcileAttempts += 1;
  try {
    await reconcileContractFlowFocusPool();
    flowReconcileSuccesses += 1;
  } catch (error) {
    flowReconcileFailures += 1;
    lastError = String(error?.message || error).slice(0, 260);
  }
  return getContractFlowInternalFocusSnapshot();
}

async function scan(reason = 'interval') {
  if (running) return await running;
  const task = (async () => {
    lastStartedAt = new Date().toISOString();
    const focus = focusRowsByProvider();
    if (!focus.ready) {
      lastError = 'focus_pool_not_ready';
      return { reason, focus_ready: false };
    }
    const signature = focusSignature(focus);
    pruneDepthState(focus);
    await maybeReconcileFlow(focus, signature);
    const targets = nextDepthTargets(focus);
    await runBoundedByProvider(targets);
    lastFocusSignature = signature;
    round += 1;
    totalScans += 1;
    lastCompletedAt = new Date().toISOString();
    lastError = '';
    responseCache = null;
    return { reason, focus_ready: true, sampled: targets.length };
  })();
  running = task;
  try {
    return await task;
  } catch (error) {
    totalScanFailures += 1;
    lastError = String(error?.message || error).slice(0, 260);
    throw error;
  } finally {
    if (running === task) running = null;
  }
}

function currentPayload() {
  if (responseCache && Date.now() - responseCache.at <= RESPONSE_CACHE_TTL_MS) {
    responseCacheHits += 1;
    return { ...clone(responseCache.payload), cache_hit: true, cache_age_ms: Date.now() - responseCache.at };
  }
  responseCacheMisses += 1;
  const now = Date.now();
  const focus = focusRowsByProvider();
  const flow = getContractFlowInternalFocusSnapshot();
  const flowByKey = new Map((flow.rows || []).map((row) => [`${normalizeProvider(row.provider)}:${normalizeSymbol(row.symbol)}`, row]));
  const rows = [];
  const providerCoverage = {};
  for (const provider of PROVIDERS) {
    let depthFresh = 0;
    let depthAny = 0;
    let flowActive = 0;
    let flowConnected = 0;
    let flowRows = 0;
    for (const focusRow of focus.byProvider[provider] || []) {
      const key = `${provider}:${focusRow.symbol}`;
      const depth = depthState.get(key) || null;
      const ageMs = depth ? Math.max(0, now - Number(depth.depth_sampled_at_ms || 0)) : null;
      const fresh = ageMs != null && ageMs <= DEPTH_STALE_MS && !String(depth?.depth_error || '').trim();
      if (depth) depthAny += 1;
      if (fresh) depthFresh += 1;
      const flowRow = flowByKey.get(key) || null;
      if (flowRow) flowRows += 1;
      rows.push({
        ...focusRow,
        depth_ready: fresh,
        depth_stale: depth != null && !fresh,
        depth_age_ms: ageMs,
        ...(depth || {}),
        flow_ready: Boolean(flowRow),
        flow_bucket_time: flowRow?.bucket_time || null,
        flow_bucket_end_time: flowRow?.bucket_end_time || null,
        flow_buy_quote: finite(flowRow?.buy_quote),
        flow_sell_quote: finite(flowRow?.sell_quote),
        flow_net_quote: finite(flowRow?.net_quote),
        flow_trade_count: finite(flowRow?.trade_count),
        flow_p70_quote: finite(flowRow?.p70_quote),
        flow_p95_quote: finite(flowRow?.p95_quote),
        flow_large_buy_quote: finite(flowRow?.large_buy_quote),
        flow_large_sell_quote: finite(flowRow?.large_sell_quote),
        flow_large_net_quote: finite(flowRow?.large_net_quote),
        flow_medium_buy_quote: finite(flowRow?.medium_buy_quote),
        flow_medium_sell_quote: finite(flowRow?.medium_sell_quote),
        flow_small_buy_quote: finite(flowRow?.small_buy_quote),
        flow_small_sell_quote: finite(flowRow?.small_sell_quote),
        flow_large_buy_count: finite(flowRow?.large_buy_count),
        flow_large_sell_count: finite(flowRow?.large_sell_count),
        flow_medium_buy_count: finite(flowRow?.medium_buy_count),
        flow_medium_sell_count: finite(flowRow?.medium_sell_count),
        flow_small_buy_count: finite(flowRow?.small_buy_count),
        flow_small_sell_count: finite(flowRow?.small_sell_count),
        flow_source: flowRow?.source || null,
      });
    }
    flowActive = Number(flow?.active_rows || 0); // aggregate is exposed separately below
    flowConnected = Number(flow?.connected_rows || 0);
    providerCoverage[provider] = {
      target: (focus.byProvider[provider] || []).length,
      depth_any: depthAny,
      depth_fresh: depthFresh,
      flow_rows: flowRows,
    };
  }
  const depthFreshRows = rows.filter((row) => row.depth_ready === true).length;
  const flowRows = rows.filter((row) => row.flow_ready === true).length;
  const largeTradeSchemaRows = rows.filter((row) =>
    row.flow_ready === true &&
    row.flow_trade_count != null && row.flow_trade_count > 0 &&
    row.flow_p95_quote != null &&
    row.flow_large_buy_quote != null &&
    row.flow_large_sell_quote != null
  ).length;
  const largeTradeValueRows = rows.filter((row) =>
    row.flow_ready === true &&
    ((row.flow_large_buy_quote ?? 0) + (row.flow_large_sell_quote ?? 0)) > 0
  ).length;
  const payload = {
    ok: true,
    version: VERSION,
    source: 'render_shared_focus_pool_depth20_plus_existing_contract_flow_websocket',
    ready: focus.ready && rows.length === TARGET_ROWS && depthFreshRows === TARGET_ROWS && Number(flow.active_rows || 0) === TARGET_ROWS,
    focus_pool_ready: focus.ready,
    focus_pool_round: focus.round,
    provider_count: PROVIDERS.length,
    target_per_provider: TARGET_PER_PROVIDER,
    target_rows: TARGET_ROWS,
    row_count: rows.length,
    depth_fresh_rows: depthFreshRows,
    depth_any_rows: rows.filter((row) => row.depth_sampled_at_ms != null).length,
    flow_active_rows: Number(flow.active_rows || 0),
    flow_connected_rows: Number(flow.connected_rows || 0),
    flow_value_rows: flowRows,
    large_trade_schema_rows: largeTradeSchemaRows,
    large_trade_value_rows: largeTradeValueRows,
    large_trade_fields_derived_from_existing_flow_histogram: true,
    large_trade_user_reads_open_exchange_work: false,
    depth_levels: DEPTH_LEVELS,
    depth_sample_per_provider_per_cycle: DEPTH_SAMPLE_PER_PROVIDER_PER_CYCLE,
    scan_interval_seconds: SCAN_INTERVAL_MS / 1000,
    worst_case_depth_refresh_seconds: Math.ceil(TARGET_PER_PROVIDER / DEPTH_SAMPLE_PER_PROVIDER_PER_CYCLE) * SCAN_INTERVAL_MS / 1000,
    provider_coverage: providerCoverage,
    rows,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_scan: false,
    user_reads_scale_with_users: false,
    full_universe_flow_rotation_preserved: true,
    focus_flow_uses_existing_contract_flow_state_map: true,
    depth_uses_existing_contract_depth_cache_inflight_governor: true,
    generated_at: lastCompletedAt,
  };
  responseCache = { at: Date.now(), payload };
  return { ...clone(payload), cache_hit: false, cache_age_ms: 0 };
}

export function getContractDeepSharedHealth() {
  const payload = currentPayload();
  return {
    ok: true,
    version: VERSION,
    enabled: started || process.env.KAKA_DISABLE_CONTRACT_DEEP_SHARED !== '1',
    snapshot_endpoint: SNAPSHOT_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    ready: payload.ready,
    focus_pool_ready: payload.focus_pool_ready,
    row_count: payload.row_count,
    target_rows: TARGET_ROWS,
    depth_fresh_rows: payload.depth_fresh_rows,
    depth_any_rows: payload.depth_any_rows,
    flow_active_rows: payload.flow_active_rows,
    flow_connected_rows: payload.flow_connected_rows,
    flow_value_rows: payload.flow_value_rows,
    large_trade_schema_rows: payload.large_trade_schema_rows,
    large_trade_value_rows: payload.large_trade_value_rows,
    large_trade_fields_derived_from_existing_flow_histogram: true,
    large_trade_user_reads_open_exchange_work: false,
    provider_coverage: payload.provider_coverage,
    depth_levels: DEPTH_LEVELS,
    depth_sample_per_provider_per_cycle: DEPTH_SAMPLE_PER_PROVIDER_PER_CYCLE,
    scan_interval_seconds: SCAN_INTERVAL_MS / 1000,
    worst_case_depth_refresh_seconds: payload.worst_case_depth_refresh_seconds,
    running: Boolean(running),
    round,
    total_scans: totalScans,
    total_scan_failures: totalScanFailures,
    depth_read_attempts: depthReadAttempts,
    depth_read_successes: depthReadSuccesses,
    depth_read_failures: depthReadFailures,
    flow_reconcile_attempts: flowReconcileAttempts,
    flow_reconcile_successes: flowReconcileSuccesses,
    flow_reconcile_failures: flowReconcileFailures,
    last_started_at: lastStartedAt,
    last_completed_at: lastCompletedAt,
    last_error: lastError,
    total_reads: totalReads,
    response_cache_ttl_seconds: RESPONSE_CACHE_TTL_MS / 1000,
    response_cache_hits: responseCacheHits,
    response_cache_misses: responseCacheMisses,
    background_depth_collection_is_bounded: true,
    background_flow_reuses_existing_contract_flow_streams: true,
    full_universe_flow_rotation_preserved: true,
    exchange_requests_started_by_user_read: 0,
    exchange_connections_started_by_user_read: 0,
    user_reads_trigger_scan: false,
    reads_scale_with_users: false,
  };
}

export function startContractDeepSharedScanner() {
  if (started || process.env.KAKA_DISABLE_CONTRACT_DEEP_SHARED === '1') return;
  started = true;
  startupTimer = setTimeout(() => scan('startup').catch(() => {}), STARTUP_DELAY_MS);
  startupTimer.unref?.();
  interval = setInterval(() => scan('interval').catch(() => {}), SCAN_INTERVAL_MS);
  interval.unref?.();
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

export async function handleContractDeepShared(req, res, url) {
  if (url.pathname === HEALTH_ROUTE) {
    sendJson(res, 200, getContractDeepSharedHealth());
    return true;
  }
  if (url.pathname !== SNAPSHOT_ROUTE) return false;
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: VERSION, error: 'method_not_allowed' });
    return true;
  }
  totalReads += 1;
  sendJson(res, 200, currentPayload());
  return true;
}

const VERSION = '650.8.15.163';
const PROVIDERS = new Set(['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate']);
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const SUPABASE_CONFIGURED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);

const CACHE_TTL_MS = 40_000;
const STALE_MS = 5 * 60_000;
const CACHE_MAX = 128;
const BUILD_MAX_ACTIVE = 4;
const BUILD_MAX_QUEUE = 64;
const BUILD_PROVIDER_MAX_ACTIVE = 2;
const BUILD_PROVIDER_MAX_QUEUE = 16;
const ACTIVATE_TTL_MS = 5 * 60_000;

const cache = new Map();
const inflight = new Map();
const providerBuildState = new Map([...PROVIDERS].map((provider) => [provider, { provider, active: 0, queue: [], started: 0, completed: 0, rejected: 0, max_queue_seen: 0 }]));
const providerBuildOrder = [...PROVIDERS];
let providerBuildCursor = 0;
const activatedAt = new Map();
let activeBuilds = 0;

const stats = {
  reads: 0,
  fresh_hits: 0,
  stale_hits: 0,
  inflight_hits: 0,
  builds_started: 0,
  builds_succeeded: 0,
  builds_partial: 0,
  builds_failed: 0,
  queue_rejections: 0,
  cache_evictions: 0,
  supabase_rpc_calls: 0,
  activation_calls: 0,
  activation_succeeded: 0,
  activation_failed: 0,
};

function providerKey(raw) {
  let value = String(raw || '').trim().toLowerCase();
  if (value === 'gate.io') value = 'gate';
  if (value === 'okex') value = 'okx';
  return PROVIDERS.has(value) ? value : '';
}

function symbolKey(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/-SWAP$/i, '')
    .replace(/_UMCBL$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}

function sendJson(res, statusCode, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
  });
  res.end(body);
}

function pruneMaps() {
  const now = Date.now();
  for (const [key, entry] of cache.entries()) {
    if (Number(entry?.staleUntil || 0) <= now) cache.delete(key);
  }
  for (const [key, time] of activatedAt.entries()) {
    if (now - Number(time || 0) > 24 * 60 * 60_000) activatedAt.delete(key);
  }
  while (cache.size > CACHE_MAX) {
    const oldest = [...cache.entries()]
      .sort((a, b) => Number(a[1]?.storedAt || 0) - Number(b[1]?.storedAt || 0))[0]?.[0];
    if (!oldest) break;
    cache.delete(oldest);
    stats.cache_evictions += 1;
  }
  while (activatedAt.size > CACHE_MAX) {
    const oldest = [...activatedAt.entries()].sort((a, b) => a[1] - b[1])[0]?.[0];
    if (!oldest) break;
    activatedAt.delete(oldest);
  }
}

function cachedPayload(entry, cacheState) {
  return {
    ...entry.payload,
    cache_state: cacheState,
    cache_age_seconds: Math.max(0, Math.floor((Date.now() - entry.storedAt) / 1000)),
  };
}

function buildQueueTotal() {
  let total = 0;
  for (const state of providerBuildState.values()) total += state.queue.length;
  return total;
}

function buildBulkheadHealth() {
  return Object.fromEntries([...providerBuildState.entries()].map(([provider, state]) => [provider, {
    active: state.active,
    queue: state.queue.length,
    max_active: BUILD_PROVIDER_MAX_ACTIVE,
    max_queue: BUILD_PROVIDER_MAX_QUEUE,
    started: state.started,
    completed: state.completed,
    rejected: state.rejected,
    max_queue_seen: state.max_queue_seen,
  }]));
}

function pumpBuildQueue() {
  if (!providerBuildOrder.length) return;
  let progress = true;
  while (progress && activeBuilds < BUILD_MAX_ACTIVE) {
    progress = false;
    for (let offset = 0; offset < providerBuildOrder.length; offset += 1) {
      const index = (providerBuildCursor + offset) % providerBuildOrder.length;
      const provider = providerBuildOrder[index];
      const state = providerBuildState.get(provider);
      if (!state || state.active >= BUILD_PROVIDER_MAX_ACTIVE) continue;
      while (state.queue.length && state.queue[0]?.signal?.aborted) state.queue.shift();
      const item = state.queue.shift();
      if (!item) continue;
      if (item.signal && item.onAbort) item.signal.removeEventListener('abort', item.onAbort);
      state.active += 1;
      activeBuilds += 1;
      state.started += 1;
      providerBuildCursor = (index + 1) % providerBuildOrder.length;
      item.resolve(() => releaseBuildSlot(provider));
      progress = true;
      break;
    }
  }
}

function acquireBuildSlot(provider, signal) {
  if (signal?.aborted) return Promise.reject(new Error('request_aborted_before_queue'));
  const state = providerBuildState.get(provider);
  if (!state) return Promise.reject(new Error('unsupported_provider_bulkhead'));
  if (buildQueueTotal() === 0 && state.active < BUILD_PROVIDER_MAX_ACTIVE && activeBuilds < BUILD_MAX_ACTIVE) {
    state.active += 1;
    activeBuilds += 1;
    state.started += 1;
    return Promise.resolve(() => releaseBuildSlot(provider));
  }
  if (state.queue.length >= BUILD_PROVIDER_MAX_QUEUE || buildQueueTotal() >= BUILD_MAX_QUEUE) {
    stats.queue_rejections += 1;
    state.rejected += 1;
    return Promise.reject(new Error(state.queue.length >= BUILD_PROVIDER_MAX_QUEUE ? 'spot_flow_snapshot_provider_queue_full' : 'spot_flow_snapshot_global_queue_full'));
  }
  return new Promise((resolve, reject) => {
    const item = { resolve, reject, signal, onAbort: null };
    if (signal) {
      item.onAbort = () => {
        const index = state.queue.indexOf(item);
        if (index >= 0) state.queue.splice(index, 1);
        reject(new Error('request_aborted_while_queued'));
      };
      signal.addEventListener('abort', item.onAbort, { once: true });
    }
    state.queue.push(item);
    state.max_queue_seen = Math.max(state.max_queue_seen, state.queue.length);
    pumpBuildQueue();
  });
}

function releaseBuildSlot(provider) {
  const state = providerBuildState.get(provider);
  if (state) {
    state.active = Math.max(0, state.active - 1);
    state.completed += 1;
  }
  activeBuilds = Math.max(0, activeBuilds - 1);
  pumpBuildQueue();
}

function linkedTimeoutSignal(parent, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('supabase_rpc_timeout')), timeoutMs);
  let onAbort = null;
  if (parent) {
    onAbort = () => controller.abort(parent.reason || new Error('request_aborted'));
    if (parent.aborted) onAbort();
    else parent.addEventListener('abort', onAbort, { once: true });
  }
  return {
    signal: controller.signal,
    cleanup: () => {
      clearTimeout(timer);
      if (parent && onAbort) parent.removeEventListener('abort', onAbort);
    },
  };
}

async function callRpc(name, body, signal, timeoutMs = 12_000) {
  if (!SUPABASE_CONFIGURED) throw new Error('supabase_service_role_not_configured');
  stats.supabase_rpc_calls += 1;
  const linked = linkedTimeoutSignal(signal, timeoutMs);
  try {
    const response = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`, {
      method: 'POST',
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        accept: 'application/json',
        'content-type': 'application/json',
      },
      body: JSON.stringify(body || {}),
      signal: linked.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`${name}_http_${response.status}:${text.slice(0, 240)}`);
    }
    if (!text.trim()) return null;
    try { return JSON.parse(text); }
    catch (_) { return text; }
  } finally {
    linked.cleanup();
  }
}

function unwrapPayload(raw) {
  if (raw && typeof raw === 'object' && !Array.isArray(raw)) return raw;
  if (Array.isArray(raw) && raw.length > 0 && raw[0] && typeof raw[0] === 'object') return raw[0];
  return null;
}

function payloadOk(raw) {
  const payload = unwrapPayload(raw);
  return payload && payload.ok === true ? payload : null;
}

async function maybeActivate(provider, symbol, signal) {
  const key = `${provider}:${symbol}`;
  const now = Date.now();
  const previous = Number(activatedAt.get(key) || 0);
  if (now - previous < ACTIVATE_TTL_MS) {
    return { attempted: false, ok: true, cache_state: 'activation_cooldown' };
  }
  stats.activation_calls += 1;
  try {
    const raw = await callRpc('app_activate_spot_trade_flow', {
      p_provider: provider,
      p_symbol: symbol,
    }, signal, 10_000);
    activatedAt.set(key, Date.now());
    stats.activation_succeeded += 1;
    return {
      attempted: true,
      ok: true,
      cache_state: 'activated',
      result: unwrapPayload(raw) || raw,
    };
  } catch (error) {
    stats.activation_failed += 1;
    return {
      attempted: true,
      ok: false,
      cache_state: 'activation_failed_but_snapshot_read_continues',
      error: String(error?.message || error),
    };
  }
}

async function buildPayload(provider, symbol, signal) {
  const activation = await maybeActivate(provider, symbol, signal);
  const settled = await Promise.allSettled([
    callRpc('app_get_spot_trade_flow_periods', {
      p_provider: provider,
      p_symbol: symbol,
    }, signal),
    callRpc('app_get_spot_trade_flow_size_periods', {
      p_provider: provider,
      p_symbol: symbol,
    }, signal),
    callRpc('app_get_spot_trade_flow_daily', {
      p_provider: provider,
      p_symbol: symbol,
      p_days: 5,
    }, signal),
  ]);

  const flowPayload = settled[0].status === 'fulfilled' ? payloadOk(settled[0].value) : null;
  const sizePayload = settled[1].status === 'fulfilled' ? payloadOk(settled[1].value) : null;
  const dailyPayload = settled[2].status === 'fulfilled' ? payloadOk(settled[2].value) : null;
  const rpcErrors = settled.map((item, index) => item.status === 'rejected'
    ? `${['periods', 'size_periods', 'daily'][index]}:${String(item.reason?.message || item.reason)}`
    : '').filter(Boolean);

  const validCount = [flowPayload, sizePayload, dailyPayload].filter(Boolean).length;
  const status = validCount === 3 ? 'ready' : (validCount > 0 ? 'partial_ready' : 'snapshot_unavailable');
  return {
    ok: validCount > 0,
    version: VERSION,
    provider,
    market_type: 'spot',
    symbol,
    status,
    flow_payload: flowPayload,
    size_payload: sizePayload,
    daily_payload: dailyPayload,
    activation,
    rpc_errors: rpcErrors,
    source: 'render_shared_exact_key_spot_trade_flow_rpc_snapshot',
    generated_at: new Date().toISOString(),
  };
}

async function getSharedSnapshot(provider, symbol, signal) {
  stats.reads += 1;
  pruneMaps();
  const key = `${provider}:${symbol}`;
  const now = Date.now();
  const existing = cache.get(key);
  if (existing && existing.freshUntil > now) {
    stats.fresh_hits += 1;
    return cachedPayload(existing, 'fresh');
  }
  const running = inflight.get(key);
  if (running) {
    stats.inflight_hits += 1;
    return await running;
  }

  const task = (async () => {
    let release = null;
    try {
      release = await acquireBuildSlot(provider, signal);
      stats.builds_started += 1;
      const payload = await buildPayload(provider, symbol, signal);
      if (!payload.ok) {
        if (existing && existing.staleUntil > Date.now()) {
          stats.stale_hits += 1;
          return cachedPayload(existing, 'stale_build_empty');
        }
        stats.builds_failed += 1;
        throw new Error(`spot_flow_snapshot_empty:${payload.rpc_errors.join('|')}`);
      }
      const entry = {
        payload,
        storedAt: Date.now(),
        freshUntil: Date.now() + CACHE_TTL_MS,
        staleUntil: Date.now() + STALE_MS,
      };
      cache.set(key, entry);
      pruneMaps();
      if (payload.status === 'ready') stats.builds_succeeded += 1;
      else stats.builds_partial += 1;
      return cachedPayload(entry, 'miss');
    } catch (error) {
      if (existing && existing.staleUntil > Date.now()) {
        stats.stale_hits += 1;
        return cachedPayload(existing, 'stale_error');
      }
      if (!String(error?.message || error).startsWith('spot_flow_snapshot_empty:')) {
        stats.builds_failed += 1;
      }
      throw error;
    } finally {
      if (release) release();
    }
  })();

  inflight.set(key, task);
  try {
    return await task;
  } finally {
    if (inflight.get(key) === task) inflight.delete(key);
  }
}

export function getSpotFlowSnapshotHealth() {
  pruneMaps();
  return {
    ok: true,
    version: VERSION,
    endpoint: '/api/spot-flow/snapshot',
    health_endpoint: '/api/spot-flow/snapshot-health',
    providers: [...PROVIDERS],
    supabase_configured: SUPABASE_CONFIGURED,
    mode: 'shared_exact_key_spot_flow_periods_size_daily_rpc_snapshot',
    cache_ttl_seconds: Math.round(CACHE_TTL_MS / 1000),
    stale_seconds: Math.round(STALE_MS / 1000),
    activation_ttl_seconds: Math.round(ACTIVATE_TTL_MS / 1000),
    cache_entries: cache.size,
    inflight_entries: inflight.size,
    activation_entries: activatedAt.size,
    cache_max: CACHE_MAX,
    build_active: activeBuilds,
    build_max_active: BUILD_MAX_ACTIVE,
    build_queue: buildQueueTotal(),
    build_max_queue: BUILD_MAX_QUEUE,
    build_provider_max_active: BUILD_PROVIDER_MAX_ACTIVE,
    build_provider_max_queue: BUILD_PROVIDER_MAX_QUEUE,
    build_bulkhead_mode: 'per_provider_round_robin_with_global_emergency_ceiling',
    provider_bulkheads: buildBulkheadHealth(),
    same_exact_key_reads_share_cache_and_inflight: true,
    snapshot_includes_periods_size_and_five_day_daily: true,
    app_no_longer_calls_spot_flow_read_rpcs_directly: true,
    app_no_longer_calls_spot_flow_activation_rpc_directly: true,
    snapshot_reads_start_exchange_requests: false,
    empty_or_failed_build_never_overwrites_verified_stale_payload: true,
    ...stats,
    time: new Date().toISOString(),
  };
}

export async function handleSpotFlowSnapshot(req, res, url, signal = null) {
  if (url.pathname === '/api/spot-flow/snapshot-health') {
    sendJson(res, 200, getSpotFlowSnapshotHealth());
    return true;
  }
  if (url.pathname !== '/api/spot-flow/snapshot') return false;
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, error: 'method_not_allowed' });
    return true;
  }
  const provider = providerKey(url.searchParams.get('provider'));
  const symbol = symbolKey(url.searchParams.get('symbol'));
  if (!provider || !symbol) {
    sendJson(res, 400, { ok: false, error: 'invalid_provider_or_symbol' });
    return true;
  }
  try {
    const payload = await getSharedSnapshot(provider, symbol, signal);
    sendJson(res, 200, payload);
  } catch (error) {
    const message = String(error?.message || error);
    const status = message.includes('queue_full') ? 503 : 502;
    sendJson(res, status, {
      ok: false,
      version: VERSION,
      provider,
      market_type: 'spot',
      symbol,
      error: message,
      flow_payload: null,
      size_payload: null,
      daily_payload: null,
      time: new Date().toISOString(),
    });
  }
  return true;
}

export const _test = {
  providerKey,
  symbolKey,
  unwrapPayload,
  payloadOk,
};

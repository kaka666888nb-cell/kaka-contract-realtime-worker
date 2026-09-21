const VERSION = '1073.r11.kline-refresh-hint.2';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');

const SYMBOLS = new Set([
  'BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT',
  'XRPUSDT','DOGEUSDT','ADAUSDT','AVAXUSDT',
  'LINKUSDT','TRXUSDT','DOTUSDT','LTCUSDT',
]);

const SPOT_INTERVALS = new Set([
  '1s','1m','3m','5m','15m','30m',
  '1h','2h','4h','6h','8h','12h',
  '1d','3d','1w','1M',
]);
const CONTRACT_INTERVALS = new Set([
  '1m','3m','5m','15m','30m',
  '1h','2h','4h','6h','8h','12h',
  '1d','3d','1w','1M',
]);

const CACHE_TTL_MS = 90_000;
const STALE_MS = 5 * 60_000;
const DEFERRED_CACHE_TTL_MS = 10_000;
const DEFERRED_STALE_MS = 30_000;
const CACHE_MAX = 256;
const BUILD_MAX_ACTIVE = 3;
const BUILD_MAX_QUEUE = 48;
const RPC_TIMEOUT_MS = 12_000;

const LOCAL_BUDGET_LIMITS = {
  spot: { minute: 12, hour: 60 },
  contract: { minute: 24, hour: 120 },
};
const localBudgetState = {
  spot: { minuteStartedAt: 0, minuteCount: 0, hourStartedAt: 0, hourCount: 0 },
  contract: { minuteStartedAt: 0, minuteCount: 0, hourStartedAt: 0, hourCount: 0 },
};

const cache = new Map();
const inflight = new Map();
const queue = [];
let activeBuilds = 0;

const stats = {
  reads: 0,
  fresh_hits: 0,
  stale_hits: 0,
  inflight_hits: 0,
  cold_misses: 0,
  builds_started: 0,
  builds_succeeded: 0,
  builds_failed: 0,
  queue_rejections: 0,
  rpc_calls: 0,
  local_budget_deferred: 0,
  cache_evictions: 0,
};

function sendJson(res, statusCode, payload, extraHeaders = {}) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
    ...extraHeaders,
  });
  res.end(body);
}

function marketKey(raw) {
  const value = String(raw || '').trim().toLowerCase();
  return value === 'spot' || value === 'contract' ? value : '';
}

function providerKey(raw) {
  const value = String(raw || '').trim().toLowerCase();
  return value === 'binance' ? value : '';
}

function symbolKey(raw) {
  const value = String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
  return SYMBOLS.has(value) ? value : '';
}

function intervalKey(market, raw) {
  const value = String(raw || '').trim();
  const allowed = market === 'contract' ? CONTRACT_INTERVALS : SPOT_INTERVALS;
  return allowed.has(value) ? value : '';
}

function canonicalLimit(market) {
  return market === 'contract' ? 500 : 300;
}

function pruneCache() {
  const now = Date.now();
  for (const [key, entry] of cache.entries()) {
    if (!entry || Number(entry.staleUntil || 0) <= now) cache.delete(key);
  }
  while (cache.size > CACHE_MAX) {
    const oldest = [...cache.entries()]
      .sort((a, b) => Number(a[1]?.storedAt || 0) - Number(b[1]?.storedAt || 0))[0]?.[0];
    if (!oldest) break;
    cache.delete(oldest);
    stats.cache_evictions += 1;
  }
}

function releaseBuildSlot() {
  activeBuilds = Math.max(0, activeBuilds - 1);
  while (activeBuilds < BUILD_MAX_ACTIVE && queue.length > 0) {
    const item = queue.shift();
    if (!item) break;
    activeBuilds += 1;
    item.resolve(releaseBuildSlot);
  }
}

function acquireBuildSlot() {
  if (activeBuilds < BUILD_MAX_ACTIVE && queue.length === 0) {
    activeBuilds += 1;
    return Promise.resolve(releaseBuildSlot);
  }
  if (queue.length >= BUILD_MAX_QUEUE) {
    stats.queue_rejections += 1;
    return Promise.reject(new Error('kline_refresh_hint_queue_full'));
  }
  return new Promise((resolve) => {
    queue.push({ resolve });
  });
}

function claimLocalBudget(market) {
  const now = Date.now();
  const state = localBudgetState[market];
  const limits = LOCAL_BUDGET_LIMITS[market];

  if (!state.minuteStartedAt || now - state.minuteStartedAt >= 60_000) {
    state.minuteStartedAt = now;
    state.minuteCount = 0;
  }
  if (!state.hourStartedAt || now - state.hourStartedAt >= 60 * 60_000) {
    state.hourStartedAt = now;
    state.hourCount = 0;
  }

  const allowed =
    state.minuteCount < limits.minute &&
    state.hourCount < limits.hour;

  if (allowed) {
    state.minuteCount += 1;
    state.hourCount += 1;
  }

  return {
    allowed,
    minute_count: state.minuteCount,
    minute_limit: limits.minute,
    minute_reset_at: new Date(state.minuteStartedAt + 60_000).toISOString(),
    hour_count: state.hourCount,
    hour_limit: limits.hour,
    hour_reset_at: new Date(state.hourStartedAt + 60 * 60_000).toISOString(),
  };
}

async function callRpc({ market, provider, symbol, interval, limit }) {
  if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
    throw new Error('supabase_service_role_not_configured');
  }

  const rpc = market === 'contract'
    ? 'app_request_contract_kline_cache'
    : 'app_request_market_kline_cache';

  const body = {
    p_provider: provider,
    p_symbol: symbol,
    p_kline_interval: interval,
    p_limit: limit,
  };

  stats.rpc_calls += 1;
  const response = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${rpc}`, {
    method: 'POST',
    headers: {
      apikey: SERVICE_ROLE_KEY,
      authorization: `Bearer ${SERVICE_ROLE_KEY}`,
      accept: 'application/json',
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(RPC_TIMEOUT_MS),
  });

  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${rpc}_http_${response.status}:${text.slice(0, 240)}`);
  }

  let result = null;
  if (text.trim()) {
    try { result = JSON.parse(text); }
    catch (_) { result = text; }
  }

  return { rpc, result };
}

async function buildPayload(spec) {
  let release = null;
  stats.builds_started += 1;
  try {
    release = await acquireBuildSlot();

    const localBudget = claimLocalBudget(spec.market);
    if (!localBudget.allowed) {
      stats.local_budget_deferred += 1;
      stats.builds_succeeded += 1;
      return {
        ok: true,
        version: VERSION,
        provider: spec.provider,
        market_type: spec.market,
        symbol: spec.symbol,
        kline_interval: spec.interval,
        limit: spec.limit,
        rpc: null,
        rpc_result: null,
        local_budget_deferred: true,
        local_budget: localBudget,
        source: 'render_shared_kline_refresh_hint',
        user_direct_supabase_rpc_calls: 0,
        reads_scale_db_calls_with_users: false,
        generated_at: new Date().toISOString(),
      };
    }

    const rpcResult = await callRpc(spec);
    const payload = {
      ok: true,
      version: VERSION,
      provider: spec.provider,
      market_type: spec.market,
      symbol: spec.symbol,
      kline_interval: spec.interval,
      limit: spec.limit,
      rpc: rpcResult.rpc,
      rpc_result: rpcResult.result,
      local_budget_deferred: false,
      local_budget: localBudget,
      source: 'render_shared_kline_refresh_hint',
      user_direct_supabase_rpc_calls: 0,
      reads_scale_db_calls_with_users: false,
      generated_at: new Date().toISOString(),
    };
    stats.builds_succeeded += 1;
    return payload;
  } catch (error) {
    stats.builds_failed += 1;
    throw error;
  } finally {
    if (release) release();
  }
}

function cachePayload(entry, state) {
  return {
    ...entry.payload,
    cache_state: state,
    cache_age_seconds: Math.max(0, Math.floor((Date.now() - entry.storedAt) / 1000)),
  };
}

function cacheEntryFor(payload) {
  const storedAt = Date.now();
  const deferred = payload?.local_budget_deferred === true;
  return {
    payload,
    storedAt,
    freshUntil: storedAt + (deferred ? DEFERRED_CACHE_TTL_MS : CACHE_TTL_MS),
    staleUntil: storedAt + (deferred ? DEFERRED_STALE_MS : STALE_MS),
  };
}

async function getShared(spec) {
  stats.reads += 1;
  pruneCache();

  const key = `${spec.market}|${spec.provider}|${spec.symbol}|${spec.interval}|${spec.limit}`;
  const now = Date.now();
  const existing = cache.get(key);

  if (existing && existing.freshUntil > now) {
    stats.fresh_hits += 1;
    return cachePayload(existing, 'fresh');
  }

  const running = inflight.get(key);
  if (running) {
    stats.inflight_hits += 1;
    return await running;
  }

  if (existing && existing.staleUntil > now) {
    stats.stale_hits += 1;
    const task = buildPayload(spec)
      .then((payload) => {
        const next = cacheEntryFor(payload);
        cache.set(key, next);
        pruneCache();
        return cachePayload(next, 'revalidated');
      })
      .finally(() => inflight.delete(key));
    inflight.set(key, task);
    task.catch(() => {});
    return cachePayload(existing, 'stale_revalidate');
  }

  stats.cold_misses += 1;
  const task = buildPayload(spec)
    .then((payload) => {
      const entry = cacheEntryFor(payload);
      cache.set(key, entry);
      pruneCache();
      return cachePayload(entry, 'cold_build');
    })
    .finally(() => inflight.delete(key));
  inflight.set(key, task);
  return await task;
}

function budgetHealth(market) {
  const state = localBudgetState[market];
  const limits = LOCAL_BUDGET_LIMITS[market];
  return {
    minute_count: state.minuteCount,
    minute_limit: limits.minute,
    hour_count: state.hourCount,
    hour_limit: limits.hour,
  };
}

export function getKlineRefreshHintHealth() {
  return {
    ok: true,
    version: VERSION,
    supabase_configured: Boolean(SUPABASE_URL && SERVICE_ROLE_KEY),
    cache_entries: cache.size,
    inflight_entries: inflight.size,
    active_builds: activeBuilds,
    queued_builds: queue.length,
    cache_ttl_seconds: Math.round(CACHE_TTL_MS / 1000),
    stale_seconds: Math.round(STALE_MS / 1000),
    cache_max: CACHE_MAX,
    build_max_active: BUILD_MAX_ACTIVE,
    build_max_queue: BUILD_MAX_QUEUE,
    canonical_limits: { spot: 300, contract: 500 },
    local_budget: {
      spot: budgetHealth('spot'),
      contract: budgetHealth('contract'),
    },
    supported_markets: ['spot', 'contract'],
    supported_provider: 'binance',
    supported_symbols: [...SYMBOLS],
    user_reads_direct_supabase_rpc: false,
    stats: { ...stats },
  };
}

export async function handleKlineRefreshHint(req, res, url) {
  if (url.pathname === '/api/kline-refresh-hint/health') {
    sendJson(res, 200, getKlineRefreshHintHealth());
    return true;
  }
  if (url.pathname !== '/api/kline-refresh-hint') return false;
  if (req.method !== 'GET') {
    sendJson(res, 405, { ok: false, version: VERSION, error: 'GET required' });
    return true;
  }

  const market = marketKey(url.searchParams.get('market_type') || url.searchParams.get('market'));
  const provider = providerKey(url.searchParams.get('provider'));
  const symbol = symbolKey(url.searchParams.get('symbol'));
  const interval = intervalKey(
    market,
    url.searchParams.get('interval') || url.searchParams.get('kline_interval'),
  );

  if (!market || !provider || !symbol || !interval) {
    sendJson(res, 400, {
      ok: false,
      version: VERSION,
      error: 'invalid_market_provider_symbol_or_interval',
      market_type: market || null,
      provider: provider || null,
      symbol: symbol || null,
      interval: interval || null,
    });
    return true;
  }

  const limit = canonicalLimit(market);
  try {
    const payload = await getShared({ market, provider, symbol, interval, limit });
    sendJson(res, 200, payload, {
      'x-kaka-shared-read': '1',
      'x-kaka-user-db-rpc-calls': '0',
      'x-kaka-cache-state': String(payload.cache_state || ''),
    });
  } catch (error) {
    const message = String(error?.message || error);
    sendJson(
      res,
      /queue_full/.test(message) ? 503 : 502,
      {
        ok: false,
        version: VERSION,
        error: message.slice(0, 320),
        provider,
        market_type: market,
        symbol,
        kline_interval: interval,
        limit,
      },
    );
  }
  return true;
}

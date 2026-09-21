const VERSION = '1073.r11.kline-refresh-hint.2';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');

const SPOT_SYMBOLS = new Set([
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
const CACHE_MAX = 256;
const BUILD_MAX_ACTIVE = 3;
const BUILD_MAX_QUEUE = 48;
const RPC_TIMEOUT_MS = 12_000;

const FIXED_BUDGETS = {
  spot: { minute: 24, hour: 120 },
  contract: { minute: 48, hour: 240 },
};
const budgetStarts = {
  spot: [],
  contract: [],
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
  budget_rejections: 0,
  rpc_calls: 0,
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

function symbolKey(market, raw) {
  const value = String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (!/^[A-Z0-9]{2,24}$/.test(value)) return '';
  if (market === 'spot' && !SPOT_SYMBOLS.has(value)) return '';
  return value;
}

function intervalKey(market, raw) {
  const value = String(raw || '').trim();
  const allowed = market === 'contract' ? CONTRACT_INTERVALS : SPOT_INTERVALS;
  return allowed.has(value) ? value : '';
}

function canonicalLimit(market, raw) {
  const parsed = Number.parseInt(String(raw ?? ''), 10);
  const fallback = market === 'contract' ? 180 : 80;
  const value = Number.isFinite(parsed) ? parsed : fallback;
  if (market === 'contract') return Math.max(20, Math.min(500, value));
  return Math.max(1, Math.min(300, value));
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

function pruneBudget(market, now = Date.now()) {
  const starts = budgetStarts[market];
  const hourAgo = now - 60 * 60_000;
  while (starts.length && starts[0] <= hourAgo) starts.shift();
  return starts;
}

function budgetHealth(market, now = Date.now()) {
  const starts = pruneBudget(market, now);
  const minuteAgo = now - 60_000;
  const minuteUsed = starts.filter((time) => time > minuteAgo).length;
  const cfg = FIXED_BUDGETS[market];
  return {
    minute_used: minuteUsed,
    minute_limit: cfg.minute,
    hour_used: starts.length,
    hour_limit: cfg.hour,
  };
}

function claimBudget(market) {
  const now = Date.now();
  const starts = pruneBudget(market, now);
  const minuteAgo = now - 60_000;
  const minuteUsed = starts.filter((time) => time > minuteAgo).length;
  const cfg = FIXED_BUDGETS[market];
  if (minuteUsed >= cfg.minute || starts.length >= cfg.hour) {
    stats.budget_rejections += 1;
    return { ok: false, ...budgetHealth(market, now) };
  }
  starts.push(now);
  return { ok: true, ...budgetHealth(market, now) };
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

async function callRpc({ market, provider, symbol, interval, limit }) {
  if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
    throw new Error('supabase_service_role_not_configured');
  }

  const budget = claimBudget(market);
  if (!budget.ok) {
    throw new Error(
      'kline_refresh_hint_fixed_budget_exceeded:' +
      JSON.stringify({
        market,
        minute_used: budget.minute_used,
        minute_limit: budget.minute_limit,
        hour_used: budget.hour_used,
        hour_limit: budget.hour_limit,
      }),
    );
  }

  const rpc = market === 'contract'
    ? 'app_request_contract_kline_cache'
    : 'app_request_market_kline_cache';

  stats.rpc_calls += 1;
  const response = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${rpc}`, {
    method: 'POST',
    headers: {
      apikey: SERVICE_ROLE_KEY,
      authorization: `Bearer ${SERVICE_ROLE_KEY}`,
      accept: 'application/json',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      p_provider: provider,
      p_symbol: symbol,
      p_kline_interval: interval,
      p_limit: limit,
    }),
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

  return { rpc, result, budget };
}

async function buildPayload(spec) {
  let release = null;
  stats.builds_started += 1;
  try {
    release = await acquireBuildSlot();
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
      fixed_budget: rpcResult.budget,
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
        const storedAt = Date.now();
        const next = {
          payload,
          storedAt,
          freshUntil: storedAt + CACHE_TTL_MS,
          staleUntil: storedAt + STALE_MS,
        };
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
      const storedAt = Date.now();
      const entry = {
        payload,
        storedAt,
        freshUntil: storedAt + CACHE_TTL_MS,
        staleUntil: storedAt + STALE_MS,
      };
      cache.set(key, entry);
      pruneCache();
      return cachePayload(entry, 'cold_build');
    })
    .finally(() => inflight.delete(key));
  inflight.set(key, task);
  return await task;
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
    supported_markets: ['spot', 'contract'],
    supported_provider: 'binance',
    spot_supported_symbols: [...SPOT_SYMBOLS],
    contract_symbol_policy: 'syntax_then_database_enabled_config',
    fixed_budgets: {
      spot: budgetHealth('spot'),
      contract: budgetHealth('contract'),
    },
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
  const symbol = symbolKey(market, url.searchParams.get('symbol'));
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

  const limit = canonicalLimit(market, url.searchParams.get('limit'));
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
      /queue_full|fixed_budget_exceeded/.test(message) ? 503 : 502,
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

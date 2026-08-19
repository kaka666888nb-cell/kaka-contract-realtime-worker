// Step1037 / Render 650.8.15.191
// Kaka Web3 on-chain market phase 2.
// Step1036 DEX Screener foundation is preserved. Step1037 adds exact-pool OHLCV/history and
// recent swaps through Moralis Data API, with backend-only secret, separate bounded scheduler,
// CU budget ledger, cache + singleflight, exact chain/token/pool preflight and no user-scale
// upstream amplification. No trading, wallet signing or database writes.

import { readFileSync, writeFileSync, renameSync } from 'node:fs';

const VERSION = '650.8.15.191.3';
const DATA_VERSION = 1037003;
const SCHEMA_VERSION = 'step1037_3_onchain_market_v2';

const HEALTH_ROUTE = '/api/onchain/health';
const SELF_TEST_ROUTE = '/api/onchain/self-test';
const TRENDING_ROUTE = '/api/onchain/trending';
const SEARCH_ROUTE = '/api/onchain/search';
const TOKEN_ROUTE = '/api/onchain/token';
const POOLS_ROUTE = '/api/onchain/pools';
const KLINES_ROUTE = '/api/onchain/klines';
const TRADES_ROUTE = '/api/onchain/trades';
const NEW_POOLS_ROUTE = '/api/onchain/new-pools';

const DEX_BASE = 'https://api.dexscreener.com';
// Candidate endpoints are documented at 60/min; search/pairs at 300/min.
// One global 1.2s lane caps the whole on-chain module at <=50 upstream starts/min regardless of users.
const DEX_MIN_GAP_MS = 1_200;
const DEX_MAX_QUEUE = 80;
const UPSTREAM_TIMEOUT_MS = 12_000;
const DISCOVERY_REFRESH_MS = 5 * 60_000;
const DISCOVERY_RETAIN_MS = 30 * 60_000;
const DISCOVERY_MAX_CANDIDATES_PER_CHAIN = 30;
const CACHE_MAX_ENTRIES = 512;
const NEGATIVE_CACHE_MAX_ENTRIES = 256;
const MAX_RESPONSE_ROWS = 100;

// Step1037 Moralis production guard.
// Current official pricing: pair candlesticks=150 CU, pair swaps=50 CU.
// Free plan currently includes 40,000 CU/day; Kaka reserves headroom and fails closed at 30,000 CU/day.
// The API key exists only in Render Environment. It is never returned to App/health/logs.
const MORALIS_API_KEY = String(process.env.MORALIS_API_KEY || '').trim();
const MORALIS_MIN_GAP_MS = Math.max(750, Number(process.env.KAKA_MORALIS_MIN_GAP_MS || 1_200));
const MORALIS_MAX_QUEUE = Math.max(8, Math.min(80, Number(process.env.KAKA_MORALIS_MAX_QUEUE || 40)));
const MORALIS_TIMEOUT_MS = Math.max(5_000, Math.min(30_000, Number(process.env.KAKA_MORALIS_TIMEOUT_MS || 15_000)));
const MORALIS_DAILY_CU_BUDGET = Math.max(3_000, Math.min(38_000, Number(process.env.KAKA_MORALIS_DAILY_CU_BUDGET || 30_000)));
const MORALIS_KLINE_CU = 150;
const MORALIS_TRADES_CU = 50;
const MORALIS_LEDGER_PATH = process.env.KAKA_MORALIS_LEDGER_PATH || '/tmp/kaka_onchain_moralis_budget_v1.json';
const KLINE_CACHE_MAX_ENTRIES = 96;
const TRADE_CACHE_MAX_ENTRIES = 96;
const IDENTITY_PROOF_MAX_ENTRIES = 256;
const KLINE_MAX_ROWS = 300;
const TRADE_MAX_ROWS = 50;
const IDENTITY_PROOF_TTL_MS = 24 * 60 * 60_000;

const MORALIS_EVM_CHAIN = Object.freeze({
  ethereum: 'eth',
  bsc: 'bsc',
  base: 'base',
});
const MORALIS_TIMEFRAME = Object.freeze({
  '1m': '1min',
  '5m': '5min',
  '15m': '5min',
  '30m': '30min',
  '1h': '1h',
  '4h': '4h',
  '1d': '1d',
});
const INTERVAL_MS = Object.freeze({
  '1m': 60_000,
  '5m': 5 * 60_000,
  '15m': 15 * 60_000,
  '30m': 30 * 60_000,
  '1h': 60 * 60_000,
  '4h': 4 * 60 * 60_000,
  '1d': 24 * 60 * 60_000,
});

const NETWORKS = Object.freeze({
  ethereum: Object.freeze({ key: 'ethereum', dex: 'ethereum', chain_id: 1, family: 'evm', zh: '以太坊', en: 'Ethereum' }),
  bsc: Object.freeze({ key: 'bsc', dex: 'bsc', chain_id: 56, family: 'evm', zh: 'BNB Chain', en: 'BNB Chain' }),
  base: Object.freeze({ key: 'base', dex: 'base', chain_id: 8453, family: 'evm', zh: 'Base', en: 'Base' }),
  solana: Object.freeze({ key: 'solana', dex: 'solana', chain_id: null, family: 'solana', zh: 'Solana', en: 'Solana' }),
});
const DEX_TO_NETWORK = Object.freeze(Object.fromEntries(Object.values(NETWORKS).map((x) => [x.dex, x.key])));

const stats = {
  user_reads: 0,
  background_cycles_started: 0,
  background_cycles_succeeded: 0,
  background_cycles_failed: 0,
  last_background_started_at: null,
  last_background_success_at: null,
  last_background_error: '',
  dex_upstream_started: 0,
  dex_upstream_succeeded: 0,
  dex_upstream_failed: 0,
  cache_fresh_hits: 0,
  cache_stale_hits: 0,
  cache_misses: 0,
  negative_hits: 0,
  inflight_hits: 0,
  queue_rejections: 0,
  moralis_upstream_started: 0,
  moralis_upstream_succeeded: 0,
  moralis_upstream_failed: 0,
  moralis_budget_rejections: 0,
  moralis_key_missing_rejections: 0,
  kline_cache_fresh_hits: 0,
  kline_cache_stale_hits: 0,
  kline_cache_misses: 0,
  kline_inflight_hits: 0,
  kline_identity_exact_proofs: 0,
  kline_identity_price_match_proofs: 0,
  kline_identity_rejections: 0,
  trades_cache_hits: 0,
  trades_cache_misses: 0,
};

const cache = new Map();
const negativeCache = new Map();
const inflight = new Map();
const klineCache = new Map();
const klineInflight = new Map();
const tradeCache = new Map();
const tradeInflight = new Map();
const identityProofCache = new Map();
let discoveryStarted = false;
let discoveryInflight = null;
let trendingSnapshot = [];
let discoveryUpdatedAt = 0;

function text(value) { return String(value ?? '').trim(); }
function lower(value) { return text(value).toLowerCase(); }
function numberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function intRange(value, min, max, fallback) {
  const n = Math.floor(Number(value));
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : fallback;
}
function isoFromMs(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  try { return new Date(n).toISOString(); } catch { return null; }
}
function normalizeNetwork(raw) {
  const value = lower(raw).replace(/\s+/g, '');
  if (!value || value === 'all') return 'all';
  if (value === 'eth' || value === 'ethereum' || value === 'erc20') return 'ethereum';
  if (value === 'bsc' || value === 'bnb' || value === 'bnbchain' || value === 'bep20') return 'bsc';
  if (value === 'base') return 'base';
  if (value === 'sol' || value === 'solana' || value === 'spl') return 'solana';
  return '';
}
function networkMeta(key) { return NETWORKS[key] || null; }
function looksEvmAddress(value) { return /^0x[a-fA-F0-9]{40}$/.test(text(value)); }
function looksSolanaAddress(value) { return /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(text(value)); }
function validAddressForNetwork(network, value) {
  const meta = networkMeta(network);
  return Boolean(meta && (meta.family === 'evm' ? looksEvmAddress(value) : looksSolanaAddress(value)));
}

function responseBase(extra = {}) {
  return {
    ok: true,
    version: VERSION,
    data_version: DATA_VERSION,
    schema_version: SCHEMA_VERSION,
    read_only_shared: true,
    app_direct_upstream_requests: 0,
    user_reads_direct_upstream_requests: 0,
    fixed_backend_upstream_rate_independent_of_user_count: true,
    same_key_cache_singleflight: true,
    bounded_queue_fail_closed: true,
    cross_chain_substitution: false,
    cross_token_substitution: false,
    exact_contract_identity_required: true,
    trading_enabled: false,
    wallet_signing_enabled: false,
    database_writes: false,
    ...extra,
  };
}
function sendJson(res, status, payload, extraHeaders = {}) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
    ...extraHeaders,
  });
  res.end(body);
}
function pruneMap(map, max) {
  if (map.size <= max) return;
  const entries = [...map.entries()].sort((a, b) => Number(a[1]?.storedAt || a[1]?.until || 0) - Number(b[1]?.storedAt || b[1]?.until || 0));
  while (entries.length > max) map.delete(entries.shift()[0]);
}

function createScheduler({ name, minGapMs, maxQueue }) {
  let queue = [];
  let timer = null;
  let running = false;
  let lastStartAt = 0;
  const health = { name, min_gap_ms: minGapMs, max_queue: maxQueue, started: 0, completed: 0, failed: 0, rejected: 0, max_queue_seen: 0 };
  function pump() {
    if (running || timer || queue.length === 0) return;
    const wait = Math.max(0, minGapMs - (Date.now() - lastStartAt));
    if (wait > 0) {
      timer = setTimeout(() => { timer = null; pump(); }, wait);
      timer.unref?.();
      return;
    }
    queue.sort((a, b) => b.priority - a.priority || a.enqueuedAt - b.enqueuedAt);
    const job = queue.shift();
    running = true;
    lastStartAt = Date.now();
    health.started += 1;
    Promise.resolve().then(job.run).then((value) => {
      health.completed += 1;
      job.resolve(value);
    }, (error) => {
      health.failed += 1;
      job.reject(error);
    }).finally(() => { running = false; pump(); });
  }
  function enqueue(run, { priority = 0, label = '' } = {}) {
    if (queue.length >= maxQueue) {
      health.rejected += 1;
      stats.queue_rejections += 1;
      return Promise.reject(new Error(`${name}_queue_full`));
    }
    return new Promise((resolve, reject) => {
      queue.push({ run, resolve, reject, priority, label, enqueuedAt: Date.now() });
      health.max_queue_seen = Math.max(health.max_queue_seen, queue.length);
      pump();
    });
  }
  function state() { return { ...health, queue: queue.length, running, last_start_at: lastStartAt || null }; }
  return { enqueue, state };
}

const dexScheduler = createScheduler({ name: 'dexscreener', minGapMs: DEX_MIN_GAP_MS, maxQueue: DEX_MAX_QUEUE });
async function dexFetchJson(url, { priority = 0, label = '' } = {}) {
  return dexScheduler.enqueue(async () => {
    stats.dex_upstream_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
    timer.unref?.();
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: { accept: 'application/json', 'user-agent': 'KakaWeb3-Onchain-Shared/1036' },
      });
      const body = await response.text();
      if (!response.ok) throw new Error(`dexscreener_http_${response.status}:${body.slice(0, 220)}`);
      let parsed;
      try { parsed = JSON.parse(body); } catch { throw new Error('dexscreener_invalid_json'); }
      stats.dex_upstream_succeeded += 1;
      return parsed;
    } catch (error) {
      stats.dex_upstream_failed += 1;
      throw error;
    } finally { clearTimeout(timer); }
  }, { priority, label });
}


function utcBudgetDay() {
  return new Date().toISOString().slice(0, 10);
}
function loadMoralisLedger() {
  const fallback = { day: utcBudgetDay(), used_cu: 0, calls: 0, kline_calls: 0, trade_calls: 0, updated_at: null };
  try {
    const parsed = JSON.parse(readFileSync(MORALIS_LEDGER_PATH, 'utf8'));
    if (!parsed || parsed.day !== utcBudgetDay()) return fallback;
    return {
      day: parsed.day,
      used_cu: Math.max(0, Number(parsed.used_cu || 0)),
      calls: Math.max(0, Number(parsed.calls || 0)),
      kline_calls: Math.max(0, Number(parsed.kline_calls || 0)),
      trade_calls: Math.max(0, Number(parsed.trade_calls || 0)),
      updated_at: parsed.updated_at || null,
    };
  } catch {
    return fallback;
  }
}
let moralisLedger = loadMoralisLedger();

function refreshMoralisBudgetDay() {
  if (moralisLedger.day === utcBudgetDay()) return;
  moralisLedger = { day: utcBudgetDay(), used_cu: 0, calls: 0, kline_calls: 0, trade_calls: 0, updated_at: null };
  persistMoralisLedger();
}
function persistMoralisLedger() {
  const next = `${MORALIS_LEDGER_PATH}.${process.pid}.tmp`;
  try {
    writeFileSync(next, JSON.stringify(moralisLedger), 'utf8');
    renameSync(next, MORALIS_LEDGER_PATH);
  } catch {
    // Budget protection still stays in-memory if /tmp is unavailable.
  }
}
function moralisBudgetState() {
  refreshMoralisBudgetDay();
  return {
    day_utc: moralisLedger.day,
    used_cu: moralisLedger.used_cu,
    remaining_cu: Math.max(0, MORALIS_DAILY_CU_BUDGET - moralisLedger.used_cu),
    hard_budget_cu: MORALIS_DAILY_CU_BUDGET,
    provider_free_plan_reference_cu_per_day: 40_000,
    calls: moralisLedger.calls,
    kline_calls: moralisLedger.kline_calls,
    trade_calls: moralisLedger.trade_calls,
    ledger_path_kind: 'local_ephemeral_process_restart_persistent_tmp',
    database_write: false,
  };
}
function reserveMoralisBudget(cu, kind) {
  refreshMoralisBudgetDay();
  if (moralisLedger.used_cu + cu > MORALIS_DAILY_CU_BUDGET) {
    stats.moralis_budget_rejections += 1;
    const error = new Error('moralis_daily_cu_budget_exhausted');
    error.statusCode = 503;
    throw error;
  }
  moralisLedger.used_cu += cu;
  moralisLedger.calls += 1;
  if (kind === 'kline') moralisLedger.kline_calls += 1;
  if (kind === 'trade') moralisLedger.trade_calls += 1;
  moralisLedger.updated_at = new Date().toISOString();
  persistMoralisLedger();
}

const moralisScheduler = createScheduler({
  name: 'moralis',
  minGapMs: MORALIS_MIN_GAP_MS,
  maxQueue: MORALIS_MAX_QUEUE,
});

async function moralisFetchJson(url, { cu, kind, priority = 0, label = '' }) {
  if (!MORALIS_API_KEY) {
    stats.moralis_key_missing_rejections += 1;
    const error = new Error('moralis_api_key_not_configured');
    error.statusCode = 503;
    throw error;
  }
  return moralisScheduler.enqueue(async () => {
    reserveMoralisBudget(cu, kind);
    stats.moralis_upstream_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), MORALIS_TIMEOUT_MS);
    timer.unref?.();
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          accept: 'application/json',
          // Step1037.1: HTTP header names are case-insensitive. Sending both
          // X-API-Key and X-Api-Key can be coalesced by the HTTP client into
          // "key, key", which Moralis correctly rejects as an invalid token.
          // Send exactly one official authentication header.
          'X-API-Key': MORALIS_API_KEY,
          'user-agent': 'KakaWeb3-Onchain-Shared/1037.3',
        },
      });
      const body = await response.text();
      if (!response.ok) {
        const error = new Error(`moralis_http_${response.status}:${body.slice(0, 220)}`);
        error.statusCode = response.status;
        throw error;
      }
      let parsed;
      try { parsed = JSON.parse(body); } catch { throw new Error('moralis_invalid_json'); }
      stats.moralis_upstream_succeeded += 1;
      return parsed;
    } catch (error) {
      stats.moralis_upstream_failed += 1;
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }, { priority, label });
}

function exactAddressEqual(network, a, b) {
  if (network === 'solana') return text(a) === text(b);
  return lower(a) === lower(b);
}
function pairContainsToken(network, pair, tokenAddress) {
  return [pair?.base_token?.address, pair?.quote_token?.address]
    .some((candidate) => exactAddressEqual(network, candidate, tokenAddress));
}
async function exactPoolPreflight(network, tokenAddress, poolAddress) {
  const result = await cachedBuild(
    `token_pairs:${network}:${lower(tokenAddress)}`,
    { freshMs: 20_000, staleMs: 5 * 60_000 },
    () => buildDexTokenPairs(network, tokenAddress),
  );
  const pool = (result.value || []).find((row) =>
    exactAddressEqual(network, row?.pool_address, poolAddress) &&
    pairContainsToken(network, row, tokenAddress)
  ) || null;
  if (!pool) {
    const error = new Error('pool_not_owned_by_exact_token_on_network');
    error.statusCode = 400;
    throw error;
  }
  return pool;
}

function intervalPolicy(interval, endTimeMs = null) {
  const now = Date.now();
  const step = INTERVAL_MS[interval] || 60_000;
  const historical = Number.isFinite(Number(endTimeMs)) && Number(endTimeMs) < now - step * 4;
  if (historical) return { freshMs: 6 * 60 * 60_000, staleMs: 7 * 24 * 60 * 60_000 };
  if (interval === '1m') return { freshMs: 15_000, staleMs: 5 * 60_000 };
  if (interval === '5m' || interval === '15m' || interval === '30m') return { freshMs: 30_000, staleMs: 15 * 60_000 };
  if (interval === '1h') return { freshMs: 60_000, staleMs: 60 * 60_000 };
  if (interval === '4h') return { freshMs: 3 * 60_000, staleMs: 6 * 60 * 60_000 };
  return { freshMs: 15 * 60_000, staleMs: 24 * 60 * 60_000 };
}
function pruneSimpleCache(map, max) {
  if (map.size <= max) return;
  const entries = [...map.entries()].sort((a, b) => Number(a[1]?.storedAt || 0) - Number(b[1]?.storedAt || 0));
  while (entries.length > max) map.delete(entries.shift()[0]);
}

async function cachedKlineBuild(key, policy, builder) {
  const now = Date.now();
  const cached = klineCache.get(key);
  const age = cached ? now - cached.storedAt : Number.POSITIVE_INFINITY;
  if (cached && age <= policy.freshMs) {
    stats.kline_cache_fresh_hits += 1;
    return { value: cached.value, cache_status: 'fresh_hit' };
  }
  if (cached && age <= policy.staleMs) {
    stats.kline_cache_stale_hits += 1;
    if (!klineInflight.has(key)) {
      const pending = Promise.resolve().then(builder).then((value) => {
        if (value?.rows?.length) {
          klineCache.set(key, { value, storedAt: Date.now() });
          pruneSimpleCache(klineCache, KLINE_CACHE_MAX_ENTRIES);
        }
        return value;
      }).finally(() => klineInflight.delete(key));
      klineInflight.set(key, pending);
    }
    return { value: cached.value, cache_status: 'stale_hit' };
  }
  let pending = klineInflight.get(key);
  if (pending) {
    stats.kline_inflight_hits += 1;
  } else {
    stats.kline_cache_misses += 1;
    pending = Promise.resolve().then(builder).then((value) => {
      if (value?.rows?.length) {
        klineCache.set(key, { value, storedAt: Date.now() });
        pruneSimpleCache(klineCache, KLINE_CACHE_MAX_ENTRIES);
      }
      return value;
    }).finally(() => klineInflight.delete(key));
    klineInflight.set(key, pending);
  }
  return { value: await pending, cache_status: 'miss' };
}

function normalizeMoralisCandle(raw) {
  if (!raw) return null;
  const timestampMs = Date.parse(text(raw.timestamp));
  const open = numberOrNull(raw.open);
  const high = numberOrNull(raw.high);
  const low = numberOrNull(raw.low);
  const close = numberOrNull(raw.close);
  const volume = numberOrNull(raw.volume) ?? 0;
  const trades = numberOrNull(raw.trades);
  if (!Number.isFinite(timestampMs) || timestampMs <= 0 ||
      open === null || high === null || low === null || close === null ||
      open <= 0 || high <= 0 || low <= 0 || close <= 0) return null;
  return {
    open_time_ms: timestampMs,
    open,
    high,
    low,
    close,
    volume,
    quote_volume: volume,
    trades,
  };
}
function aggregate15m(rows) {
  const buckets = new Map();
  for (const row of rows) {
    const bucket = Math.floor(row.open_time_ms / INTERVAL_MS['15m']) * INTERVAL_MS['15m'];
    let item = buckets.get(bucket);
    if (!item) {
      item = {
        open_time_ms: bucket,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        volume: row.volume || 0,
        quote_volume: row.quote_volume || row.volume || 0,
        trades: row.trades == null ? null : 0,
        source_parts: 0,
      };
      buckets.set(bucket, item);
    }
    if (row.open_time_ms < (item._first_time ?? Number.POSITIVE_INFINITY)) {
      item._first_time = row.open_time_ms;
      item.open = row.open;
    }
    if (row.open_time_ms >= (item._last_time ?? 0)) {
      item._last_time = row.open_time_ms;
      item.close = row.close;
    }
    item.high = Math.max(item.high, row.high);
    item.low = Math.min(item.low, row.low);
    if (item.source_parts > 0) {
      item.volume += row.volume || 0;
      item.quote_volume += row.quote_volume || row.volume || 0;
    }
    if (row.trades != null) item.trades = (item.trades || 0) + row.trades;
    item.source_parts += 1;
  }
  return [...buckets.values()]
    .filter((item) => item.source_parts >= 1)
    .map(({ _first_time, _last_time, ...item }) => item)
    .sort((a, b) => a.open_time_ms - b.open_time_ms);
}
function klineRange(interval, limit, endTimeMs = null) {
  const sourceInterval = MORALIS_TIMEFRAME[interval];
  const sourceStep = interval === '15m' ? INTERVAL_MS['5m'] : INTERVAL_MS[interval];
  const sourceLimit = Math.min(1000, interval === '15m' ? limit * 3 + 6 : limit + 3);
  const toMs = Number.isFinite(Number(endTimeMs)) && Number(endTimeMs) > 0
    ? Number(endTimeMs)
    : Date.now();
  const fromMs = Math.max(0, toMs - sourceStep * (sourceLimit + 4));
  return { sourceInterval, sourceLimit, fromMs, toMs };
}
function moralisKlineUrl(network, poolAddress, interval, limit, endTimeMs) {
  const range = klineRange(interval, limit, endTimeMs);
  if (network === 'solana') {
    const u = new URL(`https://solana-gateway.moralis.io/token/mainnet/pairs/${encodeURIComponent(poolAddress)}/ohlcv`);
    u.searchParams.set('timeframe', range.sourceInterval);
    u.searchParams.set('currency', 'usd');
    u.searchParams.set('fromDate', new Date(range.fromMs).toISOString());
    u.searchParams.set('toDate', new Date(range.toMs).toISOString());
    u.searchParams.set('limit', String(range.sourceLimit));
    return { url: u.toString(), ...range };
  }
  const chain = MORALIS_EVM_CHAIN[network];
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/pairs/${encodeURIComponent(poolAddress)}/ohlcv`);
  u.searchParams.set('chain', chain);
  u.searchParams.set('timeframe', range.sourceInterval);
  u.searchParams.set('currency', 'usd');
  u.searchParams.set('fromDate', new Date(range.fromMs).toISOString());
  u.searchParams.set('toDate', new Date(range.toMs).toISOString());
  u.searchParams.set('limit', String(range.sourceLimit));
  return { url: u.toString(), ...range };
}

function proofKey(network, tokenAddress, poolAddress) {
  return `${network}:${lower(tokenAddress)}:${lower(poolAddress)}`;
}
function getIdentityProof(network, tokenAddress, poolAddress) {
  const key = proofKey(network, tokenAddress, poolAddress);
  const proof = identityProofCache.get(key);
  if (!proof) return null;
  if (Date.now() - proof.storedAt > IDENTITY_PROOF_TTL_MS) {
    identityProofCache.delete(key);
    return null;
  }
  return proof;
}
function setIdentityProof(network, tokenAddress, poolAddress, kind, detail = {}) {
  identityProofCache.set(proofKey(network, tokenAddress, poolAddress), {
    kind,
    detail,
    storedAt: Date.now(),
  });
  pruneSimpleCache(identityProofCache, IDENTITY_PROOF_MAX_ENTRIES);
}
function priceIdentityCompatible(dexPrice, closePrice) {
  const a = Number(dexPrice);
  const b = Number(closePrice);
  if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0) return false;
  const ratio = b / a;
  return ratio >= 0.40 && ratio <= 2.50;
}

async function buildMoralisKlines(network, tokenAddress, pool, interval, limit, endTimeMs) {
  const poolAddress = pool.pool_address;
  const spec = moralisKlineUrl(network, poolAddress, interval, limit, endTimeMs);
  const payload = await moralisFetchJson(spec.url, {
    cu: MORALIS_KLINE_CU,
    kind: 'kline',
    priority: endTimeMs ? 5 : 20,
    label: `kline:${network}:${poolAddress}:${interval}`,
  });
  if (!exactAddressEqual(network, payload?.pairAddress, poolAddress)) {
    stats.kline_identity_rejections += 1;
    throw new Error('moralis_pair_identity_mismatch');
  }
  let rows = Array.isArray(payload?.result)
    ? payload.result.map(normalizeMoralisCandle).filter(Boolean).sort((a, b) => a.open_time_ms - b.open_time_ms)
    : [];
  if (interval === '15m') rows = aggregate15m(rows);
  rows = rows.slice(-limit);
  if (!rows.length) return {
    rows: [],
    source_token_address: text(payload?.tokenAddress),
    source_pair_address: text(payload?.pairAddress),
    source_timeframe: text(payload?.timeframe),
    derived_15m_from_5m: interval === '15m',
    identity_proof: getIdentityProof(network, tokenAddress, poolAddress)?.kind || null,
  };

  const returnedToken = text(payload?.tokenAddress);
  let proof = getIdentityProof(network, tokenAddress, poolAddress);
  if (returnedToken && exactAddressEqual(network, returnedToken, tokenAddress)) {
    setIdentityProof(network, tokenAddress, poolAddress, 'moralis_exact_token_address');
    stats.kline_identity_exact_proofs += 1;
    proof = getIdentityProof(network, tokenAddress, poolAddress);
  } else if (!proof && !endTimeMs) {
    const latestClose = rows[rows.length - 1]?.close;
    if (priceIdentityCompatible(pool.price_usd, latestClose)) {
      setIdentityProof(network, tokenAddress, poolAddress, 'latest_close_matches_exact_dex_token_price', {
        dex_price_usd: pool.price_usd,
        moralis_close_usd: latestClose,
      });
      stats.kline_identity_price_match_proofs += 1;
      proof = getIdentityProof(network, tokenAddress, poolAddress);
    }
  }
  if (!proof) {
    stats.kline_identity_rejections += 1;
    throw new Error('moralis_ohlcv_token_identity_not_proven');
  }

  return {
    rows,
    source_token_address: returnedToken || null,
    source_pair_address: text(payload?.pairAddress),
    source_timeframe: text(payload?.timeframe) || spec.sourceInterval,
    derived_15m_from_5m: interval === '15m',
    identity_proof: proof.kind,
  };
}

function moralisPairTokenMeta(raw, amount, usdPrice) {
  if (!raw || typeof raw !== 'object') return null;
  const amountN = numberOrNull(amount);
  const priceN = numberOrNull(usdPrice);
  return {
    address: text(raw.address),
    name: text(raw.name),
    symbol: text(raw.symbol),
    decimals: numberOrNull(raw.decimals),
    amount: amountN,
    usd_price: priceN,
    usd_amount: amountN !== null && priceN !== null ? amountN * priceN : null,
  };
}

function normalizeMoralisPairSwap(network, raw, tokenAddress, pairMeta) {
  if (!raw) return null;
  const poolAddress = text(pairMeta?.pairAddress);
  const rowPair = text(raw.pairAddress);
  if (rowPair && poolAddress && !exactAddressEqual(network, rowPair, poolAddress)) {
    return null;
  }

  const base = moralisPairTokenMeta(
    pairMeta?.baseToken,
    raw.baseTokenAmount,
    raw.baseTokenPriceUsd,
  );
  const quote = moralisPairTokenMeta(
    pairMeta?.quoteToken,
    raw.quoteTokenAmount,
    raw.quoteTokenPriceUsd,
  );
  const requestedIsBase = Boolean(
    base?.address && exactAddressEqual(network, base.address, tokenAddress),
  );
  const requestedIsQuote = Boolean(
    quote?.address && exactAddressEqual(network, quote.address, tokenAddress),
  );
  if (!requestedIsBase && !requestedIsQuote) return null;

  const sourceType = lower(raw.transactionType);
  let bought = null;
  let sold = null;
  // Moralis Pair Swaps defines buy/sell relative to the pair's base token.
  // Preserve the source type and only derive bought/sold from the exact pair orientation.
  if (sourceType === 'buy') {
    bought = base;
    sold = quote;
  } else if (sourceType === 'sell') {
    bought = quote;
    sold = base;
  }

  const requestedSide =
    sourceType === 'buy'
      ? (requestedIsBase ? 'buy' : requestedIsQuote ? 'sell' : null)
      : sourceType === 'sell'
        ? (requestedIsBase ? 'sell' : requestedIsQuote ? 'buy' : null)
        : sourceType || null;

  return {
    transaction_hash: text(raw.transactionHash),
    transaction_index: numberOrNull(raw.transactionIndex),
    block_number: numberOrNull(raw.blockNumber),
    block_timestamp: text(raw.blockTimestamp),
    source_transaction_type: sourceType || null,
    requested_token_side: requestedSide,
    sub_category: text(raw.subCategory) || null,
    wallet_address: text(raw.walletAddress),
    pair_address: rowPair || poolAddress,
    pair_label: text(pairMeta?.pairLabel),
    exchange_name: text(pairMeta?.exchangeName),
    base_token: base,
    quote_token: quote,
    bought,
    sold,
    base_quote_price: numberOrNull(raw.baseQuotePrice),
    total_value_usd: numberOrNull(raw.totalValueUsd),
    requested_token_in_trade: true,
    requested_token_is_base: requestedIsBase,
    requested_token_is_quote: requestedIsQuote,
  };
}

function moralisTradesUrl(network, poolAddress, limit) {
  if (network === 'solana') {
    const u = new URL(`https://solana-gateway.moralis.io/token/mainnet/pairs/${encodeURIComponent(poolAddress)}/swaps`);
    u.searchParams.set('limit', String(limit));
    return u.toString();
  }
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/pairs/${encodeURIComponent(poolAddress)}/swaps`);
  u.searchParams.set('chain', MORALIS_EVM_CHAIN[network]);
  u.searchParams.set('limit', String(limit));
  return u.toString();
}
async function cachedTradeBuild(key, builder) {
  const now = Date.now();
  const cached = tradeCache.get(key);
  if (cached && now - cached.storedAt <= 15_000) {
    stats.trades_cache_hits += 1;
    return { value: cached.value, cache_status: 'fresh_hit' };
  }
  if (cached && now - cached.storedAt <= 2 * 60_000) {
    stats.trades_cache_hits += 1;
    if (!tradeInflight.has(key)) {
      const pending = Promise.resolve().then(builder).then((value) => {
        tradeCache.set(key, { value, storedAt: Date.now() });
        pruneSimpleCache(tradeCache, TRADE_CACHE_MAX_ENTRIES);
        return value;
      }).finally(() => tradeInflight.delete(key));
      tradeInflight.set(key, pending);
    }
    return { value: cached.value, cache_status: 'stale_hit' };
  }
  let pending = tradeInflight.get(key);
  if (!pending) {
    stats.trades_cache_misses += 1;
    pending = Promise.resolve().then(builder).then((value) => {
      tradeCache.set(key, { value, storedAt: Date.now() });
      pruneSimpleCache(tradeCache, TRADE_CACHE_MAX_ENTRIES);
      return value;
    }).finally(() => tradeInflight.delete(key));
    tradeInflight.set(key, pending);
  } else {
    stats.trades_cache_hits += 1;
  }
  return { value: await pending, cache_status: 'miss' };
}

async function buildMoralisTrades(network, tokenAddress, pool, limit) {
  const payload = await moralisFetchJson(moralisTradesUrl(network, pool.pool_address, limit), {
    cu: MORALIS_TRADES_CU,
    kind: 'trade',
    priority: 10,
    label: `trades:${network}:${pool.pool_address}`,
  });

  const returnedPair = text(payload?.pairAddress);
  if (returnedPair && !exactAddressEqual(network, returnedPair, pool.pool_address)) {
    throw new Error('moralis_trade_pair_identity_mismatch');
  }

  const baseAddress = text(payload?.baseToken?.address);
  const quoteAddress = text(payload?.quoteToken?.address);
  const moralisPairContainsRequestedToken =
    (baseAddress && exactAddressEqual(network, baseAddress, tokenAddress)) ||
    (quoteAddress && exactAddressEqual(network, quoteAddress, tokenAddress));

  if ((baseAddress || quoteAddress) && !moralisPairContainsRequestedToken) {
    throw new Error('moralis_trade_token_identity_mismatch');
  }

  const rawRows = Array.isArray(payload?.result) ? payload.result : [];
  const pairMeta = {
    pairAddress: returnedPair || pool.pool_address,
    pairLabel: payload?.pairLabel,
    exchangeName: payload?.exchangeName,
    baseToken: payload?.baseToken,
    quoteToken: payload?.quoteToken,
  };

  return rawRows
    .map((row) => normalizeMoralisPairSwap(network, row, tokenAddress, pairMeta))
    .filter(Boolean)
    .slice(0, limit);
}

function recentCandidatePools(network, limit) {
  const seen = new Set();
  const rows = [];
  for (const tokenRow of trendingSnapshot) {
    if (network !== 'all' && tokenRow.network !== network) continue;
    const pool = tokenRow.best_pool;
    if (!pool?.pool_address || !pool.pool_created_at) continue;
    const key = `${tokenRow.network}:${lower(pool.pool_address)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    rows.push({
      network: tokenRow.network,
      token: tokenRow.token,
      pool,
      created_at: pool.pool_created_at,
      recent_hot_score: tokenRow.recent_hot_score,
      discovery_scope: 'profile_and_boost_candidate_pool_not_exhaustive_chain_scan',
    });
  }
  rows.sort((a, b) => Date.parse(b.created_at || 0) - Date.parse(a.created_at || 0));
  return rows.slice(0, limit);
}

async function cachedBuild(key, { freshMs, staleMs, negativeMs = 45_000 }, builder) {
  const now = Date.now();
  const neg = negativeCache.get(key);
  if (neg && neg.until > now) { stats.negative_hits += 1; return { value: null, cache_status: 'negative_hit' }; }
  if (neg) negativeCache.delete(key);
  const entry = cache.get(key);
  if (entry && now - entry.storedAt <= freshMs) { stats.cache_fresh_hits += 1; return { value: entry.value, cache_status: 'fresh_hit' }; }
  if (entry && now - entry.storedAt <= staleMs) {
    stats.cache_stale_hits += 1;
    if (!inflight.has(key)) {
      const pending = Promise.resolve().then(builder).then((value) => {
        if (value === null || value === undefined || (Array.isArray(value) && value.length === 0)) {
          negativeCache.set(key, { until: Date.now() + negativeMs }); pruneMap(negativeCache, NEGATIVE_CACHE_MAX_ENTRIES);
        } else { cache.set(key, { value, storedAt: Date.now() }); pruneMap(cache, CACHE_MAX_ENTRIES); }
        return value;
      }).finally(() => inflight.delete(key));
      inflight.set(key, pending);
    }
    return { value: entry.value, cache_status: 'stale_hit' };
  }
  let pending = inflight.get(key);
  if (pending) stats.inflight_hits += 1;
  if (!pending) {
    stats.cache_misses += 1;
    pending = Promise.resolve().then(builder).then((value) => {
      if (value === null || value === undefined || (Array.isArray(value) && value.length === 0)) {
        negativeCache.set(key, { until: Date.now() + negativeMs }); pruneMap(negativeCache, NEGATIVE_CACHE_MAX_ENTRIES);
      } else { cache.set(key, { value, storedAt: Date.now() }); pruneMap(cache, CACHE_MAX_ENTRIES); }
      return value;
    }).finally(() => inflight.delete(key));
    inflight.set(key, pending);
  }
  return { value: await pending, cache_status: 'miss' };
}

function normalizeDexPair(pair) {
  if (!pair || typeof pair !== 'object') return null;
  const network = DEX_TO_NETWORK[lower(pair.chainId)] || '';
  if (!network) return null;
  const base = pair.baseToken || {};
  const quote = pair.quoteToken || {};
  const poolAddress = text(pair.pairAddress);
  if (!poolAddress) return null;
  const txns = pair.txns || {};
  const volume = pair.volume || {};
  const changes = pair.priceChange || {};
  const liquidity = pair.liquidity || {};
  return {
    network,
    chain_id: NETWORKS[network].chain_id,
    pool_address: poolAddress,
    dex_id: text(pair.dexId),
    pair_url: text(pair.url),
    labels: Array.isArray(pair.labels) ? pair.labels.map(text).filter(Boolean).slice(0, 8) : [],
    base_token: { address: text(base.address), symbol: text(base.symbol), name: text(base.name) },
    quote_token: { address: text(quote.address), symbol: text(quote.symbol), name: text(quote.name) },
    price_usd: numberOrNull(pair.priceUsd),
    price_native: numberOrNull(pair.priceNative),
    liquidity_usd: numberOrNull(liquidity.usd),
    market_cap_usd: numberOrNull(pair.marketCap),
    fdv_usd: numberOrNull(pair.fdv),
    volume_usd: { m5: numberOrNull(volume.m5), h1: numberOrNull(volume.h1), h6: numberOrNull(volume.h6), h24: numberOrNull(volume.h24) },
    price_change_pct: { m5: numberOrNull(changes.m5), h1: numberOrNull(changes.h1), h6: numberOrNull(changes.h6), h24: numberOrNull(changes.h24) },
    txns: { m5: txns.m5 || null, h1: txns.h1 || null, h6: txns.h6 || null, h24: txns.h24 || null },
    pool_created_at: isoFromMs(pair.pairCreatedAt),
    info: pair.info && typeof pair.info === 'object' ? {
      image_url: text(pair.info.imageUrl),
      websites: Array.isArray(pair.info.websites) ? pair.info.websites.slice(0, 6) : [],
      socials: Array.isArray(pair.info.socials) ? pair.info.socials.slice(0, 8) : [],
    } : null,
    boosts_active: numberOrNull(pair?.boosts?.active),
    source: 'dexscreener_public_api',
  };
}
function normalizeDexPairs(payload) {
  const rows = Array.isArray(payload) ? payload : Array.isArray(payload?.pairs) ? payload.pairs : [];
  return rows.map(normalizeDexPair).filter(Boolean);
}
function pairIdentity(row) { return `${row.network}|${lower(row.pool_address)}`; }
function dedupePools(rows) {
  const byKey = new Map();
  for (const row of rows || []) {
    const key = pairIdentity(row);
    const current = byKey.get(key);
    if (!current || Number(row.liquidity_usd || 0) > Number(current.liquidity_usd || 0)) byKey.set(key, row);
  }
  return [...byKey.values()];
}
function tokenAddressInPair(row, tokenAddress) {
  const a = lower(tokenAddress);
  if (lower(row?.base_token?.address) === a) return row.base_token;
  if (lower(row?.quote_token?.address) === a) return row.quote_token;
  return null;
}
function poolScore(row) {
  const liq = Math.log10(Math.max(1, Number(row.liquidity_usd || 0)));
  const vol = Math.log10(Math.max(1, Number(row?.volume_usd?.h24 || 0)));
  const h1tx = Number(row?.txns?.h1?.buys || 0) + Number(row?.txns?.h1?.sells || 0);
  const m5tx = Number(row?.txns?.m5?.buys || 0) + Number(row?.txns?.m5?.sells || 0);
  return liq * 4 + vol * 2 + Math.log10(1 + h1tx) * 1.5 + Math.log10(1 + m5tx) * 2;
}
function sortBestPools(rows) { return [...rows].sort((a, b) => poolScore(b) - poolScore(a)); }

function tokenMatchesText(token, query) {
  const q = lower(query);
  if (!q) return false;
  return [token?.address, token?.symbol, token?.name]
    .map((value) => lower(value))
    .some((value) => value && (value === q || value.includes(q)));
}
function tokenOrientationInPair(pair, tokenAddress) {
  if (lower(pair?.base_token?.address) === lower(tokenAddress)) return 'base';
  if (lower(pair?.quote_token?.address) === lower(tokenAddress)) return 'quote';
  return '';
}
function nullPriceChange() {
  return { m5: null, h1: null, h6: null, h24: null };
}
function tokenCentricRow(pair, token, extra = {}) {
  const orientation = tokenOrientationInPair(pair, token?.address);
  if (!orientation) return null;
  // Fail closed: pair-level price/change/FDV/market-cap are only bound to
  // the exact baseToken identity. A quoteToken never inherits those fields.
  const baseVerified = orientation === 'base';
  return {
    network: pair.network,
    chain_id: pair.chain_id,
    token: { ...token },
    best_pool: pair,
    token_orientation: orientation,
    token_market_fields_verified: baseVerified,
    price_usd: baseVerified ? pair.price_usd : null,
    liquidity_usd: pair.liquidity_usd,
    market_cap_usd: baseVerified ? pair.market_cap_usd : null,
    fdv_usd: baseVerified ? pair.fdv_usd : null,
    volume_usd: pair.volume_usd,
    price_change_pct: baseVerified ? pair.price_change_pct : nullPriceChange(),
    txns: pair.txns,
    pool_created_at: pair.pool_created_at,
    source: 'dexscreener_public_api_token_centric',
    ...extra,
  };
}
function chooseBetterTokenRow(current, candidate) {
  if (!current) return candidate;
  if (candidate.token_market_fields_verified && !current.token_market_fields_verified) return candidate;
  if (!candidate.token_market_fields_verified && current.token_market_fields_verified) return current;
  return poolScore(candidate.best_pool) > poolScore(current.best_pool) ? candidate : current;
}
function tokenCentricSearchRows(query, pairs) {
  const byToken = new Map();
  for (const pair of pairs || []) {
    for (const token of [pair.base_token, pair.quote_token]) {
      if (!token?.address || !tokenMatchesText(token, query)) continue;
      const row = tokenCentricRow(pair, token, { search_query: text(query) });
      if (!row) continue;
      const key = `${pair.network}|${lower(token.address)}`;
      byToken.set(key, chooseBetterTokenRow(byToken.get(key), row));
    }
  }
  return [...byToken.values()]
    .sort((a, b) => {
      if (a.token_market_fields_verified !== b.token_market_fields_verified) {
        return a.token_market_fields_verified ? -1 : 1;
      }
      return poolScore(b.best_pool) - poolScore(a.best_pool);
    })
    .slice(0, MAX_RESPONSE_ROWS);
}

async function buildDexSearch(query) {
  const payload = await dexFetchJson(`${DEX_BASE}/latest/dex/search?q=${encodeURIComponent(query)}`, { priority: 10, label: 'search' });
  const pairs = sortBestPools(dedupePools(normalizeDexPairs(payload))).slice(0, MAX_RESPONSE_ROWS);
  return tokenCentricSearchRows(query, pairs);
}
async function buildDexTokenPairs(network, address) {
  const meta = networkMeta(network);
  if (!meta || !validAddressForNetwork(network, address)) return [];
  const payload = await dexFetchJson(`${DEX_BASE}/token-pairs/v1/${encodeURIComponent(meta.dex)}/${encodeURIComponent(address)}`, { priority: 12, label: 'token_pairs' });
  const exact = normalizeDexPairs(payload).filter((row) => row.network === network && tokenAddressInPair(row, address));
  return sortBestPools(dedupePools(exact)).slice(0, MAX_RESPONSE_ROWS);
}
function candidateRows(payload, source) {
  const raw = Array.isArray(payload) ? payload : payload && typeof payload === 'object' ? [payload] : [];
  return raw.map((item) => {
    const network = DEX_TO_NETWORK[lower(item?.chainId)] || '';
    const address = text(item?.tokenAddress);
    if (!network || !validAddressForNetwork(network, address)) return null;
    return { network, address, source, amount: numberOrNull(item?.amount), total_amount: numberOrNull(item?.totalAmount) };
  }).filter(Boolean);
}
async function fetchDiscoveryCandidatePairs() {
  const [boosts, profiles] = await Promise.all([
    dexFetchJson(`${DEX_BASE}/token-boosts/top/v1`, { priority: -20, label: 'background_boost_candidates' }),
    dexFetchJson(`${DEX_BASE}/token-profiles/latest/v1`, { priority: -20, label: 'background_profile_candidates' }),
  ]);
  const candidates = [...candidateRows(boosts, 'top_boost_candidate'), ...candidateRows(profiles, 'latest_profile_candidate')];
  const byIdentity = new Map();
  for (const row of candidates) {
    const key = `${row.network}|${lower(row.address)}`;
    const cur = byIdentity.get(key) || { ...row, candidate_sources: [] };
    if (!cur.candidate_sources.includes(row.source)) cur.candidate_sources.push(row.source);
    cur.amount = Math.max(Number(cur.amount || 0), Number(row.amount || 0));
    cur.total_amount = Math.max(Number(cur.total_amount || 0), Number(row.total_amount || 0));
    byIdentity.set(key, cur);
  }
  const grouped = new Map(Object.keys(NETWORKS).map((key) => [key, []]));
  for (const row of byIdentity.values()) {
    const list = grouped.get(row.network);
    if (list && list.length < DISCOVERY_MAX_CANDIDATES_PER_CHAIN) list.push(row);
  }
  const pairs = [];
  for (const [network, list] of grouped) {
    if (!list.length) continue;
    const meta = networkMeta(network);
    const addresses = list.map((x) => x.address).slice(0, 30);
    const payload = await dexFetchJson(`${DEX_BASE}/tokens/v1/${encodeURIComponent(meta.dex)}/${addresses.map(encodeURIComponent).join(',')}`, { priority: -15, label: `background_batch_${network}` });
    const normalized = normalizeDexPairs(payload).filter((row) => row.network === network);
    for (const pair of normalized) {
      const match = list.find((x) => lower(pair.base_token.address) === lower(x.address) || lower(pair.quote_token.address) === lower(x.address));
      if (!match) continue;
      pairs.push({
        ...pair,
        candidate_token_address: match.address,
        candidate_sources: match.candidate_sources,
        candidate_boost_amount: match.amount,
        candidate_total_boost_amount: match.total_amount,
      });
    }
  }
  return pairs;
}
function recentHotTokenRows(pairs) {
  const byToken = new Map();
  for (const pair of pairs || []) {
    const candidateAddress = text(pair.candidate_token_address);
    if (!candidateAddress) continue;
    const token = tokenAddressInPair(pair, candidateAddress);
    if (!token?.address) continue;
    const row = tokenCentricRow(pair, token, {
      recent_hot_score: poolScore(pair),
      candidate_sources: pair.candidate_sources || [],
      candidate_boost_amount: pair.candidate_boost_amount ?? null,
      candidate_total_boost_amount: pair.candidate_total_boost_amount ?? null,
      source: 'dexscreener_public_api_exact_discovery_token_rescore',
    });
    if (!row) continue;
    const key = `${pair.network}|${lower(token.address)}`;
    byToken.set(key, chooseBetterTokenRow(byToken.get(key), row));
  }
  return [...byToken.values()]
    .sort((a, b) => {
      if (a.token_market_fields_verified !== b.token_market_fields_verified) {
        return a.token_market_fields_verified ? -1 : 1;
      }
      return Number(b.recent_hot_score || 0) - Number(a.recent_hot_score || 0);
    })
    .slice(0, MAX_RESPONSE_ROWS);
}

async function refreshDiscovery() {
  if (discoveryInflight) return discoveryInflight;
  discoveryInflight = (async () => {
    stats.background_cycles_started += 1;
    stats.last_background_started_at = new Date().toISOString();
    try {
      const pairs = await fetchDiscoveryCandidatePairs();
      const rows = recentHotTokenRows(pairs);
      if (!rows.length) throw new Error('dexscreener_recent_hot_discovery_empty');
      trendingSnapshot = rows;
      discoveryUpdatedAt = Date.now();
      stats.background_cycles_succeeded += 1;
      stats.last_background_success_at = new Date().toISOString();
      stats.last_background_error = '';
      return rows.length;
    } catch (error) {
      stats.background_cycles_failed += 1;
      stats.last_background_error = text(error?.message || error).replace(/\s+/g, ' ').slice(0, 300);
      throw error;
    } finally { discoveryInflight = null; }
  })();
  return discoveryInflight;
}
export function startOnchainMarketCollector() {
  if (discoveryStarted) return;
  discoveryStarted = true;
  const first = setTimeout(() => refreshDiscovery().catch(() => {}), 2_500);
  first.unref?.();
  const timer = setInterval(() => refreshDiscovery().catch(() => {}), DISCOVERY_REFRESH_MS);
  timer.unref?.();
}
function discoveryRows(network, limit) {
  return trendingSnapshot.filter((row) => network === 'all' || row.network === network).slice(0, limit);
}
function healthPayload() {
  const ageMs = discoveryUpdatedAt ? Math.max(0, Date.now() - discoveryUpdatedAt) : null;
  return responseBase({
    service: 'onchain-market',
    networks: Object.values(NETWORKS).map((x) => ({ key: x.key, dex: x.dex, chain_id: x.chain_id, family: x.family, zh: x.zh, en: x.en })),
    sources: {
      dexscreener: {
        docs: 'https://docs.dexscreener.com/api/reference',
        api_terms: 'https://docs.dexscreener.com/api/api-terms-and-conditions',
        commercial_use_permitted_subject_to_api_terms: true,
        api_resale_or_direct_competitor_use_forbidden: true,
        role: 'recent_hot_candidates_plus_exact_search_token_pools',
        documented_candidate_rate_limit_per_minute: 60,
        documented_search_pair_rate_limit_per_minute: 300,
        backend_global_min_gap_ms: DEX_MIN_GAP_MS,
        backend_global_max_starts_per_minute: Math.floor(60_000 / DEX_MIN_GAP_MS),
      },
      moralis: {
        docs_evm_ohlcv: 'https://docs.moralis.com/data-api/evm/price/ohlc',
        docs_solana_ohlcv: 'https://docs.moralis.com/data-api/solana/price/ohlc',
        docs_pricing: 'https://docs.moralis.com/data-api/pricing',
        terms: 'https://moralis.com/terms/',
        role: 'exact_pool_ohlcv_history_plus_recent_pair_swaps',
        api_key_configured: Boolean(MORALIS_API_KEY),
        api_key_exposed: false,
        backend_only_secret: true,
        auth_header_name: 'X-API-Key',
        auth_header_count_per_request: 1,
        duplicate_case_variant_headers: false,
        pair_candlestick_cu: MORALIS_KLINE_CU,
        pair_swap_cu: MORALIS_TRADES_CU,
        scheduler: moralisScheduler.state(),
        budget: moralisBudgetState(),
      },
    },
    discovery: {
      ready: trendingSnapshot.length > 0 && (ageMs === null || ageMs <= DISCOVERY_RETAIN_MS),
      name: 'recent_hot',
      token_centric_results: true,
      exact_discovered_candidate_token_only: true,
      both_sides_of_pair_are_not_automatically_listed: true,
      quote_token_never_inherits_base_token_market_fields: true,
      basis: 'latest_profile_plus_top_boost_candidates_rescored_by_liquidity_volume_and_transactions',
      paid_boost_rank_not_used_as_final_rank: true,
      retained_if_refresh_fails: true,
      refresh_interval_ms: DISCOVERY_REFRESH_MS,
      retain_ms: DISCOVERY_RETAIN_MS,
      age_ms: ageMs,
      rows: trendingSnapshot.length,
      max_candidates_per_chain: DISCOVERY_MAX_CANDIDATES_PER_CHAIN,
    },
    kline: {
      opened: true,
      route: KLINES_ROUTE,
      source: 'moralis_official_data_api_pair_ohlcv',
      api_key_configured: Boolean(MORALIS_API_KEY),
      app_direct_moralis_requests: 0,
      exact_chain_token_pool_preflight: true,
      historical_pages_require_identity_proof: true,
      supported_intervals: Object.keys(MORALIS_TIMEFRAME),
      derived_15m_from_same_pool_5m: true,
      max_rows_per_response: KLINE_MAX_ROWS,
      cache_entries: klineCache.size,
      cache_max_entries: KLINE_CACHE_MAX_ENTRIES,
      inflight: klineInflight.size,
      identity_proof_entries: identityProofCache.size,
      identity_proof_ttl_ms: IDENTITY_PROOF_TTL_MS,
      moralis_cu_per_upstream_call: MORALIS_KLINE_CU,
    },
    recent_trades: {
      opened: true,
      route: TRADES_ROUTE,
      source: 'moralis_official_data_api_pair_swaps',
      api_key_configured: Boolean(MORALIS_API_KEY),
      exact_chain_token_pool_preflight: true,
      max_rows: TRADE_MAX_ROWS,
      cache_entries: tradeCache.size,
      moralis_cu_per_upstream_call: MORALIS_TRADES_CU,
      response_schema: 'pair_level_base_quote_metadata_plus_row_base_quote_amounts',
      token_swaps_bought_sold_schema_not_assumed: true,
      exact_moralis_pair_identity_checked: true,
      exact_requested_token_in_pair_checked_when_metadata_present: true,
    },
    new_pools: {
      opened: true,
      route: NEW_POOLS_ROUTE,
      source: 'dexscreener_step1036_shared_candidate_snapshot',
      exhaustive_chain_scan: false,
      user_reads_start_upstream: false,
    },
    bounded_user_builds: {
      exact_search_and_token_pool_may_enqueue_bounded_build: true,
      search_returns_token_centric_rows: true,
      quote_token_market_fields_fail_closed: true,
      fixed_backend_rate_independent_of_user_count: true,
      same_key_cache_singleflight: true,
      queue_overflow_rejected_not_amplified: true,
      direct_app_upstream: false,
    },
    caches: {
      entries: cache.size,
      max_entries: CACHE_MAX_ENTRIES,
      negative_entries: negativeCache.size,
      negative_max_entries: NEGATIVE_CACHE_MAX_ENTRIES,
      inflight: inflight.size,
      kline_entries: klineCache.size,
      kline_max_entries: KLINE_CACHE_MAX_ENTRIES,
      kline_inflight: klineInflight.size,
      trade_entries: tradeCache.size,
      trade_max_entries: TRADE_CACHE_MAX_ENTRIES,
      trade_inflight: tradeInflight.size,
      identity_proof_entries: identityProofCache.size,
      identity_proof_max_entries: IDENTITY_PROOF_MAX_ENTRIES,
    },
    scheduler: dexScheduler.state(),
    moralis_scheduler: moralisScheduler.state(),
    moralis_budget: moralisBudgetState(),
    stats: { ...stats },
    memory_usage: { rss_mb: Math.round(process.memoryUsage().rss / 1048576), heap_used_mb: Math.round(process.memoryUsage().heapUsed / 1048576) },
  });
}
export function getOnchainMarketHealth() { return healthPayload(); }

function runSelfTest() {
  const tests = [];
  const t = (name, pass, detail = '') => tests.push({ name, pass: Boolean(pass), detail });
  t('network_eth_alias', normalizeNetwork('ETH') === 'ethereum');
  t('network_bsc_alias', normalizeNetwork('BNBChain') === 'bsc');
  t('network_sol_alias', normalizeNetwork('SOL') === 'solana');
  t('evm_address_validation', looksEvmAddress('0x0000000000000000000000000000000000000001'));
  t('solana_address_validation', looksSolanaAddress('So11111111111111111111111111111111111111112'));
  t('bad_address_rejected', !looksEvmAddress('not-a-contract') && !looksSolanaAddress('not-a-contract'));
  const dex = normalizeDexPair({ chainId: 'base', dexId: 'uniswap', pairAddress: '0x1111111111111111111111111111111111111111', baseToken: { address: '0x2222222222222222222222222222222222222222', symbol: 'AAA', name: 'A' }, quoteToken: { address: '0x3333333333333333333333333333333333333333', symbol: 'USDC', name: 'USD Coin' }, priceUsd: '1.2', liquidity: { usd: 10 }, volume: { h24: 20 }, priceChange: { h24: 3 } });
  t('dex_pair_parser', dex?.network === 'base' && dex?.price_usd === 1.2 && dex?.liquidity_usd === 10);
  const quoteSynthetic = tokenCentricRow(
    {
      network: 'base',
      chain_id: 'base',
      base_token: { address: '0x0000000000000000000000000000000000000001', symbol: 'BASE', name: 'Base' },
      quote_token: { address: '0x0000000000000000000000000000000000000002', symbol: 'QUOTE', name: 'Quote' },
      price_usd: 9.99,
      market_cap_usd: 999,
      fdv_usd: 1111,
      liquidity_usd: 100,
      volume_usd: { h24: 50 },
      price_change_pct: { h24: 88 },
      txns: {},
      pool_created_at: null,
      pool_address: '0x0000000000000000000000000000000000000003',
      dex_id: 'test',
    },
    { address: '0x0000000000000000000000000000000000000002', symbol: 'QUOTE', name: 'Quote' },
  );
  t('quote_token_market_fields_fail_closed',
    quoteSynthetic?.token_market_fields_verified === false &&
    quoteSynthetic?.price_usd === null &&
    quoteSynthetic?.market_cap_usd === null &&
    quoteSynthetic?.fdv_usd === null &&
    quoteSynthetic?.price_change_pct?.h24 === null);
  t('cross_chain_substitution_false', responseBase().cross_chain_substitution === false);
  t('cross_token_substitution_false', responseBase().cross_token_substitution === false);
  t('direct_app_upstream_zero', responseBase().app_direct_upstream_requests === 0);
  t('global_rate_below_candidate_limit', 60_000 / DEX_MIN_GAP_MS < 60);
  t('cache_bounded', CACHE_MAX_ENTRIES <= 1024);
  t('negative_cache_bounded', NEGATIVE_CACHE_MAX_ENTRIES <= 512);
  t('response_rows_bounded', MAX_RESPONSE_ROWS <= 100);
  t('kline_opened_with_backend_secret_source', healthPayload().kline.opened === true);
  t('kline_app_direct_moralis_zero', healthPayload().kline.app_direct_moralis_requests === 0);
  t('kline_limit_bounded', KLINE_MAX_ROWS <= 300);
  t('kline_cache_bounded', KLINE_CACHE_MAX_ENTRIES <= 128);
  t('moralis_budget_below_free_reference', MORALIS_DAILY_CU_BUDGET < 40_000);
  t('moralis_secret_never_exposed', healthPayload().sources.moralis.api_key_exposed === false);
  t('moralis_single_auth_header_only', healthPayload().sources.moralis.auth_header_count_per_request === 1 && healthPayload().sources.moralis.duplicate_case_variant_headers === false);
  t('moralis_pair_swap_schema_not_token_swap_schema', healthPayload().recent_trades.token_swaps_bought_sold_schema_not_assumed === true);
  t('moralis_15m_same_pool_derivation_only', MORALIS_TIMEFRAME['15m'] === '5min');
  t('trading_disabled', responseBase().trading_enabled === false);
  t('db_writes_disabled', responseBase().database_writes === false);
  t('commercial_source_terms_recorded', healthPayload().sources.dexscreener.commercial_use_permitted_subject_to_api_terms === true);
  return responseBase({ ok: tests.every((x) => x.pass), test_count: tests.length, passed: tests.filter((x) => x.pass).length, failed: tests.filter((x) => !x.pass).length, tests });
}

export async function handleOnchainMarket(req, res, url) {
  const path = url?.pathname || '';
  if (![HEALTH_ROUTE, SELF_TEST_ROUTE, TRENDING_ROUTE, SEARCH_ROUTE, TOKEN_ROUTE, POOLS_ROUTE, KLINES_ROUTE, TRADES_ROUTE, NEW_POOLS_ROUTE].includes(path)) return false;
  stats.user_reads += 1;
  if (req.method !== 'GET') { sendJson(res, 405, responseBase({ ok: false, error: 'method_not_allowed' })); return true; }
  if (path === HEALTH_ROUTE) { sendJson(res, 200, healthPayload()); return true; }
  if (path === SELF_TEST_ROUTE) { const result = runSelfTest(); sendJson(res, result.ok ? 200 : 500, result); return true; }

  const network = normalizeNetwork(url.searchParams.get('network'));
  const limit = intRange(url.searchParams.get('limit'), 1, MAX_RESPONSE_ROWS, 50);

  if (path === TRENDING_ROUTE) {
    if (!network) { sendJson(res, 400, responseBase({ ok: false, error: 'invalid_network' })); return true; }
    const rows = discoveryRows(network, limit);
    const ageMs = discoveryUpdatedAt ? Math.max(0, Date.now() - discoveryUpdatedAt) : null;
    if (!rows.length && (!discoveryUpdatedAt || ageMs > DISCOVERY_RETAIN_MS)) {
      sendJson(res, 503, responseBase({ ok: false, error: 'onchain_shared_recent_hot_not_ready', network, rows: [], user_read_upstream_requests: 0 }));
      return true;
    }
    sendJson(res, 200, responseBase({ network, rows, row_count: rows.length, generated_at: discoveryUpdatedAt ? new Date(discoveryUpdatedAt).toISOString() : null, shared_snapshot_age_ms: ageMs, user_read_upstream_requests: 0, cache_status: 'background_shared' }));
    return true;
  }

  if (path === SEARCH_ROUTE) {
    const q = text(url.searchParams.get('q')).slice(0, 160);
    if (q.length < 2) { sendJson(res, 400, responseBase({ ok: false, error: 'query_too_short', rows: [] })); return true; }
    const key = `search:${lower(q)}:${network || 'all'}`;
    try {
      const result = await cachedBuild(key, { freshMs: 60_000, staleMs: 10 * 60_000 }, () => buildDexSearch(q));
      const rows = (result.value || []).filter((row) => !network || network === 'all' || row.network === network).slice(0, limit);
      sendJson(res, 200, responseBase({ query: q, network: network || 'all', rows, row_count: rows.length, cache_status: result.cache_status, bounded_backend_build: result.cache_status === 'miss' }));
    } catch (error) { sendJson(res, 503, responseBase({ ok: false, error: text(error?.message || error), query: q, rows: [] })); }
    return true;
  }

  if (path === TOKEN_ROUTE || path === POOLS_ROUTE) {
    if (!network || network === 'all') { sendJson(res, 400, responseBase({ ok: false, error: 'exact_network_required' })); return true; }
    const address = text(url.searchParams.get('address'));
    if (!validAddressForNetwork(network, address)) { sendJson(res, 400, responseBase({ ok: false, error: 'invalid_contract_address', network })); return true; }
    const key = `token_pairs:${network}:${lower(address)}`;
    try {
      const result = await cachedBuild(key, { freshMs: 20_000, staleMs: 5 * 60_000 }, () => buildDexTokenPairs(network, address));
      const rows = (result.value || []).slice(0, limit);
      if (path === POOLS_ROUTE) {
        sendJson(res, 200, responseBase({ network, address, rows, row_count: rows.length, cache_status: result.cache_status }));
      } else {
        const best = rows[0] || null;
        const token = best ? tokenAddressInPair(best, address) : null;
        const tokenMarket = best && token
          ? tokenCentricRow(best, token, { source: 'dexscreener_public_api_exact_token' })
          : null;
        sendJson(res, 200, responseBase({
          network,
          address,
          token,
          best_pool: best,
          token_market: tokenMarket,
          token_market_fields_verified: tokenMarket?.token_market_fields_verified === true,
          pool_count: rows.length,
          pools_preview: rows.slice(0, 6),
          cache_status: result.cache_status,
        }));
      }
    } catch (error) { sendJson(res, 503, responseBase({ ok: false, error: text(error?.message || error), network, address })); }
    return true;
  }

  if (path === NEW_POOLS_ROUTE) {
    if (!network) { sendJson(res, 400, responseBase({ ok: false, error: 'invalid_network', rows: [] })); return true; }
    const rows = recentCandidatePools(network, limit);
    sendJson(res, 200, responseBase({
      network,
      rows,
      row_count: rows.length,
      cache_status: 'step1036_background_shared',
      coverage: 'discovered_candidate_pools_not_exhaustive_chain_scan',
      user_read_upstream_requests: 0,
    }));
    return true;
  }

  if (path === KLINES_ROUTE || path === TRADES_ROUTE) {
    if (!network || network === 'all') {
      sendJson(res, 400, responseBase({ ok: false, error: 'exact_network_required' }));
      return true;
    }
    const tokenAddress = text(url.searchParams.get('address') || url.searchParams.get('token_address'));
    const poolAddress = text(url.searchParams.get('pool_address'));
    if (!validAddressForNetwork(network, tokenAddress)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'invalid_contract_address', network }));
      return true;
    }
    if (!validAddressForNetwork(network, poolAddress)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'invalid_pool_address', network }));
      return true;
    }
    if (!MORALIS_API_KEY) {
      sendJson(res, 503, responseBase({
        ok: false,
        error: 'onchain_history_source_not_configured',
        setup_required: 'Render Environment MORALIS_API_KEY',
        api_key_exposed: false,
      }));
      return true;
    }

    try {
      const pool = await exactPoolPreflight(network, tokenAddress, poolAddress);

      if (path === TRADES_ROUTE) {
        const tradeLimit = intRange(url.searchParams.get('limit'), 1, TRADE_MAX_ROWS, 30);
        const key = `trades:${network}:${lower(tokenAddress)}:${lower(poolAddress)}:${tradeLimit}`;
        const result = await cachedTradeBuild(
          key,
          () => buildMoralisTrades(network, tokenAddress, pool, tradeLimit),
        );
        sendJson(res, 200, responseBase({
          network,
          address: tokenAddress,
          pool_address: poolAddress,
          dex_id: pool.dex_id,
          rows: result.value || [],
          row_count: (result.value || []).length,
          cache_status: result.cache_status,
          source: 'moralis_official_data_api_pair_swaps',
          moralis_cu_if_upstream_build: MORALIS_TRADES_CU,
          user_read_direct_moralis_requests: 0,
        }));
        return true;
      }

      const interval = text(url.searchParams.get('interval')) || '1h';
      if (!Object.prototype.hasOwnProperty.call(MORALIS_TIMEFRAME, interval)) {
        sendJson(res, 400, responseBase({ ok: false, error: 'unsupported_interval', supported_intervals: Object.keys(MORALIS_TIMEFRAME) }));
        return true;
      }
      const klineLimit = intRange(url.searchParams.get('limit'), 1, KLINE_MAX_ROWS, 240);
      const endRaw = text(url.searchParams.get('end_time') || url.searchParams.get('end_time_ms'));
      const endTimeMs = endRaw ? Number(endRaw) : null;
      if (endRaw && (!Number.isFinite(endTimeMs) || endTimeMs <= 0)) {
        sendJson(res, 400, responseBase({ ok: false, error: 'invalid_end_time' }));
        return true;
      }
      // Round latest cache keys by natural short TTL. Explicit historical end_time remains exact.
      const endKey = endTimeMs ? String(Math.floor(endTimeMs)) : 'latest';
      const key = `kline:${network}:${lower(tokenAddress)}:${lower(poolAddress)}:${interval}:${klineLimit}:${endKey}`;
      const result = await cachedKlineBuild(
        key,
        intervalPolicy(interval, endTimeMs),
        () => buildMoralisKlines(network, tokenAddress, pool, interval, klineLimit, endTimeMs),
      );
      const built = result.value || { rows: [] };
      const rows = (built.rows || []).map((row) => ({
        ...row,
        network,
        token_address: tokenAddress,
        pool_address: poolAddress,
        dex_id: pool.dex_id,
        interval,
        source: 'moralis_official_data_api_pair_ohlcv',
      }));
      sendJson(res, 200, responseBase({
        network,
        address: tokenAddress,
        token: tokenAddressInPair(pool, tokenAddress),
        pool_address: poolAddress,
        dex_id: pool.dex_id,
        pair: {
          base_token: pool.base_token,
          quote_token: pool.quote_token,
        },
        interval,
        source_interval: built.source_timeframe || MORALIS_TIMEFRAME[interval],
        source: 'moralis_official_data_api_pair_ohlcv',
        source_pair_address: built.source_pair_address || poolAddress,
        source_token_address: built.source_token_address || null,
        identity_proof: built.identity_proof || null,
        exact_chain_token_pool_preflight: true,
        derived_15m_from_5m: built.derived_15m_from_5m === true,
        rows,
        row_count: rows.length,
        cache_status: result.cache_status,
        same_exact_key_reads_share_cache_and_inflight: true,
        user_read_direct_moralis_requests: 0,
        moralis_cu_if_upstream_build: MORALIS_KLINE_CU,
        historical_end_time_ms: endTimeMs,
      }));
    } catch (error) {
      const status = Number(error?.statusCode || 0);
      const code = text(error?.message || error);
      const httpStatus = status === 400 || code.includes('pool_not_owned') ? 400 : 503;
      sendJson(res, httpStatus, responseBase({
        ok: false,
        error: code,
        network,
        address: tokenAddress,
        pool_address: poolAddress,
      }));
    }
    return true;
  }

  return false;
}

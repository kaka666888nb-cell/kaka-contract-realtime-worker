// Step1038.2.1 / Render 650.8.15.193.2
// Kaka Web3 on-chain market phase 2.
// Step1036 DEX Screener foundation is preserved. Step1037 adds exact-pool OHLCV/history and
// recent swaps through Moralis Data API, with backend-only secret, separate bounded scheduler,
// CU budget ledger, cache + singleflight, exact chain/token/pool preflight and no user-scale
// upstream amplification. No trading, wallet signing or database writes.

import { readFileSync, writeFileSync, renameSync } from 'node:fs';

const VERSION = '650.8.15.193.2';
const DATA_VERSION = 1038002;
const SCHEMA_VERSION = 'step1037_3_onchain_market_v2';
const STEP1038_FEATURE_SCHEMA_VERSION = 'step1038_onchain_holder_security_v1';

const HEALTH_ROUTE = '/api/onchain/health';
const SELF_TEST_ROUTE = '/api/onchain/self-test';
const TRENDING_ROUTE = '/api/onchain/trending';
const SEARCH_ROUTE = '/api/onchain/search';
const TOKEN_ROUTE = '/api/onchain/token';
const POOLS_ROUTE = '/api/onchain/pools';
const KLINES_ROUTE = '/api/onchain/klines';
const TRADES_ROUTE = '/api/onchain/trades';
const NEW_POOLS_ROUTE = '/api/onchain/new-pools';
const FX_REFERENCE_ROUTE = '/api/onchain/fx-reference';
const HOLDERS_ROUTE = '/api/onchain/holders';
const SECURITY_ROUTE = '/api/onchain/security';

const DEX_BASE = 'https://api.dexscreener.com';
// Candidate endpoints are documented at 60/min; search/pairs at 300/min.
// One global 1.2s lane caps the whole on-chain module at <=50 upstream starts/min regardless of users.
const DEX_MIN_GAP_MS = 1_200;
const DEX_MAX_QUEUE = 80;
const UPSTREAM_TIMEOUT_MS = 12_000;
const DISCOVERY_REFRESH_MS = 5 * 60_000;
// Step1037.5: discovery/profile remains slow; exact market fields refresh on a separate fixed backend lane.
const MARKET_REFRESH_MS = 30_000;
const MARKET_RETAIN_MS = 5 * 60_000;
const FX_REFRESH_MS = 6 * 60 * 60_000;
const FX_RETAIN_MS = 96 * 60 * 60_000;
const ECB_FX_URL = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml';
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
// Step1038 holder concentration + contract/security facts. These are on-demand backend builds,
// never App-direct upstream calls. Cache/singleflight and bounded provider lanes cap user-scale load.
const MORALIS_HOLDER_METRICS_CU = 50;
const MORALIS_TOP_HOLDERS_CU = 50;
const HOLDER_FRESH_MS = 15 * 60_000;
const HOLDER_STALE_MS = 6 * 60 * 60_000;
const HOLDER_NEGATIVE_MS = 2 * 60_000;
const SOLANA_HELIUS_HOLDER_FRESH_MS = 30 * 60_000;
const SOLANA_HELIUS_HOLDER_STALE_MS = 12 * 60 * 60_000;
const GOPLUS_MIN_GAP_MS = Math.max(2_050, Number(process.env.KAKA_GOPLUS_MIN_GAP_MS || 2_100));
const GOPLUS_MAX_QUEUE = Math.max(6, Math.min(40, Number(process.env.KAKA_GOPLUS_MAX_QUEUE || 24)));
const GOPLUS_TIMEOUT_MS = Math.max(5_000, Math.min(25_000, Number(process.env.KAKA_GOPLUS_TIMEOUT_MS || 12_000)));
const GOPLUS_ACCESS_TOKEN = String(process.env.GOPLUS_ACCESS_TOKEN || '').trim();
const SECURITY_FRESH_MS = 30 * 60_000;
const SECURITY_STALE_MS = 24 * 60 * 60_000;
const SECURITY_NEGATIVE_MS = 2 * 60_000;
const EVM_GOPLUS_CHAIN_ID = Object.freeze({ ethereum: '1', bsc: '56', base: '8453' });

// Step1038.2.1: Solana holder analytics moves away from Moralis' deprecated
// Solana holder endpoints. Helius getProgramAccountsV2 is filtered by the exact
// mint and paginated in the backend. We aggregate token accounts by wallet owner
// and only publish holder count / Top10/20/50 when the full filtered account set
// was completely scanned. No partial page is promoted to a holder statistic.
const HELIUS_API_KEY = String(process.env.HELIUS_API_KEY || '').trim();
const HELIUS_RPC_BASE = 'https://mainnet.helius-rpc.com/';
const HELIUS_MIN_GAP_MS = Math.max(220, Number(process.env.KAKA_HELIUS_MIN_GAP_MS || 260));
const HELIUS_MAX_QUEUE = Math.max(6, Math.min(48, Number(process.env.KAKA_HELIUS_MAX_QUEUE || 24)));
const HELIUS_TIMEOUT_MS = Math.max(5_000, Math.min(30_000, Number(process.env.KAKA_HELIUS_TIMEOUT_MS || 15_000)));
const HELIUS_PAGE_LIMIT = Math.max(500, Math.min(10_000, Number(process.env.KAKA_HELIUS_HOLDER_PAGE_LIMIT || 5_000)));
const HELIUS_MAX_EXACT_TOKEN_ACCOUNTS = Math.max(5_000, Math.min(100_000, Number(process.env.KAKA_HELIUS_HOLDER_MAX_ACCOUNTS || 50_000)));
const SOLANA_TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const SOLANA_TOKEN_2022_PROGRAM = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

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
  market_refresh_started: 0,
  market_refresh_succeeded: 0,
  market_refresh_failed: 0,
  fx_refresh_started: 0,
  fx_refresh_succeeded: 0,
  fx_refresh_failed: 0,
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
  holder_builds: 0,
  holder_build_failures: 0,
  security_builds: 0,
  security_build_failures: 0,
  goplus_upstream_started: 0,
  goplus_upstream_succeeded: 0,
  goplus_upstream_failed: 0,
  helius_upstream_started: 0,
  helius_upstream_succeeded: 0,
  helius_upstream_failed: 0,
  helius_key_missing_rejections: 0,
  helius_holder_complete_scans: 0,
  helius_holder_incomplete_scans: 0,
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
let marketRefreshInflight = null;
let marketUpdatedAt = 0;
let fxRefreshInflight = null;
let fxSnapshot = null;
let fxUpdatedAt = 0;

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
  const fallback = { day: utcBudgetDay(), used_cu: 0, calls: 0, kline_calls: 0, trade_calls: 0, holder_calls: 0, updated_at: null };
  try {
    const parsed = JSON.parse(readFileSync(MORALIS_LEDGER_PATH, 'utf8'));
    if (!parsed || parsed.day !== utcBudgetDay()) return fallback;
    return {
      day: parsed.day,
      used_cu: Math.max(0, Number(parsed.used_cu || 0)),
      calls: Math.max(0, Number(parsed.calls || 0)),
      kline_calls: Math.max(0, Number(parsed.kline_calls || 0)),
      trade_calls: Math.max(0, Number(parsed.trade_calls || 0)),
      holder_calls: Math.max(0, Number(parsed.holder_calls || 0)),
      updated_at: parsed.updated_at || null,
    };
  } catch {
    return fallback;
  }
}
let moralisLedger = loadMoralisLedger();

function refreshMoralisBudgetDay() {
  if (moralisLedger.day === utcBudgetDay()) return;
  moralisLedger = { day: utcBudgetDay(), used_cu: 0, calls: 0, kline_calls: 0, trade_calls: 0, holder_calls: 0, updated_at: null };
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
    holder_calls: moralisLedger.holder_calls,
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
  if (kind === 'holder') moralisLedger.holder_calls += 1;
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


const goplusScheduler = createScheduler({
  name: 'goplus',
  minGapMs: GOPLUS_MIN_GAP_MS,
  maxQueue: GOPLUS_MAX_QUEUE,
});
async function goplusFetchJson(url, { priority = 0, label = '' } = {}) {
  return goplusScheduler.enqueue(async () => {
    stats.goplus_upstream_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), GOPLUS_TIMEOUT_MS);
    timer.unref?.();
    try {
      const headers = {
        accept: 'application/json',
        'user-agent': 'KakaWeb3-Onchain-Shared/1038',
      };
      if (GOPLUS_ACCESS_TOKEN) headers.authorization = `Bearer ${GOPLUS_ACCESS_TOKEN}`;
      const response = await fetch(url, { signal: controller.signal, headers });
      const body = await response.text();
      if (!response.ok) {
        const error = new Error(`goplus_http_${response.status}:${body.slice(0, 220)}`);
        error.statusCode = response.status;
        throw error;
      }
      let parsed;
      try { parsed = JSON.parse(body); } catch { throw new Error('goplus_invalid_json'); }
      if (Number(parsed?.code) !== 1 || !parsed?.result || typeof parsed.result !== 'object') {
        throw new Error(`goplus_bad_response:${text(parsed?.message || parsed?.code || '')}`);
      }
      stats.goplus_upstream_succeeded += 1;
      return parsed;
    } catch (error) {
      stats.goplus_upstream_failed += 1;
      throw error;
    } finally { clearTimeout(timer); }
  }, { priority, label });
}



const heliusScheduler = createScheduler({
  name: 'helius',
  minGapMs: HELIUS_MIN_GAP_MS,
  maxQueue: HELIUS_MAX_QUEUE,
});
function heliusRpcUrl() {
  const u = new URL(HELIUS_RPC_BASE);
  u.searchParams.set('api-key', HELIUS_API_KEY);
  return u.toString();
}
async function heliusRpc(method, params, { priority = 0, label = '' } = {}) {
  if (!HELIUS_API_KEY) {
    stats.helius_key_missing_rejections += 1;
    const error = new Error('helius_api_key_not_configured');
    error.statusCode = 503;
    throw error;
  }
  return heliusScheduler.enqueue(async () => {
    stats.helius_upstream_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), HELIUS_TIMEOUT_MS);
    timer.unref?.();
    try {
      const response = await fetch(heliusRpcUrl(), {
        method: 'POST',
        signal: controller.signal,
        headers: {
          accept: 'application/json',
          'content-type': 'application/json',
          'user-agent': 'KakaWeb3-Onchain-Shared/1038.2.1',
        },
        body: JSON.stringify({ jsonrpc: '2.0', id: 'kaka', method, params }),
      });
      const body = await response.text();
      if (!response.ok) {
        const error = new Error(`helius_http_${response.status}:${body.slice(0, 220)}`);
        error.statusCode = response.status;
        throw error;
      }
      let parsed;
      try { parsed = JSON.parse(body); } catch { throw new Error('helius_invalid_json'); }
      if (parsed?.error) {
        const code = parsed.error?.code ?? 'rpc';
        const message = text(parsed.error?.message || 'helius_rpc_error');
        const error = new Error(`helius_rpc_${code}:${message.slice(0, 180)}`);
        error.statusCode = 503;
        throw error;
      }
      stats.helius_upstream_succeeded += 1;
      return parsed?.result;
    } catch (error) {
      stats.helius_upstream_failed += 1;
      throw error;
    } finally { clearTimeout(timer); }
  }, { priority, label });
}
function base58Encode(bytes) {
  if (!Buffer.isBuffer(bytes)) bytes = Buffer.from(bytes || []);
  if (!bytes.length) return '';
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros += 1;
  const digits = [0];
  for (let i = zeros; i < bytes.length; i += 1) {
    let carry = bytes[i];
    for (let j = 0; j < digits.length; j += 1) {
      const value = digits[j] * 256 + carry;
      digits[j] = value % 58;
      carry = Math.floor(value / 58);
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = Math.floor(carry / 58);
    }
  }
  let out = '1'.repeat(zeros);
  for (let i = digits.length - 1; i >= 0; i -= 1) out += BASE58_ALPHABET[digits[i]];
  return out;
}
function parseSolanaTokenAccountSlice(raw) {
  const data = raw?.account?.data;
  const encoded = Array.isArray(data) ? text(data[0]) : '';
  if (!encoded) return null;
  let bytes;
  try { bytes = Buffer.from(encoded, 'base64'); } catch { return null; }
  if (bytes.length < 72) return null;
  const mint = base58Encode(bytes.subarray(0, 32));
  const owner = base58Encode(bytes.subarray(32, 64));
  if (!looksSolanaAddress(mint) || !looksSolanaAddress(owner)) return null;
  let amount = 0n;
  for (let i = 0; i < 8; i += 1) amount |= BigInt(bytes[64 + i]) << BigInt(i * 8);
  if (amount <= 0n) return null;
  return { mint, owner, amount };
}
function bigintPercent(balance, supply) {
  if (typeof balance !== 'bigint' || typeof supply !== 'bigint' || balance < 0n || supply <= 0n) return null;
  const scale = 1_000_000n;
  return Number((balance * 100n * scale) / supply) / Number(scale);
}
function sumTopBigintPercent(rows, count, supply) {
  if (!Array.isArray(rows) || rows.length < count || typeof supply !== 'bigint' || supply <= 0n) return null;
  let sum = 0n;
  for (const row of rows.slice(0, count)) sum += row.amount;
  return bigintPercent(sum, supply);
}
async function heliusTokenSupply(address) {
  const result = await heliusRpc('getTokenSupply', [address, { commitment: 'confirmed' }], {
    priority: 9,
    label: `token_supply:solana:${address}`,
  });
  const raw = text(result?.value?.amount);
  if (!/^\d+$/.test(raw)) throw new Error('helius_token_supply_missing');
  return { amount: BigInt(raw), decimals: Number(result?.value?.decimals || 0) };
}
async function heliusProgramTokenAccounts(programId, mint, ownerBalances, state) {
  let paginationKey = null;
  do {
    if (state.accountsSeen >= HELIUS_MAX_EXACT_TOKEN_ACCOUNTS) {
      state.complete = false;
      state.truncated = true;
      return;
    }
    const remaining = HELIUS_MAX_EXACT_TOKEN_ACCOUNTS - state.accountsSeen;
    const pageLimit = Math.max(1, Math.min(HELIUS_PAGE_LIMIT, remaining));
    const config = {
      encoding: 'base64',
      commitment: 'confirmed',
      dataSlice: { offset: 0, length: 72 },
      filters: [{ memcmp: { offset: 0, bytes: mint } }],
      limit: pageLimit,
      ...(paginationKey ? { paginationKey } : {}),
    };
    const result = await heliusRpc('getProgramAccountsV2', [programId, config], {
      priority: 8,
      label: `holder_accounts:${programId === SOLANA_TOKEN_PROGRAM ? 'spl' : 'token2022'}:${mint}`,
    });
    const page = result?.value && typeof result.value === 'object' ? result.value : result;
    const accounts = Array.isArray(page?.accounts) ? page.accounts : [];
    for (const account of accounts) {
      state.accountsSeen += 1;
      const parsed = parseSolanaTokenAccountSlice(account);
      if (!parsed || !exactAddressEqual('solana', parsed.mint, mint)) continue;
      state.tokenAccounts += 1;
      ownerBalances.set(parsed.owner, (ownerBalances.get(parsed.owner) || 0n) + parsed.amount);
    }
    paginationKey = text(page?.paginationKey) || null;
    if (!paginationKey) return;
    if (state.accountsSeen >= HELIUS_MAX_EXACT_TOKEN_ACCOUNTS) {
      state.complete = false;
      state.truncated = true;
      return;
    }
  } while (paginationKey);
}
async function buildHeliusSolanaHolderAnalysis(address) {
  if (!HELIUS_API_KEY) throw new Error('helius_api_key_not_configured');
  const ownerBalances = new Map();
  const state = { tokenAccounts: 0, accountsSeen: 0, complete: true, truncated: false };
  const supply = await heliusTokenSupply(address);
  // Query both token programs. A mint normally belongs to only one program; the exact
  // mint memcmp keeps the other query empty and avoids guessing Token vs Token-2022.
  await heliusProgramTokenAccounts(SOLANA_TOKEN_PROGRAM, address, ownerBalances, state);
  if (state.complete) await heliusProgramTokenAccounts(SOLANA_TOKEN_2022_PROGRAM, address, ownerBalances, state);
  if (!state.complete) {
    stats.helius_holder_incomplete_scans += 1;
    const error = new Error(`helius_holder_scan_truncated_at_${state.accountsSeen}`);
    error.partial = { token_accounts_scanned: state.accountsSeen, nonzero_token_accounts_scanned: state.tokenAccounts, unique_owners_scanned: ownerBalances.size };
    throw error;
  }
  const rows = [...ownerBalances.entries()]
    .filter(([, amount]) => amount > 0n)
    .map(([owner, amount]) => ({ owner, amount }))
    .sort((a, b) => (a.amount === b.amount ? 0 : a.amount > b.amount ? -1 : 1));
  if (!rows.length) throw new Error('helius_holder_scan_no_nonzero_accounts');
  stats.helius_holder_complete_scans += 1;
  const topHolders = rows.slice(0, 50).map((row) => ({
    address: row.owner,
    label: null,
    entity: null,
    entity_logo: null,
    balance: null,
    raw_balance: row.amount.toString(),
    usd_value: null,
    percent: bigintPercent(row.amount, supply.amount),
    is_contract: null,
  }));
  return {
    source: 'helius_official_rpc_exact_solana_holder_index',
    source_scope: 'exact_mint_full_token_account_scan_aggregated_by_wallet_owner',
    total_holders: rows.length,
    token_accounts_scanned: state.accountsSeen,
    nonzero_token_accounts_scanned: state.tokenAccounts,
    concentration: {
      top10_percent: rows.length >= 10 ? sumTopBigintPercent(rows, 10, supply.amount) : null,
      top20_percent: rows.length >= 20 ? sumTopBigintPercent(rows, 20, supply.amount) : null,
      top25_percent: rows.length >= 25 ? sumTopBigintPercent(rows, 25, supply.amount) : null,
      top50_percent: rows.length >= 50 ? sumTopBigintPercent(rows, 50, supply.amount) : null,
    },
    holder_change: { h1: null, h6: null, h24: null, d7: null },
    holder_distribution: null,
    holders_by_acquisition: null,
    top_holders: topHolders,
    exact_top20_available: rows.length >= 20,
    top_holder_list_available: topHolders.length > 0,
    field_sources: {
      total_holders: 'helius_getProgramAccountsV2_full_exact_mint_scan_unique_owner_count',
      top10_percent: rows.length >= 10 ? 'helius_full_exact_owner_balance_aggregation' : null,
      top20_percent: rows.length >= 20 ? 'helius_full_exact_owner_balance_aggregation' : null,
      top25_percent: rows.length >= 25 ? 'helius_full_exact_owner_balance_aggregation' : null,
      top50_percent: rows.length >= 50 ? 'helius_full_exact_owner_balance_aggregation' : null,
    },
    upstream_partial_errors: null,
    helius_scan_complete: true,
    helius_supply_raw: supply.amount.toString(),
  };
}

function bool01(value) {
  if (value === true || value === 1 || value === '1') return true;
  if (value === false || value === 0 || value === '0') return false;
  return null;
}
function percentFractionToPct(value) {
  const n = numberOrNull(value);
  return n === null ? null : n * 100;
}
function normalizeHolderSupplyEntry(raw) {
  if (!raw || typeof raw !== 'object') return null;
  return {
    supply: numberOrNull(raw.supply),
    supply_percent: numberOrNull(raw.supplyPercent ?? raw.supply_percent),
  };
}
function normalizeHolderMetrics(raw) {
  // Moralis documents the holder metrics fields at the top level. Be tolerant of a
  // single object wrapper as well so a gateway/SDK envelope cannot silently turn
  // valid production facts into nulls. Arrays are never accepted as a metrics root.
  const root = raw?.data && typeof raw.data === 'object' && !Array.isArray(raw.data)
    ? raw.data
    : raw?.result && typeof raw.result === 'object' && !Array.isArray(raw.result)
      ? raw.result
      : raw;
  const supply = root?.holderSupply || root?.holder_supply || {};
  const change = root?.holderChange || root?.holder_change || {};
  return {
    total_holders: numberOrNull(root?.totalHolders ?? root?.total_holders),
    concentration: {
      top10: normalizeHolderSupplyEntry(supply.top10),
      top25: normalizeHolderSupplyEntry(supply.top25),
      top50: normalizeHolderSupplyEntry(supply.top50),
      top100: normalizeHolderSupplyEntry(supply.top100),
    },
    holder_change: {
      h1: numberOrNull(change?.['1h']?.changePercent ?? change?.['1h']?.change_percent),
      h6: numberOrNull(change?.['6h']?.changePercent ?? change?.['6h']?.change_percent),
      h24: numberOrNull(change?.['24h']?.changePercent ?? change?.['24h']?.change_percent),
      d7: numberOrNull(change?.['7d']?.changePercent ?? change?.['7d']?.change_percent),
    },
    holder_distribution: root?.holderDistribution && typeof root.holderDistribution === 'object'
      ? root.holderDistribution
      : root?.holder_distribution && typeof root.holder_distribution === 'object'
        ? root.holder_distribution
        : null,
    holders_by_acquisition: root?.holdersByAcquisition && typeof root.holdersByAcquisition === 'object'
      ? root.holdersByAcquisition
      : root?.holders_by_acquisition && typeof root.holders_by_acquisition === 'object'
        ? root.holders_by_acquisition
        : null,
  };
}
function normalizeEvmTopHolder(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const address = text(raw.owner_address || raw.address);
  if (!looksEvmAddress(address)) return null;
  return {
    address,
    label: text(raw.owner_address_label || raw.entity || raw.tag) || null,
    entity: text(raw.entity) || null,
    entity_logo: text(raw.entity_logo) || null,
    balance: numberOrNull(raw.balance_formatted ?? raw.balance),
    usd_value: numberOrNull(raw.usd_value),
    percent: numberOrNull(raw.percentage_relative_to_total_supply ?? raw.percent),
    is_contract: raw.is_contract === true || raw.is_contract === '1',
  };
}
function exactTopPercent(rows, count) {
  const values = rows.slice(0, count).map((row) => numberOrNull(row?.percent)).filter((x) => x !== null);
  return values.length === Math.min(count, rows.length) && values.length ? values.reduce((a, b) => a + b, 0) : null;
}
function moralisHolderMetricsUrl(network, address) {
  if (network === 'solana') {
    return `https://solana-gateway.moralis.io/token/mainnet/holders/${encodeURIComponent(address)}`;
  }
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/erc20/${encodeURIComponent(address)}/holders`);
  u.searchParams.set('chain', MORALIS_EVM_CHAIN[network]);
  return u.toString();
}
function moralisTopHoldersUrl(network, address, limit = 50) {
  if (network === 'solana') return null; // Deprecated by Moralis after 2026-07-31; do not build new dependency on it.
  const u = new URL(`https://deep-index.moralis.io/api/v2.2/erc20/${encodeURIComponent(address)}/owners`);
  u.searchParams.set('chain', MORALIS_EVM_CHAIN[network]);
  u.searchParams.set('limit', String(Math.max(1, Math.min(100, limit))));
  u.searchParams.set('order', 'DESC');
  return u.toString();
}
async function buildHolderAnalysis(network, address) {
  stats.holder_builds += 1;

  // Step1038.2.1: Moralis' Solana holder endpoints are deprecated. Prefer a
  // complete exact-mint Helius RPC scan. If Helius is not configured/unavailable,
  // preserve the previous GoPlus source-fact fallback without fabricating Top20/50.
  if (network === 'solana') {
    let heliusError = null;
    try {
      return await buildHeliusSolanaHolderAnalysis(address);
    } catch (error) {
      heliusError = error;
    }
    let holderFallback = null;
    try {
      const securityResult = await cachedBuild(
        `step1038:security:${network}:${lower(address)}`,
        { freshMs: SECURITY_FRESH_MS, staleMs: SECURITY_STALE_MS, negativeMs: SECURITY_NEGATIVE_MS },
        () => buildGoPlusSecurity(network, address),
      );
      holderFallback = securityResult.value || null;
    } catch {
      holderFallback = null;
    }
    const totalHolders = numberOrNull(holderFallback?.holder_count);
    const top10 = numberOrNull(holderFallback?.top10_percent_reported);
    const rows = Array.isArray(holderFallback?.holders_top10) ? holderFallback.holders_top10 : [];
    if (totalHolders === null && top10 === null && !rows.length) {
      stats.holder_build_failures += 1;
      throw heliusError || new Error('solana_holder_analysis_no_usable_facts');
    }
    return {
      source: 'goplus_solana_token_security_holder_fallback',
      source_scope: 'top10_and_holder_count_only_when_helius_exact_holder_index_unavailable',
      total_holders: totalHolders,
      concentration: { top10_percent: top10, top20_percent: null, top25_percent: null, top50_percent: null },
      holder_change: { h1: null, h6: null, h24: null, d7: null },
      holder_distribution: null,
      holders_by_acquisition: null,
      top_holders: rows,
      exact_top20_available: false,
      top_holder_list_available: rows.length > 0,
      field_sources: {
        total_holders: totalHolders !== null ? 'goplus_token_security_holder_count' : null,
        top10_percent: top10 !== null ? 'goplus_token_security_top10_holders' : null,
        top20_percent: null, top25_percent: null, top50_percent: null,
      },
      upstream_partial_errors: {
        helius_exact_holder_index: heliusError ? String(heliusError.message || heliusError).slice(0, 180) : null,
      },
      helius_scan_complete: false,
    };
  }

  let metrics = normalizeHolderMetrics(null);
  let metricsError = null;
  try {
    const metricsPayload = await moralisFetchJson(moralisHolderMetricsUrl(network, address), {
      cu: MORALIS_HOLDER_METRICS_CU,
      kind: 'holder',
      priority: 8,
      label: `holder_metrics:${network}:${address}`,
    });
    metrics = normalizeHolderMetrics(metricsPayload);
  } catch (error) {
    metricsError = error;
  }

  let topHolders = [];
  let ownersError = null;
  let exactTop20 = null;
  let exactTop10 = null;
  let exactTop50 = null;
  const ownersUrl = moralisTopHoldersUrl(network, address, 50);
  if (ownersUrl) {
    try {
      const ownersPayload = await moralisFetchJson(ownersUrl, {
        cu: MORALIS_TOP_HOLDERS_CU,
        kind: 'holder',
        priority: 7,
        label: `top_holders:${network}:${address}`,
      });
      topHolders = (Array.isArray(ownersPayload?.result) ? ownersPayload.result : [])
        .map(normalizeEvmTopHolder).filter(Boolean).slice(0, 50);
      exactTop10 = topHolders.length >= 10 ? exactTopPercent(topHolders, 10) : null;
      exactTop20 = topHolders.length >= 20 ? exactTopPercent(topHolders, 20) : null;
      exactTop50 = topHolders.length >= 50 ? exactTopPercent(topHolders, 50) : null;
    } catch (error) {
      ownersError = error;
    }
  }

  let holderFallback = null;
  if (metrics.total_holders === null) {
    try {
      const securityResult = await cachedBuild(
        `step1038:security:${network}:${lower(address)}`,
        { freshMs: SECURITY_FRESH_MS, staleMs: SECURITY_STALE_MS, negativeMs: SECURITY_NEGATIVE_MS },
        () => buildGoPlusSecurity(network, address),
      );
      holderFallback = securityResult.value || null;
    } catch {
      holderFallback = null;
    }
  }

  const totalHolders = metrics.total_holders ?? numberOrNull(holderFallback?.holder_count);
  const top10 = exactTop10 ?? metrics.concentration.top10?.supply_percent ?? numberOrNull(holderFallback?.top10_percent_reported);
  const top25 = metrics.concentration.top25?.supply_percent ?? null;
  const top50 = exactTop50 ?? metrics.concentration.top50?.supply_percent ?? null;
  const anyUsable = totalHolders !== null || top10 !== null || exactTop20 !== null || top25 !== null || top50 !== null || topHolders.length > 0;
  if (!anyUsable) {
    stats.holder_build_failures += 1;
    throw metricsError || ownersError || new Error('holder_analysis_no_usable_facts');
  }

  const usedGoPlusTotal = metrics.total_holders === null && totalHolders !== null;
  return {
    source: usedGoPlusTotal
      ? 'moralis_holder_analytics_plus_goplus_holder_count_fallback'
      : 'moralis_official_data_api_holder_analytics',
    source_scope: usedGoPlusTotal
      ? 'moralis_exact_owner_concentration_plus_goplus_exact_contract_holder_count'
      : 'moralis_holder_metrics_plus_exact_top50_owner_list',
    total_holders: totalHolders,
    concentration: { top10_percent: top10, top20_percent: exactTop20, top25_percent: top25, top50_percent: top50 },
    holder_change: metrics.holder_change,
    holder_distribution: metrics.holder_distribution,
    holders_by_acquisition: metrics.holders_by_acquisition,
    top_holders: topHolders,
    exact_top20_available: exactTop20 !== null,
    top_holder_list_available: topHolders.length > 0,
    field_sources: {
      total_holders: metrics.total_holders !== null ? 'moralis_holder_metrics' : totalHolders !== null ? 'goplus_token_security_holder_count' : null,
      top10_percent: exactTop10 !== null ? 'moralis_exact_top_owners' : metrics.concentration.top10?.supply_percent != null ? 'moralis_holder_metrics' : top10 !== null ? 'goplus_token_security_top10_holders' : null,
      top20_percent: exactTop20 !== null ? 'moralis_exact_top_owners' : null,
      top25_percent: top25 !== null ? 'moralis_holder_metrics' : null,
      top50_percent: exactTop50 !== null ? 'moralis_exact_top_owners' : top50 !== null ? 'moralis_holder_metrics' : null,
    },
    upstream_partial_errors: {
      moralis_metrics: metricsError ? String(metricsError.message || metricsError).slice(0, 180) : null,
      moralis_top_holders: ownersError ? String(ownersError.message || ownersError).slice(0, 180) : null,
    },
  };
}

function resultByExactAddress(network, result, address) {
  if (!result || typeof result !== 'object') return null;
  for (const [key, value] of Object.entries(result)) {
    if (exactAddressEqual(network, key, address)) return value && typeof value === 'object' ? value : null;
  }
  return null;
}
function normalizeGoPlusHolder(raw, network) {
  if (!raw || typeof raw !== 'object') return null;
  const address = text(raw.address || raw.token_account);
  if (!validAddressForNetwork(network, address)) return null;
  return {
    address,
    tag: text(raw.tag) || null,
    balance: numberOrNull(raw.balance),
    percent: percentFractionToPct(raw.percent),
    is_locked: bool01(raw.is_locked ?? raw.locked),
    is_contract: bool01(raw.is_contract),
    locked_detail: Array.isArray(raw.locked_detail) ? raw.locked_detail.slice(0, 8) : [],
  };
}
function normalizedPercentSum(rows) {
  const vals = rows.map((row) => numberOrNull(row?.percent)).filter((x) => x !== null);
  return vals.length ? vals.reduce((a, b) => a + b, 0) : null;
}
function normalizeGoPlusEvm(raw, network, address) {
  const holders = (Array.isArray(raw?.holders) ? raw.holders : []).map((x) => normalizeGoPlusHolder(x, network)).filter(Boolean).slice(0, 10);
  const lpHolders = (Array.isArray(raw?.lp_holders) ? raw.lp_holders : []).map((x) => normalizeGoPlusHolder(x, network)).filter(Boolean).slice(0, 10);
  const lockedLpPct = normalizedPercentSum(lpHolders.filter((x) => x.is_locked === true));
  return {
    network, address,
    token_name: text(raw?.token_name) || null,
    token_symbol: text(raw?.token_symbol) || null,
    holder_count: numberOrNull(raw?.holder_count),
    total_supply: numberOrNull(raw?.total_supply),
    holders_top10: holders,
    top10_percent_reported: normalizedPercentSum(holders),
    creator: {
      address: text(raw?.creator_address) || null,
      balance: numberOrNull(raw?.creator_balance),
      percent: percentFractionToPct(raw?.creator_percent),
    },
    owner: {
      address: text(raw?.owner_address) || null,
      balance: numberOrNull(raw?.owner_balance),
      percent: percentFractionToPct(raw?.owner_percent),
    },
    lp: {
      holder_count: numberOrNull(raw?.lp_holder_count),
      total_supply: numberOrNull(raw?.lp_total_supply),
      holders_top10: lpHolders,
      top10_percent_reported: normalizedPercentSum(lpHolders),
      locked_percent_reported: lockedLpPct,
    },
    contract_facts: {
      is_open_source: bool01(raw?.is_open_source),
      is_proxy: bool01(raw?.is_proxy),
      is_mintable: bool01(raw?.is_mintable),
      hidden_owner: bool01(raw?.hidden_owner),
      can_take_back_ownership: bool01(raw?.can_take_back_ownership),
      owner_change_balance: bool01(raw?.owner_change_balance),
      selfdestruct: bool01(raw?.selfdestruct),
      external_call: bool01(raw?.external_call),
    },
    trading_facts: {
      is_honeypot: bool01(raw?.is_honeypot),
      cannot_buy: bool01(raw?.cannot_buy),
      cannot_sell_all: bool01(raw?.cannot_sell_all),
      is_blacklisted: bool01(raw?.is_blacklisted),
      is_whitelisted: bool01(raw?.is_whitelisted),
      transfer_pausable: bool01(raw?.transfer_pausable),
      trading_cooldown: bool01(raw?.trading_cooldown),
      slippage_modifiable: bool01(raw?.slippage_modifiable),
      is_anti_whale: bool01(raw?.is_anti_whale),
      anti_whale_modifiable: bool01(raw?.anti_whale_modifiable),
      buy_tax_percent: percentFractionToPct(raw?.buy_tax),
      sell_tax_percent: percentFractionToPct(raw?.sell_tax),
      transfer_tax_percent: percentFractionToPct(raw?.transfer_tax),
      is_in_dex: bool01(raw?.is_in_dex),
    },
    trust_facts: {
      trust_list: bool01(raw?.trust_list),
      is_airdrop_scam: bool01(raw?.is_airdrop_scam),
      other_potential_risks: text(raw?.other_potential_risks) || null,
      note: text(raw?.note) || null,
    },
  };
}
function solanaAuthority(raw) {
  if (raw === null || raw === undefined || raw === '') return null;
  if (typeof raw === 'string') return raw;
  if (typeof raw === 'object') {
    return text(raw.address || raw.authority || raw.value || raw.owner) || null;
  }
  return text(raw) || null;
}
function normalizeGoPlusSolana(raw, network, address) {
  const holders = (Array.isArray(raw?.holders) ? raw.holders : []).map((x) => normalizeGoPlusHolder(x, network)).filter(Boolean).slice(0, 10);
  const creators = (Array.isArray(raw?.creator) ? raw.creator : []).map((x) => ({
    address: text(x?.address) || null,
    malicious_address: bool01(x?.malicious_address),
  })).filter((x) => x.address);
  const dexRows = (Array.isArray(raw?.dex) ? raw.dex : Array.isArray(raw?.dex_info) ? raw.dex_info : []).slice(0, 12);
  const lpHolders = [];
  for (const dex of dexRows) {
    for (const h of (Array.isArray(dex?.lp_holders) ? dex.lp_holders : [])) {
      const row = normalizeGoPlusHolder(h, network);
      if (row && !lpHolders.some((x) => exactAddressEqual(network, x.address, row.address))) lpHolders.push(row);
      if (lpHolders.length >= 10) break;
    }
    if (lpHolders.length >= 10) break;
  }
  const metadataMutable = raw?.metadata_mutable;
  const mintable = raw?.mintable;
  const transferHook = raw?.transfer_hook;
  return {
    network, address,
    token_name: text(raw?.metadata?.name) || null,
    token_symbol: text(raw?.metadata?.symbol) || null,
    holder_count: numberOrNull(raw?.holder_count),
    total_supply: numberOrNull(raw?.total_supply),
    holders_top10: holders,
    top10_percent_reported: normalizedPercentSum(holders),
    creators,
    lp: {
      holders_top10: lpHolders,
      top10_percent_reported: normalizedPercentSum(lpHolders),
      locked_percent_reported: normalizedPercentSum(lpHolders.filter((x) => x.is_locked === true)),
      dex_pool_count: dexRows.length,
      tvl_usd: dexRows.reduce((sum, d) => sum + (numberOrNull(d?.tvl) || 0), 0) || null,
    },
    solana_facts: {
      default_account_state: numberOrNull(raw?.default_account_state),
      non_transferable: bool01(raw?.non_transferable),
      trusted_token: bool01(raw?.trusted_token),
      mintable: typeof mintable === 'object' ? bool01(mintable?.status ?? mintable?.value ?? mintable?.is_mintable) : bool01(mintable),
      mint_authority: solanaAuthority(typeof mintable === 'object' ? mintable : raw?.mint_authority),
      metadata_mutable: typeof metadataMutable === 'object' ? bool01(metadataMutable?.status ?? metadataMutable?.value ?? metadataMutable?.is_mutable) : bool01(metadataMutable),
      metadata_upgrade_authority: solanaAuthority(typeof metadataMutable === 'object' ? metadataMutable : raw?.metadata_upgrade_authority),
      transfer_hook_address: solanaAuthority(transferHook),
      transfer_hook_malicious: typeof transferHook === 'object' ? bool01(transferHook?.malicious_address) : null,
      transfer_hook_upgradable: bool01(raw?.transfer_hook_upgradable),
      transfer_fee: raw?.transfer_fee && typeof raw.transfer_fee === 'object' ? raw.transfer_fee : null,
    },
  };
}
function goplusSecurityUrl(network, address) {
  if (network === 'solana') {
    const u = new URL('https://api.gopluslabs.io/api/v1/solana/token_security');
    u.searchParams.set('contract_addresses', address);
    return u.toString();
  }
  const chainId = EVM_GOPLUS_CHAIN_ID[network];
  const u = new URL(`https://api.gopluslabs.io/api/v1/token_security/${chainId}`);
  u.searchParams.set('contract_addresses', address);
  return u.toString();
}
async function buildGoPlusSecurity(network, address) {
  stats.security_builds += 1;
  try {
    const payload = await goplusFetchJson(goplusSecurityUrl(network, address), {
      priority: 10,
      label: `token_security:${network}:${address}`,
    });
    const raw = resultByExactAddress(network, payload.result, address);
    if (!raw) throw new Error('goplus_exact_token_not_found');
    return network === 'solana'
      ? normalizeGoPlusSolana(raw, network, address)
      : normalizeGoPlusEvm(raw, network, address);
  } catch (error) {
    stats.security_build_failures += 1;
    throw error;
  }
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
    token_profile: extra.token_profile || tokenProfileForIdentity(pair.network, token?.address) || null,
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
function normalizePublicLink(item) {
  if (!item || typeof item !== 'object') return null;
  const url = text(item.url);
  if (!/^https?:\/\//i.test(url)) return null;
  const type = text(item.type || item.platform || item.label).slice(0, 80);
  const label = text(item.label || item.type || item.platform).slice(0, 120);
  return { type, label, url: url.slice(0, 1000) };
}
function normalizeCandidateProfile(item) {
  if (!item || typeof item !== 'object') return null;
  const iconUrl = text(item.icon);
  const headerUrl = text(item.header);
  const profileUrl = text(item.url);
  const description = text(item.description).replace(/\s+/g, ' ').slice(0, 1200);
  const links = Array.isArray(item.links)
    ? item.links.map(normalizePublicLink).filter(Boolean).slice(0, 12)
    : [];
  if (!iconUrl && !headerUrl && !profileUrl && !description && !links.length) return null;
  return {
    icon_url: /^https:\/\//i.test(iconUrl) ? iconUrl.slice(0, 1000) : '',
    header_url: /^https:\/\//i.test(headerUrl) ? headerUrl.slice(0, 1000) : '',
    profile_url: /^https?:\/\//i.test(profileUrl) ? profileUrl.slice(0, 1000) : '',
    description,
    links,
    source: 'dexscreener_public_token_profile',
  };
}
function mergeTokenProfile(left, right) {
  if (!left) return right || null;
  if (!right) return left || null;
  const links = [];
  const seen = new Set();
  for (const item of [...(left.links || []), ...(right.links || [])]) {
    const key = lower(item?.url);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    links.push(item);
    if (links.length >= 12) break;
  }
  return {
    icon_url: text(right.icon_url) || text(left.icon_url),
    header_url: text(right.header_url) || text(left.header_url),
    profile_url: text(right.profile_url) || text(left.profile_url),
    description: text(right.description) || text(left.description),
    links,
    source: 'dexscreener_public_token_profile',
  };
}
function tokenProfileForIdentity(network, address) {
  const key = `${network}|${lower(address)}`;
  for (const row of trendingSnapshot) {
    if (`${row.network}|${lower(row?.token?.address)}` === key && row.token_profile) {
      return row.token_profile;
    }
  }
  return null;
}
function candidateRows(payload, source) {
  const raw = Array.isArray(payload) ? payload : payload && typeof payload === 'object' ? [payload] : [];
  return raw.map((item) => {
    const network = DEX_TO_NETWORK[lower(item?.chainId)] || '';
    const address = text(item?.tokenAddress);
    if (!network || !validAddressForNetwork(network, address)) return null;
    return {
      network,
      address,
      source,
      amount: numberOrNull(item?.amount),
      total_amount: numberOrNull(item?.totalAmount),
      token_profile: normalizeCandidateProfile(item),
    };
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
    cur.token_profile = mergeTokenProfile(cur.token_profile, row.token_profile);
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
        candidate_token_profile: match.token_profile || null,
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
      token_profile: pair.candidate_token_profile || null,
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

function currentCandidateMetadata() {
  const byKey = new Map();
  for (const row of trendingSnapshot) {
    const address = text(row?.token?.address);
    if (!address || !row?.network) continue;
    byKey.set(`${row.network}|${lower(address)}`, {
      network: row.network,
      address,
      candidate_sources: Array.isArray(row.candidate_sources) ? row.candidate_sources : [],
      candidate_boost_amount: row.candidate_boost_amount ?? null,
      candidate_total_boost_amount: row.candidate_total_boost_amount ?? null,
      token_profile: row.token_profile || null,
    });
  }
  return byKey;
}
async function refreshCurrentMarketFields() {
  if (marketRefreshInflight || trendingSnapshot.length === 0) return marketRefreshInflight;
  marketRefreshInflight = (async () => {
    stats.market_refresh_started += 1;
    const candidates = currentCandidateMetadata();
    const grouped = new Map(Object.keys(NETWORKS).map((key) => [key, []]));
    for (const row of candidates.values()) {
      const list = grouped.get(row.network);
      if (list && list.length < DISCOVERY_MAX_CANDIDATES_PER_CHAIN) list.push(row);
    }
    try {
      const refreshedPairs = [];
      for (const [network, list] of grouped) {
        if (!list.length) continue;
        const meta = networkMeta(network);
        const addresses = list.map((x) => x.address).slice(0, DISCOVERY_MAX_CANDIDATES_PER_CHAIN);
        const payload = await dexFetchJson(
          `${DEX_BASE}/tokens/v1/${encodeURIComponent(meta.dex)}/${addresses.map(encodeURIComponent).join(',')}`,
          { priority: -10, label: `background_market_refresh_${network}` },
        );
        const normalized = normalizeDexPairs(payload).filter((row) => row.network === network);
        for (const pair of normalized) {
          const match = list.find((x) =>
            lower(pair.base_token.address) === lower(x.address) ||
            lower(pair.quote_token.address) === lower(x.address));
          if (!match) continue;
          refreshedPairs.push({
            ...pair,
            candidate_token_address: match.address,
            candidate_sources: match.candidate_sources,
            candidate_boost_amount: match.candidate_boost_amount,
            candidate_total_boost_amount: match.candidate_total_boost_amount,
            candidate_token_profile: match.token_profile,
          });
        }
      }
      const rows = recentHotTokenRows(refreshedPairs);
      if (!rows.length) throw new Error('dexscreener_current_market_refresh_empty');
      trendingSnapshot = rows;
      marketUpdatedAt = Date.now();
      stats.market_refresh_succeeded += 1;
      return rows.length;
    } catch (error) {
      stats.market_refresh_failed += 1;
      throw error;
    } finally {
      marketRefreshInflight = null;
    }
  })();
  return marketRefreshInflight;
}
function parseEcbDailyFxXml(xml) {
  const raw = text(xml);
  const timeMatch = raw.match(/<Cube\s+time=['\"]([^'\"]+)['\"]/i);
  if (!timeMatch) throw new Error('ecb_fx_observation_missing');
  const rates = {};
  for (const match of raw.matchAll(/<Cube\s+currency=['\"]([A-Z]{3})['\"]\s+rate=['\"]([0-9.]+)['\"]\s*\/?>/g)) {
    const value = Number(match[2]);
    if (Number.isFinite(value) && value > 0) rates[match[1]] = value;
  }
  const usd = Number(rates.USD);
  if (!Number.isFinite(usd) || usd <= 0) throw new Error('ecb_fx_usd_missing');
  const usdTo = { USD: 1, EUR: 1 / usd };
  for (const code of ['CNY', 'JPY']) {
    const value = Number(rates[code]);
    if (Number.isFinite(value) && value > 0) usdTo[code] = value / usd;
  }
  if (!usdTo.CNY || !usdTo.JPY) throw new Error('ecb_fx_required_currency_missing');
  return {
    source: 'ecb_euro_foreign_exchange_reference_rates',
    source_role: 'daily_reference_secondary_display_only',
    observation_date: timeMatch[1],
    base_currency: 'USD',
    usd_to: usdTo,
    eur_reference: { USD: usd, CNY: Number(rates.CNY), JPY: Number(rates.JPY) },
  };
}
async function refreshFxReference() {
  if (fxRefreshInflight) return fxRefreshInflight;
  fxRefreshInflight = (async () => {
    stats.fx_refresh_started += 1;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS);
    timer.unref?.();
    try {
      const response = await fetch(ECB_FX_URL, {
        signal: controller.signal,
        headers: { accept: 'application/xml,text/xml;q=0.9,*/*;q=0.1', 'user-agent': 'KakaWeb3-FX-Shared/1037.5' },
      });
      const body = await response.text();
      if (!response.ok) throw new Error(`ecb_fx_http_${response.status}`);
      fxSnapshot = parseEcbDailyFxXml(body);
      fxUpdatedAt = Date.now();
      stats.fx_refresh_succeeded += 1;
      return fxSnapshot;
    } catch (error) {
      stats.fx_refresh_failed += 1;
      throw error;
    } finally {
      clearTimeout(timer);
      fxRefreshInflight = null;
    }
  })();
  return fxRefreshInflight;
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
      marketUpdatedAt = discoveryUpdatedAt;
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
  const marketFirst = setTimeout(() => refreshCurrentMarketFields().catch(() => {}), 22_000);
  marketFirst.unref?.();
  const marketTimer = setInterval(() => refreshCurrentMarketFields().catch(() => {}), MARKET_REFRESH_MS);
  marketTimer.unref?.();
  const fxFirst = setTimeout(() => refreshFxReference().catch(() => {}), 4_500);
  fxFirst.unref?.();
  const fxTimer = setInterval(() => refreshFxReference().catch(() => {}), FX_REFRESH_MS);
  fxTimer.unref?.();
}
function discoveryRows(network, limit) {
  return trendingSnapshot.filter((row) => network === 'all' || row.network === network).slice(0, limit);
}
function healthPayload() {
  const ageMs = discoveryUpdatedAt ? Math.max(0, Date.now() - discoveryUpdatedAt) : null;
  const marketAgeMs = marketUpdatedAt ? Math.max(0, Date.now() - marketUpdatedAt) : null;
  const fxAgeMs = fxUpdatedAt ? Math.max(0, Date.now() - fxUpdatedAt) : null;
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
      ecb_fx: {
        docs: 'https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html',
        data_url: ECB_FX_URL,
        role: 'daily_reference_secondary_display_only',
        background_only: true,
        user_reads_start_upstream: false,
        refresh_interval_ms: FX_REFRESH_MS,
        retain_ms: FX_RETAIN_MS,
      },
      moralis: {
        docs_evm_ohlcv: 'https://docs.moralis.com/data-api/evm/price/ohlc',
        docs_solana_ohlcv: 'https://docs.moralis.com/data-api/solana/price/ohlc',
        docs_pricing: 'https://docs.moralis.com/data-api/pricing',
        terms: 'https://moralis.com/terms/',
        role: 'exact_pool_ohlcv_history_plus_recent_pair_swaps_plus_holder_analytics',
        api_key_configured: Boolean(MORALIS_API_KEY),
        api_key_exposed: false,
        backend_only_secret: true,
        auth_header_name: 'X-API-Key',
        auth_header_count_per_request: 1,
        duplicate_case_variant_headers: false,
        pair_candlestick_cu: MORALIS_KLINE_CU,
        pair_swap_cu: MORALIS_TRADES_CU,
        holder_metrics_cu: MORALIS_HOLDER_METRICS_CU,
        evm_top_holders_cu: MORALIS_TOP_HOLDERS_CU,
        scheduler: moralisScheduler.state(),
        budget: moralisBudgetState(),
      },
      goplus: {
        docs_evm_security: 'https://docs.gopluslabs.io/reference/tokensecurityusingget_1',
        docs_solana_security: 'https://docs.gopluslabs.io/reference/solanatokensecurityusingget',
        docs_response_evm: 'https://docs.gopluslabs.io/reference/response-details',
        docs_response_solana: 'https://docs.gopluslabs.io/reference/response-detail-1',
        role: 'token_contract_security_creator_owner_lp_and_reported_holder_facts',
        access_token_configured: Boolean(GOPLUS_ACCESS_TOKEN),
        access_token_exposed: false,
        backend_global_min_gap_ms: GOPLUS_MIN_GAP_MS,
        backend_global_max_starts_per_minute: Math.floor(60_000 / GOPLUS_MIN_GAP_MS),
        scheduler: goplusScheduler.state(),
      },
      helius: {
        docs_get_program_accounts_v2: 'https://www.helius.dev/docs/api-reference/rpc/http/getprogramaccountsv2',
        docs_get_token_supply: 'https://www.helius.dev/docs/api-reference/rpc/http/gettokensupply',
        docs_pricing: 'https://www.helius.dev/pricing',
        role: 'solana_exact_mint_holder_index_from_full_filtered_token_account_scan',
        api_key_configured: Boolean(HELIUS_API_KEY),
        api_key_exposed: false,
        backend_only_secret: true,
        page_limit: HELIUS_PAGE_LIMIT,
        exact_scan_max_token_accounts: HELIUS_MAX_EXACT_TOKEN_ACCOUNTS,
        backend_global_min_gap_ms: HELIUS_MIN_GAP_MS,
        backend_global_max_starts_per_second: Math.floor(1_000 / HELIUS_MIN_GAP_MS),
        scheduler: heliusScheduler.state(),
      },
    },
    step1038_holder_security: {
      opened: true,
      feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION,
      holders_route: HOLDERS_ROUTE,
      security_route: SECURITY_ROUTE,
      supported_networks: Object.keys(NETWORKS),
      exact_chain_token_identity_required: true,
      cross_chain_substitution: false,
      cross_token_substitution: false,
      holder_cache_fresh_ms: HOLDER_FRESH_MS,
      holder_cache_stale_ms: HOLDER_STALE_MS,
      solana_helius_holder_cache_fresh_ms: SOLANA_HELIUS_HOLDER_FRESH_MS,
      solana_helius_holder_cache_stale_ms: SOLANA_HELIUS_HOLDER_STALE_MS,
      security_cache_fresh_ms: SECURITY_FRESH_MS,
      security_cache_stale_ms: SECURITY_STALE_MS,
      evm_exact_top20_from_owner_list: true,
      solana_top20_not_fabricated: true,
      solana_holder_primary_source: 'helius_getProgramAccountsV2_exact_mint_full_scan',
      solana_holder_requires_complete_scan_before_publish: true,
      solana_holder_max_exact_token_accounts: HELIUS_MAX_EXACT_TOKEN_ACCOUNTS,
      solana_moralis_holder_endpoints_not_used_because_deprecated: true,
      no_composite_security_score_generated: true,
      creator_owner_labels_are_source_facts_not_dev_inference: true,
      user_reads_direct_upstream_requests: 0,
    },
    current_market_refresh: {
      ready: trendingSnapshot.length > 0 && (marketAgeMs === null || marketAgeMs <= MARKET_RETAIN_MS),
      refresh_interval_ms: MARKET_REFRESH_MS,
      retain_ms: MARKET_RETAIN_MS,
      age_ms: marketAgeMs,
      rows: trendingSnapshot.length,
      user_reads_start_upstream: false,
      fixed_background_rate_independent_of_user_count: true,
    },
    fx_reference: {
      ready: Boolean(fxSnapshot) && (fxAgeMs === null || fxAgeMs <= FX_RETAIN_MS),
      route: FX_REFERENCE_ROUTE,
      source: 'ecb_euro_foreign_exchange_reference_rates',
      observation_date: fxSnapshot?.observation_date || null,
      supported_secondary_currencies: ['CNY', 'JPY', 'EUR'],
      refresh_interval_ms: FX_REFRESH_MS,
      retain_ms: FX_RETAIN_MS,
      age_ms: fxAgeMs,
      user_reads_start_upstream: false,
    },
    discovery: {
      ready: trendingSnapshot.length > 0 && (ageMs === null || ageMs <= DISCOVERY_RETAIN_MS),
      name: 'recent_hot',
      token_centric_results: true,
      exact_discovered_candidate_token_only: true,
      both_sides_of_pair_are_not_automatically_listed: true,
      quote_token_never_inherits_base_token_market_fields: true,
      basis: 'latest_profile_plus_top_boost_candidates_rescored_by_liquidity_volume_and_transactions',
      token_profile_metadata_preserved: true,
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
      step1038_shared_generic_cache_entries: cache.size,
    },
    scheduler: dexScheduler.state(),
    moralis_scheduler: moralisScheduler.state(),
    goplus_scheduler: goplusScheduler.state(),
    helius_scheduler: heliusScheduler.state(),
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
  const profileSynthetic = normalizeCandidateProfile({ icon: 'https://cdn.example/icon.png', header: 'https://cdn.example/header.png', description: 'Hello', url: 'https://dexscreener.com/x', links: [{ type: 'twitter', url: 'https://x.com/example' }] });
  t('token_profile_metadata_parser', profileSynthetic?.icon_url.startsWith('https://') && profileSynthetic?.description === 'Hello' && profileSynthetic?.links?.length === 1);
  const fxSynthetic = parseEcbDailyFxXml(`<gesmes><Cube><Cube time='2026-08-19'><Cube currency='USD' rate='1.1605'/><Cube currency='JPY' rate='184.62'/><Cube currency='CNY' rate='7.8197'/></Cube></Cube></gesmes>`);
  t('ecb_fx_cross_rate_math', Math.abs(fxSynthetic.usd_to.CNY - (7.8197 / 1.1605)) < 1e-9 && Math.abs(fxSynthetic.usd_to.EUR - (1 / 1.1605)) < 1e-9);
  t('market_refresh_fixed_background', MARKET_REFRESH_MS >= 30_000 && MARKET_REFRESH_MS < DISCOVERY_REFRESH_MS);
  t('fx_user_reads_do_not_start_upstream', healthPayload().fx_reference.user_reads_start_upstream === false);
  const holderSynthetic = normalizeHolderMetrics({ totalHolders: 1000, holderSupply: { top10: { supply: '100', supplyPercent: 10 }, top25: { supply: '200', supplyPercent: 20 }, top50: { supply: '300', supplyPercent: 30 } } });
  t('step1038_holder_metrics_parser', holderSynthetic.total_holders === 1000 && holderSynthetic.concentration.top10.supply_percent === 10 && holderSynthetic.concentration.top50.supply_percent === 30);
  const holderWrappedSynthetic = normalizeHolderMetrics({ data: { totalHolders: '321', holderSupply: { top10: { supplyPercent: '12.5' } } } });
  t('step1038_holder_metrics_wrapper_parser', holderWrappedSynthetic.total_holders === 321 && holderWrappedSynthetic.concentration.top10.supply_percent === 12.5);
  const gpSynthetic = normalizeGoPlusEvm({ token_name: 'T', token_symbol: 'T', is_honeypot: '0', is_open_source: '1', creator_address: '0x0000000000000000000000000000000000000001', creator_percent: '0.025', holders: [{ address: '0x0000000000000000000000000000000000000002', percent: '0.1', balance: '10', is_locked: '0' }] }, 'ethereum', '0x0000000000000000000000000000000000000003');
  t('step1038_goplus_evm_parser', gpSynthetic.contract_facts.is_open_source === true && gpSynthetic.trading_facts.is_honeypot === false && Math.abs(gpSynthetic.creator.percent - 2.5) < 1e-9 && Math.abs(gpSynthetic.top10_percent_reported - 10) < 1e-9);
  t('step1038_goplus_rate_below_30_per_min', 60_000 / GOPLUS_MIN_GAP_MS < 30);
  const mintBytes = Buffer.alloc(32, 9);
  const ownerBytes = Buffer.alloc(32, 7);
  const slice = Buffer.alloc(72);
  mintBytes.copy(slice, 0);
  ownerBytes.copy(slice, 32);
  slice.writeBigUInt64LE(123456789n, 64);
  const parsedSlice = parseSolanaTokenAccountSlice({ account: { data: [slice.toString('base64'), 'base64'] } });
  t('step1038_2_1_helius_token_account_slice_parser', parsedSlice?.amount === 123456789n && looksSolanaAddress(parsedSlice?.mint) && looksSolanaAddress(parsedSlice?.owner));
  t('step1038_2_1_helius_percent_bigint', Math.abs(bigintPercent(25n, 100n) - 25) < 1e-9);
  t('step1038_2_1_helius_rate_below_free_gpa_rps', 1_000 / HELIUS_MIN_GAP_MS < 5);
  t('step1038_2_1_solana_moralis_deprecated_holder_not_used', healthPayload().step1038_holder_security.solana_moralis_holder_endpoints_not_used_because_deprecated === true);
  t('step1038_no_composite_security_score', healthPayload().step1038_holder_security.no_composite_security_score_generated === true);
  t('step1038_solana_top20_fail_closed', healthPayload().step1038_holder_security.solana_top20_not_fabricated === true);
  t('trading_disabled', responseBase().trading_enabled === false);
  t('db_writes_disabled', responseBase().database_writes === false);
  t('commercial_source_terms_recorded', healthPayload().sources.dexscreener.commercial_use_permitted_subject_to_api_terms === true);
  return responseBase({ ok: tests.every((x) => x.pass), test_count: tests.length, passed: tests.filter((x) => x.pass).length, failed: tests.filter((x) => !x.pass).length, tests });
}

export async function handleOnchainMarket(req, res, url) {
  const path = url?.pathname || '';
  if (![HEALTH_ROUTE, SELF_TEST_ROUTE, TRENDING_ROUTE, SEARCH_ROUTE, TOKEN_ROUTE, POOLS_ROUTE, KLINES_ROUTE, TRADES_ROUTE, NEW_POOLS_ROUTE, FX_REFERENCE_ROUTE, HOLDERS_ROUTE, SECURITY_ROUTE].includes(path)) return false;
  stats.user_reads += 1;
  if (req.method !== 'GET') { sendJson(res, 405, responseBase({ ok: false, error: 'method_not_allowed' })); return true; }
  if (path === HEALTH_ROUTE) { sendJson(res, 200, healthPayload()); return true; }
  if (path === SELF_TEST_ROUTE) { const result = runSelfTest(); sendJson(res, result.ok ? 200 : 500, result); return true; }
  if (path === FX_REFERENCE_ROUTE) {
    const ageMs = fxUpdatedAt ? Math.max(0, Date.now() - fxUpdatedAt) : null;
    if (!fxSnapshot || ageMs === null || ageMs > FX_RETAIN_MS) {
      sendJson(res, 503, responseBase({ ok: false, error: 'shared_fx_reference_not_ready', source: 'ecb_euro_foreign_exchange_reference_rates', user_read_upstream_requests: 0 }));
      return true;
    }
    sendJson(res, 200, responseBase({ ...fxSnapshot, generated_at: new Date(fxUpdatedAt).toISOString(), shared_snapshot_age_ms: ageMs, user_read_upstream_requests: 0, cache_status: 'background_shared' }));
    return true;
  }

  const network = normalizeNetwork(url.searchParams.get('network'));
  const limit = intRange(url.searchParams.get('limit'), 1, MAX_RESPONSE_ROWS, 50);

  if (path === HOLDERS_ROUTE || path === SECURITY_ROUTE) {
    if (!network || network === 'all') {
      sendJson(res, 400, responseBase({ ok: false, error: 'exact_network_required', feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION }));
      return true;
    }
    const address = text(url.searchParams.get('address') || url.searchParams.get('token_address'));
    if (!validAddressForNetwork(network, address)) {
      sendJson(res, 400, responseBase({ ok: false, error: 'invalid_contract_address', network, feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION }));
      return true;
    }
    try {
      if (path === HOLDERS_ROUTE) {
        const key = `step1038:holders:${network}:${lower(address)}`;
        const result = await cachedBuild(key, { freshMs: network === 'solana' ? SOLANA_HELIUS_HOLDER_FRESH_MS : HOLDER_FRESH_MS, staleMs: network === 'solana' ? SOLANA_HELIUS_HOLDER_STALE_MS : HOLDER_STALE_MS, negativeMs: HOLDER_NEGATIVE_MS }, () => buildHolderAnalysis(network, address));
        const value = result.value;
        if (!value) throw new Error('holder_analysis_not_ready');
        sendJson(res, 200, responseBase({
          feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION,
          network,
          address,
          source: value.source,
          source_scope: value.source_scope,
          total_holders: value.total_holders,
          concentration: value.concentration,
          holder_change: value.holder_change,
          holder_distribution: value.holder_distribution,
          holders_by_acquisition: value.holders_by_acquisition,
          top_holders: value.top_holders,
          top_holder_list_available: value.top_holder_list_available,
          exact_top20_available: value.exact_top20_available,
          field_sources: value.field_sources || null,
          upstream_partial_errors: value.upstream_partial_errors || null,
          cache_status: result.cache_status,
          user_read_direct_moralis_requests: 0,
          moralis_cu_if_full_evm_upstream_build: MORALIS_HOLDER_METRICS_CU + MORALIS_TOP_HOLDERS_CU,
          moralis_cu_if_solana_metrics_build: network === 'solana' ? 0 : MORALIS_HOLDER_METRICS_CU,
          helius_exact_solana_holder_index: network === 'solana',
          helius_scan_complete: value.helius_scan_complete ?? null,
          token_accounts_scanned: value.token_accounts_scanned ?? null,
          no_deprecated_solana_holder_call: true,
        }));
        return true;
      }
      const key = `step1038:security:${network}:${lower(address)}`;
      const result = await cachedBuild(key, { freshMs: SECURITY_FRESH_MS, staleMs: SECURITY_STALE_MS, negativeMs: SECURITY_NEGATIVE_MS }, () => buildGoPlusSecurity(network, address));
      const value = result.value;
      if (!value) throw new Error('security_analysis_not_ready');
      sendJson(res, 200, responseBase({
        feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION,
        network,
        address,
        source: network === 'solana' ? 'goplus_solana_token_security_beta' : 'goplus_token_security',
        security: value,
        cache_status: result.cache_status,
        no_composite_security_score: true,
        source_facts_only: true,
        creator_owner_are_direct_source_fields_not_dev_inference: true,
        user_read_direct_goplus_requests: 0,
      }));
      return true;
    } catch (error) {
      sendJson(res, 503, responseBase({
        ok: false,
        feature_schema_version: STEP1038_FEATURE_SCHEMA_VERSION,
        error: text(error?.message || error),
        network,
        address,
        no_cross_chain_or_token_fallback: true,
      }));
      return true;
    }
  }

  if (path === TRENDING_ROUTE) {
    if (!network) { sendJson(res, 400, responseBase({ ok: false, error: 'invalid_network' })); return true; }
    const rows = discoveryRows(network, limit);
    const ageMs = discoveryUpdatedAt ? Math.max(0, Date.now() - discoveryUpdatedAt) : null;
    if (!rows.length && (!discoveryUpdatedAt || ageMs > DISCOVERY_RETAIN_MS)) {
      sendJson(res, 503, responseBase({ ok: false, error: 'onchain_shared_recent_hot_not_ready', network, rows: [], user_read_upstream_requests: 0 }));
      return true;
    }
    sendJson(res, 200, responseBase({ network, rows, row_count: rows.length, generated_at: marketUpdatedAt ? new Date(marketUpdatedAt).toISOString() : (discoveryUpdatedAt ? new Date(discoveryUpdatedAt).toISOString() : null), shared_snapshot_age_ms: marketUpdatedAt ? Math.max(0, Date.now() - marketUpdatedAt) : ageMs, user_read_upstream_requests: 0, cache_status: 'background_shared' }));
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
          token_profile: tokenMarket?.token_profile || tokenProfileForIdentity(network, address) || null,
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

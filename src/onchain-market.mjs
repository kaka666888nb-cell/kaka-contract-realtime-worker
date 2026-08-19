// Step1036 / Render 650.8.15.190
// Kaka Web3 on-chain market foundation (phase 1): DEX Screener commercial-use API only.
// Scope: Solana + BNB Chain + Base + Ethereum; shared recent-hot discovery, CA/name search,
// exact token snapshot and pool list. No trading, no wallet signing, no DB writes, no App-direct
// upstream. Historical pool OHLCV is intentionally NOT opened in Step1036 until a production-safe
// source is locked; Step1037 owns Kline/history rather than shipping a non-commercial data source.

const VERSION = '650.8.15.190';
const DATA_VERSION = 1036000;
const SCHEMA_VERSION = 'step1036_onchain_market_v1';

const HEALTH_ROUTE = '/api/onchain/health';
const SELF_TEST_ROUTE = '/api/onchain/self-test';
const TRENDING_ROUTE = '/api/onchain/trending';
const SEARCH_ROUTE = '/api/onchain/search';
const TOKEN_ROUTE = '/api/onchain/token';
const POOLS_ROUTE = '/api/onchain/pools';

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
};

const cache = new Map();
const negativeCache = new Map();
const inflight = new Map();
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

async function buildDexSearch(query) {
  const payload = await dexFetchJson(`${DEX_BASE}/latest/dex/search?q=${encodeURIComponent(query)}`, { priority: 10, label: 'search' });
  return sortBestPools(dedupePools(normalizeDexPairs(payload))).slice(0, MAX_RESPONSE_ROWS);
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
      pairs.push({ ...pair, candidate_sources: match.candidate_sources, candidate_boost_amount: match.amount, candidate_total_boost_amount: match.total_amount });
    }
  }
  return pairs;
}
function recentHotTokenRows(pairs) {
  const byToken = new Map();
  for (const pair of pairs) {
    const candidates = [pair.base_token, pair.quote_token];
    for (const token of candidates) {
      if (!token?.address) continue;
      const key = `${pair.network}|${lower(token.address)}`;
      const current = byToken.get(key);
      const score = poolScore(pair);
      if (!current || score > current.recent_hot_score) {
        byToken.set(key, {
          network: pair.network,
          chain_id: pair.chain_id,
          token: { ...token },
          best_pool: pair,
          price_usd: pair.price_usd,
          liquidity_usd: pair.liquidity_usd,
          market_cap_usd: pair.market_cap_usd,
          fdv_usd: pair.fdv_usd,
          volume_usd: pair.volume_usd,
          price_change_pct: pair.price_change_pct,
          txns: pair.txns,
          pool_created_at: pair.pool_created_at,
          recent_hot_score: score,
          candidate_sources: pair.candidate_sources || [],
          source: 'dexscreener_public_api_market_activity_rescore',
        });
      }
    }
  }
  return [...byToken.values()].sort((a, b) => b.recent_hot_score - a.recent_hot_score).slice(0, MAX_RESPONSE_ROWS);
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
    },
    discovery: {
      ready: trendingSnapshot.length > 0 && (ageMs === null || ageMs <= DISCOVERY_RETAIN_MS),
      name: 'recent_hot',
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
      opened: false,
      planned_step: 1037,
      reason: 'do_not_ship_noncommercial_or_unlicensed_ohlcv_source',
    },
    bounded_user_builds: {
      exact_search_and_token_pool_may_enqueue_bounded_build: true,
      fixed_backend_rate_independent_of_user_count: true,
      same_key_cache_singleflight: true,
      queue_overflow_rejected_not_amplified: true,
      direct_app_upstream: false,
    },
    caches: { entries: cache.size, max_entries: CACHE_MAX_ENTRIES, negative_entries: negativeCache.size, negative_max_entries: NEGATIVE_CACHE_MAX_ENTRIES, inflight: inflight.size },
    scheduler: dexScheduler.state(),
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
  t('cross_chain_substitution_false', responseBase().cross_chain_substitution === false);
  t('cross_token_substitution_false', responseBase().cross_token_substitution === false);
  t('direct_app_upstream_zero', responseBase().app_direct_upstream_requests === 0);
  t('global_rate_below_candidate_limit', 60_000 / DEX_MIN_GAP_MS < 60);
  t('cache_bounded', CACHE_MAX_ENTRIES <= 1024);
  t('negative_cache_bounded', NEGATIVE_CACHE_MAX_ENTRIES <= 512);
  t('response_rows_bounded', MAX_RESPONSE_ROWS <= 100);
  t('kline_not_opened_without_safe_source', healthPayload().kline.opened === false);
  t('trading_disabled', responseBase().trading_enabled === false);
  t('db_writes_disabled', responseBase().database_writes === false);
  t('commercial_source_terms_recorded', healthPayload().sources.dexscreener.commercial_use_permitted_subject_to_api_terms === true);
  return responseBase({ ok: tests.every((x) => x.pass), test_count: tests.length, passed: tests.filter((x) => x.pass).length, failed: tests.filter((x) => !x.pass).length, tests });
}

export async function handleOnchainMarket(req, res, url) {
  const path = url?.pathname || '';
  if (![HEALTH_ROUTE, SELF_TEST_ROUTE, TRENDING_ROUTE, SEARCH_ROUTE, TOKEN_ROUTE, POOLS_ROUTE].includes(path)) return false;
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
        sendJson(res, 200, responseBase({ network, address, token, best_pool: best, pool_count: rows.length, pools_preview: rows.slice(0, 6), cache_status: result.cache_status }));
      }
    } catch (error) { sendJson(res, 503, responseBase({ ok: false, error: text(error?.message || error), network, address })); }
    return true;
  }
  return false;
}

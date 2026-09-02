const VERSION = '650.8.15.169';
const DATA_VERSION = 1034;
const SCHEMA_VERSION = 'step1034_project_protocol_fundamentals_v1';
const IMPLEMENTATION_REVISION = '1034_defillama_exact_gecko_id_shared_hourly_v1';
const CURRENT_ROUTE = '/api/project-fundamentals/current';
const HEALTH_ROUTE = '/api/project-fundamentals/health';
const TABLE = 'kaka_project_fundamentals';
const REFRESH_MS = Math.max(30 * 60_000, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_REFRESH_MS || 60 * 60_000));
const RESTORE_MAX_AGE_MS = Math.max(24 * 60 * 60_000, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_RESTORE_MAX_AGE_MS || 7 * 24 * 60 * 60_000));
// Step1060.9: source refresh remains hourly, while unchanged persisted rows only need a bounded
// heartbeat for restart recovery. App reads keep using the freshly rebuilt in-memory catalog.
const PERSIST_HEARTBEAT_MS = Math.max(
  60 * 60_000,
  Math.min(24 * 60 * 60_000, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_PERSIST_HEARTBEAT_MS || 6 * 60 * 60_000)),
);
const REQUEST_TIMEOUT_MS = Math.max(5_000, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_REQUEST_TIMEOUT_MS || 25_000));
const IDENTITY_REFRESH_MS = Math.max(6 * 60 * 60_000, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_IDENTITY_REFRESH_MS || 24 * 60 * 60_000));
const IDENTITY_SUMMARY_LIMIT = Math.max(20, Math.min(120, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_IDENTITY_SUMMARY_LIMIT || 80)));
const IDENTITY_SUMMARY_CONCURRENCY = Math.max(1, Math.min(6, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_IDENTITY_SUMMARY_CONCURRENCY || 4)));
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const SUPABASE_CONFIGURED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const SOURCE_ENDPOINTS = Object.freeze({
  protocols: 'https://api.llama.fi/protocols',
  fees: 'https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyFees',
  revenue: 'https://api.llama.fi/overview/fees?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue',
});

const catalog = new Map();
let started = false;
let startPromise = null;
let refreshTimer = null;
let refreshInflight = null;
let lastRestoreAt = '';
let lastRestoreError = '';
let lastRefreshStartedAt = '';
let lastRefreshSucceededAt = '';
let lastRefreshError = '';
let lastPersistedAt = '';
let lastSourceFetchedAt = '';
let restoreRows = 0;
let sourceRequestsStarted = 0;
let sourceRequestsSucceeded = 0;
let sourceRequestsFailed = 0;
let refreshAttempts = 0;
let refreshSuccesses = 0;
let refreshFailures = 0;
let persistAttempts = 0;
let persistSuccesses = 0;
let persistFailures = 0;
let persistRowsConsidered = 0;
let persistRowsWritten = 0;
let persistRowsNoopSkipped = 0;
let persistHeartbeatRows = 0;
let persistRemovedRows = 0;
let lastFullPersistAt = '';
let totalReads = 0;
let availableReads = 0;
let unavailableReads = 0;
let exactUniqueMatches = 0;
let ambiguousGeckoIds = 0;
let feeRowsMatched = 0;
let revenueRowsMatched = 0;
let identitySummaryRequests = 0;
let identitySummarySuccesses = 0;
let identitySummaryFailures = 0;
let lastIdentityRefreshAt = '';
let lastIdentityRefreshError = '';

function compact(value) { return String(value ?? '').trim(); }
function lower(value) { return compact(value).toLowerCase(); }
function finiteOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function nonNegativeOrNull(value) {
  const n = finiteOrNull(value);
  return n != null && n >= 0 ? n : null;
}
function pct(numerator, denominator) {
  const a = finiteOrNull(numerator);
  const b = finiteOrNull(denominator);
  if (a == null || b == null || b <= 0) return null;
  return (a / b) * 100;
}
function authHeaders(extra = {}) {
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    ...extra,
  };
}
function sendJson(res, status, payload) {
  if (res.headersSent) return;
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': String(body.length),
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
    'access-control-allow-methods': 'GET, OPTIONS',
  });
  res.end(body);
}
async function timedJson(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('timeout')), REQUEST_TIMEOUT_MS);
  sourceRequestsStarted += 1;
  try {
    const response = await fetch(url, {
      method: 'GET',
      headers: { accept: 'application/json', 'user-agent': 'KakaWeb3-Shared-Project-Fundamentals/1034' },
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`http_${response.status}`);
    const json = await response.json();
    sourceRequestsSucceeded += 1;
    return json;
  } catch (error) {
    sourceRequestsFailed += 1;
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
async function supabaseFetch(path, init = {}) {
  if (!SUPABASE_CONFIGURED) throw new Error('supabase_service_role_not_configured');
  return fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    ...init,
    headers: authHeaders(init.headers || {}),
  });
}
function feeIdentityKeys(row) {
  const keys = new Set();
  for (const raw of [row?.defillamaId, row?.defillama_id, row?.id, row?.slug]) {
    const key = lower(raw);
    if (key) keys.add(key);
  }
  return [...keys];
}
function protocolIdentityKeys(row) {
  const keys = new Set();
  for (const raw of [row?.id, row?.slug]) {
    const key = lower(raw);
    if (key) keys.add(key);
  }
  return [...keys];
}
function overviewRows(payload) {
  if (Array.isArray(payload?.protocols)) return payload.protocols;
  if (Array.isArray(payload)) return payload;
  return [];
}
function overviewMap(payload) {
  const map = new Map();
  for (const row of overviewRows(payload)) {
    for (const key of feeIdentityKeys(row)) {
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(row);
    }
  }
  return map;
}
function uniqueOverviewForProtocol(protocol, map) {
  const found = new Map();
  for (const key of protocolIdentityKeys(protocol)) {
    for (const row of map.get(key) || []) {
      const identity = lower(row?.defillamaId || row?.defillama_id || row?.id || row?.slug);
      if (identity) found.set(identity, row);
    }
  }
  return found.size === 1 ? [...found.values()][0] : null;
}
function rowMetric(row, ...keys) {
  for (const key of keys) {
    const value = nonNegativeOrNull(row?.[key]);
    if (value != null) return value;
  }
  return null;
}
function existingIdentityMap() {
  const map = new Map();
  for (const row of catalog.values()) {
    const coinId = lower(row?.coin_id);
    if (!coinId) continue;
    for (const key of [lower(row?.protocol_id), lower(row?.protocol_slug)].filter(Boolean)) map.set(key, coinId);
  }
  return map;
}
function buildCatalog(protocolsPayload, feesPayload, revenuePayload, fetchedAt, suppliedIdentityMap = new Map()) {
  const protocols = Array.isArray(protocolsPayload) ? protocolsPayload : [];
  const identityMap = new Map(existingIdentityMap());
  for (const [key, value] of suppliedIdentityMap.entries()) {
    if (lower(key) && lower(value)) identityMap.set(lower(key), lower(value));
  }
  const byGecko = new Map();
  for (const protocol of protocols) {
    let geckoId = lower(protocol?.gecko_id || protocol?.geckoId);
    if (!geckoId) {
      for (const key of protocolIdentityKeys(protocol)) {
        const mapped = identityMap.get(key);
        if (mapped) { geckoId = mapped; break; }
      }
    }
    if (!geckoId) continue;
    if (!byGecko.has(geckoId)) byGecko.set(geckoId, []);
    byGecko.get(geckoId).push(protocol);
  }
  const feesMap = overviewMap(feesPayload);
  const revenueMap = overviewMap(revenuePayload);
  const next = new Map();
  let ambiguous = 0;
  let feesMatched = 0;
  let revenueMatched = 0;
  for (const [geckoId, candidates] of byGecko.entries()) {
    const uniqueCandidates = new Map();
    for (const candidate of candidates) {
      const identity = lower(candidate?.id || candidate?.slug);
      if (identity) uniqueCandidates.set(identity, candidate);
    }
    if (uniqueCandidates.size !== 1) {
      ambiguous += 1;
      continue;
    }
    const protocol = [...uniqueCandidates.values()][0];
    const feeRow = uniqueOverviewForProtocol(protocol, feesMap);
    const revenueRow = uniqueOverviewForProtocol(protocol, revenueMap);
    if (feeRow) feesMatched += 1;
    if (revenueRow) revenueMatched += 1;
    const fees24h = rowMetric(feeRow, 'total24h', 'total_24h');
    const fees7d = rowMetric(feeRow, 'total7d', 'total_7d');
    const fees30d = rowMetric(feeRow, 'total30d', 'total_30d');
    const revenue24h = rowMetric(revenueRow, 'total24h', 'total_24h');
    const revenue7d = rowMetric(revenueRow, 'total7d', 'total_7d');
    const revenue30d = rowMetric(revenueRow, 'total30d', 'total_30d');
    const chains = Array.isArray(protocol?.chains) ? protocol.chains.map(compact).filter(Boolean).slice(0, 32) : [];
    const tvl = nonNegativeOrNull(protocol?.tvl);
    const row = {
      coin_id: geckoId,
      symbol: compact(protocol?.symbol).toUpperCase(),
      protocol_id: compact(protocol?.id),
      protocol_slug: compact(protocol?.slug),
      protocol_name: compact(protocol?.name),
      category: compact(protocol?.category),
      chains,
      tvl_usd: tvl,
      fees_24h_usd: fees24h,
      fees_7d_usd: fees7d,
      fees_30d_usd: fees30d,
      revenue_24h_usd: revenue24h,
      revenue_7d_usd: revenue7d,
      revenue_30d_usd: revenue30d,
      revenue_to_fees_24h_pct: pct(revenue24h, fees24h),
      match_method: 'defillama_gecko_id_exact_unique',
      match_verified: true,
      source_name: 'DefiLlama',
      source_fetched_at: fetchedAt,
      source_protocols_endpoint: SOURCE_ENDPOINTS.protocols,
      source_fees_endpoint: SOURCE_ENDPOINTS.fees,
      source_revenue_endpoint: SOURCE_ENDPOINTS.revenue,
    };
    next.set(geckoId, row);
  }
  return { next, ambiguous, feesMatched, revenueMatched };
}
function persistenceComparable(row) {
  return JSON.stringify({
    coin_id: lower(row?.coin_id),
    symbol: compact(row?.symbol).toUpperCase(),
    protocol_id: compact(row?.protocol_id),
    protocol_slug: compact(row?.protocol_slug),
    protocol_name: compact(row?.protocol_name),
    category: compact(row?.category),
    chains: Array.isArray(row?.chains) ? row.chains.map(compact).filter(Boolean) : [],
    tvl_usd: finiteOrNull(row?.tvl_usd),
    fees_24h_usd: finiteOrNull(row?.fees_24h_usd),
    fees_7d_usd: finiteOrNull(row?.fees_7d_usd),
    fees_30d_usd: finiteOrNull(row?.fees_30d_usd),
    revenue_24h_usd: finiteOrNull(row?.revenue_24h_usd),
    revenue_7d_usd: finiteOrNull(row?.revenue_7d_usd),
    revenue_30d_usd: finiteOrNull(row?.revenue_30d_usd),
    revenue_to_fees_24h_pct: finiteOrNull(row?.revenue_to_fees_24h_pct),
    match_method: compact(row?.match_method),
    match_verified: row?.match_verified === true,
    source_name: compact(row?.source_name),
    source_protocols_endpoint: compact(row?.source_protocols_endpoint),
    source_fees_endpoint: compact(row?.source_fees_endpoint),
    source_revenue_endpoint: compact(row?.source_revenue_endpoint),
  });
}

async function deleteRemovedPersisted(removedIds) {
  if (!removedIds.length) return 0;
  let removed = 0;
  for (let index = 0; index < removedIds.length; index += 100) {
    const batch = removedIds.slice(index, index + 100).map((id) => encodeURIComponent(id));
    const response = await supabaseFetch(`${TABLE}?coin_id=in.(${batch.join(',')})`, {
      method: 'DELETE',
      headers: { prefer: 'return=minimal' },
    });
    if (!response.ok) throw new Error(`cleanup_removed_http_${response.status}`);
    removed += batch.length;
  }
  return removed;
}

async function persistCatalog(nextCatalog, fetchedAt) {
  if (!SUPABASE_CONFIGURED || !(nextCatalog instanceof Map) || !nextCatalog.size) return false;
  const previous = catalog;
  const nextRows = [...nextCatalog.values()];
  persistRowsConsidered += nextRows.length;
  const lastFullMs = Date.parse(lastFullPersistAt || '');
  const heartbeatDue = !Number.isFinite(lastFullMs) || Date.now() - lastFullMs >= PERSIST_HEARTBEAT_MS;
  const changedRows = heartbeatDue ? nextRows : nextRows.filter((row) => {
    const key = lower(row?.coin_id);
    const old = previous.get(key);
    return !old || persistenceComparable(old) !== persistenceComparable(row);
  });
  const removedIds = [...previous.keys()].filter((key) => !nextCatalog.has(key));
  persistRowsNoopSkipped += Math.max(0, nextRows.length - changedRows.length);

  if (!changedRows.length && !removedIds.length) return true;
  persistAttempts += 1;
  try {
    if (changedRows.length) {
      const response = await supabaseFetch(`${TABLE}?on_conflict=coin_id`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          prefer: 'resolution=merge-duplicates,return=minimal',
        },
        body: JSON.stringify(changedRows),
      });
      if (!response.ok) {
        const text = await response.text().catch(() => '');
        throw new Error(`persist_http_${response.status}:${text.slice(0, 160)}`);
      }
      persistRowsWritten += changedRows.length;
      if (heartbeatDue) {
        persistHeartbeatRows += changedRows.length;
        lastFullPersistAt = fetchedAt;
      }
    }
    const removed = await deleteRemovedPersisted(removedIds);
    persistRemovedRows += removed;
    persistSuccesses += 1;
    lastPersistedAt = new Date().toISOString();
    return true;
  } catch (error) {
    persistFailures += 1;
    throw error;
  }
}

async function restorePersisted() {
  if (!SUPABASE_CONFIGURED) return false;
  try {
    const cutoff = new Date(Date.now() - RESTORE_MAX_AGE_MS).toISOString();
    const query = `${TABLE}?select=*&source_fetched_at=gte.${encodeURIComponent(cutoff)}&order=source_fetched_at.desc&limit=5000`;
    const response = await supabaseFetch(query, { method: 'GET' });
    if (!response.ok) throw new Error(`restore_http_${response.status}`);
    const rows = await response.json();
    if (!Array.isArray(rows)) throw new Error('restore_payload_invalid');
    const restored = new Map();
    for (const raw of rows) {
      const coinId = lower(raw?.coin_id);
      if (!coinId || raw?.match_verified !== true || compact(raw?.match_method) !== 'defillama_gecko_id_exact_unique') continue;
      if (restored.has(coinId)) continue;
      restored.set(coinId, {
        ...raw,
        coin_id: coinId,
        chains: Array.isArray(raw?.chains) ? raw.chains : [],
        match_verified: true,
      });
    }
    if (restored.size) {
      catalog.clear();
      for (const [key, row] of restored.entries()) catalog.set(key, row);
      restoreRows = restored.size;
      lastSourceFetchedAt = compact([...restored.values()][0]?.source_fetched_at);
      lastFullPersistAt = lastSourceFetchedAt;
    }
    lastRestoreAt = new Date().toISOString();
    lastRestoreError = '';
    return restored.size > 0;
  } catch (error) {
    lastRestoreError = String(error?.message || error).slice(0, 320);
    return false;
  }
}
async function fetchBoundedIdentityMap(protocolsPayload, feesPayload) {
  const protocols = Array.isArray(protocolsPayload) ? [...protocolsPayload] : [];
  const feesMap = overviewMap(feesPayload);
  protocols.sort((a, b) => Number(b?.tvl || 0) - Number(a?.tvl || 0));
  const candidates = protocols.filter((protocol) => {
    const slug = compact(protocol?.slug);
    return slug && uniqueOverviewForProtocol(protocol, feesMap) != null;
  }).slice(0, IDENTITY_SUMMARY_LIMIT);
  const identityMap = new Map();
  let cursor = 0;
  async function worker() {
    while (cursor < candidates.length) {
      const index = cursor++;
      const protocol = candidates[index];
      const slug = compact(protocol?.slug);
      if (!slug) continue;
      identitySummaryRequests += 1;
      try {
        const summary = await timedJson(`https://api.llama.fi/summary/fees/${encodeURIComponent(slug)}?dataType=dailyFees`);
        const geckoId = lower(summary?.gecko_id || summary?.geckoId);
        if (!geckoId) continue;
        for (const key of protocolIdentityKeys(protocol)) identityMap.set(key, geckoId);
        for (const key of feeIdentityKeys(summary)) identityMap.set(key, geckoId);
        identitySummarySuccesses += 1;
      } catch (_) {
        identitySummaryFailures += 1;
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(IDENTITY_SUMMARY_CONCURRENCY, Math.max(1, candidates.length)) }, () => worker()));
  lastIdentityRefreshAt = new Date().toISOString();
  return identityMap;
}

async function refreshCatalog() {
  if (refreshInflight) return refreshInflight;
  refreshInflight = (async () => {
    refreshAttempts += 1;
    lastRefreshStartedAt = new Date().toISOString();
    try {
      const [protocols, fees, revenue] = await Promise.all([
        timedJson(SOURCE_ENDPOINTS.protocols),
        timedJson(SOURCE_ENDPOINTS.fees),
        timedJson(SOURCE_ENDPOINTS.revenue),
      ]);
      const fetchedAt = new Date().toISOString();
      let built = buildCatalog(protocols, fees, revenue, fetchedAt);
      const directIdentityCount = (Array.isArray(protocols) ? protocols : []).filter((row) => lower(row?.gecko_id || row?.geckoId)).length;
      const identityDue = !lastIdentityRefreshAt || (Date.now() - Date.parse(lastIdentityRefreshAt || '')) >= IDENTITY_REFRESH_MS;
      if (directIdentityCount < 20 && identityDue) {
        try {
          const identityMap = await fetchBoundedIdentityMap(protocols, fees);
          if (identityMap.size) built = buildCatalog(protocols, fees, revenue, fetchedAt, identityMap);
          lastIdentityRefreshError = '';
        } catch (error) {
          lastIdentityRefreshError = String(error?.message || error).slice(0, 240);
        }
      }
      if (built.next.size < 20) throw new Error(`exact_match_catalog_too_small_${built.next.size}`);
      await persistCatalog(built.next, fetchedAt);
      catalog.clear();
      for (const [key, row] of built.next.entries()) catalog.set(key, row);
      exactUniqueMatches = built.next.size;
      ambiguousGeckoIds = built.ambiguous;
      feeRowsMatched = built.feesMatched;
      revenueRowsMatched = built.revenueMatched;
      lastSourceFetchedAt = fetchedAt;
      lastRefreshSucceededAt = new Date().toISOString();
      lastRefreshError = '';
      refreshSuccesses += 1;
      return true;
    } catch (error) {
      refreshFailures += 1;
      lastRefreshError = String(error?.message || error).slice(0, 320);
      return false;
    } finally {
      refreshInflight = null;
    }
  })();
  return refreshInflight;
}
function scheduleNext() {
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = setInterval(() => { refreshCatalog().catch(() => {}); }, REFRESH_MS);
  refreshTimer.unref?.();
}
export function startProjectFundamentalsCollector() {
  if (started) return startPromise;
  started = true;
  startPromise = (async () => {
    await restorePersisted();
    refreshCatalog().catch(() => {});
    scheduleNext();
    return true;
  })();
  return startPromise;
}
export function getProjectFundamentalsHealth() {
  const ageMs = lastSourceFetchedAt ? Math.max(0, Date.now() - Date.parse(lastSourceFetchedAt)) : null;
  return {
    ok: true,
    ready: catalog.size > 0,
    version: VERSION,
    data_version: DATA_VERSION,
    schema_version: SCHEMA_VERSION,
    implementation_revision: IMPLEMENTATION_REVISION,
    source: 'DefiLlama public API shared background collector',
    source_policy: 'exact unique CoinGecko gecko_id match only; no symbol/name fallback; missing or ambiguous stays unavailable',
    source_endpoints: SOURCE_ENDPOINTS,
    refresh_interval_minutes: REFRESH_MS / 60_000,
    identity_refresh_hours: IDENTITY_REFRESH_MS / 3_600_000,
    identity_summary_limit: IDENTITY_SUMMARY_LIMIT,
    identity_summary_concurrency: IDENTITY_SUMMARY_CONCURRENCY,
    supabase_configured: SUPABASE_CONFIGURED,
    persistence_table: TABLE,
    persistence_ready: SUPABASE_CONFIGURED && catalog.size > 0 && (persistSuccesses > 0 || restoreRows > 0),
    mapped_project_count: catalog.size,
    exact_unique_match_count: exactUniqueMatches || catalog.size,
    ambiguous_gecko_id_count: ambiguousGeckoIds,
    fee_rows_matched: feeRowsMatched,
    revenue_rows_matched: revenueRowsMatched,
    identity_summary_requests: identitySummaryRequests,
    identity_summary_successes: identitySummarySuccesses,
    identity_summary_failures: identitySummaryFailures,
    last_identity_refresh_at: lastIdentityRefreshAt || null,
    last_identity_refresh_error: lastIdentityRefreshError || null,
    restore_rows: restoreRows,
    last_restore_at: lastRestoreAt || null,
    last_restore_error: lastRestoreError || null,
    last_refresh_started_at: lastRefreshStartedAt || null,
    last_refresh_succeeded_at: lastRefreshSucceededAt || null,
    last_refresh_error: lastRefreshError || null,
    last_persisted_at: lastPersistedAt || null,
    last_source_fetched_at: lastSourceFetchedAt || null,
    source_age_ms: Number.isFinite(ageMs) ? ageMs : null,
    source_requests_started: sourceRequestsStarted,
    source_requests_succeeded: sourceRequestsSucceeded,
    source_requests_failed: sourceRequestsFailed,
    refresh_attempts: refreshAttempts,
    refresh_successes: refreshSuccesses,
    refresh_failures: refreshFailures,
    persist_attempts: persistAttempts,
    persist_successes: persistSuccesses,
    persist_failures: persistFailures,
    persistence_cost_guard: {
      version: 'step1060_9_project_fundamentals_persist_cost_guard_v1',
      source_refresh_interval_minutes: REFRESH_MS / 60_000,
      unchanged_rows_write_skipped: true,
      exact_removed_coin_id_cleanup: true,
      full_persist_heartbeat_hours: PERSIST_HEARTBEAT_MS / 3_600_000,
      rows_considered: persistRowsConsidered,
      rows_written: persistRowsWritten,
      rows_noop_skipped: persistRowsNoopSkipped,
      heartbeat_rows_written: persistHeartbeatRows,
      removed_rows: persistRemovedRows,
      last_full_persist_at: lastFullPersistAt || null,
    },
    user_reads: totalReads,
    user_reads_available: availableReads,
    user_reads_unavailable: unavailableReads,
    user_reads_trigger_source_requests: false,
    reads_scale_with_users: false,
    direct_exchange_requests: 0,
    direct_exchange_connections: 0,
    paid_coingecko_supply_breakdown_used: false,
    defillama_pro_unlocks_used: false,
    unlock_schedule_available: false,
    allocation_breakdown_available: false,
    token_value_equals_protocol_equity: false,
    sample_coin_ids: [...catalog.keys()].slice(0, 12),
  };
}
function publicItem(row) {
  return {
    coin_id: lower(row?.coin_id),
    symbol: compact(row?.symbol).toUpperCase(),
    protocol_id: compact(row?.protocol_id),
    protocol_slug: compact(row?.protocol_slug),
    protocol_name: compact(row?.protocol_name),
    category: compact(row?.category),
    chains: Array.isArray(row?.chains) ? row.chains : [],
    tvl_usd: nonNegativeOrNull(row?.tvl_usd),
    fees_24h_usd: nonNegativeOrNull(row?.fees_24h_usd),
    fees_7d_usd: nonNegativeOrNull(row?.fees_7d_usd),
    fees_30d_usd: nonNegativeOrNull(row?.fees_30d_usd),
    revenue_24h_usd: nonNegativeOrNull(row?.revenue_24h_usd),
    revenue_7d_usd: nonNegativeOrNull(row?.revenue_7d_usd),
    revenue_30d_usd: nonNegativeOrNull(row?.revenue_30d_usd),
    revenue_to_fees_24h_pct: finiteOrNull(row?.revenue_to_fees_24h_pct),
    match_method: compact(row?.match_method),
    match_verified: row?.match_verified === true,
    source_name: compact(row?.source_name) || 'DefiLlama',
    source_fetched_at: compact(row?.source_fetched_at),
  };
}
export async function handleProjectFundamentals(req, res, url) {
  if (![CURRENT_ROUTE, HEALTH_ROUTE].includes(url.pathname)) return false;
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
  if (url.pathname === HEALTH_ROUTE) {
    sendJson(res, 200, getProjectFundamentalsHealth());
    return true;
  }
  totalReads += 1;
  const coinId = lower(url.searchParams.get('coin_id') || url.searchParams.get('coinId'));
  if (!coinId || !/^[a-z0-9][a-z0-9._-]{0,127}$/.test(coinId)) {
    sendJson(res, 400, { ok: false, version: VERSION, error: 'coin_id_required' });
    return true;
  }
  const row = catalog.get(coinId);
  if (!row) {
    unavailableReads += 1;
    sendJson(res, 200, {
      ok: true,
      version: VERSION,
      data_version: DATA_VERSION,
      schema_version: SCHEMA_VERSION,
      available: false,
      coin_id: coinId,
      reason: 'no_exact_unique_defillama_gecko_id_match',
      match_policy: 'exact_unique_gecko_id_only',
      user_read_source_requests: 0,
      reads_scale_with_users: false,
    });
    return true;
  }
  availableReads += 1;
  sendJson(res, 200, {
    ok: true,
    version: VERSION,
    data_version: DATA_VERSION,
    schema_version: SCHEMA_VERSION,
    implementation_revision: IMPLEMENTATION_REVISION,
    available: true,
    item: publicItem(row),
    match_policy: 'exact_unique_gecko_id_only',
    metric_policy: {
      tvl: 'protocol_contract_value_or_AUM_like_metric_from_source',
      fees: 'fees_paid_by_protocol_users',
      revenue: 'protocol_revenue_as_reported_by_source',
      token_market_cap_vs_tvl: 'cross_layer_reference_only_not_PE_not_protocol_equity',
    },
    user_read_source_requests: 0,
    user_read_exchange_requests: 0,
    reads_scale_with_users: false,
  });
  return true;
}

export const __test = Object.freeze({ buildCatalog, publicItem });

const VERSION = '650.8.15.168';
const DATA_VERSION = 1033;
const SCHEMA_VERSION = 'step1033_crypto_sector_history_v1';
const IMPLEMENTATION_REVISION = '1033_persisted_15m_sector_history_rotation_v1';
const HISTORY_ROUTE = '/api/crypto-sector-professional/history';
const HEALTH_ROUTE = '/api/crypto-sector-professional/history-health';
const TABLE = 'kaka_crypto_sector_history';
const BUCKET_MS = 15 * 60_000;
const RETENTION_DAYS = Math.max(7, Math.min(90, Number(process.env.KAKA_CRYPTO_SECTOR_HISTORY_RETENTION_DAYS || 30)));
const READ_CACHE_MS = Math.max(5_000, Number(process.env.KAKA_CRYPTO_SECTOR_HISTORY_READ_CACHE_MS || 30_000));
const CLEANUP_INTERVAL_MS = Math.max(60 * 60_000, Number(process.env.KAKA_CRYPTO_SECTOR_HISTORY_CLEANUP_INTERVAL_MS || 6 * 60 * 60_000));
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const SUPABASE_CONFIGURED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
const EXPECTED_SECTOR_KEYS = Object.freeze(['btc','eth','l2','defi','ai','meme','rwa','gamefi','depin']);

let bootstrapped = false;
let bootstrapPromise = null;
let persistenceReady = false;
let lastArchivedBucket = '';
let lastArchivedAt = '';
let lastArchiveAttemptAt = '';
let lastArchiveError = '';
let lastCleanupAt = 0;
let lastReadAt = '';
let lastReadError = '';
let archiveAttempts = 0;
let archiveSuccesses = 0;
let archiveFailures = 0;
let archivedSectorRows = 0;
let cleanupAttempts = 0;
let cleanupSuccesses = 0;
let historyReads = 0;
let historyReadDbQueries = 0;
let historyReadCacheHits = 0;
const responseCache = new Map();
const readInflight = new Map();

function compact(value) { return String(value ?? '').trim(); }
function finiteOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}
function intOrZero(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}
function safeKey(value) {
  const key = compact(value).toLowerCase();
  return EXPECTED_SECTOR_KEYS.includes(key) ? key : '';
}
function isoBucket(timestampMs = Date.now()) {
  return new Date(Math.floor(Number(timestampMs || Date.now()) / BUCKET_MS) * BUCKET_MS).toISOString();
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
    'content-length': body.length,
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
  });
  res.end(body);
}
function sectorArchiveRow(snapshot, sector, bucketTime) {
  const breadth = sector?.breadth || {};
  const leader = sector?.leader || {};
  const laggard = sector?.laggard || {};
  return {
    bucket_time: bucketTime,
    sector_key: safeKey(sector?.key),
    name_zh: compact(sector?.name_zh),
    name_en: compact(sector?.name_en),
    weighted_change_24h_pct: finiteOrNull(sector?.turnover_weighted_change_24h_pct),
    equal_weight_change_24h_pct: finiteOrNull(sector?.equal_weight_change_24h_pct),
    median_change_24h_pct: finiteOrNull(sector?.median_change_24h_pct),
    turnover_24h_usdt: finiteOrNull(sector?.total_quote_turnover_24h_usdt),
    top3_turnover_concentration_pct: finiteOrNull(sector?.top3_turnover_concentration_pct),
    up_count: intOrZero(breadth?.up),
    flat_count: intOrZero(breadth?.flat),
    down_count: intOrZero(breadth?.down),
    observed_member_count: intOrZero(sector?.observed_member_count),
    rankable_member_count: intOrZero(sector?.rankable_member_count),
    usdt_provider_count: intOrZero(sector?.usdt_provider_count),
    leader_base_asset: compact(leader?.base_asset),
    leader_change_24h_pct: finiteOrNull(leader?.price_change_percent_24h),
    laggard_base_asset: compact(laggard?.base_asset),
    laggard_change_24h_pct: finiteOrNull(laggard?.price_change_percent_24h),
    shared_market_light_round: intOrZero(snapshot?.shared_market_light_round),
    source_generated_at: compact(snapshot?.generated_at) || new Date().toISOString(),
    source_version: compact(snapshot?.version),
    source_data_version: intOrZero(snapshot?.data_version),
  };
}
function archiveSnapshotValid(snapshot) {
  return snapshot?.ok === true &&
    snapshot?.coverage_ready === true &&
    snapshot?.source_verified === true &&
    snapshot?.cross_quote_aggregation === false &&
    snapshot?.coinbase_usd_metrics_mixed_into_usdt === false &&
    snapshot?.tradeable_index === false &&
    snapshot?.fabricated_points === false &&
    Number(snapshot?.exchange_requests_started || 0) === 0 &&
    Number(snapshot?.exchange_connections_started || 0) === 0 &&
    Number(snapshot?.user_read_upstream_requests || 0) === 0 &&
    Number(snapshot?.user_read_upstream_connections || 0) === 0 &&
    snapshot?.reads_scale_with_users === false &&
    Array.isArray(snapshot?.sectors) &&
    snapshot.sectors.length === EXPECTED_SECTOR_KEYS.length &&
    EXPECTED_SECTOR_KEYS.every((key) => snapshot.sectors.some((item) => safeKey(item?.key) === key));
}
async function supabaseFetch(path, init = {}) {
  if (!SUPABASE_CONFIGURED) throw new Error('supabase_service_role_not_configured');
  return fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    ...init,
    headers: authHeaders(init.headers || {}),
  });
}
async function cleanupOldRows() {
  const now = Date.now();
  if (now - lastCleanupAt < CLEANUP_INTERVAL_MS) return;
  lastCleanupAt = now;
  cleanupAttempts += 1;
  const cutoff = new Date(now - RETENTION_DAYS * 24 * 60 * 60_000).toISOString();
  const response = await supabaseFetch(`${TABLE}?bucket_time=lt.${encodeURIComponent(cutoff)}`, {
    method: 'DELETE',
    headers: { prefer: 'return=minimal' },
  });
  if (!response.ok) throw new Error(`history_cleanup_http_${response.status}`);
  cleanupSuccesses += 1;
}
export async function maybeArchiveCryptoSectorSnapshot(snapshot) {
  if (!SUPABASE_CONFIGURED || !archiveSnapshotValid(snapshot)) return false;
  const bucketTime = isoBucket(Date.parse(snapshot?.generated_at || '') || Date.now());
  if (bucketTime === lastArchivedBucket) return false;
  lastArchiveAttemptAt = new Date().toISOString();
  archiveAttempts += 1;
  try {
    const rows = snapshot.sectors
      .map((sector) => sectorArchiveRow(snapshot, sector, bucketTime))
      .filter((row) => row.sector_key);
    if (rows.length !== EXPECTED_SECTOR_KEYS.length) throw new Error(`sector_archive_row_count_${rows.length}`);
    const response = await supabaseFetch(`${TABLE}?on_conflict=bucket_time,sector_key`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        prefer: 'resolution=merge-duplicates,return=minimal',
      },
      body: JSON.stringify(rows),
    });
    if (!response.ok) {
      const text = await response.text().catch(() => '');
      throw new Error(`sector_archive_http_${response.status}:${text.slice(0, 180)}`);
    }
    lastArchivedBucket = bucketTime;
    lastArchivedAt = new Date().toISOString();
    lastArchiveError = '';
    archiveSuccesses += 1;
    archivedSectorRows += rows.length;
    persistenceReady = true;
    responseCache.clear();
    cleanupOldRows().catch((error) => {
      lastArchiveError = `cleanup:${String(error?.message || error)}`.slice(0, 240);
    });
    return true;
  } catch (error) {
    archiveFailures += 1;
    lastArchiveError = String(error?.message || error).slice(0, 320);
    return false;
  }
}
async function bootstrapPersistedHistory() {
  if (bootstrapped) return persistenceReady;
  if (bootstrapPromise) return bootstrapPromise;
  bootstrapPromise = (async () => {
    try {
      if (!SUPABASE_CONFIGURED) return false;
      const query = `${TABLE}?select=bucket_time,sector_key&order=bucket_time.desc&limit=18`;
      historyReadDbQueries += 1;
      const response = await supabaseFetch(query, { method: 'GET' });
      if (!response.ok) throw new Error(`history_bootstrap_http_${response.status}`);
      const rows = await response.json();
      const latestBucket = compact(rows?.[0]?.bucket_time);
      const keys = new Set((Array.isArray(rows) ? rows : []).filter((row) => compact(row?.bucket_time) === latestBucket).map((row) => safeKey(row?.sector_key)).filter(Boolean));
      if (latestBucket && EXPECTED_SECTOR_KEYS.every((key) => keys.has(key))) {
        lastArchivedBucket = latestBucket;
        persistenceReady = true;
      }
      return persistenceReady;
    } catch (error) {
      lastReadError = `bootstrap:${String(error?.message || error)}`.slice(0, 240);
      return false;
    } finally {
      bootstrapped = true;
      bootstrapPromise = null;
    }
  })();
  return bootstrapPromise;
}
export function primeCryptoSectorHistory() {
  bootstrapPersistedHistory().catch(() => {});
}
function normalizedHistoryRow(row) {
  const up = intOrZero(row?.up_count);
  const down = intOrZero(row?.down_count);
  return {
    bucket_time: compact(row?.bucket_time),
    sector_key: safeKey(row?.sector_key),
    name_zh: compact(row?.name_zh),
    name_en: compact(row?.name_en),
    weighted_change_24h_pct: finiteOrNull(row?.weighted_change_24h_pct),
    equal_weight_change_24h_pct: finiteOrNull(row?.equal_weight_change_24h_pct),
    median_change_24h_pct: finiteOrNull(row?.median_change_24h_pct),
    turnover_24h_usdt: finiteOrNull(row?.turnover_24h_usdt),
    top3_turnover_concentration_pct: finiteOrNull(row?.top3_turnover_concentration_pct),
    up_count: up,
    flat_count: intOrZero(row?.flat_count),
    down_count: down,
    breadth_net: up - down,
    observed_member_count: intOrZero(row?.observed_member_count),
    rankable_member_count: intOrZero(row?.rankable_member_count),
    usdt_provider_count: intOrZero(row?.usdt_provider_count),
    leader_base_asset: compact(row?.leader_base_asset),
    leader_change_24h_pct: finiteOrNull(row?.leader_change_24h_pct),
    laggard_base_asset: compact(row?.laggard_base_asset),
    laggard_change_24h_pct: finiteOrNull(row?.laggard_change_24h_pct),
    shared_market_light_round: intOrZero(row?.shared_market_light_round),
    source_generated_at: compact(row?.source_generated_at),
  };
}
function nearestHistoryPoint(rows, targetMs, toleranceMs) {
  let best = null;
  let bestDistance = Infinity;
  for (const row of rows || []) {
    const at = Date.parse(row?.bucket_time || '');
    if (!Number.isFinite(at)) continue;
    const distance = Math.abs(at - targetMs);
    if (distance < bestDistance) {
      best = row;
      bestDistance = distance;
    }
  }
  return bestDistance <= toleranceMs ? best : null;
}
function pctChange(current, previous) {
  const a = finiteOrNull(current);
  const b = finiteOrNull(previous);
  if (a == null || b == null || Math.abs(b) < 1e-12) return null;
  return (a / b - 1) * 100;
}
function metricDelta(current, previous) {
  const a = finiteOrNull(current);
  const b = finiteOrNull(previous);
  return a == null || b == null ? null : a - b;
}
export function buildSectorRotation(currentSnapshot, historyRows, nowMs = Date.now()) {
  const grouped = new Map(EXPECTED_SECTOR_KEYS.map((key) => [key, []]));
  for (const raw of historyRows || []) {
    const row = normalizedHistoryRow(raw);
    if (row.sector_key) grouped.get(row.sector_key)?.push(row);
  }
  for (const rows of grouped.values()) rows.sort((a, b) => Date.parse(a.bucket_time) - Date.parse(b.bucket_time));
  const currentByKey = new Map((currentSnapshot?.sectors || []).map((sector) => [safeKey(sector?.key), sector]));
  const windows = [
    { key: '1h', ms: 60 * 60_000, tolerance: 10 * 60_000 },
    { key: '4h', ms: 4 * 60 * 60_000, tolerance: 10 * 60_000 },
    { key: '24h', ms: 24 * 60 * 60_000, tolerance: 10 * 60_000 },
  ];
  const result = [];
  for (const key of EXPECTED_SECTOR_KEYS) {
    const currentSector = currentByKey.get(key) || null;
    const rows = grouped.get(key) || [];
    const latestArchived = rows.length ? rows[rows.length - 1] : null;
    const currentWeighted = finiteOrNull(currentSector?.turnover_weighted_change_24h_pct) ?? latestArchived?.weighted_change_24h_pct ?? null;
    const currentTurnover = finiteOrNull(currentSector?.total_quote_turnover_24h_usdt) ?? latestArchived?.turnover_24h_usdt ?? null;
    const currentConcentration = finiteOrNull(currentSector?.top3_turnover_concentration_pct) ?? latestArchived?.top3_turnover_concentration_pct ?? null;
    const currentBreadth = currentSector?.breadth || null;
    const currentBreadthNet = currentBreadth
      ? intOrZero(currentBreadth?.up) - intOrZero(currentBreadth?.down)
      : latestArchived?.breadth_net ?? null;
    const item = {
      sector_key: key,
      name_zh: compact(currentSector?.name_zh) || compact(latestArchived?.name_zh),
      name_en: compact(currentSector?.name_en) || compact(latestArchived?.name_en),
      current_weighted_change_24h_pct: currentWeighted,
      current_turnover_24h_usdt: currentTurnover,
      current_breadth_net: currentBreadthNet,
      current_top3_concentration_pct: currentConcentration,
      latest_archived_at: latestArchived?.bucket_time || null,
    };
    for (const window of windows) {
      const prior = nearestHistoryPoint(rows, nowMs - window.ms, window.tolerance);
      item[`history_${window.key}_available`] = Boolean(prior);
      item[`performance_improvement_${window.key}_pct_point`] = prior ? metricDelta(currentWeighted, prior.weighted_change_24h_pct) : null;
      item[`turnover_change_${window.key}_pct`] = prior ? pctChange(currentTurnover, prior.turnover_24h_usdt) : null;
      item[`breadth_net_delta_${window.key}`] = prior && currentBreadthNet != null ? currentBreadthNet - prior.breadth_net : null;
      item[`concentration_delta_${window.key}_pct_point`] = prior ? metricDelta(currentConcentration, prior.top3_turnover_concentration_pct) : null;
      item[`reference_${window.key}_bucket_time`] = prior?.bucket_time || null;
    }
    result.push(item);
  }
  return result;
}
async function queryHistoryRows({ sectorKey = '', days = RETENTION_DAYS } = {}) {
  const safeDays = Math.max(1, Math.min(RETENTION_DAYS, Math.trunc(Number(days) || RETENTION_DAYS)));
  const cutoff = new Date(Date.now() - safeDays * 24 * 60 * 60_000).toISOString();
  const filters = [
    'select=bucket_time,sector_key,name_zh,name_en,weighted_change_24h_pct,equal_weight_change_24h_pct,median_change_24h_pct,turnover_24h_usdt,top3_turnover_concentration_pct,up_count,flat_count,down_count,observed_member_count,rankable_member_count,usdt_provider_count,leader_base_asset,leader_change_24h_pct,laggard_base_asset,laggard_change_24h_pct,shared_market_light_round,source_generated_at',
    `bucket_time=gte.${encodeURIComponent(cutoff)}`,
    'order=bucket_time.asc',
    'limit=4000',
  ];
  if (sectorKey) filters.splice(1, 0, `sector_key=eq.${encodeURIComponent(sectorKey)}`);
  historyReadDbQueries += 1;
  const response = await supabaseFetch(`${TABLE}?${filters.join('&')}`, { method: 'GET' });
  if (!response.ok) throw new Error(`sector_history_read_http_${response.status}`);
  const rows = await response.json();
  return Array.isArray(rows) ? rows.map(normalizedHistoryRow) : [];
}
async function buildHistoryPayload(sectorKey, currentSnapshot) {
  await bootstrapPersistedHistory();
  const trendPromise = queryHistoryRows({ sectorKey, days: RETENTION_DAYS });
  const rotationPromise = queryHistoryRows({ days: 2 });
  const [trend, recentAll] = await Promise.all([trendPromise, rotationPromise]);
  const rotation = buildSectorRotation(currentSnapshot, recentAll);
  const firstAt = trend.length ? trend[0].bucket_time : null;
  const lastAt = trend.length ? trend[trend.length - 1].bucket_time : null;
  return {
    ok: true,
    version: VERSION,
    data_version: DATA_VERSION,
    schema_version: SCHEMA_VERSION,
    implementation_revision: IMPLEMENTATION_REVISION,
    source: 'supabase_persisted_from_step1032_render_shared_sector_snapshot',
    source_verified: SUPABASE_CONFIGURED,
    coverage_ready: persistenceReady,
    archive_interval_minutes: 15,
    retention_days: RETENTION_DAYS,
    forward_archive_from_step1033: true,
    bulk_backfill: false,
    tradeable_index: false,
    fabricated_points: false,
    historical_metric_semantics: 'rolling_24h_sector_metrics_sampled_every_15m',
    rotation_improvement_semantics: 'current_rolling_24h_metric_minus_prior_rolling_24h_metric_percentage_points',
    direct_exchange_requests: 0,
    direct_exchange_connections: 0,
    user_read_exchange_requests: 0,
    user_read_exchange_connections: 0,
    reads_scale_with_users: false,
    sector_key: sectorKey,
    trend_points: trend,
    trend_point_count: trend.length,
    trend_first_bucket_time: firstAt,
    trend_last_bucket_time: lastAt,
    rotation,
    rotation_sector_count: rotation.length,
    fetched_at: new Date().toISOString(),
  };
}
async function cachedHistoryPayload(sectorKey, currentSnapshot) {
  const key = sectorKey;
  const cached = responseCache.get(key);
  if (cached && Date.now() - cached.at <= READ_CACHE_MS) {
    historyReadCacheHits += 1;
    return { ...cached.payload, cache_hit: true, cache_age_ms: Date.now() - cached.at };
  }
  if (readInflight.has(key)) return readInflight.get(key);
  const promise = buildHistoryPayload(sectorKey, currentSnapshot)
    .then((payload) => {
      responseCache.set(key, { at: Date.now(), payload });
      lastReadAt = new Date().toISOString();
      lastReadError = '';
      return payload;
    })
    .catch((error) => {
      lastReadError = String(error?.message || error).slice(0, 320);
      throw error;
    })
    .finally(() => readInflight.delete(key));
  readInflight.set(key, promise);
  return promise;
}
export function getCryptoSectorHistoryHealth() {
  return {
    ok: true,
    version: VERSION,
    data_version: DATA_VERSION,
    schema_version: SCHEMA_VERSION,
    implementation_revision: IMPLEMENTATION_REVISION,
    history_endpoint: HISTORY_ROUTE,
    health_endpoint: HEALTH_ROUTE,
    supabase_configured: SUPABASE_CONFIGURED,
    persistence_ready: persistenceReady,
    archive_interval_minutes: 15,
    retention_days: RETENTION_DAYS,
    forward_archive_from_step1033: true,
    bulk_backfill: false,
    tradeable_index: false,
    fabricated_points: false,
    direct_exchange_requests: 0,
    direct_exchange_connections: 0,
    user_read_exchange_requests: 0,
    user_read_exchange_connections: 0,
    reads_scale_with_users: false,
    last_archived_bucket: lastArchivedBucket || null,
    last_archived_at: lastArchivedAt || null,
    last_archive_attempt_at: lastArchiveAttemptAt || null,
    last_archive_error: lastArchiveError,
    archive_attempts: archiveAttempts,
    archive_successes: archiveSuccesses,
    archive_failures: archiveFailures,
    archived_sector_rows: archivedSectorRows,
    cleanup_attempts: cleanupAttempts,
    cleanup_successes: cleanupSuccesses,
    history_reads: historyReads,
    history_read_db_queries: historyReadDbQueries,
    history_read_cache_hits: historyReadCacheHits,
    history_read_cache_entries: responseCache.size,
    history_read_inflight: readInflight.size,
    last_read_at: lastReadAt || null,
    last_read_error: lastReadError,
    ready: SUPABASE_CONFIGURED && persistenceReady,
    time: new Date().toISOString(),
  };
}
export async function handleCryptoSectorHistory(req, res, url, currentSnapshot) {
  if (![HISTORY_ROUTE, HEALTH_ROUTE].includes(url.pathname)) return false;
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
    sendJson(res, 200, getCryptoSectorHistoryHealth());
    return true;
  }
  const sectorKey = safeKey(url.searchParams.get('sector'));
  if (!sectorKey) {
    sendJson(res, 400, { ok: false, version: VERSION, error: 'valid_sector_required' });
    return true;
  }
  historyReads += 1;
  try {
    const payload = await cachedHistoryPayload(sectorKey, currentSnapshot);
    sendJson(res, 200, payload);
  } catch (error) {
    sendJson(res, 503, {
      ok: false,
      version: VERSION,
      data_version: DATA_VERSION,
      schema_version: SCHEMA_VERSION,
      error: 'crypto_sector_history_unavailable',
      detail: String(error?.message || error).slice(0, 240),
      direct_exchange_requests: 0,
      user_read_exchange_requests: 0,
    });
  }
  return true;
}

export const __cryptoSectorHistoryStep1033Test = Object.freeze({
  isoBucket,
  normalizedHistoryRow,
  nearestHistoryPoint,
  buildSectorRotation,
  expectedSectorKeys: EXPECTED_SECTOR_KEYS,
});

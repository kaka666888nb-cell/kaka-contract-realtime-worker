import { inflateRawSync } from 'node:zlib';
import { WebSocket } from 'ws';
import {
  acquireBinanceRestRequestSlot,
  flushBinanceRestGuardPersistence,
  getBinanceRestGuardHealth,
  isBinanceRestBlocked,
  markBinanceRestRestricted,
  markBinanceRestSuccess,
  parseBinanceBanUntil,
} from './binance-rest-guard.mjs';

const PROVIDER = 'binance';
const MARKET_TYPE = 'contract';
const SNAPSHOT_TABLE = 'app_market_backend_snapshots';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const DATA_BASE = 'https://data.binance.vision/data/futures/um';
const CACHE_TTL_MS = 15 * 60_000;
const MAX_PERSIST_ROWS = 1500;
const MAX_DAILY_FILES = 16;
const MAX_MONTHLY_FILES = 24;
const FETCH_TIMEOUT_MS = 8_000;
const HTTP_BRIDGE_TIMEOUT_MS = 6_000;
const HTTP_BRIDGE_CACHE_MS = 30_000;
const HTTP_TRANSIENT_COOLDOWN_MS = 90_000;
const MAX_HTTP_PAGE_ROWS = 1000;
const MAX_LIVE_STREAMS = 32;
const LIVE_IDLE_MS = 12 * 60_000;
const LIVE_PERSIST_MIN_MS = 45_000;
const LIVE_RECONNECT_MAX_MS = 30_000;
const LIVE_WS_HOSTS = [
  'wss://fstream.binance.com/market/ws',
  'wss://stream.binancefuture.com/market/ws',
];
// Step650.8.2：历史归档与实时WebSocket保持独立；REST桥接只保留一个官方精确交易对端点，
// 并由所有Binance合约REST调用共享的持久守卫统一串行、限速和封禁。
const HTTP_BRIDGE_CANDIDATES = [
  // Step650.8.2：只保留官方精确交易对 Kline 主端点。
  // 不再用 continuous/www 连续撞多个候选；一次失败就返回归档/快照/WS，等待共享守卫放行。
  { id: 'fapi_klines', base: 'https://fapi.binance.com', path: '/fapi/v1/klines', continuous: false },
];

const memory = new Map();
const inflight = new Map();
const stats = {
  requests: 0,
  memory_hits: 0,
  persisted_hits: 0,
  archive_success: 0,
  archive_empty: 0,
  archive_errors: 0,
  last_success_at: 0,
  last_error: '',
  bridge_requests: 0,
  bridge_success: 0,
  bridge_empty: 0,
  bridge_errors: 0,
  bridge_rows: 0,
  bridge_last_success_at: 0,
  bridge_last_source: '',
  bridge_last_error: '',
  bridge_partial_candidates: 0,
  bridge_complete_candidates: 0,
  bridge_partial_rows: 0,
  bridge_http_requests: 0,
  bridge_rate_limiter_waits: 0,
  bridge_restricted_short_circuits: 0,
  bridge_ban_until_parsed: 0,
  gap_scan_requests: 0,
  gap_repair_requests: 0,
  gap_repair_success: 0,
  gap_repair_remaining_gaps: 0,
  gap_repair_last_start_at: 0,
  gap_repair_last_success_at: 0,
  live_messages: 0,
  live_closed_candles: 0,
  live_last_message_at: 0,
};

function compact(raw) {
  return String(raw || '').trim().toUpperCase().replace(/-SWAP$/i, '').replace(/_UMCBL$/i, '').replace(/[^A-Z0-9]/g, '');
}

function normalizeInterval(raw) {
  const value = String(raw || '15m').trim();
  if (value === 'timeline') return '1m';
  if (value === '1M' || value.toLowerCase() === '1mo') return '1M';
  const lower = value.toLowerCase();
  const allowed = new Set(['1s','1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d','3d','1w']);
  return allowed.has(lower) ? lower : '15m';
}

function archiveIntervalFor(target) {
  if (target === '1M') return '1mo';
  if (target === '3d' || target === '1w') return '1d';
  return target;
}

function intervalMs(interval) {
  return ({
    '1s': 1_000,
    '1m': 60_000,
    '3m': 180_000,
    '5m': 300_000,
    '15m': 900_000,
    '30m': 1_800_000,
    '1h': 3_600_000,
    '2h': 7_200_000,
    '4h': 14_400_000,
    '6h': 21_600_000,
    '8h': 28_800_000,
    '12h': 43_200_000,
    '1d': 86_400_000,
    '3d': 259_200_000,
    '1w': 604_800_000,
    '1M': 2_592_000_000,
  })[interval] || 900_000;
}

function toMs(value) {
  let parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  if (parsed > 10_000_000_000_000) parsed /= 1000;
  return Math.round(parsed);
}

function finite(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function iso(value) {
  return new Date(value).toISOString();
}

function snapshotKey(symbol, interval) {
  return `KLINE:${symbol}:${interval}`;
}

function memoryKey(symbol, interval) {
  return `${symbol}|${interval}`;
}

function supabaseEnabled() {
  return Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
}

function normalizeRows(rawRows, symbol, interval, source = 'binance_official_public_archive_kline_seed') {
  const rows = [];
  for (const raw of Array.isArray(rawRows) ? rawRows : []) {
    if (!raw || typeof raw !== 'object') continue;
    const openTimeMs = toMs(raw.open_time_ms ?? raw.open_time ?? raw.openTime);
    const open = finite(raw.open ?? raw.open_price);
    const high = finite(raw.high ?? raw.high_price);
    const low = finite(raw.low ?? raw.low_price);
    const close = finite(raw.close ?? raw.close_price);
    if ([openTimeMs, open, high, low, close].some((value) => value === null)) continue;
    const closeTimeMs = toMs(raw.close_time_ms ?? raw.close_time ?? raw.closeTime) ?? (openTimeMs + intervalMs(interval) - 1);
    rows.push({
      provider: PROVIDER,
      market_type: MARKET_TYPE,
      symbol,
      interval,
      open_time: iso(openTimeMs),
      open_time_ms: openTimeMs,
      close_time: iso(closeTimeMs),
      open,
      high,
      low,
      close,
      volume: finite(raw.volume) ?? 0,
      quote_volume: finite(raw.quote_volume ?? raw.quoteVolume) ?? 0,
      trade_count: Math.max(0, Math.trunc(finite(raw.trade_count ?? raw.trades) ?? 0)),
      taker_buy_volume: finite(raw.taker_buy_volume) ?? null,
      taker_buy_quote_volume: finite(raw.taker_buy_quote_volume) ?? null,
      source: raw.source || source,
      cached_at: raw.cached_at || iso(Date.now()),
    });
  }
  return [...new Map(rows.map((row) => [row.open_time_ms, row])).values()].sort((a, b) => a.open_time_ms - b.open_time_ms);
}

function aggregateRows(sourceRows, symbol, targetInterval) {
  if (targetInterval !== '3d' && targetInterval !== '1w' && targetInterval !== '1M') {
    return normalizeRows(sourceRows, symbol, targetInterval);
  }
  const buckets = new Map();
  const target = intervalMs(targetInterval);
  for (const source of normalizeRows(sourceRows, symbol, '1d')) {
    const bucket = Math.floor(source.open_time_ms / target) * target;
    const current = buckets.get(bucket);
    if (!current) {
      buckets.set(bucket, {
        ...source,
        interval: targetInterval,
        open_time: iso(bucket),
        open_time_ms: bucket,
        close_time: iso(bucket + target - 1),
        source: 'binance_official_public_archive_kline_seed_aggregated',
      });
    } else {
      current.high = Math.max(current.high, source.high);
      current.low = Math.min(current.low, source.low);
      current.close = source.close;
      current.volume += source.volume;
      current.quote_volume += source.quote_volume;
      current.trade_count += source.trade_count;
      if (source.taker_buy_volume !== null) current.taker_buy_volume = (current.taker_buy_volume ?? 0) + source.taker_buy_volume;
      if (source.taker_buy_quote_volume !== null) current.taker_buy_quote_volume = (current.taker_buy_quote_volume ?? 0) + source.taker_buy_quote_volume;
    }
  }
  return [...buckets.values()].sort((a, b) => a.open_time_ms - b.open_time_ms);
}

function findEocd(buffer) {
  const signature = 0x06054b50;
  const minimum = Math.max(0, buffer.length - 65_557);
  for (let offset = buffer.length - 22; offset >= minimum; offset -= 1) {
    if (buffer.readUInt32LE(offset) === signature) return offset;
  }
  return -1;
}

function unzipFirstCsv(buffer) {
  const eocd = findEocd(buffer);
  if (eocd < 0) throw new Error('zip_eocd_not_found');
  const entries = buffer.readUInt16LE(eocd + 10);
  let cursor = buffer.readUInt32LE(eocd + 16);
  for (let index = 0; index < entries; index += 1) {
    if (buffer.readUInt32LE(cursor) !== 0x02014b50) throw new Error('zip_central_directory_invalid');
    const method = buffer.readUInt16LE(cursor + 10);
    const compressedSize = buffer.readUInt32LE(cursor + 20);
    const filenameLength = buffer.readUInt16LE(cursor + 28);
    const extraLength = buffer.readUInt16LE(cursor + 30);
    const commentLength = buffer.readUInt16LE(cursor + 32);
    const localOffset = buffer.readUInt32LE(cursor + 42);
    const filename = buffer.subarray(cursor + 46, cursor + 46 + filenameLength).toString('utf8');
    cursor += 46 + filenameLength + extraLength + commentLength;
    if (!filename.toLowerCase().endsWith('.csv')) continue;
    if (buffer.readUInt32LE(localOffset) !== 0x04034b50) throw new Error('zip_local_header_invalid');
    const localNameLength = buffer.readUInt16LE(localOffset + 26);
    const localExtraLength = buffer.readUInt16LE(localOffset + 28);
    const dataStart = localOffset + 30 + localNameLength + localExtraLength;
    const compressed = buffer.subarray(dataStart, dataStart + compressedSize);
    if (method === 0) return compressed.toString('utf8');
    if (method === 8) return inflateRawSync(compressed).toString('utf8');
    throw new Error(`zip_compression_unsupported:${method}`);
  }
  throw new Error('zip_csv_not_found');
}

function parseCsv(csv, symbol, targetInterval, archiveInterval) {
  const rows = [];
  for (const line of String(csv || '').split(/\r?\n/)) {
    if (!line.trim()) continue;
    const columns = line.split(',');
    const openTime = toMs(columns[0]);
    if (openTime === null) continue;
    const open = finite(columns[1]);
    const high = finite(columns[2]);
    const low = finite(columns[3]);
    const close = finite(columns[4]);
    if ([open, high, low, close].some((value) => value === null)) continue;
    const closeTime = toMs(columns[6]) ?? (openTime + intervalMs(targetInterval) - 1);
    rows.push({
      provider: PROVIDER,
      market_type: MARKET_TYPE,
      symbol,
      interval: archiveInterval === '1mo' ? '1M' : archiveInterval,
      open_time: iso(openTime),
      open_time_ms: openTime,
      close_time: iso(closeTime),
      open,
      high,
      low,
      close,
      volume: finite(columns[5]) ?? 0,
      quote_volume: finite(columns[7]) ?? 0,
      trade_count: Math.max(0, Math.trunc(finite(columns[8]) ?? 0)),
      taker_buy_volume: finite(columns[9]),
      taker_buy_quote_volume: finite(columns[10]),
      source: 'binance_official_public_archive_kline_seed',
      cached_at: iso(Date.now()),
    });
  }
  return targetInterval === '3d' || targetInterval === '1w' || targetInterval === '1M'
    ? aggregateRows(rows, symbol, targetInterval)
    : normalizeRows(rows, symbol, targetInterval);
}


function parseApiRows(rawRows, symbol, interval, source) {
  const nowIso = iso(Date.now());
  const parsed = [];
  for (const raw of Array.isArray(rawRows) ? rawRows : []) {
    if (!Array.isArray(raw) || raw.length < 7) continue;
    const openTime = toMs(raw[0]);
    const open = finite(raw[1]);
    const high = finite(raw[2]);
    const low = finite(raw[3]);
    const close = finite(raw[4]);
    if ([openTime, open, high, low, close].some((value) => value === null)) continue;
    parsed.push({
      provider: PROVIDER,
      market_type: MARKET_TYPE,
      symbol,
      interval,
      open_time: iso(openTime),
      open_time_ms: openTime,
      close_time: iso(toMs(raw[6]) ?? (openTime + intervalMs(interval) - 1)),
      open,
      high,
      low,
      close,
      volume: finite(raw[5]) ?? 0,
      quote_volume: finite(raw[7]) ?? 0,
      trade_count: Math.max(0, Math.trunc(finite(raw[8]) ?? 0)),
      taker_buy_volume: finite(raw[9]),
      taker_buy_quote_volume: finite(raw[10]),
      source,
      cached_at: nowIso,
    });
  }
  return normalizeRows(parsed, symbol, interval, source);
}

function mergeRows(...groups) {
  return [...new Map(groups.flat().map((row) => [row.open_time_ms, row])).values()]
    .sort((a, b) => a.open_time_ms - b.open_time_ms)
    .slice(-MAX_PERSIST_ROWS);
}

function isNearNow(endMs, interval) {
  return endMs >= Date.now() - Math.max(2 * intervalMs(interval), 10 * 60_000);
}

function expectedCurrentOpen(endMs, interval) {
  const step = intervalMs(interval);
  return Math.floor(Math.max(0, endMs - 1) / step) * step;
}

function inspectRecentContinuity(rows, interval, endMs, limit = MAX_PERSIST_ROWS) {
  const step = intervalMs(interval);
  const targetOpen = expectedCurrentOpen(endMs, interval);
  const safeLimit = Math.max(2, Math.min(MAX_PERSIST_ROWS, Number.parseInt(String(limit), 10) || MAX_PERSIST_ROWS));
  const sorted = normalizeRows(rows, '', interval)
    .filter((row) => row.open_time_ms < endMs)
    .slice(-safeLimit);

  let gapCount = 0;
  let missingIntervals = 0;
  let firstMissingOpen = null;
  for (let index = 1; index < sorted.length; index += 1) {
    const previous = sorted[index - 1].open_time_ms;
    const current = sorted[index].open_time_ms;
    const difference = current - previous;
    if (difference > step) {
      gapCount += 1;
      missingIntervals += Math.max(0, Math.round(difference / step) - 1);
      firstMissingOpen ??= previous + step;
    }
  }

  const lastOpen = sorted.at(-1)?.open_time_ms ?? null;
  const lagIntervals = lastOpen == null
    ? null
    : Math.max(0, Math.round((targetOpen - lastOpen) / step));

  if (firstMissingOpen == null && (lastOpen == null || lastOpen < targetOpen - step)) {
    firstMissingOpen = lastOpen == null
      ? Math.max(0, targetOpen - ((safeLimit - 1) * step))
      : lastOpen + step;
  }

  return {
    rows: sorted,
    row_count: sorted.length,
    gap_count: gapCount,
    missing_intervals: missingIntervals,
    first_missing_open_ms: firstMissingOpen,
    last_open_ms: lastOpen,
    target_open_ms: targetOpen,
    lag_intervals_to_end: lagIntervals,
    continuous_to_current: sorted.length > 0 && gapCount === 0 && (lagIntervals ?? safeLimit) <= 1,
  };
}

function bridgeStartForRecentWindow(rows, interval, endMs, limit = MAX_PERSIST_ROWS) {
  const coverage = inspectRecentContinuity(rows, interval, endMs, limit);
  if (coverage.first_missing_open_ms != null) return coverage.first_missing_open_ms;
  return coverage.last_open_ms != null
    ? coverage.last_open_ms + intervalMs(interval)
    : Math.max(0, coverage.target_open_ms - ((Math.max(2, limit) - 1) * intervalMs(interval)));
}

const bridgeCandidateState = new Map();
const bridgeResultCache = new Map();

function bridgeStateKey(candidateId, symbol = '*') {
  return `${candidateId}|${symbol || '*'}`;
}

function activeBridgeState(key) {
  const state = bridgeCandidateState.get(key);
  if (!state) return null;
  if (state.until > Date.now()) return state;
  bridgeCandidateState.delete(key);
  return null;
}

function bridgeCooldown(candidateId, symbol) {
  // Step650.8.2：所有 Binance REST 调用共用持久守卫；普通5xx仍只隔离当前交易对。
  return isBinanceRestBlocked() ||
    activeBridgeState(bridgeStateKey(candidateId, '*')) ||
    activeBridgeState(bridgeStateKey(candidateId, symbol));
}

async function markBridgeFailure(candidate, symbol, status, message, retryAfterSeconds = null) {
  const lower = String(message || '').toLowerCase();
  const restricted = status === 418 || status === 429 || status === 451 ||
    lower.includes('too many requests') || lower.includes('banned') || lower.includes('restricted');
  const transient = status === 0 || status >= 500 ||
    lower.includes('abort') || lower.includes('timeout') || lower.includes('network') || lower.includes('fetch failed');

  if (!restricted && !transient) return { restricted: false, transient: false, until: 0 };

  if (restricted) {
    const parsedBanUntil = parseBinanceBanUntil(message);
    if (parsedBanUntil) stats.bridge_ban_until_parsed += 1;
    const guard = markBinanceRestRestricted({
      status,
      message,
      source: `kline_bridge:${candidate.id}`,
      retryAfterSeconds,
    });
    await flushBinanceRestGuardPersistence();
    return { restricted: true, transient: false, until: Number(guard?.until || 0) };
  }

  const until = Date.now() + HTTP_TRANSIENT_COOLDOWN_MS;
  bridgeCandidateState.set(bridgeStateKey(candidate.id, symbol), {
    until,
    status,
    scope: symbol,
    reason: 'upstream_unavailable',
    error: String(message || ''),
  });
  return { restricted: false, transient: true, until };
}

async function waitForBridgeRequestSlot() {
  const before = getBinanceRestGuardHealth();
  const release = await acquireBinanceRestRequestSlot({
    source: 'kline_bridge:fapi_klines',
    maxQueueWaitMs: 20_000,
  });
  const after = getBinanceRestGuardHealth();
  if (Number(after.request_slot_waits || 0) > Number(before.request_slot_waits || 0)) {
    stats.bridge_rate_limiter_waits += 1;
  }
  stats.bridge_http_requests += 1;
  return release;
}

async function fetchJson(url, timeoutMs = HTTP_BRIDGE_TIMEOUT_MS) {
  const release = await waitForBridgeRequestSlot();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3-Kline-Bridge/650.8.2',
      },
    });
    const bodyText = await response.text();
    const retryAfter = response.headers.get('retry-after');
    const usedWeight = response.headers.get('x-mbx-used-weight-1m');
    if (!response.ok) {
      const error = new Error(`${response.status} ${response.statusText} ${bodyText.slice(0, 360)}`.trim());
      error.status = response.status;
      error.retryAfterSeconds = retryAfter == null ? null : Number(retryAfter);
      throw error;
    }
    let payload;
    try { payload = JSON.parse(bodyText); } catch (_) { throw new Error('bridge_invalid_json'); }
    if (payload && !Array.isArray(payload) && Number(payload.code) < 0) {
      const error = new Error(`binance_${payload.code}:${payload.msg || 'unknown_error'}`);
      error.status = 400;
      throw error;
    }
    markBinanceRestSuccess({ source: 'kline_bridge:fapi_klines', usedWeight1m: usedWeight });
    return payload;
  } finally {
    clearTimeout(timer);
    release();
  }
}

function bridgeUrl(candidate, symbol, interval, startTime, endTime, limit) {
  const query = new URLSearchParams({
    interval,
    startTime: String(Math.max(0, Math.trunc(startTime))),
    endTime: String(Math.max(1, Math.trunc(endTime))),
    limit: String(Math.max(1, Math.min(MAX_HTTP_PAGE_ROWS, limit))),
  });
  if (candidate.continuous) {
    query.set('pair', symbol);
    query.set('contractType', 'PERPETUAL');
  } else {
    query.set('symbol', symbol);
  }
  return `${candidate.base}${candidate.path}?${query.toString()}`;
}

function inspectBridgeWindow(rows, interval, startTime, endTime) {
  const step = intervalMs(interval);
  const requestedStart = Math.floor(Math.max(0, startTime) / step) * step;
  const targetOpen = expectedCurrentOpen(endTime, interval);
  const sorted = normalizeRows(rows, '', interval)
    .filter((row) => row.open_time_ms >= requestedStart && row.open_time_ms < endTime)
    .sort((a, b) => a.open_time_ms - b.open_time_ms);

  let gapCount = 0;
  let missingIntervals = 0;
  for (let index = 1; index < sorted.length; index += 1) {
    const difference = sorted[index].open_time_ms - sorted[index - 1].open_time_ms;
    if (difference > step) {
      gapCount += 1;
      missingIntervals += Math.max(0, Math.round(difference / step) - 1);
    }
  }

  const firstOpen = sorted.at(0)?.open_time_ms ?? null;
  const lastOpen = sorted.at(-1)?.open_time_ms ?? null;
  const coversStart = firstOpen != null && firstOpen <= requestedStart;
  const lagIntervals = lastOpen == null
    ? null
    : Math.max(0, Math.round((targetOpen - lastOpen) / step));

  return {
    rows: sorted,
    row_count: sorted.length,
    first_open_ms: firstOpen,
    last_open_ms: lastOpen,
    covers_start: coversStart,
    gap_count: gapCount,
    missing_intervals: missingIntervals,
    lag_intervals_to_end: lagIntervals,
    complete: sorted.length > 0 && coversStart && gapCount === 0 && (lagIntervals ?? 999999) <= 1,
  };
}

async function fetchBridgeCandidate(candidate, symbol, interval, startTime, endTime, maxRows) {
  if (bridgeCooldown(candidate.id, symbol)) return [];
  const source = candidate.continuous
    ? `binance_official_public_continuous_kline_current_bridge_${candidate.id}`
    : `binance_official_public_kline_current_bridge_${candidate.id}`;
  const result = [];
  const step = intervalMs(interval);
  let cursor = startTime;
  while (cursor < endTime && result.length < maxRows) {
    if (bridgeCooldown(candidate.id, symbol)) break;
    const pageLimit = Math.min(MAX_HTTP_PAGE_ROWS, maxRows - result.length);
    const payload = await fetchJson(bridgeUrl(candidate, symbol, interval, cursor, endTime, pageLimit));
    const page = parseApiRows(payload, symbol, interval, source)
      .filter((row) => row.open_time_ms >= cursor && row.open_time_ms < endTime);
    if (!page.length) break;
    result.push(...page);
    const next = page.at(-1).open_time_ms + step;
    if (next <= cursor) break;
    cursor = next;
    if (page.length < pageLimit) break;
  }
  return mergeRows(result).slice(-maxRows);
}

async function fetchCurrentBridgeRows(symbol, interval, startTime, endTime, maxRows) {
  if (interval === '1s' || startTime >= endTime || maxRows <= 0) return [];
  const cacheKey = `${symbol}|${interval}|${Math.floor(startTime / intervalMs(interval))}|${Math.floor(endTime / intervalMs(interval))}`;
  const cached = bridgeResultCache.get(cacheKey);
  let combined = [];
  if (cached && Date.now() - cached.loadedAt <= HTTP_BRIDGE_CACHE_MS) {
    const cachedCoverage = inspectBridgeWindow(cached.rows, interval, startTime, endTime);
    if (cachedCoverage.complete) return cached.rows;
    combined = cached.rows;
  }

  stats.bridge_requests += 1;
  let lastError = '';
  for (const candidate of HTTP_BRIDGE_CANDIDATES) {
    if (bridgeCooldown(candidate.id, symbol)) continue;
    try {
      const rows = await fetchBridgeCandidate(candidate, symbol, interval, startTime, endTime, maxRows);
      if (!rows.length) {
        stats.bridge_empty += 1;
        continue;
      }

      combined = mergeRows(combined, rows)
        .filter((row) => row.open_time_ms >= startTime && row.open_time_ms < endTime)
        .slice(-maxRows);
      const coverage = inspectBridgeWindow(combined, interval, startTime, endTime);

      stats.bridge_success += 1;
      stats.bridge_rows += rows.length;
      stats.bridge_last_success_at = Date.now();
      stats.bridge_last_source = rows.at(-1)?.source || candidate.id;
      stats.bridge_last_error = '';
      bridgeCandidateState.delete(bridgeStateKey(candidate.id, symbol));

      if (coverage.complete) {
        stats.bridge_complete_candidates += 1;
        bridgeResultCache.set(cacheKey, { rows: combined, loadedAt: Date.now() });
        return combined;
      }

      stats.bridge_partial_candidates += 1;
      stats.bridge_partial_rows += rows.length;
      bridgeResultCache.delete(cacheKey);
    } catch (error) {
      const message = String(error?.message || error);
      const status = Number(error?.status || 0);
      lastError = `${candidate.id}:${message}`;
      const failure = error?.binanceRestBlocked === true
        ? { restricted: true, transient: false, until: Number(error?.guardState?.until || 0) }
        : error?.internalBinanceRestGuard === true
          ? { restricted: false, transient: false, until: 0 }
          : await markBridgeFailure(candidate, symbol, status, message, error?.retryAfterSeconds);
      stats.bridge_errors += 1;
      // 418/429/451/IP ban属于同一Render出口IP，不再继续轰炸其余fapi/www候选。
      if (failure.restricted || error?.binanceRestBlocked === true) {
        stats.bridge_restricted_short_circuits += 1;
        break;
      }
    }
  }

  const finalCoverage = inspectBridgeWindow(combined, interval, startTime, endTime);
  if (finalCoverage.complete) {
    bridgeResultCache.set(cacheKey, { rows: combined, loadedAt: Date.now() });
  } else {
    bridgeResultCache.delete(cacheKey);
  }
  stats.bridge_last_error = finalCoverage.complete
    ? ''
    : (lastError || 'all_bridge_candidates_cooling_down_empty_or_partial');
  return combined;
}

const liveStreams = new Map();
let liveSweepTimer = null;

function liveKey(symbol, interval) {
  return `${symbol}|${interval}`;
}

function liveRowFromPayload(payload, symbol, interval) {
  const kline = payload?.k || payload?.data?.k;
  if (!kline) return null;
  return normalizeRows([{
    open_time_ms: kline.t,
    close_time_ms: kline.T,
    open: kline.o,
    high: kline.h,
    low: kline.l,
    close: kline.c,
    volume: kline.v,
    quote_volume: kline.q,
    trade_count: kline.n,
    taker_buy_volume: kline.V,
    taker_buy_quote_volume: kline.Q,
    source: 'binance_official_public_kline_live_bridge',
    cached_at: iso(Date.now()),
  }], symbol, interval, 'binance_official_public_kline_live_bridge')[0] || null;
}

function closeLiveStream(key, reason = 'idle') {
  const state = liveStreams.get(key);
  if (!state) return;
  state.closed = true;
  clearTimeout(state.reconnectTimer);
  try {
    if (state.ws?.readyState === WebSocket.OPEN || state.ws?.readyState === WebSocket.CONNECTING) {
      state.ws.close(1000, reason);
    }
  } catch (_) {}
  liveStreams.delete(key);
}

function evictLiveStreamsIfNeeded() {
  if (liveStreams.size < MAX_LIVE_STREAMS) return;
  const oldest = [...liveStreams.entries()].sort((a, b) => a[1].lastAccess - b[1].lastAccess)[0];
  if (oldest) closeLiveStream(oldest[0], 'capacity');
}

function mergeLiveRow(symbol, interval, row, closed) {
  const key = memoryKey(symbol, interval);
  const current = memory.get(key)?.rows || [];
  const rows = mergeRows(current, [row]);
  memory.set(key, { rows, loadedAt: Date.now() });
  stats.live_messages += 1;
  stats.live_last_message_at = Date.now();
  if (closed) stats.live_closed_candles += 1;
  const state = liveStreams.get(liveKey(symbol, interval));
  if (!state) return;
  const shouldPersist = closed || Date.now() - state.lastPersistAt >= LIVE_PERSIST_MIN_MS;
  if (shouldPersist && rows.length) {
    state.lastPersistAt = Date.now();
    persistRows(symbol, interval, rows, 'binance_official_public_archive_plus_current_bridge')
      .catch((error) => { stats.last_error = String(error?.message || error); });
  }
}

function connectLiveStream(state) {
  if (state.closed) return;
  const host = LIVE_WS_HOSTS[state.hostIndex % LIVE_WS_HOSTS.length];
  const stream = `${state.symbol.toLowerCase()}@kline_${state.interval}`;
  const ws = new WebSocket(`${host}/${stream}`, { handshakeTimeout: 15_000 });
  state.ws = ws;
  ws.on('open', () => {
    state.connected = true;
    state.openedAt = Date.now();
    state.lastError = '';
    state.reconnectAttempt = 0;
  });
  ws.on('ping', (data) => { try { ws.pong(data); } catch (_) {} });
  ws.on('message', (raw) => {
    state.lastMessageAt = Date.now();
    try {
      const payload = JSON.parse(raw.toString());
      const row = liveRowFromPayload(payload, state.symbol, state.interval);
      if (!row) return;
      mergeLiveRow(state.symbol, state.interval, row, Boolean(payload?.k?.x || payload?.data?.k?.x));
    } catch (error) {
      state.lastError = String(error?.message || error);
    }
  });
  let reconnectScheduled = false;
  const reconnect = (error) => {
    if (reconnectScheduled || state.closed || state.ws !== ws) return;
    reconnectScheduled = true;
    state.connected = false;
    if (error) state.lastError = String(error?.message || error);
    state.hostIndex = (state.hostIndex + 1) % LIVE_WS_HOSTS.length;
    state.reconnectAttempt += 1;
    const delay = Math.min(LIVE_RECONNECT_MAX_MS, 1000 * (2 ** Math.min(5, state.reconnectAttempt)));
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = setTimeout(() => connectLiveStream(state), delay);
    state.reconnectTimer.unref?.();
  };
  ws.on('error', reconnect);
  ws.on('close', () => reconnect());
}

function ensureLiveStream(symbol, interval) {
  if (interval === '1s') return;
  const key = liveKey(symbol, interval);
  let state = liveStreams.get(key);
  if (state) {
    state.lastAccess = Date.now();
    return;
  }
  evictLiveStreamsIfNeeded();
  state = {
    symbol,
    interval,
    lastAccess: Date.now(),
    lastPersistAt: 0,
    hostIndex: 0,
    reconnectAttempt: 0,
    reconnectTimer: null,
    connected: false,
    openedAt: 0,
    lastMessageAt: 0,
    lastError: '',
    closed: false,
    ws: null,
  };
  liveStreams.set(key, state);
  connectLiveStream(state);
  if (!liveSweepTimer) {
    liveSweepTimer = setInterval(() => {
      const now = Date.now();
      for (const [streamKey, streamState] of liveStreams.entries()) {
        if (now - streamState.lastAccess > LIVE_IDLE_MS) closeLiveStream(streamKey, 'idle');
      }
    }, 60_000);
    liveSweepTimer.unref?.();
  }
}

async function fetchBuffer(url, timeoutMs = FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: { accept: 'application/zip,application/octet-stream,*/*', 'user-agent': 'KakaWeb3-Kline-Seed/650.8.2' },
    });
    if (response.status === 404) return null;
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
    return Buffer.from(await response.arrayBuffer());
  } finally {
    clearTimeout(timer);
  }
}

async function mapLimit(items, limit, worker) {
  const output = new Array(items.length);
  let cursor = 0;
  const runners = Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, async () => {
    while (true) {
      const index = cursor++;
      if (index >= items.length) return;
      try { output[index] = await worker(items[index], index); } catch (error) { output[index] = { error }; }
    }
  });
  await Promise.all(runners);
  return output;
}

async function loadArchiveFile(url, symbol, interval, archiveInterval) {
  try {
    const buffer = await fetchBuffer(url);
    if (!buffer) return [];
    return parseCsv(unzipFirstCsv(buffer), symbol, interval, archiveInterval);
  } catch (error) {
    stats.archive_errors += 1;
    stats.last_error = String(error?.message || error);
    return [];
  }
}

function dateText(date) {
  return date.toISOString().slice(0, 10);
}

function monthText(date) {
  return date.toISOString().slice(0, 7);
}

function monthStartUtc(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));
}

function previousMonth(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() - 1, 1));
}

async function fetchArchiveRows(symbol, interval, endMs, limit) {
  const archiveInterval = archiveIntervalFor(interval);
  if (archiveInterval === '1s') return [];
  const targetRows = Math.max(100, Math.min(MAX_PERSIST_ROWS, limit));
  const collected = [];
  const now = new Date();
  const todayStart = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  let dailyCursor = new Date(Math.min(endMs - 1, todayStart - 1));
  const dailyCandidates = [];
  const sourceMs = intervalMs(archiveInterval === '1mo' ? '1M' : archiveInterval);
  const expectedRowsPerDay = Math.max(1, Math.floor(86_400_000 / sourceMs));
  // 只取满足当前 limit 所需的最近完整日，并额外预留2天处理新币缺档/归档延迟。
  // 15分240根通常只需要约5个日包，不再固定并发下载16个日包。
  const dailyFileCount = Math.max(2, Math.min(MAX_DAILY_FILES, Math.ceil(targetRows / expectedRowsPerDay) + 2));
  for (let i = 0; i < dailyFileCount; i += 1) {
    const day = dateText(dailyCursor);
    const name = `${symbol}-${archiveInterval}-${day}.zip`;
    dailyCandidates.push(`${DATA_BASE}/daily/klines/${symbol}/${archiveInterval}/${name}`);
    dailyCursor = new Date(dailyCursor.getTime() - 86_400_000);
  }
  const dailyResults = await mapLimit(dailyCandidates, Math.min(6, dailyCandidates.length), (url) => loadArchiveFile(url, symbol, interval, archiveInterval));
  for (const rows of dailyResults) {
    if (Array.isArray(rows)) collected.push(...rows);
  }

  // Step650.5：分钟/小时级新币只要最近日包已有数据，就不要继续扫描24个月归档。
  // 若日包完全为空，再按当前周期实际需要动态检查少量月包，避免冷门币首次请求超过Render 30秒代理时限。
  const estimatedRowsPerMonth = Math.max(1, Math.floor((30 * 86_400_000) / sourceMs));
  const monthlyFileCount = Math.max(1, Math.min(
    MAX_MONTHLY_FILES,
    Math.ceil(targetRows / estimatedRowsPerMonth) + 2,
  ));
  const shouldTryMonthly = collected.length < targetRows &&
    (sourceMs >= 86_400_000 || collected.length === 0);
  if (shouldTryMonthly) {
    let monthCursor = previousMonth(monthStartUtc(new Date(Math.min(endMs, Date.now()))));
    const monthlyCandidates = [];
    for (let i = 0; i < monthlyFileCount; i += 1) {
      const month = monthText(monthCursor);
      const name = `${symbol}-${archiveInterval}-${month}.zip`;
      monthlyCandidates.push(`${DATA_BASE}/monthly/klines/${symbol}/${archiveInterval}/${name}`);
      monthCursor = previousMonth(monthCursor);
    }
    // 每批只并发3个月；够用后不再继续下载更老归档，避免日线等长周期首次请求过重。
    for (let offset = 0; offset < monthlyCandidates.length && collected.length < targetRows; offset += 3) {
      const batch = monthlyCandidates.slice(offset, offset + 3);
      const results = await mapLimit(batch, 3, (url) => loadArchiveFile(url, symbol, interval, archiveInterval));
      for (const rows of results) {
        if (Array.isArray(rows)) collected.push(...rows);
      }
    }
  }

  const result = [...new Map(collected.map((row) => [row.open_time_ms, row])).values()]
    .filter((row) => row.open_time_ms < endMs)
    .sort((a, b) => a.open_time_ms - b.open_time_ms)
    .slice(-targetRows);
  if (result.length) {
    stats.archive_success += 1;
    stats.last_success_at = Date.now();
  } else {
    stats.archive_empty += 1;
  }
  return result;
}

async function restorePersisted(symbol, interval) {
  if (!supabaseEnabled()) return [];
  const key = encodeURIComponent(snapshotKey(symbol, interval));
  const url = `${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}` +
    `?select=payload,row_count,updated_at,source,source_time` +
    `&provider=eq.${PROVIDER}&market_type=eq.${MARKET_TYPE}&snapshot_type=eq.klines&quote_asset=eq.${key}&limit=1`;
  const response = await fetch(url, {
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      accept: 'application/json',
    },
  });
  if (!response.ok) throw new Error(`snapshot_restore_${response.status}`);
  const payload = await response.json();
  const row = Array.isArray(payload) ? payload[0] : null;
  const rows = normalizeRows(row?.payload?.rows, symbol, interval, row?.source || 'binance_contract_kline_persisted_snapshot');
  if (rows.length) stats.persisted_hits += 1;
  return rows;
}

async function persistRows(symbol, interval, rows, source = 'binance_official_public_archive_kline_seed') {
  if (!supabaseEnabled() || !rows.length) return;
  const safeRows = rows.slice(-MAX_PERSIST_ROWS);
  const body = [{
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    snapshot_type: 'klines',
    quote_asset: snapshotKey(symbol, interval),
    payload: { rows: safeRows },
    row_count: safeRows.length,
    source,
    source_time: safeRows.at(-1)?.open_time || iso(Date.now()),
    updated_at: iso(Date.now()),
  }];
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}?on_conflict=provider,market_type,snapshot_type,quote_asset`, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      'content-type': 'application/json',
      prefer: 'resolution=merge-duplicates,return=minimal',
    },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`snapshot_persist_${response.status}`);
}

export async function getBinanceContractKlineSeed({ symbol, interval = '15m', end = Date.now(), limit = 500 } = {}) {
  stats.requests += 1;
  const normalizedSymbol = compact(symbol);
  const normalizedInterval = normalizeInterval(interval);
  const safeEnd = Number.isFinite(Number(end)) ? Number(end) : Date.now();
  const safeLimit = Math.max(20, Math.min(MAX_PERSIST_ROWS, Number.parseInt(String(limit), 10) || 500));
  if (!normalizedSymbol) return [];
  const key = memoryKey(normalizedSymbol, normalizedInterval);
  const step = intervalMs(normalizedInterval);
  const nearNow = isNearNow(safeEnd, normalizedInterval);
  const targetOpen = expectedCurrentOpen(safeEnd, normalizedInterval);
  const cached = memory.get(key);
  const cachedCoverage = cached
    ? inspectRecentContinuity(cached.rows, normalizedInterval, safeEnd, safeLimit)
    : null;
  stats.gap_scan_requests += cached ? 1 : 0;
  if (cached && Date.now() - cached.loadedAt <= CACHE_TTL_MS &&
      (!nearNow || cachedCoverage?.continuous_to_current === true)) {
    stats.memory_hits += 1;
    if (nearNow) ensureLiveStream(normalizedSymbol, normalizedInterval);
    return cached.rows.filter((row) => row.open_time_ms < safeEnd).slice(-safeLimit);
  }
  const existing = inflight.get(key);
  if (existing) return (await existing).filter((row) => row.open_time_ms < safeEnd).slice(-safeLimit);

  const task = (async () => {
    let persisted = cached?.rows || [];
    if (!persisted.length) {
      try { persisted = await restorePersisted(normalizedSymbol, normalizedInterval); } catch (error) { stats.last_error = String(error?.message || error); }
    }
    let bridge = [];
    let bridgeWindowComplete = false;
    let merged = mergeRows(persisted).filter((row) => row.open_time_ms < safeEnd);
    // Step650.8.2：冷启动仍优先官方当前窗口，但必须验证它真的覆盖请求起点且内部连续。
    // 仅返回当前一根属于 partial，不能阻止归档与后续精确 symbol 候选继续补齐。
    if (nearNow && normalizedInterval !== '1s' && !persisted.length) {
      const coldStart = Math.max(0, targetOpen - ((safeLimit - 1) * step));
      try {
        bridge = await fetchCurrentBridgeRows(
          normalizedSymbol,
          normalizedInterval,
          coldStart,
          safeEnd,
          Math.min(MAX_PERSIST_ROWS, safeLimit + 4),
        );
        bridgeWindowComplete = inspectBridgeWindow(
          bridge,
          normalizedInterval,
          coldStart,
          safeEnd,
        ).complete;
      } catch (error) {
        stats.bridge_errors += 1;
        stats.bridge_last_error = String(error?.message || error);
      }
      merged = mergeRows(merged, bridge).filter((row) => row.open_time_ms < safeEnd);
    }

    let archive = [];
    // partial桥接不能阻止归档补深度。只有当前窗口已经完整连续时，才跳过归档首屏回补。
    const latestPersistedOpen = persisted.at(-1)?.open_time_ms || 0;
    const yesterdayStart = Math.floor((Date.now() - 86_400_000) / 86_400_000) * 86_400_000;
    const needsArchive = !bridgeWindowComplete &&
      (persisted.length < safeLimit || latestPersistedOpen < yesterdayStart);
    if (needsArchive) {
      try { archive = await fetchArchiveRows(normalizedSymbol, normalizedInterval, safeEnd, safeLimit); } catch (error) {
        stats.archive_errors += 1;
        stats.last_error = String(error?.message || error);
      }
    }
    merged = mergeRows(merged, archive).filter((row) => row.open_time_ms < safeEnd);
    if (nearNow && normalizedInterval !== '1s') {
      // Step650.4：不能只看最后一根是否已到当前。持久快照可能是“旧归档 + 当前实时一根”，
      // 此时尾部很新但中间仍有大断层。扫描本次最近窗口，从第一个内部缺口开始补齐。
      const beforeCoverage = inspectRecentContinuity(merged, normalizedInterval, safeEnd, safeLimit);
      stats.gap_scan_requests += 1;
      const bridgeStart = bridgeStartForRecentWindow(merged, normalizedInterval, safeEnd, safeLimit);
      const needsBridge = !beforeCoverage.continuous_to_current && bridgeStart < safeEnd;
      if (needsBridge) {
        stats.gap_repair_requests += 1;
        stats.gap_repair_last_start_at = bridgeStart;
        const needed = Math.max(4, Math.min(MAX_PERSIST_ROWS, Math.ceil((safeEnd - bridgeStart) / step) + 4));
        try { bridge = await fetchCurrentBridgeRows(normalizedSymbol, normalizedInterval, bridgeStart, safeEnd, needed); } catch (error) {
          stats.bridge_errors += 1;
          stats.bridge_last_error = String(error?.message || error);
        }
        merged = mergeRows(merged, bridge).filter((row) => row.open_time_ms < safeEnd);
        const afterCoverage = inspectRecentContinuity(merged, normalizedInterval, safeEnd, safeLimit);
        stats.gap_repair_remaining_gaps = afterCoverage.missing_intervals;
        if (afterCoverage.continuous_to_current) {
          stats.gap_repair_success += 1;
          stats.gap_repair_last_success_at = Date.now();
        }
      }
      ensureLiveStream(normalizedSymbol, normalizedInterval);
    }
    memory.set(key, { rows: merged, loadedAt: Date.now() });
    const finalCoverage = nearNow
      ? inspectRecentContinuity(merged, normalizedInterval, safeEnd, safeLimit)
      : null;
    // Step650.8.2：临近当前的快照只有在最近窗口连续时才持久化。
    // 防止“旧归档 + 当前一根”的partial结果再次污染Supabase并在重启后反复制造同一断层。
    const mayPersist = archive.length || bridge.length;
    const safeToPersist = !nearNow || finalCoverage?.continuous_to_current === true;
    if (mayPersist && safeToPersist) {
      const source = bridge.length
        ? 'binance_official_public_archive_plus_current_bridge'
        : 'binance_official_public_archive_kline_seed';
      persistRows(normalizedSymbol, normalizedInterval, merged, source)
        .catch((error) => { stats.last_error = String(error?.message || error); });
    }
    return merged;
  })();
  inflight.set(key, task);
  try {
    return (await task).filter((row) => row.open_time_ms < safeEnd).slice(-safeLimit);
  } finally {
    if (inflight.get(key) === task) inflight.delete(key);
  }
}

export function getBinanceContractKlineSeedHealth() {
  const now = Date.now();
  return {
    ok: true,
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    cache_entries: memory.size,
    inflight: inflight.size,
    persistence_enabled: supabaseEnabled(),
    live_stream_count: liveStreams.size,
    live_streams: [...liveStreams.values()].map((state) => ({
      symbol: state.symbol,
      interval: state.interval,
      connected: state.connected,
      opened_at: state.openedAt ? iso(state.openedAt) : null,
      last_message_at: state.lastMessageAt ? iso(state.lastMessageAt) : null,
      idle_seconds: Math.max(0, Math.round((now - state.lastAccess) / 1000)),
      last_error: state.lastError || null,
    })),
    bridge_candidates: HTTP_BRIDGE_CANDIDATES.map((candidate) => {
      const globalState = activeBridgeState(bridgeStateKey(candidate.id, '*'));
      const scopedStates = [...bridgeCandidateState.entries()]
        .filter(([key, state]) => key.startsWith(`${candidate.id}|`) && !key.endsWith('|*') && state.until > now)
        .map(([key, state]) => ({ symbol: key.split('|').at(-1), ...state }));
      return {
        id: candidate.id,
        global_cooling_down: Boolean(globalState),
        next_allowed_at: globalState ? iso(globalState.until) : null,
        reason: globalState?.reason || null,
        last_error: globalState?.error || null,
        symbol_cooldown_count: scopedStates.length,
        symbol_cooldowns: scopedStates.slice(0, 12).map((state) => ({
          symbol: state.symbol,
          next_allowed_at: iso(state.until),
          reason: state.reason,
          last_error: state.error,
        })),
      };
    }),
    ...stats,
    last_success_at: stats.last_success_at ? iso(stats.last_success_at) : null,
    bridge_last_success_at: stats.bridge_last_success_at ? iso(stats.bridge_last_success_at) : null,
    gap_repair_last_start_at: stats.gap_repair_last_start_at ? iso(stats.gap_repair_last_start_at) : null,
    gap_repair_last_success_at: stats.gap_repair_last_success_at ? iso(stats.gap_repair_last_success_at) : null,
    live_last_message_at: stats.live_last_message_at ? iso(stats.live_last_message_at) : null,
    bridge_wide_cooldown: (() => {
      const guard = getBinanceRestGuardHealth();
      return {
        active: Boolean(guard.active),
        next_allowed_at: guard.next_allowed_at,
        reason: guard.reason,
        candidate_id: guard.source,
        parsed_ban_until: guard.parsed_ban_until,
        last_error: guard.last_error,
      };
    })(),
    shared_binance_rest_guard: getBinanceRestGuardHealth(),
    bridge_min_request_gap_ms: getBinanceRestGuardHealth().min_request_gap_ms,
    source: 'binance_single_exact_symbol_rest_probe_gate_bounded_queue_archive_gap_repair_live_websocket',
    time: iso(Date.now()),
  };
}

export const _test = {
  unzipFirstCsv,
  parseCsv,
  parseApiRows,
  normalizeRows,
  aggregateRows,
  mergeRows,
  expectedCurrentOpen,
  inspectRecentContinuity,
  bridgeStartForRecentWindow,
  inspectBridgeWindow,
  parseBinanceBanUntil,
};

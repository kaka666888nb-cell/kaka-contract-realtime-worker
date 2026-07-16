import { inflateRawSync } from 'node:zlib';

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
const FETCH_TIMEOUT_MS = 18_000;

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

async function fetchBuffer(url, timeoutMs = FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: { accept: 'application/zip,application/octet-stream,*/*', 'user-agent': 'KakaWeb3-Kline-Seed/650.2' },
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
  const dailyResults = await mapLimit(dailyCandidates, 4, (url) => loadArchiveFile(url, symbol, interval, archiveInterval));
  for (const rows of dailyResults) {
    if (Array.isArray(rows)) collected.push(...rows);
  }

  if (collected.length < targetRows) {
    let monthCursor = previousMonth(monthStartUtc(new Date(Math.min(endMs, Date.now()))));
    const monthlyCandidates = [];
    for (let i = 0; i < MAX_MONTHLY_FILES; i += 1) {
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

async function persistRows(symbol, interval, rows) {
  if (!supabaseEnabled() || !rows.length) return;
  const safeRows = rows.slice(-MAX_PERSIST_ROWS);
  const body = [{
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    snapshot_type: 'klines',
    quote_asset: snapshotKey(symbol, interval),
    payload: { rows: safeRows },
    row_count: safeRows.length,
    source: 'binance_official_public_archive_kline_seed',
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
  const cached = memory.get(key);
  if (cached && Date.now() - cached.loadedAt <= CACHE_TTL_MS) {
    stats.memory_hits += 1;
    return cached.rows.filter((row) => row.open_time_ms < safeEnd).slice(-safeLimit);
  }
  const existing = inflight.get(key);
  if (existing) return (await existing).filter((row) => row.open_time_ms < safeEnd).slice(-safeLimit);

  const task = (async () => {
    let persisted = [];
    try { persisted = await restorePersisted(normalizedSymbol, normalizedInterval); } catch (error) { stats.last_error = String(error?.message || error); }
    let archive = [];
    try { archive = await fetchArchiveRows(normalizedSymbol, normalizedInterval, safeEnd, safeLimit); } catch (error) {
      stats.archive_errors += 1;
      stats.last_error = String(error?.message || error);
    }
    const merged = [...new Map([...persisted, ...archive].map((row) => [row.open_time_ms, row])).values()]
      .filter((row) => row.open_time_ms < safeEnd)
      .sort((a, b) => a.open_time_ms - b.open_time_ms)
      .slice(-MAX_PERSIST_ROWS);
    memory.set(key, { rows: merged, loadedAt: Date.now() });
    if (archive.length) persistRows(normalizedSymbol, normalizedInterval, merged).catch((error) => { stats.last_error = String(error?.message || error); });
    return merged;
  })();
  inflight.set(key, task);
  try {
    return (await task).slice(-safeLimit);
  } finally {
    if (inflight.get(key) === task) inflight.delete(key);
  }
}

export function getBinanceContractKlineSeedHealth() {
  return {
    ok: true,
    provider: PROVIDER,
    market_type: MARKET_TYPE,
    cache_entries: memory.size,
    inflight: inflight.size,
    persistence_enabled: supabaseEnabled(),
    ...stats,
    last_success_at: stats.last_success_at ? iso(stats.last_success_at) : null,
    source: 'binance_official_public_data_archive_with_persistent_snapshot',
    time: iso(Date.now()),
  };
}

export const _test = { unzipFirstCsv, parseCsv, normalizeRows, aggregateRows };

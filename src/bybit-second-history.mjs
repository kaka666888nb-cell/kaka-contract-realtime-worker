import { WebSocket } from 'ws';

const VERSION = '650.8.15.61';
const PROVIDER = 'bybit';
const MAX_ROWS = 3600;
const MAX_ENTRIES = 64;
const SPOT_RECENT_TRADE_MAX = 60;
const ON_DEMAND_SPOT_LATEST_RESERVE_RATIO = 0.35;
const ON_DEMAND_SPOT_LATEST_RESERVE_MIN_ROWS = 2;
const DISCOVERED_HOT_BASES = new Set(['BTC', 'ETH']);
const ON_DEMAND_LEASE_MS = 10 * 60_000;
const RECONNECT_MAX_MS = 30_000;
const FIRST_PERSIST_MS = 60_000;
const PERSIST_INTERVAL_MS = 15 * 60_000;
const SEED_REFRESH_MS = 60_000;
const SUPABASE_URL =
  String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY =
  String(process.env.SUPABASE_SERVICE_ROLE_KEY || '');
const SNAPSHOT_TABLE = 'app_market_backend_snapshots';
const STATIC_HOT_TARGETS = [
  { market: 'spot', symbol: 'BTCUSDT', nativeSymbol: 'BTCUSDT', category: 'spot' },
  { market: 'spot', symbol: 'ETHUSDT', nativeSymbol: 'ETHUSDT', category: 'spot' },
  { market: 'contract', symbol: 'BTCUSDT', nativeSymbol: 'BTCUSDT', category: 'linear' },
  { market: 'contract', symbol: 'ETHUSDT', nativeSymbol: 'ETHUSDT', category: 'linear' },
];
let activeHotTargets = [...STATIC_HOT_TARGETS];

const entries = new Map();
const stats = {
  created: 0,
  reused: 0,
  evicted: 0,
  seed_requests: 0,
  seed_success: 0,
  seed_empty: 0,
  seed_errors: 0,
  ws_connects: 0,
  ws_reconnects: 0,
  ws_messages: 0,
  ws_trades: 0,
  history_reads: 0,
  history_waits: 0,
  history_empty: 0,
  rows_written: 0,
  restore_attempts: 0,
  restore_hits: 0,
  restore_errors: 0,
  persist_attempts: 0,
  persist_success: 0,
  persist_errors: 0,
  spot_discovery_requests: 0,
  spot_discovery_success: 0,
  spot_discovery_errors: 0,
  spot_discovered_hot_targets: 0,
  latest_pages_with_reserved_older_rows: 0,
  latest_rows_reserved_for_pagination: 0,
  older_pages_served_from_verified_ring: 0,
  last_error: '',
  last_restore_error: '',
  last_persist_error: '',
};

function compact(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/-SWAP$/i, '')
    .replace(/_UMCBL$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}

function marketKey(raw) {
  return String(raw || '').toLowerCase() === 'contract'
    ? 'contract'
    : 'spot';
}

function categoryKey(raw, market, symbol) {
  const value = String(raw || '').trim().toLowerCase();
  if (['spot', 'linear', 'inverse'].includes(value)) return value;
  if (market === 'spot') return 'spot';
  return symbol.endsWith('USD') ? 'inverse' : 'linear';
}

function finite(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function persistenceEnabled() {
  return Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);
}

function entryKey(market, symbol) {
  return `${market}|${symbol}`;
}

function snapshotKey(market, symbol) {
  return `SECOND:BYBIT:${market}:${symbol}`;
}

function normalizeRow(raw, market, symbol) {
  if (!raw || typeof raw !== 'object') return null;
  let openTimeMs = Number(
    raw.open_time_ms ??
    raw.openTime ??
    raw.open_time ??
    raw.t,
  );
  if (!Number.isFinite(openTimeMs) && typeof raw.open_time === 'string') {
    openTimeMs = Date.parse(raw.open_time);
  }
  if (!Number.isFinite(openTimeMs) || openTimeMs <= 0) return null;
  if (openTimeMs < 10_000_000_000) openTimeMs *= 1000;
  if (openTimeMs > 10_000_000_000_000) openTimeMs /= 1000;
  openTimeMs = Math.floor(openTimeMs / 1000) * 1000;

  const open = finite(raw.open ?? raw.open_price ?? raw.o);
  const high = finite(raw.high ?? raw.high_price ?? raw.h);
  const low = finite(raw.low ?? raw.low_price ?? raw.l);
  const close = finite(raw.close ?? raw.close_price ?? raw.c);
  if ([open, high, low, close].some((value) => value == null || value <= 0)) {
    return null;
  }

  return {
    provider: PROVIDER,
    market_type: market,
    symbol,
    interval: '1s',
    kline_interval: '1s',
    open_time: new Date(openTimeMs).toISOString(),
    open_time_ms: openTimeMs,
    close_time: new Date(openTimeMs + 999).toISOString(),
    close_time_ms: openTimeMs + 999,
    open,
    high,
    low,
    close,
    volume: Math.max(0, finite(raw.volume ?? raw.v) || 0),
    quote_volume: Math.max(
      0,
      finite(raw.quote_volume ?? raw.quoteVolume ?? raw.q) || 0,
    ),
    trade_count: Math.max(
      0,
      Math.round(finite(raw.trade_count ?? raw.n) || 0),
    ),
    source:
      raw.source ||
      'bybit_official_public_trade_1s_shared_ws',
    cached_at: new Date().toISOString(),
  };
}

function mergeRows(existing, incoming, market, symbol) {
  const byTime = new Map();
  for (const raw of [...(existing || []), ...(incoming || [])]) {
    const row = normalizeRow(raw, market, symbol);
    if (!row) continue;
    byTime.set(row.open_time_ms, row);
  }
  return [...byTime.values()]
    .sort((a, b) => a.open_time_ms - b.open_time_ms)
    .slice(-MAX_ROWS);
}

function rebuildRowIndex(entry) {
  entry.rowByTime = new Map();
  for (const row of entry.rows || []) {
    const time = Number(row?.open_time_ms || 0);
    if (Number.isFinite(time) && time > 0) {
      entry.rowByTime.set(time, row);
    }
  }
}

function trimEntryRows(entry) {
  const overflow = Math.max(0, entry.rows.length - MAX_ROWS);
  if (overflow <= 0) return;
  const removed = entry.rows.splice(0, overflow);
  for (const row of removed) {
    entry.rowByTime.delete(Number(row?.open_time_ms || 0));
  }
}

function insertEntryRow(entry, row) {
  const time = Number(row?.open_time_ms || 0);
  if (!Number.isFinite(time) || time <= 0) return;
  const lastTime = Number(entry.rows.at(-1)?.open_time_ms || 0);
  if (!entry.rows.length || time > lastTime) {
    entry.rows.push(row);
  } else {
    let low = 0;
    let high = entry.rows.length;
    while (low < high) {
      const middle = (low + high) >> 1;
      const middleTime = Number(entry.rows[middle]?.open_time_ms || 0);
      if (middleTime < time) low = middle + 1;
      else high = middle;
    }
    entry.rows.splice(low, 0, row);
  }
  entry.rowByTime.set(time, row);
  trimEntryRows(entry);
}

function notify(entry) {
  for (const waiter of [...entry.waiters]) {
    try { waiter(); } catch (_) {}
  }
}

function ingestTrade(entry, rawTime, rawPrice, rawSize) {
  let time = Number(rawTime);
  const price = finite(rawPrice);
  let size = finite(rawSize);
  if (!Number.isFinite(time) || price == null || price <= 0 || size == null) {
    return;
  }
  if (time < 10_000_000_000) time *= 1000;
  if (time > 10_000_000_000_000) time /= 1000;
  const bucket = Math.floor(time / 1000) * 1000;

  size = Math.abs(size);
  if (entry.category === 'inverse') {
    size = price > 0 ? size / price : 0;
  }
  const quote = size * price;

  const previous = entry.rowByTime.get(bucket);
  if (previous) {
    // Step781.2.7: mutate the current one-second bucket in place. The old
    // implementation scanned, filtered, normalized and sorted all 3600 rows
    // for every single public trade. BTC/ETH can deliver thousands of trades
    // per second, starving the child HTTP server and preventing contract hot
    // targets from ever starting. Exact-key O(1) updates keep the event loop
    // responsive while preserving the same real-trade-only candle semantics.
    previous.high = Math.max(Number(previous.high), price);
    previous.low = Math.min(Number(previous.low), price);
    previous.close = price;
    previous.volume = Number(previous.volume || 0) + size;
    previous.quote_volume = Number(previous.quote_volume || 0) + quote;
    previous.trade_count = Number(previous.trade_count || 0) + 1;
    previous.cached_at = new Date().toISOString();
  } else {
    const next = normalizeRow({
      open_time_ms: bucket,
      open: price,
      high: price,
      low: price,
      close: price,
      volume: size,
      quote_volume: quote,
      trade_count: 1,
      source: 'bybit_official_public_trade_1s_shared_ws',
    }, entry.market, entry.symbol);
    if (!next) return;
    insertEntryRow(entry, next);
  }
  entry.dirty = true;
  stats.rows_written += 1;
  schedulePersist(entry);
  notify(entry);
}

async function seedRecent(entry, { force = false } = {}) {
  if (entry.seedPromise) return entry.seedPromise;
  if (
    !force &&
    entry.rows.length > 0 &&
    Number(entry.lastSeedAt || 0) > 0 &&
    Date.now() - Number(entry.lastSeedAt || 0) < SEED_REFRESH_MS
  ) {
    return;
  }
  entry.seedPromise = (async () => {
    stats.seed_requests += 1;
    const limit = entry.market === 'spot' ? SPOT_RECENT_TRADE_MAX : 1000;
    const url =
      `https://api.bybit.com/v5/market/recent-trade` +
      `?category=${encodeURIComponent(entry.category)}` +
      `&symbol=${encodeURIComponent(entry.nativeSymbol)}` +
      `&limit=${limit}`;
    try {
      const response = await fetch(url, {
        headers: { accept: 'application/json' },
        signal: AbortSignal.timeout(12_000),
      });
      const text = await response.text();
      if (!response.ok) {
        throw new Error(`bybit_second_seed_http_${response.status}:${text.slice(0, 160)}`);
      }
      const payload = JSON.parse(text);
      if (Number(payload?.retCode || 0) !== 0) {
        throw new Error(
          `bybit_second_seed_${payload?.retCode}:${payload?.retMsg}`,
        );
      }
      const rows = Array.isArray(payload?.result?.list)
        ? payload.result.list
        : [];
      if (!rows.length) {
        stats.seed_empty += 1;
        return;
      }
      rows
        .slice()
        .sort(
          (a, b) =>
            Number(a?.time ?? a?.T ?? 0) -
            Number(b?.time ?? b?.T ?? 0),
        )
        .forEach((trade) => {
          ingestTrade(
            entry,
            trade.time ?? trade.T,
            trade.price ?? trade.p,
            trade.size ?? trade.v,
          );
        });
      stats.seed_success += 1;
    } catch (error) {
      stats.seed_errors += 1;
      stats.last_error = String(error?.message || error);
    }
  })().finally(() => {
    entry.lastSeedAt = Date.now();
    entry.seedPromise = null;
  });
  return entry.seedPromise;
}

function scheduleReconnect(entry) {
  if (entry.closed || entry.reconnectTimer) return;
  const delay = Math.min(
    RECONNECT_MAX_MS,
    1000 * (2 ** Math.min(5, entry.reconnectAttempt)),
  );
  entry.reconnectAttempt += 1;
  entry.reconnectTimer = setTimeout(() => {
    entry.reconnectTimer = null;
    if (!entry.closed) {
      stats.ws_reconnects += 1;
      connect(entry).catch(() => {});
    }
  }, delay);
  entry.reconnectTimer.unref?.();
}

async function connect(entry) {
  if (
    entry.closed ||
    entry.ws?.readyState === WebSocket.OPEN ||
    entry.connectPromise
  ) {
    return entry.connectPromise;
  }

  entry.connectPromise = new Promise((resolve) => {
    const ws = new WebSocket(
      `wss://stream.bybit.com/v5/public/${entry.category}`,
      { handshakeTimeout: 15_000 },
    );
    entry.ws = ws;
    stats.ws_connects += 1;

    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      resolve();
    };

    ws.on('open', () => {
      entry.reconnectAttempt = 0;
      try {
        ws.send(JSON.stringify({
          op: 'subscribe',
          args: [`publicTrade.${entry.nativeSymbol}`],
        }));
      } catch (_) {}
      clearInterval(entry.heartbeat);
      entry.heartbeat = setInterval(() => {
        try {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ op: 'ping' }));
          }
        } catch (_) {}
      }, 20_000);
      entry.heartbeat.unref?.();
      finish();
    });

    ws.on('message', (raw) => {
      stats.ws_messages += 1;
      try {
        const message = JSON.parse(raw.toString());
        if (
          !String(message?.topic || '')
            .startsWith('publicTrade.')
        ) {
          return;
        }
        for (const trade of
            Array.isArray(message?.data) ? message.data : []) {
          stats.ws_trades += 1;
          ingestTrade(
            entry,
            trade.T ?? message.ts,
            trade.p ?? trade.price,
            trade.v ?? trade.size,
          );
        }
      } catch (_) {}
    });

    ws.on('close', () => {
      clearInterval(entry.heartbeat);
      entry.heartbeat = null;
      if (entry.ws === ws) entry.ws = null;
      finish();
      scheduleReconnect(entry);
    });

    ws.on('error', (error) => {
      stats.last_error = String(error?.message || error);
      finish();
    });
  }).finally(() => {
    entry.connectPromise = null;
  });

  return entry.connectPromise;
}

function createEntry({
  market,
  symbol,
  nativeSymbol,
  category,
  hotPinned = false,
}) {
  const key = entryKey(market, symbol);
  const entry = {
    key,
    market,
    symbol,
    nativeSymbol,
    category,
    hotPinned,
    leaseUntil: hotPinned
      ? Number.MAX_SAFE_INTEGER
      : Date.now() + ON_DEMAND_LEASE_MS,
    rows: [],
    rowByTime: new Map(),
    ws: null,
    connectPromise: null,
    reconnectTimer: null,
    reconnectAttempt: 0,
    heartbeat: null,
    waiters: new Set(),
    seedPromise: null,
    lastSeedAt: 0,
    restorePromise: null,
    restored: false,
    persistPromise: null,
    persistTimer: null,
    lastPersistAt: 0,
    dirty: false,
    closed: false,
    createdAt: Date.now(),
  };
  entries.set(key, entry);
  stats.created += 1;
  return entry;
}

function closeEntry(entry, reason = 'idle') {
  if (!entry || entry.closed || entry.hotPinned) return;
  entry.closed = true;
  clearTimeout(entry.reconnectTimer);
  clearTimeout(entry.persistTimer);
  clearInterval(entry.heartbeat);
  try {
    if (
      entry.ws?.readyState === WebSocket.OPEN ||
      entry.ws?.readyState === WebSocket.CONNECTING
    ) {
      entry.ws.close(1000, reason);
    }
  } catch (_) {}
  if (entry.dirty) persist(entry).catch(() => {});
  entries.delete(entry.key);
}

function evictIfNeeded() {
  const now = Date.now();
  for (const entry of [...entries.values()]) {
    if (
      !entry.hotPinned &&
      Number(entry.leaseUntil || 0) <= now
    ) {
      closeEntry(entry, 'lease_expired');
      stats.evicted += 1;
    }
  }
  while (entries.size >= MAX_ENTRIES) {
    const candidate = [...entries.values()]
      .filter((entry) => !entry.hotPinned)
      .sort((a, b) => a.leaseUntil - b.leaseUntil)[0];
    if (!candidate) break;
    closeEntry(candidate, 'capacity');
    stats.evicted += 1;
  }
}

async function restore(entry) {
  if (entry.restored) return;
  if (entry.restorePromise) return entry.restorePromise;
  entry.restorePromise = (async () => {
    stats.restore_attempts += 1;
    if (!persistenceEnabled()) return;
    const key = encodeURIComponent(
      snapshotKey(entry.market, entry.symbol),
    );
    const url =
      `${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}` +
      `?select=payload,updated_at` +
      `&provider=eq.bybit` +
      `&market_type=eq.${entry.market}` +
      `&snapshot_type=eq.klines` +
      `&quote_asset=eq.${key}` +
      `&limit=1`;
    try {
      const response = await fetch(url, {
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          accept: 'application/json',
        },
        signal: AbortSignal.timeout(8_000),
      });
      if (!response.ok) {
        throw new Error(`bybit_second_restore_${response.status}`);
      }
      const records = await response.json();
      const restored = mergeRows(
        [],
        Array.isArray(records) ? records[0]?.payload?.rows : [],
        entry.market,
        entry.symbol,
      );
      if (restored.length) {
        entry.rows = mergeRows(
          restored,
          entry.rows,
          entry.market,
          entry.symbol,
        );
        rebuildRowIndex(entry);
        stats.restore_hits += 1;
        notify(entry);
      }
    } catch (error) {
      stats.restore_errors += 1;
      stats.last_restore_error =
        String(error?.message || error);
    }
  })().finally(() => {
    entry.restored = true;
    entry.restorePromise = null;
  });
  return entry.restorePromise;
}

async function persist(entry) {
  if (
    !persistenceEnabled() ||
    !entry.rows.length ||
    entry.persistPromise
  ) {
    return entry.persistPromise;
  }
  entry.persistPromise = (async () => {
    stats.persist_attempts += 1;
    const now = new Date().toISOString();
    const body = [{
      provider: PROVIDER,
      market_type: entry.market,
      snapshot_type: 'klines',
      quote_asset: snapshotKey(
        entry.market,
        entry.symbol,
      ),
      payload: {
        rows: entry.rows.slice(-MAX_ROWS),
      },
      row_count: entry.rows.length,
      source:
        'bybit_official_public_trade_1s_shared_ws',
      source_time:
        entry.rows.at(-1)?.open_time || now,
      updated_at: now,
    }];
    try {
      const response = await fetch(
        `${SUPABASE_URL}/rest/v1/${SNAPSHOT_TABLE}` +
        `?on_conflict=provider,market_type,snapshot_type,quote_asset`,
        {
          method: 'POST',
          headers: {
            apikey: SUPABASE_SERVICE_ROLE_KEY,
            authorization:
              `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            'content-type': 'application/json',
            prefer:
              'resolution=merge-duplicates,return=minimal',
          },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(8_000),
        },
      );
      if (!response.ok) {
        throw new Error(
          `bybit_second_persist_${response.status}`,
        );
      }
      entry.lastPersistAt = Date.now();
      entry.dirty = false;
      stats.persist_success += 1;
    } catch (error) {
      stats.persist_errors += 1;
      stats.last_persist_error =
        String(error?.message || error);
    }
  })().finally(() => {
    entry.persistPromise = null;
  });
  return entry.persistPromise;
}

function schedulePersist(entry) {
  if (
    !persistenceEnabled() ||
    entry.persistTimer ||
    !entry.dirty
  ) {
    return;
  }
  const delay = entry.lastPersistAt > 0
    ? PERSIST_INTERVAL_MS
    : FIRST_PERSIST_MS;
  entry.persistTimer = setTimeout(() => {
    entry.persistTimer = null;
    if (entry.dirty) persist(entry).catch(() => {});
  }, delay);
  entry.persistTimer.unref?.();
}

async function ensureEntry({
  market,
  symbol,
  nativeSymbol,
  category,
  hotPinned = false,
}) {
  evictIfNeeded();
  const key = entryKey(market, symbol);
  let entry = entries.get(key);
  if (!entry) {
    entry = createEntry({
      market,
      symbol,
      nativeSymbol,
      category,
      hotPinned,
    });
  } else {
    stats.reused += 1;
    entry.nativeSymbol = nativeSymbol || entry.nativeSymbol;
    entry.category = category || entry.category;
    if (hotPinned) {
      entry.hotPinned = true;
      entry.leaseUntil = Number.MAX_SAFE_INTEGER;
    } else {
      entry.leaseUntil = Math.max(
        entry.leaseUntil,
        Date.now() + ON_DEMAND_LEASE_MS,
      );
    }
  }

  // Step781.2.4: reads of an already-warmed exact market+symbol must never
  // wait behind a fresh REST seed or a WebSocket handshake. The child can
  // already hold thousands of verified seconds; serve those rows first and
  // repair the seed/connection in the background. This prevents the parent
  // from timing out while valid cached rows are available.
  if (entry.rows.length > 0) {
    if (!entry.restored && !entry.restorePromise) {
      restore(entry).catch(() => {});
    }
    seedRecent(entry).catch(() => {});
    if (
      entry.ws?.readyState !== WebSocket.OPEN &&
      !entry.connectPromise
    ) {
      connect(entry).catch(() => {});
    }
    return entry;
  }

  await restore(entry);
  if (entry.rows.length === 0) {
    await seedRecent(entry, { force: true });
  } else {
    seedRecent(entry).catch(() => {});
  }
  if (entry.rows.length === 0) {
    await connect(entry);
  } else if (
    entry.ws?.readyState !== WebSocket.OPEN &&
    !entry.connectPromise
  ) {
    connect(entry).catch(() => {});
  }
  return entry;
}

function rowsBeforeEnd(entry, endTime) {
  const end = Number.isFinite(Number(endTime))
    ? Number(endTime)
    : Date.now() + 1000;
  let low = 0;
  let high = entry.rows.length;
  while (low < high) {
    const middle = (low + high) >> 1;
    const time = Number(entry.rows[middle]?.open_time_ms || 0);
    if (time < end) low = middle + 1;
    else high = middle;
  }
  return entry.rows.slice(0, low);
}

function filteredRows(entry, endTime, limit, { endTimeProvided = false } = {}) {
  const available = rowsBeforeEnd(entry, endTime);
  if (!available.length) return [];

  // Step781.2.10: Bybit's official spot recent-trade endpoint exposes at most
  // 60 trades and no public older cursor. For a newly activated exact spot
  // pair, returning every verified second in the first "latest" page leaves
  // nothing for the immediately-following end_time request even though older
  // real rows are already present in the same official seed/ring. Keep a
  // bounded oldest slice for the next page. This is true pagination over
  // verified Bybit trades: no synthetic seconds, no quote alias and no
  // cross-platform fallback. Once the shared WebSocket/persisted ring grows,
  // normal full-size paging naturally resumes.
  if (
    endTimeProvided !== true &&
    entry.market === 'spot' &&
    available.length <= limit &&
    available.length >= 4
  ) {
    const reserve = Math.min(
      available.length - 2,
      Math.max(
        ON_DEMAND_SPOT_LATEST_RESERVE_MIN_ROWS,
        Math.ceil(
          available.length * ON_DEMAND_SPOT_LATEST_RESERVE_RATIO,
        ),
      ),
    );
    const pageSize = Math.max(
      2,
      Math.min(limit, available.length - reserve),
    );
    if (pageSize < available.length) {
      stats.latest_pages_with_reserved_older_rows += 1;
      stats.latest_rows_reserved_for_pagination +=
        available.length - pageSize;
      return available.slice(-pageSize);
    }
  }

  const rows = available.slice(-limit);
  if (endTimeProvided === true && rows.length) {
    stats.older_pages_served_from_verified_ring += 1;
  }
  return rows;
}

async function waitForRows(entry, endTime, limit, waitMs, options = {}) {
  let rows = filteredRows(entry, endTime, limit, options);
  if (rows.length >= Math.min(2, limit) || waitMs <= 0) {
    return rows;
  }

  stats.history_waits += 1;
  await new Promise((resolve) => {
    const deadline = Date.now() + waitMs;
    let timer = null;
    const check = () => {
      rows = filteredRows(entry, endTime, limit, options);
      if (
        rows.length >= Math.min(2, limit) ||
        Date.now() >= deadline
      ) {
        clearTimeout(timer);
        entry.waiters.delete(check);
        resolve();
        return;
      }
      timer = setTimeout(check, 250);
      timer.unref?.();
    };
    entry.waiters.add(check);
    check();
  });
  return filteredRows(entry, endTime, limit, options);
}

export async function readBybitSecondHistory({
  market,
  symbol,
  nativeSymbol,
  category,
  endTime,
  limit,
  waitMs,
  endTimeProvided = false,
}) {
  const safeMarket = marketKey(market);
  const safeSymbol = compact(symbol);
  const safeNative = compact(nativeSymbol || symbol);
  const safeCategory = categoryKey(
    category,
    safeMarket,
    safeSymbol,
  );
  const safeLimit = Math.max(
    2,
    Math.min(MAX_ROWS, Number(limit) || 1000),
  );
  const safeWait = Math.max(
    0,
    Math.min(8_000, Number(waitMs) || 0),
  );
  if (!safeSymbol || !safeNative) {
    throw new Error('bybit_second_symbol_required');
  }

  stats.history_reads += 1;
  const entry = await ensureEntry({
    market: safeMarket,
    symbol: safeSymbol,
    nativeSymbol: safeNative,
    category: safeCategory,
  });
  const rows = await waitForRows(
    entry,
    endTime,
    safeLimit,
    safeWait,
    { endTimeProvided: endTimeProvided === true },
  );
  if (!rows.length) stats.history_empty += 1;
  return {
    entry,
    rows,
  };
}

export function getBybitSecondHistoryHealth() {
  evictIfNeeded();
  return {
    ok: true,
    version: VERSION,
    mode:
      'shared_bybit_spot_contract_public_trade_ws_bounded_history',
    source:
      'bybit_official_public_trade_1s_shared_ws',
    max_rows_per_exact_market_symbol: MAX_ROWS,
    max_entries: MAX_ENTRIES,
    on_demand_lease_seconds:
      Math.round(ON_DEMAND_LEASE_MS / 1000),
    persistence_enabled: persistenceEnabled(),
    render_direct_private_or_user_trade_api_used: false,
    recent_trade_time_range_parameters_used: false,
    empty_seconds_generated_by_backend: false,
    cached_rows_served_before_seed_or_ws_repair: true,
    blocking_seed_on_warmed_read: false,
    indexed_second_bucket_updates: true,
    full_history_resort_per_trade: false,
    hot_target_start_mode: 'parallel_all_targets_created_before_wait',
    seed_refresh_seconds: Math.round(SEED_REFRESH_MS / 1000),
    spot_recent_trade_rest_limit: SPOT_RECENT_TRADE_MAX,
    spot_recent_trade_public_older_cursor_available: false,
    shallow_spot_latest_page_reserves_verified_older_rows: true,
    shallow_spot_latest_page_reserve_ratio:
      ON_DEMAND_SPOT_LATEST_RESERVE_RATIO,
    spot_history_source:
      'official_recent_trade_seed_plus_live_publicTrade_ring',
    btc_eth_quote_pairs_prestarted_from_official_directory: true,
    hot_targets: activeHotTargets.map((item) => ({
      market: item.market,
      symbol: item.symbol,
    })),
    entries: [...entries.values()].map((entry) => ({
      market: entry.market,
      symbol: entry.symbol,
      native_symbol: entry.nativeSymbol,
      category: entry.category,
      hot_pinned: entry.hotPinned,
      connected:
        entry.ws?.readyState === WebSocket.OPEN,
      row_count: entry.rows.length,
      oldest_open_time:
        entry.rows[0]?.open_time || null,
      latest_open_time:
        entry.rows.at(-1)?.open_time || null,
      lease_until: entry.hotPinned
        ? 'pinned'
        : new Date(entry.leaseUntil).toISOString(),
    })),
    ...stats,
    time: new Date().toISOString(),
  };
}

export async function handleBybitSecondHistoryInternal(
  req,
  res,
  url,
) {
  if (
    url.pathname ===
    '/internal/bybit-second-history-health'
  ) {
    res.writeHead(200, {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    });
    res.end(JSON.stringify(
      getBybitSecondHistoryHealth(),
    ));
    return true;
  }

  if (
    url.pathname !==
    '/internal/bybit-second-history'
  ) {
    return false;
  }

  try {
    const market = marketKey(
      url.searchParams.get('market'),
    );
    const symbol = compact(
      url.searchParams.get('symbol'),
    );
    const nativeSymbol = compact(
      url.searchParams.get('native_symbol') ||
      symbol,
    );
    const category = categoryKey(
      url.searchParams.get('category'),
      market,
      symbol,
    );
    const endTime = Number(
      url.searchParams.get('end_time') ||
      Date.now() + 1000,
    );
    const limit = Number(
      url.searchParams.get('limit') || 1000,
    );
    const waitMs = Number(
      url.searchParams.get('wait_ms') || 4500,
    );
    const endTimeProvided =
      url.searchParams.get('end_time_provided') === '1';

    const { entry, rows } =
      await readBybitSecondHistory({
        market,
        symbol,
        nativeSymbol,
        category,
        endTime,
        limit,
        waitMs,
        endTimeProvided,
      });

    res.writeHead(200, {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    });
    res.end(JSON.stringify({
      ok: true,
      version: VERSION,
      provider: PROVIDER,
      market_type: market,
      symbol,
      interval: '1s',
      source:
        'bybit_official_public_trade_1s_shared_ws',
      row_count: rows.length,
      cached_row_count: entry.rows.length,
      end_time_provided: endTimeProvided,
      rows,
      generated_at: new Date().toISOString(),
    }));
  } catch (error) {
    res.writeHead(503, {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    });
    res.end(JSON.stringify({
      ok: false,
      version: VERSION,
      error: String(error?.message || error),
      rows: [],
    }));
  }
  return true;
}

async function discoverBtcEthSpotHotTargets() {
  stats.spot_discovery_requests += 1;
  try {
    const response = await fetch(
      'https://api.bybit.com/v5/market/instruments-info?category=spot',
      {
        headers: { accept: 'application/json' },
        signal: AbortSignal.timeout(12_000),
      },
    );
    const text = await response.text();
    if (!response.ok) {
      throw new Error(
        `bybit_spot_instruments_${response.status}:${text.slice(0, 160)}`,
      );
    }
    const payload = JSON.parse(text);
    if (Number(payload?.retCode || 0) !== 0) {
      throw new Error(
        `bybit_spot_instruments_${payload?.retCode}:${payload?.retMsg}`,
      );
    }
    const rows = Array.isArray(payload?.result?.list)
      ? payload.result.list
      : [];
    const discovered = rows
      .filter((item) => (
        DISCOVERED_HOT_BASES.has(compact(item?.baseCoin)) &&
        compact(item?.symbol) &&
        String(item?.status || 'Trading').toLowerCase() === 'trading'
      ))
      .map((item) => ({
        market: 'spot',
        symbol: compact(item.symbol),
        nativeSymbol: compact(item.symbol),
        category: 'spot',
      }));
    stats.spot_discovery_success += 1;
    stats.spot_discovered_hot_targets = discovered.length;
    return discovered;
  } catch (error) {
    stats.spot_discovery_errors += 1;
    stats.last_error = String(error?.message || error);
    return [];
  }
}

export async function startBybitSecondHistoryHotSeeds() {
  // Step781.2.10: discover all currently-listed BTC/ETH spot quote pairs once
  // from Bybit's official directory and create every exact identity before
  // waiting for restore/seed/WS work. This starts collecting real seconds at
  // process boot instead of only after the first user's left-drag.
  const discovered = await discoverBtcEthSpotHotTargets();
  const unique = new Map();
  for (const target of [...STATIC_HOT_TARGETS, ...discovered]) {
    unique.set(entryKey(target.market, target.symbol), target);
  }
  activeHotTargets = [...unique.values()];
  await Promise.allSettled(activeHotTargets.map(async (target) => {
    try {
      await ensureEntry({
        ...target,
        hotPinned: true,
      });
    } catch (error) {
      stats.last_error = String(error?.message || error);
    }
  }));
}

setInterval(() => {
  evictIfNeeded();
}, 60_000).unref?.();


export const _test = {
  mergeRows,
  ingestTrade,
  rebuildRowIndex,
  readBybitSecondHistory,
  filteredRows,
  rowsBeforeEnd,
  primeEntry({
    market,
    symbol,
    nativeSymbol = symbol,
    category = market === 'contract' ? 'linear' : 'spot',
    rows = [],
    connected = true,
    hotPinned = true,
  }) {
    const safeMarket = marketKey(market);
    const safeSymbol = compact(symbol);
    let entry = entries.get(entryKey(safeMarket, safeSymbol));
    if (!entry) {
      entry = createEntry({
        market: safeMarket,
        symbol: safeSymbol,
        nativeSymbol: compact(nativeSymbol),
        category: categoryKey(category, safeMarket, safeSymbol),
        hotPinned,
      });
    }
    entry.rows = mergeRows(
      [],
      rows,
      safeMarket,
      safeSymbol,
    );
    rebuildRowIndex(entry);
    entry.restored = true;
    entry.lastSeedAt = Date.now();
    if (connected) entry.ws = { readyState: WebSocket.OPEN };
    return entry;
  },
};

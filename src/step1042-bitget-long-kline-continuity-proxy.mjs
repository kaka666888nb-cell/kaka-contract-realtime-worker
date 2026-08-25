import http from 'node:http';

const STEP_VERSION = '650.8.15.197.3.3.6';
const WRAPPER_SCHEMA = 'step1042_1_2_8_16_7_7_1_bitget_long_kline_continuity_v1';
const LONG_INTERVALS = new Set(['3d', '1w', '1M']);
const CACHE_TTL_MS = 15 * 60_000;
const CACHE_MAX = 192;
const cache = new Map();
const inflight = new Map();

function compact(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/-SWAP$/i, '')
    .replace(/_UMCBL$/i, '')
    .replace(/[^A-Z0-9]/g, '');
}

function intervalMs(interval) {
  return ({
    '3d': 3 * 86_400_000,
    '1w': 7 * 86_400_000,
    '1M': 30 * 86_400_000,
  })[interval] || 60_000;
}

function bitgetBar(interval) {
  return ({ '3d': '3D', '1w': '1W', '1M': '1M' })[interval] || null;
}

function bitgetProductType(quote) {
  const safe = String(quote || 'USDT').trim().toUpperCase();
  if (safe === 'USDC') return 'usdc-futures';
  if (safe === 'USD') return 'coin-futures';
  return 'usdt-futures';
}

function payloadRows(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.data?.list)) return payload.data.list;
  if (Array.isArray(payload?.result?.list)) return payload.result.list;
  return [];
}

function pruneCache() {
  const now = Date.now();
  for (const [key, entry] of cache.entries()) {
    if (Number(entry?.expiresAt || 0) <= now) cache.delete(key);
  }
  while (cache.size > CACHE_MAX) {
    const oldest = cache.keys().next().value;
    if (oldest == null) break;
    cache.delete(oldest);
  }
}

async function sharedResult(key, loader) {
  pruneCache();
  const cached = cache.get(key);
  if (cached && cached.expiresAt > Date.now()) return cached.value;
  const running = inflight.get(key);
  if (running) return await running;
  const task = Promise.resolve().then(loader);
  inflight.set(key, task);
  try {
    const value = await task;
    cache.set(key, { value, expiresAt: Date.now() + CACHE_TTL_MS });
    pruneCache();
    return value;
  } finally {
    if (inflight.get(key) === task) inflight.delete(key);
  }
}

async function jsonFetch(url, timeout = 15_000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        accept: 'application/json',
        'user-agent': 'KakaWeb3-Step1042-Bitget-Long-Kline/650.8.15.197.3.3.5',
      },
    });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(
        `Bitget HTTP ${response.status} ${response.statusText} ${text.slice(0, 180)}`.trim(),
      );
    }
    const payload = text ? JSON.parse(text) : null;
    if (String(payload?.code || '00000') !== '00000') {
      throw new Error(
        `Bitget code=${payload?.code} msg=${payload?.msg || ''}`,
      );
    }
    return payload;
  } finally {
    clearTimeout(timer);
  }
}

function makeRow({
  displaySymbol,
  nativeSymbol,
  quote,
  interval,
  raw,
}) {
  if (!Array.isArray(raw) || raw.length < 5) return null;
  let openTime = Number(raw[0]);
  if (!Number.isFinite(openTime) || openTime <= 0) return null;
  if (openTime < 10_000_000_000) openTime *= 1000;
  if (openTime > 10_000_000_000_000) openTime /= 1000;

  const open = Number(raw[1]);
  const high = Number(raw[2]);
  const low = Number(raw[3]);
  const close = Number(raw[4]);
  if (![open, high, low, close].every(Number.isFinite)) return null;

  const baseVolume = Number(raw[5]);
  const quoteVolume = Number(raw[6]);
  const step = intervalMs(interval);
  return {
    provider: 'bitget',
    market_type: 'contract',
    symbol: displaySymbol,
    interval,
    kline_interval: interval,
    open_time: new Date(openTime).toISOString(),
    open_time_ms: openTime,
    close_time: new Date(openTime + step - 1).toISOString(),
    close_time_ms: openTime + step - 1,
    open,
    high,
    low,
    close,
    volume: Number.isFinite(baseVolume) ? Math.max(0, baseVolume) : 0,
    quote_volume: Number.isFinite(quoteVolume) ? Math.max(0, quoteVolume) : 0,
    trade_count: 0,
    raw_symbol: nativeSymbol,
    native_symbol: nativeSymbol,
    quote_asset: quote,
    source: `bitget_official_${String(quote || '').toLowerCase()}_contract_kline_render_boundary_paged_v2`,
  };
}

function dedupeSort(rows) {
  return [...new Map(
    rows
      .filter(Boolean)
      .map((row) => [Number(row.open_time_ms), row]),
  ).values()]
    .filter((row) => Number.isFinite(Number(row.open_time_ms)))
    .sort((a, b) => Number(a.open_time_ms) - Number(b.open_time_ms));
}

function wideGaps(rows, interval) {
  const result = [];
  const sorted = dedupeSort(rows);
  for (let i = 1; i < sorted.length; i += 1) {
    const left = Number(sorted[i - 1].open_time_ms);
    const right = Number(sorted[i].open_time_ms);
    const delta = right - left;
    const wide =
      interval === '3d'
        ? delta > 90 * 60 * 60_000
        : interval === '1w'
          ? delta > 216 * 60 * 60_000
          : delta > 35 * 86_400_000;
    if (wide) result.push({ left, right, delta });
  }
  return result;
}

function coverage(rows, interval, endMs) {
  const sorted = dedupeSort(rows);
  if (!sorted.length) {
    return {
      row_count: 0,
      first_open_time: null,
      last_open_time: null,
      gap_count: 0,
      missing_intervals: 0,
      lag_intervals_to_end: null,
      continuous_to_current: false,
    };
  }

  let gapCount = 0;
  let missingIntervals = 0;
  const step = intervalMs(interval);
  for (let i = 1; i < sorted.length; i += 1) {
    const delta =
      Number(sorted[i].open_time_ms) -
      Number(sorted[i - 1].open_time_ms);
    if (interval === '1M') {
      if (delta > 32 * 86_400_000) {
        gapCount += 1;
        missingIntervals += Math.max(
          1,
          Math.round(delta / (30.4375 * 86_400_000)) - 1,
        );
      }
    } else if (delta > step) {
      gapCount += 1;
      missingIntervals += Math.max(0, Math.round(delta / step) - 1);
    }
  }

  const lastOpen = Number(sorted.at(-1).open_time_ms);
  const safeEnd = Math.max(0, Number(endMs || Date.now()) - 1);
  let lag = 0;
  if (interval === '1w') {
    const anchor = ((lastOpen % step) + step) % step;
    const target =
      safeEnd - (((safeEnd - anchor) % step) + step) % step;
    lag = Math.max(0, Math.round((target - lastOpen) / step));
  } else if (interval === '1M') {
    const age = Math.max(0, safeEnd - lastOpen);
    lag =
      age <= 32 * 86_400_000
        ? 0
        : Math.max(
            1,
            Math.floor(age / (30.4375 * 86_400_000)),
          );
  } else {
    const target = Math.floor(safeEnd / step) * step;
    lag = Math.max(0, Math.round((target - lastOpen) / step));
  }

  return {
    row_count: sorted.length,
    first_open_time: sorted[0].open_time,
    last_open_time: sorted.at(-1).open_time,
    gap_count: gapCount,
    missing_intervals: missingIntervals,
    lag_intervals_to_end: lag,
    continuous_to_current: gapCount === 0 && lag <= 1,
  };
}

function sendJson(res, status, body) {
  if (!res.headersSent) {
    res.writeHead(status, {
      'content-type': 'application/json; charset=utf-8',
      'access-control-allow-origin': '*',
      'cache-control': 'no-store',
    });
  }
  res.end(JSON.stringify(body));
}

async function resolveBitgetIdentity(displaySymbol) {
  const marketRest = await import('./market-rest.mjs');
  return await marketRest.resolveNativeMarketIdentity(
    'bitget',
    'contract',
    displaySymbol,
  );
}

async function fetchBitgetLongRows({
  displaySymbol,
  interval,
  requestedEnd,
  endTimeProvided,
  limit,
}) {
  const identity = await resolveBitgetIdentity(displaySymbol);
  const nativeSymbol = compact(
    identity?.native_symbol ||
      identity?.raw_symbol ||
      displaySymbol,
  );
  const quote = String(
    identity?.quote_asset ||
      (displaySymbol.endsWith('USDC')
        ? 'USDC'
        : displaySymbol.endsWith('USD')
          ? 'USD'
          : 'USDT'),
  ).toUpperCase();
  const productType = bitgetProductType(quote);
  const granularity = bitgetBar(interval);
  if (!nativeSymbol || !granularity) {
    throw new Error('Bitget exact contract identity/interval unavailable');
  }

  const safeLimit = Math.max(
    20,
    Math.min(1000, Number(limit) || 120),
  );
  const targetRows = Math.min(
    safeLimit,
    interval === '1M' ? 24 : interval === '1w' ? 52 : 60,
  );
  const safeRequestedEnd = Math.min(
    Date.now(),
    Math.max(1, Number(requestedEnd) || Date.now()),
  );

  const cacheKey =
    `bitget_long_continuity_v2:${nativeSymbol}:${productType}:` +
    `${interval}:${endTimeProvided ? safeRequestedEnd : 'current'}:${targetRows}`;

  return await sharedResult(cacheKey, async () => {
    const collected = [];
    let calls = 0;
    let lastError = null;

    const appendPayload = (payload) => {
      const mapped = payloadRows(payload)
        .map((raw) =>
          makeRow({
            displaySymbol,
            nativeSymbol,
            quote,
            interval,
            raw,
          }),
        )
        .filter(Boolean);
      collected.push(...mapped);
      return mapped;
    };

    const historyRead = async (endTime) => {
      if (calls >= 8) return [];
      const params = new URLSearchParams({
        symbol: nativeSymbol,
        productType,
        granularity,
        limit: '200',
      });
      if (Number.isFinite(Number(endTime)) && Number(endTime) > 0) {
        params.set('endTime', String(Math.floor(Number(endTime))));
      }
      calls += 1;
      const payload = await jsonFetch(
        `https://api.bitget.com/api/v2/mix/market/history-candles?${params.toString()}`,
      );
      return appendPayload(payload);
    };

    const currentRead = async () => {
      if (calls >= 8) return [];
      const params = new URLSearchParams({
        symbol: nativeSymbol,
        productType,
        granularity,
        limit: String(Math.min(1000, Math.max(targetRows, 100))),
      });
      calls += 1;
      const payload = await jsonFetch(
        `https://api.bitget.com/api/v2/mix/market/candles?${params.toString()}`,
      );
      return appendPayload(payload);
    };

    if (!endTimeProvided) {
      try {
        // Historical page first: it is the continuity authority.
        await historyRead(Date.now());
      } catch (error) {
        lastError = error;
      }
      try {
        // Merge current page only to keep the newest native/current row.
        await currentRead();
      } catch (error) {
        lastError = error;
      }
    } else {
      const overlap =
        interval === '1M'
          ? 35 * 86_400_000
          : interval === '1w'
            ? 8 * 86_400_000
            : 4 * 86_400_000;
      try {
        await historyRead(
          Math.min(Date.now(), safeRequestedEnd + overlap),
        );
      } catch (error) {
        lastError = error;
      }
    }

    // Exact-open overlap pagination. Never use oldest - 1ms.
    while (calls < 8) {
      let rows = dedupeSort(collected);
      if (endTimeProvided) {
        rows = rows.filter(
          (row) => Number(row.open_time_ms) <= safeRequestedEnd,
        );
      }
      if (rows.length >= targetRows) break;
      if (!rows.length) break;

      const oldest = Number(rows[0].open_time_ms);
      const beforeCount = rows.length;
      try {
        await historyRead(oldest);
      } catch (error) {
        lastError = error;
        break;
      }

      let after = dedupeSort(collected);
      if (endTimeProvided) {
        after = after.filter(
          (row) => Number(row.open_time_ms) <= safeRequestedEnd,
        );
      }
      const afterOldest = Number(after[0]?.open_time_ms);
      if (
        after.length <= beforeCount ||
        !Number.isFinite(afterOldest) ||
        afterOldest >= oldest
      ) {
        break;
      }
    }

    // Target any remaining interior join gap with the exact right-hand open.
    while (calls < 8) {
      let rows = dedupeSort(collected);
      if (endTimeProvided) {
        rows = rows.filter(
          (row) => Number(row.open_time_ms) <= safeRequestedEnd,
        );
      }
      const gaps = wideGaps(rows, interval);
      if (!gaps.length) break;

      const beforeCount = rows.length;
      try {
        await historyRead(gaps[0].right);
      } catch (error) {
        lastError = error;
        break;
      }
      let after = dedupeSort(collected);
      if (endTimeProvided) {
        after = after.filter(
          (row) => Number(row.open_time_ms) <= safeRequestedEnd,
        );
      }
      if (after.length <= beforeCount) break;
    }

    let rows = dedupeSort(collected);
    if (endTimeProvided) {
      rows = rows.filter(
        (row) => Number(row.open_time_ms) <= safeRequestedEnd,
      );
    }
    rows = rows.slice(-safeLimit);

    if (!rows.length) {
      throw (
        lastError ||
        new Error(`Bitget long Kline unavailable for ${displaySymbol}`)
      );
    }

    return {
      rows,
      nativeSymbol,
      quote,
      productType,
      calls,
    };
  });
}

function shouldHandleBitgetLong(reqUrl, reqMethod) {
  if (reqMethod !== 'GET') return false;
  let url;
  try {
    url = new URL(reqUrl || '/', 'http://127.0.0.1');
  } catch (_) {
    return false;
  }
  const provider = String(url.searchParams.get('provider') || '')
    .trim()
    .toLowerCase();
  const market = String(
    url.searchParams.get('market_type') ||
      url.searchParams.get('market') ||
      '',
  )
    .trim()
    .toLowerCase();
  const interval = String(
    url.searchParams.get('interval') || '',
  ).trim();
  return (
    url.pathname === '/api/klines' &&
    provider === 'bitget' &&
    /contract|future|perpetual|swap|linear/.test(market) &&
    LONG_INTERVALS.has(interval)
  );
}

async function handleBitgetLong(req, res) {
  const url = new URL(req.url || '/', 'http://127.0.0.1');
  const symbol = compact(url.searchParams.get('symbol'));
  const interval = String(url.searchParams.get('interval') || '');
  const endTimeProvided = url.searchParams.has('end_time');
  const requestedEnd = endTimeProvided
    ? Number(url.searchParams.get('end_time'))
    : Date.now();
  const requestedLimit = Math.max(
    20,
    Math.min(1000, Number(url.searchParams.get('limit')) || 120),
  );

  if (!symbol) {
    sendJson(res, 400, {
      ok: false,
      version: STEP_VERSION,
      error: 'symbol required',
      rows: [],
    });
    return;
  }

  try {
    const result = await fetchBitgetLongRows({
      displaySymbol: symbol,
      interval,
      requestedEnd,
      endTimeProvided,
      limit: requestedLimit,
    });
    const rows = result.rows;
    const cv = coverage(rows, interval, requestedEnd);
    sendJson(res, 200, {
      ok: true,
      version: STEP_VERSION,
      provider: 'bitget',
      market_type: 'contract',
      symbol,
      interval,
      transport: 'official_public_market_rest_shared_continuity_wrapper',
      requested_limit: requestedLimit,
      returned_rows: rows.length,
      rows,
      coverage: cv,
      source:
        rows.at(-1)?.source ||
        rows[0]?.source ||
        'bitget_official_contract_kline_render_boundary_paged_v2',
      cached_at: new Date().toISOString(),
      step1042_bitget_long_kline_continuity: {
        schema: WRAPPER_SCHEMA,
        cache_ttl_ms: CACHE_TTL_MS,
        cache_max: CACHE_MAX,
        shared_singleflight: true,
        max_official_calls_per_cold_exact_key: 8,
        exact_open_overlap_cursor: true,
        oldest_minus_one_ms_retired: true,
        targeted_gap_repair: true,
        upstream_calls_this_build: result.calls,
        product_type: result.productType,
        native_symbol: result.nativeSymbol,
      },
    });
  } catch (error) {
    sendJson(res, 502, {
      ok: false,
      version: STEP_VERSION,
      provider: 'bitget',
      market_type: 'contract',
      symbol,
      interval,
      error: String(error?.message || error),
      rows: [],
      cached_at: new Date().toISOString(),
      step1042_bitget_long_kline_continuity: {
        schema: WRAPPER_SCHEMA,
        shared_singleflight: true,
        max_official_calls_per_cold_exact_key: 8,
      },
    });
  }
}

// Wrap the parent HTTP server before importing the existing proxy.
// All non-Bitget-long-Kline routes remain byte-for-byte owned by proxy.mjs.
const originalCreateServer = http.createServer.bind(http);
http.createServer = function patchedCreateServer(listener, ...rest) {
  return originalCreateServer(async (req, res) => {
    if (shouldHandleBitgetLong(req.url, req.method)) {
      await handleBitgetLong(req, res);
      return;
    }

    // Preserve the full original /health body but expose this wrapper's
    // service version/health marker for manual deployment verification.
    let restoreEnd = null;
    let originalEnd = null;
    let originalRemoveHeader = null;
    let healthUrl = false;
    try {
      const parsed = new URL(req.url || '/', 'http://127.0.0.1');
      healthUrl = parsed.pathname === '/health';
    } catch (_) {
      healthUrl = false;
    }

    if (healthUrl) {
      originalEnd = res.end.bind(res);
      originalRemoveHeader = res.removeHeader.bind(res);
      const patchedEnd = (chunk, encoding, callback) => {
        try {
          const text = Buffer.isBuffer(chunk)
            ? chunk.toString('utf8')
            : String(chunk ?? '');
          const body = JSON.parse(text);
          body.version = STEP_VERSION;
          body.step1042_bitget_long_kline_continuity = {
            enabled: true,
            schema: WRAPPER_SCHEMA,
            cache_ttl_ms: CACHE_TTL_MS,
            cache_max: CACHE_MAX,
            shared_singleflight: true,
            max_official_calls_per_cold_exact_key: 8,
            exact_open_overlap_cursor: true,
            oldest_minus_one_ms_retired: true,
            targeted_gap_repair: true,
          };
          originalRemoveHeader('content-length');
          return originalEnd(
            JSON.stringify(body),
            encoding,
            callback,
          );
        } catch (_) {
          return originalEnd(chunk, encoding, callback);
        }
      };
      restoreEnd = () => {
        // Nothing to restore after the request has ended; retain for clarity.
      };
      res.end = patchedEnd;
    }

    try {
      return await listener(req, res);
    } finally {
      if (restoreEnd) restoreEnd();
    }
  }, ...rest);
};

await import('./proxy.mjs');

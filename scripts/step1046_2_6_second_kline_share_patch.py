from pathlib import Path

root = Path(__file__).resolve().parents[1]
market_path = root / 'src' / 'market-rest.mjs'
proxy_path = root / 'src' / 'proxy.mjs'
text = market_path.read_text(encoding='utf-8')

anchor = "const bitgetLongKlineInflight = new Map();\n"
if anchor not in text:
    raise SystemExit('missing bitget long cache anchor')
insert = r'''

// Step1046.2.6: one-second history is an on-demand exact-key feature, but user
// fan-out must never multiply the same official trade-history build. Share the
// newest page for a few seconds (the live WebSocket owns subsequent ticks) and
// share historical pages much longer. This cache stores only real-trade 1s
// candles; empty-second horizontal bars remain an App rendering concern.
const SECOND_KLINE_CACHE_MAX = 256;
const SECOND_KLINE_LATEST_BUCKET_MS = 2_000;
const SECOND_KLINE_LATEST_TTL_MS = 4_000;
const SECOND_KLINE_HISTORY_TTL_MS = 15 * 60_000;
const SECOND_KLINE_LATEST_WINDOW_MS = 30_000;
const secondKlineCache = new Map();
const secondKlineInflight = new Map();
const secondKlineShareStats = {
  reads: 0,
  cache_hits: 0,
  inflight_hits: 0,
  builds_started: 0,
  builds_succeeded: 0,
  builds_failed: 0,
  evictions: 0,
};
'''
text = text.replace(anchor, anchor + insert, 1)

helper_anchor = "\n// Step788.1:\n"
if helper_anchor not in text:
    raise SystemExit('missing Step788.1 anchor')
helpers = r'''

function pruneSecondKlineCache() {
  const now = Date.now();
  for (const [key, entry] of secondKlineCache.entries()) {
    if (Number(entry?.expiresAt || 0) <= now) secondKlineCache.delete(key);
  }
  while (secondKlineCache.size > SECOND_KLINE_CACHE_MAX) {
    const oldest = secondKlineCache.keys().next().value;
    if (oldest == null) break;
    secondKlineCache.delete(oldest);
    secondKlineShareStats.evictions += 1;
  }
}

function secondKlineSharedKey(provider, market, symbol, end, limit) {
  const now = Date.now();
  const rawEnd = Number(end);
  const safeEnd = Number.isFinite(rawEnd) && rawEnd > 0
    ? Math.min(rawEnd, now)
    : now;
  const latest = safeEnd >= now - SECOND_KLINE_LATEST_WINDOW_MS;
  // Current-page requests from different phones naturally differ by a few ms.
  // Canonicalize only the cache key, never the official request boundary.
  const endKey = latest
    ? Math.floor(safeEnd / SECOND_KLINE_LATEST_BUCKET_MS) * SECOND_KLINE_LATEST_BUCKET_MS
    : Math.floor(safeEnd / 1_000) * 1_000;
  return {
    key: `second_kline:${provider}:${market}:${symbol}:${Math.max(1, Number(limit) || 1)}:${endKey}`,
    latest,
  };
}

async function sharedSecondKlineResult(provider, market, symbol, end, limit, loader) {
  secondKlineShareStats.reads += 1;
  pruneSecondKlineCache();
  const { key, latest } = secondKlineSharedKey(provider, market, symbol, end, limit);
  const cached = secondKlineCache.get(key);
  if (cached && cached.expiresAt > Date.now()) {
    secondKlineShareStats.cache_hits += 1;
    return cached.rows;
  }
  const running = secondKlineInflight.get(key);
  if (running) {
    secondKlineShareStats.inflight_hits += 1;
    return await running;
  }

  secondKlineShareStats.builds_started += 1;
  const task = Promise.resolve()
    .then(loader)
    .then((rows) => {
      const safeRows = Array.isArray(rows) ? rows : [];
      secondKlineCache.set(key, {
        rows: safeRows,
        expiresAt: Date.now() + (latest
          ? SECOND_KLINE_LATEST_TTL_MS
          : SECOND_KLINE_HISTORY_TTL_MS),
      });
      pruneSecondKlineCache();
      secondKlineShareStats.builds_succeeded += 1;
      return safeRows;
    })
    .catch((error) => {
      secondKlineShareStats.builds_failed += 1;
      throw error;
    });
  secondKlineInflight.set(key, task);
  try {
    return await task;
  } finally {
    if (secondKlineInflight.get(key) === task) secondKlineInflight.delete(key);
  }
}

function getSecondKlineShareHealth() {
  pruneSecondKlineCache();
  return {
    mode: 'exact_provider_market_symbol_end_bucket_limit_shared_cache_singleflight',
    cache_entries: secondKlineCache.size,
    cache_max: SECOND_KLINE_CACHE_MAX,
    inflight_entries: secondKlineInflight.size,
    latest_bucket_ms: SECOND_KLINE_LATEST_BUCKET_MS,
    latest_ttl_ms: SECOND_KLINE_LATEST_TTL_MS,
    historical_ttl_ms: SECOND_KLINE_HISTORY_TTL_MS,
    real_trade_rows_only: true,
    empty_second_rows_fabricated_by_backend: false,
    same_exact_key_reads_scale_upstream_with_users: false,
    ...secondKlineShareStats,
  };
}
'''
text = text.replace(helper_anchor, helpers + helper_anchor, 1)

old = r'''  if (interval === '1s') {
    return fetchSecondMarketKlines(
      provider, market, symbol, end, limit, options,
    );
  }
'''
new = r'''  if (interval === '1s') {
    return await sharedSecondKlineResult(
      provider,
      market,
      symbol,
      end,
      limit,
      () => fetchSecondMarketKlines(
        provider, market, symbol, end, limit, options,
      ),
    );
  }
'''
if old not in text:
    raise SystemExit('missing 1s fetch branch anchor')
text = text.replace(old, new, 1)

health_anchor = "    one_second_history_end_time_pagination: {\n"
if health_anchor not in text:
    raise SystemExit('missing one-second health anchor')
health_insert = r'''    one_second_history_shared_cache: getSecondKlineShareHealth(),
    one_second_history_same_exact_key_reads_share_cache_and_inflight: true,
'''
text = text.replace(health_anchor, health_insert + health_anchor, 1)

market_path.write_text(text, encoding='utf-8')

proxy = proxy_path.read_text(encoding='utf-8')
old_version = "const STEP_VERSION = '650.8.15.197.3.3.25';"
new_version = "const STEP_VERSION = '650.8.15.197.3.3.26';"
if old_version not in proxy:
    raise SystemExit('missing proxy version anchor')
proxy = proxy.replace(old_version, new_version, 1)
proxy_path.write_text(proxy, encoding='utf-8')

print('patched market-rest 1s shared cache/singleflight and proxy .26')

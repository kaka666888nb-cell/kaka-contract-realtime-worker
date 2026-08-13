// Step1004.7 / original Step999 support bus.
// Keeps only already-collected same-venue 5m flow metrics in the deep-market
// worker. It starts no exchange requests/connections and exists solely to let
// contract-focus-pool score dynamic hot symbols without importing contract-flow
// (which would create a circular dependency).

const VERSION = '650.8.15.129';
const FLOW_TTL_MS = 45 * 60_000;
const MAX_KEYS = 6000;
const flowHistoryByKey = new Map();
let publishedRows = 0;
let publishedBatches = 0;
let lastPublishedAt = 0;

function finite(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function providerKey(raw) {
  const value = String(raw || '').trim().toLowerCase().replaceAll('gate.io', 'gate');
  return value === 'okex' ? 'okx' : value;
}

function symbolKey(raw) {
  return String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function timeMs(raw) {
  if (raw == null) return null;
  if (typeof raw === 'number' && Number.isFinite(raw)) return raw > 1e12 ? raw : raw * 1000;
  const parsed = Date.parse(String(raw));
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeRow(raw) {
  const provider = providerKey(raw?.provider);
  const symbol = symbolKey(raw?.symbol);
  const at = timeMs(raw?.bucket_time ?? raw?.bucket_start ?? raw?.start ?? raw?.updated_at);
  const buy = finite(raw?.buy_quote ?? raw?.taker_buy_quote_volume ?? raw?.taker_buy_quote);
  const sell = finite(raw?.sell_quote ?? raw?.taker_sell_quote_volume ?? raw?.taker_sell_quote);
  const tradeCount = finite(raw?.trade_count ?? raw?.sample_count);
  if (!provider || !symbol || !Number.isFinite(at) || at <= 0 || buy == null || sell == null) return null;
  return {
    provider,
    symbol,
    at,
    buy_quote: Math.max(0, buy),
    sell_quote: Math.max(0, sell),
    total_quote: Math.max(0, buy) + Math.max(0, sell),
    trade_count: Math.max(0, tradeCount || 0),
  };
}

function prune(now = Date.now()) {
  const cutoff = now - FLOW_TTL_MS;
  for (const [key, rows] of flowHistoryByKey.entries()) {
    const kept = rows.filter((row) => row.at >= cutoff).slice(-3);
    if (kept.length) flowHistoryByKey.set(key, kept);
    else flowHistoryByKey.delete(key);
  }
  while (flowHistoryByKey.size > MAX_KEYS) {
    const first = flowHistoryByKey.keys().next().value;
    flowHistoryByKey.delete(first);
  }
}

export function publishContractFlowHotScoreRows(rows) {
  const now = Date.now();
  let accepted = 0;
  for (const raw of Array.isArray(rows) ? rows : []) {
    const row = normalizeRow(raw);
    if (!row) continue;
    const key = `${row.provider}:${row.symbol}`;
    const existing = flowHistoryByKey.get(key) || [];
    const byTime = new Map(existing.map((item) => [item.at, item]));
    byTime.set(row.at, row);
    const next = [...byTime.values()].sort((a, b) => a.at - b.at).slice(-3);
    flowHistoryByKey.set(key, next);
    accepted += 1;
  }
  if (accepted > 0) {
    publishedRows += accepted;
    publishedBatches += 1;
    lastPublishedAt = now;
  }
  prune(now);
  return accepted;
}

export function getContractFlowHotScoreMetric(providerRaw, symbolRaw, now = Date.now()) {
  const provider = providerKey(providerRaw);
  const symbol = symbolKey(symbolRaw);
  const rows = flowHistoryByKey.get(`${provider}:${symbol}`) || [];
  const fresh = rows.filter((row) => now - row.at <= FLOW_TTL_MS).sort((a, b) => a.at - b.at);
  if (!fresh.length) return null;
  const current = fresh.at(-1);
  const previous = fresh.length >= 2 ? fresh.at(-2) : null;
  const currentTotal = Math.max(0, current.total_quote || 0);
  const previousTotal = previous ? Math.max(0, previous.total_quote || 0) : null;
  const activeChangeLog = previousTotal != null
    ? Math.abs(Math.log1p(currentTotal) - Math.log1p(previousTotal))
    : null;
  return {
    provider,
    symbol,
    source: 'existing_shared_contract_flow_5m_same_venue',
    source_time: new Date(current.at).toISOString(),
    source_age_ms: Math.max(0, now - current.at),
    taker_quote_5m: currentTotal,
    trade_count_5m: Math.max(0, current.trade_count || 0),
    taker_imbalance_abs_ratio: currentTotal > 0
      ? Math.abs(current.buy_quote - current.sell_quote) / currentTotal
      : 0,
    active_trade_change_log_abs: activeChangeLog,
    previous_source_time: previous ? new Date(previous.at).toISOString() : null,
  };
}

export function getHotScoreMetricsHealth() {
  prune(Date.now());
  let symbolsWithTwoBuckets = 0;
  for (const rows of flowHistoryByKey.values()) if (rows.length >= 2) symbolsWithTwoBuckets += 1;
  return {
    ok: true,
    version: VERSION,
    mode: 'same_venue_existing_flow_metrics_only',
    key_count: flowHistoryByKey.size,
    symbols_with_two_buckets: symbolsWithTwoBuckets,
    published_rows: publishedRows,
    published_batches: publishedBatches,
    last_published_at: lastPublishedAt ? new Date(lastPublishedAt).toISOString() : null,
    flow_ttl_minutes: FLOW_TTL_MS / 60_000,
    max_keys: MAX_KEYS,
    exchange_requests_started: 0,
    exchange_connections_started: 0,
    reads_scale_with_users: false,
  };
}

export const __hotScoreMetricsTest = Object.freeze({ normalizeRow });

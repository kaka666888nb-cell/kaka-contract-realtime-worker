// Step1046.2.5 shared K-line derivation + bounded zero-trade continuity helpers.
// Financial correctness contract:
// - never creates price history before the first or after the last real source bar;
// - only fills INTERNAL missing buckets bounded by two real/derived bars;
// - a fill is flat at the previous close with volume=0;
// - source outages / unsupported history are not converted into bars;
// - derived OHLCV uses the same exact provider/product/pool source only.

export const KAKA_DERIVED_INTERVALS = Object.freeze([
  '3m', '30m', '2h', '6h', '8h', '12h', '3d', '1w', '1M',
]);

export const KAKA_FULL_INTERVALS = Object.freeze([
  '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h',
  '1d', '3d', '1w', '1M',
]);

export const KAKA_DERIVED_PLAN = Object.freeze({
  '3m': Object.freeze({ base: '1m' }),
  '30m': Object.freeze({ base: '5m' }),
  '2h': Object.freeze({ base: '1h' }),
  '6h': Object.freeze({ base: '1h' }),
  '8h': Object.freeze({ base: '4h' }),
  '12h': Object.freeze({ base: '4h' }),
  '3d': Object.freeze({ base: '1d' }),
  '1w': Object.freeze({ base: '1d' }),
  '1M': Object.freeze({ base: '1d' }),
});

const FIXED_MS = Object.freeze({
  '1m': 60_000,
  '3m': 3 * 60_000,
  '5m': 5 * 60_000,
  '15m': 15 * 60_000,
  '30m': 30 * 60_000,
  '1h': 60 * 60_000,
  '2h': 2 * 60 * 60_000,
  '4h': 4 * 60 * 60_000,
  '6h': 6 * 60 * 60_000,
  '8h': 8 * 60 * 60_000,
  '12h': 12 * 60 * 60_000,
  '1d': 24 * 60 * 60_000,
  '3d': 3 * 24 * 60 * 60_000,
  '1w': 7 * 24 * 60 * 60_000,
});

export function klineIntervalMs(interval) {
  return Number(FIXED_MS[interval] || 0) || null;
}

export function klineDerivedPlan(interval) {
  return KAKA_DERIVED_PLAN[interval] || null;
}

function numberOrNull(value) {
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

function cleanRow(row) {
  if (!row || typeof row !== 'object') return null;
  const t = numberOrNull(row.open_time_ms);
  const o = numberOrNull(row.open);
  const h = numberOrNull(row.high);
  const l = numberOrNull(row.low);
  const c = numberOrNull(row.close);
  if (t === null || t <= 0 || o === null || h === null || l === null || c === null || h < l) return null;
  return {
    ...row,
    open_time_ms: Math.trunc(t),
    open: o,
    high: h,
    low: l,
    close: c,
    volume: Math.max(0, numberOrNull(row.volume) ?? 0),
    quote_volume: Math.max(0, numberOrNull(row.quote_volume) ?? numberOrNull(row.volume) ?? 0),
  };
}

export function uniqueSortedKlineRows(rows = []) {
  const byTime = new Map();
  for (const raw of rows) {
    const row = cleanRow(raw);
    if (row) byTime.set(row.open_time_ms, row);
  }
  return [...byTime.values()].sort((a, b) => a.open_time_ms - b.open_time_ms);
}

function mondayUtcBucket(ts) {
  const d = new Date(ts);
  const day = d.getUTCDay();
  const delta = (day + 6) % 7;
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - delta);
}

export function klineBucketStart(ts, interval) {
  const n = Number(ts);
  if (!Number.isFinite(n) || n <= 0) return null;
  if (interval === '1M') {
    const d = new Date(n);
    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1);
  }
  if (interval === '1w') return mondayUtcBucket(n);
  const step = klineIntervalMs(interval);
  if (!step) return null;
  return Math.floor(n / step) * step;
}

export function aggregateExactKlineRows(rows, targetInterval, {
  sourceInterval = '',
  limit = 300,
  source = '',
} = {}) {
  const input = uniqueSortedKlineRows(rows);
  const buckets = new Map();
  for (const row of input) {
    const bucket = klineBucketStart(row.open_time_ms, targetInterval);
    if (bucket === null) continue;
    let item = buckets.get(bucket);
    if (!item) {
      item = {
        ...row,
        open_time_ms: bucket,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        volume: 0,
        quote_volume: 0,
        trades: row.trades == null ? null : 0,
        _first: row.open_time_ms,
        _last: row.open_time_ms,
        _parts: 0,
      };
      buckets.set(bucket, item);
    }
    if (row.open_time_ms < item._first) {
      item._first = row.open_time_ms;
      item.open = row.open;
    }
    if (row.open_time_ms >= item._last) {
      item._last = row.open_time_ms;
      item.close = row.close;
    }
    item.high = Math.max(item.high, row.high);
    item.low = Math.min(item.low, row.low);
    item.volume += Math.max(0, numberOrNull(row.volume) ?? 0);
    item.quote_volume += Math.max(0, numberOrNull(row.quote_volume) ?? numberOrNull(row.volume) ?? 0);
    if (row.trades != null) item.trades = (item.trades || 0) + Math.max(0, numberOrNull(row.trades) ?? 0);
    item._parts += 1;
  }
  const out = [...buckets.values()]
    .sort((a, b) => a.open_time_ms - b.open_time_ms)
    .map(({ _first, _last, _parts, ...item }) => ({
      ...item,
      bar_origin: 'shared_derived',
      derived_from_interval: sourceInterval || null,
      source_parts: _parts,
      source: source || item.source,
    }));
  return out.slice(-Math.max(1, Math.min(1000, Number(limit) || 300)));
}

function sameUtcDay(a, b) {
  const da = new Date(a);
  const db = new Date(b);
  return da.getUTCFullYear() === db.getUTCFullYear() &&
    da.getUTCMonth() === db.getUTCMonth() && da.getUTCDate() === db.getUTCDate();
}

function nextCalendarBucket(ts, interval) {
  if (interval === '1M') {
    const d = new Date(ts);
    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1);
  }
  const step = klineIntervalMs(interval);
  return step ? ts + step : null;
}

// sessionMode:
// - continuous_24x7: chain pools / truly continuous token markets; gaps may cross dates.
// - intraday_only: stock/RWA-like markets; never fills across UTC day boundaries, so weekends/
//   overnight closures cannot be misrepresented as zero-trade bars.
// - none: no synthetic flat bars.
export function fillBoundedZeroTradeGaps(rows, interval, {
  sessionMode = 'none',
  maxGapBars = 96,
  source = '',
  limit = 300,
} = {}) {
  const input = uniqueSortedKlineRows(rows);
  if (input.length < 2 || sessionMode === 'none') {
    return { rows: input.slice(-limit), filled: 0 };
  }
  const out = [input[0]];
  let filled = 0;
  for (let i = 1; i < input.length; i += 1) {
    const prevReal = input[i - 1];
    const nextReal = input[i];
    let cursor = nextCalendarBucket(prevReal.open_time_ms, interval);
    if (cursor === null || cursor >= nextReal.open_time_ms) {
      out.push(nextReal);
      continue;
    }
    const missing = [];
    while (cursor < nextReal.open_time_ms && missing.length <= maxGapBars) {
      if (sessionMode === 'intraday_only' && !sameUtcDay(prevReal.open_time_ms, cursor)) break;
      if (sessionMode === 'intraday_only' && !sameUtcDay(cursor, nextReal.open_time_ms)) break;
      missing.push(cursor);
      cursor = nextCalendarBucket(cursor, interval);
      if (cursor === null) break;
    }
    // Fill only when the entire gap is exactly representable and bounded. If not, fail closed.
    if (missing.length > 0 && missing.length <= maxGapBars && cursor === nextReal.open_time_ms) {
      let lastClose = prevReal.close;
      for (const ts of missing) {
        out.push({
          ...prevReal,
          open_time_ms: ts,
          open: lastClose,
          high: lastClose,
          low: lastClose,
          close: lastClose,
          volume: 0,
          quote_volume: 0,
          trades: 0,
          confirm: '1',
          bar_origin: 'bounded_zero_trade_gap_fill',
          zero_trade_evidence: 'internal_missing_bucket_bounded_by_real_bars_same_exact_source',
          source: source || prevReal.source,
        });
        filled += 1;
      }
    }
    out.push(nextReal);
  }
  return { rows: uniqueSortedKlineRows(out).slice(-Math.max(1, limit)), filled };
}

export function deriveAndFillKlines(rows, targetInterval, {
  sourceInterval = '',
  source = '',
  sessionMode = 'none',
  maxGapBars = 96,
  limit = 300,
} = {}) {
  const aggregated = sourceInterval && sourceInterval !== targetInterval
    ? aggregateExactKlineRows(rows, targetInterval, { sourceInterval, source, limit: Math.max(limit, 300) })
    : uniqueSortedKlineRows(rows);
  const filled = fillBoundedZeroTradeGaps(aggregated, targetInterval, {
    sessionMode,
    maxGapBars,
    source,
    limit,
  });
  return {
    rows: filled.rows.slice(-Math.max(1, limit)),
    derived: Boolean(sourceInterval && sourceInterval !== targetInterval),
    derived_from_interval: sourceInterval || targetInterval,
    zero_trade_fill_count: filled.filled,
    zero_trade_fill_policy: sessionMode === 'none'
      ? 'disabled'
      : sessionMode === 'intraday_only'
        ? 'internal_exact_bucket_gap_same_utc_day_only'
        : 'internal_exact_bucket_gap_bounded_between_real_bars',
  };
}

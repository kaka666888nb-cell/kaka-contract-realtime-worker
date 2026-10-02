import assert from 'node:assert/strict';
import { _test } from '../src/binance-contract-kline-seed.mjs';

const {
  normalizeRows,
  inspectRecentContinuity,
  bridgeStartForRecentWindow,
  liveRowNeedsFinalization,
  persistableSnapshotRows,
} = _test;

const LIVE = 'binance_official_public_kline_live_bridge';
const RELAY = 'binance_official_public_kline_supabase_edge_relay_fapi_klines';
const interval = '15m';

function row(openIso, { open = 86000, close = open, source = LIVE, cachedAt, isClosed } = {}) {
  const openMs = Date.parse(openIso);
  return {
    provider: 'binance',
    market_type: 'contract',
    symbol: 'BTCUSDT',
    interval,
    open_time_ms: openMs,
    open_time: new Date(openMs).toISOString(),
    close_time: new Date(openMs + 900000 - 1).toISOString(),
    open,
    high: Math.max(open, close) + 10,
    low: Math.min(open, close) - 10,
    close,
    volume: 1,
    quote_volume: 1,
    trade_count: 1,
    source,
    cached_at: cachedAt ?? new Date(openMs + 900000).toISOString(),
    ...(isClosed === true || isClosed === false ? { is_closed: isClosed } : {}),
  };
}

// Exact production failure pattern observed on 2026-10-02:
// 05:30 candle was persisted at 05:35:30, 9.5 minutes before its 05:45 close.
const staleLegacy = row('2026-10-02T05:30:00.000Z', {
  open: 86319.7,
  close: 86226.5,
  cachedAt: '2026-10-02T05:35:30.285Z',
});
assert.equal(
  liveRowNeedsFinalization(staleLegacy, interval, Date.parse('2026-10-02T06:00:00Z')),
  true,
);

// A legacy live row cached after its bucket closed is not automatically condemned.
const legacyAfterClose = row('2026-10-02T05:15:00.000Z', {
  cachedAt: '2026-10-02T05:30:00.515Z',
});
assert.equal(
  liveRowNeedsFinalization(legacyAfterClose, interval, Date.parse('2026-10-02T06:00:00Z')),
  false,
);

// New code preserves Binance k.x. Closed rows are final; expired explicit-open rows are not.
const finalClosed = row('2026-10-02T05:00:00.000Z', {
  isClosed: true,
  cachedAt: '2026-10-02T05:15:00.050Z',
});
const expiredOpen = row('2026-10-02T05:30:00.000Z', {
  isClosed: false,
  cachedAt: '2026-10-02T05:40:00.000Z',
});
assert.equal(
  liveRowNeedsFinalization(finalClosed, interval, Date.parse('2026-10-02T06:00:00Z')),
  false,
);
assert.equal(
  liveRowNeedsFinalization(expiredOpen, interval, Date.parse('2026-10-02T06:00:00Z')),
  true,
);

// Current in-progress row is allowed in memory/UI, but is never persisted.
const currentOpen = row('2026-10-02T05:45:00.000Z', {
  isClosed: false,
  cachedAt: '2026-10-02T05:50:00.000Z',
});
assert.equal(
  liveRowNeedsFinalization(currentOpen, interval, Date.parse('2026-10-02T05:55:00Z')),
  false,
);
const persistable = persistableSnapshotRows(
  [finalClosed, staleLegacy, currentOpen, row('2026-10-02T05:30:00.000Z', { source: RELAY })],
  interval,
  Date.parse('2026-10-02T06:00:00Z'),
);
assert.equal(persistable.some((r) => r === staleLegacy), false);
assert.equal(persistable.some((r) => r === currentOpen), false);
assert.equal(persistable.some((r) => r === finalClosed), true);
assert.equal(persistable.some((r) => r.source === RELAY), true);

// Timestamp continuity alone must no longer certify a stale in-progress historical close.
const rows = normalizeRows([
  row('2026-10-02T05:15:00.000Z', { close: 86319.7, isClosed: true }),
  staleLegacy,
  row('2026-10-02T05:45:00.000Z', {
    open: 86101.6,
    close: 85964.8,
    source: RELAY,
    cachedAt: '2026-10-02T06:49:30.166Z',
  }),
], 'BTCUSDT', interval);
const end = Date.parse('2026-10-02T06:00:00Z');
const coverage = inspectRecentContinuity(rows, interval, end, 20);
assert.equal(coverage.gap_count, 0);
assert.equal(coverage.missing_intervals, 0);
assert.equal(coverage.unfinalized_live_rows, 1);
assert.equal(coverage.first_unfinalized_open_ms, Date.parse('2026-10-02T05:30:00Z'));
assert.equal(coverage.continuous_to_current, false);
assert.equal(
  bridgeStartForRecentWindow(rows, interval, end, 20),
  Date.parse('2026-10-02T05:30:00Z'),
);

console.log('PASS Step1077.15.18.4.12.15.10 Binance contract Kline finality guard');

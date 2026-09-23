import assert from 'node:assert/strict';

const { _test } = await import(
  `../src/bybit-second-history.mjs?step1073-v101-persist=${Date.now()}`
);

assert.equal(
  typeof _test.buildPersistChunks,
  'function',
  'the production chunk builder must be testable',
);

const fiveMinutesMs = 5 * 60_000;
const retentionMs = 2 * 60 * 60_000;
const newestMs = Date.parse('2026-09-23T10:00:00.000Z');
const rows = Array.from({ length: 63 }, (_, index) => {
  const openTimeMs = newestMs - ((62 - index) * fiveMinutesMs);
  return {
    open_time_ms: openTimeMs,
    open_time: new Date(openTimeMs).toISOString(),
  };
});

const { chunks, newestMs: builtNewestMs } = _test.buildPersistChunks({
  rows,
  lastPersistedSourceTimeMs: 0,
});

assert.equal(builtNewestMs, newestMs);
assert.ok(chunks.length > 0);
assert.ok(
  chunks.length <= 30,
  `Supabase RPC accepts at most 30 chunks, received ${chunks.length}`,
);
assert.ok(
  Date.parse(chunks[0].chunk_end) >= newestMs - retentionMs,
  'persist payload must not include chunks already outside the two-hour retention window',
);
assert.equal(
  chunks.at(-1).chunk_start,
  new Date(newestMs).toISOString(),
  'the newest verified second must remain in the payload',
);

console.log('PASS Step1073 V101 Bybit persistence stays inside the RPC and retention bounds');

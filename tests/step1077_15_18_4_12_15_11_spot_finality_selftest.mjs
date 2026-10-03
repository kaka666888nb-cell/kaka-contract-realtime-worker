import assert from 'node:assert/strict';
import fs from 'node:fs';

const files = {
  binance: 'supabase/functions/kaka-market-kline-proxy/index.ts',
  okx: 'supabase/functions/kaka-okx-market-kline-proxy/index.ts',
  gate: 'supabase/functions/kaka-gate-market-kline-proxy/index.ts',
  bitget: 'supabase/functions/kaka-bitget-market-kline-proxy/index.ts',
};

const source = Object.fromEntries(
  Object.entries(files).map(([key, path]) => [key, fs.readFileSync(path, 'utf8')]),
);

for (const [provider, code] of Object.entries(source)) {
  assert.match(code, /finalized_for_persistence/);
  assert.match(code, /skipped_unfinalized/);
  assert.match(code, /rpcUpsertKlinesBatch\(supabase, finalized\)/);
  assert.doesNotMatch(code, /rpcUpsertKlinesBatch\(supabase, normalized\)/);
}

assert.match(source.okx, /row\.confirm === "1"/);
assert.match(source.binance, /closeMs <= nowMs - 2_000/);
assert.match(source.gate, /closeMs <= finalityNowMs - 2_000/);
assert.match(source.bitget, /closeMs <= finalityNowMs - 2_000/);

function nextWindowContainsPriorCurrent({ intervalMinutes, cadenceMinutes, limit }) {
  const barsAdvanced = cadenceMinutes / intervalMinutes;
  assert.equal(Number.isInteger(barsAdvanced), true);
  // index 0 is the new current bar. The previous run's current bar is
  // barsAdvanced positions behind it and is present iff index < limit.
  return barsAdvanced < limit;
}

assert.equal(
  nextWindowContainsPriorCurrent({ intervalMinutes: 3, cadenceMinutes: 15, limit: 5 }),
  false,
);
assert.equal(
  nextWindowContainsPriorCurrent({ intervalMinutes: 3, cadenceMinutes: 15, limit: 6 }),
  true,
);
assert.equal(
  nextWindowContainsPriorCurrent({ intervalMinutes: 3, cadenceMinutes: 15, limit: 7 }),
  true,
);

console.log('PASS Step1077.15.18.4.12.15.11 spot Kline finality source gates');

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { __contractFundingSharedReadTest as t } from '../src/contract-funding.mjs';

const now=Date.parse('2026-09-18T16:10:00.000Z');
const fresh={
  provider:'okx',
  market_type:'contract',
  symbol:'BTCUSDT',
  last_funding_rate:0.0001,
  funding_rate:0.0001,
  last_funding_rate_percent:0.01,
  funding_rate_percent:0.01,
  next_funding_time:'2026-09-18T20:00:00.000Z',
  mark_price:77000,
  index_price:76990,
  source_time:'2026-09-18T16:00:00.000Z',
  cached_at:'2026-09-18T16:00:05.000Z',
};
const history=[{
  provider:'okx',
  market_type:'contract',
  symbol:'BTCUSDT',
  funding_time:'2026-09-18T12:00:00.000Z',
  funding_rate:0.00008,
  funding_rate_percent:0.008,
  mark_price:76000,
}];

const freshState=t.persistedFundingCurrentFreshness(fresh,now);
assert.equal(freshState.present,true);
assert.equal(freshState.stale,false);
assert.equal(freshState.age_ms,10*60_000);

const freshPayload=t.sharedFundingReadPayload({
  provider:'okx',symbol:'BTCUSDT',limit:24,includeHistory:true,
  bundle:{current:fresh,history,cache_hit:true,cache_age_ms:1000},
  nowMs:now,
});
assert.equal(freshPayload.current_stale,false);
assert.equal(freshPayload.partial,false);
assert.equal(freshPayload.history.length,1);
assert.equal(freshPayload.exchange_requests_started,0);
assert.equal(freshPayload.user_reads_trigger_exchange_requests,false);
assert.equal(freshPayload.reads_scale_with_users,false);
assert.equal(freshPayload.source,'render_shared_persisted_funding_cache');

const stale={...fresh,source_time:'2026-09-18T15:00:00.000Z'};
const stalePayload=t.sharedFundingReadPayload({
  provider:'okx',symbol:'BTCUSDT',limit:24,includeHistory:false,
  bundle:{current:stale,history:[]},
  nowMs:now,
});
assert.equal(stalePayload.current_stale,true);
assert.equal(stalePayload.partial,true);
assert.equal(stalePayload.current.last_funding_rate,0.0001);
assert.ok(stalePayload.warnings.includes('shared_current_stale_retained'));
assert.equal(stalePayload.history.length,0);

const missingPayload=t.sharedFundingReadPayload({
  provider:'gate',symbol:'XMRUSDT',limit:24,includeHistory:true,
  bundle:{current:null,history:[]},
  nowMs:now,
});
assert.equal(missingPayload.current,null);
assert.equal(missingPayload.partial,true);
assert.ok(missingPayload.warnings.includes('shared_current_missing'));
assert.equal(missingPayload.exchange_requests_started,0);

const source=readFileSync(new URL('../src/contract-funding.mjs',import.meta.url),'utf8');
const start=source.indexOf("if (url.pathname !== ROUTE) return false;");
const end=source.indexOf("\n  return true;\n}",start);
assert.ok(start>=0 && end>start);
const handler=source.slice(start,end);
for(const forbidden of [
  'load(provider, symbol',
  'serveBinanceFunding(',
  'scheduleBinanceFundingHistoryRefresh(',
  'fetchOkx(',
  'fetchBybit(',
  'fetchBitget(',
  'fetchGate(',
]){
  assert.equal(handler.includes(forbidden),false,`user handler must not contain ${forbidden}`);
}
assert.ok(handler.includes('readPersistedFundingBundle('));
assert.ok(handler.includes('user_reads_trigger_exchange_requests: false'));

console.log('Step1072.9 contract funding shared-read-only tests passed');

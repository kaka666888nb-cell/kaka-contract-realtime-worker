import assert from 'node:assert/strict';
import { __gateAdvancedFocusRecoveryTest as t } from '../src/gate-advanced-stats.mjs';

const symbols=[
  'BTCUSDT','ETHUSDT','BNBUSDT','XRPUSDT','SOLUSDT',
  'TRXUSDT','HYPEUSDT','DOGEUSDT','ZECUSDT','XMRUSDT',
  'TRUMPUSDT','SNXXUSDT','ONEUSDT','ARBUSDT','UNIUSDT',
];

const row=(symbol)=>({
  symbol,
  updated_at:new Date().toISOString(),
  official_contract_stats_available:true,
  official_risk_limit_tiers_available:true,
  contract_stats:{open_interest_contracts:1},
});

assert.equal(
  t.focusMembershipSignature(symbols.map(symbol=>({symbol}))),
  t.focusMembershipSignature([...symbols].reverse().map(symbol=>({symbol}))),
  'membership signature must ignore order-only resorting',
);

const all=t.coverageFromRows(symbols,symbols.map(row));
assert.equal(all.target,15);
assert.equal(all.complete,15);
assert.equal(all.ready,true);
assert.deepEqual(all.missing,[]);

const fourteen=t.coverageFromRows(symbols,symbols.slice(0,14).map(row));
assert.equal(fourteen.target,15);
assert.equal(fourteen.complete,14);
assert.equal(fourteen.ready,false);
assert.deepEqual(fourteen.missing,['UNIUSDT']);

const incomplete=t.coverageFromRows(symbols,[
  ...symbols.slice(0,14).map(row),
  {
    symbol:'UNIUSDT',
    updated_at:new Date().toISOString(),
    official_contract_stats_available:true,
    official_risk_limit_tiers_available:false,
    contract_stats:{open_interest_contracts:1},
  },
]);
assert.equal(incomplete.ready,false);
assert.deepEqual(incomplete.missing,['UNIUSDT']);

console.log('Step1072.9 Gate focus-change recovery tests passed');

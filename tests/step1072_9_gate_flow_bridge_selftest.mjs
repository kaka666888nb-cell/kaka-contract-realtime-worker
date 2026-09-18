import assert from 'node:assert/strict';
import { __contractFlowGateAdvancedBridgeTest as bridge } from '../src/contract-flow.mjs';

const sourceMs=Date.parse('2026-09-18T14:50:00.000Z');
const stat={
  source_time:'2026-09-18T14:50:00.000Z',
  source_time_s:Math.floor(sourceMs/1000),
  mark_price:100,
  lsr_taker:1.5,
  long_taker_size:6,
  short_taker_size:4,
};

assert.equal(bridge.gateAdvancedStatSourceTimeMs(stat),sourceMs);

{
  const state={
    provider:'gate',
    symbol:'SOLUSDT',
    gateQuantoMultiplier:0.01,
    gateQuoteValuePerContract:null,
    lastPrice:100,
  };
  const flow=bridge.gateAdvancedStatToOfficialFlow(state,stat,sourceMs+30_000);
  assert.ok(flow);
  assert.equal(flow.buyQuote,6);
  assert.equal(flow.sellQuote,4);
  assert.equal(flow.buySellRatio,1.5);
  assert.equal(flow.source,'gate_contract_stats_direct_multiplier');
}

{
  const state={
    provider:'gate',
    symbol:'SOLUSDT',
    gateQuantoMultiplier:0.01,
    gateQuoteValuePerContract:null,
    lastPrice:100,
  };
  const stale=bridge.gateAdvancedStatToOfficialFlow(
    state,
    stat,
    sourceMs+12*60_000+1,
  );
  assert.equal(stale,null);
}

{
  const state={
    provider:'gate',
    symbol:'SOLUSDT',
    gateQuantoMultiplier:null,
    gateQuoteValuePerContract:null,
    lastPrice:100,
  };
  const missingSizing=bridge.gateAdvancedStatToOfficialFlow(
    state,
    stat,
    sourceMs+30_000,
  );
  assert.equal(missingSizing,null);
}

{
  const inverseState={
    provider:'gate',
    symbol:'BTCUSD',
    gateQuantoMultiplier:null,
    gateQuoteValuePerContract:1,
    lastPrice:100000,
  };
  const inverseStat={...stat,mark_price:100000};
  const flow=bridge.gateAdvancedStatToOfficialFlow(
    inverseState,
    inverseStat,
    sourceMs+30_000,
  );
  assert.ok(flow);
  assert.equal(flow.buyQuote,6);
  assert.equal(flow.sellQuote,4);
  assert.equal(flow.source,'gate_contract_stats_inverse_one_usd_per_contract');
}

console.log('Step1072.9 Gate focus15 slow-stats flow bridge selftest passed');

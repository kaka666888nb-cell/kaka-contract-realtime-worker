import fs from 'node:fs';
import assert from 'node:assert/strict';

const moduleSource = fs.readFileSync(new URL('../src/market-light-snapshot.mjs', import.meta.url), 'utf8');
const proxySource = fs.readFileSync(new URL('../src/proxy.mjs', import.meta.url), 'utf8');
const marketRestSource = fs.readFileSync(new URL('../src/market-rest.mjs', import.meta.url), 'utf8');
const packageJson = JSON.parse(fs.readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const packageLock = JSON.parse(fs.readFileSync(new URL('../package-lock.json', import.meta.url), 'utf8'));

assert.equal(packageJson.version, '650.8.15.76');
assert.equal(packageLock.version, '650.8.15.76');
assert.equal(packageLock.packages[''].version, '650.8.15.76');

assert.match(moduleSource, /const STEP_VERSION = '650\.8\.15\.76'/);
assert.match(moduleSource, /SPOT_PROVIDERS = Object\.freeze\(\['binance', 'coinbase', 'okx', 'bybit', 'bitget', 'gate'\]\)/);
assert.match(moduleSource, /CONTRACT_PROVIDERS = Object\.freeze\(\['binance', 'okx', 'bybit', 'bitget', 'gate'\]\)/);
assert.match(moduleSource, /coinbase_advanced_trade_public_ticker_batch_websocket/);
assert.match(moduleSource, /api\/v3\/market\/tickers\?category=/);
assert.match(moduleSource, /public\/mark-price\?instType=SWAP/);
assert.match(moduleSource, /public\/open-interest\?instType=SWAP/);
assert.match(moduleSource, /snapshot_reads_start_exchange_requests: false/);
assert.match(moduleSource, /snapshot_reads_start_exchange_connections: false/);
assert.match(moduleSource, /snapshot_reads_scale_with_users: false/);
assert.match(moduleSource, /failed_refresh_never_overwrites_last_verified_rows: true/);
assert.match(moduleSource, /market_light_partial_snapshot_rejected/);
assert.match(moduleSource, /market_light_initial_snapshot_too_partial/);
assert.match(moduleSource, /severe_partial_refresh_never_overwrites_last_verified_rows: true/);
assert.match(moduleSource, /designed_upstream_budget/);
assert.match(moduleSource, /include_rows/);
assert.match(moduleSource, /limit/);
assert.match(moduleSource, /offset/);
assert.match(moduleSource, /Gate.*total_size preserved separately and not relabeled as OI/i);

assert.match(proxySource, /650\.8\.15\.76/);
assert.match(proxySource, /startMarketLightSnapshotScanner\(\)/);
assert.match(proxySource, /handleMarketLightSnapshot/);
assert.match(proxySource, /\/api\/spot-market\/current-snapshot/); // old route intentionally retained
assert.match(proxySource, /step980_6_3_side_by_side_primary_quote_full_directory_no_app_cutover/);

assert.match(marketRestSource, /next_funding_time/);
assert.match(marketRestSource, /funding_interval_hours/);
assert.match(marketRestSource, /basis_rate/);
assert.match(marketRestSource, /provider_total_size/);

console.log('PASS Step980.6.3 selftest: side-by-side full-directory market-light snapshot static invariants hold.');

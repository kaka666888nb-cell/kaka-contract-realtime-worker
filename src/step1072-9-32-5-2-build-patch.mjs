import { readFileSync, writeFileSync } from 'node:fs';

const STEP = 'Step1072.9.32.5.2';
const snapshotUrl = new URL('./market-light-snapshot.mjs', import.meta.url);

function countExact(text, needle) {
  if (!needle) return 0;
  let count = 0;
  let offset = 0;
  while (true) {
    const index = text.indexOf(needle, offset);
    if (index < 0) return count;
    count += 1;
    offset = index + needle.length;
  }
}

function replaceExactlyOnce(text, oldText, newText, label) {
  const count = countExact(text, oldText);
  if (count !== 1) {
    throw new Error(`${STEP} ${label} anchor_count_expected_1_actual_${count}`);
  }
  return text.replace(oldText, newText);
}

function compactFixture(value) {
  return String(value ?? '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

function fixtureLegacyIdentity(row, cap) {
  const provider = String(row?.provider || '').trim().toLowerCase();
  const symbol = compactFixture(row?.symbol);
  const coinId = String(cap?.coingecko_id || '').trim();
  return coinId ? `crypto|${coinId}` : `${provider}|spot|${symbol}`;
}

function fixtureGuardedIdentity(row, cap) {
  const provider = String(row?.provider || '').trim().toLowerCase();
  const symbol = compactFixture(row?.symbol);
  const rawBase = compactFixture(row?.base_asset);
  const normalizedBase = /^1000[A-Z0-9]+$/.test(rawBase) ? rawBase.slice(4) : rawBase;
  const unitWrapped = provider === 'binance' && /^1000[A-Z0-9]+$/.test(rawBase);
  const knownCollision = normalizedBase === 'HOLD';
  if (unitWrapped || knownCollision) return `${provider}|spot|${symbol}`;
  const coinId = String(cap?.coingecko_id || '').trim();
  return coinId ? `crypto|${coinId}` : `${provider}|spot|${symbol}`;
}

const satsBinance = { provider: 'binance', symbol: '1000SATSUSDT', base_asset: '1000SATS' };
const satsOkx = { provider: 'okx', symbol: 'SATSUSDT', base_asset: 'SATS' };
const satsCap = { coingecko_id: 'sats-ordinals' };
const cheemsBinance = { provider: 'binance', symbol: '1000CHEEMSUSDT', base_asset: '1000CHEEMS' };
const cheemsGate = { provider: 'gate', symbol: 'CHEEMSUSDT', base_asset: 'CHEEMS' };
const cheemsCap = { coingecko_id: 'cheems-token' };
const catBinance = { provider: 'binance', symbol: '1000CATUSDT', base_asset: '1000CAT' };
const catOkx = { provider: 'okx', symbol: 'CATUSDT', base_asset: 'CAT' };
const catCap = { coingecko_id: 'simons-cat' };
const holdBitget = { provider: 'bitget', symbol: 'HOLDUSDT', base_asset: 'HOLD' };
const holdGate = { provider: 'gate', symbol: 'HOLDUSDT', base_asset: 'HOLD' };
const holdCap = { coingecko_id: 'holdstation-2' };
const btcBinance = { provider: 'binance', symbol: 'BTCUSDT', base_asset: 'BTC' };
const btcGate = { provider: 'gate', symbol: 'BTCUSDT', base_asset: 'BTC' };
const btcCap = { coingecko_id: 'bitcoin' };

const semantic = {
  legacy_sats_collision: fixtureLegacyIdentity(satsBinance, satsCap) === fixtureLegacyIdentity(satsOkx, satsCap),
  guarded_sats_split: fixtureGuardedIdentity(satsBinance, satsCap) !== fixtureGuardedIdentity(satsOkx, satsCap),
  guarded_cheems_split: fixtureGuardedIdentity(cheemsBinance, cheemsCap) !== fixtureGuardedIdentity(cheemsGate, cheemsCap),
  guarded_cat_split: fixtureGuardedIdentity(catBinance, catCap) !== fixtureGuardedIdentity(catOkx, catCap),
  legacy_hold_collision: fixtureLegacyIdentity(holdBitget, holdCap) === fixtureLegacyIdentity(holdGate, holdCap),
  guarded_hold_split: fixtureGuardedIdentity(holdBitget, holdCap) !== fixtureGuardedIdentity(holdGate, holdCap),
  guarded_btc_still_aggregates: fixtureGuardedIdentity(btcBinance, btcCap) === fixtureGuardedIdentity(btcGate, btcCap),
};
console.log(`${STEP} SEMANTIC`, JSON.stringify(semantic));
if (
  !semantic.legacy_sats_collision ||
  !semantic.guarded_sats_split ||
  !semantic.guarded_cheems_split ||
  !semantic.guarded_cat_split ||
  !semantic.legacy_hold_collision ||
  !semantic.guarded_hold_split ||
  !semantic.guarded_btc_still_aggregates
) {
  throw new Error(`${STEP} semantic fixture failed before mutation`);
}

let snapshot = readFileSync(snapshotUrl, 'utf8');

const oldIdentityBlock = [
  'function marketRankCapForRow(row) {',
  '  if (!marketRankRowUsesCryptoIdentity(row)) return null;',
  '  const base = marketRankBaseFromRow(row);',
  '  return base ? marketRankCapForBase(base) : null;',
  '}',
  '',
  'function marketRankSpotAggregateIdentity(row, cap = null) {',
  "  const provider = String(row?.provider || '').trim().toLowerCase();",
  '  const symbol = compact(row?.symbol);',
  "  if (!provider || !symbol) return '';",
  '  if (!marketRankRowUsesCryptoIdentity(row)) {',
  '    return `${provider}|spot|${symbol}`;',
  '  }',
  "  const coinId = String(cap?.coingecko_id || '').trim();",
  '  return coinId ? `crypto|${coinId}` : `${provider}|spot|${symbol}`;',
  '}',
].join('\n');

const newIdentityBlock = [
  `// ${STEP}: exact provider identity wins for unit-wrapped products and for`,
  '// symbol collisions verified from official listings. Do not merge a quoted',
  '// 1000-unit Binance product with a 1-unit product merely because the',
  '// normalized symbol resolves to the same CoinGecko id.',
  "const MARKET_RANK_EXACT_COLLISION_BASES = new Set(['HOLD']);",
  '',
  'function marketRankRawBaseFromRow(row) {',
  '  const explicit = compact(row?.base_asset);',
  "  if (explicit) return explicit === 'XBT' ? 'BTC' : explicit;",
  '  const quote = compact(row?.quote_asset ?? row?.quote_symbol);',
  '  const symbol = compact(row?.symbol);',
  '  if (quote && symbol.endsWith(quote) && symbol.length > quote.length) {',
  '    const raw = symbol.slice(0, -quote.length);',
  "    return raw === 'XBT' ? 'BTC' : raw;",
  '  }',
  "  return symbol === 'XBT' ? 'BTC' : symbol;",
  '}',
  '',
  'function marketRankSpotUnitWrapped(row) {',
  "  const provider = String(row?.provider || '').trim().toLowerCase();",
  "  const market = String(row?.market_type || '').trim().toLowerCase();",
  '  const rawBase = marketRankRawBaseFromRow(row);',
  "  return market === 'spot' && provider === 'binance' && /^1000[A-Z0-9]+$/.test(rawBase);",
  '}',
  '',
  'function marketRankSpotAggregateBase(row) {',
  '  const rawBase = marketRankRawBaseFromRow(row);',
  '  if (marketRankSpotUnitWrapped(row) && rawBase) return rawBase;',
  '  return marketRankBaseFromRow(row);',
  '}',
  '',
  'function marketRankCapForRow(row) {',
  '  if (!marketRankRowUsesCryptoIdentity(row)) return null;',
  "  const provider = String(row?.provider || '').trim().toLowerCase();",
  '  const base = marketRankBaseFromRow(row);',
  '  if (!base) return null;',
  '  if (marketRankSpotUnitWrapped(row)) return null;',
  "  if (MARKET_RANK_EXACT_COLLISION_BASES.has(base) && provider !== 'bitget') return null;",
  '  return marketRankCapForBase(base);',
  '}',
  '',
  'function marketRankSpotAggregateIdentity(row, cap = null) {',
  "  const provider = String(row?.provider || '').trim().toLowerCase();",
  '  const symbol = compact(row?.symbol);',
  "  if (!provider || !symbol) return '';",
  '  const base = marketRankBaseFromRow(row);',
  '  if (marketRankSpotUnitWrapped(row) || MARKET_RANK_EXACT_COLLISION_BASES.has(base)) {',
  '    return `${provider}|spot|${symbol}`;',
  '  }',
  '  if (!marketRankRowUsesCryptoIdentity(row)) {',
  '    return `${provider}|spot|${symbol}`;',
  '  }',
  "  const coinId = String(cap?.coingecko_id || '').trim();",
  '  return coinId ? `crypto|${coinId}` : `${provider}|spot|${symbol}`;',
  '}',
].join('\n');

const buildGroupOld = [
  '    for (const row of rows) {',
  '      const base = marketRankBaseFromRow(row);',
  '      if (!base) continue;',
  '      const cap = marketRankCapForRow(row);',
  '      const rankIdentity = marketRankSpotAggregateIdentity(row, cap);',
].join('\n');
const buildGroupNew = [
  '    for (const row of rows) {',
  '      const base = marketRankSpotAggregateBase(row);',
  '      if (!base) continue;',
  '      const cap = marketRankCapForRow(row);',
  '      const rankIdentity = marketRankSpotAggregateIdentity(row, cap);',
].join('\n');

const currentGroupOld = [
  "      if (market === 'spot') {",
  '        const base = marketRankBaseFromRow(row);',
  '        if (!base) continue;',
  '        const cap = marketRankCapForRow(row);',
  '        const rankIdentity = marketRankSpotAggregateIdentity(row, cap);',
].join('\n');
const currentGroupNew = [
  "      if (market === 'spot') {",
  '        const base = marketRankSpotAggregateBase(row);',
  '        if (!base) continue;',
  '        const cap = marketRankCapForRow(row);',
  '        const rankIdentity = marketRankSpotAggregateIdentity(row, cap);',
].join('\n');

const payloadOld = [
  "    identity_policy_version: 'Step1072.9.32.5',",
  "    spot_all_provider_identity_policy: 'official_rwa_exact_plus_verified_crypto_coingecko_else_exact_venue',",
  '    official_rwa_symbol_merge_disabled: true,',
  '    unverified_symbol_merge_disabled: true,',
].join('\n');
const payloadNew = [
  `    identity_policy_version: '${STEP}',`,
  "    spot_all_provider_identity_policy: 'official_rwa_exact_plus_verified_crypto_coingecko_plus_unit_wrapper_and_known_collision_guards_else_exact_venue',",
  '    official_rwa_symbol_merge_disabled: true,',
  '    unverified_symbol_merge_disabled: true,',
  '    unit_wrapped_symbol_merge_disabled: true,',
  '    known_collision_symbol_merge_disabled: true,',
].join('\n');

const red = {
  identity_block: countExact(snapshot, oldIdentityBlock),
  build_group: countExact(snapshot, buildGroupOld),
  current_group: countExact(snapshot, currentGroupOld),
  payload_policy: countExact(snapshot, payloadOld),
  exact_collision_set_before: countExact(snapshot, 'MARKET_RANK_EXACT_COLLISION_BASES'),
  unit_wrapper_helper_before: countExact(snapshot, 'function marketRankSpotUnitWrapped(row) {'),
};
console.log(`${STEP} RED`, JSON.stringify(red));
if (
  red.identity_block !== 1 ||
  red.build_group !== 1 ||
  red.current_group !== 1 ||
  red.payload_policy !== 1 ||
  red.exact_collision_set_before !== 0 ||
  red.unit_wrapper_helper_before !== 0
) {
  throw new Error(`${STEP} RED baseline mismatch; refusing build-time mutation`);
}

snapshot = replaceExactlyOnce(snapshot, oldIdentityBlock, newIdentityBlock, 'unit_and_collision_identity_helpers');
snapshot = replaceExactlyOnce(snapshot, buildGroupOld, buildGroupNew, 'rank_build_aggregate_base');
snapshot = replaceExactlyOnce(snapshot, currentGroupOld, currentGroupNew, 'rank_materialize_aggregate_base');
snapshot = replaceExactlyOnce(snapshot, payloadOld, payloadNew, 'identity_policy_metadata');

const green = {
  collision_set: countExact(snapshot, "const MARKET_RANK_EXACT_COLLISION_BASES = new Set(['HOLD']);"),
  raw_base_helper: countExact(snapshot, 'function marketRankRawBaseFromRow(row) {'),
  unit_wrapper_helper: countExact(snapshot, 'function marketRankSpotUnitWrapped(row) {'),
  aggregate_base_helper: countExact(snapshot, 'function marketRankSpotAggregateBase(row) {'),
  aggregate_base_calls: countExact(snapshot, 'const base = marketRankSpotAggregateBase(row);'),
  payload_version: countExact(snapshot, `identity_policy_version: '${STEP}'`),
  unit_policy_marker: countExact(snapshot, '    unit_wrapped_symbol_merge_disabled: true,'),
  collision_policy_marker: countExact(snapshot, '    known_collision_symbol_merge_disabled: true,'),
  old_identity_remaining: countExact(snapshot, oldIdentityBlock),
  old_payload_remaining: countExact(snapshot, payloadOld),
};
console.log(`${STEP} GREEN`, JSON.stringify(green));
if (
  green.collision_set !== 1 ||
  green.raw_base_helper !== 1 ||
  green.unit_wrapper_helper !== 1 ||
  green.aggregate_base_helper !== 1 ||
  green.aggregate_base_calls !== 2 ||
  green.payload_version !== 1 ||
  green.unit_policy_marker !== 1 ||
  green.collision_policy_marker !== 1 ||
  green.old_identity_remaining !== 0 ||
  green.old_payload_remaining !== 0
) {
  throw new Error(`${STEP} GREEN invariant failed; refusing image build`);
}

writeFileSync(snapshotUrl, snapshot, 'utf8');
console.log(`${STEP} BUILD_PATCH_PASS`);

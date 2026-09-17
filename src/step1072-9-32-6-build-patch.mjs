import { readFileSync, writeFileSync } from 'node:fs';

const STEP = 'Step1072.9.32.6';
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

function fixtureCryptoCandidate(row) {
  const provider = String(row?.provider || '').trim().toLowerCase();
  if (provider === 'bitget') {
    const reality = String(row?.is_reality ?? '').trim().toLowerCase();
    const rwa = String(row?.is_rwa ?? '').trim().toUpperCase();
    if (reality === 'yes' || rwa === 'YES') return false;
  }
  if (provider === 'okx') {
    const category = String(row?.instrument_asset_category ?? '').trim();
    if (['3','4','5','6'].includes(category)) return false;
  }
  return true;
}

const semantic = {
  bitget_reality_ron_excluded: !fixtureCryptoCandidate({ provider: 'bitget', base_asset: 'RON', is_reality: 'yes' }),
  bitget_crypto_btc_kept: fixtureCryptoCandidate({ provider: 'bitget', base_asset: 'BTC', is_reality: 'no', instrument_symbol_type: 'crypto' }),
  okx_stock_excluded: !fixtureCryptoCandidate({ provider: 'okx', base_asset: 'XTER', instrument_asset_category: '3' }),
  okx_crypto_ron_kept: fixtureCryptoCandidate({ provider: 'okx', base_asset: 'RON', instrument_asset_category: '1' }),
  gate_crypto_kept: fixtureCryptoCandidate({ provider: 'gate', base_asset: 'RON' }),
};
console.log(`${STEP} SEMANTIC`, JSON.stringify(semantic));
if (Object.values(semantic).some((ok) => !ok)) {
  throw new Error(`${STEP} semantic fixture failed before mutation`);
}

let snapshot = readFileSync(snapshotUrl, 'utf8');

const dedupeOld = [
  'function sectorDedupedUsdtRows(healthyByProvider) {',
  '  const byBase = new Map();',
  '  for (const provider of SECTOR_USDT_PROVIDERS) {',
  '    const snapshot = healthyByProvider.get(provider);',
  '    if (!snapshot) continue;',
  '    for (const row of snapshot.rows) {',
  "      if (compact(row?.quote_asset ?? row?.quote_symbol) !== 'USDT') continue;",
].join('\n');
const dedupeNew = [
  'function sectorDedupedUsdtRows(healthyByProvider) {',
  '  const byBase = new Map();',
  '  for (const provider of SECTOR_USDT_PROVIDERS) {',
  '    const snapshot = healthyByProvider.get(provider);',
  '    if (!snapshot) continue;',
  '    for (const row of snapshot.rows) {',
  `      // ${STEP}: crypto sector metrics must never select tokenized-stock/RWA`,
  '      // venue rows merely because their base symbol matches a crypto asset.',
  '      if (!marketRankRowUsesCryptoIdentity(row)) continue;',
  "      if (compact(row?.quote_asset ?? row?.quote_symbol) !== 'USDT') continue;",
].join('\n');

const presenceOld = [
  'function sectorUsdtVenuePresence(healthyByProvider) {',
  '  const byBase = new Map();',
  '  for (const provider of SECTOR_USDT_PROVIDERS) {',
  '    const snapshot = healthyByProvider.get(provider);',
  '    if (!snapshot) continue;',
  '    for (const row of snapshot.rows) {',
  "      if (compact(row?.quote_asset ?? row?.quote_symbol) !== 'USDT') continue;",
].join('\n');
const presenceNew = [
  'function sectorUsdtVenuePresence(healthyByProvider) {',
  '  const byBase = new Map();',
  '  for (const provider of SECTOR_USDT_PROVIDERS) {',
  '    const snapshot = healthyByProvider.get(provider);',
  '    if (!snapshot) continue;',
  '    for (const row of snapshot.rows) {',
  `      // ${STEP}: provider-presence counts use the same crypto-only identity`,
  '      // gate as the representative row, so Reality/RWA cannot inflate coverage.',
  '      if (!marketRankRowUsesCryptoIdentity(row)) continue;',
  "      if (compact(row?.quote_asset ?? row?.quote_symbol) !== 'USDT') continue;",
].join('\n');

const payloadOld = [
  "    duplicate_asset_policy: 'highest_real_24h_usdt_turnover_venue_represents_asset',",
  "    leveraged_product_policy: 'exclude_2l_2s_3l_3s_5l_5s',",
].join('\n');
const payloadNew = [
  "    duplicate_asset_policy: 'highest_real_24h_usdt_turnover_venue_represents_asset',",
  `    identity_policy_version: '${STEP}',`,
  "    official_noncrypto_identity_policy: 'exclude_before_sector_dedupe_and_venue_presence',",
  '    official_noncrypto_identity_rows_excluded: true,',
  "    leveraged_product_policy: 'exclude_2l_2s_3l_3s_5l_5s',",
].join('\n');

const healthOld = [
  '    source: snapshot.source,',
  '    source_verified: snapshot.source_verified,',
  '    partial_ready: snapshot.partial_ready,',
].join('\n');
const healthNew = [
  '    source: snapshot.source,',
  `    identity_policy_version: snapshot.identity_policy_version || '${STEP}',`,
  '    official_noncrypto_identity_rows_excluded: snapshot.official_noncrypto_identity_rows_excluded === true,',
  '    source_verified: snapshot.source_verified,',
  '    partial_ready: snapshot.partial_ready,',
].join('\n');

const red = {
  dedupe_anchor: countExact(snapshot, dedupeOld),
  presence_anchor: countExact(snapshot, presenceOld),
  payload_anchor: countExact(snapshot, payloadOld),
  health_anchor: countExact(snapshot, healthOld),
  crypto_filter_before: countExact(snapshot, 'if (!marketRankRowUsesCryptoIdentity(row)) continue;'),
  policy_before: countExact(snapshot, "identity_policy_version: 'Step1072.9.32.6'"),
};
console.log(`${STEP} RED`, JSON.stringify(red));
if (
  red.dedupe_anchor !== 1 ||
  red.presence_anchor !== 1 ||
  red.payload_anchor !== 1 ||
  red.health_anchor !== 1 ||
  red.crypto_filter_before !== 0 ||
  red.policy_before !== 0
) {
  throw new Error(`${STEP} RED baseline mismatch; refusing build-time mutation`);
}

snapshot = replaceExactlyOnce(snapshot, dedupeOld, dedupeNew, 'sector_representative_identity_filter');
snapshot = replaceExactlyOnce(snapshot, presenceOld, presenceNew, 'sector_presence_identity_filter');
snapshot = replaceExactlyOnce(snapshot, payloadOld, payloadNew, 'sector_identity_policy_metadata');
snapshot = replaceExactlyOnce(snapshot, healthOld, healthNew, 'sector_health_identity_metadata');

const green = {
  crypto_filter_count: countExact(snapshot, 'if (!marketRankRowUsesCryptoIdentity(row)) continue;'),
  policy_version: countExact(snapshot, `identity_policy_version: '${STEP}'`),
  policy_marker: countExact(snapshot, "official_noncrypto_identity_policy: 'exclude_before_sector_dedupe_and_venue_presence'"),
  exclusion_marker: countExact(snapshot, '    official_noncrypto_identity_rows_excluded: true,'),
  health_marker: countExact(snapshot, '    official_noncrypto_identity_rows_excluded: snapshot.official_noncrypto_identity_rows_excluded === true,'),
  old_dedupe_remaining: countExact(snapshot, dedupeOld),
  old_presence_remaining: countExact(snapshot, presenceOld),
};
console.log(`${STEP} GREEN`, JSON.stringify(green));
if (
  green.crypto_filter_count !== 2 ||
  green.policy_version !== 1 ||
  green.policy_marker !== 1 ||
  green.exclusion_marker !== 1 ||
  green.health_marker !== 1 ||
  green.old_dedupe_remaining !== 0 ||
  green.old_presence_remaining !== 0
) {
  throw new Error(`${STEP} GREEN invariant failed; refusing image build`);
}

writeFileSync(snapshotUrl, snapshot, 'utf8');
console.log(`${STEP} BUILD_PATCH_PASS`);

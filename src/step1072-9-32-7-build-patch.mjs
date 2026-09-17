import { readFileSync, writeFileSync } from 'node:fs';

const STEP = 'Step1072.9.32.7';
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
    const groupId = String(row?.instrument_group_id ?? '').trim();
    if (groupId === '22' && category !== '1') return false;
  }
  return true;
}

const semantic = {
  bitget_reality_excluded: !fixtureCryptoCandidate({ provider: 'bitget', symbol: 'RGNRCUSDT', is_reality: 'yes' }),
  bitget_crypto_kept: fixtureCryptoCandidate({ provider: 'bitget', symbol: 'BTCUSDT', is_reality: 'no' }),
  okx_stock_excluded: !fixtureCryptoCandidate({ provider: 'okx', symbol: 'XTERUSDT', instrument_asset_category: '3' }),
  okx_crypto_kept: fixtureCryptoCandidate({ provider: 'okx', symbol: 'BTCUSDT', instrument_asset_category: '1' }),
  gate_crypto_kept: fixtureCryptoCandidate({ provider: 'gate', symbol: 'BTCUSDT' }),
};
console.log(`${STEP} SEMANTIC`, JSON.stringify(semantic));
if (Object.values(semantic).some((ok) => !ok)) {
  throw new Error(`${STEP} semantic fixture failed before mutation`);
}

let snapshot = readFileSync(snapshotUrl, 'utf8');

const loopOld = [
  '      if (dataHubSpotSummaryIsLeveraged(row)) continue;',
  '      const change = marketRankNumber(row?.price_change_percent_24h);',
  '      const normalized = dataHubSpotSummaryRow(row);',
].join('\n');
const loopNew = [
  '      if (dataHubSpotSummaryIsLeveraged(row)) continue;',
  `      // ${STEP}: the crypto spot summary must not surface tokenized stocks/RWA`,
  '      // simply because they trade in USDT on a shared exchange ticker feed.',
  '      if (!marketRankRowUsesCryptoIdentity(row)) continue;',
  '      const change = marketRankNumber(row?.price_change_percent_24h);',
  '      const normalized = dataHubSpotSummaryRow(row);',
].join('\n');

const payloadOld = [
  "    source: 'render_shared_market_light_data_hub_spot_summary',",
  "    market_type: 'spot',",
].join('\n');
const payloadNew = [
  "    source: 'render_shared_market_light_data_hub_spot_summary',",
  `    identity_policy_version: '${STEP}',`,
  "    official_noncrypto_identity_policy: 'exclude_before_data_hub_spot_summary_ranking',",
  '    official_noncrypto_identity_rows_excluded: true,',
  "    market_type: 'spot',",
].join('\n');

const red = {
  loop_anchor: countExact(snapshot, loopOld),
  payload_anchor: countExact(snapshot, payloadOld),
  crypto_filter_before: countExact(snapshot, 'if (!marketRankRowUsesCryptoIdentity(row)) continue;'),
  policy_before: countExact(snapshot, `identity_policy_version: '${STEP}'`),
};
console.log(`${STEP} RED`, JSON.stringify(red));
if (
  red.loop_anchor !== 1 ||
  red.payload_anchor !== 1 ||
  red.crypto_filter_before !== 2 ||
  red.policy_before !== 0
) {
  throw new Error(`${STEP} RED baseline mismatch; refusing build-time mutation`);
}

snapshot = replaceExactlyOnce(snapshot, loopOld, loopNew, 'data_hub_crypto_identity_filter');
snapshot = replaceExactlyOnce(snapshot, payloadOld, payloadNew, 'data_hub_identity_policy_metadata');

const green = {
  crypto_filter_count: countExact(snapshot, 'if (!marketRankRowUsesCryptoIdentity(row)) continue;'),
  policy_version: countExact(snapshot, `identity_policy_version: '${STEP}'`),
  policy_marker: countExact(snapshot, "official_noncrypto_identity_policy: 'exclude_before_data_hub_spot_summary_ranking'"),
  exclusion_marker: countExact(snapshot, '    official_noncrypto_identity_rows_excluded: true,'),
  old_loop_remaining: countExact(snapshot, loopOld),
};
console.log(`${STEP} GREEN`, JSON.stringify(green));
if (
  green.crypto_filter_count !== 3 ||
  green.policy_version !== 1 ||
  green.policy_marker !== 1 ||
  green.exclusion_marker < 2 ||
  green.old_loop_remaining !== 0
) {
  throw new Error(`${STEP} GREEN invariant failed; refusing image build`);
}

writeFileSync(snapshotUrl, snapshot, 'utf8');
console.log(`${STEP} BUILD_PATCH_PASS`);

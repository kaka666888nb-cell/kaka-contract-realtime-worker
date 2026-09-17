import { readFileSync, writeFileSync } from 'node:fs';

const STEP = 'Step1072.9.32.5.1';
const snapshotUrl = new URL('./market-light-snapshot.mjs', import.meta.url);
const restUrl = new URL('./market-rest.mjs', import.meta.url);

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

function legacyOkxClassFixture(row) {
  const groupId = String(
    row?.instrument_group_id ?? row?.group_id ?? row?.groupId ?? '',
  ).trim();
  return groupId === '22' ? 'rwa' : 'crypto_candidate';
}

function officialOkxClassFixture(row) {
  const assetCategory = String(
    row?.instrument_asset_category ?? row?.inst_category ?? row?.instCategory ?? '',
  ).trim();
  if (assetCategory === '3') return 'tokenized_equity';
  if (['4', '5', '6'].includes(assetCategory)) return 'rwa';
  return legacyOkxClassFixture(row);
}

function identityFixture(row, cap, classifier) {
  const provider = String(row?.provider || '').trim().toLowerCase();
  const symbol = compactFixture(row?.symbol);
  if (!provider || !symbol) return '';
  if (provider === 'okx' && classifier(row) !== 'crypto_candidate') {
    return `${provider}|spot|${symbol}`;
  }
  const coinId = String(cap?.coingecko_id || '').trim();
  return coinId ? `crypto|${coinId}` : `${provider}|spot|${symbol}`;
}

const xterOkx = {
  provider: 'okx',
  market_type: 'spot',
  symbol: 'XTERUSDT',
  instrument_group_id: '12',
  instrument_asset_category: '3',
};
const xterGate = {
  provider: 'gate',
  market_type: 'spot',
  symbol: 'XTERUSDT',
};
const xterCap = { coingecko_id: 'xterio' };
const legacyCollision =
  identityFixture(xterOkx, xterCap, legacyOkxClassFixture) ===
  identityFixture(xterGate, xterCap, legacyOkxClassFixture);
console.log(`${STEP} RED_FIXTURE`, JSON.stringify({
  legacy_group12_stock_collides_with_crypto: legacyCollision,
  okx_identity: identityFixture(xterOkx, xterCap, legacyOkxClassFixture),
  gate_identity: identityFixture(xterGate, xterCap, legacyOkxClassFixture),
}));
if (!legacyCollision) {
  throw new Error(`${STEP} RED fixture no longer reproduces XTER collision`);
}

const officialSplit =
  identityFixture(xterOkx, xterCap, officialOkxClassFixture) !==
  identityFixture(xterGate, xterCap, officialOkxClassFixture);
console.log(`${STEP} GREEN_FIXTURE`, JSON.stringify({
  okx_inst_category_3_splits_from_crypto: officialSplit,
  okx_identity: identityFixture(xterOkx, xterCap, officialOkxClassFixture),
  gate_identity: identityFixture(xterGate, xterCap, officialOkxClassFixture),
}));
if (!officialSplit) {
  throw new Error(`${STEP} GREEN semantic fixture failed`);
}

let snapshot = readFileSync(snapshotUrl, 'utf8');
let rest = readFileSync(restUrl, 'utf8');

const restIdentityOld = [
  "    instrument_group_id: String(extra.instrument_group_id ?? extra.group_id ?? '').trim() || null,",
  "    instrument_symbol_type: String(extra.instrument_symbol_type ?? extra.symbol_type ?? '').trim() || null,",
  "    is_rwa: String(extra.is_rwa ?? '').trim() || null,",
  "    is_reality: String(extra.is_reality ?? '').trim() || null,",
].join('\n');
const restIdentityNew = [
  "    instrument_group_id: String(extra.instrument_group_id ?? extra.group_id ?? '').trim() || null,",
  "    instrument_symbol_type: String(extra.instrument_symbol_type ?? extra.symbol_type ?? '').trim() || null,",
  `    // ${STEP}: OKX documents instCategory as the base asset category:`,
  '    // 1=Crypto, 3=Stocks, 4=Commodities, 5=Forex, 6=Bonds.',
  "    instrument_asset_category: String(extra.instrument_asset_category ?? extra.inst_category ?? '').trim() || null,",
  "    is_rwa: String(extra.is_rwa ?? '').trim() || null,",
  "    is_reality: String(extra.is_reality ?? '').trim() || null,",
].join('\n');

const okxSpotOld = [
  '            {',
  '              instrument_group_id: item.groupId,',
  "              instrument_symbol_type: item.instType || 'SPOT',",
  '            },',
].join('\n');
const okxSpotNew = [
  '            {',
  '              instrument_group_id: item.groupId,',
  "              instrument_symbol_type: item.instType || 'SPOT',",
  '              instrument_asset_category: item.instCategory,',
  '            },',
].join('\n');

const restOkxSelfTestOld = [
  `  const okxRwaIdentity = marketRow(`,
  `    'okx', 'spot', 'XTERUSDT', 'XTER', 'USDT', 'XTER-USDT',`,
  `    { instrument_group_id: '22', instrument_symbol_type: 'SPOT' },`,
  `  );`,
  `  add(`,
  `    'okx_spot_rwa_group_identity_metadata_preserved',`,
  `    okxRwaIdentity.instrument_group_id === '22' &&`,
  `      okxRwaIdentity.instrument_symbol_type === 'SPOT',`,
  `    okxRwaIdentity,`,
  `  );`,
].join('\n');
const restOkxSelfTestNew = [
  `  const okxRwaIdentity = marketRow(`,
  `    'okx', 'spot', 'XTERUSDT', 'XTER', 'USDT', 'XTER-USDT',`,
  `    {`,
  `      instrument_group_id: '12',`,
  `      instrument_symbol_type: 'SPOT',`,
  `      instrument_asset_category: '3',`,
  `    },`,
  `  );`,
  `  add(`,
  `    'okx_spot_rwa_group_identity_metadata_preserved',`,
  `    okxRwaIdentity.instrument_group_id === '12' &&`,
  `      okxRwaIdentity.instrument_symbol_type === 'SPOT' &&`,
  `      okxRwaIdentity.instrument_asset_category === '3',`,
  `    okxRwaIdentity,`,
  `  );`,
  `  add(`,
  `    'okx_spot_stock_inst_category_metadata_preserved',`,
  `    okxRwaIdentity.instrument_asset_category === '3',`,
  `    okxRwaIdentity,`,
  `  );`,
].join('\n');

const snapshotIdentityOld = [
  '    instrument_group_id:',
  '      identity?.instrument_group_id ?? raw.instrument_group_id ?? raw.group_id ?? raw.groupId ?? null,',
  '    instrument_symbol_type:',
  '      identity?.instrument_symbol_type ?? raw.instrument_symbol_type ?? raw.symbol_type ?? raw.symbolType ?? null,',
  '    is_rwa: identity?.is_rwa ?? raw.is_rwa ?? raw.isRwa ?? null,',
  '    is_reality: identity?.is_reality ?? raw.is_reality ?? raw.isReality ?? null,',
].join('\n');
const snapshotIdentityNew = [
  '    instrument_group_id:',
  '      identity?.instrument_group_id ?? raw.instrument_group_id ?? raw.group_id ?? raw.groupId ?? null,',
  '    instrument_symbol_type:',
  '      identity?.instrument_symbol_type ?? raw.instrument_symbol_type ?? raw.symbol_type ?? raw.symbolType ?? null,',
  '    instrument_asset_category:',
  '      identity?.instrument_asset_category ?? raw.instrument_asset_category ?? raw.inst_category ?? raw.instCategory ?? null,',
  '    is_rwa: identity?.is_rwa ?? raw.is_rwa ?? raw.isRwa ?? null,',
  '    is_reality: identity?.is_reality ?? raw.is_reality ?? raw.isReality ?? null,',
].join('\n');

const okxClassifierOld = [
  "  if (provider === 'okx') {",
  '    const groupId = String(',
  "      row?.instrument_group_id ?? row?.group_id ?? row?.groupId ?? '',",
  '    ).trim();',
  "    if (groupId === '22') return 'rwa';",
  '  }',
].join('\n');
const okxClassifierNew = [
  "  if (provider === 'okx') {",
  `    // ${STEP}: groupId is a fee group, not the authoritative asset class.`,
  '    // Use OKX instCategory first so e.g. XTER can be Stock (3) even while',
  '    // its current fee group is 12. Keep groupId=22 only as a secondary RWA',
  '    // signal for rows where the category is temporarily unavailable.',
  '    const assetCategory = String(',
  "      row?.instrument_asset_category ?? row?.inst_category ?? row?.instCategory ?? '',",
  '    ).trim();',
  "    if (assetCategory === '3') return 'tokenized_equity';",
  "    if (['4', '5', '6'].includes(assetCategory)) return 'rwa';",
  '    const groupId = String(',
  "      row?.instrument_group_id ?? row?.group_id ?? row?.groupId ?? '',",
  '    ).trim();',
  "    if (groupId === '22') return 'rwa';",
  '  }',
].join('\n');

const red = {
  rest_identity_anchor: countExact(rest, restIdentityOld),
  rest_okx_spot_anchor: countExact(rest, okxSpotOld),
  rest_okx_self_test_anchor: countExact(rest, restOkxSelfTestOld),
  snapshot_identity_anchor: countExact(snapshot, snapshotIdentityOld),
  snapshot_okx_classifier_anchor: countExact(snapshot, okxClassifierOld),
  category_field_before_rest: countExact(rest, '    instrument_asset_category:'),
  category_field_before_snapshot: countExact(snapshot, '    instrument_asset_category:'),
};
console.log(`${STEP} RED`, JSON.stringify(red));
if (
  red.rest_identity_anchor !== 1 ||
  red.rest_okx_spot_anchor !== 1 ||
  red.rest_okx_self_test_anchor !== 1 ||
  red.snapshot_identity_anchor !== 1 ||
  red.snapshot_okx_classifier_anchor !== 1 ||
  red.category_field_before_rest !== 0 ||
  red.category_field_before_snapshot !== 0
) {
  throw new Error(`${STEP} RED baseline mismatch; refusing build-time mutation`);
}

rest = replaceExactlyOnce(rest, restIdentityOld, restIdentityNew, 'market_row_asset_category');
rest = replaceExactlyOnce(rest, okxSpotOld, okxSpotNew, 'okx_spot_inst_category');
rest = replaceExactlyOnce(rest, restOkxSelfTestOld, restOkxSelfTestNew, 'okx_inst_category_self_test');
snapshot = replaceExactlyOnce(snapshot, snapshotIdentityOld, snapshotIdentityNew, 'snapshot_asset_category_propagation');
snapshot = replaceExactlyOnce(snapshot, okxClassifierOld, okxClassifierNew, 'okx_asset_category_classifier');

const green = {
  rest_category_field: countExact(
    rest,
    "    instrument_asset_category: String(extra.instrument_asset_category ?? extra.inst_category ?? '').trim() || null,",
  ),
  rest_okx_category_assignment: countExact(rest, '              instrument_asset_category: item.instCategory,'),
  rest_category_self_test: countExact(rest, "'okx_spot_stock_inst_category_metadata_preserved'"),
  snapshot_category_field: countExact(
    snapshot,
    '    instrument_asset_category:\n      identity?.instrument_asset_category ?? raw.instrument_asset_category ?? raw.inst_category ?? raw.instCategory ?? null,',
  ),
  snapshot_stock_classifier: countExact(snapshot, "    if (assetCategory === '3') return 'tokenized_equity';"),
  snapshot_noncrypto_classifier: countExact(snapshot, "    if (['4', '5', '6'].includes(assetCategory)) return 'rwa';"),
  old_classifier_remaining: countExact(snapshot, okxClassifierOld),
};
console.log(`${STEP} GREEN`, JSON.stringify(green));
if (
  green.rest_category_field !== 1 ||
  green.rest_okx_category_assignment !== 1 ||
  green.rest_category_self_test !== 1 ||
  green.snapshot_category_field !== 1 ||
  green.snapshot_stock_classifier !== 1 ||
  green.snapshot_noncrypto_classifier !== 1 ||
  green.old_classifier_remaining !== 0
) {
  throw new Error(`${STEP} GREEN invariant failed; refusing image build`);
}

writeFileSync(restUrl, rest, 'utf8');
writeFileSync(snapshotUrl, snapshot, 'utf8');
console.log(`${STEP} BUILD_PATCH_PASS`);

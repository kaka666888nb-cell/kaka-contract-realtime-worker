import { readFileSync, writeFileSync } from 'node:fs';

const STEP = 'Step1072.9.32.5';
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

function officialClassFixture(row) {
  const provider = String(row?.provider || '').trim().toLowerCase();
  const market = String(row?.market_type || '').trim().toLowerCase();
  if (market !== 'spot') return 'crypto_candidate';
  if (provider === 'bitget') {
    const reality = String(row?.is_reality ?? row?.isReality ?? '').trim().toLowerCase();
    const rwa = String(row?.is_rwa ?? row?.isRwa ?? '').trim().toUpperCase();
    if (reality === 'yes') return 'tokenized_equity';
    if (rwa === 'YES') return 'rwa';
  }
  if (provider === 'okx') {
    const groupId = String(
      row?.instrument_group_id ?? row?.group_id ?? row?.groupId ?? '',
    ).trim();
    if (groupId === '22') return 'rwa';
  }
  return 'crypto_candidate';
}

function aggregateIdentityFixture(row, cap = null) {
  const provider = String(row?.provider || '').trim().toLowerCase();
  const symbol = compactFixture(row?.symbol);
  if (!provider || !symbol) return '';
  if (officialClassFixture(row) !== 'crypto_candidate') {
    return `${provider}|spot|${symbol}`;
  }
  const coinId = String(cap?.coingecko_id || '').trim();
  return coinId ? `crypto|${coinId}` : `${provider}|spot|${symbol}`;
}

function semanticSelfTest() {
  const tests = [];
  const add = (name, ok, actual = null) => tests.push({ name, ok: Boolean(ok), actual });

  const bitgetReality = {
    provider: 'bitget', market_type: 'spot', symbol: 'RZTOUSDT', base_asset: 'RZTO',
    is_reality: 'yes', is_rwa: 'YES',
  };
  const gateRzto = {
    provider: 'gate', market_type: 'spot', symbol: 'RZTOUSDT', base_asset: 'RZTO',
  };
  add(
    'bitget_reality_never_merges_with_same_symbol_crypto',
    aggregateIdentityFixture(bitgetReality, { coingecko_id: 'rzto' }) !==
      aggregateIdentityFixture(gateRzto, { coingecko_id: 'rzto' }),
  );

  const okxRwa = {
    provider: 'okx', market_type: 'spot', symbol: 'XTERUSDT', base_asset: 'XTER',
    instrument_group_id: '22',
  };
  const gateXter = {
    provider: 'gate', market_type: 'spot', symbol: 'XTERUSDT', base_asset: 'XTER',
  };
  add(
    'okx_spot_group_22_never_merges_with_same_symbol_crypto',
    aggregateIdentityFixture(okxRwa, { coingecko_id: 'xterio' }) !==
      aggregateIdentityFixture(gateXter, { coingecko_id: 'xterio' }),
  );

  const btcBinance = {
    provider: 'binance', market_type: 'spot', symbol: 'BTCUSDT', base_asset: 'BTC',
  };
  const btcGate = {
    provider: 'gate', market_type: 'spot', symbol: 'BTCUSDT', base_asset: 'BTC',
  };
  add(
    'verified_crypto_identity_still_aggregates_across_venues',
    aggregateIdentityFixture(btcBinance, { coingecko_id: 'bitcoin' }) ===
      aggregateIdentityFixture(btcGate, { coingecko_id: 'bitcoin' }),
  );

  const unknownA = {
    provider: 'binance', market_type: 'spot', symbol: 'ZZZUSDT', base_asset: 'ZZZ',
  };
  const unknownB = {
    provider: 'gate', market_type: 'spot', symbol: 'ZZZUSDT', base_asset: 'ZZZ',
  };
  add(
    'unverified_same_symbol_fails_closed_to_exact_venue_identity',
    aggregateIdentityFixture(unknownA, null) !== aggregateIdentityFixture(unknownB, null),
  );

  return { ok: tests.every((item) => item.ok), checks: tests.length, tests };
}

let snapshot = readFileSync(snapshotUrl, 'utf8');
let rest = readFileSync(restUrl, 'utf8');

const marketRowIdentityAnchor =
  "    quantity_semantics: extra.quantity_semantics || (market === 'contract' ? 'base_asset' : 'base_asset'),";
const marketRowIdentityReplacement = [
  marketRowIdentityAnchor,
  `    // ${STEP}: preserve provider instrument identity metadata used by the`,
  '    // all-provider spot rank collision guard. These are directory fields',
  '    // already fetched by the shared catalog; no user-scaled upstream work.',
  "    instrument_group_id: String(extra.instrument_group_id ?? extra.group_id ?? '').trim() || null,",
  "    instrument_symbol_type: String(extra.instrument_symbol_type ?? extra.symbol_type ?? '').trim() || null,",
  "    is_rwa: String(extra.is_rwa ?? '').trim() || null,",
  "    is_reality: String(extra.is_reality ?? '').trim() || null,",
].join('\n');

const okxSpotDirectoryOld = [
  '          rows.push(marketRow(',
  '            provider,',
  '            market,',
  '            item.instId,',
  '            base,',
  '            quote,',
  '            item.instId,',
  '          ));',
].join('\n');
const okxSpotDirectoryNew = [
  '          rows.push(marketRow(',
  '            provider,',
  '            market,',
  '            item.instId,',
  '            base,',
  '            quote,',
  '            item.instId,',
  '            {',
  '              instrument_group_id: item.groupId,',
  "              instrument_symbol_type: item.instType || 'SPOT',",
  '            },',
  '          ));',
].join('\n');

const bitgetSpotUrlOld = "        'https://api.bitget.com/api/v2/spot/public/symbols',";
const bitgetSpotUrlNew = [
  `        // ${STEP}: v3 Instruments is the official successor to v2 spot`,
  '        // symbols and carries isRwa/isReality. Keep one shared catalog',
  '        // request; this does not add an extra provider request lane.',
  "        'https://api.bitget.com/api/v3/market/instruments?category=SPOT',",
].join('\n');

const bitgetSpotDirectoryOld = [
  '          rows.push(marketRow(',
  '            provider,',
  '            market,',
  '            item.symbol,',
  '            item.baseCoin,',
  '            item.quoteCoin,',
  '            item.symbol,',
  '          ));',
].join('\n');
const bitgetSpotDirectoryNew = [
  '          rows.push(marketRow(',
  '            provider,',
  '            market,',
  '            item.symbol,',
  '            item.baseCoin,',
  '            item.quoteCoin,',
  '            item.symbol,',
  '            {',
  '              is_rwa: item.isRwa,',
  '              is_reality: item.isReality,',
  '              instrument_symbol_type: item.symbolType,',
  '            },',
  '          ));',
].join('\n');

const restSelfTestAnchor = [
  '  const coinbase = tickerVolumeSemantics(',
  "    'coinbase',",
  "    'spot',",
  "    { volume: '2.5', quote_volume_24h: '250' },",
].join('\n');
const restSelfTestInsert = [
  `  const bitgetRealityIdentity = marketRow(`,
  `    'bitget', 'spot', 'RZTOUSDT', 'RZTO', 'USDT', 'RZTOUSDT',`,
  `    { is_rwa: 'YES', is_reality: 'yes', instrument_symbol_type: 'equity' },`,
  `  );`,
  `  add(`,
  `    'bitget_spot_reality_identity_metadata_preserved',`,
  `    bitgetRealityIdentity.is_rwa === 'YES' &&`,
  `      bitgetRealityIdentity.is_reality === 'yes' &&`,
  `      bitgetRealityIdentity.instrument_symbol_type === 'equity',`,
  `    bitgetRealityIdentity,`,
  `  );`,
  ``,
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
  ``,
  restSelfTestAnchor,
].join('\n');

const snapshotIdentityAnchor = [
  '    settle_asset: identity?.settle_asset ?? raw.settle_asset ?? null,',
  '    contract_type: identity?.contract_type ?? raw.contract_type ?? null,',
  '    // Step1019: directory-backed official product facts use the exact same',
].join('\n');
const snapshotIdentityReplacement = [
  '    settle_asset: identity?.settle_asset ?? raw.settle_asset ?? null,',
  '    contract_type: identity?.contract_type ?? raw.contract_type ?? null,',
  `    // ${STEP}: carry exact provider identity classification from the shared`,
  '    // instrument directory into the ticker row before global ranking.',
  '    instrument_group_id:',
  '      identity?.instrument_group_id ?? raw.instrument_group_id ?? raw.group_id ?? raw.groupId ?? null,',
  '    instrument_symbol_type:',
  '      identity?.instrument_symbol_type ?? raw.instrument_symbol_type ?? raw.symbol_type ?? raw.symbolType ?? null,',
  '    is_rwa: identity?.is_rwa ?? raw.is_rwa ?? raw.isRwa ?? null,',
  '    is_reality: identity?.is_reality ?? raw.is_reality ?? raw.isReality ?? null,',
  '    // Step1019: directory-backed official product facts use the exact same',
].join('\n');

const helperAnchor = 'function marketRankEntryComparator(sortKey) {';
const helperInsert = [
  `// ${STEP}: symbol equality is not asset identity. Official tokenized/RWA`,
  '// instruments never merge with crypto rows by base symbol. Ordinary crypto',
  '// rows aggregate across venues only when the shared project market-cap index',
  '// resolves that symbol to one verified CoinGecko identity; otherwise fail',
  '// closed to provider+market+symbol.',
  'function marketRankOfficialIdentityClass(row) {',
  "  const provider = String(row?.provider || '').trim().toLowerCase();",
  "  const market = String(row?.market_type || '').trim().toLowerCase();",
  "  if (market !== 'spot') return 'crypto_candidate';",
  "  if (provider === 'bitget') {",
  "    const reality = String(row?.is_reality ?? row?.isReality ?? '').trim().toLowerCase();",
  "    const rwa = String(row?.is_rwa ?? row?.isRwa ?? '').trim().toUpperCase();",
  "    if (reality === 'yes') return 'tokenized_equity';",
  "    if (rwa === 'YES') return 'rwa';",
  '  }',
  "  if (provider === 'okx') {",
  '    const groupId = String(',
  "      row?.instrument_group_id ?? row?.group_id ?? row?.groupId ?? '',",
  '    ).trim();',
  "    if (groupId === '22') return 'rwa';",
  '  }',
  "  return 'crypto_candidate';",
  '}',
  '',
  'function marketRankRowUsesCryptoIdentity(row) {',
  "  return marketRankOfficialIdentityClass(row) === 'crypto_candidate';",
  '}',
  '',
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
  '',
  helperAnchor,
].join('\n');

const groupedOld = [
  '    const grouped = new Map();',
  '    for (const row of rows) {',
  '      const base = marketRankBaseFromRow(row);',
  '      if (!base) continue;',
  '      const list = grouped.get(base) || [];',
  '      list.push(row);',
  '      grouped.set(base, list);',
  '    }',
  '    const entries = [];',
  '    for (const [base, venueRows] of grouped.entries()) {',
  '      const changes = venueRows.map((row) => marketRankNumber(row?.price_change_percent_24h)).filter((v) => v != null);',
  '      const volumes = venueRows.map((row) => marketRankNumber(row?.quote_volume_24h)).filter((v) => v != null && v >= 0);',
  '      const cap = marketRankCapForBase(base);',
  '      const representative = marketRankRepresentativeRow(venueRows, sortKey);',
].join('\n');
const groupedNew = [
  '    const grouped = new Map();',
  '    for (const row of rows) {',
  '      const base = marketRankBaseFromRow(row);',
  '      if (!base) continue;',
  '      const cap = marketRankCapForRow(row);',
  '      const rankIdentity = marketRankSpotAggregateIdentity(row, cap);',
  '      if (!rankIdentity) continue;',
  '      const group = grouped.get(rankIdentity) || { base, cap, venue_rows: [] };',
  '      group.venue_rows.push(row);',
  '      if (!group.cap && cap) group.cap = cap;',
  '      grouped.set(rankIdentity, group);',
  '    }',
  '    const entries = [];',
  '    for (const [rankIdentity, group] of grouped.entries()) {',
  '      const base = group.base;',
  '      const cap = group.cap;',
  '      const venueRows = group.venue_rows;',
  '      const changes = venueRows.map((row) => marketRankNumber(row?.price_change_percent_24h)).filter((v) => v != null);',
  '      const volumes = venueRows.map((row) => marketRankNumber(row?.quote_volume_24h)).filter((v) => v != null && v >= 0);',
  '      const representative = marketRankRepresentativeRow(venueRows, sortKey);',
].join('\n');

const groupedRankIdentityOld = '        rank_identity: base,';
const groupedRankIdentityNew = '        rank_identity: rankIdentity,';
const providerCapOld = '    const cap = marketRankCapForBase(base);';
const providerCapNew = '    const cap = marketRankCapForRow(row);';

const currentMapOld = '  const spotByBase = new Map();';
const currentMapNew = '  const spotByRankIdentity = new Map();';
const currentSpotGroupOld = [
  "      if (market === 'spot') {",
  '        const base = marketRankBaseFromRow(row);',
  '        if (!base) continue;',
  '        const list = spotByBase.get(base) || [];',
  '        list.push(row);',
  '        spotByBase.set(base, list);',
  '      }',
].join('\n');
const currentSpotGroupNew = [
  "      if (market === 'spot') {",
  '        const base = marketRankBaseFromRow(row);',
  '        if (!base) continue;',
  '        const cap = marketRankCapForRow(row);',
  '        const rankIdentity = marketRankSpotAggregateIdentity(row, cap);',
  '        if (!rankIdentity) continue;',
  '        const list = spotByRankIdentity.get(rankIdentity) || [];',
  '        list.push(row);',
  '        spotByRankIdentity.set(rankIdentity, list);',
  '      }',
].join('\n');
const currentReturnOld = '  return { exact, spotByBase };';
const currentReturnNew = '  return { exact, spotByRankIdentity };';
const materializeLookupOld =
  '    const liveVenueRows = current.spotByBase.get(compact(orderEntry?.base_asset)) || [];';
const materializeLookupNew =
  "    const liveVenueRows = current.spotByRankIdentity.get(String(orderEntry?.rank_identity || '')) || [];";

const payloadPolicyOld = [
  "    source: 'render_shared_market_light_rank_order_before_pagination',",
  '    market_type: market,',
].join('\n');
const payloadPolicyNew = [
  "    source: 'render_shared_market_light_rank_order_before_pagination',",
  `    identity_policy_version: '${STEP}',`,
  "    spot_all_provider_identity_policy: 'official_rwa_exact_plus_verified_crypto_coingecko_else_exact_venue',",
  '    official_rwa_symbol_merge_disabled: true,',
  '    unverified_symbol_merge_disabled: true,',
  '    market_type: market,',
].join('\n');

const semantic = semanticSelfTest();
console.log(`${STEP} SEMANTIC`, JSON.stringify(semantic));
if (!semantic.ok) {
  throw new Error(`${STEP} semantic fixture failed before mutation`);
}

const red = {
  rest_market_row_identity_anchor: countExact(rest, marketRowIdentityAnchor),
  rest_okx_spot_directory_anchor: countExact(rest, okxSpotDirectoryOld),
  rest_bitget_v2_url_anchor: countExact(rest, bitgetSpotUrlOld),
  rest_bitget_spot_directory_anchor: countExact(rest, bitgetSpotDirectoryOld),
  rest_self_test_anchor: countExact(rest, restSelfTestAnchor),
  snapshot_identity_anchor: countExact(snapshot, snapshotIdentityAnchor),
  snapshot_helper_before: countExact(snapshot, 'function marketRankOfficialIdentityClass(row) {'),
  snapshot_helper_anchor: countExact(snapshot, helperAnchor),
  snapshot_grouped_old: countExact(snapshot, groupedOld),
  snapshot_rank_identity_old: countExact(snapshot, groupedRankIdentityOld),
  snapshot_provider_cap_old: countExact(snapshot, providerCapOld),
  snapshot_current_map_old: countExact(snapshot, currentMapOld),
  snapshot_current_group_old: countExact(snapshot, currentSpotGroupOld),
  snapshot_current_return_old: countExact(snapshot, currentReturnOld),
  snapshot_materialize_lookup_old: countExact(snapshot, materializeLookupOld),
  snapshot_payload_policy_old: countExact(snapshot, payloadPolicyOld),
};
console.log(`${STEP} RED`, JSON.stringify(red));
if (
  red.rest_market_row_identity_anchor !== 1 ||
  red.rest_okx_spot_directory_anchor !== 1 ||
  red.rest_bitget_v2_url_anchor !== 1 ||
  red.rest_bitget_spot_directory_anchor !== 1 ||
  red.rest_self_test_anchor !== 1 ||
  red.snapshot_identity_anchor !== 1 ||
  red.snapshot_helper_before !== 0 ||
  red.snapshot_helper_anchor !== 1 ||
  red.snapshot_grouped_old !== 1 ||
  red.snapshot_rank_identity_old !== 1 ||
  red.snapshot_provider_cap_old !== 2 ||
  red.snapshot_current_map_old !== 1 ||
  red.snapshot_current_group_old !== 1 ||
  red.snapshot_current_return_old !== 1 ||
  red.snapshot_materialize_lookup_old !== 1 ||
  red.snapshot_payload_policy_old !== 1
) {
  throw new Error(`${STEP} RED baseline mismatch; refusing build-time mutation`);
}

rest = replaceExactlyOnce(rest, marketRowIdentityAnchor, marketRowIdentityReplacement, 'market_row_identity_metadata');
rest = replaceExactlyOnce(rest, okxSpotDirectoryOld, okxSpotDirectoryNew, 'okx_spot_group_identity');
rest = replaceExactlyOnce(rest, bitgetSpotUrlOld, bitgetSpotUrlNew, 'bitget_spot_v3_instruments');
rest = replaceExactlyOnce(rest, bitgetSpotDirectoryOld, bitgetSpotDirectoryNew, 'bitget_spot_identity_flags');
rest = replaceExactlyOnce(rest, restSelfTestAnchor, restSelfTestInsert, 'market_identity_self_tests');

snapshot = replaceExactlyOnce(snapshot, snapshotIdentityAnchor, snapshotIdentityReplacement, 'snapshot_identity_propagation');
snapshot = replaceExactlyOnce(snapshot, helperAnchor, helperInsert, 'rank_identity_helpers');
snapshot = replaceExactlyOnce(snapshot, groupedOld, groupedNew, 'all_provider_verified_identity_grouping');
snapshot = replaceExactlyOnce(snapshot, groupedRankIdentityOld, groupedRankIdentityNew, 'all_provider_rank_identity');
snapshot = replaceExactlyOnce(snapshot, providerCapOld, providerCapNew, 'provider_specific_rwa_market_cap_guard');
snapshot = replaceExactlyOnce(snapshot, currentMapOld, currentMapNew, 'materialize_rank_identity_map');
snapshot = replaceExactlyOnce(snapshot, currentSpotGroupOld, currentSpotGroupNew, 'materialize_rank_identity_grouping');
snapshot = replaceExactlyOnce(snapshot, currentReturnOld, currentReturnNew, 'materialize_rank_identity_return');
snapshot = replaceExactlyOnce(snapshot, materializeLookupOld, materializeLookupNew, 'materialize_rank_identity_lookup');
snapshot = replaceExactlyOnce(snapshot, payloadPolicyOld, payloadPolicyNew, 'rank_identity_policy_metadata');

const green = {
  rest_bitget_v3_url: countExact(rest, "'https://api.bitget.com/api/v3/market/instruments?category=SPOT'"),
  rest_bitget_identity_flags: countExact(rest, '              is_reality: item.isReality,'),
  rest_okx_group_id: countExact(rest, '              instrument_group_id: item.groupId,'),
  rest_identity_test_bitget: countExact(rest, "'bitget_spot_reality_identity_metadata_preserved'"),
  rest_identity_test_okx: countExact(rest, "'okx_spot_rwa_group_identity_metadata_preserved'"),
  snapshot_identity_class_helper: countExact(snapshot, 'function marketRankOfficialIdentityClass(row) {'),
  snapshot_aggregate_helper: countExact(snapshot, 'function marketRankSpotAggregateIdentity(row, cap = null) {'),
  snapshot_verified_grouping: countExact(snapshot, '      const rankIdentity = marketRankSpotAggregateIdentity(row, cap);'),
  snapshot_exact_rwa_group_marker: countExact(snapshot, '    official_rwa_symbol_merge_disabled: true,'),
  snapshot_unverified_group_marker: countExact(snapshot, '    unverified_symbol_merge_disabled: true,'),
  snapshot_old_spot_by_base_remaining: countExact(snapshot, 'spotByBase'),
  snapshot_old_grouped_remaining: countExact(snapshot, groupedOld),
  snapshot_old_rank_identity_remaining: countExact(snapshot, groupedRankIdentityOld),
};
console.log(`${STEP} GREEN`, JSON.stringify(green));
if (
  green.rest_bitget_v3_url !== 1 ||
  green.rest_bitget_identity_flags !== 1 ||
  green.rest_okx_group_id !== 1 ||
  green.rest_identity_test_bitget !== 1 ||
  green.rest_identity_test_okx !== 1 ||
  green.snapshot_identity_class_helper !== 1 ||
  green.snapshot_aggregate_helper !== 1 ||
  green.snapshot_verified_grouping !== 2 ||
  green.snapshot_exact_rwa_group_marker !== 1 ||
  green.snapshot_unverified_group_marker !== 1 ||
  green.snapshot_old_spot_by_base_remaining !== 0 ||
  green.snapshot_old_grouped_remaining !== 0 ||
  green.snapshot_old_rank_identity_remaining !== 0
) {
  throw new Error(`${STEP} GREEN invariant failed; refusing image build`);
}

writeFileSync(restUrl, rest, 'utf8');
writeFileSync(snapshotUrl, snapshot, 'utf8');
console.log(`${STEP} BUILD_PATCH_PASS`);

import { readFileSync, writeFileSync } from 'node:fs';

const STEP = 'Step1072.9.32.4';
const fileUrl = new URL('./market-light-snapshot.mjs', import.meta.url);

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

let source = readFileSync(fileUrl, 'utf8');

const helperAnchor = 'function marketRankEntryComparator(sortKey) {';
const helper = [
  `// ${STEP}: the representative venue shown to the App must match the`,
  '// metric that actually determined the all-provider rank. Change sorts pick',
  '// the venue carrying that extreme change; volume and market-cap sorts keep',
  '// the existing highest-turnover representative policy.',
  'function marketRankRepresentativeRow(rows, sortKey, fallbackRow = null) {',
  '  const safeRows = Array.isArray(rows) ? rows.filter(Boolean) : [];',
  '  const representative = [...safeRows].sort((a, b) => {',
  '    let cmp = 0;',
  "    if (sortKey === 'change_desc') {",
  '      cmp = marketRankCompareNullable(',
  '        a?.price_change_percent_24h,',
  '        b?.price_change_percent_24h,',
  '        { descending: true },',
  '      );',
  "    } else if (sortKey === 'change_asc') {",
  '      cmp = marketRankCompareNullable(',
  '        a?.price_change_percent_24h,',
  '        b?.price_change_percent_24h,',
  '        { descending: false },',
  '      );',
  '    } else {',
  '      cmp = marketRankCompareNullable(',
  '        a?.quote_volume_24h,',
  '        b?.quote_volume_24h,',
  '        { descending: true },',
  '      );',
  '    }',
  '    if (cmp) return cmp;',
  '    const byVolume = marketRankCompareNullable(',
  '      a?.quote_volume_24h,',
  '      b?.quote_volume_24h,',
  '      { descending: true },',
  '    );',
  '    if (byVolume) return byVolume;',
  "    return `${a?.provider || ''}|${a?.symbol || ''}`.localeCompare(",
  "      `${b?.provider || ''}|${b?.symbol || ''}`,",
  '    );',
  '  })[0];',
  '  return representative || fallbackRow || null;',
  '}',
  '',
  helperAnchor,
].join('\n');

const firstRepresentative = [
  '      const representative = [...venueRows].sort((a, b) => {',
  '        const byVolume = marketRankCompareNullable(a?.quote_volume_24h, b?.quote_volume_24h, { descending: true });',
  '        if (byVolume) return byVolume;',
  "        return `${a?.provider || ''}|${a?.symbol || ''}`.localeCompare(`${b?.provider || ''}|${b?.symbol || ''}`);",
  '      })[0] || null;',
].join('\n');
const firstReplacement = '      const representative = marketRankRepresentativeRow(venueRows, sortKey);';

const secondRepresentative = [
  '    const representative = [...venueRows].sort((a, b) => {',
  '      const byVolume = marketRankCompareNullable(a?.quote_volume_24h, b?.quote_volume_24h, { descending: true });',
  '      if (byVolume) return byVolume;',
  "      return `${a?.provider || ''}|${a?.symbol || ''}`.localeCompare(`${b?.provider || ''}|${b?.symbol || ''}`);",
  '    })[0] || fallbackRow || null;',
].join('\n');
const secondReplacement = '    const representative = marketRankRepresentativeRow(venueRows, sortKey, fallbackRow);';

const oldSignature = "function materializeMarketRankItem(orderEntry, { market, provider = '', quote = '' }, current) {";
const newSignature = "function materializeMarketRankItem(orderEntry, { market, provider = '', quote = '', sortKey = 'market_cap_desc' }, current) {";

const oldCaller = '    ...materializeMarketRankItem(entry, { market, provider, quote }, current),';
const newCaller = '    ...materializeMarketRankItem(entry, { market, provider, quote, sortKey: safeSort }, current),';

const red = {
  helper_before: countExact(source, 'function marketRankRepresentativeRow('),
  helper_anchor: countExact(source, helperAnchor),
  build_representative_old: countExact(source, firstRepresentative),
  materialize_representative_old: countExact(source, secondRepresentative),
  materialize_signature_old: countExact(source, oldSignature),
  materialize_caller_old: countExact(source, oldCaller),
};
console.log(`${STEP} RED`, JSON.stringify(red));
if (
  red.helper_before !== 0 ||
  red.helper_anchor !== 1 ||
  red.build_representative_old !== 1 ||
  red.materialize_representative_old !== 1 ||
  red.materialize_signature_old !== 1 ||
  red.materialize_caller_old !== 1
) {
  throw new Error(`${STEP} RED baseline mismatch; refusing build-time mutation`);
}

source = replaceExactlyOnce(source, helperAnchor, helper, 'representative_helper');
source = replaceExactlyOnce(source, firstRepresentative, firstReplacement, 'rank_build_representative');
source = replaceExactlyOnce(source, secondRepresentative, secondReplacement, 'rank_materialize_representative');
source = replaceExactlyOnce(source, oldSignature, newSignature, 'materialize_sort_context');
source = replaceExactlyOnce(source, oldCaller, newCaller, 'materialize_caller_sort_context');

const green = {
  helper: countExact(source, 'function marketRankRepresentativeRow('),
  build_call: countExact(source, 'marketRankRepresentativeRow(venueRows, sortKey);'),
  materialize_call: countExact(source, 'marketRankRepresentativeRow(venueRows, sortKey, fallbackRow);'),
  signature: countExact(source, newSignature),
  caller: countExact(source, newCaller),
  old_build_remaining: countExact(source, firstRepresentative),
  old_materialize_remaining: countExact(source, secondRepresentative),
};
console.log(`${STEP} GREEN`, JSON.stringify(green));
if (
  green.helper !== 1 ||
  green.build_call !== 1 ||
  green.materialize_call !== 1 ||
  green.signature !== 1 ||
  green.caller !== 1 ||
  green.old_build_remaining !== 0 ||
  green.old_materialize_remaining !== 0
) {
  throw new Error(`${STEP} GREEN invariant failed; refusing image build`);
}

writeFileSync(fileUrl, source, 'utf8');
console.log(`${STEP} BUILD_PATCH_PASS`);

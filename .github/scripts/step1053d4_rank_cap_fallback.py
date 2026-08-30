from pathlib import Path

p = Path('src/market-light-snapshot.mjs')
s = p.read_text(encoding='utf-8')

old_version = "const STEP_VERSION = '650.8.15.197.3.3.6.1';"
new_version = "const STEP_VERSION = '650.8.15.197.3.3.6.2';"
if old_version in s:
    s = s.replace(old_version, new_version, 1)
else:
    assert new_version in s, 'market-light version anchor missing'

old_maps = """const marketCapBySymbol = new Map();
const marketCapAmbiguousSymbols = new Set();
const marketCapGlobalSymbolCounts = new Map(); // compatibility: counts inside the verified shared catalog + collision guard
"""
new_maps = """const marketCapBySymbol = new Map();
const marketCapAmbiguousSymbols = new Set();
const marketCapVerifiedFundIdsBySymbol = new Map();
const marketCapGlobalSymbolCounts = new Map(); // compatibility: counts inside the verified shared catalog + collision guard
"""
if 'const marketCapVerifiedFundIdsBySymbol = new Map();' not in s:
    assert old_maps in s, 'market cap map anchor missing'
    s = s.replace(old_maps, new_maps, 1)

old_cap = """function marketRankCapForBase(base) {
  const key = marketRankNormalizeBase(base);
  const item = key ? marketCapBySymbol.get(key) : null;
  return item ? { ...item } : null;
}
"""
new_cap = """function marketRankCapForBase(base) {
  const key = marketRankNormalizeBase(base);
  if (!key) return null;
  const item = marketCapBySymbol.get(key);
  if (item) return { ...item };

  // Step1053D.4: the project Top3000 rank is the canonical market-cap catalog.
  // If the fast symbol index ever misses a catalog-unique identity (BTC exposed
  // this in production), recover only from the already-verified project snapshot.
  // The same verified-fundamentals guard is re-applied here, so ambiguous symbols
  // remain null and turnover is still never used as a market-cap proxy.
  if (Number(marketCapGlobalSymbolCounts.get(key) || 0) !== 1) return null;
  const verifiedIds = marketCapVerifiedFundIdsBySymbol.get(key);
  if (verifiedIds && verifiedIds.size !== 1) return null;
  const snapshot = projectMarketCapRankSnapshots.get(projectMarketCapCurrentRankVersion) || null;
  const rows = Array.isArray(snapshot?.rows) ? snapshot.rows : [];
  const matches = rows.filter((row) => marketRankNormalizeBase(row?.symbol) === key);
  if (matches.length !== 1) return null;
  const candidate = matches[0];
  const coinId = String(candidate?.coin_id || '').trim();
  if (!coinId) return null;
  if (verifiedIds && !verifiedIds.has(coinId)) return null;
  return {
    coingecko_id: coinId,
    market_cap_rank: marketRankNumber(candidate?.market_cap_rank),
    market_cap_usd: marketRankNumber(candidate?.market_cap_usd),
    image_url: String(candidate?.image_url || '').trim() || null,
    total_volume_usd: marketRankNumber(candidate?.total_volume_usd),
  };
}
"""
if 'Step1053D.4: the project Top3000 rank is the canonical market-cap catalog.' not in s:
    assert old_cap in s, 'marketRankCapForBase anchor missing'
    s = s.replace(old_cap, new_cap, 1)

old_verified = """      const next = new Map();
      const ambiguous = new Set();
      let knownAmbiguousByFundamentals = 0;
"""
new_verified = """      marketCapVerifiedFundIdsBySymbol.clear();
      for (const [symbol, ids] of verifiedFundIdsBySymbol.entries()) {
        marketCapVerifiedFundIdsBySymbol.set(symbol, new Set(ids));
      }

      const next = new Map();
      const ambiguous = new Set();
      let knownAmbiguousByFundamentals = 0;
"""
if 'marketCapVerifiedFundIdsBySymbol.clear();' not in s:
    assert old_verified in s, 'verified fundamentals copy anchor missing'
    s = s.replace(old_verified, new_verified, 1)

p.write_text(s, encoding='utf-8')

proxy = Path('src/proxy.mjs')
q = proxy.read_text(encoding='utf-8')
old_proxy = "const STEP_VERSION = '650.8.15.197.3.3.32.1';"
new_proxy = "const STEP_VERSION = '650.8.15.197.3.3.32.2';"
if old_proxy in q:
    q = q.replace(old_proxy, new_proxy, 1)
else:
    assert new_proxy in q, 'proxy version anchor missing'
proxy.write_text(q, encoding='utf-8')

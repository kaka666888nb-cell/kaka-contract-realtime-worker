from pathlib import Path

p=Path('src/market-light-snapshot.mjs')
s=p.read_text(encoding='utf-8')
old="const STEP_VERSION = '650.8.15.197.3.3.6.2';"
new="const STEP_VERSION = '650.8.15.197.3.3.6.3';"
if old in s:
    s=s.replace(old,new,1)
else:
    assert new in s, 'market-light version anchor missing'

anchor="""  const item = marketCapBySymbol.get(key);
  if (item) return { ...item };

  // Step1053D.4: the project Top3000 rank is the canonical market-cap catalog.
"""
replacement="""  const item = marketCapBySymbol.get(key);
  if (item) return { ...item };

  // Step1053D.4.1: BTC is an independently verified core identity in both
  // project fundamentals (bitcoin) and the canonical project Top3000 snapshot.
  // Production proved the generic fast symbol index can still omit rank #1 while
  // all five real BTC/USDT contract rows are present. Recover BTC only from the
  // exact project identity; never infer it from turnover or a similarly named token.
  if (key === 'BTC') {
    const snapshot = projectMarketCapRankSnapshots.get(projectMarketCapCurrentRankVersion) || null;
    const rows = Array.isArray(snapshot?.rows) ? snapshot.rows : [];
    const btc = rows.find((row) =>
      String(row?.symbol || '').trim().toUpperCase() === 'BTC' &&
      String(row?.coin_id || '').trim() === 'bitcoin' &&
      Number(row?.market_cap_rank) === 1
    );
    if (btc) {
      return {
        coingecko_id: 'bitcoin',
        market_cap_rank: 1,
        market_cap_usd: marketRankNumber(btc?.market_cap_usd),
        image_url: String(btc?.image_url || '').trim() || null,
        total_volume_usd: marketRankNumber(btc?.total_volume_usd),
      };
    }
  }

  // Step1053D.4: the project Top3000 rank is the canonical market-cap catalog.
"""
if 'Step1053D.4.1: BTC is an independently verified core identity' not in s:
    assert anchor in s, 'BTC fallback anchor missing'
    s=s.replace(anchor,replacement,1)
p.write_text(s,encoding='utf-8')

proxy=Path('src/proxy.mjs')
q=proxy.read_text(encoding='utf-8')
oldp="const STEP_VERSION = '650.8.15.197.3.3.32.2';"
newp="const STEP_VERSION = '650.8.15.197.3.3.32.3';"
if oldp in q:
    q=q.replace(oldp,newp,1)
else:
    assert newp in q, 'proxy version anchor missing'
proxy.write_text(q,encoding='utf-8')

from pathlib import Path

p = Path("src/market-light-snapshot.mjs")
s = p.read_text(encoding="utf-8")

old_version = "const STEP_VERSION = '650.8.15.197.3.3.6';"
new_version = "const STEP_VERSION = '650.8.15.197.3.3.6.1';"
assert old_version in s, "market-light STEP_VERSION anchor missing"
s = s.replace(old_version, new_version, 1)

anchor = "function createMarketRankOrderSnapshot({ market, provider = '', quote = '', sortKey }) {\n"
helper = """function marketRankFallbackRow(raw, market) {
  if (!raw || typeof raw !== 'object') return null;
  const provider = String(raw?.provider || '').trim().toLowerCase();
  const symbol = compact(raw?.symbol);
  const quote = compact(raw?.quote_asset ?? raw?.quote_symbol);
  const base = marketRankBaseFromRow(raw);
  if (!provider || !symbol || !quote || !base || base === quote) return null;
  return {
    provider,
    market_type: market,
    symbol,
    base_asset: base,
    quote_asset: quote,
    quote_symbol: quote,
    last_price: marketRankNumber(raw?.last_price ?? raw?.price ?? raw?.contract_price ?? raw?.mark_price),
    price: marketRankNumber(raw?.price ?? raw?.last_price ?? raw?.contract_price ?? raw?.mark_price),
    contract_price: marketRankNumber(raw?.contract_price ?? raw?.last_price ?? raw?.price ?? raw?.mark_price),
    mark_price: marketRankNumber(raw?.mark_price),
    price_change_percent_24h: marketRankNumber(raw?.price_change_percent_24h),
    quote_volume_24h: marketRankNumber(raw?.quote_volume_24h),
    base_volume_24h: marketRankNumber(raw?.base_volume_24h),
    source_time: raw?.source_time ?? raw?.cached_at ?? null,
    cached_at: raw?.cached_at ?? raw?.source_time ?? null,
  };
}

function createMarketRankOrderSnapshot({ market, provider = '', quote = '', sortKey }) {
"""
assert anchor in s, "createMarketRankOrderSnapshot anchor missing"
s = s.replace(anchor, helper, 1)

old_order = """    coingecko_total_volume_usd: marketRankNumber(entry?.coingecko_total_volume_usd),
    rank_metric_value: marketRankMetricValue(entry, sortKey),
  })).filter((entry) => entry.rank_identity);
"""
new_order = """    coingecko_total_volume_usd: marketRankNumber(entry?.coingecko_total_volume_usd),
    rank_metric_value: marketRankMetricValue(entry, sortKey),
    fallback_row: marketRankFallbackRow(entry?.row ?? entry?.representative_row, market),
  })).filter((entry) => entry.rank_identity);
"""
assert old_order in s, "rank order projection anchor missing"
s = s.replace(old_order, new_order, 1)

old_materialize = """function materializeMarketRankItem(orderEntry, { market, provider = '', quote = '' }, current) {
  if (market === 'spot' && (!provider || provider === 'all')) {
    const venueRows = current.spotByBase.get(compact(orderEntry?.base_asset)) || [];
    const representative = [...venueRows].sort((a, b) => {
      const byVolume = marketRankCompareNullable(a?.quote_volume_24h, b?.quote_volume_24h, { descending: true });
      if (byVolume) return byVolume;
      return `${a?.provider || ''}|${a?.symbol || ''}`.localeCompare(`${b?.provider || ''}|${b?.symbol || ''}`);
    })[0] || null;
    return {
      ...orderEntry,
      representative_row: representative,
      venue_rows: venueRows,
    };
  }
  return {
    ...orderEntry,
    row: current.exact.get(orderEntry.rank_identity) || null,
  };
}
"""
new_materialize = """function materializeMarketRankItem(orderEntry, { market, provider = '', quote = '' }, current) {
  const { fallback_row: fallbackRowRaw, ...publicEntry } = orderEntry || {};
  const fallbackRow = fallbackRowRaw && typeof fallbackRowRaw === 'object'
    ? { ...fallbackRowRaw }
    : null;
  if (market === 'spot' && (!provider || provider === 'all')) {
    const liveVenueRows = current.spotByBase.get(compact(orderEntry?.base_asset)) || [];
    const venueRows = liveVenueRows.length
      ? liveVenueRows
      : fallbackRow
        ? [fallbackRow]
        : [];
    const representative = [...venueRows].sort((a, b) => {
      const byVolume = marketRankCompareNullable(a?.quote_volume_24h, b?.quote_volume_24h, { descending: true });
      if (byVolume) return byVolume;
      return `${a?.provider || ''}|${a?.symbol || ''}`.localeCompare(`${b?.provider || ''}|${b?.symbol || ''}`);
    })[0] || fallbackRow || null;
    return {
      ...publicEntry,
      representative_row: representative,
      venue_rows: venueRows,
      row_fallback_used: liveVenueRows.length === 0 && Boolean(fallbackRow),
    };
  }
  const liveRow = current.exact.get(orderEntry.rank_identity) || null;
  return {
    ...publicEntry,
    row: liveRow || fallbackRow || null,
    row_fallback_used: !liveRow && Boolean(fallbackRow),
  };
}
"""
assert old_materialize in s, "materializeMarketRankItem anchor missing"
s = s.replace(old_materialize, new_materialize, 1)

old_flags = """    ranking_happens_before_pagination: true,
    pagination_order_frozen_by_rank_version: true,
    app_page_size_remains_50: true,
"""
new_flags = """    ranking_happens_before_pagination: true,
    pagination_order_frozen_by_rank_version: true,
    rank_order_fallback_identity_preserved: true,
    rank_index_scope_contiguous: true,
    app_page_size_remains_50: true,
"""
assert old_flags in s, "rank payload flags anchor missing"
s = s.replace(old_flags, new_flags, 1)
p.write_text(s, encoding="utf-8")

proxy = Path("src/proxy.mjs")
q = proxy.read_text(encoding="utf-8")
old_proxy = "const STEP_VERSION = '650.8.15.197.3.3.32';"
new_proxy = "const STEP_VERSION = '650.8.15.197.3.3.32.1';"
assert old_proxy in q, "proxy STEP_VERSION anchor missing"
q = q.replace(old_proxy, new_proxy, 1)
proxy.write_text(q, encoding="utf-8")

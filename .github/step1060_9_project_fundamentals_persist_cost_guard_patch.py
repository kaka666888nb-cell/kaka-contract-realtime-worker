from pathlib import Path
p=Path('src/project-fundamentals.mjs')
s=p.read_text(encoding='utf-8')

old_const="""const RESTORE_MAX_AGE_MS = Math.max(24 * 60 * 60_000, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_RESTORE_MAX_AGE_MS || 7 * 24 * 60 * 60_000));
const REQUEST_TIMEOUT_MS = Math.max(5_000, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_REQUEST_TIMEOUT_MS || 25_000));
"""
new_const="""const RESTORE_MAX_AGE_MS = Math.max(24 * 60 * 60_000, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_RESTORE_MAX_AGE_MS || 7 * 24 * 60 * 60_000));
// Step1060.9: source refresh remains hourly, while unchanged persisted rows only need a bounded
// heartbeat for restart recovery. App reads keep using the freshly rebuilt in-memory catalog.
const PERSIST_HEARTBEAT_MS = Math.max(
  60 * 60_000,
  Math.min(24 * 60 * 60_000, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_PERSIST_HEARTBEAT_MS || 6 * 60 * 60_000)),
);
const REQUEST_TIMEOUT_MS = Math.max(5_000, Number(process.env.KAKA_PROJECT_FUNDAMENTALS_REQUEST_TIMEOUT_MS || 25_000));
"""
assert s.count(old_const)==1, f'const anchor {s.count(old_const)}'
s=s.replace(old_const,new_const,1)

old_vars="""let persistAttempts = 0;
let persistSuccesses = 0;
let persistFailures = 0;
let totalReads = 0;
"""
new_vars="""let persistAttempts = 0;
let persistSuccesses = 0;
let persistFailures = 0;
let persistRowsConsidered = 0;
let persistRowsWritten = 0;
let persistRowsNoopSkipped = 0;
let persistHeartbeatRows = 0;
let persistRemovedRows = 0;
let lastFullPersistAt = '';
let totalReads = 0;
"""
assert s.count(old_vars)==1, f'vars anchor {s.count(old_vars)}'
s=s.replace(old_vars,new_vars,1)

start=s.index('async function persistCatalog(rows, fetchedAt) {')
end=s.index('async function restorePersisted() {', start)
old=s[start:end]
new="""function persistenceComparable(row) {
  return JSON.stringify({
    coin_id: lower(row?.coin_id),
    symbol: compact(row?.symbol).toUpperCase(),
    protocol_id: compact(row?.protocol_id),
    protocol_slug: compact(row?.protocol_slug),
    protocol_name: compact(row?.protocol_name),
    category: compact(row?.category),
    chains: Array.isArray(row?.chains) ? row.chains.map(compact).filter(Boolean) : [],
    tvl_usd: finiteOrNull(row?.tvl_usd),
    fees_24h_usd: finiteOrNull(row?.fees_24h_usd),
    fees_7d_usd: finiteOrNull(row?.fees_7d_usd),
    fees_30d_usd: finiteOrNull(row?.fees_30d_usd),
    revenue_24h_usd: finiteOrNull(row?.revenue_24h_usd),
    revenue_7d_usd: finiteOrNull(row?.revenue_7d_usd),
    revenue_30d_usd: finiteOrNull(row?.revenue_30d_usd),
    revenue_to_fees_24h_pct: finiteOrNull(row?.revenue_to_fees_24h_pct),
    match_method: compact(row?.match_method),
    match_verified: row?.match_verified === true,
    source_name: compact(row?.source_name),
    source_protocols_endpoint: compact(row?.source_protocols_endpoint),
    source_fees_endpoint: compact(row?.source_fees_endpoint),
    source_revenue_endpoint: compact(row?.source_revenue_endpoint),
  });
}

async function deleteRemovedPersisted(removedIds) {
  if (!removedIds.length) return 0;
  let removed = 0;
  for (let index = 0; index < removedIds.length; index += 100) {
    const batch = removedIds.slice(index, index + 100).map((id) => encodeURIComponent(id));
    const response = await supabaseFetch(`${TABLE}?coin_id=in.(${batch.join(',')})`, {
      method: 'DELETE',
      headers: { prefer: 'return=minimal' },
    });
    if (!response.ok) throw new Error(`cleanup_removed_http_${response.status}`);
    removed += batch.length;
  }
  return removed;
}

async function persistCatalog(nextCatalog, fetchedAt) {
  if (!SUPABASE_CONFIGURED || !(nextCatalog instanceof Map) || !nextCatalog.size) return false;
  const previous = catalog;
  const nextRows = [...nextCatalog.values()];
  persistRowsConsidered += nextRows.length;
  const lastFullMs = Date.parse(lastFullPersistAt || '');
  const heartbeatDue = !Number.isFinite(lastFullMs) || Date.now() - lastFullMs >= PERSIST_HEARTBEAT_MS;
  const changedRows = heartbeatDue ? nextRows : nextRows.filter((row) => {
    const key = lower(row?.coin_id);
    const old = previous.get(key);
    return !old || persistenceComparable(old) !== persistenceComparable(row);
  });
  const removedIds = [...previous.keys()].filter((key) => !nextCatalog.has(key));
  persistRowsNoopSkipped += Math.max(0, nextRows.length - changedRows.length);

  if (!changedRows.length && !removedIds.length) return true;
  persistAttempts += 1;
  try {
    if (changedRows.length) {
      const response = await supabaseFetch(`${TABLE}?on_conflict=coin_id`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          prefer: 'resolution=merge-duplicates,return=minimal',
        },
        body: JSON.stringify(changedRows),
      });
      if (!response.ok) {
        const text = await response.text().catch(() => '');
        throw new Error(`persist_http_${response.status}:${text.slice(0, 160)}`);
      }
      persistRowsWritten += changedRows.length;
      if (heartbeatDue) {
        persistHeartbeatRows += changedRows.length;
        lastFullPersistAt = fetchedAt;
      }
    }
    const removed = await deleteRemovedPersisted(removedIds);
    persistRemovedRows += removed;
    persistSuccesses += 1;
    lastPersistedAt = new Date().toISOString();
    return true;
  } catch (error) {
    persistFailures += 1;
    throw error;
  }
}

"""
s=s[:start]+new+s[end:]

old_restore="""      restoreRows = restored.size;
      lastSourceFetchedAt = compact([...restored.values()][0]?.source_fetched_at);
"""
new_restore="""      restoreRows = restored.size;
      lastSourceFetchedAt = compact([...restored.values()][0]?.source_fetched_at);
      lastFullPersistAt = lastSourceFetchedAt;
"""
assert s.count(old_restore)==1, f'restore anchor {s.count(old_restore)}'
s=s.replace(old_restore,new_restore,1)

old_call="""      await persistCatalog([...built.next.values()], fetchedAt);
      catalog.clear();
"""
new_call="""      await persistCatalog(built.next, fetchedAt);
      catalog.clear();
"""
assert s.count(old_call)==1, f'call anchor {s.count(old_call)}'
s=s.replace(old_call,new_call,1)

old_health="""    persist_attempts: persistAttempts,
    persist_successes: persistSuccesses,
    persist_failures: persistFailures,
    user_reads: totalReads,
"""
new_health="""    persist_attempts: persistAttempts,
    persist_successes: persistSuccesses,
    persist_failures: persistFailures,
    persistence_cost_guard: {
      version: 'step1060_9_project_fundamentals_persist_cost_guard_v1',
      source_refresh_interval_minutes: REFRESH_MS / 60_000,
      unchanged_rows_write_skipped: true,
      exact_removed_coin_id_cleanup: true,
      full_persist_heartbeat_hours: PERSIST_HEARTBEAT_MS / 3_600_000,
      rows_considered: persistRowsConsidered,
      rows_written: persistRowsWritten,
      rows_noop_skipped: persistRowsNoopSkipped,
      heartbeat_rows_written: persistHeartbeatRows,
      removed_rows: persistRemovedRows,
      last_full_persist_at: lastFullPersistAt || null,
    },
    user_reads: totalReads,
"""
assert s.count(old_health)==1, f'health anchor {s.count(old_health)}'
s=s.replace(old_health,new_health,1)

p.write_text(s,encoding='utf-8')

from pathlib import Path

p = Path('src/onchain-market.mjs')
s = p.read_text(encoding='utf-8')

old_import = "import { readFileSync, writeFileSync, renameSync } from 'node:fs';\nimport { KAKA_FULL_INTERVALS, KAKA_DERIVED_PLAN, klineIntervalMs, klineDerivedPlan, deriveAndFillKlines } from './kline-derived.mjs';"
new_import = "import { readFileSync, writeFileSync, renameSync } from 'node:fs';\nimport { KAKA_FULL_INTERVALS, KAKA_DERIVED_PLAN, klineIntervalMs, klineDerivedPlan, deriveAndFillKlines } from './kline-derived.mjs';\nimport { restoreOnchainHotSnapshot, persistOnchainHotSnapshot, getOnchainHotPersistenceHealth } from './onchain-hot-persistence.mjs';"
assert s.count(old_import) == 1, 'import anchor mismatch'
s = s.replace(old_import, new_import, 1)

old_state = "let trendingSnapshot = [];\nlet discoveryUpdatedAt = 0;\nlet marketRefreshInflight = null;"
new_state = "let trendingSnapshot = [];\nlet discoveryUpdatedAt = 0;\n// Step1060.5.1: restart-safe shared hot snapshot. Restored rows retain the original\n// verified source timestamp; user reads never start an upstream or database request.\nlet persistedHotSnapshotRestored = false;\nlet persistedHotSnapshotRestoreAgeMs = null;\nlet persistedHotSnapshotRestoreSource = '';\nlet marketRefreshInflight = null;"
assert s.count(old_state) == 1, 'state anchor mismatch'
s = s.replace(old_state, new_state, 1)

old_response = "    fixed_backend_upstream_rate_independent_of_user_count: true,\n    same_key_cache_singleflight: true,"
new_response = "    fixed_backend_upstream_rate_independent_of_user_count: true,\n    persisted_hot_snapshot_restored: persistedHotSnapshotRestored,\n    persisted_hot_snapshot_restore_age_ms: persistedHotSnapshotRestoreAgeMs,\n    persisted_hot_snapshot_restore_source: persistedHotSnapshotRestoreSource || null,\n    onchain_hot_persistence: getOnchainHotPersistenceHealth(),\n    same_key_cache_singleflight: true,"
assert s.count(old_response) == 1, 'responseBase anchor mismatch'
s = s.replace(old_response, new_response, 1)

old_success = """      trendingSnapshot = rows;
      discoveryUpdatedAt = Date.now();
      marketUpdatedAt = discoveryUpdatedAt;
      marketNetworkUpdatedAt.clear();
      for (const network of new Set(rows.map((row) => row.network).filter(Boolean))) {
        marketNetworkUpdatedAt.set(network, discoveryUpdatedAt);
      }
"""
new_success = """      trendingSnapshot = rows;
      discoveryUpdatedAt = Date.now();
      marketUpdatedAt = discoveryUpdatedAt;
      marketNetworkUpdatedAt.clear();
      for (const network of new Set(rows.map((row) => row.network).filter(Boolean))) {
        marketNetworkUpdatedAt.set(network, discoveryUpdatedAt);
      }
      // Persist only the slow 5-minute verified discovery snapshot. Do not persist the
      // 30-second market-field refresh; this keeps database/Render cost fixed and bounded.
      persistOnchainHotSnapshot({
        rows: trendingSnapshot,
        sourceTimeMs: discoveryUpdatedAt,
        source: 'render_onchain_verified_discovery',
      }).catch(() => {});
"""
assert s.count(old_success) == 1, 'discovery success anchor mismatch'
s = s.replace(old_success, new_success, 1)

old_start = """export function startOnchainMarketCollector() {
  if (discoveryStarted) return;
  discoveryStarted = true;
  const first = setTimeout(() => refreshDiscovery().catch(() => {}), 2_500);
"""
new_start = """async function restorePersistedTrendingSnapshot() {
  const baselineUpdatedAt = discoveryUpdatedAt;
  const restored = await restoreOnchainHotSnapshot({
    maxAgeMs: DISCOVERY_RETAIN_MS,
    maxRows: STEP1042_INTERNAL_HOT_MAX_ROWS_PER_CHAIN * Object.keys(GECKO_NETWORK).length,
  });
  if (!restored?.rows?.length) return false;
  // Never let an older persisted snapshot overwrite a fresh discovery that won the race.
  if (discoveryUpdatedAt !== baselineUpdatedAt || trendingSnapshot.length) return false;
  const sourceTimeMs = Number(restored.source_time_ms) || 0;
  if (!sourceTimeMs) return false;
  trendingSnapshot = restored.rows;
  discoveryUpdatedAt = sourceTimeMs;
  marketUpdatedAt = sourceTimeMs;
  marketNetworkUpdatedAt.clear();
  for (const network of new Set(trendingSnapshot.map((row) => row?.network).filter(Boolean))) {
    marketNetworkUpdatedAt.set(network, sourceTimeMs);
  }
  persistedHotSnapshotRestored = true;
  persistedHotSnapshotRestoreAgeMs = Math.max(0, Date.now() - sourceTimeMs);
  persistedHotSnapshotRestoreSource = restored.source || 'supabase_backend_snapshot';
  return true;
}

export function startOnchainMarketCollector() {
  if (discoveryStarted) return;
  discoveryStarted = true;
  // Restore previous exact verified rows immediately. This is one fixed backend read on
  // process startup, never a user-triggered DB/upstream request. Fresh discovery still runs.
  restorePersistedTrendingSnapshot().catch(() => {});
  const first = setTimeout(() => refreshDiscovery().catch(() => {}), 2_500);
"""
assert s.count(old_start) == 1, 'collector startup anchor mismatch'
s = s.replace(old_start, new_start, 1)

p.write_text(s, encoding='utf-8')

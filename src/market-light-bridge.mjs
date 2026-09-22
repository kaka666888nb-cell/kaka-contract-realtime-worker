import { requestIsolatedJson } from './collector-isolation.mjs';

const VERSION = '650.8.15.125';
const CONSUMER_ROLE = String(process.env.KAKA_ISOLATED_COLLECTOR_ROLE || 'parent').trim() || 'parent';
const STATE_SCOPE = CONSUMER_ROLE === 'deep-market' ? 'deep-market' : CONSUMER_ROLE === 'slow-stats' ? 'slow-stats' : 'parent';
const DEFAULT_POLL_MS =
  CONSUMER_ROLE === 'slow-stats' ? 8_000 :
  CONSUMER_ROLE === 'deep-market' ? 5_000 :
  10_000;
const DEFAULT_STALE_MS =
  CONSUMER_ROLE === 'slow-stats' ? 30_000 :
  CONSUMER_ROLE === 'deep-market' ? 20_000 :
  30_000;
// Step1073 R59: the parent consumes a 30s market-light shared round for Basis,
// Reality rank and the 2m asset-rank layer. Polling the same projected ~7k-row
// round every 2.5s only repeats clone/projection/JSON work. Keep parent at a
// hard 10s floor while preserving the faster dedicated deep/slow bridges.
const MIN_POLL_MS = CONSUMER_ROLE === 'parent' ? 10_000 : 750;
const POLL_MS = Math.max(MIN_POLL_MS, Number(process.env.KAKA_MARKET_LIGHT_BRIDGE_POLL_MS || DEFAULT_POLL_MS));
const STALE_MS = Math.max(5_000, Number(process.env.KAKA_MARKET_LIGHT_BRIDGE_STALE_MS || DEFAULT_STALE_MS));

let timer = null;
let running = false;
let lastSuccessAt = 0;
let lastAttemptAt = 0;
let lastError = '';
let remoteHealth = null;
let pollAttempts = 0;
let fullUpdates = 0;
let notModifiedHits = 0;
let lastAppliedRound = 0;
const providers = new Map();

async function poll() {
  if (running) return;
  running = true;
  pollAttempts += 1;
  lastAttemptAt = Date.now();
  try {
    const knownRound = Math.max(0, Number(remoteHealth?.round || lastAppliedRound || 0));
    const query = new URLSearchParams({ scope: STATE_SCOPE });
    if (knownRound > 0) query.set('if_round', String(knownRound));
    const payload = await requestIsolatedJson('market-light', `/_isolated/state?${query.toString()}`, 8_000);
    if (!payload?.ok || !payload?.health) throw new Error('market_light_bridge_invalid_payload');
    remoteHealth = payload.health;
    if (payload.not_modified === true) {
      notModifiedHits += 1;
      lastSuccessAt = Date.now();
      lastError = '';
      return;
    }
    providers.clear();
    for (const [key, value] of Object.entries(payload.providers || {})) {
      providers.set(key, value);
    }
    fullUpdates += 1;
    lastAppliedRound = Math.max(0, Number(payload.shared_round || payload.health?.round || 0));
    lastSuccessAt = Date.now();
    lastError = '';
  } catch (error) {
    lastError = String(error?.message || error).slice(0, 400);
  } finally {
    running = false;
  }
}

export function startMarketLightBridge() {
  if (timer) return;
  poll().catch(() => {});
  timer = setInterval(() => poll().catch(() => {}), POLL_MS);
  timer.unref?.();
}

export function getMarketLightInternalSnapshot({ market = '', provider = '' } = {}) {
  const key = `${String(market || '').toLowerCase()}:${String(provider || '').toLowerCase()}`;
  const payload = providers.get(key);
  if (payload) {
    return {
      ...payload,
      isolated_bridge: true,
      isolated_bridge_version: VERSION,
      isolated_bridge_age_ms: lastSuccessAt > 0 ? Date.now() - lastSuccessAt : null,
    };
  }
  return {
    ok: false,
    version: remoteHealth?.version || null,
    error: 'isolated_market_light_bridge_not_ready',
    market_type: String(market || '').toLowerCase(),
    provider: String(provider || '').toLowerCase(),
    row_count: 0,
    directory_count: 0,
    stale: true,
    last_error: lastError || 'bridge_not_ready',
    rows: [],
    reads_scale_with_users: false,
    isolated_bridge: true,
  };
}

export function getMarketLightSnapshotHealth() {
  const age = lastSuccessAt > 0 ? Date.now() - lastSuccessAt : null;
  const bridgeFresh = age != null && age <= STALE_MS;
  return {
    ...(remoteHealth || {
      ok: false,
      version: null,
      provider_coverage: {},
      last_error: lastError || 'bridge_not_ready',
    }),
    isolated_bridge: true,
    isolated_bridge_consumer_role: CONSUMER_ROLE,
    isolated_bridge_state_scope: STATE_SCOPE,
    isolated_bridge_provider_snapshot_count: providers.size,
    isolated_bridge_projected_payloads: true,
    isolated_bridge_full_row_copy_disabled: true,
    isolated_bridge_version: VERSION,
    isolated_bridge_poll_ms: POLL_MS,
    isolated_bridge_stale_ms: STALE_MS,
    isolated_bridge_age_ms: age,
    isolated_bridge_fresh: bridgeFresh,
    isolated_bridge_last_attempt_at: lastAttemptAt ? new Date(lastAttemptAt).toISOString() : null,
    isolated_bridge_last_success_at: lastSuccessAt ? new Date(lastSuccessAt).toISOString() : null,
    isolated_bridge_last_error: lastError,
    isolated_bridge_conditional_round_reads: true,
    isolated_bridge_source_round: Math.max(0, Number(remoteHealth?.round || lastAppliedRound || 0)),
    isolated_bridge_poll_attempts: pollAttempts,
    isolated_bridge_full_updates: fullUpdates,
    isolated_bridge_not_modified_hits: notModifiedHits,
    parent_starts_market_light_scanner: false,
    reads_scale_with_users: false,
  };
}

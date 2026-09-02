const VERSION = 'step1060_5_1_onchain_hot_snapshot_persistence_v1';
const TABLE = 'app_onchain_shared_snapshots';
const SNAPSHOT_KEY = 'recent_hot';
const MAX_ROWS = 300;
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const CONFIGURED = Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY);

const health = {
  version: VERSION,
  configured: CONFIGURED,
  restore_attempts: 0,
  restore_successes: 0,
  restore_failures: 0,
  persist_attempts: 0,
  persist_successes: 0,
  persist_failures: 0,
  last_restore_at: '',
  last_persist_at: '',
  last_error: '',
};

function headers(extra = {}) {
  return {
    authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    ...extra,
  };
}

function safeRows(value, maxRows = MAX_ROWS) {
  if (!Array.isArray(value)) return [];
  return value
    .filter((row) => row && typeof row === 'object' && !Array.isArray(row))
    .slice(0, Math.max(1, Math.min(MAX_ROWS, Number(maxRows) || MAX_ROWS)));
}

export async function restoreOnchainHotSnapshot({ maxAgeMs, maxRows = MAX_ROWS } = {}) {
  health.restore_attempts += 1;
  if (!CONFIGURED) {
    health.restore_failures += 1;
    health.last_error = 'supabase_not_configured';
    return null;
  }
  try {
    const url = `${SUPABASE_URL}/rest/v1/${TABLE}?snapshot_key=eq.${encodeURIComponent(SNAPSHOT_KEY)}&select=payload,row_count,source_time,updated_at,source,schema_version&limit=1`;
    const response = await fetch(url, { headers: headers({ accept: 'application/json' }) });
    if (!response.ok) throw new Error(`restore_http_${response.status}`);
    const data = await response.json();
    const row = Array.isArray(data) ? data[0] : null;
    if (!row) return null;
    const rows = safeRows(row?.payload?.rows, maxRows);
    if (!rows.length) return null;
    const sourceTimeMs = Date.parse(String(row.source_time || row.updated_at || '')) || 0;
    const ageMs = sourceTimeMs > 0 ? Math.max(0, Date.now() - sourceTimeMs) : Number.POSITIVE_INFINITY;
    if (Number.isFinite(Number(maxAgeMs)) && ageMs > Number(maxAgeMs)) return null;
    health.restore_successes += 1;
    health.last_restore_at = new Date().toISOString();
    health.last_error = '';
    return {
      rows,
      source_time_ms: sourceTimeMs,
      age_ms: ageMs,
      source: String(row.source || ''),
      schema_version: String(row.schema_version || ''),
    };
  } catch (error) {
    health.restore_failures += 1;
    health.last_error = String(error?.message || error).slice(0, 240);
    return null;
  }
}

export async function persistOnchainHotSnapshot({ rows, sourceTimeMs, source = 'render_shared_onchain_hot' } = {}) {
  health.persist_attempts += 1;
  if (!CONFIGURED) {
    health.persist_failures += 1;
    health.last_error = 'supabase_not_configured';
    return false;
  }
  const safe = safeRows(rows, MAX_ROWS);
  if (!safe.length) return false;
  const sourceTime = new Date(Number(sourceTimeMs) || Date.now()).toISOString();
  const body = JSON.stringify({
    snapshot_key: SNAPSHOT_KEY,
    payload: { rows: safe },
    row_count: safe.length,
    source_time: sourceTime,
    source,
    schema_version: VERSION,
    updated_at: new Date().toISOString(),
  });
  try {
    const response = await fetch(`${SUPABASE_URL}/rest/v1/${TABLE}?on_conflict=snapshot_key`, {
      method: 'POST',
      headers: headers({
        'content-type': 'application/json',
        prefer: 'resolution=merge-duplicates,return=minimal',
      }),
      body,
    });
    if (!response.ok) throw new Error(`persist_http_${response.status}:${(await response.text()).slice(0, 180)}`);
    health.persist_successes += 1;
    health.last_persist_at = new Date().toISOString();
    health.last_error = '';
    return true;
  } catch (error) {
    health.persist_failures += 1;
    health.last_error = String(error?.message || error).slice(0, 240);
    return false;
  }
}

export function getOnchainHotPersistenceHealth() {
  return { ...health };
}

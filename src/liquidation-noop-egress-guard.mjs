const STEP = 'Step1060.26';
const TARGET_PATH = '/rest/v1/app_contract_liquidation_1h_cache';
const MAX_SIGNATURES = 25000;
const INSTALL_KEY = Symbol.for('kaka.step1060.26.liquidation1hNoopEgressGuard');

const state = globalThis[INSTALL_KEY] || {
  installed: false,
  originalFetch: null,
  signatures: new Map(),
  requestsSeen: 0,
  rowsSeen: 0,
  rowsForwarded: 0,
  rowsSkipped: 0,
  fullySkippedRequests: 0,
  parseBypassRequests: 0,
  lastForwardedAt: 0,
  lastSkippedAt: 0,
};
globalThis[INSTALL_KEY] = state;

function requestUrl(input) {
  if (typeof input === 'string') return input;
  if (input instanceof URL) return input.toString();
  if (input && typeof input.url === 'string') return input.url;
  return '';
}

function requestMethod(input, init) {
  return String(init?.method || input?.method || 'GET').trim().toUpperCase();
}

function targetRequest(input, init) {
  if (requestMethod(input, init) !== 'POST') return false;
  const raw = requestUrl(input);
  if (!raw) return false;
  try {
    return new URL(raw).pathname === TARGET_PATH;
  } catch (_) {
    return false;
  }
}

function stableValue(value) {
  if (Array.isArray(value)) return value.map(stableValue);
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.keys(value)
        .filter((key) => key !== 'cached_at')
        .sort()
        .map((key) => [key, stableValue(value[key])]),
    );
  }
  return value;
}

function rowIdentity(row) {
  if (!row || typeof row !== 'object') return '';
  const provider = String(row.provider || '').trim().toLowerCase();
  const marketType = String(row.market_type || '').trim().toLowerCase();
  const symbol = String(row.symbol || '').trim().toUpperCase();
  const bucketStart = String(row.bucket_start || '').trim();
  if (!provider || !marketType || !symbol || !bucketStart) return '';
  return `${provider}|${marketType}|${symbol}|${bucketStart}`;
}

function rowSignature(row) {
  return JSON.stringify(stableValue(row));
}

function remember(key, signature) {
  if (state.signatures.has(key)) state.signatures.delete(key);
  state.signatures.set(key, signature);
  while (state.signatures.size > MAX_SIGNATURES) {
    const oldest = state.signatures.keys().next().value;
    if (oldest == null) break;
    state.signatures.delete(oldest);
  }
}

function parseRows(init) {
  if (!init || typeof init.body !== 'string') return null;
  let decoded;
  try {
    decoded = JSON.parse(init.body);
  } catch (_) {
    return null;
  }
  if (!Array.isArray(decoded) || decoded.length === 0) return null;
  if (!decoded.every((row) => row && typeof row === 'object' && !Array.isArray(row))) return null;
  return decoded;
}

function syntheticNoopResponse() {
  return new Response(null, {
    status: 204,
    statusText: 'No Content',
    headers: {
      'x-kaka-step1060-26-noop': '1',
    },
  });
}

export function getLiquidation1hNoopEgressHealth() {
  return {
    step: STEP,
    installed: state.installed,
    target_path: TARGET_PATH,
    signature_entries: state.signatures.size,
    max_signature_entries: MAX_SIGNATURES,
    requests_seen: state.requestsSeen,
    rows_seen: state.rowsSeen,
    rows_forwarded: state.rowsForwarded,
    rows_skipped: state.rowsSkipped,
    fully_skipped_requests: state.fullySkippedRequests,
    parse_bypass_requests: state.parseBypassRequests,
    last_forwarded_at: state.lastForwardedAt ? new Date(state.lastForwardedAt).toISOString() : null,
    last_skipped_at: state.lastSkippedAt ? new Date(state.lastSkippedAt).toISOString() : null,
    cached_at_excluded_from_signature: true,
    real_semantic_changes_forward_immediately: true,
    minute_liquidation_table_untouched: true,
  };
}

export function installLiquidation1hNoopEgressGuard() {
  if (state.installed) return getLiquidation1hNoopEgressHealth();
  if (typeof globalThis.fetch !== 'function') throw new Error('global_fetch_unavailable');

  const upstreamFetch = globalThis.fetch.bind(globalThis);
  state.originalFetch = upstreamFetch;

  globalThis.fetch = async function kakaStep106026Liquidation1hNoopFetch(input, init = undefined) {
    if (!targetRequest(input, init)) return upstreamFetch(input, init);

    state.requestsSeen += 1;
    const rows = parseRows(init);
    if (!rows) {
      state.parseBypassRequests += 1;
      return upstreamFetch(input, init);
    }

    state.rowsSeen += rows.length;
    const changed = [];
    const pending = [];
    let skipped = 0;

    for (const row of rows) {
      const key = rowIdentity(row);
      if (!key) {
        changed.push(row);
        continue;
      }
      const signature = rowSignature(row);
      if (state.signatures.get(key) === signature) {
        skipped += 1;
        remember(key, signature);
        continue;
      }
      changed.push(row);
      pending.push({ key, signature });
    }

    if (skipped > 0) {
      state.rowsSkipped += skipped;
      state.lastSkippedAt = Date.now();
    }

    if (changed.length === 0) {
      state.fullySkippedRequests += 1;
      return syntheticNoopResponse();
    }

    const nextInit = {
      ...init,
      body: JSON.stringify(changed),
    };
    const response = await upstreamFetch(input, nextInit);
    if (response?.ok) {
      for (const item of pending) remember(item.key, item.signature);
      state.rowsForwarded += changed.length;
      state.lastForwardedAt = Date.now();
    }
    return response;
  };

  state.installed = true;
  console.log(`[${STEP}] liquidation 1H no-op egress guard installed; cached_at excluded from semantic signature`);
  return getLiquidation1hNoopEgressHealth();
}

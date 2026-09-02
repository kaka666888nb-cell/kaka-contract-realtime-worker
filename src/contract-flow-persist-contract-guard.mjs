const STEP = '1060.31.13';
const FLOW_TABLE_PATH = '/rest/v1/app_contract_flow_5m_cache';
const USDT_HISTORY_SYMBOL_RE = /^[A-Z0-9]{2,30}USDT$/;

const health = {
  installed: false,
  inspected_posts: 0,
  forwarded_rows: 0,
  skipped_rows: 0,
  all_invalid_batches: 0,
  parse_failures: 0,
  last_skip_at: '',
  last_skipped_symbols: [],
};

function targetUrl(input) {
  if (typeof input === 'string' || input instanceof URL) return String(input);
  if (input && typeof input.url === 'string') return input.url;
  return '';
}

function isTargetPost(input, init) {
  const url = targetUrl(input);
  if (!url.includes(FLOW_TABLE_PATH)) return false;
  const method = String(init?.method || input?.method || 'GET').toUpperCase();
  return method === 'POST';
}

function validHistoryRow(row) {
  if (!row || typeof row !== 'object' || Array.isArray(row)) return false;
  const symbol = String(row.symbol || '').trim().toUpperCase();
  return USDT_HISTORY_SYMBOL_RE.test(symbol);
}

function filteredBody(body) {
  if (typeof body !== 'string') return null;
  let parsed;
  try {
    parsed = JSON.parse(body);
  } catch {
    health.parse_failures += 1;
    return null;
  }

  const rows = Array.isArray(parsed) ? parsed : [parsed];
  const kept = [];
  const skippedSymbols = [];
  for (const row of rows) {
    if (validHistoryRow(row)) {
      kept.push(row);
    } else {
      skippedSymbols.push(String(row?.symbol || '').trim().toUpperCase().slice(0, 64));
    }
  }
  return { wasArray: Array.isArray(parsed), kept, skippedSymbols };
}

export function getContractFlowPersistContractGuardHealth() {
  return { ...health, last_skipped_symbols: [...health.last_skipped_symbols] };
}

export function installContractFlowPersistContractGuard() {
  if (health.installed) return;
  const previousFetch = globalThis.fetch;
  if (typeof previousFetch !== 'function') throw new Error('global_fetch_unavailable');

  globalThis.fetch = async function kakaContractFlowPersistGuard(input, init = undefined) {
    if (!isTargetPost(input, init)) return previousFetch(input, init);

    health.inspected_posts += 1;
    const result = filteredBody(init?.body);
    // Unknown/non-string bodies are left untouched. The DB constraint remains
    // the final safety net rather than risking corruption of an unfamiliar call.
    if (!result) return previousFetch(input, init);

    const skipped = result.skippedSymbols.length;
    health.skipped_rows += skipped;
    health.forwarded_rows += result.kept.length;
    if (skipped > 0) {
      health.last_skip_at = new Date().toISOString();
      health.last_skipped_symbols = [...new Set(result.skippedSymbols)].slice(0, 12);
    }

    if (result.kept.length === 0) {
      health.all_invalid_batches += 1;
      return new Response(null, {
        status: 204,
        headers: {
          'x-kaka-step': STEP,
          'x-kaka-contract-flow-persist': 'unsupported-non-usdt-skipped',
        },
      });
    }

    if (skipped === 0) return previousFetch(input, init);

    const nextBody = JSON.stringify(result.wasArray ? result.kept : result.kept[0]);
    return previousFetch(input, { ...init, body: nextBody });
  };

  health.installed = true;
}

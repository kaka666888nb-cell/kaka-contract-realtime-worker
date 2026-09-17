import fs from 'node:fs';

const target = 'src/spot-flow-snapshot.mjs';
let source = fs.readFileSync(target, 'utf8').replace(/\r\n/g, '\n');

function count(text, needle) {
  if (!needle) return 0;
  let total = 0;
  let at = 0;
  while (true) {
    const next = text.indexOf(needle, at);
    if (next < 0) break;
    total += 1;
    at = next + needle.length;
  }
  return total;
}

function replaceOnce(text, oldText, newText, label) {
  const hits = count(text, oldText);
  if (hits !== 1) throw new Error(`Step1072.9.32.10 ${label} anchor_count=${hits}`);
  return text.replace(oldText, newText);
}

const OLD_VERSION = "const VERSION = '650.8.15.163';";
const OLD_CALL_RPC_SIG = 'async function callRpc(name, body, signal, timeoutMs = 12_000) {';
const OLD_AUTH_HEADER = '        authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,';
const OLD_MAYBE_ACTIVATE = `async function maybeActivate(provider, symbol, signal) {
  const key = \`${'${provider}:${symbol}'}\`;
  const now = Date.now();
  const previous = Number(activatedAt.get(key) || 0);
  if (now - previous < ACTIVATE_TTL_MS) {
    return { attempted: false, ok: true, cache_state: 'activation_cooldown' };
  }
  stats.activation_calls += 1;
  try {
    const raw = await callRpc('app_activate_spot_trade_flow', {
      p_provider: provider,
      p_symbol: symbol,
    }, signal, 10_000);
    activatedAt.set(key, Date.now());
    stats.activation_succeeded += 1;
    return {
      attempted: true,
      ok: true,
      cache_state: 'activated',
      result: unwrapPayload(raw) || raw,
    };
  } catch (error) {
    stats.activation_failed += 1;
    return {
      attempted: true,
      ok: false,
      cache_state: 'activation_failed_but_snapshot_read_continues',
      error: String(error?.message || error),
    };
  }
}`;
const OLD_BUILD_ACTIVATION = `async function buildPayload(provider, symbol, signal) {
  const activation = await maybeActivate(provider, symbol, signal);
  const settled = await Promise.allSettled([`;
const OLD_PAYLOAD_ACTIVATION = '    activation,\n';
const OLD_HANDLER = `  try {
    const payload = await getSharedSnapshot(provider, symbol, signal);
    sendJson(res, 200, payload);`;
const OLD_TEST_EXPORT = `export const _test = {
  providerKey,
  symbolKey,
  unwrapPayload,
  payloadOk,
};`;

const red = {
  version_anchor: count(source, OLD_VERSION),
  call_rpc_signature: count(source, OLD_CALL_RPC_SIG),
  service_role_auth_header: count(source, OLD_AUTH_HEADER),
  maybe_activate_old: count(source, OLD_MAYBE_ACTIVATE),
  build_activation_coupled: count(source, OLD_BUILD_ACTIVATION),
  payload_activation_cached: count(source, OLD_PAYLOAD_ACTIVATION),
  handler_shared_only: count(source, OLD_HANDLER),
  test_export: count(source, OLD_TEST_EXPORT),
};
console.log('Step1072.9.32.10 RED', JSON.stringify(red));
for (const [key, value] of Object.entries(red)) {
  if (value !== 1) throw new Error(`Step1072.9.32.10 RED baseline mismatch ${key}=${value}`);
}

source = replaceOnce(
  source,
  OLD_VERSION,
  `import { getMarketLightInternalSnapshot } from './market-light-snapshot.mjs';\n\nconst VERSION = '650.8.15.163.1';`,
  'version_import',
);
source = replaceOnce(
  source,
  OLD_CALL_RPC_SIG,
  "async function callRpc(name, body, signal, timeoutMs = 12_000, bearerToken = '') {",
  'call_rpc_signature',
);
source = replaceOnce(
  source,
  OLD_AUTH_HEADER,
  "        authorization: `Bearer ${String(bearerToken || '').trim() || SUPABASE_SERVICE_ROLE_KEY}`,",
  'activation_bearer_forward',
);

const NEW_MAYBE_ACTIVATE = `function requestBearer(req) {
  const raw = String(req?.headers?.authorization || '').trim();
  const match = raw.match(/^Bearer\\s+(.+)$/i);
  return match ? String(match[1] || '').trim() : '';
}

function activationCooldownKey(provider, symbol, bearerToken = '') {
  const authClass = String(bearerToken || '').trim() ? 'auth' : 'anon';
  return \`${'${provider}:${symbol}:${authClass}'}\`;
}

function gateMarketLightExactIdentity(symbol) {
  const targetSymbol = symbolKey(symbol);
  let snapshot;
  try {
    snapshot = getMarketLightInternalSnapshot({ market: 'spot', provider: 'gate' });
  } catch (error) {
    return {
      ready: false,
      verified: false,
      reason: 'gate_shared_market_light_exception',
      error: String(error?.message || error),
    };
  }
  const rows = Array.isArray(snapshot?.rows) ? snapshot.rows : [];
  if (snapshot?.ok !== true || rows.length === 0) {
    return {
      ready: false,
      verified: false,
      reason: 'gate_shared_market_light_not_ready',
      row_count: rows.length,
    };
  }
  const row = rows.find((item) =>
    symbolKey(item?.symbol ?? item?.native_symbol) === targetSymbol,
  );
  if (!row) {
    return {
      ready: true,
      verified: false,
      reason: 'gate_exact_symbol_not_in_shared_spot_rows',
      row_count: rows.length,
    };
  }
  const rowProvider = providerKey(row?.provider || 'gate');
  const rowMarket = String(row?.market_type || 'spot').trim().toLowerCase();
  const reality = String(row?.is_reality ?? '').trim().toLowerCase();
  const explicitReality = ['1', 'true', 'yes'].includes(reality);
  const verified = rowProvider === 'gate' && rowMarket === 'spot' && !explicitReality;
  return {
    ready: true,
    verified,
    reason: verified
      ? 'gate_exact_symbol_verified_by_shared_market_light'
      : 'gate_shared_row_not_crypto_spot',
    row_count: rows.length,
    source: String(row?.source || ''),
  };
}

async function maybeActivate(provider, symbol, signal, bearerToken = '') {
  const bearer = String(bearerToken || '').trim();
  const authClass = bearer ? 'auth' : 'anon';
  const key = activationCooldownKey(provider, symbol, bearer);
  const now = Date.now();
  const previous = Number(activatedAt.get(key) || 0);
  if (now - previous < ACTIVATE_TTL_MS) {
    return {
      attempted: false,
      ok: true,
      cache_state: 'activation_cooldown',
      auth_class: authClass,
    };
  }
  stats.activation_calls += 1;
  try {
    const raw = await callRpc('app_activate_spot_trade_flow', {
      p_provider: provider,
      p_symbol: symbol,
    }, signal, 10_000, bearer);
    let result = unwrapPayload(raw) || raw;

    if (
      provider === 'gate' &&
      bearer &&
      result?.authenticated_request === true &&
      result?.activated !== true &&
      result?.will_collect !== true
    ) {
      const gateIdentity = gateMarketLightExactIdentity(symbol);
      if (gateIdentity.verified === true) {
        const gateRaw = await callRpc(
          'app_activate_spot_trade_flow_backend_verified_gate',
          { p_symbol: symbol },
          signal,
          10_000,
        );
        const gateResult = unwrapPayload(gateRaw) || gateRaw;
        result = {
          ...gateResult,
          user_auth_verified: true,
          gate_shared_identity_verified: true,
          gate_shared_identity_source: gateIdentity.source,
          gate_shared_identity_row_count: gateIdentity.row_count,
        };
      } else {
        result = {
          ...result,
          gate_shared_identity_verified: false,
          gate_shared_identity_ready: gateIdentity.ready === true,
          gate_shared_identity_reason: gateIdentity.reason,
          gate_shared_identity_row_count: gateIdentity.row_count ?? 0,
        };
      }
    }

    const gateNotReady =
      provider === 'gate' &&
      result?.gate_shared_identity_ready === false;
    if (!gateNotReady) activatedAt.set(key, Date.now());
    stats.activation_succeeded += 1;
    return {
      attempted: true,
      ok: true,
      cache_state: result?.activated === true
        ? 'activated'
        : 'activation_checked',
      auth_class: authClass,
      result,
    };
  } catch (error) {
    stats.activation_failed += 1;
    return {
      attempted: true,
      ok: false,
      cache_state: 'activation_failed_but_snapshot_read_continues',
      auth_class: authClass,
      error: String(error?.message || error),
    };
  }
}`;

source = replaceOnce(source, OLD_MAYBE_ACTIVATE, NEW_MAYBE_ACTIVATE, 'maybe_activate');
source = replaceOnce(
  source,
  OLD_BUILD_ACTIVATION,
  `async function buildPayload(provider, symbol, signal) {\n  const settled = await Promise.allSettled([`,
  'build_activation_decouple',
);
source = replaceOnce(source, OLD_PAYLOAD_ACTIVATION, '', 'cached_activation_remove');
source = replaceOnce(
  source,
  OLD_HANDLER,
  `  try {\n    const activation = await maybeActivate(\n      provider,\n      symbol,\n      signal,\n      requestBearer(req),\n    );\n    const payload = await getSharedSnapshot(provider, symbol, signal);\n    sendJson(res, 200, { ...payload, activation });`,
  'handler_activation_per_request',
);

const HEALTH_ANCHOR = "    activation_ttl_seconds: Math.round(ACTIVATE_TTL_MS / 1000),\n";
source = replaceOnce(
  source,
  HEALTH_ANCHOR,
  HEALTH_ANCHOR +
    "    activation_auth_forwarding: 'incoming_user_bearer_only_for_activation_rpc',\n" +
    "    activation_cache_key_scope: 'provider_symbol_auth_class',\n" +
    "    anonymous_activation_cannot_block_authenticated_activation: true,\n" +
    "    shared_snapshot_cache_contains_user_auth: false,\n" +
    "    gate_activation_identity_source: 'shared_market_light_internal_gate_spot_rows',\n" +
    "    gate_activation_user_read_starts_exchange_requests: false,\n",
  'health_activation_contract',
);

source = replaceOnce(
  source,
  OLD_TEST_EXPORT,
  `export const _test = {\n  providerKey,\n  symbolKey,\n  unwrapPayload,\n  payloadOk,\n  requestBearer,\n  activationCooldownKey,\n  gateMarketLightExactIdentity,\n};`,
  'test_exports',
);

const green = {
  version: count(source, "const VERSION = '650.8.15.163.1';"),
  market_light_import: count(source, "import { getMarketLightInternalSnapshot } from './market-light-snapshot.mjs';"),
  user_bearer_rpc: count(source, "authorization: `Bearer ${String(bearerToken || '').trim() || SUPABASE_SERVICE_ROLE_KEY}`"),
  gate_backend_rpc: count(source, 'app_activate_spot_trade_flow_backend_verified_gate'),
  auth_lane: count(source, "const authClass = bearer ? 'auth' : 'anon';"),
  cached_activation_old: count(source, OLD_BUILD_ACTIVATION),
  payload_activation_old: count(source, OLD_PAYLOAD_ACTIVATION),
  request_bearer: count(source, 'function requestBearer(req)'),
  gate_identity: count(source, 'function gateMarketLightExactIdentity(symbol)'),
};
console.log('Step1072.9.32.10 GREEN', JSON.stringify(green));
if (
  green.version !== 1 ||
  green.market_light_import !== 1 ||
  green.user_bearer_rpc !== 1 ||
  green.gate_backend_rpc !== 1 ||
  green.auth_lane !== 1 ||
  green.cached_activation_old !== 0 ||
  green.payload_activation_old !== 0 ||
  green.request_bearer !== 1 ||
  green.gate_identity !== 1
) {
  throw new Error('Step1072.9.32.10 GREEN verification failed');
}

fs.writeFileSync(target, source, 'utf8');
console.log('Step1072.9.32.10 BUILD_PATCH_PASS');

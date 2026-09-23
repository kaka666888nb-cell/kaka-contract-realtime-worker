import assert from 'node:assert/strict';
import fs from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';
import { gzipSync } from 'node:zlib';

const supabaseUrl = 'https://project-ref.supabase.co';
const serviceRole = 'test-service-role-key';
const snapshotRpcPath = '/rest/v1/rpc/app_upsert_market_backend_snapshots_diff';
const bybitChunkRpcPath = '/rest/v1/rpc/app_upsert_bybit_second_history_chunks';
const allowedTablePath = '/rest/v1/app_airdrop_events?on_conflict=event_key';
const unknownRpcPath = '/rest/v1/rpc/not_approved_for_egress_proxy';
const calls = [];
let handler = null;

const originalDeno = globalThis.Deno;
const originalFetch = globalThis.fetch;

globalThis.Deno = {
  env: {
    get(name) {
      if (name === 'SUPABASE_URL') return supabaseUrl;
      if (name === 'SUPABASE_SERVICE_ROLE_KEY') return serviceRole;
      return '';
    },
  },
  serve(nextHandler) {
    handler = nextHandler;
  },
};

globalThis.fetch = async (input, init = {}) => {
  const url = input instanceof Request ? input.url : String(input);
  let body = Buffer.alloc(0);
  if (typeof init.body === 'string') body = Buffer.from(init.body);
  else if (init.body instanceof Uint8Array) body = Buffer.from(init.body);
  else if (init.body instanceof ArrayBuffer) body = Buffer.from(init.body);
  calls.push({ url, init, body, headers: new Headers(init.headers) });

  if (url === `${supabaseUrl}/rest/v1/rpc/kaka_verify_render_egress_service_role`) {
    return new Response('true', { status: 200, headers: { 'content-type': 'application/json' } });
  }
  if (url === `${supabaseUrl}${snapshotRpcPath}` || url === `${supabaseUrl}${bybitChunkRpcPath}`) {
    return new Response(JSON.stringify({ accepted: true }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  }
  if (url === `${supabaseUrl}${allowedTablePath}`) {
    return new Response('', { status: 201 });
  }
  throw new Error(`unexpected fetch: ${url}`);
};

function compressedRequest(targetPath, rawBody) {
  const compressed = gzipSync(rawBody, { level: 6 });
  return new Request(`${supabaseUrl}/functions/v1/kaka-render-egress-ingest`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${serviceRole}`,
      apikey: serviceRole,
      'x-kaka-caller-key': serviceRole,
      'x-kaka-compression': 'gzip',
      'x-kaka-target': encodeURIComponent(targetPath),
      'x-kaka-original-method': 'POST',
      'x-kaka-original-content-type': 'application/json',
      'x-kaka-raw-bytes': String(rawBody.length),
    },
    body: compressed,
  });
}

try {
  let source = fs.readFileSync(
    new URL('../supabase/functions/kaka-render-egress-ingest/index.ts', import.meta.url),
    'utf8',
  );
  source = source.replace(/^import "jsr:[^"]+";\r?\n/, '');
  const runnableSource = stripTypeScriptTypes(source, { mode: 'transform' });
  await import(`data:text/javascript;base64,${Buffer.from(runnableSource).toString('base64')}`);
  assert.equal(typeof handler, 'function', 'Edge Function must register a request handler');

  const rawBody = Buffer.from(JSON.stringify({
    p_rows: [{
      provider: 'binance',
      snapshot_type: 'tickers',
      payload: { rows: [{ symbol: 'BTCUSDT', price: '60000.1' }] },
    }],
  }));
  const accepted = await handler(compressedRequest(snapshotRpcPath, rawBody));
  assert.equal(accepted.status, 200);
  assert.equal(accepted.headers.get('x-kaka-render-egress-proxy'), 'ok');
  assert.equal(calls.length, 2);
  assert.equal(calls[1].url, `${supabaseUrl}${snapshotRpcPath}`);
  assert.equal(calls[1].init.method, 'POST');
  assert.equal(calls[1].body.toString('utf8'), rawBody.toString('utf8'));
  assert.equal(calls[1].headers.get('authorization'), `Bearer ${serviceRole}`);

  const acceptedBybitChunks = await handler(compressedRequest(bybitChunkRpcPath, rawBody));
  assert.equal(acceptedBybitChunks.status, 200);
  assert.equal(calls.length, 3);
  assert.equal(calls[2].url, `${supabaseUrl}${bybitChunkRpcPath}`);

  const acceptedTable = await handler(compressedRequest(allowedTablePath, rawBody));
  assert.equal(acceptedTable.status, 201, 'existing allowlisted table writes must remain supported');
  assert.equal(calls.length, 4);
  assert.equal(calls[3].url, `${supabaseUrl}${allowedTablePath}`);

  const callsBeforeUnknown = calls.length;
  const rejected = await handler(compressedRequest(unknownRpcPath, rawBody));
  assert.equal(rejected.status, 403);
  assert.equal(calls.length, callsBeforeUnknown, 'unapproved RPC must not reach Supabase');

  console.log('PASS Step1073 V101 egress ingest accepts only approved large-write RPCs');
} finally {
  globalThis.Deno = originalDeno;
  globalThis.fetch = originalFetch;
}

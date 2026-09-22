import assert from 'node:assert/strict';
import { gunzipSync } from 'node:zlib';

const supabaseUrl = 'https://project-ref.supabase.co';
const snapshotRpcPath = '/rest/v1/rpc/app_upsert_market_backend_snapshots_diff';
const snapshotRpcUrl = `${supabaseUrl}${snapshotRpcPath}`;
const unknownRpcUrl = `${supabaseUrl}/rest/v1/rpc/not_approved_for_egress_proxy`;

process.env.SUPABASE_URL = supabaseUrl;
process.env.SUPABASE_SERVICE_ROLE_KEY = 'test-service-role-key';
process.env.KAKA_RENDER_SUPABASE_PROXY_MIN_BYTES = String(8 * 1024);

const originalFetch = globalThis.fetch;
const calls = [];

globalThis.fetch = async (input, init = {}) => {
  const url = input instanceof Request ? input.url : String(input);
  const body = init.body == null ? Buffer.alloc(0) : Buffer.from(init.body);
  calls.push({ url, init, body, headers: new Headers(init.headers) });
  return new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: {
      'content-type': 'application/json',
      'x-kaka-render-egress-proxy': 'ok',
    },
  });
};

try {
  const { installRenderSupabaseEgressProxy } = await import(
    `../src/render-supabase-egress-proxy.mjs?step1073-v101=${Date.now()}`
  );
  installRenderSupabaseEgressProxy();

  const rawBody = JSON.stringify({
    p_rows: [{
      provider: 'binance',
      market_type: 'contract',
      snapshot_type: 'tickers',
      payload: {
        rows: Array.from({ length: 1400 }, (_, index) => ({
          symbol: `ASSET${index}USDT`,
          price: `${60000 + index}.12345678`,
          source_time: '2026-09-22T22:00:00.000Z',
        })),
      },
    }],
  });
  assert.ok(Buffer.byteLength(rawBody) > 32 * 1024, 'fixture must exercise the large-body path');

  const response = await globalThis.fetch(snapshotRpcUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: rawBody,
  });
  assert.equal(response.ok, true);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, `${supabaseUrl}/functions/v1/kaka-render-egress-ingest`);
  assert.equal(calls[0].headers.get('x-kaka-compression'), 'gzip');
  assert.equal(calls[0].headers.get('x-kaka-original-method'), 'POST');
  assert.equal(calls[0].headers.get('x-kaka-target'), encodeURIComponent(snapshotRpcPath));
  assert.equal(Number(calls[0].headers.get('x-kaka-raw-bytes')), Buffer.byteLength(rawBody));
  assert.equal(gunzipSync(calls[0].body).toString('utf8'), rawBody);
  assert.ok(calls[0].body.length < Buffer.byteLength(rawBody));

  calls.length = 0;
  await globalThis.fetch(unknownRpcUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: rawBody,
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, unknownRpcUrl, 'unapproved RPCs must bypass the privileged gzip proxy');

  console.log('PASS Step1073 V101 snapshot diff RPC uses the bounded gzip proxy');
} finally {
  globalThis.fetch = originalFetch;
  delete process.env.SUPABASE_URL;
  delete process.env.SUPABASE_SERVICE_ROLE_KEY;
  delete process.env.KAKA_RENDER_SUPABASE_PROXY_MIN_BYTES;
}

import assert from 'node:assert/strict';
import fs from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';

const workerBase = 'https://kaka-contract-realtime-worker.onrender.com';
const fetched = [];
let handler = null;

const originalDeno = globalThis.Deno;
const originalFetch = globalThis.fetch;
const originalServe = globalThis.__testServe;
const originalCreateClient = globalThis.__testCreateClient;

globalThis.Deno = {
  env: {
    get(name) {
      if (name === 'SUPABASE_URL') return 'https://project-ref.supabase.co';
      if (name === 'SUPABASE_SERVICE_ROLE_KEY') return 'test-service-role-key';
      if (name === 'KAKA_SYNC_SECRET') return 'test-sync-secret';
      if (name === 'KAKA_RENDER_WORKER_HEALTH_URL') return `${workerBase}/health`;
      return '';
    },
  },
};
globalThis.__testServe = (nextHandler) => { handler = nextHandler; };
globalThis.__testCreateClient = () => ({
  async rpc(name, params) {
    if (name === 'kaka_edge_capacity_dimensions_alert_decide') {
      assert.equal(params.p_dry_run, true);
      assert.equal(params.p_dimensions.length, 2);
      assert.equal(params.p_dimensions[0].runtime_id, 'runtime-test');
      return { data: { notify: false, level: 'normal', dimension: 'realtime_ws_clients' }, error: null };
    }
    if (name === 'app_get_resend_platform_budget_status') {
      return { data: { allowed: true }, error: null };
    }
    throw new Error(`unexpected rpc: ${name}`);
  },
});
globalThis.fetch = async (input) => {
  const url = input instanceof Request ? input.url : String(input);
  fetched.push(url);
  return new Response(JSON.stringify({
    ok: true,
    runtime_id: 'runtime-test',
    schema: 'step1073_v101_capacity_health_v1',
    dimensions: [
      { id: 'realtime_ws_clients', label: 'Kline WebSocket clients', used: 37, limit: 1000, percent: 3.7, rejected: 3 },
      { id: 'depth_stream_keys', label: 'Depth exact keys', used: 5, limit: 96, percent: 5.21, rejected: 0 },
    ],
    highest_utilization: { id: 'depth_stream_keys', used: 5, limit: 96, percent: 5.21 },
    total_rejected_capacity: 3,
  }), { status: 200, headers: { 'content-type': 'application/json' } });
};

try {
  let source = fs.readFileSync(
    new URL('../supabase/functions/kaka-admin-capacity-alert/index.ts', import.meta.url),
    'utf8',
  );
  source = source.replace(/^import .*;\r?\n/gm, '');
  source = [
    'const serve = globalThis.__testServe;',
    'const createClient = globalThis.__testCreateClient;',
    source,
  ].join('\n');
  const runnableSource = stripTypeScriptTypes(source, { mode: 'transform' });
  await import(`data:text/javascript;base64,${Buffer.from(runnableSource).toString('base64')}`);
  assert.equal(typeof handler, 'function');

  const response = await handler(new Request('https://project-ref.supabase.co/functions/v1/kaka-admin-capacity-alert', {
    method: 'POST',
    headers: { 'x-kaka-sync-secret': 'test-sync-secret', 'content-type': 'application/json' },
    body: JSON.stringify({ dry_run: true }),
  }));
  const body = await response.json();
  assert.equal(response.status, 200);
  assert.deepEqual(fetched, [`${workerBase}/api/capacity-health`]);
  assert.equal(body.highest_utilization.id, 'depth_stream_keys');
  assert.equal(body.dimensions.length, 2);
  assert.equal(body.rejected_capacity, 3);

  console.log('PASS Step1073 V101 capacity monitor covers all compact capacity dimensions');
} finally {
  globalThis.Deno = originalDeno;
  globalThis.fetch = originalFetch;
  globalThis.__testServe = originalServe;
  globalThis.__testCreateClient = originalCreateClient;
}

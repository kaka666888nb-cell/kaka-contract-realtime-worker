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
  async rpc(name) {
    if (name === 'kaka_edge_capacity_alert_decide') {
      return { data: { notify: false, level: 'normal' }, error: null };
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
    binance_shared_ws: {
      total_clients: 37,
      max_total_clients: 1000,
      rejected_capacity: 2,
      downstream_ip_capacity_rejections: 1,
    },
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
  assert.deepEqual(fetched, [`${workerBase}/api/realtime-ws-health`]);
  assert.equal(body.total_clients, 37);
  assert.equal(body.max_total_clients, 1000);
  assert.equal(body.rejected_capacity, 3);

  console.log('PASS Step1073 V101 capacity monitor uses compact realtime WS health');
} finally {
  globalThis.Deno = originalDeno;
  globalThis.fetch = originalFetch;
  globalThis.__testServe = originalServe;
  globalThis.__testCreateClient = originalCreateClient;
}

import assert from 'node:assert/strict';
import fs from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';

const workerBase = 'https://kaka-contract-realtime-worker.onrender.com';
const fetched = [];
let handler = null;

const originalDeno = globalThis.Deno;
const originalFetch = globalThis.fetch;
const originalCreateClient = globalThis.__testCreateClient;

globalThis.Deno = {
  env: {
    get(name) {
      if (name === 'SUPABASE_URL') return 'https://project-ref.supabase.co';
      if (name === 'SUPABASE_SERVICE_ROLE_KEY') return 'test-service-role-key';
      if (name === 'KAKA_SYNC_SECRET') return 'test-sync-secret';
      return '';
    },
  },
  serve(nextHandler) { handler = nextHandler; },
};
globalThis.__testCreateClient = () => ({
  async rpc(name) {
    if (name === 'app_edge_get_ops_alert_config') {
      return {
        data: {
          enabled: true,
          worker_health_url: `${workerBase}/health`,
          worker_timeout_ms: 8000,
        },
        error: null,
      };
    }
    if (name === 'app_edge_collect_ops_alerts') {
      return { data: { collected: 0 }, error: null };
    }
    throw new Error(`unexpected rpc: ${name}`);
  },
});
globalThis.fetch = async (input) => {
  const url = input instanceof Request ? input.url : String(input);
  fetched.push(url);
  return new Response(JSON.stringify({ ok: true }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
};

try {
  let source = fs.readFileSync(
    new URL('../supabase/functions/kaka-ops-alert-dispatch/index.ts', import.meta.url),
    'utf8',
  );
  source = source.replace(/^import .*;\r?\n/gm, '');
  source = `const createClient = globalThis.__testCreateClient;\n${source}`;
  const runnableSource = stripTypeScriptTypes(source, { mode: 'transform' });
  await import(`data:text/javascript;base64,${Buffer.from(runnableSource).toString('base64')}`);
  assert.equal(typeof handler, 'function');

  const response = await handler(new Request(
    'https://project-ref.supabase.co/functions/v1/kaka-ops-alert-dispatch?health=1',
    { method: 'GET', headers: { 'x-kaka-sync-secret': 'test-sync-secret' } },
  ));
  const body = await response.json();
  assert.equal(response.status, 200);
  assert.deepEqual(fetched, [`${workerBase}/health/ready`]);
  assert.equal(body.worker.ok, true);
  assert.equal(body.worker.httpStatus, 200);

  console.log('PASS Step1073 V101 ops monitor uses the tiny readiness health endpoint');
} finally {
  globalThis.Deno = originalDeno;
  globalThis.fetch = originalFetch;
  globalThis.__testCreateClient = originalCreateClient;
}

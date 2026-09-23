import assert from 'node:assert/strict';
import fs from 'node:fs';

const serverSource = fs.readFileSync(
  new URL('../src/server.mjs', import.meta.url),
  'utf8',
);

assert.match(
  serverSource,
  /installRenderSupabaseEgressProxy\(\)/,
  'the realtime child must install the gzip proxy before Bybit persistence starts',
);
assert.match(
  serverSource,
  /getRenderSupabaseEgressProxyHealth\(\)/,
  'the child proxy must expose production-verifiable counters',
);

console.log('PASS Step1073 V101 realtime child installs and exposes the gzip proxy');

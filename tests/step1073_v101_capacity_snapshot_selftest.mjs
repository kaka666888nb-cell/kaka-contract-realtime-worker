import assert from 'node:assert/strict';

const { buildCapacitySnapshot } = await import(
  `../src/step1073-capacity-health.mjs?step1073-v101=${Date.now()}`
);

const snapshot = buildCapacitySnapshot({
  runtimeId: 'runtime-test',
  realtimeWs: {
    total_clients: 125,
    max_total_clients: 1000,
    active_streams: 55,
    max_streams: 64,
    rejected_capacity: 2,
    downstream_ip_capacity_rejections: 1,
  },
  marketLight: {
    client_count: 700,
    client_max: 1500,
    active_spec_count: 90,
    active_spec_max: 256,
    rejected_capacity: 3,
  },
  overlay: {
    client_count: 80,
    client_max: 1500,
    active_spec_count: 20,
    active_spec_max: 256,
    rejected_capacity: 0,
  },
  overlayNat: {
    active_connections_tracked: 80,
    real_ip_active_max: 1200,
    admission_queue_rejected: 4,
    real_ip_capacity_rejected: 1,
    real_ip_rate_rejected: 2,
  },
  depth: {
    client_count: 1400,
    client_max: 1500,
    active_key_count: 80,
    active_key_max: 96,
    rejected_capacity: 5,
    rejected_ip_capacity: 1,
  },
});

assert.equal(snapshot.ok, true);
assert.equal(snapshot.runtime_id, 'runtime-test');
assert.equal(snapshot.dimensions.length, 9);
assert.equal(snapshot.highest_utilization.id, 'depth_stream_clients');
assert.equal(snapshot.highest_utilization.percent, 93.33);
assert.equal(snapshot.total_rejected_capacity, 19);
assert.ok(snapshot.dimensions.every((item) => item.limit > 0));
assert.deepEqual(
  snapshot.dimensions.map((item) => item.id),
  [
    'realtime_ws_clients',
    'realtime_ws_streams',
    'market_stream_clients',
    'market_stream_specs',
    'overlay_stream_clients',
    'overlay_stream_specs',
    'overlay_nat_connections',
    'depth_stream_clients',
    'depth_stream_keys',
  ],
);

console.log('PASS Step1073 V101 capacity snapshot covers every bounded downstream dimension');

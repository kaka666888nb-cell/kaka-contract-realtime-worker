import http from 'node:http';
import { randomUUID } from 'node:crypto';

const VERSION = '1073.101.1';
const SCHEMA = 'step1073_v101_capacity_health_v1';
const HEALTH_ROUTE = '/api/capacity-health';
const RUNTIME_ID = `${process.pid}:${Date.now()}:${randomUUID()}`;

function finiteNonnegative(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.max(0, parsed) : 0;
}

function positiveLimit(value) {
  return Math.max(1, finiteNonnegative(value));
}

function sum(source, keys) {
  return keys.reduce(
    (total, key) => total + finiteNonnegative(source?.[key]),
    0,
  );
}

function dimension(id, label, usedValue, limitValue, rejectedValue = 0) {
  const used = finiteNonnegative(usedValue);
  const limit = positiveLimit(limitValue);
  return {
    id,
    label,
    used,
    limit,
    percent: Number(((used * 100) / limit).toFixed(2)),
    rejected: finiteNonnegative(rejectedValue),
  };
}

export function buildCapacitySnapshot({
  runtimeId = RUNTIME_ID,
  realtimeWs = {},
  marketLight = {},
  overlay = {},
  overlayNat = {},
  depth = {},
} = {}) {
  const realtimeRejected = sum(realtimeWs, [
    'rejected_capacity',
    'downstream_ip_capacity_rejections',
    'downstream_ip_rate_rejections',
    'connect_rate_rejections',
  ]);
  const marketRejected = sum(marketLight, [
    'rejected_capacity',
    'rejected_ip_capacity',
    'rejected_ip_rate',
  ]);
  const overlayRejected = sum(overlay, [
    'rejected_capacity',
    'rejected_ip_capacity',
    'rejected_ip_rate',
  ]);
  const overlayNatRejected = sum(overlayNat, [
    'admission_queue_rejected',
    'real_ip_capacity_rejected',
    'real_ip_rate_rejected',
  ]);
  const depthRejected = sum(depth, [
    'rejected_capacity',
    'rejected_ip_capacity',
    'rejected_ip_rate',
  ]);

  const dimensions = [
    dimension(
      'realtime_ws_clients',
      'Kline WebSocket 客户端',
      realtimeWs.total_clients,
      realtimeWs.max_total_clients,
      realtimeRejected,
    ),
    dimension(
      'realtime_ws_streams',
      'Kline 精确上游流',
      realtimeWs.active_streams,
      realtimeWs.max_streams,
    ),
    dimension(
      'market_stream_clients',
      '行情列表 SSE 客户端',
      marketLight.client_count,
      marketLight.client_max,
      marketRejected,
    ),
    dimension(
      'market_stream_specs',
      '行情列表精确身份',
      marketLight.active_spec_count,
      marketLight.active_spec_max,
    ),
    dimension(
      'overlay_stream_clients',
      '系统悬浮窗 SSE 客户端',
      overlay.client_count,
      overlay.client_max,
      overlayRejected,
    ),
    dimension(
      'overlay_stream_specs',
      '系统悬浮窗精确身份',
      overlay.active_spec_count,
      overlay.active_spec_max,
    ),
    dimension(
      'overlay_nat_connections',
      '单一真实 IP 悬浮窗连接',
      overlayNat.max_active_per_real_ip ?? overlayNat.active_connections_tracked,
      overlayNat.real_ip_active_max,
      overlayNatRejected,
    ),
    dimension(
      'depth_stream_clients',
      '深度 SSE 客户端',
      depth.client_count,
      depth.client_max,
      depthRejected,
    ),
    dimension(
      'depth_stream_keys',
      '深度精确身份',
      depth.active_key_count,
      depth.active_key_max,
    ),
  ];
  const highest = dimensions.reduce(
    (best, current) => (
      !best || current.percent > best.percent ? current : best
    ),
    null,
  );
  return {
    ok: true,
    version: VERSION,
    schema: SCHEMA,
    runtime_id: String(runtimeId || RUNTIME_ID),
    dimensions,
    highest_utilization: highest,
    total_rejected_capacity:
      realtimeRejected +
      marketRejected +
      overlayRejected +
      overlayNatRejected +
      depthRejected,
    thresholds: {
      warning_percent: 70,
      high_percent: 85,
      critical_percent: 95,
      new_capacity_rejection: 'critical',
    },
    now: new Date().toISOString(),
  };
}

function sendJson(res, status, payload) {
  const body = Buffer.from(JSON.stringify(payload));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': String(body.length),
  });
  res.end(body);
}

export function installStep1073CapacityHealth({
  childPort,
  getMarketLightHealth,
  getOverlayHealth,
  getOverlayNatHealth,
  getDepthHealth,
}) {
  if (http.createServer.__kakaStep1073CapacityHealthWrapped) return;
  const previousCreateServer = http.createServer.bind(http);
  function patchedCreateServer(listener, ...rest) {
    return previousCreateServer(async (req, res) => {
      let pathname = '/';
      try {
        pathname = new URL(req.url || '/', 'http://127.0.0.1').pathname;
      } catch (_) {}
      if (pathname !== HEALTH_ROUTE) return await listener(req, res);
      if (String(req.method || 'GET').toUpperCase() !== 'GET') {
        sendJson(res, 405, { ok: false, version: VERSION, schema: SCHEMA, error: 'method_not_allowed' });
        return;
      }

      try {
        const response = await fetch(
          `http://127.0.0.1:${Number(childPort || 10001)}/internal/capacity-health`,
          {
            headers: { accept: 'application/json' },
            signal: AbortSignal.timeout(4_000),
          },
        );
        const child = await response.json();
        if (!response.ok || child?.ok !== true || !child?.binance_shared_ws) {
          throw new Error(`realtime_child_capacity_http_${response.status}`);
        }
        const payload = buildCapacitySnapshot({
          realtimeWs: child.binance_shared_ws,
          marketLight: getMarketLightHealth(),
          overlay: getOverlayHealth(),
          overlayNat: getOverlayNatHealth(),
          depth: getDepthHealth(),
        });
        sendJson(res, 200, payload);
      } catch (error) {
        sendJson(res, 503, {
          ok: false,
          version: VERSION,
          schema: SCHEMA,
          runtime_id: RUNTIME_ID,
          error: String(error?.message || error).slice(0, 240),
        });
      }
    }, ...rest);
  }
  patchedCreateServer.__kakaStep1073CapacityHealthWrapped = true;
  http.createServer = patchedCreateServer;
}

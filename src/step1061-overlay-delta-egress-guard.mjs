import http from 'node:http';

// Step1061.9.1: downstream delta fan-out for the system market overlay.
// The overlay stream already shares upstream collection across users, but the old
// downstream payload resent every selected row whenever any row/timestamp changed.
// This guard keeps one tiny semantic state per response and forwards only rows whose
// effective visible values changed. Timestamp/source-only churn is suppressed.

const INSTALL_KEY = Symbol.for('kaka.step1061.overlay.delta.egress.guard.v1');
const RESPONSE_STATE = new WeakMap();

const state = globalThis[INSTALL_KEY] || {
  installed: false,
  tickerWritesSeen: 0,
  tickerWritesForwarded: 0,
  tickerWritesSuppressed: 0,
  itemsSeen: 0,
  itemsForwarded: 0,
  itemsSuppressed: 0,
  parseErrors: 0,
};
globalThis[INSTALL_KEY] = state;

function finite(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function positive(value) {
  const n = finite(value);
  return n != null && n > 0 ? n : null;
}

function responseMap(res) {
  let map = RESPONSE_STATE.get(res);
  if (!map) {
    map = new Map();
    RESPONSE_STATE.set(res, map);
  }
  return map;
}

function effectiveState(item, previous = null) {
  const row = item?.row && typeof item.row === 'object' ? item.row : {};
  const p = previous || { price: null, mark: null, change: null };

  const hasPrice = Object.prototype.hasOwnProperty.call(row, 'price');
  const hasMark = Object.prototype.hasOwnProperty.call(row, 'mark_price');
  const hasChange = Object.prototype.hasOwnProperty.call(row, 'change_percent_24h');

  const nextPrice = hasPrice ? (positive(row.price) ?? p.price ?? null) : (p.price ?? null);
  const nextMark = hasMark ? (positive(row.mark_price) ?? p.mark ?? null) : (p.mark ?? null);
  const parsedChange = hasChange ? finite(row.change_percent_24h) : null;
  const nextChange = parsedChange != null ? parsedChange : (p.change ?? null);

  return {
    price: nextPrice,
    mark: nextMark,
    change: nextChange,
  };
}

function fingerprint(spec, effective) {
  return JSON.stringify([
    String(spec || ''),
    effective.price,
    effective.mark,
    effective.change,
  ]);
}

function deltaTickerChunk(res, chunk) {
  const input = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk ?? '');
  if (!input.includes('event: ticker\n') || !input.includes('\ndata: ')) return null;

  const marker = '\ndata: ';
  const dataStart = input.indexOf(marker);
  if (dataStart < 0) return null;
  const jsonStart = dataStart + marker.length;
  const jsonEnd = input.indexOf('\n\n', jsonStart);
  if (jsonEnd < 0) return null;

  try {
    const payload = JSON.parse(input.slice(jsonStart, jsonEnd));
    if (!payload || !Array.isArray(payload.items)) return null;

    state.tickerWritesSeen += 1;
    state.itemsSeen += payload.items.length;

    const sent = responseMap(res);
    const changed = [];

    for (const item of payload.items) {
      const spec = String(item?.spec || '').trim();
      if (!spec) {
        changed.push(item);
        continue;
      }

      const previous = sent.get(spec) || null;
      const effective = effectiveState(item, previous);
      const nextFingerprint = fingerprint(spec, effective);
      if (previous?.fingerprint === nextFingerprint) {
        state.itemsSuppressed += 1;
        continue;
      }

      sent.set(spec, { ...effective, fingerprint: nextFingerprint });
      changed.push(item);
      state.itemsForwarded += 1;
    }

    if (!changed.length) {
      state.tickerWritesSuppressed += 1;
      return '';
    }

    payload.items = changed;
    payload.delta_only = true;
    payload.delta_changed_items = changed.length;
    payload.semantic_fingerprint_excludes_timestamp = true;
    payload.downstream_delta_guard_version = '1061.9.1';
    state.tickerWritesForwarded += 1;

    return `${input.slice(0, jsonStart)}${JSON.stringify(payload)}${input.slice(jsonEnd)}`;
  } catch (_) {
    state.parseErrors += 1;
    return null;
  }
}

export function installOverlayDeltaEgressGuard() {
  if (state.installed) return;
  state.installed = true;

  const originalCreateServer = http.createServer.bind(http);
  function patchedCreateServer(listener, ...rest) {
    const wrappedListener = async (req, res) => {
      const originalWrite = res.write.bind(res);
      res.write = function kakaOverlayDeltaWrite(chunk, encoding, callback) {
        try {
          const schema = String(res.getHeader?.('x-kaka-stream-schema') || '');
          if (schema.startsWith('step1061_') && schema.includes('all_market_overlay')) {
            const delta = deltaTickerChunk(res, chunk);
            if (delta === '') return true;
            if (delta != null) return originalWrite(delta, encoding, callback);
          }
        } catch (_) {
          state.parseErrors += 1;
        }
        return originalWrite(chunk, encoding, callback);
      };
      return await listener(req, res);
    };
    return originalCreateServer(wrappedListener, ...rest);
  }
  patchedCreateServer.__kakaOverlayDeltaWrapped = true;
  http.createServer = patchedCreateServer;

  console.log('[Step1061.9.1] overlay semantic delta egress guard installed');
}

export function getOverlayDeltaEgressGuardHealth() {
  return {
    ok: true,
    version: '1061.9.1',
    schema: 'step1061_9_1_overlay_semantic_delta_egress_guard_v1',
    scope: ['spot', 'contract', 'asset', 'onchain'],
    delta_only: true,
    semantic_fingerprint_excludes_timestamp: true,
    ...state,
  };
}

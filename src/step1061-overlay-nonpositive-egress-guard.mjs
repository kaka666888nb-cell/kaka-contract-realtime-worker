import http from 'node:http';

// Step1061.8.1: final downstream invariant for the system market overlay.
// A missing/zero/negative price is a bad snapshot, never a real market price.
// Preserve the last positive price per exact stream spec across spot, contract,
// exchange-assets (stocks/RWA/etc.) and on-chain rows. This guard runs only on
// the Step1061 overlay SSE response and does not touch other HTTP/SSE traffic.

const INSTALL_KEY = Symbol.for('kaka.step1061.overlay.nonpositive.egress.guard.v1');
const MAX_SPECS = 2048;
const TTL_MS = 30 * 60 * 1000;
const CLEAN_INTERVAL_MS = 60 * 1000;

const state = globalThis[INSTALL_KEY] || {
  installed: false,
  lastPositiveBySpec: new Map(),
  ignoredNonpositive: 0,
  preservedLastPositive: 0,
  firstBadPriceDropped: 0,
  parseErrors: 0,
  lastCleanAt: 0,
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

function cleanCache(now = Date.now()) {
  if (now - state.lastCleanAt < CLEAN_INTERVAL_MS && state.lastPositiveBySpec.size <= MAX_SPECS) return;
  state.lastCleanAt = now;
  for (const [spec, entry] of state.lastPositiveBySpec) {
    if (!entry || now - Number(entry.updatedAt || 0) > TTL_MS) {
      state.lastPositiveBySpec.delete(spec);
    }
  }
  if (state.lastPositiveBySpec.size <= MAX_SPECS) return;
  const overflow = state.lastPositiveBySpec.size - MAX_SPECS;
  const oldest = [...state.lastPositiveBySpec.entries()]
    .sort((a, b) => Number(a[1]?.updatedAt || 0) - Number(b[1]?.updatedAt || 0))
    .slice(0, overflow);
  for (const [spec] of oldest) state.lastPositiveBySpec.delete(spec);
}

function sanitizeItem(item, now) {
  if (!item || typeof item !== 'object') return item;
  const spec = String(item.spec || '').trim();
  const row = item.row && typeof item.row === 'object' ? { ...item.row } : null;
  if (!spec || !row) return item;

  const previous = state.lastPositiveBySpec.get(spec) || { price: null, markPrice: null, updatedAt: 0 };
  const rawPricePresent = Object.prototype.hasOwnProperty.call(row, 'price');
  const rawPrice = rawPricePresent ? finite(row.price) : null;
  const currentPrice = rawPricePresent ? positive(row.price) : null;

  if (currentPrice != null) {
    previous.price = currentPrice;
    previous.updatedAt = now;
    row.price = currentPrice;
  } else if (rawPricePresent) {
    if (rawPrice == null || rawPrice <= 0) state.ignoredNonpositive += 1;
    if (positive(previous.price) != null) {
      row.price = previous.price;
      state.preservedLastPositive += 1;
    } else {
      delete row.price;
      state.firstBadPriceDropped += 1;
    }
  } else if (positive(previous.price) != null) {
    row.price = previous.price;
    state.preservedLastPositive += 1;
  }

  const rawMarkPresent = Object.prototype.hasOwnProperty.call(row, 'mark_price');
  const rawMark = rawMarkPresent ? finite(row.mark_price) : null;
  const currentMark = rawMarkPresent ? positive(row.mark_price) : null;
  if (currentMark != null) {
    previous.markPrice = currentMark;
    previous.updatedAt = now;
    row.mark_price = currentMark;
  } else if (rawMarkPresent) {
    if (rawMark == null || rawMark <= 0) state.ignoredNonpositive += 1;
    if (positive(previous.markPrice) != null) {
      row.mark_price = previous.markPrice;
      state.preservedLastPositive += 1;
    } else {
      delete row.mark_price;
    }
  } else if (positive(previous.markPrice) != null) {
    row.mark_price = previous.markPrice;
  }

  if (positive(previous.price) != null || positive(previous.markPrice) != null) {
    previous.updatedAt = now;
    state.lastPositiveBySpec.set(spec, previous);
  }

  return { ...item, row };
}

function sanitizeTickerChunk(chunk) {
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
    const now = Date.now();
    cleanCache(now);
    payload.items = payload.items.map((item) => sanitizeItem(item, now));
    payload.nonpositive_price_overwrite = false;
    payload.last_positive_price_preserved = true;
    payload.zero_price_guard_version = '1061.8.1';
    return `${input.slice(0, jsonStart)}${JSON.stringify(payload)}${input.slice(jsonEnd)}`;
  } catch (_) {
    state.parseErrors += 1;
    return null;
  }
}

export function installOverlayNonpositiveEgressGuard() {
  if (state.installed) return;
  state.installed = true;

  const originalWrite = http.ServerResponse.prototype.write;
  http.ServerResponse.prototype.write = function kakaOverlayNonpositiveGuardWrite(chunk, encoding, callback) {
    try {
      const schema = String(this.getHeader?.('x-kaka-stream-schema') || '');
      if (schema.startsWith('step1061_') && schema.includes('all_market_overlay')) {
        const sanitized = sanitizeTickerChunk(chunk);
        if (sanitized != null) {
          return originalWrite.call(this, sanitized, encoding, callback);
        }
      }
    } catch (_) {
      state.parseErrors += 1;
    }
    return originalWrite.call(this, chunk, encoding, callback);
  };

  console.log('[Step1061.8.1] overlay all-market nonpositive egress guard installed');
}

export function getOverlayNonpositiveEgressGuardHealth() {
  cleanCache();
  return {
    ok: true,
    version: '1061.8.1',
    schema: 'step1061_8_1_overlay_nonpositive_egress_guard_v1',
    scope: ['spot', 'contract', 'asset', 'onchain'],
    nonpositive_price_overwrite: false,
    last_positive_price_preserved: true,
    cached_specs: state.lastPositiveBySpec.size,
    ignored_nonpositive: state.ignoredNonpositive,
    preserved_last_positive: state.preservedLastPositive,
    first_bad_price_dropped: state.firstBadPriceDropped,
    parse_errors: state.parseErrors,
  };
}

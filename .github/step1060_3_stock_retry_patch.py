from pathlib import Path

p = Path('src/stock-catalog-v2.mjs')
s = p.read_text(encoding='utf-8')

old_const = "const START_DELAY_MS = Math.max(15_000, Number(process.env.KAKA_STOCK_CATALOG_START_DELAY_MS || 75_000));\nconst FETCH_TIMEOUT_MS = Math.max(8_000, Number(process.env.KAKA_STOCK_CATALOG_FETCH_TIMEOUT_MS || 20_000));"
new_const = """const START_DELAY_MS = Math.max(15_000, Number(process.env.KAKA_STOCK_CATALOG_START_DELAY_MS || 75_000));
// Step1060.3: a transient provider failure must not make every Render deploy restage
// the entire already-healthy stock catalog. Keep fast recovery only for genuinely
// incomplete/integrity-broken committed catalogs; otherwise back off failed retries
// across process restarts.
const FAILURE_RETRY_BACKOFF_MS = Math.max(
  15 * 60_000,
  Math.min(6 * 60 * 60_000, Number(process.env.KAKA_STOCK_CATALOG_FAILURE_RETRY_BACKOFF_MS || 60 * 60_000) || 60 * 60_000),
);
const FETCH_TIMEOUT_MS = Math.max(8_000, Number(process.env.KAKA_STOCK_CATALOG_FETCH_TIMEOUT_MS || 20_000));"""
if s.count(old_const) != 1:
    raise SystemExit(f'constant anchor mismatch count={s.count(old_const)}')
s = s.replace(old_const, new_const, 1)

old_start = """    const restoreIncomplete = restoredCoinbaseRows < COINBASE_COMPLETE_MIN_PRODUCTS || restoredGateRows < GATE_COMPLETE_MIN_ROWS || integrityRecoveryRequired || Boolean(lastRefreshError);
    const age = Date.now() - (Date.parse(lastRefreshSucceededAt || '') || 0);
    const delay = restoreIncomplete ? 15_000 : (age >= REFRESH_MS ? START_DELAY_MS : Math.max(START_DELAY_MS, REFRESH_MS - age));
    const startupReason = integrityRecoveryRequired ? 'startup_catalog_integrity_recovery' : (Boolean(lastRefreshError) ? 'startup_previous_refresh_failure' : (restoreIncomplete ? 'startup_restore_fallback' : 'startup_or_due'));
"""
new_start = """    const restoreIncomplete = restoredCoinbaseRows < COINBASE_COMPLETE_MIN_PRODUCTS || restoredGateRows < GATE_COMPLETE_MIN_ROWS || integrityRecoveryRequired;
    const age = Date.now() - (Date.parse(lastRefreshSucceededAt || '') || 0);
    const lastAttemptAtMs = Date.parse(lastRefreshStartedAt || '') || 0;
    const lastAttemptAge = lastAttemptAtMs > 0 ? Date.now() - lastAttemptAtMs : Number.POSITIVE_INFINITY;
    const failedRecently = Boolean(lastRefreshError) && !restoreIncomplete && lastAttemptAge >= 0 && lastAttemptAge < FAILURE_RETRY_BACKOFF_MS;
    const delay = restoreIncomplete
      ? 15_000
      : (failedRecently
          ? Math.max(START_DELAY_MS, FAILURE_RETRY_BACKOFF_MS - lastAttemptAge)
          : (age >= REFRESH_MS ? START_DELAY_MS : Math.max(START_DELAY_MS, REFRESH_MS - age)));
    const startupReason = integrityRecoveryRequired
      ? 'startup_catalog_integrity_recovery'
      : (restoreIncomplete
          ? 'startup_restore_fallback'
          : (failedRecently
              ? 'startup_previous_refresh_failure_backoff'
              : (Boolean(lastRefreshError) ? 'startup_previous_refresh_failure_due' : 'startup_or_due')));
"""
if s.count(old_start) != 1:
    raise SystemExit(f'startup anchor mismatch count={s.count(old_start)}')
s = s.replace(old_start, new_start, 1)

p.write_text(s, encoding='utf-8')
print('patched', p)

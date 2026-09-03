from pathlib import Path
p=Path('src/binance-contract-market.mjs')
s=p.read_text(encoding='utf-8')
old="""const deliveryBySymbol = new Map();
let lastDeliveryEventAt = 0;
const connectionState = new Map();
"""
new="""const deliveryBySymbol = new Map();
let lastDeliveryEventAt = 0;
const coinMDeliveryDiagnostics = {
  st2_seen: 0,
  normalized: 0,
  rejected_no_dated_symbol: 0,
  rejected_other: 0,
  last_st2_symbol: '',
  last_st2_pair: '',
  last_st2_contract_type: '',
  last_st2_source: '',
  last_st2_keys: [],
  last_st2_seen_at: 0,
  by_source: {},
};
const connectionState = new Map();
"""
if s.count(old)!=1: raise SystemExit(f'diag anchor1 count={s.count(old)}')
s=s.replace(old,new,1)
old2="""function upsertCoinMDelivery(item, source, observedAt = Date.now()) {
  const identity = normalizeBinanceCoinMDeliveryPublicRow(item);
  if (!identity) return false;
"""
new2="""function upsertCoinMDelivery(item, source, observedAt = Date.now()) {
  const isCoinM = isCoinMPayload(item);
  if (isCoinM) {
    coinMDeliveryDiagnostics.st2_seen += 1;
    coinMDeliveryDiagnostics.last_st2_symbol = String(item?.s ?? item?.symbol ?? '').trim().toUpperCase();
    coinMDeliveryDiagnostics.last_st2_pair = String(item?.ps ?? item?.pair ?? '').trim().toUpperCase();
    coinMDeliveryDiagnostics.last_st2_contract_type = String(item?.ct ?? item?.contractType ?? '').trim().toUpperCase();
    coinMDeliveryDiagnostics.last_st2_source = String(source || '');
    coinMDeliveryDiagnostics.last_st2_keys = Object.keys(item || {}).slice(0, 32);
    coinMDeliveryDiagnostics.last_st2_seen_at = observedAt;
    coinMDeliveryDiagnostics.by_source[source] = Number(coinMDeliveryDiagnostics.by_source[source] || 0) + 1;
  }
  const identity = normalizeBinanceCoinMDeliveryPublicRow(item);
  if (!identity) {
    if (isCoinM) {
      const rawSymbol = String(item?.s ?? item?.symbol ?? '').trim().toUpperCase();
      if (!/_\\d{6}$/.test(rawSymbol)) coinMDeliveryDiagnostics.rejected_no_dated_symbol += 1;
      else coinMDeliveryDiagnostics.rejected_other += 1;
    }
    return false;
  }
  coinMDeliveryDiagnostics.normalized += 1;
"""
if s.count(old2)!=1: raise SystemExit(f'diag anchor2 count={s.count(old2)}')
s=s.replace(old2,new2,1)
old3="""    reuses_existing_mark_price_stream: true, reads_scale_with_users: false,
  };
}
"""
new3="""    reuses_existing_mark_price_stream: true, reads_scale_with_users: false,
    diagnostics: {
      ...coinMDeliveryDiagnostics,
      last_st2_seen_at: coinMDeliveryDiagnostics.last_st2_seen_at ? iso(coinMDeliveryDiagnostics.last_st2_seen_at) : null,
      by_source: { ...coinMDeliveryDiagnostics.by_source },
    },
  };
}
"""
if s.count(old3)!=1: raise SystemExit(f'diag anchor3 count={s.count(old3)}')
s=s.replace(old3,new3,1)
p.write_text(s,encoding='utf-8')
print('PASS Step1060.33.5 Binance delivery diagnostics patch')

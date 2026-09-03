from pathlib import Path
p=Path('src/contract-basis.mjs')
s=p.read_text(encoding='utf-8')
old="""    const { inputByProvider, sharedRound } = collectInputs();
    const snapshot = buildContractBasisSnapshotFromInputs(inputByProvider, { nowMs: Date.now(), sharedRound });
    if (!snapshot.full_input_ready) {
      const reasons = PROVIDERS.flatMap((provider) => {
        const coverage = snapshot.provider_coverage?.[provider];
        return coverage?.input_ready ? [] : [`${provider}:spot=${coverage?.spot_input_ready},contract=${coverage?.contract_input_ready}`];
      });
      throw new Error(`market_light_inputs_not_ready:${reasons.join('|')}`);
    }

    // Delivery is deliberately non-fatal to the already-stable perpetual basis layer.
    // A provider outage can only make delivery partial; it cannot remove the existing basis snapshot.
    await maybeRefreshDeliverySnapshot(inputByProvider, { reason: `contract_basis_${reason}` });

    latestVerifiedSnapshot = {
"""
new="""    const { inputByProvider, sharedRound } = collectInputs();

    // Step1060.33.5: delivery refresh is independent from the five-provider perpetual
    // completeness gate. A transient Gate/other market-light outage must not prevent
    // Binance COIN-M WS-only delivery (or another healthy delivery venue) from refreshing.
    // This remains background-shared and non-fatal; user reads still start zero upstream work.
    await maybeRefreshDeliverySnapshot(inputByProvider, { reason: `contract_basis_${reason}` });

    const snapshot = buildContractBasisSnapshotFromInputs(inputByProvider, { nowMs: Date.now(), sharedRound });
    if (!snapshot.full_input_ready) {
      const reasons = PROVIDERS.flatMap((provider) => {
        const coverage = snapshot.provider_coverage?.[provider];
        return coverage?.input_ready ? [] : [`${provider}:spot=${coverage?.spot_input_ready},contract=${coverage?.contract_input_ready}`];
      });
      throw new Error(`market_light_inputs_not_ready:${reasons.join('|')}`);
    }

    latestVerifiedSnapshot = {
"""
if s.count(old)!=1:
    raise SystemExit(f'expected exact run-cycle block once, found {s.count(old)}')
s=s.replace(old,new,1)
p.write_text(s,encoding='utf-8')
print('PASS moved delivery refresh before full-input readiness gate')

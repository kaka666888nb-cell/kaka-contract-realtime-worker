from pathlib import Path
p=Path('src/social-watch.mjs')
s=p.read_text(encoding='utf-8')

old_const="""const RULE_BATCH_SIZE = 25;
const X_USAGE_MIN_REFRESH_MS = 5 * 60_000;
"""
new_const="""const RULE_BATCH_SIZE = 25;
// Step1060.7: keep exact per-account rule state writes event-driven. A low-frequency heartbeat
// preserves admin observability without rewriting every account on each hourly verification.
const RULE_ACCOUNT_STATUS_HEARTBEAT_MS = Math.max(
  60 * 60_000,
  Number(process.env.KAKA_SOCIAL_RULE_ACCOUNT_HEARTBEAT_MS || 6 * 60 * 60_000),
);
const X_USAGE_MIN_REFRESH_MS = 5 * 60_000;
"""
assert s.count(old_const)==1, f'const anchor count={s.count(old_const)}'
s=s.replace(old_const,new_const,1)

old_state="""  ruleReads: 0,
  ruleWrites: 0,
  ruleSyncFailures: 0,
  ruleSyncNextAllowedAt: null,
"""
new_state="""  ruleReads: 0,
  ruleWrites: 0,
  ruleSyncFailures: 0,
  ruleSyncNextAllowedAt: null,
  ruleSecondReadsAvoided: 0,
  ruleAccountStatusWrites: 0,
  ruleAccountStatusNoopSkips: 0,
  ruleAccountHeartbeatWrites: 0,
"""
assert s.count(old_state)==1, f'state anchor count={s.count(old_state)}'
s=s.replace(old_state,new_state,1)

old_sync="""  for (const batch of chunks(deleteIds.filter(Boolean), RULE_BATCH_SIZE)) {
    if (batch.length) await writeXRules({ delete: { ids: batch } });
  }
  for (const batch of chunks(additions, RULE_BATCH_SIZE)) {
    if (batch.length) await writeXRules({ add: batch });
  }

  const finalRules = await listXRules();
  const finalManaged = finalRules.filter((r) => text(r?.tag).startsWith(RULE_TAG_PREFIX));
  state.allManagedRules = finalManaged.length;
  state.managedRules = finalManaged.filter((rule) => desiredAccountsByTag.get(text(rule?.tag))?.public_visible === true).length;
  state.internalManagedRules = state.allManagedRules - state.managedRules;
  state.lastRuleSyncAt = nowIso();
  state.ruleSyncFailures = 0;
  state.ruleSyncNextAllowedAt = null;
  const finalByTag = new Map(finalManaged.map((r) => [text(r.tag), r]));
  await Promise.all([...desiredAccountsByTag.entries()].map(async ([tag, account]) => {
    const rule = finalByTag.get(tag);
    await updateAccountStatus(account.id, {
      rule_id: rule ? text(rule.id) : null,
      rule_status: rule ? 'active' : 'pending',
      last_rule_sync_at: state.lastRuleSyncAt,
      last_error: rule ? null : 'X rule pending',
    });
  }));
"""
new_sync="""  for (const batch of chunks(deleteIds.filter(Boolean), RULE_BATCH_SIZE)) {
    if (batch.length) await writeXRules({ delete: { ids: batch } });
  }
  for (const batch of chunks(additions, RULE_BATCH_SIZE)) {
    if (batch.length) await writeXRules({ add: batch });
  }

  const rulesChanged = deleteIds.some(Boolean) || additions.length > 0;
  // A second GET is only needed after we actually changed X rules. When the first read already
  // proves the remote rule-set equals the desired set, re-reading it immediately is pure cost.
  const finalRules = rulesChanged ? await listXRules() : currentRules;
  if (!rulesChanged) state.ruleSecondReadsAvoided++;
  const finalManaged = finalRules.filter((r) => text(r?.tag).startsWith(RULE_TAG_PREFIX));
  state.allManagedRules = finalManaged.length;
  state.managedRules = finalManaged.filter((rule) => desiredAccountsByTag.get(text(rule?.tag))?.public_visible === true).length;
  state.internalManagedRules = state.allManagedRules - state.managedRules;
  state.lastRuleSyncAt = nowIso();
  state.ruleSyncFailures = 0;
  state.ruleSyncNextAllowedAt = null;
  const finalByTag = new Map(finalManaged.map((r) => [text(r.tag), r]));
  await Promise.all([...desiredAccountsByTag.entries()].map(async ([tag, account]) => {
    const rule = finalByTag.get(tag);
    const nextRuleId = rule ? text(rule.id) : '';
    const nextRuleStatus = rule ? 'active' : 'pending';
    const nextError = rule ? '' : 'X rule pending';
    const stateChanged =
      text(account?.rule_id) !== nextRuleId ||
      text(account?.rule_status) !== nextRuleStatus ||
      text(account?.last_error) !== nextError;
    const lastAccountSyncMs = Date.parse(text(account?.last_rule_sync_at));
    const heartbeatDue = !Number.isFinite(lastAccountSyncMs) ||
      Date.now() - lastAccountSyncMs >= RULE_ACCOUNT_STATUS_HEARTBEAT_MS;
    if (!stateChanged && !heartbeatDue) {
      state.ruleAccountStatusNoopSkips++;
      return;
    }
    await updateAccountStatus(account.id, {
      rule_id: nextRuleId || null,
      rule_status: nextRuleStatus,
      last_rule_sync_at: state.lastRuleSyncAt,
      last_error: nextError || null,
    });
    state.ruleAccountStatusWrites++;
    if (!stateChanged) state.ruleAccountHeartbeatWrites++;
    account.rule_id = nextRuleId || null;
    account.rule_status = nextRuleStatus;
    account.last_rule_sync_at = state.lastRuleSyncAt;
    account.last_error = nextError || null;
  }));
"""
assert s.count(old_sync)==1, f'sync anchor count={s.count(old_sync)}'
s=s.replace(old_sync,new_sync,1)

old_health="""    rule_reads: state.ruleReads,
    rule_writes: state.ruleWrites,
    rule_sync_failures: state.ruleSyncFailures,
    rule_sync_next_allowed_at: state.ruleSyncNextAllowedAt,
"""
new_health="""    rule_reads: state.ruleReads,
    rule_writes: state.ruleWrites,
    rule_sync_failures: state.ruleSyncFailures,
    rule_sync_next_allowed_at: state.ruleSyncNextAllowedAt,
    rule_cost_guard: {
      version: 'step1060_7_social_rule_sync_cost_guard_v1',
      second_x_rules_read_only_after_remote_mutation: true,
      account_status_write_on_change_or_heartbeat_only: true,
      account_status_heartbeat_hours: RULE_ACCOUNT_STATUS_HEARTBEAT_MS / 3_600_000,
      second_x_rule_reads_avoided: state.ruleSecondReadsAvoided,
      account_status_writes: state.ruleAccountStatusWrites,
      account_status_noop_skips: state.ruleAccountStatusNoopSkips,
      account_status_heartbeat_writes: state.ruleAccountHeartbeatWrites,
    },
"""
assert s.count(old_health)==1, f'health anchor count={s.count(old_health)}'
s=s.replace(old_health,new_health,1)

p.write_text(s,encoding='utf-8')

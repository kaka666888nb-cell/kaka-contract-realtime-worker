from pathlib import Path
p=Path('src/social-watch.mjs')
s=p.read_text(encoding='utf-8')

old_state="""  avatarMirrorBytes: 0,
  avatarMirrorLastSuccessAt: null,
  avatarMirrorLastError: null,
};
"""
new_state="""  avatarMirrorBytes: 0,
  avatarMirrorLastSuccessAt: null,
  avatarMirrorLastError: null,
  duplicateEventsSkipped: 0,
  duplicateMediaReuseHits: 0,
  duplicateDbWritesAvoided: 0,
};
"""
assert s.count(old_state)==1, f'state anchor count={s.count(old_state)}'
s=s.replace(old_state,new_state,1)

old_query="""  const existingRows = await supabaseFetch(
    `${EVENTS_TABLE}?source=eq.x&source_post_id=eq.${encodeURIComponent(sourcePostId)}&select=id,post_url,content,media_items&limit=1`,
  );
  const existingEvent = Array.isArray(existingRows) ? existingRows[0] : null;
  const existed = Boolean(existingEvent?.id);
  const unchangedRepeat = existed &&
    text(existingEvent?.post_url) === postUrl &&
    text(existingEvent?.content) === contentText;
  const mediaItems = unchangedRepeat && Array.isArray(existingEvent?.media_items)
    ? existingEvent.media_items
    : await prepareSocialPostMedia(postId, post);
  const body = {
"""
new_query="""  const existingRows = await supabaseFetch(
    `${EVENTS_TABLE}?source=eq.x&source_post_id=eq.${encodeURIComponent(sourcePostId)}&select=id,post_url,content,media_items,is_active,lifecycle_status&limit=1`,
  );
  const existingEvent = Array.isArray(existingRows) ? existingRows[0] : null;
  const existed = Boolean(existingEvent?.id);
  const sameContent = existed &&
    text(existingEvent?.post_url) === postUrl &&
    text(existingEvent?.content) === contentText;
  const unchangedRepeat = sameContent &&
    existingEvent?.is_active === true &&
    (text(existingEvent?.lifecycle_status).toLowerCase() || 'active') === 'active';
  if (unchangedRepeat) {
    // Step1060.6.1: an unchanged active redelivery is not lifecycle evidence and must not
    // create a second upsert/PATCH/translation pass. Removed rows and edited posts still flow
    // through the normal update path so reactivation and content edits remain correct.
    state.duplicateEventsSkipped++;
    state.duplicateDbWritesAvoided += 2; // event upsert + trailing account/event status write set
    if (Array.isArray(existingEvent?.media_items) && existingEvent.media_items.length) {
      state.duplicateMediaReuseHits++;
    }
    return true;
  }
  const mediaItems = sameContent && Array.isArray(existingEvent?.media_items)
    ? existingEvent.media_items
    : await prepareSocialPostMedia(postId, post);
  if (sameContent && Array.isArray(existingEvent?.media_items) && existingEvent.media_items.length) {
    state.duplicateMediaReuseHits++;
  }
  const body = {
"""
assert s.count(old_query)==1, f'query anchor count={s.count(old_query)}'
s=s.replace(old_query,new_query,1)

old_health="""    content_media_rows: publicEventsSnapshot.filter((row) => Array.isArray(row?.media_items) && row.media_items.length > 0).length,
    airdrop_bridge_accounts: state.airdropBridgeAccounts,
"""
new_health="""    content_media_rows: publicEventsSnapshot.filter((row) => Array.isArray(row?.media_items) && row.media_items.length > 0).length,
    duplicate_event_cost_guard: {
      version: 'step1060_6_1_social_duplicate_event_cost_guard_v1',
      exact_source_identity: 'source_plus_source_post_id',
      unchanged_active_event_db_write_skipped: true,
      removed_event_can_reactivate: true,
      edited_post_can_update: true,
      duplicate_events_skipped: state.duplicateEventsSkipped,
      duplicate_media_reuse_hits: state.duplicateMediaReuseHits,
      estimated_db_writes_avoided: state.duplicateDbWritesAvoided,
    },
    airdrop_bridge_accounts: state.airdropBridgeAccounts,
"""
assert s.count(old_health)==1, f'health anchor count={s.count(old_health)}'
s=s.replace(old_health,new_health,1)

p.write_text(s,encoding='utf-8')

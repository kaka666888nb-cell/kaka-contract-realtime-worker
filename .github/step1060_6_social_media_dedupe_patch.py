from pathlib import Path
p=Path('src/social-watch.mjs')
s=p.read_text(encoding='utf-8')

old="""  const postUrl = `https://x.com/${handle}/status/${postId}`;
  const publishedAt = text(post?.created_at) || nowIso();
  const mediaItems = await prepareSocialPostMedia(postId, post);
  const body = {
"""
new="""  const postUrl = `https://x.com/${handle}/status/${postId}`;
  const publishedAt = text(post?.created_at) || nowIso();
  const contentText = postFullText(post);
  // Step1060.6: resolve exact event identity before downloading/uploading media. Filtered
  // stream reconnect/backfill can deliver the same post more than once; unchanged repeats
  // must reuse the already mirrored media instead of paying another X download + Storage upload.
  const existingRows = await supabaseFetch(
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
assert s.count(old)==1, f'pre-media anchor count={s.count(old)}'
s=s.replace(old,new,1)

old2="""    content: postFullText(post),
"""
new2="""    content: contentText,
"""
assert s.count(old2)>=1, 'content anchor missing'
# Only replace the first occurrence after insertEvent patch; this exact line in body is the target.
pos=s.index(new)
idx=s.index(old2,pos)
s=s[:idx]+new2+s[idx+len(old2):]

old3="""  const existingRows = await supabaseFetch(
    `${EVENTS_TABLE}?source=eq.x&source_post_id=eq.${encodeURIComponent(sourcePostId)}&select=id&limit=1`,
  );
  const existed = Array.isArray(existingRows) && Boolean(existingRows[0]?.id);
  const inserted = await supabaseFetch(
"""
new3="""  const inserted = await supabaseFetch(
"""
assert s.count(old3)==1, f'late existence anchor count={s.count(old3)}'
s=s.replace(old3,new3,1)

old4="""    const content = notificationText(postFullText(post));
"""
new4="""    const content = notificationText(contentText);
"""
assert s.count(old4)==1, 'notification content anchor mismatch'
s=s.replace(old4,new4,1)

p.write_text(s,encoding='utf-8')

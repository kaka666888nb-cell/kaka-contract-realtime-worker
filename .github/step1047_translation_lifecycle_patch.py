from pathlib import Path

p=Path('src/content-publication-translation.mjs')
s=p.read_text(encoding='utf-8')
changes=[
("${NEWS_TABLE}?is_active=eq.true&select=id,title,content,translations,translation_source_hash,translation_updated_at,published_at,updated_at&order=sort_order.asc,published_at.desc&limit=${NEWS_SCAN_LIMIT}",
 "${NEWS_TABLE}?is_active=eq.true&lifecycle_status=eq.active&select=id,title,content,translations,translation_source_hash,translation_updated_at,published_at,updated_at&order=sort_order.asc,published_at.desc&limit=${NEWS_SCAN_LIMIT}"),
("${NEWS_TABLE}?is_active=eq.true&primary_source_key=in.(${sourceFilter})&select=id,title,content,translations,translation_source_hash,translation_updated_at,published_at,updated_at,primary_source_key&order=published_at.desc&limit=${OFFICIAL_ENGLISH_NEWS_SCAN_LIMIT}",
 "${NEWS_TABLE}?is_active=eq.true&lifecycle_status=eq.active&primary_source_key=in.(${sourceFilter})&select=id,title,content,translations,translation_source_hash,translation_updated_at,published_at,updated_at,primary_source_key&order=published_at.desc&limit=${OFFICIAL_ENGLISH_NEWS_SCAN_LIMIT}")
]
for old,new in changes:
    c=s.count(old)
    if c!=1:
        raise SystemExit(f'anchor mismatch count={c}: {old[:80]}')
    s=s.replace(old,new,1)
p.write_text(s,encoding='utf-8')
print('patched',p)

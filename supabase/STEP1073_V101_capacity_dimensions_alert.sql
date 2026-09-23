create table if not exists kaka_private.app_capacity_dimension_alert_state (
  dimension text primary key,
  label text not null default '',
  runtime_id text not null default '',
  last_level text not null default 'normal',
  last_level_rank integer not null default 0,
  last_used integer not null default 0,
  last_limit integer not null default 1,
  last_rejected_capacity bigint not null default 0,
  last_notified_at timestamptz,
  last_email_event_id uuid,
  updated_at timestamptz not null default now(),
  constraint app_capacity_dimension_id_format
    check (dimension ~ '^[a-z0-9_:-]{1,80}$'),
  constraint app_capacity_dimension_nonnegative
    check (
      last_level_rank between 0 and 4
      and last_used >= 0
      and last_limit > 0
      and last_rejected_capacity >= 0
    )
);

revoke all on table kaka_private.app_capacity_dimension_alert_state
  from public, anon, authenticated;
grant select, insert, update on table kaka_private.app_capacity_dimension_alert_state
  to service_role;

create or replace function public.kaka_edge_capacity_dimensions_alert_decide(
  p_dimensions jsonb,
  p_dry_run boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path = 'pg_catalog', 'public', 'kaka_private'
as $function$
declare
  v_item jsonb;
  v_state kaka_private.app_capacity_dimension_alert_state%rowtype;
  v_state_found boolean;
  v_seen text[] := array[]::text[];
  v_evaluated jsonb := '[]'::jsonb;
  v_count integer := 0;
  v_id text;
  v_label text;
  v_runtime text;
  v_used integer;
  v_limit integer;
  v_rejected bigint;
  v_new_rejected bigint;
  v_pct numeric;
  v_level text;
  v_rank integer;
  v_candidate_notify boolean;
  v_best_id text := '';
  v_best_label text := '';
  v_best_runtime text := '';
  v_best_used integer := 0;
  v_best_limit integer := 1;
  v_best_rejected bigint := 0;
  v_best_new_rejected bigint := 0;
  v_best_pct numeric := -1;
  v_best_level text := 'normal';
  v_best_rank integer := -1;
  v_notify boolean := false;
  v_title text := '';
  v_body text := '';
  v_admin_emails jsonb := '[]'::jsonb;
  v_inserted integer := 0;
  v_event_id uuid := null;
begin
  if p_dimensions is null or jsonb_typeof(p_dimensions) <> 'array' then
    raise exception 'p_dimensions must be a JSON array';
  end if;
  if jsonb_array_length(p_dimensions) < 1
     or jsonb_array_length(p_dimensions) > 20 then
    raise exception 'capacity dimension count out of range';
  end if;

  for v_item in select value from jsonb_array_elements(p_dimensions)
  loop
    v_count := v_count + 1;
    v_id := lower(trim(coalesce(v_item->>'id', '')));
    if v_id !~ '^[a-z0-9_:-]{1,80}$' then
      raise exception 'invalid capacity dimension id';
    end if;
    if v_id = any(v_seen) then
      raise exception 'duplicate capacity dimension id: %', v_id;
    end if;
    v_seen := array_append(v_seen, v_id);

    v_label := left(
      regexp_replace(trim(coalesce(v_item->>'label', v_id)), '[[:cntrl:]]+', ' ', 'g'),
      80
    );
    if v_label = '' then v_label := v_id; end if;
    v_runtime := left(trim(coalesce(v_item->>'runtime_id', 'unknown')), 160);
    if v_runtime = '' then v_runtime := 'unknown'; end if;
    v_used := greatest(0, coalesce((v_item->>'used')::integer, 0));
    v_limit := greatest(1, coalesce((v_item->>'limit')::integer, 1));
    v_rejected := greatest(0, coalesce((v_item->>'rejected')::bigint, 0));
    v_pct := round((v_used::numeric * 100) / v_limit, 2);

    select * into v_state
    from kaka_private.app_capacity_dimension_alert_state
    where dimension = v_id;
    v_state_found := found;
    v_new_rejected := case
      when v_state_found and v_state.runtime_id = v_runtime
        then greatest(0, v_rejected - v_state.last_rejected_capacity)
      else v_rejected
    end;

    if v_new_rejected > 0 then
      v_level := 'rejected'; v_rank := 4;
    elsif v_pct >= 95 then
      v_level := 'critical'; v_rank := 3;
    elsif v_pct >= 85 then
      v_level := 'high'; v_rank := 2;
    elsif v_pct >= 70 then
      v_level := 'warning'; v_rank := 1;
    else
      v_level := 'normal'; v_rank := 0;
    end if;

    v_candidate_notify := v_rank > 0 and (
      not v_state_found
      or v_rank > coalesce(v_state.last_level_rank, 0)
      or v_state.last_notified_at is null
      or v_state.last_notified_at <= now() - interval '6 hours'
    );

    v_evaluated := v_evaluated || jsonb_build_array(jsonb_build_object(
      'dimension', v_id,
      'label', v_label,
      'runtime_id', v_runtime,
      'used', v_used,
      'limit', v_limit,
      'percent', v_pct,
      'rejected_capacity', v_rejected,
      'new_rejected_capacity', v_new_rejected,
      'level', v_level,
      'level_rank', v_rank,
      'would_notify', v_candidate_notify
    ));

    if v_rank > v_best_rank
       or (v_rank = v_best_rank and v_pct > v_best_pct) then
      v_best_id := v_id;
      v_best_label := v_label;
      v_best_runtime := v_runtime;
      v_best_used := v_used;
      v_best_limit := v_limit;
      v_best_rejected := v_rejected;
      v_best_new_rejected := v_new_rejected;
      v_best_pct := v_pct;
      v_best_level := v_level;
      v_best_rank := v_rank;
      v_notify := v_candidate_notify;
    end if;
  end loop;

  if coalesce(p_dry_run, false) then
    return jsonb_build_object(
      'ok', true,
      'dry_run', true,
      'dimension_count', v_count,
      'dimension', v_best_id,
      'label', v_best_label,
      'level', v_best_level,
      'level_rank', greatest(0, v_best_rank),
      'used', v_best_used,
      'limit', v_best_limit,
      'percent', greatest(0, v_best_pct),
      'rejected_capacity', v_best_rejected,
      'new_rejected_capacity', v_best_new_rejected,
      'would_notify', v_notify,
      'evaluated', v_evaluated
    );
  end if;

  if v_notify then
    v_event_id := gen_random_uuid();
    v_title := case v_best_level
      when 'rejected' then '实时行情容量出现新的拒绝'
      when 'critical' then '实时行情容量达到95%'
      when 'high' then '实时行情容量达到85%'
      when 'warning' then '实时行情容量达到70%'
      else '实时行情容量正常'
    end;
    v_body := format(
      '当前最高维度：%s，使用 %s / %s（%s%%）；本运行实例该维度新增拒绝 %s、累计拒绝 %s。请检查对应连接或精确身份上限，并评估扩容。',
      v_best_label,
      v_best_used,
      v_best_limit,
      v_best_pct,
      v_best_new_rejected,
      v_best_rejected
    );
    insert into public.app_notifications(
      user_id, is_global, title, content, type, is_active,
      target_type, target_id, translations
    )
    select
      profile.id, false, v_title, v_body, 'admin_capacity', true,
      'admin_ops_capacity', v_best_id, '{}'::jsonb
    from public.app_profiles profile
    where profile.role in ('admin', 'super_admin')
      and coalesce(profile.is_banned, false) = false;
    get diagnostics v_inserted = row_count;
  end if;

  for v_item in select value from jsonb_array_elements(v_evaluated)
  loop
    v_id := v_item->>'dimension';
    insert into kaka_private.app_capacity_dimension_alert_state(
      dimension,
      label,
      runtime_id,
      last_level,
      last_level_rank,
      last_used,
      last_limit,
      last_rejected_capacity,
      last_notified_at,
      last_email_event_id,
      updated_at
    ) values (
      v_id,
      v_item->>'label',
      v_item->>'runtime_id',
      v_item->>'level',
      (v_item->>'level_rank')::integer,
      (v_item->>'used')::integer,
      (v_item->>'limit')::integer,
      (v_item->>'rejected_capacity')::bigint,
      case when v_notify and v_id = v_best_id then now() else null end,
      case when v_notify and v_id = v_best_id then v_event_id else null end,
      now()
    )
    on conflict (dimension) do update set
      label = excluded.label,
      runtime_id = excluded.runtime_id,
      last_level = excluded.last_level,
      last_level_rank = excluded.last_level_rank,
      last_used = excluded.last_used,
      last_limit = excluded.last_limit,
      last_rejected_capacity = excluded.last_rejected_capacity,
      last_notified_at = case
        when v_notify and excluded.dimension = v_best_id then now()
        else kaka_private.app_capacity_dimension_alert_state.last_notified_at
      end,
      last_email_event_id = case
        when v_notify and excluded.dimension = v_best_id then v_event_id
        else kaka_private.app_capacity_dimension_alert_state.last_email_event_id
      end,
      updated_at = now();
  end loop;

  select coalesce(jsonb_agg(email order by email), '[]'::jsonb)
    into v_admin_emails
  from public.app_profiles
  where role in ('admin', 'super_admin')
    and coalesce(email, '') <> ''
    and coalesce(is_banned, false) = false;

  return jsonb_build_object(
    'ok', true,
    'dry_run', false,
    'dimension_count', v_count,
    'dimension', v_best_id,
    'label', v_best_label,
    'level', v_best_level,
    'level_rank', greatest(0, v_best_rank),
    'used', v_best_used,
    'limit', v_best_limit,
    'percent', greatest(0, v_best_pct),
    'rejected_capacity', v_best_rejected,
    'new_rejected_capacity', v_best_new_rejected,
    'notify', v_notify,
    'notifications_inserted', v_inserted,
    'admin_emails', case when v_notify then v_admin_emails else '[]'::jsonb end,
    'title', case when v_notify then v_title else '' end,
    'body', case when v_notify then v_body else '' end,
    'email_event_id', case when v_notify then v_event_id else null end,
    'evaluated', v_evaluated
  );
end;
$function$;

revoke all on function public.kaka_edge_capacity_dimensions_alert_decide(jsonb, boolean)
  from public, anon, authenticated;
grant execute on function public.kaka_edge_capacity_dimensions_alert_decide(jsonb, boolean)
  to service_role;

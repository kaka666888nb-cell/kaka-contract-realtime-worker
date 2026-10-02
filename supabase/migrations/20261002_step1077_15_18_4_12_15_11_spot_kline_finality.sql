-- KakaWeb3 Step1077.15.18.4.12.15.11
-- Spot Kline finality + 3m cron overlap repair.
-- Review source SHA/version gates in PR before applying to Supabase production.
--
-- Root cause:
-- 3m collectors ran every 15 minutes with limit=5. Because the response includes
-- the currently-open bar, the previous run's open bar had already fallen outside
-- the next 5-row window by the time it was closed. After the Edge finality guard
-- stops persisting the open bar, limit must be >= 6; use 7 for one-bar safety.

do $$
declare
  v_command text;
  v_schedule text;
begin
  select command, schedule into v_command, v_schedule from cron.job where jobid = 78;
  if v_command is null
     or v_schedule <> '0,15,30,45 * * * *'
     or position('kaka-market-kline-proxy' in v_command) = 0
     or position('interval=3m&limit=5' in v_command) = 0 then
    raise exception 'job 78 Binance spot 3m gate mismatch; abort';
  end if;
  perform cron.alter_job(
    78,
    command := replace(v_command, 'interval=3m&limit=5', 'interval=3m&limit=7')
  );

  select command, schedule into v_command, v_schedule from cron.job where jobid = 100;
  if v_command is null
     or v_schedule <> '1,16,31,46 * * * *'
     or position('kaka-okx-market-kline-proxy' in v_command) = 0
     or position('''interval'', ''3m''' in v_command) = 0
     or position('''limit'', 5' in v_command) = 0 then
    raise exception 'job 100 OKX spot 3m gate mismatch; abort';
  end if;
  perform cron.alter_job(
    100,
    command := replace(v_command, '''limit'', 5', '''limit'', 7')
  );

  select command, schedule into v_command, v_schedule from cron.job where jobid = 122;
  if v_command is null
     or v_schedule <> '3,18,33,48 * * * *'
     or position('kaka-bitget-market-kline-proxy' in v_command) = 0
     or position('''interval'', ''3m''' in v_command) = 0
     or position('''limit'', 5' in v_command) = 0 then
    raise exception 'job 122 Bitget spot 3m gate mismatch; abort';
  end if;
  perform cron.alter_job(
    122,
    command := replace(v_command, '''limit'', 5', '''limit'', 7')
  );
end
$$;

-- Post-apply gate: each finality-filtered 3m collector must retain at least one
-- closed copy of the prior run's current bar in the next request window.
select jobid, schedule,
       case
         when command like '%interval=3m&limit=7%' then 'binance'
         when command like '%kaka-okx-market-kline-proxy%'
              and command like '%''interval'', ''3m''%'
              and command like '%''limit'', 7%' then 'okx'
         when command like '%kaka-bitget-market-kline-proxy%'
              and command like '%''interval'', ''3m''%'
              and command like '%''limit'', 7%' then 'bitget'
         else 'unexpected'
       end as finality_overlap_gate
from cron.job
where jobid in (78,100,122)
order by jobid;

const VERSION='1073.r24.shared-history.1';
const SUPABASE_URL=String(process.env.SUPABASE_URL||'').replace(/\/+$/,'');
const SERVICE_KEY=String(process.env.SUPABASE_SERVICE_ROLE_KEY||'');
const PROVIDERS=new Set(['binance','okx','bybit','bitget','gate']);
const CONFIG={
  app_get_contract_klines_cache:{period:'p_interval',def:'5m',max:1500,fresh:5000,stale:180000},
  app_get_contract_open_interest_history:{period:'p_period',def:'5m',max:1500,fresh:20000,stale:300000},
  app_get_contract_long_short_ratios:{period:'p_period',def:'5m',max:4500,fresh:20000,stale:300000},
  app_get_contract_taker_buy_sell:{period:'p_period',def:'5m',max:1500,fresh:20000,stale:300000},
  app_get_contract_cvd:{period:'p_interval',def:'5m',max:1500,fresh:20000,stale:300000},
};
const CACHE_MAX=24, ACTIVE_MAX=3, QUEUE_MAX=48, RPC_TIMEOUT_MS=15000;
const cache=new Map(), inflight=new Map(), queue=[];
let active=0;
const stats={reads:0,fresh_hits:0,stale_hits:0,inflight_hits:0,cold_misses:0,builds:0,failures:0,rejected:0,rpc_calls:0,evictions:0};

function providerKey(v){
  v=String(v||'').trim().toLowerCase();
  if(v==='gate.io')v='gate'; if(v==='okex')v='okx';
  return PROVIDERS.has(v)?v:'';
}
function symbolKey(v){
  v=String(v||'').trim().toUpperCase().replace(/-SWAP$/i,'').replace(/_UMCBL$/i,'').replace(/[^A-Z0-9]/g,'');
  return v.length>=2&&v.length<=40?v:'';
}
function periodKey(v,fallback){
  v=String(v||fallback||'').trim();
  return /^[A-Za-z0-9]{1,12}$/.test(v)?v:'';
}
function clampInt(v,fallback,max){
  const n=Number.parseInt(String(v??''),10);
  return Math.max(1,Math.min(max,Number.isFinite(n)?n:fallback));
}
function prune(){
  const now=Date.now();
  for(const [k,e] of cache) if(!e||e.staleUntil<=now) cache.delete(k);
  while(cache.size>CACHE_MAX){
    const k=[...cache.entries()].sort((a,b)=>(a[1]?.storedAt||0)-(b[1]?.storedAt||0))[0]?.[0];
    if(!k)break; cache.delete(k); stats.evictions++;
  }
}
function send(res,status,payload,headers={}){
  if(res.headersSent)return;
  const body=typeof payload==='string'?payload:JSON.stringify(payload);
  res.writeHead(status,{
    'content-type':'application/json; charset=utf-8',
    'content-length':String(Buffer.byteLength(body)),
    ...headers,
  });
  res.end(body);
}
function release(){
  active=Math.max(0,active-1);
  while(active<ACTIVE_MAX&&queue.length){
    const item=queue.shift();
    if(!item)break;
    active++; item.resolve(release);
  }
}
function acquire(){
  if(active<ACTIVE_MAX&&queue.length===0){active++;return Promise.resolve(release);}
  if(queue.length>=QUEUE_MAX){stats.rejected++;return Promise.reject(new Error('contract_history_shared_queue_full'));}
  return new Promise((resolve,reject)=>queue.push({resolve,reject}));
}
async function rpcCall(name,cfg,provider,symbol,period,limit){
  if(!SUPABASE_URL||!SERVICE_KEY)throw new Error('supabase_service_role_not_configured');
  const body={p_provider:provider,p_symbol:symbol,p_limit:limit,[cfg.period]:period};
  stats.rpc_calls++;
  const response=await fetch(`${SUPABASE_URL}/rest/v1/rpc/${name}`,{
    method:'POST',
    headers:{apikey:SERVICE_KEY,authorization:`Bearer ${SERVICE_KEY}`,accept:'application/json','content-type':'application/json'},
    body:JSON.stringify(body),
    signal:AbortSignal.timeout(RPC_TIMEOUT_MS),
  });
  const text=await response.text();
  if(!response.ok)throw new Error(`${name}_http_${response.status}:${text.slice(0,200)}`);
  const rows=text.trim()?JSON.parse(text):[];
  if(!Array.isArray(rows))throw new Error(`${name}_payload_not_array`);
  return rows;
}
async function build(key,name,cfg,provider,symbol,period,limit){
  let done=null; stats.builds++;
  try{
    done=await acquire();
    const rows=await rpcCall(name,cfg,provider,symbol,period,limit);
    const body=JSON.stringify(rows), now=Date.now();
    const entry={body,rowCount:rows.length,storedAt:now,freshUntil:now+(rows.length?cfg.fresh:30000),staleUntil:now+cfg.stale};
    cache.set(key,entry); prune(); return entry;
  }catch(e){stats.failures++;throw e;}
  finally{if(done)done();}
}
async function getEntry(key,name,cfg,provider,symbol,period,limit){
  stats.reads++; prune();
  const now=Date.now(), existing=cache.get(key);
  if(existing&&existing.freshUntil>now){stats.fresh_hits++;return [existing,'fresh'];}
  if(inflight.has(key)){
    stats.inflight_hits++;
    try{return [await inflight.get(key),'inflight'];}
    catch(e){if(existing&&existing.staleUntil>Date.now()){stats.stale_hits++;return [existing,'stale_after_error'];}throw e;}
  }
  if(existing&&existing.staleUntil>now){
    stats.stale_hits++;
    const task=build(key,name,cfg,provider,symbol,period,limit).finally(()=>inflight.delete(key));
    inflight.set(key,task); task.catch(()=>{});
    return [existing,'stale_revalidate'];
  }
  stats.cold_misses++;
  const task=build(key,name,cfg,provider,symbol,period,limit).finally(()=>inflight.delete(key));
  inflight.set(key,task);
  return [await task,'cold_build'];
}
export function getContractHistorySharedHealth(){
  return {ok:true,version:VERSION,supabase_configured:Boolean(SUPABASE_URL&&SERVICE_KEY),allowed_rpcs:Object.keys(CONFIG),cache_entries:cache.size,inflight_entries:inflight.size,active_builds:active,queued_builds:queue.length,cache_max:CACHE_MAX,active_max:ACTIVE_MAX,queue_max:QUEUE_MAX,stats:{...stats},user_reads_direct_supabase_rpc:false,service_role_server_side_only:true};
}
export async function handleContractHistoryShared(req,res,url){
  if(url.pathname==='/api/contract-history-shared/health'){
    send(res,200,getContractHistorySharedHealth(),{'cache-control':'no-store'}); return true;
  }
  if(url.pathname!=='/api/contract-history-shared')return false;
  if(req.method!=='GET'){send(res,405,{ok:false,version:VERSION,error:'GET required'},{'cache-control':'no-store'});return true;}
  const name=String(url.searchParams.get('rpc')||'').trim(), cfg=CONFIG[name];
  const provider=providerKey(url.searchParams.get('p_provider')||url.searchParams.get('provider'));
  const symbol=symbolKey(url.searchParams.get('p_symbol')||url.searchParams.get('symbol'));
  if(!cfg||!provider||!symbol){
    send(res,400,{ok:false,version:VERSION,error:'invalid_rpc_provider_or_symbol'},{'cache-control':'no-store'});return true;
  }
  const period=periodKey(url.searchParams.get(cfg.period)||url.searchParams.get('period')||url.searchParams.get('interval'),cfg.def);
  if(!period){send(res,400,{ok:false,version:VERSION,error:'invalid_period_or_interval'},{'cache-control':'no-store'});return true;}
  const limit=clampInt(url.searchParams.get('p_limit')||url.searchParams.get('limit'),300,cfg.max);
  const key=`${name}|${provider}|${symbol}|${period}|${limit}`;
  try{
    const [entry,state]=await getEntry(key,name,cfg,provider,symbol,period,limit);
    send(res,200,entry.body,{
      'cache-control':`public, max-age=2, s-maxage=${Math.max(1,Math.floor(cfg.fresh/1000))}, stale-while-revalidate=300`,
      'x-kaka-contract-history-version':VERSION,
      'x-kaka-cache-state':state,
      'x-kaka-row-count':String(entry.rowCount),
      'x-kaka-shared-read':'1',
      'x-kaka-user-supabase-rpc-calls':'0',
    });
  }catch(e){
    const message=String(e?.message||e);
    send(res,/queue_full/.test(message)?503:502,{ok:false,version:VERSION,error:message.slice(0,280),rpc:name,provider,symbol,period,limit},{'cache-control':'no-store'});
  }
  return true;
}

import { serve } from 'https://deno.land/std@0.224.0/http/server.ts';
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4';

const SCHEMA='step1073_v101_admin_capacity_compact_health_v2';
const SUPABASE_URL=(Deno.env.get('SUPABASE_URL')||'').trim();
const SERVICE_ROLE_KEY=(Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')||'').trim();
const SYNC_SECRET=(Deno.env.get('KAKA_SYNC_SECRET')||Deno.env.get('KAKA_SYNC_SECRET_VALUE')||'').trim();
const RESEND_API_KEY=(Deno.env.get('RESEND_API_KEY')||'').trim();
const EMAIL_FROM=(Deno.env.get('PRICE_ALERT_EMAIL_FROM')||Deno.env.get('KAKA_PRICE_ALERT_EMAIL_FROM')||'').trim();
const RENDER_HEALTH=capacityHealthUrl(Deno.env.get('KAKA_RENDER_WORKER_HEALTH_URL')||'https://kaka-contract-realtime-worker.onrender.com/api/realtime-ws-health');
const sb=createClient(SUPABASE_URL,SERVICE_ROLE_KEY,{auth:{persistSession:false}});
function capacityHealthUrl(value:string){const raw=String(value||'').trim();try{const url=new URL(raw);if(url.pathname.replace(/\/+$/,'')==='/health'){url.pathname='/api/realtime-ws-health';url.search='';url.hash=''}return url.toString()}catch{return raw}}
function out(status:number,body:unknown){return new Response(JSON.stringify(body),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store'}})}
function txt(v:unknown){return String(v??'').trim()}
function esc(v:unknown){return txt(v).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;')}
async function fetchHealth(){const c=new AbortController();const t=setTimeout(()=>c.abort(),8000);try{const r=await fetch(RENDER_HEALTH,{headers:{accept:'application/json','user-agent':'kaka-capacity-monitor/1073.101'},signal:c.signal});const j=await r.json();if(!r.ok)throw new Error(`render_http_${r.status}`);const ws=j?.binance_shared_ws??j?.realtime_ws_health?.binance_shared_ws??{};return {total:Number(ws.total_clients??0),max:Number(ws.max_total_clients??1000),rejected:Number(ws.rejected_capacity??0)+Number(ws.downstream_ip_capacity_rejections??0),raw_status:r.status};}finally{clearTimeout(t)}}
async function sendEmail(to:string[],subject:string,body:string,idempotencyKey:string){if(!to.length)return {ok:false,error:'no_admin_email',response_received:true};if(!RESEND_API_KEY||!EMAIL_FROM)return {ok:false,error:'resend_not_configured',response_received:true};const html=`<div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial,sans-serif;line-height:1.7;color:#111827"><h3>${esc(subject)}</h3><p>${esc(body)}</p><p style="color:#6b7280">仅发送给 Kaka Web3 管理员，不发送给普通用户。</p></div>`;try{const r=await fetch('https://api.resend.com/emails',{method:'POST',headers:{authorization:`Bearer ${RESEND_API_KEY}`,'content-type':'application/json','Idempotency-Key':idempotencyKey},body:JSON.stringify({from:EMAIL_FROM,to,subject,text:body,html})});const raw=await r.text();let providerId='';if(r.ok){try{providerId=String(JSON.parse(raw)?.id||'')}catch{}}return {ok:r.ok,status:r.status,error:r.ok?'':raw.slice(0,300),provider_id:providerId,response_received:true}}catch(e){return {ok:false,status:null,error:e instanceof Error?e.message:String(e),provider_id:'',response_received:false}}}
serve(async(req)=>{try{if(!['GET','POST'].includes(req.method))return out(405,{ok:false,schema:SCHEMA,error:'method_not_allowed'});const provided=(req.headers.get('x-kaka-sync-secret')||'').trim();if(!SYNC_SECRET||provided!==SYNC_SECRET)return out(401,{ok:false,schema:SCHEMA,error:'unauthorized'});if(!SUPABASE_URL||!SERVICE_ROLE_KEY)return out(500,{ok:false,schema:SCHEMA,error:'missing_supabase_env'});let dry=false;try{if(req.method==='POST'){const b=await req.json();dry=b?.dry_run===true}}catch{}const h=await fetchHealth();const {data,error}=await sb.rpc('kaka_edge_capacity_alert_decide',{p_total_clients:h.total,p_max_clients:h.max,p_rejected_capacity:h.rejected,p_dry_run:dry});if(error)return out(500,{ok:false,schema:SCHEMA,error:`decision:${error.message}`});let email:any={ok:true,skipped:true};let budget:any=null;
if(!dry&&data?.notify===true){
  const emails=Array.isArray(data?.admin_emails)?data.admin_emails.map(txt).filter(Boolean):[];
  const eventId=txt(data?.email_event_id);
  const budgetKey=eventId?`admin-capacity/${eventId}`:'';
  if(!budgetKey){
    email={ok:false,skipped:true,error:'missing_email_event_id'};
  }else{
    const {data:budgetRows,error:budgetErr}=await sb.rpc('app_claim_resend_platform_budget',{
      p_idempotency_key:budgetKey,
      p_category:'admin_capacity',
      p_units:Math.max(1,emails.length),
      p_metadata:{level:txt(data?.level),total_clients:h.total,max_clients:h.max}
    });
    budget=Array.isArray(budgetRows)&&budgetRows.length?budgetRows[0]:null;
    if(budgetErr||budget?.allowed!==true){
      email={ok:false,skipped:true,error:budgetErr?.message||budget?.reason||'resend_platform_budget_denied'};
    }else{
      email=await sendEmail(emails,txt(data?.title),txt(data?.body),`kaka-admin-capacity/${eventId}`);
      if(email?.ok===true){
        await sb.rpc('app_commit_resend_platform_budget',{p_idempotency_key:budgetKey,p_provider:'resend',p_provider_message_id:txt(email?.provider_id)});
      }else if(email?.response_received===true){
        await sb.rpc('app_release_resend_platform_budget',{p_idempotency_key:budgetKey,p_error:txt(email?.error)});
      }
    }
  }
}
const {data:platformBudget}=await sb.rpc('app_get_resend_platform_budget_status');
return out(200,{ok:true,schema:SCHEMA,dry_run:dry,total_clients:h.total,max_total_clients:h.max,percent:h.max>0?Math.round(h.total*10000/h.max)/100:0,rejected_capacity:h.rejected,decision:data,budget_claim:budget,platform_budget:platformBudget??null,email:{ok:Boolean(email?.ok),skipped:Boolean(email?.skipped),error:txt(email?.error)}})}catch(e){return out(500,{ok:false,schema:SCHEMA,error:e instanceof Error?e.message:String(e)})}});

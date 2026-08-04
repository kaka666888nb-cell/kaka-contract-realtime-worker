// Step650.8.15.72 / Step930: one scheduled CME official fetch, persistent shared expiry universe, zero user upstream reads.
import { inflateRaw } from 'node:zlib';
import { createConnection } from 'node:net';
import { promisify } from 'node:util';

const inflateRawAsync = promisify(inflateRaw);

const STEP_VERSION = '650.8.15.72.4';
const SUPABASE_URL = String(process.env.SUPABASE_URL || '').replace(/\/$/, '');
const LEGACY_SERVICE_ROLE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();
const KAKA_CALENDAR_REFRESH_KEY = String(process.env.KAKA_CALENDAR_REFRESH_KEY || '').trim();
const SECRET_KEYS = (() => {
  try {
    const raw = JSON.parse(process.env.SUPABASE_SECRET_KEYS || '{}');
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
    return Object.fromEntries(Object.entries(raw).map(([k,v]) => [k, String(v || '').trim()]).filter(([,v]) => v));
  } catch (_) { return {}; }
})();
const ADMIN_API_KEY = SECRET_KEYS.default || Object.values(SECRET_KEYS)[0] || LEGACY_SERVICE_ROLE_KEY;
const SNAPSHOT_TABLE = 'kaka_calendar_snapshot_cache';
const SHARED_KEY = 'expiries|shared_universe|cme_fprf_v3';
const SHARED_COVERAGE_VERSION = 'step930_cme_fprf_shared_universe_v4';
// CME's public web gateway currently lists the file but returns HTTP 400 for direct binary downloads.
// Use CME's documented public anonymous FTP production path instead. This is still one official backend fetch;
// normal users and Edge reads continue to use only the persisted Supabase snapshot.
const OFFICIAL_FTP_HOST = String(process.env.CME_FPRF_FTP_HOST || 'ftp.cmegroup.com').trim();
const OFFICIAL_FTP_PORT = Number(process.env.CME_FPRF_FTP_PORT || 21);
const OFFICIAL_FTP_PATH = String(process.env.CME_FPRF_FTP_PATH || '/pub/fprf/csv/cmeg.fut.prf.csv.zip').trim();
const OFFICIAL_SOURCE_URL = `ftp://${OFFICIAL_FTP_HOST}${OFFICIAL_FTP_PATH}`;
const OFFICIAL_WEB_DIRECTORY_URL = 'https://www.cmegroup.com/ftp/fprf/csv/';
const SOURCE_HELP_URL = 'https://www.cmegroup.com/clearing/files/cme-group-product-reference-file-futures.pdf';
const REFRESH_MS = 6 * 60 * 60 * 1000;
const BLOCK_COOLDOWN_MS = 24 * 60 * 60 * 1000;
const MAX_COMPRESSED_BYTES = 64 * 1024 * 1024;
const MAX_UNCOMPRESSED_BYTES = 180 * 1024 * 1024;
const FTP_CONNECT_TIMEOUT_MS = 8_000;
const FTP_TRANSFER_TIMEOUT_MS = 45_000;
const FTP_OVERALL_TIMEOUT_MS = 60_000;
const FTP_MAX_ATTEMPTS = 3;
const FTP_RETRY_DELAYS_MS = [1500, 4000];
const MAX_SOURCE_LINES = 500_000;
const EVENT_PAST_DAYS = 45;
const EVENT_FUTURE_DAYS = 3 * 366;

const PRODUCT_IDENTITIES={
  ES:{zh:"E-mini标普500指数期货",en:"E-mini S&P 500 Futures",sectorKey:"equity_index",sectorZh:"股票指数",sectorEn:"Equity indexes",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  MES:{zh:"微型E-mini标普500指数期货",en:"Micro E-mini S&P 500 Futures",sectorKey:"equity_index",sectorZh:"股票指数",sectorEn:"Equity indexes",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  NQ:{zh:"E-mini纳斯达克100指数期货",en:"E-mini Nasdaq-100 Futures",sectorKey:"equity_index",sectorZh:"股票指数",sectorEn:"Equity indexes",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  MNQ:{zh:"微型E-mini纳斯达克100指数期货",en:"Micro E-mini Nasdaq-100 Futures",sectorKey:"equity_index",sectorZh:"股票指数",sectorEn:"Equity indexes",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  YM:{zh:"E-mini道琼斯工业平均指数期货",en:"E-mini Dow Futures",sectorKey:"equity_index",sectorZh:"股票指数",sectorEn:"Equity indexes",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  MYM:{zh:"微型E-mini道琼斯指数期货",en:"Micro E-mini Dow Futures",sectorKey:"equity_index",sectorZh:"股票指数",sectorEn:"Equity indexes",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  RTY:{zh:"E-mini罗素2000指数期货",en:"E-mini Russell 2000 Futures",sectorKey:"equity_index",sectorZh:"股票指数",sectorEn:"Equity indexes",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  M2K:{zh:"微型E-mini罗素2000指数期货",en:"Micro E-mini Russell 2000 Futures",sectorKey:"equity_index",sectorZh:"股票指数",sectorEn:"Equity indexes",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  ZB:{zh:"30年期美国国债期货",en:"30-Year U.S. Treasury Bond Futures",sectorKey:"rates",sectorZh:"利率与债券",sectorEn:"Rates & bonds",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  ZN:{zh:"10年期美国国债期货",en:"10-Year U.S. Treasury Note Futures",sectorKey:"rates",sectorZh:"利率与债券",sectorEn:"Rates & bonds",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  ZF:{zh:"5年期美国国债期货",en:"5-Year U.S. Treasury Note Futures",sectorKey:"rates",sectorZh:"利率与债券",sectorEn:"Rates & bonds",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  ZT:{zh:"2年期美国国债期货",en:"2-Year U.S. Treasury Note Futures",sectorKey:"rates",sectorZh:"利率与债券",sectorEn:"Rates & bonds",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  UB:{zh:"超长期美国国债期货",en:"Ultra U.S. Treasury Bond Futures",sectorKey:"rates",sectorZh:"利率与债券",sectorEn:"Rates & bonds",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  SR3:{zh:"三个月SOFR期货",en:"Three-Month SOFR Futures",sectorKey:"rates",sectorZh:"利率与债券",sectorEn:"Rates & bonds",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  CL:{zh:"WTI原油期货",en:"WTI Crude Oil Futures",sectorKey:"energy",sectorZh:"能源",sectorEn:"Energy",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  MCL:{zh:"微型WTI原油期货",en:"Micro WTI Crude Oil Futures",sectorKey:"energy",sectorZh:"能源",sectorEn:"Energy",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  NG:{zh:"亨利港天然气期货",en:"Henry Hub Natural Gas Futures",sectorKey:"energy",sectorZh:"能源",sectorEn:"Energy",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  GC:{zh:"黄金期货",en:"Gold Futures",sectorKey:"metals",sectorZh:"金属",sectorEn:"Metals",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  MGC:{zh:"微型黄金期货",en:"Micro Gold Futures",sectorKey:"metals",sectorZh:"金属",sectorEn:"Metals",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  SI:{zh:"白银期货",en:"Silver Futures",sectorKey:"metals",sectorZh:"金属",sectorEn:"Metals",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  HG:{zh:"铜期货",en:"Copper Futures",sectorKey:"metals",sectorZh:"金属",sectorEn:"Metals",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  BTC:{zh:"比特币期货",en:"Bitcoin Futures",sectorKey:"crypto",sectorZh:"加密资产",sectorEn:"Crypto",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  MBT:{zh:"微型比特币期货",en:"Micro Bitcoin Futures",sectorKey:"crypto",sectorZh:"加密资产",sectorEn:"Crypto",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  ETH:{zh:"以太坊期货",en:"Ether Futures",sectorKey:"crypto",sectorZh:"加密资产",sectorEn:"Crypto",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  MET:{zh:"微型以太坊期货",en:"Micro Ether Futures",sectorKey:"crypto",sectorZh:"加密资产",sectorEn:"Crypto",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  "6E":{zh:"欧元兑美元期货",en:"Euro FX Futures",sectorKey:"fx",sectorZh:"外汇",sectorEn:"FX",countryCode:"EU",countryZh:"欧元区",countryEn:"Euro area"},
  "6J":{zh:"日元兑美元期货",en:"Japanese Yen Futures",sectorKey:"fx",sectorZh:"外汇",sectorEn:"FX",countryCode:"JP",countryZh:"日本",countryEn:"Japan"},
  "6B":{zh:"英镑兑美元期货",en:"British Pound Futures",sectorKey:"fx",sectorZh:"外汇",sectorEn:"FX",countryCode:"GB",countryZh:"英国",countryEn:"United Kingdom"},
  "6A":{zh:"澳元兑美元期货",en:"Australian Dollar Futures",sectorKey:"fx",sectorZh:"外汇",sectorEn:"FX",countryCode:"AU",countryZh:"澳大利亚",countryEn:"Australia"},
  "6C":{zh:"加元兑美元期货",en:"Canadian Dollar Futures",sectorKey:"fx",sectorZh:"外汇",sectorEn:"FX",countryCode:"CA",countryZh:"加拿大",countryEn:"Canada"},
  "6S":{zh:"瑞郎兑美元期货",en:"Swiss Franc Futures",sectorKey:"fx",sectorZh:"外汇",sectorEn:"FX",countryCode:"CH",countryZh:"瑞士",countryEn:"Switzerland"},
  ZC:{zh:"玉米期货",en:"Corn Futures",sectorKey:"agriculture",sectorZh:"农产品",sectorEn:"Agriculture",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  ZW:{zh:"小麦期货",en:"Wheat Futures",sectorKey:"agriculture",sectorZh:"农产品",sectorEn:"Agriculture",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  ZS:{zh:"大豆期货",en:"Soybean Futures",sectorKey:"agriculture",sectorZh:"农产品",sectorEn:"Agriculture",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  LE:{zh:"活牛期货",en:"Live Cattle Futures",sectorKey:"agriculture",sectorZh:"农产品",sectorEn:"Agriculture",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  HE:{zh:"瘦肉猪期货",en:"Lean Hogs Futures",sectorKey:"agriculture",sectorZh:"农产品",sectorEn:"Agriculture",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  KC:{zh:"咖啡期货",en:"Coffee Futures",sectorKey:"agriculture",sectorZh:"农产品",sectorEn:"Agriculture",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  SB:{zh:"原糖期货",en:"Sugar No. 11 Futures",sectorKey:"agriculture",sectorZh:"农产品",sectorEn:"Agriculture",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  CT:{zh:"棉花期货",en:"Cotton Futures",sectorKey:"agriculture",sectorZh:"农产品",sectorEn:"Agriculture",countryCode:"US",countryZh:"美国",countryEn:"United States"},
  LBR:{zh:"木材期货",en:"Lumber Futures",sectorKey:"agriculture",sectorZh:"农产品",sectorEn:"Agriculture",countryCode:"US",countryZh:"美国",countryEn:"United States"},
};
const COUNTRY_OPTIONS=[...new Map(Object.values(PRODUCT_IDENTITIES).map((item)=>[item.countryCode,{code:item.countryCode,name_zh:item.countryZh,name_en:item.countryEn}])).values()];
const SECTOR_OPTIONS=[...new Map(Object.values(PRODUCT_IDENTITIES).map((item)=>[item.sectorKey,{key:item.sectorKey,name_zh:item.sectorZh,name_en:item.sectorEn}])).values()];

const state = {
  started: false,
  refresh_inflight: false,
  snapshot_ready: false,
  snapshot_fetched_at: '',
  snapshot_event_count: 0,
  source_row_count: 0,
  matched_benchmark_row_count: 0,
  official_requests_started: 0,
  official_requests_succeeded: 0,
  official_requests_failed: 0,
  last_ftp_attempt_count: 0,
  last_ftp_attempt_errors: [],
  ftp_retry_limit: FTP_MAX_ATTEMPTS,
  last_error: '',
  last_reason: '',
  blocked_until: '',
  collector_transport: 'render_shared_single_official_fetch',
  official_transport: 'cme_public_anonymous_passive_ftp',
  official_ftp_host: OFFICIAL_FTP_HOST,
  official_ftp_path: OFFICIAL_FTP_PATH,
  user_read_upstream_requests: 0,
  stage: 'idle',
  last_duration_ms: 0,
  scheduler_active: false,
  next_scheduled_refresh_at: '',
  startup_auto_fetch: false,
  main_process_safety: 'bounded_async_parse_no_startup_upstream_fetch',
};
let refreshPromise = null;
let scheduleTimer = null;

function text(value) { return String(value ?? '').trim(); }
function cleanValue(value) {
  const raw = text(value);
  return ['n/a','na','--','-','null','undefined'].includes(raw.toLowerCase()) ? '' : raw;
}
function civilDate(y,m,d) { return `${String(y).padStart(4,'0')}-${String(m).padStart(2,'0')}-${String(d).padStart(2,'0')}`; }
function dateParts(value) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!m) return null;
  const y=Number(m[1]),mo=Number(m[2]),d=Number(m[3]);
  const dt=new Date(Date.UTC(y,mo-1,d));
  return dt.getUTCFullYear()===y&&dt.getUTCMonth()+1===mo&&dt.getUTCDate()===d?[y,mo,d]:null;
}
function normalizeDate(value) {
  const raw=cleanValue(value); if(!raw) return '';
  if(dateParts(raw)) return raw;
  let m=/^(\d{4})(\d{2})(\d{2})$/.exec(raw); if(m) return `${m[1]}-${m[2]}-${m[3]}`;
  m=/^(\d{4})-(\d{2})-(\d{2})T/.exec(raw); if(m) return `${m[1]}-${m[2]}-${m[3]}`;
  m=/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(raw); if(m) return civilDate(Number(m[3]),Number(m[1]),Number(m[2]));
  return '';
}
function parseCsvLine(line) {
  const values=[]; let current='', quoted=false;
  for(let i=0;i<line.length;i++){
    const ch=line[i];
    if(ch==='"'){ if(quoted&&line[i+1]==='"'){current+='"';i++;} else quoted=!quoted; }
    else if(ch===','&&!quoted){ values.push(current); current=''; }
    else current+=ch;
  }
  values.push(current); return values;
}
function parseCsv(raw) {
  const lines=raw.replace(/^\uFEFF/,'').split(/\r?\n/).filter((line)=>line.trim()!=='');
  if(lines.length<2) return [];
  const headers=parseCsvLine(lines[0]).map((value)=>value.trim());
  const rows=[];
  for(let i=1;i<lines.length;i++){
    const values=parseCsvLine(lines[i]); const row={};
    for(let j=0;j<headers.length;j++) row[headers[j]]=cleanValue(values[j]);
    rows.push(row);
  }
  return rows;
}

async function unzipFirstCsv(input) {
  const buffer = Buffer.isBuffer(input) ? input : Buffer.from(input);
  const min = Math.max(0, buffer.length - 65557);
  let eocd=-1;
  for(let i=buffer.length-22;i>=min;i--){ if(buffer.readUInt32LE(i)===0x06054b50){eocd=i;break;} }
  if(eocd<0) throw new Error('zip_eocd_missing');
  const entries=buffer.readUInt16LE(eocd+10);
  const centralOffset=buffer.readUInt32LE(eocd+16);
  let offset=centralOffset;
  for(let i=0;i<entries;i++){
    if(buffer.readUInt32LE(offset)!==0x02014b50) throw new Error('zip_central_header_invalid');
    const method=buffer.readUInt16LE(offset+10);
    const compressedSize=buffer.readUInt32LE(offset+20);
    const uncompressedSize=buffer.readUInt32LE(offset+24);
    const nameLen=buffer.readUInt16LE(offset+28);
    const extraLen=buffer.readUInt16LE(offset+30);
    const commentLen=buffer.readUInt16LE(offset+32);
    const localOffset=buffer.readUInt32LE(offset+42);
    const name=buffer.subarray(offset+46,offset+46+nameLen).toString('utf8');
    if(name.toLowerCase().endsWith('.csv')){
      if(uncompressedSize>MAX_UNCOMPRESSED_BYTES) throw new Error(`zip_csv_too_large:${uncompressedSize}`);
      if(buffer.readUInt32LE(localOffset)!==0x04034b50) throw new Error('zip_local_header_invalid');
      const localNameLen=buffer.readUInt16LE(localOffset+26);
      const localExtraLen=buffer.readUInt16LE(localOffset+28);
      const dataStart=localOffset+30+localNameLen+localExtraLen;
      const compressed=buffer.subarray(dataStart,dataStart+compressedSize);
      let output;
      if(method===0) output=Buffer.from(compressed);
      else if(method===8) output=await inflateRawAsync(compressed, { maxOutputLength: MAX_UNCOMPRESSED_BYTES });
      else throw new Error(`zip_compression_unsupported:${method}`);
      if(output.length>MAX_UNCOMPRESSED_BYTES) throw new Error(`zip_csv_too_large:${output.length}`);
      return output.toString('utf8');
    }
    offset += 46 + nameLen + extraLen + commentLen;
  }
  throw new Error('zip_csv_entry_missing');
}

function ftpReplyReader(socket) {
  let buffer = '';
  let multilineCode = '';
  let multilineLines = [];
  const queued = [];
  const waiters = [];
  let terminalError = null;

  const deliver = (reply) => {
    const waiter = waiters.shift();
    if (waiter) waiter.resolve(reply);
    else queued.push(reply);
  };
  const fail = (error) => {
    if (terminalError) return;
    terminalError = error instanceof Error ? error : new Error(String(error));
    while (waiters.length) waiters.shift().reject(terminalError);
  };
  const consumeLine = (line) => {
    if (multilineCode) {
      multilineLines.push(line);
      if (line.startsWith(`${multilineCode} `)) {
        const code = Number(multilineCode);
        const message = multilineLines.join('\n');
        multilineCode = '';
        multilineLines = [];
        deliver({ code, message });
      }
      return;
    }
    const match = /^(\d{3})([- ])(.*)$/.exec(line);
    if (!match) return;
    if (match[2] === '-') {
      multilineCode = match[1];
      multilineLines = [line];
      return;
    }
    deliver({ code: Number(match[1]), message: line });
  };

  socket.setEncoding('utf8');
  socket.on('data', (chunk) => {
    buffer += chunk;
    while (true) {
      const index = buffer.indexOf('\n');
      if (index < 0) break;
      const line = buffer.slice(0, index).replace(/\r$/, '');
      buffer = buffer.slice(index + 1);
      consumeLine(line);
    }
  });
  socket.on('error', fail);
  socket.on('close', () => fail(new Error('cme_ftp_control_closed')));

  return {
    next(timeoutMs = FTP_TRANSFER_TIMEOUT_MS) {
      if (queued.length) return Promise.resolve(queued.shift());
      if (terminalError) return Promise.reject(terminalError);
      return new Promise((resolve, reject) => {
        const waiter = { resolve, reject };
        waiters.push(waiter);
        const timer = setTimeout(() => {
          const index = waiters.indexOf(waiter);
          if (index >= 0) waiters.splice(index, 1);
          reject(new Error('cme_ftp_reply_timeout'));
        }, timeoutMs);
        const originalResolve = waiter.resolve;
        const originalReject = waiter.reject;
        waiter.resolve = (value) => { clearTimeout(timer); originalResolve(value); };
        waiter.reject = (error) => { clearTimeout(timer); originalReject(error); };
      });
    },
  };
}

function expectFtp(reply, allowed, label) {
  if (!reply || !allowed.includes(reply.code)) {
    throw new Error(`cme_ftp_${label}_${reply?.code || 'missing'}:${text(reply?.message).slice(0, 220)}`);
  }
  return reply;
}

function connectSocket({ host, port, timeoutMs, label }) {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ host, port });
    let settled = false;
    const finish = (error) => {
      if (settled) return;
      settled = true;
      socket.removeListener('connect', onConnect);
      socket.removeListener('error', onError);
      socket.removeListener('timeout', onTimeout);
      if (error) {
        socket.destroy();
        reject(error);
      } else {
        socket.setTimeout(0);
        resolve(socket);
      }
    };
    const onConnect = () => finish();
    const onError = (error) => finish(new Error(`cme_ftp_${label}_connect:${error?.message || error}`));
    const onTimeout = () => finish(new Error(`cme_ftp_${label}_connect_timeout`));
    socket.once('connect', onConnect);
    socket.once('error', onError);
    socket.once('timeout', onTimeout);
    socket.setTimeout(timeoutMs);
  });
}

function parseEpsvPort(message) {
  const match = /\(\|\|\|(\d+)\|\)/.exec(message);
  const port = Number(match?.[1] || 0);
  return Number.isInteger(port) && port > 0 && port <= 65535 ? port : 0;
}

function parsePasvEndpoint(message, fallbackHost) {
  const match = /\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)/.exec(message);
  if (!match) return null;
  const values = match.slice(1).map(Number);
  const candidateHost = values.slice(0, 4).join('.');
  const port = values[4] * 256 + values[5];
  if (!Number.isInteger(port) || port <= 0 || port > 65535) return null;
  const privateOrInvalid = /^(?:0\.|10\.|127\.|169\.254\.|192\.168\.|172\.(?:1[6-9]|2\d|3[01])\.)/.test(candidateHost);
  return { host: privateOrInvalid ? fallbackHost : candidateHost, port };
}

function readDataSocket(socket, maxBytes) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    let settled = false;
    const finish = (error) => {
      if (settled) return;
      settled = true;
      socket.removeAllListeners();
      if (error) {
        socket.destroy();
        reject(error);
      } else {
        resolve(Buffer.concat(chunks, total));
      }
    };
    socket.setTimeout(FTP_TRANSFER_TIMEOUT_MS);
    socket.on('data', (chunk) => {
      total += chunk.length;
      if (total > maxBytes) {
        finish(new Error(`cme_ftp_body_too_large:${total}`));
        return;
      }
      chunks.push(chunk);
    });
    socket.once('end', () => finish());
    socket.once('error', (error) => finish(new Error(`cme_ftp_data_error:${error?.message || error}`)));
    socket.once('timeout', () => finish(new Error('cme_ftp_data_timeout')));
  });
}

function ftpTimestampToIso(reply) {
  const match = /213\s+(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/.exec(text(reply?.message));
  if (!match) return '';
  const iso = `${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6]}Z`;
  return Number.isFinite(Date.parse(iso)) ? iso : '';
}


async function downloadOfficialFtpFileOnce({ preferPasv = false } = {}) {
  let control = null;
  let dataSocket = null;
  let deadlineTimer = null;
  const task = (async () => {
    try {
      control = await connectSocket({ host: OFFICIAL_FTP_HOST, port: OFFICIAL_FTP_PORT, timeoutMs: FTP_CONNECT_TIMEOUT_MS, label: 'control' });
      control.setKeepAlive(true, 15_000);
      const replies = ftpReplyReader(control);
      expectFtp(await replies.next(FTP_CONNECT_TIMEOUT_MS), [220], 'greeting');

      const command = async (line, allowed, label, timeoutMs = FTP_CONNECT_TIMEOUT_MS) => {
        control.write(`${line}\r\n`);
        return expectFtp(await replies.next(timeoutMs), allowed, label);
      };

      let login = await command('USER anonymous', [230, 331], 'user');
      if (login.code === 331) login = await command('PASS kakaweb3@kakaweb3.com', [230, 202], 'password');
      await command('TYPE I', [200], 'binary_type');

      let expectedSize = 0;
      try {
        const sizeReply = await command(`SIZE ${OFFICIAL_FTP_PATH}`, [213], 'size');
        expectedSize = Number(/213\s+(\d+)/.exec(sizeReply.message)?.[1] || 0);
        if (expectedSize > MAX_COMPRESSED_BYTES) throw new Error(`cme_ftp_file_too_large:${expectedSize}`);
      } catch (error) {
        if (/file_too_large/.test(String(error?.message || error))) throw error;
        expectedSize = 0;
      }

      let lastModified = '';
      try {
        const mdtmReply = await command(`MDTM ${OFFICIAL_FTP_PATH}`, [213], 'mdtm');
        lastModified = ftpTimestampToIso(mdtmReply);
      } catch (_) {}

      let passiveHost = OFFICIAL_FTP_HOST;
      let passivePort = 0;
      if (!preferPasv) {
        control.write('EPSV\r\n');
        const epsv = await replies.next(FTP_CONNECT_TIMEOUT_MS);
        if (epsv.code === 229) passivePort = parseEpsvPort(epsv.message);
      }
      if (!passivePort) {
        const pasv = await command('PASV', [227], 'pasv');
        const endpoint = parsePasvEndpoint(pasv.message, OFFICIAL_FTP_HOST);
        if (!endpoint) throw new Error(`cme_ftp_pasv_parse:${text(pasv.message).slice(0, 220)}`);
        passiveHost = endpoint.host;
        passivePort = endpoint.port;
      }

      dataSocket = await connectSocket({ host: passiveHost, port: passivePort, timeoutMs: FTP_CONNECT_TIMEOUT_MS, label: 'data' });
      // Convert the data-socket rejection to an outcome immediately. This prevents Node's
      // unhandled-rejection policy from terminating the Render process if CME resets the
      // data channel before the control channel has returned the 150 reply.
      const dataOutcomePromise = readDataSocket(dataSocket, MAX_COMPRESSED_BYTES).then(
        (bytes) => ({ bytes, error: null }),
        (error) => ({ bytes: null, error }),
      );
      control.write(`RETR ${OFFICIAL_FTP_PATH}\r\n`);
      expectFtp(await replies.next(FTP_CONNECT_TIMEOUT_MS), [125, 150], 'retr_start');
      const outcome = await dataOutcomePromise;
      if (outcome.error) throw outcome.error;
      const bytes = outcome.bytes;
      dataSocket = null;
      expectFtp(await replies.next(FTP_TRANSFER_TIMEOUT_MS), [226, 250], 'retr_complete');
      if (expectedSize && bytes.length !== expectedSize) throw new Error(`cme_ftp_size_mismatch:${bytes.length}/${expectedSize}`);
      if (bytes.length < 4) throw new Error(`cme_ftp_body_too_small:${bytes.length}`);
      try { control.write('QUIT\r\n'); } catch (_) {}
      return { bytes, lastModified, passiveMode: preferPasv ? 'PASV' : 'EPSV_or_PASV' };
    } finally {
      if (dataSocket) dataSocket.destroy();
      if (control) control.destroy();
    }
  })();

  const deadline = new Promise((_, reject) => {
    deadlineTimer = setTimeout(() => {
      const error = new Error('cme_ftp_overall_timeout');
      try { if (dataSocket) dataSocket.destroy(error); } catch (_) {}
      try { if (control) control.destroy(error); } catch (_) {}
      reject(error);
    }, FTP_OVERALL_TIMEOUT_MS);
  });

  try {
    return await Promise.race([task, deadline]);
  } finally {
    if (deadlineTimer) clearTimeout(deadlineTimer);
    try { if (dataSocket) dataSocket.destroy(); } catch (_) {}
    try { if (control) control.destroy(); } catch (_) {}
  }
}

function retryableFtpError(message) {
  return /ECONNRESET|ETIMEDOUT|EPIPE|ftp_(?:data|control|reply|overall|retr|pasv|epsv|size_mismatch)|connect|closed|timeout/i.test(message);
}

async function downloadOfficialFtpFile() {
  const errors = [];
  for (let attempt = 1; attempt <= FTP_MAX_ATTEMPTS; attempt += 1) {
    try {
      const result = await downloadOfficialFtpFileOnce({ preferPasv: attempt === 2 });
      state.last_ftp_attempt_count = attempt;
      state.last_ftp_attempt_errors = errors;
      return { ...result, attemptCount: attempt, attemptErrors: errors };
    } catch (error) {
      const message = String(error?.message || error);
      errors.push(`attempt_${attempt}:${message}`);
      state.last_ftp_attempt_count = attempt;
      state.last_ftp_attempt_errors = [...errors];
      if (attempt >= FTP_MAX_ATTEMPTS || !retryableFtpError(message)) {
        throw new Error(`cme_ftp_refresh_failed_after_${attempt}_attempts:${errors.join('|')}`);
      }
      await new Promise((resolve) => setTimeout(resolve, FTP_RETRY_DELAYS_MS[attempt - 1] || 4000));
    }
  }
  throw new Error(`cme_ftp_refresh_failed:${errors.join('|')}`);
}

function rootCode(row) {
  const candidates=[row.Sym,row.GBXAlias,row.ClrAlias,row.TCCAlias,row.ITCAlias]
    .map((value)=>text(value).toUpperCase().replace(/\s+/g,''));
  for(const value of candidates) if(PRODUCT_IDENTITIES[value]) return value;
  for(const value of candidates){
    const match=/^([A-Z0-9]+?)[FGHJKMNQUVXZ]\d{1,4}$/.exec(value);
    if(match&&PRODUCT_IDENTITIES[match[1]]) return match[1];
  }
  return '';
}
function fprfDate(value) { return normalizeDate(value); }
function authHeaders() {
  const headers={'content-type':'application/json','apikey':ADMIN_API_KEY};
  if(ADMIN_API_KEY&&!ADMIN_API_KEY.startsWith('sb_secret_')) headers.authorization=`Bearer ${ADMIN_API_KEY}`;
  return headers;
}
async function supabaseRest(path, init={}) {
  if(!SUPABASE_URL||!ADMIN_API_KEY) throw new Error('render_supabase_environment_missing');
  const headers={...authHeaders(),...(init.headers||{})};
  return fetch(`${SUPABASE_URL}/rest/v1/${path}`,{...init,headers});
}
async function readSnapshot() {
  const response=await supabaseRest(`${SNAPSHOT_TABLE}?calendar_key=eq.${encodeURIComponent(SHARED_KEY)}&select=payload,source_fetched_at,cached_at&limit=1`);
  if(!response.ok) return null;
  const rows=await response.json(); const row=rows[0];
  if(!row||!row.payload||typeof row.payload!=='object') return null;
  return row;
}
async function writeSnapshot(payload) {
  const now=new Date();
  const response=await supabaseRest(`${SNAPSHOT_TABLE}?on_conflict=calendar_key`,{
    method:'POST',headers:{Prefer:'resolution=merge-duplicates,return=minimal'},
    body:JSON.stringify({
      calendar_key:SHARED_KEY,calendar_type:'expiries',payload,
      source:'cme_group_official_fprf_shared_render',source_verified:true,
      source_fetched_at:payload.fetched_at,cached_at:now.toISOString(),
      expires_at:new Date(now.getTime()+30*24*60*60*1000).toISOString(),
      last_error:'',consecutive_errors:0,updated_at:now.toISOString(),
      metadata:{coverage_version:SHARED_COVERAGE_VERSION,collector_version:STEP_VERSION,collector_transport:'render_shared_single_official_fetch',official_transport:text(payload?.item?.official_transport)||'cme_public_anonymous_passive_ftp',user_read_upstream_requests:0},
    }),
  });
  if(!response.ok) throw new Error(`shared_snapshot_write_${response.status}:${(await response.text()).slice(0,220)}`);
}
async function fetchOfficialCsv() {
  state.official_requests_started++;
  try {
    const official = await downloadOfficialFtpFile();
    const bytes = official.bytes;
    if (bytes.length > MAX_COMPRESSED_BYTES) throw new Error(`cme_official_body_too_large:${bytes.length}`);
    const csv = bytes.readUInt32LE(0) === 0x04034b50 ? await unzipFirstCsv(bytes) : bytes.toString('utf8');
    if (!csv.trim()) throw new Error('cme_official_csv_empty');
    state.official_requests_succeeded++;
    state.last_transport = 'cme_public_anonymous_passive_ftp';
    return {
      csv,
      etag: '',
      lastModified: official.lastModified,
      sourceUrl: OFFICIAL_SOURCE_URL,
      transport: 'cme_public_anonymous_passive_ftp',
      ftp_attempt_count: official.attemptCount,
      ftp_attempt_errors: official.attemptErrors,
      ftp_passive_mode: official.passiveMode,
    };
  } catch (error) {
    state.official_requests_failed++;
    throw error;
  }
}

async function buildUniverse(csv, meta) {
  const raw = String(csv || '').replace(/^\uFEFF/, '');
  const firstBreak = raw.indexOf('\n');
  if (firstBreak < 0) throw new Error('cme_official_csv_header_missing');
  const headerLine = raw.slice(0, firstBreak).replace(/\r$/, '');
  const headers = parseCsvLine(headerLine).map((value) => value.trim());
  const index = Object.fromEntries(headers.map((name, i) => [name, i]));
  const required = ['Sym','GBXAlias','ClrAlias','TCCAlias','ITCAlias','LastTrdDt','FirstTrdDt','MatDt','Status','Tradable','MMY','Desc','Exch','SettlMeth'];
  for (const name of required) if (!(name in index)) throw new Error(`cme_official_csv_column_missing:${name}`);

  const get = (values, name) => cleanValue(values[index[name]]);
  const now = Date.now();
  const minDate = now - EVENT_PAST_DAYS * 86400000;
  const maxDate = now + EVENT_FUTURE_DAYS * 86400000;
  const unique = new Map();
  let lineStart = firstBreak + 1;
  let sourceRowCount = 0;
  let processedSinceYield = 0;

  while (lineStart < raw.length) {
    let lineEnd = raw.indexOf('\n', lineStart);
    if (lineEnd < 0) lineEnd = raw.length;
    const line = raw.slice(lineStart, lineEnd).replace(/\r$/, '');
    lineStart = lineEnd + 1;
    if (!line.trim()) continue;
    sourceRowCount++;
    if (sourceRowCount > MAX_SOURCE_LINES) throw new Error(`cme_official_csv_too_many_rows:${sourceRowCount}`);

    const values = parseCsvLine(line);
    const candidates = [get(values,'Sym'),get(values,'GBXAlias'),get(values,'ClrAlias'),get(values,'TCCAlias'),get(values,'ITCAlias')]
      .map((value)=>text(value).toUpperCase().replace(/\s+/g,''));
    let code = '';
    for (const value of candidates) {
      if (PRODUCT_IDENTITIES[value]) { code = value; break; }
    }
    if (!code) {
      for (const value of candidates) {
        const match=/^([A-Z0-9]+?)[FGHJKMNQUVXZ]\d{1,4}$/.exec(value);
        if(match&&PRODUCT_IDENTITIES[match[1]]) { code=match[1]; break; }
      }
    }
    if (!code) {
      if (++processedSinceYield >= 2500) { processedSinceYield = 0; await new Promise(setImmediate); }
      continue;
    }

    const lastTrade = fprfDate(get(values,'LastTrdDt'));
    if (!dateParts(lastTrade)) continue;
    const lastTradeMs = Date.parse(`${lastTrade}T12:00:00Z`);
    if (!Number.isFinite(lastTradeMs) || lastTradeMs < minDate || lastTradeMs > maxDate) continue;

    const status=get(values,'Status').toUpperCase();
    const tradable=get(values,'Tradable').toUpperCase();
    if(status&&!['A','ACTIVE','1'].includes(status)) continue;
    if(['N','NO','FALSE','0'].includes(tradable)) continue;

    const identity=PRODUCT_IDENTITIES[code];
    const firstTrade=fprfDate(get(values,'FirstTrdDt'));
    const maturity=fprfDate(get(values,'MatDt'));
    const contractMonth=get(values,'MMY');
    const contractCode=get(values,'GBXAlias')||get(values,'ClrAlias')||get(values,'Sym');
    const description=get(values,'Desc')||identity.en;
    const exchangeCode=get(values,'Exch')||'CME';
    const eventId=`cme_fprf_${lastTrade.replaceAll('-','')}_${exchangeCode}_${code}_${(contractMonth||contractCode).replace(/[^A-Za-z0-9._-]/g,'_')}`;
    unique.set(eventId,{
      event_id:eventId,product_code:code,product_name:description,product_name_zh:identity.zh,product_name_en:identity.en||description,
      product_translation_verified:true,contract_code:contractCode,contract_month:contractMonth,first_trade_date:firstTrade,
      last_trade_date:lastTrade,maturity_date:maturity,expiry_date:maturity||lastTrade,final_settlement_date:'',
      exchange_code:exchangeCode,exchange_name_zh:'CME集团',exchange_name_en:'CME Group',
      country_code:identity.countryCode,country_name_zh:identity.countryZh,country_name_en:identity.countryEn,
      sector_key:identity.sectorKey,sector_name_zh:identity.sectorZh,sector_name_en:identity.sectorEn,
      settlement_method:get(values,'SettlMeth'),source_verified:true,
      source_name:'CME Group Futures Product Reference File',source_name_zh:'CME集团期货产品参考文件',
      source_name_en:'CME Group Futures Product Reference File',source_url:meta.sourceUrl,official_timezone:'America/Chicago'
    });

    if (++processedSinceYield >= 2500) { processedSinceYield = 0; await new Promise(setImmediate); }
  }

  const finalEvents=[...unique.values()].sort((a,b)=>a.last_trade_date.localeCompare(b.last_trade_date)||a.product_code.localeCompare(b.product_code)||a.contract_month.localeCompare(b.contract_month));
  if (!finalEvents.length) throw new Error('cme_official_csv_no_matched_benchmark_events');
  const fetchedAt=new Date().toISOString();
  return {ok:true,source:'cme_group_official_fprf_shared_render',cache_status:'official_shared_render_snapshot',fetched_at:fetchedAt,item:{
    source_verified:true,time_identity_verified:true,coverage_version:SHARED_COVERAGE_VERSION,collector_version:STEP_VERSION,
    collector_transport:'render_shared_single_official_fetch',official_transport:meta.transport||'cme_public_anonymous_passive_ftp',
    official_request_count:1,user_read_upstream_requests:0,source_file_count:1,source_row_count:sourceRowCount,
    ftp_attempt_count:Number(meta.ftp_attempt_count||1),ftp_attempt_errors:Array.isArray(meta.ftp_attempt_errors)?meta.ftp_attempt_errors:[],ftp_passive_mode:text(meta.ftp_passive_mode),
    matched_benchmark_row_count:finalEvents.length,official_etag:meta.etag,official_last_modified:meta.lastModified,
    source_url:meta.sourceUrl,source_help_url:SOURCE_HELP_URL,country_filter_options:COUNTRY_OPTIONS,
    country_filter_option_count:COUNTRY_OPTIONS.length,country_filter_options_date_independent:true,
    sector_filter_options:SECTOR_OPTIONS,sector_filter_option_count:SECTOR_OPTIONS.length,events:finalEvents
  }};
}

function updateStateFromPayload(payload, reason='restore') {
  const item=payload?.item||{};
  state.snapshot_ready=item.source_verified===true&&item.coverage_version===SHARED_COVERAGE_VERSION&&Array.isArray(item.events);
  state.snapshot_fetched_at=text(payload?.fetched_at);
  state.snapshot_event_count=Array.isArray(item.events)?item.events.length:0;
  state.source_row_count=Number(item.source_row_count||0);
  state.matched_benchmark_row_count=Number(item.matched_benchmark_row_count||0);
  state.last_reason=reason;
}
async function refreshSharedUniverse({force=false,reason='cron'}={}) {
  if(refreshPromise) return refreshPromise;
  refreshPromise=(async()=>{
    const startedAt=Date.now();
    state.stage='read_snapshot';
    const existing=await readSnapshot().catch(()=>null);
    const existingFetched=Date.parse(existing?.payload?.fetched_at||existing?.source_fetched_at||'');
    if(existing?.payload) updateStateFromPayload(existing.payload,'restore_before_refresh');
    if(!force&&Number.isFinite(existingFetched)&&Date.now()-existingFetched<REFRESH_MS) return {ok:true,skipped:true,reason:'shared_snapshot_fresh',...getCmeExpirySharedHealth()};
    const blocked=Date.parse(state.blocked_until||'');
    if(!force&&Number.isFinite(blocked)&&blocked>Date.now()) return {ok:true,skipped:true,reason:'collector_upstream_cooldown',...getCmeExpirySharedHealth()};
    state.refresh_inflight=true; state.last_reason=reason; state.stage='download';
    try{
      const official=await fetchOfficialCsv();
      state.stage='parse';
      const payload=await buildUniverse(official.csv,official);
      state.stage='write_snapshot';
      await writeSnapshot(payload);
      state.last_error=''; state.blocked_until=''; updateStateFromPayload(payload,reason); state.stage='ready';
      return {ok:true,skipped:false,coverage_version:SHARED_COVERAGE_VERSION,fetched_at:payload.fetched_at,event_count:payload.item.events.length,source_row_count:payload.item.source_row_count,official_requests_started_this_refresh:1,collector_transport:'render_shared_single_official_fetch',official_transport:'cme_public_anonymous_passive_ftp',user_read_upstream_requests:0};
    }catch(error){
      const message=String(error?.message||error); state.last_error=message; state.stage='failed';
      if(/http_403|blocked|scraping/i.test(message)) state.blocked_until=new Date(Date.now()+BLOCK_COOLDOWN_MS).toISOString();
      else if(/ftp|timeout|connect|closed/i.test(message)) state.blocked_until=new Date(Date.now()+15*60*1000).toISOString();
      if(existing?.payload) return {ok:true,skipped:true,reason:'official_refresh_failed_kept_shared_snapshot',warning:message,...getCmeExpirySharedHealth()};
      throw error;
    }finally{state.refresh_inflight=false;state.last_duration_ms=Date.now()-startedAt;if(state.stage!=='ready'&&state.stage!=='failed')state.stage='idle';}
  })().finally(()=>{refreshPromise=null;});
  return refreshPromise;
}
function requestKey(req) {
  const authorization=text(req.headers.authorization);
  return text(req.headers['x-kaka-calendar-key']||req.headers.apikey)||(authorization.toLowerCase().startsWith('bearer ')?authorization.slice(7).trim():'');
}
function authorized(req) {
  const candidate=requestKey(req); if(!candidate) return false;
  return candidate===KAKA_CALENDAR_REFRESH_KEY||candidate===ADMIN_API_KEY||candidate===LEGACY_SERVICE_ROLE_KEY||Object.values(SECRET_KEYS).includes(candidate);
}
function sendJson(res,status,payload) {
  const body=Buffer.from(JSON.stringify(payload));
  res.writeHead(status,{'content-type':'application/json; charset=utf-8','cache-control':'no-store','content-length':String(body.length)});res.end(body);
}
async function readJson(req) {
  const chunks=[];let total=0;
  for await (const chunk of req){ total+=chunk.length;if(total>64*1024)throw new Error('request_body_too_large');chunks.push(chunk); }
  if(chunks.length===0)return{}; try{return JSON.parse(Buffer.concat(chunks).toString('utf8'));}catch(_){return{};}
}
export function getCmeExpirySharedHealth() { return {...state,step_version:STEP_VERSION,refresh_key_configured:Boolean(KAKA_CALENDAR_REFRESH_KEY),refresh_auth_mode:'dedicated_calendar_key_with_server_secret_fallback',shared_key:SHARED_KEY,shared_coverage_version:SHARED_COVERAGE_VERSION,refresh_interval_seconds:REFRESH_MS/1000,official_source_url:OFFICIAL_SOURCE_URL,official_web_directory_url:OFFICIAL_WEB_DIRECTORY_URL,health_path:'/api/calendar/cme-expiry/health',refresh_path:'/api/calendar/cme-expiry/refresh'}; }

export function startCmeExpirySharedCollector() {
  if(state.started)return;
  state.started=true;
  state.scheduler_active=true;
  state.startup_auto_fetch=false;
  state.next_scheduled_refresh_at=new Date(Date.now()+REFRESH_MS).toISOString();

  setTimeout(async()=>{
    try{
      state.stage='startup_restore';
      const existing=await readSnapshot();
      if(existing?.payload)updateStateFromPayload(existing.payload,'startup_restore');
      state.stage=state.snapshot_ready?'ready':'idle';
    }catch(error){
      state.last_error=String(error?.message||error);
      state.stage='idle';
    }
  },15000).unref();

  scheduleTimer=setInterval(async()=>{
    state.next_scheduled_refresh_at=new Date(Date.now()+REFRESH_MS).toISOString();
    try{await refreshSharedUniverse({force:false,reason:'render_6h_schedule'});}
    catch(error){state.last_error=String(error?.message||error);}
  },REFRESH_MS);
  scheduleTimer.unref();
}

export async function handleCmeExpirySharedCalendar(req,res,url) {
  if(url.pathname==='/api/calendar/cme-expiry/health'&&req.method==='GET'){sendJson(res,200,{ok:true,...getCmeExpirySharedHealth()});return true;}
  if(url.pathname==='/api/calendar/cme-expiry/refresh'&&req.method==='POST'){
    if(!authorized(req)){sendJson(res,401,{ok:false,error:'service_role_required'});return true;}
    try{const body=await readJson(req);const result=await refreshSharedUniverse({force:body.force===true,reason:text(body.reason)||'cron'});sendJson(res,200,result);}
    catch(error){sendJson(res,503,{ok:false,error:'cme_expiry_shared_refresh_failed',message:String(error?.message||error),health:getCmeExpirySharedHealth()});}
    return true;
  }
  return false;
}

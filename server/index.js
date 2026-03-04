import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';
import compression from 'compression';
import helmet from 'helmet';
import cors from 'cors';
import rateLimit from 'express-rate-limit';
import { createServer } from 'http';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3141;
const isProd = process.env.NODE_ENV === 'production';

const connections = new Map();
const MAX_CONNECTIONS = 20;
const IDLE_TIMEOUT = 30 * 60 * 1000;
const runningQueries = new Map();
const executionConfigs = new Map();
const DEFAULT_EXEC_CONFIG = {
  mode: 'safe', maxTimeMS: 10000, maxResultSize: 1000,
  allowDiskUse: false, blockedOperators: ['$where'],
};
const auditLog = [];
const MAX_AUDIT = 500;

app.use(compression());
app.use(helmet({ contentSecurityPolicy: false, crossOriginEmbedderPolicy: false }));
app.use(cors());
app.use(express.json({ limit: '16mb' }));
app.use((req, res, next) => {
  const t0 = Date.now();
  res.on('finish', () => {
    if (!req.path.startsWith('/api/')) return;
    const elapsed = Date.now() - t0;
    const connId = String(req.headers['x-connection-id'] || 'anon').slice(0, 8);
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl} ${res.statusCode} ${elapsed}ms conn=${connId}`);
  });
  next();
});

if (isProd) {
  app.use(express.static(join(__dirname, '..', 'dist'), { maxAge: '1h', etag: true }));
}

const apiLimiter = rateLimit({ windowMs: 60000, max: 200, message: { error: 'Too many requests.' }, standardHeaders: true, legacyHeaders: false });
app.use('/api/', apiLimiter);
const heavyLimiter = rateLimit({ windowMs: 60000, max: 30, message: { error: 'Too many heavy operations.' }, standardHeaders: true, legacyHeaders: false });

setInterval(() => {
  const now = Date.now();
  for (const [id, conn] of connections) {
    if (now - conn.lastActivity > IDLE_TIMEOUT) {
      conn.client.close().catch(() => {});
      connections.delete(id); executionConfigs.delete(id);
    }
  }
}, 60000);

function parseVersion(v) { const p = (v||'0.0.0').split('.').map(Number); return { major:p[0]||0, minor:p[1]||0, patch:p[2]||0 }; }
function versionAtLeast(v, maj, min=0) { return v.major > maj || (v.major === maj && v.minor >= min); }

function getCapabilities(v) {
  return {
    hasCountDocuments: versionAtLeast(v,3,6), hasEstimatedCount: versionAtLeast(v,4,0),
    hasMergeStage: versionAtLeast(v,4,2), hasUnionWith: versionAtLeast(v,4,4),
    hasStableApi: versionAtLeast(v,5,0), hasTimeSeries: versionAtLeast(v,5,0),
    hasClustered: versionAtLeast(v,5,3), hasDensifyFill: versionAtLeast(v,5,1),
    hasQueryableEncryption: versionAtLeast(v,7,0), hasAggFacet: versionAtLeast(v,3,4),
    hasAggLookup: versionAtLeast(v,3,2), hasChangeStreams: versionAtLeast(v,3,6),
    hasTransactions: versionAtLeast(v,4,0), hasWildcardIndexes: versionAtLeast(v,4,2),
  };
}

async function compatCount(col, filter, caps) {
  if (caps.hasCountDocuments) try { return await col.countDocuments(filter); } catch {}
  try { return await col.count(filter); } catch { return 0; }
}

async function compatEstCount(col, caps) {
  if (caps.hasEstimatedCount) try { return await col.estimatedDocumentCount(); } catch {}
  try { return await col.count({}); } catch { return 0; }
}

async function compatCollStats(db, name, caps) {
  try {
    const s = await db.command({ collStats: name });
    return { count:s.count??0, size:s.size??0, avgObjSize:s.avgObjSize??0, storageSize:s.storageSize??0, totalIndexSize:s.totalIndexSize??0, indexSizes:s.indexSizes??{}, nindexes:s.nindexes??0 };
  } catch (err) {
    try { return { count: await compatEstCount(db.collection(name), caps), size:0, avgObjSize:0, storageSize:0, totalIndexSize:0, nindexes:0 }; }
    catch { throw err; }
  }
}

function extractPermissionDetails(message = '') {
  const normalized = String(message).replace(/\s+/g, ' ').trim();
  const m = normalized.match(/not authorized on ([^\s]+) to execute command \{ ([^:]+): "([^"]+)"/i);
  if (m) {
    const [, dbName, command, collection] = m;
    return `Not authorized for "${command}" on ${dbName}.${collection}.`;
  }
  const m2 = normalized.match(/not authorized on ([^\s]+) to execute command/i);
  if (m2) return `Not authorized on database "${m2[1]}".`;
  return null;
}

function classifyError(err) {
  const m = String(err?.message || '');
  const lower = m.toLowerCase();
  const c = err?.code;
  if (lower.includes('authentication failed') || c === 18) return { type: 'auth', friendly: 'Authentication failed.', code: 401 };
  if (lower.includes('not authorized') || c === 13) return { type: 'permission', friendly: 'Insufficient permissions for this operation.', code: 403 };
  if (lower.includes('econnrefused') || lower.includes('econnreset')) return { type: 'network', friendly: 'Cannot reach MongoDB server.', code: 503 };
  if (lower.includes('enotfound')) return { type: 'network', friendly: 'Host not found. Check that the hostname resolves from this machine.', code: 503 };
  if (lower.includes('ssl') || lower.includes('tls')) return { type: 'tls', friendly: 'TLS/SSL error. Try ?tls=true or ?tls=false.', code: 400 };
  if (lower.includes('server selection timed out') || lower.includes('server selection timeout')) {
    return { type: 'topology', friendly: 'Cannot select a server — all replica set member hostnames must be reachable from this machine. Try enabling DirectConnect and connecting to one specific node.', code: 503 };
  }
  if (lower.includes('topology')) return { type: 'topology', friendly: 'Cluster topology error. If using a replica set, ensure all member hostnames resolve correctly from this machine.', code: 503 };
  if (c === 50 || lower.includes('operation exceeded time limit') || lower.includes('exceeded time limit') || lower.includes('timed out')) {
    return { type: 'timeout', friendly: 'Query timed out. Try filters or indexes.', code: 408 };
  }
  if (lower.includes('duplicate key') || c === 11000) return { type: 'duplicate', friendly: 'Duplicate key error.', code: 409 };
  return { type: 'unknown', friendly: m || 'Unexpected server error.', code: 500 };
}

function sendError(res, err, ctx = '') {
  const cl = classifyError(err);
  const raw = String(err?.message || '').replace(/\s+/g, ' ').trim();
  const details = cl.type === 'permission' ? (extractPermissionDetails(raw) || raw) : raw;
  const base = ctx ? `${ctx}: ${cl.friendly}` : cl.friendly;
  const error = cl.type === 'permission' && details ? `${base} ${details}` : base;
  console.error(`[${new Date().toISOString()}] [error:${cl.type}] ${ctx || 'request'} code=${err?.code ?? '-'} msg=${raw}`);
  res.status(cl.code).json({ error, errorType: cl.type, details, mongoCode: err?.code ?? null });
}

function getConnection(req, res, next) {
  const id = req.headers['x-connection-id'];
  if (!id || !connections.has(id)) return res.status(401).json({ error:'Not connected.', errorType:'auth' });
  const conn = connections.get(id);
  conn.lastActivity = Date.now();
  req.conn = conn; req.caps = conn.capabilities; req.connId = id;
  req.execConfig = executionConfigs.get(id) || { ...DEFAULT_EXEC_CONFIG };
  next();
}

function parseId(id) { try { if (/^[a-f0-9]{24}$/i.test(id)) return new ObjectId(id); } catch {} return id; }
function parseFilter(str) {
  try {
    const parsed = JSON.parse(str);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
    return transformFilter(parsed);
  } catch {
    return {};
  }
}
function parseFilterInput(input) {
  if (input && typeof input === 'object' && !Array.isArray(input)) return transformFilter(input);
  if (typeof input === 'string') {
    const parsed = parseFilter(input);
    if (Object.keys(parsed).length > 0) return parsed;
    try {
      const firstPass = JSON.parse(input);
      if (typeof firstPass === 'string') return parseFilter(firstPass);
    } catch {}
  }
  return {};
}
function transformFilter(obj) {
  if (obj === null || typeof obj !== 'object') return obj;
  if (Array.isArray(obj)) return obj.map(transformFilter);
  const r = {};
  for (const [k, v] of Object.entries(obj)) {
    if (k==='_id' && typeof v==='string' && /^[a-f0-9]{24}$/i.test(v)) r[k] = new ObjectId(v);
    else if (v && typeof v === 'object' && v.$oid) r[k] = new ObjectId(v.$oid);
    else r[k] = transformFilter(v);
  }
  return r;
}

function getTopologyInfo(hello = {}) {
  const setName = hello?.setName || null;
  const isPrimary = Boolean(hello?.isWritablePrimary ?? hello?.ismaster);
  const isSecondary = Boolean(hello?.secondary);
  const isArbiter = Boolean(hello?.arbiterOnly);
  const isMongos = hello?.msg === 'isdbgrid';
  let kind = 'standalone';
  let role = 'standalone';
  if (isMongos) {
    kind = 'sharded';
    role = 'mongos';
  } else if (setName) {
    kind = 'replicaSet';
    role = isPrimary ? 'primary' : (isSecondary ? 'secondary' : (isArbiter ? 'arbiter' : 'member'));
  }
  const hosts = hello?.hosts || null;
  const passives = hello?.passives || null;
  const allMembers = hosts && passives ? [...hosts, ...passives] : (hosts || passives || null);
  return { kind, role, setName, primary: hello?.primary || null, me: hello?.me || null, hosts: allMembers };
}

function audit(connId, action, details={}) {
  const conn = connections.get(connId);
  auditLog.push({ ts: Date.now(), connId, host: conn?.host||'?', action, ...details });
  if (auditLog.length > MAX_AUDIT) auditLog.shift();
}

function validatePipeline(pipeline, cfg) {
  for (const stage of pipeline) {
    const key = Object.keys(stage)[0];
    if ((cfg.blockedOperators||[]).includes(key)) throw new Error(`"${key}" is blocked in Safe Mode.`);
    if (key === '$match' && JSON.stringify(stage.$match).includes('$where') && cfg.blockedOperators.includes('$where'))
      throw new Error('$where is blocked in Safe Mode.');
  }
}

function analyzeFields(obj, prefix, schema) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return;
  for (const [key, val] of Object.entries(obj)) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (!schema[path]) schema[path] = { types:{}, count:0, hasNull:false, sample:null };
    schema[path].count++;
    const type = val===null?'null': Array.isArray(val)?'array': (val?.$oid)?'ObjectId': (val?.$date)?'Date': typeof val;
    schema[path].types[type] = (schema[path].types[type]||0) + 1;
    if (val===null) schema[path].hasNull = true;
    if (!schema[path].sample && val!==null && typeof val!=='object') schema[path].sample = String(val).slice(0,100);
    if (val && typeof val==='object' && !Array.isArray(val) && !val.$oid && !val.$date) analyzeFields(val, path, schema);
  }
}

function extractExplainSummary(explain) {
  const s = { executionTimeMs:0, totalDocsExamined:0, totalKeysExamined:0, nReturned:0, isCollScan:false, indexUsed:null, stages:[] };
  try {
    const es = explain.executionStats || {};
    s.executionTimeMs = es.executionTimeMillis||0;
    s.totalDocsExamined = es.totalDocsExamined||0;
    s.totalKeysExamined = es.totalKeysExamined||0;
    s.nReturned = es.nReturned||0;
    const ps = JSON.stringify(explain);
    s.isCollScan = ps.includes('COLLSCAN');
    if (!s.isCollScan) { const m = ps.match(/"indexName"\s*:\s*"([^"]+)"/); if (m) s.indexUsed = m[1]; }
    if (explain.stages) s.stages = explain.stages.map(st => Object.keys(st)[0]).filter(Boolean);
  } catch {}
  return s;
}

// ═══ ROUTES ═══

app.get('/api/health', (_, res) => res.json({ status:'ok', uptime:process.uptime(), connections:connections.size, memory:process.memoryUsage(), ts:new Date().toISOString() }));
app.get('/api/ready', (_, res) => res.json({ ready:true, connections:connections.size }));
app.get('/api/metrics', (_, res) => res.json({ uptime:process.uptime(), connections:connections.size, max:MAX_CONNECTIONS, memMB:Math.round(process.memoryUsage().rss/1048576), audit:auditLog.length, running:runningQueries.size }));

app.post('/api/connect', async (req, res) => {
  const { uri, options={} } = req.body;
  if (!uri) return res.status(400).json({ error:'URI required', errorType:'validation' });
  if (connections.size >= MAX_CONNECTIONS) return res.status(429).json({ error:`Max connections (${MAX_CONNECTIONS}) reached.`, errorType:'limit' });
  try {
    const markAsProduction = options.markAsProduction === true;
    const connectTimeoutMS = Math.max(1000, Math.min(parseInt(options.connectTimeoutMS) || 15000, 120000));
    const authUsername = typeof options.username === 'string' ? options.username.trim() : '';
    const hasAuth = Boolean(authUsername);
    const opts = {
      connectTimeoutMS, serverSelectionTimeoutMS:connectTimeoutMS, socketTimeoutMS:Math.max(connectTimeoutMS * 2, 30000),
      maxPoolSize:10, minPoolSize:1,
      ...(options.tls!==undefined&&{tls:options.tls}), ...(options.tlsAllowInvalidCertificates&&{tlsAllowInvalidCertificates:true}),
      ...(options.authSource&&{authSource:options.authSource}), ...(options.replicaSet&&{replicaSet:options.replicaSet}),
      ...(options.directConnection!==undefined&&{directConnection:options.directConnection}), ...(options.readPreference&&{readPreference:options.readPreference}),
      ...(hasAuth && { auth: { username: authUsername, password: String(options.password ?? '') } }),
    };
    const client = new MongoClient(uri, opts);
    await client.connect();
    let versionStr='unknown', version={major:0,minor:0,patch:0}, warnings=[];
    try { const bi = await client.db('admin').command({buildInfo:1}); versionStr=bi.version||'unknown'; version=parseVersion(versionStr); }
    catch { try {
      let h; try{h=await client.db('admin').command({hello:1})}catch{h=await client.db('admin').command({isMaster:1})}
      if(h.maxWireVersion!==undefined){const wm={0:'2.6',1:'2.6',2:'2.6',3:'3.0',4:'3.2',5:'3.4',6:'3.6',7:'4.0',8:'4.2',9:'4.4',10:'4.9',11:'5.0',12:'5.1',13:'5.3',14:'6.0',15:'6.1',16:'6.2',17:'7.0',18:'7.1',19:'7.2',20:'7.3',21:'8.0',22:'8.1'};versionStr=wm[h.maxWireVersion]||`wire-${h.maxWireVersion}`;version=parseVersion(versionStr)}
    } catch { warnings.push('Could not detect version. Legacy mode.'); }}
    const capabilities = getCapabilities(version);
    if(version.major>0&&version.major<3) warnings.push(`MongoDB ${versionStr} is very old.`);
    else if(version.major===3&&version.minor<6) warnings.push(`MongoDB ${versionStr}: Using legacy fallbacks.`);
    else if(version.major===3) warnings.push(`MongoDB ${versionStr}: 3.x compatibility mode.`);
    else if(version.major===4&&version.minor<4) warnings.push(`MongoDB ${versionStr}: Some stages unavailable.`);

    const isProduction = markAsProduction;
    let topology = { kind:'unknown', role:'unknown', setName:null, primary:null, me:null };
    try {
      const hello = await client.db('admin').command({hello:1}).catch(() => client.db('admin').command({isMaster:1}));
      topology = getTopologyInfo(hello || {});
    } catch {}

    try { await client.db('admin').command({ping:1}); }
    catch { try { await client.db(new URL(uri).pathname.slice(1)||'test').command({ping:1}); } catch { warnings.push('Ping failed — restricted permissions.'); }}

    const connId = Math.random().toString(36).slice(2)+Date.now().toString(36);
    let host='unknown';
    let username = hasAuth ? authUsername : '';
    try {
      // Handle multi-host URIs (new URL() fails on "host1:port,host2:port" syntax)
      const hostMatch = uri.match(/^mongodb(?:\+srv)?:\/\/(?:[^@]+@)?([^/?]+)/);
      if (hostMatch) host = hostMatch[1];
      if (!username) {
        const userMatch = uri.match(/^mongodb(?:\+srv)?:\/\/([^:@/]+)(?::[^@]*)?@/);
        if (userMatch) { try { username = decodeURIComponent(userMatch[1]); } catch { username = userMatch[1]; } }
      }
    } catch {
      host=uri.split('@').pop()?.split('/')[0]||'unknown';
    }
    // Extract readPreference from URI query params as fallback
    let effectiveReadPref = options.readPreference || null;
    if (!effectiveReadPref) {
      try { const m = uri.match(/[?&]readPreference=([^&]+)/i); if (m) effectiveReadPref = decodeURIComponent(m[1]); } catch {}
    }

    connections.set(connId, { client, uri, host, username, connectedAt:Date.now(), lastActivity:Date.now(), version, versionStr, capabilities, isProduction, topology });
    executionConfigs.set(connId, { ...DEFAULT_EXEC_CONFIG });
    audit(connId, 'connect', { host, user:username||'anonymous', version:versionStr });

    res.json({
      connectionId:connId, host, username:username||null, version:versionStr, isProduction, warnings, topology,
      readPreference: effectiveReadPref, ok:true,
      capabilities:{ countDocuments:capabilities.hasCountDocuments, estimatedCount:capabilities.hasEstimatedCount, changeStreams:capabilities.hasChangeStreams, transactions:capabilities.hasTransactions, aggregationFacet:capabilities.hasAggFacet, aggregationLookup:capabilities.hasAggLookup, wildcardIndexes:capabilities.hasWildcardIndexes, timeSeries:capabilities.hasTimeSeries, stableApi:capabilities.hasStableApi },
    });
  } catch (err) { sendError(res, err, 'Connection failed'); }
});

app.post('/api/disconnect', (req, res) => {
  const id = req.headers['x-connection-id'];
  if (id && connections.has(id)) { audit(id,'disconnect'); connections.get(id).client.close().catch(()=>{}); connections.delete(id); executionConfigs.delete(id); }
  res.json({ ok:true });
});

app.get('/api/execution-config', getConnection, (req, res) => res.json(req.execConfig));
app.put('/api/execution-config', getConnection, (req, res) => {
  const { mode, maxTimeMS, maxResultSize, allowDiskUse } = req.body;
  const c = executionConfigs.get(req.connId) || { ...DEFAULT_EXEC_CONFIG };
  if (mode==='safe'||mode==='power') c.mode=mode;
  if (typeof maxTimeMS==='number') c.maxTimeMS=Math.max(0,Math.min(300000,maxTimeMS));
  if (typeof maxResultSize==='number') c.maxResultSize=Math.max(1,Math.min(50000,maxResultSize));
  if (typeof allowDiskUse==='boolean') c.allowDiskUse=allowDiskUse;
  if (mode==='safe') { c.blockedOperators=['$where']; c.allowDiskUse=false; } else if (mode==='power') { c.blockedOperators=[]; }
  executionConfigs.set(req.connId, c);
  audit(req.connId, 'config_change', { mode:c.mode });
  res.json(c);
});

app.get('/api/status', getConnection, async (req, res) => {
  try {
    const admin = req.conn.client.db('admin');
    const r = { version:req.conn.versionStr, capabilities:req.conn.capabilities, isProduction:req.conn.isProduction, topology:req.conn.topology || null };
    try { r.buildInfo = await admin.command({buildInfo:1}); } catch {}
    try { const ss = await admin.command({serverStatus:1}); r.serverStatus = { host:ss.host, uptime:ss.uptime, connections:ss.connections, opcounters:ss.opcounters, mem:ss.mem, storageEngine:ss.storageEngine, repl:ss.repl?{setName:ss.repl.setName,hosts:ss.repl.hosts,primary:ss.repl.primary}:null }; } catch {}
    try { r.hello = await admin.command({hello:1}); } catch { try { r.hello = await admin.command({isMaster:1}); } catch {} }
    if (r.hello) r.topology = getTopologyInfo(r.hello);
    res.json(r);
  } catch (err) { sendError(res, err); }
});

app.get('/api/databases', getConnection, async (req, res) => {
  try {
    const r = await req.conn.client.db('admin').command({ listDatabases: 1, nameOnly: false, authorizedDatabases: true });
    res.json({ databases:(r.databases||[]).map(d=>({name:d.name,sizeOnDisk:d.sizeOnDisk,empty:d.empty})), totalSize:r.totalSize, version:req.conn.versionStr });
  } catch(err) {
    try {
      const r = await req.conn.client.db('admin').command({ listDatabases: 1, nameOnly: false });
      res.json({ databases:(r.databases||[]).map(d=>({name:d.name,sizeOnDisk:d.sizeOnDisk,empty:d.empty})), totalSize:r.totalSize, version:req.conn.versionStr });
    } catch {
      try {
        const n=new URL(req.conn.uri).pathname.slice(1);
        if(n) res.json({databases:[{name:n,sizeOnDisk:0,empty:false}],totalSize:0,version:req.conn.versionStr,warning:'Limited permissions.'});
        else throw err;
      }
      catch { sendError(res, err); }
    }
  }
});

app.post('/api/databases', getConnection, async (req, res) => {
  try { const{name}=req.body; if(!name||!/^[a-zA-Z0-9_-]+$/.test(name)) return res.status(400).json({error:'Invalid name',errorType:'validation'}); await req.conn.client.db(name).createCollection('_init'); await req.conn.client.db(name).collection('_init').drop().catch(()=>{}); audit(req.connId,'create_db',{db:name}); res.json({ok:true}); }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db', getConnection, async (req, res) => {
  try { await req.conn.client.db(req.params.db).dropDatabase(); audit(req.connId,'drop_db',{db:req.params.db}); res.json({ok:true}); }
  catch(err) { sendError(res, err); }
});

app.get('/api/databases/:db/collections', getConnection, async (req, res) => {
  try {
    const db = req.conn.client.db(req.params.db);
    const c = await db.listCollections({}, { nameOnly: true, authorizedCollections: true }).toArray();
    res.json({collections:c.map(x=>({name:x.name,type:x.type||'collection',options:x.options||{}})).sort((a,b)=>a.name.localeCompare(b.name))});
  }
  catch(err) {
    try {
      const c = await req.conn.client.db(req.params.db).listCollections().toArray();
      res.json({collections:c.map(x=>({name:x.name,type:x.type||'collection',options:x.options||{}})).sort((a,b)=>a.name.localeCompare(b.name))});
    } catch {
      try {
        const c=await req.conn.client.db(req.params.db).collections();
        res.json({collections:c.map(x=>({name:x.collectionName,type:'collection',options:{}})).sort((a,b)=>a.name.localeCompare(b.name))});
      } catch { sendError(res,err); }
    }
  }
});

app.post('/api/databases/:db/collections', getConnection, async (req, res) => {
  try { if(!req.body.name) return res.status(400).json({error:'Name required',errorType:'validation'}); await req.conn.client.db(req.params.db).createCollection(req.body.name); audit(req.connId,'create_col',{db:req.params.db,col:req.body.name}); res.json({ok:true}); }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db/collections/:col', getConnection, async (req, res) => {
  try { await req.conn.client.db(req.params.db).collection(req.params.col).drop(); audit(req.connId,'drop_col',{db:req.params.db,col:req.params.col}); res.json({ok:true}); }
  catch(err) { sendError(res, err); }
});

app.get('/api/databases/:db/collections/:col/stats', getConnection, async (req, res) => {
  try { res.json(await compatCollStats(req.conn.client.db(req.params.db), req.params.col, req.caps)); }
  catch(err) { sendError(res, err); }
});

app.get('/api/databases/:db/collections/:col/schema', getConnection, async (req, res) => {
  try {
    const col = req.conn.client.db(req.params.db).collection(req.params.col);
    const n = Math.min(parseInt(req.query.sample)||100, 500);
    const docs = await col.aggregate([{$sample:{size:n}}]).toArray();
    const schema = {};
    for (const d of docs) analyzeFields(d, '', schema);
    const fields = Object.entries(schema).map(([path,info])=>({ path, types:Object.entries(info.types).sort((a,b)=>b[1]-a[1]).map(([t,c])=>({type:t,count:c,pct:Math.round(c/docs.length*100)})), count:info.count, pct:Math.round(info.count/docs.length*100), hasNull:info.hasNull, sample:info.sample })).sort((a,b)=>b.count-a.count);
    res.json({ fields, sampleSize:docs.length });
  } catch(err) { sendError(res, err); }
});

app.get('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  const qid = Math.random().toString(36).slice(2), t0=Date.now();
  try {
    const col=req.conn.client.db(req.params.db).collection(req.params.col);
    const filter=parseFilter(req.query.filter||'{}'), sort=parseFilter(req.query.sort||'{}'), projection=parseFilter(req.query.projection||'{}');
    const hint = typeof req.query.hint === 'string' && req.query.hint.trim() && req.query.hint !== 'auto'
      ? req.query.hint.trim()
      : null;
    const skip=parseInt(req.query.skip)||0;
    const maxLim=req.execConfig.mode==='safe'?Math.min(req.execConfig.maxResultSize,1000):50000;
    const limit=Math.min(parseInt(req.query.limit)||50, maxLim);
    const fo={projection}; if(req.execConfig.mode==='safe' && req.execConfig.maxTimeMS>0) fo.maxTimeMS=req.execConfig.maxTimeMS;
    runningQueries.set(qid, {connId:req.connId,t0,type:'find'});
    let cursor = col.find(filter,fo);
    if (hint) cursor = cursor.hint(hint);
    const [documents, total] = await Promise.all([cursor.sort(sort).skip(skip).limit(limit).toArray(), compatCount(col,filter,req.caps)]);
    const elapsed=Date.now()-t0;
    audit(req.connId,'query',{db:req.params.db,col:req.params.col,elapsed,count:documents.length});
    const r={documents,total,_elapsed:elapsed}; if(elapsed>5000) r._slow=true; res.json(r);
  } catch(err) { sendError(res, err); } finally { runningQueries.delete(qid); }
});

app.get('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try { const d=await req.conn.client.db(req.params.db).collection(req.params.col).findOne({_id:parseId(req.params.id)}); if(!d) return res.status(404).json({error:'Not found',errorType:'not_found'}); res.json(d); }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  try { const r=await req.conn.client.db(req.params.db).collection(req.params.col).insertOne(req.body.document); audit(req.connId,'insert',{db:req.params.db,col:req.params.col}); res.json({insertedId:r.insertedId,ok:true}); }
  catch(err) { sendError(res, err); }
});

app.put('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try { const r=await req.conn.client.db(req.params.db).collection(req.params.col).replaceOne({_id:parseId(req.params.id)},{...req.body.update,_id:parseId(req.params.id)}); audit(req.connId,'update',{db:req.params.db,col:req.params.col}); res.json({modifiedCount:r.modifiedCount,ok:true}); }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try { const r=await req.conn.client.db(req.params.db).collection(req.params.col).deleteOne({_id:parseId(req.params.id)}); audit(req.connId,'delete',{db:req.params.db,col:req.params.col}); res.json({deletedCount:r.deletedCount,ok:true}); }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  try { const f=parseFilter(JSON.stringify(req.body.filter||{})); const r=await req.conn.client.db(req.params.db).collection(req.params.col).deleteMany(f); audit(req.connId,'delete_many',{db:req.params.db,col:req.params.col,count:r.deletedCount}); res.json({deletedCount:r.deletedCount,ok:true}); }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/:col/aggregate', heavyLimiter, getConnection, async (req, res) => {
  const qid=Math.random().toString(36).slice(2), t0=Date.now();
  try {
    const col=req.conn.client.db(req.params.db).collection(req.params.col), pipeline=req.body.pipeline||[], warnings=[];
    validatePipeline(pipeline, req.execConfig);
    for (const stage of pipeline) { const k=Object.keys(stage)[0]; if(k==='$facet'&&!req.caps.hasAggFacet) warnings.push('$facet requires 3.4+'); if(k==='$lookup'&&!req.caps.hasAggLookup) warnings.push('$lookup requires 3.2+'); if(k==='$merge'&&!req.caps.hasMergeStage) warnings.push('$merge requires 4.2+'); if(k==='$unionWith'&&!req.caps.hasUnionWith) warnings.push('$unionWith requires 4.4+'); if((k==='$densify'||k==='$fill')&&!req.caps.hasDensifyFill) warnings.push(`${k} requires 5.1+`); }
    const ao={};
    if(req.execConfig.mode==='safe' && req.execConfig.maxTimeMS>0) ao.maxTimeMS=req.execConfig.maxTimeMS;
    if(req.execConfig.allowDiskUse) ao.allowDiskUse=true;
    runningQueries.set(qid, {connId:req.connId,t0,type:'aggregate'});
    const results = await col.aggregate(pipeline, ao).toArray();
    const elapsed=Date.now()-t0;
    audit(req.connId,'aggregate',{db:req.params.db,col:req.params.col,elapsed,stages:pipeline.length,count:results.length});
    const maxR=req.execConfig.mode==='safe'?req.execConfig.maxResultSize:50000;
    const trimmed=results.length>maxR;
    const r={results:trimmed?results.slice(0,maxR):results, total:results.length, _elapsed:elapsed};
    if(trimmed) r.trimmed=true; if(warnings.length) r.warnings=warnings; if(elapsed>5000) r._slow=true;
    res.json(r);
  } catch(err) { if((err.message||'').includes('Unrecognized pipeline stage')) err.message+=` (unsupported on MongoDB ${req.conn.versionStr})`; sendError(res, err); }
  finally { runningQueries.delete(qid); }
});

app.post('/api/databases/:db/collections/:col/explain', getConnection, async (req, res) => {
  try {
    const db=req.conn.client.db(req.params.db), {type,filter,pipeline,sort,hint}=req.body;
    let explain;
    if (type==='aggregate'&&pipeline) explain = await db.command({explain:{aggregate:req.params.col,pipeline,cursor:{}},verbosity:'executionStats'});
    else {
      const cmd = { find:req.params.col, filter:parseFilterInput(filter) };
      const sortObj = parseFilterInput(sort);
      if (typeof hint === 'string' && hint.trim() && hint !== 'auto') cmd.hint = hint.trim();
      if (Object.keys(sortObj).length > 0) cmd.sort = sortObj;
      explain = await db.command({explain:cmd,verbosity:'executionStats'});
    }
    res.json({explain,summary:extractExplainSummary(explain)});
  } catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/:col/export', getConnection, async (req, res) => {
  try {
    const col=req.conn.client.db(req.params.db).collection(req.params.col);
    const {format='json',filter:fs='{}',limit:ls='1000',projection:ps='{}'}=req.body;
    const filter=parseFilter(fs), projection=parseFilter(ps), limit=Math.min(parseInt(ls)||1000,50000);
    const docs=await col.find(filter,{projection}).limit(limit).toArray();
    if(format==='csv'){
      if(!docs.length) return res.json({data:'',count:0,format:'csv'});
      const keys=[...new Set(docs.flatMap(d=>Object.keys(d)))];
      const header=keys.join(',');
      const rows=docs.map(d=>keys.map(k=>{const v=d[k];if(v==null)return'';if(typeof v==='object')return`"${JSON.stringify(v).replace(/"/g,'""')}"`;const s=String(v);return s.includes(',')||s.includes('"')||s.includes('\n')?`"${s.replace(/"/g,'""')}"`:s}).join(','));
      audit(req.connId,'export',{format:'csv',count:docs.length});
      res.json({data:[header,...rows].join('\n'),count:docs.length,format:'csv'});
    } else {
      audit(req.connId,'export',{format:'json',count:docs.length});
      res.json({data:JSON.stringify(docs,null,2),count:docs.length,format:'json'});
    }
  } catch(err) { sendError(res, err); }
});

app.get('/api/databases/:db/collections/:col/indexes', getConnection, async (req, res) => {
  try {
    const indexes = await req.conn.client.db(req.params.db).collection(req.params.col).indexes();
    try { const s=await req.conn.client.db(req.params.db).command({collStats:req.params.col}); for(const i of indexes) i.size=s.indexSizes?.[i.name]||0; } catch{}
    res.json({indexes});
  } catch { try { res.json({indexes:await req.conn.client.db(req.params.db).collection(req.params.col).listIndexes().toArray()}); } catch(err){sendError(res,err);} }
});

app.post('/api/databases/:db/collections/:col/indexes', getConnection, async (req, res) => {
  try { const k=req.body.keys||{}; if(Object.values(k).includes('$**')&&!req.caps.hasWildcardIndexes) return res.status(400).json({error:'Wildcard indexes require 4.2+',errorType:'version'}); const r=await req.conn.client.db(req.params.db).collection(req.params.col).createIndex(k,req.body.options||{}); audit(req.connId,'create_index',{name:r}); res.json({name:r,ok:true}); }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db/collections/:col/indexes/:name', getConnection, async (req, res) => {
  try { await req.conn.client.db(req.params.db).collection(req.params.col).dropIndex(req.params.name); audit(req.connId,'drop_index',{name:req.params.name}); res.json({ok:true}); }
  catch(err) { sendError(res, err); }
});

app.get('/api/audit', getConnection, (req, res) => { res.json({entries:auditLog.filter(e=>e.connId===req.connId).slice(-100)}); });
app.get('/api/databases/:db/collections/:col/distinct/:field', getConnection, async (req, res) => {
  try { const v=await req.conn.client.db(req.params.db).collection(req.params.col).distinct(req.params.field); res.json({values:v.slice(0,100)}); }
  catch(err) { sendError(res, err); }
});

if (isProd) app.get('*', (_, res) => res.sendFile(join(__dirname,'..','dist','index.html')));

const server = createServer(app);
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n  ⚡ MongoStudio v2.0.0 → http://localhost:${PORT}\n  MongoDB 3.6 → 8.x | ${isProd?'production':'development'}\n`);
});

function shutdown() { console.log('\nShutting down…'); for(const[,{client}]of connections)client.close().catch(()=>{}); server.close(()=>process.exit(0)); setTimeout(()=>process.exit(1),5000); }
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

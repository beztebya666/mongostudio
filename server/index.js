import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';
import compression from 'compression';
import helmet from 'helmet';
import cors from 'cors';
import { createServer } from 'http';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { existsSync, readFileSync, writeFileSync } from 'fs';

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
const DB_INIT_COLLECTION = '__mongostudio_init__';
const isHiddenCollectionName = (name = '') => name === DB_INIT_COLLECTION;
const SERVICE_CONFIG_PATH = join(__dirname, 'service-config.json');
const DEFAULT_SERVICE_CONFIG = {
  rateLimit: {
    windowMs: 60000,
    apiMax: 3000,
    heavyMax: 300,
  },
};

function normalizeServiceConfig(input = {}) {
  const source = input && typeof input === 'object' ? input : {};
  const rate = source.rateLimit && typeof source.rateLimit === 'object' ? source.rateLimit : {};
  const windowMs = Math.max(1000, Math.min(Number(rate.windowMs) || DEFAULT_SERVICE_CONFIG.rateLimit.windowMs, 15 * 60 * 1000));
  const apiMax = Math.max(10, Math.min(Number(rate.apiMax) || DEFAULT_SERVICE_CONFIG.rateLimit.apiMax, 100000));
  const heavyMax = Math.max(1, Math.min(Number(rate.heavyMax) || DEFAULT_SERVICE_CONFIG.rateLimit.heavyMax, 10000));
  return { rateLimit: { windowMs, apiMax, heavyMax } };
}

function loadServiceConfig() {
  try {
    if (!existsSync(SERVICE_CONFIG_PATH)) return { ...DEFAULT_SERVICE_CONFIG };
    const raw = readFileSync(SERVICE_CONFIG_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    return normalizeServiceConfig({ ...DEFAULT_SERVICE_CONFIG, ...parsed });
  } catch {
    return { ...DEFAULT_SERVICE_CONFIG };
  }
}

function persistServiceConfig(config) {
  try {
    writeFileSync(SERVICE_CONFIG_PATH, `${JSON.stringify(config, null, 2)}\n`, 'utf8');
  } catch (err) {
    console.warn('Failed to persist service config:', err?.message || err);
  }
}

const serviceConfig = loadServiceConfig();
persistServiceConfig(serviceConfig);

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

const rateBuckets = {
  api: new Map(),
  heavy: new Map(),
};

function getRateLimitKey(req) {
  return String(req.ip || req.headers['x-forwarded-for'] || req.headers['x-real-ip'] || 'anon');
}

function dynamicRateLimiter(kind = 'api') {
  return (req, res, next) => {
    const cfg = serviceConfig.rateLimit || DEFAULT_SERVICE_CONFIG.rateLimit;
    const max = kind === 'heavy' ? cfg.heavyMax : cfg.apiMax;
    const windowMs = cfg.windowMs;
    const now = Date.now();
    const key = getRateLimitKey(req);
    const bucketMap = kind === 'heavy' ? rateBuckets.heavy : rateBuckets.api;
    const current = bucketMap.get(key);
    if (!current || now >= current.resetAt) {
      bucketMap.set(key, { count: 1, resetAt: now + windowMs });
      return next();
    }
    if (current.count >= max) {
      const retrySec = Math.max(1, Math.ceil((current.resetAt - now) / 1000));
      res.setHeader('Retry-After', String(retrySec));
      return res.status(429).json({ error: kind === 'heavy' ? 'Too many heavy operations.' : 'Too many requests.' });
    }
    current.count += 1;
    bucketMap.set(key, current);
    return next();
  };
}

const apiLimiter = dynamicRateLimiter('api');
const heavyLimiter = dynamicRateLimiter('heavy');
app.use('/api/', apiLimiter);

setInterval(() => {
  const now = Date.now();
  for (const [id, conn] of connections) {
    if (now - conn.lastActivity > IDLE_TIMEOUT) {
      conn.client.close().catch(() => {});
      connections.delete(id); executionConfigs.delete(id);
    }
  }
  for (const bucket of [rateBuckets.api, rateBuckets.heavy]) {
    for (const [key, state] of bucket) {
      if (!state || now >= state.resetAt) bucket.delete(key);
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

async function compatDbStats(db, caps) {
  try {
    const s = await db.command({ dbStats: 1, scale: 1 });
    return {
      db: db.databaseName,
      collections: s.collections ?? 0,
      views: s.views ?? 0,
      objects: s.objects ?? 0,
      avgObjSize: s.avgObjSize ?? 0,
      dataSize: s.dataSize ?? 0,
      storageSize: s.storageSize ?? 0,
      indexes: s.indexes ?? 0,
      indexSize: s.indexSize ?? 0,
      fsUsedSize: s.fsUsedSize ?? 0,
      fsTotalSize: s.fsTotalSize ?? 0,
      ok: s.ok ?? 1,
    };
  } catch (err) {
    try {
      let list = [];
      try {
        list = await db.listCollections({}, { nameOnly: true, authorizedCollections: true }).toArray();
      } catch {
        list = await db.listCollections({}, { nameOnly: true }).toArray();
      }
      const visible = list.filter((col) => !isHiddenCollectionName(col?.name));
      let objects = 0;
      let dataSize = 0;
      for (const col of visible) {
        try {
          const stats = await compatCollStats(db, col.name, caps);
          objects += stats.count || 0;
          dataSize += stats.size || 0;
        } catch {}
      }
      return {
        db: db.databaseName,
        collections: visible.length,
        views: 0,
        objects,
        avgObjSize: objects > 0 ? Math.round(dataSize / objects) : 0,
        dataSize,
        storageSize: dataSize,
        indexes: 0,
        indexSize: 0,
        fsUsedSize: 0,
        fsTotalSize: 0,
        degraded: true,
      };
    } catch {
      throw err;
    }
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
  const uiUser = typeof req.headers['x-ui-user'] === 'string' ? req.headers['x-ui-user'].trim() : '';
  if (uiUser) {
    conn.lastUiUser = uiUser;
    if (!conn.username || conn.username === 'anonymous') conn.username = uiUser;
    if (!conn.authUser || conn.authUser === 'anonymous') conn.authUser = uiUser;
  }
  req.auditUser = uiUser || conn.lastUiUser || conn.username || conn.authUser || 'anonymous';
  conn.lastActivity = Date.now();
  req.conn = conn; req.caps = conn.capabilities; req.connId = id;
  req.execConfig = executionConfigs.get(id) || { ...DEFAULT_EXEC_CONFIG };
  next();
}

function auditReq(req, action, details = {}) {
  const headerUser = typeof req.headers['x-ui-user'] === 'string' ? req.headers['x-ui-user'].trim() : '';
  const resolvedUser = headerUser || req.auditUser || req.conn?.lastUiUser || req.conn?.username || req.conn?.authUser || 'anonymous';
  audit(req.connId, action, { ...details, user: resolvedUser });
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

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function serializeForExport(value) {
  if (value === null || value === undefined) return value;
  if (value instanceof ObjectId) return { $oid: value.toString() };
  if (value instanceof Date) return { $date: value.toISOString() };
  if (value instanceof RegExp) return { $regex: value.source, $options: value.flags };
  if (Array.isArray(value)) return value.map(serializeForExport);
  if (typeof value !== 'object') return value;

  const bsonType = value._bsontype;
  if (bsonType === 'Decimal128') return { $numberDecimal: value.toString() };
  if (bsonType === 'Long') return { $numberLong: value.toString() };
  if (bsonType === 'Double' || bsonType === 'Int32') return value.valueOf();
  if (bsonType === 'Binary') {
    try {
      return {
        $binary: Buffer.from(value.buffer).toString('base64'),
        $type: value.sub_type ?? value.subType ?? 0,
      };
    } catch {}
  }

  const out = {};
  for (const [k, v] of Object.entries(value)) out[k] = serializeForExport(v);
  return out;
}

function deserializeImportValue(value) {
  if (value === null || value === undefined) return value;
  if (Array.isArray(value)) return value.map(deserializeImportValue);
  if (!isPlainObject(value)) return value;

  const keys = Object.keys(value);
  if (keys.length === 1 && keys[0] === '$oid' && typeof value.$oid === 'string') {
    try { return new ObjectId(value.$oid); } catch {}
  }
  if (keys.length === 1 && keys[0] === '$date') {
    const d = new Date(value.$date);
    if (!Number.isNaN(d.getTime())) return d;
  }
  if (keys.length === 1 && keys[0] === '$numberLong') {
    const n = Number(value.$numberLong);
    return Number.isSafeInteger(n) ? n : String(value.$numberLong);
  }
  if (keys.length === 1 && keys[0] === '$numberDecimal') {
    const n = Number(value.$numberDecimal);
    return Number.isFinite(n) ? n : String(value.$numberDecimal);
  }
  if (keys.length === 2 && keys.includes('$binary') && keys.includes('$type')) {
    return value.$binary;
  }

  const out = {};
  for (const [k, v] of Object.entries(value)) out[k] = deserializeImportValue(v);
  return out;
}

function sanitizeCollectionOptions(options = {}) {
  const allowed = [
    'validator',
    'validationLevel',
    'validationAction',
    'capped',
    'size',
    'max',
    'timeseries',
    'expireAfterSeconds',
    'clusteredIndex',
    'collation',
    'changeStreamPreAndPostImages',
  ];
  const out = {};
  for (const key of allowed) {
    if (Object.prototype.hasOwnProperty.call(options, key)) out[key] = options[key];
  }
  return out;
}

function sanitizeIndexForCreate(indexSpec = {}) {
  if (!isPlainObject(indexSpec) || !isPlainObject(indexSpec.key)) return null;
  const options = {};
  for (const [key, value] of Object.entries(indexSpec)) {
    if (key === 'key' || key === 'ns' || key === 'v') continue;
    options[key] = value;
  }
  return { key: indexSpec.key, options };
}

function safeFilename(name, fallback = 'export') {
  const base = String(name || '').trim() || fallback;
  return base.replace(/[^a-zA-Z0-9._-]+/g, '_').replace(/^_+|_+$/g, '') || fallback;
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
  const explicitUser = typeof details.user === 'string' ? details.user.trim() : '';
  const resolvedUser = explicitUser || conn?.lastUiUser || conn?.username || conn?.authUser || 'anonymous';
  const payload = { ...details };
  if (Object.prototype.hasOwnProperty.call(payload, 'user')) delete payload.user;
  auditLog.push({
    ts: Date.now(),
    connId,
    host: conn?.host || '?',
    user: resolvedUser,
    action,
    ...payload,
  });
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

app.get('/api/health', (_, res) => res.json({
  status:'ok',
  uptime:process.uptime(),
  connections:connections.size,
  memory:process.memoryUsage(),
  ts:new Date().toISOString(),
  rateLimit: serviceConfig.rateLimit,
}));
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
    if (!username) {
      try {
        const fromClient = client?.options?.credentials?.username || client?.s?.options?.credentials?.username;
        if (fromClient) username = String(fromClient);
      } catch {}
    }
    // Extract readPreference from URI query params as fallback
    let effectiveReadPref = options.readPreference || null;
    if (!effectiveReadPref) {
      try { const m = uri.match(/[?&]readPreference=([^&]+)/i); if (m) effectiveReadPref = decodeURIComponent(m[1]); } catch {}
    }

    connections.set(connId, { client, uri, host, username, authUser: username, lastUiUser: username, connectedAt:Date.now(), lastActivity:Date.now(), version, versionStr, capabilities, isProduction, topology });
    executionConfigs.set(connId, { ...DEFAULT_EXEC_CONFIG });
    audit(connId, 'connect', { host, version:versionStr });

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
  auditReq(req, 'config_change', { mode:c.mode });
  res.json(c);
});

app.get('/api/service-config', (req, res) => {
  res.json(serviceConfig);
});

app.put('/api/service-config', (req, res) => {
  const incoming = req.body && typeof req.body === 'object' ? req.body : {};
  const next = normalizeServiceConfig({ ...serviceConfig, ...incoming });
  serviceConfig.rateLimit = next.rateLimit;
  persistServiceConfig(serviceConfig);
  const headerUser = typeof req.headers['x-ui-user'] === 'string' ? req.headers['x-ui-user'].trim() : '';
  const connId = typeof req.headers['x-connection-id'] === 'string' ? req.headers['x-connection-id'] : 'service';
  audit(connId, 'service_config_change', {
    user: headerUser || 'system',
    rateWindowMs: serviceConfig.rateLimit.windowMs,
    rateApiMax: serviceConfig.rateLimit.apiMax,
    rateHeavyMax: serviceConfig.rateLimit.heavyMax,
  });
  res.json(serviceConfig);
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
  try {
    const { name } = req.body;
    if (!name || !/^[a-zA-Z0-9_-]+$/.test(name)) {
      return res.status(400).json({ error:'Invalid name', errorType:'validation' });
    }
    const db = req.conn.client.db(name);
    const exists = await db.listCollections({ name: DB_INIT_COLLECTION }, { nameOnly: true }).toArray();
    if (exists.length === 0) {
      await db.createCollection(DB_INIT_COLLECTION);
    }
    auditReq(req,'create_db',{db:name});
    res.json({ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db', getConnection, async (req, res) => {
  try { await req.conn.client.db(req.params.db).dropDatabase(); auditReq(req,'drop_db',{db:req.params.db}); res.json({ok:true}); }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/export', heavyLimiter, getConnection, async (req, res) => {
  try {
    const dbName = req.params.db;
    const db = req.conn.client.db(dbName);
    const includeDocuments = req.body?.includeDocuments !== false;
    const includeIndexes = req.body?.includeIndexes !== false;
    const includeOptions = req.body?.includeOptions !== false;
    const includeSchema = req.body?.includeSchema !== false;
    const limitPerCollection = Math.max(0, Math.min(parseInt(req.body?.limitPerCollection) || 0, 200000));
    const schemaSampleSize = Math.max(25, Math.min(parseInt(req.body?.schemaSampleSize) || 150, 500));

    const collections = (await db.listCollections({}, { nameOnly: false }).toArray())
      .filter((entry) => !isHiddenCollectionName(entry?.name));
    const exported = {
      type: 'mongostudio-db-package',
      version: 1,
      exportedAt: new Date().toISOString(),
      source: {
        host: req.conn.host || null,
        version: req.conn.versionStr || null,
      },
      database: { name: dbName },
      collections: [],
    };

    let exportedDocs = 0;
    for (const info of collections) {
      const colName = info.name;
      const col = db.collection(colName);
      const entry = {
        name: colName,
        type: info.type || 'collection',
      };

      if (includeOptions) entry.options = info.options || {};

      if (includeIndexes) {
        try {
          entry.indexes = await col.indexes();
        } catch {
          entry.indexes = [];
        }
      }

      if (includeDocuments) {
        let cursor = col.find({});
        if (limitPerCollection > 0) cursor = cursor.limit(limitPerCollection);
        const docs = await cursor.toArray();
        entry.documents = docs.map(serializeForExport);
        exportedDocs += docs.length;
        if (limitPerCollection > 0 && docs.length >= limitPerCollection) entry.truncated = true;
      }

      if (includeSchema) {
        try {
          const sample = await col.aggregate([{ $sample: { size: schemaSampleSize } }]).toArray();
          const schema = {};
          for (const d of sample) analyzeFields(d, '', schema);
          entry.schemaSampleSize = sample.length;
          entry.schema = Object.entries(schema).map(([path, field]) => ({
            path,
            count: field.count,
            pct: sample.length > 0 ? Math.round((field.count / sample.length) * 100) : 0,
            hasNull: field.hasNull,
            sample: field.sample,
            types: Object.entries(field.types)
              .sort((a, b) => b[1] - a[1])
              .map(([type, count]) => ({
                type,
                count,
                pct: sample.length > 0 ? Math.round((count / sample.length) * 100) : 0,
              })),
          })).sort((a, b) => b.count - a.count);
        } catch {
          entry.schema = [];
          entry.schemaSampleSize = 0;
        }
      }

      exported.collections.push(entry);
    }

    const data = JSON.stringify(exported, null, 2);
    auditReq(req, 'export_db', {
      db: dbName,
      collections: exported.collections.length,
      docs: exportedDocs,
    });
    res.json({
      ok: true,
      format: 'json',
      filename: `${safeFilename(dbName)}.mongostudio-db.json`,
      collections: exported.collections.length,
      documents: exportedDocs,
      data,
    });
  } catch (err) {
    sendError(res, err);
  }
});

app.post('/api/databases/import', heavyLimiter, getConnection, async (req, res) => {
  try {
    const mode = req.body?.mode === 'replace' ? 'replace' : 'merge';
    const targetDbInput = typeof req.body?.targetDb === 'string' ? req.body.targetDb.trim() : '';
    let payload = req.body?.package ?? req.body?.payload ?? req.body?.data;

    if (typeof payload === 'string') {
      try { payload = JSON.parse(payload); }
      catch { return res.status(400).json({ error: 'Invalid package JSON.', errorType: 'validation' }); }
    }

    if (!isPlainObject(payload)) {
      return res.status(400).json({ error: 'Import package is required.', errorType: 'validation' });
    }

    const sourceDbName = payload.database?.name || payload.db || '';
    const dbName = targetDbInput || sourceDbName;
    if (!dbName || !/^[a-zA-Z0-9_-]+$/.test(dbName)) {
      return res.status(400).json({ error: 'Invalid target database name.', errorType: 'validation' });
    }

    const collections = Array.isArray(payload.collections) ? payload.collections : [];
    if (collections.length === 0) {
      return res.status(400).json({ error: 'Package has no collections to import.', errorType: 'validation' });
    }

    const db = req.conn.client.db(dbName);
    const warnings = [];
    let createdCollections = 0;
    let insertedDocuments = 0;
    let createdIndexes = 0;

    if (mode === 'replace') {
      await db.dropDatabase();
    }

    const existing = new Set((await db.listCollections({}, { nameOnly: true }).toArray()).map(c => c.name));

    for (const colSpec of collections) {
      const colName = typeof colSpec?.name === 'string' ? colSpec.name.trim() : '';
      if (!colName) {
        warnings.push('Skipped collection with empty name.');
        continue;
      }

      if (mode === 'replace' && existing.has(colName)) {
        try {
          await db.collection(colName).drop();
          existing.delete(colName);
        } catch (err) {
          warnings.push(`Could not drop ${colName}: ${err.message}`);
        }
      }

      if (!existing.has(colName)) {
        try {
          const createOptions = sanitizeCollectionOptions(colSpec.options || {});
          if (Object.keys(createOptions).length > 0) await db.createCollection(colName, createOptions);
          else await db.createCollection(colName);
          createdCollections++;
          existing.add(colName);
        } catch (err) {
          if (!String(err?.message || '').toLowerCase().includes('already exists')) {
            warnings.push(`Could not create ${colName}: ${err.message}`);
          }
        }
      }

      const col = db.collection(colName);
      const docs = Array.isArray(colSpec.documents) ? colSpec.documents : [];
      if (docs.length > 0) {
        const parsedDocs = docs.map(deserializeImportValue);
        try {
          const r = await col.insertMany(parsedDocs, { ordered: false });
          insertedDocuments += Object.keys(r.insertedIds || {}).length;
        } catch (err) {
          const inserted = err?.result?.insertedCount ?? err?.result?.result?.nInserted ?? 0;
          insertedDocuments += inserted;
          warnings.push(`Insert warnings in ${colName}: ${err.message}`);
        }
      }

      const idxSpecs = Array.isArray(colSpec.indexes) ? colSpec.indexes : [];
      for (const idx of idxSpecs) {
        const spec = sanitizeIndexForCreate(idx);
        if (!spec) continue;
        if (spec.options?.name === '_id_' || (Object.keys(spec.key).length === 1 && spec.key._id === 1)) continue;
        try {
          await col.createIndex(spec.key, spec.options || {});
          createdIndexes++;
        } catch (err) {
          warnings.push(`Index skipped on ${colName}: ${err.message}`);
        }
      }
    }

    auditReq(req, 'import_db', {
      db: dbName,
      mode,
      collections: collections.length,
      docs: insertedDocuments,
    });

    res.json({
      ok: true,
      db: dbName,
      mode,
      importedCollections: collections.length,
      createdCollections,
      insertedDocuments,
      createdIndexes,
      warnings,
    });
  } catch (err) {
    sendError(res, err);
  }
});

app.get('/api/databases/:db/stats', getConnection, async (req, res) => {
  try {
    const db = req.conn.client.db(req.params.db);
    const stats = await compatDbStats(db, req.caps);
    const initExists = await db.listCollections({ name: DB_INIT_COLLECTION }, { nameOnly: true }).toArray();
    if (initExists.length > 0) {
      try {
        const initStats = await compatCollStats(db, DB_INIT_COLLECTION, req.caps);
        stats.collections = Math.max(0, Number(stats.collections || 0) - 1);
        stats.objects = Math.max(0, Number(stats.objects || 0) - Number(initStats.count || 0));
        stats.dataSize = Math.max(0, Number(stats.dataSize || 0) - Number(initStats.size || 0));
        stats.storageSize = Math.max(0, Number(stats.storageSize || 0) - Number(initStats.storageSize || initStats.size || 0));
        stats.indexes = Math.max(0, Number(stats.indexes || 0) - Number(initStats.nindexes || 0));
        stats.indexSize = Math.max(0, Number(stats.indexSize || 0) - Number(initStats.totalIndexSize || 0));
        stats.avgObjSize = stats.objects > 0 ? Math.round(stats.dataSize / stats.objects) : 0;
      } catch {
        stats.collections = Math.max(0, Number(stats.collections || 0) - 1);
      }
    }
    res.json(stats);
  } catch (err) {
    sendError(res, err);
  }
});

app.get('/api/databases/:db/collections', getConnection, async (req, res) => {
  try {
    const db = req.conn.client.db(req.params.db);
    const withStats = String(req.query?.withStats || '') === '1';
    const c = await db.listCollections({}, { nameOnly: !withStats, authorizedCollections: true }).toArray();
    let collections = c
      .map(x=>({name:x.name,type:x.type||'collection',options:x.options||{}}))
      .filter((entry) => !isHiddenCollectionName(entry.name))
      .sort((a,b)=>a.name.localeCompare(b.name));
    if (withStats) {
      collections = await Promise.all(collections.map(async (entry) => {
        try {
          const s = await compatCollStats(db, entry.name, req.caps);
          return { ...entry, count: s.count, size: s.size, avgObjSize: s.avgObjSize, nindexes: s.nindexes };
        } catch {
          return { ...entry, count: 0, size: 0, avgObjSize: 0, nindexes: 0 };
        }
      }));
    }
    res.json({ collections });
  }
  catch(err) {
    try {
      const db = req.conn.client.db(req.params.db);
      const withStats = String(req.query?.withStats || '') === '1';
      const c = await db.listCollections().toArray();
      let collections = c
        .map(x=>({name:x.name,type:x.type||'collection',options:x.options||{}}))
        .filter((entry) => !isHiddenCollectionName(entry.name))
        .sort((a,b)=>a.name.localeCompare(b.name));
      if (withStats) {
        collections = await Promise.all(collections.map(async (entry) => {
          try {
            const s = await compatCollStats(db, entry.name, req.caps);
            return { ...entry, count: s.count, size: s.size, avgObjSize: s.avgObjSize, nindexes: s.nindexes };
          } catch {
            return { ...entry, count: 0, size: 0, avgObjSize: 0, nindexes: 0 };
          }
        }));
      }
      res.json({ collections });
    } catch {
      try {
        const withStats = String(req.query?.withStats || '') === '1';
        const c=await req.conn.client.db(req.params.db).collections();
        let collections = c
          .map(x=>({name:x.collectionName,type:'collection',options:{}}))
          .filter((entry) => !isHiddenCollectionName(entry.name))
          .sort((a,b)=>a.name.localeCompare(b.name));
        if (withStats) {
          collections = collections.map((entry) => ({ ...entry, count: 0, size: 0, avgObjSize: 0, nindexes: 0 }));
        }
        res.json({ collections });
      } catch { sendError(res,err); }
    }
  }
});

app.post('/api/databases/:db/collections', getConnection, async (req, res) => {
  try {
    const colName = String(req.body?.name || '').trim();
    if (!colName) return res.status(400).json({error:'Name required',errorType:'validation'});
    if (isHiddenCollectionName(colName)) return res.status(400).json({error:'Reserved collection name.',errorType:'validation'});
    const db = req.conn.client.db(req.params.db);
    await db.createCollection(colName);
    await db.collection(DB_INIT_COLLECTION).drop().catch(() => {});
    auditReq(req,'create_col',{db:req.params.db,col:colName});
    res.json({ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/import', heavyLimiter, getConnection, async (req, res) => {
  try {
    const dbName = req.params.db;
    const {
      name,
      documents = [],
      indexes = [],
      options = {},
      dropExisting = false,
    } = req.body || {};

    const colName = typeof name === 'string' ? name.trim() : '';
    if (!colName || !/^[a-zA-Z0-9_.-]+$/.test(colName)) {
      return res.status(400).json({ error: 'Invalid collection name.', errorType: 'validation' });
    }
    if (isHiddenCollectionName(colName)) {
      return res.status(400).json({ error: 'Reserved collection name.', errorType: 'validation' });
    }
    if (!Array.isArray(documents)) {
      return res.status(400).json({ error: 'documents must be an array.', errorType: 'validation' });
    }

    const db = req.conn.client.db(dbName);
    const exists = await db.listCollections({ name: colName }, { nameOnly: true }).toArray();
    if (dropExisting && exists.length > 0) {
      await db.collection(colName).drop().catch(() => {});
    }
    if (exists.length === 0 || dropExisting) {
      const createOptions = sanitizeCollectionOptions(options || {});
      if (Object.keys(createOptions).length > 0) await db.createCollection(colName, createOptions);
      else await db.createCollection(colName);
    }

    const collection = db.collection(colName);
    await db.collection(DB_INIT_COLLECTION).drop().catch(() => {});
    const parsedDocs = documents.map(deserializeImportValue);
    let insertedCount = 0;
    if (parsedDocs.length > 0) {
      const result = await collection.insertMany(parsedDocs, { ordered: false });
      insertedCount = Object.keys(result.insertedIds || {}).length;
    }

    let indexCount = 0;
    for (const idx of indexes) {
      const spec = sanitizeIndexForCreate(idx);
      if (!spec) continue;
      if (spec.options?.name === '_id_' || (Object.keys(spec.key).length === 1 && spec.key._id === 1)) continue;
      try {
        await collection.createIndex(spec.key, spec.options || {});
        indexCount++;
      } catch {}
    }

    auditReq(req, 'import_col', {
      db: dbName,
      col: colName,
      docs: insertedCount,
      indexes: indexCount,
    });
    res.json({
      ok: true,
      db: dbName,
      collection: colName,
      insertedCount,
      indexCount,
    });
  } catch (err) {
    sendError(res, err);
  }
});

app.delete('/api/databases/:db/collections/:col', getConnection, async (req, res) => {
  try { await req.conn.client.db(req.params.db).collection(req.params.col).drop(); auditReq(req,'drop_col',{db:req.params.db,col:req.params.col}); res.json({ok:true}); }
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
    const maxLim=req.execConfig.mode==='safe'
      ? Math.max(1, Math.min(Number(req.execConfig.maxResultSize) || 50, 50000))
      : 50000;
    const limit=Math.min(parseInt(req.query.limit)||50, maxLim);
    const fo={projection}; if(req.execConfig.mode==='safe' && req.execConfig.maxTimeMS>0) fo.maxTimeMS=req.execConfig.maxTimeMS;
    runningQueries.set(qid, {connId:req.connId,t0,type:'find'});
    let cursor = col.find(filter,fo);
    if (hint) cursor = cursor.hint(hint);
    const [documents, total] = await Promise.all([cursor.sort(sort).skip(skip).limit(limit).toArray(), compatCount(col,filter,req.caps)]);
    const elapsed=Date.now()-t0;
    auditReq(req,'query',{db:req.params.db,col:req.params.col,elapsed,count:documents.length});
    const r={documents,total,_elapsed:elapsed}; if(elapsed>5000) r._slow=true; res.json(r);
  } catch(err) { sendError(res, err); } finally { runningQueries.delete(qid); }
});

app.get('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try { const d=await req.conn.client.db(req.params.db).collection(req.params.col).findOne({_id:parseId(req.params.id)}); if(!d) return res.status(404).json({error:'Not found',errorType:'not_found'}); res.json(d); }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  try { const r=await req.conn.client.db(req.params.db).collection(req.params.col).insertOne(deserializeImportValue(req.body.document)); auditReq(req,'insert',{db:req.params.db,col:req.params.col}); res.json({insertedId:r.insertedId,ok:true}); }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/:col/documents/bulk', heavyLimiter, getConnection, async (req, res) => {
  try {
    const docs = Array.isArray(req.body?.documents) ? req.body.documents : null;
    if (!docs || docs.length === 0) {
      return res.status(400).json({ error: 'documents array is required.', errorType: 'validation' });
    }
    if (docs.length > 10000) {
      return res.status(400).json({ error: 'Too many documents in one request (max 10000).', errorType: 'validation' });
    }
    const parsed = docs.map(deserializeImportValue);
    const r = await req.conn.client.db(req.params.db).collection(req.params.col).insertMany(parsed, { ordered: false });
    const insertedCount = Object.keys(r.insertedIds || {}).length;
    auditReq(req, 'insert_many', { db: req.params.db, col: req.params.col, count: insertedCount });
    res.json({ ok: true, insertedCount, insertedIds: r.insertedIds });
  } catch (err) {
    sendError(res, err);
  }
});

app.put('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try { const r=await req.conn.client.db(req.params.db).collection(req.params.col).replaceOne({_id:parseId(req.params.id)},{...req.body.update,_id:parseId(req.params.id)}); auditReq(req,'update',{db:req.params.db,col:req.params.col}); res.json({modifiedCount:r.modifiedCount,ok:true}); }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try { const r=await req.conn.client.db(req.params.db).collection(req.params.col).deleteOne({_id:parseId(req.params.id)}); auditReq(req,'delete',{db:req.params.db,col:req.params.col}); res.json({deletedCount:r.deletedCount,ok:true}); }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  try { const f=parseFilter(JSON.stringify(req.body.filter||{})); const r=await req.conn.client.db(req.params.db).collection(req.params.col).deleteMany(f); auditReq(req,'delete_many',{db:req.params.db,col:req.params.col,count:r.deletedCount}); res.json({deletedCount:r.deletedCount,ok:true}); }
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
    auditReq(req,'aggregate',{db:req.params.db,col:req.params.col,elapsed,stages:pipeline.length,count:results.length});
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
    const {format='json',filter:fs='{}',sort:ss='{}',limit:ls='1000',projection:ps='{}'}=req.body;
    const filter=parseFilter(fs), sort=parseFilter(ss), projection=parseFilter(ps), limit=Math.min(parseInt(ls)||1000,50000);
    let cursor = col.find(filter,{projection});
    if (sort && typeof sort === 'object' && Object.keys(sort).length > 0) cursor = cursor.sort(sort);
    const docsRaw=await cursor.limit(limit).toArray();
    const docs=docsRaw.map(serializeForExport);
    if(format==='csv'){
      if(!docs.length) return res.json({data:'',count:0,format:'csv'});
      const keys=[...new Set(docs.flatMap(d=>Object.keys(d)))];
      const header=keys.join(',');
      const rows=docs.map(d=>keys.map(k=>{const v=d[k];if(v==null)return'';if(typeof v==='object')return`"${JSON.stringify(v).replace(/"/g,'""')}"`;const s=String(v);return s.includes(',')||s.includes('"')||s.includes('\n')?`"${s.replace(/"/g,'""')}"`:s}).join(','));
      auditReq(req,'export',{format:'csv',count:docs.length});
      res.json({data:[header,...rows].join('\n'),count:docs.length,format:'csv'});
    } else {
      auditReq(req,'export',{format:'json',count:docs.length});
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
  try { const k=req.body.keys||{}; if(Object.values(k).includes('$**')&&!req.caps.hasWildcardIndexes) return res.status(400).json({error:'Wildcard indexes require 4.2+',errorType:'version'}); const r=await req.conn.client.db(req.params.db).collection(req.params.col).createIndex(k,req.body.options||{}); auditReq(req,'create_index',{name:r}); res.json({name:r,ok:true}); }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db/collections/:col/indexes/:name', getConnection, async (req, res) => {
  try { await req.conn.client.db(req.params.db).collection(req.params.col).dropIndex(req.params.name); auditReq(req,'drop_index',{name:req.params.name}); res.json({ok:true}); }
  catch(err) { sendError(res, err); }
});

app.get('/api/audit', getConnection, (req, res) => {
  const limit = Math.max(1, Math.min(parseInt(req.query.limit) || 200, 1000));
  const action = typeof req.query.action === 'string' ? req.query.action.trim() : '';
  const search = typeof req.query.search === 'string' ? req.query.search.trim().toLowerCase() : '';
  const fromTs = Number(req.query.from) || 0;
  const toTs = Number(req.query.to) || 0;

  let entries = auditLog.filter((entry) => entry.connId === req.connId);
  if (action) entries = entries.filter((entry) => entry.action === action);
  if (fromTs > 0) entries = entries.filter((entry) => entry.ts >= fromTs);
  if (toTs > 0) entries = entries.filter((entry) => entry.ts <= toTs);
  if (search) {
    entries = entries.filter((entry) => JSON.stringify(entry).toLowerCase().includes(search));
  }

  const total = entries.length;
  const items = entries.slice(-limit).reverse();
  res.json({ entries: items, total });
});
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

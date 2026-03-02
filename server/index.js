import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';
import compression from 'compression';
import helmet from 'helmet';
import cors from 'cors';
import { createServer } from 'http';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3141;
const isProd = process.env.NODE_ENV === 'production';

// ─── Connection Pool ────────────────────────────────────────────────
const connections = new Map();

// ─── Middleware ──────────────────────────────────────────────────────
app.use(compression());
app.use(helmet({ contentSecurityPolicy: false, crossOriginEmbedderPolicy: false }));
app.use(cors());
app.use(express.json({ limit: '16mb' }));

if (isProd) {
  const distPath = join(__dirname, '..', 'dist');
  app.use(express.static(distPath, { maxAge: '1h', etag: true }));
}

// ─── Version Utilities ──────────────────────────────────────────────
function parseVersion(versionStr) {
  const parts = (versionStr || '0.0.0').split('.').map(Number);
  return { major: parts[0] || 0, minor: parts[1] || 0, patch: parts[2] || 0 };
}

function versionAtLeast(version, major, minor = 0) {
  if (version.major > major) return true;
  if (version.major === major && version.minor >= minor) return true;
  return false;
}

// Feature support matrix by MongoDB server version
function getCapabilities(version) {
  return {
    hasCountDocuments: versionAtLeast(version, 3, 6),
    hasEstimatedCount: versionAtLeast(version, 4, 0),
    hasMergeStage: versionAtLeast(version, 4, 2),
    hasUnionWith: versionAtLeast(version, 4, 4),
    hasStableApi: versionAtLeast(version, 5, 0),
    hasTimeSeries: versionAtLeast(version, 5, 0),
    hasClustered: versionAtLeast(version, 5, 3),
    hasDensifyFill: versionAtLeast(version, 5, 1),
    hasQueryableEncryption: versionAtLeast(version, 7, 0),
    hasAggFacet: versionAtLeast(version, 3, 4),
    hasAggLookup: versionAtLeast(version, 3, 2),
    hasChangeStreams: versionAtLeast(version, 3, 6),
    hasTransactions: versionAtLeast(version, 4, 0),
    hasWildcardIndexes: versionAtLeast(version, 4, 2),
  };
}

// ─── Compat Layer ───────────────────────────────────────────────────
async function compatCountDocuments(collection, filter, caps) {
  if (caps.hasCountDocuments) {
    try { return await collection.countDocuments(filter); } catch {}
  }
  try { return await collection.count(filter); } catch { return 0; }
}

async function compatEstimatedCount(collection, caps) {
  if (caps.hasEstimatedCount) {
    try { return await collection.estimatedDocumentCount(); } catch {}
  }
  try { return await collection.count({}); } catch { return 0; }
}

async function compatCollStats(db, colName, caps) {
  try {
    const stats = await db.command({ collStats: colName });
    return {
      count: stats.count ?? 0,
      size: stats.size ?? 0,
      avgObjSize: stats.avgObjSize ?? 0,
      storageSize: stats.storageSize ?? 0,
      totalIndexSize: stats.totalIndexSize ?? 0,
      indexSizes: stats.indexSizes ?? {},
      nindexes: stats.nindexes ?? 0,
      wiredTiger: stats.wiredTiger || null,
    };
  } catch (err) {
    try {
      const col = db.collection(colName);
      const count = await compatEstimatedCount(col, caps);
      return { count, size: 0, avgObjSize: 0, storageSize: 0, totalIndexSize: 0, nindexes: 0 };
    } catch { throw err; }
  }
}

// ─── Helpers ────────────────────────────────────────────────────────
function getConnection(req, res, next) {
  const connId = req.headers['x-connection-id'];
  if (!connId || !connections.has(connId)) {
    return res.status(401).json({ error: 'Not connected. Please connect first.' });
  }
  req.conn = connections.get(connId);
  req.caps = req.conn.capabilities;
  next();
}

function parseId(id) {
  try { if (/^[a-f0-9]{24}$/i.test(id)) return new ObjectId(id); } catch {}
  return id;
}

function parseFilter(str) {
  try { return transformFilter(JSON.parse(str)); } catch { return {}; }
}

function transformFilter(obj) {
  if (obj === null || typeof obj !== 'object') return obj;
  if (Array.isArray(obj)) return obj.map(transformFilter);
  const result = {};
  for (const [key, val] of Object.entries(obj)) {
    if (key === '_id' && typeof val === 'string' && /^[a-f0-9]{24}$/i.test(val)) {
      result[key] = new ObjectId(val);
    } else if (val && typeof val === 'object' && val.$oid) {
      result[key] = new ObjectId(val.$oid);
    } else {
      result[key] = transformFilter(val);
    }
  }
  return result;
}

// ─── ROUTES ─────────────────────────────────────────────────────────

// Connect — with version detection
app.post('/api/connect', async (req, res) => {
  const { uri } = req.body;
  if (!uri) return res.status(400).json({ error: 'Connection URI is required' });

  try {
    const clientOpts = {
      connectTimeoutMS: 15000,
      serverSelectionTimeoutMS: 15000,
      socketTimeoutMS: 30000,
      maxPoolSize: 10,
      minPoolSize: 1,
      retryWrites: false,
      retryReads: false,
    };

    const client = new MongoClient(uri, clientOpts);
    await client.connect();

    // ── Detect server version ──
    let versionStr = 'unknown';
    let version = { major: 0, minor: 0, patch: 0 };
    const warnings = [];

    try {
      const buildInfo = await client.db('admin').command({ buildInfo: 1 });
      versionStr = buildInfo.version || 'unknown';
      version = parseVersion(versionStr);
    } catch {
      // Fallback: wire protocol version → approximate server version
      try {
        let hello;
        try { hello = await client.db('admin').command({ hello: 1 }); }
        catch { hello = await client.db('admin').command({ isMaster: 1 }); }

        if (hello.maxWireVersion !== undefined) {
          const wireMap = {
            0:'2.6',1:'2.6',2:'2.6',3:'3.0',4:'3.2',5:'3.4',6:'3.6',
            7:'4.0',8:'4.2',9:'4.4',10:'4.9',11:'5.0',12:'5.1',
            13:'5.3',14:'6.0',15:'6.1',16:'6.2',17:'7.0',18:'7.1',
            19:'7.2',20:'7.3',21:'8.0',22:'8.1',
          };
          versionStr = wireMap[hello.maxWireVersion] || `wire-${hello.maxWireVersion}`;
          version = parseVersion(versionStr);
        }
      } catch {
        warnings.push('Could not detect server version. Running in legacy compatibility mode.');
      }
    }

    const capabilities = getCapabilities(version);

    // Version warnings
    if (version.major > 0 && version.major < 3) {
      warnings.push(`MongoDB ${versionStr} is very old. Some features may not work. Consider upgrading.`);
    } else if (version.major === 3 && version.minor < 6) {
      warnings.push(`MongoDB ${versionStr}: countDocuments and change streams unavailable. Using legacy fallbacks.`);
    } else if (version.major === 3) {
      warnings.push(`MongoDB ${versionStr}: Running in 3.x compatibility mode. Most features work.`);
    } else if (version.major === 4 && version.minor < 4) {
      warnings.push(`MongoDB ${versionStr}: Some newer aggregation stages ($unionWith) unavailable.`);
    }

    // Ping to verify
    try {
      await client.db('admin').command({ ping: 1 });
    } catch {
      try {
        const dbName = new URL(uri).pathname.slice(1) || 'test';
        await client.db(dbName).command({ ping: 1 });
      } catch {
        warnings.push('Ping failed — you may have restricted admin permissions, but browsing should still work.');
      }
    }

    const connId = Math.random().toString(36).slice(2) + Date.now().toString(36);

    let host = 'unknown';
    try {
      const url = new URL(uri);
      host = url.hostname + (url.port ? ':' + url.port : '');
    } catch {
      host = uri.split('@').pop()?.split('/')[0] || 'unknown';
    }

    connections.set(connId, { client, uri, connectedAt: Date.now(), version, versionStr, capabilities });

    res.json({
      connectionId: connId,
      host,
      version: versionStr,
      capabilities: {
        countDocuments: capabilities.hasCountDocuments,
        estimatedCount: capabilities.hasEstimatedCount,
        changeStreams: capabilities.hasChangeStreams,
        transactions: capabilities.hasTransactions,
        aggregationFacet: capabilities.hasAggFacet,
        aggregationLookup: capabilities.hasAggLookup,
        wildcardIndexes: capabilities.hasWildcardIndexes,
        timeSeries: capabilities.hasTimeSeries,
        stableApi: capabilities.hasStableApi,
      },
      warnings,
      ok: true,
    });
  } catch (err) {
    let msg = err.message;
    if (msg.includes('ECONNREFUSED')) msg = `Cannot reach MongoDB server. Is it running? (${msg})`;
    else if (msg.includes('Authentication failed')) msg = 'Authentication failed. Check username and password.';
    else if (msg.includes('ENOTFOUND')) msg = 'Host not found. Check the hostname in your connection string.';
    else if (msg.includes('SSL') || msg.includes('TLS')) msg = `TLS/SSL error. Try ?tls=true or ?tls=false in your URI. (${msg})`;
    res.status(400).json({ error: msg });
  }
});

app.post('/api/disconnect', (req, res) => {
  const connId = req.headers['x-connection-id'];
  if (connId && connections.has(connId)) {
    connections.get(connId).client.close().catch(() => {});
    connections.delete(connId);
  }
  res.json({ ok: true });
});

app.get('/api/status', getConnection, async (req, res) => {
  try {
    const admin = req.conn.client.db('admin');
    const results = { version: req.conn.versionStr, capabilities: req.conn.capabilities };
    try { results.buildInfo = await admin.command({ buildInfo: 1 }); } catch {}
    try { results.serverStatus = await admin.command({ serverStatus: 1 }); } catch {}
    try { results.hello = await admin.command({ hello: 1 }); }
    catch { try { results.hello = await admin.command({ isMaster: 1 }); } catch {} }
    res.json(results);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/databases', getConnection, async (req, res) => {
  try {
    const result = await req.conn.client.db('admin').command({ listDatabases: 1, nameOnly: false });
    res.json({
      databases: (result.databases || []).map(db => ({ name: db.name, sizeOnDisk: db.sizeOnDisk, empty: db.empty })),
      totalSize: result.totalSize,
      version: req.conn.versionStr,
    });
  } catch (err) {
    try {
      const dbName = new URL(req.conn.uri).pathname.slice(1);
      if (dbName) {
        res.json({ databases: [{ name: dbName, sizeOnDisk: 0, empty: false }], totalSize: 0, version: req.conn.versionStr,
          warning: 'Limited permissions — showing only your database.' });
      } else throw err;
    } catch { res.status(500).json({ error: err.message }); }
  }
});

app.get('/api/databases/:db/collections', getConnection, async (req, res) => {
  try {
    const collections = await req.conn.client.db(req.params.db).listCollections().toArray();
    res.json({ collections: collections.map(c => ({ name: c.name, type: c.type, options: c.options })).sort((a,b) => a.name.localeCompare(b.name)) });
  } catch (err) {
    try {
      const cols = await req.conn.client.db(req.params.db).collections();
      res.json({ collections: cols.map(c => ({ name: c.collectionName, type: 'collection', options: {} })).sort((a,b) => a.name.localeCompare(b.name)) });
    } catch { res.status(500).json({ error: err.message }); }
  }
});

app.post('/api/databases/:db/collections', getConnection, async (req, res) => {
  try { await req.conn.client.db(req.params.db).createCollection(req.body.name); res.json({ ok: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/databases/:db/collections/:col', getConnection, async (req, res) => {
  try { await req.conn.client.db(req.params.db).collection(req.params.col).drop(); res.json({ ok: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/databases/:db', getConnection, async (req, res) => {
  try { await req.conn.client.db(req.params.db).dropDatabase(); res.json({ ok: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/databases/:db/collections/:col/stats', getConnection, async (req, res) => {
  try { res.json(await compatCollStats(req.conn.client.db(req.params.db), req.params.col, req.caps)); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  try {
    const col = req.conn.client.db(req.params.db).collection(req.params.col);
    const filter = parseFilter(req.query.filter || '{}');
    const sort = parseFilter(req.query.sort || '{}');
    const projection = parseFilter(req.query.projection || '{}');
    const skip = parseInt(req.query.skip) || 0;
    const limit = Math.min(parseInt(req.query.limit) || 50, 1000);
    const [documents, total] = await Promise.all([
      col.find(filter, { projection }).sort(sort).skip(skip).limit(limit).toArray(),
      compatCountDocuments(col, filter, req.caps),
    ]);
    res.json({ documents, total });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try {
    const doc = await req.conn.client.db(req.params.db).collection(req.params.col).findOne({ _id: parseId(req.params.id) });
    if (!doc) return res.status(404).json({ error: 'Document not found' });
    res.json(doc);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  try {
    const result = await req.conn.client.db(req.params.db).collection(req.params.col).insertOne(req.body.document);
    res.json({ insertedId: result.insertedId, ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try {
    const col = req.conn.client.db(req.params.db).collection(req.params.col);
    const result = await col.replaceOne({ _id: parseId(req.params.id) }, { ...req.body.update, _id: parseId(req.params.id) });
    res.json({ modifiedCount: result.modifiedCount, ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try {
    const result = await req.conn.client.db(req.params.db).collection(req.params.col).deleteOne({ _id: parseId(req.params.id) });
    res.json({ deletedCount: result.deletedCount, ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  try {
    const filter = parseFilter(JSON.stringify(req.body.filter || {}));
    const result = await req.conn.client.db(req.params.db).collection(req.params.col).deleteMany(filter);
    res.json({ deletedCount: result.deletedCount, ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/databases/:db/collections/:col/aggregate', getConnection, async (req, res) => {
  try {
    const col = req.conn.client.db(req.params.db).collection(req.params.col);
    const pipeline = req.body.pipeline || [];
    const warnings = [];
    for (const stage of pipeline) {
      const key = Object.keys(stage)[0];
      if (key === '$facet' && !req.caps.hasAggFacet) warnings.push(`$facet requires MongoDB 3.4+ (yours: ${req.conn.versionStr})`);
      if (key === '$lookup' && !req.caps.hasAggLookup) warnings.push(`$lookup requires MongoDB 3.2+ (yours: ${req.conn.versionStr})`);
      if (key === '$merge' && !req.caps.hasMergeStage) warnings.push(`$merge requires MongoDB 4.2+ (yours: ${req.conn.versionStr})`);
      if (key === '$unionWith' && !req.caps.hasUnionWith) warnings.push(`$unionWith requires MongoDB 4.4+ (yours: ${req.conn.versionStr})`);
      if ((key === '$densify' || key === '$fill') && !req.caps.hasDensifyFill) warnings.push(`${key} requires MongoDB 5.1+ (yours: ${req.conn.versionStr})`);
    }
    const results = await col.aggregate(pipeline).toArray();
    const response = { results };
    if (warnings.length) response.warnings = warnings;
    res.json(response);
  } catch (err) {
    let msg = err.message;
    if (msg.includes('Unrecognized pipeline stage')) msg += ` (may be unsupported on MongoDB ${req.conn.versionStr})`;
    res.status(500).json({ error: msg });
  }
});

app.get('/api/databases/:db/collections/:col/indexes', getConnection, async (req, res) => {
  try { res.json({ indexes: await req.conn.client.db(req.params.db).collection(req.params.col).indexes() }); }
  catch {
    try { res.json({ indexes: await req.conn.client.db(req.params.db).collection(req.params.col).listIndexes().toArray() }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  }
});

app.post('/api/databases/:db/collections/:col/indexes', getConnection, async (req, res) => {
  try {
    const keys = req.body.keys || {};
    if (Object.values(keys).includes('$**') && !req.caps.hasWildcardIndexes) {
      return res.status(400).json({ error: `Wildcard indexes require MongoDB 4.2+ (yours: ${req.conn.versionStr})` });
    }
    const result = await req.conn.client.db(req.params.db).collection(req.params.col).createIndex(keys, req.body.options || {});
    res.json({ name: result, ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/databases/:db/collections/:col/indexes/:name', getConnection, async (req, res) => {
  try { await req.conn.client.db(req.params.db).collection(req.params.col).dropIndex(req.params.name); res.json({ ok: true }); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── SPA Catch-all ──────────────────────────────────────────────────
if (isProd) {
  app.get('*', (req, res) => { res.sendFile(join(__dirname, '..', 'dist', 'index.html')); });
}

// ─── Start ──────────────────────────────────────────────────────────
const server = createServer(app);
server.listen(PORT, '0.0.0.0', () => {
  console.log(`
  ╔══════════════════════════════════════════════╗
  ║                                              ║
  ║   ⚡ MongoStudio v1.0.0                      ║
  ║                                              ║
  ║   http://localhost:${PORT}                     ║
  ║   Supports MongoDB 3.6 → 8.x                ║
  ║   Mode: ${(isProd ? 'production' : 'development').padEnd(12)}                 ║
  ║                                              ║
  ╚══════════════════════════════════════════════╝
  `);
});

function shutdown() {
  console.log('\nShutting down…');
  for (const [, { client }] of connections) client.close().catch(() => {});
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5000);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

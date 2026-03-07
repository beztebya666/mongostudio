const STORAGE_KEY = 'mongostudio_mock_state_v1';
const MAX_AUDIT = 500;
const MOCK_VERSION = '8.0.0-mock';
const MOCK_HOST = 'demo.local:27017';
const OID_RE = /^[a-f0-9]{24}$/i;

const DEFAULT_EXEC_CONFIG = {
  mode: 'safe',
  maxTimeMS: 10000,
  maxResultSize: 1000,
  allowDiskUse: false,
  blockedOperators: ['$where'],
};

const DEFAULT_SERVICE_CONFIG = {
  rateLimit: { windowMs: 60000, apiMax: 3000, heavyMax: 300 },
};

const CONNECT_CAPABILITIES = {
  countDocuments: true,
  estimatedCount: true,
  changeStreams: true,
  transactions: true,
  aggregationFacet: true,
  aggregationLookup: true,
  wildcardIndexes: true,
  timeSeries: true,
  stableApi: true,
};

const STATUS_CAPABILITIES = {
  hasCountDocuments: true,
  hasEstimatedCount: true,
  hasMergeStage: true,
  hasUnionWith: true,
  hasStableApi: true,
  hasTimeSeries: true,
  hasClustered: true,
  hasDensifyFill: true,
  hasQueryableEncryption: true,
  hasAggFacet: true,
  hasAggLookup: true,
  hasChangeStreams: true,
  hasTransactions: true,
  hasWildcardIndexes: true,
};

function now() {
  return Date.now();
}

function perfNow() {
  try {
    if (typeof performance !== 'undefined' && typeof performance.now === 'function') return performance.now();
  } catch {}
  return Date.now();
}

function clone(value) {
  try {
    if (typeof structuredClone === 'function') return structuredClone(value);
  } catch {}
  return JSON.parse(JSON.stringify(value));
}

function isObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function randomHex(len) {
  let out = '';
  while (out.length < len) out += Math.floor(Math.random() * 16).toString(16);
  return out.slice(0, len);
}

function toObjectId(value) {
  if (isObject(value) && typeof value.$oid === 'string' && OID_RE.test(value.$oid)) return { $oid: value.$oid.toLowerCase() };
  if (typeof value === 'string' && OID_RE.test(value)) return { $oid: value.toLowerCase() };
  return null;
}

function toDateObject(value) {
  if (isObject(value) && Object.prototype.hasOwnProperty.call(value, '$date')) {
    const date = new Date(value.$date);
    if (!Number.isNaN(date.getTime())) return { $date: date.toISOString() };
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) return { $date: value.toISOString() };
  return null;
}

function normalizeValue(value) {
  const oid = toObjectId(value);
  if (oid) return oid;
  const date = toDateObject(value);
  if (date) return date;
  if (Array.isArray(value)) return value.map((item) => normalizeValue(item));
  if (!isObject(value)) return value;
  const out = {};
  for (const [key, item] of Object.entries(value)) out[key] = normalizeValue(item);
  return out;
}

function normalizeDocument(doc) {
  const out = isObject(doc) ? normalizeValue(doc) : {};
  if (!Object.prototype.hasOwnProperty.call(out, '_id')) out._id = { $oid: randomHex(24) };
  const oid = toObjectId(out._id);
  if (oid) out._id = oid;
  return out;
}

function parseHost(uri) {
  const text = String(uri || '');
  if (text.startsWith('mock://')) return text.replace(/^mock:\/\//, '').split('/')[0] || MOCK_HOST;
  const match = text.match(/^mongodb(?:\+srv)?:\/\/(?:[^@]+@)?([^/?]+)/i);
  return match?.[1] || MOCK_HOST;
}

function parseUsername(uri) {
  const match = String(uri || '').match(/^mongodb(?:\+srv)?:\/\/([^@/]+)@/i);
  if (!match) return '';
  const raw = match[1];
  const idx = raw.indexOf(':');
  try {
    return decodeURIComponent(idx === -1 ? raw : raw.slice(0, idx));
  } catch {
    return idx === -1 ? raw : raw.slice(0, idx);
  }
}

function parseReadPreference(uri, options = {}) {
  if (options.readPreference) return options.readPreference;
  const match = String(uri || '').match(/[?&]readPreference=([^&]+)/i);
  if (!match) return 'primary';
  try {
    return decodeURIComponent(match[1]);
  } catch {
    return match[1];
  }
}

function parseObjectInput(input) {
  if (isObject(input)) return clone(input);
  if (typeof input === 'string') {
    try {
      const parsed = JSON.parse(input);
      if (isObject(parsed)) return parsed;
    } catch {}
    try {
      const parsed = JSON.parse(JSON.parse(input));
      if (isObject(parsed)) return parsed;
    } catch {}
  }
  return {};
}

function getByPath(doc, path) {
  if (!path) return doc;
  const parts = String(path).split('.').filter(Boolean);
  let cur = doc;
  for (const part of parts) {
    if (!cur || typeof cur !== 'object') return undefined;
    cur = cur[part];
  }
  return cur;
}

function setByPath(doc, path, value) {
  const parts = String(path).split('.').filter(Boolean);
  if (!parts.length) return;
  let cur = doc;
  for (let i = 0; i < parts.length - 1; i += 1) {
    const part = parts[i];
    if (!isObject(cur[part])) cur[part] = {};
    cur = cur[part];
  }
  cur[parts[parts.length - 1]] = value;
}

function delByPath(doc, path) {
  const parts = String(path).split('.').filter(Boolean);
  if (!parts.length) return;
  let cur = doc;
  for (let i = 0; i < parts.length - 1; i += 1) {
    if (!isObject(cur)) return;
    cur = cur[parts[i]];
  }
  if (isObject(cur)) delete cur[parts[parts.length - 1]];
}

function oidString(value) {
  const oid = toObjectId(value);
  return oid ? oid.$oid : null;
}

function dateMs(value) {
  const date = toDateObject(value);
  if (!date) return null;
  const ms = Date.parse(date.$date);
  return Number.isFinite(ms) ? ms : null;
}

function equals(left, right) {
  const leftOid = oidString(left);
  const rightOid = oidString(right);
  if (leftOid || rightOid) return leftOid === rightOid;
  const leftDate = dateMs(left);
  const rightDate = dateMs(right);
  if (leftDate !== null || rightDate !== null) return leftDate === rightDate;
  if (Array.isArray(left) || Array.isArray(right) || isObject(left) || isObject(right)) {
    try {
      return JSON.stringify(left) === JSON.stringify(right);
    } catch {
      return String(left) === String(right);
    }
  }
  return left === right;
}

function cmp(left, right) {
  const leftDate = dateMs(left);
  const rightDate = dateMs(right);
  if (leftDate !== null || rightDate !== null) return (leftDate || 0) - (rightDate || 0);
  const leftOid = oidString(left);
  const rightOid = oidString(right);
  if (leftOid || rightOid) return String(leftOid || '').localeCompare(String(rightOid || ''));
  if (typeof left === 'number' && typeof right === 'number') return left - right;
  return String(left ?? '').localeCompare(String(right ?? ''));
}

function matchesField(value, condition) {
  if (!isObject(condition)) return equals(value, condition);
  const keys = Object.keys(condition);
  if (!keys.some((key) => key.startsWith('$'))) return equals(value, condition);
  for (const [op, expected] of Object.entries(condition)) {
    if (op === '$options') continue;
    if (op === '$eq' && !equals(value, expected)) return false;
    if (op === '$ne' && equals(value, expected)) return false;
    if (op === '$gt' && !(cmp(value, expected) > 0)) return false;
    if (op === '$gte' && !(cmp(value, expected) >= 0)) return false;
    if (op === '$lt' && !(cmp(value, expected) < 0)) return false;
    if (op === '$lte' && !(cmp(value, expected) <= 0)) return false;
    if (op === '$exists') {
      const exists = value !== undefined;
      if (Boolean(expected) !== exists) return false;
    }
    if (op === '$in') {
      const values = Array.isArray(expected) ? expected : [];
      const ok = Array.isArray(value)
        ? value.some((item) => values.some((candidate) => equals(item, candidate)))
        : values.some((candidate) => equals(value, candidate));
      if (!ok) return false;
    }
    if (op === '$nin') {
      const values = Array.isArray(expected) ? expected : [];
      const ok = Array.isArray(value)
        ? value.every((item) => values.every((candidate) => !equals(item, candidate)))
        : values.every((candidate) => !equals(value, candidate));
      if (!ok) return false;
    }
    if (op === '$regex') {
      try {
        const regex = new RegExp(String(expected || ''), typeof condition.$options === 'string' ? condition.$options : '');
        if (!regex.test(String(value ?? ''))) return false;
      } catch {
        return false;
      }
    }
  }
  return true;
}

function matchesFilter(doc, filter) {
  if (!isObject(filter) || Object.keys(filter).length === 0) return true;
  for (const [key, condition] of Object.entries(filter)) {
    if (key === '$and') {
      const list = Array.isArray(condition) ? condition : [];
      if (!list.every((item) => matchesFilter(doc, item))) return false;
      continue;
    }
    if (key === '$or') {
      const list = Array.isArray(condition) ? condition : [];
      if (!list.some((item) => matchesFilter(doc, item))) return false;
      continue;
    }
    if (key === '$nor') {
      const list = Array.isArray(condition) ? condition : [];
      if (list.some((item) => matchesFilter(doc, item))) return false;
      continue;
    }
    if (key.startsWith('$')) continue;
    if (!matchesField(getByPath(doc, key), condition)) return false;
  }
  return true;
}

function applyProjection(doc, projection) {
  const entries = Object.entries(projection || {});
  if (!entries.length) return clone(doc);
  const include = entries.filter(([, value]) => Number(value) === 1 || value === true);
  const exclude = entries.filter(([, value]) => Number(value) === 0 || value === false);
  if (include.length > 0) {
    const out = {};
    for (const [path] of include) {
      const value = getByPath(doc, path);
      if (value !== undefined) setByPath(out, path, clone(value));
    }
    if (!Object.prototype.hasOwnProperty.call(projection, '_id') || projection._id !== 0) {
      if (Object.prototype.hasOwnProperty.call(doc, '_id')) out._id = clone(doc._id);
    }
    return out;
  }
  const out = clone(doc);
  for (const [path] of exclude) delByPath(out, path);
  return out;
}

function applySort(list, sortObj) {
  const entries = Object.entries(sortObj || {});
  if (!entries.length) return list;
  return [...list].sort((a, b) => {
    for (const [field, direction] of entries) {
      const result = cmp(getByPath(a, field), getByPath(b, field));
      if (result !== 0) return Number(direction) === -1 ? -result : result;
    }
    return 0;
  });
}

function containsBlockedOperator(value, blocked = []) {
  if (!blocked.length) return false;
  if (Array.isArray(value)) return value.some((item) => containsBlockedOperator(item, blocked));
  if (!isObject(value)) return false;
  for (const [key, child] of Object.entries(value)) {
    if (blocked.includes(key)) return true;
    if (containsBlockedOperator(child, blocked)) return true;
  }
  return false;
}

function estimateBytes(value) {
  try {
    return Math.max(2, JSON.stringify(value).length);
  } catch {
    return 2;
  }
}

function summarize(value) {
  if (value === undefined) return '';
  if (value === null) return 'null';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  const oid = oidString(value);
  if (oid) return oid;
  const date = toDateObject(value);
  if (date) return date.$date;
  try {
    const text = JSON.stringify(value);
    return text.length > 120 ? `${text.slice(0, 117)}...` : text;
  } catch {
    return String(value);
  }
}

function detectType(value) {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  if (oidString(value)) return 'ObjectId';
  if (toDateObject(value)) return 'Date';
  if (typeof value === 'object') return 'object';
  return typeof value;
}

function collectSchema(value, prefix, map) {
  if (!isObject(value)) return;
  for (const [key, child] of Object.entries(value)) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (!map[path]) map[path] = { count: 0, hasNull: false, sample: '', types: {} };
    const entry = map[path];
    const type = detectType(child);
    entry.count += 1;
    if (child === null) entry.hasNull = true;
    if (!entry.sample) entry.sample = summarize(child);
    entry.types[type] = (entry.types[type] || 0) + 1;
    if (Array.isArray(child)) child.forEach((item) => isObject(item) && collectSchema(item, path, map));
    else if (isObject(child) && !oidString(child) && !toDateObject(child)) collectSchema(child, path, map);
  }
}

function makeSchema(documents, sampleSize = 100) {
  const list = Array.isArray(documents) ? documents : [];
  const size = Math.max(1, Math.min(Number(sampleSize) || 100, 500));
  const shuffled = [...list];
  for (let i = shuffled.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  const sample = shuffled.slice(0, Math.min(size, shuffled.length));
  const total = sample.length || 1;
  const map = {};
  sample.forEach((doc) => collectSchema(doc, '', map));
  const fields = Object.entries(map).map(([path, info]) => ({
    path,
    count: info.count,
    pct: Math.round((info.count / total) * 100),
    hasNull: info.hasNull,
    sample: info.sample,
    types: Object.entries(info.types)
      .sort((a, b) => b[1] - a[1])
      .map(([type, count]) => ({ type, count, pct: Math.round((count / total) * 100) })),
  })).sort((a, b) => b.count - a.count);
  return { fields, sampleSize: sample.length };
}

function buildApiError(message, status = 400, errorType = 'validation') {
  const error = new Error(message);
  error.status = status;
  error.errorType = errorType;
  return error;
}

function normalizeServiceConfig(input = {}) {
  const source = isObject(input) ? input : {};
  const rate = isObject(source.rateLimit) ? source.rateLimit : {};
  return {
    rateLimit: {
      windowMs: Math.max(1000, Math.min(Number(rate.windowMs) || 60000, 15 * 60 * 1000)),
      apiMax: Math.max(10, Math.min(Number(rate.apiMax) || 3000, 100000)),
      heavyMax: Math.max(1, Math.min(Number(rate.heavyMax) || 300, 10000)),
    },
  };
}

function createSeedState() {
  return {
    version: 1,
    databases: {
      sample_mflix: {
        name: 'sample_mflix',
        collections: {
          movies: {
            name: 'movies',
            type: 'collection',
            options: {},
            documents: [
              { _id: { $oid: '000000000000000000000201' }, title: 'Neon City', year: 2022, rating: 8.1, genres: ['sci-fi', 'thriller'], director: 'A. Lane', releasedAt: { $date: '2022-06-14T00:00:00.000Z' } },
              { _id: { $oid: '000000000000000000000202' }, title: 'Forest Line', year: 2019, rating: 7.4, genres: ['drama'], director: 'M. Stone', releasedAt: { $date: '2019-09-21T00:00:00.000Z' } },
              { _id: { $oid: '000000000000000000000203' }, title: 'Zero Degrees', year: 2024, rating: 8.8, genres: ['action', 'adventure'], director: 'K. Wells', releasedAt: { $date: '2024-02-09T00:00:00.000Z' } },
            ],
            indexes: [
              { name: '_id_', key: { _id: 1 }, unique: true, v: 2, size: 192 },
              { name: 'year_-1', key: { year: -1 }, v: 2, size: 192 },
              { name: 'rating_1', key: { rating: 1 }, v: 2, size: 192 },
            ],
          },
          users: {
            name: 'users',
            type: 'collection',
            options: {},
            documents: [
              { _id: { $oid: '000000000000000000000101' }, username: 'alice', email: 'alice@example.com', role: 'admin', active: true, age: 31, lastLogin: { $date: '2026-03-06T09:10:00.000Z' } },
              { _id: { $oid: '000000000000000000000102' }, username: 'bob', email: 'bob@example.com', role: 'analyst', active: true, age: 27, lastLogin: { $date: '2026-03-04T11:45:00.000Z' } },
              { _id: { $oid: '000000000000000000000103' }, username: 'carol', email: 'carol@example.com', role: 'editor', active: false, age: 34, lastLogin: { $date: '2026-02-28T16:20:00.000Z' } },
            ],
            indexes: [
              { name: '_id_', key: { _id: 1 }, unique: true, v: 2, size: 192 },
              { name: 'username_1', key: { username: 1 }, unique: true, v: 2, size: 192 },
              { name: 'email_1', key: { email: 1 }, unique: true, v: 2, size: 192 },
            ],
          },
        },
      },
      analytics: {
        name: 'analytics',
        collections: {
          events: {
            name: 'events',
            type: 'collection',
            options: {},
            documents: [
              { _id: { $oid: '000000000000000000000401' }, type: 'click', page: '/dashboard', user: 'alice', latencyMs: 41, createdAt: { $date: '2026-03-07T09:30:00.000Z' } },
              { _id: { $oid: '000000000000000000000402' }, type: 'query', page: '/collection/users', user: 'bob', latencyMs: 83, createdAt: { $date: '2026-03-07T09:35:00.000Z' } },
            ],
            indexes: [
              { name: '_id_', key: { _id: 1 }, unique: true, v: 2, size: 192 },
              { name: 'type_1', key: { type: 1 }, v: 2, size: 192 },
            ],
          },
        },
      },
    },
    serviceConfig: clone(DEFAULT_SERVICE_CONFIG),
    auditLog: [],
  };
}

function loadState() {
  try {
    if (typeof localStorage === 'undefined') return createSeedState();
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return createSeedState();
    const parsed = JSON.parse(raw);
    if (!isObject(parsed) || !isObject(parsed.databases)) return createSeedState();
    return parsed;
  } catch {
    return createSeedState();
  }
}

function saveState(state) {
  try {
    if (typeof localStorage === 'undefined') return;
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {}
}

export class MockApiClient {
  constructor() {
    this.connectionId = null;
    this.uiUsername = null;
    this.session = null;
    this.execConfig = { ...DEFAULT_EXEC_CONFIG };
    this.state = loadState();
    this.startedAt = now();
    this.opCounters = { insert: 0, query: 0, update: 0, delete: 0 };
    if (!isObject(this.state.serviceConfig)) this.state.serviceConfig = clone(DEFAULT_SERVICE_CONFIG);
    if (!Array.isArray(this.state.auditLog)) this.state.auditLog = [];
    saveState(this.state);
  }

  _reload() {
    this.state = loadState();
    if (!isObject(this.state.serviceConfig)) this.state.serviceConfig = clone(DEFAULT_SERVICE_CONFIG);
    if (!Array.isArray(this.state.auditLog)) this.state.auditLog = [];
  }

  _persist() {
    saveState(this.state);
  }

  _requireConnection() {
    if (!this.connectionId || !this.session) throw buildApiError('Not connected.', 401, 'auth');
  }

  _withElapsed(start, payload) {
    if (isObject(payload) && !Object.prototype.hasOwnProperty.call(payload, '_elapsed')) {
      payload._elapsed = Math.max(1, Math.round(perfNow() - start));
    }
    return payload;
  }

  _audit(action, details = {}) {
    const entry = {
      ts: now(),
      action,
      connId: this.connectionId || 'mock',
      user: this.uiUsername || this.session?.username || 'anonymous',
      host: this.session?.host || MOCK_HOST,
      ...details,
    };
    this.state.auditLog.push(entry);
    if (this.state.auditLog.length > MAX_AUDIT) this.state.auditLog = this.state.auditLog.slice(-MAX_AUDIT);
    this._persist();
  }

  _db(name, create = false) {
    const dbName = String(name || '').trim();
    if (!dbName) return null;
    if (!this.state.databases[dbName] && create) this.state.databases[dbName] = { name: dbName, collections: {} };
    return this.state.databases[dbName] || null;
  }

  _col(dbName, colName, create = false) {
    const db = this._db(dbName, create);
    if (!db) return null;
    const name = String(colName || '').trim();
    if (!name) return null;
    if (!db.collections[name] && create) {
      db.collections[name] = {
        name,
        type: 'collection',
        options: {},
        documents: [],
        indexes: [{ name: '_id_', key: { _id: 1 }, unique: true, v: 2, size: 192 }],
      };
    }
    return db.collections[name] || null;
  }

  _collectionStats(collection) {
    const docs = collection?.documents || [];
    const indexes = collection?.indexes || [];
    const size = docs.reduce((sum, doc) => sum + estimateBytes(doc), 0);
    const count = docs.length;
    return {
      count,
      size,
      avgObjSize: count ? Math.round(size / count) : 0,
      storageSize: size,
      totalIndexSize: indexes.length * 192,
      indexSizes: Object.fromEntries(indexes.map((idx) => [idx.name, idx.size || 192])),
      nindexes: indexes.length,
    };
  }

  _dbStats(dbRecord) {
    const collections = Object.values(dbRecord?.collections || {});
    let objects = 0;
    let dataSize = 0;
    let indexes = 0;
    let indexSize = 0;
    collections.forEach((collection) => {
      const stats = this._collectionStats(collection);
      objects += stats.count;
      dataSize += stats.size;
      indexes += stats.nindexes;
      indexSize += stats.totalIndexSize;
    });
    return {
      db: dbRecord?.name || '',
      collections: collections.length,
      views: 0,
      objects,
      avgObjSize: objects ? Math.round(dataSize / objects) : 0,
      dataSize,
      storageSize: dataSize,
      indexes,
      indexSize,
      fsUsedSize: 0,
      fsTotalSize: 0,
      ok: 1,
    };
  }

  _find(db, col, { filter = '{}', sort = '{}', skip = 0, limit = 50, projection = '{}' } = {}) {
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const filterObj = parseObjectInput(filter);
    if (this.execConfig.mode === 'safe' && containsBlockedOperator(filterObj, this.execConfig.blockedOperators || [])) {
      throw buildApiError('Blocked operator in Safe mode.', 400, 'validation');
    }
    const sortObj = parseObjectInput(sort);
    const projectionObj = parseObjectInput(projection);
    const matched = collection.documents.filter((doc) => matchesFilter(doc, filterObj));
    const sorted = applySort(matched, sortObj);
    const start = Math.max(0, Number(skip) || 0);
    const requested = Math.max(1, Number(limit) || 50);
    const cap = this.execConfig.mode === 'safe'
      ? Math.max(1, Math.min(Number(this.execConfig.maxResultSize) || 50, 50000))
      : 50000;
    const effective = Math.min(requested, cap);
    const docs = sorted.slice(start, start + effective).map((doc) => applyProjection(doc, projectionObj));
    return { documents: docs, total: matched.length };
  }

  async connect(uri, options = {}) {
    const start = perfNow();
    this._reload();
    const value = String(uri || '').trim();
    if (!value) throw buildApiError('URI required', 400, 'validation');
    this.connectionId = `${randomHex(10)}${now().toString(36)}`;
    this.uiUsername = String(options.username || parseUsername(value) || '').trim() || null;
    const host = parseHost(value);
    const readPreference = parseReadPreference(value, options);
    const topology = { kind: 'standalone', role: 'standalone', setName: null, primary: host, me: host, hosts: [host] };
    this.session = { host, readPreference, isProduction: options.markAsProduction === true, topology, username: this.uiUsername };
    this.execConfig = { ...DEFAULT_EXEC_CONFIG };
    this._audit('connect', { host, version: MOCK_VERSION, mode: 'mock' });
    return this._withElapsed(start, {
      connectionId: this.connectionId,
      host,
      username: this.uiUsername,
      version: MOCK_VERSION,
      isProduction: this.session.isProduction,
      warnings: ['Demo mode: local mock backend in your browser.'],
      topology,
      readPreference,
      ok: true,
      capabilities: CONNECT_CAPABILITIES,
    });
  }

  async disconnect() {
    const start = perfNow();
    if (this.connectionId) this._audit('disconnect');
    this.connectionId = null;
    this.uiUsername = null;
    this.session = null;
    return this._withElapsed(start, { ok: true });
  }

  async listDatabases() {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const databases = Object.values(this.state.databases).map((db) => ({
      name: db.name,
      sizeOnDisk: this._dbStats(db).storageSize,
      empty: this._dbStats(db).objects === 0,
    })).sort((a, b) => a.name.localeCompare(b.name));
    return this._withElapsed(start, {
      databases,
      totalSize: databases.reduce((sum, db) => sum + Number(db.sizeOnDisk || 0), 0),
      version: MOCK_VERSION,
    });
  }

  async createDatabase(name) {
    const start = perfNow();
    this._requireConnection();
    const value = String(name || '').trim();
    if (!value || !/^[a-zA-Z0-9_-]+$/.test(value)) throw buildApiError('Invalid name', 400, 'validation');
    this._reload();
    this._db(value, true);
    this._persist();
    this._audit('create_db', { db: value });
    return this._withElapsed(start, { ok: true });
  }

  async dropDatabase(db) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const value = String(db || '').trim();
    delete this.state.databases[value];
    this._persist();
    this._audit('drop_db', { db: value });
    return this._withElapsed(start, { ok: true });
  }

  async listCollections(db, { withStats = false } = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const record = this._db(db, false);
    if (!record) throw buildApiError('Database not found.', 404, 'not_found');
    let collections = Object.values(record.collections).map((col) => ({ name: col.name, type: col.type || 'collection', options: col.options || {} })).sort((a, b) => a.name.localeCompare(b.name));
    if (withStats) {
      collections = collections.map((entry) => ({ ...entry, ...this._collectionStats(record.collections[entry.name]) }));
    }
    return this._withElapsed(start, { collections });
  }

  async getDatabaseStats(db) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const record = this._db(db, false);
    if (!record) throw buildApiError('Database not found.', 404, 'not_found');
    return this._withElapsed(start, this._dbStats(record));
  }

  async createCollection(db, name) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const colName = String(name || '').trim();
    if (!colName) throw buildApiError('Name required', 400, 'validation');
    this._col(db, colName, true);
    this._persist();
    this._audit('create_col', { db, col: colName });
    return this._withElapsed(start, { ok: true });
  }

  async dropCollection(db, col) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const record = this._db(db, false);
    if (!record) throw buildApiError('Database not found.', 404, 'not_found');
    delete record.collections[String(col || '').trim()];
    this._persist();
    this._audit('drop_col', { db, col });
    return this._withElapsed(start, { ok: true });
  }

  async getCollectionStats(db, col) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    return this._withElapsed(start, this._collectionStats(collection));
  }

  async getSchema(db, col, sample = 100) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    return this._withElapsed(start, makeSchema(collection.documents, sample));
  }

  async getDocuments(db, col, options = {}, controller) {
    const start = perfNow();
    this._requireConnection();
    if (controller?.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    this._reload();
    const result = this._find(db, col, options);
    this.opCounters.query += 1;
    this._audit('query', { db, col, count: result.documents.length });
    return this._withElapsed(start, { documents: result.documents, total: result.total, _slow: false });
  }

  async getDocument(db, col, id) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const doc = collection.documents.find((item) => equals(item._id, id));
    if (!doc) throw buildApiError('Not found', 404, 'not_found');
    return this._withElapsed(start, clone(doc));
  }

  async insertDocument(db, col, document) {
    const start = perfNow();
    this._requireConnection();
    if (!isObject(document)) throw buildApiError('Document must be an object.', 400, 'validation');
    this._reload();
    const collection = this._col(db, col, true);
    const doc = normalizeDocument(document);
    collection.documents.push(doc);
    this.opCounters.insert += 1;
    this._persist();
    this._audit('insert', { db, col, count: 1 });
    return this._withElapsed(start, { insertedId: clone(doc._id), ok: true });
  }

  async insertDocuments(db, col, documents = []) {
    const start = perfNow();
    this._requireConnection();
    if (!Array.isArray(documents) || documents.length === 0) throw buildApiError('documents array is required.', 400, 'validation');
    if (documents.length > 10000) throw buildApiError('Too many documents in one request (max 10000).', 400, 'validation');
    this._reload();
    const collection = this._col(db, col, true);
    const insertedIds = {};
    documents.forEach((item, idx) => {
      if (!isObject(item)) throw buildApiError(`Invalid document at index ${idx}.`, 400, 'validation');
      const doc = normalizeDocument(item);
      collection.documents.push(doc);
      insertedIds[idx] = clone(doc._id);
    });
    this.opCounters.insert += documents.length;
    this._persist();
    this._audit('insert_many', { db, col, count: documents.length });
    return this._withElapsed(start, { ok: true, insertedCount: documents.length, insertedIds });
  }

  async updateDocument(db, col, id, update) {
    const start = perfNow();
    this._requireConnection();
    if (!isObject(update) || Array.isArray(update)) throw buildApiError('Update requires a JSON object.', 400, 'validation');
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const idx = collection.documents.findIndex((item) => equals(item._id, id));
    if (idx < 0) throw buildApiError('Not found', 404, 'not_found');
    const doc = normalizeDocument(update);
    doc._id = clone(collection.documents[idx]._id);
    collection.documents[idx] = doc;
    this.opCounters.update += 1;
    this._persist();
    this._audit('update', { db, col, count: 1 });
    return this._withElapsed(start, { modifiedCount: 1, ok: true });
  }

  async deleteDocument(db, col, id) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const before = collection.documents.length;
    collection.documents = collection.documents.filter((item) => !equals(item._id, id));
    const deletedCount = before - collection.documents.length;
    this.opCounters.delete += deletedCount;
    this._persist();
    this._audit('delete', { db, col, count: deletedCount });
    return this._withElapsed(start, { deletedCount, ok: true });
  }

  async deleteMany(db, col, filter) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const filterObj = parseObjectInput(filter);
    const before = collection.documents.length;
    collection.documents = collection.documents.filter((item) => !matchesFilter(item, filterObj));
    const deletedCount = before - collection.documents.length;
    this.opCounters.delete += deletedCount;
    this._persist();
    this._audit('delete_many', { db, col, count: deletedCount });
    return this._withElapsed(start, { deletedCount, ok: true });
  }

  async getIndexes(db, col) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    return this._withElapsed(start, { indexes: clone(collection.indexes || []) });
  }

  async createIndex(db, col, keys, options = {}) {
    const start = perfNow();
    this._requireConnection();
    if (!isObject(keys) || Object.keys(keys).length === 0) throw buildApiError('Index keys are required.', 400, 'validation');
    this._reload();
    const collection = this._col(db, col, true);
    const name = options.name || Object.entries(keys).map(([field, direction]) => `${field}_${direction}`).join('_') || `idx_${randomHex(6)}`;
    if (collection.indexes.some((idx) => idx.name === name)) throw buildApiError('Index already exists.', 409, 'duplicate');
    collection.indexes.push({ name, key: clone(keys), unique: options.unique === true, sparse: options.sparse === true, background: options.background !== false, v: 2, size: 192 });
    this._persist();
    this._audit('create_index', { db, col, name });
    return this._withElapsed(start, { name, ok: true });
  }

  async dropIndex(db, col, name) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const value = String(name || '').trim();
    if (value === '_id_') throw buildApiError('Cannot drop _id index.', 400, 'validation');
    const before = collection.indexes.length;
    collection.indexes = collection.indexes.filter((idx) => idx.name !== value);
    if (before === collection.indexes.length) throw buildApiError('Index not found.', 404, 'not_found');
    this._persist();
    this._audit('drop_index', { db, col, name: value });
    return this._withElapsed(start, { ok: true });
  }

  async runAggregation(db, col, pipeline, controller) {
    const start = perfNow();
    this._requireConnection();
    if (controller?.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    if (!Array.isArray(pipeline)) throw buildApiError('pipeline must be an array.', 400, 'validation');
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    if (this.execConfig.mode === 'safe' && containsBlockedOperator(pipeline, this.execConfig.blockedOperators || [])) {
      throw buildApiError('Blocked operator in Safe mode.', 400, 'validation');
    }
    let docs = clone(collection.documents || []);
    for (const stage of pipeline) {
      if (!isObject(stage)) continue;
      const [op] = Object.keys(stage);
      const spec = stage[op];
      if (op === '$match') docs = docs.filter((item) => matchesFilter(item, parseObjectInput(spec)));
      else if (op === '$sort') docs = applySort(docs, parseObjectInput(spec));
      else if (op === '$skip') docs = docs.slice(Math.max(0, Number(spec) || 0));
      else if (op === '$limit') docs = docs.slice(0, Math.max(0, Number(spec) || 0));
      else if (op === '$project') docs = docs.map((item) => applyProjection(item, spec));
      else if (op === '$count') docs = [{ [String(spec || 'count')]: docs.length }];
      else throw buildApiError(`Unrecognized pipeline stage name: ${op}`, 400, 'validation');
    }
    const cap = this.execConfig.mode === 'safe'
      ? Math.max(1, Math.min(Number(this.execConfig.maxResultSize) || 50, 50000))
      : 50000;
    const trimmed = docs.length > cap;
    const results = trimmed ? docs.slice(0, cap) : docs;
    this.opCounters.query += 1;
    this._audit('aggregate', { db, col, stages: pipeline.length, count: results.length });
    return this._withElapsed(start, { results, total: docs.length, trimmed, _slow: false });
  }

  async explain(db, col, { type = 'find', filter, pipeline, sort, hint = 'auto' } = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    let nReturned = 0;
    let indexUsed = null;
    if (type === 'aggregate') {
      const result = await this.runAggregation(db, col, Array.isArray(pipeline) ? pipeline : []);
      nReturned = Array.isArray(result.results) ? result.results.length : 0;
    } else {
      const result = this._find(db, col, { filter, sort, skip: 0, limit: 100, projection: '{}' });
      nReturned = result.documents.length;
      const parsedFilter = parseObjectInput(filter);
      if (hint && hint !== 'auto') indexUsed = hint;
      else if (Object.prototype.hasOwnProperty.call(parsedFilter, '_id')) indexUsed = '_id_';
    }
    const summary = {
      totalDocsExamined: collection.documents.length,
      totalKeysExamined: indexUsed ? Math.max(1, nReturned) : 0,
      nReturned,
      executionTimeMs: Math.max(1, Math.round(perfNow() - start)),
      isCollScan: !indexUsed,
      indexUsed,
    };
    return this._withElapsed(start, { explain: { ok: 1, mock: true, indexUsed }, summary });
  }

  async exportData(db, col, { format = 'json', filter = '{}', sort = '{}', limit = 1000, projection = '{}' } = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const result = this._find(db, col, { filter, sort, skip: 0, limit: Math.min(Number(limit) || 1000, 50000), projection });
    if (format === 'csv') {
      const keys = [...new Set(result.documents.flatMap((doc) => Object.keys(doc || {})))];
      const rows = result.documents.map((doc) => keys.map((key) => {
        const value = doc?.[key];
        if (value === null || value === undefined) return '';
        if (typeof value === 'object') return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
        const text = String(value);
        return (text.includes(',') || text.includes('"') || text.includes('\n'))
          ? `"${text.replace(/"/g, '""')}"`
          : text;
      }).join(','));
      const data = [keys.join(','), ...rows].join('\n');
      this._audit('export', { db, col, format: 'csv', count: result.documents.length });
      return this._withElapsed(start, { data, count: result.documents.length, format: 'csv' });
    }
    const data = JSON.stringify(result.documents, null, 2);
    this._audit('export', { db, col, format: 'json', count: result.documents.length });
    return this._withElapsed(start, { data, count: result.documents.length, format: 'json' });
  }

  async exportDatabase(db, { includeDocuments = true, includeIndexes = true, includeOptions = true, includeSchema = true, limitPerCollection = 0, schemaSampleSize = 150 } = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const record = this._db(db, false);
    if (!record) throw buildApiError('Database not found.', 404, 'not_found');
    const exported = {
      type: 'mongostudio-db-package',
      version: 1,
      exportedAt: new Date().toISOString(),
      source: { host: this.session?.host || MOCK_HOST, version: MOCK_VERSION },
      database: { name: record.name },
      collections: [],
    };
    let exportedDocs = 0;
    Object.values(record.collections).forEach((collection) => {
      const entry = { name: collection.name, type: collection.type || 'collection' };
      if (includeOptions) entry.options = clone(collection.options || {});
      if (includeIndexes) entry.indexes = clone(collection.indexes || []);
      if (includeDocuments) {
        const docs = clone(collection.documents || []);
        entry.documents = limitPerCollection > 0 ? docs.slice(0, limitPerCollection) : docs;
        if (limitPerCollection > 0 && docs.length > limitPerCollection) entry.truncated = true;
        exportedDocs += entry.documents.length;
      }
      if (includeSchema) {
        const schema = makeSchema(collection.documents || [], schemaSampleSize);
        entry.schema = schema.fields;
        entry.schemaSampleSize = schema.sampleSize;
      }
      exported.collections.push(entry);
    });
    const data = JSON.stringify(exported, null, 2);
    this._audit('export_db', { db: record.name, collections: exported.collections.length, docs: exportedDocs });
    return this._withElapsed(start, {
      ok: true,
      format: 'json',
      filename: `${record.name}.mongostudio-db.json`,
      collections: exported.collections.length,
      documents: exportedDocs,
      data,
    });
  }

  async importDatabase(pkg, { targetDb = '', mode = 'merge' } = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    let payload = pkg;
    if (typeof payload === 'string') {
      try {
        payload = JSON.parse(payload);
      } catch {
        throw buildApiError('Invalid package JSON.', 400, 'validation');
      }
    }
    if (!isObject(payload)) throw buildApiError('Import package is required.', 400, 'validation');
    const sourceName = payload.database?.name || payload.db || '';
    const dbName = String(targetDb || sourceName || '').trim();
    if (!dbName || !/^[a-zA-Z0-9_-]+$/.test(dbName)) throw buildApiError('Invalid target database name.', 400, 'validation');
    const collections = Array.isArray(payload.collections) ? payload.collections : [];
    if (!collections.length) throw buildApiError('Package has no collections to import.', 400, 'validation');
    if (mode === 'replace') delete this.state.databases[dbName];
    const dbRecord = this._db(dbName, true);
    let createdCollections = 0;
    let insertedDocuments = 0;
    let createdIndexes = 0;
    const warnings = [];
    collections.forEach((spec) => {
      const colName = String(spec?.name || '').trim();
      if (!colName) {
        warnings.push('Skipped collection with empty name.');
        return;
      }
      if (!dbRecord.collections[colName]) {
        dbRecord.collections[colName] = {
          name: colName,
          type: 'collection',
          options: clone(spec.options || {}),
          documents: [],
          indexes: [{ name: '_id_', key: { _id: 1 }, unique: true, v: 2, size: 192 }],
        };
        createdCollections += 1;
      }
      const collection = dbRecord.collections[colName];
      const docs = Array.isArray(spec.documents) ? spec.documents : [];
      docs.forEach((doc) => collection.documents.push(normalizeDocument(doc)));
      insertedDocuments += docs.length;
      const indexes = Array.isArray(spec.indexes) ? spec.indexes : [];
      indexes.forEach((idx) => {
        const key = isObject(idx.key) ? idx.key : null;
        if (!key) return;
        const name = idx.name || Object.entries(key).map(([field, direction]) => `${field}_${direction}`).join('_');
        if (!name || name === '_id_' || collection.indexes.some((entry) => entry.name === name)) return;
        collection.indexes.push({
          name,
          key: clone(key),
          unique: idx.unique === true,
          sparse: idx.sparse === true,
          background: idx.background !== false,
          v: Number(idx.v) || 2,
          size: 192,
        });
        createdIndexes += 1;
      });
    });
    this._persist();
    this._audit('import_db', { db: dbName, mode, collections: collections.length, docs: insertedDocuments });
    return this._withElapsed(start, { ok: true, db: dbName, mode, importedCollections: collections.length, createdCollections, insertedDocuments, createdIndexes, warnings });
  }

  async importCollection(db, { name, documents = [], indexes = [], options = {}, dropExisting = false } = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const dbName = String(db || '').trim();
    if (!dbName) throw buildApiError('Invalid database name.', 400, 'validation');
    const colName = String(name || '').trim();
    if (!colName || !/^[a-zA-Z0-9_.-]+$/.test(colName)) throw buildApiError('Invalid collection name.', 400, 'validation');
    if (!Array.isArray(documents)) throw buildApiError('documents must be an array.', 400, 'validation');
    const record = this._db(dbName, true);
    if (dropExisting) delete record.collections[colName];
    const collection = this._col(dbName, colName, true);
    collection.options = clone(options || {});
    const initialIndexes = collection.indexes.length;
    documents.forEach((doc) => collection.documents.push(normalizeDocument(doc)));
    (Array.isArray(indexes) ? indexes : []).forEach((idx) => {
      const key = isObject(idx.key) ? idx.key : null;
      if (!key) return;
      const idxName = idx.name || Object.entries(key).map(([field, direction]) => `${field}_${direction}`).join('_');
      if (!idxName || idxName === '_id_' || collection.indexes.some((entry) => entry.name === idxName)) return;
      collection.indexes.push({
        name: idxName,
        key: clone(key),
        unique: idx.unique === true,
        sparse: idx.sparse === true,
        background: idx.background !== false,
        v: Number(idx.v) || 2,
        size: 192,
      });
    });
    const insertedCount = documents.length;
    const indexCount = Math.max(0, collection.indexes.length - initialIndexes);
    this._persist();
    this._audit('import_col', { db: dbName, col: colName, docs: insertedCount, indexes: indexCount });
    return this._withElapsed(start, { ok: true, db: dbName, collection: colName, insertedCount, indexCount });
  }

  async getExecutionConfig() {
    const start = perfNow();
    this._requireConnection();
    return this._withElapsed(start, clone(this.execConfig));
  }

  async setExecutionConfig(config = {}) {
    const start = perfNow();
    this._requireConnection();
    const next = { ...this.execConfig };
    if (config.mode === 'safe' || config.mode === 'power') next.mode = config.mode;
    if (typeof config.maxTimeMS === 'number') next.maxTimeMS = Math.max(0, Math.min(300000, Math.round(config.maxTimeMS)));
    if (typeof config.maxResultSize === 'number') next.maxResultSize = Math.max(1, Math.min(50000, Math.round(config.maxResultSize)));
    if (typeof config.allowDiskUse === 'boolean') next.allowDiskUse = config.allowDiskUse;
    if (next.mode === 'safe') {
      next.blockedOperators = ['$where'];
      next.allowDiskUse = false;
    } else {
      next.blockedOperators = [];
    }
    this.execConfig = next;
    this._audit('config_change', { mode: next.mode });
    try {
      if (typeof window !== 'undefined') window.dispatchEvent(new CustomEvent('mongostudio:exec-config', { detail: clone(next) }));
    } catch {}
    return this._withElapsed(start, clone(next));
  }

  async getServerStatus() {
    const start = perfNow();
    this._requireConnection();
    const host = this.session?.host || MOCK_HOST;
    return this._withElapsed(start, {
      version: MOCK_VERSION,
      capabilities: clone(STATUS_CAPABILITIES),
      isProduction: this.session?.isProduction === true,
      topology: clone(this.session?.topology || { kind: 'standalone', role: 'standalone', primary: host, hosts: [host] }),
      serverStatus: {
        host,
        uptime: (now() - this.startedAt) / 1000,
        connections: { current: this.connectionId ? 1 : 0, available: 100 },
        opcounters: clone(this.opCounters),
        mem: { resident: 128 },
        storageEngine: { name: 'wiredTiger-mock' },
        repl: null,
      },
      hello: { isWritablePrimary: true, me: host, hosts: [host] },
    });
  }

  async getHealth() {
    const start = perfNow();
    this._requireConnection();
    return this._withElapsed(start, {
      status: 'ok',
      uptime: (now() - this.startedAt) / 1000,
      connections: this.connectionId ? 1 : 0,
      memory: { rss: 128 * 1024 * 1024, heapTotal: 64 * 1024 * 1024, heapUsed: 42 * 1024 * 1024 },
      ts: new Date().toISOString(),
      rateLimit: clone(this.state.serviceConfig?.rateLimit || DEFAULT_SERVICE_CONFIG.rateLimit),
    });
  }

  async getMetrics() {
    const start = perfNow();
    this._requireConnection();
    return this._withElapsed(start, {
      uptime: (now() - this.startedAt) / 1000,
      connections: this.connectionId ? 1 : 0,
      max: 20,
      memMB: 128,
      audit: this.state.auditLog.length,
      running: 0,
    });
  }

  async getServiceConfig() {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    return this._withElapsed(start, clone(this.state.serviceConfig || DEFAULT_SERVICE_CONFIG));
  }

  async setServiceConfig(config = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const next = normalizeServiceConfig({ ...(this.state.serviceConfig || DEFAULT_SERVICE_CONFIG), ...(isObject(config) ? config : {}) });
    this.state.serviceConfig = next;
    this._persist();
    this._audit('service_config_change', { rateWindowMs: next.rateLimit.windowMs, rateApiMax: next.rateLimit.apiMax, rateHeavyMax: next.rateLimit.heavyMax });
    return this._withElapsed(start, clone(next));
  }

  async getAuditLog({ action = '', search = '', from = null, to = null, limit = 200 } = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const actionValue = String(action || '').trim();
    const searchValue = String(search || '').trim().toLowerCase();
    const fromTs = Number(from) || 0;
    const toTs = Number(to) || 0;
    const max = Math.max(1, Math.min(Number(limit) || 200, 1000));
    let entries = this.state.auditLog.filter((entry) => entry.connId === this.connectionId);
    if (actionValue) entries = entries.filter((entry) => entry.action === actionValue);
    if (fromTs > 0) entries = entries.filter((entry) => Number(entry.ts) >= fromTs);
    if (toTs > 0) entries = entries.filter((entry) => Number(entry.ts) <= toTs);
    if (searchValue) entries = entries.filter((entry) => JSON.stringify(entry).toLowerCase().includes(searchValue));
    return this._withElapsed(start, { entries: entries.slice(-max).reverse(), total: entries.length });
  }

  async getDistinct(db, col, field) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const path = String(field || '').trim();
    const values = [];
    const seen = new Set();
    collection.documents.forEach((doc) => {
      const value = getByPath(doc, path);
      const key = JSON.stringify(value);
      if (seen.has(key)) return;
      seen.add(key);
      values.push(clone(value));
    });
    return this._withElapsed(start, { values: values.slice(0, 100) });
  }
}

export default MockApiClient;

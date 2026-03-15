const STORAGE_KEY = 'mongostudio_mock_state_v1';
const MAX_AUDIT = 500;
const MOCK_VERSION = '8.0.0-mock';
const MOCK_HOST = 'demo.local:27017';
const OID_RE = /^[a-f0-9]{24}$/i;
const SAFE_QUERY_LIMIT_MAX = 1000;
const POWER_QUERY_LIMIT_PRESET_MAX = 5000;
const QUERY_LIMIT_OVERRIDE_MAX = 50000;
const POWER_QUERY_LIMIT_MAX = 2147483000;
const SAFE_QUERY_TIMEOUT_MAX_MS = 30000;
const POWER_QUERY_TIMEOUT_PRESET_MAX_MS = 120000;
const POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS = 2147483000;
const POWER_QUERY_TIMEOUT_MAX_MS = POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS;

const DEFAULT_EXEC_CONFIG = {
  mode: 'safe',
  maxTimeMS: 5000,
  maxResultSize: 50,
  allowDiskUse: false,
  blockedOperators: ['$where'],
};

const DEFAULT_SERVICE_CONFIG = {
  rateLimit: {
    windowMs: 60000,
    apiMax: 3000,
    heavyMax: 300,
  },
  governor: {
    interactivePerConnection: 4,
    metadataPerConnection: 2,
    heavyPerConnection: 1,
    heavyGlobal: 20,
  },
  metadataCache: {
    maxEntriesPerConnection: 2000,
    ttlDbStatsMs: 30000,
    ttlCollectionStatsMs: 30000,
    ttlIndexListMs: 60000,
    ttlSchemaQuickMs: 60000,
    ttlApproxTotalMs: 30000,
    ttlExactTotalMs: 300000,
  },
};

const CONNECT_CAPABILITIES = {
  countDocuments: true,
  estimatedCount: true,
  changeStreams: true,
  transactions: true,
  shardedTransactions: true,
  aggregationFacet: true,
  aggregationLookup: true,
  wildcardIndexes: true,
  setWindowFields: true,
  timeSeries: true,
  stableApi: true,
  columnstoreIndexes: true,
  compoundWildcard: true,
  queryableEncryption: true,
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
  hasShardedTransactions: true,
  hasWildcardIndexes: true,
  hasSetWindowFields: true,
  hasColumnstoreIndexes: true,
  hasCompoundWildcard: true,
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

function getModeQueryLimitMax(mode = 'safe') {
  return mode === 'power' ? POWER_QUERY_LIMIT_MAX : QUERY_LIMIT_OVERRIDE_MAX;
}

function getModeQueryTimeoutMax(mode = 'safe') {
  return mode === 'power' ? POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS : SAFE_QUERY_TIMEOUT_MAX_MS;
}

function normalizeBudgetLimit(value, mode = 'safe', fallback = 50) {
  const modeLimitMax = getModeQueryLimitMax(mode);
  const parsed = Number(value);
  if (Number.isFinite(parsed)) return Math.max(50, Math.min(Math.round(parsed), modeLimitMax));
  return Math.max(50, Math.min(Math.round(Number(fallback) || 50), modeLimitMax));
}

function normalizeBudgetTimeoutMs(value, mode = 'safe', fallback = 5000) {
  const modeTimeoutMax = getModeQueryTimeoutMax(mode);
  const parsed = Number(value);
  if (Number.isFinite(parsed)) return Math.max(5000, Math.min(Math.round(parsed), modeTimeoutMax));
  return Math.max(5000, Math.min(Math.round(Number(fallback) || 5000), modeTimeoutMax));
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

function parseHosts(uri) {
  const host = parseHost(uri);
  return String(host || '').split(',').map((entry) => entry.trim()).filter(Boolean);
}

function parseReplicaSetName(uri, options = {}) {
  if (options.replicaSet && String(options.replicaSet).trim()) return String(options.replicaSet).trim();
  const match = String(uri || '').match(/[?&]replicaSet=([^&]+)/i);
  if (!match) return null;
  try {
    const value = decodeURIComponent(match[1]).trim();
    return value || null;
  } catch {
    return String(match[1] || '').trim() || null;
  }
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
  const optionValue = typeof options.readPreference === 'string' ? options.readPreference.trim() : '';
  if (optionValue) return { value: optionValue, explicit: true };
  const match = String(uri || '').match(/[?&]readPreference=([^&]+)/i);
  if (!match) return { value: 'primary', explicit: false };
  try {
    const decoded = decodeURIComponent(match[1] || '').trim();
    return { value: decoded || 'primary', explicit: true };
  } catch {
    const raw = String(match[1] || '').trim();
    return { value: raw || 'primary', explicit: true };
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

function parseBooleanLike(input, fallback = undefined) {
  if (input === undefined || input === null || input === '') return fallback;
  if (typeof input === 'boolean') return input;
  if (typeof input === 'number') return input !== 0;
  if (typeof input === 'string') {
    const lowered = input.trim().toLowerCase();
    if (['true', '1', 'yes', 'on'].includes(lowered)) return true;
    if (['false', '0', 'no', 'off'].includes(lowered)) return false;
  }
  return fallback;
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

function projectionFlag(value) {
  if (value === 1 || value === true) return 1;
  if (value === 0 || value === false) return 0;
  return null;
}

function strictIncludeProjectionKeys(projection) {
  if (!projection || typeof projection !== 'object' || Array.isArray(projection)) return null;
  const entries = Object.entries(projection);
  if (entries.length === 0) return null;

  const idRaw = Object.prototype.hasOwnProperty.call(projection, '_id') ? projection._id : undefined;
  const idFlag = idRaw === undefined ? null : projectionFlag(idRaw);
  if (idRaw !== undefined && idFlag === null) return null;

  const nonIdEntries = entries.filter(([key]) => key !== '_id');
  const includeOnly = nonIdEntries.every(([, value]) => projectionFlag(value) === 1);
  if (!includeOnly) return null;
  if (nonIdEntries.length === 0 && idFlag !== 1) return null;

  const keys = nonIdEntries.map(([key]) => key);
  if (idFlag !== 0) keys.unshift('_id');
  return keys;
}

function csvCell(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
  const text = String(value);
  return (text.includes(',') || text.includes('"') || text.includes('\n'))
    ? `"${text.replace(/"/g, '""')}"`
    : text;
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

function isUpdateOperatorDoc(value) {
  if (!isObject(value)) return false;
  const keys = Object.keys(value);
  return keys.length > 0 && keys.every((key) => key.startsWith('$'));
}

function toArrayValue(value) {
  if (Array.isArray(value)) return value;
  return [];
}

function valuesEqual(left, right) {
  return equals(normalizeValue(left), normalizeValue(right));
}

function buildUpsertBaseDocument(filter = {}) {
  const out = {};
  if (!isObject(filter)) return out;
  for (const [key, condition] of Object.entries(filter)) {
    if (key.startsWith('$')) continue;
    if (isObject(condition) && Object.keys(condition).some((op) => op.startsWith('$'))) continue;
    setByPath(out, key, clone(condition));
  }
  return out;
}

function applyUpdateOperators(doc, updateSpec, { isUpsertInsert = false } = {}) {
  const out = clone(doc || {});
  for (const [operator, payload] of Object.entries(updateSpec || {})) {
    if (!isObject(payload) && operator !== '$unset') {
      throw buildApiError(`Operator ${operator} requires an object payload.`, 400, 'validation');
    }
    if (operator === '$set' || operator === '$setOnInsert') {
      if (operator === '$setOnInsert' && !isUpsertInsert) continue;
      if (!isObject(payload)) throw buildApiError(`${operator} must be an object.`, 400, 'validation');
      for (const [path, value] of Object.entries(payload)) setByPath(out, path, clone(value));
      continue;
    }
    if (operator === '$unset') {
      if (!isObject(payload)) throw buildApiError('$unset must be an object.', 400, 'validation');
      for (const path of Object.keys(payload)) delByPath(out, path);
      continue;
    }
    if (operator === '$inc' || operator === '$mul') {
      for (const [path, rawValue] of Object.entries(payload || {})) {
        const amount = Number(rawValue);
        if (!Number.isFinite(amount)) throw buildApiError(`${operator} value for "${path}" must be numeric.`, 400, 'validation');
        const current = Number(getByPath(out, path) ?? 0);
        const next = operator === '$inc' ? current + amount : current * amount;
        setByPath(out, path, next);
      }
      continue;
    }
    if (operator === '$rename') {
      for (const [fromPath, toPathRaw] of Object.entries(payload || {})) {
        const toPath = String(toPathRaw || '').trim();
        if (!toPath) continue;
        const current = getByPath(out, fromPath);
        if (current !== undefined) {
          setByPath(out, toPath, clone(current));
          delByPath(out, fromPath);
        }
      }
      continue;
    }
    if (operator === '$currentDate') {
      const nowValue = { $date: new Date().toISOString() };
      for (const [path, mode] of Object.entries(payload || {})) {
        if (mode === true) {
          setByPath(out, path, clone(nowValue));
          continue;
        }
        if (isObject(mode) && mode.$type === 'timestamp') {
          setByPath(out, path, { $timestamp: String(Date.now()) });
          continue;
        }
        setByPath(out, path, clone(nowValue));
      }
      continue;
    }
    if (operator === '$min' || operator === '$max') {
      for (const [path, value] of Object.entries(payload || {})) {
        const current = getByPath(out, path);
        if (current === undefined) {
          setByPath(out, path, clone(value));
          continue;
        }
        const compare = cmp(current, value);
        if ((operator === '$min' && compare > 0) || (operator === '$max' && compare < 0)) {
          setByPath(out, path, clone(value));
        }
      }
      continue;
    }
    if (operator === '$push' || operator === '$addToSet') {
      for (const [path, rawValue] of Object.entries(payload || {})) {
        const current = toArrayValue(getByPath(out, path));
        const descriptor = isObject(rawValue) && Object.prototype.hasOwnProperty.call(rawValue, '$each')
          ? rawValue
          : null;
        const values = descriptor && Array.isArray(descriptor.$each) ? descriptor.$each : [rawValue];
        const next = [...current];
        values.forEach((value) => {
          if (operator === '$addToSet') {
            if (next.some((item) => valuesEqual(item, value))) return;
          }
          next.push(clone(value));
        });
        setByPath(out, path, next);
      }
      continue;
    }
    if (operator === '$pull') {
      for (const [path, expected] of Object.entries(payload || {})) {
        const current = toArrayValue(getByPath(out, path));
        const next = current.filter((item) => !matchesField(item, expected));
        setByPath(out, path, next);
      }
      continue;
    }
    if (operator === '$pop') {
      for (const [path, value] of Object.entries(payload || {})) {
        const current = [...toArrayValue(getByPath(out, path))];
        if (current.length === 0) continue;
        if (Number(value) === -1) current.shift();
        else current.pop();
        setByPath(out, path, current);
      }
      continue;
    }
    throw buildApiError(`Unsupported update operator "${operator}" in mock mode.`, 400, 'validation');
  }
  return normalizeValue(out);
}

function applyUpdatePipeline(doc, pipeline = []) {
  let out = clone(doc || {});
  pipeline.forEach((stage, index) => {
    if (!isObject(stage)) throw buildApiError(`update pipeline stage ${index} must be an object.`, 400, 'validation');
    const stageKeys = Object.keys(stage);
    if (stageKeys.length !== 1) throw buildApiError(`update pipeline stage ${index} must have exactly one operator.`, 400, 'validation');
    const op = stageKeys[0];
    const payload = stage[op];
    if (op === '$set' || op === '$addFields') {
      if (!isObject(payload)) throw buildApiError(`${op} stage requires an object payload.`, 400, 'validation');
      for (const [path, value] of Object.entries(payload)) setByPath(out, path, clone(value));
      return;
    }
    if (op === '$unset') {
      if (Array.isArray(payload)) {
        payload.forEach((path) => delByPath(out, path));
        return;
      }
      if (typeof payload === 'string') {
        delByPath(out, payload);
        return;
      }
      if (isObject(payload)) {
        Object.keys(payload).forEach((path) => delByPath(out, path));
        return;
      }
      throw buildApiError(`$unset stage in pipeline stage ${index} is invalid.`, 400, 'validation');
    }
    throw buildApiError(`Unsupported update pipeline stage "${op}" in mock mode.`, 400, 'validation');
  });
  return normalizeValue(out);
}

function oidString(value) {
  const oid = toObjectId(value);
  return oid ? oid.$oid : null;
}

function idCursorValue(value) {
  const oid = oidString(value);
  if (oid) return oid;
  if (typeof value === 'string') return value;
  if (value === null || value === undefined) return null;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function getDefaultIdSortDirection(sortInput) {
  const rawText = typeof sortInput === 'string' ? sortInput.trim() : '';
  const sortObj = parseObjectInput(sortInput);
  const entries = Object.entries(sortObj || {});
  if (entries.length === 0) return rawText && rawText !== '{}' ? 0 : 1;
  if (entries.length !== 1) return 0;
  const [field, dirRaw] = entries[0];
  if (field !== '_id') return 0;
  return Number(dirRaw) === -1 ? -1 : Number(dirRaw) === 1 ? 1 : 0;
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
  const size = Math.max(1, Math.min(Number(sampleSize) || 100, 5000));
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

function getAdaptiveSchemaSample(requestedSample, docCount) {
  let sample = Math.max(1, Math.min(Number(requestedSample) || 100, 5000));
  const count = Number(docCount) || 0;
  if (count >= 100_000_000) sample = Math.min(sample, 25);
  else if (count >= 50_000_000) sample = Math.min(sample, 30);
  else if (count >= 10_000_000) sample = Math.min(sample, 50);
  return sample;
}

function getCollectionIndexes(collection) {
  return Array.isArray(collection?.indexes) ? collection.indexes : [];
}

function findIndexByName(collection, name) {
  const wanted = String(name || '').trim();
  if (!wanted) return null;
  return getCollectionIndexes(collection).find((idx) => String(idx?.name || '').trim() === wanted) || null;
}

function inferIndexFromFind(collection, {
  filter = {},
  sort = {},
  hint = 'auto',
} = {}) {
  const explicitHint = typeof hint === 'string' ? hint.trim() : '';
  if (explicitHint && explicitHint !== 'auto') {
    const hinted = findIndexByName(collection, explicitHint);
    if (hinted) return hinted.name;
  }
  const sortObj = parseObjectInput(sort);
  const filterObj = parseObjectInput(filter);
  const indexes = getCollectionIndexes(collection);
  const sortFields = Object.keys(sortObj || {});
  const filterFields = Object.keys(filterObj || {}).filter((key) => !key.startsWith('$'));

  for (const idx of indexes) {
    const idxFields = Object.keys(idx?.key || {});
    if (!idxFields.length) continue;
    if (sortFields.length > 0 && sortFields.every((field, pos) => idxFields[pos] === field)) return idx.name || null;
    const firstField = idxFields[0];
    if (filterFields.includes(firstField)) return idx.name || null;
  }

  if (Object.prototype.hasOwnProperty.call(filterObj, '_id') || Object.prototype.hasOwnProperty.call(sortObj, '_id')) {
    const idIndex = findIndexByName(collection, '_id_');
    if (idIndex) return idIndex.name;
  }
  return null;
}

function inferIndexFromAggregate(collection, {
  pipeline = [],
  hint = 'auto',
} = {}) {
  const explicitHint = typeof hint === 'string' ? hint.trim() : '';
  if (explicitHint && explicitHint !== 'auto') {
    const hinted = findIndexByName(collection, explicitHint);
    if (hinted) return hinted.name;
  }
  const stages = Array.isArray(pipeline) ? pipeline : [];
  const matchStage = stages.find((stage) => isObject(stage) && isObject(stage.$match));
  const sortStage = stages.find((stage) => isObject(stage) && isObject(stage.$sort));
  return inferIndexFromFind(collection, {
    filter: matchStage?.$match || {},
    sort: sortStage?.$sort || {},
    hint: 'auto',
  });
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
  const governor = isObject(source.governor) ? source.governor : {};
  const metadataCache = isObject(source.metadataCache) ? source.metadataCache : {};
  return {
    rateLimit: {
      windowMs: Math.max(1000, Math.min(Number(rate.windowMs) || DEFAULT_SERVICE_CONFIG.rateLimit.windowMs, 15 * 60 * 1000)),
      apiMax: Math.max(10, Math.min(Number(rate.apiMax) || DEFAULT_SERVICE_CONFIG.rateLimit.apiMax, 100000)),
      heavyMax: Math.max(1, Math.min(Number(rate.heavyMax) || DEFAULT_SERVICE_CONFIG.rateLimit.heavyMax, 10000)),
    },
    governor: {
      interactivePerConnection: Math.max(1, Math.min(Number(governor.interactivePerConnection) || DEFAULT_SERVICE_CONFIG.governor.interactivePerConnection, 16)),
      metadataPerConnection: Math.max(1, Math.min(Number(governor.metadataPerConnection) || DEFAULT_SERVICE_CONFIG.governor.metadataPerConnection, 16)),
      heavyPerConnection: Math.max(1, Math.min(Number(governor.heavyPerConnection) || DEFAULT_SERVICE_CONFIG.governor.heavyPerConnection, 8)),
      heavyGlobal: Math.max(1, Math.min(Number(governor.heavyGlobal) || DEFAULT_SERVICE_CONFIG.governor.heavyGlobal, 200)),
    },
    metadataCache: {
      maxEntriesPerConnection: Math.max(200, Math.min(Number(metadataCache.maxEntriesPerConnection) || DEFAULT_SERVICE_CONFIG.metadataCache.maxEntriesPerConnection, 10000)),
      ttlDbStatsMs: Math.max(5000, Math.min(Number(metadataCache.ttlDbStatsMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlDbStatsMs, 10 * 60 * 1000)),
      ttlCollectionStatsMs: Math.max(5000, Math.min(Number(metadataCache.ttlCollectionStatsMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlCollectionStatsMs, 10 * 60 * 1000)),
      ttlIndexListMs: Math.max(5000, Math.min(Number(metadataCache.ttlIndexListMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlIndexListMs, 10 * 60 * 1000)),
      ttlSchemaQuickMs: Math.max(5000, Math.min(Number(metadataCache.ttlSchemaQuickMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlSchemaQuickMs, 10 * 60 * 1000)),
      ttlApproxTotalMs: Math.max(5000, Math.min(Number(metadataCache.ttlApproxTotalMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlApproxTotalMs, 10 * 60 * 1000)),
      ttlExactTotalMs: Math.max(5000, Math.min(Number(metadataCache.ttlExactTotalMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlExactTotalMs, 10 * 60 * 1000)),
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
    parsed.serviceConfig = normalizeServiceConfig({
      ...DEFAULT_SERVICE_CONFIG,
      ...(isObject(parsed.serviceConfig) ? parsed.serviceConfig : {}),
    });
    if (!Array.isArray(parsed.auditLog)) parsed.auditLog = [];
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
    this.jobs = new Map();
    this.state = loadState();
    this.startedAt = now();
    this.opCounters = { insert: 0, query: 0, update: 0, delete: 0 };
    this.state.serviceConfig = normalizeServiceConfig({
      ...DEFAULT_SERVICE_CONFIG,
      ...(isObject(this.state.serviceConfig) ? this.state.serviceConfig : {}),
    });
    if (!Array.isArray(this.state.auditLog)) this.state.auditLog = [];
    saveState(this.state);
  }

  _reload() {
    this.state = loadState();
    this.state.serviceConfig = normalizeServiceConfig({
      ...DEFAULT_SERVICE_CONFIG,
      ...(isObject(this.state.serviceConfig) ? this.state.serviceConfig : {}),
    });
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

  _find(db, col, { filter = '{}', sort = '{}', skip = 0, limit = 50, projection = '{}', keysetCursor } = {}, queryBudget = null) {
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const filterObj = parseObjectInput(filter);
    if (this.execConfig.mode === 'safe' && containsBlockedOperator(filterObj, this.execConfig.blockedOperators || [])) {
      throw buildApiError('Blocked operator in Safe mode.', 400, 'validation');
    }
    const rawSort = typeof sort === 'string' ? sort : JSON.stringify(sort || {});
    const sortObj = parseObjectInput(rawSort);
    const projectionObj = parseObjectInput(projection);
    const matched = collection.documents.filter((doc) => matchesFilter(doc, filterObj));
    let sorted = applySort(matched, sortObj);
    // Apply keyset pagination: filter by _id range when cursor provided and sort is default
    const idSortDirection = getDefaultIdSortDirection(rawSort);
    const isDefaultSort = idSortDirection !== 0;
    if (keysetCursor && isDefaultSort) {
      if (Object.keys(sortObj).length === 0) {
        sorted = applySort(sorted, { _id: idSortDirection });
      }
      const cursorValue = String(keysetCursor);
      sorted = sorted.filter((doc) => {
        const id = idCursorValue(doc?._id);
        if (!id) return false;
        return idSortDirection === -1 ? id < cursorValue : id > cursorValue;
      });
    }
    const start = keysetCursor && isDefaultSort ? 0 : Math.max(0, Number(skip) || 0);
    const requested = Math.max(1, Number(limit) || 50);
    const budgetLimit = Number(queryBudget?.limit);
    const modeLimitMax = getModeQueryLimitMax(this.execConfig.mode);
    const cap = Number.isFinite(budgetLimit)
      ? Math.max(50, Math.min(Math.round(budgetLimit), modeLimitMax))
      : (
        this.execConfig.mode === 'safe'
          ? Math.max(50, Math.min(Number(this.execConfig.maxResultSize) || 50, QUERY_LIMIT_OVERRIDE_MAX))
          : POWER_QUERY_LIMIT_MAX
      );
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
    const hosts = parseHosts(value);
    const readPrefMeta = parseReadPreference(value, options);
    const readPreference = readPrefMeta.value || 'primary';
    const readPreferenceExplicit = readPrefMeta.explicit === true;
    const setName = parseReplicaSetName(value, options);
    const isReplicaSet = Boolean(setName) || hosts.length > 1;
    const primary = hosts[0] || host || MOCK_HOST;
    const me = hosts[0] || host || MOCK_HOST;
    const topology = {
      kind: isReplicaSet ? 'replicaSet' : 'standalone',
      role: isReplicaSet ? 'primary' : 'standalone',
      setName: isReplicaSet ? (setName || 'rs0') : null,
      primary,
      me,
      hosts: hosts.length > 0 ? hosts : [host || MOCK_HOST],
    };
    this.session = { host, readPreference, readPreferenceExplicit, isProduction: options.markAsProduction === true, topology, username: this.uiUsername };
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
      readPreference: readPreferenceExplicit ? readPreference : null,
      readPreferenceExplicit,
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
    return this._withElapsed(start, {
      ok: true,
      closedCursors: 0,
      killedOps: { matched: 0, killed: 0, failed: 0 },
    });
  }

  abortInFlight() {}

  async resetDemoState(options = {}) {
    const start = perfNow();
    const preserveConfig = options?.preserveServiceConfig !== false;
    const previousConfig = preserveConfig ? clone(this.state?.serviceConfig || DEFAULT_SERVICE_CONFIG) : null;
    this.state = createSeedState();
    if (preserveConfig) {
      this.state.serviceConfig = normalizeServiceConfig({
        ...DEFAULT_SERVICE_CONFIG,
        ...(isObject(previousConfig) ? previousConfig : {}),
      });
    }
    this._persist();
    if (this.connectionId) {
      this._audit('demo_reset', { mode: 'mock' });
    }
    return this._withElapsed(start, { ok: true, reset: true });
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

  async dropDatabase(db, options = {}) { // eslint-disable-line no-unused-vars
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const source = String(options?.source || '').trim();
    const value = String(db || '').trim();
    delete this.state.databases[value];
    this._persist();
    this._audit('drop_db', { db: value, ...(source ? { source } : {}) });
    return this._withElapsed(start, { ok: true });
  }

  async listCollections(db, { withStats = false, source = '' } = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const record = this._db(db, false);
    if (!record) throw buildApiError('Database not found.', 404, 'not_found');
    let collections = Object.values(record.collections).map((col) => ({ name: col.name, type: col.type || 'collection', options: col.options || {} })).sort((a, b) => a.name.localeCompare(b.name));
    if (withStats) {
      const ts = now();
      collections = collections.map((entry) => {
        const stats = this._collectionStats(record.collections[entry.name]);
        return {
          ...entry,
          count: stats.count,
          size: stats.size,
          avgObjSize: stats.avgObjSize,
          nindexes: stats.nindexes,
          _source: 'live',
          _ts: ts,
        };
      });
    }
    if (source) {
      this._audit('metadata', {
        db,
        method: withStats ? 'listCollectionsWithStats' : 'listCollections',
        source,
        count: collections.length,
      });
    }
    return this._withElapsed(start, { collections });
  }

  async getDatabaseStats(db, options = {}) { // eslint-disable-line no-unused-vars
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const source = String(options?.source || '').trim();
    const record = this._db(db, false);
    if (!record) throw buildApiError('Database not found.', 404, 'not_found');
    const stats = this._dbStats(record);
    if (source) {
      this._audit('metadata', { db, method: 'dbStats', source, collections: stats.collections, count: stats.objects });
    }
    return this._withElapsed(start, { ...stats, _source: 'live', _ts: now(), _fresh: true });
  }

  async createCollection(db, name, options = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const source = String(options?.source || '').trim();
    const colName = String(name || '').trim();
    if (!colName) throw buildApiError('Name required', 400, 'validation');
    this._col(db, colName, true);
    this._persist();
    this._audit('create_col', { db, col: colName, ...(source ? { source } : {}) });
    return this._withElapsed(start, { ok: true });
  }

  async dropCollection(db, col, options = {}) { // eslint-disable-line no-unused-vars
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

  async getCollectionStats(db, col, options = {}) { // eslint-disable-line no-unused-vars
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const source = String(options?.source || '').trim();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const stats = this._collectionStats(collection);
    if (source) {
      this._audit('metadata', { db, col, method: 'collStats', source, count: stats.count });
    }
    return this._withElapsed(start, { ...stats, _source: 'live', _ts: now(), _fresh: true });
  }

  async getSchema(db, col, sample = 100, options = {}) { // eslint-disable-line no-unused-vars
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const adaptiveSample = getAdaptiveSchemaSample(sample, collection.documents?.length || 0);
    const payload = makeSchema(collection.documents, adaptiveSample);
    return this._withElapsed(start, { ...payload, _source: 'live', _ts: now() });
  }

  async getDocuments(db, col, options = {}, controller, requestOptions = {}) {
    const start = perfNow();
    this._requireConnection();
    if (controller?.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    this._reload();
    const budgetTimeout = Number(requestOptions?.budget?.timeoutMs);
    const budgetLimit = Number(requestOptions?.budget?.limit);
    const modeLimitMax = getModeQueryLimitMax(this.execConfig.mode);
    const modeTimeoutMax = getModeQueryTimeoutMax(this.execConfig.mode);
    const timeoutMs = Number.isFinite(budgetTimeout)
      ? Math.max(5000, Math.min(Math.round(budgetTimeout), modeTimeoutMax))
      : Math.max(5000, Math.min(Number(this.execConfig.maxTimeMS || 5000), modeTimeoutMax));
    const limit = Number.isFinite(budgetLimit) ? Math.max(50, Math.min(Math.round(budgetLimit), modeLimitMax)) : null;
    const keysetCursor = options?.keysetCursor ?? null;
    const rawSort = typeof options?.sort === 'string' ? options.sort : JSON.stringify(options?.sort || {});
    const isDefaultSort = getDefaultIdSortDirection(rawSort) !== 0;
    const result = this._find(db, col, options, { limit });
    const skip = keysetCursor ? 0 : Math.max(0, Number(options?.skip) || 0);
    const effectiveLimit = Math.max(50, Math.min(Number(options?.limit) || 50, limit || modeLimitMax));
    const hasMore = skip + result.documents.length < result.total;
    const lastDoc = result.documents[result.documents.length - 1];
    const returnedKeysetCursor = (isDefaultSort && lastDoc?._id != null) ? idCursorValue(lastDoc._id) : null;
    this.opCounters.query += 1;
    const source = String(requestOptions?.source || '').trim();
    this._audit('query', { db, col, count: result.documents.length, ...(source ? { source } : {}) });
    return this._withElapsed(start, {
      documents: result.documents,
      page: {
        mode: keysetCursor ? 'keyset' : 'skip',
        skip,
        limit: effectiveLimit,
        hasMore,
        nextCursor: null,
        prevCursor: null,
        keysetCursor: returnedKeysetCursor,
      },
      total: {
        state: 'ready',
        value: result.total,
        approx: false,
        source: 'exact',
        ts: now(),
      },
      totalLegacy: result.total,
      budget: {
        timeoutMs,
        overrideApplied: Boolean(requestOptions?.budget),
      },
      warnings: [],
      _slow: false,
    });
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

  async insertDocuments(db, col, documents = [], options = {}) { // eslint-disable-line no-unused-vars
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

  async deleteMany(db, col, filter, options = {}) { // eslint-disable-line no-unused-vars
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

  async operateCollection(db, col, method, payload = {}, options = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const operation = String(method || '').trim();
    if (!operation) throw buildApiError('method is required.', 400, 'validation');
    const shouldCreateCollection = [
      'insertOne',
      'insertMany',
      'updateOne',
      'updateMany',
      'replaceOne',
      'findOneAndUpdate',
      'findOneAndReplace',
      'findAndModify',
      'bulkWrite',
      'renameCollection',
      'dropIndexes',
      'hideIndex',
      'unhideIndex',
      'validateCollection',
      'reIndex',
      'countDocuments',
      'estimatedDocumentCount',
    ].includes(operation);
    const collection = this._col(db, col, shouldCreateCollection);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const input = isObject(payload) ? payload : {};
    const sortObj = parseObjectInput(input.sort || '{}');
    const projectionObj = parseObjectInput(input.projection || '{}');
    let result = null;

    const pickFirstMatch = (filterObj) => {
      const matched = collection.documents.filter((doc) => matchesFilter(doc, filterObj));
      const sorted = applySort(matched, sortObj);
      return sorted[0] || null;
    };

    const mutateWithUpdateSpec = (doc, updateSpec, context = {}) => {
      if (Array.isArray(updateSpec)) return applyUpdatePipeline(doc, updateSpec);
      if (!isUpdateOperatorDoc(updateSpec)) throw buildApiError('Update must use operators like $set or be an update pipeline.', 400, 'validation');
      return applyUpdateOperators(doc, updateSpec, context);
    };

    if (operation === 'insertOne') {
      const document = input.document;
      if (!isObject(document)) throw buildApiError('payload.document must be an object.', 400, 'validation');
      const normalized = normalizeDocument(document);
      collection.documents.push(normalized);
      this.opCounters.insert += 1;
      result = { insertedId: clone(normalized._id), insertedCount: 1 };
    } else if (operation === 'insertMany') {
      const docs = Array.isArray(input.documents) ? input.documents : null;
      if (!docs || docs.length === 0) throw buildApiError('payload.documents must be a non-empty array.', 400, 'validation');
      if (docs.length > 10000) throw buildApiError('Too many documents in one request (max 10000).', 400, 'validation');
      const insertedIds = {};
      docs.forEach((doc, idx) => {
        if (!isObject(doc)) throw buildApiError(`payload.documents[${idx}] must be an object.`, 400, 'validation');
        const normalized = normalizeDocument(doc);
        collection.documents.push(normalized);
        insertedIds[idx] = clone(normalized._id);
      });
      this.opCounters.insert += docs.length;
      result = { insertedIds, insertedCount: docs.length };
    } else if (operation === 'replaceOne') {
      const filterObj = parseObjectInput(input.filter || '{}');
      const replacement = input.replacement;
      if (!isObject(replacement)) throw buildApiError('payload.replacement must be an object.', 400, 'validation');
      if (isUpdateOperatorDoc(replacement)) throw buildApiError('replacement must be a replacement document, not update operators.', 400, 'validation');
      const target = pickFirstMatch(filterObj);
      if (!target) {
        if (parseBooleanLike(input.upsert, false)) {
          const upserted = normalizeDocument({ ...buildUpsertBaseDocument(filterObj), ...replacement });
          collection.documents.push(upserted);
          this.opCounters.update += 1;
          result = { matchedCount: 0, modifiedCount: 0, upsertedCount: 1, upsertedId: clone(upserted._id) };
        } else {
          result = { matchedCount: 0, modifiedCount: 0, upsertedCount: 0, upsertedId: null };
        }
      } else {
        const idx = collection.documents.findIndex((item) => equals(item._id, target._id));
        const next = normalizeDocument(replacement);
        next._id = clone(target._id);
        collection.documents[idx] = next;
        this.opCounters.update += 1;
        result = { matchedCount: 1, modifiedCount: 1, upsertedCount: 0, upsertedId: null };
      }
    } else if (operation === 'updateOne' || operation === 'updateMany') {
      const filterObj = parseObjectInput(input.filter || '{}');
      const updateSpec = Array.isArray(input.update) ? clone(input.update) : parseObjectInput(input.update || '{}');
      const matched = applySort(collection.documents.filter((doc) => matchesFilter(doc, filterObj)), sortObj);
      const targets = operation === 'updateOne' ? matched.slice(0, 1) : matched;
      let modifiedCount = 0;
      targets.forEach((doc) => {
        const idx = collection.documents.findIndex((item) => equals(item._id, doc._id));
        if (idx < 0) return;
        const next = mutateWithUpdateSpec(collection.documents[idx], updateSpec, { isUpsertInsert: false });
        next._id = clone(collection.documents[idx]._id);
        if (!equals(next, collection.documents[idx])) modifiedCount += 1;
        collection.documents[idx] = next;
      });
      let upsertedId = null;
      let upsertedCount = 0;
      if (targets.length === 0 && parseBooleanLike(input.upsert, false)) {
        let created = normalizeDocument(buildUpsertBaseDocument(filterObj));
        created = mutateWithUpdateSpec(created, updateSpec, { isUpsertInsert: true });
        created = normalizeDocument(created);
        collection.documents.push(created);
        upsertedId = clone(created._id);
        upsertedCount = 1;
      }
      this.opCounters.update += modifiedCount + upsertedCount;
      result = {
        matchedCount: targets.length,
        modifiedCount,
        upsertedCount,
        upsertedId,
      };
    } else if (operation === 'deleteOne' || operation === 'deleteMany') {
      const filterObj = parseObjectInput(input.filter || '{}');
      const matched = applySort(collection.documents.filter((doc) => matchesFilter(doc, filterObj)), sortObj);
      const targetIds = operation === 'deleteOne'
        ? matched.slice(0, 1).map((doc) => doc._id)
        : matched.map((doc) => doc._id);
      const before = collection.documents.length;
      collection.documents = collection.documents.filter((doc) => !targetIds.some((id) => equals(id, doc._id)));
      const deletedCount = before - collection.documents.length;
      this.opCounters.delete += deletedCount;
      result = { deletedCount };
    } else if (operation === 'remove') {
      const filterObj = parseObjectInput((input.query ?? input.filter) || '{}');
      const justOne = parseBooleanLike(input.justOne, false);
      const matched = applySort(collection.documents.filter((doc) => matchesFilter(doc, filterObj)), sortObj);
      const targetIds = justOne
        ? matched.slice(0, 1).map((doc) => doc._id)
        : matched.map((doc) => doc._id);
      const before = collection.documents.length;
      collection.documents = collection.documents.filter((doc) => !targetIds.some((id) => equals(id, doc._id)));
      const deletedCount = before - collection.documents.length;
      this.opCounters.delete += deletedCount;
      result = { deletedCount, justOne: Boolean(justOne) };
    } else if (operation === 'findOneAndDelete') {
      const filterObj = parseObjectInput(input.filter || '{}');
      const target = pickFirstMatch(filterObj);
      if (!target) {
        result = { value: null, ok: 1, lastErrorObject: { n: 0 } };
      } else {
        collection.documents = collection.documents.filter((doc) => !equals(doc._id, target._id));
        this.opCounters.delete += 1;
        result = { value: applyProjection(clone(target), projectionObj), ok: 1, lastErrorObject: { n: 1 } };
      }
    } else if (operation === 'findOneAndUpdate' || operation === 'findOneAndReplace') {
      const filterObj = parseObjectInput(input.filter || '{}');
      const target = pickFirstMatch(filterObj);
      const returnDocument = String(input.returnDocument || (parseBooleanLike(input.returnNewDocument, false) ? 'after' : 'before')).toLowerCase() === 'after'
        ? 'after'
        : 'before';
      let value = null;
      let n = 0;
      let upserted = null;
      if (!target) {
        if (parseBooleanLike(input.upsert, false)) {
          let created = normalizeDocument(buildUpsertBaseDocument(filterObj));
          if (operation === 'findOneAndUpdate') {
            const updateSpec = Array.isArray(input.update) ? clone(input.update) : parseObjectInput(input.update || '{}');
            created = mutateWithUpdateSpec(created, updateSpec, { isUpsertInsert: true });
          } else {
            const replacement = input.replacement;
            if (!isObject(replacement)) throw buildApiError('payload.replacement must be an object.', 400, 'validation');
            if (isUpdateOperatorDoc(replacement)) throw buildApiError('replacement must not use update operators.', 400, 'validation');
            created = normalizeDocument({ ...created, ...replacement });
          }
          created = normalizeDocument(created);
          collection.documents.push(created);
          this.opCounters.update += 1;
          n = 1;
          upserted = clone(created._id);
          value = returnDocument === 'after' ? applyProjection(clone(created), projectionObj) : null;
        } else {
          value = null;
          n = 0;
        }
      } else {
        const idx = collection.documents.findIndex((doc) => equals(doc._id, target._id));
        const beforeDoc = clone(collection.documents[idx]);
        let afterDoc = null;
        if (operation === 'findOneAndUpdate') {
          const updateSpec = Array.isArray(input.update) ? clone(input.update) : parseObjectInput(input.update || '{}');
          afterDoc = mutateWithUpdateSpec(collection.documents[idx], updateSpec, { isUpsertInsert: false });
          afterDoc._id = clone(collection.documents[idx]._id);
        } else {
          const replacement = input.replacement;
          if (!isObject(replacement)) throw buildApiError('payload.replacement must be an object.', 400, 'validation');
          if (isUpdateOperatorDoc(replacement)) throw buildApiError('replacement must not use update operators.', 400, 'validation');
          afterDoc = normalizeDocument(replacement);
          afterDoc._id = clone(collection.documents[idx]._id);
        }
        collection.documents[idx] = afterDoc;
        this.opCounters.update += 1;
        n = 1;
        value = returnDocument === 'after'
          ? applyProjection(clone(afterDoc), projectionObj)
          : applyProjection(clone(beforeDoc), projectionObj);
      }
      result = {
        value,
        ok: 1,
        lastErrorObject: {
          n,
          updatedExisting: upserted ? false : n > 0,
          ...(upserted ? { upserted } : {}),
        },
      };
    } else if (operation === 'findAndModify') {
      const filterObj = parseObjectInput((input.query ?? input.filter) || '{}');
      const fieldsObj = parseObjectInput((input.fields ?? input.projection) || '{}');
      const removeMode = parseBooleanLike(input.remove, false);
      if (removeMode) {
        const target = pickFirstMatch(filterObj);
        if (!target) {
          result = { value: null, ok: 1, lastErrorObject: { n: 0 } };
        } else {
          collection.documents = collection.documents.filter((doc) => !equals(doc._id, target._id));
          this.opCounters.delete += 1;
          result = { value: applyProjection(clone(target), fieldsObj), ok: 1, lastErrorObject: { n: 1 } };
        }
      } else {
        const returnDocument = String(input.returnDocument || (parseBooleanLike(input.new, false) ? 'after' : 'before')).toLowerCase() === 'after'
          ? 'after'
          : 'before';
        const updateInput = input.update;
        if (updateInput === undefined || updateInput === null) throw buildApiError('payload.update is required.', 400, 'validation');
        const parsedUpdate = Array.isArray(updateInput) ? clone(updateInput) : parseObjectInput(updateInput);
        const isOperatorMode = Array.isArray(parsedUpdate) || isUpdateOperatorDoc(parsedUpdate);
        if (!isOperatorMode && !isObject(parsedUpdate)) throw buildApiError('payload.update must be an object or update pipeline.', 400, 'validation');
        if (!isOperatorMode && Object.keys(parsedUpdate).some((key) => String(key).startsWith('$'))) {
          throw buildApiError('payload.update must be either update operators or replacement fields, not a mix.', 400, 'validation');
        }

        const target = pickFirstMatch(filterObj);
        let value = null;
        let n = 0;
        let upserted = null;
        if (!target) {
          if (parseBooleanLike(input.upsert, false)) {
            let created = normalizeDocument(buildUpsertBaseDocument(filterObj));
            if (isOperatorMode) {
              created = mutateWithUpdateSpec(created, parsedUpdate, { isUpsertInsert: true });
            } else {
              created = normalizeDocument({ ...created, ...parsedUpdate });
            }
            created = normalizeDocument(created);
            collection.documents.push(created);
            this.opCounters.update += 1;
            n = 1;
            upserted = clone(created._id);
            value = returnDocument === 'after' ? applyProjection(clone(created), fieldsObj) : null;
          } else {
            value = null;
            n = 0;
          }
        } else {
          const idx = collection.documents.findIndex((doc) => equals(doc._id, target._id));
          const beforeDoc = clone(collection.documents[idx]);
          let afterDoc = null;
          if (isOperatorMode) {
            afterDoc = mutateWithUpdateSpec(collection.documents[idx], parsedUpdate, { isUpsertInsert: false });
            afterDoc._id = clone(collection.documents[idx]._id);
          } else {
            afterDoc = normalizeDocument(parsedUpdate);
            afterDoc._id = clone(collection.documents[idx]._id);
          }
          collection.documents[idx] = afterDoc;
          this.opCounters.update += 1;
          n = 1;
          value = returnDocument === 'after'
            ? applyProjection(clone(afterDoc), fieldsObj)
            : applyProjection(clone(beforeDoc), fieldsObj);
        }
        result = {
          value,
          ok: 1,
          lastErrorObject: {
            n,
            updatedExisting: upserted ? false : n > 0,
            ...(upserted ? { upserted } : {}),
          },
        };
      }
    } else if (operation === 'countDocuments') {
      const filterObj = parseObjectInput((input.filter ?? input.query) || '{}');
      const value = collection.documents.filter((doc) => matchesFilter(doc, filterObj)).length;
      this.opCounters.query += 1;
      result = { value };
    } else if (operation === 'estimatedDocumentCount') {
      this.opCounters.query += 1;
      result = { value: collection.documents.length };
    } else if (operation === 'renameCollection') {
      const to = String(input.to ?? input.newName ?? '').trim();
      if (!to) throw buildApiError('payload.to (or payload.newName) is required.', 400, 'validation');
      if (to.includes('\0')) throw buildApiError('payload.to contains an invalid null character.', 400, 'validation');
      if (to === col) {
        result = { from: col, to, renamed: false };
      } else {
        const dbRecord = this._db(db, false);
        if (!dbRecord) throw buildApiError('Database not found.', 404, 'not_found');
        const sourceCol = dbRecord.collections[col];
        if (!sourceCol) throw buildApiError('Collection not found.', 404, 'not_found');
        if (dbRecord.collections[to] && parseBooleanLike(input.dropTarget, false) !== true) {
          throw buildApiError('Target collection already exists. Use dropTarget=true to overwrite.', 409, 'duplicate');
        }
        if (dbRecord.collections[to] && parseBooleanLike(input.dropTarget, false) === true) {
          delete dbRecord.collections[to];
        }
        dbRecord.collections[to] = { ...sourceCol, name: to };
        delete dbRecord.collections[col];
        result = { from: col, to, renamed: true };
      }
    } else if (operation === 'dropIndexes') {
      const namesRaw = input.names;
      if (!Array.isArray(collection.indexes)) collection.indexes = [];
      if (namesRaw === undefined || namesRaw === null || namesRaw === '' || namesRaw === '*') {
        const before = collection.indexes.length;
        collection.indexes = collection.indexes.filter((idx) => idx.name === '_id_');
        result = { dropped: '*', droppedCount: Math.max(0, before - collection.indexes.length) };
      } else {
        const names = Array.isArray(namesRaw)
          ? namesRaw.map((value) => String(value || '').trim()).filter(Boolean)
          : [String(namesRaw || '').trim()].filter(Boolean);
        if (names.length === 0) throw buildApiError('payload.names must not be empty.', 400, 'validation');
        if (names.some((name) => name === '_id_')) throw buildApiError('Cannot drop the required _id index.', 400, 'validation');
        const before = collection.indexes.length;
        collection.indexes = collection.indexes.filter((idx) => !names.includes(String(idx?.name || '')));
        const droppedCount = Math.max(0, before - collection.indexes.length);
        result = { dropped: names, droppedCount };
      }
    } else if (operation === 'hideIndex' || operation === 'unhideIndex') {
      const name = String(input.name || '').trim();
      if (!name) throw buildApiError('payload.name is required.', 400, 'validation');
      const index = (collection.indexes || []).find((idx) => String(idx?.name || '') === name);
      if (!index) throw buildApiError('Index not found.', 404, 'not_found');
      index.hidden = operation === 'hideIndex';
      result = { name, hidden: Boolean(index.hidden), ok: 1 };
    } else if (operation === 'validateCollection') {
      const full = parseBooleanLike(input.full, false);
      result = {
        valid: true,
        result: full ? 'Mock validate (full) completed.' : 'Mock validate completed.',
        warnings: [],
        ok: 1,
      };
    } else if (operation === 'reIndex') {
      result = {
        ok: 1,
        nIndexesWas: Array.isArray(collection.indexes) ? collection.indexes.length : 0,
        nIndexes: Array.isArray(collection.indexes) ? collection.indexes.length : 0,
        msg: 'reIndex simulated in mock mode',
      };
    } else if (operation === 'bulkWrite') {
      const operations = Array.isArray(input.operations) ? input.operations : null;
      if (!operations || operations.length === 0) throw buildApiError('payload.operations must be a non-empty array.', 400, 'validation');
      if (operations.length > 10000) throw buildApiError('Too many bulk operations in one request (max 10000).', 400, 'validation');
      const ordered = parseBooleanLike(input.ordered, true);
      const insertedIds = {};
      const upsertedIds = {};
      let insertedCount = 0;
      let matchedCount = 0;
      let modifiedCount = 0;
      let deletedCount = 0;
      let upsertedCount = 0;
      for (let index = 0; index < operations.length; index += 1) {
        const item = operations[index];
        if (!isObject(item)) throw buildApiError(`payload.operations[${index}] must be an object.`, 400, 'validation');
        const keys = Object.keys(item);
        if (keys.length !== 1) throw buildApiError(`payload.operations[${index}] must define exactly one operation.`, 400, 'validation');
        const op = keys[0];
        const spec = item[op];
        try {
          if (op === 'insertOne') {
            const document = spec?.document;
            if (!isObject(document)) throw buildApiError(`payload.operations[${index}].insertOne.document must be an object.`, 400, 'validation');
            const normalized = normalizeDocument(document);
            collection.documents.push(normalized);
            insertedIds[index] = clone(normalized._id);
            insertedCount += 1;
            continue;
          }
          if (op === 'updateOne' || op === 'updateMany') {
            const filterObj = parseObjectInput(spec?.filter || '{}');
            const updateSpec = Array.isArray(spec?.update) ? clone(spec.update) : parseObjectInput(spec?.update || '{}');
            const candidates = applySort(collection.documents.filter((doc) => matchesFilter(doc, filterObj)), parseObjectInput(spec?.sort || '{}'));
            const targets = op === 'updateOne' ? candidates.slice(0, 1) : candidates;
            targets.forEach((doc) => {
              const docIndex = collection.documents.findIndex((entry) => equals(entry._id, doc._id));
              if (docIndex < 0) return;
              const next = mutateWithUpdateSpec(collection.documents[docIndex], updateSpec, { isUpsertInsert: false });
              next._id = clone(collection.documents[docIndex]._id);
              if (!equals(next, collection.documents[docIndex])) modifiedCount += 1;
              collection.documents[docIndex] = next;
            });
            matchedCount += targets.length;
            if (targets.length === 0 && parseBooleanLike(spec?.upsert, false)) {
              let created = normalizeDocument(buildUpsertBaseDocument(filterObj));
              created = mutateWithUpdateSpec(created, updateSpec, { isUpsertInsert: true });
              created = normalizeDocument(created);
              collection.documents.push(created);
              upsertedIds[index] = clone(created._id);
              upsertedCount += 1;
            }
            continue;
          }
          if (op === 'replaceOne') {
            const filterObj = parseObjectInput(spec?.filter || '{}');
            const replacement = spec?.replacement;
            if (!isObject(replacement)) throw buildApiError(`payload.operations[${index}].replaceOne.replacement must be an object.`, 400, 'validation');
            if (isUpdateOperatorDoc(replacement)) throw buildApiError('replaceOne.replacement must not use update operators.', 400, 'validation');
            const target = pickFirstMatch(filterObj);
            if (!target) {
              if (parseBooleanLike(spec?.upsert, false)) {
                const created = normalizeDocument({ ...buildUpsertBaseDocument(filterObj), ...replacement });
                collection.documents.push(created);
                upsertedIds[index] = clone(created._id);
                upsertedCount += 1;
              }
              continue;
            }
            const docIndex = collection.documents.findIndex((entry) => equals(entry._id, target._id));
            const next = normalizeDocument(replacement);
            next._id = clone(target._id);
            collection.documents[docIndex] = next;
            matchedCount += 1;
            modifiedCount += 1;
            continue;
          }
          if (op === 'deleteOne' || op === 'deleteMany') {
            const filterObj = parseObjectInput(spec?.filter || '{}');
            const matchedDocs = applySort(collection.documents.filter((doc) => matchesFilter(doc, filterObj)), parseObjectInput(spec?.sort || '{}'));
            const targetIds = op === 'deleteOne'
              ? matchedDocs.slice(0, 1).map((doc) => doc._id)
              : matchedDocs.map((doc) => doc._id);
            const before = collection.documents.length;
            collection.documents = collection.documents.filter((doc) => !targetIds.some((id) => equals(id, doc._id)));
            deletedCount += before - collection.documents.length;
            continue;
          }
          throw buildApiError(`Unsupported bulk operation "${op}".`, 400, 'validation');
        } catch (err) {
          if (ordered !== false) throw err;
        }
      }
      this.opCounters.insert += insertedCount;
      this.opCounters.update += modifiedCount + upsertedCount;
      this.opCounters.delete += deletedCount;
      result = {
        insertedCount,
        matchedCount,
        modifiedCount,
        deletedCount,
        upsertedCount,
        insertedIds,
        upsertedIds,
      };
    } else {
      throw buildApiError(`Unsupported method "${operation}".`, 400, 'validation');
    }

    this._persist();
    const action = (
      operation === 'insertMany' ? 'insert_many'
      : operation === 'deleteMany' || (operation === 'remove' && parseBooleanLike(input.justOne, false) !== true) ? 'delete_many'
      : operation === 'bulkWrite' ? 'bulk_write'
      : operation === 'renameCollection' ? 'rename_collection'
      : operation === 'dropIndexes' ? 'drop_index'
      : operation === 'hideIndex' || operation === 'unhideIndex' ? 'update_index'
      : operation === 'countDocuments' || operation === 'estimatedDocumentCount' ? 'query'
      : operation === 'validateCollection' || operation === 'reIndex' ? 'admin'
      : operation === 'deleteOne'
        || operation === 'findOneAndDelete'
        || (operation === 'remove' && parseBooleanLike(input.justOne, false) === true)
        || (operation === 'findAndModify' && parseBooleanLike(input.remove, false) === true)
        ? 'delete'
      : operation === 'insertOne' ? 'insert'
      : operation === 'updateMany' ? 'update_many'
      : 'update'
    );
    const count = Number(
      result?.insertedCount
      ?? result?.deletedCount
      ?? result?.modifiedCount
      ?? result?.matchedCount
      ?? 0
    );
    const source = String(options?.source || '').trim();
    this._audit(action, {
      db,
      col,
      method: operation,
      count: Number.isFinite(count) ? count : 0,
      ...(source ? { source } : {}),
    });
    return this._withElapsed(start, { ok: true, method: operation, db, col, result });
  }

  async getIndexes(db, col, options = {}) { // eslint-disable-line no-unused-vars
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const limit = normalizeBudgetLimit(options?.budget?.limit, this.execConfig.mode, this.execConfig.maxResultSize);
    const allIndexes = clone(collection.indexes || []);
    const indexes = allIndexes.slice(0, limit);
    const source = String(options?.source || '').trim();
    if (source) {
      this._audit('metadata', { db, col, method: 'listIndexes', source, count: allIndexes.length });
    }
    return this._withElapsed(start, {
      indexes,
      total: allIndexes.length,
      limit,
      truncated: allIndexes.length > indexes.length,
      _source: 'live',
      _ts: now(),
    });
  }

  async createIndex(db, col, keys, options = {}) {
    const start = perfNow();
    this._requireConnection();
    if (!isObject(keys) || Object.keys(keys).length === 0) throw buildApiError('Index keys are required.', 400, 'validation');
    this._reload();
    const collection = this._col(db, col, true);
    const source = String(options?.source || '').trim();
    const name = options.name || Object.entries(keys).map(([field, direction]) => `${field}_${direction}`).join('_') || `idx_${randomHex(6)}`;
    if (collection.indexes.some((idx) => idx.name === name)) throw buildApiError('Index already exists.', 409, 'duplicate');
    collection.indexes.push({ name, key: clone(keys), unique: options.unique === true, sparse: options.sparse === true, background: options.background !== false, v: 2, size: 192 });
    this._persist();
    this._audit('create_index', { db, col, name, ...(source ? { source } : {}) });
    return this._withElapsed(start, { name, ok: true });
  }

  async dropIndex(db, col, name, options = {}) { // eslint-disable-line no-unused-vars
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
    const source = String(options?.source || '').trim();
    this._audit('drop_index', { db, col, name: value, ...(source ? { source } : {}) });
    return this._withElapsed(start, { ok: true });
  }

  async runAggregation(db, col, pipeline, controller, options = {}) {
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
    const explicitHint = typeof options?.hint === 'string' ? options.hint.trim() : '';
    if (explicitHint && explicitHint !== 'auto' && !findIndexByName(collection, explicitHint)) {
      throw buildApiError(`Hint index "${explicitHint}" not found.`, 400, 'validation');
    }
    const cap = normalizeBudgetLimit(
      options?.budget?.limit,
      this.execConfig.mode,
      this.execConfig.mode === 'safe'
        ? Math.max(50, Math.min(Number(this.execConfig.maxResultSize) || 50, QUERY_LIMIT_OVERRIDE_MAX))
        : POWER_QUERY_LIMIT_MAX,
    );
    const timeoutMs = normalizeBudgetTimeoutMs(options?.budget?.timeoutMs, this.execConfig.mode, this.execConfig.maxTimeMS);
    const inferredIndex = inferIndexFromAggregate(collection, { pipeline, hint: options?.hint || 'auto' });
    const estimatedDocsExamined = inferredIndex
      ? Math.min((collection.documents || []).length, Math.max(1, cap) + 1)
      : (collection.documents || []).length;
    const estimatedExecutionMs = Math.max(1, Math.round(estimatedDocsExamined / 2000));
    if (estimatedExecutionMs > timeoutMs) {
      throw buildApiError('Operation exceeded time budget.', 408, 'timeout');
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
    const trimmed = docs.length > cap;
    const results = trimmed ? docs.slice(0, cap) : docs;
    this.opCounters.query += 1;
    const source = String(options?.source || '').trim();
    this._audit('aggregate', { db, col, stages: pipeline.length, count: results.length, ...(source ? { source } : {}) });
    const response = {
      results,
      total: results.length,
      budget: {
        timeoutMs,
        limit: cap,
        overrideApplied: Boolean(options?.budget),
      },
    };
    if (trimmed) response.trimmed = true;
    if (estimatedExecutionMs > 5000) response._slow = true;
    return this._withElapsed(start, response);
  }

  async explain(db, col, { type = 'find', filter, pipeline, sort, hint = 'auto', limit, verbosity } = {}, options = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const budgetCap = normalizeBudgetLimit(
      options?.budget?.limit,
      this.execConfig.mode,
      this.execConfig.mode === 'safe'
        ? Math.max(50, Math.min(Number(this.execConfig.maxResultSize) || 50, QUERY_LIMIT_OVERRIDE_MAX))
        : POWER_QUERY_LIMIT_MAX,
    );
    const timeoutMs = normalizeBudgetTimeoutMs(options?.budget?.timeoutMs, this.execConfig.mode, this.execConfig.maxTimeMS);
    const requestedLimit = Number(limit);
    const explainLimit = Number.isFinite(requestedLimit) && requestedLimit > 0
      ? Math.max(1, Math.min(Math.round(requestedLimit), budgetCap))
      : budgetCap;
    const budgetSummary = {
      timeoutMs,
      limit: budgetCap,
      overrideApplied: Boolean(options?.budget),
    };
    const parsedFilter = parseObjectInput(filter);
    const parsedSort = parseObjectInput(sort);
    const pipelineInput = Array.isArray(pipeline) ? pipeline : [];
    const explicitHint = typeof hint === 'string' ? hint.trim() : '';
    if (explicitHint && explicitHint !== 'auto' && !findIndexByName(collection, explicitHint)) {
      throw buildApiError(`Hint index "${explicitHint}" not found.`, 400, 'validation');
    }

    // Determine plan info without executing (used for both verbosity modes)
    const indexUsed = type === 'aggregate'
      ? inferIndexFromAggregate(collection, { pipeline: pipelineInput, hint })
      : inferIndexFromFind(collection, { filter: parsedFilter, sort: parsedSort, hint });
    const isCollScan = !indexUsed;
    const indexedReadCap = type === 'aggregate'
      ? Math.min((collection.documents || []).length, Math.max(1, explainLimit) + 1)
      : Math.min((collection.documents || []).length, Math.max(1, explainLimit));
    const estimatedDocsExamined = isCollScan
      ? collection.documents.length
      : indexedReadCap;
    const estimatedKeysExamined = indexUsed ? indexedReadCap : 0;
    const estimatedExecutionMs = Math.max(1, Math.round(estimatedDocsExamined / 2000));
    if (estimatedExecutionMs > timeoutMs) {
      throw buildApiError('Operation exceeded time budget.', 408, 'timeout');
    }

    // queryPlanner: return plan info only, no execution stats
    if (verbosity === 'queryPlanner') {
      const summary = {
        totalDocsExamined: estimatedDocsExamined,
        totalKeysExamined: estimatedKeysExamined,
        nReturned: null,
        executionTimeMs: Math.min(estimatedExecutionMs, timeoutMs),
        isCollScan,
        indexUsed,
      };
      summary.isCovered = !summary.isCollScan
        && summary.totalDocsExamined === 0
        && summary.totalKeysExamined > 0;
      return this._withElapsed(start, {
        explain: { ok: 1, mock: true, indexUsed },
        summary,
        budget: budgetSummary,
      });
    }

    // executionStats (default): execute and return actual stats
    let nReturned = 0;
    if (type === 'aggregate') {
      let docs = clone(collection.documents || []);
      for (const stage of pipelineInput) {
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
      nReturned = docs.slice(0, Math.max(1, explainLimit) + 1).length;
    } else {
      const result = this._find(db, col, { filter: parsedFilter, sort: parsedSort, skip: 0, limit: explainLimit, projection: '{}' }, { limit: explainLimit });
      nReturned = result.documents.length;
    }
    const summary = {
      totalDocsExamined: isCollScan ? estimatedDocsExamined : Math.max(0, nReturned),
      totalKeysExamined: indexUsed ? Math.max(0, nReturned) : 0,
      nReturned,
      executionTimeMs: Math.max(Math.round(perfNow() - start), Math.min(estimatedExecutionMs, timeoutMs)),
      isCollScan,
      indexUsed,
    };
    summary.isCovered = !summary.isCollScan
      && summary.totalDocsExamined === 0
      && summary.totalKeysExamined > 0;
    return this._withElapsed(start, {
      explain: { ok: 1, mock: true, indexUsed },
      summary,
      budget: budgetSummary,
    });
  }

  async exportData(db, col, { format = 'json', filter = '{}', sort = '{}', limit = 1000, projection = '{}' } = {}, options = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    const filterObj = parseObjectInput(filter);
    const sortObj = parseObjectInput(sort);
    const projectionObj = parseObjectInput(projection);
    const source = String(options?.source || '').trim();
    const strictKeys = strictIncludeProjectionKeys(projectionObj);
    const rawLimit = String(limit ?? '').trim().toLowerCase();
    let effectiveLimit = 1000;
    if (rawLimit === 'unlimited' || rawLimit === 'all') effectiveLimit = Number.POSITIVE_INFINITY;
    else if (rawLimit === 'exact') {
      effectiveLimit = (collection.documents || []).filter((doc) => matchesFilter(doc, filterObj)).length;
    } else {
      const parsedLimit = Math.floor(Number(limit));
      if (!Number.isFinite(parsedLimit) || parsedLimit <= 0) effectiveLimit = Number.POSITIVE_INFINITY;
      else effectiveLimit = Math.max(1, Math.min(parsedLimit, 2147483000));
    }
    const sorted = applySort(
      (collection.documents || []).filter((doc) => matchesFilter(doc, filterObj)),
      sortObj,
    );
    const docs = sorted
      .slice(0, Number.isFinite(effectiveLimit) ? effectiveLimit : sorted.length)
      .map((doc) => applyProjection(doc, projectionObj));
    if (format === 'csv') {
      const keys = strictKeys || [...new Set(docs.flatMap((doc) => Object.keys(doc || {})))];
      const rows = docs.map((doc) => keys.map((key) => {
        const value = strictKeys ? getByPath(doc, key) : doc?.[key];
        return csvCell(value);
      }).join(','));
      const data = [keys.join(','), ...rows].join('\n');
      this._audit('export', {
        db,
        col,
        format: 'csv',
        count: docs.length,
        ...(source ? { source } : {}),
      });
      return this._withElapsed(start, { data, count: docs.length, format: 'csv' });
    }
    const data = JSON.stringify(docs, null, 2);
    this._audit('export', {
      db,
      col,
      format: 'json',
      count: docs.length,
      ...(source ? { source } : {}),
    });
    return this._withElapsed(start, { data, count: docs.length, format: 'json' });
  }

  async exportDatabase(db, { includeDocuments = true, includeIndexes = true, includeOptions = true, includeSchema = true, limitPerCollection = 0, schemaSampleSize = 150 } = {}, options = {}) { // eslint-disable-line no-unused-vars
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

  async importDatabase(pkg, { targetDb = '', mode = 'merge' } = {}, options = {}) { // eslint-disable-line no-unused-vars
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

  async importCollection(db, { name, documents = [], indexes = [], options = {}, dropExisting = false } = {}, requestOptions = {}) { // eslint-disable-line no-unused-vars
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

  async getMetadataOverview({ refresh = false, budget } = {}) { // eslint-disable-line no-unused-vars
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const databases = Object.values(this.state.databases).map((db) => ({
      name: db.name,
      sizeOnDisk: this._dbStats(db).storageSize,
      empty: this._dbStats(db).objects === 0,
    })).sort((a, b) => a.name.localeCompare(b.name));
    const stats = {};
    const freshness = {};
    const ts = now();
    databases.forEach((entry) => {
      const dbRecord = this._db(entry.name, false);
      if (!dbRecord) return;
      stats[entry.name] = this._dbStats(dbRecord);
      freshness[entry.name] = { source: 'live', ts, fresh: true };
    });
    return this._withElapsed(start, {
      databases,
      totalSize: databases.reduce((sum, db) => sum + Number(db.sizeOnDisk || 0), 0),
      version: MOCK_VERSION,
      warning: '',
      stats,
      freshness,
      budget: {
        timeoutMs: Number.isFinite(Number(budget?.timeoutMs)) ? Number(budget.timeoutMs) : Math.max(5000, Number(this.execConfig.maxTimeMS || 5000)),
        overrideApplied: Boolean(budget),
      },
    });
  }

  async startExactTotal(db, col, { filter = '{}', projection = '{}', hint = 'auto', timeoutMs = 30000 } = {}, options = {}) { // eslint-disable-line no-unused-vars
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const jobId = `job_${randomHex(8)}${now().toString(36)}`;
    const createdAt = now();
    this.jobs.set(jobId, {
      jobId,
      type: 'exact_total',
      state: 'queued',
      progressPct: 0,
      result: null,
      error: null,
      createdAt,
      updatedAt: createdAt,
    });
    setTimeout(() => {
      const current = this.jobs.get(jobId);
      if (!current) return;
      this.jobs.set(jobId, { ...current, state: 'running', progressPct: 40, updatedAt: now() });
      try {
        const collection = this._col(db, col, false);
        if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
        const filterObj = parseObjectInput(filter);
        parseObjectInput(projection);
        if (hint && hint !== 'auto' && !collection.indexes.some((idx) => idx.name === hint)) {
          throw buildApiError(`Hint index "${hint}" not found.`, 400, 'validation');
        }
        const value = collection.documents.filter((doc) => matchesFilter(doc, filterObj)).length;
        this.jobs.set(jobId, {
          ...this.jobs.get(jobId),
          state: 'done',
          progressPct: 100,
          result: { value, approx: false },
          error: null,
          updatedAt: now(),
        });
      } catch (err) {
        this.jobs.set(jobId, {
          ...this.jobs.get(jobId),
          state: 'error',
          progressPct: 100,
          result: null,
          error: String(err?.message || 'Exact total failed'),
          updatedAt: now(),
        });
      }
    }, Math.max(20, Math.min(Number(timeoutMs) || 30, 180)));
    return this._withElapsed(start, { jobId, state: 'queued' });
  }

  async getJob(jobId, options = {}) { // eslint-disable-line no-unused-vars
    const start = perfNow();
    this._requireConnection();
    const value = this.jobs.get(String(jobId || ''));
    if (!value) throw buildApiError('Job not found.', 404, 'not_found');
    return this._withElapsed(start, clone(value));
  }

  async preflight(db, col, payload = {}, options = {}) { // eslint-disable-line no-unused-vars
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const operation = String(payload?.operation || 'deleteMany').trim();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const warnings = [];
    let estimate = null;
    let risk = 'low';
    let riskScore = null;
    let explainSummary = null;
    let updateSummary = null;
    let bulkSummary = null;
    const toRisk = (score) => {
      if (score >= 80) return 'critical';
      if (score >= 55) return 'high';
      if (score >= 30) return 'medium';
      return 'low';
    };
    if (operation === 'deleteMany' || operation === 'updateMany') {
      const filterObj = parseObjectInput(payload?.filter || '{}');
      const filterIsEmpty = Object.keys(filterObj).length === 0;
      riskScore = 0;
      if (filterIsEmpty) {
        warnings.push('Empty filter targets all documents in this collection.');
      }
      estimate = collection.documents.filter((doc) => matchesFilter(doc, filterObj)).length;
      try {
        const explainResult = await this.explain(
          db,
          col,
          { type: 'find', filter: JSON.stringify(filterObj), sort: '{}', limit: 1, verbosity: 'queryPlanner' },
          options,
        );
        explainSummary = explainResult?.summary || null;
      } catch {
        warnings.push('Could not run explain within budget for deleteMany.');
      }
      if (filterIsEmpty) riskScore += 35;
      if (operation === 'updateMany') riskScore += 8;
      if (explainSummary?.isCollScan) {
        riskScore += 45;
        warnings.push('Explain detected COLLSCAN (no index used).');
      } else if (!explainSummary?.indexUsed) {
        riskScore += 10;
      }
      if (estimate !== null) {
        if (estimate > 10000000) riskScore += 25;
        else if (estimate > 1000000) riskScore += 20;
        else if (estimate > 100000) riskScore += 15;
        else if (estimate > 10000) riskScore += 10;
      }
      riskScore = Math.max(0, Math.min(100, riskScore));
      risk = toRisk(riskScore);
      if (explainSummary) {
        warnings.push(`Explain summary: ${explainSummary.isCollScan ? 'collection scan' : `index ${explainSummary.indexUsed || 'unknown'}`}.`);
      }
      if (operation === 'updateMany') {
        const updateSpec = payload?.update;
        if (Array.isArray(updateSpec)) {
          const stages = updateSpec
            .map((stage) => (isObject(stage) ? Object.keys(stage) : []))
            .flat()
            .filter(Boolean)
            .slice(0, 20);
          updateSummary = { kind: 'pipeline', stageCount: updateSpec.length, stages };
        } else if (isObject(updateSpec)) {
          const keys = Object.keys(updateSpec);
          const operators = keys.filter((key) => key.startsWith('$'));
          updateSummary = operators.length > 0
            ? { kind: 'operator', operatorCount: operators.length, operators: operators.slice(0, 20) }
            : { kind: 'replacement', fieldCount: keys.length, fields: keys.slice(0, 20) };
          if (operators.length === 0) warnings.push('Update appears to be a full-document replacement.');
        } else {
          updateSummary = { kind: 'unknown' };
        }
      }
    } else if (operation === 'bulkWrite') {
      let operations = payload?.operations;
      if (typeof operations === 'string') {
        try {
          operations = JSON.parse(operations);
        } catch (err) {
          throw buildApiError(`Invalid operations JSON: ${err.message}`, 400, 'validation');
        }
      }
      if (!Array.isArray(operations)) throw buildApiError('bulkWrite preflight expects operations array.', 400, 'validation');
      const summary = {
        total: operations.length,
        insertOne: 0,
        insertMany: 0,
        updateOne: 0,
        updateMany: 0,
        replaceOne: 0,
        deleteOne: 0,
        deleteMany: 0,
      };
      riskScore = 0;
      let estimatedAffected = 0;
      let estimatedOpsChecked = 0;
      let estimatedOpsSkipped = 0;
      for (const item of operations) {
        if (!isObject(item)) continue;
        const op = String(Object.keys(item)[0] || '');
        if (!op) continue;
        if (summary[op] !== undefined) summary[op] += 1;
        const body = item[op];
        if (
          (op === 'updateOne' || op === 'updateMany' || op === 'replaceOne' || op === 'deleteOne' || op === 'deleteMany')
          && isObject(body)
        ) {
          if (estimatedOpsChecked >= 20) {
            estimatedOpsSkipped += 1;
            continue;
          }
          estimatedOpsChecked += 1;
          const filter = parseObjectInput(body.filter || {});
          const matched = collection.documents.filter((doc) => matchesFilter(doc, filter)).length;
          if (op === 'updateMany' || op === 'deleteMany') estimatedAffected += matched;
          else estimatedAffected += Math.min(1, matched);
        }
      }
      estimate = estimatedAffected;
      if (estimatedOpsSkipped > 0) warnings.push(`Estimate limited to first ${estimatedOpsChecked} filter-based operations.`);
      riskScore += Math.min(45, Math.round(summary.total / 4));
      riskScore += summary.deleteMany * 12;
      riskScore += summary.updateMany * 10;
      riskScore += summary.deleteOne * 3;
      riskScore += summary.updateOne * 2;
      if (summary.deleteMany > 0 && estimatedAffected > 10000) riskScore += 20;
      if (summary.updateMany > 0 && estimatedAffected > 10000) riskScore += 15;
      if (summary.total >= 1000) riskScore += 25;
      else if (summary.total >= 200) riskScore += 15;
      else if (summary.total >= 50) riskScore += 8;
      riskScore = Math.max(0, Math.min(100, riskScore));
      risk = toRisk(riskScore);
      if (summary.deleteMany > 0) warnings.push('bulkWrite contains deleteMany operations.');
      if (summary.updateMany > 0) warnings.push('bulkWrite contains updateMany operations.');
      bulkSummary = {
        ...summary,
        estimatedAffected,
        estimatedOpsChecked,
        estimatedOpsSkipped,
      };
    } else if (operation === 'dropCollection') {
      estimate = collection.documents.length;
      risk = estimate > 100000 ? 'critical' : 'high';
    } else if (operation === 'export') {
      const limitRaw = String(payload?.limit ?? '').trim().toLowerCase();
      if (limitRaw === 'exact' || limitRaw === 'unlimited' || limitRaw === 'all' || !limitRaw) {
        const filterObj = parseObjectInput(payload?.filter || {});
        estimate = (collection.documents || []).filter((doc) => matchesFilter(doc, filterObj)).length;
      } else {
        const requested = Number(payload?.limit);
        estimate = Number.isFinite(requested) && requested > 0
          ? Math.floor(Math.min(requested, 2147483000))
          : (collection.documents || []).length;
      }
      risk = estimate > 50000 ? 'high' : estimate > 10000 ? 'medium' : 'low';
    } else if (operation === 'import') {
      const countFromBody = Number(payload?.documentsCount);
      estimate = Number.isFinite(countFromBody) && countFromBody >= 0
        ? Math.round(countFromBody)
        : (Array.isArray(payload?.documents) ? payload.documents.length : 0);
      risk = estimate > 50000 ? 'high' : estimate > 5000 ? 'medium' : 'low';
    } else {
      risk = 'unknown';
      warnings.push(`Unsupported preflight operation "${operation}".`);
    }
    return this._withElapsed(start, {
      operation,
      db,
      col,
      estimate,
      risk,
      riskScore,
      explainSummary,
      updateSummary,
      bulkSummary,
      warnings,
      budget: {
        timeoutMs: Number.isFinite(Number(options?.budget?.timeoutMs)) ? Number(options.budget.timeoutMs) : Math.max(5000, Number(this.execConfig.maxTimeMS || 5000)),
        overrideApplied: Boolean(options?.budget),
      },
    });
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
    const modeLimitMax = getModeQueryLimitMax(next.mode);
    const modeTimeoutMax = getModeQueryTimeoutMax(next.mode);
    if (typeof config.maxTimeMS === 'number') next.maxTimeMS = Math.max(5000, Math.min(modeTimeoutMax, Math.round(config.maxTimeMS)));
    if (typeof config.maxResultSize === 'number') next.maxResultSize = Math.max(50, Math.min(modeLimitMax, Math.round(config.maxResultSize)));
    if (typeof config.allowDiskUse === 'boolean') next.allowDiskUse = config.allowDiskUse;
    if (next.mode === 'safe') {
      next.blockedOperators = ['$where'];
      next.allowDiskUse = false;
      next.maxTimeMS = Math.max(5000, Math.min(SAFE_QUERY_TIMEOUT_MAX_MS, Math.round(next.maxTimeMS || 5000)));
      next.maxResultSize = Math.max(50, Math.min(QUERY_LIMIT_OVERRIDE_MAX, Math.round(next.maxResultSize || 50)));
    } else {
      next.blockedOperators = [];
      next.maxTimeMS = Math.max(5000, Math.min(POWER_QUERY_TIMEOUT_MAX_MS, Math.round(next.maxTimeMS || 5000)));
      next.maxResultSize = Math.max(50, Math.min(POWER_QUERY_LIMIT_MAX, Math.round(next.maxResultSize || 50)));
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
    const topology = clone(this.session?.topology || { kind: 'standalone', role: 'standalone', primary: host, me: host, hosts: [host] });
    const helloHosts = Array.isArray(topology?.hosts) && topology.hosts.length > 0 ? topology.hosts : [host];
    const helloMe = topology?.me || helloHosts[0] || host;
    const helloPrimary = topology?.primary || helloHosts[0] || host;
    const isReplicaSet = topology?.kind === 'replicaSet';
    const isWritablePrimary = topology?.role === 'primary' || topology?.role === 'standalone';
    return this._withElapsed(start, {
      version: MOCK_VERSION,
      capabilities: clone(STATUS_CAPABILITIES),
      isProduction: this.session?.isProduction === true,
      topology,
      buildInfo: {
        version: MOCK_VERSION,
        gitVersion: 'mock',
        modules: [],
        allocator: 'mock',
        javascriptEngine: 'mock',
        bits: 64,
        debug: false,
        maxBsonObjectSize: 16777216,
        storageEngines: ['wiredTiger'],
        ok: 1,
      },
      serverStatus: {
        host,
        uptime: (now() - this.startedAt) / 1000,
        connections: { current: this.connectionId ? 1 : 0, available: 100 },
        opcounters: clone(this.opCounters),
        mem: { resident: 128 },
        storageEngine: { name: 'wiredTiger-mock' },
        repl: isReplicaSet ? { setName: topology?.setName || 'rs0', hosts: helloHosts, primary: helloPrimary } : null,
      },
      hello: {
        isWritablePrimary,
        me: helloMe,
        hosts: helloHosts,
        primary: helloPrimary,
        ...(isReplicaSet ? { setName: topology?.setName || 'rs0' } : {}),
      },
      consoleShells: {
        mongo: {
          bin: 'mongo',
          displayName: 'mongo (legacy)',
          available: false,
          version: null,
          status: null,
          errorCode: 'DEMO_MODE',
          error: 'Shell execution is disabled in Demo mode.',
        },
        mongosh: {
          bin: 'mongosh',
          displayName: 'mongosh',
          available: false,
          version: null,
          status: null,
          errorCode: 'DEMO_MODE',
          error: 'Shell execution is disabled in Demo mode.',
        },
      },
    });
  }

  async getServerManagementContext(options = {}) {
    const start = perfNow();
    this._requireConnection();
    const host = this.session?.host || MOCK_HOST;
    const topology = clone(this.session?.topology || { kind: 'standalone', role: 'standalone', primary: host, me: host, hosts: [host] });
    const hosts = Array.isArray(topology?.hosts) && topology.hosts.length > 0 ? topology.hosts : [host];
    const defaultNode = topology?.me || hosts[0] || host;
    const context = {
      machine: {
        host: 'mock-runtime',
        cwd: '/mock',
        platform: 'demo',
        pid: 0,
      },
      basePath: '/tmp/mongostudio/mock',
      selectedPath: '/tmp/mongostudio/mock',
      defaultNode,
      selectedNode: defaultNode,
      nodeOverride: false,
      confirmNodeSelection: false,
      customPath: false,
      confirmPathSelection: false,
      availableNodes: hosts.map((item) => ({
        host: item,
        role: item === topology?.primary ? 'primary' : (item === topology?.me ? (topology?.role || 'member') : 'secondary'),
        isDefault: item === defaultNode,
        isPrimary: item === topology?.primary,
      })),
    };
    return this._withElapsed(start, {
      context,
      topology,
      routing: clone(this.session?.routing || { readPreference: 'primary', directConnection: false }),
      tools: [
        { id: 'serverInfo', kind: 'read', source: 'mongo', displayName: 'Server Info' },
        { id: 'mongostat', kind: 'read', source: 'binary', displayName: 'mongostat' },
        { id: 'mongotop', kind: 'read', source: 'binary', displayName: 'mongotop' },
        { id: 'slowOps', kind: 'read', source: 'mongo', displayName: 'Slow Ops' },
        { id: 'mongodump', kind: 'run', source: 'binary', displayName: 'mongodump' },
        { id: 'mongorestore', kind: 'run', source: 'binary', displayName: 'mongorestore' },
        { id: 'mongoexport', kind: 'run', source: 'binary', displayName: 'mongoexport' },
        { id: 'mongoimport', kind: 'run', source: 'binary', displayName: 'mongoimport' },
        { id: 'mongofiles', kind: 'run', source: 'binary', displayName: 'mongofiles' },
      ],
      binaries: {
        mongostat: { id: 'mongostat', bin: 'mongostat', displayName: 'mongostat', available: false, version: null, status: null, errorCode: 'DEMO_MODE', error: 'Binary execution disabled in Demo mode.' },
        mongotop: { id: 'mongotop', bin: 'mongotop', displayName: 'mongotop', available: false, version: null, status: null, errorCode: 'DEMO_MODE', error: 'Binary execution disabled in Demo mode.' },
        mongodump: { id: 'mongodump', bin: 'mongodump', displayName: 'mongodump', available: false, version: null, status: null, errorCode: 'DEMO_MODE', error: 'Binary execution disabled in Demo mode.' },
        mongorestore: { id: 'mongorestore', bin: 'mongorestore', displayName: 'mongorestore', available: false, version: null, status: null, errorCode: 'DEMO_MODE', error: 'Binary execution disabled in Demo mode.' },
        mongoexport: { id: 'mongoexport', bin: 'mongoexport', displayName: 'mongoexport', available: false, version: null, status: null, errorCode: 'DEMO_MODE', error: 'Binary execution disabled in Demo mode.' },
        mongoimport: { id: 'mongoimport', bin: 'mongoimport', displayName: 'mongoimport', available: false, version: null, status: null, errorCode: 'DEMO_MODE', error: 'Binary execution disabled in Demo mode.' },
        mongofiles: { id: 'mongofiles', bin: 'mongofiles', displayName: 'mongofiles', available: false, version: null, status: null, errorCode: 'DEMO_MODE', error: 'Binary execution disabled in Demo mode.' },
      },
      budget: {
        timeoutMs: normalizeBudgetTimeoutMs(undefined, this.execConfig.mode, this.execConfig.maxTimeMS),
        limit: normalizeBudgetLimit(undefined, this.execConfig.mode, this.execConfig.maxResultSize),
        heavyTimeoutMs: 60000,
      },
      refreshed: options?.refresh === true,
    });
  }

  async getServerManagementTool(tool, payload = {}, options = {}) {
    const start = perfNow();
    this._requireConnection();
    if (options?.controller?.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    const id = String(tool || '').trim();
    if (!id) throw buildApiError('Server-management tool id is required.', 400, 'validation');
    if (id === 'serverInfo') {
      const status = await this.getServerStatus();
      return this._withElapsed(start, {
        tool: id,
        context: {
          selectedNode: payload?.node || this.session?.host || MOCK_HOST,
          selectedPath: payload?.path || '/tmp/mongostudio/mock',
        },
        result: {
          version: status.version,
          capabilities: status.capabilities,
          topology: status.topology,
          routing: status.routing || { readPreference: 'primary', directConnection: false },
          machine: { host: 'mock-runtime', cwd: '/mock', platform: 'demo', pid: 0 },
          targetNode: payload?.node || this.session?.host || MOCK_HOST,
          buildInfo: status.buildInfo,
          serverStatus: status.serverStatus,
          hello: status.hello,
        },
      });
    }
    if (id === 'slowOps') {
      const thresholdMs = Math.max(100, Number(payload?.thresholdMs) || 1000);
      return this._withElapsed(start, {
        tool: id,
        context: {
          selectedNode: payload?.node || this.session?.host || MOCK_HOST,
          selectedPath: payload?.path || '/tmp/mongostudio/mock',
        },
        result: {
          thresholdMs,
          thresholdSec: thresholdMs / 1000,
          limit: Math.max(1, Number(payload?.limit) || 30),
          entries: [],
          total: 0,
        },
      });
    }
    if (id === 'mongostat' || id === 'mongotop') {
      return this._withElapsed(start, {
        tool: id,
        context: {
          selectedNode: payload?.node || this.session?.host || MOCK_HOST,
          selectedPath: payload?.path || '/tmp/mongostudio/mock',
        },
        result: {
          ok: false,
          code: null,
          signal: null,
          timedOut: false,
          stdout: '',
          stderr: 'Binary execution disabled in Demo mode.',
          commandPreview: `${id} --uri "mock://demo.local:27017" --rowcount 1 --json`,
          parsed: null,
          durationMs: 0,
        },
      });
    }
    throw buildApiError(`Unsupported read tool "${id}" in Demo mode.`, 400, 'validation');
  }

  async runServerManagementTool(tool, payload = {}, options = {}) { // eslint-disable-line no-unused-vars
    const start = perfNow();
    this._requireConnection();
    const id = String(tool || '').trim();
    if (!id) throw buildApiError('Server-management tool id is required.', 400, 'validation');
    return this._withElapsed(start, {
      tool: id,
      context: {
        selectedNode: payload?.node || this.session?.host || MOCK_HOST,
        selectedPath: payload?.path || '/tmp/mongostudio/mock',
      },
      result: {
        ok: false,
        code: null,
        signal: null,
        timedOut: false,
        stdout: '',
        stderr: 'Tool execution disabled in Demo mode. Connect to real MongoDB to run server-management commands.',
        commandPreview: `${id} --uri "mock://demo.local:27017"`,
        durationMs: 0,
        executionInfo: {
          tool: id,
          node: payload?.node || this.session?.host || MOCK_HOST,
          path: payload?.path || '/tmp/mongostudio/mock',
          machine: { host: 'mock-runtime', cwd: '/mock', platform: 'demo', pid: 0 },
          db: payload?.db || null,
          collection: payload?.collection || null,
        },
      },
    });
  }

  async getHealth() {
    const start = perfNow();
    return this._withElapsed(start, {
      status: 'ok',
      uptime: (now() - this.startedAt) / 1000,
      connections: this.connectionId ? 1 : 0,
      memory: {
        rss: 128 * 1024 * 1024,
        heapTotal: 64 * 1024 * 1024,
        heapUsed: 42 * 1024 * 1024,
        external: 8 * 1024 * 1024,
        arrayBuffers: 2 * 1024 * 1024,
      },
      ts: new Date().toISOString(),
      rateLimit: clone(this.state.serviceConfig?.rateLimit || DEFAULT_SERVICE_CONFIG.rateLimit),
    });
  }

  async getMetrics() {
    const start = perfNow();
    return this._withElapsed(start, {
      uptime: (now() - this.startedAt) / 1000,
      connections: this.connectionId ? 1 : 0,
      max: 20,
      memMB: 128,
      audit: this.state.auditLog.length,
      running: 0,
      mongoshSessions: 0,
    });
  }

  async getServiceConfig() {
    const start = perfNow();
    this._reload();
    return this._withElapsed(start, clone(this.state.serviceConfig || DEFAULT_SERVICE_CONFIG));
  }

  async setServiceConfig(config = {}) {
    const start = perfNow();
    this._reload();
    const next = normalizeServiceConfig({ ...(this.state.serviceConfig || DEFAULT_SERVICE_CONFIG), ...(isObject(config) ? config : {}) });
    this.state.serviceConfig = next;
    this._persist();
    this._audit('service_config_change', {
      rateWindowMs: next.rateLimit.windowMs,
      rateApiMax: next.rateLimit.apiMax,
      rateHeavyMax: next.rateLimit.heavyMax,
      govInteractive: next.governor.interactivePerConnection,
      govMetadata: next.governor.metadataPerConnection,
      govHeavy: next.governor.heavyPerConnection,
      govHeavyGlobal: next.governor.heavyGlobal,
    });
    return this._withElapsed(start, clone(next));
  }

  async getAuditLog({
    action = '',
    source = '',
    method = '',
    scope = '',
    search = '',
    from = null,
    to = null,
    limit = 200,
  } = {}, options = {}) {
    const start = perfNow();
    this._requireConnection();
    if (options?.controller?.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
    this._reload();
    const actionValue = String(action || '').trim();
    const sourceValue = String(source || '').trim().toLowerCase();
    const methodValue = String(method || '').trim().toLowerCase();
    const scopeValue = String(scope || '').trim().toLowerCase();
    const searchValue = String(search || '').trim().toLowerCase();
    const fromTs = Number(from) || 0;
    const toTs = Number(to) || 0;
    const max = Math.max(1, Math.min(Number(limit) || 200, 1000));
    let entries = this.state.auditLog.filter((entry) => entry.connId === this.connectionId);
    if (actionValue) entries = entries.filter((entry) => entry.action === actionValue);
    if (sourceValue) entries = entries.filter((entry) => String(entry.source || '').toLowerCase() === sourceValue);
    if (methodValue) entries = entries.filter((entry) => String(entry.method || '').toLowerCase() === methodValue);
    if (scopeValue) entries = entries.filter((entry) => String(entry.scope || '').toLowerCase() === scopeValue);
    if (fromTs > 0) entries = entries.filter((entry) => Number(entry.ts) >= fromTs);
    if (toTs > 0) entries = entries.filter((entry) => Number(entry.ts) <= toTs);
    if (searchValue) entries = entries.filter((entry) => JSON.stringify(entry).toLowerCase().includes(searchValue));
    return this._withElapsed(start, { entries: entries.slice(-max).reverse(), total: entries.length });
  }

  async createConsoleSession() {
    this._requireConnection();
    throw buildApiError('True shell console is unavailable in Demo mode. Switch Console to ConsoleUI.', 400, 'unsupported');
  }

  async sendConsoleCommand() {
    this._requireConnection();
    throw buildApiError('True shell console is unavailable in Demo mode. Switch Console to ConsoleUI.', 400, 'unsupported');
  }

  async interruptConsoleSession() {
    this._requireConnection();
    throw buildApiError('True shell console is unavailable in Demo mode. Switch Console to ConsoleUI.', 400, 'unsupported');
  }

  async closeConsoleSession() {
    this._requireConnection();
    return { ok: true };
  }

  async streamConsoleSession() {
    this._requireConnection();
    throw buildApiError('True shell console is unavailable in Demo mode. Switch Console to ConsoleUI.', 400, 'unsupported');
  }

  async createMongoshSession() { return this.createConsoleSession(); }

  async sendMongoshCommand() { return this.sendConsoleCommand(); }

  async interruptMongoshSession() { return this.interruptConsoleSession(); }

  async closeMongoshSession() { return this.closeConsoleSession(); }

  async streamMongoshSession() { return this.streamConsoleSession(); }

  async getDistinct(db, col, field, options = {}) {
    const start = perfNow();
    this._requireConnection();
    this._reload();
    const collection = this._col(db, col, false);
    if (!collection) throw buildApiError('Collection not found.', 404, 'not_found');
    const path = String(field || '').trim();
    if (!path) throw buildApiError('Field is required.', 400, 'validation');
    const modeLimitMax = getModeQueryLimitMax(this.execConfig.mode);
    const limit = Math.min(
      Math.max(50, Math.min(Number(this.execConfig.maxResultSize || 50), modeLimitMax)),
      1000,
    );
    const scanCap = Math.max(limit * 200, 5000);
    const distinctKey = (value) => {
      const oid = oidString(value);
      if (oid) return `oid:${oid}`;
      const ms = dateMs(value);
      if (ms !== null) return `date:${ms}`;
      if (value === null) return 'null';
      const t = typeof value;
      if (t === 'string' || t === 'number' || t === 'boolean') return `${t}:${String(value)}`;
      try {
        return `json:${JSON.stringify(value)}`;
      } catch {
        return `str:${String(value)}`;
      }
    };
    const flatten = (value, out) => {
      if (Array.isArray(value)) {
        value.forEach((item) => flatten(item, out));
        return;
      }
      if (value !== undefined) out.push(value);
    };
    const values = [];
    const seen = new Set();
    let scanned = 0;
    for (const doc of collection.documents) {
      if (scanned >= scanCap || values.length >= limit) break;
      scanned += 1;
      const inDoc = [];
      flatten(getByPath(doc, path), inDoc);
      for (const value of inDoc) {
        const key = distinctKey(value);
        if (seen.has(key)) continue;
        seen.add(key);
        values.push(clone(value));
        if (values.length >= limit) break;
      }
    }
    const source = String(options?.source || '').trim();
    if (source) {
      this._audit('query', { db, col, method: 'distinct', field: path, source, count: values.length });
    }
    return this._withElapsed(start, { values });
  }
}

export default MockApiClient;

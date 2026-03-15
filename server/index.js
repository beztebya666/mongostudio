import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';
import compression from 'compression';
import helmet from 'helmet';
import cors from 'cors';
import { createServer } from 'http';
import os from 'os';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { existsSync, readFileSync, writeFileSync } from 'fs';
import { randomUUID } from 'crypto';
import { spawn, spawnSync } from 'child_process';

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
const adminVerifiedConnections = new Set();
const ADMIN_ACCESS_KEY = (process.env.ADMIN_ACCESS_KEY || '').trim() || null;
const DEFAULT_EXEC_CONFIG = {
  mode: 'safe', maxTimeMS: 5000, maxResultSize: 50,
  allowDiskUse: false, blockedOperators: ['$where'],
};
const auditLog = [];
const MAX_AUDIT = 500;
const DB_INIT_COLLECTION = '__mongostudio_init__';
const isHiddenCollectionName = (name = '') => name === DB_INIT_COLLECTION;
const SERVICE_CONFIG_PATH = join(__dirname, 'service-config.json');
const QUERY_TIMEOUT_MIN_MS = 5000;
const SAFE_QUERY_TIMEOUT_MAX_MS = 30000;
const POWER_QUERY_TIMEOUT_PRESET_MAX_MS = 120000;
const POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS = 2147483000;
const POWER_QUERY_TIMEOUT_MAX_MS = POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS;
const QUERY_TIMEOUT_MAX_MS = POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS;
const QUERY_TIMEOUT_DEFAULT_MS = 5000;
const QUERY_LIMIT_DEFAULT = 50;
const QUERY_LIMIT_MIN = 50;
const SAFE_QUERY_LIMIT_MAX = 1000;
const POWER_QUERY_LIMIT_PRESET_MAX = 5000;
const QUERY_LIMIT_OVERRIDE_MAX = 50000;
const POWER_QUERY_LIMIT_MAX = 2147483000;
const QUERY_LIMIT_MAX = POWER_QUERY_LIMIT_MAX;
const EXPORT_LIMIT_MAX = POWER_QUERY_LIMIT_MAX;
const SCHEMA_MAX_FIELDS = Math.max(
  500,
  Math.min(Number(process.env.SCHEMA_MAX_FIELDS) || 20000, 250000),
);
const EXPORT_SCHEMA_MAX_FIELDS = Math.max(
  500,
  Math.min(Number(process.env.EXPORT_SCHEMA_MAX_FIELDS) || 8000, 100000),
);
const SCHEMA_MAX_DEPTH = Math.max(
  4,
  Math.min(Number(process.env.SCHEMA_MAX_DEPTH) || 24, 64),
);
const HEAVY_TIMEOUT_DEFAULT_MS = 30000;
const HEAVY_TIMEOUT_MIN_MS = 5000;
const HEAVY_TIMEOUT_MAX_MS = POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS;
const EXPORT_CURSOR_BATCH_SIZE = Math.max(
  1,
  Math.min(Number(process.env.EXPORT_CURSOR_BATCH_SIZE) || 100, 1000),
);
const GOVERNOR_QUEUE_WAIT_MS = 8000;
const GOVERNOR_QUEUE_BASE_POLL_MS = 25;
const GOVERNOR_QUEUE_MAX_POLL_MS = 200;
const STATUS_SHELL_RUNTIME_TTL_MS = Math.max(
  1000,
  Math.min(Number(process.env.STATUS_SHELL_RUNTIME_TTL_MS) || 15000, 300000),
);
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

function normalizeServiceConfig(input = {}) {
  const source = input && typeof input === 'object' ? input : {};
  const rate = source.rateLimit && typeof source.rateLimit === 'object' ? source.rateLimit : {};
  const governor = source.governor && typeof source.governor === 'object' ? source.governor : {};
  const metadataCache = source.metadataCache && typeof source.metadataCache === 'object' ? source.metadataCache : {};
  const windowMs = Math.max(1000, Math.min(Number(rate.windowMs) || DEFAULT_SERVICE_CONFIG.rateLimit.windowMs, 15 * 60 * 1000));
  const apiMax = Math.max(10, Math.min(Number(rate.apiMax) || DEFAULT_SERVICE_CONFIG.rateLimit.apiMax, 100000));
  const heavyMax = Math.max(1, Math.min(Number(rate.heavyMax) || DEFAULT_SERVICE_CONFIG.rateLimit.heavyMax, 10000));
  const interactivePerConnection = Math.max(1, Math.min(Number(governor.interactivePerConnection) || DEFAULT_SERVICE_CONFIG.governor.interactivePerConnection, 16));
  const metadataPerConnection = Math.max(1, Math.min(Number(governor.metadataPerConnection) || DEFAULT_SERVICE_CONFIG.governor.metadataPerConnection, 16));
  const heavyPerConnection = Math.max(1, Math.min(Number(governor.heavyPerConnection) || DEFAULT_SERVICE_CONFIG.governor.heavyPerConnection, 8));
  const heavyGlobal = Math.max(1, Math.min(Number(governor.heavyGlobal) || DEFAULT_SERVICE_CONFIG.governor.heavyGlobal, 200));
  const maxEntriesPerConnection = Math.max(200, Math.min(Number(metadataCache.maxEntriesPerConnection) || DEFAULT_SERVICE_CONFIG.metadataCache.maxEntriesPerConnection, 10000));
  const ttlDbStatsMs = Math.max(5000, Math.min(Number(metadataCache.ttlDbStatsMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlDbStatsMs, 10 * 60 * 1000));
  const ttlCollectionStatsMs = Math.max(5000, Math.min(Number(metadataCache.ttlCollectionStatsMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlCollectionStatsMs, 10 * 60 * 1000));
  const ttlIndexListMs = Math.max(5000, Math.min(Number(metadataCache.ttlIndexListMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlIndexListMs, 10 * 60 * 1000));
  const ttlSchemaQuickMs = Math.max(5000, Math.min(Number(metadataCache.ttlSchemaQuickMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlSchemaQuickMs, 10 * 60 * 1000));
  const ttlApproxTotalMs = Math.max(5000, Math.min(Number(metadataCache.ttlApproxTotalMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlApproxTotalMs, 10 * 60 * 1000));
  const ttlExactTotalMs = Math.max(5000, Math.min(Number(metadataCache.ttlExactTotalMs) || DEFAULT_SERVICE_CONFIG.metadataCache.ttlExactTotalMs, 10 * 60 * 1000));
  return {
    rateLimit: { windowMs, apiMax, heavyMax },
    governor: { interactivePerConnection, metadataPerConnection, heavyPerConnection, heavyGlobal },
    metadataCache: { maxEntriesPerConnection, ttlDbStatsMs, ttlCollectionStatsMs, ttlIndexListMs, ttlSchemaQuickMs, ttlApproxTotalMs, ttlExactTotalMs },
  };
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
const opCountersByConn = new Map();
let activeHeavyGlobal = 0;
const jobs = new Map();
const jobsByConn = new Map();
const activeCursorsByConn = new Map();
const JOB_TTL_MS = 30 * 60 * 1000;
const opCommentsByConn = new Map();
const OP_COMMENT_PREFIX = 'mongostudio-op';
const MAX_TRACKED_OP_COMMENTS = 4000;
const MONGOSH_BIN = process.env.MONGOSH_BIN || 'mongosh';
const MONGO_BIN = process.env.MONGO_BIN || 'mongo';
const MONGOSH_MAX_SESSIONS_PER_CONN = 6;
const MONGOSH_MAX_BUFFERED_EVENTS = 800;
const MONGOSH_MAX_COMMAND_LENGTH = 120000;
const MONGOSH_SESSION_HEARTBEAT_MS = 15000;
const MONGOSH_TERMINATE_GRACE_MS = 1200;
const MONGOSH_FINALIZED_TTL_MS = 45000;
const MONGOSH_BIN_CHECK_TTL_MS = 5000;
const SERVER_TOOL_RUNTIME_TTL_MS = Math.max(
  1000,
  Math.min(Number(process.env.SERVER_TOOL_RUNTIME_TTL_MS) || 15000, 300000),
);
const SERVER_TOOL_EXEC_TIMEOUT_DEFAULT_MS = Math.max(
  1000,
  Math.min(Number(process.env.SERVER_TOOL_EXEC_TIMEOUT_DEFAULT_MS) || 60000, 600000),
);
const SERVER_TOOL_EXEC_TIMEOUT_MAX_MS = Math.max(
  SERVER_TOOL_EXEC_TIMEOUT_DEFAULT_MS,
  Math.min(Number(process.env.SERVER_TOOL_EXEC_TIMEOUT_MAX_MS) || 30 * 60 * 1000, 4 * 60 * 60 * 1000),
);
const SERVER_TOOL_OUTPUT_LIMIT = Math.max(
  4096,
  Math.min(Number(process.env.SERVER_TOOL_OUTPUT_LIMIT) || 40000, 200000),
);
const SERVER_TOOL_DEFAULT_WORKDIR = String(process.env.SERVER_TOOL_DEFAULT_WORKDIR || '/tmp/mongostudio').trim() || '/tmp/mongostudio';
const SERVER_TOOL_HOST_WORKDIR = String(process.env.SERVER_TOOL_HOST_WORKDIR || '').trim();
const SERVER_TOOL_WIRE_COMPAT_RE = /wire version\s+\d+[\s\S]*requires at least\s+\d+/i;
const SERVER_TOOL_REQUIRES_42_RE = /requires(?:\s+at\s+least)?\s+mongodb\s*4\.2/i;
const SERVER_TOOL_MISSING_BIN_RE = /\benoent\b|spawn\s+\S+\s+enoent|executable file not found|binary not found/i;
const SERVER_TOOL_CONFIG = {
  serverInfo: { id: 'serverInfo', kind: 'read', source: 'mongo', displayName: 'Server Info', opClass: 'metadata' },
  mongostat: { id: 'mongostat', kind: 'read', source: 'binary', bin: process.env.MONGOSTAT_BIN || 'mongostat', legacyBin: process.env.MONGOSTAT_LEGACY_BIN || 'mongostat_legacy', displayName: 'mongostat', opClass: 'metadata' },
  mongotop: { id: 'mongotop', kind: 'read', source: 'binary', bin: process.env.MONGOTOP_BIN || 'mongotop', legacyBin: process.env.MONGOTOP_LEGACY_BIN || 'mongotop_legacy', displayName: 'mongotop', opClass: 'metadata' },
  slowOps: { id: 'slowOps', kind: 'read', source: 'mongo', displayName: 'Slow Ops', opClass: 'metadata' },
  mongodump: { id: 'mongodump', kind: 'run', source: 'binary', bin: process.env.MONGODUMP_BIN || 'mongodump', legacyBin: process.env.MONGODUMP_LEGACY_BIN || 'mongodump_legacy', displayName: 'mongodump', opClass: 'heavy' },
  mongorestore: { id: 'mongorestore', kind: 'run', source: 'binary', bin: process.env.MONGORESTORE_BIN || 'mongorestore', legacyBin: process.env.MONGORESTORE_LEGACY_BIN || 'mongorestore_legacy', displayName: 'mongorestore', opClass: 'heavy' },
  mongoexport: { id: 'mongoexport', kind: 'run', source: 'binary', bin: process.env.MONGOEXPORT_BIN || 'mongoexport', legacyBin: process.env.MONGOEXPORT_LEGACY_BIN || 'mongoexport_legacy', displayName: 'mongoexport', opClass: 'heavy' },
  mongoimport: { id: 'mongoimport', kind: 'run', source: 'binary', bin: process.env.MONGOIMPORT_BIN || 'mongoimport', legacyBin: process.env.MONGOIMPORT_LEGACY_BIN || 'mongoimport_legacy', displayName: 'mongoimport', opClass: 'heavy' },
  mongofiles: { id: 'mongofiles', kind: 'run', source: 'binary', bin: process.env.MONGOFILES_BIN || 'mongofiles', legacyBin: process.env.MONGOFILES_LEGACY_BIN || 'mongofiles_legacy', displayName: 'mongofiles', opClass: 'heavy' },
};
const CONSOLE_SHELL_CONFIG = {
  mongosh: {
    mode: 'mongosh',
    bin: MONGOSH_BIN,
    displayName: 'mongosh 2.x',
  },
  mongo: {
    mode: 'mongo',
    bin: MONGO_BIN,
    displayName: 'mongosh 1.x',
  },
};
const mongoshSessions = new Map();
const mongoshSessionsByConn = new Map();
const consoleShellBinCheckCache = new Map();
let consoleShellRuntimeStatusCache = {
  checkedAt: 0,
  value: null,
};
let serverToolRuntimeStatusCache = {
  checkedAt: 0,
  value: null,
};

function clampNumber(value, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return min;
  return Math.max(min, Math.min(Math.round(n), max));
}

function waitMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(1, Number(ms) || 1)));
}

function getRemainingMs(deadlineMs) {
  return Math.max(1, Number(deadlineMs) - Date.now());
}

function rememberOperationComment(connId, comment) {
  if (!connId || !comment) return;
  let store = opCommentsByConn.get(connId);
  if (!store) {
    store = new Map();
    opCommentsByConn.set(connId, store);
  }
  store.set(comment, Date.now());
  if (store.size <= MAX_TRACKED_OP_COMMENTS) return;
  const overflow = store.size - MAX_TRACKED_OP_COMMENTS;
  let removed = 0;
  for (const key of store.keys()) {
    store.delete(key);
    removed += 1;
    if (removed >= overflow) break;
  }
}

function createOperationComment(connId, kind = 'op') {
  if (!connId) return null;
  const rawKind = String(kind || 'op').toLowerCase();
  const safeKind = rawKind.replace(/[^a-z0-9:_-]/g, '').slice(0, 48) || 'op';
  const comment = `${OP_COMMENT_PREFIX}:${connId}:${safeKind}:${randomUUID().replace(/-/g, '')}`;
  rememberOperationComment(connId, comment);
  return comment;
}

function getTrackedOperationComments(connId) {
  const store = opCommentsByConn.get(connId);
  if (!store) return [];
  return [...store.keys()];
}

function clearTrackedOperationComments(connId) {
  opCommentsByConn.delete(connId);
}

async function killTrackedOperationsForConnection(connId, client) {
  const comments = getTrackedOperationComments(connId);
  if (!client || comments.length === 0) return { matched: 0, killed: 0, failed: 0 };
  const admin = client.db('admin');
  let inprog = [];
  try {
    const current = await admin.command({ currentOp: 1, $all: true, localOps: true });
    inprog = Array.isArray(current?.inprog) ? current.inprog : [];
  } catch (err) {
    return { matched: 0, killed: 0, failed: 0, error: String(err?.message || err) };
  }
  const commentSet = new Set(comments);
  const opIds = [];
  for (const entry of inprog) {
    const opComment = entry?.command?.comment ?? entry?.comment;
    if (!commentSet.has(opComment)) continue;
    const opId = entry?.opid ?? entry?.opId ?? entry?.operationId;
    if (opId === undefined || opId === null) continue;
    opIds.push(opId);
  }
  let killed = 0;
  let failed = 0;
  for (const opId of opIds) {
    try {
      await admin.command({ killOp: 1, op: opId });
      killed += 1;
    } catch {
      failed += 1;
    }
  }
  return { matched: opIds.length, killed, failed };
}

function buildCommandMeta(options = {}) {
  const meta = {};
  const comment = typeof options?.comment === 'string' ? options.comment.trim() : '';
  if (comment) meta.comment = comment;
  const maxTimeMS = Number(options?.maxTimeMS);
  if (Number.isFinite(maxTimeMS) && maxTimeMS > 0) meta.maxTimeMS = Math.round(maxTimeMS);
  return meta;
}

function isUnsupportedCommentOptionError(err) {
  const message = String(err?.message || '');
  if (!message) return false;
  if (!/comment/i.test(message)) return false;
  return (
    /unknown field/i.test(message)
    || /unrecognized field/i.test(message)
    || /unknown option/i.test(message)
    || /invalid.*comment/i.test(message)
    || /BSON field .*comment/i.test(message)
  );
}

async function callMongoWithCommentFallback(runWithOptions, options = undefined) {
  try {
    return await runWithOptions(options);
  } catch (err) {
    const hasComment = Boolean(options && typeof options === 'object' && Object.prototype.hasOwnProperty.call(options, 'comment'));
    if (!hasComment || !isUnsupportedCommentOptionError(err)) throw err;
    const fallbackOptions = { ...options };
    delete fallbackOptions.comment;
    return runWithOptions(fallbackOptions);
  }
}

function trackActiveCursor(connId, cursor) {
  if (!connId || !cursor || typeof cursor.close !== 'function') return () => {};
  let store = activeCursorsByConn.get(connId);
  if (!store) {
    store = new Set();
    activeCursorsByConn.set(connId, store);
  }
  store.add(cursor);
  return () => {
    const active = activeCursorsByConn.get(connId);
    if (!active) return;
    active.delete(cursor);
    if (active.size === 0) activeCursorsByConn.delete(connId);
  };
}

async function closeActiveCursorsForConnection(connId) {
  const store = activeCursorsByConn.get(connId);
  if (!store || store.size === 0) return 0;
  const cursors = [...store];
  activeCursorsByConn.delete(connId);
  await Promise.allSettled(cursors.map(async (cursor) => {
    try { await cursor.close(); } catch {}
  }));
  return cursors.length;
}

function getModeQueryLimitMax(mode = 'safe') {
  return mode === 'power' ? POWER_QUERY_LIMIT_MAX : QUERY_LIMIT_OVERRIDE_MAX;
}

function getModeQueryTimeoutMax(mode = 'safe') {
  return mode === 'power' ? POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS : SAFE_QUERY_TIMEOUT_MAX_MS;
}

function getGovernorLimits() {
  const cfg = serviceConfig.governor || DEFAULT_SERVICE_CONFIG.governor;
  return {
    interactivePerConnection: Math.max(1, Number(cfg.interactivePerConnection) || DEFAULT_SERVICE_CONFIG.governor.interactivePerConnection),
    metadataPerConnection: Math.max(1, Number(cfg.metadataPerConnection) || DEFAULT_SERVICE_CONFIG.governor.metadataPerConnection),
    heavyPerConnection: Math.max(1, Number(cfg.heavyPerConnection) || DEFAULT_SERVICE_CONFIG.governor.heavyPerConnection),
    heavyGlobal: Math.max(1, Number(cfg.heavyGlobal) || DEFAULT_SERVICE_CONFIG.governor.heavyGlobal),
  };
}

function getConnCounters(connId) {
  if (!opCountersByConn.has(connId)) {
    opCountersByConn.set(connId, { interactive: 0, metadata: 0, heavy: 0 });
  }
  return opCountersByConn.get(connId);
}

async function runWithGovernor(req, opClass, fn) {
  const cls = opClass === 'metadata' || opClass === 'heavy' ? opClass : 'interactive';
  const limits = getGovernorLimits();
  const counters = getConnCounters(req.connId || 'anon');
  const maxByClass = cls === 'interactive'
    ? limits.interactivePerConnection
    : cls === 'metadata'
      ? limits.metadataPerConnection
      : limits.heavyPerConnection;
  const budgetForQueue = cls === 'heavy'
    ? Number(req?.heavyBudget)
    : Number(req?.queryBudget?.timeoutMs);
  const queueWaitMs = Math.max(
    GOVERNOR_QUEUE_BASE_POLL_MS,
    Math.min(
      GOVERNOR_QUEUE_WAIT_MS,
      Number.isFinite(budgetForQueue) && budgetForQueue > 0 ? budgetForQueue : GOVERNOR_QUEUE_WAIT_MS,
    ),
  );
  const deadline = Date.now() + queueWaitMs;
  const canRunNow = () => counters[cls] < maxByClass && (cls !== 'heavy' || activeHeavyGlobal < limits.heavyGlobal);
  let waitTick = 0;
  while (!canRunNow()) {
    if (Date.now() >= deadline) {
      const err = new Error(`Too many ${cls} operations in progress.`);
      err.statusCode = 429;
      err.errorType = 'limit';
      throw err;
    }
    const backoff = Math.min(
      GOVERNOR_QUEUE_BASE_POLL_MS * (2 ** Math.min(waitTick, 3)),
      GOVERNOR_QUEUE_MAX_POLL_MS,
    );
    await waitMs(Math.min(backoff, getRemainingMs(deadline)));
    waitTick += 1;
  }
  counters[cls] += 1;
  if (cls === 'heavy') activeHeavyGlobal += 1;
  try {
    return await fn();
  } finally {
    counters[cls] = Math.max(0, (counters[cls] || 1) - 1);
    if (cls === 'heavy') activeHeavyGlobal = Math.max(0, activeHeavyGlobal - 1);
  }
}

class SessionMetadataCache {
  constructor() {
    this.byConn = new Map();
  }

  _maxEntries() {
    const cfg = serviceConfig.metadataCache || DEFAULT_SERVICE_CONFIG.metadataCache;
    return Math.max(200, Number(cfg.maxEntriesPerConnection) || DEFAULT_SERVICE_CONFIG.metadataCache.maxEntriesPerConnection);
  }

  _ensure(connId) {
    if (!this.byConn.has(connId)) this.byConn.set(connId, new Map());
    return this.byConn.get(connId);
  }

  _enforceLimit(store) {
    const max = this._maxEntries();
    if (store.size <= max) return;
    const sorted = [...store.entries()].sort((a, b) => Number(a[1]?.lastAccess || 0) - Number(b[1]?.lastAccess || 0));
    const removeCount = Math.max(0, store.size - max);
    for (let i = 0; i < removeCount; i += 1) {
      const key = sorted[i]?.[0];
      if (key) store.delete(key);
    }
  }

  get(connId, key) {
    const store = this.byConn.get(connId);
    if (!store) return null;
    const item = store.get(key);
    if (!item) return null;
    if (Date.now() >= item.expiresAt) {
      store.delete(key);
      return null;
    }
    item.lastAccess = Date.now();
    return {
      value: item.value,
      source: item.source || 'cache',
      ts: item.ts || Date.now(),
      expiresAt: item.expiresAt,
    };
  }

  set(connId, key, value, ttlMs, source = 'live') {
    const store = this._ensure(connId);
    const now = Date.now();
    store.set(key, {
      value,
      source,
      ts: now,
      expiresAt: now + Math.max(1, Number(ttlMs) || 1),
      lastAccess: now,
    });
    this._enforceLimit(store);
  }

  invalidateByPrefix(connId, prefix) {
    const store = this.byConn.get(connId);
    if (!store) return;
    for (const key of store.keys()) {
      if (String(key).startsWith(prefix)) store.delete(key);
    }
  }

  clearConn(connId) {
    this.byConn.delete(connId);
  }

  pruneExpired() {
    const now = Date.now();
    for (const [connId, store] of this.byConn) {
      for (const [key, item] of store) {
        if (!item || now >= Number(item.expiresAt || 0)) store.delete(key);
      }
      if (store.size === 0) this.byConn.delete(connId);
    }
  }
}

const sessionMetadataCache = new SessionMetadataCache();

function createJob(connId, type, payload = {}) {
  const jobId = `job_${randomUUID().replace(/-/g,'')}`;
  const now = Date.now();
  const job = {
    jobId,
    connId,
    type,
    state: 'queued',
    progressPct: 0,
    result: null,
    error: null,
    createdAt: now,
    updatedAt: now,
    payload,
  };
  jobs.set(jobId, job);
  if (!jobsByConn.has(connId)) jobsByConn.set(connId, new Set());
  jobsByConn.get(connId).add(jobId);
  return job;
}

function patchJob(jobId, patch = {}) {
  const job = jobs.get(jobId);
  if (!job) return null;
  const next = { ...job, ...patch, updatedAt: Date.now() };
  jobs.set(jobId, next);
  return next;
}

function getJobForConnection(connId, jobId) {
  const job = jobs.get(jobId);
  if (!job || job.connId !== connId) return null;
  return job;
}

function pruneJobs() {
  const now = Date.now();
  for (const [jobId, job] of jobs) {
    if (!job || now - Number(job.updatedAt || job.createdAt || now) > JOB_TTL_MS) {
      jobs.delete(jobId);
      const connSet = jobsByConn.get(job?.connId);
      if (connSet) {
        connSet.delete(jobId);
        if (connSet.size === 0) jobsByConn.delete(job?.connId);
      }
    }
  }
}

function normalizeQueryBudget(rawTimeoutMs, rawLimit, mode = 'safe') {
  const timeoutMs = clampNumber(rawTimeoutMs, QUERY_TIMEOUT_MIN_MS, getModeQueryTimeoutMax(mode));
  const limit = clampNumber(rawLimit, QUERY_LIMIT_MIN, getModeQueryLimitMax(mode));
  return { timeoutMs, limit };
}

function requireHeavyConfirm(req, res) {
  if (req.heavyConfirmed) return true;
  res.status(428).json({
    error: 'Heavy operation requires explicit confirmation. Run preflight first and resend with X-Heavy-Confirm: 1.',
    errorType: 'precondition',
  });
  return false;
}

function createRequestError(message, statusCode = 400, errorType = 'validation') {
  const err = new Error(String(message || 'Invalid request.'));
  err.statusCode = statusCode;
  err.errorType = errorType;
  return err;
}

function hashQueryParts(parts = []) {
  return parts
    .map((part) => {
      const text = typeof part === 'string' ? part : JSON.stringify(part);
      return Buffer.from(String(text || ''), 'utf8').toString('base64');
    })
    .join(':');
}

function sortObjectKeys(value) {
  if (Array.isArray(value)) return value.map(sortObjectKeys);
  if (value && typeof value === 'object' && !(value instanceof Date)) {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = sortObjectKeys(value[key]);
    }
    return out;
  }
  return value;
}

function canonicalQueryShape(raw = '{}') {
  const parsed = typeof raw === 'string'
    ? parseFilter(raw)
    : parseFilterInput(raw || {});
  return JSON.stringify(sortObjectKeys(parsed || {}));
}

function parseHeaderNumber(req, name) {
  const raw = req?.headers?.[name];
  if (Array.isArray(raw)) return Number(raw[0]);
  return Number(raw);
}

function shouldUseCompression(req, res) {
  const path = String(req.path || req.originalUrl || '');
  if (/^\/api\/databases\/[^/]+\/export$/i.test(path)) return false;
  if (/^\/api\/databases\/[^/]+\/collections\/[^/]+\/export$/i.test(path)) return false;
  return compression.filter(req, res);
}

app.use(compression({ filter: shouldUseCompression }));
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
      terminateMongoshSessionsForConnection(id, 'idle_timeout').catch(() => {});
      closeActiveCursorsForConnection(id).catch(() => {});
      conn.client.close().catch(() => {});
      connections.delete(id); executionConfigs.delete(id);
      opCountersByConn.delete(id);
      clearTrackedOperationComments(id);
      sessionMetadataCache.clearConn(id);
      const connJobs = jobsByConn.get(id);
      if (connJobs) {
        for (const jobId of connJobs) jobs.delete(jobId);
        jobsByConn.delete(id);
      }
    }
  }
  for (const bucket of [rateBuckets.api, rateBuckets.heavy]) {
    for (const [key, state] of bucket) {
      if (!state || now >= state.resetAt) bucket.delete(key);
    }
  }
  sessionMetadataCache.pruneExpired();
  pruneJobs();
}, 60000);

function parseVersion(v) { const p = (v||'0.0.0').split('.').map(Number); return { major:p[0]||0, minor:p[1]||0, patch:p[2]||0 }; }
function versionAtLeast(v, maj, min=0) { return v.major > maj || (v.major === maj && v.minor >= min); }

function getCapabilities(v) {
  return {
    hasAggLookup: versionAtLeast(v,3,2),
    hasAggFacet: versionAtLeast(v,3,4),
    hasCountDocuments: versionAtLeast(v,3,6), hasChangeStreams: versionAtLeast(v,3,6),
    hasEstimatedCount: versionAtLeast(v,4,0), hasTransactions: versionAtLeast(v,4,0),
    hasMergeStage: versionAtLeast(v,4,2), hasWildcardIndexes: versionAtLeast(v,4,2),
    hasShardedTransactions: versionAtLeast(v,4,2),
    hasUnionWith: versionAtLeast(v,4,4),
    hasSetWindowFields: versionAtLeast(v,5,0), hasStableApi: versionAtLeast(v,5,0),
    hasTimeSeries: versionAtLeast(v,5,0),
    hasDensifyFill: versionAtLeast(v,5,1),
    hasClustered: versionAtLeast(v,5,3),
    hasColumnstoreIndexes: versionAtLeast(v,6,0),
    hasCompoundWildcard: versionAtLeast(v,7,0), hasQueryableEncryption: versionAtLeast(v,7,0),
  };
}

async function compatCount(col, filter, caps, options = {}) {
  const meta = buildCommandMeta(options);
  if (caps.hasCountDocuments) {
    try { return await col.countDocuments(filter, meta); } catch {}
  }
  if (Object.keys(meta).length > 0) {
    try { return await col.count(filter, meta); } catch {}
  }
  try { return await col.count(filter); } catch { return 0; }
}

async function compatEstCount(col, caps, options = {}) {
  const meta = buildCommandMeta(options);
  if (caps.hasEstimatedCount) {
    try { return await col.estimatedDocumentCount(meta); } catch {}
  }
  if (Object.keys(meta).length > 0) {
    try { return await col.count({}, meta); } catch {}
  }
  try { return await col.count({}); } catch { return 0; }
}

async function compatCollStats(db, name, caps, options = {}) {
  const meta = buildCommandMeta(options);
  try {
    const s = await db.command({ collStats: name, ...meta });
    return { count:s.count??0, size:s.size??0, avgObjSize:s.avgObjSize??0, storageSize:s.storageSize??0, totalIndexSize:s.totalIndexSize??0, indexSizes:s.indexSizes??{}, nindexes:s.nindexes??0 };
  } catch (err) {
    try { return { count: await compatEstCount(db.collection(name), caps, options), size:0, avgObjSize:0, storageSize:0, totalIndexSize:0, nindexes:0 }; }
    catch { throw err; }
  }
}

async function compatDbStats(db, caps, options = {}) {
  const meta = buildCommandMeta(options);
  try {
    const s = await db.command({ dbStats: 1, scale: 1, ...meta });
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
          const stats = await compatCollStats(db, col.name, caps, options);
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
  if (err?.statusCode) {
    return {
      type: err?.errorType || 'unknown',
      friendly: String(err?.message || 'Unexpected server error.'),
      code: Number(err.statusCode) || 500,
    };
  }
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
  req.adminKeyRequired = Boolean(ADMIN_ACCESS_KEY);
  req.adminVerified = !ADMIN_ACCESS_KEY || adminVerifiedConnections.has(id);
  req.execConfig = executionConfigs.get(id) || { ...DEFAULT_EXEC_CONFIG };
  const execMode = req.execConfig?.mode === 'power' ? 'power' : 'safe';
  const modeLimitMax = getModeQueryLimitMax(execMode);
  const modeTimeoutMax = getModeQueryTimeoutMax(execMode);
  req.execConfig.maxResultSize = clampNumber(
    Number(req.execConfig?.maxResultSize) || QUERY_LIMIT_DEFAULT,
    QUERY_LIMIT_MIN,
    modeLimitMax,
  );
  req.execConfig.maxTimeMS = clampNumber(
    Number(req.execConfig?.maxTimeMS) || QUERY_TIMEOUT_DEFAULT_MS,
    QUERY_TIMEOUT_MIN_MS,
    modeTimeoutMax,
  );
  const baseTimeout = clampNumber(
    Number(req.execConfig?.maxTimeMS) || QUERY_TIMEOUT_DEFAULT_MS,
    QUERY_TIMEOUT_MIN_MS,
    modeTimeoutMax,
  );
  const baseLimit = clampNumber(
    Number(req.execConfig?.maxResultSize) || QUERY_LIMIT_DEFAULT,
    QUERY_LIMIT_MIN,
    QUERY_LIMIT_MAX,
  );
  const timeoutOverride = parseHeaderNumber(req, 'x-query-timeout-ms');
  const limitOverride = parseHeaderNumber(req, 'x-query-limit');
  const hasTimeoutOverride = Number.isFinite(timeoutOverride) && timeoutOverride >= QUERY_TIMEOUT_MIN_MS;
  const hasLimitOverride = Number.isFinite(limitOverride) && limitOverride >= QUERY_LIMIT_MIN;
  const normalized = normalizeQueryBudget(
    hasTimeoutOverride ? timeoutOverride : baseTimeout,
    hasLimitOverride ? limitOverride : Math.min(baseLimit, modeLimitMax),
    execMode,
  );
  req.queryBudget = {
    timeoutMs: normalized.timeoutMs,
    limit: normalized.limit,
    overrideApplied: hasTimeoutOverride || hasLimitOverride,
  };
  const heavyTimeoutRaw = parseHeaderNumber(req, 'x-heavy-timeout-ms');
  req.heavyBudget = clampNumber(
    Number.isFinite(heavyTimeoutRaw) && heavyTimeoutRaw >= HEAVY_TIMEOUT_MIN_MS ? heavyTimeoutRaw : HEAVY_TIMEOUT_DEFAULT_MS,
    HEAVY_TIMEOUT_MIN_MS,
    HEAVY_TIMEOUT_MAX_MS,
  );
  req.heavyConfirmed = String(req.headers['x-heavy-confirm'] || '').trim() === '1';
  req.createOpComment = (kind = 'op') => createOperationComment(id, kind);
  const routing = conn?.routing && typeof conn.routing === 'object' ? conn.routing : {};
  const readPreferenceHeader = typeof routing.readPreference === 'string' && routing.readPreference.trim()
    ? routing.readPreference.trim()
    : (conn?.client?.readPreference?.mode || 'primary');
  const directConnectionHeader = routing.directConnection === true ? '1' : '0';
  try {
    res.setHeader('X-Mongo-Connection-Id', String(id));
    res.setHeader('X-Mongo-Read-Preference', String(readPreferenceHeader));
    res.setHeader('X-Mongo-Direct-Connection', directConnectionHeader);
  } catch {}
  next();
}

function safeDecodeUriComponent(value = '') {
  try {
    return decodeURIComponent(String(value || ''));
  } catch {
    return String(value || '');
  }
}

function deriveConnectionDbContext(conn = {}) {
  const sourceUri = String(conn?.uri || '').trim();
  const optionAuthSource = String(conn?.connectOptions?.authSource || '').trim();
  const storedDefaultDb = String(conn?.defaultDb || '').trim();
  let uriAuthSource = '';
  let uriDefaultDb = '';
  if (sourceUri) {
    try {
      const authMatch = sourceUri.match(/[?&]authSource=([^&]+)/i);
      if (authMatch) uriAuthSource = safeDecodeUriComponent(authMatch[1] || '').trim();
      const dbMatch = sourceUri.match(/^mongodb(?:\+srv)?:\/\/(?:[^@]+@)?[^/?]+\/([^?]*)/i);
      if (dbMatch) uriDefaultDb = safeDecodeUriComponent(dbMatch[1] || '').trim();
    } catch {}
  }
  const effectiveAuthSource = optionAuthSource || uriAuthSource || '';
  const effectiveDefaultDb = storedDefaultDb || uriDefaultDb || effectiveAuthSource || '';
  return {
    authSource: effectiveAuthSource || null,
    defaultDb: effectiveDefaultDb || null,
  };
}

function auditReq(req, action, details = {}) {
  const headerUser = typeof req.headers['x-ui-user'] === 'string' ? req.headers['x-ui-user'].trim() : '';
  const resolvedUser = headerUser || req.auditUser || req.conn?.lastUiUser || req.conn?.username || req.conn?.authUser || 'anonymous';
  audit(req.connId, action, { ...details, user: resolvedUser });
}

function parseAuditSource(value) {
  const src = String(value || '').trim().toLowerCase();
  if (!src) return '';
  if (!/^[a-z0-9_-]{1,32}$/.test(src)) return '';
  return src;
}

function parseAuditScope(value) {
  const scope = String(value || '').trim().toLowerCase();
  if (!scope) return '';
  if (scope === 'global' || scope === 'database' || scope === 'collection') return scope;
  return '';
}

function normalizeServerToolId(value = '') {
  const id = String(value || '').trim();
  if (!id) return '';
  return Object.prototype.hasOwnProperty.call(SERVER_TOOL_CONFIG, id) ? id : '';
}

function parseHostToken(value = '') {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return { full: '', host: '', port: '' };
  const noProtocol = raw.replace(/^mongodb(?:\+srv)?:\/\//, '');
  const noAuth = noProtocol.includes('@') ? noProtocol.split('@').pop() : noProtocol;
  const base = String(noAuth || '').split('/')[0].trim();
  if (!base) return { full: '', host: '', port: '' };
  const ipv6 = base.match(/^\[(.+)\](?::(\d+))?$/);
  if (ipv6) {
    const host = ipv6[1] || '';
    const port = ipv6[2] || '';
    return { full: port ? `${host}:${port}` : host, host, port };
  }
  const lastColon = base.lastIndexOf(':');
  if (lastColon > -1 && base.indexOf(':') === lastColon) {
    const host = base.slice(0, lastColon).trim();
    const port = base.slice(lastColon + 1).trim();
    return { full: port ? `${host}:${port}` : host, host, port };
  }
  return { full: base, host: base, port: '' };
}

function sameHostToken(a = '', b = '') {
  const left = parseHostToken(a);
  const right = parseHostToken(b);
  if (!left.full || !right.full) return false;
  if (left.full === right.full) return true;
  if (left.host && right.host && left.host === right.host) {
    if (!left.port || !right.port) return true;
    return left.port === right.port;
  }
  return false;
}

function dedupeHostTokens(input = []) {
  const values = [];
  for (const value of input) {
    const text = String(value || '').trim();
    if (!text) continue;
    if (!values.some((item) => sameHostToken(item, text))) values.push(text);
  }
  return values;
}

function listConnectionUriHosts(conn = {}) {
  const connHost = String(conn?.host || '');
  const connValues = connHost.split(',').map((host) => String(host || '').trim()).filter(Boolean);
  return dedupeHostTokens(connValues);
}

function listTopologyHosts(conn = {}) {
  const topology = conn?.topology && typeof conn.topology === 'object' ? conn.topology : {};
  const topologyValues = [
    ...(Array.isArray(topology.hosts) ? topology.hosts : []),
    topology?.primary,
    topology?.me,
  ];
  return dedupeHostTokens(topologyValues);
}

function resolveTopologyAliasForHost(conn = {}, host = '') {
  const hostText = String(host || '').trim();
  if (!hostText) return '';
  const topologyHosts = listTopologyHosts(conn);
  if (topologyHosts.length === 0) return '';
  const direct = topologyHosts.find((entry) => sameHostToken(entry, hostText));
  if (direct) return direct;
  const hostPort = parseHostToken(hostText).port;
  if (hostPort) {
    const portMatched = topologyHosts.filter((entry) => parseHostToken(entry).port === hostPort);
    if (portMatched.length === 1) return portMatched[0];
  }
  const connectionHosts = listConnectionUriHosts(conn);
  if (connectionHosts.length > 0 && connectionHosts.length === topologyHosts.length) {
    const index = connectionHosts.findIndex((entry) => sameHostToken(entry, hostText));
    if (index >= 0) return topologyHosts[index] || '';
  }
  return '';
}

function nodeMatchesTopologyTarget(conn = {}, node = '', topologyTarget = '') {
  const nodeText = String(node || '').trim();
  const targetText = String(topologyTarget || '').trim();
  if (!nodeText || !targetText) return false;
  if (sameHostToken(nodeText, targetText)) return true;
  const alias = resolveTopologyAliasForHost(conn, nodeText);
  return Boolean(alias && sameHostToken(alias, targetText));
}

function listConnectionHosts(conn = {}) {
  const topologyHosts = listTopologyHosts(conn);
  const connectionHosts = listConnectionUriHosts(conn);
  if (connectionHosts.length > 0 && topologyHosts.length > 0) {
    if (connectionHosts.length === topologyHosts.length) return connectionHosts;
    return topologyHosts;
  }
  if (topologyHosts.length > 0) return topologyHosts;
  return connectionHosts;
}

function normalizeFsPath(value = '') {
  const text = String(value || '').trim();
  if (!text) return '';
  const normalized = text.replace(/\\/g, '/').replace(/\/{2,}/g, '/');
  if (/^[a-z]:\//i.test(normalized)) {
    return normalized[0].toUpperCase() + normalized.slice(1);
  }
  return normalized;
}

function isAbsoluteFsPath(value = '') {
  const text = normalizeFsPath(value);
  if (!text) return false;
  if (text.startsWith('/')) return true;
  return /^[A-Z]:\//i.test(text);
}

function isPathWithin(basePath = '', targetPath = '') {
  const base = normalizeFsPath(basePath);
  const target = normalizeFsPath(targetPath);
  if (!base || !target) return false;
  if (base === target) return true;
  if (/^[A-Z]:\//i.test(base)) {
    const baseLower = base.toLowerCase();
    const targetLower = target.toLowerCase();
    return targetLower.startsWith(`${baseLower}/`);
  }
  return target.startsWith(`${base}/`);
}

function parseBooleanLike(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return fallback;
}

function parsePositiveInt(value, fallback, min = 1, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return clampNumber(parsed, min, max);
}

function redactMongoUriCredentials(uri = '') {
  const text = String(uri || '');
  return text.replace(/^(mongodb(?:\+srv)?:\/\/)([^/@]+)@/i, (full, prefix, userInfo) => {
    const username = String(userInfo || '').split(':')[0];
    return `${prefix}${username ? `${username}:***` : '***'}@`;
  });
}

function quoteCommandArg(arg = '') {
  const value = String(arg ?? '');
  if (!value) return '""';
  if (!/[\s"'\\]/.test(value)) return value;
  return `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`;
}

function redactSensitiveCommandArg(value = '', previous = '') {
  const current = String(value ?? '');
  const prev = String(previous ?? '').trim().toLowerCase();
  if (prev === '--password' || prev === '-p' || prev === '--pass') return '***';
  const lower = current.trim().toLowerCase();
  if (lower.startsWith('--password=')) return `${current.split('=')[0]}=***`;
  if (lower.startsWith('--pass=')) return `${current.split('=')[0]}=***`;
  return current;
}

function buildCommandPreview(binary = '', args = []) {
  const rawArgs = Array.isArray(args) ? args.map((value) => String(value ?? '')) : [];
  const sanitizedArgs = [];
  let previous = '';
  for (const value of rawArgs) {
    const secretSafe = redactSensitiveCommandArg(value, previous);
    const safeValue = /^mongodb(?:\+srv)?:\/\//i.test(secretSafe)
      ? redactMongoUriCredentials(secretSafe)
      : secretSafe;
    sanitizedArgs.push(safeValue);
    previous = String(value ?? '');
  }
  const parts = [String(binary || '').trim(), ...sanitizedArgs]
    .filter((value) => value.length > 0)
    .map((value) => quoteCommandArg(value));
  return parts.join(' ');
}

function replaceMongoUriHosts(uri = '', nextHosts = '') {
  const source = String(uri || '').trim();
  const hosts = String(nextHosts || '').trim();
  if (!source || !hosts) return source;
  const match = source.match(/^(mongodb(?:\+srv)?:\/\/)([^/?#]*)(.*)$/i);
  if (!match) return source;
  const [, scheme, authorityRaw, suffix] = match;
  const authority = String(authorityRaw || '');
  const atIndex = authority.lastIndexOf('@');
  const authPrefix = atIndex === -1 ? '' : authority.slice(0, atIndex + 1);
  return `${scheme}${authPrefix}${hosts}${suffix || ''}`;
}

function inferNodeRole(conn = {}, node = '') {
  const topology = conn?.topology && typeof conn.topology === 'object' ? conn.topology : {};
  if (!node) return 'unknown';
  const alias = resolveTopologyAliasForHost(conn, node);
  const roleNode = alias || node;
  if (sameHostToken(roleNode, topology?.primary || '')) return 'primary';
  if (sameHostToken(roleNode, topology?.me || '')) return topology?.role || 'member';
  if (topology?.kind === 'replicaSet' && !alias) return 'member';
  return topology?.kind === 'replicaSet' ? 'secondary' : topology?.role || 'member';
}

function buildServerToolBasePath() {
  return normalizeFsPath(SERVER_TOOL_DEFAULT_WORKDIR) || '/tmp/mongostudio';
}

function resolveServerManagementContext(req, source = {}, { requirePath = false } = {}) {
  const conn = req?.conn || {};
  const availableNodes = listConnectionHosts(conn);
  const defaultNode = availableNodes.find((entry) => nodeMatchesTopologyTarget(conn, entry, conn?.topology?.me || ''))
    || availableNodes[0]
    || '';
  const requestedNode = String(source?.node || '').trim();
  const selectedNode = requestedNode || defaultNode;
  if (!selectedNode) {
    throw createRequestError('No target node is available for this connection.', 400);
  }
  if (!availableNodes.some((entry) => sameHostToken(entry, selectedNode))) {
    throw createRequestError(`Node "${selectedNode}" is not part of the current connection topology.`, 400);
  }
  const nodeOverride = !sameHostToken(selectedNode, defaultNode);
  const confirmNodeSelection = parseBooleanLike(source?.confirmNodeSelection, false);
  if (nodeOverride && !confirmNodeSelection) {
    throw createRequestError('Target node differs from connection default. Set confirmNodeSelection=true to continue.', 428, 'precondition');
  }

  const basePath = buildServerToolBasePath();
  const requestedPath = String(source?.path || '').trim();
  const selectedPath = normalizeFsPath(requestedPath || basePath);
  if (requirePath && !isAbsoluteFsPath(selectedPath)) {
    throw createRequestError('Execution path must be absolute.', 400);
  }
  const customPath = Boolean(selectedPath && !isPathWithin(basePath, selectedPath));
  const confirmPathSelection = parseBooleanLike(source?.confirmPathSelection, false);
  if (customPath && !confirmPathSelection) {
    throw createRequestError('Execution path is outside the default workspace path. Set confirmPathSelection=true to continue.', 428, 'precondition');
  }

  return {
    machine: {
      host: os.hostname(),
      cwd: process.cwd(),
      platform: process.platform,
      pid: process.pid,
    },
    basePath,
    selectedPath: selectedPath || basePath,
    defaultNode,
    selectedNode,
    nodeOverride,
    confirmNodeSelection,
    customPath,
    confirmPathSelection,
    availableNodes: availableNodes.map((host) => ({
      host,
      role: inferNodeRole(conn, host),
      isDefault: sameHostToken(host, defaultNode),
      isPrimary: nodeMatchesTopologyTarget(conn, host, conn?.topology?.primary || ''),
    })),
  };
}

function buildServerToolUri(conn = {}, node = '') {
  const sourceUri = String(conn?.uri || '').trim();
  if (!sourceUri) throw createRequestError('Connection URI is missing.', 500);
  const selectedNode = String(node || '').trim();
  if (!selectedNode) return sourceUri;
  const isSameNode = nodeMatchesTopologyTarget(conn, selectedNode, conn?.topology?.me || '')
    || sameHostToken(conn?.topology?.me || conn?.host || '', selectedNode);
  let targetUri = sourceUri;
  if (!isSameNode) {
    const isSrv = /^mongodb\+srv:\/\//i.test(sourceUri);
    if (isSrv && selectedNode.includes(':')) {
      throw createRequestError('Cannot target host:port with mongodb+srv URI. Reconnect using mongodb:// URI to pick a specific node.', 400, 'validation');
    }
    targetUri = replaceMongoUriHosts(targetUri, selectedNode);
    targetUri = withMongoUriOption(targetUri, 'directConnection', 'true');
  }
  return targetUri;
}

function getMongoUriOptionValue(uri = '', key = '') {
  const source = String(uri || '').trim();
  const optionKey = String(key || '').trim();
  if (!source || !optionKey) return '';
  try {
    const re = new RegExp(`[?&]${optionKey}=([^&]+)`, 'i');
    const match = source.match(re);
    if (!match) return '';
    return safeDecodeUriComponent(match[1] || '').trim();
  } catch {
    return '';
  }
}

function readMongoUriUserInfo(uri = '') {
  const source = String(uri || '').trim();
  if (!source) return { username: '', password: '' };
  try {
    const match = source.match(/^mongodb(?:\+srv)?:\/\/([^@/]+)@/i);
    if (!match) return { username: '', password: '' };
    const raw = String(match[1] || '');
    const sep = raw.indexOf(':');
    if (sep === -1) {
      return {
        username: safeDecodeUriComponent(raw).trim(),
        password: '',
      };
    }
    return {
      username: safeDecodeUriComponent(raw.slice(0, sep)).trim(),
      password: safeDecodeUriComponent(raw.slice(sep + 1)).trim(),
    };
  } catch {
    return { username: '', password: '' };
  }
}

function buildLegacyServerToolHost(conn = {}, node = '') {
  const selectedNode = String(node || '').trim();
  if (selectedNode) return selectedNode;
  const hosts = listConnectionHosts(conn);
  if (hosts.length > 0) return hosts.join(',');
  const hostRaw = String(conn?.host || '').trim();
  if (hostRaw) return hostRaw;
  const sourceUri = String(conn?.uri || '').trim();
  if (!sourceUri) return '';
  const match = sourceUri.match(/^mongodb(?:\+srv)?:\/\/(?:[^@]+@)?([^/?]+)/i);
  return String(match?.[1] || '').trim();
}

function buildServerToolConnectionArgs(conn = {}, node = '', { mode = 'new' } = {}) {
  const normalizedMode = String(mode || '').trim().toLowerCase() === 'legacy'
    ? 'legacy'
    : 'new';
  if (normalizedMode !== 'legacy') {
    return ['--uri', buildServerToolUri(conn, node)];
  }
  const host = buildLegacyServerToolHost(conn, node);
  if (!host) {
    throw createRequestError('Unable to resolve target node for legacy server tool binary.', 500);
  }
  const sourceUri = String(conn?.uri || '').trim();
  const connectOptions = conn?.connectOptions && typeof conn.connectOptions === 'object'
    ? conn.connectOptions
    : {};
  const uriAuth = readMongoUriUserInfo(sourceUri);
  const username = String(connectOptions.username || uriAuth.username || '').trim();
  const password = String(connectOptions.password || uriAuth.password || '').trim();
  const authSource = String(
    connectOptions.authSource
    || getMongoUriOptionValue(sourceUri, 'authSource')
    || conn?.defaultDb
    || '',
  ).trim();
  const args = ['--host', host];
  if (username) args.push('--username', username);
  if (password) args.push('--password', password);
  if (authSource) args.push('--authenticationDatabase', authSource);
  if (connectOptions.tls === true) args.push('--ssl');
  if (connectOptions.tlsAllowInvalidCertificates === true) args.push('--sslAllowInvalidCertificates');
  return args;
}

function buildServerToolMongoClientOptions(conn = {}) {
  const sourceUri = String(conn?.uri || '').trim();
  const connectOptions = conn?.connectOptions && typeof conn.connectOptions === 'object'
    ? conn.connectOptions
    : {};
  const hasCredsInUri = hasCredentialsInMongoUri(sourceUri);
  const username = typeof connectOptions.username === 'string' ? connectOptions.username.trim() : '';
  const hasAuth = Boolean(username) && !hasCredsInUri;
  return {
    connectTimeoutMS: 15000,
    serverSelectionTimeoutMS: 15000,
    socketTimeoutMS: 30000,
    maxPoolSize: 2,
    minPoolSize: 0,
    ...(connectOptions.tls === true && { tls: true }),
    ...(connectOptions.tlsAllowInvalidCertificates === true && { tlsAllowInvalidCertificates: true }),
    ...(connectOptions.authSource && { authSource: String(connectOptions.authSource).trim() }),
    ...(connectOptions.replicaSet && { replicaSet: String(connectOptions.replicaSet).trim() }),
    ...(connectOptions.readPreference && { readPreference: String(connectOptions.readPreference).trim() }),
    ...(connectOptions.directConnection === true && { directConnection: true }),
    ...(hasAuth && {
      auth: {
        username,
        password: String(connectOptions.password ?? ''),
      },
    }),
  };
}

async function runServerToolMongoCommand(conn = {}, node = '', runner = null) {
  const uri = buildServerToolUri(conn, node);
  const client = new MongoClient(uri, buildServerToolMongoClientOptions(conn));
  await client.connect();
  try {
    if (typeof runner !== 'function') return null;
    return await runner(client);
  } finally {
    try { await client.close(); } catch {}
  }
}

function trimCommandOutput(text = '', maxChars = SERVER_TOOL_OUTPUT_LIMIT) {
  const value = String(text || '');
  if (value.length <= maxChars) return value;
  return value.slice(value.length - maxChars);
}

function appendCommandOutput(current = '', chunk = '', maxChars = SERVER_TOOL_OUTPUT_LIMIT) {
  return trimCommandOutput(`${String(current || '')}${String(chunk || '')}`, maxChars);
}

function parseLastJsonLine(text = '') {
  const lines = String(text || '').split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = lines[i];
    if (!(line.startsWith('{') || line.startsWith('['))) continue;
    try {
      return JSON.parse(line);
    } catch {}
  }
  return null;
}

function probeToolBinary(bin = '', displayName = 'binary') {
  const normalizedBin = String(bin || '').trim();
  if (!normalizedBin) {
    return {
      bin: '',
      displayName,
      available: false,
      version: null,
      status: null,
      errorCode: null,
      error: `${displayName} binary is not configured.`,
    };
  }
  try {
    const probe = spawnSync(normalizedBin, ['--version'], {
      windowsHide: true,
      encoding: 'utf8',
      timeout: 2500,
      env: {
        ...process.env,
        NO_COLOR: '1',
        TERM: 'dumb',
      },
    });
    const errorCodeRaw = String(probe?.error?.code || '').trim();
    const errorCode = errorCodeRaw.toUpperCase();
    const hasSpawnError = Boolean(probe?.error);
    const timedOut = errorCode === 'ETIMEDOUT';
    const missing = errorCode === 'ENOENT';
    const spawned = !hasSpawnError || timedOut;
    const ok = spawned && !missing;
    const statusCode = Number.isFinite(Number(probe?.status)) ? Number(probe.status) : null;
    const output = firstNonEmptyLine(probe?.stdout) || firstNonEmptyLine(probe?.stderr);
    let errorText = null;
    if (missing) {
      errorText = `${displayName} binary not found ("${normalizedBin}").`;
    } else if (hasSpawnError && !timedOut) {
      errorText = String(probe?.error?.message || probe?.error || '').trim() || 'probe failed';
    } else if (timedOut) {
      errorText = 'probe timed out after 2500ms';
    } else if (statusCode !== null && statusCode !== 0) {
      errorText = `probe exited with code ${statusCode}`;
    }
    return {
      bin: normalizedBin,
      displayName,
      available: ok,
      version: output || null,
      status: statusCode,
      errorCode: errorCodeRaw || null,
      error: errorText,
    };
  } catch (err) {
    return {
      bin: normalizedBin,
      displayName,
      available: false,
      version: null,
      status: null,
      errorCode: null,
      error: String(err?.message || err),
    };
  }
}

function getServerToolBinarySelection(tool = {}, conn = {}) {
  const newBin = String(tool?.bin || '').trim();
  const legacyBinRaw = String(tool?.legacyBin || '').trim();
  const hasLegacy = Boolean(legacyBinRaw) && legacyBinRaw !== newBin;
  const preferLegacy = hasLegacy && isLegacyServerForMongosh(conn);
  if (preferLegacy) {
    return {
      primaryBin: legacyBinRaw,
      primaryMode: 'legacy',
      fallbackBin: newBin || '',
      fallbackMode: 'new',
    };
  }
  return {
    primaryBin: newBin,
    primaryMode: 'new',
    fallbackBin: hasLegacy ? legacyBinRaw : '',
    fallbackMode: 'legacy',
  };
}

function getServerToolRuntimeStatus({ force = false } = {}) {
  const now = Date.now();
  if (!force && serverToolRuntimeStatusCache.value && (now - Number(serverToolRuntimeStatusCache.checkedAt || 0) < SERVER_TOOL_RUNTIME_TTL_MS)) {
    return serverToolRuntimeStatusCache.value;
  }
  const status = {};
  const tools = Object.values(SERVER_TOOL_CONFIG).filter((tool) => tool.source === 'binary');
  for (const tool of tools) {
    const mainProbe = probeToolBinary(tool.bin, tool.displayName);
    const legacyProbe = tool.legacyBin
      ? probeToolBinary(tool.legacyBin, `${tool.displayName} legacy`)
      : null;
    const availableLegacy = Boolean(legacyProbe?.available);
    const availableNew = Boolean(mainProbe?.available);
    const availableAny = availableNew || availableLegacy;
    status[tool.id] = {
      id: tool.id,
      bin: mainProbe.bin,
      legacyBin: legacyProbe?.bin || null,
      displayName: tool.displayName,
      available: availableAny,
      availableNew,
      availableLegacy,
      version: mainProbe.version || legacyProbe?.version || null,
      legacyVersion: legacyProbe?.version || null,
      status: mainProbe.status,
      legacyStatus: legacyProbe?.status ?? null,
      errorCode: mainProbe.errorCode,
      legacyErrorCode: legacyProbe?.errorCode || null,
      error: availableAny ? null : (mainProbe.error || legacyProbe?.error || null),
      legacyError: legacyProbe?.error || null,
    };
  }
  serverToolRuntimeStatusCache = { checkedAt: now, value: status };
  return status;
}

function ensureServerToolBinaryAvailable(toolId = '', conn = {}) {
  const tool = SERVER_TOOL_CONFIG[toolId];
  if (!tool || tool.source !== 'binary') return;
  const status = getServerToolRuntimeStatus({ force: false })?.[toolId];
  const selection = getServerToolBinarySelection(tool, conn);
  const primaryAvailable = selection.primaryMode === 'legacy'
    ? Boolean(status?.availableLegacy)
    : Boolean(status?.availableNew);
  if (primaryAvailable) return;
  const fallbackAvailable = selection.fallbackMode === 'legacy'
    ? Boolean(status?.availableLegacy)
    : Boolean(status?.availableNew);
  if (fallbackAvailable) return;
  const checkedBins = [selection.primaryBin, selection.fallbackBin]
    .map((value) => String(value || '').trim())
    .filter(Boolean)
    .join(', ');
  const errorMessage = status?.error || `${tool.displayName} binary not found (${checkedBins || tool.bin}).`;
  throw createRequestError(errorMessage, 503, 'unsupported');
}

function isServerToolCompatibilityErrorText(text = '') {
  const value = String(text || '');
  if (!value) return false;
  return SERVER_TOOL_WIRE_COMPAT_RE.test(value) || SERVER_TOOL_REQUIRES_42_RE.test(value);
}

function isServerToolMissingBinaryErrorText(text = '') {
  const value = String(text || '');
  if (!value) return false;
  return SERVER_TOOL_MISSING_BIN_RE.test(value);
}

function resolveServerToolCommandArgs(argsOrBuilder = [], mode = 'new') {
  const resolved = typeof argsOrBuilder === 'function'
    ? argsOrBuilder(mode)
    : argsOrBuilder;
  if (!Array.isArray(resolved)) return [];
  return resolved.map((value) => String(value ?? '')).filter((value) => value.length > 0);
}

async function runServerToolCommandWithFallback(tool = {}, argsOrBuilder = [], { timeoutMs = SERVER_TOOL_EXEC_TIMEOUT_DEFAULT_MS, conn = {} } = {}) {
  const selection = getServerToolBinarySelection(tool, conn);
  const primaryBin = String(selection.primaryBin || '').trim();
  const fallbackBin = String(selection.fallbackBin || '').trim();
  const attempts = [];
  const primaryArgs = resolveServerToolCommandArgs(argsOrBuilder, selection.primaryMode);
  const first = await runSpawnedCommand(primaryBin, primaryArgs, { timeoutMs });
  attempts.push({
    mode: selection.primaryMode,
    bin: primaryBin,
    ok: first.ok === true,
    code: first.code,
    timedOut: first.timedOut === true,
  });
  if (first.ok || !fallbackBin || fallbackBin === primaryBin) {
    return {
      ...first,
      binUsed: primaryBin,
      binMode: selection.primaryMode,
      fallbackUsed: false,
      fallbackReason: null,
      fallbackFrom: null,
      attempts,
    };
  }

  const firstOutput = `${first.stderr || ''}\n${first.stdout || ''}`;
  const firstIsCompat = isServerToolCompatibilityErrorText(firstOutput);
  const firstMissingBin = isServerToolMissingBinaryErrorText(firstOutput);
  const shouldFallback = firstMissingBin || (selection.primaryMode === 'new' && firstIsCompat);
  if (!shouldFallback) {
    return {
      ...first,
      binUsed: primaryBin,
      binMode: selection.primaryMode,
      fallbackUsed: false,
      fallbackReason: null,
      fallbackFrom: null,
      attempts,
    };
  }

  const fallbackArgs = resolveServerToolCommandArgs(argsOrBuilder, selection.fallbackMode);
  const second = await runSpawnedCommand(fallbackBin, fallbackArgs, { timeoutMs });
  attempts.push({
    mode: selection.fallbackMode,
    bin: fallbackBin,
    ok: second.ok === true,
    code: second.code,
    timedOut: second.timedOut === true,
  });
  return {
    ...second,
    binUsed: fallbackBin,
    binMode: selection.fallbackMode,
    fallbackUsed: true,
    fallbackReason: firstMissingBin ? 'missing_binary' : 'wire_compatibility',
    fallbackFrom: primaryBin,
    attempts,
  };
}

async function runSpawnedCommand(bin = '', args = [], { timeoutMs = SERVER_TOOL_EXEC_TIMEOUT_DEFAULT_MS } = {}) {
  const start = Date.now();
  const commandTimeoutMs = Math.max(1000, Math.min(Number(timeoutMs) || SERVER_TOOL_EXEC_TIMEOUT_DEFAULT_MS, SERVER_TOOL_EXEC_TIMEOUT_MAX_MS));
  return new Promise((resolve) => {
    const child = spawn(bin, args, {
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        NO_COLOR: '1',
        TERM: 'dumb',
      },
    });
    let stdout = '';
    let stderr = '';
    let timedOut = false;
    let settled = false;
    let killTimer = null;
    const done = (payload) => {
      if (settled) return;
      settled = true;
      if (killTimer) clearTimeout(killTimer);
      resolve({
        ...payload,
        stdout: trimCommandOutput(stdout),
        stderr: trimCommandOutput(stderr),
        durationMs: Date.now() - start,
      });
    };
    killTimer = setTimeout(() => {
      timedOut = true;
      try { child.kill('SIGTERM'); } catch {}
      setTimeout(() => { try { child.kill('SIGKILL'); } catch {} }, 800);
    }, commandTimeoutMs);
    child.stdout?.setEncoding?.('utf8');
    child.stderr?.setEncoding?.('utf8');
    child.stdout?.on('data', (chunk) => {
      stdout = appendCommandOutput(stdout, chunk);
    });
    child.stderr?.on('data', (chunk) => {
      stderr = appendCommandOutput(stderr, chunk);
    });
    child.on('error', (err) => {
      stderr = appendCommandOutput(stderr, String(err?.message || err));
      done({
        ok: false,
        code: null,
        signal: null,
        timedOut,
      });
    });
    child.on('exit', (code, signal) => {
      done({
        ok: !timedOut && Number(code) === 0,
        code: Number.isFinite(Number(code)) ? Number(code) : null,
        signal: signal || null,
        timedOut,
      });
    });
  });
}

function ensureSafeIdentifier(value = '', label = 'value') {
  const text = String(value || '').trim();
  if (!text) return '';
  if (!/^[a-zA-Z0-9._-]+$/.test(text)) {
    throw createRequestError(`${label} contains unsupported characters.`, 400, 'validation');
  }
  return text;
}

function ensureSafeIdentifierList(value, label = 'values', { max = 256 } = {}) {
  const items = Array.isArray(value)
    ? value
    : (typeof value === 'string' ? value.split(',') : []);
  const parsed = [];
  for (const item of items) {
    const normalized = ensureSafeIdentifier(item, label);
    if (!normalized) continue;
    if (!parsed.includes(normalized)) parsed.push(normalized);
    if (parsed.length >= max) break;
  }
  return parsed;
}

function ensureJsonText(value, label = 'JSON') {
  const text = String(value || '').trim();
  if (!text) return '';
  try {
    JSON.parse(text);
    return text;
  } catch (err) {
    throw createRequestError(`${label} must be valid JSON: ${String(err?.message || err)}`, 400, 'validation');
  }
}

function resolveFilePath(basePath = '', value = '', fallbackName = 'output.dat') {
  const base = normalizeFsPath(basePath);
  const raw = String(value || '').trim();
  if (!raw) return normalizeFsPath(`${base}/${fallbackName}`);
  if (isAbsoluteFsPath(raw)) return normalizeFsPath(raw);
  return normalizeFsPath(`${base}/${raw}`);
}

function normalizeConsoleScopeInput(input = {}) {
  const source = input && typeof input === 'object' ? input : {};
  const levelRaw = String(source.level || '').trim().toLowerCase();
  const level = (levelRaw === 'database' || levelRaw === 'global') ? levelRaw : 'collection';
  const db = typeof source.db === 'string' ? source.db.trim() : '';
  const collection = typeof source.collection === 'string' ? source.collection.trim() : '';

  if (level === 'collection') {
    if (!db || !collection) {
      throw createRequestError('Collection scope requires both db and collection.', 400);
    }
    return { level, db, collection };
  }

  if (level === 'database') {
    if (!db) throw createRequestError('Database scope requires db.', 400);
    return { level, db, collection: null };
  }

  return { level: 'global', db: db || null, collection: null };
}

function getConsoleScopeTag(scope = {}) {
  if (scope.level === 'collection') return `collection:${scope.db}.${scope.collection}`;
  if (scope.level === 'database') return `database:${scope.db}`;
  return 'global';
}

function getMongoshSessionAuditDb(session = null) {
  if (!session) return null;
  if (session.scope?.level === 'global') {
    return session.activeDb || session.scope?.db || null;
  }
  return session.scope?.db || session.activeDb || null;
}

function normalizeConsoleShell(rawShell = 'mongosh') {
  const value = String(rawShell || '').trim().toLowerCase();
  if (!value) return 'mongosh';
  if (value === 'mongosh') return 'mongosh';
  if (value === 'mongo' || value === 'legacy') return 'mongo';
  return null;
}

function getConsoleShellConfig(rawShell = 'mongosh') {
  const normalized = normalizeConsoleShell(rawShell);
  if (!normalized) {
    throw createRequestError('Unsupported console shell. Use "mongosh" or "mongo".', 400, 'validation');
  }
  return CONSOLE_SHELL_CONFIG[normalized];
}

function resolveConsoleShellMode(rawShell = 'mongosh', conn = null) {
  const normalized = normalizeConsoleShell(rawShell);
  if (!normalized) {
    throw createRequestError('Unsupported console shell. Use "mongosh" or "mongo".', 400, 'validation');
  }
  if (!conn) return normalized;
  // Always use the right binary for the server version, regardless of which tab the user picked:
  // Old MongoDB (< 4.2) → always mongosh1 (mongo mode)
  // New MongoDB (>= 4.2) → always mongosh2 (mongosh mode)
  if (isLegacyServerForMongosh(conn)) return 'mongo';
  return 'mongosh';
}

function parseConsoleCommandMeta(rawCommand = '') {
  const text = String(rawCommand || '').trim();
  if (!text) return { kind: 'empty', method: '', targetDb: '', targetCollection: '' };

  const useMatch = text.match(/^use\s+([a-zA-Z0-9_.-]+)\s*;?$/i);
  if (useMatch) {
    return { kind: 'use', method: 'use', targetDb: String(useMatch[1] || '').trim(), targetCollection: '' };
  }

  const collectionMatch = text.match(/^db\.(?:getCollection\((['"])(.+?)\1\)|([\w.$-]+))\.(\w+)\s*\(/);
  if (collectionMatch) {
    return {
      kind: 'collection',
      method: String(collectionMatch[4] || '').trim(),
      targetDb: '',
      targetCollection: String(collectionMatch[2] || collectionMatch[3] || '').trim(),
    };
  }

  const dbMatch = text.match(/^db\.(\w+)\s*\(/);
  if (dbMatch) {
    return { kind: 'db', method: String(dbMatch[1] || '').trim(), targetDb: '', targetCollection: '' };
  }

  if (text === 'help' || text === '.help' || text === '?' || text === 'clear' || text === '.clear') {
    return { kind: 'builtin', method: text.replace(/^\./, ''), targetDb: '', targetCollection: '' };
  }

  return { kind: 'script', method: 'script', targetDb: '', targetCollection: '' };
}

function validateMongoshCommandForScope(rawCommand, scope = {}) {
  const command = String(rawCommand || '').trim();
  if (!command) throw createRequestError('Command is empty.', 400);
  if (command.length > MONGOSH_MAX_COMMAND_LENGTH) {
    throw createRequestError(`Command is too long (max ${MONGOSH_MAX_COMMAND_LENGTH} chars).`, 400);
  }
  const meta = parseConsoleCommandMeta(command);
  if (meta.kind === 'empty') throw createRequestError('Command is empty.', 400);

  if (scope.level === 'collection') {
    if (meta.kind === 'use') {
      throw createRequestError(`Context is locked to collection ${scope.collection}.`, 400);
    }
    if (meta.kind === 'db') {
      throw createRequestError('Collection scope only allows db.<collection>.<method>(...) commands.', 400);
    }
    if (meta.kind === 'collection') {
      if (!meta.targetCollection) throw createRequestError('Collection name is required.', 400);
      if (meta.targetCollection !== scope.collection) {
        throw createRequestError(`Context is locked to collection ${scope.collection}.`, 400);
      }
      return { command, meta };
    }
    if (meta.kind === 'builtin') return { command, meta };
    throw createRequestError('Unsupported command in locked collection scope.', 400);
  }

  if (scope.level === 'database') {
    if (meta.kind === 'use') {
      throw createRequestError(`Context is locked to database ${scope.db}.`, 400);
    }
    if (meta.kind === 'script') {
      throw createRequestError('Unsupported command in locked database scope.', 400);
    }
    return { command, meta };
  }

  if (/db\.getSiblingDB\s*\(/i.test(command) || /(?:^|[^.\w$])(Mongo|connect)\s*\(/i.test(command)) {
    throw createRequestError('Changing connection topology inside Console is not allowed.', 400);
  }

  return { command, meta };
}

function hasCredentialsInMongoUri(uri = '') {
  const normalized = String(uri || '').trim();
  const withoutScheme = normalized.replace(/^mongodb(?:\+srv)?:\/\//i, '');
  const authority = withoutScheme.split('/')[0] || '';
  return authority.includes('@');
}

function withMongoUriOption(uri = '', key = '', value = '') {
  const source = String(uri || '').trim();
  const optionKey = String(key || '').trim();
  const optionValue = value === undefined || value === null ? '' : String(value).trim();
  if (!source || !optionKey || optionValue === '') return source;

  const hashIndex = source.indexOf('#');
  const withoutHash = hashIndex === -1 ? source : source.slice(0, hashIndex);
  const hashPart = hashIndex === -1 ? '' : source.slice(hashIndex);
  const qIndex = withoutHash.indexOf('?');
  const base = qIndex === -1 ? withoutHash : withoutHash.slice(0, qIndex);
  const rawQuery = qIndex === -1 ? '' : withoutHash.slice(qIndex + 1);
  const params = new URLSearchParams(rawQuery);
  params.set(optionKey, optionValue);
  const nextQuery = params.toString();
  return `${base}${nextQuery ? `?${nextQuery}` : ''}${hashPart}`;
}

function isLegacyServerForMongosh(conn = {}) {
  const major = Number(conn?.version?.major || 0);
  const minor = Number(conn?.version?.minor || 0);
  if (!Number.isFinite(major) || major <= 0) return false;
  if (!Number.isFinite(minor) || minor < 0) return false;
  return major < 4 || (major === 4 && minor < 2);
}

function buildMongoshArgs(conn) {
  const args = [];
  const uri = String(conn?.uri || '').trim();
  if (!uri) throw createRequestError('Connection URI is missing for mongosh session.', 500);
  let targetUri = uri;

  const connectOptions = conn?.connectOptions && typeof conn.connectOptions === 'object'
    ? conn.connectOptions
    : {};
  const hasCredsInUri = hasCredentialsInMongoUri(uri);
  const username = typeof connectOptions.username === 'string' ? connectOptions.username.trim() : '';
  const replicaSet = typeof connectOptions.replicaSet === 'string' ? connectOptions.replicaSet.trim() : '';
  const readPreference = typeof connectOptions.readPreference === 'string' ? connectOptions.readPreference.trim() : '';

  // Minimal-strip policy: never pass credentials via argv.
  // Real shell mode uses URI auth only to avoid leaking creds in process arguments.
  if (!hasCredsInUri && username) {
    throw createRequestError(
      'Real shell mode requires credentials in the MongoDB URI. Add username/password to the URI or use ConsoleUI.',
      400,
      'validation',
    );
  }

  // Keep topology/read routing in URI to support older and stricter mongosh binaries.
  if (connectOptions.tls === true) targetUri = withMongoUriOption(targetUri, 'tls', 'true');
  if (connectOptions.tlsAllowInvalidCertificates === true) {
    targetUri = withMongoUriOption(targetUri, 'tlsAllowInvalidCertificates', 'true');
  }
  if (connectOptions.directConnection === true) targetUri = withMongoUriOption(targetUri, 'directConnection', 'true');
  if (replicaSet) targetUri = withMongoUriOption(targetUri, 'replicaSet', replicaSet);
  if (readPreference) targetUri = withMongoUriOption(targetUri, 'readPreference', readPreference);

  args.push(targetUri);
  args.push('--quiet', '--norc');
  return args;
}

function ensureMongoshBinaryAvailable(shellMode = 'mongosh') {
  const shell = getConsoleShellConfig(shellMode);
  const now = Date.now();
  const cached = consoleShellBinCheckCache.get(shell.mode);
  if (cached && (now - Number(cached.checkedAt || 0) < MONGOSH_BIN_CHECK_TTL_MS) && cached.ok !== null) {
    if (cached.ok) return;
    throw createRequestError(cached.error || `${shell.displayName} binary not found ("${shell.bin}").`, 503, 'unsupported');
  }
  let ok = false;
  let error = '';
  try {
    const probe = spawnSync(shell.bin, ['--version'], {
      windowsHide: true,
      stdio: 'ignore',
      timeout: 2500,
      env: {
        ...process.env,
        NO_COLOR: '1',
        TERM: 'dumb',
      },
    });
    if (probe?.error) {
      const code = String(probe.error?.code || '').toUpperCase();
      if (code === 'ENOENT') {
        error = `${shell.displayName} binary not found ("${shell.bin}"). Install ${shell.displayName} or set ${shell.mode === 'mongo' ? 'MONGO_BIN' : 'MONGOSH_BIN'}.`;
      } else {
        error = `${shell.displayName} probe failed: ${String(probe.error?.message || probe.error)}`;
      }
    } else {
      ok = Number(probe?.status) === 0;
      if (!ok) error = `${shell.displayName} probe failed with exit code ${Number(probe?.status) || 1}.`;
    }
  } catch (err) {
    error = `${shell.displayName} probe failed: ${String(err?.message || err)}`;
  }
  consoleShellBinCheckCache.set(shell.mode, {
    checkedAt: now,
    ok,
    error,
  });
  if (!ok) throw createRequestError(error || `${shell.displayName} binary not found ("${shell.bin}").`, 503, 'unsupported');
}

function firstNonEmptyLine(text = '') {
  return String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find(Boolean) || '';
}

function getConsoleShellRuntimeStatus({ force = false } = {}) {
  const now = Date.now();
  if (!force && consoleShellRuntimeStatusCache.value && (now - Number(consoleShellRuntimeStatusCache.checkedAt || 0) < STATUS_SHELL_RUNTIME_TTL_MS)) {
    return consoleShellRuntimeStatusCache.value;
  }
  const shells = {};
  for (const shell of Object.values(CONSOLE_SHELL_CONFIG)) {
    try {
      const probe = spawnSync(shell.bin, ['--version'], {
        windowsHide: true,
        encoding: 'utf8',
        timeout: 2500,
        env: {
          ...process.env,
          NO_COLOR: '1',
          TERM: 'dumb',
        },
      });
      const ok = !probe?.error && Number(probe?.status) === 0;
      const output = firstNonEmptyLine(probe?.stdout) || firstNonEmptyLine(probe?.stderr);
      const errorCode = String(probe?.error?.code || '').trim();
      const errorMessage = String(probe?.error?.message || '').trim();
      shells[shell.mode] = {
        bin: shell.bin,
        displayName: shell.displayName,
        available: ok,
        version: output || null,
        status: Number.isFinite(Number(probe?.status)) ? Number(probe.status) : null,
        errorCode: errorCode || null,
        error: errorMessage || (!ok ? `exit ${Number(probe?.status) || 1}` : null),
      };
    } catch (err) {
      shells[shell.mode] = {
        bin: shell.bin,
        displayName: shell.displayName,
        available: false,
        version: null,
        status: null,
        errorCode: null,
        error: String(err?.message || err),
      };
    }
  }
  consoleShellRuntimeStatusCache = {
    checkedAt: now,
    value: shells,
  };
  return shells;
}

function writeSseEvent(res, eventName, payload, eventId = null) {
  const idLine = Number.isFinite(Number(eventId)) ? `id: ${Math.round(Number(eventId))}\n` : '';
  const eventLine = eventName ? `event: ${eventName}\n` : '';
  const dataText = typeof payload === 'string' ? payload : JSON.stringify(payload);
  const dataLines = String(dataText || '')
    .split(/\r?\n/)
    .map((line) => `data: ${line}`)
    .join('\n');
  res.write(`${idLine}${eventLine}${dataLines}\n\n`);
  if (typeof res.flush === 'function') {
    try { res.flush(); } catch {}
  }
}

function getSessionIdsByConnection(connId) {
  let ids = mongoshSessionsByConn.get(connId);
  if (!ids) {
    ids = new Set();
    mongoshSessionsByConn.set(connId, ids);
  }
  return ids;
}

function registerMongoshSession(session) {
  mongoshSessions.set(session.id, session);
  getSessionIdsByConnection(session.connId).add(session.id);
}

function unregisterMongoshSession(session) {
  if (session?.cleanupTimer) {
    clearTimeout(session.cleanupTimer);
    session.cleanupTimer = null;
  }
  mongoshSessions.delete(session.id);
  const ids = mongoshSessionsByConn.get(session.connId);
  if (!ids) return;
  ids.delete(session.id);
  if (ids.size === 0) mongoshSessionsByConn.delete(session.connId);
}

function scheduleMongoshSessionCleanup(session, ttlMs = MONGOSH_FINALIZED_TTL_MS) {
  if (!session) return;
  if (session.cleanupTimer) return;
  const delayMs = Math.max(1000, Math.round(Number(ttlMs) || MONGOSH_FINALIZED_TTL_MS));
  session.cleanupTimer = setTimeout(() => {
    session.cleanupTimer = null;
    unregisterMongoshSession(session);
  }, delayMs);
}

function emitMongoshEvent(session, type, payload = {}) {
  if (!session || session.finalized) return null;
  const id = session.nextEventId;
  session.nextEventId += 1;
  const event = {
    id,
    ts: Date.now(),
    type,
    ...payload,
  };
  session.events.push(event);
  if (session.events.length > MONGOSH_MAX_BUFFERED_EVENTS) {
    session.events.shift();
  }
  for (const subscriber of [...session.subscribers]) {
    try {
      writeSseEvent(subscriber.res, 'message', event, event.id);
    } catch {
      try { subscriber.res.end(); } catch {}
      session.subscribers.delete(subscriber);
    }
  }
  return event;
}

function finalizeMongoshSession(session, meta = {}) {
  if (!session || session.finalized) return;
  const exitPayload = {
    code: Number.isFinite(Number(meta.code)) ? Number(meta.code) : null,
    signal: meta.signal || null,
    reason: meta.reason || 'exit',
    forced: Boolean(meta.forced),
  };
  emitMongoshEvent(session, 'exit', exitPayload);
  session.finalized = true;
  session.closedAt = Date.now();
  session.finalizedMeta = exitPayload;
  audit(
    session.connId,
    'console_close',
    {
      source: session.mode || 'mongosh',
      scope: session.scope.level,
      db: getMongoshSessionAuditDb(session),
      col: session.scope.collection || null,
      method: 'session',
      reason: exitPayload.reason,
      sessionId: session.id,
      user: session.user || 'anonymous',
    },
  );

  for (const subscriber of [...session.subscribers]) {
    try {
      writeSseEvent(subscriber.res, 'end', exitPayload, null);
    } catch {}
    try { subscriber.res.end(); } catch {}
  }
  session.subscribers.clear();
  scheduleMongoshSessionCleanup(session);
}

function getMongoshSessionForRequest(req, sessionId) {
  const session = mongoshSessions.get(sessionId);
  if (!session || session.connId !== req.connId) return null;
  return session;
}

function getActiveMongoshSessionCount(connId) {
  const ids = [...(mongoshSessionsByConn.get(connId) || [])];
  if (ids.length === 0) return 0;
  let active = 0;
  for (const id of ids) {
    const session = mongoshSessions.get(id);
    if (!session) continue;
    if (session.finalized) {
      unregisterMongoshSession(session);
      continue;
    }
    active += 1;
  }
  return active;
}

function createMongoshSession(req, scope, shellMode = 'mongosh') {
  const conn = req?.conn;
  if (!conn) throw createRequestError('Connection is required.', 500);
  const resolvedShellMode = resolveConsoleShellMode(shellMode, conn);
  const shell = getConsoleShellConfig(resolvedShellMode);
  ensureMongoshBinaryAvailable(shell.mode);
  const activeSessionCount = getActiveMongoshSessionCount(req.connId);
  if (activeSessionCount >= MONGOSH_MAX_SESSIONS_PER_CONN) {
    throw createRequestError(`Too many mongosh sessions for this connection (max ${MONGOSH_MAX_SESSIONS_PER_CONN}).`, 429);
  }

  const args = buildMongoshArgs(conn);
  const child = spawn(shell.bin, args, {
    stdio: ['pipe', 'pipe', 'pipe'],
    windowsHide: true,
    env: {
      ...process.env,
      NO_COLOR: '1',
      TERM: 'dumb',
    },
  });

  const sessionId = randomUUID().replace(/-/g, '');
  const session = {
    id: sessionId,
    connId: req.connId,
    mode: shell.mode,
    shellBin: shell.bin,
    scope,
    user: req.auditUser || conn.lastUiUser || conn.username || 'anonymous',
    process: child,
    createdAt: Date.now(),
    lastActivity: Date.now(),
    activeDb: scope.db || null,
    events: [],
    subscribers: new Set(),
    nextEventId: 1,
    finalized: false,
    closing: false,
    finalizedMeta: null,
    closedAt: null,
    cleanupTimer: null,
  };
  registerMongoshSession(session);

  child.stdout?.setEncoding?.('utf8');
  child.stderr?.setEncoding?.('utf8');

  child.stdout?.on('data', (chunk) => {
    session.lastActivity = Date.now();
    const text = String(chunk || '');
    if (!text) return;
    emitMongoshEvent(session, 'stdout', { text });
  });

  child.stderr?.on('data', (chunk) => {
    session.lastActivity = Date.now();
    const text = String(chunk || '');
    if (!text) return;
    emitMongoshEvent(session, 'stderr', { text });
  });

  child.on('error', (err) => {
    const isNotFound = String(err?.code || '').toUpperCase() === 'ENOENT';
    const text = isNotFound
      ? `${shell.displayName} binary not found ("${shell.bin}"). Install ${shell.displayName} or set ${shell.mode === 'mongo' ? 'MONGO_BIN' : 'MONGOSH_BIN'}.`
      : String(err?.message || err || `${shell.displayName} process error`);
    emitMongoshEvent(session, 'error', { text });
    finalizeMongoshSession(session, { reason: isNotFound ? 'spawn_enoent' : 'spawn_error' });
  });

  child.on('exit', (code, signal) => {
    finalizeMongoshSession(session, {
      code,
      signal,
      reason: session.closing ? 'closed' : 'exit',
    });
  });

  emitMongoshEvent(session, 'system', { text: `${shell.mode} session started (${getConsoleScopeTag(scope)})` });
  if (scope.db) {
    try {
      child.stdin.write(`use ${scope.db}\n`);
      session.activeDb = scope.db;
    } catch {}
  }
  return session;
}

async function terminateMongoshSession(session, reason = 'terminated') {
  if (!session) return false;
  if (session.finalized) return true;
  if (session.closing) return true;
  session.closing = true;
  emitMongoshEvent(session, 'system', { text: `closing session (${reason})` });
  try { session.process.stdin.write('.exit\n'); } catch {}
  try { session.process.stdin.end(); } catch {}
  try { session.process.kill('SIGTERM'); } catch {}
  await waitMs(MONGOSH_TERMINATE_GRACE_MS);
  if (!session.finalized) {
    try { session.process.kill('SIGKILL'); } catch {}
    finalizeMongoshSession(session, { reason, forced: true });
  }
  return true;
}

async function terminateMongoshSessionsForConnection(connId, reason = 'connection_closed') {
  const ids = [...(mongoshSessionsByConn.get(connId) || [])];
  if (ids.length === 0) return 0;
  const sessions = ids.map((id) => mongoshSessions.get(id)).filter(Boolean);
  const activeSessions = [];
  for (const session of sessions) {
    if (session.finalized) {
      unregisterMongoshSession(session);
      continue;
    }
    activeSessions.push(session);
  }
  await Promise.allSettled(activeSessions.map((session) => terminateMongoshSession(session, reason)));
  return sessions.length;
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
function getDefaultIdSortDirection(input) {
  const rawText = typeof input === 'string' ? input.trim() : '';
  const sortObj = parseFilterInput(input);
  if (!sortObj || typeof sortObj !== 'object' || Array.isArray(sortObj)) return 1;
  const entries = Object.entries(sortObj);
  if (entries.length === 0) return rawText && rawText !== '{}' ? 0 : 1;
  if (entries.length !== 1 || entries[0][0] !== '_id') return 0;
  const direction = Number(entries[0][1]);
  if (direction === -1) return -1;
  if (direction === 1) return 1;
  return 0;
}
function transformFilter(obj) {
  if (obj === null || typeof obj !== 'object') return obj;
  if (!Array.isArray(obj)) {
    const keys = Object.keys(obj);
    if (keys.length === 1 && keys[0] === '$oid' && typeof obj.$oid === 'string' && /^[a-f0-9]{24}$/i.test(obj.$oid)) {
      return new ObjectId(obj.$oid);
    }
    if (keys.length === 1 && keys[0] === '$date') {
      try {
        const parsedDate = new Date(obj.$date);
        if (!Number.isNaN(parsedDate.getTime())) return parsedDate;
      } catch {}
    }
  }
  if (Array.isArray(obj)) return obj.map(transformFilter);
  const r = {};
  for (const [k, v] of Object.entries(obj)) {
    if (k==='_id' && typeof v==='string' && /^[a-f0-9]{24}$/i.test(v)) r[k] = new ObjectId(v);
    else if (v && typeof v === 'object' && v.$oid) r[k] = new ObjectId(v.$oid);
    else r[k] = transformFilter(v);
  }
  return r;
}

function getValueByPath(obj, path) {
  const parts = String(path || '').split('.').filter(Boolean);
  if (parts.length === 0) return obj;
  let current = obj;
  for (const part of parts) {
    if (!current || typeof current !== 'object') return undefined;
    current = current[part];
  }
  return current;
}

function collectDistinctScalars(value, out) {
  if (Array.isArray(value)) {
    for (const item of value) collectDistinctScalars(item, out);
    return;
  }
  if (value !== undefined) out.push(value);
}

function distinctValueKey(value) {
  if (value instanceof ObjectId) return `oid:${value.toString()}`;
  if (value instanceof Date) return `date:${value.toISOString()}`;
  if (value === null) return 'null';
  const t = typeof value;
  if (t === 'string' || t === 'number' || t === 'boolean') return `${t}:${String(value)}`;
  try {
    return `json:${JSON.stringify(value)}`;
  } catch {
    return `str:${String(value)}`;
  }
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function serializeForExport(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === 'bigint') return { $numberLong: value.toString() };
  if (value instanceof ObjectId) return { $oid: value.toString() };
  if (value instanceof Date) {
    const ts = value.getTime();
    if (!Number.isFinite(ts)) return { $date: null };
    return { $date: value.toISOString() };
  }
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

function safeSerializeForExport(value) {
  try {
    return serializeForExport(value);
  } catch (err) {
    return {
      $serializeError: String(err?.message || err || 'serialize_failed'),
    };
  }
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

function parseJsonInputStrict(input, fieldName = 'value') {
  if (typeof input !== 'string') return input;
  const raw = input.trim();
  if (!raw) return undefined;
  try {
    return JSON.parse(raw);
  } catch (err) {
    throw createRequestError(`Invalid ${fieldName} JSON: ${err.message}`);
  }
}

function parseBooleanInput(value, fieldName = 'value') {
  if (value === undefined || value === null || value === '') return undefined;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const lowered = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'on'].includes(lowered)) return true;
    if (['false', '0', 'no', 'off'].includes(lowered)) return false;
  }
  throw createRequestError(`${fieldName} must be a boolean.`);
}

function parseObjectInputStrict(input, fieldName = 'value', { allowEmpty = true, mode = 'document' } = {}) {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null) throw createRequestError(`${fieldName} is required.`);
  if (!isPlainObject(parsed)) throw createRequestError(`${fieldName} must be an object.`);
  let normalized = deserializeImportValue(parsed);
  if (mode === 'filter') normalized = transformFilter(normalized);
  if (!allowEmpty && Object.keys(normalized).length === 0) throw createRequestError(`${fieldName} must not be empty.`);
  return normalized;
}

function parseOptionalObjectInputStrict(input, fieldName = 'value', options = {}) {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null || parsed === '') return undefined;
  return parseObjectInputStrict(parsed, fieldName, options);
}

function parseArrayInputStrict(input, fieldName = 'value', { allowEmpty = true } = {}) {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null) throw createRequestError(`${fieldName} is required.`);
  if (!Array.isArray(parsed)) throw createRequestError(`${fieldName} must be an array.`);
  if (!allowEmpty && parsed.length === 0) throw createRequestError(`${fieldName} must not be empty.`);
  return parsed;
}

function parseOptionalArrayInputStrict(input, fieldName = 'value', options = {}) {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null || parsed === '') return undefined;
  return parseArrayInputStrict(parsed, fieldName, options);
}

function isOperatorUpdateDoc(value) {
  if (!isPlainObject(value)) return false;
  const keys = Object.keys(value);
  return keys.length > 0 && keys.every((key) => key.startsWith('$'));
}

function parseUpdateSpecStrict(input, fieldName = 'update') {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null) throw createRequestError(`${fieldName} is required.`);
  if (Array.isArray(parsed)) {
    if (parsed.length === 0) throw createRequestError(`${fieldName} pipeline must not be empty.`);
    return parsed.map((stage, index) => {
      if (!isPlainObject(stage)) throw createRequestError(`${fieldName}[${index}] must be an object stage.`);
      return deserializeImportValue(stage);
    });
  }
  if (!isPlainObject(parsed)) throw createRequestError(`${fieldName} must be an object or pipeline array.`);
  const normalized = deserializeImportValue(parsed);
  if (!isOperatorUpdateDoc(normalized)) {
    throw createRequestError(`${fieldName} must use update operators like $set or be an update pipeline.`);
  }
  return normalized;
}

function parseFindAndModifyUpdateOrReplacement(input, fieldName = 'update') {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null) throw createRequestError(`${fieldName} is required.`);
  if (Array.isArray(parsed)) {
    return { kind: 'update', value: parseUpdateSpecStrict(parsed, fieldName) };
  }
  if (!isPlainObject(parsed)) throw createRequestError(`${fieldName} must be an object or pipeline array.`);
  const normalized = deserializeImportValue(parsed);
  if (isOperatorUpdateDoc(normalized)) {
    return { kind: 'update', value: normalized };
  }
  const keys = Object.keys(normalized || {});
  if (keys.length === 0) throw createRequestError(`${fieldName} must not be empty.`);
  if (keys.some((key) => key.startsWith('$'))) {
    throw createRequestError(`${fieldName} must be either full replacement fields or update operators, not a mix.`);
  }
  return { kind: 'replacement', value: normalized };
}

function parseReplacementDocumentStrict(input, fieldName = 'replacement') {
  const replacement = parseObjectInputStrict(input, fieldName, { allowEmpty: false, mode: 'document' });
  if (Object.keys(replacement).some((key) => key.startsWith('$'))) {
    throw createRequestError(`${fieldName} must be a replacement document, not update operators.`);
  }
  return replacement;
}

function parseHintInputStrict(input, fieldName = 'hint') {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null || parsed === '') return undefined;
  if (typeof parsed === 'string') {
    const trimmed = parsed.trim();
    if (!trimmed || trimmed === 'auto') return undefined;
    return trimmed;
  }
  if (isPlainObject(parsed)) return parsed;
  throw createRequestError(`${fieldName} must be a string or object.`);
}

function parseProjectionInputStrict(input, fieldName = 'projection') {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null || parsed === '') return undefined;
  return parseObjectInputStrict(parsed, fieldName, { allowEmpty: true, mode: 'document' });
}

function parseFilterInputStrict(input, fieldName = 'filter') {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null || parsed === '') return {};
  return parseObjectInputStrict(parsed, fieldName, { allowEmpty: true, mode: 'filter' });
}

function parseSortInputStrict(input, fieldName = 'sort') {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null || parsed === '') return undefined;
  return parseObjectInputStrict(parsed, fieldName, { allowEmpty: true, mode: 'filter' });
}

function parseExportLimitInput(input, fieldName = 'limit', defaultLimit = 1000) {
  if (input === undefined || input === null || input === '') {
    return { mode: 'fixed', limit: Math.max(1, Math.min(defaultLimit, EXPORT_LIMIT_MAX)) };
  }
  if (typeof input === 'string') {
    const raw = input.trim().toLowerCase();
    if (!raw) return { mode: 'fixed', limit: Math.max(1, Math.min(defaultLimit, EXPORT_LIMIT_MAX)) };
    if (raw === 'exact') return { mode: 'exact', limit: null };
    if (raw === 'unlimited' || raw === 'all') return { mode: 'unlimited', limit: null };
    const numeric = Number(raw);
    if (!Number.isFinite(numeric)) throw createRequestError(`${fieldName} must be a number, "exact", or "unlimited".`);
    const normalized = Math.floor(numeric);
    if (normalized <= 0) return { mode: 'unlimited', limit: null };
    return { mode: 'fixed', limit: Math.max(1, Math.min(normalized, EXPORT_LIMIT_MAX)) };
  }
  if (typeof input === 'number') {
    if (!Number.isFinite(input)) throw createRequestError(`${fieldName} must be a finite number, "exact", or "unlimited".`);
    const normalized = Math.floor(input);
    if (normalized <= 0) return { mode: 'unlimited', limit: null };
    return { mode: 'fixed', limit: Math.max(1, Math.min(normalized, EXPORT_LIMIT_MAX)) };
  }
  throw createRequestError(`${fieldName} must be a number, "exact", or "unlimited".`);
}

function parseCollationInputStrict(input, fieldName = 'collation') {
  const parsed = parseJsonInputStrict(input, fieldName);
  if (parsed === undefined || parsed === null || parsed === '') return undefined;
  return parseObjectInputStrict(parsed, fieldName, { allowEmpty: false, mode: 'document' });
}

function parseArrayFiltersInputStrict(input, fieldName = 'arrayFilters') {
  const arrayFilters = parseOptionalArrayInputStrict(input, fieldName, { allowEmpty: false });
  if (!arrayFilters) return undefined;
  return arrayFilters.map((item, index) => {
    if (!isPlainObject(item)) throw createRequestError(`${fieldName}[${index}] must be an object.`);
    return transformFilter(deserializeImportValue(item));
  });
}

function parseReturnDocumentOption(input = {}) {
  const raw = input?.returnDocument;
  if (typeof raw === 'string') {
    const normalized = raw.trim().toLowerCase();
    if (normalized === 'after' || normalized === 'before') return normalized;
    if (normalized === 'new') return 'after';
  }
  const returnNewDocument = parseBooleanInput(input?.returnNewDocument, 'returnNewDocument');
  if (returnNewDocument === true) return 'after';
  const returnOriginal = parseBooleanInput(input?.returnOriginal, 'returnOriginal');
  if (returnOriginal === false) return 'after';
  return 'before';
}

function buildFindOneAndResult(result) {
  if (result && typeof result === 'object' && Object.prototype.hasOwnProperty.call(result, 'value')) {
    return {
      value: result.value ?? null,
      ok: result.ok ?? 1,
      lastErrorObject: result.lastErrorObject || null,
    };
  }
  return {
    value: result ?? null,
    ok: 1,
    lastErrorObject: null,
  };
}

function parseBulkWriteOperations(input) {
  const operations = parseArrayInputStrict(input, 'operations', { allowEmpty: false });
  if (operations.length > 10000) {
    throw createRequestError('Too many bulk operations in one request (max 10000).');
  }
  return operations.map((entry, index) => {
    if (!isPlainObject(entry)) throw createRequestError(`operations[${index}] must be an object.`);
    const keys = Object.keys(entry);
    if (keys.length !== 1) throw createRequestError(`operations[${index}] must define exactly one operation.`);
    const op = keys[0];
    const spec = entry[op];
    if (!isPlainObject(spec)) throw createRequestError(`operations[${index}].${op} must be an object.`);

    if (op === 'insertOne') {
      const document = parseObjectInputStrict(spec.document, `operations[${index}].insertOne.document`, { allowEmpty: false, mode: 'document' });
      return { insertOne: { document } };
    }

    if (op === 'updateOne' || op === 'updateMany') {
      const filter = parseObjectInputStrict(spec.filter, `operations[${index}].${op}.filter`, { allowEmpty: true, mode: 'filter' });
      const update = parseUpdateSpecStrict(spec.update, `operations[${index}].${op}.update`);
      const item = { filter, update };
      const upsert = parseBooleanInput(spec.upsert, `operations[${index}].${op}.upsert`);
      if (upsert !== undefined) item.upsert = upsert;
      const arrayFilters = parseArrayFiltersInputStrict(spec.arrayFilters, `operations[${index}].${op}.arrayFilters`);
      if (arrayFilters) item.arrayFilters = arrayFilters;
      const collation = parseCollationInputStrict(spec.collation, `operations[${index}].${op}.collation`);
      if (collation) item.collation = collation;
      const hint = parseHintInputStrict(spec.hint, `operations[${index}].${op}.hint`);
      if (hint !== undefined) item.hint = hint;
      return { [op]: item };
    }

    if (op === 'replaceOne') {
      const filter = parseObjectInputStrict(spec.filter, `operations[${index}].replaceOne.filter`, { allowEmpty: true, mode: 'filter' });
      const replacement = parseReplacementDocumentStrict(spec.replacement, `operations[${index}].replaceOne.replacement`);
      const item = { filter, replacement };
      const upsert = parseBooleanInput(spec.upsert, `operations[${index}].replaceOne.upsert`);
      if (upsert !== undefined) item.upsert = upsert;
      const collation = parseCollationInputStrict(spec.collation, `operations[${index}].replaceOne.collation`);
      if (collation) item.collation = collation;
      const hint = parseHintInputStrict(spec.hint, `operations[${index}].replaceOne.hint`);
      if (hint !== undefined) item.hint = hint;
      return { replaceOne: item };
    }

    if (op === 'deleteOne' || op === 'deleteMany') {
      const filter = parseObjectInputStrict(spec.filter, `operations[${index}].${op}.filter`, { allowEmpty: true, mode: 'filter' });
      const item = { filter };
      const collation = parseCollationInputStrict(spec.collation, `operations[${index}].${op}.collation`);
      if (collation) item.collation = collation;
      const hint = parseHintInputStrict(spec.hint, `operations[${index}].${op}.hint`);
      if (hint !== undefined) item.hint = hint;
      return { [op]: item };
    }

    throw createRequestError(`Unsupported bulk operation "${op}".`);
  });
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

function analyzeFields(obj, prefix, schema, limits = null, depth = 0) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return;
  const maxDepth = Number(limits?.maxDepth);
  const depthLimited = Number.isFinite(maxDepth) && maxDepth > 0 && depth >= maxDepth;
  for (const [key, val] of Object.entries(obj)) {
    const path = prefix ? `${prefix}.${key}` : key;
    let field = schema[path];
    if (!field) {
      if (limits && Number.isFinite(Number(limits.maxFields)) && Number(limits.maxFields) > 0) {
        if (!Number.isFinite(Number(limits.fieldCount))) limits.fieldCount = 0;
        if (limits.fieldCount >= Number(limits.maxFields)) {
          limits.truncated = true;
          continue;
        }
      }
      field = { types:{}, count:0, hasNull:false, sample:null };
      schema[path] = field;
      if (limits && Number.isFinite(Number(limits.maxFields)) && Number(limits.maxFields) > 0) {
        limits.fieldCount += 1;
      }
    }
    field.count++;
    const type = val===null?'null': Array.isArray(val)?'array': (val?.$oid)?'ObjectId': (val?.$date)?'Date': typeof val;
    field.types[type] = (field.types[type]||0) + 1;
    if (val===null) field.hasNull = true;
    if (!field.sample && val!==null && typeof val!=='object') field.sample = String(val).slice(0,100);
    if (val && typeof val==='object' && !Array.isArray(val) && !val.$oid && !val.$date) {
      if (depthLimited) {
        if (limits) limits.depthTruncated = true;
      } else {
        analyzeFields(val, path, schema, limits, depth + 1);
      }
    }
  }
}

function finalizeSchemaFields(schemaMap = {}, sampleSize = 0) {
  const safeSampleSize = Math.max(0, Number(sampleSize) || 0);
  return Object.entries(schemaMap).map(([path, field]) => ({
    path,
    count: field.count,
    pct: safeSampleSize > 0 ? Math.round((field.count / safeSampleSize) * 100) : 0,
    hasNull: field.hasNull,
    sample: field.sample,
    types: Object.entries(field.types)
      .sort((a, b) => b[1] - a[1])
      .map(([type, count]) => ({
        type,
        count,
        pct: safeSampleSize > 0 ? Math.round((count / safeSampleSize) * 100) : 0,
      })),
  })).sort((a, b) => b.count - a.count);
}

function walkExplainTree(root, visit) {
  if (!root || typeof root !== 'object') return;
  const stack = [root];
  const seen = new Set();
  while (stack.length > 0) {
    const node = stack.pop();
    if (!node || typeof node !== 'object') continue;
    if (seen.has(node)) continue;
    seen.add(node);
    visit(node);
    if (Array.isArray(node)) {
      for (let i = node.length - 1; i >= 0; i -= 1) stack.push(node[i]);
      continue;
    }
    for (const value of Object.values(node)) {
      if (value && typeof value === 'object') stack.push(value);
    }
  }
}

function toFiniteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function extractExplainSummary(explain) {
  const s = {
    executionTimeMs: null,
    totalDocsExamined: null,
    totalKeysExamined: null,
    nReturned: null,
    isCollScan: false,
    indexUsed: null,
    isCovered: false,
    stages: [],
  };
  try {
    const executionStatsCandidates = [];
    walkExplainTree(explain, (node) => {
      if (node.executionStats && typeof node.executionStats === 'object') {
        executionStatsCandidates.push(node.executionStats);
      }
      if (node.$cursor?.executionStats && typeof node.$cursor.executionStats === 'object') {
        executionStatsCandidates.push(node.$cursor.executionStats);
      }
    });
    const uniqueExecutionStats = [...new Set(executionStatsCandidates)];
    if (uniqueExecutionStats.length > 0) {
      let docsTotal = 0;
      let keysTotal = 0;
      let returnedTotal = 0;
      let execTimeMax = 0;
      let hasDocs = false;
      let hasKeys = false;
      let hasReturned = false;
      let hasTime = false;
      for (const stats of uniqueExecutionStats) {
        const docs = toFiniteNumber(stats?.totalDocsExamined);
        if (docs !== null) { docsTotal += docs; hasDocs = true; }
        const keys = toFiniteNumber(stats?.totalKeysExamined);
        if (keys !== null) { keysTotal += keys; hasKeys = true; }
        const returned = toFiniteNumber(stats?.nReturned);
        if (returned !== null) { returnedTotal += returned; hasReturned = true; }
        const timeMs = toFiniteNumber(stats?.executionTimeMillis);
        if (timeMs !== null) {
          execTimeMax = Math.max(execTimeMax, timeMs);
          hasTime = true;
        }
      }
      s.executionTimeMs = hasTime ? execTimeMax : null;
      s.totalDocsExamined = hasDocs ? docsTotal : null;
      s.totalKeysExamined = hasKeys ? keysTotal : null;
      s.nReturned = hasReturned ? returnedTotal : null;
    }

    const executionPlanRoots = [];
    const winningPlanRoots = [];
    walkExplainTree(explain, (node) => {
      const execStages = node.executionStats?.executionStages;
      if (execStages && typeof execStages === 'object') executionPlanRoots.push(execStages);
      const cursorExecStages = node.$cursor?.executionStats?.executionStages;
      if (cursorExecStages && typeof cursorExecStages === 'object') executionPlanRoots.push(cursorExecStages);
      const winningPlan = node.queryPlanner?.winningPlan;
      if (winningPlan && typeof winningPlan === 'object') winningPlanRoots.push(winningPlan);
      const cursorWinningPlan = node.$cursor?.queryPlanner?.winningPlan;
      if (cursorWinningPlan && typeof cursorWinningPlan === 'object') winningPlanRoots.push(cursorWinningPlan);
    });
    const planRoots = executionPlanRoots.length > 0 ? [...new Set(executionPlanRoots)] : [...new Set(winningPlanRoots)];
    for (const root of planRoots) {
      walkExplainTree(root, (planNode) => {
        const stage = typeof planNode?.stage === 'string' ? planNode.stage : '';
        if (stage === 'COLLSCAN') s.isCollScan = true;
        if (!s.indexUsed && stage === 'IXSCAN' && typeof planNode.indexName === 'string' && planNode.indexName.trim()) {
          s.indexUsed = planNode.indexName.trim();
        }
      });
      if (s.isCollScan && s.indexUsed) break;
    }
    if (!s.isCollScan) {
      const ps = JSON.stringify(explain);
      if (ps.includes('COLLSCAN')) s.isCollScan = true;
      if (!s.indexUsed) {
        const m = ps.match(/"indexName"\s*:\s*"([^"]+)"/);
        if (m) s.indexUsed = m[1];
      }
    }

    const docsExamined = toFiniteNumber(s.totalDocsExamined);
    const keysExamined = toFiniteNumber(s.totalKeysExamined);
    s.isCovered = !s.isCollScan
      && docsExamined !== null
      && keysExamined !== null
      && docsExamined === 0
      && keysExamined > 0;
    if (explain?.stages) s.stages = explain.stages.map(st => Object.keys(st)[0]).filter(Boolean);
  } catch {}
  return s;
}

// ═══ ROUTES ═══

function timeoutError(message = 'Operation exceeded time budget.') {
  const err = new Error(message);
  err.code = 50;
  return err;
}

function withTimeout(promise, ms = QUERY_TIMEOUT_DEFAULT_MS) {
  const timeoutMs = Math.max(1, Number(ms) || QUERY_TIMEOUT_DEFAULT_MS);
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(timeoutError());
    }, timeoutMs);

    Promise.resolve(promise).then(
      (value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve(value);
      },
      (err) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}

function escapeJsonStringChunk(value) {
  return JSON.stringify(String(value)).slice(1, -1);
}

async function writeChunk(res, chunk) {
  if (res.writableEnded || res.destroyed) return;
  const ok = res.write(chunk);
  if (ok) return;
  await new Promise((resolve) => {
    const done = () => {
      try { res.off('drain', onDrain); } catch {}
      try { res.off('close', onClose); } catch {}
      try { res.off('error', onError); } catch {}
      resolve();
    };
    const onDrain = () => done();
    const onClose = () => done();
    const onError = () => done();
    res.on('drain', onDrain);
    res.on('close', onClose);
    res.on('error', onError);
  });
}

const ZIP_LOCAL_FILE_HEADER_SIGNATURE = 0x04034b50;
const ZIP_CENTRAL_HEADER_SIGNATURE = 0x02014b50;
const ZIP_END_OF_CENTRAL_SIGNATURE = 0x06054b50;
const ZIP64_END_OF_CENTRAL_SIGNATURE = 0x06064b50;
const ZIP64_END_OF_CENTRAL_LOCATOR_SIGNATURE = 0x07064b50;
const ZIP_DATA_DESCRIPTOR_SIGNATURE = 0x08074b50;
const ZIP_VERSION_NEEDED = 45;
const ZIP_VERSION_MADE_BY = 45;
const ZIP_METHOD_STORE = 0;
const ZIP_FLAG_DATA_DESCRIPTOR = 0x08;
const ZIP_U32_MAX = 0xFFFFFFFF;
const ZIP_U16_MAX = 0xFFFF;
const ZIP_U64_MAX = 0xFFFFFFFFFFFFFFFFn;
const ZIP64_EXTRA_FIELD_ID = 0x0001;
const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i += 1) {
    let c = i;
    for (let k = 0; k < 8; k += 1) {
      c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[i] = c >>> 0;
  }
  return table;
})();

function crc32Update(seed, chunk) {
  let crc = seed >>> 0;
  for (let i = 0; i < chunk.length; i += 1) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ chunk[i]) & 0xFF];
  }
  return crc >>> 0;
}

function getZipDosDateTime(now = new Date()) {
  const year = Math.min(2107, Math.max(1980, now.getUTCFullYear()));
  const month = Math.min(12, Math.max(1, now.getUTCMonth() + 1));
  const day = Math.min(31, Math.max(1, now.getUTCDate()));
  const hours = Math.min(23, Math.max(0, now.getUTCHours()));
  const minutes = Math.min(59, Math.max(0, now.getUTCMinutes()));
  const seconds = Math.min(59, Math.max(0, now.getUTCSeconds()));
  const dosTime = ((hours & 0x1F) << 11) | ((minutes & 0x3F) << 5) | (Math.floor(seconds / 2) & 0x1F);
  const dosDate = (((year - 1980) & 0x7F) << 9) | ((month & 0x0F) << 5) | (day & 0x1F);
  return { dosDate, dosTime };
}

function assertZip64Range(value, field) {
  const v = BigInt(value);
  if (v < 0n || v > ZIP_U64_MAX) {
    throw new Error(`ZIP ${field} exceeds ZIP64 range.`);
  }
}

async function createZipSingleEntryWriter(res, entryName = 'export.json') {
  const zipName = safeFilename(entryName, 'export.json');
  const filename = Buffer.from(zipName, 'utf8');
  const { dosDate, dosTime } = getZipDosDateTime(new Date());
  let streamOffset = 0n;
  let localHeaderOffset = 0n;
  let dataSize = 0n;
  let crc = 0xFFFFFFFF;

  const writeBuffer = async (buffer) => {
    if (!buffer || buffer.length === 0) return;
    await writeChunk(res, buffer);
    streamOffset += BigInt(buffer.length);
  };

  const localZip64Extra = Buffer.alloc(4 + 16);
  localZip64Extra.writeUInt16LE(ZIP64_EXTRA_FIELD_ID, 0);
  localZip64Extra.writeUInt16LE(16, 2);
  localZip64Extra.writeBigUInt64LE(0n, 4);
  localZip64Extra.writeBigUInt64LE(0n, 12);

  const localHeader = Buffer.alloc(30 + filename.length + localZip64Extra.length);
  localHeader.writeUInt32LE(ZIP_LOCAL_FILE_HEADER_SIGNATURE, 0);
  localHeader.writeUInt16LE(ZIP_VERSION_NEEDED, 4);
  localHeader.writeUInt16LE(ZIP_FLAG_DATA_DESCRIPTOR, 6);
  localHeader.writeUInt16LE(ZIP_METHOD_STORE, 8);
  localHeader.writeUInt16LE(dosTime, 10);
  localHeader.writeUInt16LE(dosDate, 12);
  localHeader.writeUInt32LE(0, 14);
  localHeader.writeUInt32LE(ZIP_U32_MAX, 18);
  localHeader.writeUInt32LE(ZIP_U32_MAX, 22);
  localHeader.writeUInt16LE(filename.length, 26);
  localHeader.writeUInt16LE(localZip64Extra.length, 28);
  filename.copy(localHeader, 30);
  localZip64Extra.copy(localHeader, 30 + filename.length);
  localHeaderOffset = streamOffset;
  await writeBuffer(localHeader);

  return {
    async write(chunk) {
      if (chunk === undefined || chunk === null) return;
      const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk), 'utf8');
      if (data.length === 0) return;
      dataSize += BigInt(data.length);
      assertZip64Range(dataSize, 'entry size');
      crc = crc32Update(crc, data);
      await writeBuffer(data);
    },
    async end() {
      const crcFinal = (crc ^ 0xFFFFFFFF) >>> 0;
      assertZip64Range(dataSize, 'entry size');
      assertZip64Range(localHeaderOffset, 'offset');
      const dataDescriptor = Buffer.alloc(24);
      dataDescriptor.writeUInt32LE(ZIP_DATA_DESCRIPTOR_SIGNATURE, 0);
      dataDescriptor.writeUInt32LE(crcFinal, 4);
      dataDescriptor.writeBigUInt64LE(dataSize, 8);
      dataDescriptor.writeBigUInt64LE(dataSize, 16);
      await writeBuffer(dataDescriptor);

      const centralOffset = streamOffset;
      const centralZip64Extra = Buffer.alloc(4 + 24);
      centralZip64Extra.writeUInt16LE(ZIP64_EXTRA_FIELD_ID, 0);
      centralZip64Extra.writeUInt16LE(24, 2);
      centralZip64Extra.writeBigUInt64LE(dataSize, 4);
      centralZip64Extra.writeBigUInt64LE(dataSize, 12);
      centralZip64Extra.writeBigUInt64LE(localHeaderOffset, 20);

      const centralHeader = Buffer.alloc(46 + filename.length + centralZip64Extra.length);
      centralHeader.writeUInt32LE(ZIP_CENTRAL_HEADER_SIGNATURE, 0);
      centralHeader.writeUInt16LE(ZIP_VERSION_MADE_BY, 4);
      centralHeader.writeUInt16LE(ZIP_VERSION_NEEDED, 6);
      centralHeader.writeUInt16LE(ZIP_FLAG_DATA_DESCRIPTOR, 8);
      centralHeader.writeUInt16LE(ZIP_METHOD_STORE, 10);
      centralHeader.writeUInt16LE(dosTime, 12);
      centralHeader.writeUInt16LE(dosDate, 14);
      centralHeader.writeUInt32LE(crcFinal, 16);
      centralHeader.writeUInt32LE(ZIP_U32_MAX, 20);
      centralHeader.writeUInt32LE(ZIP_U32_MAX, 24);
      centralHeader.writeUInt16LE(filename.length, 28);
      centralHeader.writeUInt16LE(centralZip64Extra.length, 30);
      centralHeader.writeUInt16LE(0, 32);
      centralHeader.writeUInt16LE(0, 34);
      centralHeader.writeUInt16LE(0, 36);
      centralHeader.writeUInt32LE(0, 38);
      centralHeader.writeUInt32LE(ZIP_U32_MAX, 42);
      filename.copy(centralHeader, 46);
      centralZip64Extra.copy(centralHeader, 46 + filename.length);
      await writeBuffer(centralHeader);

      const centralSize = streamOffset - centralOffset;
      assertZip64Range(centralOffset, 'central directory offset');
      assertZip64Range(centralSize, 'central directory size');

      const zip64EndOffset = streamOffset;
      const zip64EndOfCentral = Buffer.alloc(56);
      zip64EndOfCentral.writeUInt32LE(ZIP64_END_OF_CENTRAL_SIGNATURE, 0);
      zip64EndOfCentral.writeBigUInt64LE(44n, 4);
      zip64EndOfCentral.writeUInt16LE(ZIP_VERSION_MADE_BY, 12);
      zip64EndOfCentral.writeUInt16LE(ZIP_VERSION_NEEDED, 14);
      zip64EndOfCentral.writeUInt32LE(0, 16);
      zip64EndOfCentral.writeUInt32LE(0, 20);
      zip64EndOfCentral.writeBigUInt64LE(1n, 24);
      zip64EndOfCentral.writeBigUInt64LE(1n, 32);
      zip64EndOfCentral.writeBigUInt64LE(centralSize, 40);
      zip64EndOfCentral.writeBigUInt64LE(centralOffset, 48);
      await writeBuffer(zip64EndOfCentral);

      const zip64Locator = Buffer.alloc(20);
      zip64Locator.writeUInt32LE(ZIP64_END_OF_CENTRAL_LOCATOR_SIGNATURE, 0);
      zip64Locator.writeUInt32LE(0, 4);
      zip64Locator.writeBigUInt64LE(zip64EndOffset, 8);
      zip64Locator.writeUInt32LE(1, 16);
      await writeBuffer(zip64Locator);

      const endOfCentral = Buffer.alloc(22);
      endOfCentral.writeUInt32LE(ZIP_END_OF_CENTRAL_SIGNATURE, 0);
      endOfCentral.writeUInt16LE(0, 4);
      endOfCentral.writeUInt16LE(0, 6);
      endOfCentral.writeUInt16LE(ZIP_U16_MAX, 8);
      endOfCentral.writeUInt16LE(ZIP_U16_MAX, 10);
      endOfCentral.writeUInt32LE(ZIP_U32_MAX, 12);
      endOfCentral.writeUInt32LE(ZIP_U32_MAX, 16);
      endOfCentral.writeUInt16LE(0, 20);
      await writeBuffer(endOfCentral);
    },
    filename: zipName,
  };
}

function csvCell(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
  const text = String(value);
  return (text.includes(',') || text.includes('"') || text.includes('\n'))
    ? `"${text.replace(/"/g, '""')}"`
    : text;
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

function getMetadataCacheConfig() {
  return serviceConfig.metadataCache || DEFAULT_SERVICE_CONFIG.metadataCache;
}

function getDbStatsCacheKey(dbName) {
  return `dbstats:${dbName}`;
}

function getCollectionStatsCacheKey(dbName, colName) {
  return `colstats:${dbName}.${colName}`;
}

function getIndexesCacheKey(dbName, colName) {
  return `indexes:${dbName}.${colName}`;
}

function getSchemaQuickCacheKey(dbName, colName, sample) {
  return `schema:quick:${dbName}.${colName}:${sample}`;
}

function getApproxTotalCacheKey(dbName, colName, filter, projection) {
  return `total:approx:${dbName}.${colName}:${hashQueryParts([canonicalQueryShape(filter), canonicalQueryShape(projection)])}`;
}

function getExactTotalCacheKey(dbName, colName, filter, projection, hint) {
  return `total:exact:${dbName}.${colName}:${hashQueryParts([canonicalQueryShape(filter), canonicalQueryShape(projection), String(hint || 'auto')])}`;
}

async function listDatabasesSafe(conn, options = {}) {
  const meta = buildCommandMeta(options);
  try {
    const r = await conn.client.db('admin').command({ listDatabases: 1, nameOnly: false, authorizedDatabases: true, ...meta });
    return { databases: (r.databases || []).map((d) => ({ name: d.name, sizeOnDisk: d.sizeOnDisk, empty: d.empty })), totalSize: r.totalSize, warning: '' };
  } catch (err) {
    try {
      const r = await conn.client.db('admin').command({ listDatabases: 1, nameOnly: false, ...meta });
      return { databases: (r.databases || []).map((d) => ({ name: d.name, sizeOnDisk: d.sizeOnDisk, empty: d.empty })), totalSize: r.totalSize, warning: '' };
    } catch {
      const n = new URL(conn.uri).pathname.slice(1);
      if (n) return { databases: [{ name: n, sizeOnDisk: 0, empty: false }], totalSize: 0, warning: 'Limited permissions.' };
      throw err;
    }
  }
}

async function normalizeDbStatsVisible(db, caps, stats, options = {}) {
  const normalized = { ...(stats || {}) };
  const initExists = await db.listCollections({ name: DB_INIT_COLLECTION }, { nameOnly: true }).toArray();
  if (initExists.length === 0) return normalized;
  try {
    const initStats = await compatCollStats(db, DB_INIT_COLLECTION, caps, options);
    normalized.collections = Math.max(0, Number(normalized.collections || 0) - 1);
    normalized.objects = Math.max(0, Number(normalized.objects || 0) - Number(initStats.count || 0));
    normalized.dataSize = Math.max(0, Number(normalized.dataSize || 0) - Number(initStats.size || 0));
    normalized.storageSize = Math.max(0, Number(normalized.storageSize || 0) - Number(initStats.storageSize || initStats.size || 0));
    normalized.indexes = Math.max(0, Number(normalized.indexes || 0) - Number(initStats.nindexes || 0));
    normalized.indexSize = Math.max(0, Number(normalized.indexSize || 0) - Number(initStats.totalIndexSize || 0));
    normalized.avgObjSize = normalized.objects > 0 ? Math.round(normalized.dataSize / normalized.objects) : 0;
  } catch {
    normalized.collections = Math.max(0, Number(normalized.collections || 0) - 1);
  }
  return normalized;
}

async function getDbStatsCached(req, dbName, { forceRefresh = false, timeoutMs = QUERY_TIMEOUT_DEFAULT_MS } = {}) {
  const cfg = getMetadataCacheConfig();
  const cacheKey = getDbStatsCacheKey(dbName);
  if (!forceRefresh) {
    const cached = sessionMetadataCache.get(req.connId, cacheKey);
    if (cached) {
      return { stats: cached.value, source: cached.source || 'cache', ts: cached.ts, fresh: true };
    }
  }
  const comment = req?.createOpComment?.(`dbstats:${dbName}`);
  const stats = await runWithGovernor(req, 'metadata', async () => {
    const db = req.conn.client.db(dbName);
    const raw = await withTimeout(compatDbStats(db, req.caps, { comment, maxTimeMS: timeoutMs }), timeoutMs);
    return normalizeDbStatsVisible(db, req.caps, raw, { comment, maxTimeMS: timeoutMs });
  });
  sessionMetadataCache.set(req.connId, cacheKey, stats, cfg.ttlDbStatsMs, 'live');
  return { stats, source: 'live', ts: Date.now(), fresh: true };
}

async function getCollectionStatsCached(req, dbName, colName, { forceRefresh = false, timeoutMs = QUERY_TIMEOUT_DEFAULT_MS } = {}) {
  const cfg = getMetadataCacheConfig();
  const cacheKey = getCollectionStatsCacheKey(dbName, colName);
  if (!forceRefresh) {
    const cached = sessionMetadataCache.get(req.connId, cacheKey);
    if (cached) return { stats: cached.value, source: cached.source || 'cache', ts: cached.ts, fresh: true };
  }
  const comment = req?.createOpComment?.(`collstats:${dbName}.${colName}`);
  const stats = await runWithGovernor(
    req,
    'metadata',
    async () => withTimeout(compatCollStats(req.conn.client.db(dbName), colName, req.caps, { comment, maxTimeMS: timeoutMs }), timeoutMs),
  );
  sessionMetadataCache.set(req.connId, cacheKey, stats, cfg.ttlCollectionStatsMs, 'live');
  return { stats, source: 'live', ts: Date.now(), fresh: true };
}

function invalidateCollectionCaches(req, dbName, colName) {
  if (!req?.connId) return;
  sessionMetadataCache.invalidateByPrefix(req.connId, `colstats:${dbName}.${colName}`);
  sessionMetadataCache.invalidateByPrefix(req.connId, `indexes:${dbName}.${colName}`);
  sessionMetadataCache.invalidateByPrefix(req.connId, `schema:quick:${dbName}.${colName}`);
  sessionMetadataCache.invalidateByPrefix(req.connId, `total:approx:${dbName}.${colName}`);
  sessionMetadataCache.invalidateByPrefix(req.connId, `total:exact:${dbName}.${colName}`);
  sessionMetadataCache.invalidateByPrefix(req.connId, `dbstats:${dbName}`);
}

function invalidateDatabaseCaches(req, dbName) {
  if (!req?.connId) return;
  sessionMetadataCache.invalidateByPrefix(req.connId, `dbstats:${dbName}`);
  sessionMetadataCache.invalidateByPrefix(req.connId, `colstats:${dbName}.`);
  sessionMetadataCache.invalidateByPrefix(req.connId, `indexes:${dbName}.`);
  sessionMetadataCache.invalidateByPrefix(req.connId, `schema:quick:${dbName}.`);
  sessionMetadataCache.invalidateByPrefix(req.connId, `total:approx:${dbName}.`);
  sessionMetadataCache.invalidateByPrefix(req.connId, `total:exact:${dbName}.`);
}

app.get('/api/health', (_, res) => res.json({
  status: 'ok',
  ready: true,
  uptime: process.uptime(),
  ts: new Date().toISOString(),
}));
app.get('/api/ready', (_, res) => res.json({ ready:true, connections:connections.size }));
app.get('/api/metrics', (_, res) => res.json({
  uptime: process.uptime(),
  connections: connections.size,
  max: MAX_CONNECTIONS,
  memMB: Math.round(process.memoryUsage().rss / 1048576),
  audit: auditLog.length,
  running: runningQueries.size,
  mongoshSessions: mongoshSessions.size,
}));

app.get('/api/connection-info', getConnection, async (req, res) => {
  try {
    const dbContext = deriveConnectionDbContext(req.conn);
    res.json({
      ok: true,
      connectionId: req.connId,
      host: req.conn?.host || null,
      username: req.conn?.username || req.conn?.authUser || null,
      version: req.conn?.versionStr || null,
      isProduction: req.conn?.isProduction === true,
      topology: req.conn?.topology || null,
      routing: req.conn?.routing || null,
      readPreference: req.conn?.routing?.readPreference || null,
      readPreferenceExplicit: req.conn?.routing?.readPreferenceExplicit === true,
      directConnection: req.conn?.routing?.directConnection === true,
      directConnectionExplicit: req.conn?.routing?.directConnectionExplicit === true,
      authSource: dbContext.authSource,
      defaultDb: dbContext.defaultDb,
    });
  } catch (err) {
    sendError(res, err, 'Failed to load connection info');
  }
});

app.post('/api/connect', async (req, res) => {
  const { uri, options={} } = req.body;
  if (!uri) return res.status(400).json({ error:'URI required', errorType:'validation' });
  if (connections.size >= MAX_CONNECTIONS) return res.status(429).json({ error:`Max connections (${MAX_CONNECTIONS}) reached.`, errorType:'limit' });
  let client = null;
  let sshTunnel = null;
  let storedConnection = false;
  try {
    const markAsProduction = options.markAsProduction === true;
    const connectTimeoutMS = Math.max(1000, Math.min(parseInt(options.connectTimeoutMS) || 15000, 120000));
    const authUsername = typeof options.username === 'string' ? options.username.trim() : '';
    const hasAuth = Boolean(authUsername);

    // Build MongoClient options
    const opts = {
      connectTimeoutMS, serverSelectionTimeoutMS: connectTimeoutMS, socketTimeoutMS: Math.max(connectTimeoutMS * 2, 30000),
      maxPoolSize: 10, minPoolSize: 1,
    };

    // TLS options
    if (options.tls !== undefined) opts.tls = options.tls;
    if (options.tlsAllowInvalidCertificates) opts.tlsAllowInvalidCertificates = true;
    if (options.tlsAllowInvalidHostnames) opts.tlsAllowInvalidHostnames = true;
    if (options.tlsInsecure) opts.tlsInsecure = true;
    if (options.tlsCAFileContent) {
      opts.ca = options.tlsCAFileContent;
    } else if (options.tlsCAFile) {
      opts.tlsCAFile = options.tlsCAFile;
    }
    if (options.tlsCertKeyFileContent) {
      opts.cert = options.tlsCertKeyFileContent;
      opts.key = options.tlsCertKeyFileContent;
    } else if (options.tlsCertificateKeyFile) {
      opts.tlsCertificateKeyFile = options.tlsCertificateKeyFile;
    }
    if (options.tlsCertificateKeyFilePassword) opts.tlsCertificateKeyFilePassword = options.tlsCertificateKeyFilePassword;

    // Auth options
    if (options.authSource) opts.authSource = options.authSource;
    if (options.authMechanism) opts.authMechanism = options.authMechanism;
    if (options.replicaSet) opts.replicaSet = options.replicaSet;
    if (options.directConnection !== undefined) opts.directConnection = options.directConnection;
    if (options.readPreference) opts.readPreference = options.readPreference;

    // Kerberos-specific
    if (options.authMechanism === 'GSSAPI' && options.kerberosServiceName) {
      opts.authMechanismProperties = { ...(opts.authMechanismProperties || {}), SERVICE_NAME: options.kerberosServiceName };
      if (options.kerberosCanonicalizeHostname) {
        opts.authMechanismProperties.CANONICALIZE_HOST_NAME = true;
      }
    }

    // AWS-specific session token
    if (options.authMechanism === 'MONGODB-AWS' && options.awsSessionToken) {
      opts.authMechanismProperties = { ...(opts.authMechanismProperties || {}), AWS_SESSION_TOKEN: options.awsSessionToken };
    }

    // OIDC-specific
    if (options.authMechanism === 'MONGODB-OIDC') {
      if (options.oidcUsername) {
        opts.auth = { ...(opts.auth || {}), username: options.oidcUsername };
      }
      if (options.oidcRedirectUri) {
        opts.authMechanismProperties = { ...(opts.authMechanismProperties || {}), REDIRECT_URI: options.oidcRedirectUri };
      }
      if (options.oidcTrustedEndpoint) {
        opts.authMechanismProperties = { ...(opts.authMechanismProperties || {}), ENVIRONMENT: 'trusted' };
      }
    }

    // Credentials
    if (hasAuth) {
      opts.auth = { username: authUsername, password: String(options.password ?? '') };
    }

    // Pass-through URI options (pool, compression, write concern, read concern, server, misc)
    const passThrough = [
      'socketTimeoutMS', 'compressors', 'zlibCompressionLevel',
      'maxPoolSize', 'minPoolSize', 'maxIdleTimeMS', 'waitQueueTimeoutMS',
      'w', 'wtimeoutMS', 'journal', 'readConcernLevel',
      'maxStalenessSeconds', 'readPreferenceTags',
      'localThresholdMS', 'serverSelectionTimeoutMS', 'heartbeatFrequencyMS',
      'appName', 'retryReads', 'retryWrites', 'srvMaxHosts', 'uuidRepresentation',
    ];
    for (const key of passThrough) {
      if (options[key] !== undefined && options[key] !== '' && options[key] !== null) {
        let val = options[key];
        // Convert string booleans
        if (val === 'true') val = true;
        else if (val === 'false') val = false;
        // Convert numeric strings
        else if (typeof val === 'string' && /^\d+$/.test(val)) val = parseInt(val, 10);
        opts[key] = val;
      }
    }

    // Handle read concern level → readConcern object
    if (opts.readConcernLevel) {
      opts.readConcern = { level: opts.readConcernLevel };
      delete opts.readConcernLevel;
    }

    // Handle write concern: w, wtimeoutMS, journal → writeConcern object
    if (opts.w !== undefined || opts.wtimeoutMS !== undefined || opts.journal !== undefined) {
      opts.writeConcern = {};
      if (opts.w !== undefined) { opts.writeConcern.w = opts.w; delete opts.w; }
      if (opts.wtimeoutMS !== undefined) { opts.writeConcern.wtimeout = opts.wtimeoutMS; delete opts.wtimeoutMS; }
      if (opts.journal !== undefined) { opts.writeConcern.j = opts.journal; delete opts.journal; }
    }

    // SSH tunnel support
    let effectiveUri = uri;
    if (options.sshTunnel && options.sshTunnel.host) {
      try {
        const { Client: SSHClient } = await import('ssh2');
        const net = await import('net');
        const sshConfig = {
          host: options.sshTunnel.host,
          port: parseInt(options.sshTunnel.port) || 22,
          username: options.sshTunnel.username || 'root',
        };
        if (options.sshTunnel.password) sshConfig.password = options.sshTunnel.password;
        if (options.sshTunnel.identityFile) {
          const fs = await import('fs');
          sshConfig.privateKey = fs.readFileSync(options.sshTunnel.identityFile);
          if (options.sshTunnel.passphrase) sshConfig.passphrase = options.sshTunnel.passphrase;
        }

        // Parse target host from URI
        const hostMatch = uri.match(/^mongodb(?:\+srv)?:\/\/(?:[^@]+@)?([^:/?]+)(?::(\d+))?/);
        const targetHost = hostMatch ? hostMatch[1] : 'localhost';
        const targetPort = hostMatch && hostMatch[2] ? parseInt(hostMatch[2]) : 27017;

        // Create SSH tunnel
        sshTunnel = await new Promise((resolve, reject) => {
          const ssh = new SSHClient();
          ssh.on('ready', () => {
            const server = net.createServer((sock) => {
              ssh.forwardOut('127.0.0.1', 0, targetHost, targetPort, (err, stream) => {
                if (err) { sock.destroy(); return; }
                sock.pipe(stream).pipe(sock);
              });
            });
            server.listen(0, '127.0.0.1', () => {
              const localPort = server.address().port;
              resolve({ ssh, server, localPort });
            });
          });
          ssh.on('error', reject);
          ssh.connect(sshConfig);
        });

        // Rewrite URI to go through local tunnel
        effectiveUri = uri.replace(
          /^(mongodb(?:\+srv)?:\/\/(?:[^@]+@)?)([^/?]+)/,
          `$1127.0.0.1:${sshTunnel.localPort}`
        );
        // Force non-SRV for tunneled connections
        if (effectiveUri.startsWith('mongodb+srv://')) {
          effectiveUri = effectiveUri.replace('mongodb+srv://', 'mongodb://');
        }
      } catch (sshErr) {
        const msg = sshErr?.code === 'MODULE_NOT_FOUND'
          ? 'SSH tunnel requires the ssh2 package. Install it with: npm install ssh2'
          : `SSH tunnel failed: ${sshErr?.message || sshErr}`;
        return res.status(400).json({ error: msg, errorType: 'ssh_tunnel' });
      }
    }

    // Socks5 proxy support
    if (options.proxyHost) {
      opts.proxyHost = options.proxyHost;
      opts.proxyPort = parseInt(options.proxyPort) || 1080;
      if (options.proxyUsername) opts.proxyUsername = options.proxyUsername;
      if (options.proxyPassword) opts.proxyPassword = options.proxyPassword;
    }

    client = new MongoClient(effectiveUri, opts);
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

    const connId = randomUUID().replace(/-/g,'');
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
    // Track whether readPreference was explicitly provided by user (options or URI).
    const optionReadPref = typeof options.readPreference === 'string' ? options.readPreference.trim() : '';
    const hasOptionDirectConnection = typeof options.directConnection === 'boolean';
    const optionDirectConnection = hasOptionDirectConnection ? options.directConnection === true : null;
    let uriReadPref = '';
    let uriDirectConnection = null;
    let uriAuthSource = '';
    let uriDefaultDb = '';
    try {
      const m = uri.match(/[?&]readPreference=([^&]+)/i);
      if (m) uriReadPref = decodeURIComponent(m[1] || '').trim();
      const directMatch = uri.match(/[?&]directConnection=([^&]+)/i);
      if (directMatch) {
        const value = String(decodeURIComponent(directMatch[1] || '')).trim().toLowerCase();
        if (value === '1' || value === 'true' || value === 'yes') uriDirectConnection = true;
        else if (value === '0' || value === 'false' || value === 'no') uriDirectConnection = false;
      }
      const authMatch = uri.match(/[?&]authSource=([^&]+)/i);
      if (authMatch) uriAuthSource = decodeURIComponent(authMatch[1] || '').trim();
      const dbMatch = uri.match(/^mongodb(?:\+srv)?:\/\/(?:[^@]+@)?[^/?]+\/([^?]*)/i);
      if (dbMatch) uriDefaultDb = decodeURIComponent(dbMatch[1] || '').trim();
    } catch {}
    const optionAuthSource = typeof options.authSource === 'string' ? options.authSource.trim() : '';
    const effectiveAuthSource = optionAuthSource || uriAuthSource || '';
    const effectiveDefaultDb = uriDefaultDb || effectiveAuthSource || '';
    const effectiveReadPref = optionReadPref || uriReadPref || null;
    const effectiveDirectConnection = optionDirectConnection !== null ? optionDirectConnection : (uriDirectConnection === true);
    const readPreferenceExplicit = Boolean(optionReadPref || uriReadPref);
    const directConnectionExplicit = optionDirectConnection !== null || uriDirectConnection !== null;

    connections.set(connId, {
      client,
      uri,
      host,
      username,
      authUser: username,
      lastUiUser: username,
      connectedAt: Date.now(),
      lastActivity: Date.now(),
      version,
      versionStr,
      capabilities,
      defaultDb: effectiveDefaultDb || null,
      isProduction,
      topology,
      sshTunnel: sshTunnel || null,
      connectOptions: {
        username: authUsername || username || '',
        password: hasAuth ? String(options.password ?? '') : '',
        authSource: effectiveAuthSource,
        authMechanism: typeof options.authMechanism === 'string' ? options.authMechanism : '',
        tls: options.tls === true,
        tlsAllowInvalidCertificates: options.tlsAllowInvalidCertificates === true,
        tlsAllowInvalidHostnames: options.tlsAllowInvalidHostnames === true,
        replicaSet: typeof options.replicaSet === 'string' ? options.replicaSet : '',
        directConnection: options.directConnection === true,
        readPreference: typeof options.readPreference === 'string' ? options.readPreference : '',
      },
      routing: {
        readPreference: effectiveReadPref || (client?.readPreference?.mode || 'primary'),
        readPreferenceExplicit,
        directConnection: effectiveDirectConnection,
        directConnectionExplicit,
      },
    });
    storedConnection = true;
    executionConfigs.set(connId, { ...DEFAULT_EXEC_CONFIG });
    audit(connId, 'connect', { host, version:versionStr });

    res.json({
      connectionId:connId, host, username:username||null, version:versionStr, isProduction, warnings, topology,
      authSource: effectiveAuthSource || null,
      defaultDb: effectiveDefaultDb || null,
      readPreference: effectiveReadPref, readPreferenceExplicit,
      directConnection: effectiveDirectConnection, directConnectionExplicit, ok:true,
      capabilities:{ countDocuments:capabilities.hasCountDocuments, estimatedCount:capabilities.hasEstimatedCount, changeStreams:capabilities.hasChangeStreams, transactions:capabilities.hasTransactions, shardedTransactions:capabilities.hasShardedTransactions, aggregationFacet:capabilities.hasAggFacet, aggregationLookup:capabilities.hasAggLookup, wildcardIndexes:capabilities.hasWildcardIndexes, setWindowFields:capabilities.hasSetWindowFields, timeSeries:capabilities.hasTimeSeries, stableApi:capabilities.hasStableApi, columnstoreIndexes:capabilities.hasColumnstoreIndexes, compoundWildcard:capabilities.hasCompoundWildcard, queryableEncryption:capabilities.hasQueryableEncryption },
    });
  } catch (err) {
    if (client && !storedConnection) client.close().catch(() => {});
    // Clean up SSH tunnel on failure
    if (sshTunnel) {
      try { sshTunnel.server?.close?.(); } catch {}
      try { sshTunnel.ssh?.end?.(); } catch {}
    }
    sendError(res, err, 'Connection failed');
  }
});

app.post('/api/disconnect', async (req, res) => {
  const id = req.headers['x-connection-id'];
  let closedCursors = 0;
  let killedOps = { matched: 0, killed: 0, failed: 0 };
  let closedMongoshSessions = 0;
  if (id && connections.has(id)) {
    const conn = connections.get(id);
    audit(id, 'disconnect');
    closedMongoshSessions = await terminateMongoshSessionsForConnection(id, 'disconnect').catch(() => 0);
    closedCursors = await closeActiveCursorsForConnection(id).catch(() => 0);
    killedOps = await killTrackedOperationsForConnection(id, conn?.client).catch(() => ({ matched: 0, killed: 0, failed: 0 }));
    conn?.client?.close?.().catch(() => {});
    // Close SSH tunnel if present
    if (conn?.sshTunnel) {
      try { conn.sshTunnel.server?.close?.(); } catch {}
      try { conn.sshTunnel.ssh?.end?.(); } catch {}
    }
    connections.delete(id);
    executionConfigs.delete(id);
    opCountersByConn.delete(id);
    clearTrackedOperationComments(id);
    sessionMetadataCache.clearConn(id);
    for (const [qid, queryState] of runningQueries) {
      if (queryState?.connId === id) runningQueries.delete(qid);
    }
    const connJobs = jobsByConn.get(id);
    if (connJobs) {
      for (const jobId of connJobs) jobs.delete(jobId);
      jobsByConn.delete(id);
    }
  }
  res.json({ ok:true, closedCursors, killedOps, closedMongoshSessions });
});

app.get('/api/execution-config', getConnection, (req, res) => res.json(req.execConfig));
app.put('/api/execution-config', getConnection, (req, res) => {
  const { mode, maxTimeMS, maxResultSize, allowDiskUse } = req.body;
  const c = executionConfigs.get(req.connId) || { ...DEFAULT_EXEC_CONFIG };
  if (mode==='safe'||mode==='power') c.mode=mode;
  const limitMaxForMode = getModeQueryLimitMax(c.mode || 'safe');
  const timeoutMaxForMode = getModeQueryTimeoutMax(c.mode || 'safe');
  if (typeof maxTimeMS === 'number') c.maxTimeMS = clampNumber(maxTimeMS, QUERY_TIMEOUT_MIN_MS, timeoutMaxForMode);
  if (typeof maxResultSize === 'number') c.maxResultSize = clampNumber(maxResultSize, QUERY_LIMIT_MIN, limitMaxForMode);
  if (typeof allowDiskUse==='boolean') c.allowDiskUse=allowDiskUse;
  if (c.mode==='safe') {
    c.blockedOperators=['$where'];
    c.allowDiskUse=false;
    c.maxTimeMS = clampNumber(c.maxTimeMS, QUERY_TIMEOUT_MIN_MS, SAFE_QUERY_TIMEOUT_MAX_MS);
    c.maxResultSize = clampNumber(c.maxResultSize, QUERY_LIMIT_MIN, QUERY_LIMIT_OVERRIDE_MAX);
  } else if (c.mode==='power') {
    c.blockedOperators=[];
    c.maxTimeMS = clampNumber(c.maxTimeMS, QUERY_TIMEOUT_MIN_MS, POWER_QUERY_TIMEOUT_MAX_MS);
    c.maxResultSize = clampNumber(c.maxResultSize, QUERY_LIMIT_MIN, POWER_QUERY_LIMIT_MAX);
  }
  executionConfigs.set(req.connId, c);
  auditReq(req, 'config_change', { mode:c.mode });
  res.json(c);
});

app.get('/api/service-config', getConnection, (req, res) => {
  res.json(serviceConfig);
});

app.put('/api/service-config', getConnection, (req, res) => {
  if (req.adminKeyRequired && !req.adminVerified) {
    return res.status(403).json({
      error: 'Admin access key is required to change service config.',
      errorType: 'admin_required',
    });
  }
  if (req.execConfig?.mode !== 'power') {
    return res.status(403).json({
      error: 'Power mode is required to change service config.',
      errorType: 'forbidden',
    });
  }
  const incoming = req.body && typeof req.body === 'object' ? req.body : {};
  const next = normalizeServiceConfig({ ...serviceConfig, ...incoming });
  serviceConfig.rateLimit = next.rateLimit;
  serviceConfig.governor = next.governor;
  serviceConfig.metadataCache = next.metadataCache;
  persistServiceConfig(serviceConfig);
  auditReq(req, 'service_config_change', {
    rateWindowMs: serviceConfig.rateLimit.windowMs,
    rateApiMax: serviceConfig.rateLimit.apiMax,
    rateHeavyMax: serviceConfig.rateLimit.heavyMax,
    govInteractive: serviceConfig.governor.interactivePerConnection,
    govMetadata: serviceConfig.governor.metadataPerConnection,
    govHeavy: serviceConfig.governor.heavyPerConnection,
    govHeavyGlobal: serviceConfig.governor.heavyGlobal,
  });
  res.json(serviceConfig);
});

app.get('/api/admin-access/status', (req, res) => {
  res.json({ configured: Boolean(ADMIN_ACCESS_KEY) });
});

app.get('/api/admin-access', getConnection, (req, res) => {
  res.json({ required: req.adminKeyRequired, verified: req.adminVerified });
});

app.post('/api/admin-access/verify', getConnection, (req, res) => {
  if (!ADMIN_ACCESS_KEY) {
    return res.json({ required: false, verified: true });
  }
  const key = typeof req.body?.key === 'string' ? req.body.key.trim() : '';
  if (!key || key !== ADMIN_ACCESS_KEY) {
    auditReq(req, 'admin_access_denied', {});
    return res.status(403).json({ error: 'Invalid admin access key.', errorType: 'forbidden', verified: false });
  }
  adminVerifiedConnections.add(req.connId);
  auditReq(req, 'admin_access_granted', {});
  res.json({ required: true, verified: true });
});

app.post('/api/admin-access/revoke', getConnection, (req, res) => {
  adminVerifiedConnections.delete(req.connId);
  auditReq(req, 'admin_access_revoked', {});
  res.json({ required: req.adminKeyRequired, verified: false });
});

app.get('/api/status', getConnection, async (req, res) => {
  try {
    const forceShellRefresh = String(req.query?.refresh || req.query?.forced || '').trim() === '1';
    const r = await runWithGovernor(req, 'metadata', async () => {
      const admin = req.conn.client.db('admin');
      const status = {
        version:req.conn.versionStr,
        capabilities:req.conn.capabilities,
        isProduction:req.conn.isProduction,
        topology:req.conn.topology || null,
        routing: req.conn.routing || {
          readPreference: req.conn?.client?.readPreference?.mode || 'primary',
          readPreferenceExplicit: false,
          directConnection: false,
          directConnectionExplicit: false,
        },
      };
      try { status.buildInfo = await withTimeout(admin.command({ buildInfo: 1 }), req.queryBudget.timeoutMs); } catch {}
      try {
        const ss = await withTimeout(admin.command({ serverStatus: 1 }), req.queryBudget.timeoutMs);
        status.serverStatus = { host:ss.host, uptime:ss.uptime, connections:ss.connections, opcounters:ss.opcounters, mem:ss.mem, storageEngine:ss.storageEngine, repl:ss.repl?{setName:ss.repl.setName,hosts:ss.repl.hosts,primary:ss.repl.primary}:null };
      } catch {}
      try { status.hello = await withTimeout(admin.command({ hello: 1 }), req.queryBudget.timeoutMs); } catch { try { status.hello = await withTimeout(admin.command({ isMaster: 1 }), req.queryBudget.timeoutMs); } catch {} }
      if (status.hello) status.topology = getTopologyInfo(status.hello);
      status.consoleShells = getConsoleShellRuntimeStatus({ force: forceShellRefresh });
      status.adminAccess = { required: req.adminKeyRequired, verified: req.adminVerified };
      return status;
    });
    res.json(r);
  } catch (err) { sendError(res, err); }
});

app.get('/api/databases', getConnection, async (req, res) => {
  try {
    const comment = req.createOpComment?.('list-databases');
    const listed = await runWithGovernor(
      req,
      'metadata',
      async () => withTimeout(listDatabasesSafe(req.conn, { comment, maxTimeMS: req.queryBudget.timeoutMs }), req.queryBudget.timeoutMs),
    );
    res.json({ databases: listed.databases || [], totalSize: listed.totalSize || 0, version: req.conn.versionStr, warning: listed.warning || '' });
  } catch(err) {
    sendError(res, err);
  }
});

app.get('/api/metadata/overview', getConnection, async (req, res) => {
  try {
    const comment = req.createOpComment?.('metadata-overview');
    const listed = await runWithGovernor(
      req,
      'metadata',
      async () => withTimeout(listDatabasesSafe(req.conn, { comment, maxTimeMS: req.queryBudget.timeoutMs }), req.queryBudget.timeoutMs),
    );
    const databases = listed.databases || [];
    const stats = {};
    const freshness = {};
    const queue = databases.map((item) => item.name);
    const workerCount = Math.min(2, queue.length);
    const force = String(req.query.refresh || '') === '1';
    const worker = async () => {
      while (queue.length > 0) {
        const dbName = queue.shift();
        if (!dbName) continue;
        try {
          const next = await getDbStatsCached(req, dbName, { forceRefresh: force, timeoutMs: req.queryBudget.timeoutMs });
          stats[dbName] = next.stats;
          freshness[dbName] = { source: next.source, ts: next.ts, fresh: true };
        } catch (err) {
          const cached = sessionMetadataCache.get(req.connId, getDbStatsCacheKey(dbName));
          if (cached) {
            stats[dbName] = cached.value;
            freshness[dbName] = { source: 'stale_cache', ts: cached.ts, fresh: false, error: String(err?.message || 'error') };
          } else {
            freshness[dbName] = { source: 'error', ts: Date.now(), fresh: false, error: String(err?.message || 'error') };
          }
        }
      }
    };
    await Promise.all(Array.from({ length: workerCount }, () => worker()));
    res.json({
      databases,
      totalSize: listed.totalSize || 0,
      warning: listed.warning || '',
      version: req.conn.versionStr,
      stats,
      freshness,
      budget: {
        timeoutMs: req.queryBudget.timeoutMs,
        overrideApplied: req.queryBudget.overrideApplied,
      },
    });
  } catch (err) {
    sendError(res, err);
  }
});

app.post('/api/databases', getConnection, async (req, res) => {
  try {
    const { name } = req.body;
    if (!name || !/^[a-zA-Z0-9_-]+$/.test(name)) {
      return res.status(400).json({ error:'Invalid name', errorType:'validation' });
    }
    await runWithGovernor(req, 'metadata', async () => {
      const db = req.conn.client.db(name);
      const exists = await db.listCollections({ name: DB_INIT_COLLECTION }, { nameOnly: true }).toArray();
      if (exists.length === 0) {
        await db.createCollection(DB_INIT_COLLECTION);
      }
    });
    invalidateDatabaseCaches(req, name);
    auditReq(req,'create_db',{db:name});
    res.json({ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db', getConnection, async (req, res) => {
  try {
    const source = parseAuditSource(req.query?.source);
    if (!requireHeavyConfirm(req, res)) return;
    await runWithGovernor(req, 'heavy', async () => req.conn.client.db(req.params.db).dropDatabase());
    invalidateDatabaseCaches(req, req.params.db);
    auditReq(req,'drop_db',{db:req.params.db, ...(source ? { source } : {})});
    res.json({ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/export', heavyLimiter, getConnection, async (req, res) => {
  let reqClosed = false;
  const activeCursors = new Set();
  const cursorReleasers = new WeakMap();
  res.on('close', () => {
    if (res.writableEnded) return;
    reqClosed = true;
    for (const cursor of activeCursors) {
      const release = cursorReleasers.get(cursor);
      if (typeof release === 'function') {
        try { release(); } catch {}
        cursorReleasers.delete(cursor);
      }
      if (typeof cursor?.close === 'function') cursor.close().catch(() => {});
    }
  });
  try {
    const dbName = req.params.db;
    const db = req.conn.client.db(dbName);
    const includeDocuments = req.body?.includeDocuments !== false;
    const includeIndexes = req.body?.includeIndexes !== false;
    const includeOptions = req.body?.includeOptions !== false;
    const includeSchema = req.body?.includeSchema !== false;
    const rawOutput = String(req.query?.raw || '') === '1' || req.body?.raw === true;
    const zipOutput = rawOutput && (String(req.query?.zip || '') === '1' || req.body?.zip === true);
    if (includeDocuments && !requireHeavyConfirm(req, res)) return;
    const rawLimitPerCollection = parseInt(req.body?.limitPerCollection, 10);
    const limitPerCollection = Number.isFinite(rawLimitPerCollection)
      ? Math.max(0, Math.min(rawLimitPerCollection, EXPORT_LIMIT_MAX))
      : 0;
    const schemaSampleSize = Math.max(25, Math.min(parseInt(req.body?.schemaSampleSize) || 150, 5000));
    const filename = `${safeFilename(dbName)}.mongostudio-db.json`;
    const zipFilename = `${safeFilename(dbName)}.mongostudio-db.zip`;
    const collections = (await runWithGovernor(req, 'metadata', async () =>
      withTimeout(db.listCollections({}, { nameOnly: false }).toArray(), req.heavyBudget)))
      .filter((entry) => !isHiddenCollectionName(entry?.name));

    const registerCursor = (cursor) => {
      cursorReleasers.set(cursor, trackActiveCursor(req.connId, cursor));
      activeCursors.add(cursor);
      return cursor;
    };
    const closeCursor = async (cursor) => {
      if (!cursor) return;
      const release = cursorReleasers.get(cursor);
      if (typeof release === 'function') {
        try { release(); } catch {}
        cursorReleasers.delete(cursor);
      }
      activeCursors.delete(cursor);
      if (typeof cursor?.close === 'function') {
        await cursor.close().catch(() => {});
      }
    };

    let exportedDocs = 0;
    await runWithGovernor(req, 'heavy', async () => {
      const exportDeadline = Date.now() + req.heavyBudget;
      let zipWriter = null;
      const writeRaw = async (chunk) => {
        if (reqClosed) return;
        if (zipWriter) {
          await zipWriter.write(chunk);
          return;
        }
        await writeChunk(res, chunk);
      };
      const writeField = async (state, key, value) => {
        if (state.wroteField) await writeRaw(',');
        await writeRaw(`${JSON.stringify(String(key))}:${JSON.stringify(value)}`);
        state.wroteField = true;
      };
      if (zipOutput) {
        res.setHeader('Content-Type', 'application/zip');
        res.setHeader('Content-Disposition', `attachment; filename="${zipFilename}"`);
        zipWriter = await createZipSingleEntryWriter(res, filename);
      } else if (rawOutput) {
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
      } else {
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
      }
      if (!rawOutput) {
        await writeRaw(`{"ok":true,"format":"json","filename":${JSON.stringify(filename)},"collections":${collections.length},"data":`);
      }
      await writeRaw('{');
      await writeRaw(`"type":"mongostudio-db-package","version":1`);
      await writeRaw(`,"exportedAt":${JSON.stringify(new Date().toISOString())}`);
      await writeRaw(`,"source":{"host":${JSON.stringify(req.conn.host || null)},"version":${JSON.stringify(req.conn.versionStr || null)}}`);
      await writeRaw(`,"database":{"name":${JSON.stringify(dbName)}}`);
      await writeRaw(',"collections":[');

      for (let idx = 0; idx < collections.length; idx += 1) {
        if (reqClosed) return;
        const info = collections[idx];
        const colName = info.name;
        const col = db.collection(colName);
        if (idx > 0) await writeRaw(',');
        await writeRaw('{');
        const fieldState = { wroteField: false };
        await writeField(fieldState, 'name', colName);
        await writeField(fieldState, 'type', info.type || 'collection');

        if (includeOptions) {
          await writeField(fieldState, 'options', info.options || {});
        }
        if (includeIndexes) {
          let indexes;
          try {
            indexes = await withTimeout(col.indexes(), req.heavyBudget);
          } catch {
            indexes = [];
          }
          await writeField(fieldState, 'indexes', indexes);
        }
        let collectionDocsCount = null;
        if (includeDocuments) {
          if (fieldState.wroteField) await writeRaw(',');
          await writeRaw('"documents":[');
          fieldState.wroteField = true;
          let docsCount = 0;
          let cursor = col.find(
            {},
            {
              maxTimeMS: req.heavyBudget,
            },
          );
          if (limitPerCollection > 0) cursor = cursor.limit(limitPerCollection);
          const batchSize = limitPerCollection > 0
            ? Math.max(1, Math.min(limitPerCollection, EXPORT_CURSOR_BATCH_SIZE))
            : EXPORT_CURSOR_BATCH_SIZE;
          cursor = cursor.batchSize(batchSize);
          registerCursor(cursor);
          try {
            while (true) {
              const next = await withTimeout(cursor.next(), getRemainingMs(exportDeadline));
              if (!next) break;
              if (docsCount > 0) await writeRaw(',');
              await writeRaw(JSON.stringify(safeSerializeForExport(next)));
              docsCount += 1;
              exportedDocs += 1;
            }
          } finally {
            await closeCursor(cursor);
          }
          collectionDocsCount = docsCount;
          await writeRaw(']');
          if (limitPerCollection > 0 && docsCount >= limitPerCollection) {
            await writeField(fieldState, 'truncated', true);
          }
        }
        if (includeSchema) {
          let schema = [];
          let schemaSampleSizeUsed = 0;
          let schemaCursor = null;
          try {
            const schemaComment = req.createOpComment?.(`export-db-schema:${dbName}.${colName}`);
            let collectionCountForSchema = includeDocuments ? collectionDocsCount : null;
            if (!Number.isFinite(Number(collectionCountForSchema))) {
              try {
                collectionCountForSchema = await withTimeout(
                  col.estimatedDocumentCount({ maxTimeMS: req.heavyBudget }),
                  req.heavyBudget,
                );
              } catch {
                collectionCountForSchema = null;
              }
            }
            const schemaPipeline = Number(collectionCountForSchema) >= 100_000_000
              ? [{ $limit: schemaSampleSize }]
              : [{ $sample: { size: schemaSampleSize } }];
            const schemaMap = {};
            const schemaLimits = {
              maxFields: EXPORT_SCHEMA_MAX_FIELDS,
              fieldCount: 0,
              truncated: false,
              maxDepth: SCHEMA_MAX_DEPTH,
              depthTruncated: false,
            };
            const createSchemaCursor = () => callMongoWithCommentFallback(
              (opts) => col.aggregate(schemaPipeline, opts),
              { maxTimeMS: req.heavyBudget, comment: schemaComment },
            );
            schemaCursor = await createSchemaCursor();
            if (schemaCursor?.batchSize) {
              schemaCursor = schemaCursor.batchSize(Math.max(1, Math.min(schemaSampleSize, 128)));
            }
            registerCursor(schemaCursor);
            while (schemaSampleSizeUsed < schemaSampleSize) {
              const doc = await withTimeout(schemaCursor.next(), getRemainingMs(exportDeadline));
              if (!doc) break;
              analyzeFields(doc, '', schemaMap, schemaLimits);
              schemaSampleSizeUsed += 1;
            }
            schema = finalizeSchemaFields(schemaMap, schemaSampleSizeUsed);
            if (schemaLimits.truncated || schemaLimits.depthTruncated) {
              await writeField(fieldState, 'schemaTruncated', true);
            }
          } catch {
            schema = [];
            schemaSampleSizeUsed = 0;
          } finally {
            await closeCursor(schemaCursor);
          }
          await writeField(fieldState, 'schemaSampleSize', schemaSampleSizeUsed);
          await writeField(fieldState, 'schema', schema);
        }
        await writeRaw('}');
      }
      await writeRaw(']');
      await writeRaw('}');
      if (reqClosed) return;
      if (!rawOutput) {
        await writeRaw(`,"documents":${exportedDocs}}`);
      }
      if (zipWriter) {
        await zipWriter.end();
      }
      res.end();
    });

    if (reqClosed) return;
    auditReq(req, 'export_db', {
      db: dbName,
      collections: collections.length,
      docs: exportedDocs,
    });
  } catch (err) {
    console.error('[EXPORT_DB] stream failed', {
      db: req?.params?.db,
      connId: req?.connId,
      message: err?.message,
      stack: err?.stack,
      reqClosed,
      headersSent: res.headersSent,
      writableEnded: res.writableEnded,
    });
    if (reqClosed || res.headersSent) {
      if (!res.writableEnded) res.end();
      return;
    }
    sendError(res, err);
  }
});

app.post('/api/databases/import', heavyLimiter, getConnection, async (req, res) => {
  try {
    if (!requireHeavyConfirm(req, res)) return;
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

    const warnings = [];
    let createdCollections = 0;
    let insertedDocuments = 0;
    let createdIndexes = 0;
    await runWithGovernor(req, 'heavy', async () => {
      const db = req.conn.client.db(dbName);
      if (mode === 'replace') {
        await db.dropDatabase();
      }
      const existing = new Set((await withTimeout(db.listCollections({}, { nameOnly: true }).toArray(), HEAVY_TIMEOUT_DEFAULT_MS)).map(c => c.name));
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
    });
    invalidateDatabaseCaches(req, dbName);

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
    const force = String(req.query.refresh || '') === '1';
    const source = parseAuditSource(req.query?.source);
    const data = await getDbStatsCached(req, req.params.db, { forceRefresh: force, timeoutMs: req.queryBudget.timeoutMs });
    if (source) {
      auditReq(req, 'metadata', {
        db: req.params.db,
        method: 'dbStats',
        source,
        collections: Number(data?.stats?.collections || 0),
        count: Number(data?.stats?.objects || 0),
      });
    }
    res.json({ ...data.stats, _source: data.source, _ts: data.ts, _fresh: data.fresh });
  } catch (err) {
    sendError(res, err);
  }
});

app.get('/api/databases/:db/collections', getConnection, async (req, res) => {
  try {
    const db = req.conn.client.db(req.params.db);
    const withStats = String(req.query?.withStats || '') === '1';
    const source = parseAuditSource(req.query?.source);
    const force = String(req.query.refresh || '') === '1';
    let c = [];
    try {
      c = await runWithGovernor(req, 'metadata', async () => withTimeout(db.listCollections({}, { nameOnly: !withStats, authorizedCollections: true }).toArray(), req.queryBudget.timeoutMs));
    } catch {
      try {
        c = await runWithGovernor(req, 'metadata', async () => withTimeout(db.listCollections().toArray(), req.queryBudget.timeoutMs));
      } catch {
        const fallback = await req.conn.client.db(req.params.db).collections();
        c = fallback.map((x) => ({ name: x.collectionName, type: 'collection', options: {} }));
      }
    }
    let collections = c
      .map((x) => ({ name: x.name, type: x.type || 'collection', options: x.options || {} }))
      .filter((entry) => !isHiddenCollectionName(entry.name))
      .sort((a, b) => a.name.localeCompare(b.name));
    if (withStats) {
      const queue = [...collections];
      const workerCount = Math.min(2, queue.length);
      const nextCollections = [];
      const worker = async () => {
        while (queue.length > 0) {
          const entry = queue.shift();
          if (!entry) continue;
          try {
            const next = await getCollectionStatsCached(req, req.params.db, entry.name, {
              forceRefresh: force,
              timeoutMs: req.queryBudget.timeoutMs,
            });
            nextCollections.push({
              ...entry,
              count: next.stats.count,
              size: next.stats.size,
              avgObjSize: next.stats.avgObjSize,
              nindexes: next.stats.nindexes,
              _source: next.source,
              _ts: next.ts,
            });
          } catch {
            nextCollections.push({ ...entry, count: 0, size: 0, avgObjSize: 0, nindexes: 0, _source: 'error', _ts: Date.now() });
          }
        }
      };
      await Promise.all(Array.from({ length: workerCount }, () => worker()));
      collections = nextCollections.sort((a, b) => a.name.localeCompare(b.name));
    }
    if (source) {
      auditReq(req, 'metadata', {
        db: req.params.db,
        method: withStats ? 'listCollectionsWithStats' : 'listCollections',
        source,
        count: collections.length,
      });
    }
    res.json({ collections });
  } catch(err) {
    sendError(res,err);
  }
});

app.post('/api/databases/:db/collections', getConnection, async (req, res) => {
  try {
    const source = parseAuditSource(req.query?.source);
    const colName = String(req.body?.name || '').trim();
    if (!colName) return res.status(400).json({error:'Name required',errorType:'validation'});
    if (isHiddenCollectionName(colName)) return res.status(400).json({error:'Reserved collection name.',errorType:'validation'});
    const db = req.conn.client.db(req.params.db);
    await runWithGovernor(req, 'metadata', async () => {
      await db.createCollection(colName);
      await db.collection(DB_INIT_COLLECTION).drop().catch(() => {});
    });
    invalidateDatabaseCaches(req, req.params.db);
    auditReq(req,'create_col',{db:req.params.db,col:colName, ...(source ? { source } : {})});
    res.json({ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/import', heavyLimiter, getConnection, async (req, res) => {
  try {
    if (!requireHeavyConfirm(req, res)) return;
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

    let insertedCount = 0;
    let indexCount = 0;
    await runWithGovernor(req, 'heavy', async () => {
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
      if (parsedDocs.length > 0) {
        const result = await collection.insertMany(parsedDocs, { ordered: false });
        insertedCount = Object.keys(result.insertedIds || {}).length;
      }

      for (const idx of indexes) {
        const spec = sanitizeIndexForCreate(idx);
        if (!spec) continue;
        if (spec.options?.name === '_id_' || (Object.keys(spec.key).length === 1 && spec.key._id === 1)) continue;
        try {
          await collection.createIndex(spec.key, spec.options || {});
          indexCount++;
        } catch {}
      }
    });
    invalidateCollectionCaches(req, dbName, colName);

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
  try {
    if (!requireHeavyConfirm(req, res)) return;
    await runWithGovernor(req, 'heavy', async () => req.conn.client.db(req.params.db).collection(req.params.col).drop());
    invalidateCollectionCaches(req, req.params.db, req.params.col);
    auditReq(req,'drop_col',{db:req.params.db,col:req.params.col});
    res.json({ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.get('/api/databases/:db/collections/:col/stats', getConnection, async (req, res) => {
  try {
    const force = String(req.query.refresh || '') === '1';
    const source = parseAuditSource(req.query?.source);
    const data = await getCollectionStatsCached(req, req.params.db, req.params.col, {
      forceRefresh: force,
      timeoutMs: req.queryBudget.timeoutMs,
    });
    if (source) {
      auditReq(req, 'metadata', {
        db: req.params.db,
        col: req.params.col,
        method: 'collStats',
        source,
        count: Number(data?.stats?.count || 0),
      });
    }
    res.json({ ...data.stats, _source: data.source, _ts: data.ts, _fresh: data.fresh });
  }
  catch(err) { sendError(res, err); }
});

app.get('/api/databases/:db/collections/:col/schema', getConnection, async (req, res) => {
  try {
    const col = req.conn.client.db(req.params.db).collection(req.params.col);
    const requestedSample = Math.min(parseInt(req.query.sample) || 100, 5000);
    let n = requestedSample;
    let collectionCount = 0;
    try {
      const statsData = await getCollectionStatsCached(req, req.params.db, req.params.col, {
        timeoutMs: req.queryBudget.timeoutMs,
      });
      collectionCount = Number(statsData?.stats?.count || 0);
      if (collectionCount >= 100_000_000) n = Math.min(n, 25);
      else if (collectionCount >= 50_000_000) n = Math.min(n, 30);
      else if (collectionCount >= 10_000_000) n = Math.min(n, 50);
    } catch { /* stats lookup failed - keep requested sample */ }
    const schemaComment = req.createOpComment?.(`schema:${req.params.db}.${req.params.col}`);
    const cacheKey = getSchemaQuickCacheKey(req.params.db, req.params.col, n);
    const cached = sessionMetadataCache.get(req.connId, cacheKey);
    if (cached) {
      return res.json({
        ...(cached.value || { fields: [], sampleSize: 0 }),
        _source: cached.source || 'cache',
        _ts: cached.ts,
      });
    }
    const payload = await runWithGovernor(
      req,
      'metadata',
      async () => {
        const schemaPipeline = collectionCount >= 100_000_000
          ? [{ $limit: n }]
          : [{ $sample: { size: n } }];
        const schemaCursor = col.aggregate(schemaPipeline, { maxTimeMS: req.queryBudget.timeoutMs, comment: schemaComment });
        const untrackSchemaCursor = trackActiveCursor(req.connId, schemaCursor);
        req.on('close', () => {
          untrackSchemaCursor();
          schemaCursor.close().catch(() => {});
        });
        const schema = {};
        const schemaLimits = {
          maxFields: SCHEMA_MAX_FIELDS,
          fieldCount: 0,
          truncated: false,
          maxDepth: SCHEMA_MAX_DEPTH,
          depthTruncated: false,
        };
        let sampleSize = 0;
        const sampleDeadline = Date.now() + req.queryBudget.timeoutMs;
        try {
          while (sampleSize < n) {
            const next = await withTimeout(schemaCursor.next(), getRemainingMs(sampleDeadline));
            if (!next) break;
            analyzeFields(next, '', schema, schemaLimits);
            sampleSize += 1;
          }
        } finally {
          untrackSchemaCursor();
          await schemaCursor.close().catch(() => {});
        }
        return {
          fields: finalizeSchemaFields(schema, sampleSize),
          sampleSize,
          schemaTruncated: Boolean(schemaLimits.truncated || schemaLimits.depthTruncated),
        };
      },
    );
    sessionMetadataCache.set(req.connId, cacheKey, payload, getMetadataCacheConfig().ttlSchemaQuickMs, 'live');
    res.json({ ...payload, _source: 'live', _ts: Date.now() });
  } catch(err) { sendError(res, err); }
});

app.get('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  const qid = randomUUID().replace(/-/g,'');
  const t0 = Date.now();
  const source = parseAuditSource(req.query?.source);
  let reqClosed = false;
  const findComment = req.createOpComment?.(`find:${req.params.db}.${req.params.col}`);
  req.on('close', () => { reqClosed = true; });
  try {
    runningQueries.set(qid, { connId: req.connId, t0, type: 'find' });
    const requestDeadline = Date.now() + req.queryBudget.timeoutMs;
    const response = await runWithGovernor(req, 'interactive', async () => {
      const col = req.conn.client.db(req.params.db).collection(req.params.col);
      const rawFilter = req.query.filter !== undefined ? req.query.filter : '{}';
      const rawProjection = req.query.projection !== undefined ? req.query.projection : '{}';
      const rawSort = req.query.sort !== undefined ? req.query.sort : '{}';
      const filter = parseFilterInputStrict(rawFilter, 'filter');
      const sort = parseSortInputStrict(rawSort, 'sort') || {};
      const projection = parseProjectionInputStrict(rawProjection, 'projection') || {};
      const hint = typeof req.query.hint === 'string' && req.query.hint.trim() && req.query.hint !== 'auto'
        ? req.query.hint.trim()
        : null;
      const skip = Math.max(0, parseInt(req.query.skip) || 0);
      const requestedLimit = parseInt(req.query.limit) || QUERY_LIMIT_DEFAULT;
      const limit = clampNumber(req.queryBudget.limit || requestedLimit, QUERY_LIMIT_MIN, QUERY_LIMIT_MAX);

      // Keyset pagination: if keysetCursor provided and sort is default (_id), use range query
      // instead of skip — O(log N) seek vs O(N) scan.
      const defaultIdSortDirection = getDefaultIdSortDirection(rawSort);
      const isDefaultSort = defaultIdSortDirection !== 0;
      const keysetCursor = req.query.keysetCursor || null;
      let effectiveFilter = filter;
      let effectiveSort = sort;
      let effectiveSkip = skip;
      if (keysetCursor && isDefaultSort) {
        try {
          const dir = defaultIdSortDirection === -1 ? '$lt' : '$gt';
          effectiveFilter = { ...filter, _id: { [dir]: new ObjectId(keysetCursor) } };
          effectiveSort = { _id: defaultIdSortDirection };
          effectiveSkip = 0;
        } catch { /* invalid cursor — fall back to skip */ }
      }

      const fo = { projection, maxTimeMS: getRemainingMs(requestDeadline), comment: findComment };
      let cursor = col.find(effectiveFilter, fo);
      if (hint) cursor = cursor.hint(hint);
      const fetchBatchSize = Math.max(1, Math.min(limit + 1, 1000));
      const fetchCursor = cursor.sort(effectiveSort).skip(effectiveSkip).limit(limit + 1).batchSize(fetchBatchSize);
      const untrackFetchCursor = trackActiveCursor(req.connId, fetchCursor);
      req.on('close', () => {
        untrackFetchCursor();
        fetchCursor.close().catch(() => {});
      });
      const fetchDeadline = requestDeadline;
      const docsRaw = [];
      try {
        while (docsRaw.length < limit + 1) {
          const next = await withTimeout(fetchCursor.next(), getRemainingMs(fetchDeadline));
          if (!next) break;
          docsRaw.push(next);
        }
      } finally {
        untrackFetchCursor();
        await fetchCursor.close().catch(() => {});
      }
      const hasMore = docsRaw.length > limit;
      const documents = hasMore ? docsRaw.slice(0, limit) : docsRaw;
      const lastDoc = documents[documents.length - 1];
      const returnedKeysetCursor = (isDefaultSort && lastDoc?._id) ? lastDoc._id.toString() : null;

      const totalInfo = {
        state: 'unknown',
        value: null,
        approx: false,
        source: 'none',
        ts: 0,
      };
      const filterIsEmpty = Object.keys(filter || {}).length === 0;
      const approxCacheKey = getApproxTotalCacheKey(req.params.db, req.params.col, rawFilter, rawProjection);
      const exactCacheKey = getExactTotalCacheKey(req.params.db, req.params.col, rawFilter, rawProjection, hint || 'auto');
      if (filterIsEmpty) {
        const cachedExact = sessionMetadataCache.get(req.connId, exactCacheKey);
        if (cachedExact) {
          totalInfo.state = 'ready';
          totalInfo.value = Number(cachedExact.value || 0);
          totalInfo.approx = false;
          totalInfo.source = cachedExact.source || 'exact-cache';
          totalInfo.ts = cachedExact.ts || Date.now();
        } else {
          const cachedApprox = sessionMetadataCache.get(req.connId, approxCacheKey);
          if (cachedApprox) {
            totalInfo.state = 'ready';
            totalInfo.value = Number(cachedApprox.value || 0);
            totalInfo.approx = true;
            totalInfo.source = cachedApprox.source || 'cache';
            totalInfo.ts = cachedApprox.ts || Date.now();
          } else {
            try {
              const approxComment = req.createOpComment?.(`estcount:${req.params.db}.${req.params.col}`);
              const remainingForApprox = getRemainingMs(requestDeadline);
              if (remainingForApprox <= 150) {
                totalInfo.state = 'timeout';
                throw timeoutError();
              }
              const metadataReq = {
                ...req,
                queryBudget: {
                  ...req.queryBudget,
                  timeoutMs: Math.max(GOVERNOR_QUEUE_BASE_POLL_MS, remainingForApprox),
                },
              };
              const approx = await runWithGovernor(
                metadataReq,
                'metadata',
                async () => {
                  const opMsRaw = requestDeadline - Date.now();
                  if (opMsRaw <= 0) throw timeoutError();
                  const opMs = Math.max(1, opMsRaw);
                  return withTimeout(
                    compatEstCount(col, req.caps, { comment: approxComment, maxTimeMS: opMs }),
                    opMs,
                  );
                },
              );
              sessionMetadataCache.set(req.connId, approxCacheKey, Number(approx || 0), getMetadataCacheConfig().ttlApproxTotalMs, 'estimated');
              totalInfo.state = 'ready';
              totalInfo.value = Number(approx || 0);
              totalInfo.approx = true;
              totalInfo.source = 'estimated';
              totalInfo.ts = Date.now();
            } catch (err) {
              totalInfo.state = err?.code === 50 ? 'timeout' : 'unknown';
            }
          }
        }
      } else {
        const cachedExact = sessionMetadataCache.get(req.connId, exactCacheKey);
        if (cachedExact) {
          totalInfo.state = 'ready';
          totalInfo.value = Number(cachedExact.value || 0);
          totalInfo.approx = false;
          totalInfo.source = cachedExact.source || 'cache';
          totalInfo.ts = cachedExact.ts || Date.now();
        }
      }
      const elapsed = Date.now() - t0;
      const totalLegacy = totalInfo.state === 'ready' ? Number(totalInfo.value || 0) : null;
      return {
        documents,
        page: {
          mode: keysetCursor ? 'keyset' : 'skip',
          skip: effectiveSkip,
          limit,
          hasMore,
          nextCursor: null,
          prevCursor: null,
          keysetCursor: returnedKeysetCursor,
        },
        total: totalInfo,
        totalLegacy,
        budget: {
          timeoutMs: req.queryBudget.timeoutMs,
          overrideApplied: req.queryBudget.overrideApplied,
        },
        warnings: [],
        _elapsed: elapsed,
        _slow: elapsed > 5000,
      };
    });
    if (reqClosed) return;
    auditReq(req, 'query', {
      db: req.params.db,
      col: req.params.col,
      elapsed: response._elapsed,
      count: response.documents.length,
      totalState: response.total?.state || 'unknown',
      ...(source ? { source } : {}),
    });
    res.json(response);
  } catch (err) {
    if (reqClosed || res.headersSent) return;
    sendError(res, err);
  } finally {
    runningQueries.delete(qid);
  }
});

app.post('/api/databases/:db/collections/:col/total/exact', heavyLimiter, getConnection, async (req, res) => {
  try {
    if (!requireHeavyConfirm(req, res)) return;
    const dbName = req.params.db;
    const colName = req.params.col;
    const source = parseAuditSource(req.query?.source);
    const filterInput = Object.prototype.hasOwnProperty.call(req.body || {}, 'filter') ? req.body.filter : '{}';
    const projectionInput = Object.prototype.hasOwnProperty.call(req.body || {}, 'projection') ? req.body.projection : '{}';
    const filter = parseFilterInputStrict(filterInput, 'filter');
    const projection = parseProjectionInputStrict(projectionInput, 'projection') || {};
    const rawFilter = typeof filterInput === 'string'
      ? filterInput
      : JSON.stringify(sortObjectKeys(filterInput || {}));
    const rawProjection = typeof projectionInput === 'string'
      ? projectionInput
      : JSON.stringify(sortObjectKeys(projectionInput || {}));
    const hint = typeof req.body?.hint === 'string' && req.body.hint.trim() && req.body.hint !== 'auto'
      ? req.body.hint.trim()
      : null;
    const timeoutMs = clampNumber(
      Number(req.body?.timeoutMs) || req.heavyBudget || HEAVY_TIMEOUT_DEFAULT_MS,
      HEAVY_TIMEOUT_MIN_MS,
      HEAVY_TIMEOUT_MAX_MS,
    );
    const exactCacheKey = getExactTotalCacheKey(dbName, colName, rawFilter, rawProjection, hint || 'auto');
    const job = createJob(req.connId, 'exact_total', {
      db: dbName,
      col: colName,
      timeoutMs,
    });
    auditReq(req, 'exact_total_start', {
      db: dbName,
      col: colName,
      jobId: job.jobId,
      timeoutMs,
      filterEmpty: Object.keys(filter || {}).length === 0,
      ...(source ? { source } : {}),
    });
    const connId = req.connId;
    const caps = req.caps;
    setImmediate(async () => {
      patchJob(job.jobId, { state: 'running', progressPct: 10 });
      try {
        const conn = connections.get(connId);
        if (!conn) throw new Error('Connection closed.');
        const total = await runWithGovernor({ connId }, 'heavy', async () => {
          const col = conn.client.db(dbName).collection(colName);
          const exactTotalComment = createOperationComment(connId, `exact-total:${dbName}.${colName}`);
          let cursor = col.find(filter, { projection, maxTimeMS: timeoutMs, comment: exactTotalComment });
          if (hint) cursor = cursor.hint(hint);
          const untrackHintCursor = trackActiveCursor(connId, cursor);
          try {
            await withTimeout(cursor.limit(1).toArray(), timeoutMs);
          } finally {
            untrackHintCursor();
            await cursor.close().catch(() => {});
          }
          return withTimeout(compatCount(col, filter, caps, { comment: exactTotalComment, maxTimeMS: timeoutMs }), timeoutMs);
        });
        const value = Number(total || 0);
        sessionMetadataCache.set(connId, exactCacheKey, value, getMetadataCacheConfig().ttlExactTotalMs, 'exact');
        patchJob(job.jobId, { state: 'done', progressPct: 100, result: { value, approx: false }, error: null });
        audit(connId, 'exact_total_done', {
          db: dbName,
          col: colName,
          jobId: job.jobId,
          value,
          ...(source ? { source } : {}),
        });
      } catch (err) {
        const state = err?.code === 50 ? 'timeout' : 'error';
        const errorMessage = String(err?.message || 'Exact count failed');
        patchJob(job.jobId, { state, progressPct: 100, error: errorMessage });
        audit(connId, state === 'timeout' ? 'exact_total_timeout' : 'exact_total_error', {
          db: dbName,
          col: colName,
          jobId: job.jobId,
          error: errorMessage,
          ...(source ? { source } : {}),
        });
      }
    });
    res.json({ jobId: job.jobId, state: job.state });
  } catch (err) {
    sendError(res, err);
  }
});

app.get('/api/jobs/:jobId', getConnection, (req, res) => {
  const job = getJobForConnection(req.connId, req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job not found.', errorType: 'not_found' });
  res.json({
    jobId: job.jobId,
    type: job.type,
    state: job.state,
    progressPct: job.progressPct || 0,
    result: job.result || null,
    error: job.error || null,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
  });
});

app.post('/api/databases/:db/collections/:col/preflight', getConnection, async (req, res) => {
  try {
    const source = parseAuditSource(req.query?.source);
    const operation = String(req.body?.operation || 'deleteMany').trim();
    const dbName = req.params.db;
    const colName = req.params.col;
    const col = req.conn.client.db(dbName).collection(colName);
    let estimate = null;
    let risk = 'low';
    const warnings = [];
    let preflightExtra = null;

    const toRiskLabel = (score) => {
      if (score >= 80) return 'critical';
      if (score >= 55) return 'high';
      if (score >= 30) return 'medium';
      return 'low';
    };

    const summarizeUpdateSpec = (value) => {
      if (Array.isArray(value)) {
        const stages = value
          .map((stage) => (stage && typeof stage === 'object' ? Object.keys(stage) : []))
          .flat()
          .filter(Boolean)
          .slice(0, 20);
        return { kind: 'pipeline', stageCount: value.length, stages };
      }
      if (value && typeof value === 'object') {
        const keys = Object.keys(value);
        const operators = keys.filter((key) => key.startsWith('$'));
        if (operators.length > 0) {
          return { kind: 'operator', operatorCount: operators.length, operators: operators.slice(0, 20) };
        }
        return { kind: 'replacement', fieldCount: keys.length, fields: keys.slice(0, 20) };
      }
      return { kind: 'unknown' };
    };

    const parseFilterForPreflight = (raw) => {
      if (typeof raw === 'string') {
        const parsed = parseFilter(raw);
        if (raw.trim() && raw.trim() !== '{}' && Object.keys(parsed).length === 0) {
          warnings.push('Filter could not be parsed reliably and was treated as {}.');
        }
        return parsed;
      }
      return parseFilterInput(raw || {});
    };

    const runExplainForFilter = async (filter, label) => {
      const explainComment = req.createOpComment?.(`preflight-explain:${label}:${dbName}.${colName}`);
      const explain = await runWithGovernor(
        req,
        'metadata',
        async () => withTimeout(
          req.conn.client.db(dbName).command(
            { explain: { find: colName, filter, limit: 1 }, verbosity: 'queryPlanner', comment: explainComment },
            { maxTimeMS: req.queryBudget.timeoutMs },
          ),
          req.queryBudget.timeoutMs,
        ),
      );
      return extractExplainSummary(explain);
    };

    const runCountForFilter = async (filter, label, fallbackEstimated = false) => {
      const estimateComment = req.createOpComment?.(`preflight-est:${label}:${dbName}.${colName}`);
      if (fallbackEstimated) {
        return runWithGovernor(
          req,
          'metadata',
          async () => withTimeout(
            compatEstCount(col, req.caps, { comment: estimateComment, maxTimeMS: req.queryBudget.timeoutMs }),
            req.queryBudget.timeoutMs,
          ),
        );
      }
      return runWithGovernor(
        req,
        'metadata',
        async () => withTimeout(
          compatCount(col, filter, req.caps, { comment: estimateComment, maxTimeMS: req.queryBudget.timeoutMs }),
          req.queryBudget.timeoutMs,
        ),
      );
    };

    if (operation === 'deleteMany' || operation === 'updateMany') {
      const filter = parseFilterForPreflight(req.body?.filter);
      const filterIsEmpty = !filter || Object.keys(filter).length === 0;
      let explainSummary = null;
      let riskScore = 0;

      if (filterIsEmpty) warnings.push('Empty filter targets all documents in this collection.');
      try {
        explainSummary = await runExplainForFilter(filter, operation);
      } catch {
        warnings.push(`Could not run explain within budget for ${operation}.`);
      }

      try {
        estimate = await runCountForFilter(filter, operation, filterIsEmpty);
      } catch {
        estimate = null;
        warnings.push(`Could not estimate matched documents for ${operation}.`);
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

      const details = { explainSummary };
      if (operation === 'updateMany') {
        let updateSpec = req.body?.update;
        if (typeof updateSpec === 'string') {
          try {
            updateSpec = JSON.parse(updateSpec);
          } catch (err) {
            throw createRequestError(`Invalid update JSON: ${err.message}`, 400);
          }
        }
        const updateSummary = summarizeUpdateSpec(updateSpec);
        details.updateSummary = updateSummary;
        if (updateSummary.kind === 'replacement') {
          warnings.push('Update appears to be a full-document replacement.');
        }
      }
      if (explainSummary) {
        warnings.push(`Explain summary: ${explainSummary.isCollScan ? 'collection scan' : `index ${explainSummary.indexUsed || 'unknown'}`}.`);
      }

      riskScore = Math.max(0, Math.min(100, riskScore));
      risk = toRiskLabel(riskScore);
      preflightExtra = { riskScore, ...details };
    } else if (operation === 'bulkWrite') {
      let operationsInput = req.body?.operations;
      if (typeof operationsInput === 'string') {
        try {
          operationsInput = JSON.parse(operationsInput);
        } catch (err) {
          throw createRequestError(`Invalid operations JSON: ${err.message}`, 400);
        }
      }
      const operations = parseBulkWriteOperations(operationsInput);
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
      let riskScore = 0;
      let estimatedAffected = 0;
      let estimateChecks = 0;
      let skippedEstimateOps = 0;

      for (const item of operations) {
        const op = String(Object.keys(item || {})[0] || '');
        if (summary[op] !== undefined) summary[op] += 1;
        const body = item[op] || {};
        if (op === 'updateMany' || op === 'deleteMany' || op === 'updateOne' || op === 'deleteOne' || op === 'replaceOne') {
          const filter = parseFilterInput(body.filter || {});
          if (estimateChecks >= 20) {
            skippedEstimateOps += 1;
            continue;
          }
          estimateChecks += 1;
          try {
            const count = await runCountForFilter(filter, `bulk:${op}`, false);
            if (op === 'updateMany' || op === 'deleteMany') estimatedAffected += Number(count || 0);
            else estimatedAffected += Math.min(1, Number(count || 0));
          } catch {
            warnings.push(`Could not estimate matches for bulk ${op}.`);
          }
        }
      }

      if (skippedEstimateOps > 0) {
        warnings.push(`Estimate limited to first ${estimateChecks} filter-based operations.`);
      }

      estimate = estimatedAffected;
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
      risk = toRiskLabel(riskScore);
      if (summary.deleteMany > 0) warnings.push('bulkWrite contains deleteMany operations.');
      if (summary.updateMany > 0) warnings.push('bulkWrite contains updateMany operations.');
      preflightExtra = {
        riskScore,
        bulkSummary: {
          ...summary,
          estimatedAffected,
          estimatedOpsChecked: estimateChecks,
          estimatedOpsSkipped: skippedEstimateOps,
        },
      };
    } else if (operation === 'dropCollection') {
      try {
        const estimateComment = req.createOpComment?.(`preflight-est:${dbName}.${colName}`);
        estimate = await runWithGovernor(
          req,
          'metadata',
          async () => withTimeout(compatEstCount(col, req.caps, { comment: estimateComment, maxTimeMS: req.queryBudget.timeoutMs }), req.queryBudget.timeoutMs),
        );
      } catch {
        estimate = null;
      }
      risk = estimate !== null && estimate > 100000 ? 'critical' : 'high';
    } else if (operation === 'export') {
      const filter = parseFilterForPreflight(req.body?.filter);
      const filterIsEmpty = !filter || Object.keys(filter).length === 0;
      const parsedLimit = parseExportLimitInput(req.body?.limit, 'limit', 1000);
      if (parsedLimit.mode === 'fixed') {
        estimate = parsedLimit.limit;
      } else {
        try {
          estimate = await runCountForFilter(filter, 'export', filterIsEmpty);
        } catch {
          estimate = null;
          warnings.push('Could not estimate export size.');
        }
      }
      const riskBase = Number.isFinite(Number(estimate)) ? Number(estimate) : 0;
      risk = riskBase > 50000 ? 'high' : riskBase > 10000 ? 'medium' : 'low';
      preflightExtra = {
        ...(preflightExtra || {}),
        exportLimitMode: parsedLimit.mode,
      };
    } else if (operation === 'import') {
      const countFromBody = Number(req.body?.documentsCount);
      const count = Number.isFinite(countFromBody) && countFromBody >= 0
        ? Math.round(countFromBody)
        : (Array.isArray(req.body?.documents) ? req.body.documents.length : 0);
      estimate = count;
      risk = count > 50000 ? 'high' : count > 5000 ? 'medium' : 'low';
    } else {
      warnings.push(`Unsupported preflight operation "${operation}".`);
      risk = 'unknown';
    }
    const responsePayload = {
      operation,
      db: dbName,
      col: colName,
      estimate,
      risk,
      warnings,
      ...(preflightExtra || {}),
      budget: {
        timeoutMs: req.queryBudget.timeoutMs,
        overrideApplied: req.queryBudget.overrideApplied,
      },
    };
    auditReq(req, 'preflight', {
      db: dbName,
      col: colName,
      operation,
      risk,
      estimate: estimate === null ? null : Number(estimate),
      warnings: warnings.length,
      riskScore: Number(preflightExtra?.riskScore || 0),
      ...(source ? { source } : {}),
    });
    res.json(responsePayload);
  } catch (err) {
    sendError(res, err);
  }
});

app.get('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try {
    const d = await runWithGovernor(
      req,
      'interactive',
      async () => withTimeout(req.conn.client.db(req.params.db).collection(req.params.col).findOne({ _id: parseId(req.params.id) }), req.queryBudget.timeoutMs),
    );
    if (!d) return res.status(404).json({ error: 'Not found', errorType: 'not_found' });
    res.json(d);
  }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  try {
    const r = await runWithGovernor(req, 'interactive', async () => req.conn.client.db(req.params.db).collection(req.params.col).insertOne(deserializeImportValue(req.body.document)));
    invalidateCollectionCaches(req, req.params.db, req.params.col);
    auditReq(req,'insert',{db:req.params.db,col:req.params.col,count:1});
    res.json({insertedId:r.insertedId,ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/:col/documents/bulk', heavyLimiter, getConnection, async (req, res) => {
  try {
    if (!requireHeavyConfirm(req, res)) return;
    const docs = Array.isArray(req.body?.documents) ? req.body.documents : null;
    if (!docs || docs.length === 0) {
      return res.status(400).json({ error: 'documents array is required.', errorType: 'validation' });
    }
    if (docs.length > 10000) {
      return res.status(400).json({ error: 'Too many documents in one request (max 10000).', errorType: 'validation' });
    }
    const parsed = docs.map(deserializeImportValue);
    const r = await runWithGovernor(
      req,
      'heavy',
      async () => req.conn.client.db(req.params.db).collection(req.params.col).insertMany(parsed, { ordered: false }),
    );
    const insertedCount = Object.keys(r.insertedIds || {}).length;
    invalidateCollectionCaches(req, req.params.db, req.params.col);
    auditReq(req, 'insert_many', { db: req.params.db, col: req.params.col, count: insertedCount });
    res.json({ ok: true, insertedCount, insertedIds: r.insertedIds });
  } catch (err) {
    sendError(res, err);
  }
});

app.put('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try {
    const r = await runWithGovernor(
      req,
      'interactive',
      async () => req.conn.client.db(req.params.db).collection(req.params.col).replaceOne({ _id: parseId(req.params.id) }, { ...req.body.update, _id: parseId(req.params.id) }),
    );
    invalidateCollectionCaches(req, req.params.db, req.params.col);
    auditReq(req,'update',{db:req.params.db,col:req.params.col,count:r.modifiedCount||0});
    res.json({modifiedCount:r.modifiedCount,ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db/collections/:col/documents/:id', getConnection, async (req, res) => {
  try {
    const deleteComment = req.createOpComment?.(`delete-one:${req.params.db}.${req.params.col}`);
    const r = await runWithGovernor(
      req,
      'interactive',
      async () => callMongoWithCommentFallback(
        (options) => req.conn.client.db(req.params.db).collection(req.params.col).deleteOne(
          { _id: parseId(req.params.id) },
          options,
        ),
        { comment: deleteComment, maxTimeMS: req.heavyBudget },
      ),
    );
    invalidateCollectionCaches(req, req.params.db, req.params.col);
    auditReq(req,'delete',{db:req.params.db,col:req.params.col,count:r.deletedCount||0});
    res.json({deletedCount:r.deletedCount,ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db/collections/:col/documents', getConnection, async (req, res) => {
  try {
    if (!requireHeavyConfirm(req, res)) return;
    const filterInput = Object.prototype.hasOwnProperty.call(req.body || {}, 'filter') ? req.body.filter : {};
    const f = parseFilterInputStrict(filterInput, 'filter');
    const deleteManyComment = req.createOpComment?.(`delete-many:${req.params.db}.${req.params.col}`);
    const r = await runWithGovernor(
      req,
      'heavy',
      async () => callMongoWithCommentFallback(
        (options) => req.conn.client.db(req.params.db).collection(req.params.col).deleteMany(f, options),
        { comment: deleteManyComment, maxTimeMS: req.heavyBudget },
      ),
    );
    invalidateCollectionCaches(req, req.params.db, req.params.col);
    auditReq(req,'delete_many',{db:req.params.db,col:req.params.col,count:r.deletedCount});
    res.json({deletedCount:r.deletedCount,ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/:col/operate', heavyLimiter, getConnection, async (req, res) => {
  try {
    const source = parseAuditSource(req.query?.source);
    const method = String(req.body?.method || '').trim();
    if (!method) throw createRequestError('method is required.');
    const payloadRaw = req.body?.payload;
    const payload = isPlainObject(payloadRaw) ? payloadRaw : {};
    const dbName = req.params.db;
    const colName = req.params.col;
    const collection = req.conn.client.db(dbName).collection(colName);
    const removeJustOne = method === 'remove' ? parseBooleanInput(payload.justOne, 'payload.justOne') : undefined;
    const findAndModifyRemove = method === 'findAndModify' ? parseBooleanInput(payload.remove, 'payload.remove') : undefined;
    const heavyMethods = new Set(['insertMany', 'updateMany', 'deleteMany', 'bulkWrite', 'dropIndexes', 'validateCollection', 'reIndex']);
    const isHeavyMethod = heavyMethods.has(method) || (method === 'remove' && removeJustOne !== true);
    if (isHeavyMethod && !requireHeavyConfirm(req, res)) return;
    const opComment = req.createOpComment?.(`operate:${method}:${dbName}.${colName}`);
    const governorClass = isHeavyMethod ? 'heavy' : 'interactive';
    let renamedCollectionName = null;

    const result = await runWithGovernor(req, governorClass, async () => {
      if (method === 'insertOne') {
        const document = parseObjectInputStrict(payload.document, 'payload.document', { allowEmpty: false, mode: 'document' });
        const insertResult = await collection.insertOne(document);
        return { insertedId: insertResult.insertedId, insertedCount: 1 };
      }

      if (method === 'insertMany') {
        const ordered = parseBooleanInput(payload.ordered, 'payload.ordered');
        const documents = parseArrayInputStrict(payload.documents, 'payload.documents', { allowEmpty: false });
        if (documents.length > 10000) throw createRequestError('Too many documents in one request (max 10000).');
        const parsedDocuments = documents.map((doc, index) => {
          if (!isPlainObject(doc)) throw createRequestError(`payload.documents[${index}] must be an object.`);
          return deserializeImportValue(doc);
        });
        const insertResult = await collection.insertMany(parsedDocuments, { ordered: ordered ?? false });
        const insertedCount = Object.keys(insertResult.insertedIds || {}).length;
        return { insertedIds: insertResult.insertedIds, insertedCount };
      }

      if (method === 'updateOne') {
        const filter = parseObjectInputStrict(payload.filter, 'payload.filter', { allowEmpty: true, mode: 'filter' });
        const update = parseUpdateSpecStrict(payload.update, 'payload.update');
        const options = { comment: opComment };
        const upsert = parseBooleanInput(payload.upsert, 'payload.upsert');
        if (upsert !== undefined) options.upsert = upsert;
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        const arrayFilters = parseArrayFiltersInputStrict(payload.arrayFilters, 'payload.arrayFilters');
        if (arrayFilters) options.arrayFilters = arrayFilters;
        const updateResult = await callMongoWithCommentFallback(
          (resolvedOptions) => collection.updateOne(filter, update, resolvedOptions),
          options,
        );
        return {
          matchedCount: updateResult.matchedCount || 0,
          modifiedCount: updateResult.modifiedCount || 0,
          upsertedCount: updateResult.upsertedCount || 0,
          upsertedId: updateResult.upsertedId ?? null,
        };
      }

      if (method === 'updateMany') {
        const filter = parseObjectInputStrict(payload.filter, 'payload.filter', { allowEmpty: true, mode: 'filter' });
        const update = parseUpdateSpecStrict(payload.update, 'payload.update');
        const options = { comment: opComment };
        const upsert = parseBooleanInput(payload.upsert, 'payload.upsert');
        if (upsert !== undefined) options.upsert = upsert;
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        const arrayFilters = parseArrayFiltersInputStrict(payload.arrayFilters, 'payload.arrayFilters');
        if (arrayFilters) options.arrayFilters = arrayFilters;
        const updateResult = await callMongoWithCommentFallback(
          (resolvedOptions) => collection.updateMany(filter, update, resolvedOptions),
          options,
        );
        return {
          matchedCount: updateResult.matchedCount || 0,
          modifiedCount: updateResult.modifiedCount || 0,
          upsertedCount: updateResult.upsertedCount || 0,
          upsertedId: updateResult.upsertedId ?? null,
        };
      }

      if (method === 'replaceOne') {
        const filter = parseObjectInputStrict(payload.filter, 'payload.filter', { allowEmpty: true, mode: 'filter' });
        const replacement = parseReplacementDocumentStrict(payload.replacement, 'payload.replacement');
        const options = { comment: opComment };
        const upsert = parseBooleanInput(payload.upsert, 'payload.upsert');
        if (upsert !== undefined) options.upsert = upsert;
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        const replaceResult = await callMongoWithCommentFallback(
          (resolvedOptions) => collection.replaceOne(filter, replacement, resolvedOptions),
          options,
        );
        return {
          matchedCount: replaceResult.matchedCount || 0,
          modifiedCount: replaceResult.modifiedCount || 0,
          upsertedCount: replaceResult.upsertedCount || 0,
          upsertedId: replaceResult.upsertedId ?? null,
        };
      }

      if (method === 'deleteOne') {
        const filter = parseObjectInputStrict(payload.filter, 'payload.filter', { allowEmpty: true, mode: 'filter' });
        const options = { comment: opComment };
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        const deleteResult = await callMongoWithCommentFallback(
          (resolvedOptions) => collection.deleteOne(filter, resolvedOptions),
          options,
        );
        return { deletedCount: deleteResult.deletedCount || 0 };
      }

      if (method === 'deleteMany') {
        const filter = parseObjectInputStrict(payload.filter, 'payload.filter', { allowEmpty: true, mode: 'filter' });
        const options = { comment: opComment, maxTimeMS: req.heavyBudget };
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        const deleteResult = await callMongoWithCommentFallback(
          (resolvedOptions) => collection.deleteMany(filter, resolvedOptions),
          options,
        );
        return { deletedCount: deleteResult.deletedCount || 0 };
      }

      if (method === 'remove') {
        const filterInput = payload.query ?? payload.filter ?? {};
        const filter = parseObjectInputStrict(filterInput, 'payload.filter', { allowEmpty: true, mode: 'filter' });
        const justOne = removeJustOne === true;
        const options = { comment: opComment };
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        if (justOne) {
          const deleteResult = await callMongoWithCommentFallback(
            (resolvedOptions) => collection.deleteOne(filter, resolvedOptions),
            options,
          );
          return { deletedCount: deleteResult.deletedCount || 0, justOne: true };
        }
        const deleteResult = await callMongoWithCommentFallback(
          (resolvedOptions) => collection.deleteMany(filter, resolvedOptions),
          { ...options, maxTimeMS: req.heavyBudget },
        );
        return { deletedCount: deleteResult.deletedCount || 0, justOne: false };
      }

      if (method === 'findOneAndUpdate') {
        const filter = parseObjectInputStrict(payload.filter, 'payload.filter', { allowEmpty: true, mode: 'filter' });
        const update = parseUpdateSpecStrict(payload.update, 'payload.update');
        const options = { comment: opComment, returnDocument: parseReturnDocumentOption(payload) };
        const sort = parseSortInputStrict(payload.sort, 'payload.sort');
        if (sort) options.sort = sort;
        const projection = parseProjectionInputStrict(payload.projection, 'payload.projection');
        if (projection) options.projection = projection;
        const upsert = parseBooleanInput(payload.upsert, 'payload.upsert');
        if (upsert !== undefined) options.upsert = upsert;
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        const arrayFilters = parseArrayFiltersInputStrict(payload.arrayFilters, 'payload.arrayFilters');
        if (arrayFilters) options.arrayFilters = arrayFilters;
        return buildFindOneAndResult(await callMongoWithCommentFallback(
          (resolvedOptions) => collection.findOneAndUpdate(filter, update, resolvedOptions),
          options,
        ));
      }

      if (method === 'findOneAndReplace') {
        const filter = parseObjectInputStrict(payload.filter, 'payload.filter', { allowEmpty: true, mode: 'filter' });
        const replacement = parseReplacementDocumentStrict(payload.replacement, 'payload.replacement');
        const options = { comment: opComment, returnDocument: parseReturnDocumentOption(payload) };
        const sort = parseSortInputStrict(payload.sort, 'payload.sort');
        if (sort) options.sort = sort;
        const projection = parseProjectionInputStrict(payload.projection, 'payload.projection');
        if (projection) options.projection = projection;
        const upsert = parseBooleanInput(payload.upsert, 'payload.upsert');
        if (upsert !== undefined) options.upsert = upsert;
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        return buildFindOneAndResult(await callMongoWithCommentFallback(
          (resolvedOptions) => collection.findOneAndReplace(filter, replacement, resolvedOptions),
          options,
        ));
      }

      if (method === 'findOneAndDelete') {
        const filter = parseObjectInputStrict(payload.filter, 'payload.filter', { allowEmpty: true, mode: 'filter' });
        const options = { comment: opComment };
        const sort = parseSortInputStrict(payload.sort, 'payload.sort');
        if (sort) options.sort = sort;
        const projection = parseProjectionInputStrict(payload.projection, 'payload.projection');
        if (projection) options.projection = projection;
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        return buildFindOneAndResult(await callMongoWithCommentFallback(
          (resolvedOptions) => collection.findOneAndDelete(filter, resolvedOptions),
          options,
        ));
      }

      if (method === 'findAndModify') {
        const queryInput = payload.query ?? payload.filter ?? {};
        const query = parseObjectInputStrict(queryInput, 'payload.query', { allowEmpty: true, mode: 'filter' });
        const remove = findAndModifyRemove === true;
        const projectionInput = payload.fields !== undefined ? payload.fields : payload.projection;
        const options = { comment: opComment };
        const sort = parseSortInputStrict(payload.sort, 'payload.sort');
        if (sort) options.sort = sort;
        const projection = parseProjectionInputStrict(projectionInput, 'payload.fields');
        if (projection) options.projection = projection;
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        if (remove) {
          return buildFindOneAndResult(await callMongoWithCommentFallback(
            (resolvedOptions) => collection.findOneAndDelete(query, resolvedOptions),
            options,
          ));
        }
        const upsert = parseBooleanInput(payload.upsert, 'payload.upsert');
        if (upsert !== undefined) options.upsert = upsert;
        options.returnDocument = parseReturnDocumentOption({
          returnDocument: payload.returnDocument,
          returnNewDocument: payload.new,
          returnOriginal: payload.returnOriginal,
        });
        const arrayFilters = parseArrayFiltersInputStrict(payload.arrayFilters, 'payload.arrayFilters');
        const bypassDocumentValidation = parseBooleanInput(payload.bypassDocumentValidation, 'payload.bypassDocumentValidation');
        if (bypassDocumentValidation !== undefined) options.bypassDocumentValidation = bypassDocumentValidation;
        const parsedUpdate = parseFindAndModifyUpdateOrReplacement(payload.update, 'payload.update');
        if (parsedUpdate.kind === 'replacement') {
          if (arrayFilters) throw createRequestError('payload.arrayFilters is only supported for update operator mode.');
          return buildFindOneAndResult(await callMongoWithCommentFallback(
            (resolvedOptions) => collection.findOneAndReplace(query, parsedUpdate.value, resolvedOptions),
            options,
          ));
        }
        if (arrayFilters) options.arrayFilters = arrayFilters;
        return buildFindOneAndResult(await callMongoWithCommentFallback(
          (resolvedOptions) => collection.findOneAndUpdate(query, parsedUpdate.value, resolvedOptions),
          options,
        ));
      }

      if (method === 'countDocuments') {
        const filterInput = payload.filter ?? payload.query ?? {};
        const filter = parseObjectInputStrict(filterInput, 'payload.filter', { allowEmpty: true, mode: 'filter' });
        const options = { comment: opComment };
        const hint = parseHintInputStrict(payload.hint, 'payload.hint');
        if (hint !== undefined) options.hint = hint;
        const collation = parseCollationInputStrict(payload.collation, 'payload.collation');
        if (collation) options.collation = collation;
        const limit = Number(payload.limit);
        if (Number.isFinite(limit) && limit >= 0) options.limit = Math.floor(limit);
        const skip = Number(payload.skip);
        if (Number.isFinite(skip) && skip >= 0) options.skip = Math.floor(skip);
        options.maxTimeMS = req.queryBudget.timeoutMs;
        let value = 0;
        try {
          value = await callMongoWithCommentFallback(
            (resolvedOptions) => collection.countDocuments(filter, resolvedOptions),
            options,
          );
        } catch (err) {
          if (isUnsupportedCommentOptionError(err) && Object.prototype.hasOwnProperty.call(options, 'comment')) {
            const fallbackOptions = { ...options };
            delete fallbackOptions.comment;
            value = await compatCount(collection, filter, req.caps, fallbackOptions);
          } else {
            value = await compatCount(collection, filter, req.caps, options);
          }
        }
        return { value };
      }

      if (method === 'estimatedDocumentCount') {
        const options = { comment: opComment, maxTimeMS: req.queryBudget.timeoutMs };
        let value;
        try {
          value = await compatEstCount(collection, req.caps, options);
        } catch (err) {
          if (!(isUnsupportedCommentOptionError(err) && Object.prototype.hasOwnProperty.call(options, 'comment'))) throw err;
          const fallbackOptions = { ...options };
          delete fallbackOptions.comment;
          value = await compatEstCount(collection, req.caps, fallbackOptions);
        }
        return { value };
      }

      if (method === 'renameCollection') {
        const to = String(payload.to ?? payload.newName ?? '').trim();
        if (!to) throw createRequestError('payload.to (or payload.newName) is required.');
        if (to.includes('\0')) throw createRequestError('payload.to contains an invalid null character.');
        if (to === colName) return { from: colName, to, renamed: false };
        const dropTarget = parseBooleanInput(payload.dropTarget, 'payload.dropTarget');
        const options = {};
        if (dropTarget !== undefined) options.dropTarget = dropTarget;
        await collection.rename(to, options);
        renamedCollectionName = to;
        return { from: colName, to, renamed: true };
      }

      if (method === 'dropIndexes') {
        const namesRaw = payload.names;
        const options = { comment: opComment, maxTimeMS: req.heavyBudget };
        if (namesRaw === undefined || namesRaw === null || namesRaw === '' || namesRaw === '*') {
          const commandResult = await collection.dropIndexes(options);
          return { dropped: '*', commandResult };
        }
        const names = Array.isArray(namesRaw)
          ? namesRaw.map((value) => String(value || '').trim()).filter(Boolean)
          : [String(namesRaw || '').trim()].filter(Boolean);
        if (names.length === 0) throw createRequestError('payload.names must not be empty.');
        if (names.some((name) => name === '_id_')) throw createRequestError('Cannot drop the required _id index.');
        const dropped = [];
        for (const name of names) {
          await collection.dropIndex(name, options);
          dropped.push(name);
        }
        return { dropped };
      }

      if (method === 'hideIndex' || method === 'unhideIndex') {
        const name = String(payload.name || '').trim();
        if (!name) throw createRequestError('payload.name is required.');
        const hidden = method === 'hideIndex';
        const command = { collMod: colName, index: { name, hidden } };
        if (opComment) command.comment = opComment;
        command.maxTimeMS = req.queryBudget.timeoutMs;
        const commandResult = await req.conn.client.db(dbName).command(command);
        return { name, hidden, ok: commandResult?.ok ?? 1 };
      }

      if (method === 'validateCollection') {
        const full = parseBooleanInput(payload.full, 'payload.full');
        const command = { validate: colName };
        if (full !== undefined) command.full = full;
        if (opComment) command.comment = opComment;
        const commandResult = await req.conn.client.db(dbName).command(command);
        return {
          valid: commandResult?.valid !== false,
          result: String(commandResult?.result || ''),
          warnings: Array.isArray(commandResult?.warnings) ? commandResult.warnings : [],
          ok: commandResult?.ok ?? 1,
        };
      }

      if (method === 'reIndex') {
        const command = { reIndex: colName };
        if (opComment) command.comment = opComment;
        const commandResult = await req.conn.client.db(dbName).command(command);
        return {
          ok: commandResult?.ok ?? 1,
          nIndexesWas: commandResult?.nIndexesWas ?? null,
          nIndexes: commandResult?.nIndexes ?? null,
          msg: commandResult?.msg || '',
        };
      }

      if (method === 'bulkWrite') {
        const operations = parseBulkWriteOperations(payload.operations);
        const options = { comment: opComment };
        const ordered = parseBooleanInput(payload.ordered, 'payload.ordered');
        if (ordered !== undefined) options.ordered = ordered;
        const bypassDocumentValidation = parseBooleanInput(payload.bypassDocumentValidation, 'payload.bypassDocumentValidation');
        if (bypassDocumentValidation !== undefined) options.bypassDocumentValidation = bypassDocumentValidation;
        const bulkResult = await collection.bulkWrite(operations, options);
        return {
          insertedCount: bulkResult.insertedCount || 0,
          matchedCount: bulkResult.matchedCount || 0,
          modifiedCount: bulkResult.modifiedCount || 0,
          deletedCount: bulkResult.deletedCount || 0,
          upsertedCount: bulkResult.upsertedCount || 0,
          insertedIds: bulkResult.insertedIds || {},
          upsertedIds: bulkResult.upsertedIds || {},
        };
      }

      throw createRequestError(`Unsupported method "${method}".`);
    });

    const readOnlyMethods = new Set(['countDocuments', 'estimatedDocumentCount', 'validateCollection']);
    if (!readOnlyMethods.has(method)) {
      invalidateCollectionCaches(req, dbName, colName);
    }
    if (renamedCollectionName) {
      invalidateCollectionCaches(req, dbName, renamedCollectionName);
      invalidateDatabaseCaches(req, dbName);
    }
    const auditAction = (
      method === 'insertMany' ? 'insert_many'
      : method === 'deleteMany' || (method === 'remove' && removeJustOne !== true) ? 'delete_many'
      : method === 'bulkWrite' ? 'bulk_write'
      : method === 'renameCollection' ? 'rename_collection'
      : method === 'dropIndexes' ? 'drop_index'
      : method === 'hideIndex' || method === 'unhideIndex' ? 'update_index'
      : method === 'countDocuments' || method === 'estimatedDocumentCount' ? 'query'
      : method === 'validateCollection' || method === 'reIndex' ? 'admin'
      : method === 'deleteOne'
        || method === 'findOneAndDelete'
        || (method === 'remove' && removeJustOne === true)
        || (method === 'findAndModify' && findAndModifyRemove === true)
        ? 'delete'
      : method === 'insertOne' ? 'insert'
      : method === 'updateMany' ? 'update_many'
      : 'update'
    );
    const affectedCount = Number(
      result?.insertedCount
      ?? result?.deletedCount
      ?? result?.modifiedCount
      ?? result?.matchedCount
      ?? 0
    );
    auditReq(req, auditAction, {
      db: dbName,
      col: colName,
      method,
      count: Number.isFinite(affectedCount) ? affectedCount : 0,
      ...(source ? { source } : {}),
    });
    res.json({ ok: true, method, db: dbName, col: colName, result });
  } catch (err) {
    sendError(res, err);
  }
});

app.post('/api/databases/:db/collections/:col/aggregate', heavyLimiter, getConnection, async (req, res) => {
  const qid=randomUUID().replace(/-/g,''), t0=Date.now();
  const source = parseAuditSource(req.query?.source);
  const requestDeadline = Date.now() + req.queryBudget.timeoutMs;
  let clientClosed = false;
  const aggregateComment = req.createOpComment?.(`aggregate:${req.params.db}.${req.params.col}`);
  const markClientClosed = () => {
    if (!res.writableEnded) clientClosed = true;
  };
  req.on('aborted', markClientClosed);
  res.on('close', markClientClosed);
  try {
    const col=req.conn.client.db(req.params.db).collection(req.params.col), pipeline=req.body.pipeline||[], warnings=[];
    const aggHint = typeof req.body?.hint === 'string' && req.body.hint.trim() && req.body.hint !== 'auto'
      ? req.body.hint.trim()
      : null;
    validatePipeline(pipeline, req.execConfig);
    for (const stage of pipeline) { const k=Object.keys(stage)[0]; if(k==='$facet'&&!req.caps.hasAggFacet) warnings.push('$facet requires 3.4+'); if(k==='$lookup'&&!req.caps.hasAggLookup) warnings.push('$lookup requires 3.2+'); if(k==='$merge'&&!req.caps.hasMergeStage) warnings.push('$merge requires 4.2+'); if(k==='$unionWith'&&!req.caps.hasUnionWith) warnings.push('$unionWith requires 4.4+'); if((k==='$densify'||k==='$fill')&&!req.caps.hasDensifyFill) warnings.push(`${k} requires 5.1+`); }
    const maxR = clampNumber(req.queryBudget.limit || req.execConfig.maxResultSize, QUERY_LIMIT_MIN, QUERY_LIMIT_MAX);
    const ao={};
    ao.maxTimeMS = req.queryBudget.timeoutMs;
    if (aggregateComment) ao.comment = aggregateComment;
    if (aggHint) ao.hint = aggHint;
    if(req.execConfig.allowDiskUse) ao.allowDiskUse=true;
    // Inject $limit before execution to prevent loading millions of docs into memory.
    // Skip for $out/$merge (write stages) — they don't return documents.
    const lastStage = pipeline.length > 0 ? Object.keys(pipeline[pipeline.length - 1])[0] : null;
    const isWritingStage = lastStage === '$out' || lastStage === '$merge';
    const effectivePipeline = isWritingStage ? pipeline : [...pipeline, { $limit: maxR + 1 }];
    runningQueries.set(qid, {connId:req.connId,t0,type:'aggregate'});
    const aggregateBatchSize = Math.max(1, Math.min(maxR + 1, 1000));
    const aggCursor = col.aggregate(effectivePipeline, ao).batchSize(aggregateBatchSize);
    const untrackAggCursor = trackActiveCursor(req.connId, aggCursor);
    const closeAggCursor = () => {
      untrackAggCursor();
      aggCursor.close().catch(() => {});
    };
    req.on('aborted', closeAggCursor);
    res.on('close', closeAggCursor);
    const fetchDeadline = requestDeadline;
    const aggregateGovernorReq = {
      ...req,
      heavyBudget: Math.max(
        GOVERNOR_QUEUE_BASE_POLL_MS,
        Math.min(
          Number(req.heavyBudget) || HEAVY_TIMEOUT_DEFAULT_MS,
          Number(req.queryBudget?.timeoutMs) || HEAVY_TIMEOUT_DEFAULT_MS,
        ),
      ),
    };
    const governorClass = isWritingStage ? 'heavy' : 'interactive';
    const governorReq = governorClass === 'heavy' ? aggregateGovernorReq : req;
    const results = await runWithGovernor(governorReq, governorClass, async () => {
      const rows = [];
      try {
        while (rows.length < maxR + 1) {
          const next = await withTimeout(aggCursor.next(), getRemainingMs(fetchDeadline));
          if (!next) break;
          rows.push(next);
        }
      } finally {
        untrackAggCursor();
        await aggCursor.close().catch(() => {});
      }
      return rows;
    });
    if (clientClosed || res.writableEnded || res.destroyed) return;
    const elapsed=Date.now()-t0;
    const trimmed = !isWritingStage && results.length > maxR;
    const visibleResults = trimmed ? results.slice(0, maxR) : results;
    auditReq(req,'aggregate',{
      db:req.params.db,
      col:req.params.col,
      elapsed,
      stages:pipeline.length,
      count:visibleResults.length,
      ...(source ? { source } : {}),
    });
    const r={results:visibleResults, total:visibleResults.length, _elapsed:elapsed};
    r.budget = {
      timeoutMs: req.queryBudget.timeoutMs,
      limit: maxR,
      overrideApplied: req.queryBudget.overrideApplied,
    };
    if(trimmed) r.trimmed=true; if(warnings.length) r.warnings=warnings; if(elapsed>5000) r._slow=true;
    res.json(r);
  } catch(err) {
    if (clientClosed || res.headersSent || res.writableEnded || res.destroyed) return;
    if((err.message||'').includes('Unrecognized pipeline stage')) err.message+=` (unsupported on MongoDB ${req.conn.versionStr})`;
    sendError(res, err);
  }
  finally { runningQueries.delete(qid); }
});

app.post('/api/databases/:db/collections/:col/explain', getConnection, async (req, res) => {
  try {
    const db=req.conn.client.db(req.params.db), {type,filter,pipeline,sort,hint,limit,verbosity:reqVerbosity}=req.body;
    const verbosity = reqVerbosity === 'queryPlanner' ? 'queryPlanner' : 'executionStats';
    const requestDeadline = Date.now() + req.queryBudget.timeoutMs;
    const remainingMs = (minMs = 1) => {
      const raw = requestDeadline - Date.now();
      if (raw <= 0) return 0;
      return Math.max(minMs, raw);
    };
    const explainComment = req.createOpComment?.(`explain:${req.params.db}.${req.params.col}`);
    let explain;
    if (type==='aggregate'&&pipeline) {
      const parsedLimit = parseInt(limit, 10);
      const budgetLimit = Number(req.queryBudget?.limit) || QUERY_LIMIT_DEFAULT;
      const safeLimit = Number.isFinite(parsedLimit) && parsedLimit > 0
        ? Math.min(parsedLimit, budgetLimit)
        : budgetLimit;
      const pipelineInput = Array.isArray(pipeline) ? pipeline : [];
      const lastStage = pipelineInput.length > 0 ? Object.keys(pipelineInput[pipelineInput.length - 1] || {})[0] : null;
      const isWritingStage = lastStage === '$out' || lastStage === '$merge';
      // Align aggregate explain with aggregate run behavior: cap read volume by budget limit.
      const explainPipeline = isWritingStage
        ? pipelineInput
        : [...pipelineInput, { $limit: Math.max(1, safeLimit) + 1 }];
      const aggregateHint = typeof hint === 'string' && hint.trim() && hint !== 'auto'
        ? hint.trim()
        : null;
      const aggregateExplainCmd = { aggregate: req.params.col, pipeline: explainPipeline, cursor: {} };
      if (aggregateHint) aggregateExplainCmd.hint = aggregateHint;
      const queueMs = remainingMs(1);
      if (queueMs <= 0) throw timeoutError();
      const explainReq = {
        ...req,
        queryBudget: {
          ...req.queryBudget,
          timeoutMs: Math.max(GOVERNOR_QUEUE_BASE_POLL_MS, queueMs),
        },
      };
      explain = await runWithGovernor(explainReq, 'interactive', async () => {
        const opMs = remainingMs(1);
        if (opMs <= 0) throw timeoutError();
        aggregateExplainCmd.maxTimeMS = opMs;
        return withTimeout(
          db.command({ explain: aggregateExplainCmd, verbosity, comment: explainComment }, { maxTimeMS: opMs }),
          opMs,
        );
      });
    } else {
      const findFilter = parseFilterInputStrict(filter ?? {}, 'filter');
      const sortObj = parseSortInputStrict(sort ?? {}, 'sort') || {};
      const cmd = { find:req.params.col, filter: findFilter };
      if (typeof hint === 'string' && hint.trim() && hint !== 'auto') cmd.hint = hint.trim();
      if (Object.keys(sortObj).length > 0) cmd.sort = sortObj;
      const parsedLimit = parseInt(limit, 10);
      const budgetLimit = Number(req.queryBudget?.limit) || QUERY_LIMIT_DEFAULT;
      const safeLimit = Number.isFinite(parsedLimit) && parsedLimit > 0
        ? Math.min(parsedLimit, budgetLimit)
        : budgetLimit;
      cmd.limit = Math.max(1, safeLimit);
      cmd.comment = explainComment;
      const queueMs = remainingMs(1);
      if (queueMs <= 0) throw timeoutError();
      const explainReq = {
        ...req,
        queryBudget: {
          ...req.queryBudget,
          timeoutMs: Math.max(GOVERNOR_QUEUE_BASE_POLL_MS, queueMs),
        },
      };
      explain = await runWithGovernor(explainReq, 'interactive', async () => {
        const opMs = remainingMs(1);
        if (opMs <= 0) throw timeoutError();
        cmd.maxTimeMS = opMs;
        return withTimeout(
          db.command({explain:cmd,verbosity}, { maxTimeMS: opMs }),
          opMs,
        );
      });
    }
    res.json({
      explain,
      summary: extractExplainSummary(explain),
      budget: {
        timeoutMs: req.queryBudget.timeoutMs,
        limit: req.queryBudget.limit,
        overrideApplied: req.queryBudget.overrideApplied,
      },
    });
  } catch(err) { sendError(res, err); }
});

app.post('/api/databases/:db/collections/:col/export', getConnection, async (req, res) => {
  let reqClosed = false;
  const source = parseAuditSource(req.query?.source);
  const activeCursors = new Set();
  const cursorReleasers = new WeakMap();
  res.on('close', () => {
    if (res.writableEnded) return;
    reqClosed = true;
    for (const cursor of activeCursors) {
      const release = cursorReleasers.get(cursor);
      if (typeof release === 'function') {
        try { release(); } catch {}
        cursorReleasers.delete(cursor);
      }
      if (typeof cursor?.close === 'function') cursor.close().catch(() => {});
    }
  });
  try {
    const col=req.conn.client.db(req.params.db).collection(req.params.col);
    const format = String(req.body?.format || 'json').trim().toLowerCase();
    if (format !== 'json' && format !== 'csv') throw createRequestError('format must be "json" or "csv".');
    const rawOutput = String(req.query?.raw || '') === '1' || req.body?.raw === true;
    const exportFilename = `${safeFilename(req.params.db)}.${safeFilename(req.params.col)}.${format === 'csv' ? 'csv' : 'json'}`;
    const filterInput = Object.prototype.hasOwnProperty.call(req.body || {}, 'filter') ? req.body.filter : '{}';
    const sortInput = Object.prototype.hasOwnProperty.call(req.body || {}, 'sort') ? req.body.sort : '{}';
    const projectionInput = Object.prototype.hasOwnProperty.call(req.body || {}, 'projection') ? req.body.projection : '{}';
    const limitInput = Object.prototype.hasOwnProperty.call(req.body || {}, 'limit') ? req.body.limit : '1000';
    const filter = parseFilterInputStrict(filterInput, 'filter');
    const sort = parseSortInputStrict(sortInput, 'sort') || {};
    const projection = parseProjectionInputStrict(projectionInput, 'projection') || {};
    const parsedLimit = parseExportLimitInput(limitInput, 'limit', 1000);
    let limitMode = parsedLimit.mode;
    let limit = parsedLimit.limit;
    if (limitMode === 'exact') {
      const exactExportComment = req.createOpComment?.(`export-exact:${req.params.db}.${req.params.col}`);
      const counted = await runWithGovernor(
        req,
        'metadata',
        async () => withTimeout(
          compatCount(col, filter, req.caps, { comment: exactExportComment, maxTimeMS: req.heavyBudget }),
          req.heavyBudget,
        ),
      );
      limit = Math.max(0, Math.min(Number(counted) || 0, EXPORT_LIMIT_MAX));
    }
    if (limitMode === 'unlimited') {
      limit = Number.POSITIVE_INFINITY;
    }
    const effectiveLimit = Number.isFinite(limit)
      ? Math.max(0, Math.min(Number(limit) || 0, EXPORT_LIMIT_MAX))
      : Number.POSITIVE_INFINITY;
    const csvStrictIncludeKeys = strictIncludeProjectionKeys(projection);
    if (
      (
        limitMode === 'unlimited'
        || (Number.isFinite(effectiveLimit) && effectiveLimit > 5000)
      )
      && !req.heavyConfirmed
    ) {
      return res.status(428).json({ error: 'Large export requires confirmation header X-Heavy-Confirm: 1.', errorType: 'precondition' });
    }
    const buildCursor = () => {
      let cursor = col.find(filter, { projection, maxTimeMS: req.heavyBudget });
      if (sort && typeof sort === 'object' && Object.keys(sort).length > 0) cursor = cursor.sort(sort);
      const batchSize = Number.isFinite(effectiveLimit)
        ? Math.max(1, Math.min(effectiveLimit, EXPORT_CURSOR_BATCH_SIZE))
        : EXPORT_CURSOR_BATCH_SIZE;
      cursor = cursor.batchSize(batchSize);
      cursorReleasers.set(cursor, trackActiveCursor(req.connId, cursor));
      activeCursors.add(cursor);
      return cursor;
    };
    const closeCursor = async (cursor) => {
      if (!cursor) return;
      const release = cursorReleasers.get(cursor);
      if (typeof release === 'function') {
        try { release(); } catch {}
        cursorReleasers.delete(cursor);
      }
      activeCursors.delete(cursor);
      if (typeof cursor?.close === 'function') {
        await cursor.close().catch(() => {});
      }
    };
    const exportDeadline = Date.now() + req.heavyBudget;
    const writeEscaped = async (chunk) => {
      if (reqClosed) return;
      await writeChunk(res, escapeJsonStringChunk(chunk));
    };
    await runWithGovernor(req, 'heavy', async () => {
      if (rawOutput) {
        res.setHeader('Content-Type', format === 'csv' ? 'text/csv; charset=utf-8' : 'application/json; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="${exportFilename}"`);
      } else {
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
      }
      if (format === 'csv') {
        if (csvStrictIncludeKeys) {
          if (rawOutput) {
            if (csvStrictIncludeKeys.length > 0) await writeChunk(res, csvStrictIncludeKeys.join(','));
          } else {
            await writeChunk(res, '{"format":"csv","data":"');
            if (csvStrictIncludeKeys.length > 0) await writeEscaped(csvStrictIncludeKeys.join(','));
          }
          let count = 0;
          let rowCursor = null;
          try {
            rowCursor = buildCursor();
            while (count < effectiveLimit) {
              const next = await withTimeout(rowCursor.next(), getRemainingMs(exportDeadline));
              if (!next) break;
              const serialized = safeSerializeForExport(next);
              const row = csvStrictIncludeKeys.map((key) => csvCell(getValueByPath(serialized, key))).join(',');
              if (count > 0 || csvStrictIncludeKeys.length > 0) {
                if (rawOutput) await writeChunk(res, '\n');
                else await writeEscaped('\n');
              }
              if (rawOutput) await writeChunk(res, row);
              else await writeEscaped(row);
              count += 1;
            }
          } finally {
            await closeCursor(rowCursor);
          }
          if (reqClosed) return;
          if (!rawOutput) await writeChunk(res, `","count":${count}}`);
          res.end();
          auditReq(req,'export',{
            db: req.params.db,
            col: req.params.col,
            format:'csv',
            count,
            ...(source ? { source } : {}),
          });
          return;
        }
        let keyCursor = null;
        const keySet = new Set();
        let scanned = 0;
        try {
          keyCursor = buildCursor();
          while (scanned < effectiveLimit) {
            const next = await withTimeout(keyCursor.next(), getRemainingMs(exportDeadline));
            if (!next) break;
            const serialized = safeSerializeForExport(next);
            for (const key of Object.keys(serialized || {})) keySet.add(key);
            scanned += 1;
          }
        } finally {
          await closeCursor(keyCursor);
        }
        const keys = [...keySet];
        if (rawOutput) {
          if (keys.length > 0) await writeChunk(res, keys.join(','));
        } else {
          await writeChunk(res, '{"format":"csv","data":"');
          if (keys.length > 0) await writeEscaped(keys.join(','));
        }
        let count = 0;
        let rowCursor = null;
        try {
          rowCursor = buildCursor();
          while (count < effectiveLimit) {
            const next = await withTimeout(rowCursor.next(), getRemainingMs(exportDeadline));
            if (!next) break;
            const serialized = safeSerializeForExport(next);
            const row = keys.map((key) => csvCell(serialized?.[key])).join(',');
            if (count > 0 || keys.length > 0) {
              if (rawOutput) await writeChunk(res, '\n');
              else await writeEscaped('\n');
            }
            if (rawOutput) await writeChunk(res, row);
            else await writeEscaped(row);
            count += 1;
          }
        } finally {
          await closeCursor(rowCursor);
        }
        if (reqClosed) return;
        if (!rawOutput) await writeChunk(res, `","count":${count}}`);
        res.end();
        auditReq(req,'export',{
          db: req.params.db,
          col: req.params.col,
          format:'csv',
          count,
          ...(source ? { source } : {}),
        });
        return;
      }
      if (rawOutput) await writeChunk(res, '[');
      else {
        await writeChunk(res, '{"format":"json","data":"');
        await writeEscaped('[');
      }
      let count = 0;
      let jsonCursor = null;
      try {
        jsonCursor = buildCursor();
        while (count < effectiveLimit) {
          const next = await withTimeout(jsonCursor.next(), getRemainingMs(exportDeadline));
          if (!next) break;
          if (count > 0) {
            if (rawOutput) await writeChunk(res, ',');
            else await writeEscaped(',');
          }
          const serialized = JSON.stringify(safeSerializeForExport(next));
          if (rawOutput) await writeChunk(res, serialized);
          else await writeEscaped(serialized);
          count += 1;
        }
      } finally {
        await closeCursor(jsonCursor);
      }
      if (rawOutput) await writeChunk(res, ']');
      else await writeEscaped(']');
      if (reqClosed) return;
      if (!rawOutput) await writeChunk(res, `","count":${count}}`);
      res.end();
      auditReq(req,'export',{
        db: req.params.db,
        col: req.params.col,
        format:'json',
        count,
        ...(source ? { source } : {}),
      });
    });
  } catch(err) {
    console.error('[EXPORT_COLLECTION] stream failed', {
      db: req?.params?.db,
      col: req?.params?.col,
      connId: req?.connId,
      message: err?.message,
      stack: err?.stack,
      reqClosed,
      headersSent: res.headersSent,
      writableEnded: res.writableEnded,
    });
    if (reqClosed || res.headersSent) {
      if (!res.writableEnded) res.end();
      return;
    }
    sendError(res, err);
  }
});

app.get('/api/databases/:db/collections/:col/indexes', getConnection, async (req, res) => {
  const limit = clampNumber(req.queryBudget.limit || QUERY_LIMIT_DEFAULT, QUERY_LIMIT_MIN, QUERY_LIMIT_MAX);
  const source = parseAuditSource(req.query?.source);
  const buildResponse = (allIndexes = [], dataSource = 'live', ts = Date.now()) => {
    const total = Array.isArray(allIndexes) ? allIndexes.length : 0;
    const indexes = Array.isArray(allIndexes) ? allIndexes.slice(0, limit) : [];
    return {
      indexes,
      total,
      limit,
      truncated: total > indexes.length,
      _source: dataSource,
      _ts: ts,
    };
  };
  try {
    const force = String(req.query.refresh || '') === '1';
    const cacheKey = getIndexesCacheKey(req.params.db, req.params.col);
    if (!force) {
      const cached = sessionMetadataCache.get(req.connId, cacheKey);
      if (cached) return res.json(buildResponse(cached.value || [], cached.source || 'cache', cached.ts));
    }
    const indexes = await runWithGovernor(
      req,
      'metadata',
      async () => withTimeout(req.conn.client.db(req.params.db).collection(req.params.col).indexes(), req.queryBudget.timeoutMs),
    );
    try {
      const s = await runWithGovernor(
        req,
        'metadata',
        async () => withTimeout(req.conn.client.db(req.params.db).command({ collStats: req.params.col }), req.queryBudget.timeoutMs),
      );
      for (const i of indexes) i.size = s.indexSizes?.[i.name] || 0;
    } catch {}
    sessionMetadataCache.set(req.connId, cacheKey, indexes, getMetadataCacheConfig().ttlIndexListMs, 'live');
    if (source) {
      auditReq(req, 'metadata', {
        db: req.params.db,
        col: req.params.col,
        method: 'listIndexes',
        source,
        count: indexes.length,
      });
    }
    res.json(buildResponse(indexes, 'live', Date.now()));
  } catch {
    try {
      const indexes = await runWithGovernor(
        req,
        'metadata',
        async () => withTimeout(req.conn.client.db(req.params.db).collection(req.params.col).listIndexes().toArray(), req.queryBudget.timeoutMs),
      );
      sessionMetadataCache.set(req.connId, getIndexesCacheKey(req.params.db, req.params.col), indexes, getMetadataCacheConfig().ttlIndexListMs, 'live');
      if (source) {
        auditReq(req, 'metadata', {
          db: req.params.db,
          col: req.params.col,
          method: 'listIndexes',
          source,
          count: indexes.length,
        });
      }
      res.json(buildResponse(indexes, 'live', Date.now()));
    } catch(err) { sendError(res,err); }
  }
});

app.post('/api/databases/:db/collections/:col/indexes', getConnection, async (req, res) => {
  try {
    const source = parseAuditSource(req.query?.source);
    const k=req.body.keys||{};
    if(Object.values(k).includes('$**')&&!req.caps.hasWildcardIndexes) return res.status(400).json({error:'Wildcard indexes require 4.2+',errorType:'version'});
    const r = await runWithGovernor(req, 'heavy', async () => req.conn.client.db(req.params.db).collection(req.params.col).createIndex(k,req.body.options||{}));
    invalidateCollectionCaches(req, req.params.db, req.params.col);
    auditReq(req,'create_index',{
      db:req.params.db,
      col:req.params.col,
      name:r,
      ...(source ? { source } : {}),
    });
    res.json({name:r,ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.delete('/api/databases/:db/collections/:col/indexes/:name', getConnection, async (req, res) => {
  try {
    const source = parseAuditSource(req.query?.source);
    if (!requireHeavyConfirm(req, res)) return;
    await runWithGovernor(req, 'heavy', async () => req.conn.client.db(req.params.db).collection(req.params.col).dropIndex(req.params.name));
    invalidateCollectionCaches(req, req.params.db, req.params.col);
    auditReq(req,'drop_index',{
      db:req.params.db,
      col:req.params.col,
      name:req.params.name,
      ...(source ? { source } : {}),
    });
    res.json({ok:true});
  }
  catch(err) { sendError(res, err); }
});

app.get('/api/audit', getConnection, (req, res) => {
  const limit = Math.max(1, Math.min(parseInt(req.query.limit) || 200, 1000));
  const action = typeof req.query.action === 'string' ? req.query.action.trim() : '';
  const source = parseAuditSource(req.query.source);
  const method = typeof req.query.method === 'string' ? req.query.method.trim() : '';
  const scope = parseAuditScope(req.query.scope);
  const search = typeof req.query.search === 'string' ? req.query.search.trim().toLowerCase() : '';
  const fromTs = Number(req.query.from) || 0;
  const toTs = Number(req.query.to) || 0;

  let entries = auditLog.filter((entry) => entry.connId === req.connId);
  if (action) entries = entries.filter((entry) => entry.action === action);
  if (source) entries = entries.filter((entry) => String(entry.source || '').toLowerCase() === source);
  if (method) entries = entries.filter((entry) => String(entry.method || '').toLowerCase() === method.toLowerCase());
  if (scope) entries = entries.filter((entry) => String(entry.scope || '').toLowerCase() === scope);
  if (fromTs > 0) entries = entries.filter((entry) => entry.ts >= fromTs);
  if (toTs > 0) entries = entries.filter((entry) => entry.ts <= toTs);
  if (search) {
    entries = entries.filter((entry) => JSON.stringify(entry).toLowerCase().includes(search));
  }

  const total = entries.length;
  const items = entries.slice(-limit).reverse();
  res.json({ entries: items, total });
});

app.post('/api/console/:shell/sessions', getConnection, async (req, res) => {
  try {
    if (req.adminKeyRequired && !req.adminVerified) {
      return res.status(403).json({ error: 'Admin access key is required to use consoles.', errorType: 'admin_required' });
    }
    const shellMode = normalizeConsoleShell(req.params.shell);
    if (!shellMode) return res.status(404).json({ error: 'Console shell not found.', errorType: 'not_found' });
    const scope = normalizeConsoleScopeInput(req.body?.scope || {});
    const session = createMongoshSession(req, scope, shellMode);
    const effectiveShellMode = session?.mode || shellMode;
    auditReq(req, 'console_open', {
      source: effectiveShellMode,
      scope: scope.level,
      db: scope.db || null,
      col: scope.collection || null,
      method: 'session',
      sessionId: session.id,
    });
    res.json({
      ok: true,
      mode: effectiveShellMode,
      sessionId: session.id,
      scope: session.scope,
      activeDb: session.activeDb || null,
      createdAt: session.createdAt,
    });
  } catch (err) {
    sendError(res, err, 'Failed to open console session');
  }
});

app.post('/api/console/:shell/sessions/:sessionId/command', getConnection, async (req, res) => {
  try {
    const shellMode = normalizeConsoleShell(req.params.shell);
    if (!shellMode) return res.status(404).json({ error: 'Console shell not found.', errorType: 'not_found' });
    const session = getMongoshSessionForRequest(req, req.params.sessionId);
    if (!session || session.mode !== shellMode) return res.status(404).json({ error: 'Console session not found.', errorType: 'not_found' });
    if (session.finalized) return res.status(410).json({ error: 'Console session is already closed.', errorType: 'state' });
    const checked = validateMongoshCommandForScope(req.body?.command, session.scope);
    const payload = checked.command.endsWith('\n') ? checked.command : `${checked.command}\n`;
    session.lastActivity = Date.now();
    session.user = req.auditUser || session.user;
    session.process.stdin.write(payload);
    emitMongoshEvent(session, 'command', { text: checked.command });
    if (checked.meta.kind === 'use' && session.scope.level === 'global') {
      session.activeDb = checked.meta.targetDb || session.activeDb;
    }
    auditReq(req, 'console_command', {
      source: session.mode || shellMode,
      scope: session.scope.level,
      db: getMongoshSessionAuditDb(session),
      col: session.scope.collection || checked.meta.targetCollection || null,
      method: checked.meta.method || checked.meta.kind || 'script',
      sessionId: session.id,
    });
    res.json({ ok: true, sessionId: session.id });
  } catch (err) {
    sendError(res, err, 'Failed to execute command');
  }
});

app.post('/api/console/:shell/sessions/:sessionId/interrupt', getConnection, async (req, res) => {
  try {
    const shellMode = normalizeConsoleShell(req.params.shell);
    if (!shellMode) return res.status(404).json({ error: 'Console shell not found.', errorType: 'not_found' });
    const session = getMongoshSessionForRequest(req, req.params.sessionId);
    if (!session || session.mode !== shellMode) return res.status(404).json({ error: 'Console session not found.', errorType: 'not_found' });
    if (session.finalized) return res.status(410).json({ error: 'Console session is already closed.', errorType: 'state' });
    session.lastActivity = Date.now();
    try { session.process.kill('SIGINT'); } catch {}
    emitMongoshEvent(session, 'system', { text: 'interrupt signal sent (SIGINT)' });
    auditReq(req, 'console_interrupt', {
      source: session.mode || shellMode,
      scope: session.scope.level,
      db: getMongoshSessionAuditDb(session),
      col: session.scope.collection || null,
      method: 'interrupt',
      sessionId: session.id,
    });
    res.json({ ok: true, sessionId: session.id });
  } catch (err) {
    sendError(res, err, 'Failed to interrupt command');
  }
});

app.get('/api/console/:shell/sessions/:sessionId/stream', getConnection, async (req, res) => {
  const shellMode = normalizeConsoleShell(req.params.shell);
  if (!shellMode) return res.status(404).json({ error: 'Console shell not found.', errorType: 'not_found' });
  const session = getMongoshSessionForRequest(req, req.params.sessionId);
  if (!session || session.mode !== shellMode) return res.status(404).json({ error: 'Console session not found.', errorType: 'not_found' });

  res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  if (typeof res.flushHeaders === 'function') {
    try { res.flushHeaders(); } catch {}
  }

  writeSseEvent(res, 'ready', {
    sessionId: session.id,
    mode: session.mode || shellMode,
    scope: session.scope,
    activeDb: session.activeDb || null,
    finalized: session.finalized === true,
  });

  const lastEventId = Number(req.headers['last-event-id'] || req.query?.since || 0);
  const replayFrom = Number.isFinite(lastEventId) ? Math.max(0, Math.round(lastEventId)) : 0;
  for (const event of session.events) {
    if (event.id <= replayFrom) continue;
    writeSseEvent(res, 'message', event, event.id);
  }

  if (session.finalized) {
    const exitPayload = session.finalizedMeta || {
      code: null,
      signal: null,
      reason: 'closed',
      forced: false,
    };
    writeSseEvent(res, 'end', exitPayload, null);
    try { res.end(); } catch {}
    return;
  }

  const subscriber = { res };
  session.subscribers.add(subscriber);
  session.lastActivity = Date.now();

  const heartbeat = setInterval(() => {
    try {
      res.write(`: ping ${Date.now()}\n\n`);
      if (typeof res.flush === 'function') {
        try { res.flush(); } catch {}
      }
    } catch {
      clearInterval(heartbeat);
      session.subscribers.delete(subscriber);
    }
  }, MONGOSH_SESSION_HEARTBEAT_MS);

  const handleClose = () => {
    clearInterval(heartbeat);
    session.subscribers.delete(subscriber);
  };
  req.on('close', handleClose);
  req.on('aborted', handleClose);
});

app.delete('/api/console/:shell/sessions/:sessionId', getConnection, async (req, res) => {
  try {
    const shellMode = normalizeConsoleShell(req.params.shell);
    if (!shellMode) return res.status(404).json({ error: 'Console shell not found.', errorType: 'not_found' });
    const session = getMongoshSessionForRequest(req, req.params.sessionId);
    if (!session || session.mode !== shellMode) return res.json({ ok: true, alreadyClosed: true, sessionId: req.params.sessionId });
    if (session.finalized) {
      unregisterMongoshSession(session);
      return res.json({ ok: true, alreadyClosed: true, sessionId: session.id });
    }
    await terminateMongoshSession(session, 'closed_by_user');
    res.json({ ok: true, sessionId: session.id });
  } catch (err) {
    sendError(res, err, 'Failed to close console session');
  }
});

app.get('/api/server-management/context', getConnection, async (req, res) => {
  if (req.adminKeyRequired && !req.adminVerified) {
    return res.status(403).json({ error: 'Admin access key is required for server management.', errorType: 'admin_required' });
  }
  try {
    const forceRefresh = parseBooleanLike(req.query?.refresh, false);
    const context = resolveServerManagementContext(req, {}, { requirePath: true });
    const binaries = getServerToolRuntimeStatus({ force: forceRefresh });
    res.json({
      context,
      hostWorkdir: SERVER_TOOL_HOST_WORKDIR || null,
      topology: req.conn?.topology || null,
      routing: req.conn?.routing || null,
      tools: Object.values(SERVER_TOOL_CONFIG).map((tool) => ({
        id: tool.id,
        kind: tool.kind,
        source: tool.source,
        displayName: tool.displayName,
      })),
      binaries,
      budget: {
        timeoutMs: req.queryBudget.timeoutMs,
        limit: req.queryBudget.limit,
        heavyTimeoutMs: req.heavyBudget,
      },
    });
  } catch (err) {
    sendError(res, err, 'Failed to load server-management context');
  }
});

app.get('/api/server-management/tools/:tool', getConnection, async (req, res) => {
  if (req.adminKeyRequired && !req.adminVerified) {
    return res.status(403).json({ error: 'Admin access key is required for server management.', errorType: 'admin_required' });
  }
  try {
    const toolId = normalizeServerToolId(req.params.tool);
    if (!toolId) return res.status(404).json({ error: 'Server-management tool not found.', errorType: 'not_found' });
    const tool = SERVER_TOOL_CONFIG[toolId];
    if (tool.kind !== 'read') {
      return res.status(405).json({ error: 'Use POST /run for executable server-management tools.', errorType: 'validation' });
    }
    if (tool.source === 'binary') ensureServerToolBinaryAvailable(toolId, req.conn);
    const sourcePayload = {
      node: req.query?.node,
      path: req.query?.path,
      confirmNodeSelection: req.query?.confirmNodeSelection,
      confirmPathSelection: req.query?.confirmPathSelection,
    };
    const context = resolveServerManagementContext(req, sourcePayload, { requirePath: true });
    const timeoutMs = parsePositiveInt(req.query?.timeoutMs, req.queryBudget.timeoutMs, 1000, SERVER_TOOL_EXEC_TIMEOUT_MAX_MS);
    const result = await runWithGovernor(req, tool.opClass || 'metadata', async () => {
      const targetIsSessionNode = nodeMatchesTopologyTarget(req.conn, context.selectedNode, req.conn?.topology?.me || '')
        || sameHostToken(req.conn?.topology?.me || req.conn?.host || '', context.selectedNode);
      if (toolId === 'serverInfo') {
        const payload = {
          version: req.conn.versionStr,
          capabilities: req.conn.capabilities,
          topology: req.conn.topology || null,
          connectionTopology: req.conn.topology || null,
          routing: req.conn.routing || null,
          machine: context.machine,
          targetNode: context.selectedNode || null,
        };
        const fillPayloadFromAdmin = async (admin) => {
          try { payload.buildInfo = await withTimeout(admin.command({ buildInfo: 1 }), req.queryBudget.timeoutMs); } catch {}
          try {
            const status = await withTimeout(admin.command({ serverStatus: 1 }), req.queryBudget.timeoutMs);
            payload.serverStatus = {
              host: status?.host || null,
              uptime: status?.uptime || null,
              process: status?.process || null,
              pid: status?.pid || null,
              connections: status?.connections || null,
              opcounters: status?.opcounters || null,
              mem: status?.mem || null,
              storageEngine: status?.storageEngine || null,
              repl: status?.repl || null,
            };
          } catch {}
          try {
            payload.hello = await withTimeout(admin.command({ hello: 1 }), req.queryBudget.timeoutMs);
          } catch {
            try {
              payload.hello = await withTimeout(admin.command({ isMaster: 1 }), req.queryBudget.timeoutMs);
            } catch {}
          }
        };
        if (targetIsSessionNode) {
          await fillPayloadFromAdmin(req.conn.client.db('admin'));
        } else {
          await runServerToolMongoCommand(req.conn, context.selectedNode, async (client) => {
            await fillPayloadFromAdmin(client.db('admin'));
          });
        }
        if (payload.hello) payload.topology = getTopologyInfo(payload.hello);
        return payload;
      }
      if (toolId === 'slowOps') {
        const thresholdMs = parsePositiveInt(req.query?.thresholdMs, 1000, 100, 24 * 60 * 60 * 1000);
        const thresholdSec = Math.max(0.1, Math.min(Number(thresholdMs) / 1000, 24 * 60 * 60));
        const limit = parsePositiveInt(req.query?.limit, 30, 1, 200);
        const collectSlowOps = async (admin) => {
          const current = await withTimeout(
            admin.command({
              currentOp: 1,
              $all: true,
              active: true,
              secs_running: { $gte: thresholdSec },
            }),
            req.queryBudget.timeoutMs,
          );
          const entries = (Array.isArray(current?.inprog) ? current.inprog : [])
            .filter((entry) => Number(entry?.secs_running || 0) >= thresholdSec)
            .sort((a, b) => Number(b?.secs_running || 0) - Number(a?.secs_running || 0))
            .slice(0, limit)
            .map((entry) => ({
              opid: entry?.opid ?? entry?.opId ?? null,
              desc: entry?.desc || entry?.op || '',
              namespace: entry?.ns || '',
              secsRunning: Number(entry?.secs_running || 0),
              waitingForLock: Boolean(entry?.waitingForLock),
              client: entry?.client || entry?.client_s || null,
              op: entry?.op || null,
              planSummary: entry?.planSummary || null,
              command: entry?.command || null,
            }));
          return { thresholdMs, thresholdSec, limit, entries, total: entries.length };
        };
        if (targetIsSessionNode) {
          return collectSlowOps(req.conn.client.db('admin'));
        }
        return runServerToolMongoCommand(req.conn, context.selectedNode, async (client) => (
          collectSlowOps(client.db('admin'))
        ));
      }
      const buildReadArgsForMode = (binMode = 'new') => ([
        ...buildServerToolConnectionArgs(req.conn, context.selectedNode, { mode: binMode }),
        '--rowcount',
        '1',
        '--json',
      ]);
      const runResult = await runServerToolCommandWithFallback(tool, buildReadArgsForMode, { timeoutMs, conn: req.conn });
      const usedMode = String(runResult?.binMode || 'new').trim().toLowerCase() === 'legacy'
        ? 'legacy'
        : 'new';
      const usedArgs = buildReadArgsForMode(usedMode);
      return {
        ...runResult,
        parsed: parseLastJsonLine(runResult.stdout),
        commandPreview: buildCommandPreview(runResult.binUsed || tool.bin, usedArgs),
      };
    });

    auditReq(req, 'server_management_read', {
      source: 'server_management',
      method: toolId,
      scope: 'global',
      node: context.selectedNode,
      path: context.selectedPath,
      user: req.auditUser || 'anonymous',
    });

    res.json({
      tool: toolId,
      context,
      result,
    });
  } catch (err) {
    sendError(res, err, 'Failed to execute server-management read tool');
  }
});

app.post('/api/server-management/kill-op', heavyLimiter, getConnection, async (req, res) => {
  if (req.adminKeyRequired && !req.adminVerified) {
    return res.status(403).json({ error: 'Admin access key is required for server management.', errorType: 'admin_required' });
  }
  try {
    const body = req.body && typeof req.body === 'object' ? req.body : {};
    const opid = body.opid;
    if (opid === undefined || opid === null) {
      return res.status(400).json({ error: 'opid is required.', errorType: 'validation' });
    }
    if (!requireHeavyConfirm(req, res)) return;
    const node = String(body.node || '').trim();
    const context = resolveServerManagementContext(req, { node, confirmNodeSelection: true }, { requirePath: false });
    const targetIsSessionNode = !node
      || nodeMatchesTopologyTarget(req.conn, node, req.conn?.topology?.me || '')
      || sameHostToken(req.conn?.topology?.me || req.conn?.host || '', node);
    const killOp = async (admin) => {
      const result = await withTimeout(admin.command({ killOp: 1, op: opid }), req.queryBudget.timeoutMs);
      return { ok: true, result };
    };
    let result;
    if (targetIsSessionNode) {
      result = await killOp(req.conn.client.db('admin'));
    } else {
      result = await runServerToolMongoCommand(req.conn, node, async (client) => killOp(client.db('admin')));
    }
    auditReq(req, 'server_management_killop', {
      source: 'server_management',
      method: 'killOp',
      opid,
      node: context.selectedNode || node,
      user: req.auditUser || 'anonymous',
    });
    res.json(result);
  } catch (err) {
    sendError(res, err, 'Failed to kill operation');
  }
});

app.post('/api/server-management/tools/:tool/run', heavyLimiter, getConnection, async (req, res) => {
  if (req.adminKeyRequired && !req.adminVerified) {
    return res.status(403).json({ error: 'Admin access key is required for server management.', errorType: 'admin_required' });
  }
  try {
    const toolId = normalizeServerToolId(req.params.tool);
    if (!toolId) return res.status(404).json({ error: 'Server-management tool not found.', errorType: 'not_found' });
    const tool = SERVER_TOOL_CONFIG[toolId];
    if (tool.kind !== 'run') return res.status(405).json({ error: 'This tool is read-only. Use GET endpoint.', errorType: 'validation' });
    if (!requireHeavyConfirm(req, res)) return;

    const body = req.body && typeof req.body === 'object' ? req.body : {};
    const context = resolveServerManagementContext(req, body, { requirePath: true });
    ensureServerToolBinaryAvailable(toolId, req.conn);
    const timeoutMs = parsePositiveInt(body.timeoutMs, req.heavyBudget || SERVER_TOOL_EXEC_TIMEOUT_DEFAULT_MS, 1000, SERVER_TOOL_EXEC_TIMEOUT_MAX_MS);
    const assertPathAllowed = (pathValue, label = 'Path') => {
      const normalized = normalizeFsPath(pathValue);
      if (!isAbsoluteFsPath(normalized)) {
        throw createRequestError(`${label} must be absolute.`, 400, 'validation');
      }
      if (!isPathWithin(context.basePath, normalized) && !context.confirmPathSelection) {
        throw createRequestError(`${label} is outside default workspace path. Set confirmPathSelection=true to continue.`, 428, 'precondition');
      }
      return normalized;
    };

    const runResult = await runWithGovernor(req, tool.opClass || 'heavy', async () => {
      const buildArgsForMode = (binMode = 'new') => {
        const args = buildServerToolConnectionArgs(req.conn, context.selectedNode, { mode: binMode });
        const executionInfo = {
          tool: toolId,
          node: context.selectedNode,
          path: context.selectedPath,
          machine: context.machine,
          db: null,
          collection: null,
        };

        if (toolId === 'mongodump') {
          const dbName = ensureSafeIdentifier(body.db || body.dumpDb || '', 'Database');
          const colName = ensureSafeIdentifier(body.collection || body.dumpCollection || '', 'Collection');
          const queryText = ensureJsonText(body.query || body.dumpFilter || '', 'Dump query');
          const outDir = assertPathAllowed(String(body.outputPath || body.dumpPath || context.selectedPath), 'Output path');
          if (dbName) {
            args.push('--db', dbName);
            executionInfo.db = dbName;
          }
          if (colName) {
            args.push('--collection', colName);
            executionInfo.collection = colName;
          }
          if (queryText && colName) args.push('--query', queryText);
          if (parseBooleanLike(body.gzip || body.dumpGzip, false)) args.push('--gzip');
          if (parseBooleanLike(body.oplog || body.dumpOplog, false)) args.push('--oplog');
          args.push('--out', outDir);
          executionInfo.path = outDir;
        } else if (toolId === 'mongorestore') {
          const restorePath = assertPathAllowed(String(body.inputPath || body.restorePath || context.selectedPath), 'Input path');
          const multiScope = parseBooleanLike(body.multiScope || body.restoreMultiScope, false);
          const dbName = ensureSafeIdentifier(body.db || body.restoreDb || '', 'Database');
          const dbList = ensureSafeIdentifierList(body.dbList || body.restoreDbList, 'Database');
          const collectionList = ensureSafeIdentifierList(body.collectionList || body.restoreCollectionList, 'Collection');
          if (parseBooleanLike(body.drop || body.restoreDrop, false)) args.push('--drop');
          if (parseBooleanLike(body.gzip || body.restoreGzip, false)) args.push('--gzip');
          if (multiScope) {
            if (dbList.length === 0) {
              throw createRequestError('Select at least one database for multi-scope restore.', 400, 'validation');
            }
            if (dbList.length > 1 && collectionList.length > 0) {
              throw createRequestError('Collection multi-select is supported only when exactly one database is selected.', 400, 'validation');
            }
            if (dbList.length === 1 && collectionList.length > 0) {
              for (const collectionName of collectionList) args.push('--nsInclude', `${dbList[0]}.${collectionName}`);
              executionInfo.db = dbList[0];
              executionInfo.collection = collectionList.join(',');
            } else {
              for (const restoreDbName of dbList) args.push('--nsInclude', `${restoreDbName}.*`);
              executionInfo.db = dbList.join(',');
            }
          } else if (dbName) {
            args.push('--nsInclude', `${dbName}.*`);
            executionInfo.db = dbName;
          }
          args.push(restorePath);
          executionInfo.path = restorePath;
        } else if (toolId === 'mongoexport') {
          const dbName = ensureSafeIdentifier(body.db || body.exportDb || '', 'Database');
          const colName = ensureSafeIdentifier(body.collection || body.exportCollection || '', 'Collection');
          if (!dbName || !colName) throw createRequestError('mongoexport requires both db and collection.', 400, 'validation');
          const formatRaw = String(body.type || body.format || body.exportFormat || 'json').trim().toLowerCase();
          const format = formatRaw === 'csv' ? 'csv' : 'json';
          const limitRaw = String(body.limit || body.exportLimit || '').trim();
          const queryText = ensureJsonText(body.query || body.exportFilter || '', 'Export query');
          const defaultFile = `${safeFilename(`${dbName}.${colName}`, 'export')}.${format}`;
          const outputFile = assertPathAllowed(resolveFilePath(context.selectedPath, body.outputFile || body.exportFile, defaultFile), 'Output file');
          const fields = String(body.fields || body.exportFields || '').trim();
          args.push('--db', dbName, '--collection', colName, '--type', format, '--out', outputFile);
          if (limitRaw) {
            const limit = parsePositiveInt(limitRaw, 0, 0, EXPORT_LIMIT_MAX);
            if (limit > 0) args.push('--limit', String(limit));
          }
          if (queryText) args.push('--query', queryText);
          if (format === 'csv' && fields) args.push('--fields', fields);
          executionInfo.db = dbName;
          executionInfo.collection = colName;
          executionInfo.path = outputFile;
        } else if (toolId === 'mongoimport') {
          const dbName = ensureSafeIdentifier(body.db || body.importDb || '', 'Database');
          const colName = ensureSafeIdentifier(body.collection || body.importCollection || '', 'Collection');
          if (!dbName || !colName) throw createRequestError('mongoimport requires both db and collection.', 400, 'validation');
          const typeRaw = String(body.type || body.format || body.importFormat || 'json').trim().toLowerCase();
          const type = ['json', 'csv', 'tsv'].includes(typeRaw) ? typeRaw : 'json';
          const modeRaw = String(body.mode || body.importMode || 'insert').trim().toLowerCase();
          const mode = ['insert', 'upsert', 'merge', 'delete'].includes(modeRaw) ? modeRaw : 'insert';
          const defaultInput = type === 'csv' ? `${safeFilename(`${dbName}.${colName}`, 'import')}.csv` : `${safeFilename(`${dbName}.${colName}`, 'import')}.json`;
          const inputFile = assertPathAllowed(resolveFilePath(context.selectedPath, body.inputFile || body.importFile, defaultInput), 'Input file');
          args.push('--db', dbName, '--collection', colName, '--type', type, '--mode', mode, '--file', inputFile);
          if (parseBooleanLike(body.drop || body.importDrop, false)) args.push('--drop');
          executionInfo.db = dbName;
          executionInfo.collection = colName;
          executionInfo.path = inputFile;
        } else if (toolId === 'mongofiles') {
          const actionRaw = String(body.action || 'list').trim().toLowerCase();
          const action = ['list', 'search', 'get', 'put', 'delete'].includes(actionRaw) ? actionRaw : 'list';
          const dbName = ensureSafeIdentifier(body.db || body.gridFsDb || body.gridfsDb || 'admin', 'Database');
          const bucket = ensureSafeIdentifier(body.bucket || body.gridFsBucket || body.gridfsBucket || 'fs', 'Bucket');
          const fileName = String(body.filename || body.fileName || '').trim();
          const localPathInput = String(body.localPath || body.localFile || '').trim();
          args.push('--db', dbName, '--prefix', bucket);
          if (action === 'search') {
            args.push('search', fileName || '.*');
          } else if (action === 'get') {
            if (!fileName) throw createRequestError('mongofiles get requires filename.', 400, 'validation');
            args.push('get', fileName);
            if (localPathInput) args.push('--local', assertPathAllowed(resolveFilePath(context.selectedPath, localPathInput, fileName), 'Local path'));
          } else if (action === 'put') {
            const localFile = assertPathAllowed(resolveFilePath(context.selectedPath, localPathInput, fileName || 'upload.bin'), 'Local file');
            args.push('put', localFile);
            if (fileName) args.push('--replace', fileName);
          } else if (action === 'delete') {
            if (!fileName) throw createRequestError('mongofiles delete requires filename.', 400, 'validation');
            args.push('delete', fileName);
          } else {
            args.push('list');
          }
          executionInfo.db = dbName;
          executionInfo.path = context.selectedPath;
        }

        return { args, executionInfo };
      };

      const result = await runServerToolCommandWithFallback(
        tool,
        (binMode) => buildArgsForMode(binMode).args,
        { timeoutMs, conn: req.conn },
      );
      const usedMode = String(result?.binMode || 'new').trim().toLowerCase() === 'legacy'
        ? 'legacy'
        : 'new';
      const applied = buildArgsForMode(usedMode);
      return {
        ...result,
        commandPreview: buildCommandPreview(result.binUsed || tool.bin, applied.args),
        executionInfo: applied.executionInfo,
      };
    });

    auditReq(req, 'server_management_run', {
      source: 'server_management',
      method: toolId,
      scope: 'global',
      node: context.selectedNode,
      path: runResult?.executionInfo?.path || context.selectedPath,
      db: runResult?.executionInfo?.db || null,
      col: runResult?.executionInfo?.collection || null,
      exitCode: runResult?.code,
      ok: runResult?.ok === true,
      timedOut: runResult?.timedOut === true,
      user: req.auditUser || 'anonymous',
    });

    res.json({
      tool: toolId,
      context,
      result: runResult,
    });
  } catch (err) {
    sendError(res, err, 'Failed to execute server-management command');
  }
});

app.get('/api/databases/:db/collections/:col/distinct/:field', getConnection, async (req, res) => {
  let reqClosed = false;
  try {
    const source = parseAuditSource(req.query?.source);
    const field = String(req.params.field || '').trim();
    if (!field) return res.status(400).json({ error: 'Field is required.', errorType: 'validation' });
    const limit = Math.min(
      clampNumber(req.queryBudget.limit || QUERY_LIMIT_DEFAULT, QUERY_LIMIT_MIN, QUERY_LIMIT_MAX),
      1000,
    );
    const scanCap = Math.max(limit * 200, 5000);
    const values = await runWithGovernor(req, 'interactive', async () => {
      const col = req.conn.client.db(req.params.db).collection(req.params.col);
      const distinctComment = req.createOpComment?.(`distinct:${req.params.db}.${req.params.col}.${field}`);
      const cursor = col.find({}, { projection: { [field]: 1, _id: 0 }, maxTimeMS: req.queryBudget.timeoutMs, comment: distinctComment }).limit(scanCap);
      const untrackDistinctCursor = trackActiveCursor(req.connId, cursor);
      req.on('close', () => {
        reqClosed = true;
        untrackDistinctCursor();
        if (typeof cursor?.close === 'function') cursor.close().catch(() => {});
      });
      const unique = new Map();
      try {
        while (unique.size < limit) {
          const next = await withTimeout(cursor.next(), req.queryBudget.timeoutMs);
          if (!next) break;
          const valuesInDoc = [];
          collectDistinctScalars(getValueByPath(next, field), valuesInDoc);
          for (const value of valuesInDoc) {
            const key = distinctValueKey(value);
            if (unique.has(key)) continue;
            unique.set(key, value);
            if (unique.size >= limit) break;
          }
        }
      } finally {
        untrackDistinctCursor();
        try { await cursor.close(); } catch {}
      }
      return [...unique.values()];
    });
    if (reqClosed || res.headersSent) return;
    if (source) {
      auditReq(req, 'query', {
        db: req.params.db,
        col: req.params.col,
        method: 'distinct',
        field,
        source,
        count: values.length,
      });
    }
    res.json({ values });
  }
  catch (err) {
    if (reqClosed || res.headersSent) return;
    sendError(res, err);
  }
});

if (isProd) app.get('*', (_, res) => res.sendFile(join(__dirname,'..','dist','index.html')));

const server = createServer(app);
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n  ⚡ MongoStudio v2.6.0 → http://localhost:${PORT}\n  MongoDB 3.6 → 8.x | ${isProd?'production':'development'}\n`);
});

let shuttingDown = false;
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('\nShutting down…');
  const closingTasks = [];
  for (const session of mongoshSessions.values()) {
    closingTasks.push(terminateMongoshSession(session, 'server_shutdown').catch(() => false));
  }
  for (const [, { client }] of connections) {
    closingTasks.push(client.close().catch(() => {}));
  }
  Promise.allSettled(closingTasks).finally(() => {
    server.close(() => process.exit(0));
  });
  setTimeout(() => process.exit(1), 5000);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

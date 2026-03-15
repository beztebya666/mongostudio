import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import api from '../utils/api';
import { Terminal, Play, Loader, Refresh, Lock, Download, Copy, Zap, AlertCircle, Eye } from './Icons';
import { formatDuration, copyToClipboard } from '../utils/formatters';
import DropdownSelect from './DropdownSelect';
import ConfirmDialog from './modals/ConfirmDialog';
import { genId } from '../utils/genId';

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function splitTopLevelArgs(raw = '') {
  const out = [];
  const text = String(raw || '');
  let chunk = '';
  let depthCurly = 0;
  let depthSquare = 0;
  let depthRound = 0;
  let quote = null;
  let escaped = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (quote) {
      chunk += ch;
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === '\'') {
      quote = ch;
      chunk += ch;
      continue;
    }
    if (ch === '{') depthCurly += 1;
    else if (ch === '}') depthCurly = Math.max(0, depthCurly - 1);
    else if (ch === '[') depthSquare += 1;
    else if (ch === ']') depthSquare = Math.max(0, depthSquare - 1);
    else if (ch === '(') depthRound += 1;
    else if (ch === ')') depthRound = Math.max(0, depthRound - 1);
    if (ch === ',' && depthCurly === 0 && depthSquare === 0 && depthRound === 0) {
      if (chunk.trim()) out.push(chunk.trim());
      chunk = '';
      continue;
    }
    chunk += ch;
  }
  if (chunk.trim()) out.push(chunk.trim());
  return out;
}

function parseLooseArg(raw, index) {
  const text = String(raw || '').trim();
  if (!text) return undefined;
  try {
    return JSON.parse(text);
  } catch {}
  if (/^'.*'$/.test(text)) return text.slice(1, -1).replace(/\\'/g, '\'');
  if (text === 'true') return true;
  if (text === 'false') return false;
  if (text === 'null') return null;
  if (/^-?\d+(\.\d+)?$/.test(text)) return Number(text);
  throw new Error(`Argument ${index + 1} must be valid JSON.`);
}

function parseArgs(raw = '') {
  const text = String(raw || '').trim();
  if (!text) return [];
  const parts = splitTopLevelArgs(text);
  return parts.map((part, idx) => parseLooseArg(part, idx));
}

function normalizeCommandForParsing(raw = '') {
  return String(raw || '')
    .normalize('NFKC')
    .replace(/[\u2018\u2019\u201B\u2039\u203A]/g, '\'')
    .replace(/[\u201C\u201D\u201E\u201F\u00AB\u00BB]/g, '"')
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .trim();
}

function parseCommand(raw = '') {
  const text = normalizeCommandForParsing(raw);
  if (!text) throw new Error('Command is empty.');
  if (/^(?:help|\.help|\?)\s*;?$/i.test(text)) return { kind: 'help' };
  if (/^(?:clear|\.clear)\s*;?$/i.test(text)) return { kind: 'clear' };
  const useMatch = text.match(/^use\s+["']?([a-zA-Z0-9_.-]+)["']?\s*;?$/i);
  if (useMatch) return { kind: 'use', dbName: useMatch[1] };

  const collectionMatch = text.match(
    /^db\s*\.\s*(?:getCollection\s*\(\s*(['"])(.+?)\1\s*\)|([\w.$-]+))\s*\.\s*([A-Za-z_$][\w$]*)\s*(?:\(([\s\S]*)\))?\s*;?$/i,
  );
  if (collectionMatch) {
    return {
      kind: 'collection',
      targetCollection: collectionMatch[2] || collectionMatch[3] || '',
      method: String(collectionMatch[4] || '').trim(),
      argsRaw: String(collectionMatch[5] || '').trim(),
    };
  }

  const dbMatch = text.match(/^db\s*\.\s*([A-Za-z_$][\w$]*)\s*(?:\(([\s\S]*)\))?\s*;?$/i);
  if (dbMatch) {
    return {
      kind: 'database',
      method: String(dbMatch[1] || '').trim(),
      argsRaw: String(dbMatch[2] || '').trim(),
    };
  }

  throw new Error('Unsupported command format. Use db.<collection>.<method>(...) or db.<method>(...).');
}

function resolveScope(scope, fallbackDb, fallbackCollection) {
  const explicitLevel = scope?.level === 'collection' || scope?.level === 'database' || scope?.level === 'global'
    ? scope.level
    : '';
  const db = String(scope?.db || fallbackDb || '').trim() || null;
  const collection = String(scope?.collection || fallbackCollection || '').trim() || null;

  if (explicitLevel === 'global') return { level: 'global', db, collection: null };
  if (explicitLevel === 'database') {
    if (db) return { level: 'database', db, collection: null };
    return { level: 'global', db: null, collection: null };
  }
  if (explicitLevel === 'collection' && db && collection) return { level: 'collection', db, collection };

  if (db && collection) return { level: 'collection', db, collection };
  if (db) return { level: 'database', db, collection: null };
  return { level: 'global', db: null, collection: null };
}

function scopeLabel(scopeInfo) {
  if (scopeInfo.level === 'collection') return `collection scope`;
  if (scopeInfo.level === 'database') return `database scope`;
  return 'global scope';
}

function getScopeTag(scopeInfo) {
  if (scopeInfo.level === 'collection') return `collection: ${scopeInfo.db}.${scopeInfo.collection}`;
  if (scopeInfo.level === 'database') return `database: ${scopeInfo.db}`;
  return 'global';
}

function buildExamples(scopeInfo) {
  const fallbackCollection = 'your_collection';
  const col = scopeInfo.collection || fallbackCollection;
  if (scopeInfo.level === 'collection') {
    return [
      { id: 'find', label: 'find()', value: `db.${col}.find({})` },
      { id: 'agg', label: 'aggregate()', value: `db.${col}.aggregate([{ "$limit": 20 }])` },
      { id: 'upd', label: 'updateMany()', value: `db.${col}.updateMany({ "active": true }, { "$set": { "touchedAt": { "$date": "2026-01-01T00:00:00.000Z" } } })` },
      { id: 'bulk', label: 'bulkWrite()', value: `db.${col}.bulkWrite([{ "insertOne": { "document": { "title": "sample" } } }])` },
      { id: 'dbstats', label: 'db.stats()', value: 'db.stats()' },
      { id: 'indexes', label: 'getIndexes()', value: `db.${col}.getIndexes()` },
    ];
  }
  if (scopeInfo.level === 'database') {
    return [
      { id: 'dbstats', label: 'db.stats()', value: 'db.stats()' },
      { id: 'collections', label: 'db.getCollectionNames()', value: 'db.getCollectionNames()' },
      { id: 'infos', label: 'db.getCollectionInfos()', value: 'db.getCollectionInfos()' },
      { id: 'create', label: 'db.createCollection()', value: 'db.createCollection("new_collection")' },
      { id: 'find', label: 'db.getCollection().find()', value: `db.getCollection("${col}").find({})` },
      { id: 'dropdb', label: 'db.dropDatabase()', value: 'db.dropDatabase()' },
    ];
  }
  return [
    { id: 'dbstats', label: 'db.stats()', value: 'db.stats()' },
    { id: 'collections', label: 'db.getCollectionNames()', value: 'db.getCollectionNames()' },
    { id: 'find', label: 'db.getCollection().find()', value: `db.getCollection("${col}").find({})` },
    { id: 'agg', label: 'aggregate()', value: `db.getCollection("${col}").aggregate([{ "$limit": 20 }])` },
    { id: 'use', label: 'use <db>', value: 'use your_database' },
  ];
}

function downloadText(filename, text, mime = 'text/plain;charset=utf-8') {
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

/* copyToClipboard imported from ../utils/formatters */

const HELP_LINES = [
  'Supported command formats:',
  '1) db.<collection>.<method>(args)',
  '2) db.getCollection("name").<method>(args)',
  '3) db.<dbMethod>(args)',
  '4) use <db> (global scope only)',
  'Explain button supports find()/aggregate() in ConsoleUI.',
  '',
  'Collection methods: find/findOne, aggregate, insertOne/insertMany, updateOne/updateMany, replaceOne, deleteOne/deleteMany, findOneAnd*, findAndModify, bulkWrite, countDocuments, estimatedDocumentCount, distinct, createIndex/dropIndex/getIndexes, stats, renameCollection, validate, reIndex.',
  'Database methods: stats/getStats, getCollectionNames/listCollections, getCollectionInfos, createCollection, dropDatabase.',
  'Quick commands: help, clear.',
];

const CONSOLE_COLOR = {
  plain: 'var(--console-token-plain)',
  dim: 'var(--console-token-dim)',
  method: 'var(--console-token-method)',
  keyword: 'var(--console-token-keyword)',
  string: 'var(--console-token-string)',
  number: 'var(--console-token-number)',
  boolean: 'var(--console-token-boolean)',
  null: 'var(--console-token-null)',
  punctuation: 'var(--console-token-punctuation)',
};

const CONSOLE_KEYWORDS = new Set(['db', 'use', 'help', 'clear']);
const BOOL_KEYWORDS = new Set(['true', 'false']);
const COLLECTION_AUTOCOMPLETE_METHODS = [
  'find',
  'findOne',
  'aggregate',
  'insertOne',
  'insertMany',
  'updateOne',
  'updateMany',
  'replaceOne',
  'deleteOne',
  'deleteMany',
  'findOneAndUpdate',
  'findOneAndReplace',
  'findOneAndDelete',
  'findAndModify',
  'bulkWrite',
  'countDocuments',
  'estimatedDocumentCount',
  'distinct',
  'createIndex',
  'dropIndex',
  'getIndexes',
  'dropIndexes',
  'hideIndex',
  'unhideIndex',
  'renameCollection',
  'validate',
  'reIndex',
  'stats',
];
const DATABASE_AUTOCOMPLETE_METHODS = [
  'stats',
  'getStats',
  'getCollectionNames',
  'listCollections',
  'getCollectionInfos',
  'createCollection',
  'dropDatabase',
  'runCommand',
];

function computeAutocomplete(command, scopeInfo) {
  const text = String(command || '');
  const collectionMatch = text.match(/db\.(?:getCollection\((['"])(.+?)\1\)|([\w.$-]+))\.([A-Za-z_]*)$/);
  if (collectionMatch) {
    const targetCollection = collectionMatch[2] || collectionMatch[3] || '';
    if (scopeInfo.level === 'collection' && targetCollection !== scopeInfo.collection) {
      return { items: [], replaceStart: 0, replaceEnd: 0 };
    }
    const partial = collectionMatch[4] || '';
    const replaceEnd = text.length;
    const replaceStart = replaceEnd - partial.length;
    const items = COLLECTION_AUTOCOMPLETE_METHODS
      .filter((item) => item.toLowerCase().startsWith(partial.toLowerCase()))
      .slice(0, 12);
    return { items, replaceStart, replaceEnd };
  }

  if (scopeInfo.level === 'collection') return { items: [], replaceStart: 0, replaceEnd: 0 };

  const dbMatch = text.match(/db\.([A-Za-z_]*)$/);
  if (dbMatch) {
    const partial = dbMatch[1] || '';
    const replaceEnd = text.length;
    const replaceStart = replaceEnd - partial.length;
    const items = DATABASE_AUTOCOMPLETE_METHODS
      .filter((item) => item.toLowerCase().startsWith(partial.toLowerCase()))
      .slice(0, 12);
    return { items, replaceStart, replaceEnd };
  }
  return { items: [], replaceStart: 0, replaceEnd: 0 };
}

function renderConsoleSyntax(text = '', keyPrefix = 'token', { dim = false } = {}) {
  const value = String(text || '');
  if (!value) return null;
  const tokenRe = /"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|\b(?:true|false|null|db|use|help|clear)\b|\b[A-Za-z_$][\w$]*(?=\()|\b-?\d+(?:\.\d+)?(?:e[+-]?\d+)?\b|[{}\[\]():.,]/gi;
  const out = [];
  let lastIndex = 0;
  let seq = 0;
  let match = tokenRe.exec(value);
  while (match) {
    const token = match[0];
    const index = match.index;
    if (index > lastIndex) {
      out.push(
        <span key={`${keyPrefix}:p:${seq++}`} style={{ color: dim ? CONSOLE_COLOR.dim : CONSOLE_COLOR.plain }}>
          {value.slice(lastIndex, index)}
        </span>,
      );
    }
    const lower = token.toLowerCase();
    let style = { color: dim ? CONSOLE_COLOR.dim : CONSOLE_COLOR.plain };
    if (token.startsWith('"') || token.startsWith('\'')) {
      style = { color: CONSOLE_COLOR.string };
    } else if (CONSOLE_KEYWORDS.has(lower)) {
      style = { color: CONSOLE_COLOR.keyword, fontWeight: 500 };
    } else if (BOOL_KEYWORDS.has(lower)) {
      style = { color: CONSOLE_COLOR.boolean };
    } else if (lower === 'null') {
      style = { color: CONSOLE_COLOR.null, fontStyle: 'italic' };
    } else if (/^-?\d+(?:\.\d+)?(?:e[+-]?\d+)?$/i.test(token)) {
      style = { color: CONSOLE_COLOR.number };
    } else if (/^[{}\[\]():.,]$/.test(token)) {
      style = { color: CONSOLE_COLOR.punctuation };
    } else {
      style = { color: CONSOLE_COLOR.method, fontWeight: 500 };
    }
    out.push(<span key={`${keyPrefix}:t:${seq++}`} style={style}>{token}</span>);
    lastIndex = tokenRe.lastIndex;
    match = tokenRe.exec(value);
  }
  if (lastIndex < value.length) {
    out.push(
      <span key={`${keyPrefix}:p:${seq++}`} style={{ color: dim ? CONSOLE_COLOR.dim : CONSOLE_COLOR.plain }}>
        {value.slice(lastIndex)}
      </span>,
    );
  }
  return out;
}

function renderJsonSyntax(value, keyPrefix = 'json') {
  let text = '';
  try {
    text = JSON.stringify(value, null, 2);
  } catch {
    text = String(value ?? '');
  }
  const lines = String(text || '').split('\n');
  return lines.map((line, lineIndex) => {
    const tokenRe = /"(?:\\.|[^"\\])*"|\b-?\d+(?:\.\d+)?(?:e[+-]?\d+)?\b|\btrue\b|\bfalse\b|\bnull\b|[{}\[\]:,]/g;
    const chunks = [];
    let lastIndex = 0;
    let seq = 0;
    let match = tokenRe.exec(line);
    while (match) {
      const token = match[0];
      const index = match.index;
      if (index > lastIndex) {
        chunks.push(
          <span key={`${keyPrefix}:${lineIndex}:p:${seq++}`} style={{ color: CONSOLE_COLOR.plain }}>
            {line.slice(lastIndex, index)}
          </span>,
        );
      }
      let style = { color: CONSOLE_COLOR.plain };
      if (token.startsWith('"')) {
        const tail = line.slice(index + token.length);
        const isKey = /^\s*:/.test(tail);
        style = { color: isKey ? CONSOLE_COLOR.method : CONSOLE_COLOR.string };
      } else if (token === 'true' || token === 'false') {
        style = { color: CONSOLE_COLOR.boolean };
      } else if (token === 'null') {
        style = { color: CONSOLE_COLOR.null, fontStyle: 'italic' };
      } else if (/^-?\d+(?:\.\d+)?(?:e[+-]?\d+)?$/i.test(token)) {
        style = { color: CONSOLE_COLOR.number };
      } else {
        style = { color: CONSOLE_COLOR.punctuation };
      }
      chunks.push(<span key={`${keyPrefix}:${lineIndex}:t:${seq++}`} style={style}>{token}</span>);
      lastIndex = tokenRe.lastIndex;
      match = tokenRe.exec(line);
    }
    if (lastIndex < line.length) {
      chunks.push(
        <span key={`${keyPrefix}:${lineIndex}:p:${seq++}`} style={{ color: CONSOLE_COLOR.plain }}>
          {line.slice(lastIndex)}
        </span>,
      );
    }
    return <div key={`${keyPrefix}:${lineIndex}`}>{chunks}</div>;
  });
}

export default function ConsoleView({
  db,
  collection,
  scope,
  databaseNames = [],
  onQueryMs,
  onDbContextChange,
  refreshToken = 0,
  adminLocked = false,
  menuZIndex = 320,
}) {
  const scopeLevel = scope?.level;
  const scopeDb = scope?.db;
  const scopeCollection = scope?.collection;
  const scopeInfo = useMemo(
    () => resolveScope({ level: scopeLevel, db: scopeDb, collection: scopeCollection }, db, collection),
    [scopeLevel, scopeDb, scopeCollection, db, collection],
  );
  const scopeKey = useMemo(
    () => `${scopeInfo.level}:${scopeInfo.db || ''}:${scopeInfo.collection || ''}`,
    [scopeInfo.level, scopeInfo.db, scopeInfo.collection],
  );
  const examples = useMemo(() => buildExamples(scopeInfo), [scopeKey]);
  const isMockMode = api.isMockMode?.() === true;
  const consolesLocked = adminLocked && !isMockMode;

  const [consoleMode, setConsoleMode] = useState('ui');
  const isRealShellMode = consoleMode === 'mongosh' || consoleMode === 'mongo';
  const selectedShell = consoleMode === 'mongo' ? 'mongo' : 'mongosh';
  const [busy, setBusy] = useState(false);
  const [sessionBusy, setSessionBusy] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [currentDb, setCurrentDb] = useState(scopeInfo.db || '');
  const [exampleId, setExampleId] = useState(examples[0]?.id || '');
  const [command, setCommand] = useState('');
  const [lines, setLines] = useState([]);
  const [history, setHistory] = useState([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const [historyDraft, setHistoryDraft] = useState('');
  const [indexHints, setIndexHints] = useState([]);
  const [selectedHint, setSelectedHint] = useState('auto');
  const [runElapsedMs, setRunElapsedMs] = useState(null);
  const [runLiveTimer, setRunLiveTimer] = useState(0);
  const [explainRunning, setExplainRunning] = useState(false);
  const [explainElapsedMs, setExplainElapsedMs] = useState(null);
  const [explainLiveTimer, setExplainLiveTimer] = useState(0);
  const [autocompleteIndex, setAutocompleteIndex] = useState(0);
  const [confirmDialog, setConfirmDialog] = useState(null);
  const [mongoshSessionId, setMongoshSessionId] = useState('');
  const [mongoshConnected, setMongoshConnected] = useState(false);
  const logRef = useRef(null);
  const commandInputRef = useRef(null);
  const commandOverlayRef = useRef(null);
  const streamAbortRef = useRef(null);
  const mongoshSessionRef = useRef('');
  const mongoshSessionShellRef = useRef('mongosh');
  const uiRunAbortRef = useRef(null);
  const explainAbortRef = useRef(null);
  const runTimerRef = useRef(null);
  const runTimerStartedAtRef = useRef(0);
  const explainTimerRef = useRef(null);
  const explainTimerStartedAtRef = useRef(0);
  const mongoshStartNonceRef = useRef(0);
  const mongoshStartGuardRef = useRef(false);
  const previousConsoleModeRef = useRef('ui');
  const confirmResolverRef = useRef(null);

  const normalizeMongoshMessage = useCallback((rawMessage) => {
    const text = String(rawMessage || '').trim();
    if (!text) return 'Console shell error.';
    if (/ENOENT|binary not found|spawn_enoent/i.test(text)) {
      return 'Console shell binary not found on server host. Install mongo/mongosh and configure MONGO_BIN or MONGOSH_BIN.';
    }
    return text;
  }, []);

  const appendLine = useCallback((kind, payload = {}) => {
    const line = { id: genId(), kind, ...payload };
    setLines((prev) => [...prev.slice(-699), line]);
    return line;
  }, []);

  const requestConfirm = useCallback((config = {}) => (
    new Promise((resolve) => {
      confirmResolverRef.current = resolve;
      setConfirmDialog({
        title: config.title || 'Confirm action',
        message: config.message || 'Continue?',
        confirmLabel: config.confirmLabel || 'Confirm',
        cancelLabel: config.cancelLabel || 'Cancel',
        danger: config.danger === true,
      });
    })
  ), []);

  const closeConfirmDialog = useCallback((approved) => {
    setConfirmDialog(null);
    const resolver = confirmResolverRef.current;
    confirmResolverRef.current = null;
    if (resolver) resolver(Boolean(approved));
  }, []);

  const startRunTimer = useCallback(() => {
    runTimerStartedAtRef.current = Date.now();
    setRunLiveTimer(0);
    if (runTimerRef.current) clearInterval(runTimerRef.current);
    runTimerRef.current = setInterval(() => {
      if (!runTimerStartedAtRef.current) return;
      setRunLiveTimer(Math.max(0, Date.now() - runTimerStartedAtRef.current));
    }, 100);
  }, []);

  const stopRunTimer = useCallback(() => {
    if (runTimerRef.current) {
      clearInterval(runTimerRef.current);
      runTimerRef.current = null;
    }
    runTimerStartedAtRef.current = 0;
  }, []);

  const startExplainTimer = useCallback(() => {
    explainTimerStartedAtRef.current = Date.now();
    setExplainLiveTimer(0);
    if (explainTimerRef.current) clearInterval(explainTimerRef.current);
    explainTimerRef.current = setInterval(() => {
      if (!explainTimerStartedAtRef.current) return;
      setExplainLiveTimer(Math.max(0, Date.now() - explainTimerStartedAtRef.current));
    }, 100);
  }, []);

  const stopExplainTimer = useCallback(() => {
    if (explainTimerRef.current) {
      clearInterval(explainTimerRef.current);
      explainTimerRef.current = null;
    }
    explainTimerStartedAtRef.current = 0;
  }, []);

  const stopMongoshStream = useCallback(() => {
    if (streamAbortRef.current) {
      try { streamAbortRef.current.abort(); } catch {}
      streamAbortRef.current = null;
    }
    setMongoshConnected(false);
  }, []);

  const cancelUiRun = useCallback(() => {
    if (!uiRunAbortRef.current) return false;
    try { uiRunAbortRef.current.abort(); } catch {}
    uiRunAbortRef.current = null;
    return true;
  }, []);

  const invalidateMongoshStart = useCallback(() => {
    mongoshStartNonceRef.current += 1;
    mongoshStartGuardRef.current = false;
    setSessionBusy(false);
    setMongoshConnected(false);
  }, []);

  const closeMongoshSession = useCallback(async ({ silent = false } = {}) => {
    stopMongoshStream();
    const sessionId = mongoshSessionRef.current;
    const sessionShell = mongoshSessionShellRef.current || 'mongosh';
    if (!sessionId) return;
    mongoshSessionRef.current = '';
    mongoshSessionShellRef.current = 'mongosh';
    setMongoshSessionId('');
    try {
      await api.closeConsoleSession(sessionId, { shell: sessionShell });
    } catch (err) {
      if (!silent) appendLine('error', { text: err?.message || 'Failed to close console shell session.' });
    }
  }, [appendLine, stopMongoshStream]);

  const startMongoshSession = useCallback(async (shell = 'mongosh') => {
    if (mongoshStartGuardRef.current) return;
    const shellMode = shell === 'mongo' ? 'mongo' : 'mongosh';
    if (isMockMode) {
      setError('True shell console is unavailable in Demo mode. Use ConsoleUI for Demo mode.');
      return;
    }
    if (consolesLocked) {
      setError('Admin access key is required to use consoles. Go to Settings → Rate Limit to enter the key.');
      return;
    }
    const startNonce = mongoshStartNonceRef.current + 1;
    mongoshStartNonceRef.current = startNonce;
    mongoshStartGuardRef.current = true;
    setSessionBusy(true);
    setMongoshConnected(false);
    setError('');
    setSuccess('');
    await closeMongoshSession({ silent: true });
    try {
      const created = await api.createConsoleSession(scopeInfo, { shell: shellMode });
      const sessionId = String(created?.sessionId || '').trim();
      if (!sessionId) throw new Error('Failed to open console shell session.');
      const createdModeRaw = String(created?.mode || shellMode).trim().toLowerCase();
      const createdMode = createdModeRaw === 'mongo' ? 'mongo' : 'mongosh';
      if (mongoshStartNonceRef.current !== startNonce) return;
      mongoshSessionRef.current = sessionId;
      mongoshSessionShellRef.current = createdMode;
      setMongoshSessionId(sessionId);
      setCurrentDb(created?.activeDb || scopeInfo.db || '');
      const controller = new AbortController();
      streamAbortRef.current = controller;
      api.streamConsoleSession(sessionId, {
        signal: controller.signal,
        onEvent: (evt) => {
          if (mongoshStartNonceRef.current !== startNonce) return;
          if (!evt) return;
          if (evt.event === 'ready') {
            const finalized = Boolean(evt?.data?.finalized);
            const readyMode = String(evt?.data?.mode || '').trim().toLowerCase();
            if (readyMode === 'mongo' || readyMode === 'mongosh') {
              mongoshSessionShellRef.current = readyMode;
            }
            setMongoshConnected(!finalized);
            const nextDb = evt?.data?.activeDb;
            if (typeof nextDb === 'string' && nextDb.trim()) setCurrentDb(nextDb.trim());
            if (finalized) {
              mongoshSessionRef.current = '';
              mongoshSessionShellRef.current = 'mongosh';
              setMongoshSessionId('');
            }
            return;
          }
          if (evt.event === 'end') {
            setMongoshConnected(false);
            mongoshSessionRef.current = '';
            mongoshSessionShellRef.current = 'mongosh';
            setMongoshSessionId('');
            return;
          }
          const data = evt.data;
          if (data && typeof data === 'object') {
            if (data.type === 'stdout') appendLine('stdout', { text: String(data.text || '') });
            else if (data.type === 'stderr' || data.type === 'error') {
              const message = normalizeMongoshMessage(String(data.text || ''));
              appendLine('stderr', { text: message });
              if (/binary not found/i.test(message)) setError(message);
            }
            else if (data.type === 'system') appendLine('info', { text: String(data.text || '') });
            else if (data.type === 'exit') {
              appendLine('info', { text: `console shell session closed (${data.reason || 'exit'}).` });
              setMongoshConnected(false);
              mongoshSessionRef.current = '';
              mongoshSessionShellRef.current = 'mongosh';
              setMongoshSessionId('');
              if (String(data.reason || '').toLowerCase() === 'spawn_enoent') {
                setError('Console shell binary not found on server host. Install mongo/mongosh and configure MONGO_BIN or MONGOSH_BIN.');
              }
            }
            return;
          }
          if (typeof evt.raw === 'string' && evt.raw.trim()) appendLine('stdout', { text: evt.raw });
        },
        onError: (err) => {
          if (mongoshStartNonceRef.current !== startNonce) return;
          const message = normalizeMongoshMessage(err?.message || 'Console shell stream failed.');
          appendLine('error', { text: message });
          setError(message);
          setMongoshConnected(false);
        },
        onClose: () => {
          if (mongoshStartNonceRef.current !== startNonce) return;
          setMongoshConnected(false);
        },
      }, { shell: createdMode }).catch((err) => {
        if (err?.name === 'AbortError') return;
        if (mongoshStartNonceRef.current !== startNonce) return;
        const message = normalizeMongoshMessage(err?.message || 'Console shell stream closed.');
        appendLine('error', { text: message });
        setError(message);
      });
    } catch (err) {
      if (mongoshStartNonceRef.current !== startNonce) return;
      const message = normalizeMongoshMessage(err?.message || `Failed to start ${shellMode} session.`);
      setError(message);
      appendLine('error', { text: message });
    } finally {
      if (mongoshStartNonceRef.current === startNonce) {
        setSessionBusy(false);
        mongoshStartGuardRef.current = false;
      }
    }
  }, [appendLine, closeMongoshSession, isMockMode, scopeInfo, normalizeMongoshMessage]);

  const appendHelp = () => {
    HELP_LINES.forEach((text) => appendLine('info', { text }));
    appendLine('info', { text: `Current scope: ${getScopeTag(scopeInfo)}.` });
  };

  useEffect(() => {
    mongoshSessionRef.current = mongoshSessionId;
  }, [mongoshSessionId]);

  useEffect(() => {
    const nextDb = scopeInfo.db || '';
    setCurrentDb(nextDb);
    setConsoleMode('ui');
    setError('');
    setSuccess('');
    setIndexHints([]);
    setSelectedHint('auto');
    setRunElapsedMs(null);
    setRunLiveTimer(0);
    setExplainRunning(false);
    setExplainElapsedMs(null);
    setExplainLiveTimer(0);
    setExampleId(examples[0]?.id || '');
    setCommand('');
    setHistory([]);
    setHistoryIndex(-1);
    setHistoryDraft('');
    setLines([
      {
        id: Date.now(),
        kind: 'info',
        text: `Console scope: ${getScopeTag(scopeInfo)}.`,
      },
      {
        id: Date.now() + 1,
        kind: 'info',
        text: scopeInfo.level === 'global'
          ? 'Use "use <dbName>" to choose active database.'
          : 'Context is locked for safety.',
      },
    ]);
    cancelUiRun();
    explainAbortRef.current?.abort();
    explainAbortRef.current = null;
    stopRunTimer();
    stopExplainTimer();
    invalidateMongoshStart();
    closeMongoshSession({ silent: true }).catch(() => {});
  }, [scopeKey, refreshToken, examples, scopeInfo, closeMongoshSession, cancelUiRun, invalidateMongoshStart, stopRunTimer, stopExplainTimer]);

  useEffect(() => {
    if (scopeInfo.level !== 'collection' || !scopeInfo.db || !scopeInfo.collection || isRealShellMode) {
      setIndexHints([]);
      setSelectedHint('auto');
      return;
    }
    let active = true;
    api.getIndexes(scopeInfo.db, scopeInfo.collection, { source: 'console' })
      .then((data) => {
        if (!active) return;
        const names = (data?.indexes || []).map((idx) => String(idx?.name || '').trim()).filter(Boolean);
        setIndexHints(names);
      })
      .catch(() => {
        if (active) setIndexHints([]);
      });
    return () => {
      active = false;
    };
  }, [scopeInfo.level, scopeInfo.db, scopeInfo.collection, scopeKey, refreshToken, isRealShellMode]);

  useEffect(() => {
    if (selectedHint === 'auto') return;
    if (!indexHints.includes(selectedHint)) setSelectedHint('auto');
  }, [indexHints, selectedHint]);

  useEffect(() => {
    if (!isRealShellMode) {
      invalidateMongoshStart();
      closeMongoshSession({ silent: true }).catch(() => {});
      return;
    }
    cancelUiRun();
    startMongoshSession(selectedShell);
  }, [consoleMode, scopeKey, selectedShell, isRealShellMode, startMongoshSession, closeMongoshSession, cancelUiRun, invalidateMongoshStart]);

  useEffect(() => {
    if (previousConsoleModeRef.current === consoleMode) return;
    previousConsoleModeRef.current = consoleMode;
    // Isolate console modes: do not transfer input/output/history between engines.
    setLines([]);
    setCommand('');
    setHistory([]);
    setHistoryIndex(-1);
    setHistoryDraft('');
    setExampleId(examples[0]?.id || '');
    setError('');
    setSuccess('');
    setRunElapsedMs(null);
    setRunLiveTimer(0);
    setExplainRunning(false);
    setExplainElapsedMs(null);
    setExplainLiveTimer(0);
    explainAbortRef.current?.abort();
    explainAbortRef.current = null;
    stopRunTimer();
    stopExplainTimer();
  }, [consoleMode, examples, stopRunTimer, stopExplainTimer]);

  useEffect(() => () => {
    if (confirmResolverRef.current) {
      try { confirmResolverRef.current(false); } catch {}
      confirmResolverRef.current = null;
    }
    cancelUiRun();
    explainAbortRef.current?.abort();
    explainAbortRef.current = null;
    stopRunTimer();
    stopExplainTimer();
    invalidateMongoshStart();
    closeMongoshSession({ silent: true }).catch(() => {});
  }, [closeMongoshSession, cancelUiRun, invalidateMongoshStart, stopRunTimer, stopExplainTimer]);

  useEffect(() => {
    if (!logRef.current) return;
    logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [lines]);

  const lastResultLine = useMemo(
    () => [...lines].reverse().find((line) => line.kind === 'result' && line.data !== undefined),
    [lines],
  );
  const hintOptions = useMemo(
    () => [
      { value: 'auto', label: 'Auto' },
      ...indexHints.map((name) => ({ value: name, label: name })),
    ],
    [indexHints],
  );

  const availableDatabases = useMemo(() => {
    const normalized = Array.isArray(databaseNames)
      ? databaseNames
        .map((name) => String(name || '').trim())
        .filter(Boolean)
      : [];
    const unique = [...new Set(normalized)].sort((a, b) => a.localeCompare(b));
    const current = String(currentDb || '').trim();
    if (current && !unique.includes(current)) {
      return [current, ...unique];
    }
    return unique;
  }, [databaseNames, currentDb]);

  const activeDbDropdownOptions = useMemo(() => {
    if (availableDatabases.length === 0) return [{ value: '', label: 'No databases' }];
    return availableDatabases.map((name) => ({ value: name, label: name }));
  }, [availableDatabases]);

  useEffect(() => {
    if (scopeInfo.level === 'collection') return;
    const normalized = String(currentDb || '').trim();
    if (!normalized) return;
    onDbContextChange?.(normalized, {
      source: 'console',
      scope: scopeInfo.level,
      mode: consoleMode,
    });
  }, [consoleMode, currentDb, onDbContextChange, scopeInfo.level]);

  const autocomplete = useMemo(() => computeAutocomplete(command, scopeInfo), [command, scopeKey]);
  useEffect(() => {
    setAutocompleteIndex(0);
  }, [autocomplete.items.join('|')]);

  const applyAutocomplete = useCallback((methodName) => {
    if (!methodName || autocomplete.items.length === 0) return;
    const before = command.slice(0, autocomplete.replaceStart);
    const after = command.slice(autocomplete.replaceEnd);
    const hasOpenParen = after.trimStart().startsWith('(');
    const nextCommand = `${before}${hasOpenParen ? methodName : `${methodName}()`}${after}`;
    setCommand(nextCommand);
    requestAnimationFrame(() => {
      if (!commandInputRef.current) return;
      const cursor = hasOpenParen ? before.length + methodName.length : before.length + methodName.length + 1;
      commandInputRef.current.focus();
      commandInputRef.current.setSelectionRange(cursor, cursor);
    });
  }, [autocomplete, command]);

  const rememberCommand = useCallback((text) => {
    const normalized = String(text || '').trim();
    if (!normalized) return;
    setHistory((prev) => {
      if (prev[prev.length - 1] === normalized) return prev;
      return [...prev.slice(-199), normalized];
    });
    setHistoryIndex(-1);
    setHistoryDraft('');
  }, []);

  const syncCommandOverlayScroll = useCallback(() => {
    if (!commandInputRef.current || !commandOverlayRef.current) return;
    commandOverlayRef.current.scrollTop = commandInputRef.current.scrollTop;
    commandOverlayRef.current.scrollLeft = commandInputRef.current.scrollLeft;
  }, []);

  useEffect(() => {
    syncCommandOverlayScroll();
  }, [command, syncCommandOverlayScroll]);

  const ensureDbAllowed = (dbName) => {
    if (!dbName) throw new Error('No active database. Use "use <dbName>".');
    if (scopeInfo.level === 'collection' || scopeInfo.level === 'database') {
      if (dbName !== scopeInfo.db) throw new Error(`Context locked to database ${scopeInfo.db}.`);
    }
  };

  const ensureCollectionAllowed = (targetCollection) => {
    if (!targetCollection) throw new Error('Collection name is required.');
    if (scopeInfo.level === 'collection' && targetCollection !== scopeInfo.collection) {
      throw new Error(`Context locked to collection ${scopeInfo.collection}.`);
    }
  };

  const runCollectionMethod = async (targetDb, targetCollection, method, args, controller) => {
    const lowered = String(method || '').toLowerCase();
    const resolveHint = (value) => {
      const explicit = typeof value === 'string' ? value.trim() : '';
      if (explicit) return explicit;
      return selectedHint !== 'auto' ? selectedHint : 'auto';
    };
    const callOperate = async (opMethod, payload = {}, heavyConfirm = false) => {
      const response = await api.operateCollection(targetDb, targetCollection, opMethod, payload, { heavyConfirm, source: 'console', controller });
      return response?.result || response;
    };

    if (lowered === 'find') {
      const filter = isPlainObject(args[0]) ? args[0] : {};
      const options = isPlainObject(args[1]) ? args[1] : {};
      const sort = isPlainObject(options.sort) ? options.sort : {};
      const projection = isPlainObject(options.projection) ? options.projection : (isPlainObject(options.fields) ? options.fields : {});
      const limit = Number.isFinite(Number(options.limit)) ? Math.max(1, Math.min(Math.floor(Number(options.limit)), 5000)) : 50;
      const hint = resolveHint(options.hint);
      const data = await api.getDocuments(targetDb, targetCollection, {
        filter: JSON.stringify(filter),
        sort: JSON.stringify(sort),
        projection: JSON.stringify(projection),
        limit,
        hint,
      }, controller, { source: 'console' });
      return {
        documents: data.documents || [],
        total: Number(data?.total?.value || data?.total || (data.documents || []).length),
      };
    }

    if (lowered === 'findone') {
      const filter = isPlainObject(args[0]) ? args[0] : {};
      const data = await api.getDocuments(targetDb, targetCollection, { filter: JSON.stringify(filter), limit: 1 }, controller, { source: 'console' });
      return { value: (data.documents || [])[0] || null };
    }

    if (lowered === 'aggregate') {
      if (!Array.isArray(args[0])) throw new Error('aggregate pipeline must be an array.');
      const options = isPlainObject(args[1]) ? args[1] : {};
      const hint = resolveHint(options.hint);
      const data = await api.runAggregation(targetDb, targetCollection, args[0], controller, { source: 'console', hint });
      return { results: data.results || [], total: Number(data.total || (data.results || []).length) };
    }

    if (lowered === 'insertone') return callOperate('insertOne', { document: args[0] }, false);
    if (lowered === 'insertmany') return callOperate('insertMany', { documents: args[0], ordered: isPlainObject(args[1]) ? args[1].ordered : undefined }, true);
    if (lowered === 'updateone' || lowered === 'updatemany') {
      const options = isPlainObject(args[2]) ? args[2] : {};
      return callOperate(
        lowered === 'updateone' ? 'updateOne' : 'updateMany',
        { filter: args[0] || {}, update: args[1], upsert: options.upsert, hint: options.hint, collation: options.collation, arrayFilters: options.arrayFilters },
        lowered === 'updatemany',
      );
    }
    if (lowered === 'replaceone') {
      const options = isPlainObject(args[2]) ? args[2] : {};
      return callOperate('replaceOne', { filter: args[0] || {}, replacement: args[1] || {}, upsert: options.upsert, hint: options.hint, collation: options.collation }, false);
    }
    if (lowered === 'deleteone' || lowered === 'deletemany') return callOperate(lowered === 'deleteone' ? 'deleteOne' : 'deleteMany', { filter: args[0] || {} }, lowered === 'deletemany');
    if (lowered === 'findoneandupdate') return callOperate('findOneAndUpdate', { filter: args[0] || {}, update: args[1], ...(isPlainObject(args[2]) ? args[2] : {}) }, false);
    if (lowered === 'findoneandreplace') return callOperate('findOneAndReplace', { filter: args[0] || {}, replacement: args[1] || {}, ...(isPlainObject(args[2]) ? args[2] : {}) }, false);
    if (lowered === 'findoneanddelete') return callOperate('findOneAndDelete', { filter: args[0] || {}, ...(isPlainObject(args[1]) ? args[1] : {}) }, false);
    if (lowered === 'findandmodify') return callOperate('findAndModify', isPlainObject(args[0]) ? args[0] : {}, false);
    if (lowered === 'remove') return callOperate('remove', { filter: args[0] || {}, justOne: args[1] === true }, args[1] !== true);
    if (lowered === 'bulkwrite') return callOperate('bulkWrite', { operations: args[0] || [], ...(isPlainObject(args[1]) ? args[1] : {}) }, true);
    if (lowered === 'countdocuments') return callOperate('countDocuments', { filter: args[0] || {}, ...(isPlainObject(args[1]) ? args[1] : {}) }, false);
    if (lowered === 'estimateddocumentcount') return callOperate('estimatedDocumentCount', {}, false);
    if (lowered === 'distinct') {
      const field = String(args[0] || '').trim();
      if (!field) throw new Error('distinct(field) requires a field name.');
      const data = await api.getDistinct(targetDb, targetCollection, field, { source: 'console', controller });
      return { field, values: data.values || [] };
    }
    if (lowered === 'createindex') return api.createIndex(targetDb, targetCollection, isPlainObject(args[0]) ? args[0] : {}, { ...(isPlainObject(args[1]) ? args[1] : {}), source: 'console', controller });
    if (lowered === 'dropindex') return api.dropIndex(targetDb, targetCollection, String(args[0] || '').trim(), { heavyConfirm: true, source: 'console', controller });
    if (lowered === 'getindexes' || lowered === 'listindexes') return api.getIndexes(targetDb, targetCollection, { source: 'console', controller });
    if (lowered === 'dropindexes') return callOperate('dropIndexes', { names: args[0] === undefined ? '*' : args[0] }, true);
    if (lowered === 'hideindex' || lowered === 'unhideindex') return callOperate(lowered === 'hideindex' ? 'hideIndex' : 'unhideIndex', { name: String(args[0] || '').trim() }, false);
    if (lowered === 'renamecollection') return callOperate('renameCollection', { to: String(args[0] || '').trim(), ...(isPlainObject(args[1]) ? args[1] : {}) }, false);
    if (lowered === 'validate') return callOperate('validateCollection', isPlainObject(args[0]) ? args[0] : {}, true);
    if (lowered === 'reindex') return callOperate('reIndex', {}, true);
    if (lowered === 'stats' || lowered === 'collstats') return api.getCollectionStats(targetDb, targetCollection, { source: 'console', controller });

    throw new Error(`Unsupported collection method "${method}".`);
  };

  const runDbMethod = async (targetDb, method, args, controller) => {
    const lowered = String(method || '').toLowerCase();
    if (lowered === 'stats' || lowered === 'getstats') return api.getDatabaseStats(targetDb, { refresh: true, source: 'console', controller });
    if (lowered === 'getcollectionnames' || lowered === 'listcollections') {
      const data = await api.listCollections(targetDb, { withStats: false, source: 'console', controller });
      const names = (data.collections || []).map((item) => item.name).filter(Boolean);
      return { count: names.length, collections: names };
    }
    if (lowered === 'getcollectioninfos') {
      const data = await api.listCollections(targetDb, { withStats: true, source: 'console', controller });
      return { collections: data.collections || [] };
    }
    if (lowered === 'createcollection') {
      const name = String(args[0] || '').trim();
      if (!name) throw new Error('createCollection(name) requires a collection name.');
      return api.createCollection(targetDb, name, { source: 'console', controller });
    }
    if (lowered === 'dropdatabase') {
      const confirmed = await requestConfirm({
        title: 'Drop Database',
        message: `Drop database "${targetDb}"?\nThis action cannot be undone.`,
        confirmLabel: 'Drop',
        danger: true,
      });
      if (!confirmed) throw new Error('Cancelled.');
      return api.dropDatabase(targetDb, { heavyConfirm: true, source: 'console', controller });
    }
    throw new Error(`Unsupported database method "${method}".`);
  };

  const handleRun = async (forcedCommand = null, options = {}) => {
    const syncInput = options?.syncInput !== false;
    if (busy || sessionBusy || explainRunning) return;
    if (consolesLocked) {
      setError('Admin access key is required to use consoles. Go to Settings → Rate Limit to enter the key.');
      return;
    }
    const text = String((forcedCommand ?? command) || '').trim();
    if (!text) {
      setError('Command is empty.');
      return;
    }
    if (forcedCommand !== null && syncInput) {
      setCommand(text);
    }
    rememberCommand(text);

    if (isRealShellMode) {
      const activeShell = mongoshSessionShellRef.current || selectedShell;
      if (!mongoshSessionRef.current || !mongoshConnected) {
        setError(`${activeShell} shell is not connected. Click Reconnect.`);
        return;
      }
      if (/^(?:clear|\.clear)\s*;?$/i.test(text)) {
        setLines([]);
        setSuccess('Log cleared.');
        return;
      }
      setBusy(true);
      setError('');
      setSuccess('');
      try {
        appendLine('command', { text });
        await api.sendConsoleCommand(mongoshSessionRef.current, text, { shell: activeShell });
        const useMatch = text.match(/^use\s+["']?([a-zA-Z0-9_.-]+)["']?\s*;?$/i);
        if (useMatch && scopeInfo.level === 'global') setCurrentDb(useMatch[1]);
        setSuccess(`Command sent to ${activeShell}.`);
      } catch (err) {
        const message = err?.message || 'Command failed.';
        setError(message);
        appendLine('error', { text: message });
      } finally {
        setBusy(false);
      }
      return;
    }

    setBusy(true);
    setError('');
    setSuccess('');
    setRunElapsedMs(null);
    startRunTimer();
    const startedAt = performance.now();
    const controller = new AbortController();
    uiRunAbortRef.current = controller;
    try {
      const parsed = parseCommand(text);

      if (parsed.kind === 'clear') {
        setLines([]);
        return;
      }
      if (parsed.kind === 'help') {
        appendHelp();
        return;
      }
      if (parsed.kind === 'use') {
        if (scopeInfo.level !== 'global') throw new Error('use is only available in global scope.');
        appendLine('command', { text });
        setCurrentDb(parsed.dbName);
        appendLine('info', { text: `Switched to database ${parsed.dbName}` });
        setSuccess(`active database: ${parsed.dbName}`);
        return;
      }

      const activeDb = String(currentDb || scopeInfo.db || '').trim();
      ensureDbAllowed(activeDb);

      appendLine('command', { text });

      let result;
      if (parsed.kind === 'collection') {
        const targetCollection = (
          scopeInfo.level === 'collection' && parsed.targetCollection === 'collection'
            ? (scopeInfo.collection || parsed.targetCollection)
            : parsed.targetCollection
        );
        ensureCollectionAllowed(targetCollection);
        const args = parseArgs(parsed.argsRaw);
        result = await runCollectionMethod(activeDb, targetCollection, parsed.method, args, controller);
      } else {
        const args = parseArgs(parsed.argsRaw);
        result = await runDbMethod(activeDb, parsed.method, args, controller);
      }

      const elapsedMs = Math.round(performance.now() - startedAt);
      onQueryMs?.(elapsedMs);
      setRunElapsedMs(elapsedMs);
      appendLine('result', { elapsedMs, data: result });
      setSuccess(`done in ${formatDuration(elapsedMs)}`);
    } catch (err) {
      if (err?.name === 'AbortError') {
        setSuccess('Command cancelled.');
        appendLine('info', { text: 'Command cancelled.' });
        return;
      }
      const message = err?.message || 'Command failed.';
      setError(message);
      appendLine('error', { text: message });
    } finally {
      if (uiRunAbortRef.current === controller) uiRunAbortRef.current = null;
      stopRunTimer();
      setBusy(false);
    }
  };

  const applySelectedDb = useCallback(async (dbName = '') => {
    if (scopeInfo.level !== 'global') return;
    const nextDb = String(dbName || currentDb || '').trim();
    if (!nextDb) {
      setError('Select a database first.');
      return;
    }
    setCurrentDb(nextDb);
    await handleRun(`use ${nextDb}`, { syncInput: false });
    setCommand('');
    requestAnimationFrame(() => commandInputRef.current?.focus());
  }, [currentDb, handleRun, scopeInfo.level]);

  const handleExplain = async () => {
    if (isRealShellMode || sessionBusy || busy) return;
    if (explainRunning) {
      explainAbortRef.current?.abort();
      return;
    }
    const text = String(command || '').trim();
    if (!text) {
      setError('Command is empty.');
      return;
    }

    const startedAt = performance.now();
    const controller = new AbortController();
    explainAbortRef.current = controller;
    setError('');
    setSuccess('');
    setExplainElapsedMs(null);
    setExplainRunning(true);
    startExplainTimer();
    try {
      const parsed = parseCommand(text);
      if (parsed.kind !== 'collection') {
        throw new Error('Explain supports only collection commands: find(...) or aggregate([...]).');
      }
      const activeDb = String(currentDb || scopeInfo.db || '').trim();
      ensureDbAllowed(activeDb);
      const targetCollection = (
        scopeInfo.level === 'collection' && parsed.targetCollection === 'collection'
          ? (scopeInfo.collection || parsed.targetCollection)
          : parsed.targetCollection
      );
      ensureCollectionAllowed(targetCollection);

      const method = String(parsed.method || '').toLowerCase();
      const args = parseArgs(parsed.argsRaw);
      let explainPayload;
      if (method === 'find') {
        const filter = isPlainObject(args[0]) ? args[0] : {};
        const options = isPlainObject(args[1]) ? args[1] : {};
        const sort = isPlainObject(options.sort) ? options.sort : {};
        const limit = Number.isFinite(Number(options.limit)) ? Math.max(1, Math.floor(Number(options.limit))) : undefined;
        const commandHint = typeof options.hint === 'string' ? options.hint.trim() : '';
        explainPayload = {
          type: 'find',
          filter,
          sort,
          hint: commandHint || selectedHint,
          ...(Number.isFinite(limit) ? { limit } : {}),
          verbosity: 'executionStats',
        };
      } else if (method === 'aggregate') {
        if (!Array.isArray(args[0])) throw new Error('aggregate pipeline must be an array.');
        const options = isPlainObject(args[1]) ? args[1] : {};
        const commandHint = typeof options.hint === 'string' ? options.hint.trim() : '';
        explainPayload = {
          type: 'aggregate',
          pipeline: args[0],
          hint: commandHint || selectedHint,
          verbosity: 'executionStats',
        };
      } else {
        throw new Error('Explain currently supports only find() and aggregate() commands.');
      }

      const data = await api.explain(activeDb, targetCollection, explainPayload, { controller });
      const elapsedMs = Number(data?._elapsed) || Math.round(performance.now() - startedAt);
      setExplainElapsedMs(elapsedMs);
      appendLine('explain', { elapsedMs, data });
      setSuccess(`explain in ${formatDuration(elapsedMs)}`);
    } catch (err) {
      if (err?.name === 'AbortError') {
        setSuccess('Explain cancelled.');
        appendLine('info', { text: 'Explain cancelled.' });
        return;
      }
      const message = err?.message || 'Explain failed.';
      setError(message);
      appendLine('error', { text: message });
    } finally {
      if (explainAbortRef.current === controller) explainAbortRef.current = null;
      setExplainRunning(false);
      stopExplainTimer();
    }
  };

  const handleInterrupt = async () => {
    if (!isRealShellMode || !mongoshSessionRef.current) return;
    const activeShell = mongoshSessionShellRef.current || selectedShell;
    try {
      await api.interruptConsoleSession(mongoshSessionRef.current, { shell: activeShell });
      setSuccess('Interrupt signal sent.');
    } catch (err) {
      setError(err?.message || 'Failed to interrupt command.');
    }
  };

  const handleCancel = () => {
    if (isRealShellMode) {
      handleInterrupt();
      return;
    }
    if (explainRunning) {
      explainAbortRef.current?.abort();
      return;
    }
    if (!cancelUiRun()) return;
    stopRunTimer();
    setBusy(false);
  };

  const handleCommandKeyDown = (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
      event.preventDefault();
      handleRun();
      return;
    }

    if (autocomplete.items.length > 0 && event.key === 'Tab') {
      event.preventDefault();
      const picked = autocomplete.items[Math.max(0, Math.min(autocompleteIndex, autocomplete.items.length - 1))];
      applyAutocomplete(picked);
      return;
    }
    if (autocomplete.items.length > 0 && event.altKey && event.key === 'ArrowDown') {
      event.preventDefault();
      setAutocompleteIndex((prev) => Math.min(prev + 1, autocomplete.items.length - 1));
      return;
    }
    if (autocomplete.items.length > 0 && event.altKey && event.key === 'ArrowUp') {
      event.preventDefault();
      setAutocompleteIndex((prev) => Math.max(prev - 1, 0));
      return;
    }

    if (event.key !== 'ArrowUp' && event.key !== 'ArrowDown') return;
    if (history.length === 0) return;
    const element = event.currentTarget;
    const start = element.selectionStart;
    const end = element.selectionEnd;
    if (start !== end) return;
    const before = command.slice(0, start);
    const after = command.slice(start);
    const atFirstLine = !before.includes('\n');
    const atLastLine = !after.includes('\n');

    if (event.key === 'ArrowUp' && atFirstLine) {
      event.preventDefault();
      if (historyIndex === -1) {
        setHistoryDraft(command);
        const next = history.length - 1;
        setHistoryIndex(next);
        setCommand(history[next]);
      } else if (historyIndex > 0) {
        const next = historyIndex - 1;
        setHistoryIndex(next);
        setCommand(history[next]);
      }
      return;
    }
    if (event.key === 'ArrowDown' && atLastLine) {
      event.preventDefault();
      if (historyIndex === -1) return;
      const next = historyIndex + 1;
      if (next >= history.length) {
        setHistoryIndex(-1);
        setCommand(historyDraft);
      } else {
        setHistoryIndex(next);
        setCommand(history[next]);
      }
    }
  };

  const exportLog = () => {
    const payload = {
      exportedAt: new Date().toISOString(),
      scope: scopeInfo,
      mode: consoleMode,
      activeDb: currentDb || null,
      lines: lines.map((line) => ({
        kind: line.kind,
        text: line.text || '',
        elapsedMs: line.elapsedMs || null,
        data: line.data === undefined ? null : line.data,
      })),
    };
    downloadText(`mongostudio-console-log-${Date.now()}.json`, JSON.stringify(payload, null, 2), 'application/json');
  };

  const exportLastResult = () => {
    if (!lastResultLine) {
      appendLine('info', { text: 'No result to export yet.' });
      return;
    }
    const payload = {
      exportedAt: new Date().toISOString(),
      scope: scopeInfo,
      mode: consoleMode,
      activeDb: currentDb || null,
      elapsedMs: lastResultLine.elapsedMs || null,
      data: lastResultLine.data,
    };
    downloadText(`mongostudio-console-result-${Date.now()}.json`, JSON.stringify(payload, null, 2), 'application/json');
  };

  const copyLastResult = async () => {
    if (!lastResultLine) {
      appendLine('info', { text: 'No result to copy yet.' });
      return;
    }
    try {
      const text = JSON.stringify(lastResultLine.data, null, 2);
      const copied = await copyToClipboard(text);
      if (!copied) throw new Error('Clipboard unavailable.');
      setSuccess('Result copied to clipboard.');
    } catch (err) {
      setError(err?.message || 'Failed to copy result.');
    }
  };

  const commandPlaceholder = scopeInfo.level === 'database'
    ? 'db.stats()'
    : scopeInfo.level === 'collection'
      ? `db.${scopeInfo.collection || 'collection'}.find({})`
      : 'db.getCollectionNames()';
  const mongoshStarting = isRealShellMode && sessionBusy;
  const mongoshWaiting = isRealShellMode && !sessionBusy && !mongoshConnected;
  const runDisabled = busy || sessionBusy || mongoshWaiting || (!isRealShellMode && explainRunning);
  const runLabel = busy
    ? 'Running...'
    : sessionBusy
      ? 'Starting...'
      : mongoshWaiting
        ? 'Shell offline'
        : 'Run';

  return (
    <div className="h-full flex flex-col ms-console-root">
      <div className="flex-shrink-0 px-4 py-2.5" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
        <div className="flex items-center justify-between gap-3 overflow-x-auto">
        <div className="flex items-center gap-2 shrink-0 whitespace-nowrap">
          <Terminal className="w-4 h-4" style={{ color: 'var(--accent)' }} />
          <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>Console</h3>
          <span className="badge-blue inline-flex items-center gap-1 whitespace-nowrap">
            <Lock className="w-3 h-3" />
            {scopeLabel(scopeInfo)}
          </span>
          <div className="inline-flex rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)' }}>
            <button
              type="button"
              className="px-2.5 py-1 text-2xs font-medium whitespace-nowrap"
              style={consoleMode === 'ui'
                ? { background: 'var(--accent)', color: 'var(--surface-0)' }
                : { background: 'transparent', color: 'var(--text-secondary)' }}
              onClick={() => setConsoleMode('ui')}
              disabled={consolesLocked}
              title={consolesLocked ? 'Admin access key required' : ''}
            >
              ConsoleUI
            </button>
            <button
              type="button"
              className="px-2.5 py-1 text-2xs font-medium whitespace-nowrap"
              style={consoleMode === 'mongo'
                ? { background: 'var(--accent)', color: 'var(--surface-0)' }
                : { background: 'transparent', color: 'var(--text-secondary)' }}
              onClick={() => setConsoleMode('mongo')}
              disabled={isMockMode || consolesLocked}
              title={consolesLocked ? 'Admin access key required' : (isMockMode ? 'Unavailable in Demo mode' : 'mongosh 1.x — supports MongoDB 3.6+')}
            >
              mongosh 1.x
            </button>
            <button
              type="button"
              className="px-2.5 py-1 text-2xs font-medium whitespace-nowrap"
              style={consoleMode === 'mongosh'
                ? { background: 'var(--accent)', color: 'var(--surface-0)' }
                : { background: 'transparent', color: 'var(--text-secondary)' }}
              onClick={() => setConsoleMode('mongosh')}
              disabled={isMockMode || consolesLocked}
              title={consolesLocked ? 'Admin access key required' : (isMockMode ? 'Unavailable in Demo mode' : 'mongosh 2.x — requires MongoDB 4.2+')}
            >
              mongosh 2.x
            </button>
          </div>
          {currentDb && <span className="badge-blue font-mono whitespace-nowrap">db: {currentDb}</span>}
          {scopeInfo.collection && <span className="badge-purple font-mono whitespace-nowrap">collection: {scopeInfo.collection}</span>}
          {isRealShellMode && (
            <span
              className="badge-blue inline-flex items-center gap-1 whitespace-nowrap"
              style={mongoshConnected ? { color: '#34d399' } : (mongoshStarting ? { color: '#fbbf24' } : { color: '#fca5a5' })}
            >
              <Zap className="w-3 h-3" />
              {mongoshConnected ? `${selectedShell} live` : (mongoshStarting ? `${selectedShell} starting` : `${selectedShell} offline`)}
            </span>
          )}
        </div>
        <div className="flex items-center gap-1 shrink-0 whitespace-nowrap">
          {isRealShellMode && (
            <>
              <button type="button" className="btn-ghost text-xs whitespace-nowrap" onClick={() => startMongoshSession(selectedShell)} disabled={sessionBusy || busy}>
                Reconnect
              </button>
              <button type="button" className="btn-ghost text-xs whitespace-nowrap" onClick={handleInterrupt} disabled={!mongoshSessionId || !mongoshConnected || sessionBusy}>
                Interrupt
              </button>
            </>
          )}
          <button type="button" className="btn-ghost text-xs inline-flex items-center gap-1 whitespace-nowrap" onClick={exportLog} disabled={lines.length === 0}>
            <Download className="w-3.5 h-3.5" />
            Export log
          </button>
          <button type="button" className="btn-ghost text-xs inline-flex items-center gap-1 whitespace-nowrap" onClick={() => setLines([])}>
            <Refresh className="w-3.5 h-3.5" />
            Clear log
          </button>
        </div>
        </div>
      </div>

      <div className="flex-shrink-0 px-4 py-3 space-y-2.5" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
          <label className="space-y-1.5">
            <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Example</span>
            <DropdownSelect
              fullWidth
              sizeClassName="text-xs"
              value={exampleId}
              options={examples.map((item) => ({ value: item.id, label: item.label }))}
              onChange={(nextId) => {
                setExampleId(nextId);
                const next = examples.find((item) => item.id === nextId);
                if (next) setCommand(next.value);
              }}
              menuZIndex={menuZIndex}
            />
          </label>
          <label className="space-y-1.5">
            <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Active Database</span>
            {scopeInfo.level === 'global' ? (
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs font-mono"
                value={currentDb}
                options={activeDbDropdownOptions}
                onChange={(nextDb) => {
                  const normalized = String(nextDb || '').trim();
                  setCurrentDb(normalized);
                  if (!normalized) return;
                  applySelectedDb(normalized);
                }}
                menuZIndex={menuZIndex}
              />
            ) : (
              <input
                value={currentDb}
                disabled
                placeholder="locked by scope"
                className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none disabled:opacity-75"
                style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
              />
            )}
          </label>
        </div>

        <div className="relative rounded-lg" style={{ background: 'var(--console-surface)', border: '1px solid var(--console-border)' }}>
          <pre
            ref={commandOverlayRef}
            className="absolute inset-0 m-0 p-3 text-xs font-mono whitespace-pre-wrap break-words pointer-events-none overflow-hidden select-none"
            style={{ color: CONSOLE_COLOR.plain, userSelect: 'none', WebkitUserSelect: 'none' }}
            aria-hidden="true"
          >
            {renderConsoleSyntax(`${command}${command.endsWith('\n') ? ' ' : ''}`, 'input')}
          </pre>
          {!command && (
            <div className="absolute left-3 top-2 text-xs font-mono pointer-events-none" style={{ color: 'var(--console-muted)' }}>
              {commandPlaceholder}
            </div>
          )}
          <textarea
            ref={commandInputRef}
            value={command}
            onChange={(event) => {
              setCommand(event.target.value);
              requestAnimationFrame(syncCommandOverlayScroll);
            }}
            onScroll={syncCommandOverlayScroll}
            onKeyDown={handleCommandKeyDown}
            spellCheck={false}
            className="relative w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none ms-console-input"
            style={{
              minHeight: '96px',
              background: 'transparent',
              border: 'none',
              color: 'transparent',
              caretColor: 'var(--console-caret)',
              resize: 'vertical',
            }}
          />
        </div>

        {autocomplete.items.length > 0 && (
          <div className="flex flex-wrap items-center gap-1">
            <span className="text-2xs mr-1" style={{ color: 'var(--text-tertiary)' }}>Autocomplete:</span>
            {autocomplete.items.slice(0, 8).map((item, index) => (
              <button
                key={item}
                type="button"
                className="btn-ghost text-2xs px-2 py-1"
                onClick={() => applyAutocomplete(item)}
                style={index === autocompleteIndex ? { borderColor: 'var(--accent)', color: 'var(--accent)' } : undefined}
              >
                {item}()
              </button>
            ))}
            <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Tab apply | Alt+Up/Down pick | Ctrl+Enter run</span>
          </div>
        )}

        <div className="flex flex-wrap items-center gap-2">
          <button type="button" className="btn-primary text-xs inline-flex items-center gap-1.5" onClick={() => handleRun()} disabled={runDisabled}>
            {(busy || sessionBusy) ? (
              <Loader className="w-3.5 h-3.5" />
            ) : mongoshWaiting ? (
              <AlertCircle className="w-3.5 h-3.5" />
            ) : (
              <Play className="w-3.5 h-3.5" />
            )}
            {runLabel}
          </button>
          <button
            type="button"
            className="btn-ghost text-xs"
            onClick={handleCancel}
            disabled={isRealShellMode ? (!mongoshSessionId || sessionBusy) : (!busy && !explainRunning)}
          >
            Cancel
          </button>
          <button
            type="button"
            className="btn-ghost text-xs"
            onClick={async () => {
              if (isRealShellMode) {
                await handleRun('help');
                commandInputRef.current?.focus();
                return;
              }
              appendHelp();
              commandInputRef.current?.focus();
            }}
            disabled={busy || sessionBusy}
          >
            Help
          </button>
          {!isRealShellMode && (
            <button
              type="button"
              className={`btn-ghost text-xs inline-flex items-center gap-1 ${explainRunning ? 'text-red-400' : ''}`}
              onClick={handleExplain}
              disabled={busy || sessionBusy}
              title={explainRunning ? 'Cancel explain' : 'Explain command'}
            >
              {explainRunning ? <Loader className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
              {explainRunning ? 'Cancel Explain' : 'Explain'}
            </button>
          )}
          {!isRealShellMode && scopeInfo.collection && (
            <div className="inline-flex items-center gap-1.5">
              <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Hint</span>
              <DropdownSelect
                value={selectedHint}
                options={hintOptions}
                onChange={(nextValue) => setSelectedHint(String(nextValue || 'auto'))}
                sizeClassName="text-2xs"
                menuZIndex={menuZIndex}
              />
            </div>
          )}
          <button type="button" className="btn-ghost text-xs inline-flex items-center gap-1" onClick={exportLastResult} disabled={!lastResultLine || isRealShellMode}>
            <Download className="w-3.5 h-3.5" />
            Export result
          </button>
          <button type="button" className="btn-ghost text-xs inline-flex items-center gap-1" onClick={copyLastResult} disabled={!lastResultLine || isRealShellMode}>
            <Copy className="w-3.5 h-3.5" />
            Copy result
          </button>
          {!isRealShellMode && busy && (
            <span className="inline-flex items-center gap-1.5 text-2xs animate-pulse" style={{ color: 'var(--accent)' }}>
              <Loader className="w-3 h-3" />
              {(runLiveTimer / 1000).toFixed(1)}s
            </span>
          )}
          {!isRealShellMode && explainRunning && (
            <span className="inline-flex items-center gap-1.5 text-2xs animate-pulse" style={{ color: 'var(--accent)' }}>
              <Eye className="w-3 h-3" />
              {(explainLiveTimer / 1000).toFixed(1)}s
            </span>
          )}
          {!isRealShellMode && ((runElapsedMs !== null && !busy) || (explainElapsedMs !== null && !explainRunning)) && (
            <span
              className="inline-flex items-center gap-1.5 text-2xs px-2 py-1 rounded-md"
              style={{ color: 'var(--text-tertiary)', background: 'var(--surface-2)', border: '1px solid var(--border)' }}
            >
              {runElapsedMs !== null && !busy && <span>Run {formatDuration(runElapsedMs)}</span>}
              {runElapsedMs !== null && !busy && explainElapsedMs !== null && !explainRunning && (
                <span className="opacity-40">|</span>
              )}
              {explainElapsedMs !== null && !explainRunning && <span>Explain {formatDuration(explainElapsedMs)}</span>}
            </span>
          )}
        </div>
      </div>

      <div className="flex-1 min-h-0 p-4 flex flex-col gap-2">
        {error && (
          <div className="text-xs px-3 py-2 rounded-lg" style={{ color: '#fca5a5', background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.25)' }}>
            {error}
          </div>
        )}
        {success && (
          <div className="text-xs px-3 py-2 rounded-lg" style={{ color: '#34d399', background: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.25)' }}>
            {success}
          </div>
        )}
        <div
          ref={logRef}
          className="flex-1 min-h-0 rounded-lg p-3 font-mono text-2xs overflow-auto"
          style={{ background: 'var(--console-surface-alt)', border: '1px solid var(--console-border)' }}
        >
          {lines.length === 0 && (
            <div style={{ color: 'var(--console-muted)' }}>No output yet.</div>
          )}
          {lines.map((line) => (
            <div key={line.id} className="mb-2">
              {line.kind === 'command' && (
                <div style={{ color: CONSOLE_COLOR.method }}>
                  <span style={{ color: 'var(--console-prompt)' }}>{'>'} </span>
                  {renderConsoleSyntax(line.text, `command:${line.id}`)}
                </div>
              )}
              {line.kind === 'info' && (
                <div className="whitespace-pre-wrap break-all">
                  {renderConsoleSyntax(line.text, `info:${line.id}`, { dim: true })}
                </div>
              )}
              {line.kind === 'stdout' && (
                <div className="whitespace-pre-wrap break-all" style={{ color: CONSOLE_COLOR.plain }}>
                  {renderConsoleSyntax(line.text, `stdout:${line.id}`)}
                </div>
              )}
              {line.kind === 'stderr' && (
                <div className="whitespace-pre-wrap break-all" style={{ color: '#fca5a5' }}>
                  {renderConsoleSyntax(line.text, `stderr:${line.id}`)}
                </div>
              )}
              {line.kind === 'error' && <div style={{ color: '#fca5a5' }}>{line.text}</div>}
              {line.kind === 'result' && (
                <>
                  <div style={{ color: 'var(--accent)' }}>ok ({formatDuration(line.elapsedMs || 0)})</div>
                  <div className="whitespace-pre-wrap break-all" style={{ color: CONSOLE_COLOR.plain }}>
                    {renderJsonSyntax(line.data, `result:${line.id}`)}
                  </div>
                </>
              )}
              {line.kind === 'explain' && (
                <>
                  <div style={{ color: '#60a5fa' }}>explain ({formatDuration(line.elapsedMs || 0)})</div>
                  <div className="whitespace-pre-wrap break-all" style={{ color: CONSOLE_COLOR.plain }}>
                    {renderJsonSyntax(line.data, `explain:${line.id}`)}
                  </div>
                </>
              )}
            </div>
          ))}
        </div>
      </div>
      <ConfirmDialog
        open={Boolean(confirmDialog)}
        title={confirmDialog?.title || 'Confirm action'}
        message={confirmDialog?.message || 'Continue?'}
        confirmLabel={confirmDialog?.confirmLabel || 'Confirm'}
        cancelLabel={confirmDialog?.cancelLabel || 'Cancel'}
        danger={Boolean(confirmDialog?.danger)}
        onCancel={() => closeConfirmDialog(false)}
        onConfirm={() => closeConfirmDialog(true)}
      />
    </div>
  );
}

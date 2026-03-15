
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import api from '../utils/api';
import {
  Activity,
  AlertTriangle,
  Check,
  Clock,
  Download,
  Refresh,
  Server,
  Upload,
} from './Icons';
import { formatDuration, formatNumber, copyToClipboard } from '../utils/formatters';
import DropdownSelect from './DropdownSelect';
import PerfChart, { MAX_POINTS } from './PerfChart';

const TOOL_TITLES = {
  statTopSlowOps: 'stat + top + slowops',
  serverInfo: 'Server Info',
  mongostat: 'mongostat',
  mongotop: 'mongotop',
  slowOps: 'Slow Ops',
  mongodump: 'mongodump',
  mongorestore: 'mongorestore',
  mongoexport: 'mongoexport',
  mongoimport: 'mongoimport',
  mongofiles: 'mongofiles',
};

const PERF_COMBO_TOOL = 'statTopSlowOps';
const READ_TOOLS = new Set([PERF_COMBO_TOOL, 'serverInfo', 'mongostat', 'mongotop', 'slowOps']);
const LIVE_TOOLS = new Set([PERF_COMBO_TOOL, 'mongostat', 'mongotop', 'slowOps']);
const MULTI_NODE_READ_TOOLS = new Set([PERF_COMBO_TOOL, 'mongostat', 'mongotop', 'slowOps']);
const DB_SCOPED_READ_TOOLS = new Set(['mongotop', 'slowOps']);
const POLL_OPTIONS = [1000, 2000, 5000, 10000];

const COLOR_ERROR = '#f87171';
const COLOR_ERROR_MUTED = '#fca5a5';
const COLOR_WARNING = '#f59e0b';
const COLOR_WARNING_TEXT = '#fcd34d';
const COLOR_SUCCESS = '#4ade80';
const STYLE_WARNING_BOX = { border: '1px solid rgba(245,158,11,0.3)', background: 'rgba(245,158,11,0.06)', color: 'var(--text-secondary)' };
const STYLE_ERROR_BOX = { border: '1px solid rgba(248,113,113,0.3)', background: 'rgba(248,113,113,0.08)', color: COLOR_ERROR_MUTED };

function isAbortError(err) {
  if (!err) return false;
  if (err.name === 'AbortError') return true;
  return /(?:^|\s)(?:abort|aborted|cancelled|canceled)(?:\s|$)/i.test(String(err.message || ''));
}

function validateJsonFilter(value) {
  const trimmed = String(value || '').trim();
  if (!trimmed || trimmed === '{}') return '';
  try { JSON.parse(trimmed); return ''; } catch (err) { return err.message || 'Invalid JSON'; }
}

function formatTime(ts) {
  if (!ts) return '--';
  try {
    return new Date(ts).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
  } catch {
    return '--';
  }
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

function normalizeFsPath(value = '') {
  const text = String(value || '').trim();
  if (!text) return '';
  const normalized = text.replace(/\\/g, '/').replace(/\/{2,}/g, '/');
  if (/^[a-z]:\//i.test(normalized)) {
    return normalized[0].toUpperCase() + normalized.slice(1);
  }
  return normalized;
}

function sanitizeFsSegment(s = '') {
  return String(s || '').trim().replace(/[^a-zA-Z0-9._-]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '') || '_';
}

function buildDynamicWorkPath(root, node, db, toolId) {
  const parts = [root || '/tmp/mongostudio'];
  if (node) parts.push(sanitizeFsSegment(node));
  if (db) parts.push(sanitizeFsSegment(db));
  if (toolId) parts.push(sanitizeFsSegment(toolId));
  return normalizeFsPath(parts.join('/'));
}

function normalizePathMode(value = '') {
  const mode = String(value || '').trim().toLowerCase();
  if (mode === 'host' || mode === 'custom') return mode;
  return 'auto';
}

function joinFsPath(basePath = '', leaf = '') {
  const base = normalizeFsPath(basePath);
  const tail = normalizeFsPath(leaf).replace(/^\/+/, '');
  if (!base) return tail;
  if (!tail) return base;
  return normalizeFsPath(`${base}/${tail}`);
}

function buildModeRoot(pathMode, basePath, hostWorkdir) {
  if (normalizePathMode(pathMode) === 'host') {
    return normalizeFsPath(hostWorkdir || '') || normalizeFsPath(basePath || '') || '/tmp/mongostudio';
  }
  return normalizeFsPath(basePath || '') || '/tmp/mongostudio';
}

function resolveToolPathDb(toolId, draftDb = '', form = {}) {
  const fallbackDb = String(draftDb || '').trim();
  if (toolId === 'mongodump') return String(form?.dumpDb || '').trim();
  if (toolId === 'mongorestore') {
    if (form?.restoreMultiScope) {
      return Array.isArray(form?.restoreDbList) && form.restoreDbList.length === 1
        ? String(form.restoreDbList[0] || '').trim()
        : '';
    }
    return String(form?.restoreDb || '').trim();
  }
  if (toolId === 'mongoexport') return String(form?.exportDb || '').trim();
  if (toolId === 'mongoimport') return String(form?.importDb || '').trim();
  if (toolId === 'mongofiles') return String(form?.gridfsDb || '').trim();
  return fallbackDb;
}

function buildSuggestedFilePath(basePath = '', fileName = '') {
  const base = normalizeFsPath(basePath);
  const name = String(fileName || '').trim();
  if (!base) return name;
  if (!name) return base;
  return joinFsPath(base, name);
}

function sameAppliedContext(left = null, right = null) {
  if (!left && !right) return true;
  if (!left || !right) return false;
  return sameHostToken(left.node || '', right.node || '')
    && normalizeFsPath(left.path || '') === normalizeFsPath(right.path || '')
    && normalizePathMode(left.pathMode || '') === normalizePathMode(right.pathMode || '')
    && Boolean(left.confirmNodeSelection) === Boolean(right.confirmNodeSelection)
    && Boolean(left.confirmPathSelection) === Boolean(right.confirmPathSelection);
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

function readSavedContextState(key = '') {
  const storageKey = String(key || '').trim();
  if (!storageKey || typeof window === 'undefined') return null;
  try {
    const raw = sessionStorage.getItem(storageKey);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const node = String(parsed?.node || '').trim();
    const path = normalizeFsPath(parsed?.path || '');
    const pathMode = normalizePathMode(parsed?.pathMode || parsed?.mode || '');
    if (!node || !path) return null;
    return {
      node,
      path,
      pathMode,
      confirmNodeSelection: false,
      confirmPathSelection: false,
    };
  } catch {
    return null;
  }
}

function writeSavedContextState(key = '', value = null) {
  const storageKey = String(key || '').trim();
  if (!storageKey || typeof window === 'undefined') return;
  try {
    if (!value?.node || !value?.path) {
      sessionStorage.removeItem(storageKey);
      return;
    }
    sessionStorage.setItem(storageKey, JSON.stringify({
      node: String(value.node || '').trim(),
      path: normalizeFsPath(value.path || ''),
      pathMode: normalizePathMode(value.pathMode || value.mode || ''),
    }));
  } catch {}
}

function readSessionFlag(key = '', fallback = true) {
  const storageKey = String(key || '').trim();
  if (!storageKey || typeof window === 'undefined') return fallback;
  try {
    const raw = sessionStorage.getItem(storageKey);
    if (raw === null || raw === undefined) return fallback;
    return raw === '1';
  } catch {
    return fallback;
  }
}

function writeSessionFlag(key = '', value = true) {
  const storageKey = String(key || '').trim();
  if (!storageKey || typeof window === 'undefined') return;
  try {
    sessionStorage.setItem(storageKey, value ? '1' : '0');
  } catch {}
}


function parseMetricNumber(value) {
  if (Number.isFinite(Number(value))) return Number(value);
  const raw = String(value || '').trim();
  if (!raw) return 0;
  const firstPart = raw.includes('|') ? raw.split('|')[0] : raw;
  const compact = firstPart.replace(/,/g, '').trim();
  const suffix = compact.slice(-1).toLowerCase();
  const numeric = compact.replace(/[^0-9.+-]/g, '');
  const base = Number(numeric);
  if (!Number.isFinite(base)) return 0;
  if (suffix === 'k') return base * 1000;
  if (suffix === 'm') return base * 1000000;
  if (suffix === 'g') return base * 1000000000;
  return base;
}

function normalizeMongostatParsed(parsed) {
  if (!parsed || typeof parsed !== 'object') {
    return {
      host: '--',
      insert: 0,
      query: 0,
      update: 0,
      command: 0,
      conn: 0,
      raw: null,
    };
  }
  if (typeof parsed.host === 'string' || Object.prototype.hasOwnProperty.call(parsed, 'insert')) {
    return {
      host: String(parsed.host || '--').trim() || '--',
      insert: parseMetricNumber(parsed.insert),
      query: parseMetricNumber(parsed.query),
      update: parseMetricNumber(parsed.update),
      command: parseMetricNumber(parsed.command),
      conn: parseMetricNumber(parsed.conn ?? parsed.connections),
      raw: parsed,
    };
  }
  const entries = Object.entries(parsed).filter(([, value]) => value && typeof value === 'object');
  if (entries.length === 0) {
    return {
      host: '--',
      insert: 0,
      query: 0,
      update: 0,
      command: 0,
      conn: 0,
      raw: parsed,
    };
  }
  const [host, payload] = entries[0];
  return {
    host: String(host || '--').trim() || '--',
    insert: parseMetricNumber(payload?.insert),
    query: parseMetricNumber(payload?.query),
    update: parseMetricNumber(payload?.update),
    command: parseMetricNumber(payload?.command),
    conn: parseMetricNumber(payload?.conn ?? payload?.connections),
    raw: payload,
  };
}

function parseMongostatFull(parsed) {
  if (!parsed || typeof parsed !== 'object') return null;
  let m = parsed;
  if (typeof m.host !== 'string' && !Object.prototype.hasOwnProperty.call(m, 'insert')) {
    const entries = Object.entries(m).filter(([, v]) => v && typeof v === 'object');
    if (entries.length === 0) return null;
    m = entries[0][1];
  }
  const parseRW = (v) => {
    const parts = String(v || '0|0').split('|');
    return [parseInt(parts[0], 10) || 0, parseInt(parts[1], 10) || 0];
  };
  const [ar, aw] = parseRW(m.arw);
  const [qr, qw] = parseRW(m.qrw);
  return {
    host: String(m.host || '').trim(),
    insert: parseMetricNumber(m.insert),
    query: parseMetricNumber(m.query),
    update: parseMetricNumber(m.update),
    delete: parseMetricNumber(m.delete),
    getmore: parseMetricNumber(m.getmore),
    command: parseMetricNumber(m.command),
    net_in: parseMetricNumber(m.net_in),
    net_out: parseMetricNumber(m.net_out),
    conn: parseMetricNumber(m.conn ?? m.connections),
    vsize: parseMetricNumber(m.vsize),
    res: parseMetricNumber(m.res),
    ar, aw, qr, qw,
  };
}

function parseMongotopHottest(parsed, limit = 8) {
  if (!parsed || typeof parsed !== 'object') return [];
  const totals = parsed.totals || parsed;
  const rows = [];
  for (const [ns, entry] of Object.entries(totals)) {
    if (!entry || typeof entry !== 'object') continue;
    if (ns === 'note' || ns === 'serverStatus') continue;
    const totalTime = Number(entry.total?.time ?? entry.total ?? 0);
    const readTime = Number(entry.read?.time ?? entry.read ?? 0);
    const writeTime = Number(entry.write?.time ?? entry.write ?? 0);
    rows.push({ ns, total: totalTime, read: readTime, write: writeTime });
  }
  const sumTotal = rows.reduce((s, r) => s + r.total, 0);
  rows.sort((a, b) => b.total - a.total);
  return rows.slice(0, limit).map((r) => ({
    ns: r.ns,
    total: r.total,
    read: r.read,
    write: r.write,
    pct: sumTotal > 0 ? (r.total / sumTotal) * 100 : 0,
  }));
}

function aggregateMongostatResults(results = []) {
  const okResults = Array.isArray(results) ? results : [];
  let merged = null;
  for (const entry of okResults) {
    const parsed = parseMongostatFull(entry?.data?.parsed);
    if (!parsed) continue;
    if (!merged) {
      merged = { ...parsed };
      continue;
    }
    const sumKeys = ['insert', 'query', 'update', 'delete', 'getmore', 'command', 'net_in', 'net_out', 'conn', 'ar', 'aw', 'qr', 'qw'];
    const maxKeys = ['vsize', 'res'];
    for (const key of sumKeys) merged[key] = Number(merged[key] || 0) + Number(parsed[key] || 0);
    for (const key of maxKeys) merged[key] = Math.max(Number(merged[key] || 0), Number(parsed[key] || 0));
  }
  if (!merged) return null;
  const nodes = okResults.map((entry) => String(entry?.node || '').trim()).filter(Boolean);
  const hostLabel = nodes.length > 1 ? 'all nodes' : (nodes[0] || merged.host || '--');
  return {
    parsed: { ...merged, host: hostLabel },
    targetNode: hostLabel,
    nodes,
  };
}

function aggregateMongotopResults(results = []) {
  const okResults = Array.isArray(results) ? results : [];
  const mergedTotals = {};
  for (const entry of okResults) {
    const parsed = entry?.data?.parsed;
    const totals = parsed?.totals || parsed || {};
    for (const [ns, value] of Object.entries(totals)) {
      if (!value || typeof value !== 'object' || ns === 'note' || ns === 'serverStatus') continue;
      if (!mergedTotals[ns]) {
        mergedTotals[ns] = { total: { time: 0 }, read: { time: 0 }, write: { time: 0 } };
      }
      mergedTotals[ns].total.time += Number(value.total?.time ?? value.total ?? 0);
      mergedTotals[ns].read.time += Number(value.read?.time ?? value.read ?? 0);
      mergedTotals[ns].write.time += Number(value.write?.time ?? value.write ?? 0);
    }
  }
  const nodes = okResults.map((entry) => String(entry?.node || '').trim()).filter(Boolean);
  return {
    parsed: { totals: mergedTotals },
    targetNode: nodes.length > 1 ? 'all nodes' : (nodes[0] || '--'),
    nodes,
  };
}

function aggregateSlowOpsResults(results = []) {
  const okResults = Array.isArray(results) ? results : [];
  const entries = [];
  for (const entry of okResults) {
    const node = String(entry?.node || '').trim();
    const items = Array.isArray(entry?.data?.entries) ? entry.data.entries : [];
    for (const item of items) entries.push({ ...item, node: String(item?.node || node).trim() });
  }
  entries.sort((left, right) => Number(right?.secsRunning || 0) - Number(left?.secsRunning || 0));
  const nodes = okResults.map((entry) => String(entry?.node || '').trim()).filter(Boolean);
  return {
    thresholdMs: okResults[0]?.data?.thresholdMs,
    thresholdSec: okResults[0]?.data?.thresholdSec,
    limit: okResults[0]?.data?.limit,
    total: entries.length,
    entries,
    targetNode: nodes.length > 1 ? 'all nodes' : (nodes[0] || '--'),
    nodes,
  };
}

function aggregateReadToolResults(toolId, results = []) {
  if (toolId === 'mongostat') return aggregateMongostatResults(results);
  if (toolId === 'mongotop') return aggregateMongotopResults(results);
  if (toolId === 'slowOps') return aggregateSlowOpsResults(results);
  return null;
}

function attachReadResultMeta(result, node = '') {
  if (!result || typeof result !== 'object') return result;
  const targetNode = String(result?.targetNode || node || '').trim();
  const nodes = Array.isArray(result?.nodes) && result.nodes.length > 0
    ? result.nodes.map((value) => String(value || '').trim()).filter(Boolean)
    : (targetNode ? [targetNode] : []);
  return {
    ...result,
    targetNode,
    nodes,
  };
}

function SafeJson({ value }) {
  const text = useMemo(() => {
    try {
      return JSON.stringify(value ?? {}, null, 2);
    } catch {
      return String(value ?? '');
    }
  }, [value]);
  return <pre className="m-0 text-2xs font-mono whitespace-pre-wrap break-all" style={{ color: 'var(--text-secondary)' }}>{text}</pre>;
}

function StatCard({ label, value, sub, subTitle = '', subWrap = false }) {
  return (
    <div className="rounded-xl px-3 py-2.5" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
      <div className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>{label}</div>
      <div className="text-sm font-semibold mt-1" style={{ color: 'var(--text-primary)' }}>{value}</div>
      {sub ? (
        <div
          className={`text-2xs mt-1 ${subWrap ? 'break-all' : 'truncate'}`}
          style={{ color: 'var(--text-tertiary)' }}
          title={subTitle || sub}
        >
          {sub}
        </div>
      ) : null}
    </div>
  );
}

function ContextLine({ label, value, mono = false, wrap = true }) {
  return (
    <div className={`flex gap-2 text-2xs ${wrap ? 'items-start' : 'items-center'}`}>
      <span className="uppercase tracking-wider" style={{ color: 'var(--text-tertiary)', minWidth: '84px' }}>{label}</span>
      <span
        className={`${mono ? 'font-mono' : ''} min-w-0 ${wrap ? 'break-all whitespace-normal' : 'truncate'}`}
        style={{ color: 'var(--text-secondary)' }}
        title={String(value || '')}
      >
        {value || '--'}
      </span>
    </div>
  );
}

const OUTPUT_TRUNCATE_LINES = 20;

function CopyButton({ text, label = 'Copy' }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = useCallback(() => {
    if (!text) return;
    copyToClipboard(text).then((ok) => {
      if (!ok) return;
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [text]);
  if (!text) return null;
  return (
    <button
      type="button"
      className="btn-ghost text-2xs px-1.5 py-0.5"
      onClick={handleCopy}
      aria-label={label}
      title={label}
      style={{ color: copied ? COLOR_SUCCESS : 'var(--text-tertiary)' }}
    >
      {copied ? 'Copied' : 'Copy'}
    </button>
  );
}

function TruncatedPre({ text, color = 'var(--text-secondary)' }) {
  const [expanded, setExpanded] = useState(false);
  if (!text) return null;
  const lines = text.split('\n');
  const needsTruncation = lines.length > OUTPUT_TRUNCATE_LINES;
  const displayText = !expanded && needsTruncation ? lines.slice(0, OUTPUT_TRUNCATE_LINES).join('\n') : text;
  return (
    <div>
      <pre className="m-0 text-2xs font-mono whitespace-pre-wrap break-all" style={{ color, maxHeight: expanded ? 'none' : undefined }}>{displayText}</pre>
      {needsTruncation ? (
        <button
          type="button"
          className="btn-ghost text-2xs mt-1 px-1.5 py-0.5"
          style={{ color: 'var(--accent)' }}
          onClick={() => setExpanded((p) => !p)}
        >
          {expanded ? 'Show less' : `Show all (${lines.length - OUTPUT_TRUNCATE_LINES} more lines)`}
        </button>
      ) : null}
    </div>
  );
}

export default function ServerManagementView({
  activeTool = 'serverInfo',
  selectedDb,
  selectedCol,
  connectionId = '',
  onDbContextChange,
}) {
  const contextStateKey = String(connectionId || '').trim()
    ? `mongostudio_server_context:${String(connectionId || '').trim()}`
    : '';
  const contextDetailsStateKey = String(connectionId || '').trim()
    ? `mongostudio_server_context_details:${String(connectionId || '').trim()}`
    : '';
  const readNodeSelectionKey = String(connectionId || '').trim()
    ? `mongostudio_server_read_scope:${String(connectionId || '').trim()}`
    : '';
  const pollMsKey = String(connectionId || '').trim()
    ? `mongostudio_server_poll_ms:${String(connectionId || '').trim()}`
    : '';
  const hiddenSeriesKey = String(connectionId || '').trim()
    ? `mongostudio_server_hidden_series:${String(connectionId || '').trim()}`
    : '';
  const initialSavedContext = readSavedContextState(contextStateKey);
  const controllersRef = useRef(new Set());
  const collectionOptionsRef = useRef({});
  const collectionLoadingRef = useRef({});
  const activeToolRef = useRef(activeTool);
  const perfHistoryRef = useRef({ ts: [], insert: [], query: [], update: [], delete: [], getmore: [], command: [], ar: [], aw: [], qr: [], qw: [], net_in: [], net_out: [], conn: [], vsize: [], res: [], hotSnapshots: [], slowSnapshots: [] });
  const perfLastTsRef = useRef(0);
  const perfPeakRef = useRef({ insert: 0, query: 0, update: 0, delete: 0, getmore: 0, command: 0, ar: 0, aw: 0, qr: 0, qw: 0, net_in: 0, net_out: 0, conn: 0, vsize: 0, res: 0 });

  const [contextState, setContextState] = useState({ loading: false, loaded: false, error: '', data: null });
  const [draftNode, setDraftNode] = useState(() => initialSavedContext?.node || '');
  const [draftPath, setDraftPath] = useState(() => initialSavedContext?.path || '');
  const [confirmNodeSelection, setConfirmNodeSelection] = useState(() => Boolean(initialSavedContext?.confirmNodeSelection));
  const [confirmPathSelection, setConfirmPathSelection] = useState(() => Boolean(initialSavedContext?.confirmPathSelection));
  const [pathMode, setPathMode] = useState(() => normalizePathMode(initialSavedContext?.pathMode || ''));
  const [appliedContext, setAppliedContext] = useState(() => initialSavedContext || null);
  const [showContextDetails, setShowContextDetails] = useState(() => readSessionFlag(contextDetailsStateKey, false));

  const [pollMs, setPollMs] = useState(() => {
    if (!pollMsKey) return 5000;
    try { const v = parseInt(sessionStorage.getItem(pollMsKey), 10); return POLL_OPTIONS.includes(v) ? v : 5000; } catch { return 5000; }
  });
  const [pollPaused, setPollPaused] = useState(false);
  const [perfScope, setPerfScope] = useState('cluster');
  const [readNodeSelection, setReadNodeSelection] = useState(() => {
    if (!readNodeSelectionKey) return 'context';
    try { const v = sessionStorage.getItem(readNodeSelectionKey); return v === 'all' ? 'all' : 'context'; } catch { return 'context'; }
  });
  const [appliedReadNodeSelection, setAppliedReadNodeSelection] = useState(() => {
    if (!readNodeSelectionKey) return 'context';
    try { const v = sessionStorage.getItem(readNodeSelectionKey); return v === 'all' ? 'all' : 'context'; } catch { return 'context'; }
  });
  const [sharedHoverIdx, setSharedHoverIdx] = useState(-1);
  const [hiddenSeriesKeys, setHiddenSeriesKeys] = useState(() => {
    if (!hiddenSeriesKey) return new Set();
    try { const v = sessionStorage.getItem(hiddenSeriesKey); return v ? new Set(JSON.parse(v)) : new Set(); } catch { return new Set(); }
  });
  const [perfTick, setPerfTick] = useState(0);
  const [readState, setReadState] = useState({ loading: false, error: '', result: null, lastTs: 0 });
  const [refreshSpinning, setRefreshSpinning] = useState(false);
  const refreshSpinTimerRef = useRef(null);

  const [pendingKillOp, setPendingKillOp] = useState(null);

  const [runState, setRunState] = useState({ busy: false, error: '', result: null, lastTs: 0 });
  const [runElapsedMs, setRunElapsedMs] = useState(0);
  const runStartRef = useRef(0);
  const runElapsedTimerRef = useRef(null);
  const [dbOptionsState, setDbOptionsState] = useState({ loading: false, error: '', items: [] });
  const [collectionOptionsByDb, setCollectionOptionsByDb] = useState({});
  const [collectionLoadingByDb, setCollectionLoadingByDb] = useState({});
  const [draftContextDb, setDraftContextDb] = useState(() => String(selectedDb || '').trim());

  const [form, setForm] = useState(() => ({
    slowThresholdMs: 1000,
    slowLimit: 30,
    dumpDb: selectedDb || '',
    dumpCollection: selectedCol || '',
    dumpGzip: true,
    dumpOplog: false,
    dumpFilter: '{}',
    dumpPath: '',
    restorePath: '',
    restoreMultiScope: false,
    restoreDb: selectedDb || '',
    restoreDbList: selectedDb ? [selectedDb] : [],
    restoreCollectionList: selectedCol ? [selectedCol] : [],
    restoreDrop: true,
    restoreGzip: false,
    exportDb: selectedDb || '',
    exportCollection: selectedCol || '',
    exportFormat: 'json',
    exportLimit: '100000',
    exportFilter: '{}',
    exportFields: '',
    exportFile: '',
    importDb: selectedDb || '',
    importCollection: selectedCol || '',
    importFormat: 'json',
    importMode: 'insert',
    importDrop: false,
    importFile: '',
    gridfsAction: 'list',
    gridfsDb: selectedDb || 'admin',
    gridfsBucket: 'fs',
    gridfsFilename: '',
    gridfsLocalPath: '',
  }));

  useEffect(() => {
    setReadState({ loading: false, error: '', result: null, lastTs: 0 });
    setRunState({ busy: false, error: '', result: null, lastTs: 0 });
    setPollPaused(false);
  }, [activeTool]);

  useEffect(() => {
    activeToolRef.current = activeTool;
  }, [activeTool]);

  useEffect(() => {
    setForm((prev) => ({
      ...prev,
      dumpDb: selectedDb || '',
      dumpCollection: selectedCol || '',
      restoreDb: selectedDb || '',
      restoreDbList: selectedDb ? [selectedDb] : [],
      restoreCollectionList: selectedCol ? [selectedCol] : [],
      exportDb: selectedDb || '',
      exportCollection: selectedCol || '',
      importDb: selectedDb || '',
      importCollection: selectedCol || '',
      gridfsDb: selectedDb || prev.gridfsDb || 'admin',
    }));
  }, [selectedDb, selectedCol]);

  useEffect(() => {
    setDraftContextDb(String(selectedDb || '').trim());
  }, [selectedDb]);

  useEffect(() => {
    setShowContextDetails(readSessionFlag(contextDetailsStateKey, false));
  }, [contextDetailsStateKey]);

  useEffect(() => {
    writeSessionFlag(contextDetailsStateKey, showContextDetails);
  }, [contextDetailsStateKey, showContextDetails]);

  useEffect(() => {
    if (!readNodeSelectionKey) return;
    try { sessionStorage.setItem(readNodeSelectionKey, readNodeSelection); } catch {}
  }, [readNodeSelectionKey, readNodeSelection]);

  useEffect(() => {
    if (!pollMsKey) return;
    try { sessionStorage.setItem(pollMsKey, String(pollMs)); } catch {}
  }, [pollMsKey, pollMs]);

  useEffect(() => {
    if (!hiddenSeriesKey) return;
    try { sessionStorage.setItem(hiddenSeriesKey, JSON.stringify([...hiddenSeriesKeys])); } catch {}
  }, [hiddenSeriesKey, hiddenSeriesKeys]);

  useEffect(() => {
    const saved = readSavedContextState(contextStateKey);
    setDraftNode(saved?.node || '');
    setDraftPath(saved?.path || '');
    setPathMode(normalizePathMode(saved?.pathMode || ''));
    if (readNodeSelectionKey) {
      try {
        const v = sessionStorage.getItem(readNodeSelectionKey);
        setReadNodeSelection(v === 'all' ? 'all' : 'context');
        setAppliedReadNodeSelection(v === 'all' ? 'all' : 'context');
      } catch {
        setReadNodeSelection('context');
        setAppliedReadNodeSelection('context');
      }
    } else {
      setReadNodeSelection('context');
      setAppliedReadNodeSelection('context');
    }
    setConfirmNodeSelection(false);
    setConfirmPathSelection(false);
    setAppliedContext(saved || null);
  }, [contextStateKey]);

  useEffect(() => {
    writeSavedContextState(contextStateKey, appliedContext);
  }, [appliedContext, contextStateKey]);

  useEffect(() => {
    collectionOptionsRef.current = collectionOptionsByDb;
  }, [collectionOptionsByDb]);

  useEffect(() => {
    collectionLoadingRef.current = collectionLoadingByDb;
  }, [collectionLoadingByDb]);

  const pushDbContextChange = useCallback((dbName, meta = {}) => {
    const nextDb = String(dbName || '').trim();
    if (!nextDb) return;
    onDbContextChange?.(nextDb, {
      source: 'server-management',
      tool: activeTool,
      ...meta,
    });
  }, [activeTool, onDbContextChange]);

  const syncToolDbContext = useCallback((dbName, meta = {}) => {
    const nextDb = String(dbName || '').trim();
    setDraftContextDb(nextDb);
    if (nextDb) {
      pushDbContextChange(nextDb, meta);
    }
    setReadState((prev) => ({ ...prev, error: '' }));
    setRunState((prev) => ({ ...prev, error: '' }));
  }, [pushDbContextChange]);

  const makeController = useCallback(() => {
    const controller = new AbortController();
    controllersRef.current.add(controller);
    return controller;
  }, []);

  const releaseController = useCallback((controller) => {
    if (!controller) return;
    controllersRef.current.delete(controller);
  }, []);

  useEffect(() => () => {
    const controllers = [...controllersRef.current];
    controllersRef.current.clear();
    controllers.forEach((controller) => {
      try { controller.abort('unmount'); } catch {}
    });
  }, []);

  const loadDbOptions = useCallback(async () => {
    setDbOptionsState((prev) => ({ ...prev, loading: true, error: '' }));
    try {
      const data = await api.listDatabases();
      const names = (Array.isArray(data?.databases) ? data.databases : [])
        .map((entry) => String(entry?.name || '').trim())
        .filter(Boolean)
        .sort((a, b) => a.localeCompare(b));
      setDbOptionsState({ loading: false, error: '', items: names });
      setForm((prev) => {
        const normalize = (value = '', fallback = '') => {
          const current = String(value || '').trim();
          if (current && names.includes(current)) return current;
          const fallbackText = String(fallback || '').trim();
          if (fallbackText && names.includes(fallbackText)) return fallbackText;
          return '';
        };
        const normalizeList = (items = []) => {
          if (!Array.isArray(items)) return [];
          const next = [];
          for (const item of items) {
            const value = String(item || '').trim();
            if (!value || !names.includes(value)) continue;
            if (!next.includes(value)) next.push(value);
          }
          return next;
        };
        const normalizedGridfs = normalize(prev.gridfsDb, selectedDb || 'admin') || (names.includes('admin') ? 'admin' : (names[0] || ''));
        return {
          ...prev,
          dumpDb: normalize(prev.dumpDb, selectedDb),
          restoreDb: normalize(prev.restoreDb, selectedDb),
          restoreDbList: normalizeList(prev.restoreDbList),
          exportDb: normalize(prev.exportDb, selectedDb),
          importDb: normalize(prev.importDb, selectedDb),
          gridfsDb: normalizedGridfs,
        };
      });
    } catch (err) {
      setDbOptionsState((prev) => ({
        ...prev,
        loading: false,
        error: err?.message || 'Failed to load databases',
      }));
    }
  }, [selectedDb]);

  const ensureCollectionOptions = useCallback(async (dbName, { force = false } = {}) => {
    const normalizedDb = String(dbName || '').trim();
    if (!normalizedDb) return [];
    if (!force && Array.isArray(collectionOptionsRef.current[normalizedDb])) return collectionOptionsRef.current[normalizedDb];
    if (collectionLoadingRef.current[normalizedDb]) return [];
    const controller = makeController();
    setCollectionLoadingByDb((prev) => ({ ...prev, [normalizedDb]: true }));
    try {
      const data = await api.listCollections(normalizedDb, { withStats: false, controller });
      const names = (Array.isArray(data?.collections) ? data.collections : [])
        .map((entry) => String(entry?.name || '').trim())
        .filter(Boolean)
        .sort((a, b) => a.localeCompare(b));
      setCollectionOptionsByDb((prev) => ({ ...prev, [normalizedDb]: names }));
      return names;
    } catch (err) {
      if (!isAbortError(err)) {
        setCollectionOptionsByDb((prev) => ({ ...prev, [normalizedDb]: [] }));
      }
      return [];
    } finally {
      setCollectionLoadingByDb((prev) => {
        const next = { ...prev };
        delete next[normalizedDb];
        return next;
      });
      releaseController(controller);
    }
  }, [makeController, releaseController]);

  useEffect(() => {
    loadDbOptions();
  }, [loadDbOptions]);

  const loadContext = useCallback(async ({ refresh = false } = {}) => {
    const controller = makeController();
    setContextState((prev) => ({ ...prev, loading: true, error: '' }));
    try {
      const data = await api.getServerManagementContext({ refresh, controller });
      const context = data?.context || null;
      const nextHostWorkdir = normalizeFsPath(data?.hostWorkdir || '');
      const availableNodes = Array.isArray(context?.availableNodes) ? context.availableNodes : [];
      const defaultNode = context?.defaultNode || context?.selectedNode || '';
      const contextBasePath = normalizeFsPath(context?.basePath || '');
      const defaultPath = normalizeFsPath(context?.selectedPath || contextBasePath || '');
      const resolveNode = (candidate = '') => {
        const raw = String(candidate || '').trim();
        if (!raw || availableNodes.length === 0) return '';
        const exact = availableNodes.find((entry) => String(entry?.host || '').trim() === raw);
        if (exact) return String(exact.host || '').trim();
        const match = availableNodes.find((entry) => sameHostToken(entry?.host || '', raw));
        return String(match?.host || '').trim();
      };
      setContextState({ loading: false, loaded: true, error: '', data });
      setDraftNode((prev) => resolveNode(prev) || defaultNode);
      setPathMode((currentMode) => {
        const nextMode = currentMode === 'host' && !nextHostWorkdir ? 'auto' : currentMode;
        if (nextMode === 'custom') {
          setDraftPath((prev) => normalizeFsPath(prev || defaultPath) || defaultPath);
        }
        return nextMode;
      });
      setConfirmNodeSelection(false);
      setConfirmPathSelection(false);
      setAppliedContext((prev) => {
        const nextMode = normalizePathMode(prev?.pathMode || pathMode);
        if (!prev) {
          return {
            node: defaultNode,
            path: defaultPath,
            pathMode: nextMode,
            confirmNodeSelection: false,
            confirmPathSelection: nextMode === 'host',
          };
        }
        const nextNode = resolveNode(prev?.node) || defaultNode;
        const nextPath = normalizeFsPath(prev?.path || defaultPath) || defaultPath;
        const nextNodeOverride = Boolean(nextNode && defaultNode && !sameHostToken(nextNode, defaultNode));
        const nextPathCustom = Boolean(nextPath && contextBasePath && !isPathWithin(contextBasePath, nextPath));
        return {
          node: nextNode,
          path: nextPath,
          pathMode: nextMode,
          confirmNodeSelection: nextNodeOverride,
          confirmPathSelection: nextMode === 'host' ? true : nextPathCustom,
        };
      });
    } catch (err) {
      if (!isAbortError(err)) {
        setContextState((prev) => ({ ...prev, loading: false, loaded: prev.loaded, error: err.message || 'Failed to load context.' }));
      }
    } finally {
      releaseController(controller);
    }
  }, [makeController, pathMode, releaseController]);

  useEffect(() => {
    loadContext();
  }, [loadContext]);

  const context = contextState.data?.context || null;
  const binaries = contextState.data?.binaries || {};
  const nodeOptions = useMemo(() => {
    const input = Array.isArray(context?.availableNodes) ? context.availableNodes : [];
    const deduped = [];
    for (const entry of input) {
      const host = String(entry?.host || '').trim();
      if (!host) continue;
      if (deduped.some((item) => sameHostToken(item?.host || '', host))) continue;
      deduped.push(entry);
    }
    return deduped;
  }, [context?.availableNodes]);
  const nodeSelectOptions = useMemo(() => {
    if (nodeOptions.length === 0) return [{ value: '', label: 'No nodes', disabled: true }];
    return nodeOptions.map((node) => {
      const host = String(node?.host || '').trim();
      const role = String(node?.role || 'member').trim().toLowerCase();
      const defaultSuffix = node?.isDefault ? ' [default]' : '';
      return {
        value: host,
        label: `${host} (${role})${defaultSuffix}`,
      };
    });
  }, [nodeOptions]);
  const isReadTool = READ_TOOLS.has(activeTool);
  const isRunTool = !isReadTool;
  const activeToolSupportsAllNodes = MULTI_NODE_READ_TOOLS.has(activeTool);
  const targetNodeSupportsAllNodes = MULTI_NODE_READ_TOOLS.has(activeTool) && nodeOptions.length > 1;
  const defaultNode = context?.defaultNode || '';
  const basePath = normalizeFsPath(context?.basePath || '');
  const hostWorkdir = contextState.data?.hostWorkdir || '';
  const binaryEntries = useMemo(() => Object.entries(binaries || {}), [binaries]);
  const missingBinaryEntries = useMemo(
    () => binaryEntries.filter(([, entry]) => !entry?.available),
    [binaryEntries],
  );
  const activeToolMissingBinary = useMemo(() => {
    if (!contextState.loaded) return null;
    const toolIds = activeTool === PERF_COMBO_TOOL ? ['mongostat', 'mongotop'] : [activeTool];
    for (const tid of toolIds) {
      const entry = binaries[tid];
      if (entry && !entry.available) return { tool: tid, error: entry.error || `${tid} binary not found` };
    }
    return null;
  }, [activeTool, binaries, contextState.loaded]);
  const databaseOptions = dbOptionsState.items;
  const dbContextOptions = useMemo(() => {
    const opts = [];
    const raw = Array.isArray(databaseOptions) ? [...databaseOptions] : [];
    const current = String(draftContextDb || '').trim();
    if (current && !raw.includes(current)) raw.unshift(current);
    for (const name of raw) opts.push({ value: name, label: name });
    return opts;
  }, [databaseOptions, draftContextDb]);
  const dumpCollectionOptions = useMemo(
    () => (form.dumpDb ? (collectionOptionsByDb[form.dumpDb] || []) : []),
    [collectionOptionsByDb, form.dumpDb],
  );
  const exportCollectionOptions = useMemo(
    () => (form.exportDb ? (collectionOptionsByDb[form.exportDb] || []) : []),
    [collectionOptionsByDb, form.exportDb],
  );
  const importCollectionOptions = useMemo(
    () => (form.importDb ? (collectionOptionsByDb[form.importDb] || []) : []),
    [collectionOptionsByDb, form.importDb],
  );
  const restoreCollectionOptions = useMemo(() => {
    if (!Array.isArray(form.restoreDbList) || form.restoreDbList.length !== 1) return [];
    const targetDb = form.restoreDbList[0];
    return collectionOptionsByDb[targetDb] || [];
  }, [collectionOptionsByDb, form.restoreDbList]);

  useEffect(() => {
    if (activeTool === 'mongodump' && form.dumpDb) {
      ensureCollectionOptions(form.dumpDb);
    } else if (activeTool === 'mongoexport' && form.exportDb) {
      ensureCollectionOptions(form.exportDb);
    } else if (activeTool === 'mongoimport' && form.importDb) {
      ensureCollectionOptions(form.importDb);
    } else if (
      activeTool === 'mongorestore'
      && form.restoreMultiScope
      && Array.isArray(form.restoreDbList)
      && form.restoreDbList.length === 1
    ) {
      ensureCollectionOptions(form.restoreDbList[0]);
    }
  }, [activeTool, ensureCollectionOptions, form.dumpDb, form.exportDb, form.importDb, form.restoreDbList, form.restoreMultiScope]);

  useEffect(() => {
    setForm((prev) => {
      let changed = false;
      const next = { ...prev };
      if (!next.dumpDb && next.dumpCollection) {
        next.dumpCollection = '';
        changed = true;
      }
      if (!next.exportDb && next.exportCollection) {
        next.exportCollection = '';
        changed = true;
      }
      if (!next.importDb && next.importCollection) {
        next.importCollection = '';
        changed = true;
      }
      if (!Array.isArray(next.restoreDbList)) {
        next.restoreDbList = [];
        changed = true;
      }
      if (!Array.isArray(next.restoreCollectionList)) {
        next.restoreCollectionList = [];
        changed = true;
      }
      if (next.restoreDbList.length !== 1 && next.restoreCollectionList.length > 0) {
        next.restoreCollectionList = [];
        changed = true;
      }
      if (
        next.dumpDb
        && next.dumpCollection
        && Object.prototype.hasOwnProperty.call(collectionOptionsByDb, next.dumpDb)
        && !(collectionOptionsByDb[next.dumpDb] || []).includes(next.dumpCollection)
      ) {
        next.dumpCollection = '';
        changed = true;
      }
      if (
        next.exportDb
        && next.exportCollection
        && Object.prototype.hasOwnProperty.call(collectionOptionsByDb, next.exportDb)
        && !(collectionOptionsByDb[next.exportDb] || []).includes(next.exportCollection)
      ) {
        next.exportCollection = '';
        changed = true;
      }
      if (
        next.importDb
        && next.importCollection
        && Object.prototype.hasOwnProperty.call(collectionOptionsByDb, next.importDb)
        && !(collectionOptionsByDb[next.importDb] || []).includes(next.importCollection)
      ) {
        next.importCollection = '';
        changed = true;
      }
      if (
        next.restoreDbList.length === 1
        && next.restoreCollectionList.length > 0
        && Object.prototype.hasOwnProperty.call(collectionOptionsByDb, next.restoreDbList[0])
      ) {
        const allowedCollections = collectionOptionsByDb[next.restoreDbList[0]] || [];
        const filtered = next.restoreCollectionList.filter((name) => allowedCollections.includes(name));
        if (filtered.length !== next.restoreCollectionList.length) {
          next.restoreCollectionList = filtered;
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [collectionOptionsByDb]);

  useEffect(() => {
    if (pathMode === 'host' && !hostWorkdir) {
      setPathMode('auto');
    }
  }, [hostWorkdir, pathMode]);

  const effectiveDraftNode = String(draftNode || defaultNode || '').trim();
  const draftReadNodeScope = useMemo(() => ({
    isAll: activeToolSupportsAllNodes && readNodeSelection === 'all',
    node: String(effectiveDraftNode || defaultNode || '').trim(),
  }), [activeToolSupportsAllNodes, defaultNode, effectiveDraftNode, readNodeSelection]);
  const appliedReadNodeScope = useMemo(() => ({
    isAll: activeToolSupportsAllNodes && appliedReadNodeSelection === 'all',
    node: String(appliedContext?.node || defaultNode || '').trim(),
  }), [activeToolSupportsAllNodes, appliedContext?.node, appliedReadNodeSelection, defaultNode]);
  const readScopeNeedsApply = isRunTool && activeToolSupportsAllNodes && draftReadNodeScope.isAll !== appliedReadNodeScope.isAll;
  const pathContextDb = useMemo(
    () => resolveToolPathDb(activeTool, draftContextDb, form),
    [
      activeTool,
      draftContextDb,
      form.dumpDb,
      form.exportDb,
      form.gridfsDb,
      form.importDb,
      form.restoreDb,
      form.restoreDbList,
      form.restoreMultiScope,
    ],
  );
  const isManagedPathMode = pathMode === 'auto' || pathMode === 'host';
  const managedPathRoot = useMemo(() => buildModeRoot(pathMode, basePath, hostWorkdir), [pathMode, basePath, hostWorkdir]);
  const desiredManagedPath = useMemo(() => {
    const pathNode = draftReadNodeScope.isAll ? 'all_nodes' : effectiveDraftNode;
    if (!isManagedPathMode || !pathNode) return '';
    return buildDynamicWorkPath(managedPathRoot, pathNode, pathContextDb, activeTool);
  }, [activeTool, draftReadNodeScope.isAll, effectiveDraftNode, isManagedPathMode, managedPathRoot, pathContextDb]);
  const normalizedDraftPath = useMemo(() => normalizeFsPath(draftPath), [draftPath]);
  const normalizedDesiredManagedPath = useMemo(() => normalizeFsPath(desiredManagedPath), [desiredManagedPath]);
  const appliedContextNode = String(appliedContext?.node || defaultNode || '').trim();
  const draftNodeIsOverride = Boolean(effectiveDraftNode && defaultNode && !sameHostToken(effectiveDraftNode, defaultNode));
  const draftNodeChanged = Boolean(effectiveDraftNode && appliedContextNode && !sameHostToken(effectiveDraftNode, appliedContextNode));
  const draftNodeNeedsConfirmation = isRunTool && !draftReadNodeScope.isAll && draftNodeChanged;
  const liveManagedNode = draftNodeNeedsConfirmation ? appliedContextNode : effectiveDraftNode;
  const liveManagedPath = useMemo(() => {
    const pathNode = appliedReadNodeScope.isAll ? 'all_nodes' : liveManagedNode;
    if (!isManagedPathMode || !pathNode) return '';
    return buildDynamicWorkPath(managedPathRoot, pathNode, pathContextDb, activeTool);
  }, [activeTool, appliedReadNodeScope.isAll, isManagedPathMode, liveManagedNode, managedPathRoot, pathContextDb]);
  const normalizedLiveManagedPath = useMemo(() => normalizeFsPath(liveManagedPath), [liveManagedPath]);
  const contextDraftPath = isManagedPathMode ? normalizedDesiredManagedPath : normalizedDraftPath;
  const draftPathIsCustom = Boolean(contextDraftPath && basePath && !isPathWithin(basePath, contextDraftPath));
  useEffect(() => {
    if (!isManagedPathMode || !normalizedLiveManagedPath) return;
    setDraftPath((prev) => (normalizeFsPath(prev) === normalizedLiveManagedPath ? prev : normalizedLiveManagedPath));
    if (pathMode === 'host') {
      if (!confirmPathSelection) setConfirmPathSelection(true);
    } else if (confirmPathSelection) {
      setConfirmPathSelection(false);
    }
  }, [confirmPathSelection, isManagedPathMode, normalizedLiveManagedPath, pathMode]);
  const autoManagedContext = useMemo(() => {
    if (!isManagedPathMode || !effectiveDraftNode || !normalizedDesiredManagedPath || draftNodeNeedsConfirmation || readScopeNeedsApply) return null;
    return {
      node: effectiveDraftNode,
      path: normalizedDesiredManagedPath,
      pathMode,
      confirmNodeSelection: draftNodeIsOverride,
      confirmPathSelection: pathMode === 'host',
    };
  }, [draftNodeIsOverride, draftNodeNeedsConfirmation, effectiveDraftNode, isManagedPathMode, normalizedDesiredManagedPath, pathMode, readScopeNeedsApply]);
  useEffect(() => {
    if (!autoManagedContext) return;
    setAppliedContext((prev) => (sameAppliedContext(prev, autoManagedContext) ? prev : autoManagedContext));
  }, [autoManagedContext]);
  const executionContext = useMemo(() => {
    if (autoManagedContext) return autoManagedContext;
    if (!appliedContext) return null;
    return {
      node: String(appliedContext.node || '').trim(),
      path: normalizeFsPath(appliedContext.path || ''),
      pathMode: normalizePathMode(appliedContext.pathMode || ''),
      confirmNodeSelection: Boolean(appliedContext.confirmNodeSelection),
      confirmPathSelection: Boolean(appliedContext.confirmPathSelection),
    };
  }, [appliedContext, autoManagedContext]);
  const appliedNode = String(executionContext?.node || '').trim();
  const appliedPathNormalized = useMemo(() => normalizeFsPath(executionContext?.path || ''), [executionContext?.path]);
  const canApplyContext = Boolean(
    effectiveDraftNode
    && contextDraftPath
    && (!draftNodeNeedsConfirmation || confirmNodeSelection)
    && (!draftPathIsCustom || confirmPathSelection)
    && !contextState.loading,
  );
  const hasPendingContextChanges = useMemo(() => {
    if (!executionContext) return true;
    const currentNode = effectiveDraftNode;
    const currentPath = contextDraftPath;
    const currentConfirmNodeSelection = draftReadNodeScope.isAll ? false : draftNodeIsOverride;
    const currentConfirmPathSelection = isManagedPathMode ? pathMode === 'host' : confirmPathSelection;
    return !sameHostToken(appliedNode, currentNode)
      || appliedPathNormalized !== currentPath
      || normalizePathMode(executionContext.pathMode || '') !== normalizePathMode(pathMode)
      || Boolean(executionContext.confirmNodeSelection) !== Boolean(currentConfirmNodeSelection)
      || Boolean(executionContext.confirmPathSelection) !== Boolean(currentConfirmPathSelection)
      || readScopeNeedsApply;
  }, [
    appliedNode,
    appliedPathNormalized,
    confirmNodeSelection,
    confirmPathSelection,
    draftReadNodeScope.isAll,
    draftNodeIsOverride,
    effectiveDraftNode,
    executionContext,
    isManagedPathMode,
    contextDraftPath,
    pathMode,
    readScopeNeedsApply,
  ]);
  const showAppliedStatus = Boolean(executionContext) && !hasPendingContextChanges;
  const currentExecutionPath = appliedPathNormalized || normalizedLiveManagedPath || contextDraftPath || managedPathRoot;
  const suggestedDumpPath = currentExecutionPath;
  const suggestedRestorePath = currentExecutionPath;
  const suggestedExportFile = useMemo(() => {
    const dbName = String(form.exportDb || pathContextDb || 'export').trim() || 'export';
    const colName = String(form.exportCollection || 'data').trim() || 'data';
    const ext = String(form.exportFormat || 'json').trim().toLowerCase() === 'csv' ? 'csv' : 'json';
    return buildSuggestedFilePath(currentExecutionPath, `${sanitizeFsSegment(`${dbName}.${colName}`)}.${ext}`);
  }, [currentExecutionPath, form.exportCollection, form.exportDb, form.exportFormat, pathContextDb]);
  const suggestedImportFile = useMemo(() => {
    const dbName = String(form.importDb || pathContextDb || 'import').trim() || 'import';
    const colName = String(form.importCollection || 'data').trim() || 'data';
    const ext = String(form.importFormat || 'json').trim().toLowerCase() === 'csv'
      ? 'csv'
      : (String(form.importFormat || 'json').trim().toLowerCase() === 'tsv' ? 'tsv' : 'json');
    return buildSuggestedFilePath(currentExecutionPath, `${sanitizeFsSegment(`${dbName}.${colName}`)}.${ext}`);
  }, [currentExecutionPath, form.importCollection, form.importDb, form.importFormat, pathContextDb]);
  const suggestedGridFsLocalPath = useMemo(() => {
    const fileName = String(form.gridfsFilename || '').trim() || 'gridfs.bin';
    return buildSuggestedFilePath(currentExecutionPath, fileName);
  }, [currentExecutionPath, form.gridfsFilename]);
  useEffect(() => {
    if (!draftNodeNeedsConfirmation && confirmNodeSelection) setConfirmNodeSelection(false);
  }, [confirmNodeSelection, draftNodeNeedsConfirmation]);
  useEffect(() => {
    if (!draftPathIsCustom && confirmPathSelection) setConfirmPathSelection(false);
  }, [confirmPathSelection, draftPathIsCustom]);
  const applyContext = useCallback(() => {
    if (!effectiveDraftNode) {
      setReadState((prev) => ({ ...prev, error: 'Select target node before apply.' }));
      return;
    }
    if (!contextDraftPath) {
      setReadState((prev) => ({ ...prev, error: 'Set execution path before apply.' }));
      return;
    }
    if (draftNodeNeedsConfirmation && !confirmNodeSelection) {
      setReadState((prev) => ({ ...prev, error: 'Confirm node switch before apply.' }));
      return;
    }
    if (draftPathIsCustom && !confirmPathSelection) {
      setReadState((prev) => ({ ...prev, error: 'Confirm custom path before apply.' }));
      return;
    }
    setReadState((prev) => ({ ...prev, error: '' }));
    setRunState((prev) => ({ ...prev, error: '' }));
    setDraftPath(contextDraftPath);
    const nextConfirmNodeSelection = draftReadNodeScope.isAll ? false : draftNodeIsOverride;
    setAppliedReadNodeSelection(draftReadNodeScope.isAll ? 'all' : 'context');
    setAppliedContext({
      node: effectiveDraftNode,
      path: contextDraftPath,
      pathMode,
      confirmNodeSelection: nextConfirmNodeSelection,
      confirmPathSelection,
    });
  }, [confirmNodeSelection, confirmPathSelection, contextDraftPath, draftNodeIsOverride, draftNodeNeedsConfirmation, draftPathIsCustom, draftReadNodeScope.isAll, effectiveDraftNode, pathMode]);

  const readNodeScope = isReadTool ? draftReadNodeScope : appliedReadNodeScope;
  const readToolActionsDisabled = isReadTool
    ? (contextState.loading || !effectiveDraftNode)
    : (!executionContext || hasPendingContextChanges || contextState.loading);
  const runToolActionsDisabled = (!executionContext || hasPendingContextChanges || contextState.loading) || runState.busy;


  const perfLoadingRef = useRef(false);
  const refreshReadTool = useCallback(async () => {
    if (!READ_TOOLS.has(activeTool)) return;
    if (!effectiveDraftNode) return;
    if (perfLoadingRef.current) return;
    perfLoadingRef.current = true;
    const requestedTool = activeTool;
    const controller = makeController();
    setReadState((prev) => ({ ...prev, loading: true, error: '' }));
    setRefreshSpinning(true);
    clearTimeout(refreshSpinTimerRef.current);
    try {
      const requestedToolIds = activeTool === PERF_COMBO_TOOL ? ['mongostat', 'mongotop', 'slowOps'] : [activeTool];
      const allNodesRequested = MULTI_NODE_READ_TOOLS.has(activeTool) && readNodeSelection === 'all' && nodeOptions.length > 1;
      const targetNodes = allNodesRequested
        ? nodeOptions.map((entry) => String(entry?.host || '').trim()).filter(Boolean)
        : [effectiveDraftNode];

      const readBasePath = normalizeFsPath(basePath || '') || '/tmp/mongostudio';
      const makePayload = (toolId, nodeOverride) => {
        const targetNode = String(nodeOverride || effectiveDraftNode || '').trim();
        const payload = {
          node: targetNode,
          path: readBasePath,
          confirmNodeSelection: !sameHostToken(targetNode, defaultNode),
          confirmPathSelection: false,
        };
        if (toolId === 'slowOps') {
          payload.thresholdMs = Number(form.slowThresholdMs) || 1000;
          payload.limit = Number(form.slowLimit) || 30;
        }
        return payload;
      };
      const requestList = [];
      for (const node of targetNodes) {
        const requestNode = String(node || effectiveDraftNode || '').trim();
        for (const toolId of requestedToolIds) {
          requestList.push(
            api.getServerManagementTool(toolId, makePayload(toolId, requestNode), { controller })
              .then((data) => ({ status: 'ok', toolId, node: requestNode, data: data?.result || null }))
              .catch((err) => ({ status: 'err', toolId, node: requestNode, error: err })),
          );
        }
      }
      const results = await Promise.all(requestList);
      if (activeToolRef.current !== requestedTool) return;

      if (activeTool === PERF_COMBO_TOOL) {
        const nextResult = {};
        const errors = {};
        let successCount = 0;
        for (const toolId of requestedToolIds) {
          const toolResults = results.filter((entry) => entry.toolId === toolId);
          const okResults = toolResults.filter((entry) => entry.status === 'ok' && entry.data);
          const errResults = toolResults.filter((entry) => entry.status === 'err' && !isAbortError(entry.error));
          if (okResults.length > 0) {
            nextResult[toolId] = allNodesRequested
              ? aggregateReadToolResults(toolId, okResults)
              : attachReadResultMeta(okResults[0].data, okResults[0].node);
            successCount += 1;
          } else if (errResults.length > 0) {
            errors[toolId] = errResults[0].error?.message || `Failed to load ${toolId}.`;
          }
        }
        setReadState({
          loading: false,
          error: successCount === 0 ? (Object.values(errors)[0] || 'Failed to load performance tools.') : '',
          result: { ...nextResult, errors },
          lastTs: Date.now(),
        });
      } else {
        const okResults = results.filter((entry) => entry.status === 'ok' && entry.data);
        const errResults = results.filter((entry) => entry.status === 'err' && !isAbortError(entry.error));
        const nextResult = okResults.length > 0
          ? (allNodesRequested
            ? {
              ...(aggregateReadToolResults(activeTool, okResults) || {}),
              errors: errResults.reduce((acc, entry) => ({ ...acc, [entry.node || 'unknown']: entry.error?.message || 'Failed to load node.' }), {}),
            }
            : attachReadResultMeta(okResults[0].data, okResults[0].node))
          : null;
        const errorText = nextResult
          ? ''
          : (errResults[0]?.error?.message || 'Failed to load tool data.');
        setReadState({
          loading: false,
          error: errorText,
          result: nextResult,
          lastTs: Date.now(),
        });
      }
    } catch (err) {
      if (!isAbortError(err)) {
        if (activeToolRef.current !== requestedTool) return;
        setReadState((prev) => ({ ...prev, loading: false, error: err.message || 'Failed to load tool data.' }));
      }
    } finally {
      perfLoadingRef.current = false;
      releaseController(controller);
      refreshSpinTimerRef.current = setTimeout(() => setRefreshSpinning(false), 600);
    }
  }, [activeTool, basePath, defaultNode, effectiveDraftNode, form.slowLimit, form.slowThresholdMs, makeController, nodeOptions, readNodeSelection, releaseController]);

  useEffect(() => {
    perfLoadingRef.current = false;
    if (!contextState.loaded) return undefined;
    if (!READ_TOOLS.has(activeTool)) {
      setReadState((prev) => ({ ...prev, loading: false, error: '', result: null }));
      return undefined;
    }
    if (!effectiveDraftNode) return undefined;
    refreshReadTool();
    if (!LIVE_TOOLS.has(activeTool) || pollPaused) return undefined;
    const timer = setInterval(() => {
      refreshReadTool();
    }, pollMs);
    return () => clearInterval(timer);
  }, [activeTool, contextState.loaded, effectiveDraftNode, pollMs, pollPaused, refreshReadTool]);

  useEffect(() => {
    if (activeTool !== PERF_COMBO_TOOL) return;
    const ts = readState.lastTs;
    if (!ts || ts === perfLastTsRef.current) return;
    perfLastTsRef.current = ts;
    const result = readState.result;
    const snap = parseMongostatFull(result?.mongostat?.parsed);
    const h = perfHistoryRef.current;
    const pk = perfPeakRef.current;
    const keys = ['insert', 'query', 'update', 'delete', 'getmore', 'command', 'ar', 'aw', 'qr', 'qw', 'net_in', 'net_out', 'conn', 'vsize', 'res'];
    h.ts.push(ts);
    for (const k of keys) {
      const v = snap ? snap[k] : 0;
      h[k].push(v);
      if (v > pk[k]) pk[k] = v;
    }
    // Store hot/slow snapshots for shared tooltip
    const scopeDb = perfScope !== 'cluster' ? perfScope : '';
    const topResult = result?.mongotop || null;
    const slowResult = result?.slowOps || null;
    h.hotSnapshots.push(parseMongotopHottest(topResult?.parsed).filter((r) => !scopeDb || r.ns.startsWith(scopeDb + '.')));
    h.slowSnapshots.push((Array.isArray(slowResult?.entries) ? slowResult.entries : []).filter((e) => !scopeDb || (e.namespace || '').startsWith(scopeDb + '.')));
    if (h.ts.length > MAX_POINTS) {
      const excess = h.ts.length - MAX_POINTS;
      h.ts.splice(0, excess);
      for (const k of keys) h[k].splice(0, excess);
      h.hotSnapshots.splice(0, excess);
      h.slowSnapshots.splice(0, excess);
    }
    setPerfTick((prev) => prev + 1); // trigger re-render for peak updates
  }, [activeTool, readState.lastTs, readState.result, perfScope]);

  const isPerfCombo = activeTool === PERF_COMBO_TOOL;

  const clearPerfHistory = useCallback(() => {
    const h = perfHistoryRef.current;
    h.ts.length = 0;
    for (const k of Object.keys(h)) {
      if (k !== 'ts') h[k].length = 0;
    }
    const pk = perfPeakRef.current;
    for (const k of Object.keys(pk)) pk[k] = 0;
    perfLastTsRef.current = 0;
    setPerfTick(0);
  }, []);

  useEffect(() => {
    clearPerfHistory();
  }, [readNodeSelection, connectionId, effectiveDraftNode, perfScope, clearPerfHistory]);

  const runTool = useCallback(async () => {
    if (READ_TOOLS.has(activeTool)) return;
    if (!executionContext) return;
    if (hasPendingContextChanges) {
      setRunState((prev) => ({ ...prev, error: 'Apply execution context before running this tool.' }));
      return;
    }
    const requestedTool = activeTool;
    const payload = {
      ...executionContext,
    };
    if (activeTool === 'mongodump') {
      payload.db = form.dumpDb;
      payload.collection = form.dumpCollection;
      payload.gzip = form.dumpGzip;
      payload.oplog = form.dumpOplog;
      payload.query = form.dumpFilter;
      payload.outputPath = form.dumpPath || executionContext.path;
    } else if (activeTool === 'mongorestore') {
      payload.multiScope = Boolean(form.restoreMultiScope);
      payload.dbList = form.restoreMultiScope ? (Array.isArray(form.restoreDbList) ? form.restoreDbList : []) : [];
      payload.collectionList = form.restoreMultiScope ? (Array.isArray(form.restoreCollectionList) ? form.restoreCollectionList : []) : [];
      payload.db = form.restoreDb;
      payload.drop = form.restoreDrop;
      payload.gzip = form.restoreGzip;
      payload.inputPath = form.restorePath || executionContext.path;
    } else if (activeTool === 'mongoexport') {
      payload.db = form.exportDb;
      payload.collection = form.exportCollection;
      payload.format = form.exportFormat;
      payload.limit = form.exportLimit;
      payload.query = form.exportFilter;
      payload.fields = form.exportFields;
      payload.outputFile = form.exportFile;
    } else if (activeTool === 'mongoimport') {
      payload.db = form.importDb;
      payload.collection = form.importCollection;
      payload.format = form.importFormat;
      payload.mode = form.importMode;
      payload.drop = form.importDrop;
      payload.inputFile = form.importFile;
    } else if (activeTool === 'mongofiles') {
      payload.action = form.gridfsAction;
      payload.db = form.gridfsDb;
      payload.bucket = form.gridfsBucket;
      payload.filename = form.gridfsFilename;
      payload.localPath = form.gridfsLocalPath;
    }

    const controller = makeController();
    setRunState((prev) => ({ ...prev, busy: true, error: '' }));
    runStartRef.current = Date.now();
    setRunElapsedMs(0);
    clearInterval(runElapsedTimerRef.current);
    runElapsedTimerRef.current = setInterval(() => {
      setRunElapsedMs(Date.now() - runStartRef.current);
    }, 250);
    try {
      const data = await api.runServerManagementTool(activeTool, payload, {
        controller,
        heavyConfirm: true,
      });
      if (activeToolRef.current !== requestedTool) return;
      setRunState({ busy: false, error: '', result: data?.result || null, lastTs: Date.now() });
    } catch (err) {
      if (!isAbortError(err)) {
        if (activeToolRef.current !== requestedTool) return;
        setRunState((prev) => ({ ...prev, busy: false, error: err.message || 'Command failed.' }));
      }
    } finally {
      clearInterval(runElapsedTimerRef.current);
      setRunElapsedMs(0);
      releaseController(controller);
    }
  }, [activeTool, executionContext, form, hasPendingContextChanges, makeController, releaseController]);

  const renderServerInfo = () => {
    const result = readState.result;
    if (!result) return <div className="text-xs" style={{ color: 'var(--text-tertiary)' }}>No data yet.</div>;
    const status = result.serverStatus || {};
    const hello = result.hello || {};
    const topo = result.topology || {};
    const uptimeMs = Math.max(0, Number(status.uptime || 0)) * 1000;
    const nodeRole = (() => {
      if (hello.isWritablePrimary || hello.ismaster) return 'Primary';
      if (hello.secondary) return 'Secondary';
      if (hello.arbiterOnly) return 'Arbiter';
      if (hello.msg === 'isdbgrid') return 'Mongos';
      return topo.type || 'Standalone';
    })();
    const replSetName = String(hello.setName || status.repl?.setName || topo.setName || '').trim();
    const fcv = String(result.capabilities?.featureCompatibilityVersion || '').trim();
    const opcounters = status.opcounters || {};
    const mem = status.mem || {};
    return (
      <div className="space-y-3">
        <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-6 gap-2">
          <StatCard label="Version" value={result.version || '--'} sub={`${status.process || 'mongod'} pid ${status.pid || '--'}`} />
          <StatCard label="Role" value={nodeRole} sub={replSetName ? `rs: ${replSetName}` : 'standalone'} />
          <StatCard label="Uptime" value={uptimeMs > 0 ? formatDuration(uptimeMs) : '--'} sub={status.host || result.targetNode || '--'} />
          <StatCard label="Connections" value={formatNumber(Number(status.connections?.current || 0))} sub={`avail ${formatNumber(Number(status.connections?.available || 0))}`} />
          <StatCard label="Storage" value={String(status.storageEngine?.name || '--')} sub={fcv ? `FCV ${fcv}` : '--'} />
          <StatCard label="Memory" value={mem.resident ? `${formatNumber(mem.resident)} MB` : '--'} sub={mem.virtual ? `virtual ${formatNumber(mem.virtual)} MB` : '--'} />
        </div>
        {Object.keys(opcounters).length > 0 ? (
          <div className="rounded-xl p-3" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
            <div className="text-2xs uppercase tracking-wider mb-2" style={{ color: 'var(--text-tertiary)' }}>Op Counters (cumulative)</div>
            <div className="flex flex-wrap gap-x-5 gap-y-1">
              {Object.entries(opcounters).map(([key, val]) => (
                <span key={key} className="text-2xs font-mono">
                  <span style={{ color: 'var(--text-tertiary)' }}>{key} </span>
                  <span style={{ color: 'var(--json-number, #f59e0b)' }}>{formatNumber(Number(val || 0))}</span>
                </span>
              ))}
            </div>
          </div>
        ) : null}
        <details className="rounded-xl" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
          <summary className="px-3 py-2 text-2xs uppercase tracking-wider cursor-pointer select-none" style={{ color: 'var(--text-tertiary)' }}>
            Raw details
          </summary>
          <div className="px-3 pb-3">
            <SafeJson value={{ hello: result.hello || null, topology: result.topology || null, routing: result.routing || null, capabilities: result.capabilities || null }} />
          </div>
        </details>
      </div>
    );
  };

  const renderMongostat = () => {
    const result = readState.result;
    if (!result) return <div className="text-xs" style={{ color: 'var(--text-tertiary)' }}>No data yet.</div>;
    const metrics = normalizeMongostatParsed(result.parsed);
    const nodeErrors = result?.errors && typeof result.errors === 'object' ? Object.entries(result.errors) : [];
    return (
      <div className="space-y-3">
        {Array.isArray(result?.nodes) && result.nodes.length > 1 ? (
          <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
            Aggregated across <span className="font-mono" style={{ color: 'var(--text-secondary)' }}>{result.nodes.length}</span> nodes.
          </div>
        ) : null}
        {nodeErrors.length > 0 ? (
          <div className="rounded-lg p-2 text-2xs" style={STYLE_WARNING_BOX}>
            {nodeErrors.map(([node, message]) => <div key={`staterr:${node}`}>{node}: {message}</div>)}
          </div>
        ) : null}
        <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-6 gap-2">
          <StatCard label="Host" value={metrics.host || '--'} />
          <StatCard label="Insert" value={formatNumber(metrics.insert)} />
          <StatCard label="Query" value={formatNumber(metrics.query)} />
          <StatCard label="Update" value={formatNumber(metrics.update)} />
          <StatCard label="Command" value={formatNumber(metrics.command)} />
          <StatCard label="Connections" value={formatNumber(metrics.conn)} />
        </div>
        {metrics.raw && typeof metrics.raw === 'object' && (
          <div className="rounded-xl p-3" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
            <div className="text-2xs uppercase tracking-wider mb-2" style={{ color: 'var(--text-tertiary)' }}>Details</div>
            <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1">
              {Object.entries(metrics.raw).map(([key, val]) => (
                <React.Fragment key={key}>
                  <span className="text-2xs font-mono font-medium" style={{ color: 'var(--json-key, var(--accent))' }}>{key}</span>
                  <span className="text-2xs font-mono" style={{
                    color: typeof val === 'number' ? 'var(--json-number, #f59e0b)'
                      : (typeof val === 'boolean' ? 'var(--json-boolean, #a78bfa)'
                      : (val && String(val).match(/^\*?\d/) ? 'var(--json-number, #f59e0b)' : 'var(--text-secondary)')),
                  }}>{String(val ?? '--')}</span>
                </React.Fragment>
              ))}
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderMongotop = () => {
    const result = readState.result;
    if (!result) return <div className="text-xs" style={{ color: 'var(--text-tertiary)' }}>No data yet.</div>;
    const allRows = parseMongotopHottest(result.parsed, 50);
    const scopeDb = draftContextDb || '';
    const rows = scopeDb ? allRows.filter((r) => r.ns.startsWith(scopeDb + '.')) : allRows;
    const nodeErrors = result?.errors && typeof result.errors === 'object' ? Object.entries(result.errors) : [];
    return (
      <div className="space-y-3">
        {Array.isArray(result?.nodes) && result.nodes.length > 1 ? (
          <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
            Aggregated across <span className="font-mono" style={{ color: 'var(--text-secondary)' }}>{result.nodes.length}</span> nodes.
          </div>
        ) : null}
        {nodeErrors.length > 0 ? (
          <div className="rounded-lg p-2 text-2xs" style={STYLE_WARNING_BOX}>
            {nodeErrors.map(([node, message]) => <div key={`toperr:${node}`}>{node}: {message}</div>)}
          </div>
        ) : null}
        {rows.length > 0 ? (
          <div className="rounded-xl overflow-hidden" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
            <div className="px-3 py-2 flex items-center justify-between" style={{ borderBottom: '1px solid var(--border)' }}>
              <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>
                Namespaces by time{scopeDb ? ` — ${scopeDb}` : ' — all databases'}
              </span>
              <span className="text-2xs font-mono" style={{ color: 'var(--text-tertiary)' }}>{rows.length} ns</span>
            </div>
            <table className="w-full text-2xs font-mono">
              <thead>
                <tr style={{ borderBottom: '1px solid var(--border)' }}>
                  <th className="text-left px-3 py-1.5 font-medium" style={{ color: 'var(--text-tertiary)' }}>Namespace</th>
                  <th className="text-right px-3 py-1.5 font-medium" style={{ color: 'var(--text-tertiary)' }}>Total</th>
                  <th className="text-right px-3 py-1.5 font-medium" style={{ color: 'var(--text-tertiary)' }}>Read</th>
                  <th className="text-right px-3 py-1.5 font-medium" style={{ color: 'var(--text-tertiary)' }}>Write</th>
                  <th className="text-right px-3 py-1.5 font-medium w-24" style={{ color: 'var(--text-tertiary)' }}>%</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.ns} className="group" style={{ borderBottom: '1px solid var(--border)' }}>
                    <td className="px-3 py-1.5" style={{ color: 'var(--json-key, var(--accent))' }}>{r.ns}</td>
                    <td className="text-right px-3 py-1.5" style={{ color: 'var(--text-primary)' }}>{r.total}ms</td>
                    <td className="text-right px-3 py-1.5" style={{ color: 'var(--json-string, #22c55e)' }}>{r.read}ms</td>
                    <td className="text-right px-3 py-1.5" style={{ color: 'var(--json-number, #f59e0b)' }}>{r.write}ms</td>
                    <td className="text-right px-3 py-1.5">
                      <div className="flex items-center gap-1.5 justify-end">
                        <div className="w-16 h-1 rounded-full overflow-hidden" style={{ background: 'var(--surface-3)' }}>
                          <div className="h-full rounded-full" style={{ width: `${Math.min(r.pct, 100)}%`, background: 'var(--accent)' }} />
                        </div>
                        <span className="w-10 text-right" style={{ color: 'var(--text-tertiary)' }}>{r.pct.toFixed(1)}%</span>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-xs" style={{ color: 'var(--text-tertiary)' }}>
            {scopeDb ? `No activity for database "${scopeDb}".` : 'No namespace activity data.'}
          </div>
        )}
      </div>
    );
  };
  const confirmKillOp = useCallback(async () => {
    if (!pendingKillOp) return;
    const { opid, node } = pendingKillOp;
    setPendingKillOp(null);
    try {
      await api.killOp(opid, node || effectiveDraftNode || '');
      refreshReadTool();
    } catch (err) {
      setReadState((prev) => ({ ...prev, error: `Kill failed: ${err.message || 'unknown error'}` }));
    }
  }, [pendingKillOp, effectiveDraftNode, refreshReadTool]);

  const renderSlowOpsPanel = (result) => {
    const allEntries = Array.isArray(result?.entries) ? result.entries : [];
    const slowDbFilter = String(draftContextDb || '').trim();
    const entries = slowDbFilter
      ? allEntries.filter((item) => String(item?.namespace || '').startsWith(slowDbFilter + '.'))
      : allEntries;
    const hasNodeColumn = entries.some((item) => String(item?.node || '').trim());
    const nodeErrors = result?.errors && typeof result.errors === 'object' ? Object.entries(result.errors) : [];
    return (
      <div className="space-y-3">
        {Array.isArray(result?.nodes) && result.nodes.length > 1 ? (
          <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
            Aggregated across <span className="font-mono" style={{ color: 'var(--text-secondary)' }}>{result.nodes.length}</span> nodes.
          </div>
        ) : null}
        {nodeErrors.length > 0 ? (
          <div className="rounded-lg p-2 text-2xs" style={STYLE_WARNING_BOX}>
            {nodeErrors.map(([node, message]) => <div key={`slowerr:${node}`}>{node}: {message}</div>)}
          </div>
        ) : null}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <div>
            <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Threshold ms</div>
            <input
              className="input-field text-xs"
              value={form.slowThresholdMs}
              onChange={(e) => setForm((prev) => ({ ...prev, slowThresholdMs: e.target.value }))}
            />
          </div>
          <div>
            <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Limit</div>
            <input
              className="input-field text-xs"
              value={form.slowLimit}
              onChange={(e) => setForm((prev) => ({ ...prev, slowLimit: e.target.value }))}
            />
          </div>
          <div className="flex items-end">
            <button
              type="button"
              className="btn-ghost text-xs px-3 py-1.5"
              onClick={refreshReadTool}
              disabled={readToolActionsDisabled}
              title={readToolActionsDisabled ? 'Apply execution context first' : 'Apply slow-op filters and refresh'}
            >
              Apply + Refresh
            </button>
          </div>
        </div>
        <div className="rounded-xl overflow-hidden" style={{ border: '1px solid var(--border)', background: 'var(--surface-1)' }}>
          <table className="w-full text-xs">
            <thead>
              <tr style={{ borderBottom: '1px solid var(--border)' }}>
                <th className="px-2.5 py-2 text-left" style={{ color: 'var(--text-tertiary)' }}>Secs</th>
                {hasNodeColumn ? <th className="px-2.5 py-2 text-left" style={{ color: 'var(--text-tertiary)' }}>Node</th> : null}
                <th className="px-2.5 py-2 text-left" style={{ color: 'var(--text-tertiary)' }}>Namespace</th>
                <th className="px-2.5 py-2 text-left" style={{ color: 'var(--text-tertiary)' }}>Operation</th>
                <th className="px-2.5 py-2 text-left" style={{ color: 'var(--text-tertiary)' }}>Client</th>
                <th className="px-2.5 py-2 text-right w-16" style={{ color: 'var(--text-tertiary)' }} />
              </tr>
            </thead>
            <tbody>
              {entries.map((item) => (
                <tr key={`${item.opid}:${item.namespace}`} style={{ borderBottom: '1px solid var(--border)' }}>
                  <td className="px-2.5 py-1.5 font-mono" style={{ color: 'var(--text-primary)' }}>{formatNumber(Number(item.secsRunning || 0))}</td>
                  {hasNodeColumn ? <td className="px-2.5 py-1.5 font-mono" style={{ color: 'var(--text-secondary)' }}>{item.node || '--'}</td> : null}
                  <td className="px-2.5 py-1.5 font-mono" style={{ color: 'var(--text-secondary)' }}>{item.namespace || '--'}</td>
                  <td className="px-2.5 py-1.5 font-mono" style={{ color: 'var(--text-secondary)' }}>{item.desc || item.op || '--'}</td>
                  <td className="px-2.5 py-1.5 font-mono" style={{ color: 'var(--text-secondary)' }}>{item.client || '--'}</td>
                  <td className="px-2.5 py-1.5 text-right whitespace-nowrap">
                    {item.opid != null ? (
                      pendingKillOp?.opid === item.opid ? (
                        <span className="inline-flex items-center gap-1.5">
                          <button
                            type="button"
                            className="btn-ghost text-2xs px-1.5 py-0.5"
                            style={{ color: 'var(--text-secondary)' }}
                            onClick={() => setPendingKillOp(null)}
                            aria-label="Cancel kill operation"
                          >
                            Cancel
                          </button>
                          <button
                            type="button"
                            className="text-2xs font-medium px-2 py-0.5 rounded-md"
                            style={{ background: '#dc2626', color: '#fff', border: 'none' }}
                            onClick={confirmKillOp}
                            aria-label={`Confirm kill operation ${item.opid}`}
                          >
                            Confirm Kill
                          </button>
                        </span>
                      ) : (
                        <button
                          type="button"
                          className="btn-ghost text-2xs px-1.5 py-0.5"
                          style={{ color: '#f87171' }}
                          onClick={() => setPendingKillOp({ opid: item.opid, node: item.node || '' })}
                          title={`Kill operation ${item.opid}`}
                          aria-label={`Kill operation ${item.opid}`}
                        >
                          Kill
                        </button>
                      )
                    ) : null}
                  </td>
                </tr>
              ))}
              {entries.length === 0 ? (
                <tr>
                  <td colSpan={hasNodeColumn ? 6 : 5} className="px-2.5 py-4 text-center" style={{ color: 'var(--text-tertiary)' }}>{slowDbFilter ? `No slow operations for "${slowDbFilter}" above threshold.` : 'No slow operations above threshold.'}</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  const renderSlowOps = () => renderSlowOpsPanel(readState.result);

  const perfScopeOptions = useMemo(() => {
    const opts = [{ value: 'cluster', label: 'All databases (cluster)' }];
    for (const db of (dbOptionsState.items || [])) opts.push({ value: db, label: db });
    return opts;
  }, [dbOptionsState.items]);

  const perfPollOptions = useMemo(() => POLL_OPTIONS.map((v) => ({ value: String(v), label: `${v / 1000}s` })), []);

  const toggleSeriesKey = useCallback((key) => {
    setHiddenSeriesKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const handleSharedHover = useCallback((idx) => setSharedHoverIdx(idx), []);
  const handleSharedLeave = useCallback(() => setSharedHoverIdx(-1), []);

  // perfTick is intentionally used as a render trigger — setPerfTick forces re-render
  // so chart components pick up the latest data from perfHistoryRef/perfPeakRef.
  const renderPerfCombo = () => {
    void perfTick;
    const result = readState.result && typeof readState.result === 'object' ? readState.result : {};
    const statResult = result?.mongostat || null;
    const topResult = result?.mongotop || null;
    const slowResult = result?.slowOps || null;
    const errors = result?.errors && typeof result.errors === 'object' ? result.errors : {};
    const hasData = Boolean(statResult || topResult || slowResult);
    const h = perfHistoryRef.current;
    const pk = perfPeakRef.current;
    const scopeDb = perfScope !== 'cluster' ? perfScope : '';
    const hottest = parseMongotopHottest(topResult?.parsed).filter((r) => !scopeDb || r.ns.startsWith(scopeDb + '.'));
    const slowEntries = (Array.isArray(slowResult?.entries) ? slowResult.entries : []).filter((e) => !scopeDb || (e.namespace || '').startsWith(scopeDb + '.'));
    const errorList = Object.entries(errors);

    // Shared tooltip: show snapshot data when hovering
    const hoverActive = sharedHoverIdx >= 0 && sharedHoverIdx < h.ts.length;
    const displayHottest = hoverActive && h.hotSnapshots[sharedHoverIdx] ? h.hotSnapshots[sharedHoverIdx] : hottest;
    const displaySlow = hoverActive && h.slowSnapshots[sharedHoverIdx] ? h.slowSnapshots[sharedHoverIdx] : slowEntries;
    const hoverTs = hoverActive ? h.ts[sharedHoverIdx] : 0;

    // Common chart props for shared crosshair
    const chartShared = { hoverIdx: sharedHoverIdx, onHover: handleSharedHover, onLeave: handleSharedLeave, hiddenKeys: hiddenSeriesKeys, onToggleKey: toggleSeriesKey };

    // Peaks per chart group
    const opsPeak = Math.max(pk.insert, pk.query, pk.update, pk.delete, pk.command, pk.getmore);
    const rwPeak = Math.max(pk.ar, pk.aw, pk.qr, pk.qw);
    const netPeak = Math.max(pk.net_in, pk.net_out, pk.conn);
    const memPeak = Math.max(pk.vsize, pk.res);

    return (
      <div className="space-y-3">
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 6, padding: '6px 0' }}>
          <button
            type="button"
            onClick={() => setPollPaused((prev) => !prev)}
            className={pollPaused ? 'btn-warning' : 'btn-primary'}
            style={{ display: 'inline-flex', alignItems: 'center', gap: 5, padding: '5px 14px', borderRadius: 6, fontSize: 12, fontWeight: 700, border: 'none', cursor: 'pointer' }}
          >
            <span style={{ fontSize: 10 }}>{pollPaused ? '\u25B6' : '\u275A\u275A'}</span>
            {pollPaused ? 'Play' : 'Pause'}
          </button>
          <span style={{ fontFamily: 'monospace', fontSize: 13, fontWeight: 600, color: 'var(--text-secondary)', padding: '4px 10px', borderRadius: 6, background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
            {hoverTs ? formatTime(hoverTs) : (readState.lastTs ? formatTime(readState.lastTs) : '--:--:--')}
          </span>
          <DropdownSelect
            sizeClassName="text-2xs"
            value={String(pollMs)}
            options={perfPollOptions}
            onChange={(v) => setPollMs(Number(v) || 5000)}
            menuZIndex={350}
          />
          <DropdownSelect
            sizeClassName="text-2xs"
            value={perfScope}
            options={perfScopeOptions}
            onChange={(v) => setPerfScope(v || 'cluster')}
            menuZIndex={350}
          />
          <DropdownSelect
            sizeClassName="text-2xs"
            value={readNodeSelection === 'all' ? '__all__' : draftNode}
            options={[{ value: '__all__', label: 'All nodes (aggregate)' }, ...nodeSelectOptions]}
            onChange={(value) => {
              if (value === '__all__') {
                setReadNodeSelection('all');
                setDraftNode(String(appliedContext?.node || defaultNode || ''));
                setConfirmNodeSelection(false);
                return;
              }
              setReadNodeSelection('context');
              setDraftNode(String(value || ''));
              setConfirmNodeSelection(false);
            }}
            disabled={contextState.loading || nodeOptions.length === 0}
            menuZIndex={350}
          />
          {readState.loading ? (
            <span style={{ width: 14, height: 14, border: '2px solid var(--border)', borderTopColor: 'var(--accent, #60a5fa)', borderRadius: '50%', animation: 'spin 0.7s linear infinite', flexShrink: 0 }} />
          ) : null}
        </div>

        {errorList.length > 0 ? (
          <div className="rounded-lg p-2 text-2xs" style={STYLE_WARNING_BOX}>
            {errorList.map(([toolId, message]) => (
              <div key={`comboerr:${toolId}`} style={{ opacity: 0.8 }}><span style={{ color: '#fcd34d' }}>{toolId}</span>: {String(message).replace(/^Failed to execute server-management read tool:\s*/i, '')}</div>
            ))}
          </div>
        ) : null}

        {!hasData && errorList.length === 0 ? (
          <div className="text-xs" style={{ color: 'var(--text-tertiary)', padding: '20px 0', textAlign: 'center' }}>Waiting for first data point...</div>
        ) : (
          <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr) minmax(280px, 340px)', gap: 12, alignItems: 'start' }} className="perf-combo-grid">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10, minWidth: 0 }}>
              <PerfChart title="Operations" unit="ops" height={150} peak={opsPeak} {...chartShared} series={[
                { key: 'insert', label: 'Insert', color: '#4ade80', data: [...h.insert] },
                { key: 'query', label: 'Query', color: '#60a5fa', data: [...h.query] },
                { key: 'update', label: 'Update', color: '#22d3ee', data: [...h.update] },
                { key: 'delete', label: 'Delete', color: '#f87171', data: [...h.delete] },
                { key: 'command', label: 'Command', color: '#c084fc', data: [...h.command] },
                { key: 'getmore', label: 'Getmore', color: '#94a3b8', data: [...h.getmore] },
              ]} />
              <PerfChart title="Read & Write" height={130} peak={rwPeak} {...chartShared} series={[
                { key: 'ar', label: 'AReads', color: '#4ade80', data: [...h.ar] },
                { key: 'aw', label: 'AWrites', color: '#60a5fa', data: [...h.aw] },
                { key: 'qr', label: 'QReads', color: '#22d3ee', data: [...h.qr] },
                { key: 'qw', label: 'QWrites', color: '#f87171', data: [...h.qw] },
              ]} />
              <PerfChart title="Network" height={130} peak={netPeak} {...chartShared} series={[
                { key: 'net_in', label: 'BytesIn', color: '#4ade80', data: [...h.net_in] },
                { key: 'net_out', label: 'BytesOut', color: '#60a5fa', data: [...h.net_out] },
                { key: 'conn', label: 'Connections', color: '#22d3ee', data: [...h.conn] },
              ]} />
              <PerfChart title="Memory" height={130} peak={memPeak} {...chartShared} series={[
                { key: 'vsize', label: 'Virtual', color: '#4ade80', data: [...h.vsize] },
                { key: 'res', label: 'Resident', color: '#60a5fa', data: [...h.res] },
              ]} />
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 10, minWidth: 0 }}>
              <div style={{ background: 'var(--perf-card-bg, rgba(0,0,0,0.18))', borderRadius: 8, border: '1px solid var(--border)', overflow: 'hidden' }}>
                <div style={{ padding: '8px 12px 4px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <span style={{ color: 'var(--text-secondary)', fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                    Hottest Collections
                    {hoverActive ? <span style={{ fontWeight: 400, fontSize: 10, marginLeft: 6, opacity: 0.6 }}>{formatTime(hoverTs)}</span> : null}
                  </span>
                  <button
                    type="button"
                    onClick={clearPerfHistory}
                    style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-tertiary)', fontSize: 10, padding: '2px 6px', borderRadius: 4, opacity: 0.7 }}
                    title="Clear all charts and history"
                  >
                    Clear
                  </button>
                </div>
                <div style={{ padding: '4px 12px 10px' }}>
                  {displayHottest.length === 0 ? (
                    <div style={{ color: 'var(--text-tertiary)', fontSize: 11, padding: '8px 0' }}>No activity.</div>
                  ) : displayHottest.map((row) => (
                    <div key={row.ns} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '5px 0', borderBottom: '1px solid var(--border)' }}>
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-primary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={row.ns}>
                          {row.ns}
                        </div>
                        <div style={{ marginTop: 3, height: 4, borderRadius: 2, background: 'var(--surface-1)', overflow: 'hidden' }}>
                          <div style={{ height: '100%', width: `${Math.min(row.pct, 100)}%`, borderRadius: 2, background: row.pct > 50 ? '#f59e0b' : row.pct > 10 ? '#60a5fa' : '#4ade80', transition: 'width 0.3s' }} />
                        </div>
                      </div>
                      <span style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-tertiary)', flexShrink: 0, minWidth: 40, textAlign: 'right' }}>
                        {row.pct.toFixed(row.pct >= 1 ? 0 : 2)}%
                      </span>
                    </div>
                  ))}
                </div>
              </div>

              <div style={{ background: 'var(--perf-card-bg, rgba(0,0,0,0.18))', borderRadius: 8, border: '1px solid var(--border)', overflow: 'hidden' }}>
                <div style={{ padding: '8px 12px 4px' }}>
                  <span style={{ color: 'var(--text-secondary)', fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
                    Slowest Operations
                    {hoverActive ? <span style={{ fontWeight: 400, fontSize: 10, marginLeft: 6, opacity: 0.6 }}>{formatTime(hoverTs)}</span> : null}
                  </span>
                </div>
                <div style={{ padding: '4px 12px 10px' }}>
                  {displaySlow.length === 0 ? (
                    <div style={{ color: 'var(--text-tertiary)', fontSize: 11, padding: '8px 0' }}>No slow operations.</div>
                  ) : displaySlow.slice(0, 8).map((item, idx) => (
                    <div key={`slow:${item.opid || idx}`} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '5px 0', borderBottom: '1px solid var(--border)' }}>
                      <span style={{ fontSize: 10, fontWeight: 700, padding: '1px 6px', borderRadius: 4, background: item.secsRunning > 10 ? 'rgba(248,113,113,0.2)' : item.secsRunning > 3 ? 'rgba(245,158,11,0.2)' : 'rgba(74,222,128,0.2)', color: item.secsRunning > 10 ? '#fca5a5' : item.secsRunning > 3 ? '#fcd34d' : '#86efac', flexShrink: 0, textTransform: 'uppercase' }}>
                        {item.desc || item.op || 'OP'}
                      </span>
                      <div style={{ flex: 1, minWidth: 0, fontSize: 11, fontFamily: 'monospace', color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={item.namespace}>
                        {item.namespace || '--'}
                      </div>
                      <span style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-tertiary)', flexShrink: 0 }}>
                        {formatNumber(Number(item.secsRunning || 0))}s
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}

        <style>{`
          @media (max-width: 900px) {
            .perf-combo-grid { grid-template-columns: 1fr !important; }
          }
        `}</style>
      </div>
    );
  };

  const renderRunForm = () => {
    if (activeTool === 'mongodump') {
      return (
        <div className="space-y-3">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Database</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs font-mono"
                value={form.dumpDb}
                options={[{ value: '', label: 'All databases' }, ...databaseOptions.map((dbName) => ({ value: dbName, label: dbName }))]}
                onChange={(value) => {
                  const nextDb = String(value || '');
                  setForm((prev) => ({ ...prev, dumpDb: nextDb, dumpCollection: '' }));
                  syncToolDbContext(nextDb, { field: 'dumpDb' });
                  if (nextDb) ensureCollectionOptions(nextDb);
                }}
                disabled={dbOptionsState.loading || databaseOptions.length === 0}
                menuZIndex={340}
              />
            </div>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Collection</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs font-mono"
                value={form.dumpCollection}
                options={[{ value: '', label: form.dumpDb ? 'All collections' : 'Select database first' }, ...dumpCollectionOptions.map((colName) => ({ value: colName, label: colName }))]}
                onChange={(value) => setForm((prev) => ({ ...prev, dumpCollection: String(value || '') }))}
                disabled={!form.dumpDb || collectionLoadingByDb[form.dumpDb]}
                menuZIndex={340}
              />
            </div>
          </div>
          <div>
            <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: form.dumpCollection ? 'var(--text-tertiary)' : 'var(--text-tertiary)' }}>
              Query JSON {!form.dumpCollection ? <span className="normal-case tracking-normal" style={{ color: 'var(--text-tertiary)', opacity: 0.7 }}> — requires a collection</span> : null}
            </div>
            <input
              className="input-field text-xs font-mono"
              value={form.dumpFilter}
              onChange={(e) => setForm((prev) => ({ ...prev, dumpFilter: e.target.value }))}
              disabled={!form.dumpCollection}
              placeholder={form.dumpCollection ? '{}' : 'Select a collection first'}
              style={dumpFilterError ? { borderColor: COLOR_ERROR } : undefined}
            />
            {dumpFilterError ? <div className="mt-0.5 text-2xs" style={{ color: COLOR_ERROR }}>{dumpFilterError}</div> : null}
          </div>
          <div>
            <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Output path</div>
            <input className="input-field text-xs font-mono" value={form.dumpPath} onChange={(e) => setForm((prev) => ({ ...prev, dumpPath: e.target.value }))} placeholder={suggestedDumpPath || ''} />
          </div>
          <label className="flex items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}><input className="ms-checkbox" type="checkbox" checked={form.dumpGzip} onChange={(e) => setForm((prev) => ({ ...prev, dumpGzip: e.target.checked }))} />gzip</label>
          <label className="flex items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}><input className="ms-checkbox" type="checkbox" checked={form.dumpOplog} onChange={(e) => setForm((prev) => ({ ...prev, dumpOplog: e.target.checked }))} />oplog</label>
        </div>
      );
    }
    if (activeTool === 'mongorestore') {
      return (
        <div className="space-y-3">
          <div>
            <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Input path</div>
            <input className="input-field text-xs font-mono" value={form.restorePath} onChange={(e) => setForm((prev) => ({ ...prev, restorePath: e.target.value }))} placeholder={suggestedRestorePath || ''} />
          </div>
          <label className="flex items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
            <input
              className="ms-checkbox"
              type="checkbox"
              checked={form.restoreMultiScope}
              onChange={(e) => {
                const enabled = e.target.checked;
                setForm((prev) => {
                  if (enabled) {
                    const seededDbList = prev.restoreDb && !(prev.restoreDbList || []).includes(prev.restoreDb)
                      ? [...(prev.restoreDbList || []), prev.restoreDb]
                      : (prev.restoreDbList || []);
                    return { ...prev, restoreMultiScope: true, restoreDbList: seededDbList };
                  }
                  return {
                    ...prev,
                    restoreMultiScope: false,
                    restoreDb: prev.restoreDb || ((prev.restoreDbList || []).length === 1 ? prev.restoreDbList[0] : ''),
                  };
                });
              }}
            />
            multi namespace scope (`--nsInclude`)
          </label>
          {!form.restoreMultiScope ? (
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Database scope (optional)</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs font-mono"
                value={form.restoreDb}
                options={[{ value: '', label: 'All namespaces from dump' }, ...databaseOptions.map((dbName) => ({ value: dbName, label: dbName }))]}
                onChange={(value) => {
                  const nextDb = String(value || '');
                  setForm((prev) => ({ ...prev, restoreDb: nextDb }));
                  syncToolDbContext(nextDb, { field: 'restoreDb' });
                }}
                disabled={dbOptionsState.loading || databaseOptions.length === 0}
                menuZIndex={340}
              />
            </div>
          ) : (
            <>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div>
                  <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Databases (multi)</div>
                  <div className="rounded-md border overflow-y-auto" style={{ borderColor: 'var(--border-secondary)', background: 'var(--bg-secondary)', maxHeight: 120 }}>
                    {(dbOptionsState.loading || databaseOptions.length === 0) ? (
                      <div className="px-2 py-1.5 text-2xs" style={{ color: 'var(--text-tertiary)' }}>No databases available</div>
                    ) : databaseOptions.map((dbName) => (
                      <label key={`restoredbm:${dbName}`} className="flex items-center gap-2 px-2 py-1 text-xs font-mono cursor-pointer hover-bg-tertiary" style={{ color: 'var(--text-secondary)' }}>
                        <input
                          className="ms-checkbox"
                          type="checkbox"
                          checked={form.restoreDbList.includes(dbName)}
                          onChange={(e) => {
                            const checked = e.target.checked;
                            setForm((prev) => {
                              const dbList = checked
                                ? [...prev.restoreDbList, dbName]
                                : prev.restoreDbList.filter((d) => d !== dbName);
                              return {
                                ...prev,
                                restoreDbList: dbList,
                                restoreCollectionList: dbList.length === 1 ? prev.restoreCollectionList : [],
                              };
                            });
                            const nextList = checked
                              ? [...form.restoreDbList, dbName]
                              : form.restoreDbList.filter((d) => d !== dbName);
                            syncToolDbContext(nextList.length === 1 ? nextList[0] : '', { field: 'restoreDbList' });
                            if (nextList.length === 1) ensureCollectionOptions(nextList[0]);
                          }}
                        />
                        {dbName}
                      </label>
                    ))}
                  </div>
                </div>
                <div>
                  <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Collections (multi, optional)</div>
                  <div className="rounded-md border overflow-y-auto" style={{ borderColor: 'var(--border-secondary)', background: 'var(--bg-secondary)', maxHeight: 120, opacity: (form.restoreDbList.length !== 1 || collectionLoadingByDb[form.restoreDbList[0]]) ? 0.5 : 1 }}>
                    {form.restoreDbList.length !== 1 ? (
                      <div className="px-2 py-1.5 text-2xs" style={{ color: 'var(--text-tertiary)' }}>Select exactly one database</div>
                    ) : restoreCollectionOptions.length === 0 ? (
                      <div className="px-2 py-1.5 text-2xs" style={{ color: 'var(--text-tertiary)' }}>No collections</div>
                    ) : restoreCollectionOptions.map((colName) => (
                      <label key={`restorecolm:${form.restoreDbList[0]}:${colName}`} className="flex items-center gap-2 px-2 py-1 text-xs font-mono cursor-pointer hover-bg-tertiary" style={{ color: 'var(--text-secondary)' }}>
                        <input
                          className="ms-checkbox"
                          type="checkbox"
                          checked={form.restoreCollectionList.includes(colName)}
                          onChange={(e) => {
                            const checked = e.target.checked;
                            setForm((prev) => ({
                              ...prev,
                              restoreCollectionList: checked
                                ? [...prev.restoreCollectionList, colName]
                                : prev.restoreCollectionList.filter((c) => c !== colName),
                            }));
                          }}
                          disabled={form.restoreDbList.length !== 1 || collectionLoadingByDb[form.restoreDbList[0]]}
                        />
                        {colName}
                      </label>
                    ))}
                  </div>
                </div>
              </div>
              <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                Multi-scope restore uses repeated `--nsInclude` patterns.
              </div>
            </>
          )}
          <label className="flex items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}><input className="ms-checkbox" type="checkbox" checked={form.restoreDrop} onChange={(e) => setForm((prev) => ({ ...prev, restoreDrop: e.target.checked }))} />drop before restore</label>
          <label className="flex items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}><input className="ms-checkbox" type="checkbox" checked={form.restoreGzip} onChange={(e) => setForm((prev) => ({ ...prev, restoreGzip: e.target.checked }))} />gzip archive</label>
        </div>
      );
    }
    if (activeTool === 'mongoexport') {
      return (
        <div className="space-y-3">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Database</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs font-mono"
                value={form.exportDb}
                options={[{ value: '', label: 'Select database' }, ...databaseOptions.map((dbName) => ({ value: dbName, label: dbName }))]}
                onChange={(value) => {
                  const nextDb = String(value || '');
                  setForm((prev) => ({ ...prev, exportDb: nextDb, exportCollection: '' }));
                  syncToolDbContext(nextDb, { field: 'exportDb' });
                  if (nextDb) ensureCollectionOptions(nextDb);
                }}
                disabled={dbOptionsState.loading || databaseOptions.length === 0}
                menuZIndex={340}
              />
            </div>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Collection</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs font-mono"
                value={form.exportCollection}
                options={[{ value: '', label: form.exportDb ? 'Select collection' : 'Select database first' }, ...exportCollectionOptions.map((colName) => ({ value: colName, label: colName }))]}
                onChange={(value) => setForm((prev) => ({ ...prev, exportCollection: String(value || '') }))}
                disabled={!form.exportDb || collectionLoadingByDb[form.exportDb]}
                menuZIndex={340}
              />
            </div>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Format</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs"
                value={form.exportFormat}
                options={[{ value: 'json', label: 'json' }, { value: 'csv', label: 'csv' }]}
                onChange={(value) => setForm((prev) => ({ ...prev, exportFormat: String(value || 'json') }))}
                menuZIndex={340}
              />
            </div>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Limit</div>
              <input className="input-field text-xs" value={form.exportLimit} onChange={(e) => setForm((prev) => ({ ...prev, exportLimit: e.target.value }))} />
            </div>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Output file</div>
              <input className="input-field text-xs font-mono" value={form.exportFile} onChange={(e) => setForm((prev) => ({ ...prev, exportFile: e.target.value }))} placeholder={suggestedExportFile} />
            </div>
          </div>
          <div>
            <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Query JSON</div>
            <input
              className="input-field text-xs font-mono"
              value={form.exportFilter}
              onChange={(e) => setForm((prev) => ({ ...prev, exportFilter: e.target.value }))}
              placeholder="{}"
              style={exportFilterError ? { borderColor: COLOR_ERROR } : undefined}
            />
            {exportFilterError ? <div className="mt-0.5 text-2xs" style={{ color: COLOR_ERROR }}>{exportFilterError}</div> : null}
          </div>
          {form.exportFormat === 'csv' ? (
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>
                Fields <span className="normal-case tracking-normal" style={{ opacity: 0.7 }}>— comma-separated, required for CSV</span>
              </div>
              <input
                className="input-field text-xs font-mono"
                value={form.exportFields}
                onChange={(e) => setForm((prev) => ({ ...prev, exportFields: e.target.value }))}
                placeholder="field1,field2,nested.field"
              />
            </div>
          ) : null}
        </div>
      );
    }
    if (activeTool === 'mongoimport') {
      return (
        <div className="space-y-3">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Database</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs font-mono"
                value={form.importDb}
                options={[{ value: '', label: 'Select database' }, ...databaseOptions.map((dbName) => ({ value: dbName, label: dbName }))]}
                onChange={(value) => {
                  const nextDb = String(value || '');
                  setForm((prev) => ({ ...prev, importDb: nextDb, importCollection: '' }));
                  syncToolDbContext(nextDb, { field: 'importDb' });
                  if (nextDb) ensureCollectionOptions(nextDb);
                }}
                disabled={dbOptionsState.loading || databaseOptions.length === 0}
                menuZIndex={340}
              />
            </div>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Collection</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs font-mono"
                value={form.importCollection}
                options={[{ value: '', label: form.importDb ? 'Select collection' : 'Select database first' }, ...importCollectionOptions.map((colName) => ({ value: colName, label: colName }))]}
                onChange={(value) => setForm((prev) => ({ ...prev, importCollection: String(value || '') }))}
                disabled={!form.importDb || collectionLoadingByDb[form.importDb]}
                menuZIndex={340}
              />
            </div>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Format</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs"
                value={form.importFormat}
                options={[{ value: 'json', label: 'json' }, { value: 'csv', label: 'csv' }, { value: 'tsv', label: 'tsv' }]}
                onChange={(value) => setForm((prev) => ({ ...prev, importFormat: String(value || 'json') }))}
                menuZIndex={340}
              />
            </div>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Mode</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs"
                value={form.importMode}
                options={[{ value: 'insert', label: 'insert' }, { value: 'upsert', label: 'upsert' }, { value: 'merge', label: 'merge' }, { value: 'delete', label: 'delete' }]}
                onChange={(value) => setForm((prev) => ({ ...prev, importMode: String(value || 'insert') }))}
                menuZIndex={340}
              />
            </div>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Input file</div>
              <input className="input-field text-xs font-mono" value={form.importFile} onChange={(e) => setForm((prev) => ({ ...prev, importFile: e.target.value }))} placeholder={suggestedImportFile} />
            </div>
          </div>
          <label className="flex items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}><input className="ms-checkbox" type="checkbox" checked={form.importDrop} onChange={(e) => setForm((prev) => ({ ...prev, importDrop: e.target.checked }))} />drop collection first</label>
        </div>
      );
    }
    if (activeTool === 'mongofiles') {
      return (
        <div className="space-y-3">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Action</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs"
                value={form.gridfsAction}
                options={[{ value: 'list', label: 'list' }, { value: 'search', label: 'search' }, { value: 'get', label: 'get' }, { value: 'put', label: 'put' }, { value: 'delete', label: 'delete' }]}
                onChange={(value) => setForm((prev) => ({ ...prev, gridfsAction: String(value || 'list') }))}
                menuZIndex={340}
              />
            </div>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Database</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs font-mono"
                value={form.gridfsDb}
                options={databaseOptions.map((dbName) => ({ value: dbName, label: dbName }))}
                onChange={(value) => {
                  const nextDb = String(value || '');
                  setForm((prev) => ({ ...prev, gridfsDb: nextDb }));
                  syncToolDbContext(nextDb, { field: 'gridfsDb' });
                }}
                disabled={dbOptionsState.loading || databaseOptions.length === 0}
                menuZIndex={340}
              />
            </div>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Bucket</div>
              <input className="input-field text-xs" value={form.gridfsBucket} onChange={(e) => setForm((prev) => ({ ...prev, gridfsBucket: e.target.value }))} />
            </div>
          </div>
          {(() => {
            const action = form.gridfsAction;
            const filenameLabel = action === 'list' ? 'Filter prefix' : action === 'search' ? 'Search pattern' : action === 'get' ? 'GridFS filename to download' : action === 'put' ? 'GridFS filename (destination)' : action === 'delete' ? 'GridFS filename to delete' : 'Filename';
            const filenamePlaceholder = action === 'list' ? 'optional prefix filter' : action === 'search' ? 'regex pattern' : '';
            const showLocal = action === 'get' || action === 'put';
            const localLabel = action === 'get' ? 'Save to local path' : 'Local file to upload';
            return (
              <div className={`grid grid-cols-1 ${showLocal ? 'md:grid-cols-2' : ''} gap-3`}>
                <div>
                  <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>{filenameLabel}</div>
                  <input className="input-field text-xs font-mono" value={form.gridfsFilename} onChange={(e) => setForm((prev) => ({ ...prev, gridfsFilename: e.target.value }))} placeholder={filenamePlaceholder} />
                </div>
                {showLocal ? (
                  <div>
                    <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>{localLabel}</div>
                    <input className="input-field text-xs font-mono" value={form.gridfsLocalPath} onChange={(e) => setForm((prev) => ({ ...prev, gridfsLocalPath: e.target.value }))} placeholder={suggestedGridFsLocalPath} />
                  </div>
                ) : null}
              </div>
            );
          })()}
        </div>
      );
    }
    return <div className="text-xs" style={{ color: 'var(--text-tertiary)' }}>Tool form is not configured.</div>;
  };

  const dumpFilterError = useMemo(() => form.dumpCollection ? validateJsonFilter(form.dumpFilter) : '', [form.dumpFilter, form.dumpCollection]);
  const exportFilterError = useMemo(() => validateJsonFilter(form.exportFilter), [form.exportFilter]);

  const runFormValidation = useMemo(() => {
    if (activeTool === 'mongodump') {
      if (dumpFilterError) return 'Invalid query JSON';
    }
    if (activeTool === 'mongoexport') {
      if (!form.exportDb) return 'Select a database';
      if (!form.exportCollection) return 'Select a collection';
      if (form.exportFormat === 'csv' && !form.exportFields.trim()) return 'Fields are required for CSV export';
      if (exportFilterError) return 'Invalid query JSON';
    }
    if (activeTool === 'mongoimport') {
      if (!form.importDb) return 'Select a database';
      if (!form.importCollection) return 'Select a collection';
    }
    if (activeTool === 'mongofiles') {
      const action = form.gridfsAction;
      if ((action === 'get' || action === 'delete') && !form.gridfsFilename.trim()) return `Filename is required for ${action}`;
    }
    return '';
  }, [activeTool, dumpFilterError, exportFilterError, form.exportCollection, form.exportDb, form.exportFields, form.exportFormat, form.gridfsAction, form.gridfsFilename, form.importCollection, form.importDb]);

  return (
    <div className="h-full flex flex-col overflow-hidden">
      <div className="h-11 px-4 flex items-center gap-2" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
        <div className="inline-flex items-center gap-2 text-xs font-semibold" style={{ color: 'var(--text-primary)' }}>
          <Server className="w-3.5 h-3.5" />
          <span>{isPerfCombo ? 'Performance' : (TOOL_TITLES[activeTool] || activeTool)}</span>
          {isPerfCombo ? (
            <>
              <span className="badge-blue font-mono">{perfScope === 'cluster' ? 'cluster' : `db: ${perfScope}`}</span>
              <span className="badge-accent font-mono" title={readNodeScope.isAll ? 'All nodes' : (readNodeScope.node || effectiveDraftNode)}>
                {readNodeScope.isAll ? 'all nodes' : (readNodeScope.node || effectiveDraftNode || '--')}
              </span>
            </>
          ) : null}
          {LIVE_TOOLS.has(activeTool) && !isPerfCombo ? (
            <span className={`inline-flex items-center gap-1 ${pollPaused ? 'badge-yellow' : 'badge-green'}`}>
              <span className={`w-1.5 h-1.5 rounded-full ${pollPaused ? 'bg-amber-400' : 'bg-emerald-400 animate-pulse-dot'}`} />
              {pollPaused ? 'Paused' : 'Live'}
            </span>
          ) : null}
        </div>
        <div className="flex-1" />
      </div>

      <div className="flex-1 p-4 space-y-4" style={{ background: 'var(--surface-0)', overflowY: 'scroll', overflowX: 'hidden' }}>
        {/* ── Monitoring Scope (read-tools except perf combo) ── */}
        {isReadTool && !isPerfCombo ? (
          <div className="rounded-xl px-3 py-2.5" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
            <div className="flex flex-wrap items-end gap-3">
              {DB_SCOPED_READ_TOOLS.has(activeTool) ? (
                <div className="min-w-[140px] flex-1" style={{ maxWidth: 240 }}>
                  <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Database</div>
                  <DropdownSelect
                    fullWidth
                    sizeClassName="text-xs font-mono"
                    value={draftContextDb}
                    options={dbContextOptions}
                    onChange={(value) => {
                      const next = String(value || '').trim();
                      setDraftContextDb(next);
                      setReadState((prev) => ({ ...prev, error: '' }));
                    }}
                    disabled={dbOptionsState.loading || dbContextOptions.length === 0}
                    menuZIndex={340}
                  />
                </div>
              ) : null}
              <div className="min-w-[160px] flex-1" style={{ maxWidth: 280 }}>
                <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Node</div>
                <DropdownSelect
                  fullWidth
                  sizeClassName="text-xs"
                  value={targetNodeSupportsAllNodes && readNodeSelection === 'all' ? '__all__' : draftNode}
                  options={targetNodeSupportsAllNodes
                    ? [{ value: '__all__', label: 'All nodes' }, ...nodeSelectOptions]
                    : nodeSelectOptions}
                  onChange={(value) => {
                    if (value === '__all__') {
                      setReadNodeSelection('all');
                      setDraftNode(String(defaultNode || ''));
                    } else {
                      setReadNodeSelection('context');
                      setDraftNode(String(value || ''));
                    }
                    setReadState((prev) => ({ ...prev, error: '' }));
                  }}
                  disabled={contextState.loading || nodeOptions.length === 0}
                  menuZIndex={340}
                />
              </div>
              {LIVE_TOOLS.has(activeTool) ? (
                <div className="flex items-center gap-1.5 pb-0.5">
                  <DropdownSelect
                    sizeClassName="text-2xs"
                    value={pollMs}
                    options={POLL_OPTIONS.map((v) => ({ value: v, label: `${v / 1000}s` }))}
                    onChange={(value) => setPollMs(Number(value) || 5000)}
                    menuZIndex={340}
                  />
                  <button type="button" className="btn-ghost text-2xs px-2 py-1" onClick={() => setPollPaused((p) => !p)} aria-label={pollPaused ? 'Resume polling' : 'Pause polling'}>
                    {pollPaused ? 'Resume' : 'Pause'}
                  </button>
                </div>
              ) : null}
              <button
                type="button"
                className="btn-ghost p-1.5 mb-0.5"
                onClick={refreshReadTool}
                title="Refresh"
                disabled={readToolActionsDisabled}
              >
                <Refresh className={`w-3.5 h-3.5 ${refreshSpinning ? 'animate-spin' : ''}`} />
              </button>
            </div>
            {contextState.error ? <div className="mt-2 text-2xs" style={{ color: COLOR_ERROR }}>{contextState.error}</div> : null}
            {dbOptionsState.error ? <div className="mt-2 text-2xs" style={{ color: COLOR_ERROR_MUTED }}>DB list unavailable: {dbOptionsState.error}</div> : null}
          </div>
        ) : null}

        {/* ── Execution Context (run-tools only) ── */}
        {isRunTool ? (
        <div className="rounded-xl p-3 space-y-3" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Execution Context</div>
            <button
              type="button"
              className="btn-ghost text-2xs px-2 py-1"
              onClick={() => { loadContext({ refresh: true }); loadDbOptions(); }}
            >
              Refresh
            </button>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Active DB</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs font-mono"
                value={draftContextDb}
                options={dbContextOptions}
                onChange={(value) => {
                  const next = String(value || '').trim();
                  setDraftContextDb(next);
                  if (next) {
                    const preferredCollection = selectedDb && selectedDb === next ? String(selectedCol || '').trim() : '';
                    setForm((prev) => ({
                      ...prev,
                      dumpDb: next,
                      dumpCollection: preferredCollection || '',
                      restoreDb: next,
                      restoreDbList: [next],
                      restoreCollectionList: preferredCollection ? [preferredCollection] : [],
                      exportDb: next,
                      exportCollection: preferredCollection || '',
                      importDb: next,
                      importCollection: preferredCollection || '',
                      gridfsDb: next,
                    }));
                    pushDbContextChange(next, { field: 'contextDb', sync: 'tools+console' });
                    setReadState((prev) => ({ ...prev, error: '' }));
                    setRunState((prev) => ({ ...prev, error: '' }));
                  }
                }}
                disabled={dbOptionsState.loading || dbContextOptions.length === 0}
                menuZIndex={340}
              />
            </div>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Target node</div>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs"
                value={draftNode}
                options={nodeSelectOptions}
                onChange={(value) => {
                  setReadNodeSelection('context');
                  setDraftNode(String(value || ''));
                  setConfirmNodeSelection(false);
                }}
                disabled={contextState.loading || nodeOptions.length === 0}
              />
              {draftNodeNeedsConfirmation ? (
                <label className="mt-1.5 flex items-center gap-2 text-2xs" style={{ color: 'var(--text-secondary)' }}>
                  <input className="ms-checkbox" type="checkbox" checked={confirmNodeSelection} onChange={(e) => setConfirmNodeSelection(e.target.checked)} />
                  Confirm switch to {draftNode}
                </label>
              ) : null}
            </div>
          </div>
          <div>
            <div className="text-2xs uppercase tracking-wider mb-1" style={{ color: 'var(--text-tertiary)' }}>Output path</div>
            <div className="flex items-center gap-2">
              <input
                className="input-field text-xs font-mono flex-1"
                value={draftPath}
                onChange={(e) => { setDraftPath(e.target.value); setPathMode('custom'); setConfirmPathSelection(false); }}
                placeholder={basePath || '/tmp/mongostudio/...'}
                disabled={contextState.loading || pathMode !== 'custom'}
                readOnly={pathMode !== 'custom'}
              />
              <div className="flex gap-1" style={{ flexShrink: 0 }}>
                {[
                  { id: 'auto', label: 'Auto' },
                  ...(hostWorkdir ? [{ id: 'host', label: 'Host' }] : []),
                  { id: 'custom', label: 'Custom' },
                ].map((opt) => (
                  <button
                    key={opt.id}
                    type="button"
                    onClick={() => { setPathMode(opt.id); if (opt.id === 'custom') setConfirmPathSelection(false); }}
                    className="text-2xs font-medium px-2.5 py-0.5 rounded-md"
                    style={{
                      background: pathMode === opt.id ? 'var(--accent)' : 'var(--surface-2)',
                      color: pathMode === opt.id ? 'var(--surface-0)' : 'var(--text-secondary)',
                      border: pathMode === opt.id ? '1px solid var(--accent)' : '1px solid var(--border)',
                    }}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            </div>
            {pathMode === 'custom' && draftPathIsCustom ? (
              <label className="mt-1.5 flex items-center gap-2 text-2xs" style={{ color: 'var(--text-secondary)' }}>
                <input className="ms-checkbox" type="checkbox" checked={confirmPathSelection} onChange={(e) => setConfirmPathSelection(e.target.checked)} />
                Confirm custom path
              </label>
            ) : null}
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <button type="button" className="btn-primary text-xs px-3 py-1.5" onClick={applyContext} disabled={!canApplyContext || !hasPendingContextChanges}>Apply Context</button>
            {showAppliedStatus ? <span className="badge-green">Applied</span> : null}
            {hasPendingContextChanges && !showAppliedStatus ? <span className="badge-yellow">Pending changes</span> : null}
          </div>
          {contextState.error ? <div className="text-2xs" style={{ color: COLOR_ERROR }}>{contextState.error}</div> : null}
          {dbOptionsState.error ? <div className="text-2xs" style={{ color: COLOR_ERROR_MUTED }}>DB list unavailable: {dbOptionsState.error}</div> : null}
          <details
            className="rounded-lg"
            open={showContextDetails}
            onToggle={(e) => setShowContextDetails(Boolean(e.currentTarget.open))}
            style={{ border: '1px solid var(--border)', background: 'var(--surface-0)' }}
          >
            <summary className="px-2.5 py-1.5 text-2xs uppercase tracking-wider cursor-pointer select-none" style={{ color: 'var(--text-tertiary)' }}>Advanced</summary>
            <div className="px-2.5 pb-2.5 space-y-2">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                <ContextLine label="Machine" value={context?.machine?.host || '--'} mono />
                <ContextLine label="BasePath" value={context?.basePath || '--'} mono wrap />
                <ContextLine label="Applied" value={executionContext?.path || '--'} mono wrap />
              </div>
              <div className="flex flex-wrap gap-1.5">
                {binaryEntries.map(([id, entry]) => (
                  <span
                    key={id}
                    className={entry?.available ? 'badge-green' : 'badge-red'}
                    title={[entry?.bin ? `bin: ${entry.bin}` : '', entry?.version ? `version: ${entry.version}` : '', entry?.error ? `reason: ${entry.error}` : ''].filter(Boolean).join('\n')}
                  >
                    {id}: {entry?.available ? 'ok' : 'missing'}
                  </span>
                ))}
              </div>
              {missingBinaryEntries.length > 0 ? (
                <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                  Install missing tools or set env paths (e.g. <span className="font-mono">MONGODUMP_BIN</span>).
                </div>
              ) : null}
            </div>
          </details>
        </div>
        ) : null}

        {readState.error && !isPerfCombo ? (
          <div className="rounded-xl p-3 text-xs" style={STYLE_ERROR_BOX}>
            {readState.error}
          </div>
        ) : null}

        {READ_TOOLS.has(activeTool) ? (
          activeToolMissingBinary ? (
            <div className="rounded-xl p-4 space-y-2" style={{ background: 'var(--surface-1)', border: '1px solid rgba(245,158,11,0.3)' }}>
              <div className="text-xs font-medium" style={{ color: COLOR_WARNING }}>
                {activeToolMissingBinary.tool} binary not available
              </div>
              <div className="text-2xs" style={{ color: 'var(--text-secondary)' }}>
                {activeToolMissingBinary.error}
              </div>
              <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                Install MongoDB Database Tools or set the <span className="font-mono">{activeToolMissingBinary.tool.toUpperCase()}_BIN</span> environment variable.
              </div>
            </div>
          )
          : activeTool === PERF_COMBO_TOOL ? renderPerfCombo()
            : activeTool === 'serverInfo' ? renderServerInfo()
            : activeTool === 'mongostat' ? renderMongostat()
              : activeTool === 'mongotop' ? renderMongotop()
                : renderSlowOps()
        ) : (
          <div className="space-y-3">
            <div className="rounded-xl p-3" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
              {renderRunForm()}
              <div className="mt-4 flex items-center gap-2">
                <button
                  type="button"
                  className="btn-primary text-xs px-3 py-1.5 inline-flex items-center gap-1.5"
                  onClick={runTool}
                  disabled={runToolActionsDisabled || Boolean(runFormValidation)}
                  title={runFormValidation || (hasPendingContextChanges ? 'Apply execution context first' : `Run ${TOOL_TITLES[activeTool] || activeTool}`)}
                >
                  {activeTool === 'mongodump' || activeTool === 'mongoexport' ? <Download className="w-3.5 h-3.5" /> : activeTool === 'mongorestore' || activeTool === 'mongoimport' ? <Upload className="w-3.5 h-3.5" /> : <Activity className="w-3.5 h-3.5" />}
                  <span>{runState.busy ? `Running... ${runElapsedMs > 1000 ? `${(runElapsedMs / 1000).toFixed(0)}s` : ''}` : `Run ${TOOL_TITLES[activeTool] || activeTool}`}</span>
                </button>
                {runState.busy && runElapsedMs > 0 ? (
                  <span className="text-2xs font-mono" style={{ color: 'var(--text-tertiary)' }}>{(runElapsedMs / 1000).toFixed(1)}s</span>
                ) : null}
              </div>
              {runFormValidation ? (
                <div className="mt-2 text-2xs" style={{ color: COLOR_WARNING_TEXT }}>{runFormValidation}</div>
              ) : null}
              {hasPendingContextChanges ? (
                <div className="mt-2 text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                  Apply execution context before running.
                </div>
              ) : null}
              {runState.error ? <div className="mt-2 text-xs" style={{ color: COLOR_ERROR }}>{runState.error}</div> : null}
            </div>

            {runState.result ? (
              <div className="rounded-xl p-3 space-y-3" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
                <div className="flex flex-wrap items-center gap-2">
                  {runState.result.ok
                    ? <span className="badge-green inline-flex items-center gap-1"><Check className="w-3 h-3" />Success</span>
                    : <span className="badge-red inline-flex items-center gap-1"><AlertTriangle className="w-3 h-3" />Failed</span>}
                  {runState.result.timedOut ? <span className="badge-yellow inline-flex items-center gap-1"><Clock className="w-3 h-3" />Timeout</span> : null}
                  <span className="text-2xs font-mono" style={{ color: 'var(--text-tertiary)' }}>{formatNumber(Number(runState.result.durationMs || 0))} ms</span>
                </div>
                {runState.result.executionInfo ? (
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                    <StatCard label="Node" value={runState.result.executionInfo.node || '--'} />
                    <StatCard label="Database" value={runState.result.executionInfo.db || 'all'} />
                    <StatCard label="Collection" value={runState.result.executionInfo.collection || 'all'} />
                    <div className="rounded-xl px-3 py-2.5" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
                      <div className="flex items-center justify-between">
                        <div className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Path</div>
                        <CopyButton text={runState.result.executionInfo.path} label="Copy path" />
                      </div>
                      <div className="text-sm font-semibold mt-1" style={{ color: 'var(--text-primary)' }}>{(() => { const p = runState.result.executionInfo.path || ''; return p.split('/').pop() || p; })()}</div>
                      <div className="text-2xs mt-1 break-all" style={{ color: 'var(--text-tertiary)' }} title={runState.result.executionInfo.path}>{runState.result.executionInfo.path}</div>
                    </div>
                  </div>
                ) : null}
                <details className="rounded-lg" style={{ border: '1px solid var(--border)', background: 'var(--surface-0)' }}>
                  <summary className="px-2.5 py-1.5 text-2xs uppercase tracking-wider cursor-pointer select-none" style={{ color: 'var(--text-tertiary)' }}>
                    Raw output
                  </summary>
                  <div className="px-2.5 pb-2.5 space-y-2">
                    {runState.result.commandPreview ? (
                      <div>
                        <div className="flex items-center justify-between mb-1">
                          <div className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Command</div>
                          <CopyButton text={runState.result.commandPreview} label="Copy command" />
                        </div>
                        <TruncatedPre text={runState.result.commandPreview} />
                      </div>
                    ) : null}
                    {runState.result.stdout ? (
                      <div>
                        <div className="flex items-center justify-between mb-1">
                          <div className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>stdout</div>
                          <CopyButton text={runState.result.stdout} label="Copy stdout" />
                        </div>
                        <TruncatedPre text={runState.result.stdout} />
                      </div>
                    ) : null}
                    {runState.result.stderr ? (
                      <div>
                        <div className="flex items-center justify-between mb-1">
                          <div className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>stderr</div>
                          <CopyButton text={runState.result.stderr} label="Copy stderr" />
                        </div>
                        <TruncatedPre text={runState.result.stderr} color="#fca5a5" />
                      </div>
                    ) : null}
                  </div>
                </details>
              </div>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}

import React, { useState, useCallback, useRef, useEffect, useMemo } from 'react';
import api from '../utils/api';
import { Play, Copy, AlertCircle, Check, Zap, X, History, StopCircle, Eye, AlertTriangle, Loader, ChevronDown, Download } from './Icons';
import { formatNumber, formatDuration, prettyJson } from '../utils/formatters';
import JsonView from './JsonView';

const HISTORY_KEY = 'mongostudio_query_history';
const TEMPLATES = [
  { label: 'Find all', query: 'db.collection.find({})' },
  { label: 'Find with filter', query: 'db.collection.find({ status: "active" })' },
  { label: 'Count', query: 'db.collection.aggregate([\n  { "$count": "total" }\n])' },
  { label: 'Group by', query: 'db.collection.aggregate([\n  { "$group": { "_id": "$field", "count": { "$sum": 1 } } },\n  { "$sort": { "count": -1 } }\n])' },
  { label: 'Lookup', query: 'db.collection.aggregate([\n  { "$lookup": {\n    "from": "other",\n    "localField": "fk",\n    "foreignField": "_id",\n    "as": "joined"\n  } }\n])' },
  { label: 'Unwind + group', query: 'db.collection.aggregate([\n  { "$unwind": "$tags" },\n  { "$group": { "_id": "$tags", "count": { "$sum": 1 } } },\n  { "$sort": { "count": -1 } }\n])' },
  { label: 'Date range', query: 'db.collection.find({\n  "createdAt": {\n    "$gte": { "$date": "2024-01-01T00:00:00Z" },\n    "$lt": { "$date": "2025-01-01T00:00:00Z" }\n  }\n})' },
];

function getHistory() { try { return JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]'); } catch { return []; } }
function saveHistory(entry) {
  try {
    const h = getHistory().filter((e) => !(e.query === entry.query && e.db === entry.db && e.collection === entry.collection));
    h.unshift(entry);
    localStorage.setItem(HISTORY_KEY, JSON.stringify(h.slice(0, 50)));
  } catch {}
}

export default function QueryConsole({ db, collection, onQueryMs, refreshToken = 0 }) {
  const [query, setQuery] = useState(`db.${collection}.find({})`);
  const [results, setResults] = useState(null);
  const [error, setError] = useState(null);
  const [running, setRunning] = useState(false);
  const [copied, setCopied] = useState(false);
  const [elapsed, setElapsed] = useState(null);
  const [liveTimer, setLiveTimer] = useState(0);
  const [showTemplates, setShowTemplates] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [history, setHistory] = useState([]);
  const [explainResult, setExplainResult] = useState(null);
  const [showExplain, setShowExplain] = useState(false);
  const [slow, setSlow] = useState(false);
  const [resultSize, setResultSize] = useState(null);
  const [indexHints, setIndexHints] = useState([]);
  const [selectedHint, setSelectedHint] = useState('auto');
  const [schemaFields, setSchemaFields] = useState([]);
  const [showAutofill, setShowAutofill] = useState(false);
  const [autofillLimit, setAutofillLimit] = useState(8);
  const [placeholderIndex, setPlaceholderIndex] = useState(0);
  const [showQueryAssist, setShowQueryAssist] = useState(false);
  const [queryAssistLimit, setQueryAssistLimit] = useState(6);
  const [showExportMenu, setShowExportMenu] = useState(false);
  const [globalToggleVersion, setGlobalToggleVersion] = useState(0);
  const [globalToggleOpen, setGlobalToggleOpen] = useState(true);

  const controllerRef = useRef(null);
  const timerRef = useRef(null);
  const textareaRef = useRef(null);
  const templatesRef = useRef(null);
  const historyRef = useRef(null);
  const autofillRef = useRef(null);
  const queryAssistRef = useRef(null);
  const exportMenuRef = useRef(null);

  useEffect(() => { setHistory(getHistory()); }, []);
  useEffect(() => {
    setQuery(`db.${collection}.find({})`);
    setResults(null);
    setError(null);
    setExplainResult(null);
    setSelectedHint('auto');
    setShowTemplates(false);
    setShowHistory(false);
    setShowAutofill(false);
    setShowQueryAssist(false);
    setAutofillLimit(8);
    setQueryAssistLimit(6);
    setPlaceholderIndex(0);
    setShowExportMenu(false);
    setGlobalToggleVersion(0);
  }, [db, collection]);

  useEffect(() => {
    let active = true;
    api.getIndexes(db, collection)
      .then(data => {
        if (!active) return;
        const names = (data.indexes || []).map(idx => idx.name).filter(Boolean);
        setIndexHints(names);
      })
      .catch(() => {
        if (active) setIndexHints([]);
      });
    return () => { active = false; };
  }, [db, collection, refreshToken]);

  useEffect(() => {
    let active = true;
    api.getSchema(db, collection, 120)
      .then((data) => {
        if (!active) return;
        const fields = (data.fields || []).map((field) => field.path).filter(Boolean).slice(0, 24);
        setSchemaFields(fields);
      })
      .catch(() => {
        if (active) setSchemaFields([]);
      });
    return () => { active = false; };
  }, [db, collection, refreshToken]);

  useEffect(() => {
    if (selectedHint === 'auto') return;
    if (!indexHints.includes(selectedHint)) setSelectedHint('auto');
  }, [indexHints, selectedHint]);

  useEffect(() => {
    const onMouseDown = (event) => {
      if (showTemplates && templatesRef.current && !templatesRef.current.contains(event.target)) setShowTemplates(false);
      if (showHistory && historyRef.current && !historyRef.current.contains(event.target)) setShowHistory(false);
      if (showAutofill && autofillRef.current && !autofillRef.current.contains(event.target)) setShowAutofill(false);
      if (showExportMenu && exportMenuRef.current && !exportMenuRef.current.contains(event.target)) setShowExportMenu(false);
      if (showQueryAssist && queryAssistRef.current && !queryAssistRef.current.contains(event.target) && textareaRef.current && !textareaRef.current.contains(event.target)) {
        setShowQueryAssist(false);
      }
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        setShowTemplates(false);
        setShowHistory(false);
        setShowAutofill(false);
        setShowExportMenu(false);
        setShowQueryAssist(false);
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [showTemplates, showHistory, showAutofill, showExportMenu, showQueryAssist]);

  const startLiveTimer = () => {
    setLiveTimer(0);
    timerRef.current = setInterval(() => setLiveTimer(t => t + 100), 100);
  };

  const stopLiveTimer = () => {
    if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
  };

  const parseQuery = (q) => {
    const trimmed = q.trim();
    if (!trimmed) throw new Error('Query is empty.');

    const parseCommand = (op) => {
      const m = trimmed.match(new RegExp(`^db\\.(?:getCollection\\((['"])(.+?)\\1\\)|([\\w.$-]+))\\.${op}\\(([\\s\\S]*)\\)$`));
      if (!m) return null;
      return { targetCollection: m[2] || m[3] || '', args: m[4] || '' };
    };

    const findCmd = parseCommand('find');
    const aggCmd = parseCommand('aggregate');

    if (findCmd) {
      if (findCmd.targetCollection && findCmd.targetCollection !== collection) {
        throw new Error(`Query context mismatch: selected collection is "${collection}", but query targets "${findCmd.targetCollection}".`);
      }
      try {
        const args = findCmd.args.trim();
        let filter = '{}';
        let sort = '{}';
        const parsed = JSON.parse(`[${args}]`);
        filter = JSON.stringify(parsed[0] || {});
        if (parsed[1]?.sort) sort = JSON.stringify(parsed[1].sort);
        return { type: 'find', filter, sort };
      } catch {
        return { type: 'find', filter: findCmd.args.trim(), sort: '{}' };
      }
    }

    if (aggCmd) {
      if (aggCmd.targetCollection && aggCmd.targetCollection !== collection) {
        throw new Error(`Query context mismatch: selected collection is "${collection}", but query targets "${aggCmd.targetCollection}".`);
      }
      try {
        const pipeline = JSON.parse(aggCmd.args);
        return { type: 'aggregate', pipeline };
      } catch (e) {
        throw new Error(`Invalid pipeline JSON: ${e.message}`);
      }
    }

    try {
      const raw = JSON.parse(trimmed);
      if (Array.isArray(raw)) return { type: 'aggregate', pipeline: raw };
      return { type: 'find', filter: trimmed, sort: '{}' };
    } catch {}

    throw new Error(`Unsupported query format. Use db.${collection}.find({...}) or db.${collection}.aggregate([...]).`);
  };

  const handleRun = useCallback(async () => {
    if (running) {
      controllerRef.current?.abort();
      return;
    }
    setRunning(true);
    setError(null);
    setResults(null);
    setExplainResult(null);
    setSlow(false);
    setResultSize(null);
    startLiveTimer();

    const controller = new AbortController();
    controllerRef.current = controller;

    try {
      const parsed = parseQuery(query);
      let data;
      if (parsed.type === 'aggregate') {
        data = await api.runAggregation(db, collection, parsed.pipeline, controller);
        data.documents = data.results;
        data.total = data.results?.length || 0;
      } else {
        data = await api.getDocuments(
          db,
          collection,
          { filter: parsed.filter, sort: parsed.sort, limit: 100, hint: selectedHint },
          controller,
        );
      }

      const ms = data._elapsed || 0;
      setResults(data);
      setElapsed(ms);
      onQueryMs?.(ms);
      if (data._slow) setSlow(true);

      const jsonStr = JSON.stringify(data.documents || data.results || []);
      setResultSize(new Blob([jsonStr]).size);

      saveHistory({
        query,
        db,
        collection,
        ts: Date.now(),
        elapsed: ms,
        count: data.total || data.documents?.length || 0,
        type: parsed.type,
      });
      setHistory(getHistory());

      if (data.warnings?.length) data.warnings.forEach(w => console.warn('[Query]', w));
    } catch (err) {
      if (err.name !== 'AbortError') setError(err.message);
    } finally {
      setRunning(false);
      stopLiveTimer();
      controllerRef.current = null;
    }
  }, [query, db, collection, running, onQueryMs, selectedHint]);

  const handleExplain = async () => {
    setExplainResult(null);
    try {
      const parsed = parseQuery(query);
      const data = await api.explain(db, collection, { ...parsed, hint: parsed.type === 'find' ? selectedHint : 'auto' });
      setExplainResult(data);
      setShowExplain(true);
    } catch (err) {
      setError(err.message);
    }
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(prettyJson(results?.documents || results?.results || []));
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const toCsv = (rows = []) => {
    if (!Array.isArray(rows) || rows.length === 0) return '';
    const keys = [...new Set(rows.flatMap((row) => Object.keys(row || {})))];
    const header = keys.join(',');
    const lines = rows.map((row) => keys.map((key) => {
      const value = row?.[key];
      if (value === null || value === undefined) return '';
      if (typeof value === 'object') return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
      const text = String(value);
      return (text.includes(',') || text.includes('"') || text.includes('\n'))
        ? `"${text.replace(/"/g, '""')}"`
        : text;
    }).join(','));
    return [header, ...lines].join('\n');
  };

  const downloadText = (filename, text, mime = 'application/json') => {
    const blob = new Blob([text], { type: mime });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    link.click();
    URL.revokeObjectURL(url);
  };

  const handleExportResults = (format = 'json') => {
    if (!docs.length) return;
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    if (format === 'csv') {
      downloadText(`${db}.${collection}.query.${ts}.csv`, toCsv(docs), 'text/csv');
      setShowExportMenu(false);
      return;
    }
    downloadText(`${db}.${collection}.query.${ts}.json`, JSON.stringify(docs, null, 2), 'application/json');
    setShowExportMenu(false);
  };

  const handleToggleAllDocs = (open) => {
    setGlobalToggleOpen(open);
    setGlobalToggleVersion((prev) => prev + 1);
  };

  const handleKeyDown = (event) => {
    if (event.key === 'Tab' && showQueryAssist && visibleQueryAssistItems.length > 0) {
      event.preventDefault();
      applyQueryAssistSuggestion(visibleQueryAssistItems[0]);
      return;
    }
    if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
      event.preventDefault();
      handleRun();
    }
  };

  let parsedType = null;
  try {
    parsedType = parseQuery(query).type;
  } catch {}
  const hintEnabled = parsedType === 'find';
  const docs = results?.documents || results?.results || [];
  const externalToggleSignal = useMemo(
    () => ({ version: globalToggleVersion, open: globalToggleOpen }),
    [globalToggleVersion, globalToggleOpen],
  );
  const scopedHistory = useMemo(
    () => history.filter((entry) => entry.db === db && entry.collection === collection),
    [history, db, collection],
  );
  const autoFillItems = useMemo(() => {
    const items = [
      { label: 'Find by _id', query: `db.${collection}.find({ "_id": { "$oid": "" } })` },
      { label: 'Recent docs', query: `db.${collection}.find({}).sort({ "_id": -1 })` },
      { label: 'Count all', query: `db.${collection}.aggregate([{ "$count": "total" }])` },
    ];
    for (const field of schemaFields.slice(0, 12)) {
      items.push({ label: `${field} equals`, query: `db.${collection}.find({ "${field}": "" })` });
      items.push({ label: `${field} exists`, query: `db.${collection}.find({ "${field}": { "$exists": true } })` });
    }
    return items;
  }, [schemaFields, collection]);
  const escapeRegExp = useCallback((value) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), []);
  const buildQueryAssist = useCallback((value) => {
    const source = (value || '').replace(/\s+$/, '');
    if (!source || source.length < 2) return [];

    const results = [];
    const push = (label, nextValue) => {
      if (!nextValue) return;
      results.push({ label, value: nextValue });
    };

    if (/db\.$/i.test(source)) {
      push(`Use database "${db}"`, `db.${db}.`);
    }

    const dbPrefix = source.match(/db\.([a-zA-Z0-9_-]*)$/i);
    if (dbPrefix) {
      const prefix = (dbPrefix[1] || '').toLowerCase();
      if (db.toLowerCase().startsWith(prefix)) {
        push(`Database "${db}"`, `db.${db}.`);
      }
    }

    const dbPattern = new RegExp(`db\\.${escapeRegExp(db)}\\.([a-zA-Z0-9_.$-]*)$`, 'i');
    const colPrefix = source.match(dbPattern);
    if (colPrefix) {
      const prefix = (colPrefix[1] || '').toLowerCase();
      const candidates = [...new Set([collection, ...schemaFields.map((field) => field.split('.')[0]).filter(Boolean)])];
      candidates
        .filter((name) => name.toLowerCase().startsWith(prefix))
        .slice(0, 8)
        .forEach((name) => {
          push(`Collection "${name}" find`, `db.${db}.${name}.find({})`);
          push(`Collection "${name}" aggregate`, `db.${db}.${name}.aggregate([])`);
        });
    }

    const deduped = [];
    const seen = new Set();
    for (const item of results) {
      const key = `${item.label}:${item.value}`;
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(item);
    }
    return deduped;
  }, [db, collection, schemaFields, escapeRegExp]);
  const queryAssistItems = useMemo(() => buildQueryAssist(query), [buildQueryAssist, query]);
  const visibleQueryAssistItems = queryAssistItems.slice(0, queryAssistLimit);
  const hasMoreQueryAssistItems = queryAssistItems.length > visibleQueryAssistItems.length;
  const applyQueryAssistSuggestion = useCallback((item) => {
    if (!item?.value) return;
    setQuery(item.value);
    setShowQueryAssist(false);
    requestAnimationFrame(() => {
      textareaRef.current?.focus();
    });
  }, []);
  const handleQueryChange = useCallback((event) => {
    const nextValue = event.target.value;
    setQuery(nextValue);

    const trimmed = nextValue.trim();
    if (!trimmed) {
      setShowQueryAssist(false);
      setQueryAssistLimit(6);
      return;
    }

    const nextSuggestions = buildQueryAssist(nextValue);
    if (nextSuggestions.length > 0) {
      setShowQueryAssist(true);
      setQueryAssistLimit(6);
    } else {
      setShowQueryAssist(false);
    }
  }, [buildQueryAssist]);
  const visibleAutoFillItems = autoFillItems.slice(0, autofillLimit);
  const hasMoreAutoFillItems = autoFillItems.length > visibleAutoFillItems.length;
  const queryPlaceholder = useMemo(() => {
    if (autoFillItems.length === 0) return `db.${collection}.find({ })`;
    const idx = placeholderIndex % autoFillItems.length;
    return autoFillItems[idx].query;
  }, [autoFillItems, placeholderIndex, collection]);

  useEffect(() => {
    if (autoFillItems.length <= 1) return undefined;
    const timer = setInterval(() => {
      setPlaceholderIndex((prev) => (prev + 1) % autoFillItems.length);
    }, 3200);
    return () => clearInterval(timer);
  }, [autoFillItems.length]);

  return (
    <div className="h-full flex flex-col">
      <div className="flex-shrink-0" style={{ borderBottom: '1px solid var(--border)' }}>
        <div className="flex items-center gap-1 px-3 py-1.5 whitespace-nowrap min-w-0" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
          <button
            onClick={handleRun}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-all shrink-0 whitespace-nowrap ${running ? 'bg-red-500/10 text-red-400' : 'text-xs'}`}
            style={running ? {} : { background: 'var(--accent)', color: 'var(--surface-0)' }}
          >
            {running ? <><StopCircle className="w-3.5 h-3.5" />Cancel</> : <><Play className="w-3.5 h-3.5" />Run</>}
          </button>

          <button onClick={handleExplain} className="btn-ghost flex items-center gap-1.5 text-xs shrink-0 whitespace-nowrap" title="Explain query">
            <Eye className="w-3.5 h-3.5" />Explain
          </button>

          <div className="relative" ref={templatesRef}>
            <button
              onClick={() => {
                setShowTemplates(!showTemplates);
                setShowHistory(false);
                setShowAutofill(false);
                setShowExportMenu(false);
                setShowQueryAssist(false);
              }}
              className="btn-ghost text-xs flex items-center gap-1 shrink-0 whitespace-nowrap"
            >
              Templates<ChevronDown className="w-3 h-3" />
            </button>
            {showTemplates && (
              <div
                className="absolute left-0 top-full mt-1 z-[80] rounded-lg shadow-xl py-1 animate-fade-in whitespace-normal"
                style={{ background: 'var(--surface-3)', border: '1px solid var(--border)', width: '280px', maxWidth: 'min(92vw, 420px)' }}
              >
                {TEMPLATES.map((t, i) => (
                  <button
                    key={i}
                    onClick={() => { setQuery(t.query.replace('collection', collection)); setShowTemplates(false); }}
                    className="block w-full text-left px-3 py-1.5 text-xs transition-colors"
                    style={{ color: 'var(--text-secondary)' }}
                    onMouseOver={e => e.currentTarget.style.background = 'var(--surface-4)'}
                    onMouseOut={e => e.currentTarget.style.background = 'transparent'}
                  >
                    {t.label}
                  </button>
                ))}
              </div>
            )}
          </div>

          <div className="relative" ref={autofillRef}>
            <button
              onClick={() => {
                setShowAutofill(!showAutofill);
                setShowTemplates(false);
                setShowHistory(false);
                setShowExportMenu(false);
                setShowQueryAssist(false);
                setAutofillLimit(8);
              }}
              className="btn-ghost text-xs flex items-center gap-1 shrink-0 whitespace-nowrap"
            >
              Auto-fill<ChevronDown className="w-3 h-3" />
            </button>
            {showAutofill && (
              <div
                className="absolute left-0 top-full mt-1 z-[80] rounded-lg shadow-xl py-1 max-h-[320px] overflow-y-auto overflow-x-hidden animate-fade-in whitespace-normal"
                style={{ background: 'var(--surface-3)', border: '1px solid var(--border)', width: '320px', maxWidth: 'min(92vw, 460px)' }}
              >
                {visibleAutoFillItems.map((item) => (
                  <button
                    key={`${item.label}:${item.query}`}
                    onClick={() => { setQuery(item.query); setShowAutofill(false); }}
                    className="block w-full text-left px-3 py-1.5 text-xs transition-colors"
                    style={{ color: 'var(--text-secondary)' }}
                    onMouseOver={e => e.currentTarget.style.background = 'var(--surface-4)'}
                    onMouseOut={e => e.currentTarget.style.background = 'transparent'}
                    title={item.query}
                  >
                    <div className="truncate">{item.label}</div>
                    <div className="mt-0.5 text-2xs font-mono truncate" style={{ color: 'var(--text-tertiary)' }}>
                      {item.query}
                    </div>
                  </button>
                ))}
                {hasMoreAutoFillItems && (
                  <>
                    <div style={{ borderTop:'1px solid var(--border)' }} />
                    <button
                      onClick={() => setAutofillLimit((prev) => prev + 8)}
                      className="w-full text-left px-3 py-1.5 text-2xs transition-colors"
                      style={{ color:'var(--accent)' }}
                      onMouseOver={(event) => { event.currentTarget.style.background = 'var(--surface-4)'; }}
                      onMouseOut={(event) => { event.currentTarget.style.background = 'transparent'; }}
                    >
                      More...
                    </button>
                  </>
                )}
              </div>
            )}
          </div>

          <div className="relative" ref={historyRef}>
            <button
              onClick={() => {
                setShowHistory(!showHistory);
                setShowTemplates(false);
                setShowAutofill(false);
                setShowExportMenu(false);
                setShowQueryAssist(false);
              }}
              className="btn-ghost text-xs flex items-center gap-1 shrink-0 whitespace-nowrap"
            >
              <History className="w-3.5 h-3.5" />History
              {scopedHistory.length > 0 && <span className="badge-accent text-2xs">{scopedHistory.length}</span>}
            </button>
            {showHistory && (
              <div
                className="absolute left-0 top-full mt-1 z-[80] rounded-lg shadow-xl py-1 max-h-[320px] overflow-y-auto overflow-x-hidden animate-fade-in whitespace-normal"
                style={{ background: 'var(--surface-3)', border: '1px solid var(--border)', width: '360px', maxWidth: 'min(92vw, 560px)' }}
              >
                {scopedHistory.length === 0 && (
                  <div className="px-3 py-2 text-2xs" style={{ color:'var(--text-tertiary)' }}>
                    No history for {db}.{collection}
                  </div>
                )}
                {scopedHistory.slice(0, 20).map((h, i) => (
                  <button
                    key={i}
                    onClick={() => { setQuery(h.query); setShowHistory(false); }}
                    className="w-full text-left px-3 py-2 text-xs transition-colors"
                    style={{ color: 'var(--text-secondary)', borderBottom: '1px solid var(--border)' }}
                    onMouseOver={e => e.currentTarget.style.background = 'var(--surface-4)'}
                    onMouseOut={e => e.currentTarget.style.background = 'transparent'}
                  >
                    <div className="font-mono text-2xs truncate mb-1" style={{ color: 'var(--text-primary)' }}>{h.query}</div>
                    <div className="flex items-center gap-3 text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                      <span>{h.count} results</span>
                      <span>{formatDuration(h.elapsed)}</span>
                      <span>{new Date(h.ts).toLocaleString()}</span>
                      {h.elapsed > 5000 && <span className="badge-yellow">Slow</span>}
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>

          <div className="flex-1" />

          {hintEnabled && (
            <div className="flex items-center gap-1 shrink-0">
              <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Hint</span>
              <select
                value={selectedHint}
                onChange={e => setSelectedHint(e.target.value)}
                className="ms-select text-2xs py-1"
                title="Apply index hint to this find query"
              >
                <option value="auto">Auto</option>
                {indexHints.map(name => <option key={name} value={name}>{name}</option>)}
              </select>
            </div>
          )}

          <span
            className="hidden lg:inline-flex items-center px-2 py-1 rounded-md text-2xs font-mono whitespace-nowrap shrink-0"
            style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-secondary)' }}
            title={`Context: ${db}.${collection}`}
          >
            {db}.{collection}
          </span>

          {running && (
            <span className="flex items-center gap-1.5 text-2xs animate-pulse shrink-0 whitespace-nowrap" style={{ color: 'var(--accent)' }}>
              <Loader className="w-3 h-3" />{(liveTimer / 1000).toFixed(1)}s
            </span>
          )}

          {elapsed !== null && !running && (
            <div className="flex items-center gap-3 text-2xs shrink-0 whitespace-nowrap" style={{ color: 'var(--text-tertiary)' }}>
              <span className="flex items-center gap-1"><Zap className="w-3 h-3" style={{ color: 'var(--accent)', opacity: 0.6 }} />{formatDuration(elapsed)}</span>
              {resultSize && <span>{(resultSize / 1024).toFixed(1)} KB</span>}
              {slow && <span className="badge-yellow">Slow</span>}
            </div>
          )}

          <span className="hidden 2xl:inline text-2xs shrink-0 whitespace-nowrap" style={{ color: 'var(--text-tertiary)' }}>Ctrl/Cmd+Enter</span>
        </div>

        <div className="relative">
          <textarea
            ref={textareaRef}
            value={query}
            onChange={handleQueryChange}
            onKeyDown={handleKeyDown}
            rows={Math.min(Math.max(query.split('\n').length, 3), 12)}
            spellCheck={false}
            className="w-full resize-none px-4 py-3 font-mono text-sm leading-relaxed focus:outline-none"
            style={{ background: 'var(--surface-0)', color: 'var(--text-primary)', minHeight: '80px' }}
            placeholder={queryPlaceholder}
          />
          {showQueryAssist && visibleQueryAssistItems.length > 0 && (
            <div
              ref={queryAssistRef}
              className="absolute left-3 right-3 top-full mt-1 z-[70] rounded-lg shadow-xl py-1 max-h-56 overflow-y-auto overflow-x-hidden animate-fade-in"
              style={{ background: 'var(--surface-3)', border: '1px solid var(--border)' }}
            >
              {visibleQueryAssistItems.map((item) => (
                <button
                  key={`${item.label}:${item.value}`}
                  type="button"
                  onClick={() => applyQueryAssistSuggestion(item)}
                  className="block w-full px-3 py-1.5 text-left transition-colors"
                  onMouseOver={(event) => { event.currentTarget.style.background = 'var(--surface-4)'; }}
                  onMouseOut={(event) => { event.currentTarget.style.background = 'transparent'; }}
                >
                  <div className="text-xs truncate" style={{ color: 'var(--text-secondary)' }}>{item.label}</div>
                  <div className="text-2xs font-mono truncate mt-0.5" style={{ color: 'var(--text-tertiary)' }}>{item.value}</div>
                </button>
              ))}
              {hasMoreQueryAssistItems && (
                <>
                  <div style={{ borderTop: '1px solid var(--border)' }} />
                  <button
                    type="button"
                    onClick={() => setQueryAssistLimit((prev) => prev + 6)}
                    className="block w-full text-left px-3 py-1.5 text-2xs transition-colors"
                    style={{ color: 'var(--accent)' }}
                    onMouseOver={(event) => { event.currentTarget.style.background = 'var(--surface-4)'; }}
                    onMouseOut={(event) => { event.currentTarget.style.background = 'transparent'; }}
                  >
                    More...
                  </button>
                </>
              )}
            </div>
          )}
        </div>
      </div>

      {showExplain && explainResult && (
        <div className="flex-shrink-0 p-3 animate-slide-up" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-semibold flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
              <Eye className="w-3.5 h-3.5" style={{ color: 'var(--accent)' }} />Query Plan
            </span>
            <button onClick={() => setShowExplain(false)} className="p-1"><X className="w-3 h-3" style={{ color: 'var(--text-tertiary)' }} /></button>
          </div>
          <div className="grid grid-cols-4 gap-2">
            {[
              { label: 'Docs Examined', value: formatNumber(explainResult.summary?.totalDocsExamined || 0), warn: explainResult.summary?.totalDocsExamined > 10000 },
              { label: 'Keys Examined', value: formatNumber(explainResult.summary?.totalKeysExamined || 0) },
              { label: 'Returned', value: formatNumber(explainResult.summary?.nReturned || 0) },
              { label: 'Time', value: formatDuration(explainResult.summary?.executionTimeMs || 0) },
            ].map(({ label, value, warn }) => (
              <div key={label} className="p-2 rounded-lg" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
                <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>{label}</div>
                <div className={`text-sm font-mono font-medium ${warn ? 'text-amber-400' : ''}`} style={warn ? {} : { color: 'var(--text-primary)' }}>{value}</div>
              </div>
            ))}
          </div>
          <div className="mt-2 flex items-center gap-2">
            {explainResult.summary?.isCollScan ? (
              <span className="badge-yellow flex items-center gap-1"><AlertTriangle className="w-3 h-3" />Collection scan warning: no index used</span>
            ) : explainResult.summary?.indexUsed ? (
              <span className="badge-green flex items-center gap-1"><Check className="w-3 h-3" />Index: {explainResult.summary.indexUsed}</span>
            ) : null}
          </div>
        </div>
      )}

      {error && (
        <div className="mx-4 mt-3 flex items-start gap-2 text-red-400 text-xs p-3 rounded-lg animate-fade-in" style={{ background: 'rgba(239,68,68,0.05)', border: '1px solid rgba(239,68,68,0.2)' }}>
          <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
          <span className="flex-1 font-mono">{error}</span>
          <button onClick={() => setError(null)}><X className="w-3.5 h-3.5 text-red-400/50" /></button>
        </div>
      )}

      <div className="flex-1 overflow-auto">
        {docs.length > 0 ? (
          <>
            <div className="px-4 py-2 flex items-center justify-between" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
              <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>
                {formatNumber(docs.length)} result{docs.length !== 1 ? 's' : ''}
                {results?.trimmed && <span className="badge-yellow ml-2">Trimmed</span>}
              </span>
              <div className="flex items-center gap-1.5">
                <button onClick={() => handleToggleAllDocs(true)} className="btn-ghost text-2xs px-2 py-1">
                  Expand all
                </button>
                <button onClick={() => handleToggleAllDocs(false)} className="btn-ghost text-2xs px-2 py-1">
                  Collapse all
                </button>
                <div className="relative" ref={exportMenuRef}>
                  <button onClick={() => setShowExportMenu((prev) => !prev)} className="btn-ghost flex items-center gap-1 text-xs">
                    <Download className="w-3 h-3" />Export
                  </button>
                  {showExportMenu && (
                    <div
                      className="absolute right-0 top-full mt-1 z-[80] rounded-lg shadow-xl py-1 min-w-[132px] animate-fade-in"
                      style={{ background: 'var(--surface-3)', border: '1px solid var(--border)' }}
                    >
                      <button
                        onClick={() => handleExportResults('json')}
                        className="block w-full text-left px-3 py-1.5 text-xs transition-colors"
                        style={{ color: 'var(--text-secondary)' }}
                        onMouseOver={e => e.currentTarget.style.background = 'var(--surface-4)'}
                        onMouseOut={e => e.currentTarget.style.background = 'transparent'}
                      >
                        Export JSON
                      </button>
                      <button
                        onClick={() => handleExportResults('csv')}
                        className="block w-full text-left px-3 py-1.5 text-xs transition-colors"
                        style={{ color: 'var(--text-secondary)' }}
                        onMouseOver={e => e.currentTarget.style.background = 'var(--surface-4)'}
                        onMouseOut={e => e.currentTarget.style.background = 'transparent'}
                      >
                        Export CSV
                      </button>
                    </div>
                  )}
                </div>
                <button onClick={handleCopy} className="btn-ghost flex items-center gap-1 text-xs">
                  {copied ? <><Check className="w-3 h-3" style={{ color: 'var(--accent)' }} />Copied</> : <><Copy className="w-3 h-3" />Copy</>}
                </button>
              </div>
            </div>
            <div className="p-4 space-y-2">
              {docs.map((doc, i) => (
                <div key={i} className="rounded-xl p-4 overflow-auto" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
                  <JsonView
                    data={doc}
                    showControls
                    externalToggle={externalToggleSignal}
                  />
                </div>
              ))}
            </div>
          </>
        ) : results && docs.length === 0 ? (
          <div className="flex items-center justify-center h-full" style={{ color: 'var(--text-tertiary)' }}>
            <div className="text-center"><div className="text-sm">No results</div><div className="text-2xs mt-1">Query returned 0 documents</div></div>
          </div>
        ) : !running && (
          <div className="flex items-center justify-center h-full" style={{ color: 'var(--text-tertiary)' }}>
            <div className="text-center"><Play className="w-8 h-8 mx-auto mb-2 opacity-20" /><div className="text-sm">Run a query to see results</div></div>
          </div>
        )}
      </div>
    </div>
  );
}

import React, { useState, useCallback, useRef, useEffect } from 'react';
import api from '../utils/api';
import { Play, Copy, AlertCircle, Check, Zap, X, History, StopCircle, Eye, AlertTriangle, Loader, ChevronDown } from './Icons';
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
    const h = getHistory().filter(e => e.query !== entry.query);
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

  const controllerRef = useRef(null);
  const timerRef = useRef(null);
  const textareaRef = useRef(null);
  const templatesRef = useRef(null);
  const historyRef = useRef(null);

  useEffect(() => { setHistory(getHistory()); }, []);
  useEffect(() => {
    setQuery(`db.${collection}.find({})`);
    setResults(null);
    setError(null);
    setExplainResult(null);
    setSelectedHint('auto');
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
    if (selectedHint === 'auto') return;
    if (!indexHints.includes(selectedHint)) setSelectedHint('auto');
  }, [indexHints, selectedHint]);

  useEffect(() => {
    const onMouseDown = (event) => {
      if (showTemplates && templatesRef.current && !templatesRef.current.contains(event.target)) setShowTemplates(false);
      if (showHistory && historyRef.current && !historyRef.current.contains(event.target)) setShowHistory(false);
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        setShowTemplates(false);
        setShowHistory(false);
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [showTemplates, showHistory]);

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

      saveHistory({ query, ts: Date.now(), elapsed: ms, count: data.total || data.documents?.length || 0, type: parsed.type });
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

  const handleKeyDown = (event) => {
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

  return (
    <div className="h-full flex flex-col">
      <div className="flex-shrink-0" style={{ borderBottom: '1px solid var(--border)' }}>
        <div className="flex items-center gap-1 px-3 py-1.5" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
          <button
            onClick={handleRun}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${running ? 'bg-red-500/10 text-red-400' : 'text-xs'}`}
            style={running ? {} : { background: 'var(--accent)', color: 'var(--surface-0)' }}
          >
            {running ? <><StopCircle className="w-3.5 h-3.5" />Cancel</> : <><Play className="w-3.5 h-3.5" />Run</>}
          </button>

          <button onClick={handleExplain} className="btn-ghost flex items-center gap-1.5 text-xs" title="Explain query">
            <Eye className="w-3.5 h-3.5" />Explain
          </button>

          <div className="relative" ref={templatesRef}>
            <button onClick={() => { setShowTemplates(!showTemplates); setShowHistory(false); }} className="btn-ghost text-xs flex items-center gap-1">
              Templates<ChevronDown className="w-3 h-3" />
            </button>
            {showTemplates && (
              <div className="absolute left-0 top-full mt-1 z-50 rounded-lg shadow-xl py-1 min-w-[220px] animate-fade-in" style={{ background: 'var(--surface-3)', border: '1px solid var(--border)' }}>
                {TEMPLATES.map((t, i) => (
                  <button
                    key={i}
                    onClick={() => { setQuery(t.query.replace('collection', collection)); setShowTemplates(false); }}
                    className="w-full text-left px-3 py-1.5 text-xs transition-colors"
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

          <div className="relative" ref={historyRef}>
            <button onClick={() => { setShowHistory(!showHistory); setShowTemplates(false); }} className="btn-ghost text-xs flex items-center gap-1">
              <History className="w-3.5 h-3.5" />History
              {history.length > 0 && <span className="badge-accent text-2xs">{history.length}</span>}
            </button>
            {showHistory && history.length > 0 && (
              <div className="absolute left-0 top-full mt-1 z-50 rounded-lg shadow-xl py-1 min-w-[300px] max-h-[300px] overflow-auto animate-fade-in" style={{ background: 'var(--surface-3)', border: '1px solid var(--border)' }}>
                {history.slice(0, 20).map((h, i) => (
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
            <div className="flex items-center gap-1">
              <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Hint</span>
              <select
                value={selectedHint}
                onChange={e => setSelectedHint(e.target.value)}
                className="text-2xs px-2 py-1 rounded-md"
                style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                title="Apply index hint to this find query"
              >
                <option value="auto">Auto</option>
                {indexHints.map(name => <option key={name} value={name}>{name}</option>)}
              </select>
            </div>
          )}

          <span
            className="hidden md:inline-flex items-center px-2 py-1 rounded-md text-2xs font-mono"
            style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-secondary)' }}
            title={`Context: ${db}.${collection}`}
          >
            {db}.{collection}
          </span>

          {running && (
            <span className="flex items-center gap-1.5 text-2xs animate-pulse" style={{ color: 'var(--accent)' }}>
              <Loader className="w-3 h-3" />{(liveTimer / 1000).toFixed(1)}s
            </span>
          )}

          {elapsed !== null && !running && (
            <div className="flex items-center gap-3 text-2xs" style={{ color: 'var(--text-tertiary)' }}>
              <span className="flex items-center gap-1"><Zap className="w-3 h-3" style={{ color: 'var(--accent)', opacity: 0.6 }} />{formatDuration(elapsed)}</span>
              {resultSize && <span>{(resultSize / 1024).toFixed(1)} KB</span>}
              {slow && <span className="badge-yellow">Slow</span>}
            </div>
          )}

          <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Ctrl/Cmd+Enter</span>
        </div>

        <div className="relative">
          <textarea
            ref={textareaRef}
            value={query}
            onChange={e => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            rows={Math.min(Math.max(query.split('\n').length, 3), 12)}
            spellCheck={false}
            className="w-full resize-none px-4 py-3 font-mono text-sm leading-relaxed focus:outline-none"
            style={{ background: 'var(--surface-0)', color: 'var(--text-primary)', minHeight: '80px' }}
            placeholder={`db.${collection}.find({ })`}
          />
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
              <button onClick={handleCopy} className="btn-ghost flex items-center gap-1 text-xs">
                {copied ? <><Check className="w-3 h-3" style={{ color: 'var(--accent)' }} />Copied</> : <><Copy className="w-3 h-3" />Copy</>}
              </button>
            </div>
            <div className="p-4 space-y-2">
              {docs.map((doc, i) => (
                <div key={i} className="rounded-xl p-4 overflow-auto" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
                  <JsonView data={doc} />
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

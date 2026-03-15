import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import api from '../utils/api';
import { Activity, Refresh, Loader, Search, Download } from './Icons';
import InlineAlert from './InlineAlert';
import DropdownSelect from './DropdownSelect';

const RANGE_OPTIONS = [
  { value: '15m', label: '15m', ms: 15 * 60 * 1000 },
  { value: '1h', label: '1h', ms: 60 * 60 * 1000 },
  { value: '6h', label: '6h', ms: 6 * 60 * 60 * 1000 },
  { value: '24h', label: '24h', ms: 24 * 60 * 60 * 1000 },
  { value: 'all', label: 'All', ms: 0 },
];
const SEARCH_DEBOUNCE_MS = 300;

function formatTimestamp(ts) {
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return String(ts);
  }
}

function summarizeEntry(entry) {
  const parts = [];
  if (entry.user) parts.push(`user=${entry.user}`);
  if (entry.source) parts.push(`source=${entry.source}`);
  if (entry.method) parts.push(`method=${entry.method}`);
  if (entry.scope) parts.push(`scope=${entry.scope}`);
  if (entry.db) parts.push(`db=${entry.db}`);
  if (entry.col) parts.push(`col=${entry.col}`);
  if (entry.count !== undefined) parts.push(`count=${entry.count}`);
  if (entry.docs !== undefined) parts.push(`docs=${entry.docs}`);
  if (entry.collections !== undefined) parts.push(`collections=${entry.collections}`);
  if (entry.mode) parts.push(`mode=${entry.mode}`);
  if (entry.elapsed !== undefined) parts.push(`elapsed=${entry.elapsed}ms`);
  return parts.join(' | ');
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

function csvEscape(value) {
  const raw = value === undefined || value === null ? '' : String(value);
  if (/[",\n]/.test(raw)) return `"${raw.replace(/"/g, '""')}"`;
  return raw;
}

export default function AuditView({ refreshToken = 0 }) {
  const [entries, setEntries] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [action, setAction] = useState('');
  const [source, setSource] = useState('');
  const [method, setMethod] = useState('');
  const [scope, setScope] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [search, setSearch] = useState('');
  const [range, setRange] = useState('1h');
  const [limit, setLimit] = useState(200);
  const [customLimitInput, setCustomLimitInput] = useState('');
  const loadAbortRef = useRef(null);
  const loadSeqRef = useRef(0);

  const fromTs = useMemo(() => {
    const selected = RANGE_OPTIONS.find((opt) => opt.value === range);
    if (!selected || selected.ms === 0) return null;
    return Date.now() - selected.ms;
  }, [range]);

  useEffect(() => {
    const timer = setTimeout(() => {
      setSearch(searchInput);
    }, SEARCH_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [searchInput]);

  const load = useCallback(async () => {
    const seq = loadSeqRef.current + 1;
    loadSeqRef.current = seq;
    loadAbortRef.current?.abort();
    const controller = new AbortController();
    loadAbortRef.current = controller;
    setLoading(true);
    setError(null);
    try {
      const data = await api.getAuditLog({
        action,
        source,
        method,
        scope,
        search,
        from: fromTs,
        limit,
      }, { controller });
      if (loadSeqRef.current !== seq) return;
      setEntries(data.entries || []);
      setTotal(data.total || 0);
    } catch (err) {
      if (err?.name === 'AbortError') return;
      if (loadSeqRef.current !== seq) return;
      setError(err.message);
    } finally {
      if (loadSeqRef.current !== seq) return;
      setLoading(false);
      if (loadAbortRef.current === controller) loadAbortRef.current = null;
    }
  }, [action, source, method, scope, search, fromTs, limit]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    if (refreshToken > 0) load();
  }, [refreshToken, load]);
  useEffect(() => () => {
    loadAbortRef.current?.abort();
    loadAbortRef.current = null;
  }, []);

  const actions = useMemo(() => {
    const set = new Set(entries.map((entry) => entry.action).filter(Boolean));
    return Array.from(set).sort();
  }, [entries]);
  const sources = useMemo(() => {
    const set = new Set(entries.map((entry) => entry.source).filter(Boolean));
    return Array.from(set).sort();
  }, [entries]);
  const methods = useMemo(() => {
    const set = new Set(entries.map((entry) => entry.method).filter(Boolean));
    return Array.from(set).sort();
  }, [entries]);
  const scopes = useMemo(() => {
    const set = new Set(entries.map((entry) => entry.scope).filter(Boolean));
    return Array.from(set).sort();
  }, [entries]);
  const actionOptions = useMemo(
    () => [{ value: '', label: 'All actions' }, ...actions.map((name) => ({ value: name, label: name }))],
    [actions],
  );
  const sourceOptions = useMemo(
    () => {
      const values = source && !sources.includes(source) ? [...sources, source] : sources;
      return [{ value: '', label: 'All sources' }, ...values.map((name) => ({ value: name, label: name }))];
    },
    [sources, source],
  );
  const methodOptions = useMemo(
    () => {
      const values = method && !methods.includes(method) ? [...methods, method] : methods;
      return [{ value: '', label: 'All methods' }, ...values.map((name) => ({ value: name, label: name }))];
    },
    [methods, method],
  );
  const scopeOptions = useMemo(
    () => {
      const extras = scopes.filter((name) => !['collection', 'database', 'global'].includes(name));
      if (scope && !['collection', 'database', 'global'].includes(scope) && !extras.includes(scope)) extras.push(scope);
      return [
        { value: '', label: 'All scopes' },
        { value: 'collection', label: 'collection' },
        { value: 'database', label: 'database' },
        { value: 'global', label: 'global' },
        ...extras.map((name) => ({ value: name, label: name })),
      ];
    },
    [scopes, scope],
  );
  const rangeOptions = useMemo(
    () => RANGE_OPTIONS.map((opt) => ({ value: opt.value, label: opt.label })),
    [],
  );
  const limitCustomEditing = ![100, 200, 500].includes(limit) || customLimitInput !== '';
  const limitOptions = useMemo(
    () => [
      { value: '100', label: '100' },
      { value: '200', label: '200' },
      { value: '500', label: '500' },
      { value: 'custom', label: 'Custom' },
    ],
    [],
  );

  const exportJson = () => {
    const payload = {
      exportedAt: new Date().toISOString(),
      filters: { action, source, method, scope, search, range, limit },
      total,
      entries,
    };
    downloadText(`mongostudio-audit-${Date.now()}.json`, JSON.stringify(payload, null, 2), 'application/json');
  };

  const exportCsv = () => {
    const header = ['ts', 'action', 'user', 'source', 'method', 'scope', 'db', 'col', 'count', 'details'];
    const rows = entries.map((entry) => {
      const details = summarizeEntry(entry);
      return [
        formatTimestamp(entry.ts),
        entry.action || '',
        entry.user || '',
        entry.source || '',
        entry.method || '',
        entry.scope || '',
        entry.db || '',
        entry.col || '',
        entry.count ?? '',
        details,
      ].map(csvEscape).join(',');
    });
    downloadText(`mongostudio-audit-${Date.now()}.csv`, [header.join(','), ...rows].join('\n'), 'text/csv;charset=utf-8');
  };

  return (
    <div className="h-full flex flex-col">
      <div
        className="flex-shrink-0 px-4 py-2.5 flex items-center gap-2 overflow-x-auto whitespace-nowrap"
        style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}
      >
        <h3 className="text-sm font-semibold flex items-center gap-2 shrink-0 pr-1" style={{ color: 'var(--text-primary)' }}>
          <Activity className="w-4 h-4" style={{ color: 'var(--accent)' }} />
          Audit
          <span className="badge-blue">{total}</span>
        </h3>
        <div className="w-px h-5 shrink-0" style={{ background: 'var(--border)' }} />
        <div className="relative w-64 max-w-[40vw] shrink-0">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3" style={{ color: 'var(--text-tertiary)' }} />
          <input
            type="text"
            value={searchInput}
            onChange={(event) => setSearchInput(event.target.value)}
            placeholder="Search audit..."
            className="w-full pl-7 pr-2 py-1.5 text-xs rounded-lg"
            style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
          />
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <DropdownSelect
            value={action}
            options={actionOptions}
            onChange={(next) => setAction(String(next))}
            fullWidth
            sizeClassName="text-xs"
            title="Filter by action"
            className="w-[120px]"
          />
          <DropdownSelect
            value={source}
            options={sourceOptions}
            onChange={(next) => setSource(String(next))}
            fullWidth
            sizeClassName="text-xs"
            title="Filter by source"
            className="w-[120px]"
          />
          <DropdownSelect
            value={method}
            options={methodOptions}
            onChange={(next) => setMethod(String(next))}
            fullWidth
            sizeClassName="text-xs"
            title="Filter by method"
            className="w-[120px]"
          />
          <DropdownSelect
            value={scope}
            options={scopeOptions}
            onChange={(next) => setScope(String(next))}
            fullWidth
            sizeClassName="text-xs"
            title="Filter by scope"
            className="w-[120px]"
          />
          <DropdownSelect
            value={range}
            options={rangeOptions}
            onChange={(next) => setRange(String(next))}
            fullWidth
            sizeClassName="text-xs"
            title="Time range"
            className="w-[72px]"
          />
          <DropdownSelect
            value={limitCustomEditing ? 'custom' : String(limit)}
            options={limitOptions}
            onChange={(next) => {
              if (next === 'custom') {
                setCustomLimitInput(String(limit));
                return;
              }
              setCustomLimitInput('');
              setLimit(parseInt(String(next), 10));
            }}
            fullWidth
            sizeClassName="text-xs"
            title="Max audit rows"
            className="w-[84px]"
          />
          {limitCustomEditing && (
            <>
              <input
                type="number"
                min={10}
                max={50000}
                step={10}
                value={customLimitInput !== '' ? customLimitInput : String(Math.max(10, Math.round(limit || 100)))}
                onChange={(event) => setCustomLimitInput(event.target.value)}
                className="ms-number w-24 text-xs px-2 py-1.5 rounded-lg font-mono"
                style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                placeholder="limit"
              />
              <button
                type="button"
                className="btn-ghost text-xs px-2 py-1.5"
                onClick={() => {
                  const parsed = Number(customLimitInput);
                  if (!Number.isFinite(parsed)) return;
                  const nextLimit = Math.max(10, Math.min(Math.round(parsed), 50000));
                  setLimit(nextLimit);
                  setCustomLimitInput(String(nextLimit));
                }}
              >
                Set
              </button>
            </>
          )}
          <button onClick={load} className="btn-ghost p-1.5" title="Refresh">
            <Refresh className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
          <button onClick={exportJson} className="btn-ghost text-xs inline-flex items-center gap-1" title="Export JSON" disabled={entries.length === 0}>
            <Download className="w-3.5 h-3.5" />
            JSON
          </button>
          <button onClick={exportCsv} className="btn-ghost text-xs inline-flex items-center gap-1" title="Export CSV" disabled={entries.length === 0}>
            <Download className="w-3.5 h-3.5" />
            CSV
          </button>
        </div>
      </div>

      {error && (
        <InlineAlert kind="error" message={error} onClose={() => setError(null)} className="mx-4 mt-3" />
      )}

      <div className="flex-1 overflow-auto">
        {loading && entries.length === 0 ? (
          <div className="h-full flex items-center justify-center">
            <Loader style={{ color: 'var(--accent)' }} />
          </div>
        ) : entries.length === 0 ? (
          <div className="h-full flex items-center justify-center text-xs" style={{ color: 'var(--text-tertiary)' }}>
            No audit entries for selected filters
          </div>
        ) : (
          <div>
            {entries.map((entry, idx) => {
              const summary = summarizeEntry(entry);
              return (
                <div key={`${entry.ts}-${idx}`} className="px-4 py-2.5 flex items-start gap-3" style={{ borderBottom: '1px solid var(--border)' }}>
                  <div className="min-w-[130px] text-2xs font-mono pt-0.5" style={{ color: 'var(--text-tertiary)' }}>
                    {formatTimestamp(entry.ts)}
                  </div>
                  <span className="badge-accent mt-0.5">{entry.action || 'event'}</span>
                  {entry.source && <span className="badge-blue mt-0.5">{entry.source}</span>}
                  {entry.method && <span className="badge-purple mt-0.5 font-mono">{entry.method}</span>}
                  {entry.scope && <span className="badge-blue mt-0.5">{entry.scope}</span>}
                  <span className="badge-blue mt-0.5">@{entry.user || 'anonymous'}</span>
                  <div className="flex-1 min-w-0">
                    <div className="text-xs truncate" style={{ color: 'var(--text-primary)' }}>
                      {summary || 'No details'}
                    </div>
                    {entry.host && (
                      <div className="text-2xs font-mono mt-0.5 truncate" style={{ color: 'var(--text-tertiary)' }}>
                        {entry.host}
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}


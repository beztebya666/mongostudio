import React, { useCallback, useEffect, useMemo, useState } from 'react';
import api from '../utils/api';
import { Activity, Refresh, Loader, AlertCircle, Search } from './Icons';

const RANGE_OPTIONS = [
  { value: '15m', label: '15m', ms: 15 * 60 * 1000 },
  { value: '1h', label: '1h', ms: 60 * 60 * 1000 },
  { value: '6h', label: '6h', ms: 6 * 60 * 60 * 1000 },
  { value: '24h', label: '24h', ms: 24 * 60 * 60 * 1000 },
  { value: 'all', label: 'All', ms: 0 },
];

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
  if (entry.db) parts.push(`db=${entry.db}`);
  if (entry.col) parts.push(`col=${entry.col}`);
  if (entry.count !== undefined) parts.push(`count=${entry.count}`);
  if (entry.docs !== undefined) parts.push(`docs=${entry.docs}`);
  if (entry.collections !== undefined) parts.push(`collections=${entry.collections}`);
  if (entry.mode) parts.push(`mode=${entry.mode}`);
  if (entry.elapsed !== undefined) parts.push(`elapsed=${entry.elapsed}ms`);
  return parts.join(' · ');
}

export default function AuditView({ refreshToken = 0 }) {
  const [entries, setEntries] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [action, setAction] = useState('');
  const [search, setSearch] = useState('');
  const [range, setRange] = useState('1h');
  const [limit, setLimit] = useState(200);

  const fromTs = useMemo(() => {
    const selected = RANGE_OPTIONS.find((opt) => opt.value === range);
    if (!selected || selected.ms === 0) return null;
    return Date.now() - selected.ms;
  }, [range]);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await api.getAuditLog({
        action,
        search,
        from: fromTs,
        limit,
      });
      setEntries(data.entries || []);
      setTotal(data.total || 0);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [action, search, fromTs, limit]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    if (refreshToken > 0) load();
  }, [refreshToken, load]);

  const actions = useMemo(() => {
    const set = new Set(entries.map((entry) => entry.action).filter(Boolean));
    return Array.from(set).sort();
  }, [entries]);

  return (
    <div className="h-full flex flex-col">
      <div className="flex-shrink-0 px-4 py-2.5 flex items-center gap-2" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
        <h3 className="text-sm font-semibold flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
          <Activity className="w-4 h-4" style={{ color: 'var(--accent)' }} />
          Audit
          <span className="badge-blue">{total}</span>
        </h3>
        <div className="w-px h-5 ml-1" style={{ background: 'var(--border)' }} />
        <div className="relative w-56 max-w-[40vw]">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3" style={{ color: 'var(--text-tertiary)' }} />
          <input
            type="text"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search audit…"
            className="w-full pl-7 pr-2 py-1.5 text-xs rounded-lg"
            style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
          />
        </div>
        <select
          value={action}
          onChange={(event) => setAction(event.target.value)}
          className="ms-select text-xs"
        >
          <option value="">All actions</option>
          {actions.map((name) => <option key={name} value={name}>{name}</option>)}
        </select>
        <select
          value={range}
          onChange={(event) => setRange(event.target.value)}
          className="ms-select text-xs"
        >
          {RANGE_OPTIONS.map((opt) => <option key={opt.value} value={opt.value}>{opt.label}</option>)}
        </select>
        <select
          value={limit}
          onChange={(event) => setLimit(parseInt(event.target.value, 10))}
          className="ms-select text-xs"
        >
          <option value="100">100</option>
          <option value="200">200</option>
          <option value="500">500</option>
        </select>
        <button onClick={load} className="btn-ghost p-1.5" title="Refresh">
          <Refresh className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
        </button>
      </div>

      {error && (
        <div className="mx-4 mt-3 flex items-start gap-2 text-red-400 text-xs p-3 rounded-lg" style={{ background: 'rgba(239,68,68,0.06)', border: '1px solid rgba(239,68,68,0.25)' }}>
          <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
          <span className="flex-1">{error}</span>
        </div>
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

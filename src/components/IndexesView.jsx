import React, { useState, useCallback, useEffect, useMemo } from 'react';
import api from '../utils/api';
import { Key, Plus, Trash, Refresh, Loader, X, Check, ArrowUp, ArrowDown } from './Icons';
import InlineAlert from './InlineAlert';
import DropdownSelect from './DropdownSelect';
import { formatBytes, formatNumber } from '../utils/formatters';

const DEFAULT_QUERY_TIMEOUT_MS = 5000;
const DEFAULT_QUERY_LIMIT = 50;
const SAFE_QUERY_TIMEOUT_MAX_MS = 30000;
const POWER_QUERY_TIMEOUT_PRESET_MAX_MS = 120000;
const POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS = 2147483000;
const POWER_QUERY_TIMEOUT_MAX_MS = POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS;
const QUERY_TIMEOUT_MAX_MS = POWER_QUERY_TIMEOUT_MAX_MS;
const SAFE_QUERY_LIMIT_MAX = 1000;
const POWER_QUERY_LIMIT_PRESET_MAX = 5000;
const QUERY_LIMIT_OVERRIDE_MAX = 50000;
const POWER_QUERY_LIMIT_MAX = 2147483000;
const QUERY_TIMEOUT_OPTIONS = [5000, 10000, 30000, 60000, POWER_QUERY_TIMEOUT_PRESET_MAX_MS];
const QUERY_LIMIT_MAX = POWER_QUERY_LIMIT_MAX;
const BASE_QUERY_LIMIT_OPTIONS = [50, 100, 200, 500, 1000, POWER_QUERY_LIMIT_PRESET_MAX];

const getModeTimeoutMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_TIMEOUT_MAX_MS : SAFE_QUERY_TIMEOUT_MAX_MS);
const clampTimeoutMs = (value, mode = 'safe') => Math.max(DEFAULT_QUERY_TIMEOUT_MS, Math.min(Number(value) || DEFAULT_QUERY_TIMEOUT_MS, getModeTimeoutMax(mode)));
const getModeLimitMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_LIMIT_MAX : QUERY_LIMIT_OVERRIDE_MAX);
const clampLimitValue = (value, mode = 'safe') => Math.max(DEFAULT_QUERY_LIMIT, Math.min(Number(value) || DEFAULT_QUERY_LIMIT, getModeLimitMax(mode)));

const formatTimeoutOptionLabel = (value) => `${Math.round(value / 1000)}s`;
const formatLimitOptionLabel = (value) => (value >= POWER_QUERY_LIMIT_MAX ? 'Unlimited' : formatNumber(value));
const parseIndexDirectionValue = (raw) => {
  const text = String(raw || '').trim();
  if (text === '1' || text === '-1') return Number(text);
  return text || 1;
};

function parseOptionalJsonInput(raw, fieldName) {
  const text = String(raw || '').trim();
  if (!text) return undefined;
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new Error(`${fieldName}: invalid JSON (${err.message})`);
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`${fieldName}: must be a JSON object.`);
  }
  return parsed;
}

export default function IndexesView({ db, collection, onQueryMs, refreshToken = 0 }) {
  const [indexes, setIndexes] = useState([]);
  const [totalIndexes, setTotalIndexes] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showCreate, setShowCreate] = useState(false);
  const [newKeys, setNewKeys] = useState([{ field: '', direction: 1 }]);
  const [indexOptions, setIndexOptions] = useState({ unique: false, sparse: false, background: true, hidden: false });
  const [indexName, setIndexName] = useState('');
  const [indexExpireAfterSeconds, setIndexExpireAfterSeconds] = useState('');
  const [indexPartialFilter, setIndexPartialFilter] = useState('');
  const [indexCollation, setIndexCollation] = useState('');
  const [creating, setCreating] = useState(false);
  const [confirmDrop, setConfirmDrop] = useState(null);
  const [sortState, setSortState] = useState({ key: 'name', dir: 'asc' });
  const [queryTimeoutMs, setQueryTimeoutMs] = useState(DEFAULT_QUERY_TIMEOUT_MS);
  const [queryLimit, setQueryLimit] = useState(DEFAULT_QUERY_LIMIT);
  const [customTimeoutInput, setCustomTimeoutInput] = useState('');
  const [customLimitInput, setCustomLimitInput] = useState('');
  const [execConfig, setExecConfig] = useState(null);
  const executionMode = execConfig?.mode === 'power' ? 'power' : 'safe';
  const queryLimitOptions = useMemo(
    () => [...BASE_QUERY_LIMIT_OPTIONS, POWER_QUERY_LIMIT_MAX],
    [],
  );
  const timeoutSelectOptions = useMemo(
    () => [
      ...QUERY_TIMEOUT_OPTIONS.map((value) => ({
        value: String(value),
        label: formatTimeoutOptionLabel(value),
        disabled: executionMode !== 'power' && value > SAFE_QUERY_TIMEOUT_MAX_MS,
      })),
      { value: 'custom', label: 'Custom' },
    ],
    [executionMode],
  );
  const limitSelectOptions = useMemo(
    () => [
      ...queryLimitOptions.map((value) => ({
        value: value >= QUERY_LIMIT_MAX ? 'unlimited' : String(value),
        label: formatLimitOptionLabel(value),
        disabled: executionMode !== 'power' && value > SAFE_QUERY_LIMIT_MAX,
      })),
      { value: 'custom', label: 'Custom' },
    ],
    [executionMode, queryLimitOptions],
  );
  const timeoutIsCustom = !QUERY_TIMEOUT_OPTIONS.includes(queryTimeoutMs);
  const limitIsCustom = !queryLimitOptions.includes(queryLimit);
  const defaultBudget = useMemo(() => ({
    timeoutMs: clampTimeoutMs(execConfig?.maxTimeMS, executionMode),
    limit: clampLimitValue(execConfig?.maxResultSize, executionMode),
  }), [execConfig, executionMode]);

  useEffect(() => {
    setQueryTimeoutMs(clampTimeoutMs(defaultBudget.timeoutMs, executionMode));
    setCustomTimeoutInput('');
  }, [defaultBudget.timeoutMs, executionMode]);
  useEffect(() => {
    setQueryLimit(defaultBudget.limit);
    setCustomLimitInput('');
  }, [defaultBudget.limit]);

  useEffect(() => {
    let active = true;
    api.getExecutionConfig()
      .then((config) => {
        if (active) setExecConfig(config || null);
      })
      .catch(() => {
        if (active) setExecConfig(null);
      });
    return () => { active = false; };
  }, [db, collection, refreshToken]);

  useEffect(() => {
    const handler = (event) => {
      if (event?.detail) setExecConfig(event.detail);
    };
    if (typeof window !== 'undefined') {
      window.addEventListener('mongostudio:exec-config', handler);
    }
    return () => {
      if (typeof window !== 'undefined') {
        window.removeEventListener('mongostudio:exec-config', handler);
      }
    };
  }, []);

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const data = await api.getIndexes(db, collection, {
        budget: { timeoutMs: clampTimeoutMs(queryTimeoutMs, executionMode), limit: clampLimitValue(queryLimit, executionMode) },
      });
      const listed = Array.isArray(data.indexes) ? data.indexes : [];
      setIndexes(listed);
      setTotalIndexes(Number(data.total || listed.length || 0));
      onQueryMs?.(data._elapsed);
    } catch (err) {
      setError(err.message);
      setIndexes([]);
      setTotalIndexes(0);
    }
    finally { setLoading(false); }
  }, [db, collection, onQueryMs, queryTimeoutMs, queryLimit, executionMode]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { if (refreshToken > 0) load(); }, [refreshToken]);

  const resetCreateForm = useCallback(() => {
    setShowCreate(false);
    setNewKeys([{ field: '', direction: 1 }]);
    setIndexOptions({ unique: false, sparse: false, background: true, hidden: false });
    setIndexName('');
    setIndexExpireAfterSeconds('');
    setIndexPartialFilter('');
    setIndexCollation('');
  }, []);

  const handleCreate = async () => {
    const keys = {};
    for (const k of newKeys) {
      if (!k.field.trim()) continue;
      keys[k.field.trim()] = k.direction;
    }
    if (!Object.keys(keys).length) { setError('Add at least one field'); return; }
    setCreating(true); setError(null);
    try {
      const opts = {};
      if (String(indexName || '').trim()) opts.name = String(indexName).trim();
      if (indexOptions.unique) opts.unique = true;
      if (indexOptions.sparse) opts.sparse = true;
      if (indexOptions.background) opts.background = true;
      if (indexOptions.hidden) opts.hidden = true;
      const ttl = Number(indexExpireAfterSeconds);
      if (String(indexExpireAfterSeconds || '').trim()) {
        if (!Number.isFinite(ttl) || ttl < 0) throw new Error('expireAfterSeconds must be a non-negative number.');
        opts.expireAfterSeconds = Math.floor(ttl);
      }
      const partialFilterExpression = parseOptionalJsonInput(indexPartialFilter, 'partialFilterExpression');
      if (partialFilterExpression) opts.partialFilterExpression = partialFilterExpression;
      const collation = parseOptionalJsonInput(indexCollation, 'collation');
      if (collation) opts.collation = collation;
      await api.createIndex(db, collection, keys, opts);
      resetCreateForm();
      load();
    } catch (err) { setError(err.message); }
    finally { setCreating(false); }
  };

  const handleDrop = async (name) => {
    try { await api.dropIndex(db, collection, name); setConfirmDrop(null); load(); }
    catch (err) { setError(err.message); }
  };

  const addKeyField = () => setNewKeys([...newKeys, { field: '', direction: 1 }]);
  const updateKeyField = (i, updates) => { const n = [...newKeys]; n[i] = { ...n[i], ...updates }; setNewKeys(n); };
  const removeKeyField = (i) => setNewKeys(newKeys.filter((_, j) => j !== i));

  const toggleSort = (key) => {
    setSortState((prev) => {
      if (prev.key === key) return { key, dir: prev.dir === 'asc' ? 'desc' : 'asc' };
      return { key, dir: key === 'size' ? 'desc' : 'asc' };
    });
  };

  const sortedIndexes = useMemo(() => {
    const rows = [...indexes];
    const dir = sortState.dir === 'asc' ? 1 : -1;
    rows.sort((a, b) => {
      if (sortState.key === 'name') return String(a.name || '').localeCompare(String(b.name || '')) * dir;
      if (sortState.key === 'keys') {
        const ak = Object.keys(a.key || {}).join(',');
        const bk = Object.keys(b.key || {}).join(',');
        return ak.localeCompare(bk) * dir;
      }
      if (sortState.key === 'props') {
        const ap = Number(Boolean(a.unique)) + Number(Boolean(a.sparse)) + Number(Boolean(a.v));
        const bp = Number(Boolean(b.unique)) + Number(Boolean(b.sparse)) + Number(Boolean(b.v));
        return (ap - bp) * dir;
      }
      if (sortState.key === 'size') return ((a.size || 0) - (b.size || 0)) * dir;
      return 0;
    });
    return rows;
  }, [indexes, sortState]);

  const sortIcon = (key) => {
    if (sortState.key !== key) return null;
    return sortState.dir === 'asc'
      ? <ArrowUp className="w-3 h-3" style={{ color:'var(--accent)' }} />
      : <ArrowDown className="w-3 h-3" style={{ color:'var(--accent)' }} />;
  };

  return (
    <div className="h-full flex flex-col">
      {/* Toolbar */}
      <div className="flex-shrink-0 px-4 py-2.5 flex items-center justify-between" style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
        <h3 className="text-sm font-semibold flex items-center gap-2" style={{color:'var(--text-primary)'}}>
          <Key className="w-4 h-4" style={{color:'var(--accent)'}} />
          Indexes
          <span className="badge-blue">{formatNumber(indexes.length)}</span>
          <span className="hidden md:inline text-2xs font-normal" style={{ color:'var(--text-tertiary)' }}>
            showing {formatNumber(indexes.length)} of {formatNumber(totalIndexes)}
          </span>
        </h3>
        <div className="flex items-center gap-2">
          <DropdownSelect
            value={timeoutIsCustom || customTimeoutInput !== '' ? 'custom' : String(queryTimeoutMs)}
            options={timeoutSelectOptions}
            onChange={(nextValue) => {
              const value = String(nextValue);
              if (value === 'custom') {
                setCustomTimeoutInput(String(Math.max(5, Math.round(queryTimeoutMs / 1000))));
                return;
              }
              setCustomTimeoutInput('');
              setQueryTimeoutMs(clampTimeoutMs(value, executionMode));
            }}
            sizeClassName="text-xs"
            title="Indexes query timeout"
          />
          {(timeoutIsCustom || customTimeoutInput !== '') && (
            <>
              <input
                type="number"
                min={5}
                max={Math.round(getModeTimeoutMax(executionMode) / 1000)}
                step={1}
                value={customTimeoutInput}
                onChange={(event) => setCustomTimeoutInput(event.target.value)}
                className="ms-number w-24 px-2 py-1 rounded-md text-2xs font-mono"
                style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                placeholder="seconds"
              />
              <button
                type="button"
                className="btn-ghost text-2xs px-2 py-1"
                onClick={() => {
                  const parsed = Number(customTimeoutInput);
                  if (!Number.isFinite(parsed)) return;
                  setQueryTimeoutMs(clampTimeoutMs(Math.round(parsed * 1000), executionMode));
                }}
              >
                Set
              </button>
            </>
          )}
          <DropdownSelect
            value={limitIsCustom || customLimitInput !== '' ? 'custom' : (queryLimit >= QUERY_LIMIT_MAX ? 'unlimited' : String(queryLimit))}
            options={limitSelectOptions}
            onChange={(nextValue) => {
              const value = String(nextValue);
              if (value === 'custom') {
                setCustomLimitInput(String(Math.max(50, Math.round(queryLimit))));
                return;
              }
              setCustomLimitInput('');
              setQueryLimit(value === 'unlimited' ? QUERY_LIMIT_MAX : clampLimitValue(value, executionMode));
            }}
            sizeClassName="text-xs"
            title="Indexes to show"
          />
          {(limitIsCustom || customLimitInput !== '') && (
            <>
              <input
                type="number"
                min={50}
                max={QUERY_LIMIT_MAX}
                step={1}
                value={customLimitInput}
                onChange={(event) => setCustomLimitInput(event.target.value)}
                className="ms-number w-24 px-2 py-1 rounded-md text-2xs font-mono"
                style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                placeholder="indexes"
              />
              <button
                type="button"
                className="btn-ghost text-2xs px-2 py-1"
                onClick={() => {
                  const parsed = Number(customLimitInput);
                  if (!Number.isFinite(parsed)) return;
                  setQueryLimit(clampLimitValue(Math.round(parsed), executionMode));
                }}
              >
                Set
              </button>
            </>
          )}
          <button
            onClick={() => {
              if (showCreate) resetCreateForm();
              else setShowCreate(true);
            }}
            className="btn-ghost flex items-center gap-1 text-xs"
            style={{color:'var(--accent)'}}
          >
            <Plus className="w-3.5 h-3.5" />Create Index
          </button>
          <button onClick={load} className="btn-ghost p-1.5"><Refresh className={`w-3.5 h-3.5 ${loading?'animate-spin':''}`} /></button>
        </div>
      </div>

      {/* Create index form */}
      {showCreate && (
        <div className="p-4 animate-slide-up" style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
          <div className="space-y-2">
            {newKeys.map((k, i) => (
              <div key={i} className="flex items-center gap-2">
                <input type="text" value={k.field} onChange={e=>updateKeyField(i,{field:e.target.value})}
                  placeholder="field name" className="flex-1 text-xs font-mono px-3 py-1.5 rounded-lg"
                  style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}} />
                <select value={String(k.direction)} onChange={e=>updateKeyField(i,{direction:parseIndexDirectionValue(e.target.value)})}
                  className="ms-select text-xs">
                  <option value={1}>Asc (1)</option>
                  <option value={-1}>Desc (-1)</option>
                  <option value="text">Text</option>
                  <option value="2dsphere">2dsphere</option>
                  <option value="hashed">Hashed</option>
                  <option value="2d">2d</option>
                  <option value="geoHaystack">geoHaystack</option>
                </select>
                {newKeys.length > 1 && (
                  <button onClick={()=>removeKeyField(i)} className="p-1 text-red-400"><X className="w-3 h-3" /></button>
                )}
              </div>
            ))}
          </div>
          <div className="mt-2 grid grid-cols-1 md:grid-cols-2 gap-2">
            <input
              type="text"
              value={indexName}
              onChange={(e) => setIndexName(e.target.value)}
              placeholder="name (optional)"
              className="text-xs font-mono px-3 py-1.5 rounded-lg"
              style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-primary)' }}
            />
            <input
              type="number"
              min={0}
              step={1}
              value={indexExpireAfterSeconds}
              onChange={(e) => setIndexExpireAfterSeconds(e.target.value)}
              placeholder="expireAfterSeconds (optional)"
              className="text-xs font-mono px-3 py-1.5 rounded-lg"
              style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-primary)' }}
            />
            <input
              type="text"
              value={indexPartialFilter}
              onChange={(e) => setIndexPartialFilter(e.target.value)}
              placeholder='partialFilterExpression JSON, e.g. { "status": "active" }'
              className="text-xs font-mono px-3 py-1.5 rounded-lg md:col-span-2"
              style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-primary)' }}
            />
            <input
              type="text"
              value={indexCollation}
              onChange={(e) => setIndexCollation(e.target.value)}
              placeholder='collation JSON, e.g. { "locale": "en", "strength": 2 }'
              className="text-xs font-mono px-3 py-1.5 rounded-lg md:col-span-2"
              style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-primary)' }}
            />
          </div>
          <div className="mt-2 flex items-center gap-4 flex-wrap">
            <button onClick={addKeyField} className="text-2xs" style={{color:'var(--accent)'}}>+ Add field</button>
            <label className="flex items-center gap-1.5 text-2xs cursor-pointer" style={{color:'var(--text-secondary)'}}>
              <input type="checkbox" className="ms-checkbox" checked={indexOptions.unique} onChange={e=>setIndexOptions({...indexOptions,unique:e.target.checked})} /> Unique
            </label>
            <label className="flex items-center gap-1.5 text-2xs cursor-pointer" style={{color:'var(--text-secondary)'}}>
              <input type="checkbox" className="ms-checkbox" checked={indexOptions.sparse} onChange={e=>setIndexOptions({...indexOptions,sparse:e.target.checked})} /> Sparse
            </label>
            <label className="flex items-center gap-1.5 text-2xs cursor-pointer" style={{color:'var(--text-secondary)'}}>
              <input type="checkbox" className="ms-checkbox" checked={indexOptions.background} onChange={e=>setIndexOptions({...indexOptions,background:e.target.checked})} /> Background
            </label>
            <label className="flex items-center gap-1.5 text-2xs cursor-pointer" style={{color:'var(--text-secondary)'}}>
              <input type="checkbox" className="ms-checkbox" checked={indexOptions.hidden} onChange={e=>setIndexOptions({...indexOptions,hidden:e.target.checked})} /> Hidden
            </label>
          </div>
          <div className="mt-3 flex items-center gap-2">
            <button onClick={handleCreate} disabled={creating} className="btn-primary text-xs flex items-center gap-1.5">
              {creating ? <Loader className="w-3 h-3" /> : <Check className="w-3 h-3" />}Create
            </button>
            <button onClick={resetCreateForm} className="btn-ghost text-xs">Cancel</button>
          </div>
        </div>
      )}

      {error && (
        <InlineAlert kind="error" message={error} onClose={() => setError(null)} className="mx-4 mt-3" />
      )}

      {/* Index list */}
      <div className="flex-1 overflow-auto">
        {loading && indexes.length === 0 ? (
          <div className="flex items-center justify-center h-full"><Loader style={{color:'var(--accent)'}} /></div>
        ) : indexes.length === 0 ? (
          <div className="flex items-center justify-center h-full text-center" style={{color:'var(--text-tertiary)'}}>
            <div><Key className="w-8 h-8 mx-auto mb-2 opacity-20" /><p className="text-sm">No indexes</p></div>
          </div>
        ) : (
          <div>
            {/* Header */}
            <div className="grid grid-cols-12 gap-2 px-4 py-2 text-2xs font-medium uppercase tracking-wider" style={{color:'var(--text-tertiary)',borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
              <button className="col-span-3 flex items-center gap-1 text-left px-1 py-0.5 rounded-md transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]" onClick={() => toggleSort('name')}>
                Name {sortIcon('name')}
              </button>
              <button className="col-span-4 flex items-center gap-1 text-left px-1 py-0.5 rounded-md transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]" onClick={() => toggleSort('keys')}>
                Keys {sortIcon('keys')}
              </button>
              <button className="col-span-2 flex items-center gap-1 text-left px-1 py-0.5 rounded-md transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]" onClick={() => toggleSort('props')}>
                Properties {sortIcon('props')}
              </button>
              <button className="col-span-2 flex items-center gap-1 text-left px-1 py-0.5 rounded-md transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]" onClick={() => toggleSort('size')}>
                Size {sortIcon('size')}
              </button>
              <div className="col-span-1"></div>
            </div>
            {sortedIndexes.map((idx) => (
              <div key={idx.name} className="grid grid-cols-12 gap-2 px-4 py-2.5 items-center transition-colors group"
                style={{borderBottom:'1px solid var(--border)'}}
                onMouseOver={e=>e.currentTarget.style.background='var(--surface-1)'}
                onMouseOut={e=>e.currentTarget.style.background='transparent'}>
                <div className="col-span-3 text-xs font-mono font-medium truncate" style={{color:'var(--text-primary)'}}>
                  {idx.name}
                </div>
                <div className="col-span-4 flex flex-wrap gap-1">
                  {Object.entries(idx.key || {}).map(([field, dir]) => (
                    <span key={field} className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-2xs font-mono"
                      style={{background:'var(--surface-3)',color:'var(--text-secondary)',border:'1px solid var(--border)'}}>
                      {field}
                      {dir === 1 ? <ArrowUp className="w-2.5 h-2.5" /> : dir === -1 ? <ArrowDown className="w-2.5 h-2.5" /> : <span className="text-2xs opacity-60">{dir}</span>}
                    </span>
                  ))}
                </div>
                <div className="col-span-2 flex flex-wrap gap-1">
                  {idx.unique && <span className="badge-purple">unique</span>}
                  {idx.sparse && <span className="badge-yellow">sparse</span>}
                  {idx.hidden && <span className="badge-blue">hidden</span>}
                  {Number.isFinite(Number(idx.expireAfterSeconds)) && <span className="badge-accent">ttl:{Number(idx.expireAfterSeconds)}s</span>}
                  {idx.v && <span className="text-2xs" style={{color:'var(--text-tertiary)'}}>v{idx.v}</span>}
                </div>
                <div className="col-span-2 text-2xs font-mono" style={{color:'var(--text-tertiary)'}}>
                  {idx.size ? formatBytes(idx.size) : '—'}
                </div>
                <div className="col-span-1 text-right">
                  {idx.name !== '_id_' && (
                    confirmDrop === idx.name ? (
                      <div className="flex items-center gap-1 justify-end animate-fade-in">
                        <button onClick={()=>handleDrop(idx.name)} className="text-2xs px-1.5 py-0.5 bg-red-500/10 text-red-400 rounded">Drop</button>
                        <button onClick={()=>setConfirmDrop(null)} className="text-2xs" style={{color:'var(--text-tertiary)'}}>No</button>
                      </div>
                    ) : (
                      <button onClick={()=>setConfirmDrop(idx.name)} className="opacity-0 group-hover:opacity-100 p-1 rounded-md hover:text-red-400 transition-all" style={{color:'var(--text-tertiary)'}}>
                        <Trash className="w-3 h-3" />
                      </button>
                    )
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

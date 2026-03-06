import React, { useState, useCallback, useEffect, useMemo } from 'react';
import api from '../utils/api';
import { Key, Plus, Trash, Refresh, Loader, AlertCircle, X, Check, ArrowUp, ArrowDown } from './Icons';
import { formatBytes, formatNumber } from '../utils/formatters';

export default function IndexesView({ db, collection, onQueryMs, refreshToken = 0 }) {
  const [indexes, setIndexes] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showCreate, setShowCreate] = useState(false);
  const [newKeys, setNewKeys] = useState([{ field: '', direction: 1 }]);
  const [indexOptions, setIndexOptions] = useState({ unique: false, sparse: false, background: true });
  const [creating, setCreating] = useState(false);
  const [confirmDrop, setConfirmDrop] = useState(null);
  const [sortState, setSortState] = useState({ key: 'name', dir: 'asc' });

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const data = await api.getIndexes(db, collection);
      setIndexes(data.indexes || []);
      onQueryMs?.(data._elapsed);
    } catch (err) { setError(err.message); }
    finally { setLoading(false); }
  }, [db, collection, onQueryMs]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { if (refreshToken > 0) load(); }, [refreshToken]);

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
      if (indexOptions.unique) opts.unique = true;
      if (indexOptions.sparse) opts.sparse = true;
      if (indexOptions.background) opts.background = true;
      await api.createIndex(db, collection, keys, opts);
      setShowCreate(false); setNewKeys([{ field: '', direction: 1 }]); load();
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
          <span className="badge-blue">{indexes.length}</span>
        </h3>
        <div className="flex items-center gap-2">
          <button onClick={()=>setShowCreate(!showCreate)} className="btn-ghost flex items-center gap-1 text-xs" style={{color:'var(--accent)'}}>
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
                <select value={k.direction} onChange={e=>updateKeyField(i,{direction:parseInt(e.target.value)})}
                  className="ms-select text-xs">
                  <option value={1}>Asc (1)</option>
                  <option value={-1}>Desc (-1)</option>
                  <option value="text">Text</option>
                  <option value="2dsphere">2dsphere</option>
                  <option value="hashed">Hashed</option>
                </select>
                {newKeys.length > 1 && (
                  <button onClick={()=>removeKeyField(i)} className="p-1 text-red-400"><X className="w-3 h-3" /></button>
                )}
              </div>
            ))}
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
          </div>
          <div className="mt-3 flex items-center gap-2">
            <button onClick={handleCreate} disabled={creating} className="btn-primary text-xs flex items-center gap-1.5">
              {creating ? <Loader className="w-3 h-3" /> : <Check className="w-3 h-3" />}Create
            </button>
            <button onClick={()=>setShowCreate(false)} className="btn-ghost text-xs">Cancel</button>
          </div>
        </div>
      )}

      {error && (
        <div className="mx-4 mt-3 flex items-start gap-2 text-red-400 text-xs p-3 rounded-lg" style={{background:'rgba(239,68,68,0.05)',border:'1px solid rgba(239,68,68,0.2)'}}>
          <AlertCircle className="w-3.5 h-3.5 mt-0.5" /><span className="flex-1">{error}</span>
          <button onClick={()=>setError(null)}><X className="w-3 h-3 text-red-400/50" /></button>
        </div>
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

import React, { useState, useCallback, useEffect, useRef } from 'react';
import api from '../utils/api';
import { Database, ChevronRight, ChevronDown, Collection, Refresh, Search, Plus, Loader, Trash, X, MoreVertical, AlertCircle } from './Icons';
import { formatBytes } from '../utils/formatters';
import InputDialog from './modals/InputDialog';

function ContextMenu({ items, onClose }) {
  const ref = useRef(null);
  useEffect(() => {
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) onClose(); };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [onClose]);

  return (
    <div ref={ref} className="absolute right-0 top-full mt-1 z-50 rounded-lg shadow-lg py-1 min-w-[140px] animate-fade-in"
      style={{background:'var(--surface-3)',border:'1px solid var(--border)'}}>
      {items.map((item, i) => (
        <button key={i} onClick={()=>{item.action();onClose();}}
          className={`w-full text-left px-3 py-1.5 text-xs transition-colors ${item.danger?'text-red-400 hover:bg-red-500/10':'hover:bg-[var(--surface-4)]'}`}
          style={{color:item.danger?undefined:'var(--text-secondary)'}}>
          {item.label}
        </button>
      ))}
    </div>
  );
}

function DbItem({ db, selectedDb, selectedCol, onSelect, onRefresh, refreshToken, onNotify }) {
  const [expanded, setExpanded] = useState(false);
  const [collections, setCollections] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [showMenu, setShowMenu] = useState(false);
  const [showColMenu, setShowColMenu] = useState(null);
  const [confirmDrop, setConfirmDrop] = useState(null); // 'db' | colName
  const [showCreateColDialog, setShowCreateColDialog] = useState(false);

  const toggle = useCallback(async () => {
    if (!expanded && !loaded) {
      setLoading(true);
      try { const d=await api.listCollections(db.name); setCollections(d.collections||[]); setLoaded(true); }
      catch (err) { console.error(err); }
      finally { setLoading(false); }
    }
    setExpanded(!expanded);
  }, [expanded, loaded, db.name]);

  useEffect(() => { if (selectedDb===db.name && !expanded) toggle(); }, [selectedDb]);

  const refreshCollections = async () => {
    setLoading(true);
    try { const d=await api.listCollections(db.name); setCollections(d.collections||[]); setLoaded(true); }
    catch(err) { console.error(err); }
    finally { setLoading(false); }
  };
  useEffect(() => {
    if (!loaded) return;
    refreshCollections();
  }, [refreshToken]);

  const handleDropDb = async () => {
    try { await api.dropDatabase(db.name); setConfirmDrop(null); onRefresh(); }
    catch(err) { onNotify?.(err.message); }
  };

  const handleCreateCol = () => {
    setShowCreateColDialog(true);
  };

  const handleCreateColSubmit = async (name) => {
    if (!name) return;
    try {
      await api.createCollection(db.name, name);
      setShowCreateColDialog(false);
      refreshCollections();
      onRefresh?.();
    } catch (err) {
      onNotify?.(err.message);
    }
  };

  const handleDropCol = async (colName) => {
    try { await api.dropCollection(db.name, colName); setConfirmDrop(null); refreshCollections(); onRefresh?.(); }
    catch(err) { onNotify?.(err.message); }
  };

  return (
    <div className="animate-slide-up">
      <div className="group relative flex items-center">
        <button onClick={toggle}
          className="flex-1 flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-all duration-100 hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]"
          style={{
            background: selectedDb===db.name&&!selectedCol ? 'var(--surface-3)' : 'transparent',
            color: selectedDb===db.name&&!selectedCol ? 'var(--text-primary)' : 'var(--text-secondary)',
          }}>
          {loading ? <Loader className="w-3.5 h-3.5" style={{color:'var(--accent)'}} />
            : expanded ? <ChevronDown className="w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} />
            : <ChevronRight className="w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} />}
          <Database className="w-3.5 h-3.5" style={{color:'var(--accent)',opacity:0.7}} />
          <span className="truncate flex-1 text-left font-medium">{db.name}</span>
          <span className="text-2xs opacity-0 group-hover:opacity-100 transition-opacity" style={{color:'var(--text-tertiary)'}}>{formatBytes(db.sizeOnDisk||0)}</span>
        </button>
        <div className="relative">
          <button onClick={()=>setShowMenu(!showMenu)} className="opacity-0 group-hover:opacity-100 p-1 rounded-md transition-all" style={{color:'var(--text-tertiary)'}}>
            <MoreVertical className="w-3.5 h-3.5" />
          </button>
          {showMenu && (
            <ContextMenu onClose={()=>setShowMenu(false)} items={[
              { label:'New Collection', action:handleCreateCol },
              { label:'Refresh', action:refreshCollections },
              { label:'Drop Database', action:()=>setConfirmDrop('db'), danger:true },
            ]} />
          )}
        </div>
      </div>

      {confirmDrop === 'db' && (
        <div className="ml-8 my-1 flex items-center gap-2 animate-fade-in">
          <span className="text-xs text-red-400">Drop "{db.name}"?</span>
          <button onClick={handleDropDb} className="text-2xs px-2 py-0.5 bg-red-500/10 text-red-400 rounded">Yes</button>
          <button onClick={()=>setConfirmDrop(null)} className="text-2xs" style={{color:'var(--text-tertiary)'}}>No</button>
        </div>
      )}

      {expanded && collections.length > 0 && (
        <div className="ml-3 mt-0.5 space-y-0.5 pl-3" style={{borderLeft:'1px solid var(--border)'}}>
          {collections.map((col) => (
            <div key={col.name} className="group/col relative flex items-center">
              <button onClick={()=>onSelect(db.name, col.name)}
                className="flex-1 flex items-center gap-2 px-2.5 py-1.5 rounded-lg text-xs transition-all duration-100 hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]"
                style={{
                  background: selectedDb===db.name&&selectedCol===col.name ? 'rgba(0, 237, 100, 0.12)' : 'transparent',
                  color: selectedDb===db.name&&selectedCol===col.name ? 'var(--accent)' : 'var(--text-secondary)',
                  border: selectedDb===db.name&&selectedCol===col.name ? '1px solid rgba(0, 237, 100, 0.28)' : '1px solid transparent',
                }}>
                <Collection className="w-3 h-3 flex-shrink-0" style={{color:'var(--text-tertiary)'}} />
                <span className="truncate text-left flex-1">{col.name}</span>
              </button>
              <div className="relative">
                <button onClick={()=>setShowColMenu(showColMenu===col.name?null:col.name)}
                  className="opacity-0 group-hover/col:opacity-100 p-0.5 rounded transition-all" style={{color:'var(--text-tertiary)'}}>
                  <MoreVertical className="w-3 h-3" />
                </button>
                {showColMenu === col.name && (
                  <ContextMenu onClose={()=>setShowColMenu(null)} items={[
                    { label:'Drop Collection', action:()=>setConfirmDrop(col.name), danger:true },
                  ]} />
                )}
              </div>
              {confirmDrop === col.name && (
                <div className="absolute inset-0 flex items-center justify-center rounded-lg animate-fade-in" style={{background:'var(--surface-2)',border:'1px solid var(--border)'}}>
                  <span className="text-2xs text-red-400 mr-2">Drop?</span>
                  <button onClick={()=>handleDropCol(col.name)} className="text-2xs px-1.5 py-0.5 bg-red-500/10 text-red-400 rounded mr-1">Yes</button>
                  <button onClick={()=>setConfirmDrop(null)} className="text-2xs" style={{color:'var(--text-tertiary)'}}>No</button>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
      {expanded && loaded && collections.length === 0 && (
        <div className="ml-9 py-1 text-2xs italic flex items-center gap-2" style={{color:'var(--text-tertiary)'}}>
          No collections
          <button onClick={handleCreateCol} className="text-2xs" style={{color:'var(--accent)'}}>Create</button>
        </div>
      )}
      <InputDialog
        open={showCreateColDialog}
        title={`Create Collection in "${db.name}"`}
        label="Collection Name"
        placeholder="new_collection"
        submitLabel="Create"
        onCancel={() => setShowCreateColDialog(false)}
        onSubmit={handleCreateColSubmit}
      />
    </div>
  );
}

export default function Sidebar({ databases, selectedDb, selectedCol, onSelect, onRefresh, width, onWidthChange, loading, refreshToken = 0 }) {
  const [filter, setFilter] = useState('');
  const [showCreateDb, setShowCreateDb] = useState(false);
  const [newDbName, setNewDbName] = useState('');
  const [error, setError] = useState('');
  const resizing = useRef(false);
  const createDbRef = useRef(null);

  const filteredDbs = databases.filter(db => db.name.toLowerCase().includes(filter.toLowerCase()));

  const handleMouseDown = useCallback((e) => {
    e.preventDefault();
    resizing.current = true;
    const startX = e.clientX, startW = width;
    const handleMove = (e) => { if (!resizing.current) return; onWidthChange(Math.max(200, Math.min(450, startW + (e.clientX - startX)))); };
    const handleUp = () => { resizing.current = false; document.removeEventListener('mousemove', handleMove); document.removeEventListener('mouseup', handleUp); };
    document.addEventListener('mousemove', handleMove);
    document.addEventListener('mouseup', handleUp);
  }, [width, onWidthChange]);

  const handleCreateDb = async () => {
    if (!newDbName.trim()) return;
    try { await api.createDatabase(newDbName.trim()); setNewDbName(''); setShowCreateDb(false); onRefresh(); }
    catch(err) { setError(err.message); }
  };

  useEffect(() => {
    if (!showCreateDb) return undefined;
    const onMouseDown = (event) => {
      if (createDbRef.current && !createDbRef.current.contains(event.target)) {
        setShowCreateDb(false);
      }
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') setShowCreateDb(false);
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [showCreateDb]);

  return (
    <aside className="flex-shrink-0 flex flex-col relative" style={{width:`${width}px`,borderRight:'1px solid var(--border)',background:'var(--surface-1)'}}>
      {/* Search + Create */}
      <div className="p-2" style={{borderBottom:'1px solid var(--border)'}} ref={createDbRef}>
        <div className="flex items-center gap-1">
          <div className="relative flex-1">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} />
            <input type="text" value={filter} onChange={e=>setFilter(e.target.value)} placeholder="Filter databases…"
              className="w-full rounded-lg pl-8 pr-3 py-1.5 text-xs transition-all"
              style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}} />
          </div>
          <button onClick={()=>setShowCreateDb(!showCreateDb)} className="btn-ghost p-1.5" title="Create Database" style={{color:'var(--accent)'}}>
            <Plus className="w-3.5 h-3.5" />
          </button>
        </div>
        {showCreateDb && (
          <div className="mt-2 flex items-center gap-1 animate-slide-up">
            <input type="text" value={newDbName} onChange={e=>setNewDbName(e.target.value)} onKeyDown={e=>e.key==='Enter'&&handleCreateDb()}
              placeholder="Database name…" autoFocus
              className="flex-1 rounded-lg px-2.5 py-1.5 text-xs font-mono"
              style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}} />
            <button onClick={handleCreateDb} disabled={!newDbName.trim()} className="text-2xs px-2 py-1.5 rounded-lg font-medium disabled:opacity-30" style={{color:'var(--accent)'}}>Create</button>
            <button onClick={()=>{setShowCreateDb(false);setNewDbName('')}} className="p-1" style={{color:'var(--text-tertiary)'}}><X className="w-3 h-3" /></button>
          </div>
        )}
        {error && (
          <div className="mt-2 flex items-start gap-2 text-red-400 text-2xs p-2 rounded-lg" style={{background:'rgba(239,68,68,0.08)',border:'1px solid rgba(239,68,68,0.2)'}}>
            <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
            <span className="flex-1">{error}</span>
            <button onClick={() => setError('')}><X className="w-3 h-3 text-red-300/70" /></button>
          </div>
        )}
      </div>

      {/* Database List */}
      <div className="flex-1 overflow-y-auto p-2 space-y-0.5">
        {loading && databases.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-8 gap-2">
            <Loader style={{color:'var(--accent)'}} />
            <span className="text-xs" style={{color:'var(--text-tertiary)'}}>Loading databases…</span>
          </div>
        ) : filteredDbs.length === 0 ? (
          <div className="text-center py-8 text-xs" style={{color:'var(--text-tertiary)'}}>
            {filter ? 'No matches' : 'No databases found'}
          </div>
        ) : filteredDbs.map((db) => (
          <DbItem key={db.name} db={db} selectedDb={selectedDb} selectedCol={selectedCol} onSelect={onSelect} onRefresh={onRefresh} refreshToken={refreshToken} onNotify={setError} />
        ))}
      </div>

      {/* Footer */}
      <div className="p-2 flex items-center justify-between" style={{borderTop:'1px solid var(--border)'}}>
        <span className="text-2xs" style={{color:'var(--text-tertiary)'}}>{databases.length} database{databases.length!==1?'s':''}</span>
        <button onClick={onRefresh} className="btn-ghost p-1" title="Refresh">
          <Refresh className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
        </button>
      </div>

      {/* Resize Handle */}
      <div className="absolute right-0 top-0 bottom-0 w-1 cursor-col-resize z-10 transition-colors"
        style={{background:'transparent'}}
        onMouseDown={handleMouseDown}
        onMouseOver={e=>e.currentTarget.style.background='var(--accent)33'}
        onMouseOut={e=>e.currentTarget.style.background='transparent'} />
    </aside>
  );
}

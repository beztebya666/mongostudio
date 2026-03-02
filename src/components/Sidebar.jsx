import React, { useState, useCallback, useEffect, useRef } from 'react';
import api from '../utils/api';
import { Database, ChevronRight, ChevronDown, Collection, Refresh, Search, Plus, Loader, Trash } from './Icons';
import { formatBytes, formatNumber } from '../utils/formatters';

function DbItem({ db, selectedDb, selectedCol, onSelect, onRefresh }) {
  const [expanded, setExpanded] = useState(false);
  const [collections, setCollections] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);

  const toggle = useCallback(async () => {
    if (!expanded && !loaded) {
      setLoading(true);
      try {
        const data = await api.listCollections(db.name);
        setCollections(data.collections || []);
        setLoaded(true);
      } catch (err) {
        console.error(err);
      } finally {
        setLoading(false);
      }
    }
    setExpanded(!expanded);
  }, [expanded, loaded, db.name]);

  useEffect(() => {
    if (selectedDb === db.name && !expanded) {
      toggle();
    }
  }, [selectedDb]);

  return (
    <div className="animate-slide-up">
      <button
        onClick={toggle}
        className={`w-full flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-all duration-100 group
          ${selectedDb === db.name && !selectedCol ? 'bg-surface-3 text-text-primary' : 'text-text-secondary hover:text-text-primary hover:bg-surface-2'}`}
      >
        {loading ? (
          <Loader className="w-3.5 h-3.5 text-accent" />
        ) : expanded ? (
          <ChevronDown className="w-3.5 h-3.5 text-text-tertiary" />
        ) : (
          <ChevronRight className="w-3.5 h-3.5 text-text-tertiary" />
        )}
        <Database className="w-3.5 h-3.5 text-accent/70" />
        <span className="truncate flex-1 text-left font-medium">{db.name}</span>
        <span className="text-2xs text-text-tertiary opacity-0 group-hover:opacity-100 transition-opacity">
          {formatBytes(db.sizeOnDisk || 0)}
        </span>
      </button>

      {expanded && collections.length > 0 && (
        <div className="ml-3 mt-0.5 space-y-0.5 border-l border-border pl-3">
          {collections.map((col) => (
            <button
              key={col.name}
              onClick={() => onSelect(db.name, col.name)}
              className={`w-full flex items-center gap-2 px-2.5 py-1.5 rounded-lg text-xs transition-all duration-100 group
                ${selectedDb === db.name && selectedCol === col.name
                  ? 'bg-accent/10 text-accent border border-accent/20'
                  : 'text-text-secondary hover:text-text-primary hover:bg-surface-2'}`}
            >
              <Collection className="w-3 h-3 flex-shrink-0 text-text-tertiary" />
              <span className="truncate text-left flex-1">{col.name}</span>
            </button>
          ))}
        </div>
      )}
      {expanded && loaded && collections.length === 0 && (
        <div className="ml-9 py-1 text-2xs text-text-tertiary italic">No collections</div>
      )}
    </div>
  );
}

export default function Sidebar({ databases, selectedDb, selectedCol, onSelect, onRefresh, width, onWidthChange, loading }) {
  const [filter, setFilter] = useState('');
  const resizing = useRef(false);

  const filteredDbs = databases.filter(db =>
    db.name.toLowerCase().includes(filter.toLowerCase())
  );

  const handleMouseDown = useCallback((e) => {
    e.preventDefault();
    resizing.current = true;
    const startX = e.clientX;
    const startW = width;

    const handleMove = (e) => {
      if (!resizing.current) return;
      const newW = Math.max(200, Math.min(450, startW + (e.clientX - startX)));
      onWidthChange(newW);
    };

    const handleUp = () => {
      resizing.current = false;
      document.removeEventListener('mousemove', handleMove);
      document.removeEventListener('mouseup', handleUp);
    };

    document.addEventListener('mousemove', handleMove);
    document.addEventListener('mouseup', handleUp);
  }, [width, onWidthChange]);

  return (
    <aside
      className="flex-shrink-0 border-r border-border bg-surface-1/30 flex flex-col relative"
      style={{ width: `${width}px` }}
    >
      {/* Search */}
      <div className="p-2 border-b border-border">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-text-tertiary" />
          <input
            type="text"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter databases…"
            className="w-full bg-surface-2 border border-border rounded-lg pl-8 pr-3 py-1.5 text-xs
                       text-text-primary placeholder-text-tertiary
                       focus:border-accent/30 focus:ring-1 focus:ring-accent/10 transition-all"
          />
        </div>
      </div>

      {/* Database List */}
      <div className="flex-1 overflow-y-auto p-2 space-y-0.5">
        {loading && databases.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-8 gap-2">
            <Loader className="text-accent" />
            <span className="text-xs text-text-tertiary">Loading databases…</span>
          </div>
        ) : filteredDbs.length === 0 ? (
          <div className="text-center py-8 text-xs text-text-tertiary">
            {filter ? 'No matches' : 'No databases found'}
          </div>
        ) : (
          filteredDbs.map((db) => (
            <DbItem
              key={db.name}
              db={db}
              selectedDb={selectedDb}
              selectedCol={selectedCol}
              onSelect={onSelect}
              onRefresh={onRefresh}
            />
          ))
        )}
      </div>

      {/* Footer */}
      <div className="p-2 border-t border-border flex items-center justify-between">
        <span className="text-2xs text-text-tertiary">
          {databases.length} database{databases.length !== 1 ? 's' : ''}
        </span>
        <button onClick={onRefresh} className="btn-ghost p-1" title="Refresh">
          <Refresh className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
        </button>
      </div>

      {/* Resize Handle */}
      <div
        className="absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-accent/20 transition-colors z-10"
        onMouseDown={handleMouseDown}
      />
    </aside>
  );
}

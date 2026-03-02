import React, { useState, useCallback, useEffect, useRef } from 'react';
import api from '../utils/api';
import {
  Search, Filter, Refresh, Plus, Trash, Edit, ChevronLeft, ChevronRight,
  Download, Copy, Loader, AlertCircle, Check, X, ArrowUp, ArrowDown, Zap, Key
} from './Icons';
import { formatNumber, formatBytes, formatDuration, truncateId, getTypeBadge, safeJsonParse, prettyJson } from '../utils/formatters';
import JsonView from './JsonView';
import DocumentEditor from './DocumentEditor';
import IndexesView from './IndexesView';

export default function CollectionView({ db, collection, onQueryMs, showIndexes }) {
  const [documents, setDocuments] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(0);
  const [limit] = useState(50);
  const [filter, setFilter] = useState('{}');
  const [filterInput, setFilterInput] = useState('{}');
  const [sort, setSort] = useState('{}');
  const [selectedDoc, setSelectedDoc] = useState(null);
  const [editingDoc, setEditingDoc] = useState(null);
  const [insertMode, setInsertMode] = useState(false);
  const [stats, setStats] = useState(null);
  const [expandedRows, setExpandedRows] = useState(new Set());
  const [copied, setCopied] = useState(null);
  const [confirmDelete, setConfirmDelete] = useState(null);

  const loadDocuments = useCallback(async (p = page) => {
    setLoading(true);
    setError(null);
    try {
      const data = await api.getDocuments(db, collection, {
        filter,
        sort,
        skip: p * limit,
        limit,
      });
      setDocuments(data.documents || []);
      setTotal(data.total || 0);
      onQueryMs?.(data._elapsed);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [db, collection, filter, sort, page, limit, onQueryMs]);

  const loadStats = useCallback(async () => {
    try {
      const data = await api.getCollectionStats(db, collection);
      setStats(data);
    } catch {}
  }, [db, collection]);

  useEffect(() => {
    setPage(0);
    setFilter('{}');
    setFilterInput('{}');
    setSort('{}');
    setSelectedDoc(null);
    setEditingDoc(null);
    setExpandedRows(new Set());
  }, [db, collection]);

  useEffect(() => {
    loadDocuments(page);
  }, [page, filter, sort, db, collection]);

  useEffect(() => {
    loadStats();
  }, [loadStats]);

  const handleApplyFilter = () => {
    const { error } = safeJsonParse(filterInput);
    if (error) {
      setError(`Invalid filter JSON: ${error}`);
      return;
    }
    setFilter(filterInput);
    setPage(0);
  };

  const handleDelete = async (id) => {
    try {
      await api.deleteDocument(db, collection, id);
      setConfirmDelete(null);
      loadDocuments();
    } catch (err) {
      setError(err.message);
    }
  };

  const handleCopy = (doc) => {
    navigator.clipboard.writeText(prettyJson(doc));
    setCopied(doc._id);
    setTimeout(() => setCopied(null), 2000);
  };

  const toggleRow = (id) => {
    setExpandedRows(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const totalPages = Math.ceil(total / limit);
  const docId = (doc) => typeof doc._id === 'object' ? (doc._id.$oid || JSON.stringify(doc._id)) : String(doc._id);

  if (showIndexes) {
    return <IndexesView db={db} collection={collection} onQueryMs={onQueryMs} />;
  }

  if (editingDoc || insertMode) {
    return (
      <DocumentEditor
        db={db}
        collection={collection}
        document={insertMode ? null : editingDoc}
        onSave={() => { setEditingDoc(null); setInsertMode(false); loadDocuments(); }}
        onCancel={() => { setEditingDoc(null); setInsertMode(false); }}
      />
    );
  }

  return (
    <div className="h-full flex flex-col">
      {/* Toolbar */}
      <div className="flex-shrink-0 border-b border-border bg-surface-1/30 px-4 py-2">
        <div className="flex items-center gap-2">
          {/* Filter input */}
          <div className="flex-1 flex items-center gap-2">
            <Filter className="w-3.5 h-3.5 text-text-tertiary flex-shrink-0" />
            <input
              type="text"
              value={filterInput}
              onChange={(e) => setFilterInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleApplyFilter()}
              placeholder='{ "field": "value" }'
              className="flex-1 bg-transparent border-none text-xs font-mono text-text-primary placeholder-text-tertiary focus:outline-none"
              spellCheck={false}
            />
            <button onClick={handleApplyFilter} className="btn-ghost text-2xs font-medium text-accent">
              Apply
            </button>
          </div>

          <div className="w-px h-5 bg-border" />

          {/* Stats badges */}
          {stats && (
            <div className="hidden lg:flex items-center gap-2 text-2xs text-text-tertiary">
              <span>{formatNumber(stats.count)} docs</span>
              <span className="text-border">·</span>
              <span>{formatBytes(stats.size || 0)}</span>
              {stats.avgObjSize && (
                <>
                  <span className="text-border">·</span>
                  <span>avg {formatBytes(stats.avgObjSize)}</span>
                </>
              )}
            </div>
          )}

          <div className="w-px h-5 bg-border" />

          {/* Actions */}
          <button onClick={() => setInsertMode(true)} className="btn-ghost flex items-center gap-1 text-accent">
            <Plus className="w-3.5 h-3.5" />
            <span className="hidden sm:inline text-xs">Insert</span>
          </button>
          <button onClick={() => loadDocuments()} className="btn-ghost p-1.5" title="Refresh">
            <Refresh className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="mx-4 mt-2 flex items-start gap-2 text-red-400 text-xs bg-red-500/5 border border-red-500/20 rounded-lg p-3 animate-fade-in">
          <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
          <span className="flex-1">{error}</span>
          <button onClick={() => setError(null)} className="text-red-400/50 hover:text-red-400">
            <X className="w-3.5 h-3.5" />
          </button>
        </div>
      )}

      {/* Documents */}
      <div className="flex-1 overflow-auto">
        {loading && documents.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="flex flex-col items-center gap-3">
              <Loader className="text-accent w-5 h-5" />
              <span className="text-xs text-text-tertiary">Loading documents…</span>
            </div>
          </div>
        ) : documents.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <div className="text-text-tertiary text-sm mb-2">No documents found</div>
              <button onClick={() => setInsertMode(true)} className="btn-primary text-xs">
                Insert Document
              </button>
            </div>
          </div>
        ) : (
          <div className="divide-y divide-border">
            {documents.map((doc, idx) => {
              const id = docId(doc);
              const isExpanded = expandedRows.has(id);
              const keys = Object.keys(doc).filter(k => k !== '_id');
              const preview = keys.slice(0, 4);

              return (
                <div
                  key={id || idx}
                  className={`group transition-colors duration-75 ${isExpanded ? 'bg-surface-1/50' : 'hover:bg-surface-1/30'}`}
                >
                  {/* Row Summary */}
                  <div
                    className="flex items-center px-4 py-2.5 cursor-pointer gap-3"
                    onClick={() => toggleRow(id)}
                  >
                    <div className="w-5 flex-shrink-0">
                      {isExpanded ? (
                        <ChevronDown className="w-3.5 h-3.5 text-text-tertiary" />
                      ) : (
                        <ChevronRight className="w-3.5 h-3.5 text-text-tertiary" />
                      )}
                    </div>

                    {/* ID */}
                    <code className="text-2xs text-accent/70 font-mono w-24 flex-shrink-0 truncate">
                      {truncateId(doc._id)}
                    </code>

                    {/* Field preview */}
                    <div className="flex-1 flex items-center gap-2 min-w-0 overflow-hidden">
                      {preview.map(key => {
                        const val = doc[key];
                        const { label, cls } = getTypeBadge(val);
                        const display = val === null ? 'null'
                          : typeof val === 'object' ? (Array.isArray(val) ? `[${val.length}]` : '{…}')
                          : String(val).slice(0, 50);
                        return (
                          <div key={key} className="flex items-center gap-1 text-2xs min-w-0">
                            <span className="text-text-tertiary font-mono truncate">{key}:</span>
                            <span className="text-text-secondary truncate max-w-[120px]">{display}</span>
                          </div>
                        );
                      })}
                      {keys.length > 4 && (
                        <span className="text-2xs text-text-tertiary">+{keys.length - 4} more</span>
                      )}
                    </div>

                    {/* Actions */}
                    <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0">
                      <button
                        onClick={(e) => { e.stopPropagation(); handleCopy(doc); }}
                        className="p-1.5 rounded-md hover:bg-surface-3 text-text-tertiary hover:text-text-secondary transition-colors"
                        title="Copy JSON"
                      >
                        {copied === doc._id ? <Check className="w-3 h-3 text-accent" /> : <Copy className="w-3 h-3" />}
                      </button>
                      <button
                        onClick={(e) => { e.stopPropagation(); setEditingDoc(doc); }}
                        className="p-1.5 rounded-md hover:bg-surface-3 text-text-tertiary hover:text-text-secondary transition-colors"
                        title="Edit"
                      >
                        <Edit className="w-3 h-3" />
                      </button>
                      <button
                        onClick={(e) => { e.stopPropagation(); setConfirmDelete(id); }}
                        className="p-1.5 rounded-md hover:bg-red-500/10 text-text-tertiary hover:text-red-400 transition-colors"
                        title="Delete"
                      >
                        <Trash className="w-3 h-3" />
                      </button>
                    </div>
                  </div>

                  {/* Delete confirmation */}
                  {confirmDelete === id && (
                    <div className="px-4 pb-2 flex items-center gap-2 animate-fade-in">
                      <span className="text-xs text-red-400">Delete this document?</span>
                      <button
                        onClick={() => handleDelete(id)}
                        className="text-xs px-2 py-1 bg-red-500/10 text-red-400 rounded hover:bg-red-500/20 transition-colors"
                      >
                        Confirm
                      </button>
                      <button
                        onClick={() => setConfirmDelete(null)}
                        className="text-xs px-2 py-1 text-text-tertiary hover:text-text-secondary transition-colors"
                      >
                        Cancel
                      </button>
                    </div>
                  )}

                  {/* Expanded JSON View */}
                  {isExpanded && (
                    <div className="px-4 pb-3 pl-12 animate-fade-in">
                      <div className="bg-surface-2 border border-border rounded-xl p-4 overflow-auto max-h-[400px]">
                        <JsonView data={doc} />
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Pagination */}
      {totalPages > 0 && (
        <div className="flex-shrink-0 border-t border-border bg-surface-1/30 px-4 py-2 flex items-center justify-between">
          <span className="text-2xs text-text-tertiary">
            {formatNumber(page * limit + 1)}–{formatNumber(Math.min((page + 1) * limit, total))} of {formatNumber(total)}
          </span>
          <div className="flex items-center gap-1">
            <button
              onClick={() => setPage(Math.max(0, page - 1))}
              disabled={page === 0}
              className="btn-ghost p-1 disabled:opacity-30"
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
            <span className="text-xs text-text-secondary px-2 min-w-[60px] text-center">
              {page + 1} / {totalPages}
            </span>
            <button
              onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
              disabled={page >= totalPages - 1}
              className="btn-ghost p-1 disabled:opacity-30"
            >
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// Import ChevronDown separately since it's used inline
function ChevronDown(props) {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" {...props}>
      <path d="M6 9l6 6 6-6" />
    </svg>
  );
}

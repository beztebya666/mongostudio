import React, { useState, useCallback, useEffect, useRef } from 'react';
import api from '../utils/api';
import {
  Filter, Refresh, Plus, Trash, Edit, ChevronLeft, ChevronRight, ChevronDown,
  Download, Copy, Loader, AlertCircle, Check, X, Zap, Table, FileJson
} from './Icons';
import { formatNumber, formatBytes, safeJsonParse, prettyJson } from '../utils/formatters';
import JsonView from './JsonView';
import DocumentEditor from './DocumentEditor';
import IndexesView from './IndexesView';

export default function CollectionView({ db, collection, onQueryMs, showIndexes, refreshToken = 0 }) {
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
  const [viewMode, setViewMode] = useState('json'); // json | table
  const [exporting, setExporting] = useState(false);
  const [exportFormat, setExportFormat] = useState(false);
  const [slow, setSlow] = useState(false);
  const exportMenuRef = useRef(null);

  const loadDocuments = useCallback(async (p = page) => {
    setLoading(true); setError(null); setSlow(false);
    try {
      const data = await api.getDocuments(db, collection, { filter, sort, skip: p * limit, limit });
      setDocuments(data.documents || []);
      setTotal(data.total || 0);
      onQueryMs?.(data._elapsed);
      if (data._slow) setSlow(true);
    } catch (err) {
      setDocuments([]);
      setTotal(0);
      setError(err.message);
    }
    finally { setLoading(false); }
  }, [db, collection, filter, sort, page, limit, onQueryMs]);

  const loadStats = useCallback(async () => {
    try { setStats(await api.getCollectionStats(db, collection)); }
    catch { setStats(null); }
  }, [db, collection]);

  useEffect(() => {
    setDocuments([]);
    setTotal(0);
    setError(null);
    setStats(null);
    setSlow(false);
    setCopied(null);
    setConfirmDelete(null);
    setPage(0);
    setFilter('{}');
    setFilterInput('{}');
    setSort('{}');
    setSelectedDoc(null);
    setEditingDoc(null);
    setInsertMode(false);
    setExportFormat(false);
    setExpandedRows(new Set());
  }, [db, collection]);
  useEffect(() => { loadDocuments(page); }, [page, filter, sort, db, collection]);
  useEffect(() => { loadStats(); }, [loadStats]);
  useEffect(() => {
    if (refreshToken <= 0) return;
    if (!db || !collection) return;
    loadDocuments(page);
    loadStats();
  }, [refreshToken]);
  useEffect(() => {
    if (!exportFormat) return;
    const onMouseDown = (event) => {
      if (exportMenuRef.current && !exportMenuRef.current.contains(event.target)) {
        setExportFormat(false);
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    return () => document.removeEventListener('mousedown', onMouseDown);
  }, [exportFormat]);

  const handleApplyFilter = () => {
    const { error } = safeJsonParse(filterInput);
    if (error) { setError(`Invalid filter JSON: ${error}`); return; }
    setFilter(filterInput); setPage(0);
  };

  const handleDelete = async (doc) => {
    const id = docId(doc);
    if (!id) { setError('Cannot delete document without _id'); return; }
    try { await api.deleteDocument(db, collection, id); setConfirmDelete(null); loadDocuments(); }
    catch (err) { setError(err.message); }
  };

  const handleCopy = (doc, key) => { navigator.clipboard.writeText(prettyJson(doc)); setCopied(key); setTimeout(() => setCopied(null), 2000); };
  const toggleRow = (id) => { setExpandedRows(prev => { const n=new Set(prev); if(n.has(id)) n.delete(id); else n.add(id); return n; }); };

  const handleExport = async (format) => {
    setExporting(true);
    try {
      const data = await api.exportData(db, collection, { format, filter, limit: 10000 });
      const blob = new Blob([data.data], { type: format === 'csv' ? 'text/csv' : 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a'); a.href = url; a.download = `${collection}.${format}`; a.click();
      URL.revokeObjectURL(url);
      setExportFormat(false);
    } catch (err) { setError(err.message); }
    finally { setExporting(false); }
  };

  const totalPages = Math.ceil(total / limit);
  const docId = (doc) => {
    if (!doc || !Object.prototype.hasOwnProperty.call(doc, '_id')) return null;
    if (doc._id === null || doc._id === undefined) return null;
    return typeof doc._id === 'object' ? (doc._id.$oid || JSON.stringify(doc._id)) : String(doc._id);
  };
  const rowKey = (doc, idx) => docId(doc) || `row:${idx}`;
  const fullId = (id) => {
    if (id === null || id === undefined) return '-';
    return typeof id === 'object' ? (id.$oid || JSON.stringify(id)) : String(id);
  };

  // Get all unique keys for table view
  const allKeys = viewMode === 'table' ? [...new Set(documents.flatMap(d => Object.keys(d)))] : [];

  if (showIndexes) return <IndexesView db={db} collection={collection} onQueryMs={onQueryMs} refreshToken={refreshToken} />;
  if (editingDoc || insertMode) return <DocumentEditor db={db} collection={collection} document={insertMode ? null : editingDoc} onSave={() => { setEditingDoc(null); setInsertMode(false); loadDocuments(); }} onCancel={() => { setEditingDoc(null); setInsertMode(false); }} />;

  return (
    <div className="h-full flex flex-col">
      {/* Toolbar */}
      <div className="flex-shrink-0 px-4 py-2" style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
        <div className="flex items-center gap-2">
          <span
            className="hidden md:inline-flex items-center px-2 py-1 rounded-md text-2xs font-mono"
            style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-secondary)' }}
            title={`${db}.${collection}`}
          >
            {db}.{collection}
          </span>
          <div className="hidden md:block w-px h-5" style={{background:'var(--border)'}} />
          <Filter className="w-3.5 h-3.5 flex-shrink-0" style={{color:'var(--text-tertiary)'}} />
          <input type="text" value={filterInput} onChange={e=>setFilterInput(e.target.value)} onKeyDown={e=>e.key==='Enter'&&handleApplyFilter()}
            placeholder='{ "field": "value" }' spellCheck={false}
            className="flex-1 bg-transparent border-none text-xs font-mono focus:outline-none" style={{color:'var(--text-primary)'}} />
          <button onClick={handleApplyFilter} className="btn-ghost text-2xs font-medium" style={{color:'var(--accent)'}}>Apply</button>
          <div className="w-px h-5" style={{background:'var(--border)'}} />

          {stats && (
            <div className="hidden lg:flex items-center gap-2 text-2xs" style={{color:'var(--text-tertiary)'}}>
              <span>{formatNumber(stats.count)} docs</span>
              <span style={{color:'var(--border)'}}>·</span>
              <span>{formatBytes(stats.size || 0)}</span>
            </div>
          )}
          <div className="w-px h-5" style={{background:'var(--border)'}} />

          {/* View mode toggle */}
          <div className="flex items-center rounded-lg p-0.5" style={{background:'var(--surface-2)'}}>
            <button onClick={()=>setViewMode('json')} className="p-1.5 rounded-md transition-all" style={{background:viewMode==='json'?'var(--surface-4)':'transparent'}} title="JSON View">
              <FileJson className="w-3.5 h-3.5" style={{color:viewMode==='json'?'var(--text-primary)':'var(--text-tertiary)'}} />
            </button>
            <button onClick={()=>setViewMode('table')} className="p-1.5 rounded-md transition-all" style={{background:viewMode==='table'?'var(--surface-4)':'transparent'}} title="Table View">
              <Table className="w-3.5 h-3.5" style={{color:viewMode==='table'?'var(--text-primary)':'var(--text-tertiary)'}} />
            </button>
          </div>

          {/* Export */}
          <div className="relative" ref={exportMenuRef}>
            <button onClick={()=>setExportFormat(v=>!v)} className="btn-ghost flex items-center gap-1" style={{color:'var(--accent)'}}>
              <Download className="w-3.5 h-3.5" />
              <span className="hidden sm:inline text-xs">Export</span>
            </button>
            {exportFormat && (
              <div className="absolute right-0 top-full mt-1 z-50 rounded-lg shadow-lg py-1 min-w-[120px] animate-fade-in" style={{background:'var(--surface-3)',border:'1px solid var(--border)'}}>
                <button onClick={()=>handleExport('json')} disabled={exporting} className="w-full text-left px-3 py-1.5 text-xs transition-colors" style={{color:'var(--text-secondary)'}}
                  onMouseOver={e=>e.currentTarget.style.background='var(--surface-4)'} onMouseOut={e=>e.currentTarget.style.background='transparent'}>
                  {exporting?'Exporting…':'Export JSON'}
                </button>
                <button onClick={()=>handleExport('csv')} disabled={exporting} className="w-full text-left px-3 py-1.5 text-xs transition-colors" style={{color:'var(--text-secondary)'}}
                  onMouseOver={e=>e.currentTarget.style.background='var(--surface-4)'} onMouseOut={e=>e.currentTarget.style.background='transparent'}>
                  {exporting?'Exporting…':'Export CSV'}
                </button>
              </div>
            )}
          </div>

          <button onClick={()=>setInsertMode(true)} className="btn-ghost flex items-center gap-1" style={{color:'var(--accent)'}}>
            <Plus className="w-3.5 h-3.5" /><span className="hidden sm:inline text-xs">Insert</span>
          </button>
          <button onClick={()=>loadDocuments()} className="btn-ghost p-1.5" title="Refresh">
            <Refresh className={`w-3.5 h-3.5 ${loading?'animate-spin':''}`} />
          </button>
        </div>
      </div>

      {/* Slow query warning */}
      {slow && (
        <div className="mx-4 mt-2 flex items-center gap-2 text-xs p-2 rounded-lg animate-fade-in" style={{background:'rgba(245,158,11,0.08)',border:'1px solid rgba(245,158,11,0.2)',color:'#fbbf24'}}>
          <Zap className="w-3.5 h-3.5" /> Slow query detected — consider adding an index or narrowing your filter
          <button onClick={()=>setSlow(false)} className="ml-auto"><X className="w-3 h-3" /></button>
        </div>
      )}

      {error && (
        <div className="mx-4 mt-2 flex items-start gap-2 text-red-400 text-xs p-3 rounded-lg animate-fade-in" style={{background:'rgba(239,68,68,0.05)',border:'1px solid rgba(239,68,68,0.2)'}}>
          <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
          <span className="flex-1">{error}</span>
          <button onClick={()=>setError(null)}><X className="w-3.5 h-3.5 text-red-400/50" /></button>
        </div>
      )}

      {/* Documents */}
      <div className="flex-1 overflow-auto">
        {loading && documents.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="flex flex-col items-center gap-3"><Loader style={{color:'var(--accent)'}} className="w-5 h-5" /><span className="text-xs" style={{color:'var(--text-tertiary)'}}>Loading…</span></div>
          </div>
        ) : documents.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center" style={{color:'var(--text-tertiary)'}}>
              <div className="text-sm mb-2">No documents found</div>
              <button onClick={()=>setInsertMode(true)} className="btn-primary text-xs">Insert Document</button>
            </div>
          </div>
        ) : viewMode === 'table' ? (
          /* TABLE VIEW */
          <div className="overflow-auto">
            <table className="w-full text-xs">
              <thead>
                <tr style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
                  {allKeys.map(k => (
                    <th key={k} className="px-3 py-2 text-left font-medium text-2xs uppercase tracking-wider whitespace-nowrap" style={{color:'var(--text-tertiary)'}}>{k}</th>
                  ))}
                  <th className="px-3 py-2 w-20"></th>
                </tr>
              </thead>
              <tbody>
                {documents.map((doc, idx) => {
                  const key = rowKey(doc, idx);
                  return (
                    <tr key={key} className="group transition-colors" style={{borderBottom:'1px solid var(--border)'}}
                      onMouseOver={e=>e.currentTarget.style.background='var(--surface-1)'} onMouseOut={e=>e.currentTarget.style.background='transparent'}>
                      {allKeys.map(k => {
                        const v = doc[k];
                        const display = v===null ? <span style={{color:'var(--json-null)'}}>null</span>
                          : v===undefined ? ''
                          : typeof v === 'object' ? <span className="font-mono" style={{color:'var(--json-bracket)'}}>{Array.isArray(v)?`[${v.length}]`:'{…}'}</span>
                          : typeof v === 'boolean' ? <span style={{color:'var(--json-boolean)'}}>{String(v)}</span>
                          : typeof v === 'number' ? <span style={{color:'var(--json-number)'}}>{v}</span>
                          : k === '_id' ? <span className="font-mono" style={{color:'var(--json-objectid)'}}>{fullId(v)}</span>
                          : <span className="truncate max-w-[200px] inline-block" style={{color:'var(--text-secondary)'}}>{String(v).slice(0,80)}</span>;
                        return <td key={k} className="px-3 py-2 font-mono whitespace-nowrap">{display}</td>;
                      })}
                      <td className="px-3 py-2">
                        <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                          <button
                            onClick={()=>handleCopy(doc, key)}
                            className="p-1 rounded-md transition-all focus:outline-none focus-visible:ring-1 focus-visible:ring-[var(--accent)]"
                            style={{color:'var(--text-tertiary)'}}
                            onMouseOver={e=>{e.currentTarget.style.background='var(--surface-2)';e.currentTarget.style.color='var(--text-primary)';}}
                            onMouseOut={e=>{e.currentTarget.style.background='transparent';e.currentTarget.style.color='var(--text-tertiary)';}}
                          >
                            {copied===key ? <Check className="w-3 h-3" style={{color:'var(--accent)'}} /> : <Copy className="w-3 h-3" />}
                          </button>
                          <button
                            onClick={()=>setEditingDoc(doc)}
                            className="p-1 rounded-md transition-all focus:outline-none focus-visible:ring-1 focus-visible:ring-[var(--accent)]"
                            style={{color:'var(--text-tertiary)'}}
                            onMouseOver={e=>{e.currentTarget.style.background='var(--surface-2)';e.currentTarget.style.color='var(--text-primary)';}}
                            onMouseOut={e=>{e.currentTarget.style.background='transparent';e.currentTarget.style.color='var(--text-tertiary)';}}
                          ><Edit className="w-3 h-3" /></button>
                          <button
                            onClick={()=>setConfirmDelete(key)}
                            className="p-1 rounded-md transition-all focus:outline-none focus-visible:ring-1 focus-visible:ring-red-400/70"
                            style={{color:'var(--text-tertiary)'}}
                            onMouseOver={e=>{e.currentTarget.style.background='rgba(239,68,68,0.15)';e.currentTarget.style.color='#f87171';}}
                            onMouseOut={e=>{e.currentTarget.style.background='transparent';e.currentTarget.style.color='var(--text-tertiary)';}}
                          ><Trash className="w-3 h-3" /></button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          /* JSON VIEW */
          <div>
            {documents.map((doc, idx) => {
              const key = rowKey(doc, idx);
              const isExpanded = expandedRows.has(key);
              const keys = Object.keys(doc).filter(k => k !== '_id');
              const preview = keys.slice(0, 4);
              return (
                <div key={key} className="group transition-colors duration-75" style={{borderBottom:'1px solid var(--border)',background:isExpanded?'var(--surface-1)':'transparent'}}>
                  <div className="flex items-center px-4 py-2.5 cursor-pointer gap-3" onClick={()=>toggleRow(key)}>
                    <div className="w-5 flex-shrink-0">
                      {isExpanded ? <ChevronDown className="w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} /> : <ChevronRight className="w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} />}
                    </div>
                    <code className="text-2xs font-mono flex-shrink-0 whitespace-nowrap" style={{color:'var(--json-objectid)',opacity:0.7}}>{fullId(doc._id)}</code>
                    <div className="flex-1 flex items-center gap-2 min-w-0 overflow-hidden">
                      {preview.map(key => {
                        const val = doc[key];
                        const display = val===null?'null': typeof val==='object'?(Array.isArray(val)?`[${val.length}]`:'{…}'): String(val).slice(0,50);
                        return (
                          <div key={key} className="flex items-center gap-1 text-2xs min-w-0">
                            <span className="font-mono truncate" style={{color:'var(--text-tertiary)'}}>{key}:</span>
                            <span className="truncate max-w-[120px]" style={{color:'var(--text-secondary)'}}>{display}</span>
                          </div>
                        );
                      })}
                      {keys.length > 4 && <span className="text-2xs" style={{color:'var(--text-tertiary)'}}>+{keys.length - 4} more</span>}
                    </div>
                    <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0">
                      <button
                        onClick={e=>{e.stopPropagation();handleCopy(doc, key)}}
                        className="p-1.5 rounded-md transition-all focus:outline-none focus-visible:ring-1 focus-visible:ring-[var(--accent)]"
                        style={{color:'var(--text-tertiary)'}}
                        title="Copy JSON"
                        onMouseOver={e=>{e.currentTarget.style.background='var(--surface-2)';e.currentTarget.style.color='var(--text-primary)';}}
                        onMouseOut={e=>{e.currentTarget.style.background='transparent';e.currentTarget.style.color='var(--text-tertiary)';}}
                      >
                        {copied===key ? <Check className="w-3 h-3" style={{color:'var(--accent)'}} /> : <Copy className="w-3 h-3" />}
                      </button>
                      <button
                        onClick={e=>{e.stopPropagation();setEditingDoc(doc)}}
                        className="p-1.5 rounded-md transition-all focus:outline-none focus-visible:ring-1 focus-visible:ring-[var(--accent)]"
                        style={{color:'var(--text-tertiary)'}}
                        title="Edit"
                        onMouseOver={e=>{e.currentTarget.style.background='var(--surface-2)';e.currentTarget.style.color='var(--text-primary)';}}
                        onMouseOut={e=>{e.currentTarget.style.background='transparent';e.currentTarget.style.color='var(--text-tertiary)';}}
                      ><Edit className="w-3 h-3" /></button>
                      <button
                        onClick={e=>{e.stopPropagation();setConfirmDelete(key)}}
                        className="p-1.5 rounded-md transition-all focus:outline-none focus-visible:ring-1 focus-visible:ring-red-400/70"
                        style={{color:'var(--text-tertiary)'}}
                        title="Delete"
                        onMouseOver={e=>{e.currentTarget.style.background='rgba(239,68,68,0.15)';e.currentTarget.style.color='#f87171';}}
                        onMouseOut={e=>{e.currentTarget.style.background='transparent';e.currentTarget.style.color='var(--text-tertiary)';}}
                      ><Trash className="w-3 h-3" /></button>
                    </div>
                  </div>
                  {confirmDelete===key && (
                    <div className="px-4 pb-2 flex items-center gap-2 animate-fade-in">
                      <span className="text-xs text-red-400">Delete this document?</span>
                      <button onClick={()=>handleDelete(doc)} className="text-xs px-2 py-1 bg-red-500/10 text-red-400 rounded">Confirm</button>
                      <button onClick={()=>setConfirmDelete(null)} className="text-xs" style={{color:'var(--text-tertiary)'}}>Cancel</button>
                    </div>
                  )}
                  {isExpanded && (
                    <div className="px-4 pb-3 pl-12 animate-fade-in">
                      <div className="rounded-xl p-4 overflow-auto max-h-[400px]" style={{background:'var(--surface-2)',border:'1px solid var(--border)'}}>
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
        <div className="flex-shrink-0 px-4 py-2 flex items-center justify-between" style={{borderTop:'1px solid var(--border)',background:'var(--surface-1)'}}>
          <span className="text-2xs" style={{color:'var(--text-tertiary)'}}>
            {formatNumber(page*limit+1)}–{formatNumber(Math.min((page+1)*limit,total))} of {formatNumber(total)}
          </span>
          <div className="flex items-center gap-1">
            <button onClick={()=>setPage(0)} disabled={page===0} className="btn-ghost p-1 disabled:opacity-30 text-2xs">First</button>
            <button onClick={()=>setPage(Math.max(0,page-1))} disabled={page===0} className="btn-ghost p-1 disabled:opacity-30"><ChevronLeft className="w-4 h-4" /></button>
            <span className="text-xs px-2 min-w-[60px] text-center" style={{color:'var(--text-secondary)'}}>{page+1} / {totalPages}</span>
            <button onClick={()=>setPage(Math.min(totalPages-1,page+1))} disabled={page>=totalPages-1} className="btn-ghost p-1 disabled:opacity-30"><ChevronRight className="w-4 h-4" /></button>
            <button onClick={()=>setPage(totalPages-1)} disabled={page>=totalPages-1} className="btn-ghost p-1 disabled:opacity-30 text-2xs">Last</button>
          </div>
        </div>
      )}
    </div>
  );
}

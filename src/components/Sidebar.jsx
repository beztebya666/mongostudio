import React, { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import api from '../utils/api';
import { Database, ChevronRight, ChevronDown, Collection, Refresh, Search, Plus, Loader, Trash, X, MoreVertical, AlertCircle, Upload, Filter, Check, Download } from './Icons';
import { formatBytes, formatNumber } from '../utils/formatters';
import InputDialog from './modals/InputDialog';
import DatabaseExportDialog from './modals/DatabaseExportDialog';
import { exportSingleDatabase, exportMultipleDatabases } from '../utils/exportUtils';

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

function guessNameFromFile(fileName, fallback = 'imported_collection') {
  const raw = String(fileName || '').trim();
  if (!raw) return fallback;
  const base = raw.replace(/\.[^.]+$/, '');
  const safe = base.replace(/[^a-zA-Z0-9_.-]+/g, '_').replace(/^_+|_+$/g, '');
  return safe || fallback;
}

function normalizeCollectionEntry(entry = {}) {
  return {
    ...entry,
    count: typeof entry.count === 'number' ? entry.count : null,
    size: typeof entry.size === 'number' ? entry.size : null,
    avgObjSize: typeof entry.avgObjSize === 'number' ? entry.avgObjSize : null,
    nindexes: typeof entry.nindexes === 'number' ? entry.nindexes : null,
    _statsLoading: false,
  };
}

const DB_SORT_OPTIONS = [
  { value: 'name', label: 'Name (A-Z)' },
  { value: 'size', label: 'Size' },
  { value: 'collections', label: 'Collections' },
  { value: 'documents', label: 'Documents' },
];

function DbItem({ db, selectedDb, selectedCol, onSelect, onRefresh, refreshToken, onError, onSuccess, onEnsureStats }) {
  const [expanded, setExpanded] = useState(false);
  const [collections, setCollections] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [showMenu, setShowMenu] = useState(false);
  const [showColMenu, setShowColMenu] = useState(null);
  const [confirmDrop, setConfirmDrop] = useState(null); // 'db' | colName
  const [showExportColDialog, setShowExportColDialog] = useState(false);
  const [exportCollectionName, setExportCollectionName] = useState('');
  const [exportCollectionFormat, setExportCollectionFormat] = useState('json');
  const [exportUseVisibleFields, setExportUseVisibleFields] = useState(false);
  const [exportSortMode, setExportSortMode] = useState('saved');
  const [exportBusy, setExportBusy] = useState(false);
  const [showDbExportDialog, setShowDbExportDialog] = useState(false);
  const [dbExportBusy, setDbExportBusy] = useState(false);
  const [dbExportMode, setDbExportMode] = useState('package');
  const [dbExportArchive, setDbExportArchive] = useState(true);
  const [dbExportCollectionFormat, setDbExportCollectionFormat] = useState('json');
  const [dbExportIncludeIndexes, setDbExportIncludeIndexes] = useState(true);
  const [dbExportIncludeSchema, setDbExportIncludeSchema] = useState(true);
  const [showCreateColDialog, setShowCreateColDialog] = useState(false);
  const [showImportColDialog, setShowImportColDialog] = useState(false);
  const [pendingCollectionImport, setPendingCollectionImport] = useState(null);
  const [importBusy, setImportBusy] = useState(false);
  const [docsImportTarget, setDocsImportTarget] = useState('');
  const [docsImportBusy, setDocsImportBusy] = useState(false);
  const colImportInputRef = useRef(null);
  const docsImportInputRef = useRef(null);
  const collectionStatsLoadingRef = useRef(new Set());
  const collectionStatsSeqRef = useRef(0);

  const ensureCollectionStats = useCallback(async (colName) => {
    if (!colName) return;
    if (collectionStatsLoadingRef.current.has(colName)) return;
    const current = collections.find((entry) => entry.name === colName);
    if (!current || (typeof current.count === 'number' && typeof current.size === 'number')) return;
    collectionStatsLoadingRef.current.add(colName);
    setCollections((prev) => prev.map((entry) => (
      entry.name === colName ? { ...entry, _statsLoading: true } : entry
    )));
    const seq = collectionStatsSeqRef.current;
    try {
      const stats = await api.getCollectionStats(db.name, colName);
      if (seq !== collectionStatsSeqRef.current) return;
      setCollections((prev) => prev.map((entry) => (
        entry.name === colName
          ? {
              ...entry,
              count: typeof stats.count === 'number' ? stats.count : null,
              size: typeof stats.size === 'number' ? stats.size : null,
              avgObjSize: typeof stats.avgObjSize === 'number' ? stats.avgObjSize : null,
              nindexes: typeof stats.nindexes === 'number' ? stats.nindexes : null,
              _statsLoading: false,
            }
          : entry
      )));
    } catch {
      if (seq !== collectionStatsSeqRef.current) return;
      setCollections((prev) => prev.map((entry) => (
        entry.name === colName ? { ...entry, _statsLoading: false } : entry
      )));
    } finally {
      collectionStatsLoadingRef.current.delete(colName);
    }
  }, [collections, db.name]);

  const prefetchCollectionStats = useCallback((colNames = []) => {
    colNames.slice(0, 10).forEach((name) => {
      ensureCollectionStats(name);
    });
  }, [ensureCollectionStats]);

  const toggle = useCallback(async () => {
    if (expanded) {
      setExpanded(false);
      return;
    }
    setExpanded(true);
    if (loaded) return;
    setLoading(true);
    collectionStatsSeqRef.current += 1;
    collectionStatsLoadingRef.current.clear();
    try {
      const d = await api.listCollections(db.name, { withStats: false });
      const cols = (d.collections || []).map((entry) => normalizeCollectionEntry(entry));
      setCollections(cols);
      setLoaded(true);
      prefetchCollectionStats(cols.map((entry) => entry.name));
    }
    catch (err) { console.error(err); }
    finally { setLoading(false); }
  }, [expanded, loaded, db.name, prefetchCollectionStats]);

  useEffect(() => { if (selectedDb===db.name && !expanded) toggle(); }, [selectedDb]);

  const refreshCollections = async () => {
    setLoading(true);
    collectionStatsSeqRef.current += 1;
    collectionStatsLoadingRef.current.clear();
    try {
      const d = await api.listCollections(db.name, { withStats: false });
      const cols = (d.collections || []).map((entry) => normalizeCollectionEntry(entry));
      setCollections(cols);
      setLoaded(true);
      prefetchCollectionStats(cols.map((entry) => entry.name));
    }
    catch(err) { console.error(err); }
    finally { setLoading(false); }
  };
  useEffect(() => {
    if (!loaded) return;
    refreshCollections();
  }, [refreshToken]);

  const handleDropDb = async () => {
    try { await api.dropDatabase(db.name); setConfirmDrop(null); onRefresh(); }
    catch(err) { onError?.(err.message); }
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
      onError?.(err.message);
    }
  };

  const handleDropCol = async (colName) => {
    try { await api.dropCollection(db.name, colName); setConfirmDrop(null); refreshCollections(); onRefresh?.(); }
    catch(err) { onError?.(err.message); }
  };

  const downloadText = (filename, text, mime = 'application/json') => {
    const blob = new Blob([text], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  const openDbExportDialog = () => {
    setDbExportMode('package');
    setDbExportArchive(true);
    setDbExportCollectionFormat('json');
    setDbExportIncludeIndexes(true);
    setDbExportIncludeSchema(true);
    setShowDbExportDialog(true);
  };

  const handleExportDb = async () => {
    setDbExportBusy(true);
    try {
      const result = await exportSingleDatabase(db.name, {
        mode: dbExportMode,
        archive: dbExportArchive,
        collectionFormat: dbExportCollectionFormat,
        includeIndexes: dbExportIncludeIndexes,
        includeSchema: dbExportIncludeSchema,
      });
      onSuccess?.(`Database "${db.name}" exported (${result.files} file${result.files === 1 ? '' : 's'}${result.archive ? ', zip' : ''}).`);
      setShowDbExportDialog(false);
    } catch (err) {
      onError?.(err.message);
    } finally {
      setDbExportBusy(false);
    }
  };

  const getStoredVisibleFields = (colName) => {
    const keys = [
      `mongostudio_columns:${db.name}.${colName}`,
      `mongostudio_preview:${db.name}.${colName}`,
    ];
    for (const key of keys) {
      try {
        const raw = localStorage.getItem(key);
        if (!raw) continue;
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed) && parsed.length > 0) {
          return [...new Set(parsed.filter((item) => typeof item === 'string' && item.trim().length > 0))];
        }
      } catch {}
    }
    return [];
  };

  const getStoredSort = (colName) => {
    try {
      const raw = localStorage.getItem(`mongostudio_sort:${db.name}.${colName}`);
      if (!raw) return '{"_id":-1}';
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return '{"_id":-1}';
      const entries = Object.entries(parsed);
      if (entries.length === 0) return '{"_id":-1}';
      const [field, dirRaw] = entries[0];
      const dir = Number(dirRaw) === -1 ? -1 : 1;
      return JSON.stringify({ [field]: dir });
    } catch {
      return '{"_id":-1}';
    }
  };

  const openExportCollectionDialog = (colName) => {
    setExportCollectionName(colName);
    setExportCollectionFormat('json');
    setExportUseVisibleFields(false);
    setExportSortMode('saved');
    setShowExportColDialog(true);
  };

  const handleExportCollection = async () => {
    if (!exportCollectionName) return;
    setExportBusy(true);
    try {
      const visibleFields = exportUseVisibleFields ? getStoredVisibleFields(exportCollectionName) : [];
      if (exportUseVisibleFields && visibleFields.length === 0) {
        onError?.('No saved visible fields for this collection yet.');
        setExportBusy(false);
        return;
      }
      const sortValue = exportSortMode === 'none'
        ? '{}'
        : exportSortMode === 'id_desc'
          ? '{"_id":-1}'
          : getStoredSort(exportCollectionName);
      const projection = exportUseVisibleFields
        ? JSON.stringify(Object.fromEntries(visibleFields.map((field) => [field, 1])))
        : '{}';
      const data = await api.exportData(db.name, exportCollectionName, {
        format: exportCollectionFormat,
        filter: '{}',
        sort: sortValue,
        limit: 50000,
        projection,
      });
      const ext = exportCollectionFormat === 'csv' ? 'csv' : 'json';
      const mime = exportCollectionFormat === 'csv' ? 'text/csv' : 'application/json';
      downloadText(`${db.name}.${exportCollectionName}.${ext}`, data.data, mime);
      onSuccess?.(`Collection "${exportCollectionName}" exported.`);
      setShowExportColDialog(false);
    } catch (err) {
      onError?.(`Collection export failed: ${err.message}`);
    } finally {
      setExportBusy(false);
    }
  };

  const handleImportCollectionFile = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      let payload = null;

      if (Array.isArray(parsed)) {
        payload = {
          name: guessNameFromFile(file.name),
          documents: parsed,
          indexes: [],
          options: {},
        };
      } else if (parsed && typeof parsed === 'object') {
        if (Array.isArray(parsed.documents)) {
          payload = {
            name: parsed.name || guessNameFromFile(file.name),
            documents: parsed.documents,
            indexes: Array.isArray(parsed.indexes) ? parsed.indexes : [],
            options: parsed.options && typeof parsed.options === 'object' ? parsed.options : {},
          };
        } else if (parsed.type === 'mongostudio-db-package' && Array.isArray(parsed.collections)) {
          if (parsed.collections.length !== 1) {
            throw new Error('Package has multiple collections. Use "Import Database" for full package import.');
          }
          const first = parsed.collections[0];
          payload = {
            name: first.name || guessNameFromFile(file.name),
            documents: Array.isArray(first.documents) ? first.documents : [],
            indexes: Array.isArray(first.indexes) ? first.indexes : [],
            options: first.options && typeof first.options === 'object' ? first.options : {},
          };
        }
      }

      if (!payload) throw new Error('Unsupported collection JSON format.');
      setPendingCollectionImport(payload);
      setShowImportColDialog(true);
    } catch (err) {
      onError?.(`Collection import failed: ${err.message}`);
    } finally {
      event.target.value = '';
    }
  };

  const handleImportCollectionSubmit = async (name) => {
    if (!pendingCollectionImport) return;
    const colName = name?.trim();
    if (!colName) return;
    setImportBusy(true);
    try {
      const dropExisting = window.confirm(`Replace collection "${colName}" if it exists?\nOK = replace, Cancel = merge.`);
      const result = await api.importCollection(db.name, {
        name: colName,
        documents: pendingCollectionImport.documents || [],
        indexes: pendingCollectionImport.indexes || [],
        options: pendingCollectionImport.options || {},
        dropExisting,
      });
      onSuccess?.(`Imported ${result.insertedCount || 0} docs into "${colName}".`);
      setShowImportColDialog(false);
      setPendingCollectionImport(null);
      refreshCollections();
      onRefresh?.();
    } catch (err) {
      onError?.(`Collection import failed: ${err.message}`);
    } finally {
      setImportBusy(false);
    }
  };

  const openImportDocumentsDialog = (colName) => {
    setDocsImportTarget(colName);
    docsImportInputRef.current?.click();
  };

  const parseDocumentsFromText = (text) => {
    const raw = String(text || '').trim();
    if (!raw) return [];
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return parsed;
      if (parsed && typeof parsed === 'object') {
        if (Array.isArray(parsed.documents)) return parsed.documents;
        return [parsed];
      }
    } catch {}

    const lines = raw.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
    if (lines.length === 0) return [];
    return lines.map((line, idx) => {
      try {
        return JSON.parse(line);
      } catch (err) {
        throw new Error(`Invalid JSON at line ${idx + 1}: ${err.message}`);
      }
    });
  };

  const handleImportDocumentsFile = async (event) => {
    const file = event.target.files?.[0];
    const targetCol = docsImportTarget;
    if (!file || !targetCol) return;
    setDocsImportBusy(true);
    try {
      const text = await file.text();
      const docs = parseDocumentsFromText(text);
      if (!Array.isArray(docs) || docs.length === 0) {
        throw new Error('No documents found in file.');
      }
      let insertedTotal = 0;
      const chunkSize = 10000;
      for (let i = 0; i < docs.length; i += chunkSize) {
        const chunk = docs.slice(i, i + chunkSize);
        const result = await api.insertDocuments(db.name, targetCol, chunk);
        insertedTotal += Number(result?.insertedCount || 0);
      }
      onSuccess?.(`Imported ${insertedTotal} docs into "${targetCol}".`);
      refreshCollections();
      onRefresh?.();
    } catch (err) {
      onError?.(`Import documents failed: ${err.message}`);
    } finally {
      setDocsImportBusy(false);
      setDocsImportTarget('');
      event.target.value = '';
    }
  };

  return (
    <div className="animate-slide-up">
      <div className="group relative flex items-center">
        <button
          data-sidebar-row
          onClick={toggle}
          onMouseEnter={() => onEnsureStats?.(db.name)}
          className="flex-1 flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-all duration-100 hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]"
          title={`${db.name}${typeof db.collections === 'number' ? ` | ${db.collections} collections` : ''}${typeof db.objects === 'number' ? ` | ${formatNumber(db.objects)} docs` : ''}${typeof db.sizeOnDisk === 'number' ? ` | ${formatBytes(db.sizeOnDisk)}` : ''}`}
          style={{
            background: selectedDb===db.name&&!selectedCol ? 'var(--surface-3)' : 'transparent',
            color: selectedDb===db.name&&!selectedCol ? 'var(--text-primary)' : 'var(--text-secondary)',
          }}>
          {loading ? <Loader className="w-3.5 h-3.5" style={{color:'var(--accent)'}} />
            : expanded ? <ChevronDown className="w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} />
            : <ChevronRight className="w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} />}
          <Database className="w-3.5 h-3.5" style={{color:'var(--accent)',opacity:0.7}} />
          <span data-sidebar-label className="text-left font-medium whitespace-nowrap shrink-0">{db.name}</span>
          <span className="ml-auto text-2xs opacity-0 group-hover:opacity-100 transition-opacity text-right whitespace-nowrap shrink-0 inline-flex items-center gap-1" style={{color:'var(--text-tertiary)'}}>
            {db._statsLoaded ? (
              <>
                {typeof db.collections === 'number' ? `${db.collections}c` : '-c'}
                {typeof db.objects === 'number' ? ` ${formatNumber(db.objects)}d` : ' -d'}
                <span>|</span>
              </>
            ) : db._statsError ? (
              <>
                <span>unavailable</span>
                <span>|</span>
              </>
            ) : (
              <>
                <Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />
                <span>loading</span>
                <span>|</span>
              </>
            )}
            {formatBytes(db.sizeOnDisk||0)}
          </span>
        </button>
        <div className="relative">
          <button onClick={()=>setShowMenu(!showMenu)} className="opacity-0 group-hover:opacity-100 p-1 rounded-md transition-all hover:bg-[var(--surface-2)]" style={{color:'var(--text-tertiary)'}}>
            <MoreVertical className="w-3.5 h-3.5" />
          </button>
              {showMenu && (
                <ContextMenu onClose={()=>setShowMenu(false)} items={[
                  { label:'New Collection', action:handleCreateCol },
                  { label:'Import Collection', action:() => colImportInputRef.current?.click() },
                  { label:'Export Database...', action:openDbExportDialog },
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
              <button
                data-sidebar-row
                onClick={()=>onSelect(db.name, col.name)}
                onMouseEnter={() => ensureCollectionStats(col.name)}
                className="flex-1 flex items-center gap-2 px-2.5 py-1.5 rounded-lg text-xs transition-all duration-100 hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]"
                title={`${db.name}.${col.name}${typeof col.count === 'number' ? ` | ${formatNumber(col.count)} docs` : ''}${typeof col.size === 'number' ? ` | ${formatBytes(col.size)}` : ''}`}
                style={{
                  background: selectedDb===db.name&&selectedCol===col.name ? 'rgba(0, 237, 100, 0.12)' : 'transparent',
                  color: selectedDb===db.name&&selectedCol===col.name ? 'var(--accent)' : 'var(--text-secondary)',
                  border: selectedDb===db.name&&selectedCol===col.name ? '1px solid rgba(0, 237, 100, 0.28)' : '1px solid transparent',
                }}>
                <Collection className="w-3 h-3 flex-shrink-0" style={{color:'var(--text-tertiary)'}} />
                <span data-sidebar-label className="text-left whitespace-nowrap shrink-0">{col.name}</span>
                <span className="ml-auto text-2xs opacity-0 group-hover/col:opacity-100 transition-opacity whitespace-nowrap shrink-0 inline-flex items-center gap-1" style={{color:'var(--text-tertiary)'}}>
                  {col._statsLoading ? (
                    <>
                      <Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />
                      loading...
                    </>
                  ) : (typeof col.count === 'number' || typeof col.size === 'number') ? (
                    <>
                      {typeof col.count === 'number' ? formatNumber(col.count) : '?'}d
                      <span>|</span>
                      {typeof col.size === 'number' ? formatBytes(col.size) : '?'}
                    </>
                  ) : (
                    <>
                      <span>stats pending</span>
                    </>
                  )}
                </span>
              </button>
              <div className="relative">
                <button onClick={()=>setShowColMenu(showColMenu===col.name?null:col.name)}
                  className="opacity-0 group-hover/col:opacity-100 p-0.5 rounded transition-all hover:bg-[var(--surface-2)]" style={{color:'var(--text-tertiary)'}}>
                  <MoreVertical className="w-3 h-3" />
                </button>
                {showColMenu === col.name && (
                  <ContextMenu onClose={()=>setShowColMenu(null)} items={[
                    { label:'Import Documents', action:()=>openImportDocumentsDialog(col.name) },
                    { label:'Export Collection', action:()=>openExportCollectionDialog(col.name) },
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
      {expanded && loading && collections.length === 0 && (
        <div className="ml-9 py-1 text-2xs inline-flex items-center gap-1.5" style={{ color:'var(--text-tertiary)' }}>
          <Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />
          Loading collections...
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
      <InputDialog
        open={showImportColDialog}
        title={`Import Collection into "${db.name}"`}
        label="Collection Name"
        placeholder="imported_collection"
        initialValue={pendingCollectionImport?.name || ''}
        submitLabel={importBusy ? 'Importing…' : 'Import'}
        onCancel={() => {
          if (importBusy) return;
          setShowImportColDialog(false);
          setPendingCollectionImport(null);
        }}
        onSubmit={handleImportCollectionSubmit}
        busy={importBusy}
      />
      <input
        ref={colImportInputRef}
        type="file"
        accept=".json,application/json,text/json,.txt"
        className="hidden"
        onChange={handleImportCollectionFile}
      />
      <input
        ref={docsImportInputRef}
        type="file"
        accept=".json,application/json,text/json,.txt,.ndjson"
        className="hidden"
        disabled={docsImportBusy}
        onChange={handleImportDocumentsFile}
      />
      {showExportColDialog && (
        <div className="fixed inset-0 z-[220] flex items-center justify-center p-4">
          <button
            type="button"
            className="absolute inset-0 bg-black/50"
            onClick={() => !exportBusy && setShowExportColDialog(false)}
            aria-label="Close collection export dialog"
          />
          <div className="relative w-full max-w-sm rounded-xl p-4 animate-fade-in" style={{ background:'var(--surface-1)', border:'1px solid var(--border)' }}>
            <div className="flex items-center justify-between mb-2">
              <div className="text-sm font-semibold" style={{ color:'var(--text-primary)' }}>
                Export Collection
              </div>
              <button type="button" className="btn-ghost p-1.5" onClick={() => setShowExportColDialog(false)} disabled={exportBusy}>
                <X className="w-3.5 h-3.5" />
              </button>
            </div>
            <div className="text-2xs mb-3 font-mono" style={{ color:'var(--text-tertiary)' }}>
              {db.name}.{exportCollectionName}
            </div>
            <div className="space-y-2.5">
              <label className="block text-2xs" style={{ color:'var(--text-tertiary)' }}>Format</label>
              <select className="ms-select w-full text-xs" value={exportCollectionFormat} onChange={(event) => setExportCollectionFormat(event.target.value)}>
                <option value="json">JSON</option>
                <option value="csv">CSV</option>
              </select>
              <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
                <input type="checkbox" checked={exportUseVisibleFields} onChange={(event) => setExportUseVisibleFields(event.target.checked)} className="ms-checkbox" />
                Only visible fields (saved Columns/Fields)
              </label>
              <label className="block text-2xs" style={{ color:'var(--text-tertiary)' }}>Sort</label>
              <select className="ms-select w-full text-xs" value={exportSortMode} onChange={(event) => setExportSortMode(event.target.value)}>
                <option value="saved">Saved sort (from view)</option>
                <option value="id_desc">Newest first (_id desc)</option>
                <option value="none">No sort</option>
              </select>
            </div>
            <div className="mt-4 flex items-center justify-end gap-2">
              <button type="button" className="btn-ghost text-xs" onClick={() => setShowExportColDialog(false)} disabled={exportBusy}>
                Cancel
              </button>
              <button type="button" className="btn-primary text-xs px-3 py-1.5" onClick={handleExportCollection} disabled={exportBusy}>
                {exportBusy ? 'Exporting...' : 'Export'}
              </button>
            </div>
          </div>
        </div>
      )}
      <DatabaseExportDialog
        open={showDbExportDialog}
        title="Export Database"
        subtitle={db.name}
        busy={dbExportBusy}
        mode={dbExportMode}
        onModeChange={setDbExportMode}
        archive={dbExportArchive}
        onArchiveChange={setDbExportArchive}
        collectionFormat={dbExportCollectionFormat}
        onCollectionFormatChange={setDbExportCollectionFormat}
        includeIndexes={dbExportIncludeIndexes}
        onIncludeIndexesChange={setDbExportIncludeIndexes}
        includeSchema={dbExportIncludeSchema}
        onIncludeSchemaChange={setDbExportIncludeSchema}
        onCancel={() => {
          if (dbExportBusy) return;
          setShowDbExportDialog(false);
        }}
        onSubmit={handleExportDb}
      />
    </div>
  );
}

export default function Sidebar({ databases, selectedDb, selectedCol, onSelect, onRefresh, width, onWidthChange, loading, refreshToken = 0 }) {
  const [filter, setFilter] = useState('');
  const [dbSort, setDbSort] = useState('name');
  const [dbStats, setDbStats] = useState({});
  const [showCreateDb, setShowCreateDb] = useState(false);
  const [showDbSortMenu, setShowDbSortMenu] = useState(false);
  const [newDbName, setNewDbName] = useState('');
  const [error, setError] = useState('');
  const [info, setInfo] = useState('');
  const [showExportAllDialog, setShowExportAllDialog] = useState(false);
  const [exportAllBusy, setExportAllBusy] = useState(false);
  const [exportAllMode, setExportAllMode] = useState('package');
  const [exportAllArchive, setExportAllArchive] = useState(true);
  const [exportAllCollectionFormat, setExportAllCollectionFormat] = useState('json');
  const [exportAllIncludeIndexes, setExportAllIncludeIndexes] = useState(true);
  const [exportAllIncludeSchema, setExportAllIncludeSchema] = useState(true);
  const [selectedExportDbs, setSelectedExportDbs] = useState([]);
  const resizing = useRef(false);
  const userResizedRef = useRef(false);
  const didInitialAutofitRef = useRef(false);
  const textMeasureCanvasRef = useRef(null);
  const asideRef = useRef(null);
  const createDbRef = useRef(null);
  const searchRowRef = useRef(null);
  const dbListRef = useRef(null);
  const dbImportRef = useRef(null);
  const dbStatsLoadingRef = useRef(new Set());
  const dbStatsSeqRef = useRef(0);
  const dbStatsRef = useRef({});

  useEffect(() => {
    dbStatsRef.current = dbStats;
  }, [dbStats]);

  const ensureDbStats = useCallback(async (dbName, attempt = 0) => {
    if (!dbName) return;
    if (dbStatsLoadingRef.current.has(dbName)) return;
    const cached = dbStatsRef.current[dbName];
    if (cached && !cached._error) return;
    dbStatsLoadingRef.current.add(dbName);
    const seq = dbStatsSeqRef.current;
    try {
      const stats = await api.getDatabaseStats(dbName);
      if (seq !== dbStatsSeqRef.current) return;
      setDbStats((prev) => ({ ...prev, [dbName]: stats || { _error: true } }));
    } catch (err) {
      if (seq !== dbStatsSeqRef.current) return;
      const retryable = err?.status === 429 || err?.status >= 500 || err?.errorType === 'network';
      if (retryable && attempt < 3) {
        const delay = Math.min(900 * (attempt + 1), 4000);
        window.setTimeout(() => {
          if (seq !== dbStatsSeqRef.current) return;
          ensureDbStats(dbName, attempt + 1);
        }, delay);
        return;
      }
      setDbStats((prev) => ({ ...prev, [dbName]: { _error: true } }));
    } finally {
      dbStatsLoadingRef.current.delete(dbName);
    }
  }, []);

  useEffect(() => {
    dbStatsSeqRef.current += 1;
    dbStatsLoadingRef.current.clear();
    const names = new Set(databases.map((db) => db.name));
    setDbStats((prev) => {
      const next = {};
      Object.entries(prev).forEach(([name, value]) => {
        if (names.has(name)) next[name] = value;
      });
      return next;
    });
  }, [databases.map((db) => db.name).join('|'), refreshToken]);

  useEffect(() => {
    if (!databases.length) return;
    const MAX_EAGER_DB_STATS = 40;
    const targets = databases.slice(0, Math.min(databases.length, MAX_EAGER_DB_STATS)).map((db) => db.name);
    targets.forEach((dbName) => ensureDbStats(dbName));
  }, [databases, ensureDbStats, refreshToken]);

  const filteredDbs = useMemo(() => {
    const rows = databases
      .filter(db => db.name.toLowerCase().includes(filter.toLowerCase()))
      .map((db) => {
        const raw = Object.prototype.hasOwnProperty.call(dbStats, db.name) ? dbStats[db.name] : null;
        const hasStats = Boolean(raw && !raw._error);
        const hasError = Boolean(raw && raw._error);
        const stats = hasStats ? raw : null;
        return {
          ...db,
          collections: typeof stats?.collections === 'number' ? stats.collections : null,
          objects: typeof stats?.objects === 'number' ? stats.objects : null,
          _statsLoaded: hasStats,
          _statsError: hasError,
        };
      });

    const byName = (a, b) => a.name.localeCompare(b.name);
    if (dbSort === 'size') return [...rows].sort((a, b) => (b.sizeOnDisk || 0) - (a.sizeOnDisk || 0) || byName(a, b));
    if (dbSort === 'collections') return [...rows].sort((a, b) => (b.collections || 0) - (a.collections || 0) || byName(a, b));
    if (dbSort === 'documents') return [...rows].sort((a, b) => (b.objects || 0) - (a.objects || 0) || byName(a, b));
    return [...rows].sort(byName);
  }, [databases, filter, dbSort, dbStats]);

  const totals = useMemo(() => {
    const statsList = Object.values(dbStats);
    return {
      collections: statsList.reduce((sum, item) => sum + (item?.collections || 0), 0),
      documents: statsList.reduce((sum, item) => sum + (item?.objects || 0), 0),
      size: databases.reduce((sum, item) => sum + Number(item?.sizeOnDisk || 0), 0),
    };
  }, [dbStats, databases]);

  const dbStatsProgress = useMemo(() => {
    const loaded = databases.reduce(
      (sum, db) => sum + (Object.prototype.hasOwnProperty.call(dbStats, db.name) ? 1 : 0),
      0,
    );
    return { loaded, total: databases.length };
  }, [dbStats, databases]);

  useEffect(() => {
    if (!filteredDbs.length) return;
    filteredDbs.slice(0, 24).forEach((db) => ensureDbStats(db.name));
  }, [filteredDbs, ensureDbStats]);

  const getMaxSidebarWidth = useCallback(() => {
    const viewport = typeof window !== 'undefined' ? window.innerWidth : 1600;
    return Math.max(320, Math.min(980, viewport - 120));
  }, []);

  const measureTextWidth = useCallback((text, font) => {
    if (!text) return 0;
    try {
      if (!textMeasureCanvasRef.current) {
        textMeasureCanvasRef.current = document.createElement('canvas');
      }
      const ctx = textMeasureCanvasRef.current.getContext('2d');
      if (!ctx) return text.length * 7;
      ctx.font = font || '500 12px system-ui, -apple-system, Segoe UI, sans-serif';
      return ctx.measureText(text).width;
    } catch {
      return text.length * 7;
    }
  }, []);

  const getSidebarLabelFont = useCallback(() => {
    try {
      const probe = asideRef.current?.querySelector('[data-sidebar-label]');
      if (!probe) return '500 12px system-ui, -apple-system, Segoe UI, sans-serif';
      const styles = window.getComputedStyle(probe);
      return `${styles.fontWeight} ${styles.fontSize} ${styles.fontFamily}`;
    } catch {
      return '500 12px system-ui, -apple-system, Segoe UI, sans-serif';
    }
  }, []);

  const handleMouseDown = useCallback((e) => {
    e.preventDefault();
    userResizedRef.current = true;
    resizing.current = true;
    const startX = e.clientX, startW = width;
    const handleMove = (e) => {
      if (!resizing.current) return;
      const minWidth = 220;
      const maxWidth = getMaxSidebarWidth();
      onWidthChange(Math.max(minWidth, Math.min(maxWidth, startW + (e.clientX - startX))));
    };
    const handleUp = () => { resizing.current = false; document.removeEventListener('mousemove', handleMove); document.removeEventListener('mouseup', handleUp); };
    document.addEventListener('mousemove', handleMove);
    document.addEventListener('mouseup', handleUp);
  }, [width, onWidthChange, getMaxSidebarWidth]);

  const handleCreateDb = async () => {
    if (!newDbName.trim()) return;
    try {
      await api.createDatabase(newDbName.trim());
      setNewDbName('');
      setShowCreateDb(false);
      setError('');
      setInfo(`Database "${newDbName.trim()}" created.`);
      onRefresh();
    } catch(err) { setError(err.message); }
  };

  const handleGlobalImportDb = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      const suggested = parsed?.database?.name || guessNameFromFile(file.name, 'imported_db');
      const targetDb = window.prompt('Target database name', suggested);
      if (!targetDb || !targetDb.trim()) return;
      const replace = window.confirm(`Replace database "${targetDb.trim()}" before import?\nOK = replace, Cancel = merge.`);
      const mode = replace ? 'replace' : 'merge';
      const result = await api.importDatabase(parsed, { targetDb: targetDb.trim(), mode });
      setError('');
      setInfo(`Database "${targetDb.trim()}" imported.`);
      onRefresh?.();
      setShowCreateDb(false);
      setNewDbName('');
      if (result?.warnings?.length) setError(`Imported with warnings: ${result.warnings[0]}`);
    } catch (err) {
      setError(`Import failed: ${err.message}`);
    } finally {
      event.target.value = '';
    }
  };

  const openExportAllDialog = () => {
    setExportAllMode('package');
    setExportAllArchive(true);
    setExportAllCollectionFormat('json');
    setExportAllIncludeIndexes(true);
    setExportAllIncludeSchema(true);
    setSelectedExportDbs(databases.map((entry) => entry?.name).filter(Boolean));
    setShowExportAllDialog(true);
  };

  const handleExportAllDatabases = async () => {
    setExportAllBusy(true);
    try {
      const dbNames = [...new Set(selectedExportDbs.map((entry) => String(entry || '').trim()).filter(Boolean))];
      if (!dbNames.length) {
        throw new Error('Select at least one database to export.');
      }
      const result = await exportMultipleDatabases(dbNames, {
        mode: exportAllMode,
        archive: exportAllArchive,
        collectionFormat: exportAllCollectionFormat,
        includeIndexes: exportAllIncludeIndexes,
        includeSchema: exportAllIncludeSchema,
        archiveName: 'mongostudio-all-databases',
      });
      setError('');
      setInfo(`Exported ${result.databases} database${result.databases === 1 ? '' : 's'} (${result.files} file${result.files === 1 ? '' : 's'}${result.archive ? ', zip' : ''}).`);
      setShowExportAllDialog(false);
    } catch (err) {
      setInfo('');
      setError(err.message);
    } finally {
      setExportAllBusy(false);
    }
  };

  useEffect(() => {
    if (!showCreateDb && !showDbSortMenu) return undefined;
    const onMouseDown = (event) => {
      if (createDbRef.current && !createDbRef.current.contains(event.target)) {
        setShowCreateDb(false);
        setShowDbSortMenu(false);
      }
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        setShowCreateDb(false);
        setShowDbSortMenu(false);
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [showCreateDb, showDbSortMenu]);

  useEffect(() => {
    if (!Array.isArray(databases) || databases.length === 0) return;
    if (didInitialAutofitRef.current || userResizedRef.current) return;
    didInitialAutofitRef.current = true;
    let active = true;
    (async () => {
      const names = databases.map((db) => String(db?.name || ''));
      const dbForSizing = selectedDb || databases[0]?.name;
      if (dbForSizing) {
        try {
          const response = await api.listCollections(dbForSizing, { withStats: false });
          const collectionNames = (response.collections || []).slice(0, 120).map((entry) => String(entry?.name || ''));
          names.push(...collectionNames);
        } catch {}
      }
      const longest = names.reduce((best, name) => (name.length > best.length ? name : best), '');
      const font = getSidebarLabelFont();
      const measuredLabelWidth = Math.ceil(measureTextWidth(longest, font));
      const minWidth = 220;
      const maxWidth = getMaxSidebarWidth();
      const controlsWidth = Math.ceil((searchRowRef.current?.scrollWidth || 220) + 18);
      const rowWidth = measuredLabelWidth + 220;
      const desired = Math.ceil(Math.max(minWidth, controlsWidth, rowWidth));
      const next = Math.max(minWidth, Math.min(maxWidth, desired));
      if (active && !userResizedRef.current && Math.abs(next - width) >= 2) {
        onWidthChange(next);
      }
    })();
    return () => { active = false; };
  }, [databases, selectedDb, getMaxSidebarWidth, getSidebarLabelFont, measureTextWidth, onWidthChange, width]);

  useEffect(() => {
    const clamp = () => {
      const minWidth = 220;
      const maxWidth = getMaxSidebarWidth();
      if (width < minWidth) {
        onWidthChange(minWidth);
        return;
      }
      if (width > maxWidth) {
        onWidthChange(maxWidth);
      }
    };
    if (typeof window !== 'undefined') {
      window.addEventListener('resize', clamp, { passive: true });
    }
    return () => {
      if (typeof window !== 'undefined') {
        window.removeEventListener('resize', clamp);
      }
    };
  }, [getMaxSidebarWidth, onWidthChange, width]);

  useEffect(() => {
    const names = new Set(databases.map((entry) => entry?.name).filter(Boolean));
    setSelectedExportDbs((prev) => prev.filter((name) => names.has(name)));
  }, [databases.map((entry) => entry?.name).join('|')]);

  return (
    <aside ref={asideRef} className="flex-shrink-0 flex flex-col relative overflow-hidden" style={{width:`${width}px`,borderRight:'1px solid var(--border)',background:'var(--surface-1)'}}>
      {/* Search + Create */}
      <div className="relative z-20 p-2 overflow-visible" style={{borderBottom:'1px solid var(--border)'}} ref={createDbRef}>
        <div className="flex items-center gap-1" ref={searchRowRef}>
          <div className="relative flex-1">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} />
            <input type="text" value={filter} onChange={e=>setFilter(e.target.value)} placeholder="Filter databases…"
              className="w-full rounded-lg pl-8 pr-3 py-1.5 text-xs transition-all"
              style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}} />
          </div>
          <button onClick={() => dbImportRef.current?.click()} className="btn-ghost p-1.5" title="Import Database Package" style={{color:'var(--text-secondary)'}}>
            <Upload className="w-3.5 h-3.5" />
          </button>
          <div className="relative">
            <button onClick={() => setShowDbSortMenu((prev) => !prev)} className="btn-ghost p-1.5" title="Sort databases" style={{color:'var(--text-secondary)'}}>
              <Filter className="w-3.5 h-3.5" />
            </button>
            {showDbSortMenu && (
              <div className="absolute right-0 top-full mt-1 z-[180] rounded-lg shadow-lg py-1 min-w-[168px] animate-fade-in" style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}>
                <div className="px-3 py-1 text-2xs uppercase tracking-wider" style={{ color:'var(--text-tertiary)' }}>
                  Sort Databases
                </div>
                {DB_SORT_OPTIONS.map((option) => (
                  <button
                    key={option.value}
                    onClick={() => {
                      setDbSort(option.value);
                      setShowDbSortMenu(false);
                    }}
                    className="w-full text-left px-3 py-1.5 text-xs flex items-center justify-between transition-colors"
                    style={{ color:'var(--text-secondary)' }}
                    onMouseOver={(event) => { event.currentTarget.style.background = 'var(--surface-4)'; }}
                    onMouseOut={(event) => { event.currentTarget.style.background = 'transparent'; }}
                  >
                    <span>{option.label}</span>
                    {dbSort === option.value && <Check className="w-3.5 h-3.5" style={{ color:'var(--accent)' }} />}
                  </button>
                ))}
              </div>
            )}
          </div>
          <button onClick={()=>{setShowCreateDb(!showCreateDb); setShowDbSortMenu(false);}} className="btn-ghost p-1.5" title="Create Database" style={{color:'var(--accent)'}}>
            <Plus className="w-3.5 h-3.5" />
          </button>
        </div>
        <input
          ref={dbImportRef}
          type="file"
          accept=".json,application/json,text/json,.txt"
          className="hidden"
          onChange={handleGlobalImportDb}
        />
        {showCreateDb && (
          <div data-sidebar-row className="mt-2 grid grid-cols-[minmax(0,1fr)_auto_auto] items-center gap-1 animate-slide-up">
            <input type="text" value={newDbName} onChange={e=>setNewDbName(e.target.value)} onKeyDown={e=>e.key==='Enter'&&handleCreateDb()}
              placeholder="Database name…" autoFocus
              className="min-w-0 rounded-lg px-2.5 py-1.5 text-xs font-mono"
              style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}} />
            <button onClick={handleCreateDb} disabled={!newDbName.trim()} className="text-2xs px-2 py-1.5 rounded-lg font-medium disabled:opacity-30 whitespace-nowrap" style={{color:'var(--accent)'}}>Create</button>
            <button onClick={()=>{setShowCreateDb(false);setNewDbName('')}} className="p-1 flex items-center justify-center" style={{color:'var(--text-tertiary)'}}><X className="w-3 h-3" /></button>
          </div>
        )}
        {error && (
          <div className="mt-2 flex items-start gap-2 text-red-400 text-2xs p-2 rounded-lg" style={{background:'rgba(239,68,68,0.08)',border:'1px solid rgba(239,68,68,0.2)'}}>
            <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
            <span className="flex-1">{error}</span>
            <button onClick={() => setError('')}><X className="w-3 h-3 text-red-300/70" /></button>
          </div>
        )}
        {info && (
          <div className="mt-2 flex items-start gap-2 text-emerald-400 text-2xs p-2 rounded-lg" style={{background:'rgba(16,185,129,0.08)',border:'1px solid rgba(16,185,129,0.2)'}}>
            <Check className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
            <span className="flex-1">{info}</span>
            <button onClick={() => setInfo('')}><X className="w-3 h-3 text-emerald-300/70" /></button>
          </div>
        )}
      </div>

      {/* Database List */}
      <div ref={dbListRef} className="flex-1 overflow-y-auto overflow-x-hidden p-2 space-y-0.5">
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
          <DbItem
            key={db.name}
            db={db}
            selectedDb={selectedDb}
            selectedCol={selectedCol}
            onSelect={onSelect}
            onRefresh={onRefresh}
            refreshToken={refreshToken}
            onEnsureStats={ensureDbStats}
            onError={(message) => { setInfo(''); setError(message); }}
            onSuccess={(message) => { setError(''); setInfo(message); }}
          />
        ))}
      </div>

      {/* Footer */}
      <div className="p-2 flex items-center justify-between" style={{borderTop:'1px solid var(--border)'}}>
        <div className="text-2xs leading-tight" style={{color:'var(--text-tertiary)'}}>
          <div>{filteredDbs.length} shown / {databases.length} db</div>
          <div className="inline-flex items-center gap-1">
            {dbStatsProgress.loaded < dbStatsProgress.total ? (
              <>
                stats {dbStatsProgress.loaded}/{dbStatsProgress.total} loaded
              </>
            ) : (
              <>{formatNumber(totals.collections)} cols | {formatNumber(totals.documents)} docs</>
            )}
          </div>
          <div>{formatBytes(totals.size)} total</div>
        </div>
        <div className="flex items-center gap-1">
          <button
            onClick={openExportAllDialog}
            className="btn-ghost p-1"
            title="Export all databases"
            disabled={databases.length === 0}
          >
            <Download className="w-3.5 h-3.5" />
          </button>
          <button onClick={onRefresh} className="btn-ghost p-1" title="Refresh">
            <Refresh className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      <DatabaseExportDialog
        open={showExportAllDialog}
        title="Export All Databases"
        subtitle={`${selectedExportDbs.length} selected / ${databases.length} total`}
        busy={exportAllBusy}
        mode={exportAllMode}
        onModeChange={setExportAllMode}
        archive={exportAllArchive}
        onArchiveChange={setExportAllArchive}
        collectionFormat={exportAllCollectionFormat}
        onCollectionFormatChange={setExportAllCollectionFormat}
        includeIndexes={exportAllIncludeIndexes}
        onIncludeIndexesChange={setExportAllIncludeIndexes}
        includeSchema={exportAllIncludeSchema}
        onIncludeSchemaChange={setExportAllIncludeSchema}
        items={databases.map((entry) => entry?.name).filter(Boolean)}
        selectedItems={selectedExportDbs}
        itemsLabel="Databases"
        onToggleItem={(name) => {
          setSelectedExportDbs((prev) => (
            prev.includes(name) ? prev.filter((item) => item !== name) : [...prev, name]
          ));
        }}
        onSelectAll={() => setSelectedExportDbs(databases.map((entry) => entry?.name).filter(Boolean))}
        onClearAll={() => setSelectedExportDbs([])}
        onCancel={() => {
          if (exportAllBusy) return;
          setShowExportAllDialog(false);
        }}
        onSubmit={handleExportAllDatabases}
      />

      {/* Resize Handle */}
      <div className="absolute right-0 top-0 bottom-0 w-1 cursor-col-resize z-10 transition-colors"
        style={{background:'transparent'}}
        onMouseDown={handleMouseDown}
        onMouseOver={e=>e.currentTarget.style.background='var(--accent)33'}
        onMouseOut={e=>e.currentTarget.style.background='transparent'} />
    </aside>
  );
}


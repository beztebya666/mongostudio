import React, { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import api from '../utils/api';
import {
  Filter, Refresh, Plus, Trash, Edit, ChevronLeft, ChevronRight, ChevronDown,
  Download, Copy, Loader, AlertCircle, Check, X, Zap, Table, FileJson, Columns, ArrowUp, ArrowDown, Eye
} from './Icons';
import { formatNumber, formatBytes, safeJsonParse, prettyJson } from '../utils/formatters';
import JsonView from './JsonView';
import DocumentEditor from './DocumentEditor';
import IndexesView from './IndexesView';
import DatabaseExportDialog from './modals/DatabaseExportDialog';
import { exportSingleDatabase } from '../utils/exportUtils';

export default function CollectionView({ db, collection, onQueryMs, showIndexes, refreshToken = 0 }) {
  const [documents, setDocuments] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(0);
  const [limit, setLimit] = useState(50);
  const [filter, setFilter] = useState('{}');
  const [filterInput, setFilterInput] = useState('{}');
  const [sort, setSort] = useState('{"_id":-1}');
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
  const [showExportDialog, setShowExportDialog] = useState(false);
  const [pendingExportFormat, setPendingExportFormat] = useState('json');
  const [exportUseVisibleFields, setExportUseVisibleFields] = useState(false);
  const [exportUseCurrentSort, setExportUseCurrentSort] = useState(true);
  const [exportUseCurrentFilter, setExportUseCurrentFilter] = useState(false);
  const [showDbExportDialog, setShowDbExportDialog] = useState(false);
  const [dbExportMode, setDbExportMode] = useState('package');
  const [dbExportArchive, setDbExportArchive] = useState(true);
  const [dbExportCollectionFormat, setDbExportCollectionFormat] = useState('json');
  const [dbExportIncludeIndexes, setDbExportIncludeIndexes] = useState(true);
  const [dbExportIncludeSchema, setDbExportIncludeSchema] = useState(true);
  const [showSortMenu, setShowSortMenu] = useState(false);
  const [showColumnsMenu, setShowColumnsMenu] = useState(false);
  const [showInlineFieldsMenu, setShowInlineFieldsMenu] = useState(false);
  const [visibleColumns, setVisibleColumns] = useState([]);
  const [jsonPreviewKeys, setJsonPreviewKeys] = useState([]);
  const [expandedObjectFields, setExpandedObjectFields] = useState([]);
  const [filterSuggestions, setFilterSuggestions] = useState([]);
  const [showFilterAutofill, setShowFilterAutofill] = useState(false);
  const [filterAutofillLimit, setFilterAutofillLimit] = useState(8);
  const [placeholderIndex, setPlaceholderIndex] = useState(0);
  const [slow, setSlow] = useState(false);
  const [execConfig, setExecConfig] = useState(null);
  const exportMenuRef = useRef(null);
  const sortMenuRef = useRef(null);
  const columnsMenuRef = useRef(null);
  const inlineFieldsMenuRef = useRef(null);
  const filterAutofillRef = useRef(null);
  const columnsStorageKey = `mongostudio_columns:${db}.${collection}`;
  const jsonPreviewStorageKey = `mongostudio_preview:${db}.${collection}`;
  const inlineFieldsStorageKey = `mongostudio_inline_fields:${db}.${collection}`;
  const sortStorageKey = `mongostudio_sort:${db}.${collection}`;

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
    setLimit(50);
    setFilter('{}');
    setFilterInput('');
    setSort('{"_id":-1}');
    setSelectedDoc(null);
    setEditingDoc(null);
    setInsertMode(false);
    setExportFormat(false);
    setShowExportDialog(false);
    setPendingExportFormat('json');
    setExportUseVisibleFields(false);
    setExportUseCurrentSort(true);
    setExportUseCurrentFilter(false);
    setShowDbExportDialog(false);
    setDbExportMode('package');
    setDbExportArchive(true);
    setDbExportCollectionFormat('json');
    setDbExportIncludeIndexes(true);
    setDbExportIncludeSchema(true);
    setShowSortMenu(false);
    setShowColumnsMenu(false);
    setShowInlineFieldsMenu(false);
    setVisibleColumns([]);
    setJsonPreviewKeys([]);
    setExpandedObjectFields([]);
    setFilterSuggestions([]);
    setShowFilterAutofill(false);
    setFilterAutofillLimit(8);
    setPlaceholderIndex(0);
    setExpandedRows(new Set());
  }, [db, collection]);
  useEffect(() => { loadDocuments(page); }, [page, filter, sort, db, collection]);
  useEffect(() => { loadStats(); }, [loadStats]);
  useEffect(() => {
    let active = true;
    api.getExecutionConfig()
      .then((config) => {
        if (!active) return;
        setExecConfig(config || null);
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
  useEffect(() => {
    if (refreshToken <= 0) return;
    if (!db || !collection) return;
    loadDocuments(page);
    loadStats();
  }, [refreshToken]);
  useEffect(() => {
    if (!exportFormat && !showSortMenu && !showColumnsMenu && !showInlineFieldsMenu && !showFilterAutofill) return;
    const onMouseDown = (event) => {
      if (exportMenuRef.current && !exportMenuRef.current.contains(event.target)) setExportFormat(false);
      if (sortMenuRef.current && !sortMenuRef.current.contains(event.target)) setShowSortMenu(false);
      if (columnsMenuRef.current && !columnsMenuRef.current.contains(event.target)) setShowColumnsMenu(false);
      if (inlineFieldsMenuRef.current && !inlineFieldsMenuRef.current.contains(event.target)) setShowInlineFieldsMenu(false);
      if (showFilterAutofill && filterAutofillRef.current && !filterAutofillRef.current.contains(event.target)) setShowFilterAutofill(false);
    };
    document.addEventListener('mousedown', onMouseDown);
    return () => document.removeEventListener('mousedown', onMouseDown);
  }, [exportFormat, showSortMenu, showColumnsMenu, showInlineFieldsMenu, showFilterAutofill]);

  useEffect(() => {
    let active = true;
    api.getSchema(db, collection, 120)
      .then((data) => {
        if (!active) return;
        const fields = (data.fields || []).map((f) => f.path).filter(Boolean).slice(0, 30);
        const hints = [];
        for (const field of fields) {
          hints.push(`{ "${field}": "" }`);
          hints.push(`{ "${field}": { "$exists": true } }`);
          hints.push(`{ "${field}": { "$in": [] } }`);
        }
        hints.push('{ "$or": [] }');
        setFilterSuggestions([...new Set(hints)].slice(0, 80));
      })
      .catch(() => {
        if (active) setFilterSuggestions(['{ "_id": "" }', '{ "$or": [] }']);
      });
    return () => { active = false; };
  }, [db, collection, refreshToken]);

  const handleApplyFilter = () => {
    const normalizedFilter = filterInput.trim() || '{}';
    const { error } = safeJsonParse(normalizedFilter);
    if (error) { setError(`Invalid filter JSON: ${error}`); return; }
    setFilter(normalizedFilter);
    setPage(0);
    if (normalizedFilter === '{}') setFilterInput('');
    setShowFilterAutofill(false);
  };

  const handleDelete = async (doc) => {
    const id = docId(doc);
    if (!id) { setError('Cannot delete document without _id'); return; }
    try { await api.deleteDocument(db, collection, id); setConfirmDelete(null); loadDocuments(); }
    catch (err) { setError(err.message); }
  };

  const handleCopy = (doc, key) => { navigator.clipboard.writeText(prettyJson(doc)); setCopied(key); setTimeout(() => setCopied(null), 2000); };
  const toggleRow = (id) => { setExpandedRows(prev => { const n=new Set(prev); if(n.has(id)) n.delete(id); else n.add(id); return n; }); };

  const downloadText = (filename, text, mime) => {
    const blob = new Blob([text], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  const openCollectionExportDialog = (format) => {
    setPendingExportFormat(format);
    setExportUseVisibleFields(false);
    setExportUseCurrentSort(true);
    setExportUseCurrentFilter(false);
    setShowExportDialog(true);
    setExportFormat(false);
  };

  const handleExport = async (format, options = {}) => {
    setExporting(true);
    try {
      const useVisibleFields = Boolean(options.useVisibleFields);
      const includeSort = options.includeSort !== false;
      const useCurrentFilter = Boolean(options.useCurrentFilter);
      if (useVisibleFields && exportVisibleFields.length === 0) {
        setError('No visible fields selected for export.');
        setExporting(false);
        return;
      }
      const projection = useVisibleFields
        ? JSON.stringify(Object.fromEntries(exportVisibleFields.map((field) => [field, 1])))
        : '{}';
      const data = await api.exportData(db, collection, {
        format,
        filter: useCurrentFilter ? filter : '{}',
        sort: includeSort ? sort : '{}',
        limit: 50000,
        projection,
      });
      downloadText(`${collection}.${format}`, data.data, format === 'csv' ? 'text/csv' : 'application/json');
      setExportFormat(false);
      setShowExportDialog(false);
    } catch (err) { setError(err.message); }
    finally { setExporting(false); }
  };

  const openDbExportDialog = () => {
    setDbExportMode('package');
    setDbExportArchive(true);
    setDbExportCollectionFormat('json');
    setDbExportIncludeIndexes(true);
    setDbExportIncludeSchema(true);
    setShowDbExportDialog(true);
    setExportFormat(false);
  };

  const handleExportDatabase = async () => {
    setExporting(true);
    try {
      await exportSingleDatabase(db, {
        mode: dbExportMode,
        archive: dbExportArchive,
        collectionFormat: dbExportCollectionFormat,
        includeIndexes: dbExportIncludeIndexes,
        includeSchema: dbExportIncludeSchema,
      });
      setShowDbExportDialog(false);
      setExportFormat(false);
    } catch (err) {
      setError(err.message);
    } finally {
      setExporting(false);
    }
  };

  const handleToggleColumn = (key) => {
    setVisibleColumns((prev) => {
      const exists = prev.includes(key);
      if (exists) {
        if (prev.length <= 1) return prev;
        return prev.filter((item) => item !== key);
      }
      const next = [...prev, key];
      return allKeys.filter((item) => next.includes(item));
    });
  };
  const handleToggleJsonPreviewKey = (key) => {
    setJsonPreviewKeys((prev) => {
      const exists = prev.includes(key);
      if (exists) {
        if (prev.length <= 1) return prev;
        return prev.filter((item) => item !== key);
      }
      const next = [...prev, key];
      return jsonSelectableKeys.filter((item) => next.includes(item));
    });
  };
  const handleToggleExpandedObjectField = (key) => {
    setExpandedObjectFields((prev) => {
      if (prev.includes(key)) return prev.filter((item) => item !== key);
      return [...prev, key];
    });
  };

  const safeResultLimit = useMemo(() => {
    if (!execConfig || execConfig.mode !== 'safe') return 50000;
    const raw = Number(execConfig.maxResultSize || 50);
    return Math.max(1, Math.min(raw, 50000));
  }, [execConfig]);
  const maxPageSize = execConfig?.mode === 'safe' ? safeResultLimit : 5000;
  const pageSizeOptions = useMemo(() => {
    const base = [25, 50, 100, 200, 500, 1000, 2000, 5000];
    const allowed = base.filter((value) => value <= maxPageSize);
    if (!allowed.length || allowed[allowed.length - 1] !== maxPageSize) allowed.push(maxPageSize);
    return [...new Set(allowed)].sort((a, b) => a - b);
  }, [maxPageSize]);
  useEffect(() => {
    if (limit <= maxPageSize) return;
    setLimit(maxPageSize);
    setPage(0);
  }, [limit, maxPageSize]);
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
  const allKeys = [...new Set(documents.flatMap((doc) => Object.keys(doc)))];
  const jsonSelectableKeys = [...allKeys];
  const visibleSet = new Set(visibleColumns);
  const tableColumns = viewMode === 'table'
    ? (visibleColumns.length > 0 ? allKeys.filter((key) => visibleSet.has(key)) : allKeys)
    : [];
  const defaultJsonPreviewKeys = jsonSelectableKeys.filter((key) => key !== '_id');
  const jsonPreviewVisibleKeys = jsonPreviewKeys.length > 0
    ? jsonSelectableKeys.filter((key) => jsonPreviewKeys.includes(key))
    : (defaultJsonPreviewKeys.length > 0 ? defaultJsonPreviewKeys.slice(0, 4) : jsonSelectableKeys.slice(0, 4));
  const inlineVisibleKeys = viewMode === 'table'
    ? tableColumns
    : jsonPreviewVisibleKeys;
  const inlineHintThreshold = viewMode === 'table' ? 24 : 16;
  const inlineFieldKeys = useMemo(() => (
    inlineVisibleKeys.filter((key) => documents.some((doc) => {
      const value = doc?.[key];
      if (value === null || value === undefined) return false;
      if (typeof value === 'object') return true;
      const text = String(value);
      return text.length >= inlineHintThreshold;
    }))
  ), [inlineVisibleKeys, documents, inlineHintThreshold]);
  const exportVisibleFields = viewMode === 'table'
    ? tableColumns
    : jsonPreviewVisibleKeys;
  const hiddenColumnsCount = Math.max(allKeys.length - tableColumns.length, 0);
  const hiddenJsonFieldsCount = Math.max(jsonSelectableKeys.length - jsonPreviewVisibleKeys.length, 0);
  const activeSort = useMemo(() => {
    const parsed = safeJsonParse(sort || '{}').value;
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return { field: '_id', dir: -1 };
    const entries = Object.entries(parsed);
    if (entries.length === 0) return { field: '_id', dir: -1 };
    const [field, dirRaw] = entries[0];
    const dir = Number(dirRaw) === -1 ? -1 : 1;
    return { field, dir };
  }, [sort]);
  const sortCandidates = useMemo(() => {
    if (viewMode === 'json') {
      return [...new Set(['_id', ...(jsonPreviewVisibleKeys.length > 0 ? jsonPreviewVisibleKeys : ['_id'])])];
    }
    return [...new Set(['_id', ...tableColumns])];
  }, [viewMode, jsonPreviewVisibleKeys, tableColumns]);
  const setSortField = (field, dir = 1) => {
    setSort(JSON.stringify({ [field]: dir === -1 ? -1 : 1 }));
    setPage(0);
  };
  const toggleSortField = (field) => {
    if (activeSort.field !== field) {
      setSortField(field, field === '_id' ? -1 : 1);
      return;
    }
    if (activeSort.dir === 1) {
      setSortField(field, -1);
      return;
    }
    setSort(JSON.stringify({ _id: -1 }));
    setPage(0);
  };
  const formatInlineValue = (value) => {
    if (value === null) return 'null';
    if (value === undefined) return 'undefined';
    if (typeof value === 'string') return value;
    if (value && typeof value === 'object' && value.$oid) return value.$oid;
    if (value && typeof value === 'object' && value.$date) {
      return typeof value.$date === 'string' ? value.$date : String(value.$date);
    }
    if (typeof value === 'number' || typeof value === 'boolean') return String(value);
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  };
  useEffect(() => {
    if (sortCandidates.length === 0) return;
    if (!sortCandidates.includes(activeSort.field)) {
      const fallback = sortCandidates.includes('_id') ? '_id' : sortCandidates[0];
      setSortField(fallback, fallback === '_id' ? -1 : 1);
    }
  }, [sortCandidates, activeSort.field]);

  const normalizedFilterInput = filterInput.trim().toLowerCase();
  const filteredSuggestions = useMemo(() => {
    const source = filterSuggestions.length > 0 ? filterSuggestions : ['{ "_id": "" }', '{ "$or": [] }'];
    if (!normalizedFilterInput || normalizedFilterInput === '{}') return source;
    const matching = source.filter((hint) => hint.toLowerCase().includes(normalizedFilterInput));
    return matching.length > 0 ? matching : source;
  }, [filterSuggestions, normalizedFilterInput]);
  const visibleSuggestions = filteredSuggestions.slice(0, filterAutofillLimit);
  const hasMoreSuggestions = filteredSuggestions.length > visibleSuggestions.length;
  const filterPlaceholder = useMemo(() => {
    if (filteredSuggestions.length === 0) return '{ "field": "value" }';
    const idx = placeholderIndex % filteredSuggestions.length;
    return filteredSuggestions[idx];
  }, [filteredSuggestions, placeholderIndex]);

  useEffect(() => {
    if (filteredSuggestions.length <= 1) return undefined;
    const timer = setInterval(() => {
      setPlaceholderIndex((prev) => (prev + 1) % filteredSuggestions.length);
    }, 3200);
    return () => clearInterval(timer);
  }, [filteredSuggestions.length]);

  const selectFilterSuggestion = (hint, applyNow = false) => {
    setFilterInput(hint);
    setShowFilterAutofill(false);
    if (applyNow) {
      setFilter(hint);
      setPage(0);
    }
  };

  useEffect(() => {
    if (!db || !collection) return;
    try {
      const raw = localStorage.getItem(columnsStorageKey);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) setVisibleColumns(parsed.filter((item) => typeof item === 'string'));
    } catch {}
  }, [columnsStorageKey, db, collection]);
  useEffect(() => {
    if (!db || !collection) return;
    try {
      const raw = localStorage.getItem(jsonPreviewStorageKey);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) setJsonPreviewKeys(parsed.filter((item) => typeof item === 'string'));
    } catch {}
  }, [jsonPreviewStorageKey, db, collection]);
  useEffect(() => {
    if (!db || !collection) return;
    try {
      const raw = localStorage.getItem(inlineFieldsStorageKey);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) setExpandedObjectFields(parsed.filter((item) => typeof item === 'string'));
    } catch {}
  }, [inlineFieldsStorageKey, db, collection]);
  useEffect(() => {
    if (!db || !collection) return;
    try {
      const raw = localStorage.getItem(sortStorageKey);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return;
      const entries = Object.entries(parsed);
      if (entries.length === 0) return;
      const [field, dirRaw] = entries[0];
      const dir = Number(dirRaw) === -1 ? -1 : 1;
      setSort(JSON.stringify({ [field]: dir }));
    } catch {}
  }, [sortStorageKey, db, collection]);

  useEffect(() => {
    if (allKeys.length === 0) return;
    setVisibleColumns((prev) => {
      const next = prev.filter((key) => allKeys.includes(key));
      if (next.length > 0) return next;
      const fallback = allKeys.slice(0, Math.min(10, allKeys.length));
      if (allKeys.includes('_id') && !fallback.includes('_id')) fallback.unshift('_id');
      return fallback;
    });
  }, [allKeys.join('|')]);
  useEffect(() => {
    if (jsonSelectableKeys.length === 0) return;
    setJsonPreviewKeys((prev) => {
      const next = prev.filter((key) => jsonSelectableKeys.includes(key));
      if (next.length > 0) return next;
      const fallback = jsonSelectableKeys.filter((key) => key !== '_id');
      if (fallback.length > 0) return fallback.slice(0, Math.min(4, fallback.length));
      return jsonSelectableKeys.slice(0, Math.min(4, jsonSelectableKeys.length));
    });
  }, [jsonSelectableKeys.join('|')]);
  useEffect(() => {
    setExpandedObjectFields((prev) => prev.filter((key) => inlineFieldKeys.includes(key)));
  }, [inlineFieldKeys.join('|')]);
  useEffect(() => {
    if (!db || !collection || visibleColumns.length === 0) return;
    try { localStorage.setItem(columnsStorageKey, JSON.stringify(visibleColumns)); } catch {}
  }, [columnsStorageKey, db, collection, visibleColumns]);
  useEffect(() => {
    if (!db || !collection || jsonPreviewKeys.length === 0) return;
    try { localStorage.setItem(jsonPreviewStorageKey, JSON.stringify(jsonPreviewKeys)); } catch {}
  }, [jsonPreviewStorageKey, db, collection, jsonPreviewKeys]);
  useEffect(() => {
    if (!db || !collection) return;
    try { localStorage.setItem(inlineFieldsStorageKey, JSON.stringify(expandedObjectFields)); } catch {}
  }, [inlineFieldsStorageKey, db, collection, expandedObjectFields]);
  useEffect(() => {
    if (!db || !collection || !sort) return;
    try { localStorage.setItem(sortStorageKey, sort); } catch {}
  }, [sortStorageKey, db, collection, sort]);

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
          <div className="relative flex-1 min-w-[220px]" ref={filterAutofillRef}>
	            <div className="flex items-center gap-2">
	              <button
	                type="button"
	                onClick={() => {
	                  setShowFilterAutofill((prev) => !prev);
	                  setFilterAutofillLimit(8);
	                }}
	                className="p-1 rounded-md transition-colors hover:bg-[var(--surface-2)]"
	                title="Filter suggestions"
	              >
	                <Filter className="w-3.5 h-3.5 flex-shrink-0" style={{color:'var(--text-tertiary)'}} />
	              </button>
	              <input
	                type="text"
	                value={filterInput}
	                onChange={(event) => {
	                  const next = event.target.value;
	                  setFilterInput(next);
	                  if (next.trim().length > 0) {
	                    setShowFilterAutofill(true);
	                    setFilterAutofillLimit(8);
	                  } else {
	                    setShowFilterAutofill(false);
	                  }
	                }}
	                onKeyDown={(event) => {
                  if (event.key === 'Enter') {
                    event.preventDefault();
                    handleApplyFilter();
                  }
                  if (event.key === 'Tab' && showFilterAutofill && visibleSuggestions.length > 0) {
                    event.preventDefault();
                    selectFilterSuggestion(visibleSuggestions[0]);
                  }
                }}
                placeholder={filterPlaceholder}
                spellCheck={false}
                className="w-full bg-transparent border-none text-xs font-mono focus:outline-none"
                style={{color:'var(--text-primary)'}}
              />
            </div>
            {showFilterAutofill && visibleSuggestions.length > 0 && (
              <div
                className="absolute left-0 right-0 top-full mt-1 z-50 rounded-lg shadow-lg py-1 max-h-64 overflow-auto animate-fade-in"
                style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}
              >
                {visibleSuggestions.map((hint) => (
                  <button
                    key={hint}
                    onClick={() => selectFilterSuggestion(hint)}
                    className="w-full text-left px-3 py-1.5 text-xs font-mono truncate transition-colors"
                    style={{ color:'var(--text-secondary)' }}
                    title={hint}
                    onMouseOver={(event) => { event.currentTarget.style.background = 'var(--surface-4)'; }}
                    onMouseOut={(event) => { event.currentTarget.style.background = 'transparent'; }}
                  >
                    {hint}
                  </button>
                ))}
                {hasMoreSuggestions && (
                  <>
                    <div style={{ borderTop:'1px solid var(--border)' }} />
                    <button
                      onClick={() => setFilterAutofillLimit((prev) => prev + 8)}
                      className="w-full text-left px-3 py-1.5 text-2xs transition-colors"
                      style={{ color:'var(--accent)' }}
                      onMouseOver={(event) => { event.currentTarget.style.background = 'var(--surface-4)'; }}
                      onMouseOut={(event) => { event.currentTarget.style.background = 'transparent'; }}
                    >
                      More...
                    </button>
                  </>
                )}
              </div>
            )}
          </div>
          <button onClick={handleApplyFilter} className="btn-ghost text-2xs font-medium" style={{color:'var(--accent)'}}>Apply</button>
          <div className="w-px h-5" style={{background:'var(--border)'}} />

          {stats && (
            <div className="hidden lg:flex items-center gap-2 text-2xs" style={{color:'var(--text-tertiary)'}}>
              <span>{formatNumber(stats.count)} docs</span>
              <span style={{color:'var(--border)'}}>·</span>
              <span>{formatBytes(stats.size || 0)}</span>
              <span style={{color:'var(--border)'}}>·</span>
              <span>avg {formatBytes(Math.round(stats.avgObjSize || 0))}</span>
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

          <div className="relative" ref={sortMenuRef}>
            <button
              onClick={() => {
                setShowSortMenu((prev) => !prev);
                setShowColumnsMenu(false);
                setShowInlineFieldsMenu(false);
              }}
              className="btn-ghost flex items-center gap-1"
              style={{ color:'var(--text-secondary)' }}
              title="Sort documents"
            >
              {activeSort.dir === -1 ? <ArrowDown className="w-3.5 h-3.5" /> : <ArrowUp className="w-3.5 h-3.5" />}
              <span className="hidden md:inline text-xs max-w-[120px] truncate">{activeSort.field}</span>
            </button>
            {showSortMenu && (
              <div className="absolute right-0 top-full mt-1 z-50 rounded-lg shadow-lg py-2 w-64 animate-fade-in" style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}>
                <div className="px-3 pb-1 text-2xs uppercase tracking-wider" style={{ color:'var(--text-tertiary)' }}>
                  Sort By
                </div>
                <div className="max-h-64 overflow-auto px-2 space-y-0.5">
                  {sortCandidates.map((field) => (
                    <button
                      key={field}
                      className="w-full text-left px-2 py-1.5 rounded-md text-xs flex items-center justify-between transition-colors"
                      style={{ color: activeSort.field === field ? 'var(--text-primary)' : 'var(--text-secondary)' }}
                      onClick={() => toggleSortField(field)}
                      onMouseOver={(event) => { event.currentTarget.style.background = 'var(--surface-4)'; }}
                      onMouseOut={(event) => { event.currentTarget.style.background = 'transparent'; }}
                    >
                      <span className="font-mono truncate">{field}</span>
                      {activeSort.field === field && (
                        activeSort.dir === -1
                          ? <ArrowDown className="w-3.5 h-3.5" style={{ color:'var(--accent)' }} />
                          : <ArrowUp className="w-3.5 h-3.5" style={{ color:'var(--accent)' }} />
                      )}
                    </button>
                  ))}
                </div>
                <div className="px-3 pt-2 mt-1 flex items-center justify-between text-2xs" style={{ borderTop:'1px solid var(--border)' }}>
                  <span style={{ color:'var(--text-tertiary)' }}>Global server sort</span>
                  <button
                    onClick={() => setSortField('_id', -1)}
                    className="btn-ghost py-1 px-2"
                    style={{ color:'var(--accent)' }}
                  >
                    Newest first
                  </button>
                </div>
              </div>
            )}
          </div>

          <div className="relative" ref={columnsMenuRef}>
            <button
              onClick={() => {
                setShowColumnsMenu((prev) => !prev);
                setShowSortMenu(false);
                setShowInlineFieldsMenu(false);
              }}
              className="btn-ghost flex items-center gap-1"
              style={{ color:'var(--text-secondary)' }}
              title={viewMode === 'table' ? 'Visible columns' : 'Preview fields'}
            >
              <Columns className="w-3.5 h-3.5" />
              <span className="hidden sm:inline text-xs">{viewMode === 'table' ? 'Columns' : 'Fields'}</span>
              {viewMode === 'table' && hiddenColumnsCount > 0 && <span className="badge-blue">{hiddenColumnsCount}</span>}
              {viewMode === 'json' && hiddenJsonFieldsCount > 0 && <span className="badge-blue">{hiddenJsonFieldsCount}</span>}
            </button>
            {showColumnsMenu && (
              <div className="absolute right-0 top-full mt-1 z-50 rounded-lg shadow-lg py-2 w-56 animate-fade-in" style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}>
                <div className="px-3 pb-1 text-2xs uppercase tracking-wider" style={{ color:'var(--text-tertiary)' }}>
                  {viewMode === 'table' ? 'Visible Columns' : 'Preview Fields'}
                </div>
                <div className="px-3 pb-2 text-2xs" style={{ color:'var(--text-tertiary)' }}>
                  {viewMode === 'table'
                    ? `${tableColumns.length} visible | ${hiddenColumnsCount} hidden items`
                    : `${jsonPreviewVisibleKeys.length} shown | ${hiddenJsonFieldsCount} hidden fields`}
                </div>
                <div className="max-h-56 overflow-auto px-2 space-y-0.5">
                  {(viewMode === 'table' ? allKeys : jsonSelectableKeys).map((key) => (
                    <label key={key} className="flex items-center gap-2 text-xs px-1.5 py-1 rounded-md cursor-pointer" style={{ color:'var(--text-secondary)' }}>
                      <input
                        type="checkbox"
                        checked={viewMode === 'table' ? tableColumns.includes(key) : jsonPreviewVisibleKeys.includes(key)}
                        onChange={() => {
                          if (viewMode === 'table') {
                            handleToggleColumn(key);
                          } else {
                            handleToggleJsonPreviewKey(key);
                          }
                        }}
                        disabled={viewMode === 'table'
                          ? (tableColumns.length <= 1 && tableColumns.includes(key))
                          : (jsonPreviewVisibleKeys.length <= 1 && jsonPreviewVisibleKeys.includes(key))}
                        className="ms-checkbox"
                      />
                      <span className="font-mono truncate">{key}</span>
                    </label>
                  ))}
                </div>
                <div className="px-3 pt-2 mt-1 flex items-center justify-between text-2xs" style={{ borderTop:'1px solid var(--border)' }}>
                  <button
                    onClick={() => {
                      if (viewMode === 'table') {
                        setVisibleColumns([...allKeys]);
                      } else {
                        setJsonPreviewKeys([...jsonSelectableKeys]);
                      }
                    }}
                    className="btn-ghost py-1 px-2"
                    style={{ color:'var(--accent)' }}
                  >
                    Show all
                  </button>
                  <button
                    onClick={() => {
                      if (viewMode === 'table') {
                        const fallback = allKeys.slice(0, Math.min(8, allKeys.length));
                        if (allKeys.includes('_id') && !fallback.includes('_id')) fallback.unshift('_id');
                        setVisibleColumns(fallback);
                        return;
                      }
                      const withoutId = jsonSelectableKeys.filter((key) => key !== '_id');
                      const fallback = (withoutId.length > 0 ? withoutId : jsonSelectableKeys).slice(0, Math.min(4, withoutId.length > 0 ? withoutId.length : jsonSelectableKeys.length));
                      setJsonPreviewKeys(fallback);
                    }}
                    className="btn-ghost py-1 px-2"
                  >
                    Reset
                  </button>
                </div>
              </div>
            )}
          </div>

          <div className="relative" ref={inlineFieldsMenuRef}>
            <button
              onClick={() => {
                setShowInlineFieldsMenu((prev) => !prev);
                setShowSortMenu(false);
                setShowColumnsMenu(false);
              }}
              className="btn-ghost flex items-center gap-1"
              style={{ color:'var(--text-secondary)' }}
              title="Show full inline values"
            >
              <Eye className="w-3.5 h-3.5" />
              <span className="hidden sm:inline text-xs">Inline</span>
              {expandedObjectFields.length > 0 && <span className="badge-blue">{expandedObjectFields.length}</span>}
            </button>
            {showInlineFieldsMenu && (
              <div className="absolute right-0 top-full mt-1 z-50 rounded-lg shadow-lg py-2 w-64 animate-fade-in" style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}>
                <div className="px-3 pb-1 text-2xs uppercase tracking-wider" style={{ color:'var(--text-tertiary)' }}>
                  Inline Fields
                </div>
                <div className="px-3 pb-2 text-2xs" style={{ color:'var(--text-tertiary)' }}>
                  Show full values instead of shortened preview
                </div>
                <div className="max-h-56 overflow-auto px-2 space-y-0.5">
                  {inlineFieldKeys.length === 0 ? (
                    <div className="px-2 py-1.5 text-2xs" style={{ color:'var(--text-tertiary)' }}>
                      No shortened fields on this page
                    </div>
                  ) : inlineFieldKeys.map((field) => (
                    <label key={field} className="flex items-center gap-2 text-xs px-1.5 py-1 rounded-md cursor-pointer" style={{ color:'var(--text-secondary)' }}>
                      <input
                        type="checkbox"
                        checked={expandedObjectFields.includes(field)}
                        onChange={() => handleToggleExpandedObjectField(field)}
                        className="ms-checkbox"
                      />
                      <span className="font-mono truncate">{field}</span>
                    </label>
                  ))}
                </div>
                <div className="px-3 pt-2 mt-1 flex items-center justify-between text-2xs" style={{ borderTop:'1px solid var(--border)' }}>
                  <button
                    onClick={() => setExpandedObjectFields([...inlineFieldKeys])}
                    className="btn-ghost py-1 px-2"
                    style={{ color:'var(--accent)' }}
                    disabled={inlineFieldKeys.length === 0}
                  >
                    Show all
                  </button>
                  <button
                    onClick={() => setExpandedObjectFields([])}
                    className="btn-ghost py-1 px-2"
                    disabled={expandedObjectFields.length === 0}
                  >
                    Clear
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Export */}
          <div className="relative" ref={exportMenuRef}>
            <button onClick={()=>setExportFormat(v=>!v)} className="btn-ghost flex items-center gap-1" style={{color:'var(--accent)'}}>
              <Download className="w-3.5 h-3.5" />
              <span className="hidden sm:inline text-xs">Export</span>
            </button>
            {exportFormat && (
              <div className="absolute right-0 top-full mt-1 z-50 rounded-lg shadow-lg py-1 min-w-[120px] animate-fade-in" style={{background:'var(--surface-3)',border:'1px solid var(--border)'}}>
                <button onClick={()=>openCollectionExportDialog('json')} disabled={exporting} className="w-full text-left px-3 py-1.5 text-xs transition-colors" style={{color:'var(--text-secondary)'}}
                  onMouseOver={e=>e.currentTarget.style.background='var(--surface-4)'} onMouseOut={e=>e.currentTarget.style.background='transparent'}>
                  Export JSON
                </button>
                <button onClick={()=>openCollectionExportDialog('csv')} disabled={exporting} className="w-full text-left px-3 py-1.5 text-xs transition-colors" style={{color:'var(--text-secondary)'}}
                  onMouseOver={e=>e.currentTarget.style.background='var(--surface-4)'} onMouseOut={e=>e.currentTarget.style.background='transparent'}>
                  Export CSV
                </button>
                <div className="my-1" style={{borderTop:'1px solid var(--border)'}} />
                <button onClick={openDbExportDialog} disabled={exporting} className="w-full text-left px-3 py-1.5 text-xs transition-colors" style={{color:'var(--text-secondary)'}}
                  onMouseOver={e=>e.currentTarget.style.background='var(--surface-4)'} onMouseOut={e=>e.currentTarget.style.background='transparent'}>
                  Export Database...
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

      {showExportDialog && (
        <div className="fixed inset-0 z-[220] flex items-center justify-center p-4">
          <button
            type="button"
            className="absolute inset-0 bg-black/50"
            onClick={() => !exporting && setShowExportDialog(false)}
            aria-label="Close export options"
          />
          <div className="relative w-full max-w-md rounded-xl p-4 animate-fade-in" style={{ background:'var(--surface-1)', border:'1px solid var(--border)' }}>
            <div className="flex items-center justify-between mb-2">
              <div className="text-sm font-semibold" style={{ color:'var(--text-primary)' }}>
                Export {pendingExportFormat.toUpperCase()}
              </div>
              <button
                type="button"
                className="btn-ghost p-1.5"
                onClick={() => setShowExportDialog(false)}
                disabled={exporting}
              >
                <X className="w-3.5 h-3.5" />
              </button>
            </div>
            <div className="text-2xs mb-3" style={{ color:'var(--text-tertiary)' }}>
              Collection: <span className="font-mono">{db}.{collection}</span> (up to 50,000 docs)
            </div>
            <div className="space-y-2.5">
              <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
                <input
                  type="checkbox"
                  checked={exportUseVisibleFields}
                  onChange={(event) => setExportUseVisibleFields(event.target.checked)}
                  className="ms-checkbox"
                />
                Export only visible fields ({exportVisibleFields.length})
              </label>
              <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
                <input
                  type="checkbox"
                  checked={exportUseCurrentSort}
                  onChange={(event) => setExportUseCurrentSort(event.target.checked)}
                  className="ms-checkbox"
                />
                Apply current sort ({activeSort.field}: {activeSort.dir === -1 ? 'desc' : 'asc'})
              </label>
              <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
                <input
                  type="checkbox"
                  checked={exportUseCurrentFilter}
                  onChange={(event) => setExportUseCurrentFilter(event.target.checked)}
                  className="ms-checkbox"
                />
                Apply current filter
              </label>
            </div>
            <div className="mt-4 flex items-center justify-end gap-2">
              <button
                type="button"
                className="btn-ghost text-xs"
                onClick={() => setShowExportDialog(false)}
                disabled={exporting}
              >
                Cancel
              </button>
              <button
                type="button"
                className="btn-primary text-xs px-3 py-1.5"
                disabled={exporting || (exportUseVisibleFields && exportVisibleFields.length === 0)}
                onClick={() => handleExport(pendingExportFormat, {
                  useVisibleFields: exportUseVisibleFields,
                  includeSort: exportUseCurrentSort,
                  useCurrentFilter: exportUseCurrentFilter,
                })}
              >
                {exporting ? 'Exporting...' : `Export ${pendingExportFormat.toUpperCase()}`}
              </button>
            </div>
          </div>
        </div>
      )}

      <DatabaseExportDialog
        open={showDbExportDialog}
        title="Export Database"
        subtitle={db}
        busy={exporting}
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
          if (exporting) return;
          setShowDbExportDialog(false);
        }}
        onSubmit={handleExportDatabase}
      />

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
          tableColumns.length === 0 ? (
            <div className="h-full flex items-center justify-center text-xs" style={{ color:'var(--text-tertiary)' }}>
              Select at least one visible column
            </div>
          ) : (
            <div className="overflow-auto">
              <table className="w-full text-xs">
	                <thead>
	                  <tr style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
	                    {tableColumns.map(k => (
	                      <th key={k} className="px-3 py-2 text-left font-medium text-2xs uppercase tracking-wider whitespace-nowrap" style={{color:'var(--text-tertiary)'}}>
                          <button
                            type="button"
                            className="inline-flex items-center gap-1 px-1 py-0.5 rounded-md transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]"
                            onClick={() => toggleSortField(k)}
                            title={`Sort by ${k}`}
                          >
                            <span>{k}</span>
                            {activeSort.field === k && (activeSort.dir === 1
                              ? <ArrowUp className="w-3 h-3" style={{ color:'var(--accent)' }} />
                              : <ArrowDown className="w-3 h-3" style={{ color:'var(--accent)' }} />)}
                          </button>
                        </th>
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
                        {tableColumns.map(k => {
                          const v = doc[k];
                          const inlineExpanded = expandedObjectFields.includes(k);
                          const display = v===null ? <span style={{color:'var(--json-null)'}}>null</span>
                            : v===undefined ? ''
                            : typeof v === 'object'
                              ? (
                                inlineExpanded
                                  ? <span className="font-mono inline-block whitespace-normal break-all" style={{color:'var(--text-secondary)'}}>{formatInlineValue(v)}</span>
                                  : <span className="font-mono" style={{color:'var(--json-bracket)'}}>{Array.isArray(v) ? `[${v.length}]` : '{...}'}</span>
                              )
                            : typeof v === 'boolean' ? <span style={{color:'var(--json-boolean)'}}>{String(v)}</span>
                            : typeof v === 'number' ? <span style={{color:'var(--json-number)'}}>{v}</span>
                            : k === '_id'
                              ? <span className={inlineExpanded ? 'font-mono inline-block whitespace-normal break-all' : 'font-mono'} style={{color:'var(--json-objectid)'}}>{fullId(v)}</span>
                              : inlineExpanded
                                ? <span className="font-mono inline-block whitespace-normal break-all" style={{color:'var(--text-secondary)'}}>{String(v)}</span>
                                : <span className="truncate max-w-[200px] inline-block" style={{color:'var(--text-secondary)'}}>{String(v).slice(0,80)}</span>;
                          return <td key={k} className={`px-3 py-2 font-mono ${inlineExpanded ? 'whitespace-normal align-top' : 'whitespace-nowrap'}`}>{display}</td>;
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
          )
        ) : (
          /* JSON VIEW */
          <div>
            {documents.map((doc, idx) => {
              const key = rowKey(doc, idx);
              const isExpanded = expandedRows.has(key);
              const keys = Object.keys(doc);
              const preferredPreview = jsonPreviewVisibleKeys.filter((field) => keys.includes(field));
              const preview = preferredPreview.length > 0 ? preferredPreview : keys.slice(0, Math.min(4, keys.length));
              const approxSize = (() => {
                try { return new Blob([JSON.stringify(doc)]).size; } catch { return 0; }
              })();
              return (
                <div key={key} className="group transition-colors duration-75 hover:bg-[var(--surface-1)]" style={{borderBottom:'1px solid var(--border)',background:isExpanded?'var(--surface-1)':'transparent'}}>
                  <div className="flex items-center px-4 py-2.5 cursor-pointer gap-3" onClick={()=>toggleRow(key)}>
                    <div className="w-5 flex-shrink-0">
                      {isExpanded ? <ChevronDown className="w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} /> : <ChevronRight className="w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} />}
                    </div>
                    <code className="text-2xs font-mono flex-shrink-0 whitespace-nowrap" style={{color:'var(--json-objectid)',opacity:0.7}}>{fullId(doc._id)}</code>
                    <div className="flex-1 flex items-start gap-2 min-w-0">
                      {preview.map(key => {
                        const val = doc[key];
                        const inlineExpanded = expandedObjectFields.includes(key);
                        const display = val===null
                          ? 'null'
                          : typeof val==='object'
                            ? (inlineExpanded ? formatInlineValue(val) : (Array.isArray(val) ? `[${val.length}]` : '{...}'))
                            : (key === '_id' ? fullId(val) : (inlineExpanded ? String(val) : String(val).slice(0,50)));
                        return (
                          <div key={key} className="flex items-center gap-1 text-2xs min-w-0">
                            <span className="font-mono truncate" style={{color:'var(--text-tertiary)'}}>{key}:</span>
                            <span className={inlineExpanded ? 'font-mono whitespace-normal break-all' : 'truncate max-w-[120px]'} style={{color:'var(--text-secondary)'}}>{display}</span>
                          </div>
                        );
                      })}
                      {keys.length > preview.length && <span className="text-2xs" style={{color:'var(--text-tertiary)'}}>+{keys.length - preview.length} more</span>}
                    </div>
                    <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0">
                      <span className="text-2xs mr-1 font-mono" style={{color:'var(--text-tertiary)'}}>
                        {formatBytes(approxSize)}
                      </span>
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
                        <JsonView data={doc} showControls />
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
          <div className="flex items-center gap-3">
            <span className="text-2xs" style={{color:'var(--text-tertiary)'}}>
              {formatNumber(page*limit+1)}-{formatNumber(Math.min((page+1)*limit,total))} of {formatNumber(total)}
            </span>
            <div className="flex items-center gap-1.5">
              <span className="text-2xs" style={{color:'var(--text-tertiary)'}}>Page size</span>
              <select
                value={limit}
                onChange={(event) => {
                  const next = parseInt(event.target.value, 10);
                  if (!Number.isFinite(next) || next <= 0) return;
                  setLimit(next);
                  setPage(0);
                }}
                className="ms-select text-2xs py-1"
              >
                {pageSizeOptions.map((value) => (
                  <option key={value} value={value}>{value}</option>
                ))}
              </select>
              {execConfig?.mode === 'safe' && (
                <span className="text-2xs" style={{color:'var(--text-tertiary)'}}>
                  Safe max {formatNumber(safeResultLimit)}
                </span>
              )}
            </div>
          </div>
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

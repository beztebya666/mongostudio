import React, { useState, useCallback, useEffect, useRef, useMemo, useLayoutEffect } from 'react';
import { createPortal } from 'react-dom';
import api from '../utils/api';
import { Database, ChevronRight, ChevronDown, ChevronLeft, Collection, Refresh, Search, Plus, Loader, Trash, X, MoreVertical, Upload, Filter, Check, Download } from './Icons';
import { formatBytes, formatNumber } from '../utils/formatters';
import AppModal from './modals/AppModal';
import ConfirmDialog from './modals/ConfirmDialog';
import InputDialog from './modals/InputDialog';
import DatabaseExportDialog from './modals/DatabaseExportDialog';
import CollectionExportDialog from './modals/CollectionExportDialog';
import { exportSingleDatabase, exportMultipleDatabases, shouldUseSmartStreamExport } from '../utils/exportUtils';
import ToastNotice from './ToastNotice';
import { genId } from '../utils/genId';

const QUERY_LIMIT_OVERRIDE_MAX = 50000;
const POWER_QUERY_LIMIT_MAX = 2147483000;
const EXPORT_LIMIT_SAFE_MAX = QUERY_LIMIT_OVERRIDE_MAX;
const EXPORT_LIMIT_MAX = POWER_QUERY_LIMIT_MAX;
const EXPORT_CONFIRM_THRESHOLD = EXPORT_LIMIT_SAFE_MAX;
const EXPORT_LIMIT_OPTIONS = ['exact', 500, 1000, 5000, 10000, EXPORT_LIMIT_SAFE_MAX, 'unlimited'];
const DB_EXPORT_TIMEOUT_MS = 4 * 60 * 60 * 1000;

function ContextMenu({ items, onClose, anchorEl, minWidth = 140 }) {
  const ref = useRef(null);
  const [position, setPosition] = useState({ top: -9999, left: -9999, origin: 'top right', ready: false });

  const updatePosition = useCallback(() => {
    if (!anchorEl || !ref.current || typeof window === 'undefined') return;
    const anchorRect = anchorEl.getBoundingClientRect();
    const menuRect = ref.current.getBoundingClientRect();
    const margin = 8;
    let top = anchorRect.bottom + 4;
    let origin = 'top right';
    if (top + menuRect.height > window.innerHeight - margin) {
      top = Math.max(margin, anchorRect.top - menuRect.height - 4);
      origin = 'bottom right';
    }
    let left = anchorRect.right - menuRect.width;
    if (left < margin) left = margin;
    if (left + menuRect.width > window.innerWidth - margin) {
      left = Math.max(margin, window.innerWidth - menuRect.width - margin);
    }
    setPosition({ top, left, origin, ready: true });
  }, [anchorEl]);

  useLayoutEffect(() => {
    if (!anchorEl) return undefined;
    updatePosition();
    const onMove = () => updatePosition();
    window.addEventListener('resize', onMove);
    window.addEventListener('scroll', onMove, true);
    return () => {
      window.removeEventListener('resize', onMove);
      window.removeEventListener('scroll', onMove, true);
    };
  }, [anchorEl, updatePosition]);

  useEffect(() => {
    const handler = (e) => {
      const target = e.target;
      if (ref.current && ref.current.contains(target)) return;
      if (anchorEl && anchorEl.contains(target)) return;
      onClose();
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [anchorEl, onClose]);

  if (!anchorEl || typeof document === 'undefined') return null;

  return createPortal(
    <div
      ref={ref}
      className="rounded-lg shadow-lg py-1 overflow-hidden animate-fade-in"
      style={{
        position: 'fixed',
        top: position.top,
        left: position.left,
        zIndex: 260,
        minWidth,
        width: 'max-content',
        maxWidth: 'min(86vw, 240px)',
        opacity: position.ready ? 1 : 0,
        transformOrigin: position.origin,
        background: 'var(--surface-3)',
        border: '1px solid var(--border)',
      }}
    >
      {items.map((item, i) => (
        <button
          key={i}
          onClick={() => { item.action(); onClose(); }}
          className={`block w-full text-left px-3 py-1.5 text-xs whitespace-nowrap transition-colors rounded-none focus:outline-none active:scale-100 ${item.danger ? 'text-red-400 hover:bg-red-500/10' : 'hover:bg-[var(--surface-4)]'}`}
          style={{
            color: item.danger ? undefined : 'var(--text-secondary)',
            boxShadow: 'none',
            border: 'none',
            appearance: 'none',
            WebkitAppearance: 'none',
          }}
        >
          {item.label}
        </button>
      ))}
    </div>,
    document.body,
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

function formatExportLimitLabel(value, mode = 'safe') {
  const normalized = String(value ?? '').trim().toLowerCase();
  if (normalized === 'exact') return 'Exact';
  if (normalized === 'unlimited' || normalized === 'all') return 'Unlimited';
  const numeric = Number(value);
  return Number.isFinite(numeric) ? formatNumber(numeric) : 'Exact';
}

function normalizeExportLimitChoice(value) {
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) return 'exact';
  if (raw === 'exact') return 'exact';
  if (raw === 'unlimited' || raw === 'all') return 'unlimited';
  const numeric = Math.floor(Number(raw));
  if (!Number.isFinite(numeric) || numeric <= 0) return 'exact';
  return String(Math.min(numeric, EXPORT_LIMIT_MAX));
}

function resolveExportLimitPayload(value) {
  const choice = normalizeExportLimitChoice(value);
  if (choice === 'exact' || choice === 'unlimited') return choice;
  return Math.max(1, Math.min(Number(choice) || 1000, EXPORT_LIMIT_MAX));
}

function resolveExportTimeoutMs(limitPayload) {
  if (limitPayload === 'exact' || limitPayload === 'unlimited') return 1800000;
  const numeric = Number(limitPayload) || 1000;
  if (numeric >= EXPORT_CONFIRM_THRESHOLD) return 600000;
  if (numeric >= 10000) return 180000;
  return 120000;
}

function isAbortError(err) {
  if (!err) return false;
  if (err.name === 'AbortError') return true;
  return /(?:^|\s)(?:abort|aborted|cancelled|canceled)(?:\s|$)/i.test(String(err.message || ''));
}

const DB_SORT_OPTIONS = [
  { value: 'name', label: 'Name (A-Z)' },
  { value: 'size', label: 'Size' },
  { value: 'collections', label: 'Collections' },
  { value: 'documents', label: 'Documents' },
];

function DbItem({
  db,
  selectedDb,
  selectedCol,
  onSelect,
  onRefresh,
  onOpenConsole,
  refreshToken,
  onError,
  onSuccess,
  onEnsureStats,
  execMode = 'safe',
  listContainerRef = null,
}) {
  const [expanded, setExpanded] = useState(false);
  const [collections, setCollections] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [showMenu, setShowMenu] = useState(false);
  const [dbMenuAnchorEl, setDbMenuAnchorEl] = useState(null);
  const [showColMenu, setShowColMenu] = useState(null);
  const [colMenuAnchorEl, setColMenuAnchorEl] = useState(null);
  const [confirmDialog, setConfirmDialog] = useState(null);
  const [importModeDialog, setImportModeDialog] = useState(null);
  const [showExportColDialog, setShowExportColDialog] = useState(false);
  const [exportCollectionName, setExportCollectionName] = useState('');
  const [exportCollectionFormat, setExportCollectionFormat] = useState('json');
  const [exportLimit, setExportLimit] = useState('exact');
  const [exportUseVisibleFields, setExportUseVisibleFields] = useState(false);
  const [exportSortMode, setExportSortMode] = useState('none');
  const [exportUseCurrentFilter, setExportUseCurrentFilter] = useState(false);
  const [exportBusy, setExportBusy] = useState(false);
  const [exportProgress, setExportProgress] = useState(null);
  const [showDbExportDialog, setShowDbExportDialog] = useState(false);
  const [dbExportBusy, setDbExportBusy] = useState(false);
  const [dbExportProgress, setDbExportProgress] = useState(null);
  const [dbExportMode, setDbExportMode] = useState('package');
  const [dbExportArchive, setDbExportArchive] = useState(false);
  const [dbExportCollectionFormat, setDbExportCollectionFormat] = useState('json');
  const [dbExportIncludeIndexes, setDbExportIncludeIndexes] = useState(false);
  const [dbExportIncludeSchema, setDbExportIncludeSchema] = useState(false);
  const [showCreateColDialog, setShowCreateColDialog] = useState(false);
  const [showImportColDialog, setShowImportColDialog] = useState(false);
  const [pendingCollectionImport, setPendingCollectionImport] = useState(null);
  const [importBusy, setImportBusy] = useState(false);
  const [docsImportTarget, setDocsImportTarget] = useState('');
  const [docsImportBusy, setDocsImportBusy] = useState(false);
  const colImportInputRef = useRef(null);
  const docsImportInputRef = useRef(null);
  const collectionExportControllerRef = useRef(null);
  const dbExportControllerRef = useRef(null);
  const collectionStatsLoadingRef = useRef(new Set());
  const collectionStatsSeqRef = useRef(0);
  const confirmResolverRef = useRef(null);
  const importModeResolverRef = useRef(null);
  const rootRef = useRef(null);
  const firstCollectionRowRef = useRef(null);
  const pendingExpandScrollRef = useRef(false);

  const ensureExpandedContentVisible = useCallback(() => {
    const container = listContainerRef?.current;
    const anchor = firstCollectionRowRef.current || rootRef.current;
    if (!container || !anchor) return;
    const containerRect = container.getBoundingClientRect();
    const anchorRect = anchor.getBoundingClientRect();
    const bottomPadding = 10;
    const visibleBottom = containerRect.bottom - bottomPadding;
    if (anchorRect.bottom > visibleBottom) {
      container.scrollTop += anchorRect.bottom - visibleBottom;
    }
  }, [listContainerRef]);

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
    pendingExpandScrollRef.current = true;
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

  useEffect(() => {
    if (!expanded || !pendingExpandScrollRef.current) return;
    if (loading && collections.length === 0) return;
    const raf = requestAnimationFrame(() => {
      ensureExpandedContentVisible();
      pendingExpandScrollRef.current = false;
    });
    return () => cancelAnimationFrame(raf);
  }, [expanded, loading, collections.length, ensureExpandedContentVisible]);

  useEffect(() => { if (selectedDb===db.name && !expanded) toggle(); }, [selectedDb]);
  useEffect(() => {
    if (!expanded || !loaded) return;
    if (selectedDb !== db.name || !selectedCol) return;
    if (!collections.some((entry) => entry.name === selectedCol)) return;
    ensureCollectionStats(selectedCol);
  }, [expanded, loaded, selectedDb, selectedCol, db.name, collections, ensureCollectionStats]);

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

  useEffect(() => () => {
    try { collectionExportControllerRef.current?.abort('unmount'); } catch {}
    try { dbExportControllerRef.current?.abort('unmount'); } catch {}
    collectionExportControllerRef.current = null;
    dbExportControllerRef.current = null;
    if (confirmResolverRef.current) {
      try { confirmResolverRef.current(false); } catch {}
      confirmResolverRef.current = null;
    }
    if (importModeResolverRef.current) {
      try { importModeResolverRef.current(null); } catch {}
      importModeResolverRef.current = null;
    }
  }, []);

  const requestConfirm = async ({
    title = 'Confirm action',
    message = 'Continue?',
    confirmLabel = 'Confirm',
    cancelLabel = 'Cancel',
    danger = false,
  } = {}) => (
    new Promise((resolve) => {
      confirmResolverRef.current = resolve;
      setConfirmDialog({ title, message, confirmLabel, cancelLabel, danger });
    })
  );

  const closeConfirmDialog = (approved) => {
    const resolver = confirmResolverRef.current;
    confirmResolverRef.current = null;
    setConfirmDialog(null);
    if (resolver) resolver(Boolean(approved));
  };

  const requestImportMode = async ({ title = 'Import mode', message = '' } = {}) => (
    new Promise((resolve) => {
      importModeResolverRef.current = resolve;
      setImportModeDialog({ title, message });
    })
  );

  const closeImportModeDialog = (mode = null) => {
    const resolver = importModeResolverRef.current;
    importModeResolverRef.current = null;
    setImportModeDialog(null);
    if (resolver) resolver(mode);
  };

  const handleDropDb = async () => {
    const confirmed = await requestConfirm({
      title: 'Drop Database',
      message: `Drop database "${db.name}"?\n\nThis action cannot be undone.`,
      confirmLabel: 'Drop database',
      cancelLabel: 'Cancel',
      danger: true,
    });
    if (!confirmed) return;
    try { await api.dropDatabase(db.name); onRefresh(); }
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
    try {
      const preflight = await api.preflight(db.name, colName, { operation: 'dropCollection' }).catch(() => null);
      const lines = [`Drop collection "${colName}"?`];
      if (preflight) {
        lines.push(
          '',
          `Estimated docs: ${typeof preflight.estimate === 'number' ? formatNumber(preflight.estimate) : 'unknown'}`,
          `Risk: ${preflight.risk || 'unknown'}`
        );
      }
      lines.push('', 'This action cannot be undone.');
      const confirmed = await requestConfirm({
        title: 'Drop Collection',
        message: lines.join('\n'),
        confirmLabel: 'Drop collection',
        cancelLabel: 'Cancel',
        danger: true,
      });
      if (!confirmed) return;
      await api.dropCollection(db.name, colName);
      refreshCollections();
      onRefresh?.();
    }
    catch(err) { onError?.(err.message); }
  };

  const downloadText = (filename, text, mime = 'application/json') => {
    const blob = new Blob([text], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.rel = 'noopener';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  };

  const openDbExportDialog = () => {
    setDbExportMode('package');
    setDbExportArchive(false);
    setDbExportCollectionFormat('json');
    setDbExportIncludeIndexes(false);
    setDbExportIncludeSchema(false);
    setDbExportProgress(null);
    setShowDbExportDialog(true);
  };

  const handleExportDb = async () => {
    try {
      const statsRes = await api.listCollections(db.name, { withStats: true, source: 'sidebar' });
      const approxDocs = (statsRes?.collections || []).reduce((sum, entry) => {
        const next = Number(entry?.count);
        return sum + (Number.isFinite(next) && next > 0 ? next : 0);
      }, 0);
      if (approxDocs > EXPORT_CONFIRM_THRESHOLD) {
        const confirmed = await requestConfirm({
          title: 'Large Database Export',
          message: `Database "${db.name}" has about ${formatNumber(approxDocs)} documents. Continue export?`,
          confirmLabel: 'Export',
          cancelLabel: 'Cancel',
        });
        if (!confirmed) return;
      }
    } catch {}

    const controller = new AbortController();
    dbExportControllerRef.current = controller;
    setDbExportBusy(true);
    setDbExportProgress(null);
    try {
      const result = await exportSingleDatabase(db.name, {
        mode: dbExportMode,
        archive: dbExportArchive,
        collectionFormat: dbExportCollectionFormat,
        includeIndexes: dbExportIncludeIndexes,
        includeSchema: dbExportIncludeSchema,
        heavyTimeoutMs: DB_EXPORT_TIMEOUT_MS,
        heavyConfirm: true,
        controller,
        onProgress: (next) => {
          setDbExportProgress(next && typeof next === 'object' ? next : null);
        },
      });
      onSuccess?.(`Database "${db.name}" exported (${result.files} file${result.files === 1 ? '' : 's'}${result.archive ? ', zip' : ''}).`);
      setShowDbExportDialog(false);
    } catch (err) {
      if (isAbortError(err)) return;
      onError?.(err.message);
    } finally {
      if (dbExportControllerRef.current === controller) dbExportControllerRef.current = null;
      setDbExportBusy(false);
      setDbExportProgress(null);
    }
  };

  const cancelDbExport = () => {
    try { dbExportControllerRef.current?.abort('user_cancel'); } catch {}
    dbExportControllerRef.current = null;
    setDbExportBusy(false);
    setDbExportProgress(null);
    setShowDbExportDialog(false);
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
    setExportLimit('exact');
    setExportUseVisibleFields(false);
    setExportSortMode('none');
    setExportUseCurrentFilter(false);
    setExportProgress(null);
    setShowExportColDialog(true);
  };

  const getStoredFilter = (colName) => {
    try {
      const raw = localStorage.getItem(`mongostudio_filter:${db.name}.${colName}`);
      if (!raw) return '{}';
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return '{}';
      return JSON.stringify(parsed);
    } catch {
      return '{}';
    }
  };

  const getStoredSortLabel = (colName) => {
    try {
      const raw = getStoredSort(colName);
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return '_id: desc';
      const entries = Object.entries(parsed);
      if (entries.length === 0) return '_id: desc';
      const [field, dirRaw] = entries[0];
      return `${field}: ${Number(dirRaw) === -1 ? 'desc' : 'asc'}`;
    } catch {
      return '_id: desc';
    }
  };

  const handleExportCollection = async () => {
    if (!exportCollectionName) return;
    const limitChoice = normalizeExportLimitChoice(exportLimit);
    const limitPayload = resolveExportLimitPayload(limitChoice);
    const sortValue = exportSortMode === 'none' ? '{}' : getStoredSort(exportCollectionName);
    const filterValue = exportUseCurrentFilter ? getStoredFilter(exportCollectionName) : '{}';
    let exportEstimate = null;
    let exportController = null;
    try {
      const visibleFields = exportUseVisibleFields ? getStoredVisibleFields(exportCollectionName) : [];
      if (exportUseVisibleFields && visibleFields.length === 0) {
        onError?.('No saved visible fields for this collection yet.');
        return;
      }
      const fixedProjection = exportUseVisibleFields
        ? JSON.stringify(Object.fromEntries(visibleFields.map((field) => [field, 1])))
        : '{}';

      try {
        const preflight = await api.preflight(
          db.name,
          exportCollectionName,
          { operation: 'export', filter: filterValue, limit: limitPayload },
          { budget: { timeoutMs: 60000, limit: 1000 } },
        );
        if (Number.isFinite(Number(preflight?.estimate))) exportEstimate = Number(preflight.estimate);
      } catch {}
      const shouldConfirm = (
        limitChoice === 'exact'
        || limitChoice === 'unlimited'
        || (Number.isFinite(Number(limitPayload)) && Number(limitPayload) > EXPORT_CONFIRM_THRESHOLD)
        || (Number.isFinite(exportEstimate) && exportEstimate > EXPORT_CONFIRM_THRESHOLD)
      );
      if (shouldConfirm) {
        const estimateText = Number.isFinite(exportEstimate) ? formatNumber(exportEstimate) : 'unknown';
        const confirmed = await requestConfirm({
          title: 'Large Export',
          message: `This export can be large (${estimateText} docs). Continue?`,
          confirmLabel: 'Export',
          cancelLabel: 'Cancel',
        });
        if (!confirmed) return;
      }

      const controller = new AbortController();
      exportController = controller;
      collectionExportControllerRef.current = controller;
      setExportBusy(true);
      setExportProgress(null);
      const exportTimeoutMs = resolveExportTimeoutMs(limitPayload);
      const shouldStreamToFile = shouldUseSmartStreamExport({
        limitChoice,
        limitValue: limitPayload,
        estimate: exportEstimate,
      });
      const ext = exportCollectionFormat === 'csv' ? 'csv' : 'json';
      if (shouldStreamToFile && typeof api?.exportDataToFile === 'function') {
        await api.exportDataToFile(db.name, exportCollectionName, {
          format: exportCollectionFormat,
          filter: filterValue,
          sort: sortValue,
          limit: limitPayload,
          projection: fixedProjection,
        }, {
          heavyTimeoutMs: exportTimeoutMs,
          heavyConfirm: true,
          controller,
          onProgress: (next) => {
            setExportProgress(next && typeof next === 'object' ? next : null);
          },
          filename: `${db.name}.${exportCollectionName}.${ext}`,
        });
      } else {
        const data = await api.exportData(db.name, exportCollectionName, {
          format: exportCollectionFormat,
          filter: filterValue,
          sort: sortValue,
          limit: limitPayload,
          projection: fixedProjection,
        }, {
          heavyTimeoutMs: exportTimeoutMs,
          heavyConfirm: true,
          controller,
          onProgress: (next) => {
            setExportProgress(next && typeof next === 'object' ? next : null);
          },
        });
        const mime = exportCollectionFormat === 'csv' ? 'text/csv' : 'application/json';
        downloadText(`${db.name}.${exportCollectionName}.${ext}`, data.data, mime);
      }
      onSuccess?.(`Collection "${exportCollectionName}" exported.`);
      setShowExportColDialog(false);
    } catch (err) {
      if (isAbortError(err)) return;
      onError?.(`Collection export failed: ${err.message}`);
    } finally {
      if (collectionExportControllerRef.current === exportController) collectionExportControllerRef.current = null;
      setExportBusy(false);
      setExportProgress(null);
    }
  };

  const cancelCollectionExport = () => {
    try { collectionExportControllerRef.current?.abort('user_cancel'); } catch {}
    collectionExportControllerRef.current = null;
    setExportBusy(false);
    setExportProgress(null);
    setShowExportColDialog(false);
  };

  const exportVisibleFieldsCount = exportCollectionName
    ? getStoredVisibleFields(exportCollectionName).length
    : 0;
  const exportSortLabel = exportCollectionName
    ? getStoredSortLabel(exportCollectionName)
    : '_id: desc';
  const exportNoModifiers = !exportUseVisibleFields && exportSortMode === 'none' && !exportUseCurrentFilter;

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
      const preflight = await api.preflight(db.name, colName, {
        operation: 'import',
        documentsCount: Array.isArray(pendingCollectionImport.documents) ? pendingCollectionImport.documents.length : 0,
      }).catch(() => null);
      if (preflight && (preflight.risk === 'medium' || preflight.risk === 'high' || preflight.risk === 'critical')) {
        const ok = await requestConfirm({
          title: 'Import Collection',
          message:
            `Import into "${colName}" with risk "${preflight.risk}".\n` +
            `Estimated docs: ${typeof preflight.estimate === 'number' ? formatNumber(preflight.estimate) : 'unknown'}\n\n` +
            'Continue?',
          confirmLabel: 'Continue',
          cancelLabel: 'Cancel',
          danger: preflight.risk === 'high' || preflight.risk === 'critical',
        });
        if (!ok) {
          setImportBusy(false);
          return;
        }
      }
      const mode = await requestImportMode({
        title: 'Collection import mode',
        message: `Choose mode for "${db.name}.${colName}".`,
      });
      if (!mode) {
        setImportBusy(false);
        return;
      }
      const dropExisting = mode === 'replace';
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
    <div className="animate-slide-up" ref={rootRef}>
      <div className="group relative flex items-center">
        <button
          data-sidebar-row
          onClick={toggle}
          onMouseEnter={() => onEnsureStats?.(db.name)}
          className="flex-1 min-w-0 flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-all duration-100 hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]"
          title={`${db.name}${typeof db.collections === 'number' ? ` | ${db.collections} collections` : ''}${typeof db.objects === 'number' ? ` | ${formatNumber(db.objects)} docs` : ''}${typeof db.sizeOnDisk === 'number' ? ` | ${formatBytes(db.sizeOnDisk)}` : ''}`}
          style={{
            background: selectedDb===db.name&&!selectedCol ? 'var(--surface-3)' : 'transparent',
            color: selectedDb===db.name&&!selectedCol ? 'var(--text-primary)' : 'var(--text-secondary)',
          }}>
          {loading ? <Loader className="w-3.5 h-3.5" style={{color:'var(--accent)'}} />
            : expanded ? <ChevronDown className="w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} />
            : <ChevronRight className="w-3.5 h-3.5" style={{color:'var(--text-tertiary)'}} />}
          <Database className="w-3.5 h-3.5" style={{color:'var(--accent)',opacity:0.7}} />
          <span data-sidebar-label className="text-left font-medium truncate min-w-0">{db.name}</span>
          <span
            className={`ml-auto text-2xs text-right whitespace-nowrap shrink-0 transition-opacity ${
              selectedDb===db.name&&!selectedCol ? 'opacity-100' : 'opacity-0 group-hover:opacity-100'
            }`}
            style={{color:'var(--text-tertiary)'}}
            title={db._statsLoaded
              ? `${typeof db.collections === 'number' ? formatNumber(db.collections) : '-'} collections | ${typeof db.objects === 'number' ? formatNumber(db.objects) : '-'} docs${!db._statsFresh ? ' | stale' : ''}`
              : db._statsError
                ? 'stats unavailable'
                : 'loading stats...'}
          >
            {db._statsLoaded
              ? `${typeof db.collections === 'number' ? formatNumber(db.collections) : '-'}c · ${typeof db.objects === 'number' ? formatNumber(db.objects) : '-'}d · ${formatBytes(db.sizeOnDisk || 0)}`
              : db._statsError
                ? `stats unavailable · ${formatBytes(db.sizeOnDisk || 0)}`
                : (
                  <span className="inline-flex items-center gap-1">
                    <Loader className="w-3 h-3 animate-spin" style={{ color:'var(--accent)' }} />
                    loading stats...
                  </span>
                )}
          </span>
        </button>
        <div className="relative">
          <button
            onClick={(event) => {
              if (showMenu) {
                setShowMenu(false);
                setDbMenuAnchorEl(null);
              } else {
                setDbMenuAnchorEl(event.currentTarget);
                setShowMenu(true);
              }
            }}
            className="opacity-70 group-hover:opacity-100 p-1 rounded-md transition-all hover:bg-[var(--surface-2)]"
            style={{color:'var(--text-tertiary)'}}
          >
            <MoreVertical className="w-3.5 h-3.5" />
          </button>
            {showMenu && (
              <ContextMenu onClose={() => { setShowMenu(false); setDbMenuAnchorEl(null); }} items={[
                  { label:'Open Console', action:() => onOpenConsole?.({ level: 'database', db: db.name }) },
                  { label:'New Collection', action:handleCreateCol },
                  { label:'Import Collection', action:() => colImportInputRef.current?.click() },
                  { label:'Export Database...', action:openDbExportDialog },
                  { label:'Refresh', action:refreshCollections },
                  { label:'Drop Database', action:handleDropDb, danger:true },
                ]} anchorEl={dbMenuAnchorEl} />
              )}
        </div>
      </div>

      {expanded && collections.length > 0 && (
        <div className="ml-3 mt-0.5 space-y-0.5 pl-3" style={{borderLeft:'1px solid var(--border)'}}>
          {collections.map((col, idx) => (
            <div key={col.name} className="group/col relative flex items-center" ref={idx === 0 ? firstCollectionRowRef : null}>
              <button
                data-sidebar-row
                onClick={()=>onSelect(db.name, col.name)}
                onMouseEnter={() => ensureCollectionStats(col.name)}
                className="flex-1 min-w-0 flex items-center gap-2 px-2.5 py-1.5 rounded-lg text-xs transition-all duration-100 hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]"
                title={`${db.name}.${col.name}${typeof col.count === 'number' ? ` | ${formatNumber(col.count)} docs` : ''}${typeof col.size === 'number' ? ` | ${formatBytes(col.size)}` : ''}`}
                style={{
                  background: selectedDb===db.name&&selectedCol===col.name ? 'rgba(0, 237, 100, 0.12)' : 'transparent',
                  color: selectedDb===db.name&&selectedCol===col.name ? 'var(--accent)' : 'var(--text-secondary)',
                  border: selectedDb===db.name&&selectedCol===col.name ? '1px solid rgba(0, 237, 100, 0.28)' : '1px solid transparent',
                }}>
                <Collection className="w-3 h-3 flex-shrink-0" style={{color:'var(--text-tertiary)'}} />
                <span data-sidebar-label className="text-left truncate min-w-0">{col.name}</span>
                <span
                  className={`ml-auto text-2xs whitespace-nowrap shrink-0 transition-opacity ${
                    selectedDb===db.name&&selectedCol===col.name ? 'opacity-100' : 'opacity-0 group-hover/col:opacity-100'
                  }`}
                  style={{color:'var(--text-tertiary)'}}
                  title={(col._statsLoading || (typeof col.count !== 'number' && typeof col.size !== 'number'))
                    ? 'loading stats...'
                    : (typeof col.count === 'number' || typeof col.size === 'number')
                      ? `${typeof col.count === 'number' ? formatNumber(col.count) : '?'} docs | ${typeof col.size === 'number' ? formatBytes(col.size) : '?'}`
                      : 'stats unavailable'}
                >
                  {(col._statsLoading || (typeof col.count !== 'number' && typeof col.size !== 'number'))
                    ? (
                      <span className="inline-flex items-center gap-1">
                        <Loader className="w-3 h-3 animate-spin" style={{ color:'var(--accent)' }} />
                        loading stats...
                      </span>
                    )
                    : (typeof col.count === 'number' || typeof col.size === 'number')
                      ? `${typeof col.count === 'number' ? formatNumber(col.count) : '?'}d · ${typeof col.size === 'number' ? formatBytes(col.size) : '?'}`
                      : 'stats unavailable'}
                </span>
              </button>
              <div className="relative">
                <button
                  onClick={(event) => {
                    if (showColMenu === col.name) {
                      setShowColMenu(null);
                      setColMenuAnchorEl(null);
                      return;
                    }
                    setShowColMenu(col.name);
                    setColMenuAnchorEl(event.currentTarget);
                  }}
                  className="opacity-70 group-hover/col:opacity-100 p-0.5 rounded transition-all hover:bg-[var(--surface-2)]" style={{color:'var(--text-tertiary)'}}>
                  <MoreVertical className="w-3 h-3" />
                </button>
                {showColMenu === col.name && (
                  <ContextMenu onClose={() => { setShowColMenu(null); setColMenuAnchorEl(null); }} items={[
                    { label:'Open Console', action:() => onOpenConsole?.({ level: 'collection', db: db.name, collection: col.name }) },
                    { label:'Refresh', action:refreshCollections },
                    { label:'Import Documents', action:()=>openImportDocumentsDialog(col.name) },
                    { label:'Export Collection', action:()=>openExportCollectionDialog(col.name) },
                    { label:'Drop Collection', action:()=>handleDropCol(col.name), danger:true },
                  ]} anchorEl={colMenuAnchorEl} />
                )}
              </div>
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
      <CollectionExportDialog
        open={showExportColDialog}
        busy={exportBusy}
        progress={exportProgress}
        title="Export Collection"
        subtitle={`${db.name}.${exportCollectionName} | ${formatExportLimitLabel(exportLimit, execMode)} docs`}
        docsValue={exportLimit}
        docsOptions={EXPORT_LIMIT_OPTIONS.map((value) => {
          if (value === 'exact' || value === 'unlimited') {
            return { value, label: formatExportLimitLabel(value, execMode) };
          }
          return { value: String(value), label: formatExportLimitLabel(value, execMode) };
        })}
        onDocsChange={setExportLimit}
        format={exportCollectionFormat}
        onFormatChange={(next) => setExportCollectionFormat(next === 'csv' ? 'csv' : 'json')}
        useVisibleFields={exportUseVisibleFields}
        onUseVisibleFieldsChange={setExportUseVisibleFields}
        visibleFieldsLabel={`Export only visible fields (${exportVisibleFieldsCount})`}
        useSort={exportSortMode !== 'none'}
        onUseSortChange={(checked) => setExportSortMode(checked ? 'saved' : 'none')}
        sortLabel={`Apply current sort (${exportSortLabel})`}
        showFilterToggle
        useFilter={exportUseCurrentFilter}
        onUseFilterChange={setExportUseCurrentFilter}
        filterLabel="Apply current filter"
        showModifiersInfo={exportNoModifiers}
        modifiersInfoText="Export uses full documents with no current filter/sort overrides."
        submitLabel="Export"
        onCancel={cancelCollectionExport}
        onSubmit={handleExportCollection}
      />
      <DatabaseExportDialog
        open={showDbExportDialog}
        title="Export Database"
        subtitle={db.name}
        busy={dbExportBusy}
        progress={dbExportProgress}
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
        onCancel={cancelDbExport}
        onSubmit={handleExportDb}
      />
      <ConfirmDialog
        open={Boolean(confirmDialog)}
        title={confirmDialog?.title || 'Confirm action'}
        message={confirmDialog?.message || 'Continue?'}
        confirmLabel={confirmDialog?.confirmLabel || 'Confirm'}
        cancelLabel={confirmDialog?.cancelLabel || 'Cancel'}
        danger={Boolean(confirmDialog?.danger)}
        onConfirm={() => closeConfirmDialog(true)}
        onCancel={() => closeConfirmDialog(false)}
      />
      <AppModal
        open={Boolean(importModeDialog)}
        onClose={() => closeImportModeDialog(null)}
        maxWidth="max-w-md"
      >
        <div className="px-5 py-4" style={{ borderBottom: '1px solid var(--border)' }}>
          <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>
            {importModeDialog?.title || 'Import mode'}
          </h3>
        </div>
        <div className="px-5 py-4">
          <p className="text-xs whitespace-pre-line" style={{ color: 'var(--text-secondary)' }}>
            {importModeDialog?.message || 'Choose import mode.'}
          </p>
          <p className="text-2xs mt-2" style={{ color: 'var(--text-tertiary)' }}>
            Replace: drop existing collection, then import.
          </p>
          <p className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
            Merge: keep existing data and append/import new documents.
          </p>
        </div>
        <div className="px-5 py-4 flex items-center justify-end gap-2" style={{ borderTop: '1px solid var(--border)' }}>
          <button type="button" className="btn-ghost text-xs" onClick={() => closeImportModeDialog(null)}>
            Cancel
          </button>
          <button type="button" className="btn-ghost text-xs" onClick={() => closeImportModeDialog('merge')}>
            Merge
          </button>
          <button type="button" className="btn-primary text-xs" onClick={() => closeImportModeDialog('replace')}>
            Replace
          </button>
        </div>
      </AppModal>
    </div>
  );
}

export default function Sidebar({
  databases,
  selectedDb,
  selectedCol,
  onSelect,
  onOpenConsole,
  onRefresh,
  width,
  onWidthChange,
  loading,
  refreshToken = 0,
  metadata = null,
  onToggleCollapse,
  execMode = 'safe',
}) {
  const [filter, setFilter] = useState('');
  const [dbSort, setDbSort] = useState('name');
  const [dbStats, setDbStats] = useState({});
  const [showCreateDb, setShowCreateDb] = useState(false);
  const [showDbSortMenu, setShowDbSortMenu] = useState(false);
  const [newDbName, setNewDbName] = useState('');
  const [toasts, setToasts] = useState([]);
  const [showExportAllDialog, setShowExportAllDialog] = useState(false);
  const [exportAllBusy, setExportAllBusy] = useState(false);
  const [exportAllProgress, setExportAllProgress] = useState(null);
  const [exportAllMode, setExportAllMode] = useState('package');
  const [exportAllArchive, setExportAllArchive] = useState(false);
  const [exportAllCollectionFormat, setExportAllCollectionFormat] = useState('json');
  const [exportAllIncludeIndexes, setExportAllIncludeIndexes] = useState(false);
  const [exportAllIncludeSchema, setExportAllIncludeSchema] = useState(false);
  const [selectedExportDbs, setSelectedExportDbs] = useState([]);
  const [dbImportTargetDialog, setDbImportTargetDialog] = useState(null);
  const [dbImportModeDialog, setDbImportModeDialog] = useState(null);
  const [dbImportBusy, setDbImportBusy] = useState(false);
  const resizing = useRef(false);
  const userResizedRef = useRef(false);
  const textMeasureCanvasRef = useRef(null);
  const asideRef = useRef(null);
  const createDbRef = useRef(null);
  const searchRowRef = useRef(null);
  const dbListRef = useRef(null);
  const dbImportRef = useRef(null);
  const dbStatsLoadingRef = useRef(new Set());
  const dbStatsSeqRef = useRef(0);
  const dbStatsRef = useRef({});
  const exportAllControllerRef = useRef(null);
  const dbImportTargetResolverRef = useRef(null);
  const dbImportModeResolverRef = useRef(null);
  const hasSharedMetadata = Boolean(metadata && metadata.loaded);
  const sharedStats = hasSharedMetadata && metadata?.stats && typeof metadata.stats === 'object' ? metadata.stats : {};
  const sharedFreshness = hasSharedMetadata && metadata?.freshness && typeof metadata.freshness === 'object' ? metadata.freshness : {};
  const statsSource = hasSharedMetadata ? sharedStats : dbStats;

  const dismissToast = useCallback((id) => {
    setToasts((prev) => prev.filter((item) => item.id !== id));
  }, []);

  const pushToast = useCallback((kind, message) => {
    if (!message) return;
    const id = genId();
    setToasts((prev) => {
      const next = [...prev, { id, kind, message: String(message) }];
      return next.slice(-4);
    });
  }, []);

  const notifySuccess = useCallback((message) => pushToast('success', message), [pushToast]);
  const notifyError = useCallback((message) => pushToast('error', message), [pushToast]);
  const notifyWarning = useCallback((message) => pushToast('warning', message), [pushToast]);

  useEffect(() => () => {
    try { exportAllControllerRef.current?.abort('unmount'); } catch {}
    exportAllControllerRef.current = null;
    if (dbImportTargetResolverRef.current) {
      try { dbImportTargetResolverRef.current(null); } catch {}
      dbImportTargetResolverRef.current = null;
    }
    if (dbImportModeResolverRef.current) {
      try { dbImportModeResolverRef.current(null); } catch {}
      dbImportModeResolverRef.current = null;
    }
  }, []);

  const requestDbImportTarget = async (initialValue = '') => (
    new Promise((resolve) => {
      dbImportTargetResolverRef.current = resolve;
      setDbImportTargetDialog({ initialValue: String(initialValue || '') });
    })
  );

  const closeDbImportTargetDialog = (value = null) => {
    const resolver = dbImportTargetResolverRef.current;
    dbImportTargetResolverRef.current = null;
    setDbImportTargetDialog(null);
    if (resolver) resolver(value ? String(value) : null);
  };

  const requestDbImportMode = async (targetDb) => (
    new Promise((resolve) => {
      dbImportModeResolverRef.current = resolve;
      setDbImportModeDialog({ targetDb: String(targetDb || '') });
    })
  );

  const closeDbImportModeDialog = (mode = null) => {
    const resolver = dbImportModeResolverRef.current;
    dbImportModeResolverRef.current = null;
    setDbImportModeDialog(null);
    if (resolver) resolver(mode || null);
  };

  useEffect(() => {
    dbStatsRef.current = dbStats;
  }, [dbStats]);

  const ensureDbStats = useCallback(async (dbName, attempt = 0) => {
    if (hasSharedMetadata) return;
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
  }, [hasSharedMetadata]);

  useEffect(() => {
    if (hasSharedMetadata) return;
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
  }, [databases.map((db) => db.name).join('|'), refreshToken, hasSharedMetadata]);

  useEffect(() => {
    if (hasSharedMetadata) return;
    if (!databases.length) return;
    const MAX_EAGER_DB_STATS = 40;
    const targets = databases.slice(0, Math.min(databases.length, MAX_EAGER_DB_STATS)).map((db) => db.name);
    targets.forEach((dbName) => ensureDbStats(dbName));
  }, [databases, ensureDbStats, refreshToken, hasSharedMetadata]);
  useEffect(() => {
    if (hasSharedMetadata) return;
    if (!selectedDb) return;
    ensureDbStats(selectedDb);
  }, [selectedDb, ensureDbStats, hasSharedMetadata]);

  const filteredDbs = useMemo(() => {
    const rows = databases
      .filter(db => db.name.toLowerCase().includes(filter.toLowerCase()))
      .map((db) => {
        const raw = Object.prototype.hasOwnProperty.call(statsSource, db.name) ? statsSource[db.name] : null;
        const freshness = hasSharedMetadata ? (sharedFreshness[db.name] || null) : null;
        const hasStats = Boolean(raw && !raw._error);
        const hasError = Boolean(raw && raw._error) || freshness?.source === 'error';
        const stats = hasStats ? raw : null;
        return {
          ...db,
          collections: typeof stats?.collections === 'number' ? stats.collections : null,
          objects: typeof stats?.objects === 'number' ? stats.objects : null,
          _statsLoaded: hasStats,
          _statsError: hasError,
          _statsFresh: typeof freshness?.fresh === 'boolean' ? freshness.fresh : hasStats,
          _statsTs: freshness?.ts || null,
        };
      });

    const byName = (a, b) => a.name.localeCompare(b.name);
    if (dbSort === 'size') return [...rows].sort((a, b) => (b.sizeOnDisk || 0) - (a.sizeOnDisk || 0) || byName(a, b));
    if (dbSort === 'collections') return [...rows].sort((a, b) => (b.collections || 0) - (a.collections || 0) || byName(a, b));
    if (dbSort === 'documents') return [...rows].sort((a, b) => (b.objects || 0) - (a.objects || 0) || byName(a, b));
    return [...rows].sort(byName);
  }, [databases, filter, dbSort, statsSource, hasSharedMetadata, sharedFreshness]);

  const totals = useMemo(() => {
    const statsList = Object.values(statsSource);
    return {
      collections: statsList.reduce((sum, item) => sum + (item?.collections || 0), 0),
      documents: statsList.reduce((sum, item) => sum + (item?.objects || 0), 0),
      size: hasSharedMetadata ? Number(metadata?.totalSize || 0) : databases.reduce((sum, item) => sum + Number(item?.sizeOnDisk || 0), 0),
    };
  }, [statsSource, databases, hasSharedMetadata, metadata?.totalSize]);

  const dbStatsProgress = useMemo(() => {
    const loaded = hasSharedMetadata
      ? databases.reduce((sum, db) => sum + (Object.prototype.hasOwnProperty.call(sharedFreshness, db.name) ? 1 : 0), 0)
      : databases.reduce((sum, db) => sum + (Object.prototype.hasOwnProperty.call(dbStats, db.name) ? 1 : 0), 0);
    return { loaded, total: databases.length };
  }, [dbStats, databases, hasSharedMetadata, sharedFreshness]);

  useEffect(() => {
    if (hasSharedMetadata) return;
    if (!filteredDbs.length) return;
    filteredDbs.slice(0, 24).forEach((db) => ensureDbStats(db.name));
  }, [filteredDbs, ensureDbStats, hasSharedMetadata]);

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
      const minWidth = 240;
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
      notifySuccess(`Database "${newDbName.trim()}" created.`);
      onRefresh();
    } catch(err) { notifyError(err.message); }
  };

  const handleGlobalImportDb = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      const suggested = parsed?.database?.name || guessNameFromFile(file.name, 'imported_db');
      const targetDb = await requestDbImportTarget(suggested);
      if (!targetDb || !targetDb.trim()) return;
      const normalizedTarget = targetDb.trim();
      const mode = await requestDbImportMode(normalizedTarget);
      if (!mode) return;
      setDbImportBusy(true);
      const result = await api.importDatabase(parsed, { targetDb: normalizedTarget, mode });
      notifySuccess(`Database "${normalizedTarget}" imported.`);
      onRefresh?.();
      setShowCreateDb(false);
      setNewDbName('');
      if (result?.warnings?.length) notifyWarning(`Imported with warnings: ${result.warnings[0]}`);
    } catch (err) {
      notifyError(`Import failed: ${err.message}`);
    } finally {
      setDbImportBusy(false);
      event.target.value = '';
    }
  };

  const openExportAllDialog = () => {
    setExportAllMode('package');
    setExportAllArchive(false);
    setExportAllCollectionFormat('json');
    setExportAllIncludeIndexes(false);
    setExportAllIncludeSchema(false);
    setSelectedExportDbs(databases.map((entry) => entry?.name).filter(Boolean));
    setExportAllProgress(null);
    setShowExportAllDialog(true);
  };

  const handleExportAllDatabases = async () => {
    const dbNames = [...new Set(selectedExportDbs.map((entry) => String(entry || '').trim()).filter(Boolean))];
    if (!dbNames.length) {
      notifyError('Select at least one database to export.');
      return;
    }
    const approxDocs = dbNames.reduce((sum, dbName) => {
      const next = Number(statsSource?.[dbName]?.documents);
      return sum + (Number.isFinite(next) && next > 0 ? next : 0);
    }, 0);
    if (approxDocs > EXPORT_CONFIRM_THRESHOLD || dbNames.length > 1) {
      const docsText = approxDocs > 0 ? formatNumber(approxDocs) : 'unknown';
      const confirmed = await requestConfirm({
        title: 'Large Multi-DB Export',
        message: `Selected databases: ${dbNames.length}. Estimated docs: ${docsText}. Continue export?`,
        confirmLabel: 'Export all',
        cancelLabel: 'Cancel',
      });
      if (!confirmed) return;
    }

    const controller = new AbortController();
    exportAllControllerRef.current = controller;
    setExportAllBusy(true);
    setExportAllProgress(null);
    try {
      const result = await exportMultipleDatabases(dbNames, {
        mode: exportAllMode,
        archive: exportAllArchive,
        collectionFormat: exportAllCollectionFormat,
        includeIndexes: exportAllIncludeIndexes,
        includeSchema: exportAllIncludeSchema,
        archiveName: 'mongostudio-all-databases',
        heavyTimeoutMs: DB_EXPORT_TIMEOUT_MS,
        heavyConfirm: true,
        controller,
        onProgress: (next) => {
          setExportAllProgress(next && typeof next === 'object' ? next : null);
        },
      });
      notifySuccess(`Exported ${result.databases} database${result.databases === 1 ? '' : 's'} (${result.files} file${result.files === 1 ? '' : 's'}${result.archive ? ', zip' : ''}).`);
      setShowExportAllDialog(false);
    } catch (err) {
      if (isAbortError(err)) return;
      notifyError(err.message);
    } finally {
      if (exportAllControllerRef.current === controller) exportAllControllerRef.current = null;
      setExportAllBusy(false);
      setExportAllProgress(null);
    }
  };

  const cancelExportAllDatabases = () => {
    try { exportAllControllerRef.current?.abort('user_cancel'); } catch {}
    exportAllControllerRef.current = null;
    setExportAllBusy(false);
    setExportAllProgress(null);
    setShowExportAllDialog(false);
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
    if (userResizedRef.current) return;
    let active = true;
    (async () => {
      const names = databases.map((db) => String(db?.name || ''));
      const dbForSizing = selectedDb || databases[0]?.name;
      if (dbForSizing) {
        try {
          const response = await api.listCollections(dbForSizing, { withStats: false });
          const collectionNames = (response.collections || []).slice(0, 160).map((entry) => String(entry?.name || ''));
          names.push(...collectionNames);
        } catch {}
      }
      const longest = names.reduce((best, name) => (name.length > best.length ? name : best), '');
      const longestDbStats = databases
        .map((entry) => `${formatNumber(entry?.collections || 0)}c · ${formatNumber(entry?.objects || 0)}d · ${formatBytes(entry?.sizeOnDisk || 0)}`)
        .reduce((best, text) => (text.length > best.length ? text : best), '0c · 0d · 0 B');
      const projectedCollectionStats = '9,999,999,999d · 999.9 GB';
      const font = getSidebarLabelFont();
      const measuredLabelWidth = Math.ceil(measureTextWidth(longest, font));
      const measuredDbStatsWidth = Math.ceil(measureTextWidth(longestDbStats, '400 11px system-ui, -apple-system, Segoe UI, sans-serif'));
      const measuredCollectionStatsWidth = Math.ceil(measureTextWidth(projectedCollectionStats, '400 11px system-ui, -apple-system, Segoe UI, sans-serif'));
      const minWidth = 280;
      const maxWidth = getMaxSidebarWidth();
      // Keep controls width bounded. Using live scrollWidth creates feedback and can grow sidebar on repeated rerenders.
      const controlsWidth = 280;
      const rowWidth = measuredLabelWidth + Math.max(measuredDbStatsWidth, measuredCollectionStatsWidth) + 170;
      const desired = Math.ceil(Math.max(minWidth, controlsWidth, rowWidth));
      const next = Math.max(minWidth, Math.min(maxWidth, desired));
      if (active && !userResizedRef.current && Math.abs(next - width) >= 2) {
        onWidthChange(next);
      }
    })();
    return () => { active = false; };
  }, [databases, statsSource, selectedDb, getMaxSidebarWidth, getSidebarLabelFont, measureTextWidth, onWidthChange]);

  useEffect(() => {
    const clamp = () => {
      const minWidth = 240;
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
          <button
            onClick={() => onToggleCollapse?.()}
            className="btn-ghost p-1.5"
            title="Collapse sidebar"
            style={{ color:'var(--text-secondary)' }}
          >
            <ChevronLeft className="w-3.5 h-3.5" />
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
            onOpenConsole={onOpenConsole}
            onRefresh={onRefresh}
            refreshToken={refreshToken}
            onEnsureStats={hasSharedMetadata ? undefined : ensureDbStats}
            onError={(message) => notifyError(message)}
            onSuccess={(message) => notifySuccess(message)}
            execMode={execMode}
            listContainerRef={dbListRef}
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
        progress={exportAllProgress}
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
        onCancel={cancelExportAllDatabases}
        onSubmit={handleExportAllDatabases}
      />
      <InputDialog
        open={Boolean(dbImportTargetDialog)}
        title="Import Database Package"
        label="Target Database Name"
        placeholder="imported_db"
        initialValue={dbImportTargetDialog?.initialValue || ''}
        submitLabel="Next"
        onCancel={() => closeDbImportTargetDialog(null)}
        onSubmit={(value) => closeDbImportTargetDialog(String(value || '').trim() || null)}
      />
      <AppModal
        open={Boolean(dbImportModeDialog)}
        onClose={() => closeDbImportModeDialog(null)}
        maxWidth="max-w-md"
      >
        <div className="px-5 py-4" style={{ borderBottom: '1px solid var(--border)' }}>
          <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>
            Database import mode
          </h3>
        </div>
        <div className="px-5 py-4 space-y-2">
          <p className="text-xs" style={{ color: 'var(--text-secondary)' }}>
            Import into <span className="font-mono">{dbImportModeDialog?.targetDb || '-'}</span>.
          </p>
          <p className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
            Replace: drop target database before import.
          </p>
          <p className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
            Merge: keep existing collections and import package data.
          </p>
        </div>
        <div className="px-5 py-4 flex items-center justify-end gap-2" style={{ borderTop: '1px solid var(--border)' }}>
          <button type="button" className="btn-ghost text-xs" onClick={() => closeDbImportModeDialog(null)} disabled={dbImportBusy}>
            Cancel
          </button>
          <button type="button" className="btn-ghost text-xs" onClick={() => closeDbImportModeDialog('merge')} disabled={dbImportBusy}>
            Merge
          </button>
          <button type="button" className="btn-primary text-xs" onClick={() => closeDbImportModeDialog('replace')} disabled={dbImportBusy}>
            Replace
          </button>
        </div>
      </AppModal>

      {/* Resize Handle */}
      <div className="absolute right-0 top-0 bottom-0 w-1 cursor-col-resize z-10 transition-colors"
        style={{background:'transparent'}}
        onMouseDown={handleMouseDown}
        onMouseOver={e=>e.currentTarget.style.background='var(--accent)33'}
        onMouseOut={e=>e.currentTarget.style.background='transparent'} />

      {toasts.length > 0 && (
        <div
          className="fixed z-[320] w-[min(92vw,380px)] flex flex-col gap-2 pointer-events-none"
          style={{ top: 'calc(var(--workspace-header-bottom, 56px) + var(--workspace-collection-toolbar-height, 0px) + 8px)', right: 'calc(var(--workspace-right-sidebar-width, 48px) + 8px)' }}
        >
          {toasts.map((toast) => (
            <ToastNotice
              key={toast.id}
              kind={toast.kind}
              message={toast.message}
              durationMs={5000}
              onClose={() => dismissToast(toast.id)}
            />
          ))}
        </div>
      )}
    </aside>
  );
}


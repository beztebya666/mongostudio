import React, { useState, useCallback, useEffect, useLayoutEffect, useMemo, useRef } from 'react';
import api from '../utils/api';
import {
  Filter, Refresh, Trash, Edit, ChevronLeft, ChevronRight, ChevronDown,
  Download, Copy, Loader, Check, X, Zap, Table, FileJson, Columns, ArrowUp, ArrowDown, Eye
} from './Icons';
import { formatNumber, formatBytes, safeJsonParse, prettyJson, copyToClipboard } from '../utils/formatters';
import JsonView from './JsonView';
import InlineAlert from './InlineAlert';
import DocumentEditor from './DocumentEditor';
import OperateModal from './OperateModal';
import IndexesView from './IndexesView';
import FloatingMenu from './FloatingMenu';
import DropdownSelect from './DropdownSelect';
import DatabaseExportDialog from './modals/DatabaseExportDialog';
import CollectionExportDialog from './modals/CollectionExportDialog';
import ConfirmDialog from './modals/ConfirmDialog';
import { exportSingleDatabase, shouldUseSmartStreamExport } from '../utils/exportUtils';
import ToastNotice from './ToastNotice';
import { genId } from '../utils/genId';

const DEFAULT_QUERY_TIMEOUT_MS = 5000;
const DEFAULT_QUERY_LIMIT = 50;
const SAFE_QUERY_TIMEOUT_MAX_MS = 30000;
const POWER_QUERY_TIMEOUT_PRESET_MAX_MS = 120000;
const POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS = 2147483000;
const POWER_QUERY_TIMEOUT_MAX_MS = POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS;
const QUERY_TIMEOUT_MAX_MS = POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS;
const SAFE_QUERY_LIMIT_MAX = 1000;
const POWER_QUERY_LIMIT_PRESET_MAX = 5000;
const QUERY_LIMIT_OVERRIDE_MAX = 50000;
const POWER_QUERY_LIMIT_MAX = 2147483000;
const QUERY_LIMIT_MAX = POWER_QUERY_LIMIT_MAX;
const EXPORT_LIMIT_SAFE_MAX = 50000;
const EXPORT_LIMIT_MAX = POWER_QUERY_LIMIT_MAX;
const EXPORT_CONFIRM_THRESHOLD = EXPORT_LIMIT_SAFE_MAX;
const QUERY_TIMEOUT_OPTIONS = [5000, 10000, 30000, 60000, POWER_QUERY_TIMEOUT_PRESET_MAX_MS];
const BASE_QUERY_LIMIT_OPTIONS = [50, 100, 200, 500, 1000, POWER_QUERY_LIMIT_PRESET_MAX];
const EXPORT_LIMIT_OPTIONS = ['exact', 500, 1000, 5000, 10000, EXPORT_LIMIT_SAFE_MAX, 'unlimited'];
const DB_EXPORT_TIMEOUT_MS = 4 * 60 * 60 * 1000;
const FILTER_HINT_FALLBACK = ['{ "_id": { "$oid": "" } }', '{ "$or": [] }'];
const TABLE_COLUMN_INITIAL_RENDER = 80;
const TABLE_COLUMN_RENDER_STEP = 80;
const TABLE_COLUMN_LAZY_THRESHOLD = 100;
const TABLE_COLUMN_MIN_WIDTH = 84;
const TABLE_COLUMN_MAX_WIDTH = 720;
const TABLE_COLUMN_DEFAULT_WIDTH = 180;
const TABLE_COLUMN_ID_WIDTH = 180;
const TABLE_COLUMN_ID_MIN_WIDTH = 96;
const TABLE_COLUMN_INLINE_MIN_WIDTH = 140;
const TABLE_COLUMN_ACTIONS_WIDTH = 96;
const TABLE_ROW_VIRTUALIZATION_THRESHOLD = 140;
const JSON_ROW_VIRTUALIZATION_THRESHOLD = 140;
const TABLE_ROW_ESTIMATED_HEIGHT = 34;
const JSON_ROW_ESTIMATED_HEIGHT = 56;
const LIST_VIRTUAL_OVERSCAN = 10;
const clampTableColumnWidth = (value, min = TABLE_COLUMN_MIN_WIDTH) => Math.max(
  Math.max(56, Number(min) || TABLE_COLUMN_MIN_WIDTH),
  Math.min(Number(value) || TABLE_COLUMN_DEFAULT_WIDTH, TABLE_COLUMN_MAX_WIDTH),
);
const isAbortError = (err) => {
  if (!err) return false;
  if (err.name === 'AbortError') return true;
  return /(?:^|\\s)(?:abort|aborted|cancelled|canceled)(?:\\s|$)/i.test(String(err.message || ''));
};
const clampTimeoutMs = (value) => Math.max(DEFAULT_QUERY_TIMEOUT_MS, Math.min(Number(value) || DEFAULT_QUERY_TIMEOUT_MS, QUERY_TIMEOUT_MAX_MS));
const getModeTimeoutMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_TIMEOUT_MAX_MS : SAFE_QUERY_TIMEOUT_MAX_MS);
const getModeLimitMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_LIMIT_MAX : QUERY_LIMIT_OVERRIDE_MAX);
const clampLimitValue = (value, mode = 'safe') => Math.max(DEFAULT_QUERY_LIMIT, Math.min(Number(value) || DEFAULT_QUERY_LIMIT, getModeLimitMax(mode)));

export default function CollectionView({ db, collection, onQueryMs, showIndexes, refreshToken = 0, isProduction = false }) {
  const [documents, setDocuments] = useState([]);
  const [total, setTotal] = useState({ state: 'unknown', value: null, approx: false, source: 'none', ts: 0 });
  const [totalJob, setTotalJob] = useState(null);
  const [loading, setLoading] = useState(false);
  const [docsReady, setDocsReady] = useState(false);
  const [error, setError] = useState(null);
  const [page, setPage] = useState(0);
  const [limit, setLimit] = useState(DEFAULT_QUERY_LIMIT);
  const [hasMore, setHasMore] = useState(false);
  const [filter, setFilter] = useState('{}');
  const [filterInput, setFilterInput] = useState('{}');
  const [filterRunTick, setFilterRunTick] = useState(0);
  const [showFilterEditor, setShowFilterEditor] = useState(false);
  const [filterEditorDraft, setFilterEditorDraft] = useState('{}');
  const [sort, setSort] = useState('{"_id":-1}');
  const [selectedDoc, setSelectedDoc] = useState(null);
  const [editingDoc, setEditingDoc] = useState(null);
  const [showOperateModal, setShowOperateModal] = useState(false);
  const [operateInitialTab, setOperateInitialTab] = useState('insert');
  const [stats, setStats] = useState(null);
  const [expandedRows, setExpandedRows] = useState(new Set());
  const [copied, setCopied] = useState(null);
  const [confirmDelete, setConfirmDelete] = useState(null);
  const [selectedDocIds, setSelectedDocIds] = useState(() => new Set());
  const [bulkDeleteBusy, setBulkDeleteBusy] = useState(false);
  const [viewMode, setViewMode] = useState('json'); // json | table
  const [tableColumnRenderLimit, setTableColumnRenderLimit] = useState(TABLE_COLUMN_INITIAL_RENDER);
  const [tableColumnWidths, setTableColumnWidths] = useState({});
  const [resizingColumnKey, setResizingColumnKey] = useState(null);
  const [documentsViewport, setDocumentsViewport] = useState({ scrollTop: 0, height: 0 });
  const [exporting, setExporting] = useState(false);
  const [exportProgress, setExportProgress] = useState(null);
  const [exportFormat, setExportFormat] = useState(false);
  const [showExportDialog, setShowExportDialog] = useState(false);
  const [pendingExportFormat, setPendingExportFormat] = useState('json');
  const [exportLimit, setExportLimit] = useState('exact');
  const [exportUseVisibleFields, setExportUseVisibleFields] = useState(false);
  const [exportUseCurrentSort, setExportUseCurrentSort] = useState(false);
  const [exportUseCurrentFilter, setExportUseCurrentFilter] = useState(false);
  const [showDbExportDialog, setShowDbExportDialog] = useState(false);
  const [dbExportMode, setDbExportMode] = useState('package');
  const [dbExportArchive, setDbExportArchive] = useState(false);
  const [dbExportCollectionFormat, setDbExportCollectionFormat] = useState('json');
  const [dbExportIncludeIndexes, setDbExportIncludeIndexes] = useState(false);
  const [dbExportIncludeSchema, setDbExportIncludeSchema] = useState(false);
  const [showSortMenu, setShowSortMenu] = useState(false);
  const [showColumnsMenu, setShowColumnsMenu] = useState(false);
  const [showInlineFieldsMenu, setShowInlineFieldsMenu] = useState(false);
  const [visibleColumns, setVisibleColumns] = useState([]);
  const [jsonPreviewKeys, setJsonPreviewKeys] = useState([]);
  const [expandedObjectFields, setExpandedObjectFields] = useState([]);
  const [filterSuggestions, setFilterSuggestions] = useState([]);
  const [filterSuggestionsLoading, setFilterSuggestionsLoading] = useState(false);
  const [showFilterAutofill, setShowFilterAutofill] = useState(false);
  const [filterAutofillLimit, setFilterAutofillLimit] = useState(8);
  const [placeholderIndex, setPlaceholderIndex] = useState(0);
  const [slow, setSlow] = useState(false);
  const [execConfig, setExecConfig] = useState(null);
  const [showBudgetMenu, setShowBudgetMenu] = useState(false);
  const [queryBudget, setQueryBudget] = useState({ timeoutMs: DEFAULT_QUERY_TIMEOUT_MS, limit: DEFAULT_QUERY_LIMIT });
  const [budgetDraft, setBudgetDraft] = useState({ timeoutMs: DEFAULT_QUERY_TIMEOUT_MS, limit: DEFAULT_QUERY_LIMIT });
  const [loadingLiveTimer, setLoadingLiveTimer] = useState(0);
  const [customTimeoutSeconds, setCustomTimeoutSeconds] = useState('');
  const [customLimitValue, setCustomLimitValue] = useState('');
  const [persistOverride, setPersistOverride] = useState(false);
  const [exactTotalTimeoutMs, setExactTotalTimeoutMs] = useState(30000);
  const [exactTotalBanner, setExactTotalBanner] = useState(null);
  const [confirmDialog, setConfirmDialog] = useState(null);
  const [showPageSizeMenu, setShowPageSizeMenu] = useState(false);
  const [showCustomPageSizeInput, setShowCustomPageSizeInput] = useState(false);
  const [customPageSizeInput, setCustomPageSizeInput] = useState('');
  const [toasts, setToasts] = useState([]);
  const exportMenuRef = useRef(null);
  const sortMenuRef = useRef(null);
  const columnsMenuRef = useRef(null);
  const inlineFieldsMenuRef = useRef(null);
  const filterAutofillRef = useRef(null);
  const budgetMenuRef = useRef(null);
  const pageSizeMenuRef = useRef(null);
  const collectionToolbarRef = useRef(null);
  const documentsViewportRef = useRef(null);
  const queryBudgetRef = useRef(queryBudget);
  const persistOverrideRef = useRef(persistOverride);
  const defaultBudgetRef = useRef({ timeoutMs: DEFAULT_QUERY_TIMEOUT_MS, limit: DEFAULT_QUERY_LIMIT });
  const documentsControllerRef = useRef(null);
  const documentsRequestIdRef = useRef(0);
  const documentsInFlightKeyRef = useRef('');
  const documentsBudgetTimeoutRef = useRef(null);
  const columnResizeCleanupRef = useRef(null);
  const filterSuggestionsWarmupTimerRef = useRef(null);
  const filterSuggestionsRequestSeqRef = useRef(0);
  const filterSuggestionsLoadedRef = useRef(false);
  const cursorHistoryRef = useRef(new Map([[0, null]])); // page -> keyset cursor (null means skip mode)
  const exportControllerRef = useRef(null);
  const totalPollTimerRef = useRef(null);
  const exactTotalBannerTimerRef = useRef(null);
  const loadingTimerRef = useRef(null);
  const loadingTimerStartedAtRef = useRef(0);
  const lastRefreshTokenRef = useRef(refreshToken);
  const loadDocumentsRef = useRef(null);
  const columnsStorageKey = `mongostudio_columns:${db}.${collection}`;
  const jsonPreviewStorageKey = `mongostudio_preview:${db}.${collection}`;
  const inlineFieldsStorageKey = `mongostudio_inline_fields:${db}.${collection}`;
  const sortStorageKey = `mongostudio_sort:${db}.${collection}`;
  const filterStorageKey = `mongostudio_filter:${db}.${collection}`;
  const totalValue = Number(total?.value);
  const totalKnown = total?.state === 'ready' && Number.isFinite(totalValue);
  const totalPages = totalKnown ? Math.max(1, Math.ceil(totalValue / Math.max(1, limit))) : null;
  const executionMode = execConfig?.mode === 'power' ? 'power' : 'safe';
  const modeLimitMax = getModeLimitMax(executionMode);
  const queryLimitOptions = useMemo(
    () => [...BASE_QUERY_LIMIT_OPTIONS, POWER_QUERY_LIMIT_MAX],
    [executionMode],
  );
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
  const defaultBudget = useMemo(() => ({
    timeoutMs: Math.min(clampTimeoutMs(execConfig?.maxTimeMS), getModeTimeoutMax(executionMode)),
    limit: clampLimitValue(execConfig?.maxResultSize, executionMode),
  }), [execConfig, executionMode]);

  useEffect(() => {
    defaultBudgetRef.current = defaultBudget;
  }, [defaultBudget]);
  useEffect(() => {
    queryBudgetRef.current = queryBudget;
  }, [queryBudget]);
  useEffect(() => {
    setBudgetDraft(queryBudget);
  }, [queryBudget.timeoutMs, queryBudget.limit]);
  useEffect(() => {
    persistOverrideRef.current = persistOverride;
  }, [persistOverride]);

  const stopTotalPolling = useCallback(() => {
    if (totalPollTimerRef.current) {
      clearTimeout(totalPollTimerRef.current);
      totalPollTimerRef.current = null;
    }
  }, []);

  const clearExactTotalBanner = useCallback(() => {
    if (exactTotalBannerTimerRef.current) {
      clearTimeout(exactTotalBannerTimerRef.current);
      exactTotalBannerTimerRef.current = null;
    }
    setExactTotalBanner(null);
  }, []);

  const showExactTotalBanner = useCallback((kind, message, timeoutMs = 5000) => {
    if (!message) return;
    if (exactTotalBannerTimerRef.current) {
      clearTimeout(exactTotalBannerTimerRef.current);
      exactTotalBannerTimerRef.current = null;
    }
    const id = Date.now();
    setExactTotalBanner({ id, kind, message });
    if (timeoutMs > 0) {
      exactTotalBannerTimerRef.current = setTimeout(() => {
        setExactTotalBanner((prev) => (prev?.id === id ? null : prev));
        exactTotalBannerTimerRef.current = null;
      }, timeoutMs);
    }
  }, []);

  const startLoadingLiveTimer = useCallback(() => {
    loadingTimerStartedAtRef.current = Date.now();
    setLoadingLiveTimer(0);
    if (loadingTimerRef.current) clearInterval(loadingTimerRef.current);
    loadingTimerRef.current = setInterval(() => {
      if (!loadingTimerStartedAtRef.current) return;
      setLoadingLiveTimer(Math.max(0, Date.now() - loadingTimerStartedAtRef.current));
    }, 100);
  }, []);

  const stopLoadingLiveTimer = useCallback(() => {
    if (loadingTimerRef.current) {
      clearInterval(loadingTimerRef.current);
      loadingTimerRef.current = null;
    }
    loadingTimerStartedAtRef.current = 0;
  }, []);

  const stopColumnResize = useCallback(() => {
    if (columnResizeCleanupRef.current) {
      columnResizeCleanupRef.current();
      columnResizeCleanupRef.current = null;
    }
    setResizingColumnKey(null);
  }, []);

  const getTableColumnMinWidth = useCallback((key) => {
    if (expandedObjectFields.includes(key)) return TABLE_COLUMN_INLINE_MIN_WIDTH;
    if (key === '_id') return TABLE_COLUMN_ID_MIN_WIDTH;
    return TABLE_COLUMN_MIN_WIDTH;
  }, [expandedObjectFields]);

  const getDefaultTableColumnWidth = useCallback((key) => {
    if (key === '_id') return TABLE_COLUMN_ID_WIDTH;
    const normalizedKey = String(key || '');
    const estimated = TABLE_COLUMN_DEFAULT_WIDTH + Math.min(normalizedKey.length, 24) * 2;
    return clampTableColumnWidth(estimated, getTableColumnMinWidth(key));
  }, [getTableColumnMinWidth]);

  const getResolvedTableColumnWidth = useCallback((key) => {
    const custom = Number(tableColumnWidths?.[key]);
    if (Number.isFinite(custom) && custom > 0) return clampTableColumnWidth(custom, getTableColumnMinWidth(key));
    return getDefaultTableColumnWidth(key);
  }, [getDefaultTableColumnWidth, getTableColumnMinWidth, tableColumnWidths]);

  const resetTableColumnWidth = useCallback((event, key) => {
    event.preventDefault();
    event.stopPropagation();
    setTableColumnWidths((prev) => {
      if (!Object.prototype.hasOwnProperty.call(prev, key)) return prev;
      const next = { ...prev };
      delete next[key];
      return next;
    });
  }, []);

  const startTableColumnResize = useCallback((event, key) => {
    if (event.button !== 0) return;
    event.preventDefault();
    event.stopPropagation();
    if (typeof document === 'undefined') return;

    stopColumnResize();

    const startX = Number(event.clientX) || 0;
    const minWidth = getTableColumnMinWidth(key);
    const startWidth = getResolvedTableColumnWidth(key);
    const body = document.body;
    const prevCursor = body?.style?.cursor || '';
    const prevSelect = body?.style?.userSelect || '';
    if (body) {
      body.style.cursor = 'col-resize';
      body.style.userSelect = 'none';
    }
    setResizingColumnKey(key);

    const onMove = (moveEvent) => {
      const delta = Number(moveEvent.clientX) - startX;
      const nextWidth = clampTableColumnWidth(startWidth + delta, minWidth);
      setTableColumnWidths((prev) => {
        if (prev[key] === nextWidth) return prev;
        return { ...prev, [key]: nextWidth };
      });
    };

    const onUp = () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      if (body) {
        body.style.cursor = prevCursor;
        body.style.userSelect = prevSelect;
      }
      setResizingColumnKey(null);
      columnResizeCleanupRef.current = null;
    };

    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
    columnResizeCleanupRef.current = () => {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      if (body) {
        body.style.cursor = prevCursor;
        body.style.userSelect = prevSelect;
      }
    };
  }, [getResolvedTableColumnWidth, getTableColumnMinWidth, stopColumnResize]);

  const clearDocumentsBudgetTimeout = useCallback(() => {
    if (documentsBudgetTimeoutRef.current) {
      clearTimeout(documentsBudgetTimeoutRef.current);
      documentsBudgetTimeoutRef.current = null;
    }
  }, []);

  const syncDocumentsViewport = useCallback(() => {
    const viewport = documentsViewportRef.current;
    if (!viewport) return;
    const next = {
      scrollTop: Math.max(0, Math.round(viewport.scrollTop || 0)),
      height: Math.max(0, Math.round(viewport.clientHeight || 0)),
    };
    setDocumentsViewport((prev) => (
      prev.scrollTop === next.scrollTop && prev.height === next.height ? prev : next
    ));
  }, []);

  const handleDocumentsViewportScroll = useCallback((event) => {
    const target = event?.currentTarget;
    if (!target) return;
    const next = {
      scrollTop: Math.max(0, Math.round(target.scrollTop || 0)),
      height: Math.max(0, Math.round(target.clientHeight || 0)),
    };
    setDocumentsViewport((prev) => (
      prev.scrollTop === next.scrollTop && prev.height === next.height ? prev : next
    ));
  }, []);

  useEffect(() => {
    syncDocumentsViewport();
  }, [syncDocumentsViewport, documents.length, viewMode]);

  useEffect(() => {
    const onResize = () => syncDocumentsViewport();
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, [syncDocumentsViewport]);

  useEffect(() => () => {
    stopLoadingLiveTimer();
    clearDocumentsBudgetTimeout();
    stopColumnResize();
  }, [stopLoadingLiveTimer, clearDocumentsBudgetTimeout, stopColumnResize]);

  const pollTotalJob = useCallback(async (jobId) => {
    if (!jobId) return;
    stopTotalPolling();
    try {
      const job = await api.getJob(jobId);
      setTotalJob(job);
      if (job.state === 'done') {
        const value = Number(job?.result?.value || 0);
        setTotal({ state: 'ready', value, approx: false, source: 'exact', ts: Date.now() });
        showExactTotalBanner('success', `Exact total updated: ${formatNumber(value)}`);
        return;
      }
      if (job.state === 'timeout') {
        showExactTotalBanner('warning', `Exact total timed out (${Math.round(exactTotalTimeoutMs / 1000)}s). Showing current total.`);
        return;
      }
      if (job.state === 'error' || job.state === 'cancelled') {
        const reason = job.error ? `: ${job.error}` : '';
        showExactTotalBanner('error', `Exact total ${job.state}${reason}`);
        return;
      }
      totalPollTimerRef.current = setTimeout(() => { pollTotalJob(jobId); }, 900);
    } catch (err) {
      if (err?.name === 'AbortError') return;
      setError(err.message || 'Failed to poll total job');
    }
  }, [exactTotalTimeoutMs, showExactTotalBanner, stopTotalPolling]);

  const loadDocuments = useCallback(async (p = page, budget = queryBudgetRef.current, options = {}) => {
    const skipRiskConfirm = Boolean(options?.skipRiskConfirm);
    const safeBudget = {
      timeoutMs: Math.min(
        clampTimeoutMs(Number.isFinite(Number(budget?.timeoutMs)) ? Number(budget.timeoutMs) : defaultBudget.timeoutMs),
        getModeTimeoutMax(executionMode),
      ),
      limit: clampLimitValue(Number.isFinite(Number(budget?.limit)) ? Number(budget.limit) : defaultBudget.limit, executionMode),
    };
    const requestLimit = clampLimitValue(safeBudget.limit, executionMode);
    const keysetCursor = cursorHistoryRef.current.get(p) ?? null;
    const skip = keysetCursor ? 0 : Math.max(0, p) * requestLimit;
    const requestKey = JSON.stringify({
      db,
      collection,
      filter,
      sort,
      page: p,
      limit: requestLimit,
      keysetCursor: keysetCursor || null,
      timeoutMs: safeBudget.timeoutMs,
    });
    if (documentsControllerRef.current && documentsInFlightKeyRef.current === requestKey) {
      return;
    }
    if (!skipRiskConfirm && executionMode === 'power' && safeBudget.timeoutMs > POWER_QUERY_TIMEOUT_PRESET_MAX_MS) {
      setConfirmDialog({
        title: 'High Timeout Override',
        message: `${Math.round(safeBudget.timeoutMs / 1000)}s timeout can be heavy on production. Continue with this documents query?`,
        confirmLabel: 'Run query',
        onConfirm: () => loadDocuments(p, safeBudget, { skipRiskConfirm: true }),
      });
      return;
    }
    setLoading(true); setError(null); setSlow(false);
    startLoadingLiveTimer();
    documentsControllerRef.current?.abort();
    const controller = new AbortController();
    documentsControllerRef.current = controller;
    const requestId = documentsRequestIdRef.current + 1;
    documentsRequestIdRef.current = requestId;
    documentsInFlightKeyRef.current = requestKey;
    let clientBudgetTimedOut = false;
    clearDocumentsBudgetTimeout();
    documentsBudgetTimeoutRef.current = setTimeout(() => {
      if (documentsRequestIdRef.current !== requestId) return;
      clientBudgetTimedOut = true;
      controller.abort();
    }, Math.max(100, safeBudget.timeoutMs + 100));
    try {
      const data = await api.getDocuments(
        db,
        collection,
        { filter, sort, skip, limit: requestLimit, keysetCursor },
        controller,
        { budget: safeBudget },
      );
      if (documentsRequestIdRef.current !== requestId) return;
      if (data?.page?.keysetCursor) {
        cursorHistoryRef.current.set(p + 1, data.page.keysetCursor);
        if (cursorHistoryRef.current.size > 500) {
          const firstKey = cursorHistoryRef.current.keys().next().value;
          cursorHistoryRef.current.delete(firstKey);
        }
      }
      const nextDocs = data.documents || [];
      const totalState = data.total && typeof data.total === 'object'
        ? data.total
        : (Number.isFinite(Number(data.totalLegacy))
          ? { state: 'ready', value: Number(data.totalLegacy), approx: false, source: 'legacy', ts: Date.now() }
          : { state: 'unknown', value: null, approx: false, source: 'none', ts: 0 });
      const pageHasMore = Boolean(data?.page?.hasMore);
      if (String(totalState?.source || '').toLowerCase().includes('exact')) {
        totalState.approx = false;
      }
      if (!pageHasMore && (nextDocs.length > 0 || skip === 0)) {
        totalState.state = 'ready';
        totalState.value = skip + nextDocs.length;
        totalState.approx = false;
        if (!String(totalState.source || '').toLowerCase().includes('exact')) {
          totalState.source = 'page';
        }
        totalState.ts = Date.now();
      }
      setDocuments(nextDocs);
      setSelectedDocIds((prev) => {
        if (!(prev instanceof Set) || prev.size === 0) return prev;
        const visibleIds = new Set(nextDocs.map((doc) => {
          if (!doc || !Object.prototype.hasOwnProperty.call(doc, '_id')) return null;
          if (doc._id === null || doc._id === undefined) return null;
          return typeof doc._id === 'object' ? (doc._id.$oid || JSON.stringify(doc._id)) : String(doc._id);
        }).filter(Boolean));
        const nextSelected = new Set();
        for (const id of prev) {
          if (visibleIds.has(id)) nextSelected.add(id);
        }
        if (nextSelected.size === prev.size) return prev;
        return nextSelected;
      });
      setTotal(totalState);
      setHasMore(pageHasMore);
      setLimit(clampLimitValue(Number(data?.page?.limit || requestLimit), executionMode));
      setDocsReady(true);
      onQueryMs?.(data._elapsed);
      if (data._slow) setSlow(true);
      const usedOverride = safeBudget.timeoutMs !== defaultBudget.timeoutMs || safeBudget.limit !== defaultBudget.limit;
      if (usedOverride && !persistOverride) {
        setQueryBudget(defaultBudget);
        if (requestLimit !== defaultBudget.limit && p !== 0) setPage(0);
      }
    } catch (err) {
      if (documentsRequestIdRef.current !== requestId) return;
      if (err?.name === 'AbortError') {
        if (!clientBudgetTimedOut) return;
        setDocuments([]);
        setTotal({ state: 'unknown', value: null, approx: false, source: 'none', ts: 0 });
        setHasMore(false);
        setDocsReady(true);
        setError('Query timed out. Try filters or indexes.');
        return;
      }
      setDocuments([]);
      setTotal({ state: 'unknown', value: null, approx: false, source: 'none', ts: 0 });
      setHasMore(false);
      setDocsReady(true);
      setError(err.message);
    } finally {
      if (documentsRequestIdRef.current !== requestId) return;
      clearDocumentsBudgetTimeout();
      if (documentsControllerRef.current === controller) documentsControllerRef.current = null;
      documentsInFlightKeyRef.current = '';
      stopLoadingLiveTimer();
      setLoading(false);
    }
  }, [db, collection, filter, sort, page, onQueryMs, persistOverride, defaultBudget, executionMode, startLoadingLiveTimer, stopLoadingLiveTimer, clearDocumentsBudgetTimeout]);

  const loadStats = useCallback(async () => {
    try { setStats(await api.getCollectionStats(db, collection, { budget: defaultBudget })); }
    catch { setStats(null); }
  }, [db, collection, defaultBudget]);

  useEffect(() => {
    loadDocumentsRef.current = loadDocuments;
  }, [loadDocuments]);

  const loadFilterSuggestions = useCallback(async () => {
    if (filterSuggestionsLoadedRef.current) return;
    filterSuggestionsLoadedRef.current = true;
    const seq = filterSuggestionsRequestSeqRef.current + 1;
    filterSuggestionsRequestSeqRef.current = seq;
    setFilterSuggestionsLoading(true);
    try {
      const data = await api.getSchema(db, collection, 120, { budget: defaultBudget });
      if (filterSuggestionsRequestSeqRef.current !== seq) return;
      const fields = (data.fields || []).filter((f) => Boolean(f?.path)).slice(0, 30);
      const hints = [];
      for (const fieldInfo of fields) {
        const path = fieldInfo.path;
        const fieldType = getPrimaryFieldType(fieldInfo);
        const literal = buildFilterLiteral(fieldInfo);
        if (fieldType === 'array') {
          hints.push(`{ "${path}.0": { "$exists": true } }`);
          hints.push(`{ "${path}": { "$size": 0 } }`);
          hints.push(`{ "${path}": { "$in": [{ "$oid": "" }] } }`);
        } else {
          hints.push(`{ "${path}": ${literal} }`);
        }
        hints.push(`{ "${path}": { "$exists": true } }`);
        if (fieldType !== 'array') hints.push(`{ "${path}": { "$in": [${literal}] } }`);
      }
      hints.push('{ "$or": [] }');
      setFilterSuggestions([...new Set(hints)].slice(0, 80));
    } catch {
      if (filterSuggestionsRequestSeqRef.current !== seq) return;
      setFilterSuggestions(FILTER_HINT_FALLBACK);
    } finally {
      if (filterSuggestionsRequestSeqRef.current === seq) setFilterSuggestionsLoading(false);
    }
  }, [db, collection, defaultBudget]);

  useEffect(() => {
    documentsControllerRef.current?.abort();
    exportControllerRef.current?.abort('scope_change');
    exportControllerRef.current = null;
    clearDocumentsBudgetTimeout();
    documentsInFlightKeyRef.current = '';
    stopLoadingLiveTimer();
    stopTotalPolling();
    clearExactTotalBanner();
    setDocuments([]);
    setTotal({ state: 'unknown', value: null, approx: false, source: 'none', ts: 0 });
    setTotalJob(null);
    setError(null);
    setDocsReady(false);
    setStats(null);
    setSlow(false);
    setCopied(null);
    setConfirmDelete(null);
    setSelectedDocIds(new Set());
    setBulkDeleteBusy(false);
    setPage(0);
    setLimit(DEFAULT_QUERY_LIMIT);
    setHasMore(false);
    setFilter('{}');
    setFilterInput('');
    setFilterRunTick(0);
    setShowFilterEditor(false);
    setFilterEditorDraft('{}');
    setSort('{"_id":-1}');
    setSelectedDoc(null);
    setEditingDoc(null);
    setShowOperateModal(false);
    setOperateInitialTab('insert');
    setExportFormat(false);
    setShowExportDialog(false);
    setPendingExportFormat('json');
    setExportLimit('exact');
    setExportUseVisibleFields(false);
    setExportUseCurrentSort(false);
    setExportUseCurrentFilter(false);
    setExporting(false);
    setExportProgress(null);
    setShowDbExportDialog(false);
    setDbExportMode('package');
    setDbExportArchive(false);
    setDbExportCollectionFormat('json');
    setDbExportIncludeIndexes(false);
    setDbExportIncludeSchema(false);
    setShowSortMenu(false);
    setShowColumnsMenu(false);
    setShowInlineFieldsMenu(false);
    stopColumnResize();
    setTableColumnWidths({});
    setVisibleColumns([]);
    setJsonPreviewKeys([]);
    setExpandedObjectFields([]);
    setFilterSuggestions([]);
    setFilterSuggestionsLoading(false);
    setShowFilterAutofill(false);
    setFilterAutofillLimit(8);
    setPlaceholderIndex(0);
    setLoadingLiveTimer(0);
    setExpandedRows(new Set());
    setShowBudgetMenu(false);
    if (!persistOverrideRef.current) {
      setQueryBudget(defaultBudgetRef.current);
      setPersistOverride(false);
    }
    setExactTotalTimeoutMs(30000);
  }, [clearExactTotalBanner, db, collection, stopTotalPolling, stopLoadingLiveTimer, clearDocumentsBudgetTimeout, stopColumnResize]);
  useEffect(() => {
    // Delay by one tick so StrictMode mount/unmount remount in dev does not emit duplicate
    // initial document queries (which pollutes audit with duplicate "query" entries).
    const timer = setTimeout(() => {
      loadDocumentsRef.current(page);
    }, 0);
    return () => clearTimeout(timer);
  }, [page, filter, sort, db, collection, filterRunTick]);
  // Reset keyset cursor history whenever the query changes (new collection, filter, or sort)
  useEffect(() => {
    cursorHistoryRef.current = new Map();
    cursorHistoryRef.current.set(0, null);
  }, [filter, sort, db, collection]);
  useEffect(() => { loadStats(); }, [loadStats]);
  useEffect(() => {
    filterSuggestionsLoadedRef.current = false;
    filterSuggestionsRequestSeqRef.current += 1;
    setFilterSuggestions([]);
    setFilterSuggestionsLoading(false);
    if (filterSuggestionsWarmupTimerRef.current) {
      clearTimeout(filterSuggestionsWarmupTimerRef.current);
      filterSuggestionsWarmupTimerRef.current = null;
    }
    filterSuggestionsWarmupTimerRef.current = setTimeout(() => {
      loadFilterSuggestions();
      filterSuggestionsWarmupTimerRef.current = null;
    }, 1500);
    return () => {
      if (filterSuggestionsWarmupTimerRef.current) {
        clearTimeout(filterSuggestionsWarmupTimerRef.current);
        filterSuggestionsWarmupTimerRef.current = null;
      }
    };
  }, [db, collection, refreshToken, loadFilterSuggestions]);
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
    if (persistOverrideRef.current) return;
    setQueryBudget(defaultBudget);
  }, [defaultBudget]);
  useEffect(() => {
    if (!db || !collection) return;
    if (lastRefreshTokenRef.current === refreshToken) return;
    lastRefreshTokenRef.current = refreshToken;
    if (refreshToken <= 0) return;
    loadDocuments(page);
    loadStats();
  }, [refreshToken, db, collection, page, loadDocuments, loadStats]);
  useEffect(() => {
    if (!showFilterEditor) return undefined;
    if (typeof document === 'undefined') return undefined;
    const body = document.body;
    const html = document.documentElement;
    const prevBodyOverflow = body?.style?.overflow || '';
    const prevHtmlOverflow = html?.style?.overflow || '';
    if (body) body.style.overflow = 'hidden';
    if (html) html.style.overflow = 'hidden';
    const onKeyDown = (event) => {
      if (event.key === 'Escape') setShowFilterEditor(false);
    };
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('keydown', onKeyDown);
      if (body) body.style.overflow = prevBodyOverflow;
      if (html) html.style.overflow = prevHtmlOverflow;
    };
  }, [showFilterEditor]);

  useEffect(() => () => {
    documentsControllerRef.current?.abort();
    exportControllerRef.current?.abort('unmount');
    exportControllerRef.current = null;
    clearDocumentsBudgetTimeout();
    documentsInFlightKeyRef.current = '';
    stopColumnResize();
    stopTotalPolling();
    clearExactTotalBanner();
    if (filterSuggestionsWarmupTimerRef.current) {
      clearTimeout(filterSuggestionsWarmupTimerRef.current);
      filterSuggestionsWarmupTimerRef.current = null;
    }
  }, [clearExactTotalBanner, stopTotalPolling, clearDocumentsBudgetTimeout, stopColumnResize]);

  useLayoutEffect(() => {
    if (typeof document === 'undefined') return undefined;
    const root = document.documentElement;
    const toolbar = collectionToolbarRef.current;
    if (showIndexes || editingDoc || !toolbar) {
      root.style.setProperty('--workspace-collection-toolbar-height', '0px');
      return () => {};
    }

    const applyToolbarHeight = () => {
      const heightPx = Math.max(0, Math.round(toolbar.getBoundingClientRect().height));
      root.style.setProperty('--workspace-collection-toolbar-height', `${heightPx}px`);
    };

    applyToolbarHeight();
    window.addEventListener('resize', applyToolbarHeight);
    if (typeof ResizeObserver === 'undefined') {
      return () => {
        window.removeEventListener('resize', applyToolbarHeight);
        root.style.removeProperty('--workspace-collection-toolbar-height');
      };
    }

    const observer = new ResizeObserver(() => applyToolbarHeight());
    observer.observe(toolbar);
    return () => {
      observer.disconnect();
      window.removeEventListener('resize', applyToolbarHeight);
      root.style.removeProperty('--workspace-collection-toolbar-height');
    };
  }, [showIndexes, editingDoc, db, collection]);

  const handleApplyFilter = (rawInput = filterInput) => {
    const normalizedFilter = String(rawInput || '').trim() || '{}';
    const { error } = safeJsonParse(normalizedFilter);
    if (error) {
      setError(`Invalid filter JSON: ${error}`);
      return false;
    }
    setFilter(normalizedFilter);
    setPage(0);
    setFilterRunTick((prev) => prev + 1);
    setFilterInput(normalizedFilter === '{}' ? '' : normalizedFilter);
    setShowFilterAutofill(false);
    return true;
  };

  const handleBudgetDraftChange = (changes = {}) => {
    setBudgetDraft((prev) => {
      const next = { ...prev, ...changes };
      next.timeoutMs = Math.min(clampTimeoutMs(next.timeoutMs), getModeTimeoutMax(executionMode));
      next.limit = clampLimitValue(next.limit, executionMode);
      return next;
    });
  };

  const applyBudgetDraft = () => {
    setQueryBudget({
      timeoutMs: Math.min(clampTimeoutMs(budgetDraft.timeoutMs), getModeTimeoutMax(executionMode)),
      limit: clampLimitValue(budgetDraft.limit, executionMode),
    });
    setShowBudgetMenu(false);
  };

  const runExactTotal = async (skipRiskConfirm = false) => {
    const budget = Math.max(5000, Math.min(Number(exactTotalTimeoutMs) || 30000, QUERY_TIMEOUT_MAX_MS));
    if (!skipRiskConfirm && executionMode === 'power' && budget > POWER_QUERY_TIMEOUT_PRESET_MAX_MS) {
      setConfirmDialog({
        title: 'High Exact Total Timeout',
        message: `${Math.round(budget / 1000)}s exact total can be expensive. Continue?`,
        confirmLabel: 'Start job',
        onConfirm: () => runExactTotal(true),
      });
      return;
    }
    setError(null);
    setTotalJob({ state: 'queued', progressPct: 0 });
    try {
      const started = await api.startExactTotal(
        db,
        collection,
        {
          filter,
          projection: '{}',
          hint: 'auto',
          timeoutMs: budget,
        },
        { heavyConfirm: true, heavyTimeoutMs: budget },
      );
      if (!started?.jobId) throw new Error('Failed to start exact total job.');
      setTotalJob({ jobId: started.jobId, state: started.state || 'queued', progressPct: 0 });
      showExactTotalBanner('info', `Exact total started (${Math.round(budget / 1000)}s budget).`, 2500);
      pollTotalJob(started.jobId);
    } catch (err) {
      setTotalJob(null);
      setError(err.message || 'Failed to start exact total');
    }
  };

  const handleDelete = async (doc) => {
    const id = docId(doc);
    if (!id) { setError('Cannot delete document without _id'); return; }
    try {
      await api.deleteDocument(db, collection, id);
      setSelectedDocIds((prev) => {
        if (!(prev instanceof Set) || !prev.has(id)) return prev;
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
      setConfirmDelete(null);
      loadDocuments();
    }
    catch (err) { setError(err.message); }
  };

  const handleCopy = (doc, key) => { copyToClipboard(prettyJson(doc)).then((ok) => { if (ok) { setCopied(key); setTimeout(() => setCopied(null), 2000); } }); };
  const toggleRow = (id) => { setExpandedRows(prev => { const n=new Set(prev); if(n.has(id)) n.delete(id); else n.add(id); return n; }); };

  const downloadText = (filename, text, mime) => {
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

  const openCollectionExportDialog = (format) => {
    setPendingExportFormat(format);
    setExportLimit('exact');
    setExportUseVisibleFields(false);
    setExportUseCurrentSort(false);
    setExportUseCurrentFilter(false);
    setShowExportDialog(true);
    setExportFormat(false);
    setExportProgress(null);
  };

  const handleExport = async (format, options = {}) => {
    const useVisibleFields = Boolean(options.useVisibleFields);
    const includeSort = options.includeSort !== false;
    const useCurrentFilter = Boolean(options.useCurrentFilter);
    const limitChoice = normalizeExportLimitChoice(options.limit ?? exportLimit);
    const limitPayload = resolveExportLimitPayload(limitChoice);
    const exportFilter = useCurrentFilter ? filter : '{}';
    let exportEstimate = Number.isFinite(Number(options?.estimate)) ? Number(options.estimate) : null;
    const projection = useVisibleFields
      ? JSON.stringify(Object.fromEntries(exportVisibleFields.map((field) => [field, 1])))
      : '{}';

    if (useVisibleFields && exportVisibleFields.length === 0) {
      setError('No visible fields selected for export.');
      return;
    }

    let exportController = null;
    try {
      if (!options?.confirmed) {
        try {
          const preflight = await api.preflight(db, collection, {
            operation: 'export',
            filter: exportFilter,
            limit: limitPayload,
          }, { budget: { timeoutMs: 60000, limit: 1000 } });
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
          const scopeText = useCurrentFilter ? 'current filter' : 'whole collection';
          setConfirmDialog({
            title: 'Large Export',
            message: `This export can be large (${estimateText} docs, ${scopeText}). Continue?`,
            confirmLabel: 'Export',
            onConfirm: () => {
              handleExport(format, { ...options, confirmed: true, estimate: exportEstimate });
            },
          });
          return;
        }
      }

      setExporting(true);
      exportController = new AbortController();
      exportControllerRef.current = exportController;
      setExportProgress(null);
      const exportTimeoutMs = resolveExportTimeoutMs(limitPayload);
      const shouldStreamToFile = shouldUseSmartStreamExport({
        limitChoice,
        limitValue: limitPayload,
        estimate: exportEstimate,
      });
      if (shouldStreamToFile && typeof api?.exportDataToFile === 'function') {
        await api.exportDataToFile(db, collection, {
          format,
          filter: exportFilter,
          sort: includeSort ? sort : '{}',
          limit: limitPayload,
          projection,
        }, {
          heavyTimeoutMs: exportTimeoutMs,
          heavyConfirm: true,
          controller: exportController,
          onProgress: (next) => {
            setExportProgress(next && typeof next === 'object' ? next : null);
          },
          filename: `${collection}.${format === 'csv' ? 'csv' : 'json'}`,
        });
      } else {
        const data = await api.exportData(db, collection, {
          format,
          filter: exportFilter,
          sort: includeSort ? sort : '{}',
          limit: limitPayload,
          projection,
        }, {
          heavyTimeoutMs: exportTimeoutMs,
          heavyConfirm: true,
          controller: exportController,
          onProgress: (next) => {
            setExportProgress(next && typeof next === 'object' ? next : null);
          },
        });
        downloadText(`${collection}.${format}`, data.data, format === 'csv' ? 'text/csv' : 'application/json');
      }
      setExportFormat(false);
      setShowExportDialog(false);
      notifySuccess(`Collection "${collection}" exported.`);
    } catch (err) {
      if (isAbortError(err)) return;
      setError(err.message);
      notifyError(err.message);
    }
    finally {
      if (exportControllerRef.current === exportController) exportControllerRef.current = null;
      setExporting(false);
      setExportProgress(null);
    }
  };

  const openDbExportDialog = () => {
    setDbExportMode('package');
    setDbExportArchive(false);
    setDbExportCollectionFormat('json');
    setDbExportIncludeIndexes(false);
    setDbExportIncludeSchema(false);
    setShowDbExportDialog(true);
    setExportFormat(false);
    setExportProgress(null);
  };

  const handleExportDatabase = async (skipConfirm = false) => {
    let exportController = null;
    if (!skipConfirm) {
      try {
        const statsRes = await api.listCollections(db, { withStats: true, source: 'collection' });
        const approxDocs = (statsRes?.collections || []).reduce((sum, entry) => {
          const next = Number(entry?.count);
          return sum + (Number.isFinite(next) && next > 0 ? next : 0);
        }, 0);
        if (approxDocs > EXPORT_CONFIRM_THRESHOLD) {
          setConfirmDialog({
            title: 'Large Database Export',
            message: `Database "${db}" has about ${formatNumber(approxDocs)} documents. Continue export?`,
            confirmLabel: 'Export',
            onConfirm: () => {
              handleExportDatabase(true);
            },
          });
          return;
        }
      } catch {}
    }
    exportController = new AbortController();
    exportControllerRef.current = exportController;
    setExporting(true);
    setExportProgress(null);
    try {
      await exportSingleDatabase(db, {
        mode: dbExportMode,
        archive: dbExportArchive,
        collectionFormat: dbExportCollectionFormat,
        includeIndexes: dbExportIncludeIndexes,
        includeSchema: dbExportIncludeSchema,
        heavyTimeoutMs: DB_EXPORT_TIMEOUT_MS,
        heavyConfirm: true,
        controller: exportController,
        onProgress: (next) => {
          setExportProgress(next && typeof next === 'object' ? next : null);
        },
      });
      setShowDbExportDialog(false);
      setExportFormat(false);
      notifySuccess(`Database "${db}" exported.`);
    } catch (err) {
      if (isAbortError(err)) return;
      setError(err.message);
      notifyError(err.message);
    } finally {
      if (exportControllerRef.current === exportController) exportControllerRef.current = null;
      setExporting(false);
      setExportProgress(null);
    }
  };

  const cancelCollectionExport = () => {
    try { exportControllerRef.current?.abort('user_cancel'); } catch {}
    exportControllerRef.current = null;
    setExporting(false);
    setExportProgress(null);
    setShowExportDialog(false);
  };

  const cancelDatabaseExport = () => {
    try { exportControllerRef.current?.abort('user_cancel'); } catch {}
    exportControllerRef.current = null;
    setExporting(false);
    setExportProgress(null);
    setShowDbExportDialog(false);
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

  const pageSizeOptions = useMemo(() => queryLimitOptions, [queryLimitOptions]);
  const pageSizeSelectOptions = useMemo(() => {
    const normalizedLimit = clampLimitValue(limit, executionMode);
    if (!Number.isFinite(normalizedLimit) || pageSizeOptions.includes(normalizedLimit)) return pageSizeOptions;
    return [...pageSizeOptions, normalizedLimit].sort((a, b) => a - b);
  }, [executionMode, limit, pageSizeOptions]);
  const formatTimeoutOptionLabel = (value) => `${Math.round(value / 1000)}s`;
  const formatLimitOptionLabel = (value) => (value >= POWER_QUERY_LIMIT_MAX ? 'Unlimited' : formatNumber(value));
  const normalizeExportLimitChoice = (value) => {
    const raw = String(value ?? '').trim().toLowerCase();
    if (!raw) return 'exact';
    if (raw === 'exact') return 'exact';
    if (raw === 'unlimited' || raw === 'all') return 'unlimited';
    const numeric = Math.floor(Number(raw));
    if (!Number.isFinite(numeric) || numeric <= 0) return 'exact';
    return String(Math.min(numeric, EXPORT_LIMIT_MAX));
  };
  const getExportLimitChoiceLabel = (value) => {
    const choice = normalizeExportLimitChoice(value);
    if (choice === 'exact') return 'Exact';
    if (choice === 'unlimited') return 'Unlimited';
    return formatNumber(Number(choice));
  };
  const resolveExportLimitPayload = (value) => {
    const choice = normalizeExportLimitChoice(value);
    if (choice === 'exact' || choice === 'unlimited') return choice;
    return Math.max(1, Math.min(Number(choice) || 1000, EXPORT_LIMIT_MAX));
  };
  const resolveExportTimeoutMs = (limitPayload) => {
    if (limitPayload === 'exact' || limitPayload === 'unlimited') return 1800000;
    const numeric = Number(limitPayload) || 1000;
    if (numeric >= EXPORT_CONFIRM_THRESHOLD) return 600000;
    if (numeric >= 10000) return 180000;
    return 120000;
  };
  const exportLimitSelectOptions = EXPORT_LIMIT_OPTIONS.map((value) => {
    if (value === 'exact' || value === 'unlimited') {
      return { value, label: getExportLimitChoiceLabel(value) };
    }
    return { value: String(value), label: getExportLimitChoiceLabel(value) };
  });
  const exportNoModifiers = !exportUseVisibleFields && !exportUseCurrentSort && !exportUseCurrentFilter;
  const buildFilterLiteral = (fieldInfo) => {
    const primaryType = String(fieldInfo?.types?.[0]?.type || '').toLowerCase();
    const sample = fieldInfo?.sample;
    if (primaryType === 'number') {
      const n = Number(sample);
      return Number.isFinite(n) ? String(n) : '0';
    }
    if (primaryType === 'boolean') {
      const normalized = String(sample || '').toLowerCase();
      return normalized === 'false' ? 'false' : 'true';
    }
    if (primaryType === 'objectid') return '{ "$oid": "" }';
    if (primaryType === 'date') return '{ "$date": "2026-01-01T00:00:00.000Z" }';
    if (primaryType === 'null') return 'null';
    if (primaryType === 'array') return '[ ]';
    if (primaryType === 'object') return '{}';
    const text = typeof sample === 'string' && sample.length > 0 ? sample : '';
    return JSON.stringify(text);
  };
  const getPrimaryFieldType = (fieldInfo) => String(fieldInfo?.types?.[0]?.type || '').toLowerCase();
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
  const lazyTableColumnsEnabled = viewMode === 'table' && tableColumns.length > TABLE_COLUMN_LAZY_THRESHOLD;
  const effectiveTableColumnRenderLimit = lazyTableColumnsEnabled
    ? Math.max(TABLE_COLUMN_INITIAL_RENDER, Math.min(tableColumnRenderLimit, tableColumns.length))
    : tableColumns.length;
  const renderedTableColumns = lazyTableColumnsEnabled
    ? tableColumns.slice(0, effectiveTableColumnRenderLimit)
    : tableColumns;
  const renderedTableMinWidth = useMemo(() => {
    const contentWidth = renderedTableColumns.reduce((sum, key) => sum + getResolvedTableColumnWidth(key), 0);
    return Math.max(640, contentWidth + TABLE_COLUMN_ACTIONS_WIDTH);
  }, [getResolvedTableColumnWidth, renderedTableColumns]);
  const hiddenRenderedTableColumns = Math.max(tableColumns.length - renderedTableColumns.length, 0);
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
  const tableVirtualizationEnabled = viewMode === 'table'
    && documents.length >= TABLE_ROW_VIRTUALIZATION_THRESHOLD
    && expandedObjectFields.length === 0
    && renderedTableColumns.length > 0;
  const jsonVirtualizationEnabled = viewMode === 'json'
    && documents.length >= JSON_ROW_VIRTUALIZATION_THRESHOLD
    && expandedRows.size === 0;
  const listVirtualizationEnabled = tableVirtualizationEnabled || jsonVirtualizationEnabled;
  const rowEstimatedHeight = tableVirtualizationEnabled ? TABLE_ROW_ESTIMATED_HEIGHT : JSON_ROW_ESTIMATED_HEIGHT;
  const viewportHeight = Math.max(1, documentsViewport.height || 1);
  const virtualWindow = useMemo(() => {
    if (!listVirtualizationEnabled) {
      return {
        start: 0,
        end: documents.length,
        topSpacer: 0,
        bottomSpacer: 0,
      };
    }
    const safeHeight = Math.max(1, rowEstimatedHeight);
    const startRaw = Math.max(0, Math.floor(documentsViewport.scrollTop / safeHeight) - LIST_VIRTUAL_OVERSCAN);
    const start = Math.min(documents.length, startRaw);
    const end = Math.min(
      documents.length,
      Math.ceil((documentsViewport.scrollTop + viewportHeight) / safeHeight) + LIST_VIRTUAL_OVERSCAN,
    );
    return {
      start,
      end,
      topSpacer: start * safeHeight,
      bottomSpacer: Math.max(0, (documents.length - end) * safeHeight),
    };
  }, [listVirtualizationEnabled, rowEstimatedHeight, documentsViewport.scrollTop, viewportHeight, documents.length]);
  const renderedDocuments = listVirtualizationEnabled
    ? documents.slice(virtualWindow.start, virtualWindow.end)
    : documents;
  const selectedVisibleDocs = useMemo(
    () => documents.filter((doc) => {
      const id = docId(doc);
      return Boolean(id) && selectedDocIds.has(id);
    }),
    [documents, selectedDocIds],
  );
  const selectedVisibleRawIds = useMemo(
    () => selectedVisibleDocs.map((doc) => doc?._id).filter((id) => id !== undefined),
    [selectedVisibleDocs],
  );
  const selectedVisibleCount = selectedVisibleRawIds.length;
  const selectionActive = selectedVisibleCount > 0;
  const selectableVisibleDocs = useMemo(
    () => documents.filter((doc) => Boolean(docId(doc))),
    [documents],
  );
  const selectableVisibleCount = selectableVisibleDocs.length;
  const allVisibleSelected = selectableVisibleCount > 0 && selectedVisibleCount === selectableVisibleCount;
  const toggleSelectDoc = useCallback((doc) => {
    const id = docId(doc);
    if (!id) return;
    setSelectedDocIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, [docId]);
  const selectAllVisibleDocs = useCallback(() => {
    if (selectableVisibleCount === 0 || allVisibleSelected) return;
    setSelectedDocIds((prev) => {
      const next = new Set(prev);
      selectableVisibleDocs.forEach((doc) => {
        const id = docId(doc);
        if (id) next.add(id);
      });
      return next;
    });
  }, [allVisibleSelected, selectableVisibleDocs, selectableVisibleCount, docId]);
  const deselectAllVisibleDocs = useCallback(() => {
    if (selectableVisibleCount === 0 || selectedVisibleCount === 0) return;
    setSelectedDocIds((prev) => {
      const next = new Set(prev);
      selectableVisibleDocs.forEach((doc) => {
        const id = docId(doc);
        if (id) next.delete(id);
      });
      return next;
    });
  }, [selectableVisibleDocs, selectableVisibleCount, selectedVisibleCount, docId]);
  const toggleAllVisibleSelection = useCallback(() => {
    if (allVisibleSelected) {
      deselectAllVisibleDocs();
      return;
    }
    selectAllVisibleDocs();
  }, [allVisibleSelected, selectAllVisibleDocs, deselectAllVisibleDocs]);
  const handleRowSelectionToggle = useCallback((doc) => {
    toggleSelectDoc(doc);
  }, [toggleSelectDoc]);
  const executeDeleteSelected = useCallback(async () => {
    if (selectedVisibleRawIds.length === 0 || bulkDeleteBusy) return;
    setBulkDeleteBusy(true);
    setError(null);
    try {
      await api.operateCollection(
        db,
        collection,
        'deleteMany',
        { filter: { _id: { $in: selectedVisibleRawIds } } },
        { heavyConfirm: true },
      );
      setConfirmDelete(null);
      setSelectedDocIds(new Set());
      loadDocuments();
      loadStats();
    } catch (err) {
      setError(err?.message || 'Failed to delete selected documents.');
    } finally {
      setBulkDeleteBusy(false);
    }
  }, [selectedVisibleRawIds, bulkDeleteBusy, db, collection, loadDocuments, loadStats]);
  const handleDeleteSelected = useCallback(() => {
    if (selectedVisibleRawIds.length === 0 || bulkDeleteBusy) return;
    setConfirmDialog({
      title: 'Delete Selected Documents',
      message: `Delete ${formatNumber(selectedVisibleRawIds.length)} selected document(s) from ${db}.${collection}?\nThis action cannot be undone.`,
      confirmLabel: 'Delete',
      danger: true,
      onConfirm: executeDeleteSelected,
    });
  }, [selectedVisibleRawIds.length, bulkDeleteBusy, db, collection, executeDeleteSelected]);
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
    if (field === '_id') {
      setSortField('_id', activeSort.dir === -1 ? 1 : -1);
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
    const source = filterSuggestions.length > 0
      ? filterSuggestions
      : (filterSuggestionsLoading ? [] : FILTER_HINT_FALLBACK);
    if (!normalizedFilterInput || normalizedFilterInput === '{}') return source;
    const matching = source.filter((hint) => hint.toLowerCase().includes(normalizedFilterInput));
    return matching.length > 0 ? matching : source;
  }, [filterSuggestions, filterSuggestionsLoading, normalizedFilterInput]);
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
    setTableColumnRenderLimit(TABLE_COLUMN_INITIAL_RENDER);
  }, [db, collection, viewMode]);
  useEffect(() => {
    stopColumnResize();
    setTableColumnWidths({});
  }, [db, collection, viewMode, stopColumnResize]);
  useEffect(() => {
    if (!lazyTableColumnsEnabled) {
      setTableColumnRenderLimit(TABLE_COLUMN_INITIAL_RENDER);
      return;
    }
    setTableColumnRenderLimit((prev) => {
      const normalized = Math.max(TABLE_COLUMN_INITIAL_RENDER, Math.min(prev, tableColumns.length));
      return normalized;
    });
  }, [lazyTableColumnsEnabled, tableColumns.length]);
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
  useEffect(() => {
    if (!db || !collection) return;
    try { localStorage.setItem(filterStorageKey, filter || '{}'); } catch {}
  }, [filterStorageKey, db, collection, filter]);
  const budgetOverrideActive = queryBudget.timeoutMs !== defaultBudget.timeoutMs || queryBudget.limit !== defaultBudget.limit;
  const timeoutCustomEditing = !QUERY_TIMEOUT_OPTIONS.includes(budgetDraft.timeoutMs) || customTimeoutSeconds !== '';
  const limitCustomEditing = !pageSizeOptions.includes(budgetDraft.limit) || customLimitValue !== '';
  const exactTotalRunning = totalJob && (totalJob.state === 'queued' || totalJob.state === 'running');
  const exactTotalRisk = exactTotalTimeoutMs > POWER_QUERY_TIMEOUT_PRESET_MAX_MS ? 'critical' : exactTotalTimeoutMs > 30000 ? 'warning' : 'normal';
  const showApprox = Boolean(total?.state === 'ready' && total?.approx && !String(total?.source || '').toLowerCase().includes('exact'));
  const pageFrom = documents.length > 0 ? (page * limit) + 1 : 0;
  const pageTo = documents.length > 0 ? (page * limit) + documents.length : 0;
  const totalSummary = (() => {
    if (totalKnown) {
      if (showApprox) return `${formatNumber(pageFrom)}-${formatNumber(pageTo)} of ${formatNumber(totalValue)}`;
      return `${formatNumber(pageFrom)}-${formatNumber(pageTo)} of ${formatNumber(totalValue)}`;
    }
    if (documents.length > 0) return `${formatNumber(pageFrom)}-${formatNumber(pageTo)} of ?`;
    return `${formatNumber(documents.length)} docs`;
  })();
  const applyPageSizeLimit = (nextValue) => {
    const normalized = nextValue === 'unlimited'
      ? modeLimitMax
      : clampLimitValue(nextValue, executionMode);
    const nextBudget = { ...queryBudgetRef.current, limit: normalized };
    setQueryBudget(nextBudget);
    setLimit(normalized);
    setShowPageSizeMenu(false);
    setShowCustomPageSizeInput(false);
    setPage(0);
    loadDocuments(0, nextBudget);
  };

  const handleQuickPageSizeChange = (value) => {
    applyPageSizeLimit(value);
  };

  const applyCustomPageSize = () => {
    const parsed = Number(customPageSizeInput);
    if (!Number.isFinite(parsed)) return;
    applyPageSizeLimit(Math.round(parsed));
  };

  const openOperate = (tab = 'insert') => {
    setOperateInitialTab(tab);
    setShowOperateModal(true);
  };

  if (showIndexes) return <IndexesView db={db} collection={collection} onQueryMs={onQueryMs} refreshToken={refreshToken} />;
  if (editingDoc) return <DocumentEditor db={db} collection={collection} document={editingDoc} onSave={() => { setEditingDoc(null); loadDocuments(); loadStats(); }} onCancel={() => { setEditingDoc(null); }} />;

  return (
    <div className="h-full min-w-0 flex flex-col overflow-hidden">
      {/* Toolbar */}
      <div ref={collectionToolbarRef} className="flex-shrink-0 px-4 py-2" style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
        <div className="overflow-x-auto">
          <div className="flex items-center gap-2 min-w-0">
          <span
            className="hidden md:inline-flex items-center px-2 py-1 rounded-md text-2xs font-mono shrink-0"
            style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-secondary)' }}
            title={`${db}.${collection}`}
          >
            {db}.{collection}
          </span>
          <div className="hidden md:block w-px h-5 shrink-0" style={{background:'var(--border)'}} />
          <div className="relative flex-1 min-w-[160px]" ref={filterAutofillRef}>
	            <div className="flex items-center gap-2">
		              <button
		                type="button"
		                onClick={() => {
		                  loadFilterSuggestions();
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
		                onFocus={() => {
                  loadFilterSuggestions();
                  if (filterInput.trim().length > 0) {
                    setShowFilterAutofill(true);
                    setFilterAutofillLimit(8);
                  }
                }}
		                onChange={(event) => {
		                  const next = event.target.value;
		                  setFilterInput(next);
                  loadFilterSuggestions();
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
            <FloatingMenu
              open={showFilterAutofill && (visibleSuggestions.length > 0 || filterSuggestionsLoading)}
              anchorRef={filterAutofillRef}
              onClose={() => setShowFilterAutofill(false)}
              align="left"
              minWidth={260}
              maxWidth={520}
              zIndex={260}
              className="rounded-lg shadow-lg py-1 max-h-64 overflow-auto animate-fade-in"
              style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}
            >
              {filterSuggestionsLoading && visibleSuggestions.length === 0 && (
                <div className="px-3 py-2 text-2xs inline-flex items-center gap-1.5" style={{ color:'var(--text-tertiary)' }}>
                  <Loader className="w-3 h-3" />
                  Loading suggestions...
                </div>
              )}
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
            </FloatingMenu>
	          </div>
          <div className="flex items-center gap-1 shrink-0 whitespace-nowrap">
            <button
              type="button"
              onClick={() => {
                setFilterEditorDraft((filterInput || '{}').trim() || '{}');
                setShowFilterEditor(true);
              }}
              className="btn-ghost text-2xs font-medium whitespace-nowrap"
              style={{ color:'var(--text-secondary)' }}
              title="Open filter editor"
            >
              Editor
            </button>
            <button
              type="button"
              onClick={() => {
                setFilterInput('');
                setFilterEditorDraft('{}');
                handleApplyFilter('{}');
              }}
              disabled={loading}
              className="btn-ghost text-2xs font-medium whitespace-nowrap"
              style={{ color:'var(--text-tertiary)' }}
              title="Reset filter"
            >
              Reset
            </button>
            <button
              onClick={() => handleApplyFilter()}
              disabled={loading}
              className="btn-ghost text-2xs font-medium inline-flex items-center gap-1 whitespace-nowrap"
              style={{color:'var(--accent)'}}
            >
              {loading && <Loader className="w-3 h-3" />}
              {loading ? `Applying... ${(loadingLiveTimer / 1000).toFixed(1)}s` : 'Apply'}
            </button>
          </div>
          {loading && (
            <div className="hidden lg:inline-flex items-center gap-1 text-2xs px-2 py-1 rounded-md shrink-0 whitespace-nowrap" style={{ background:'var(--surface-2)', color:'var(--text-tertiary)' }}>
              <Loader className="w-3 h-3" />
              Running query... {(loadingLiveTimer / 1000).toFixed(1)}s
            </div>
          )}
          <div className="relative shrink-0" ref={budgetMenuRef}>
            <button
              type="button"
              className={`btn-ghost text-2xs font-medium whitespace-nowrap ${budgetOverrideActive ? 'text-amber-300' : ''}`}
              onClick={() => setShowBudgetMenu((prev) => !prev)}
              title="Per-query budget"
            >
              {formatTimeoutOptionLabel(queryBudget.timeoutMs)}/{formatLimitOptionLabel(queryBudget.limit)}
            </button>
            {showBudgetMenu && (
              <FloatingMenu
                open={showBudgetMenu}
                anchorRef={budgetMenuRef}
                onClose={() => setShowBudgetMenu(false)}
                align="left"
                width={288}
                zIndex={260}
                className="rounded-lg shadow-lg p-3 animate-fade-in"
                style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}
              >
                <div className="text-2xs uppercase tracking-wider mb-2" style={{ color:'var(--text-tertiary)' }}>Per-query override</div>
                <div className="mb-2">
                  <div className="text-2xs mb-1" style={{ color:'var(--text-tertiary)' }}>Timeout</div>
                  <div className="flex flex-wrap gap-1">
                    {QUERY_TIMEOUT_OPTIONS.map((value) => {
                      const disabled = executionMode !== 'power' && value > SAFE_QUERY_TIMEOUT_MAX_MS;
                      return (
                        <button
                          key={value}
                          type="button"
                          disabled={disabled}
                          className="px-2 py-1 rounded text-2xs font-mono"
                          style={{
                            background: (!timeoutCustomEditing && budgetDraft.timeoutMs === value) ? 'var(--surface-4)' : 'var(--surface-2)',
                            color: disabled ? 'var(--text-tertiary)' : ((!timeoutCustomEditing && budgetDraft.timeoutMs === value) ? 'var(--text-primary)' : 'var(--text-secondary)'),
                            border: '1px solid var(--border)',
                            opacity: disabled ? 0.45 : 1,
                            cursor: disabled ? 'not-allowed' : 'pointer',
                          }}
                          onClick={() => {
                            if (disabled) return;
                            handleBudgetDraftChange({ timeoutMs: value });
                            setCustomTimeoutSeconds('');
                          }}
                        >
                          {formatTimeoutOptionLabel(value)}
                        </button>
                      );
                    })}
                    <button
                      type="button"
                      className="px-2 py-1 rounded text-2xs font-mono"
                      style={{
                        background: timeoutCustomEditing ? 'var(--surface-4)' : 'var(--surface-2)',
                        color: timeoutCustomEditing ? 'var(--text-primary)' : 'var(--text-secondary)',
                        border: '1px solid var(--border)',
                      }}
                      onClick={() => setCustomTimeoutSeconds(String(Math.max(5, Math.round((budgetDraft.timeoutMs || DEFAULT_QUERY_TIMEOUT_MS) / 1000))))}
                    >
                      Custom
                    </button>
                  </div>
                  {timeoutCustomEditing && (
                    <div className="mt-2 flex items-center gap-1.5">
                      <input
                        type="number"
                        min={5}
                        max={Math.round(getModeTimeoutMax(executionMode) / 1000)}
                        step={1}
                        value={customTimeoutSeconds !== '' ? customTimeoutSeconds : String(Math.max(5, Math.round((budgetDraft.timeoutMs || DEFAULT_QUERY_TIMEOUT_MS) / 1000)))}
                        onChange={(event) => setCustomTimeoutSeconds(event.target.value)}
                        className="ms-number w-24 px-2 py-1 rounded-md text-2xs font-mono"
                        style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                        placeholder="seconds"
                      />
                      <button
                        type="button"
                        className="btn-ghost text-2xs px-2 py-1"
                        onClick={() => {
                          const seconds = Number(customTimeoutSeconds !== '' ? customTimeoutSeconds : Math.max(5, Math.round((budgetDraft.timeoutMs || DEFAULT_QUERY_TIMEOUT_MS) / 1000)));
                          if (!Number.isFinite(seconds)) return;
                          const nextTimeoutMs = Math.round(seconds * 1000);
                          handleBudgetDraftChange({ timeoutMs: nextTimeoutMs });
                          setCustomTimeoutSeconds(String(Math.max(5, Math.round(Math.min(clampTimeoutMs(nextTimeoutMs), getModeTimeoutMax(executionMode)) / 1000))));
                        }}
                      >
                        Set
                      </button>
                      <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                        {executionMode === 'safe' ? 'Custom timeout up to 30s in Safe Mode.' : 'Custom timeout can exceed 120s in Power Mode.'}
                      </span>
                    </div>
                  )}
                </div>
                <div className="mb-2">
                  <div className="text-2xs mb-1" style={{ color:'var(--text-tertiary)' }}>Documents</div>
                  <div className="flex flex-wrap gap-1">
                    {pageSizeOptions.map((value) => {
                      const disabled = executionMode !== 'power' && value > SAFE_QUERY_LIMIT_MAX;
                      return (
                        <button
                          key={value}
                          type="button"
                          disabled={disabled}
                          className="px-2 py-1 rounded text-2xs font-mono"
                          style={{
                            background: (!limitCustomEditing && budgetDraft.limit === value) ? 'var(--surface-4)' : 'var(--surface-2)',
                            color: disabled ? 'var(--text-tertiary)' : ((!limitCustomEditing && budgetDraft.limit === value) ? 'var(--text-primary)' : 'var(--text-secondary)'),
                            border: '1px solid var(--border)',
                            opacity: disabled ? 0.45 : 1,
                            cursor: disabled ? 'not-allowed' : 'pointer',
                          }}
                          onClick={() => {
                            if (disabled) return;
                            handleBudgetDraftChange({ limit: value });
                            setCustomLimitValue('');
                          }}
                        >
                          {formatLimitOptionLabel(value)}
                        </button>
                      );
                    })}
                    <button
                      type="button"
                      className="px-2 py-1 rounded text-2xs font-mono"
                      style={{
                        background: limitCustomEditing ? 'var(--surface-4)' : 'var(--surface-2)',
                        color: limitCustomEditing ? 'var(--text-primary)' : 'var(--text-secondary)',
                        border: '1px solid var(--border)',
                      }}
                      onClick={() => setCustomLimitValue(String(Math.max(50, Math.round(budgetDraft.limit || DEFAULT_QUERY_LIMIT))))}
                    >
                      Custom
                    </button>
                  </div>
                  {limitCustomEditing && (
                    <div className="mt-2 flex items-center gap-1.5">
                      <input
                        type="number"
                        min={50}
                        max={QUERY_LIMIT_MAX}
                        step={1}
                        value={customLimitValue !== '' ? customLimitValue : String(Math.max(50, Math.round(budgetDraft.limit || DEFAULT_QUERY_LIMIT)))}
                        onChange={(event) => setCustomLimitValue(event.target.value)}
                        className="ms-number w-24 px-2 py-1 rounded-md text-2xs font-mono"
                        style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                        placeholder="documents"
                      />
                      <button
                        type="button"
                        className="btn-ghost text-2xs px-2 py-1"
                        onClick={() => {
                          const value = Number(customLimitValue !== '' ? customLimitValue : Math.max(50, Math.round(budgetDraft.limit || DEFAULT_QUERY_LIMIT)));
                          if (!Number.isFinite(value)) return;
                          const nextLimit = Math.round(value);
                          handleBudgetDraftChange({ limit: nextLimit });
                          setCustomLimitValue(String(Math.max(50, Math.round(clampLimitValue(nextLimit, executionMode)))));
                        }}
                      >
                        Set
                      </button>
                      <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                        Custom can be over Safe preset limits. Be careful.
                      </span>
                    </div>
                  )}
                </div>
                <div className="mb-2 flex items-center justify-end gap-2">
                  <button
                    type="button"
                    className="btn-primary text-2xs px-2.5 py-1"
                    onClick={applyBudgetDraft}
                  >
                    Apply
                  </button>
                  <button
                    type="button"
                    className="btn-ghost text-2xs"
                    onClick={() => {
                      setQueryBudget(defaultBudget);
                      setBudgetDraft(defaultBudget);
                      setCustomTimeoutSeconds('');
                      setCustomLimitValue('');
                      persistOverrideRef.current = false;
                      setPersistOverride(false);
                    }}
                  >
                    Reset to default
                  </button>
                </div>
                <label className="flex items-center gap-2 text-2xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
                  <input
                    type="checkbox"
                    className="ms-checkbox"
                    checked={persistOverride}
                    onChange={(event) => {
                      const checked = event.target.checked;
                      persistOverrideRef.current = checked;
                      setPersistOverride(checked);
                    }}
                  />
                  Keep override in Documents tab
                </label>
                <div className="mt-1 text-2xs" style={{ color:'var(--text-tertiary)' }}>
                  Keeps timeout/page size while you browse collections in this tab. If disabled, override is one-shot.
                </div>
                {queryBudget.timeoutMs > 30000 && (
                  <div className={`mt-2 text-2xs ${queryBudget.timeoutMs > POWER_QUERY_TIMEOUT_PRESET_MAX_MS ? 'text-red-400' : 'text-amber-300'}`}>
                    {queryBudget.timeoutMs > POWER_QUERY_TIMEOUT_PRESET_MAX_MS
                      ? 'Timeout above 120s is high-risk. Confirm before use.'
                      : 'Timeout above 30s may load heavy server resources.'}
                  </div>
                )}
              </FloatingMenu>
            )}
          </div>
	          <div className="w-px h-5 shrink-0" style={{background:'var(--border)'}} />

          {/* View mode toggle */}
          <div className="flex items-center rounded-lg p-0.5 shrink-0" style={{background:'var(--surface-2)'}}>
            <button onClick={()=>setViewMode('json')} className="p-1.5 rounded-md transition-all" style={{background:viewMode==='json'?'var(--surface-4)':'transparent'}} title="JSON View">
              <FileJson className="w-3.5 h-3.5" style={{color:viewMode==='json'?'var(--text-primary)':'var(--text-tertiary)'}} />
            </button>
            <button onClick={()=>setViewMode('table')} className="p-1.5 rounded-md transition-all" style={{background:viewMode==='table'?'var(--surface-4)':'transparent'}} title="Table View">
              <Table className="w-3.5 h-3.5" style={{color:viewMode==='table'?'var(--text-primary)':'var(--text-tertiary)'}} />
            </button>
          </div>
          <div className="w-px h-5 shrink-0" style={{background:'var(--border)'}} />

          <div className="relative shrink-0" ref={sortMenuRef}>
            <button
              onClick={() => {
                setShowSortMenu((prev) => !prev);
                setShowColumnsMenu(false);
                setShowInlineFieldsMenu(false);
              }}
              className="btn-ghost flex items-center gap-1 whitespace-nowrap"
              style={{ color:'var(--text-secondary)' }}
              title="Sort documents"
            >
              {activeSort.dir === -1 ? <ArrowDown className="w-3.5 h-3.5" /> : <ArrowUp className="w-3.5 h-3.5" />}
              <span className="hidden md:inline text-xs max-w-[120px] truncate">{activeSort.field}</span>
            </button>
            {showSortMenu && (
              <FloatingMenu
                open={showSortMenu}
                anchorRef={sortMenuRef}
                onClose={() => setShowSortMenu(false)}
                align="right"
                width={256}
                zIndex={260}
                className="rounded-lg shadow-lg py-2 animate-fade-in"
                style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}
              >
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
                <div className="px-3 pt-2 mt-1 flex items-center justify-between text-2xs gap-2" style={{ borderTop:'1px solid var(--border)' }}>
                  <span style={{ color:'var(--text-tertiary)' }}>Global _id sort</span>
                  <button
                    onClick={() => setSortField('_id', activeSort.field === '_id' && activeSort.dir === -1 ? 1 : -1)}
                    className="btn-ghost py-1 px-2"
                    style={{ color:'var(--accent)' }}
                  >
                    {activeSort.field === '_id' && activeSort.dir === -1 ? 'Oldest first' : 'Newest first'}
                  </button>
                </div>
              </FloatingMenu>
            )}
          </div>

          <div className="relative shrink-0" ref={columnsMenuRef}>
            <button
              onClick={() => {
                setShowColumnsMenu((prev) => !prev);
                setShowSortMenu(false);
                setShowInlineFieldsMenu(false);
              }}
              className="btn-ghost flex items-center gap-1 whitespace-nowrap"
              style={{ color:'var(--text-secondary)' }}
              title={viewMode === 'table' ? 'Visible columns' : 'Preview fields'}
            >
              <Columns className="w-3.5 h-3.5" />
              <span className="hidden sm:inline text-xs">{viewMode === 'table' ? 'Columns' : 'Fields'}</span>
              {viewMode === 'table' && hiddenColumnsCount > 0 && <span className="badge-blue">{hiddenColumnsCount}</span>}
              {viewMode === 'json' && hiddenJsonFieldsCount > 0 && <span className="badge-blue">{hiddenJsonFieldsCount}</span>}
            </button>
            {showColumnsMenu && (
              <FloatingMenu
                open={showColumnsMenu}
                anchorRef={columnsMenuRef}
                onClose={() => setShowColumnsMenu(false)}
                align="right"
                width={224}
                zIndex={260}
                className="rounded-lg shadow-lg py-2 animate-fade-in"
                style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}
              >
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
              </FloatingMenu>
            )}
          </div>

          <div className="relative shrink-0" ref={inlineFieldsMenuRef}>
            <button
              onClick={() => {
                setShowInlineFieldsMenu((prev) => !prev);
                setShowSortMenu(false);
                setShowColumnsMenu(false);
              }}
              className="btn-ghost flex items-center gap-1 whitespace-nowrap"
              style={{ color:'var(--text-secondary)' }}
              title="Show full inline values"
            >
              <Eye className="w-3.5 h-3.5" />
              <span className="hidden sm:inline text-xs">Inline</span>
              {expandedObjectFields.length > 0 && <span className="badge-blue">{expandedObjectFields.length}</span>}
            </button>
            {showInlineFieldsMenu && (
              <FloatingMenu
                open={showInlineFieldsMenu}
                anchorRef={inlineFieldsMenuRef}
                onClose={() => setShowInlineFieldsMenu(false)}
                align="right"
                width={256}
                zIndex={260}
                className="rounded-lg shadow-lg py-2 animate-fade-in"
                style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}
              >
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
              </FloatingMenu>
            )}
          </div>
          <div className="w-px h-5 shrink-0" style={{background:'var(--border)'}} />

          {/* Export */}
          <div className="relative shrink-0" ref={exportMenuRef}>
            <button
              onClick={()=>setExportFormat(v=>!v)}
              className="btn-ghost p-1.5 flex items-center justify-center"
              style={{color:'var(--accent)'}}
              title="Export"
            >
              <Download className="w-3.5 h-3.5" />
            </button>
            {exportFormat && (
              <FloatingMenu
                open={exportFormat}
                anchorRef={exportMenuRef}
                onClose={() => setExportFormat(false)}
                align="right"
                minWidth={120}
                zIndex={260}
                className="rounded-lg shadow-lg py-1 animate-fade-in"
                style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}
              >
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
              </FloatingMenu>
            )}
          </div>

          <button onClick={() => openOperate('insert')} className="btn-ghost flex items-center gap-1 shrink-0 whitespace-nowrap" style={{color:'var(--accent)'}}>
            <Zap className="w-3.5 h-3.5" /><span className="hidden sm:inline text-xs">Operate</span>
          </button>
          {selectionActive && (
            <>
              <div className="w-px h-5 shrink-0" style={{background:'var(--border)'}} />
              <button
                type="button"
                onClick={toggleAllVisibleSelection}
                className="btn-ghost flex items-center gap-1 shrink-0 whitespace-nowrap"
                style={allVisibleSelected ? { color: 'var(--accent)' } : { color: 'var(--text-secondary)' }}
                title={allVisibleSelected ? 'Unselect all documents on this page' : 'Select all documents on this page'}
              >
                <span className="hidden sm:inline text-xs">All</span>
              </button>
              <button
                type="button"
                onClick={handleDeleteSelected}
                disabled={bulkDeleteBusy}
                className="btn-ghost flex items-center gap-1 shrink-0 whitespace-nowrap"
                style={{ color: '#f87171' }}
                title={`Delete (${selectedVisibleCount})`}
              >
                <Trash className="w-3.5 h-3.5" />
                <span className="hidden sm:inline text-xs">
                  {bulkDeleteBusy ? 'Deleting...' : `Delete (${formatNumber(selectedVisibleCount)})`}
                </span>
              </button>
            </>
          )}
          <div className="w-px h-5 shrink-0" style={{background:'var(--border)'}} />
          <button onClick={()=>loadDocuments()} className="btn-ghost p-1.5 shrink-0" title="Refresh">
            <Refresh className={`w-3.5 h-3.5 ${loading?'animate-spin':''}`} />
          </button>
        </div>
        </div>
      </div>

      {/* Slow query warning */}
      {slow && (
        <div className="px-4 mt-2">
          <InlineAlert kind="warning" message="Slow query detected - consider adding an index or narrowing your filter" onClose={() => setSlow(false)} icon={Zap} className="mx-0 mt-0" />
        </div>
      )}

      {exactTotalBanner && (
        <div
          className="mx-4 mt-2 flex items-center gap-2 text-2xs p-2 rounded-lg animate-fade-in"
          style={exactTotalBanner.kind === 'success'
            ? { background:'rgba(16,185,129,0.08)', border:'1px solid rgba(16,185,129,0.2)', color:'#34d399' }
            : exactTotalBanner.kind === 'error'
              ? { background:'rgba(239,68,68,0.08)', border:'1px solid rgba(239,68,68,0.25)', color:'#f87171' }
              : exactTotalBanner.kind === 'warning'
                ? { background:'rgba(245,158,11,0.08)', border:'1px solid rgba(245,158,11,0.2)', color:'#fbbf24' }
                : { background:'var(--surface-1)', border:'1px solid var(--border)', color:'var(--text-secondary)' }}
        >
          <Zap className="w-3.5 h-3.5" />
          <span className="flex-1">{exactTotalBanner.message}</span>
          <button onClick={clearExactTotalBanner} className="ml-auto"><X className="w-3 h-3" /></button>
        </div>
      )}

      {error && (
        <div className="px-4 mt-2">
          <InlineAlert kind="error" message={error} onClose={() => setError(null)} className="mx-0 mt-0" />
        </div>
      )}

      {showFilterEditor && (
        <div className="fixed inset-0 z-[230] flex items-center justify-center p-4">
          <button
            type="button"
            className="absolute inset-0 bg-black/50"
            onClick={() => setShowFilterEditor(false)}
            aria-label="Close filter editor"
          />
          <div
            className="relative w-full max-w-3xl rounded-2xl p-4 overflow-auto"
            style={{ background:'var(--surface-1)', border:'1px solid var(--border)', maxHeight:'88vh' }}
          >
            <div className="flex items-center justify-between mb-3">
              <div className="text-xs font-semibold uppercase tracking-wider" style={{ color:'var(--text-tertiary)' }}>
                Filter Editor
              </div>
              <button type="button" onClick={() => setShowFilterEditor(false)} className="btn-ghost p-1.5">
                <X className="w-3.5 h-3.5" />
              </button>
            </div>
            <textarea
              value={filterEditorDraft}
              onChange={(event) => setFilterEditorDraft(event.target.value)}
              spellCheck={false}
              className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
              style={{
                background:'var(--surface-2)',
                border:'1px solid var(--border)',
                color:'var(--text-primary)',
                minHeight:'220px',
                maxHeight:'55vh',
                resize:'vertical',
              }}
            />
            <div className="mt-3 flex items-center justify-end gap-2">
              <button type="button" className="btn-ghost text-xs" onClick={() => setShowFilterEditor(false)}>
                Cancel
              </button>
              <button
                type="button"
                className="btn-primary text-xs px-3 py-1.5"
                onClick={() => {
                  if (handleApplyFilter(filterEditorDraft)) {
                    setShowFilterEditor(false);
                  }
                }}
              >
                Apply Filter
              </button>
            </div>
          </div>
        </div>
      )}

      <CollectionExportDialog
        open={showExportDialog}
        busy={exporting}
        progress={exportProgress}
        title={`Export ${pendingExportFormat.toUpperCase()}`}
        subtitle={`${db}.${collection} | ${getExportLimitChoiceLabel(exportLimit)} docs`}
        docsValue={exportLimit}
        docsOptions={exportLimitSelectOptions}
        onDocsChange={setExportLimit}
        format={pendingExportFormat}
        onFormatChange={(next) => setPendingExportFormat(next === 'csv' ? 'csv' : 'json')}
        useVisibleFields={exportUseVisibleFields}
        onUseVisibleFieldsChange={setExportUseVisibleFields}
        visibleFieldsLabel={`Export only visible fields (${exportVisibleFields.length})`}
        useSort={exportUseCurrentSort}
        onUseSortChange={setExportUseCurrentSort}
        sortLabel={`Apply current sort (${activeSort.field}: ${activeSort.dir === -1 ? 'desc' : 'asc'})`}
        showFilterToggle
        useFilter={exportUseCurrentFilter}
        onUseFilterChange={setExportUseCurrentFilter}
        filterLabel="Apply current filter"
        showModifiersInfo={exportNoModifiers}
        modifiersInfoText="Export uses full documents with no current filter/sort overrides."
        submitLabel={`Export ${pendingExportFormat.toUpperCase()}`}
        onCancel={cancelCollectionExport}
        onSubmit={() => handleExport(pendingExportFormat, {
          useVisibleFields: exportUseVisibleFields,
          includeSort: exportUseCurrentSort,
          useCurrentFilter: exportUseCurrentFilter,
          limit: exportLimit,
        })}
      />

      <DatabaseExportDialog
        open={showDbExportDialog}
        title="Export Database"
        subtitle={db}
        busy={exporting}
        progress={exportProgress}
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
        onCancel={cancelDatabaseExport}
        onSubmit={handleExportDatabase}
      />
      <OperateModal
        open={showOperateModal}
        db={db}
        collection={collection}
        isProduction={isProduction}
        executionMode={executionMode}
        budget={queryBudget}
        initialTab={operateInitialTab}
        onClose={() => setShowOperateModal(false)}
        onApplied={() => {
          loadDocuments(page);
          loadStats();
        }}
      />
      <ConfirmDialog
        open={Boolean(confirmDialog)}
        title={confirmDialog?.title || 'Confirm action'}
        message={confirmDialog?.message || 'Continue?'}
        confirmLabel={confirmDialog?.confirmLabel || 'Continue'}
        danger={Boolean(confirmDialog?.danger)}
        busy={Boolean(confirmDialog?.busy)}
        onCancel={() => setConfirmDialog(null)}
        onConfirm={() => {
          const action = confirmDialog?.onConfirm;
          setConfirmDialog(null);
          action?.();
        }}
      />

      {/* Documents */}
      <div
        ref={documentsViewportRef}
        className="flex-1 overflow-auto"
        onScroll={handleDocumentsViewportScroll}
      >
        {(loading || !docsReady) && documents.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="flex flex-col items-center gap-3"><Loader style={{color:'var(--accent)'}} className="w-5 h-5" /><span className="text-xs" style={{color:'var(--text-tertiary)'}}>Loading...</span></div>
          </div>
        ) : documents.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center" style={{color:'var(--text-tertiary)'}}>
              <div className="text-sm mb-2">No documents found</div>
              <button onClick={() => openOperate('insert')} className="btn-primary text-xs">Operate</button>
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
              {hiddenRenderedTableColumns > 0 && (
                <div
                  className="mx-3 mt-2 mb-1 rounded-lg px-3 py-2 text-2xs flex flex-wrap items-center gap-2"
                  style={{ background:'var(--surface-1)', border:'1px solid var(--border)', color:'var(--text-secondary)' }}
                >
                  <span>
                    Rendering {formatNumber(renderedTableColumns.length)} / {formatNumber(tableColumns.length)} columns for performance.
                  </span>
                  <button
                    type="button"
                    className="btn-ghost py-1 px-2 text-2xs"
                    style={{ color:'var(--accent)' }}
                    onClick={() => setTableColumnRenderLimit((prev) => Math.min(tableColumns.length, prev + TABLE_COLUMN_RENDER_STEP))}
                  >
                    Load +{TABLE_COLUMN_RENDER_STEP}
                  </button>
                  <button
                    type="button"
                    className="btn-ghost py-1 px-2 text-2xs"
                    onClick={() => setTableColumnRenderLimit(tableColumns.length)}
                  >
                    Show all
                  </button>
                  <button
                    type="button"
                    className="btn-ghost py-1 px-2 text-2xs"
                    onClick={() => setTableColumnRenderLimit(TABLE_COLUMN_INITIAL_RENDER)}
                  >
                    Reset
                  </button>
                </div>
              )}
              <table
                className="w-full text-xs"
                style={{
                  tableLayout: 'fixed',
                  minWidth: `${renderedTableMinWidth}px`,
                }}
              >
	                <thead>
	                  <tr style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
	                    {renderedTableColumns.map((k) => {
                        const columnWidth = getResolvedTableColumnWidth(k);
                        return (
                          <th
                            key={k}
                            className="relative px-3 py-2 pr-5 text-left font-medium text-2xs uppercase tracking-wider whitespace-nowrap"
                            style={{
                              color: 'var(--text-tertiary)',
                              width: `${columnWidth}px`,
                              minWidth: `${columnWidth}px`,
                              maxWidth: `${columnWidth}px`,
                            }}
                          >
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
                            <button
                              type="button"
                              className="absolute right-0 top-0 h-full w-3 cursor-col-resize"
                              style={{
                                background: resizingColumnKey === k ? 'rgba(16,185,129,0.22)' : 'transparent',
                                borderLeft: resizingColumnKey === k ? '1px solid rgba(16,185,129,0.55)' : '1px solid transparent',
                              }}
                              title="Drag to resize. Double-click to reset width."
                              aria-label={`Resize ${k} column`}
                              onMouseDown={(event) => startTableColumnResize(event, k)}
                              onDoubleClick={(event) => resetTableColumnWidth(event, k)}
                            />
                          </th>
                        );
                      })}
	                    <th
                          className="px-3 py-2"
                          style={{
                            width: `${TABLE_COLUMN_ACTIONS_WIDTH}px`,
                            minWidth: `${TABLE_COLUMN_ACTIONS_WIDTH}px`,
                            maxWidth: `${TABLE_COLUMN_ACTIONS_WIDTH}px`,
                          }}
                        />
	                  </tr>
	                </thead>
	                <tbody>
                    {listVirtualizationEnabled && virtualWindow.topSpacer > 0 && (
                      <tr style={{ height: `${virtualWindow.topSpacer}px` }}>
                        <td colSpan={renderedTableColumns.length + 1} />
                      </tr>
                    )}
	                  {renderedDocuments.map((doc, localIdx) => {
	                    const idx = listVirtualizationEnabled ? (virtualWindow.start + localIdx) : localIdx;
	                    const key = rowKey(doc, idx);
                      const currentId = docId(doc);
                      const isSelected = Boolean(currentId) && selectedDocIds.has(currentId);
                    return (
                      <tr
                        key={key}
                        className="group transition-colors"
                        style={{borderBottom:'1px solid var(--border)', background:isSelected ? 'rgba(34,197,94,0.08)' : 'transparent'}}
                        onMouseOver={e=>e.currentTarget.style.background='var(--surface-1)'}
                        onMouseOut={e=>e.currentTarget.style.background=(isSelected ? 'rgba(34,197,94,0.08)' : 'transparent')}
                      >
                        {renderedTableColumns.map(k => {
                          const v = doc[k];
                          const inlineExpanded = expandedObjectFields.includes(k);
                          const columnWidth = getResolvedTableColumnWidth(k);
                          const renderEllipsis = (
                            text,
                            color = 'var(--text-secondary)',
                            extraClass = '',
                            options = {},
                          ) => {
                            const value = String(text);
                            const enableFade = options?.fade !== false;
                            const fadeActive = enableFade && value.length >= 14;
                            return (
                              <span
                                className={`block min-w-0 max-w-full truncate ${extraClass}`.trim()}
                                style={{
                                  color,
                                  ...(fadeActive ? {
                                    WebkitMaskImage: 'linear-gradient(90deg,#000 78%,rgba(0,0,0,0.55) 92%,transparent)',
                                    maskImage: 'linear-gradient(90deg,#000 78%,rgba(0,0,0,0.55) 92%,transparent)',
                                  } : {}),
                                }}
                                title={value}
                              >
                                {value}
                              </span>
                            );
                          };
                          let display = '';
                          if (v === null) {
                            display = <span style={{color:'var(--json-null)'}}>null</span>;
                          } else if (v === undefined) {
                            display = '';
                          } else if (typeof v === 'object') {
                            if (inlineExpanded) {
                              display = <span className="font-mono inline-block whitespace-normal break-all" style={{color:'var(--text-secondary)'}}>{formatInlineValue(v)}</span>;
                            } else {
                              display = <span className="font-mono" style={{color:'var(--json-bracket)'}}>{Array.isArray(v) ? `[${v.length}]` : '{...}'}</span>;
                            }
                          } else if (typeof v === 'boolean') {
                            display = inlineExpanded
                              ? <span style={{color:'var(--json-boolean)'}}>{String(v)}</span>
                              : renderEllipsis(String(v), 'var(--json-boolean)', '', { fade: false });
                          } else if (typeof v === 'number') {
                            display = inlineExpanded
                              ? <span style={{color:'var(--json-number)'}}>{String(v)}</span>
                              : renderEllipsis(String(v), 'var(--json-number)', '', { fade: false });
                          } else if (k === '_id') {
                            const idValue = fullId(v);
                            display = inlineExpanded
                              ? <span className="font-mono inline-block whitespace-normal break-all" style={{color:'var(--json-objectid)'}}>{idValue}</span>
                              : renderEllipsis(idValue, 'var(--json-objectid)', 'font-mono');
                          } else if (inlineExpanded) {
                            display = <span className="font-mono inline-block whitespace-normal break-all" style={{color:'var(--text-secondary)'}}>{String(v)}</span>;
                          } else {
                            display = renderEllipsis(String(v), 'var(--text-secondary)');
                          }
                          return (
                            <td
                              key={k}
                              className={`px-3 py-2 font-mono ${inlineExpanded ? 'whitespace-normal align-top' : 'whitespace-nowrap overflow-hidden'}`}
                              style={{
                                width: `${columnWidth}px`,
                                minWidth: `${columnWidth}px`,
                                maxWidth: `${columnWidth}px`,
                              }}
                            >
                              {display}
                            </td>
                          );
                        })}
                        <td
                          className="px-3 py-2"
                          style={{
                            width: `${TABLE_COLUMN_ACTIONS_WIDTH}px`,
                            minWidth: `${TABLE_COLUMN_ACTIONS_WIDTH}px`,
                            maxWidth: `${TABLE_COLUMN_ACTIONS_WIDTH}px`,
                          }}
                        >
                          <div className={`flex items-center gap-0.5 transition-opacity ${isSelected ? 'opacity-100' : 'opacity-0 group-hover:opacity-100'}`}>
                            <button
                              onClick={() => handleRowSelectionToggle(doc)}
                              className="p-1 rounded-md transition-all focus:outline-none focus-visible:ring-1 focus-visible:ring-[var(--accent)]"
                              style={isSelected ? { color:'var(--accent)', background:'rgba(16,185,129,0.14)' } : { color:'var(--text-tertiary)' }}
                              title={isSelected ? 'Unselect' : 'Select'}
                            >
                              <Check className="w-3 h-3" />
                            </button>
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
                    {listVirtualizationEnabled && virtualWindow.bottomSpacer > 0 && (
                      <tr style={{ height: `${virtualWindow.bottomSpacer}px` }}>
                        <td colSpan={renderedTableColumns.length + 1} />
                      </tr>
                    )}
                </tbody>
              </table>
            </div>
          )
        ) : (
          /* JSON VIEW */
          <div>
            {listVirtualizationEnabled && virtualWindow.topSpacer > 0 && (
              <div style={{ height: `${virtualWindow.topSpacer}px` }} />
            )}
            {renderedDocuments.map((doc, localIdx) => {
              const idx = listVirtualizationEnabled ? (virtualWindow.start + localIdx) : localIdx;
              const key = rowKey(doc, idx);
              const currentId = docId(doc);
              const isSelected = Boolean(currentId) && selectedDocIds.has(currentId);
              const isExpanded = expandedRows.has(key);
              const keys = Object.keys(doc);
              const preferredPreview = jsonPreviewVisibleKeys.filter((field) => keys.includes(field));
              const preview = preferredPreview.length > 0 ? preferredPreview : keys.slice(0, Math.min(4, keys.length));
              const approxSize = (() => {
                try { return new Blob([JSON.stringify(doc)]).size; } catch { return 0; }
              })();
              const jsonRowBackground = isSelected
                ? 'rgba(34,197,94,0.08)'
                : (isExpanded ? 'var(--surface-1)' : 'transparent');
              return (
                <div key={key} className="group transition-colors duration-75 hover:bg-[var(--surface-1)]" style={{borderBottom:'1px solid var(--border)',background:jsonRowBackground}}>
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
                    <div className={`flex items-center gap-0.5 transition-opacity flex-shrink-0 ${isSelected ? 'opacity-100' : 'opacity-0 group-hover:opacity-100'}`}>
                      <span className="text-2xs mr-1 font-mono" style={{color:'var(--text-tertiary)'}}>
                        {formatBytes(approxSize)}
                      </span>
                      <button
                        onClick={e=>{e.stopPropagation();handleRowSelectionToggle(doc);}}
                        className="p-1.5 rounded-md transition-all focus:outline-none focus-visible:ring-1 focus-visible:ring-[var(--accent)]"
                        style={isSelected ? { color:'var(--accent)', background:'rgba(16,185,129,0.14)' } : { color:'var(--text-tertiary)' }}
                        title={isSelected ? 'Unselect' : 'Select'}
                      >
                        <Check className="w-3 h-3" />
                      </button>
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
            {listVirtualizationEnabled && virtualWindow.bottomSpacer > 0 && (
              <div style={{ height: `${virtualWindow.bottomSpacer}px` }} />
            )}
          </div>
        )}
      </div>

      {/* Pagination */}
      {(documents.length > 0 || page > 0) && (
        <div className="flex-shrink-0 px-4 py-2 flex items-center justify-between" style={{borderTop:'1px solid var(--border)',background:'var(--surface-1)'}}>
          <div className="flex items-center gap-3">
            <span className="text-2xs" style={{color:'var(--text-tertiary)'}}>
              {totalSummary}
            </span>
            {stats && (
              <div className="hidden md:flex items-center gap-2 text-2xs" style={{ color:'var(--text-tertiary)' }}>
                <span>{formatBytes(stats.size || 0)}</span>
                <span style={{ color:'var(--border)' }}>|</span>
                <span>avg {formatBytes(Math.round(stats.avgObjSize || 0))}</span>
              </div>
            )}
          </div>
          <div className="flex items-center gap-2">
            <div className="hidden md:flex items-center gap-1">
              <span className="text-2xs" style={{ color:'var(--text-tertiary)' }}>Page size</span>
              <div className="relative" ref={pageSizeMenuRef}>
                <button
                  type="button"
                  className="flex items-center gap-1 px-2 py-1 rounded-md text-2xs transition-colors"
                  style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-primary)' }}
                  onClick={() => setShowPageSizeMenu((prev) => !prev)}
                  title="Documents per page"
                >
                  <span>{limit >= QUERY_LIMIT_MAX ? 'Unlimited' : formatNumber(limit)}</span>
                  <ChevronDown className="w-3 h-3" style={{ color:'var(--text-tertiary)' }} />
                </button>
                {showPageSizeMenu && (
                  <FloatingMenu
                    open={showPageSizeMenu}
                    anchorRef={pageSizeMenuRef}
                    onClose={() => setShowPageSizeMenu(false)}
                    align="right"
                    placement="top"
                    width={160}
                    zIndex={260}
                    className="rounded-lg p-1 animate-fade-in"
                    style={{ background:'var(--surface-3)', border:'1px solid var(--border)', boxShadow:'0 12px 28px rgba(0,0,0,0.35)' }}
                  >
                    {pageSizeSelectOptions.map((value) => {
                      const isCurrentCustomValue = !pageSizeOptions.includes(value);
                      const isDisabledPreset = !isCurrentCustomValue && executionMode !== 'power' && value > SAFE_QUERY_LIMIT_MAX;
                      const isActive = limit === value;
                      return (
                        <button
                          key={value}
                          type="button"
                          disabled={isDisabledPreset}
                          className="w-full text-left px-2 py-1.5 rounded-md text-2xs transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                          style={{
                            background: isActive ? 'var(--surface-4)' : 'transparent',
                            color: isDisabledPreset ? 'var(--text-tertiary)' : (isActive ? 'var(--text-primary)' : 'var(--text-secondary)'),
                          }}
                          onClick={() => handleQuickPageSizeChange(value >= QUERY_LIMIT_MAX ? 'unlimited' : String(value))}
                        >
                          {isCurrentCustomValue ? `${formatLimitOptionLabel(value)} (current)` : formatLimitOptionLabel(value)}
                        </button>
                      );
                    })}
                    <div className="mt-1 pt-1" style={{ borderTop:'1px solid var(--border)' }}>
                      <button
                        type="button"
                        className="w-full text-left px-2 py-1.5 rounded-md text-2xs transition-colors"
                        style={{ color:'var(--text-secondary)' }}
                        onClick={() => {
                          setShowCustomPageSizeInput(true);
                          setCustomPageSizeInput(String(limit));
                          setShowPageSizeMenu(false);
                        }}
                      >
                        Custom
                      </button>
                    </div>
                  </FloatingMenu>
                )}
              </div>
              {showCustomPageSizeInput && (
                <>
                  <input
                    type="number"
                    min={50}
                    max={QUERY_LIMIT_MAX}
                    step={1}
                    value={customPageSizeInput}
                    onChange={(event) => setCustomPageSizeInput(event.target.value)}
                    className="ms-number w-20 px-2 py-1 rounded-md text-2xs font-mono"
                    style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                  />
                  <button type="button" className="btn-ghost text-2xs px-2 py-1" onClick={applyCustomPageSize}>
                    Apply
                  </button>
                </>
              )}
            </div>
            <button onClick={()=>setPage(0)} disabled={page===0} className="btn-ghost p-1 disabled:opacity-30 text-2xs">First</button>
            <button onClick={()=>setPage(Math.max(0,page-1))} disabled={page===0} className="btn-ghost p-1 disabled:opacity-30"><ChevronLeft className="w-4 h-4" /></button>
            <span className="text-xs px-2 min-w-[70px] text-center" style={{color:'var(--text-secondary)'}}>
              {totalPages ? `${page + 1} / ${totalPages}` : `Page ${page + 1}`}
            </span>
            <button
              onClick={() => setPage((prev) => prev + 1)}
              disabled={totalPages ? page >= totalPages - 1 : !hasMore}
              className="btn-ghost p-1 disabled:opacity-30"
            >
              <ChevronRight className="w-4 h-4" />
            </button>
            {totalPages && (
              <button
                onClick={() => setPage(totalPages - 1)}
                disabled={page >= totalPages - 1 || (totalKnown && totalValue > 100000)}
                title={totalKnown && totalValue > 100000 ? 'Disabled for large collections - navigate with Next' : undefined}
                className="btn-ghost p-1 disabled:opacity-30 text-2xs"
              >Last</button>
            )}
          </div>
        </div>
      )}
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
    </div>
  );
}


import React, { useState, useCallback, useRef, useEffect, useMemo } from 'react';
import api from '../utils/api';
import { Play, Copy, Check, Zap, X, History, StopCircle, Eye, AlertTriangle, Loader, ChevronDown, Download } from './Icons';
import { formatNumber, formatDuration, prettyJson, copyToClipboard } from '../utils/formatters';
import JsonView from './JsonView';
import InlineAlert from './InlineAlert';
import ConfirmDialog from './modals/ConfirmDialog';
import DropdownSelect from './DropdownSelect';
import { genId } from '../utils/genId';

const HISTORY_KEY = 'mongostudio_query_history';
const QUERY_TAB_OVERRIDE_KEY = 'mongostudio_query_tab_override';
const TEMPLATE_SECTIONS = [
  {
    title: 'Find',
    items: [
      { label: 'Find all', query: 'db.collection.find({})' },
      { label: 'Find with filter', query: 'db.collection.find({ "status": "active" })' },
      { label: 'Projection', query: 'db.collection.aggregate([\n  { "$project": { "_id": 1, "field": 1 } },\n  { "$limit": 50 }\n])' },
      { label: 'Sort', query: 'db.collection.find({}, { "sort": { "_id": -1 } })' },
      { label: 'Pagination', query: 'db.collection.aggregate([\n  { "$sort": { "_id": -1 } },\n  { "$skip": 100 },\n  { "$limit": 50 }\n])' },
    ],
  },
  {
    title: 'Aggregation',
    items: [
      { label: 'Match', query: 'db.collection.aggregate([\n  { "$match": { "status": "active" } },\n  { "$limit": 50 }\n])' },
      { label: 'Group by', query: 'db.collection.aggregate([\n  { "$group": { "_id": "$field", "count": { "$sum": 1 } } },\n  { "$sort": { "count": -1 } }\n])' },
      { label: 'Unwind', query: 'db.collection.aggregate([\n  { "$unwind": "$tags" },\n  { "$limit": 50 }\n])' },
      { label: 'Unwind + group', query: 'db.collection.aggregate([\n  { "$unwind": "$tags" },\n  { "$group": { "_id": "$tags", "count": { "$sum": 1 } } },\n  { "$sort": { "count": -1 } }\n])' },
      { label: 'Lookup', query: 'db.collection.aggregate([\n  { "$lookup": {\n    "from": "other",\n    "localField": "fk",\n    "foreignField": "_id",\n    "as": "joined"\n  } }\n])' },
    ],
  },
  {
    title: 'Utility',
    items: [
      { label: 'Count', query: 'db.collection.aggregate([\n  { "$count": "total" }\n])' },
      { label: 'Distinct', query: 'db.collection.aggregate([\n  { "$group": { "_id": "$field" } },\n  { "$limit": 50 }\n])' },
      { label: 'Date range', query: 'db.collection.find({\n  "createdAt": {\n    "$gte": { "$date": "2024-01-01T00:00:00Z" },\n    "$lt": { "$date": "2025-01-01T00:00:00Z" }\n  }\n})' },
      { label: 'Text search', query: 'db.collection.find({ "$text": { "$search": "error timeout" } })' },
    ],
  },
];
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
const QUERY_TIMEOUT_OPTIONS = [5000, 10000, 30000, 60000, POWER_QUERY_TIMEOUT_PRESET_MAX_MS];
const BASE_QUERY_LIMIT_OPTIONS = [50, 100, 200, 500, 1000, POWER_QUERY_LIMIT_PRESET_MAX];
const clampTimeoutMs = (value) => Math.max(DEFAULT_QUERY_TIMEOUT_MS, Math.min(Number(value) || DEFAULT_QUERY_TIMEOUT_MS, QUERY_TIMEOUT_MAX_MS));
const getModeTimeoutMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS : SAFE_QUERY_TIMEOUT_MAX_MS);
const getModeLimitMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_LIMIT_MAX : QUERY_LIMIT_OVERRIDE_MAX);
const clampLimitValue = (value, mode = 'safe') => Math.max(DEFAULT_QUERY_LIMIT, Math.min(Number(value) || DEFAULT_QUERY_LIMIT, getModeLimitMax(mode)));
const isIdSortFirstStage = (pipeline = []) => {
  if (!Array.isArray(pipeline) || pipeline.length === 0) return false;
  const firstStage = pipeline[0];
  if (!firstStage || typeof firstStage !== 'object' || Array.isArray(firstStage)) return false;
  const firstKey = Object.keys(firstStage)[0];
  if (firstKey !== '$sort') return false;
  const sortSpec = firstStage.$sort;
  if (!sortSpec || typeof sortSpec !== 'object' || Array.isArray(sortSpec)) return false;
  const sortEntries = Object.entries(sortSpec);
  if (sortEntries.length !== 1) return false;
  const [field, direction] = sortEntries[0];
  if (field !== '_id') return false;
  if (direction !== 1 && direction !== -1) return false;
  return true;
};
const isIdSortPipelineWithLimit = (pipeline = []) => {
  if (!isIdSortFirstStage(pipeline)) return false;
  return pipeline.some((stage) => {
    if (!stage || typeof stage !== 'object' || Array.isArray(stage)) return false;
    const stageName = Object.keys(stage)[0];
    if (stageName !== '$limit') return false;
    const n = Number(stage.$limit);
    return Number.isFinite(n) && n > 0;
  });
};
const getAggregatePipelineWarning = (pipeline = []) => {
  if (!Array.isArray(pipeline) || pipeline.length === 0) return '';
  const matchIndex = pipeline.findIndex((stage) => {
    if (!stage || typeof stage !== 'object' || Array.isArray(stage)) return false;
    const stageName = Object.keys(stage)[0];
    return stageName === '$match';
  });
  if (matchIndex === -1) {
    if (isIdSortFirstStage(pipeline) || isIdSortPipelineWithLimit(pipeline)) return '';
    return 'No $match stage detected. This aggregation may scan a large part of the collection.';
  }
  if (matchIndex > 0) {
    return '$match appears late in the pipeline. Consider moving it earlier to reduce scan volume.';
  }
  return '';
};
const buildFieldLiteral = (fieldInfo) => {
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
  if (primaryType === 'array') return '[]';
  if (primaryType === 'object') return '{}';
  const text = typeof sample === 'string' && sample.length > 0 ? sample : '';
  return JSON.stringify(text);
};
const formatTimeoutOptionLabel = (value) => `${Math.round(value / 1000)}s`;

function getHistory() {
  try {
    const parsed = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]');
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter((entry) => entry && typeof entry === 'object')
      .map((entry) => ({ ...entry, id: typeof entry.id === 'string' && entry.id ? entry.id : genId() }));
  } catch {
    return [];
  }
}
function saveHistory(entry) {
  try {
    const h = getHistory().filter((e) => !(e.query === entry.query && e.db === entry.db && e.collection === entry.collection));
    h.unshift({ id: genId(), ...entry });
    localStorage.setItem(HISTORY_KEY, JSON.stringify(h.slice(0, 50)));
  } catch {}
}

function getVisibleResultCount(data) {
  if (Array.isArray(data?.documents)) return data.documents.length;
  if (Array.isArray(data?.results)) return data.results.length;
  return Number(data?.total?.value || data?.total || 0);
}

export default function QueryConsole({ db, collection, onQueryMs, refreshToken = 0 }) {
  const [query, setQuery] = useState(`db.${collection}.find({})`);
  const [results, setResults] = useState(null);
  const [error, setError] = useState(null);
  const [running, setRunning] = useState(false);
  const [copied, setCopied] = useState(false);
  const [elapsed, setElapsed] = useState(null);
  const [liveTimer, setLiveTimer] = useState(0);
  const [showTemplates, setShowTemplates] = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const [history, setHistory] = useState([]);
  const [explainResult, setExplainResult] = useState(null);
  const [showExplain, setShowExplain] = useState(false);
  const [explainError, setExplainError] = useState(null);
  const [explainRunning, setExplainRunning] = useState(false);
  const [explainLiveTimer, setExplainLiveTimer] = useState(0);
  const [explainElapsed, setExplainElapsed] = useState(null);
  const [slow, setSlow] = useState(false);
  const [resultSize, setResultSize] = useState(null);
  const [indexHints, setIndexHints] = useState([]);
  const [selectedHint, setSelectedHint] = useState('auto');
  const [schemaFields, setSchemaFields] = useState([]);
  const [showAutofill, setShowAutofill] = useState(false);
  const [autofillLimit, setAutofillLimit] = useState(8);
  const [placeholderIndex, setPlaceholderIndex] = useState(0);
  const [showQueryAssist, setShowQueryAssist] = useState(false);
  const [queryAssistLimit, setQueryAssistLimit] = useState(6);
  const [showExportMenu, setShowExportMenu] = useState(false);
  const [globalToggleVersion, setGlobalToggleVersion] = useState(0);
  const [globalToggleOpen, setGlobalToggleOpen] = useState(true);
  const [showBudgetMenu, setShowBudgetMenu] = useState(false);
  const [queryBudget, setQueryBudget] = useState({ timeoutMs: DEFAULT_QUERY_TIMEOUT_MS, limit: DEFAULT_QUERY_LIMIT });
  const [budgetDraft, setBudgetDraft] = useState({ timeoutMs: DEFAULT_QUERY_TIMEOUT_MS, limit: DEFAULT_QUERY_LIMIT });
  const [customTimeoutSeconds, setCustomTimeoutSeconds] = useState('');
  const [customLimitValue, setCustomLimitValue] = useState('');
  const [persistOverride, setPersistOverride] = useState(false);
  const [preRunWarning, setPreRunWarning] = useState('');
  const [collscanGuardPending, setCollscanGuardPending] = useState(false);
  const [confirmDialog, setConfirmDialog] = useState(null);
  const [execConfig, setExecConfig] = useState(null);

  const controllerRef = useRef(null);
  const explainControllerRef = useRef(null);
  const explainRequestIdRef = useRef(0);
  const pendingRunRef = useRef(null); // stores { parsed, budget } when collscan guard is waiting
  const timerRef = useRef(null);
  const timerStartedAtRef = useRef(0);
  const explainTimerRef = useRef(null);
  const explainTimerStartedAtRef = useRef(0);
  const textareaRef = useRef(null);
  const templatesRef = useRef(null);
  const historyRef = useRef(null);
  const autofillRef = useRef(null);
  const queryAssistRef = useRef(null);
  const exportMenuRef = useRef(null);
  const budgetMenuRef = useRef(null);
  const budgetRef = useRef(queryBudget);
  const persistOverrideRef = useRef(persistOverride);
  const overrideHydratedRef = useRef(false);
  const defaultBudgetRef = useRef({ timeoutMs: DEFAULT_QUERY_TIMEOUT_MS, limit: DEFAULT_QUERY_LIMIT });
  const executionMode = execConfig?.mode === 'power' ? 'power' : 'safe';
  const queryLimitOptions = useMemo(
    () => [...BASE_QUERY_LIMIT_OPTIONS, POWER_QUERY_LIMIT_MAX],
    [executionMode],
  );
  const hintOptions = useMemo(
    () => [
      { value: 'auto', label: 'Auto' },
      ...indexHints.map((name) => ({ value: name, label: name })),
    ],
    [indexHints],
  );
  const formatLimitOptionLabel = useCallback(
    (value) => (value >= POWER_QUERY_LIMIT_MAX ? 'Unlimited' : formatNumber(value)),
    [],
  );
  const defaultBudget = useMemo(() => ({
    timeoutMs: Math.min(clampTimeoutMs(execConfig?.maxTimeMS), getModeTimeoutMax(executionMode)),
    limit: clampLimitValue(execConfig?.maxResultSize, executionMode),
  }), [execConfig, executionMode]);

  useEffect(() => {
    defaultBudgetRef.current = defaultBudget;
  }, [defaultBudget]);
  useEffect(() => {
    budgetRef.current = queryBudget;
  }, [queryBudget]);
  useEffect(() => {
    setBudgetDraft(queryBudget);
  }, [queryBudget.timeoutMs, queryBudget.limit]);
  useEffect(() => {
    persistOverrideRef.current = persistOverride;
  }, [persistOverride]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      const raw = sessionStorage.getItem(QUERY_TAB_OVERRIDE_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (!parsed || parsed.persistOverride !== true) return;
      const timeoutMs = Math.min(clampTimeoutMs(parsed.timeoutMs), getModeTimeoutMax(executionMode));
      const limit = clampLimitValue(parsed.limit, executionMode);
      persistOverrideRef.current = true;
      setPersistOverride(true);
      setQueryBudget({ timeoutMs, limit });
    } catch {}
    finally {
      overrideHydratedRef.current = true;
    }
  }, [executionMode]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (!overrideHydratedRef.current) return;
    try {
      if (!persistOverride) {
        sessionStorage.removeItem(QUERY_TAB_OVERRIDE_KEY);
        return;
      }
      sessionStorage.setItem(QUERY_TAB_OVERRIDE_KEY, JSON.stringify({
        persistOverride: true,
        timeoutMs: Math.min(clampTimeoutMs(queryBudget.timeoutMs), getModeTimeoutMax(executionMode)),
        limit: clampLimitValue(queryBudget.limit, executionMode),
      }));
    } catch {}
  }, [persistOverride, queryBudget.timeoutMs, queryBudget.limit, executionMode]);

  useEffect(() => { setHistory(getHistory()); }, []);
  useEffect(() => {
    setQuery(`db.${collection}.find({})`);
    setResults(null);
    setError(null);
    setExplainResult(null);
    setExplainError(null);
    setExplainRunning(false);
    setExplainElapsed(null);
    explainControllerRef.current?.abort();
    explainControllerRef.current = null;
    if (explainTimerRef.current) {
      clearInterval(explainTimerRef.current);
      explainTimerRef.current = null;
    }
    explainTimerStartedAtRef.current = 0;
    setExplainLiveTimer(0);
    setSelectedHint('auto');
    setShowTemplates(false);
    setShowHistory(false);
    setShowAutofill(false);
    setShowQueryAssist(false);
    setAutofillLimit(8);
    setQueryAssistLimit(6);
    setPlaceholderIndex(0);
    setShowExportMenu(false);
    setGlobalToggleVersion(0);
    setShowBudgetMenu(false);
    if (!persistOverrideRef.current) {
      setQueryBudget(defaultBudgetRef.current);
      setPersistOverride(false);
    }
    setPreRunWarning('');
  }, [db, collection]);

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

  useEffect(() => {
    if (persistOverrideRef.current) return;
    setQueryBudget(defaultBudget);
  }, [defaultBudget]);

  useEffect(() => {
    let active = true;
    api.getIndexes(db, collection, { budget: defaultBudget })
      .then(data => {
        if (!active) return;
        const names = (data.indexes || []).map(idx => idx.name).filter(Boolean);
        setIndexHints(names);
      })
      .catch(() => {
        if (active) setIndexHints([]);
      });
    return () => { active = false; };
  }, [db, collection, refreshToken, defaultBudget]);

  useEffect(() => {
    let active = true;
    api.getSchema(db, collection, 120, { budget: defaultBudget })
      .then((data) => {
        if (!active) return;
        const fields = (data.fields || [])
          .filter((field) => Boolean(field?.path))
          .slice(0, 24)
          .map((field) => ({
            path: field.path,
            types: Array.isArray(field.types) ? field.types : [],
            sample: field.sample ?? null,
          }));
        setSchemaFields(fields);
      })
      .catch(() => {
        if (active) setSchemaFields([]);
      });
    return () => { active = false; };
  }, [db, collection, refreshToken, defaultBudget]);

  useEffect(() => {
    if (selectedHint === 'auto') return;
    if (!indexHints.includes(selectedHint)) setSelectedHint('auto');
  }, [indexHints, selectedHint]);

  useEffect(() => {
    const onMouseDown = (event) => {
      if (showTemplates && templatesRef.current && !templatesRef.current.contains(event.target)) setShowTemplates(false);
      if (showHistory && historyRef.current && !historyRef.current.contains(event.target)) setShowHistory(false);
      if (showAutofill && autofillRef.current && !autofillRef.current.contains(event.target)) setShowAutofill(false);
      if (showExportMenu && exportMenuRef.current && !exportMenuRef.current.contains(event.target)) setShowExportMenu(false);
      if (showBudgetMenu && budgetMenuRef.current && !budgetMenuRef.current.contains(event.target)) setShowBudgetMenu(false);
      if (showQueryAssist && queryAssistRef.current && !queryAssistRef.current.contains(event.target) && textareaRef.current && !textareaRef.current.contains(event.target)) {
        setShowQueryAssist(false);
      }
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') {
        setShowTemplates(false);
        setShowHistory(false);
        setShowAutofill(false);
        setShowExportMenu(false);
        setShowBudgetMenu(false);
        setShowQueryAssist(false);
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [showTemplates, showHistory, showAutofill, showExportMenu, showBudgetMenu, showQueryAssist]);

  const startLiveTimer = useCallback(() => {
    timerStartedAtRef.current = Date.now();
    setLiveTimer(0);
    if (timerRef.current) clearInterval(timerRef.current);
    timerRef.current = setInterval(() => {
      if (!timerStartedAtRef.current) return;
      setLiveTimer(Math.max(0, Date.now() - timerStartedAtRef.current));
    }, 100);
  }, []);

  const stopLiveTimer = useCallback(() => {
    if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
    timerStartedAtRef.current = 0;
  }, []);

  const startExplainLiveTimer = useCallback(() => {
    explainTimerStartedAtRef.current = Date.now();
    setExplainLiveTimer(0);
    if (explainTimerRef.current) clearInterval(explainTimerRef.current);
    explainTimerRef.current = setInterval(() => {
      if (!explainTimerStartedAtRef.current) return;
      setExplainLiveTimer(Math.max(0, Date.now() - explainTimerStartedAtRef.current));
    }, 100);
  }, []);

  const stopExplainLiveTimer = useCallback(() => {
    if (explainTimerRef.current) {
      clearInterval(explainTimerRef.current);
      explainTimerRef.current = null;
    }
    explainTimerStartedAtRef.current = 0;
  }, []);

  const restoreBudgetIfOneShot = useCallback((budget) => {
    const usedOverride = budget.timeoutMs !== defaultBudget.timeoutMs || budget.limit !== defaultBudget.limit;
    if (usedOverride && !persistOverride) {
      setQueryBudget(defaultBudget);
    }
  }, [defaultBudget, persistOverride]);

  useEffect(() => () => {
    controllerRef.current?.abort();
    explainControllerRef.current?.abort();
    stopLiveTimer();
    stopExplainLiveTimer();
  }, [stopLiveTimer, stopExplainLiveTimer]);

  const parseQuery = (q) => {
    const trimmed = q.trim();
    if (!trimmed) throw new Error('Query is empty.');

    const parseCommand = (op) => {
      const m = trimmed.match(new RegExp(`^db\\.(?:getCollection\\((['"])(.+?)\\1\\)|([\\w.$-]+))\\.${op}\\(([\\s\\S]*)\\)$`));
      if (!m) return null;
      return { targetCollection: m[2] || m[3] || '', args: m[4] || '' };
    };

    const findCmd = parseCommand('find');
    const aggCmd = parseCommand('aggregate');

    if (findCmd) {
      if (findCmd.targetCollection && findCmd.targetCollection !== collection) {
        throw new Error(`Query context mismatch: selected collection is "${collection}", but query targets "${findCmd.targetCollection}".`);
      }
      try {
        const args = findCmd.args.trim();
        let filter = '{}';
        let sort = '{}';
        const parsed = JSON.parse(`[${args}]`);
        filter = JSON.stringify(parsed[0] || {});
        if (parsed[1]?.sort) sort = JSON.stringify(parsed[1].sort);
        return { type: 'find', filter, sort };
      } catch {
        return { type: 'find', filter: findCmd.args.trim(), sort: '{}' };
      }
    }

    if (aggCmd) {
      if (aggCmd.targetCollection && aggCmd.targetCollection !== collection) {
        throw new Error(`Query context mismatch: selected collection is "${collection}", but query targets "${aggCmd.targetCollection}".`);
      }
      try {
        const pipeline = JSON.parse(aggCmd.args);
        return { type: 'aggregate', pipeline };
      } catch (e) {
        throw new Error(`Invalid pipeline JSON: ${e.message}`);
      }
    }

    try {
      const raw = JSON.parse(trimmed);
      if (Array.isArray(raw)) return { type: 'aggregate', pipeline: raw };
      return { type: 'find', filter: trimmed, sort: '{}' };
    } catch {}

    throw new Error(`Unsupported query format. Use db.${collection}.find({...}) or db.${collection}.aggregate([...]).`);
  };

  const handleRunWithBudget = useCallback(async (budget) => {
    setError(null);
    setResults(null);
    setExplainResult(null);
    setExplainError(null);
    setShowExplain(false);
    setSlow(false);
    setResultSize(null);
    setPreRunWarning('');
    setCollscanGuardPending(false);

    let queryStarted = false;
    let controller = null;
    try {
      const runStartedAt = Date.now();
      const getRemainingBudget = (options = {}) => {
        const minMs = Math.max(0, Number(options.minMs) || 0);
        const capMs = Number(options.capMs);
        const totalBudgetMs = Math.max(1000, Number(budget?.timeoutMs) || DEFAULT_QUERY_TIMEOUT_MS);
        const spentMs = Math.max(0, Date.now() - runStartedAt);
        let remainingMs = totalBudgetMs - spentMs;
        if (Number.isFinite(capMs) && capMs > 0) remainingMs = Math.min(remainingMs, capMs);
        if (remainingMs < minMs) return null;
        return {
          ...budget,
          timeoutMs: Math.max(minMs, Math.round(remainingMs)),
        };
      };
      const parsed = parseQuery(query);
      const aggregatePipelineWarning = parsed.type === 'aggregate'
        ? getAggregatePipelineWarning(parsed.pipeline)
        : '';
      const skipAggregatePrecheck = parsed.type === 'aggregate' && isIdSortFirstStage(parsed.pipeline);
      if (parsed.type === 'find' && executionMode === 'safe') {
        try {
          const preBudget = getRemainingBudget({ minMs: 500, capMs: 2000 });
          if (preBudget) {
            const pre = await api.explain(db, collection, { ...parsed, hint: selectedHint, limit: budget.limit, verbosity: 'queryPlanner' }, { budget: preBudget });
            if (pre?.summary?.isCollScan) {
              pendingRunRef.current = { parsed, budget };
              setCollscanGuardPending(true);
              return;
            }
            const docsExamined = Number(pre?.summary?.totalDocsExamined || 0);
            if (docsExamined > 50000) {
              setPreRunWarning(`Explain warning: high scan volume (${formatNumber(docsExamined)} docs examined).`);
            }
          }
        } catch { /* explain failed - continue with query */ }
      } else if (parsed.type === 'aggregate' && executionMode === 'safe' && !skipAggregatePrecheck) {
        try {
          const preBudget = getRemainingBudget({ minMs: 500, capMs: 2000 });
          if (preBudget) {
            const pre = await api.explain(db, collection, { ...parsed, hint: selectedHint, limit: budget.limit, verbosity: 'queryPlanner' }, { budget: preBudget });
            if (pre?.summary?.isCollScan) {
              pendingRunRef.current = { parsed, budget };
              setCollscanGuardPending(true);
              return;
            }
          }
          if (aggregatePipelineWarning) setPreRunWarning(aggregatePipelineWarning);
        } catch {
          if (aggregatePipelineWarning) setPreRunWarning(aggregatePipelineWarning);
        }
      } else if (parsed.type === 'find') {
        api.explain(db, collection, { ...parsed, hint: selectedHint, limit: budget.limit, verbosity: 'queryPlanner' }, { budget })
          .then((pre) => {
            const docsExamined = Number(pre?.summary?.totalDocsExamined || 0);
            if (pre?.summary?.isCollScan) {
              setPreRunWarning('Explain warning: collection scan detected (no index used).');
            } else if (docsExamined > 50000) {
              setPreRunWarning(`Explain warning: high scan volume (${formatNumber(docsExamined)} docs examined).`);
            }
          })
          .catch(() => {});
      } else if (parsed.type === 'aggregate' && aggregatePipelineWarning) {
        setPreRunWarning(aggregatePipelineWarning);
      }
      const runBudget = getRemainingBudget({ minMs: 1000 });
      if (!runBudget) throw new Error('Query timed out. Try filters or indexes.');
      queryStarted = true;
      setRunning(true);
      startLiveTimer();
      controller = new AbortController();
      controllerRef.current = controller;
      let data;
      if (parsed.type === 'aggregate') {
        data = await api.runAggregation(db, collection, parsed.pipeline, controller, { budget: runBudget, hint: selectedHint });
        data.documents = data.results;
        data.total = data.results?.length || 0;
      } else {
        data = await api.getDocuments(
          db,
          collection,
          { filter: parsed.filter, sort: parsed.sort, limit: budget.limit, hint: selectedHint },
          controller,
          { budget: runBudget },
        );
      }

      const ms = data._elapsed || 0;
      setResults(data);
      setElapsed(ms);
      onQueryMs?.(ms);
      if (data._slow) setSlow(true);

      const jsonStr = JSON.stringify(data.documents || data.results || []);
      setResultSize(new Blob([jsonStr]).size);

      saveHistory({
        query,
        db,
        collection,
        ts: Date.now(),
        elapsed: ms,
        count: getVisibleResultCount(data),
        type: parsed.type,
      });
      setHistory(getHistory());

      if (data.warnings?.length) data.warnings.forEach(w => console.warn('[Query]', w));
    } catch (err) {
      if (err.name !== 'AbortError') setError(err.message);
    } finally {
      if (queryStarted) {
        setRunning(false);
        stopLiveTimer();
        controllerRef.current = null;
        restoreBudgetIfOneShot(budget);
      }
    }
  }, [query, db, collection, onQueryMs, selectedHint, executionMode, restoreBudgetIfOneShot, startLiveTimer, stopLiveTimer]);

  const handleRun = useCallback(async () => {
    if (running) {
      controllerRef.current?.abort();
      pendingRunRef.current = null;
      setCollscanGuardPending(false);
      setRunning(false);
      stopLiveTimer();
      controllerRef.current = null;
      return;
    }
    const budget = {
      timeoutMs: Math.min(
        clampTimeoutMs(Number.isFinite(Number(budgetRef.current?.timeoutMs)) ? Number(budgetRef.current.timeoutMs) : defaultBudget.timeoutMs),
        getModeTimeoutMax(executionMode),
      ),
      limit: clampLimitValue(Number.isFinite(Number(budgetRef.current?.limit)) ? Number(budgetRef.current.limit) : defaultBudget.limit, executionMode),
    };
    if (executionMode === 'power' && budget.timeoutMs > POWER_QUERY_TIMEOUT_PRESET_MAX_MS) {
      setConfirmDialog({
        title: 'High Timeout Override',
        message: `${Math.round(budget.timeoutMs / 1000)}s timeout can be heavy on production. Continue with this query?`,
        confirmLabel: 'Run query',
        onConfirm: () => handleRunWithBudget(budget),
      });
      return;
    }
    handleRunWithBudget(budget);
  }, [running, defaultBudget, executionMode, handleRunWithBudget, stopLiveTimer]);

  const handleConfirmCollscan = useCallback(async () => {
    const pending = pendingRunRef.current;
    if (!pending) return;
    pendingRunRef.current = null;
    setCollscanGuardPending(false);
    setError(null);
    setPreRunWarning('');
    const { parsed, budget } = pending;
    setRunning(true);
    startLiveTimer();
    const controller = new AbortController();
    controllerRef.current = controller;
    try {
      let data;
      if (parsed.type === 'aggregate') {
        data = await api.runAggregation(db, collection, parsed.pipeline, controller, { budget, hint: selectedHint });
        data.documents = data.results;
        data.total = data.results?.length || 0;
      } else {
        data = await api.getDocuments(
          db,
          collection,
          { filter: parsed.filter, sort: parsed.sort, limit: budget.limit, hint: selectedHint },
          controller,
          { budget },
        );
      }
      const ms = data._elapsed || 0;
      setResults(data);
      setElapsed(ms);
      onQueryMs?.(ms);
      if (data._slow) setSlow(true);
      const jsonStr = JSON.stringify(data.documents || data.results || []);
      setResultSize(new Blob([jsonStr]).size);
      saveHistory({
        query, db, collection, ts: Date.now(), elapsed: ms,
        count: getVisibleResultCount(data),
        type: parsed.type,
      });
      setHistory(getHistory());
      if (data.warnings?.length) data.warnings.forEach(w => console.warn('[Query]', w));
    } catch (err) {
      if (err.name !== 'AbortError') setError(err.message);
    } finally {
      setRunning(false);
      stopLiveTimer();
      controllerRef.current = null;
      restoreBudgetIfOneShot(budget);
    }
  }, [db, collection, query, onQueryMs, selectedHint, restoreBudgetIfOneShot, startLiveTimer, stopLiveTimer]);

  const handleCancelCollscan = useCallback(() => {
    controllerRef.current?.abort();
    pendingRunRef.current = null;
    setCollscanGuardPending(false);
    setRunning(false);
    stopLiveTimer();
    controllerRef.current = null;
  }, [stopLiveTimer]);

  const handleExplain = async () => {
    if (explainRunning || explainControllerRef.current) {
      explainControllerRef.current?.abort();
      return;
    }
    const requestId = explainRequestIdRef.current + 1;
    explainRequestIdRef.current = requestId;
    setError(null);
    setExplainResult(null);
    setExplainError(null);
    setExplainElapsed(null);
    setExplainRunning(true);
    startExplainLiveTimer();
    const startedAt = Date.now();
    const controller = new AbortController();
    explainControllerRef.current = controller;
    try {
      const parsed = parseQuery(query);
      const budget = {
        timeoutMs: Math.min(
          clampTimeoutMs(Number.isFinite(Number(budgetRef.current?.timeoutMs)) ? Number(budgetRef.current.timeoutMs) : defaultBudget.timeoutMs),
          getModeTimeoutMax(executionMode),
        ),
        limit: clampLimitValue(Number.isFinite(Number(budgetRef.current?.limit)) ? Number(budgetRef.current.limit) : defaultBudget.limit, executionMode),
      };
      const data = await api.explain(
        db,
        collection,
        { ...parsed, hint: selectedHint, limit: budget.limit, verbosity: 'executionStats' },
        { budget, controller },
      );
      if (explainRequestIdRef.current !== requestId) return;
      setExplainResult(data);
      setExplainElapsed(Number(data?._elapsed) || Math.max(0, Date.now() - startedAt));
      setShowExplain(true);
    } catch (err) {
      if (explainRequestIdRef.current !== requestId) return;
      if (err.name === 'AbortError') {
        return;
      } else {
        setExplainError(err.message);
        setExplainElapsed(Math.max(0, Date.now() - startedAt));
        setShowExplain(true);
      }
    } finally {
      if (explainRequestIdRef.current !== requestId) return;
      if (explainControllerRef.current === controller) explainControllerRef.current = null;
      stopExplainLiveTimer();
      setExplainRunning(false);
    }
  };

  const handleCopy = () => {
    copyToClipboard(prettyJson(results?.documents || results?.results || [])).then((ok) => {
      if (!ok) return;
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  const toCsv = (rows = []) => {
    if (!Array.isArray(rows) || rows.length === 0) return '';
    const keys = [...new Set(rows.flatMap((row) => Object.keys(row || {})))];
    const header = keys.join(',');
    const lines = rows.map((row) => keys.map((key) => {
      const value = row?.[key];
      if (value === null || value === undefined) return '';
      if (typeof value === 'object') return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
      const text = String(value);
      return (text.includes(',') || text.includes('"') || text.includes('\n'))
        ? `"${text.replace(/"/g, '""')}"`
        : text;
    }).join(','));
    return [header, ...lines].join('\n');
  };

  const downloadText = (filename, text, mime = 'application/json') => {
    const blob = new Blob([text], { type: mime });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    link.click();
    URL.revokeObjectURL(url);
  };

  const handleExportResults = (format = 'json') => {
    if (!docs.length) return;
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    if (format === 'csv') {
      downloadText(`${db}.${collection}.query.${ts}.csv`, toCsv(docs), 'text/csv');
      setShowExportMenu(false);
      return;
    }
    downloadText(`${db}.${collection}.query.${ts}.json`, JSON.stringify(docs, null, 2), 'application/json');
    setShowExportMenu(false);
  };

  const handleToggleAllDocs = (open) => {
    setGlobalToggleOpen(open);
    setGlobalToggleVersion((prev) => prev + 1);
  };

  const handleKeyDown = (event) => {
    if (event.key === 'Tab' && showQueryAssist && visibleQueryAssistItems.length > 0) {
      event.preventDefault();
      applyQueryAssistSuggestion(visibleQueryAssistItems[0]);
      return;
    }
    if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
      event.preventDefault();
      handleRun();
    }
  };

  let parsedType = null;
  try {
    parsedType = parseQuery(query).type;
  } catch {}
  const hintEnabled = parsedType === 'find' || parsedType === 'aggregate';
  const docs = results?.documents || results?.results || [];
  const budgetOverrideActive = queryBudget.timeoutMs !== defaultBudget.timeoutMs || queryBudget.limit !== defaultBudget.limit;
  const timeoutIsCustom = !QUERY_TIMEOUT_OPTIONS.includes(budgetDraft.timeoutMs);
  const limitIsCustom = !queryLimitOptions.includes(budgetDraft.limit);
  const timeoutCustomEditing = timeoutIsCustom || customTimeoutSeconds !== '';
  const limitCustomEditing = limitIsCustom || customLimitValue !== '';
  const externalToggleSignal = useMemo(
    () => ({ version: globalToggleVersion, open: globalToggleOpen }),
    [globalToggleVersion, globalToggleOpen],
  );
  const scopedHistory = useMemo(
    () => history.filter((entry) => entry.db === db && entry.collection === collection),
    [history, db, collection],
  );
  const autoFillItems = useMemo(() => {
    const items = [
      { label: 'Find by _id', query: `db.${collection}.find({ "_id": { "$oid": "" } })` },
      { label: 'Recent docs', query: `db.${collection}.find({}).sort({ "_id": -1 })` },
      { label: 'Count all', query: `db.${collection}.aggregate([{ "$count": "total" }])` },
    ];
    for (const fieldInfo of schemaFields.slice(0, 12)) {
      const fieldPath = fieldInfo.path;
      const fieldType = String(fieldInfo?.types?.[0]?.type || '').toLowerCase();
      const literal = buildFieldLiteral(fieldInfo);
      if (fieldType === 'array') {
        items.push({ label: `${fieldPath} has items`, query: `db.${collection}.find({ "${fieldPath}.0": { "$exists": true } })` });
        items.push({ label: `${fieldPath} empty array`, query: `db.${collection}.find({ "${fieldPath}": { "$size": 0 } })` });
      } else {
        items.push({ label: `${fieldPath} equals`, query: `db.${collection}.find({ "${fieldPath}": ${literal} })` });
      }
      items.push({ label: `${fieldPath} exists`, query: `db.${collection}.find({ "${fieldPath}": { "$exists": true } })` });
    }
    return items;
  }, [schemaFields, collection]);
  const escapeRegExp = useCallback((value) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), []);
  const buildQueryAssist = useCallback((value) => {
    const source = (value || '').replace(/\s+$/, '');
    if (!source || source.length < 2) return [];

    const results = [];
    const push = (label, nextValue) => {
      if (!nextValue) return;
      results.push({ label, value: nextValue });
    };

    if (/db\.$/i.test(source)) {
      push(`Use database "${db}"`, `db.${db}.`);
    }

    const dbPrefix = source.match(/db\.([a-zA-Z0-9_-]*)$/i);
    if (dbPrefix) {
      const prefix = (dbPrefix[1] || '').toLowerCase();
      if (db.toLowerCase().startsWith(prefix)) {
        push(`Database "${db}"`, `db.${db}.`);
      }
    }

    const dbPattern = new RegExp(`db\\.${escapeRegExp(db)}\\.([a-zA-Z0-9_.$-]*)$`, 'i');
    const colPrefix = source.match(dbPattern);
    if (colPrefix) {
      const prefix = (colPrefix[1] || '').toLowerCase();
      const candidates = [...new Set([collection, ...schemaFields.map((field) => field.path.split('.')[0]).filter(Boolean)])];
      candidates
        .filter((name) => name.toLowerCase().startsWith(prefix))
        .slice(0, 8)
        .forEach((name) => {
          push(`Collection "${name}" find`, `db.${db}.${name}.find({})`);
          push(`Collection "${name}" aggregate`, `db.${db}.${name}.aggregate([])`);
        });
    }

    const deduped = [];
    const seen = new Set();
    for (const item of results) {
      const key = `${item.label}:${item.value}`;
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(item);
    }
    return deduped;
  }, [db, collection, schemaFields, escapeRegExp]);
  const queryAssistItems = useMemo(() => buildQueryAssist(query), [buildQueryAssist, query]);
  const visibleQueryAssistItems = queryAssistItems.slice(0, queryAssistLimit);
  const hasMoreQueryAssistItems = queryAssistItems.length > visibleQueryAssistItems.length;
  const applyQueryAssistSuggestion = useCallback((item) => {
    if (!item?.value) return;
    setQuery(item.value);
    setShowQueryAssist(false);
    requestAnimationFrame(() => {
      textareaRef.current?.focus();
    });
  }, []);
  const handleQueryChange = useCallback((event) => {
    const nextValue = event.target.value;
    setQuery(nextValue);

    const trimmed = nextValue.trim();
    if (!trimmed) {
      setShowQueryAssist(false);
      setQueryAssistLimit(6);
      return;
    }

    const nextSuggestions = buildQueryAssist(nextValue);
    if (nextSuggestions.length > 0) {
      setShowQueryAssist(true);
      setQueryAssistLimit(6);
    } else {
      setShowQueryAssist(false);
    }
  }, [buildQueryAssist]);
  const visibleAutoFillItems = autoFillItems.slice(0, autofillLimit);
  const hasMoreAutoFillItems = autoFillItems.length > visibleAutoFillItems.length;
  const queryPlaceholder = useMemo(() => {
    if (autoFillItems.length === 0) return `db.${collection}.find({ })`;
    const idx = placeholderIndex % autoFillItems.length;
    return autoFillItems[idx].query;
  }, [autoFillItems, placeholderIndex, collection]);

  useEffect(() => {
    if (autoFillItems.length <= 1) return undefined;
    const timer = setInterval(() => {
      setPlaceholderIndex((prev) => (prev + 1) % autoFillItems.length);
    }, 3200);
    return () => clearInterval(timer);
  }, [autoFillItems.length]);

  return (
    <div className="h-full flex flex-col">
      <div className="flex-shrink-0" style={{ borderBottom: '1px solid var(--border)' }}>
        <div className="flex items-center gap-1 px-3 py-1.5 whitespace-nowrap min-w-0" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
          <button
            onClick={handleRun}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-all shrink-0 whitespace-nowrap ${running ? 'bg-red-500/10 text-red-400' : 'text-xs'}`}
            style={running ? {} : { background: 'var(--accent)', color: 'var(--surface-0)' }}
            title={running ? 'Cancel query' : 'Run query (Ctrl/Cmd+Enter)'}
          >
            {running ? <><StopCircle className="w-3.5 h-3.5" />Cancel</> : <><Play className="w-3.5 h-3.5" />Run</>}
          </button>

          <button
            onClick={handleExplain}
            className={`btn-ghost flex items-center gap-1.5 text-xs shrink-0 whitespace-nowrap ${explainRunning ? 'text-red-400' : ''}`}
            title={explainRunning ? 'Cancel explain' : 'Explain query'}
          >
            {explainRunning ? <StopCircle className="w-3.5 h-3.5" /> : <Eye className="w-3.5 h-3.5" />}
            {explainRunning ? 'Cancel Explain' : 'Explain'}
          </button>

          <div className="relative" ref={budgetMenuRef}>
            <button
              onClick={() => {
                setShowBudgetMenu((prev) => !prev);
                setShowTemplates(false);
                setShowHistory(false);
                setShowAutofill(false);
                setShowExportMenu(false);
                setShowQueryAssist(false);
              }}
              className={`btn-ghost text-xs flex items-center gap-1 shrink-0 whitespace-nowrap ${budgetOverrideActive ? 'text-amber-300' : ''}`}
              title="Per-query budget"
            >
              Budget {formatTimeoutOptionLabel(queryBudget.timeoutMs)}/{formatLimitOptionLabel(queryBudget.limit)}<ChevronDown className="w-3 h-3" />
            </button>
            {showBudgetMenu && (
              <div
                className="absolute left-0 top-full mt-1 z-[80] rounded-lg shadow-xl p-3 animate-fade-in whitespace-normal"
                style={{ background: 'var(--surface-3)', border: '1px solid var(--border)', width: '300px', maxWidth: 'min(92vw, 420px)' }}
              >
                <div className="text-2xs uppercase tracking-wider mb-2" style={{ color: 'var(--text-tertiary)' }}>Per-query override</div>
                <div className="mb-2">
                  <div className="text-2xs mb-1" style={{ color: 'var(--text-tertiary)' }}>Timeout</div>
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
                            setBudgetDraft((prev) => ({ ...prev, timeoutMs: Math.min(clampTimeoutMs(value), getModeTimeoutMax(executionMode)) }));
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
                      onClick={() => {
                        setBudgetDraft((prev) => ({ ...prev, timeoutMs: Math.min(clampTimeoutMs(prev.timeoutMs), getModeTimeoutMax(executionMode)) }));
                        setCustomTimeoutSeconds(String(Math.max(5, Math.round((budgetDraft.timeoutMs || DEFAULT_QUERY_TIMEOUT_MS) / 1000))));
                      }}
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
                          const requested = Math.round(seconds * 1000);
                          const nextTimeoutMs = Math.min(clampTimeoutMs(requested), getModeTimeoutMax(executionMode));
                          setBudgetDraft((prev) => ({ ...prev, timeoutMs: nextTimeoutMs }));
                          setCustomTimeoutSeconds(String(Math.max(5, Math.round(nextTimeoutMs / 1000))));
                        }}
                      >
                        Set
                      </button>
                      <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                        Custom timeout {executionMode === 'safe' ? 'up to 30s in Safe Mode' : 'can exceed 120s in Power Mode'}
                      </span>
                    </div>
                  )}
                  {!timeoutCustomEditing && (
                    <div className="mt-2 text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                      {executionMode === 'safe' ? 'Safe Mode allows custom timeout up to 30s.' : 'Custom timeout can go over 120s in Power Mode.'}
                    </div>
                  )}
                </div>
                <div className="mb-2">
                  <div className="text-2xs mb-1" style={{ color: 'var(--text-tertiary)' }}>Documents</div>
                  <div className="flex flex-wrap gap-1">
                    {queryLimitOptions.map((value) => {
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
                            setBudgetDraft((prev) => ({ ...prev, limit: clampLimitValue(value, executionMode) }));
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
                      onClick={() => {
                        setBudgetDraft((prev) => ({ ...prev, limit: clampLimitValue(prev.limit, executionMode) }));
                        setCustomLimitValue(String(Math.max(50, Math.round(budgetDraft.limit || DEFAULT_QUERY_LIMIT))));
                      }}
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
                          const nextLimit = clampLimitValue(Math.round(value), executionMode);
                          setBudgetDraft((prev) => ({ ...prev, limit: nextLimit }));
                          setCustomLimitValue(String(Math.max(50, Math.round(nextLimit))));
                        }}
                      >
                        Set
                      </button>
                      <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                        Custom can be over Safe preset limits. Be careful.
                      </span>
                    </div>
                  )}
                  {!limitCustomEditing && (
                    <div className="mt-2 text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                      Presets above 1,000 are Power-only, but Custom lets you set any page size.
                    </div>
                  )}
                </div>
                <div className="mb-2 flex items-center justify-end gap-2">
                  <button
                    type="button"
                    className="btn-primary text-2xs px-2.5 py-1"
                    onClick={() => {
                      setQueryBudget({
                        timeoutMs: Math.min(clampTimeoutMs(budgetDraft.timeoutMs), getModeTimeoutMax(executionMode)),
                        limit: clampLimitValue(budgetDraft.limit, executionMode),
                      });
                      setShowBudgetMenu(false);
                    }}
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
                <label className="flex items-center gap-2 text-2xs cursor-pointer" style={{ color: 'var(--text-secondary)' }}>
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
                  Keep override in Query tab
                </label>
                <div className="mt-1 text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                  Keeps timeout/limit while you browse collections in this tab. If disabled, override is one-shot.
                </div>
                {queryBudget.timeoutMs > 30000 && (
                  <div className={`mt-2 text-2xs ${queryBudget.timeoutMs > POWER_QUERY_TIMEOUT_PRESET_MAX_MS ? 'text-red-400' : 'text-amber-300'}`}>
                    {queryBudget.timeoutMs > POWER_QUERY_TIMEOUT_PRESET_MAX_MS
                      ? 'Timeout above 120s is high-risk and can stress production resources.'
                      : 'Timeout above 30s may be heavy on production.'}
                  </div>
                )}
              </div>
            )}
          </div>

          <div className="relative" ref={templatesRef}>
            <button
              onClick={() => {
                setShowTemplates(!showTemplates);
                setShowHistory(false);
                setShowAutofill(false);
                setShowExportMenu(false);
                setShowQueryAssist(false);
              }}
              className="btn-ghost text-xs flex items-center gap-1 shrink-0 whitespace-nowrap"
            >
              Templates<ChevronDown className="w-3 h-3" />
            </button>
            {showTemplates && (
              <div
                className="absolute left-0 top-full mt-1 z-[80] rounded-lg shadow-xl py-1 animate-fade-in whitespace-normal max-h-[320px] overflow-y-auto overflow-x-hidden"
                style={{ background: 'var(--surface-3)', border: '1px solid var(--border)', width: '300px', maxWidth: 'min(92vw, 420px)' }}
              >
                {TEMPLATE_SECTIONS.map((section, sectionIndex) => (
                  <div key={section.title}>
                    <div
                      className="px-3 py-1.5 text-sm font-semibold"
                      style={{ color: 'var(--text-primary)' }}
                    >
                      {section.title}
                    </div>
                    {section.items.map((template) => (
                      <button
                        key={`${section.title}:${template.label}`}
                        onClick={() => { setQuery(template.query.replace('collection', collection)); setShowTemplates(false); }}
                        className="block w-full text-left px-3 py-1.5 text-xs transition-colors"
                        style={{ color: 'var(--text-secondary)' }}
                        onMouseOver={e => e.currentTarget.style.background = 'var(--surface-4)'}
                        onMouseOut={e => e.currentTarget.style.background = 'transparent'}
                      >
                        <span className="inline-flex items-center gap-2">
                          <span style={{ color: 'var(--text-tertiary)' }}>&bull;</span>
                          <span>{template.label}</span>
                        </span>
                      </button>
                    ))}
                    {sectionIndex < TEMPLATE_SECTIONS.length - 1 && (
                      <div className="my-1" style={{ borderTop: '1px solid var(--border)' }} />
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="relative" ref={autofillRef}>
            <button
              onClick={() => {
                setShowAutofill(!showAutofill);
                setShowTemplates(false);
                setShowHistory(false);
                setShowExportMenu(false);
                setShowQueryAssist(false);
                setAutofillLimit(8);
              }}
              className="btn-ghost text-xs flex items-center gap-1 shrink-0 whitespace-nowrap"
            >
              Auto-fill<ChevronDown className="w-3 h-3" />
            </button>
            {showAutofill && (
              <div
                className="absolute left-0 top-full mt-1 z-[80] rounded-lg shadow-xl py-1 max-h-[320px] overflow-y-auto overflow-x-hidden animate-fade-in whitespace-normal"
                style={{ background: 'var(--surface-3)', border: '1px solid var(--border)', width: '320px', maxWidth: 'min(92vw, 460px)' }}
              >
                {visibleAutoFillItems.map((item) => (
                  <button
                    key={`${item.label}:${item.query}`}
                    onClick={() => { setQuery(item.query); setShowAutofill(false); }}
                    className="block w-full text-left px-3 py-1.5 text-xs transition-colors"
                    style={{ color: 'var(--text-secondary)' }}
                    onMouseOver={e => e.currentTarget.style.background = 'var(--surface-4)'}
                    onMouseOut={e => e.currentTarget.style.background = 'transparent'}
                    title={item.query}
                  >
                    <div className="truncate">{item.label}</div>
                    <div className="mt-0.5 text-2xs font-mono truncate" style={{ color: 'var(--text-tertiary)' }}>
                      {item.query}
                    </div>
                  </button>
                ))}
                {hasMoreAutoFillItems && (
                  <>
                    <div style={{ borderTop:'1px solid var(--border)' }} />
                    <button
                      onClick={() => setAutofillLimit((prev) => prev + 8)}
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

          <div className="relative" ref={historyRef}>
            <button
              onClick={() => {
                setShowHistory(!showHistory);
                setShowTemplates(false);
                setShowAutofill(false);
                setShowExportMenu(false);
                setShowQueryAssist(false);
              }}
              className="btn-ghost text-xs flex items-center gap-1 shrink-0 whitespace-nowrap"
            >
              <History className="w-3.5 h-3.5" />History
              {scopedHistory.length > 0 && <span className="badge-accent text-2xs">{scopedHistory.length}</span>}
            </button>
            {showHistory && (
              <div
                className="absolute left-0 top-full mt-1 z-[80] rounded-lg shadow-xl py-1 max-h-[320px] overflow-y-auto overflow-x-hidden animate-fade-in whitespace-normal"
                style={{ background: 'var(--surface-3)', border: '1px solid var(--border)', width: '360px', maxWidth: 'min(92vw, 560px)' }}
              >
                {scopedHistory.length === 0 && (
                  <div className="px-3 py-2 text-2xs" style={{ color:'var(--text-tertiary)' }}>
                    No history for {db}.{collection}
                  </div>
                )}
                {scopedHistory.slice(0, 20).map((h) => (
                  <button
                    key={h.id || `${h.ts}:${h.query || ''}`}
                    onClick={() => { setQuery(h.query); setShowHistory(false); }}
                    className="w-full text-left px-3 py-2 text-xs transition-colors"
                    style={{ color: 'var(--text-secondary)', borderBottom: '1px solid var(--border)' }}
                    onMouseOver={e => e.currentTarget.style.background = 'var(--surface-4)'}
                    onMouseOut={e => e.currentTarget.style.background = 'transparent'}
                  >
                    <div className="font-mono text-2xs truncate mb-1" style={{ color: 'var(--text-primary)' }}>{h.query}</div>
                    <div className="flex items-center gap-3 text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                      <span>{h.count} results</span>
                      <span>{formatDuration(h.elapsed)}</span>
                      <span>{new Date(h.ts).toLocaleString()}</span>
                      {h.elapsed > 5000 && <span className="badge-yellow">Slow</span>}
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>

          <div className="flex-1" />

          <div className="flex items-center gap-1 shrink-0">
            <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Hint</span>
            <DropdownSelect
              value={selectedHint}
              options={hintOptions}
              onChange={(nextValue) => setSelectedHint(String(nextValue || 'auto'))}
              sizeClassName="text-2xs"
              disabled={!hintEnabled}
              title={hintEnabled ? 'Apply index hint to this query' : 'Hint is available for find/aggregate queries only'}
            />
          </div>

          <span
            className="hidden lg:inline-flex items-center px-2 py-1 rounded-md text-2xs font-mono whitespace-nowrap shrink-0"
            style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-secondary)' }}
            title={`Context: ${db}.${collection}`}
          >
            {db}.{collection}
          </span>

          {running && (
            <span className="flex items-center gap-1.5 text-2xs animate-pulse shrink-0 whitespace-nowrap" style={{ color: 'var(--accent)' }}>
              <Loader className="w-3 h-3" />{(liveTimer / 1000).toFixed(1)}s
            </span>
          )}

          {explainRunning && (
            <span className="flex items-center gap-1.5 text-2xs animate-pulse shrink-0 whitespace-nowrap" style={{ color: 'var(--accent)' }}>
              <Eye className="w-3 h-3" />{(explainLiveTimer / 1000).toFixed(1)}s
            </span>
          )}

          {((elapsed !== null && !running) || (explainElapsed !== null && !explainRunning)) && (
            <div
              className="inline-flex items-center gap-1.5 text-2xs shrink-0 whitespace-nowrap px-2 py-1 rounded-md"
              style={{ color: 'var(--text-tertiary)', background: 'var(--surface-2)', border: '1px solid var(--border)' }}
              title={resultSize && elapsed !== null && !running ? `Result size: ${(resultSize / 1024).toFixed(1)} KB` : undefined}
            >
              {elapsed !== null && !running && (
                <>
                  <span className="inline-flex items-center gap-1">
                    <Zap className="w-3 h-3" style={{ color: 'var(--accent)', opacity: 0.6 }} />
                    Run {formatDuration(elapsed)}
                  </span>
                  {resultSize && (
                    <span className="inline-flex items-center opacity-70">
                      {(resultSize / 1024).toFixed(1)} KB
                    </span>
                  )}
                  {slow && <span className="badge-yellow">Slow</span>}
                </>
              )}
              {elapsed !== null && !running && explainElapsed !== null && !explainRunning && (
                <span className="px-0.5 opacity-35" style={{ color: 'var(--text-tertiary)' }}>•</span>
              )}
              {explainElapsed !== null && !explainRunning && (
                <span className="inline-flex items-center gap-1">
                  <Eye className="w-3 h-3" style={{ color: 'var(--accent)', opacity: 0.6 }} />
                  Explain {formatDuration(explainElapsed)}
                </span>
              )}
            </div>
          )}
        </div>

        <div className="relative">
          <textarea
            ref={textareaRef}
            value={query}
            onChange={handleQueryChange}
            onKeyDown={handleKeyDown}
            rows={Math.min(Math.max(query.split('\n').length, 3), 12)}
            spellCheck={false}
            className="w-full resize-none px-4 py-3 font-mono text-sm leading-relaxed focus:outline-none"
            style={{ background: 'var(--surface-0)', color: 'var(--text-primary)', minHeight: '80px' }}
            placeholder={queryPlaceholder}
          />
          {showQueryAssist && visibleQueryAssistItems.length > 0 && (
            <div
              ref={queryAssistRef}
              className="absolute left-3 right-3 top-full mt-1 z-[70] rounded-lg shadow-xl py-1 max-h-56 overflow-y-auto overflow-x-hidden animate-fade-in"
              style={{ background: 'var(--surface-3)', border: '1px solid var(--border)' }}
            >
              {visibleQueryAssistItems.map((item) => (
                <button
                  key={`${item.label}:${item.value}`}
                  type="button"
                  onClick={() => applyQueryAssistSuggestion(item)}
                  className="block w-full px-3 py-1.5 text-left transition-colors"
                  onMouseOver={(event) => { event.currentTarget.style.background = 'var(--surface-4)'; }}
                  onMouseOut={(event) => { event.currentTarget.style.background = 'transparent'; }}
                >
                  <div className="text-xs truncate" style={{ color: 'var(--text-secondary)' }}>{item.label}</div>
                  <div className="text-2xs font-mono truncate mt-0.5" style={{ color: 'var(--text-tertiary)' }}>{item.value}</div>
                </button>
              ))}
              {hasMoreQueryAssistItems && (
                <>
                  <div style={{ borderTop: '1px solid var(--border)' }} />
                  <button
                    type="button"
                    onClick={() => setQueryAssistLimit((prev) => prev + 6)}
                    className="block w-full text-left px-3 py-1.5 text-2xs transition-colors"
                    style={{ color: 'var(--accent)' }}
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
      </div>

      {showExplain && (explainRunning || explainError || explainResult) && (
        <div className="flex-shrink-0 p-3 animate-slide-up" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-semibold flex items-center gap-2" style={{ color: 'var(--text-primary)' }}>
              <Eye className="w-3.5 h-3.5" style={{ color: 'var(--accent)' }} />Query Plan
            </span>
            <button onClick={() => { setShowExplain(false); setExplainError(null); setExplainResult(null); }} className="p-1"><X className="w-3 h-3" style={{ color: 'var(--text-tertiary)' }} /></button>
          </div>
          {explainError ? (
            <InlineAlert kind="error" message={explainError} className="mx-0 mt-0" />
          ) : explainResult && (
            <>
              {(() => {
                const returnedRaw = Number(explainResult.summary?.nReturned);
                const budgetLimitRaw = Number(explainResult?.budget?.limit);
                const hasReturned = Number.isFinite(returnedRaw);
                const hasBudgetLimit = Number.isFinite(budgetLimitRaw) && budgetLimitRaw > 0;
                const returnedDisplay = hasReturned
                  ? (hasBudgetLimit && returnedRaw > budgetLimitRaw ? budgetLimitRaw : returnedRaw)
                  : null;
                const returnedTrimmed = hasReturned && hasBudgetLimit && returnedRaw > budgetLimitRaw;
                return (
                  <>
              <div className="grid grid-cols-4 gap-2">
                {[
                  { label: 'Docs Examined', value: explainResult.summary?.totalDocsExamined != null ? formatNumber(explainResult.summary.totalDocsExamined) : '-', warn: explainResult.summary?.totalDocsExamined > 10000 },
                  { label: 'Keys Examined', value: explainResult.summary?.totalKeysExamined != null ? formatNumber(explainResult.summary.totalKeysExamined) : '-' },
                  { label: 'Returned', value: returnedDisplay != null ? formatNumber(returnedDisplay) : '-' },
                  { label: 'Time', value: explainResult.summary?.executionTimeMs != null ? formatDuration(explainResult.summary.executionTimeMs) : '-' },
                ].map(({ label, value, warn }) => (
                  <div key={label} className="p-2 rounded-lg" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
                    <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>{label}</div>
                    <div className={`text-sm font-mono font-medium ${warn ? 'text-amber-400' : ''}`} style={warn ? {} : { color: 'var(--text-primary)' }}>{value}</div>
                  </div>
                ))}
              </div>
              <div className="mt-2 flex items-center gap-2">
                {explainResult.summary?.isCollScan ? (
                  <span className="badge-yellow flex items-center gap-1"><AlertTriangle className="w-3 h-3" />Collection scan warning: no index used</span>
                ) : explainResult.summary?.indexUsed ? (
                  <span className="badge-green flex items-center gap-1"><Check className="w-3 h-3" />Index: {explainResult.summary.indexUsed}</span>
                ) : null}
                {explainResult.summary?.isCovered && (
                  <span className="badge-green flex items-center gap-1"><Zap className="w-3 h-3" />Covered (index-only)</span>
                )}
                {returnedTrimmed && (
                  <span className="badge-yellow">Trimmed</span>
                )}
              </div>
                  </>
                );
              })()}
            </>
          )}
        </div>
      )}

      {collscanGuardPending && (
        <div className="px-4 py-2 flex-shrink-0">
          <InlineAlert
            kind="warning"
            message="Collection scan - no index used. Running on a large collection may examine all documents and impact performance."
            actions={[
              { label: 'Run Anyway', onClick: handleConfirmCollscan, primary: true },
              { label: 'Cancel', onClick: handleCancelCollscan },
            ]}
          />
        </div>
      )}
      {!collscanGuardPending && (preRunWarning || error) && (
        <div className="px-4 py-2 space-y-2 flex-shrink-0">
          {preRunWarning && (
            <InlineAlert kind="warning" message={preRunWarning} onClose={() => setPreRunWarning('')} />
          )}
          {error && (
            <InlineAlert kind="error" message={error} onClose={() => setError(null)} />
          )}
        </div>
      )}

      <div className="flex-1 overflow-auto">
        {docs.length > 0 ? (
          <>
            <div className="px-4 py-2 flex items-center justify-between" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-1)' }}>
              <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>
                {formatNumber(docs.length)} result{docs.length !== 1 ? 's' : ''}
                {results?.trimmed && <span className="badge-yellow ml-2">Trimmed</span>}
              </span>
              <div className="flex items-center gap-1.5">
                <button onClick={() => handleToggleAllDocs(true)} className="btn-ghost text-2xs px-2 py-1">
                  Expand all
                </button>
                <button onClick={() => handleToggleAllDocs(false)} className="btn-ghost text-2xs px-2 py-1">
                  Collapse all
                </button>
                <div className="relative" ref={exportMenuRef}>
                  <button onClick={() => setShowExportMenu((prev) => !prev)} className="btn-ghost flex items-center gap-1 text-xs">
                    <Download className="w-3 h-3" />Export
                  </button>
                  {showExportMenu && (
                    <div
                      className="absolute right-0 top-full mt-1 z-[80] rounded-lg shadow-xl py-1 min-w-[132px] animate-fade-in"
                      style={{ background: 'var(--surface-3)', border: '1px solid var(--border)' }}
                    >
                      <button
                        onClick={() => handleExportResults('json')}
                        className="block w-full text-left px-3 py-1.5 text-xs transition-colors"
                        style={{ color: 'var(--text-secondary)' }}
                        onMouseOver={e => e.currentTarget.style.background = 'var(--surface-4)'}
                        onMouseOut={e => e.currentTarget.style.background = 'transparent'}
                      >
                        Export JSON
                      </button>
                      <button
                        onClick={() => handleExportResults('csv')}
                        className="block w-full text-left px-3 py-1.5 text-xs transition-colors"
                        style={{ color: 'var(--text-secondary)' }}
                        onMouseOver={e => e.currentTarget.style.background = 'var(--surface-4)'}
                        onMouseOut={e => e.currentTarget.style.background = 'transparent'}
                      >
                        Export CSV
                      </button>
                    </div>
                  )}
                </div>
                <button onClick={handleCopy} className="btn-ghost flex items-center gap-1 text-xs">
                  {copied ? <><Check className="w-3 h-3" style={{ color: 'var(--accent)' }} />Copied</> : <><Copy className="w-3 h-3" />Copy</>}
                </button>
              </div>
            </div>
            <div className="p-4 space-y-2">
              {docs.map((doc, i) => (
                <div key={i} className="rounded-xl p-4 overflow-auto" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
                  <JsonView
                    data={doc}
                    showControls
                    externalToggle={externalToggleSignal}
                  />
                </div>
              ))}
            </div>
          </>
        ) : results && docs.length === 0 ? (
          <div className="flex items-center justify-center h-full" style={{ color: 'var(--text-tertiary)' }}>
            <div className="text-center"><div className="text-sm">No results</div><div className="text-2xs mt-1">Query returned 0 documents</div></div>
          </div>
        ) : !running && (
          <div className="flex items-center justify-center h-full" style={{ color: 'var(--text-tertiary)' }}>
            <div className="text-center"><Play className="w-8 h-8 mx-auto mb-2 opacity-20" /><div className="text-sm">Run a query to see results</div></div>
          </div>
        )}
      </div>
      <ConfirmDialog
        open={Boolean(confirmDialog)}
        title={confirmDialog?.title || 'Confirm action'}
        message={confirmDialog?.message || 'Continue?'}
        confirmLabel={confirmDialog?.confirmLabel || 'Continue'}
        onCancel={() => setConfirmDialog(null)}
        onConfirm={() => {
          const action = confirmDialog?.onConfirm;
          setConfirmDialog(null);
          action?.();
        }}
      />
    </div>
  );
}


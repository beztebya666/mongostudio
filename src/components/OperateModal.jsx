import React, { useEffect, useMemo, useRef, useState } from 'react';
import api from '../utils/api';
import AppModal from './modals/AppModal';
import ConfirmDialog from './modals/ConfirmDialog';
import ConsoleView from './ConsoleView';
import DropdownSelect from './DropdownSelect';
import {
  X,
  Lock,
  Loader,
  Plus,
  Trash,
  Edit,
  Zap,
  History,
  Settings,
  Terminal,
} from './Icons';
import { formatNumber, formatDuration } from '../utils/formatters';

const SECTION_TABS = [
  { id: 'operate', label: 'Operate', icon: Zap },
  { id: 'others', label: 'Others', icon: Settings },
  { id: 'console', label: 'Console', icon: Terminal },
  { id: 'legacy', label: 'Legacy', icon: History },
];

const OPERATE_TABS = [
  { id: 'insert', label: 'Insert', icon: Plus },
  { id: 'delete', label: 'Delete', icon: Trash },
  { id: 'update', label: 'Update', icon: Edit },
  { id: 'bulk', label: 'Bulk', icon: Zap },
];

const OTHERS_TABS = [
  { id: 'collection', label: 'Collection' },
  { id: 'indexes', label: 'Indexes' },
  { id: 'diagnostics', label: 'Diagnostics' },
];

const LEGACY_GROUPS = [
  {
    title: 'Implemented',
    kind: 'ok',
    items: [
      'aggregate(), find()/findOne(), explain(), stats()',
      'countDocuments()/estimatedDocumentCount(), distinct()',
      'insertOne()/insertMany(), deleteOne()/deleteMany()',
      'createIndex()/dropIndex()/getIndexes()',
      'replaceOne(), updateOne()/updateMany(), bulkWrite()',
      'findOneAndUpdate/Delete/Replace(), findAndModify()',
    ],
  },
  {
    title: 'Deprecated',
    kind: 'warn',
    items: [
      'count(), insert(), remove(), update(), ensureIndex()',
      'Kept only for compatibility in old scripts',
    ],
  },
  {
    title: 'Gaps / Niche',
    kind: 'gap',
    items: [
      'watch(), mapReduce(), isCapped(), latencyStats()',
      'sharding-only admin methods',
      'Available via external mongosh/driver workflows',
    ],
  },
];

const INSERT_TEMPLATE_SINGLE = '{\n  "title": "example",\n  "active": true\n}';
const INSERT_TEMPLATE_MANY = '[\n  {\n    "title": "example-1",\n    "active": true\n  },\n  {\n    "title": "example-2",\n    "active": false\n  }\n]';
const FILTER_TEMPLATE = '{\n  "_id": { "$oid": "" }\n}';
const UPDATE_TEMPLATE = '{\n  "$set": {\n    "updatedAt": { "$date": "2026-01-01T00:00:00.000Z" }\n  }\n}';
const REPLACEMENT_TEMPLATE = '{\n  "title": "replacement",\n  "active": true,\n  "tags": ["demo"]\n}';
const BULK_TEMPLATE = '[\n  {\n    "insertOne": {\n      "document": { "title": "bulk-item", "active": true }\n    }\n  },\n  {\n    "updateOne": {\n      "filter": { "title": "bulk-item" },\n      "update": { "$set": { "active": false } },\n      "upsert": false\n    }\n  },\n  {\n    "deleteOne": {\n      "filter": { "title": "obsolete" }\n    }\n  }\n]';

const INSERT_METHODS = [
  { value: 'auto', label: 'Auto' },
  { value: 'insertOne', label: 'insertOne' },
  { value: 'insertMany', label: 'insertMany' },
];

const DELETE_METHODS = [
  { value: 'deleteOne', label: 'deleteOne' },
  { value: 'deleteMany', label: 'deleteMany' },
  { value: 'findOneAndDelete', label: 'findOneAndDelete' },
  { value: 'remove', label: 'remove (legacy)' },
];

const UPDATE_METHODS = [
  { value: 'replaceOne', label: 'replaceOne' },
  { value: 'updateOne', label: 'updateOne' },
  { value: 'updateMany', label: 'updateMany' },
  { value: 'findOneAndUpdate', label: 'findOneAndUpdate' },
  { value: 'findOneAndReplace', label: 'findOneAndReplace' },
  { value: 'findAndModify', label: 'findAndModify (legacy)' },
];

const FIND_AND_MODIFY_MODES = [
  { value: 'update', label: 'update' },
  { value: 'replace', label: 'replace' },
  { value: 'delete', label: 'delete' },
];

const INSERT_PRESETS = [
  { id: 'single', label: 'Single doc', method: 'insertOne', value: INSERT_TEMPLATE_SINGLE },
  { id: 'many', label: 'Many docs', method: 'insertMany', value: INSERT_TEMPLATE_MANY },
  {
    id: 'typed',
    label: 'Typed doc',
    method: 'insertOne',
    value: '{\n  "title": "typed",\n  "count": 42,\n  "active": true,\n  "createdAt": { "$date": "2026-01-01T00:00:00.000Z" },\n  "ownerId": { "$oid": "000000000000000000000001" },\n  "tags": ["ui", "mongo"],\n  "meta": { "nested": true }\n}',
  },
  {
    id: 'array',
    label: 'Array heavy',
    method: 'insertOne',
    value: '{\n  "title": "array-heavy",\n  "values": [1,2,3,4,5],\n  "objects": [{ "i": 1 }, { "i": 2 }],\n  "flags": [true, false, null]\n}',
  },
];
const DELETE_PRESETS = [
  { id: 'id', label: 'By _id', method: 'deleteOne', filter: FILTER_TEMPLATE },
  { id: 'many', label: 'DeleteMany status', method: 'deleteMany', filter: '{\n  "status": "archived"\n}' },
  {
    id: 'find-delete',
    label: 'findOneAndDelete latest',
    method: 'findOneAndDelete',
    filter: '{\n  "status": "queued"\n}',
    sort: '{\n  "createdAt": -1\n}',
  },
  {
    id: 'legacy-remove',
    label: 'remove many',
    method: 'remove',
    filter: '{\n  "legacy": true\n}',
    justOne: false,
  },
];

const UPDATE_PRESETS = [
  {
    id: 'replace',
    label: 'Replacement',
    method: 'replaceOne',
    filter: FILTER_TEMPLATE,
    payload: REPLACEMENT_TEMPLATE,
  },
  {
    id: 'set',
    label: '$set one',
    method: 'updateOne',
    filter: '{\n  "status": "active"\n}',
    payload: UPDATE_TEMPLATE,
  },
  {
    id: 'inc-many',
    label: '$inc many',
    method: 'updateMany',
    filter: '{\n  "counter": { "$exists": true }\n}',
    payload: '{\n  "$inc": {\n    "counter": 1\n  }\n}',
  },
  {
    id: 'pipeline',
    label: 'Pipeline',
    method: 'updateMany',
    filter: '{\n  "score": { "$exists": true }\n}',
    payload: '[\n  {\n    "$set": {\n      "score": { "$add": ["$score", 1] }\n    }\n  }\n]',
  },
  {
    id: 'fam',
    label: 'findAndModify',
    method: 'findAndModify',
    mode: 'update',
    filter: '{\n  "status": "queued"\n}',
    sort: '{\n  "createdAt": 1\n}',
    payload: '{\n  "$set": {\n    "status": "processed"\n  }\n}',
  },
  {
    id: 'fa-replace',
    label: 'findOneAndReplace',
    method: 'findOneAndReplace',
    filter: '{\n  "kind": "profile"\n}',
    payload: '{\n  "kind": "profile",\n  "name": "updated",\n  "active": true\n}',
  },
];

const BULK_PRESETS = [
  { id: 'default', label: 'Mixed write', value: BULK_TEMPLATE },
  {
    id: 'upsert',
    label: 'Upsert batch',
    value: '[\n  {\n    "updateOne": {\n      "filter": { "key": "alpha" },\n      "update": { "$set": { "v": 1 } },\n      "upsert": true\n    }\n  },\n  {\n    "updateOne": {\n      "filter": { "key": "beta" },\n      "update": { "$set": { "v": 2 } },\n      "upsert": true\n    }\n  }\n]',
  },
  {
    id: 'delete-heavy',
    label: 'Delete batch',
    value: '[\n  {\n    "deleteMany": {\n      "filter": { "status": "obsolete" }\n    }\n  },\n  {\n    "deleteOne": {\n      "filter": { "priority": "low" }\n    }\n  }\n]',
  },
  {
    id: 'replace-chain',
    label: 'Replace chain',
    value: '[\n  {\n    "replaceOne": {\n      "filter": { "_id": { "$oid": "000000000000000000000001" } },\n      "replacement": { "title": "A", "active": true },\n      "upsert": true\n    }\n  },\n  {\n    "replaceOne": {\n      "filter": { "_id": { "$oid": "000000000000000000000002" } },\n      "replacement": { "title": "B", "active": false },\n      "upsert": true\n    }\n  }\n]',
  },
];

const DEFAULT_QUERY_TIMEOUT_MS = 5000;
const SAFE_QUERY_TIMEOUT_MAX_MS = 30000;
const POWER_QUERY_TIMEOUT_PRESET_MAX_MS = 120000;
const POWER_QUERY_TIMEOUT_MAX_MS = 2147483000;
const DEFAULT_QUERY_LIMIT = 50;
const SAFE_QUERY_LIMIT_MAX = 1000;
const POWER_QUERY_LIMIT_PRESET_MAX = 5000;
const QUERY_LIMIT_OVERRIDE_MAX = 50000;
const POWER_QUERY_LIMIT_MAX = 2147483000;
const QUERY_TIMEOUT_OPTIONS = [5000, 10000, 30000, 60000, POWER_QUERY_TIMEOUT_PRESET_MAX_MS];
const QUERY_LIMIT_OPTIONS = [50, 100, 200, 500, 1000, POWER_QUERY_LIMIT_PRESET_MAX, POWER_QUERY_LIMIT_MAX];
const getModeTimeoutMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_TIMEOUT_MAX_MS : SAFE_QUERY_TIMEOUT_MAX_MS);
const getModeLimitMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_LIMIT_MAX : QUERY_LIMIT_OVERRIDE_MAX);
const clampTimeoutMs = (value, mode = 'safe') => Math.max(
  DEFAULT_QUERY_TIMEOUT_MS,
  Math.min(Number(value) || DEFAULT_QUERY_TIMEOUT_MS, getModeTimeoutMax(mode)),
);
const clampLimitValue = (value, mode = 'safe') => Math.max(
  DEFAULT_QUERY_LIMIT,
  Math.min(Number(value) || DEFAULT_QUERY_LIMIT, getModeLimitMax(mode)),
);
const formatTimeoutLabel = (value) => `${Math.max(1, Math.round(Number(value) / 1000))}s`;
const formatLimitLabel = (value, mode = 'safe') => (
  mode === 'power' && Number(value) >= POWER_QUERY_LIMIT_MAX
    ? 'Unlimited'
    : formatNumber(Number(value) || DEFAULT_QUERY_LIMIT)
);
const buildBudget = (input = {}, mode = 'safe') => ({
  timeoutMs: clampTimeoutMs(input?.timeoutMs, mode),
  limit: clampLimitValue(input?.limit, mode),
});

function parseJsonText(raw, fieldName, { allowArray = true, allowObject = true, allowEmpty = false } = {}) {
  const text = String(raw || '').trim();
  if (!text) {
    if (allowEmpty) return null;
    throw new Error(`${fieldName} is required.`);
  }
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new Error(`Invalid ${fieldName} JSON: ${err.message}`);
  }
  if (Array.isArray(parsed)) {
    if (!allowArray) throw new Error(`${fieldName} must be an object.`);
    return parsed;
  }
  if (parsed && typeof parsed === 'object') {
    if (!allowObject) throw new Error(`${fieldName} must be an array.`);
    return parsed;
  }
  throw new Error(`${fieldName} must be ${allowArray && allowObject ? 'an object or array' : allowArray ? 'an array' : 'an object'}.`);
}

function parseOptionalJsonObject(raw, fieldName) {
  const text = String(raw || '').trim();
  if (!text) return undefined;
  return parseJsonText(text, fieldName, { allowArray: false, allowObject: true, allowEmpty: false });
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function summarizeResult(method, result = {}) {
  if (!result || typeof result !== 'object') return method;
  if (Number.isFinite(Number(result.insertedCount))) return `${method}: inserted ${formatNumber(Number(result.insertedCount))}`;
  if (Number.isFinite(Number(result.deletedCount))) return `${method}: deleted ${formatNumber(Number(result.deletedCount))}`;
  if (Object.prototype.hasOwnProperty.call(result, 'value') && Number.isFinite(Number(result.value))) {
    return `${method}: value ${formatNumber(Number(result.value))}`;
  }
  if (Number.isFinite(Number(result.modifiedCount)) || Number.isFinite(Number(result.matchedCount))) {
    return `${method}: matched ${formatNumber(Number(result.matchedCount || 0))}, modified ${formatNumber(Number(result.modifiedCount || 0))}`;
  }
  if (Object.prototype.hasOwnProperty.call(result, 'value')) {
    return `${method}: ${result.value ? 'value returned' : 'no matching document'}`;
  }
  return `${method}: completed`;
}

function cardTone(kind) {
  if (kind === 'ok') {
    return {
      border: '1px solid rgba(52,211,153,0.25)',
      background: 'rgba(16,185,129,0.06)',
      label: '#34d399',
    };
  }
  if (kind === 'warn') {
    return {
      border: '1px solid rgba(245,158,11,0.25)',
      background: 'rgba(245,158,11,0.07)',
      label: '#fbbf24',
    };
  }
  return {
    border: '1px solid rgba(239,68,68,0.25)',
    background: 'rgba(239,68,68,0.06)',
    label: '#fca5a5',
  };
}
export default function OperateModal({
  open,
  db,
  collection,
  isProduction = false,
  executionMode = 'safe',
  budget = null,
  initialTab = 'insert',
  onClose,
  onApplied,
}) {
  const mode = executionMode === 'power' ? 'power' : 'safe';
  const [activeSection, setActiveSection] = useState('operate');
  const [activeOperateTab, setActiveOperateTab] = useState(initialTab);
  const [activeOthersTab, setActiveOthersTab] = useState('collection');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [lastRunElapsedMs, setLastRunElapsedMs] = useState(null);
  const [lastResult, setLastResult] = useState(null);
  const [confirmDialog, setConfirmDialog] = useState(null);
  const [phraseDialog, setPhraseDialog] = useState(null);
  const [phraseInput, setPhraseInput] = useState('');

  const [insertMethod, setInsertMethod] = useState('auto');
  const [insertOrdered, setInsertOrdered] = useState(false);
  const [insertPayload, setInsertPayload] = useState(INSERT_TEMPLATE_SINGLE);

  const [deleteMethod, setDeleteMethod] = useState('deleteOne');
  const [deleteFilter, setDeleteFilter] = useState(FILTER_TEMPLATE);
  const [deleteSort, setDeleteSort] = useState('{\n  "_id": -1\n}');
  const [deleteProjection, setDeleteProjection] = useState('');
  const [deleteHint, setDeleteHint] = useState('');
  const [deleteCollation, setDeleteCollation] = useState('');
  const [deleteJustOne, setDeleteJustOne] = useState(false);

  const [updateMethod, setUpdateMethod] = useState('replaceOne');
  const [updateFindAndModifyMode, setUpdateFindAndModifyMode] = useState('update');
  const [updateFilter, setUpdateFilter] = useState(FILTER_TEMPLATE);
  const [updatePayload, setUpdatePayload] = useState(REPLACEMENT_TEMPLATE);
  const [updateSort, setUpdateSort] = useState('{\n  "_id": -1\n}');
  const [updateProjection, setUpdateProjection] = useState('');
  const [updateHint, setUpdateHint] = useState('');
  const [updateCollation, setUpdateCollation] = useState('');
  const [updateArrayFilters, setUpdateArrayFilters] = useState('');
  const [updateUpsert, setUpdateUpsert] = useState(false);
  const [updateReturnDocument, setUpdateReturnDocument] = useState('before');

  const [bulkOperations, setBulkOperations] = useState(BULK_TEMPLATE);
  const [bulkOrdered, setBulkOrdered] = useState(true);
  const [bulkBypassValidation, setBulkBypassValidation] = useState(false);

  const [otherCountFilter, setOtherCountFilter] = useState('{}');
  const [otherRenameTo, setOtherRenameTo] = useState('');
  const [otherRenameDropTarget, setOtherRenameDropTarget] = useState(false);
  const [otherIndexName, setOtherIndexName] = useState('');
  const [otherDropIndexes, setOtherDropIndexes] = useState('*');
  const [otherValidateFull, setOtherValidateFull] = useState(false);
  const [operateBudget, setOperateBudget] = useState(() => buildBudget(budget, mode));
  const [showCustomTimeoutInput, setShowCustomTimeoutInput] = useState(false);
  const [customTimeoutSeconds, setCustomTimeoutSeconds] = useState('');
  const [showCustomLimitInput, setShowCustomLimitInput] = useState(false);
  const [customLimitValue, setCustomLimitValue] = useState('');

  const operationAbortRef = useRef(null);
  const confirmResolverRef = useRef(null);
  const phraseResolverRef = useRef(null);

  const isDeleteRemoveMethod = deleteMethod === 'remove';
  const isDeleteManyMode = deleteMethod === 'deleteMany' || (isDeleteRemoveMethod && !deleteJustOne);
  const showDeleteSort = deleteMethod === 'findOneAndDelete';
  const showDeleteProjection = deleteMethod === 'findOneAndDelete';
  const isFindAndModify = updateMethod === 'findAndModify';
  const findAndModifyIsDelete = isFindAndModify && updateFindAndModifyMode === 'delete';
  const updateNeedsReplacement = (
    updateMethod === 'replaceOne'
    || updateMethod === 'findOneAndReplace'
    || (isFindAndModify && updateFindAndModifyMode === 'replace')
  );
  const updateNeedsPayload = !findAndModifyIsDelete;
  const updateNeedsSort = updateMethod === 'findOneAndUpdate' || updateMethod === 'findOneAndReplace' || isFindAndModify;
  const updateNeedsProjection = updateMethod === 'findOneAndUpdate' || updateMethod === 'findOneAndReplace' || isFindAndModify;
  const updateNeedsReturnDocument = (
    updateMethod === 'findOneAndUpdate'
    || updateMethod === 'findOneAndReplace'
    || (isFindAndModify && !findAndModifyIsDelete)
  );
  const updateNeedsArrayFilters = (
    updateMethod === 'updateOne'
    || updateMethod === 'updateMany'
    || updateMethod === 'findOneAndUpdate'
    || (isFindAndModify && updateFindAndModifyMode === 'update')
  );
  const updateAllowsUpsert = !findAndModifyIsDelete;

  const contextText = useMemo(() => `${db || '-'} / ${collection || '-'}`, [db, collection]);
  const consoleScope = useMemo(() => ({ level: 'collection', db, collection }), [db, collection]);
  const timeoutBudgetValue = clampTimeoutMs(operateBudget.timeoutMs, mode);
  const limitBudgetValue = clampLimitValue(operateBudget.limit, mode);
  const timeoutCustomPresent = !QUERY_TIMEOUT_OPTIONS.includes(timeoutBudgetValue);
  const limitCustomPresent = !QUERY_LIMIT_OPTIONS.includes(limitBudgetValue);
  const timeoutSelectOptions = useMemo(() => {
    const options = QUERY_TIMEOUT_OPTIONS.map((value) => ({
      value: String(value),
      label: formatTimeoutLabel(value),
      disabled: mode !== 'power' && value > SAFE_QUERY_TIMEOUT_MAX_MS,
    }));
    if (timeoutCustomPresent) {
      options.push({
        value: String(timeoutBudgetValue),
        label: `${formatTimeoutLabel(timeoutBudgetValue)} (Custom)`,
      });
    }
    options.push({ value: '__custom__', label: 'Custom' });
    return options;
  }, [mode, timeoutCustomPresent, timeoutBudgetValue]);
  const limitSelectOptions = useMemo(() => {
    const options = QUERY_LIMIT_OPTIONS.map((value) => ({
      value: String(value),
      label: formatLimitLabel(value, mode),
      disabled: mode !== 'power' && value > SAFE_QUERY_LIMIT_MAX,
    }));
    if (limitCustomPresent) {
      options.push({
        value: String(limitBudgetValue),
        label: `${formatLimitLabel(limitBudgetValue, mode)} (Custom)`,
      });
    }
    options.push({ value: '__custom__', label: 'Custom' });
    return options;
  }, [mode, limitCustomPresent, limitBudgetValue]);
  const timeoutSelectValue = showCustomTimeoutInput ? '__custom__' : String(timeoutBudgetValue);
  const limitSelectValue = showCustomLimitInput ? '__custom__' : String(limitBudgetValue);

  const applyCustomTimeout = () => {
    const seconds = Number(customTimeoutSeconds !== '' ? customTimeoutSeconds : Math.max(5, Math.round(timeoutBudgetValue / 1000)));
    if (!Number.isFinite(seconds)) return;
    const nextTimeoutMs = Math.round(seconds * 1000);
    const normalized = clampTimeoutMs(nextTimeoutMs, mode);
    setOperateBudget((prev) => ({ ...prev, timeoutMs: normalized }));
    setCustomTimeoutSeconds(String(Math.max(5, Math.round(normalized / 1000))));
    setShowCustomTimeoutInput(false);
  };

  const applyCustomLimit = () => {
    const value = Number(customLimitValue !== '' ? customLimitValue : Math.max(DEFAULT_QUERY_LIMIT, Math.round(limitBudgetValue)));
    if (!Number.isFinite(value)) return;
    const normalized = clampLimitValue(Math.round(value), mode);
    setOperateBudget((prev) => ({ ...prev, limit: normalized }));
    setCustomLimitValue(String(Math.max(DEFAULT_QUERY_LIMIT, Math.round(normalized))));
    setShowCustomLimitInput(false);
  };

  const handleTimeoutSelectChange = (nextValue) => {
    const raw = String(nextValue || '');
    if (raw === '__custom__') {
      setShowCustomTimeoutInput(true);
      setCustomTimeoutSeconds(String(Math.max(5, Math.round(timeoutBudgetValue / 1000))));
      return;
    }
    const numeric = Number(raw);
    if (!Number.isFinite(numeric)) return;
    setShowCustomTimeoutInput(false);
    setCustomTimeoutSeconds('');
    setOperateBudget((prev) => ({
      ...prev,
      timeoutMs: clampTimeoutMs(numeric, mode),
    }));
  };

  const handleLimitSelectChange = (nextValue) => {
    const raw = String(nextValue || '');
    if (raw === '__custom__') {
      setShowCustomLimitInput(true);
      setCustomLimitValue(String(Math.max(DEFAULT_QUERY_LIMIT, Math.round(limitBudgetValue))));
      return;
    }
    const numeric = Number(raw);
    if (!Number.isFinite(numeric)) return;
    setShowCustomLimitInput(false);
    setCustomLimitValue('');
    setOperateBudget((prev) => ({
      ...prev,
      limit: clampLimitValue(numeric, mode),
    }));
  };

  const abortCurrentOperation = () => {
    if (!operationAbortRef.current) return false;
    try { operationAbortRef.current.abort(); } catch {}
    operationAbortRef.current = null;
    return true;
  };

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

  const requestPhraseConfirmation = async ({
    title = 'Production confirmation',
    message = '',
    phrase = '',
    confirmLabel = 'Confirm',
  } = {}) => (
    new Promise((resolve) => {
      phraseResolverRef.current = resolve;
      setPhraseInput('');
      setPhraseDialog({ title, message, phrase, confirmLabel });
    })
  );

  const closePhraseDialog = (approved) => {
    const resolver = phraseResolverRef.current;
    phraseResolverRef.current = null;
    const expected = String(phraseDialog?.phrase || '');
    const typed = String(phraseInput || '');
    setPhraseDialog(null);
    setPhraseInput('');
    if (!resolver) return;
    if (!approved) {
      resolver(false);
      return;
    }
    resolver(typed === expected);
  };

  useEffect(() => {
    if (!open) return;
    abortCurrentOperation();
    if (confirmResolverRef.current) {
      try { confirmResolverRef.current(false); } catch {}
      confirmResolverRef.current = null;
    }
    if (phraseResolverRef.current) {
      try { phraseResolverRef.current(false); } catch {}
      phraseResolverRef.current = null;
    }
    setConfirmDialog(null);
    setPhraseDialog(null);
    setPhraseInput('');
    setActiveSection('operate');
    setActiveOperateTab(OPERATE_TABS.some((tab) => tab.id === initialTab) ? initialTab : 'insert');
    setActiveOthersTab('collection');
    setBusy(false);
    setError('');
    setSuccess('');
    setLastRunElapsedMs(null);
    setLastResult(null);
    setOperateBudget(buildBudget(budget, mode));
    setShowCustomTimeoutInput(false);
    setCustomTimeoutSeconds('');
    setShowCustomLimitInput(false);
    setCustomLimitValue('');
  }, [open, initialTab, db, collection, budget, mode]);

  useEffect(() => {
    if (open) return;
    abortCurrentOperation();
    if (confirmResolverRef.current) {
      try { confirmResolverRef.current(false); } catch {}
      confirmResolverRef.current = null;
    }
    if (phraseResolverRef.current) {
      try { phraseResolverRef.current(false); } catch {}
      phraseResolverRef.current = null;
    }
    setConfirmDialog(null);
    setPhraseDialog(null);
    setPhraseInput('');
    setBusy(false);
  }, [open]);

  useEffect(() => () => {
    abortCurrentOperation();
    if (confirmResolverRef.current) {
      try { confirmResolverRef.current(false); } catch {}
      confirmResolverRef.current = null;
    }
    if (phraseResolverRef.current) {
      try { phraseResolverRef.current(false); } catch {}
      phraseResolverRef.current = null;
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    if (insertMethod === 'insertMany' && !String(insertPayload || '').trim().startsWith('[')) {
      setInsertPayload(INSERT_TEMPLATE_MANY);
    }
    if (insertMethod === 'insertOne' && String(insertPayload || '').trim().startsWith('[')) {
      setInsertPayload(INSERT_TEMPLATE_SINGLE);
    }
  }, [insertMethod, open, insertPayload]);

  useEffect(() => {
    if (!open) return;
    if (!updateNeedsPayload) return;
    if (updateNeedsReplacement && String(updatePayload || '').trim().startsWith('{\n  "$')) {
      setUpdatePayload(REPLACEMENT_TEMPLATE);
    }
    if (!updateNeedsReplacement && !String(updatePayload || '').trim().startsWith('{\n  "$')) {
      setUpdatePayload(UPDATE_TEMPLATE);
    }
  }, [updateMethod, updateNeedsPayload, updateNeedsReplacement, updatePayload, open]);

  const runOperation = async (method, payload, requestOptions = {}) => {
    const controller = new AbortController();
    operationAbortRef.current = controller;
    setBusy(true);
    setError('');
    setSuccess('');
    setLastRunElapsedMs(null);
    const startedAt = performance.now();
    try {
      const response = await api.operateCollection(db, collection, method, payload, {
        ...requestOptions,
        budget: requestOptions?.budget || operateBudget,
        controller,
      });
      const elapsedMs = Math.round(performance.now() - startedAt);
      setLastRunElapsedMs(elapsedMs);
      setLastResult(response?.result || null);
      setSuccess(`${summarizeResult(method, response?.result || {})} in ${formatDuration(elapsedMs)}`);
      onApplied?.(response);
    } catch (err) {
      if (err?.name === 'AbortError') {
        setSuccess('Operation cancelled.');
        return;
      }
      setError(err.message || 'Operation failed.');
    } finally {
      if (operationAbortRef.current === controller) operationAbortRef.current = null;
      setBusy(false);
    }
  };

  const summarizePreflight = (preflight, fallback = '') => {
    if (!preflight || typeof preflight !== 'object') return fallback || '';
    const lines = [];
    if (preflight.risk) lines.push(`Risk: ${preflight.risk}`);
    if (Number.isFinite(Number(preflight.estimate))) lines.push(`Estimate: ${formatNumber(Number(preflight.estimate))}`);
    if (Number.isFinite(Number(preflight.riskScore))) lines.push(`Risk score: ${Math.round(Number(preflight.riskScore))}`);
    if (preflight?.updateSummary?.kind) {
      const kind = preflight.updateSummary.kind;
      if (kind === 'operator') {
        lines.push(`Update operators: ${(preflight.updateSummary.operators || []).join(', ') || 'unknown'}`);
      } else if (kind === 'pipeline') {
        lines.push(`Update pipeline stages: ${(preflight.updateSummary.stages || []).join(', ') || preflight.updateSummary.stageCount || 0}`);
      } else if (kind === 'replacement') {
        lines.push(`Replacement fields: ${Number(preflight.updateSummary.fieldCount || 0)}`);
      }
    }
    if (preflight?.bulkSummary) {
      lines.push(`Bulk ops: ${formatNumber(Number(preflight.bulkSummary.total || 0))}`);
      if (Number(preflight.bulkSummary.deleteMany || 0) > 0) lines.push(`deleteMany ops: ${formatNumber(Number(preflight.bulkSummary.deleteMany || 0))}`);
      if (Number(preflight.bulkSummary.updateMany || 0) > 0) lines.push(`updateMany ops: ${formatNumber(Number(preflight.bulkSummary.updateMany || 0))}`);
      if (Number.isFinite(Number(preflight.bulkSummary.estimatedAffected))) {
        lines.push(`Estimated affected docs: ${formatNumber(Number(preflight.bulkSummary.estimatedAffected || 0))}`);
      }
    }
    const warningLines = Array.isArray(preflight.warnings) ? preflight.warnings.filter(Boolean).slice(0, 5) : [];
    if (warningLines.length > 0) {
      lines.push('', 'Warnings:');
      warningLines.forEach((warning) => lines.push(`- ${warning}`));
    }
    return lines.join('\n').trim();
  };

  const confirmWithProductionPhrase = async (title, details = '') => {
    const message = [title, details].filter(Boolean).join('\n\n');
    const approved = await requestConfirm({
      title,
      message: `${message}\n\nContinue?`,
      confirmLabel: 'Continue',
      cancelLabel: 'Cancel',
      danger: true,
    });
    if (!approved) return false;
    if (!isProduction) return true;
    const phrase = `${db}.${collection}`;
    const typedOk = await requestPhraseConfirmation({
      title: 'Production guard enabled',
      message: `Type "${phrase}" to confirm this write operation.`,
      phrase,
      confirmLabel: 'Confirm write',
    });
    if (!typedOk) {
      setError('Confirmation phrase mismatch. Operation cancelled.');
      return false;
    }
    return true;
  };

  const runPreflight = async (operation, payload = {}) => {
    try {
      return await api.preflight(db, collection, { operation, ...payload }, { budget: operateBudget });
    } catch {
      return null;
    }
  };

  const handleInsert = async () => {
    const parsed = parseJsonText(insertPayload, 'Insert payload', { allowArray: true, allowObject: true });
    const method = insertMethod === 'auto'
      ? (Array.isArray(parsed) ? 'insertMany' : 'insertOne')
      : insertMethod;
    if (method === 'insertOne' && Array.isArray(parsed)) throw new Error('insertOne expects an object payload.');
    if (method === 'insertMany' && !Array.isArray(parsed)) throw new Error('insertMany expects an array payload.');
    if (method === 'insertMany' && parsed.length === 0) throw new Error('insertMany payload must not be empty.');
    const payload = method === 'insertMany'
      ? { documents: parsed, ordered: insertOrdered }
      : { document: parsed };
    await runOperation(method, payload, { heavyConfirm: method === 'insertMany' });
  };

  const handleDelete = async () => {
    const filter = parseJsonText(deleteFilter, 'Filter', { allowArray: false, allowObject: true });
    const payload = { filter };
    if (isDeleteRemoveMethod) payload.justOne = deleteJustOne;
    if (deleteHint.trim()) payload.hint = deleteHint.trim();
    const collation = parseOptionalJsonObject(deleteCollation, 'Collation');
    if (collation) payload.collation = collation;
    if (showDeleteSort) payload.sort = parseJsonText(deleteSort, 'Sort', { allowArray: false, allowObject: true });
    if (showDeleteProjection) {
      const projection = parseOptionalJsonObject(deleteProjection, 'Projection');
      if (projection) payload.projection = projection;
    }
    if (isDeleteManyMode) {
      const deleteLabel = deleteMethod === 'remove' ? 'remove (many)' : 'deleteMany';
      const preflight = await runPreflight('deleteMany', {
        filter: JSON.stringify(filter),
      });
      const details = summarizePreflight(preflight, `Operation: ${deleteLabel}`);
      const confirmed = await confirmWithProductionPhrase(`Run ${deleteLabel} on ${db}.${collection}`, details);
      if (!confirmed) return;
    }
    await runOperation(deleteMethod, payload, { heavyConfirm: isDeleteManyMode });
  };
  const handleUpdate = async () => {
    const filter = parseJsonText(updateFilter, 'Filter', { allowArray: false, allowObject: true });
    const payload = isFindAndModify ? { query: filter } : { filter };
    if (isFindAndModify) payload.remove = findAndModifyIsDelete;
    if (updateAllowsUpsert) payload.upsert = Boolean(updateUpsert);
    if (updateHint.trim()) payload.hint = updateHint.trim();
    const collation = parseOptionalJsonObject(updateCollation, 'Collation');
    if (collation) payload.collation = collation;
    if (updateNeedsSort) payload.sort = parseJsonText(updateSort, 'Sort', { allowArray: false, allowObject: true });
    if (updateNeedsProjection) {
      const projection = parseOptionalJsonObject(updateProjection, 'Projection');
      if (projection) {
        if (isFindAndModify) payload.fields = projection;
        else payload.projection = projection;
      }
    }
    if (updateNeedsReturnDocument) {
      payload.returnDocument = updateReturnDocument;
      if (isFindAndModify) payload.new = updateReturnDocument === 'after';
    }
    if (updateNeedsArrayFilters) {
      const parsedArrayFilters = String(updateArrayFilters || '').trim()
        ? parseJsonText(updateArrayFilters, 'Array Filters', { allowArray: true, allowObject: false })
        : null;
      if (parsedArrayFilters) payload.arrayFilters = parsedArrayFilters;
    }
    if (updateNeedsPayload) {
      if (updateNeedsReplacement) {
        const replacement = parseJsonText(updatePayload, 'Replacement', { allowArray: false, allowObject: true });
        const replacementKeys = Object.keys(replacement || {});
        if (replacementKeys.some((key) => key.startsWith('$'))) throw new Error('Replacement must not contain update operators.');
        if (isFindAndModify) payload.update = replacement;
        else payload.replacement = replacement;
      } else {
        const updateSpec = parseJsonText(updatePayload, 'Update payload', { allowArray: true, allowObject: true });
        if (!Array.isArray(updateSpec)) {
          const keys = Object.keys(updateSpec || {});
          if (keys.length === 0 || !keys.every((key) => key.startsWith('$'))) {
            throw new Error('Update payload must use operators like $set or be a pipeline array.');
          }
        }
        payload.update = updateSpec;
      }
    }

    if (updateMethod === 'updateMany') {
      const preflight = await runPreflight('updateMany', {
        filter: JSON.stringify(filter),
        update: payload.update,
      });
      const details = summarizePreflight(preflight, 'Operation: updateMany');
      const confirmed = await confirmWithProductionPhrase(`Run updateMany on ${db}.${collection}`, details);
      if (!confirmed) return;
    }
    await runOperation(updateMethod, payload, { heavyConfirm: updateMethod === 'updateMany' });
  };

  const handleBulk = async () => {
    const operations = parseJsonText(bulkOperations, 'Bulk operations', { allowArray: true, allowObject: false });
    if (!Array.isArray(operations) || operations.length === 0) throw new Error('Bulk operations must be a non-empty array.');
    const preflight = await runPreflight('bulkWrite', { operations });
    const details = summarizePreflight(preflight, `Operations: ${formatNumber(operations.length)}`);
    const confirmed = await confirmWithProductionPhrase(`Run bulkWrite on ${db}.${collection}`, details);
    if (!confirmed) return;
    await runOperation('bulkWrite', {
      operations,
      ordered: bulkOrdered,
      bypassDocumentValidation: bulkBypassValidation,
    }, { heavyConfirm: true });
  };

  const runOtherCount = async () => {
    try {
      const filter = parseJsonText(otherCountFilter, 'Filter', { allowArray: false, allowObject: true });
      await runOperation('countDocuments', { filter });
    } catch (err) {
      setError(err.message || 'countDocuments failed.');
    }
  };

  const runOtherEstimated = async () => {
    await runOperation('estimatedDocumentCount', {});
  };

  const runOtherRename = async () => {
    try {
      const to = String(otherRenameTo || '').trim();
      if (!to) throw new Error('New collection name is required.');
      if (to === collection) throw new Error('New name must be different from current collection.');
      const confirmed = await requestConfirm({
        title: 'Rename Collection',
        message: `Rename ${db}.${collection} -> ${db}.${to}?`,
        confirmLabel: 'Rename',
        cancelLabel: 'Cancel',
      });
      if (!confirmed) return;
      await runOperation('renameCollection', { to, dropTarget: otherRenameDropTarget });
    } catch (err) {
      setError(err.message || 'renameCollection failed.');
    }
  };

  const runOtherHideToggle = async (hidden) => {
    try {
      const name = String(otherIndexName || '').trim();
      if (!name) throw new Error('Index name is required.');
      await runOperation(hidden ? 'hideIndex' : 'unhideIndex', { name });
    } catch (err) {
      setError(err.message || 'Index operation failed.');
    }
  };

  const runOtherDropIndexes = async () => {
    try {
      const raw = String(otherDropIndexes || '').trim();
      if (!raw) throw new Error('Indexes input is required.');
      const names = raw === '*'
        ? '*'
        : parseJsonText(raw, 'Indexes', { allowArray: true, allowObject: false, allowEmpty: false });
      const confirmed = await requestConfirm({
        title: 'Drop Indexes',
        message: `Drop indexes in ${db}.${collection}?`,
        confirmLabel: 'Drop',
        cancelLabel: 'Cancel',
        danger: true,
      });
      if (!confirmed) return;
      await runOperation('dropIndexes', { names }, { heavyConfirm: true });
    } catch (err) {
      setError(err.message || 'dropIndexes failed.');
    }
  };

  const runOtherValidate = async () => {
    const confirmed = await requestConfirm({
      title: 'Validate Collection',
      message: `Run validate on ${db}.${collection}?`,
      confirmLabel: 'Validate',
      cancelLabel: 'Cancel',
    });
    if (!confirmed) return;
    await runOperation('validateCollection', { full: otherValidateFull }, { heavyConfirm: true });
  };

  const runOtherReIndex = async () => {
    const confirmed = await requestConfirm({
      title: 'ReIndex Collection',
      message: `Run reIndex on ${db}.${collection}?`,
      confirmLabel: 'ReIndex',
      cancelLabel: 'Cancel',
      danger: true,
    });
    if (!confirmed) return;
    await runOperation('reIndex', {}, { heavyConfirm: true });
  };

  const handleRun = async () => {
    if (busy) return;
    try {
      if (!db || !collection) throw new Error('Collection context is required.');
      if (activeOperateTab === 'insert') await handleInsert();
      else if (activeOperateTab === 'delete') await handleDelete();
      else if (activeOperateTab === 'update') await handleUpdate();
      else await handleBulk();
    } catch (err) {
      setError(err.message || 'Operation failed.');
    }
  };

  const handleCancelBusy = () => {
    const cancelled = abortCurrentOperation();
    if (!cancelled) return;
    setBusy(false);
    setSuccess('Operation cancelled.');
  };

  if (!open) return null;

  return (
    <>
    <AppModal open={open} onClose={onClose} maxWidth="max-w-7xl">
      <div className="flex flex-col" style={{ maxHeight: '90vh' }}>
        <div className="px-5 py-4 flex items-center justify-between" style={{ borderBottom: '1px solid var(--border)' }}>
          <div className="flex items-center gap-3">
            <div className="inline-flex items-center gap-2">
              <Zap className="w-4 h-4" style={{ color: 'var(--accent)' }} />
              <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>Operate</h3>
            </div>
            <span className="badge-blue">Locked context</span>
            <span
              className="inline-flex items-center gap-1.5 text-2xs font-mono px-2 py-1 rounded-md"
              style={{
                background: 'var(--surface-2)',
                border: '1px solid var(--border)',
                color: 'var(--text-secondary)',
              }}
            >
              <Lock className="w-3 h-3" />
              {contextText}
            </span>
          </div>
          <button type="button" onClick={onClose} className="btn-ghost p-1.5"><X className="w-4 h-4" /></button>
        </div>

        <div className="px-5 pt-3 flex items-center gap-1" style={{ borderBottom: '1px solid var(--border)' }}>
          {SECTION_TABS.map((tab) => (
            <button
              key={tab.id}
              type="button"
              onClick={() => {
                setActiveSection(tab.id);
                setError('');
                setSuccess('');
              }}
              className="px-3 py-2 text-xs font-medium inline-flex items-center gap-1.5 transition-all -mb-px"
              style={{
                color: activeSection === tab.id ? 'var(--accent)' : 'var(--text-tertiary)',
                borderBottom: activeSection === tab.id ? '2px solid var(--accent)' : '2px solid transparent',
              }}
            >
              <tab.icon className="w-3.5 h-3.5" />
              {tab.label}
            </button>
          ))}
        </div>

        {(activeSection === 'operate' || activeSection === 'others') && (
          <div className="px-5 pt-2 flex items-center gap-1" style={{ borderBottom: '1px solid var(--border)' }}>
            {(activeSection === 'operate' ? OPERATE_TABS : OTHERS_TABS).map((tab) => (
              <button
                key={tab.id}
                type="button"
                onClick={() => {
                  if (activeSection === 'operate') setActiveOperateTab(tab.id);
                  else setActiveOthersTab(tab.id);
                  setError('');
                  setSuccess('');
                }}
                className="px-3 py-1.5 text-2xs font-medium transition-all -mb-px"
                style={{
                  color: (activeSection === 'operate' ? activeOperateTab : activeOthersTab) === tab.id ? 'var(--accent)' : 'var(--text-tertiary)',
                  borderBottom: (activeSection === 'operate' ? activeOperateTab : activeOthersTab) === tab.id ? '2px solid var(--accent)' : '2px solid transparent',
                }}
              >
                {tab.label}
              </button>
            ))}
          </div>
        )}

        <div className="px-5 py-4 overflow-auto space-y-3 flex-1 min-h-0">
        {activeSection === 'operate' && activeOperateTab === 'insert' && (
          <div className="space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <label className="space-y-1.5">
                <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Method</span>
                <DropdownSelect
                  fullWidth
                  sizeClassName="text-xs"
                  value={insertMethod}
                  options={INSERT_METHODS}
                  onChange={setInsertMethod}
                  menuZIndex={520}
                />
              </label>
              <div className="space-y-1.5">
                <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Prefill</span>
                <div className="flex flex-wrap items-center gap-1.5">
                  {INSERT_PRESETS.map((preset) => (
                    <button
                      key={preset.id}
                      type="button"
                      className="btn-ghost text-2xs px-2 py-1.5"
                      onClick={() => {
                        setInsertMethod(preset.method);
                        setInsertPayload(preset.value);
                      }}
                    >
                      {preset.label}
                    </button>
                  ))}
                </div>
              </div>
            </div>
            <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color: 'var(--text-secondary)' }}>
              <input type="checkbox" className="ms-checkbox" checked={insertOrdered} onChange={(event) => setInsertOrdered(event.target.checked)} />
              ordered insert (for insertMany)
            </label>
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color: 'var(--text-tertiary)' }}>Payload</div>
              <textarea
                value={insertPayload}
                onChange={(event) => setInsertPayload(event.target.value)}
                spellCheck={false}
                className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                style={{ minHeight: '220px', background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
              />
            </div>
          </div>
        )}
        {activeSection === 'operate' && activeOperateTab === 'delete' && (
          <div className="space-y-3">
            <div className="space-y-1.5">
              <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Method</span>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs"
                value={deleteMethod}
                options={DELETE_METHODS}
                onChange={setDeleteMethod}
                menuZIndex={520}
              />
            </div>
            <div className="space-y-1.5">
              <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Prefill</span>
              <div className="flex flex-wrap items-center gap-1.5">
                {DELETE_PRESETS.map((preset) => (
                  <button
                    key={preset.id}
                    type="button"
                    className="btn-ghost text-2xs px-2 py-1.5"
                    onClick={() => {
                      setDeleteMethod(preset.method);
                      setDeleteFilter(preset.filter);
                      if (preset.sort) setDeleteSort(preset.sort);
                      if (typeof preset.justOne === 'boolean') setDeleteJustOne(preset.justOne);
                    }}
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
            </div>
            {isDeleteRemoveMethod && (
              <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color: 'var(--text-secondary)' }}>
                <input type="checkbox" className="ms-checkbox" checked={deleteJustOne} onChange={(event) => setDeleteJustOne(event.target.checked)} />
                justOne (legacy remove option)
              </label>
            )}
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color: 'var(--text-tertiary)' }}>Filter</div>
              <textarea
                value={deleteFilter}
                onChange={(event) => setDeleteFilter(event.target.value)}
                spellCheck={false}
                className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                style={{ minHeight: '120px', background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
              />
            </div>
            {showDeleteSort && (
              <div>
                <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color: 'var(--text-tertiary)' }}>Sort</div>
                <textarea
                  value={deleteSort}
                  onChange={(event) => setDeleteSort(event.target.value)}
                  spellCheck={false}
                  className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                  style={{ minHeight: '80px', background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                />
              </div>
            )}
            {showDeleteProjection && (
              <div>
                <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color: 'var(--text-tertiary)' }}>Projection (optional)</div>
                <textarea
                  value={deleteProjection}
                  onChange={(event) => setDeleteProjection(event.target.value)}
                  spellCheck={false}
                  placeholder='e.g. { "_id": 1, "title": 1 }'
                  className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                  style={{ minHeight: '80px', background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                />
              </div>
            )}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <label className="space-y-1.5 block">
                <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Hint (optional)</span>
                <input
                  value={deleteHint}
                  onChange={(event) => setDeleteHint(event.target.value)}
                  className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                  style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                  placeholder="index_name"
                />
              </label>
              <label className="space-y-1.5 block">
                <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Collation (optional JSON)</span>
                <input
                  value={deleteCollation}
                  onChange={(event) => setDeleteCollation(event.target.value)}
                  className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                  style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                  placeholder='{ "locale": "en", "strength": 2 }'
                />
              </label>
            </div>
          </div>
        )}

        {activeSection === 'operate' && activeOperateTab === 'update' && (
          <div className="space-y-3">
            <label className="space-y-1.5 block">
              <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Method</span>
              <DropdownSelect
                fullWidth
                sizeClassName="text-xs"
                value={updateMethod}
                options={UPDATE_METHODS}
                onChange={setUpdateMethod}
                menuZIndex={520}
              />
            </label>
            <div className="space-y-1.5">
              <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Prefill</span>
              <div className="flex flex-wrap items-center gap-1.5">
                {UPDATE_PRESETS.map((preset) => (
                  <button
                    key={preset.id}
                    type="button"
                    className="btn-ghost text-2xs px-2 py-1.5"
                    onClick={() => {
                      setUpdateMethod(preset.method);
                      if (preset.mode) setUpdateFindAndModifyMode(preset.mode);
                      if (preset.filter) setUpdateFilter(preset.filter);
                      if (preset.sort) setUpdateSort(preset.sort);
                      if (preset.payload) setUpdatePayload(preset.payload);
                    }}
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
            </div>
            {isFindAndModify && (
              <label className="space-y-1.5 block">
                <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>findAndModify mode</span>
                <DropdownSelect
                  fullWidth
                  sizeClassName="text-xs"
                  value={updateFindAndModifyMode}
                  options={FIND_AND_MODIFY_MODES}
                  onChange={setUpdateFindAndModifyMode}
                  menuZIndex={520}
                />
              </label>
            )}
            <div>
              <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color: 'var(--text-tertiary)' }}>Filter</div>
              <textarea
                value={updateFilter}
                onChange={(event) => setUpdateFilter(event.target.value)}
                spellCheck={false}
                className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                style={{ minHeight: '110px', background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
              />
            </div>
            {updateNeedsPayload && (
              <div>
                <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color: 'var(--text-tertiary)' }}>
                  {updateNeedsReplacement ? 'Replacement' : 'Update payload'}
                </div>
                <textarea
                  value={updatePayload}
                  onChange={(event) => setUpdatePayload(event.target.value)}
                  spellCheck={false}
                  className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                  style={{ minHeight: '170px', background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                />
              </div>
            )}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {updateNeedsSort && (
                <div>
                  <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color: 'var(--text-tertiary)' }}>Sort</div>
                  <textarea
                    value={updateSort}
                    onChange={(event) => setUpdateSort(event.target.value)}
                    spellCheck={false}
                    className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                    style={{ minHeight: '80px', background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                  />
                </div>
              )}
              {updateNeedsProjection && (
                <div>
                  <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color: 'var(--text-tertiary)' }}>Projection (optional)</div>
                  <textarea
                    value={updateProjection}
                    onChange={(event) => setUpdateProjection(event.target.value)}
                    spellCheck={false}
                    className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                    style={{ minHeight: '80px', background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                  />
                </div>
              )}
            </div>
            {updateNeedsArrayFilters && (
              <div>
                <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color: 'var(--text-tertiary)' }}>Array Filters (optional JSON array)</div>
                <textarea
                  value={updateArrayFilters}
                  onChange={(event) => setUpdateArrayFilters(event.target.value)}
                  spellCheck={false}
                  className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                  style={{ minHeight: '80px', background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                />
              </div>
            )}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <label className="space-y-1.5 block">
                <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Hint (optional)</span>
                <input
                  value={updateHint}
                  onChange={(event) => setUpdateHint(event.target.value)}
                  className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                  style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                  placeholder="index_name"
                />
              </label>
              <label className="space-y-1.5 block">
                <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Collation (optional JSON)</span>
                <input
                  value={updateCollation}
                  onChange={(event) => setUpdateCollation(event.target.value)}
                  className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                  style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                  placeholder='{ "locale": "en", "strength": 2 }'
                />
              </label>
            </div>
            <div className="flex items-center gap-4">
              {updateAllowsUpsert && (
                <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color: 'var(--text-secondary)' }}>
                  <input type="checkbox" className="ms-checkbox" checked={updateUpsert} onChange={(event) => setUpdateUpsert(event.target.checked)} />
                  upsert
                </label>
              )}
              {updateNeedsReturnDocument && (
                <label className="inline-flex items-center gap-2 text-xs" style={{ color: 'var(--text-secondary)' }}>
                  return
                  <DropdownSelect
                    sizeClassName="text-xs"
                    value={updateReturnDocument}
                    options={[
                      { value: 'before', label: 'before' },
                      { value: 'after', label: 'after' },
                    ]}
                    onChange={setUpdateReturnDocument}
                    menuZIndex={520}
                  />
                </label>
              )}
            </div>
          </div>
        )}

        {activeSection === 'operate' && activeOperateTab === 'bulk' && (
          <div className="space-y-3">
            <div className="space-y-1.5">
              <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Prefill</span>
              <div className="flex flex-wrap items-center gap-1.5">
                {BULK_PRESETS.map((preset) => (
                  <button
                    key={preset.id}
                    type="button"
                    className="btn-ghost text-2xs px-2 py-1.5"
                    onClick={() => setBulkOperations(preset.value)}
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
            </div>
            <textarea
              value={bulkOperations}
              onChange={(event) => setBulkOperations(event.target.value)}
              spellCheck={false}
              className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
              style={{ minHeight: '260px', background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
            />
            <div className="flex flex-wrap items-center gap-4">
              <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color: 'var(--text-secondary)' }}>
                <input type="checkbox" className="ms-checkbox" checked={bulkOrdered} onChange={(event) => setBulkOrdered(event.target.checked)} />
                ordered
              </label>
              <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color: 'var(--text-secondary)' }}>
                <input type="checkbox" className="ms-checkbox" checked={bulkBypassValidation} onChange={(event) => setBulkBypassValidation(event.target.checked)} />
                bypassDocumentValidation
              </label>
            </div>
          </div>
        )}

        {activeSection === 'others' && activeOthersTab === 'collection' && (
          <div className="space-y-3">
            <div className="rounded-lg p-3" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              <div className="text-xs font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>Count Documents</div>
              <textarea
                value={otherCountFilter}
                onChange={(event) => setOtherCountFilter(event.target.value)}
                spellCheck={false}
                className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                style={{ minHeight: '90px', background: 'var(--surface-1)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
              />
              <div className="mt-2 flex items-center gap-2">
                <button type="button" className="btn-primary text-xs" onClick={runOtherCount} disabled={busy}>countDocuments()</button>
                <button type="button" className="btn-ghost text-xs" onClick={runOtherEstimated} disabled={busy}>estimatedDocumentCount()</button>
              </div>
            </div>

            <div className="rounded-lg p-3" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              <div className="text-xs font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>Rename Collection</div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <label className="space-y-1.5 block">
                  <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>New Name</span>
                  <input
                    value={otherRenameTo}
                    onChange={(event) => setOtherRenameTo(event.target.value)}
                    className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                    style={{ background: 'var(--surface-1)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                    placeholder={`${collection}_backup`}
                  />
                </label>
                <label className="flex items-center gap-2 text-xs cursor-pointer mt-5" style={{ color: 'var(--text-secondary)' }}>
                  <input type="checkbox" className="ms-checkbox" checked={otherRenameDropTarget} onChange={(event) => setOtherRenameDropTarget(event.target.checked)} />
                  dropTarget
                </label>
              </div>
              <div className="mt-2">
                <button type="button" className="btn-primary text-xs" onClick={runOtherRename} disabled={busy}>renameCollection()</button>
              </div>
            </div>
          </div>
        )}

        {activeSection === 'others' && activeOthersTab === 'indexes' && (
          <div className="space-y-3">
            <div className="rounded-lg p-3" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              <div className="text-xs font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>Hide / Unhide Index</div>
              <input
                value={otherIndexName}
                onChange={(event) => setOtherIndexName(event.target.value)}
                className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                style={{ background: 'var(--surface-1)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                placeholder="index_name"
              />
              <div className="mt-2 flex items-center gap-2">
                <button type="button" className="btn-primary text-xs" onClick={() => runOtherHideToggle(true)} disabled={busy}>hideIndex()</button>
                <button type="button" className="btn-ghost text-xs" onClick={() => runOtherHideToggle(false)} disabled={busy}>unhideIndex()</button>
              </div>
            </div>

            <div className="rounded-lg p-3" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              <div className="text-xs font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>Drop Indexes</div>
              <input
                value={otherDropIndexes}
                onChange={(event) => setOtherDropIndexes(event.target.value)}
                className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                style={{ background: 'var(--surface-1)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                placeholder='* or ["idx_name","idx_name_2"]'
              />
              <div className="mt-1 text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                Use <span className="font-mono">*</span> for all non-_id indexes.
              </div>
              <div className="mt-2 flex items-center gap-2">
                <button type="button" className="btn-primary text-xs" onClick={runOtherDropIndexes} disabled={busy}>dropIndexes()</button>
              </div>
            </div>
          </div>
        )}
        {activeSection === 'others' && activeOthersTab === 'diagnostics' && (
          <div className="space-y-3">
            <div className="rounded-lg p-3" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              <div className="text-xs font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>Validate Collection</div>
              <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color: 'var(--text-secondary)' }}>
                <input type="checkbox" className="ms-checkbox" checked={otherValidateFull} onChange={(event) => setOtherValidateFull(event.target.checked)} />
                full validate (heavier)
              </label>
              <div className="mt-2">
                <button type="button" className="btn-primary text-xs" onClick={runOtherValidate} disabled={busy}>validate()</button>
              </div>
            </div>

            <div className="rounded-lg p-3" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              <div className="text-xs font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>ReIndex</div>
              <div className="text-2xs mb-2" style={{ color: 'var(--text-tertiary)' }}>
                Heavy maintenance command. Use only when needed.
              </div>
              <button type="button" className="btn-primary text-xs" onClick={runOtherReIndex} disabled={busy}>reIndex()</button>
            </div>
          </div>
        )}

        {activeSection === 'console' && (
          <div
            className="rounded-xl overflow-hidden"
            style={{
              border: '1px solid var(--border)',
              minHeight: '560px',
              background: 'var(--surface-1)',
            }}
          >
            <ConsoleView
              db={db}
              collection={collection}
              scope={consoleScope}
              onQueryMs={() => {}}
              menuZIndex={520}
            />
          </div>
        )}

        {activeSection === 'legacy' && (
          <div className="space-y-3">
            <div className="text-xs" style={{ color: 'var(--text-secondary)' }}>
              Quick reference of legacy/compat coverage in current UI and server API layer.
            </div>
            <div className="grid grid-cols-1 gap-3">
              {LEGACY_GROUPS.map((group) => {
                const tone = cardTone(group.kind);
                return (
                  <div key={group.title} className="rounded-lg p-3" style={{ border: tone.border, background: tone.background }}>
                    <div className="text-xs font-semibold mb-2" style={{ color: tone.label }}>{group.title}</div>
                    <div className="space-y-1">
                      {group.items.map((item) => (
                        <div key={item} className="text-2xs" style={{ color: 'var(--text-secondary)' }}>
                          {item}
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>
            <div className="rounded-lg p-3" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              <div className="text-xs font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>Query Tab Scope</div>
              <div className="text-2xs" style={{ color: 'var(--text-secondary)' }}>
                Query tab remains read-focused (find/aggregate/explain). Write methods are centralized in Operate and Console.
              </div>
            </div>
          </div>
        )}

        {activeSection !== 'console' && error && (
          <div className="text-xs px-3 py-2 rounded-lg" style={{ color: '#fca5a5', background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.25)' }}>
            {error}
          </div>
        )}
        {activeSection !== 'console' && success && (
          <div className="text-xs px-3 py-2 rounded-lg" style={{ color: '#34d399', background: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.25)' }}>
            {success}
          </div>
        )}
        {activeSection !== 'console' && lastResult && (
          <div className="rounded-lg p-3" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
            <div className="text-2xs uppercase tracking-wider mb-2" style={{ color: 'var(--text-tertiary)' }}>Last result</div>
            <pre className="text-2xs font-mono whitespace-pre-wrap break-all" style={{ color: 'var(--text-secondary)' }}>
              {JSON.stringify(lastResult, null, 2)}
            </pre>
          </div>
        )}
        </div>

        <div className="px-5 py-4 flex items-center justify-end gap-2 flex-shrink-0" style={{ borderTop: '1px solid var(--border)' }}>
          {activeSection !== 'console' && lastRunElapsedMs !== null && (
            <span
              className="inline-flex items-center px-2 py-1 rounded-md text-2xs"
              style={{ color: 'var(--text-tertiary)', background: 'var(--surface-2)', border: '1px solid var(--border)' }}
            >
              Run {formatDuration(lastRunElapsedMs)}
            </span>
          )}
          {(activeSection === 'operate' || activeSection === 'others') && (
            <div className="mr-auto inline-flex flex-none items-center justify-start gap-2 whitespace-nowrap">
              <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>
                Budget
              </span>
              <DropdownSelect
                value={timeoutSelectValue}
                options={timeoutSelectOptions}
                onChange={handleTimeoutSelectChange}
                sizeClassName="text-2xs"
                className="shrink-0"
                menuZIndex={560}
              />
              {showCustomTimeoutInput && (
                <div className="inline-flex items-center gap-1.5">
                  <input
                    type="number"
                    min={5}
                    max={Math.round(getModeTimeoutMax(mode) / 1000)}
                    step={1}
                    value={customTimeoutSeconds}
                    onChange={(event) => setCustomTimeoutSeconds(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') applyCustomTimeout();
                    }}
                    className="ms-number w-16 px-2 py-1 rounded-md text-2xs font-mono"
                    style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                    placeholder="sec"
                  />
                  <button
                    type="button"
                    className="btn-ghost text-2xs px-2 py-1"
                    onClick={applyCustomTimeout}
                  >
                    Set
                  </button>
                </div>
              )}
              <DropdownSelect
                value={limitSelectValue}
                options={limitSelectOptions}
                onChange={handleLimitSelectChange}
                sizeClassName="text-2xs"
                className="shrink-0"
                menuZIndex={560}
              />
              {showCustomLimitInput && (
                <div className="inline-flex items-center gap-1.5">
                  <input
                    type="number"
                    min={DEFAULT_QUERY_LIMIT}
                    max={getModeLimitMax(mode)}
                    step={1}
                    value={customLimitValue}
                    onChange={(event) => setCustomLimitValue(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter') applyCustomLimit();
                    }}
                    className="ms-number w-20 px-2 py-1 rounded-md text-2xs font-mono"
                    style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                    placeholder="docs"
                  />
                  <button
                    type="button"
                    className="btn-ghost text-2xs px-2 py-1"
                    onClick={applyCustomLimit}
                  >
                    Set
                  </button>
                </div>
              )}
            </div>
          )}
          {activeSection !== 'console' && (
            <button type="button" className="btn-ghost text-xs" onClick={handleCancelBusy} disabled={!busy}>Cancel</button>
          )}
          <button type="button" className="btn-ghost text-xs" onClick={onClose} disabled={busy}>Close</button>
          {activeSection === 'operate' && (
            <button type="button" className="btn-primary text-xs inline-flex items-center gap-1.5" onClick={handleRun} disabled={busy}>
              {busy && <Loader className="w-3.5 h-3.5" />}
              {busy ? 'Running...' : 'Run operation'}
            </button>
          )}
        </div>
      </div>
    </AppModal>
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
      open={Boolean(phraseDialog)}
      onClose={() => closePhraseDialog(false)}
      maxWidth="max-w-md"
    >
      <div className="px-5 py-4" style={{ borderBottom: '1px solid var(--border)' }}>
        <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>
          {phraseDialog?.title || 'Production confirmation'}
        </h3>
      </div>
      <div className="px-5 py-4 space-y-2">
        <p className="text-xs whitespace-pre-line" style={{ color: 'var(--text-secondary)' }}>
          {phraseDialog?.message || 'Type confirmation phrase to continue.'}
        </p>
        <div className="rounded-lg px-3 py-2 text-2xs font-mono" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-secondary)' }}>
          {phraseDialog?.phrase || ''}
        </div>
        <input
          type="text"
          autoFocus
          value={phraseInput}
          onChange={(event) => setPhraseInput(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter') {
              closePhraseDialog(true);
            }
          }}
          className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
          style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
          placeholder={phraseDialog?.phrase || 'Type phrase'}
        />
      </div>
      <div className="px-5 py-4 flex items-center justify-end gap-2" style={{ borderTop: '1px solid var(--border)' }}>
        <button type="button" className="btn-ghost text-xs" onClick={() => closePhraseDialog(false)}>
          Cancel
        </button>
        <button
          type="button"
          className="px-3 py-1.5 rounded-lg text-xs font-medium text-red-300 bg-red-500/15 border border-red-500/30 hover:bg-red-500/25 disabled:opacity-40"
          onClick={() => closePhraseDialog(true)}
          disabled={!String(phraseInput || '').trim()}
        >
          {phraseDialog?.confirmLabel || 'Confirm write'}
        </button>
      </div>
    </AppModal>
    </>
  );
}

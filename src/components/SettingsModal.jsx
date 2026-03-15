import React, { useState, useEffect, useMemo } from 'react';
import api from '../utils/api';
import { X, Shield, ShieldOff, Settings, Cpu, Eye, ChevronDown, Loader } from './Icons';
import { formatBytes, formatDuration, formatNumber } from '../utils/formatters';
import AppModal from './modals/AppModal';
import DropdownSelect from './DropdownSelect';

const SAFE_QUERY_LIMIT_MAX = 1000;
const POWER_QUERY_LIMIT_PRESET_MAX = 5000;
const QUERY_LIMIT_OVERRIDE_MAX = 50000;
const POWER_QUERY_LIMIT_MAX = 2147483000;
const SAFE_QUERY_TIMEOUT_MAX_MS = 30000;
const POWER_QUERY_TIMEOUT_PRESET_MAX_MS = 120000;
const POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS = 2147483000;
const EXEC_TIMEOUT_PRESETS = [5000, 10000, 30000, 60000, POWER_QUERY_TIMEOUT_PRESET_MAX_MS];
const EXEC_LIMIT_PRESETS = [50, 100, 200, 500, 1000, POWER_QUERY_LIMIT_PRESET_MAX, QUERY_LIMIT_OVERRIDE_MAX, POWER_QUERY_LIMIT_MAX];

const getModePresetLimitMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_LIMIT_PRESET_MAX : SAFE_QUERY_LIMIT_MAX);
const getModeCustomLimitMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_LIMIT_MAX : QUERY_LIMIT_OVERRIDE_MAX);
const getModePresetTimeoutMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_TIMEOUT_PRESET_MAX_MS : SAFE_QUERY_TIMEOUT_MAX_MS);
const getModeCustomTimeoutMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS : SAFE_QUERY_TIMEOUT_MAX_MS);
const formatTimeoutSecondsLabel = (valueMs) => `${Math.max(5, Math.round((Number(valueMs) || 5000) / 1000))}s`;
const replaceNearestScalePoint = (baseValues = [], currentValue) => {
  const base = Array.isArray(baseValues) ? baseValues.map((value) => Number(value)).filter(Number.isFinite) : [];
  if (!base.length || !Number.isFinite(Number(currentValue))) return base;
  if (base.includes(currentValue)) return base;
  let nearestIndex = 0;
  let nearestDiff = Number.POSITIVE_INFINITY;
  base.forEach((value, index) => {
    const diff = Math.abs(value - Number(currentValue));
    if (diff < nearestDiff) {
      nearestDiff = diff;
      nearestIndex = index;
    }
  });
  const next = [...base];
  next[nearestIndex] = Number(currentValue);
  return next;
};

const ADMIN_KEY_STORAGE = 'mongostudio_admin_key';

export default function SettingsModal({
  execMode,
  onModeChangeRequest,
  onConfigApplied,
  onClose,
  connectionInfo,
  databases = [],
  metadataOverview = null,
  displaySettings = { showTopTags: false, showReadSourceTag: true },
  onDisplaySettingsChange,
  adminAccess,
  onAdminAccessChange,
}) {
  const EMPTY_DB_SUMMARY = {
    loading: false,
    databases: null,
    collections: null,
    documents: null,
    totalSize: null,
    loadedStats: 0,
    totalStats: 0,
  };
  const [config, setConfig] = useState(null);
  const [health, setHealth] = useState(null);
  const [serviceConfig, setServiceConfig] = useState(null);
  const [serviceError, setServiceError] = useState('');
  const [serviceSaving, setServiceSaving] = useState(false);
  const [executionSaving, setExecutionSaving] = useState(false);
  const [executionDraft, setExecutionDraft] = useState({ maxTimeMS: 5000, maxResultSize: 50 });
  const [executionCustomTimeoutInput, setExecutionCustomTimeoutInput] = useState('');
  const [executionCustomLimitInput, setExecutionCustomLimitInput] = useState('');
  const [rateDraft, setRateDraft] = useState({ windowMs: 60000, apiMax: 3000, heavyMax: 300 });
  const [adminKeyInput, setAdminKeyInput] = useState('');
  const [adminKeyError, setAdminKeyError] = useState('');
  const [adminKeySaving, setAdminKeySaving] = useState(false);
  const [dbSummary, setDbSummary] = useState(EMPTY_DB_SUMMARY);
  const [hostsExpanded, setHostsExpanded] = useState(false);
  const [activeSection, setActiveSection] = useState('execution');
  const hosts = useMemo(
    () => String(connectionInfo?.host || '').split(',').map((item) => item.trim()).filter(Boolean),
    [connectionInfo?.host],
  );
  const hasSharedMetadata = Boolean(
    metadataOverview
      && Array.isArray(databases)
      && (
        metadataOverview.loaded
        || metadataOverview.loading
        || (metadataOverview.stats && Object.keys(metadataOverview.stats).length > 0)
      ),
  );
  const sharedDbSummary = useMemo(() => {
    if (!hasSharedMetadata) return null;
    const statsMap = metadataOverview?.stats && typeof metadataOverview.stats === 'object' ? metadataOverview.stats : {};
    const freshnessMap = metadataOverview?.freshness && typeof metadataOverview.freshness === 'object' ? metadataOverview.freshness : {};
    const dbNames = databases.map((entry) => String(entry?.name || '')).filter(Boolean);
    const totalStats = dbNames.length;
    const loadedStats = dbNames.reduce(
      (sum, dbName) => sum + (Object.prototype.hasOwnProperty.call(freshnessMap, dbName) ? 1 : 0),
      0,
    );
    const statsValues = Object.values(statsMap);
    const collections = statsValues.reduce((sum, item) => sum + Number(item?.collections || 0), 0);
    const documents = statsValues.reduce((sum, item) => sum + Number(item?.objects || 0), 0);
    const fallbackTotalSize = dbNames.length
      ? databases.reduce((sum, item) => sum + Number(item?.sizeOnDisk || 0), 0)
      : 0;
    const totalSize = Number.isFinite(Number(metadataOverview?.totalSize))
      ? Number(metadataOverview.totalSize)
      : fallbackTotalSize;
    return {
      loading: Boolean(metadataOverview?.loading) || (totalStats > 0 && loadedStats < totalStats),
      databases: totalStats,
      collections,
      documents,
      totalSize,
      loadedStats,
      totalStats,
    };
  }, [hasSharedMetadata, metadataOverview, databases]);
  const effectiveDbSummary = sharedDbSummary || dbSummary;

  useEffect(() => {
    api.getExecutionConfig().then(setConfig).catch(() => {});
    api.getHealth().then(setHealth).catch(() => {});
    api.getServiceConfig().then(setServiceConfig).catch(() => {});
  }, []);

  useEffect(() => {
    const rate = serviceConfig?.rateLimit;
    if (!rate) return;
    setRateDraft({
      windowMs: Number(rate.windowMs || 60000),
      apiMax: Number(rate.apiMax || 3000),
      heavyMax: Number(rate.heavyMax || 300),
    });
  }, [serviceConfig]);

  useEffect(() => {
    if (activeSection !== 'health' || hosts.length <= 1) {
      setHostsExpanded(false);
    }
  }, [activeSection, hosts.length]);

  useEffect(() => {
    setConfig(prev => (prev ? { ...prev, mode: execMode } : prev));
  }, [execMode]);

  useEffect(() => {
    if (!config) return;
    const timeoutMax = getModeCustomTimeoutMax(config.mode);
    const timeout = Math.max(5000, Math.min(Number(config.maxTimeMS) || 5000, timeoutMax));
    const limitMax = getModeCustomLimitMax(config.mode);
    const limit = Math.max(50, Math.min(Number(config.maxResultSize) || 50, limitMax));
    setExecutionDraft((prev) => {
      if (prev.maxTimeMS === timeout && prev.maxResultSize === limit) return prev;
      return { maxTimeMS: timeout, maxResultSize: limit };
    });
    setExecutionCustomTimeoutInput('');
    setExecutionCustomLimitInput('');
  }, [config?.maxTimeMS, config?.maxResultSize, config?.mode]);

  useEffect(() => {
    if (activeSection !== 'health') return;
    if (hasSharedMetadata) return;
    let active = true;
    (async () => {
      setDbSummary({
        loading: true,
        databases: null,
        collections: null,
        documents: null,
        totalSize: null,
        loadedStats: 0,
        totalStats: 0,
      });
      try {
        const list = await api.listDatabases();
        const databases = Array.isArray(list?.databases) ? list.databases : [];
        const totalSize = databases.reduce((sum, entry) => sum + Number(entry?.sizeOnDisk || 0), 0);
        let collections = 0;
        let documents = 0;
        let completed = 0;
        const totalStats = databases.length;
        if (active) {
          setDbSummary({
            loading: totalStats > 0,
            databases: databases.length,
            collections: totalStats > 0 ? null : 0,
            documents: totalStats > 0 ? null : 0,
            totalSize,
            loadedStats: 0,
            totalStats,
          });
        }

        if (totalStats === 0) {
          if (active) {
            setDbSummary({
              loading: false,
              databases: 0,
              collections: 0,
              documents: 0,
              totalSize: 0,
              loadedStats: 0,
              totalStats: 0,
            });
          }
          return;
        }

        const queue = [...databases];
        const workers = Math.min(2, queue.length);
        const runWorker = async () => {
          while (queue.length > 0 && active) {
            const entry = queue.shift();
            if (!entry?.name) continue;
            try {
              const stats = await api.getDatabaseStats(entry.name);
              collections += Number(stats?.collections || 0);
              documents += Number(stats?.objects || 0);
            } catch {}
            completed += 1;
            if (active) {
              setDbSummary({
                loading: completed < totalStats,
                databases: databases.length,
                collections,
                documents,
                totalSize,
                loadedStats: completed,
                totalStats,
              });
            }
          }
        };
        await Promise.all(Array.from({ length: workers }, () => runWorker()));
        if (active) {
          setDbSummary({
            loading: false,
            databases: databases.length,
            collections,
            documents,
            totalSize,
            loadedStats: totalStats,
            totalStats,
          });
        }
      } catch {
        if (active) setDbSummary(EMPTY_DB_SUMMARY);
      }
    })();
    return () => { active = false; };
  }, [activeSection, connectionInfo?.connectionId, hasSharedMetadata]);

  const updateConfig = async (changes) => {
    try {
      const c = await api.setExecutionConfig(changes);
      setConfig(c);
      if (changes.mode) onConfigApplied?.(c.mode);
      return true;
    } catch (err) {
      console.error(err);
      return false;
    }
  };

  const updateServiceConfig = async (changes) => {
    if (!changes || typeof changes !== 'object') return;
    const next = {
      ...serviceConfig,
      ...changes,
      rateLimit: {
        ...(serviceConfig?.rateLimit || {}),
        ...(changes.rateLimit || {}),
      },
    };
    setServiceError('');
    setServiceSaving(true);
    setServiceConfig(next);
    try {
      const saved = await api.setServiceConfig(next);
      setServiceConfig(saved);
    } catch (err) {
      setServiceError(err.message || 'Failed to update service config.');
      api.getServiceConfig().then(setServiceConfig).catch(() => {});
    } finally {
      setServiceSaving(false);
    }
  };

  const executionDirty = Boolean(
    config
    && (
      Number(executionDraft.maxTimeMS) !== Number(config.maxTimeMS)
      || Number(executionDraft.maxResultSize) !== Number(config.maxResultSize)
    )
  );
  const timeoutCustomEditing = !EXEC_TIMEOUT_PRESETS.includes(executionDraft.maxTimeMS) || executionCustomTimeoutInput !== '';
  const limitCustomEditing = !EXEC_LIMIT_PRESETS.includes(executionDraft.maxResultSize) || executionCustomLimitInput !== '';
  const timeoutPresetMax = config ? getModePresetTimeoutMax(config.mode) : SAFE_QUERY_TIMEOUT_MAX_MS;
  const timeoutSliderValue = Math.max(5000, Math.min(Number(executionDraft.maxTimeMS) || 5000, timeoutPresetMax));
  const timeoutScaleBase = config?.mode === 'safe'
    ? [5000, 10000, 20000, 30000]
    : [5000, 30000, 60000, 120000];
  const timeoutScaleValues = timeoutCustomEditing
    ? replaceNearestScalePoint(timeoutScaleBase, timeoutSliderValue)
    : timeoutScaleBase;
  const docsPresetMax = config ? getModePresetLimitMax(config.mode) : SAFE_QUERY_LIMIT_MAX;
  const docsSliderValue = Math.max(50, Math.min(Number(executionDraft.maxResultSize) || 50, docsPresetMax));
  const docsScaleBase = config?.mode === 'safe'
    ? [50, 200, 500, 1000]
    : [50, 500, 1000, 5000];
  const docsScaleValues = limitCustomEditing
    ? replaceNearestScalePoint(docsScaleBase, docsSliderValue)
    : docsScaleBase;
  const timeoutSelectOptions = useMemo(
    () => [
      ...EXEC_TIMEOUT_PRESETS.map((value) => ({
        value: String(value),
        label: `${Math.round(value / 1000)}s`,
        disabled: config?.mode !== 'power' && value > SAFE_QUERY_TIMEOUT_MAX_MS,
      })),
      { value: 'custom', label: 'Custom' },
    ],
    [config?.mode],
  );
  const limitSelectOptions = useMemo(
    () => [
      { value: '50', label: '50 documents' },
      { value: '100', label: '100 documents' },
      { value: '200', label: '200 documents' },
      { value: '500', label: '500 documents' },
      { value: '1000', label: '1,000 documents' },
      { value: String(POWER_QUERY_LIMIT_PRESET_MAX), label: '5,000 documents', disabled: config?.mode !== 'power' },
      { value: 'unlimited', label: 'Unlimited', disabled: config?.mode !== 'power' },
      { value: 'custom', label: 'Custom' },
    ],
    [config?.mode],
  );

  const applyExecutionDefaults = async () => {
    if (!config || !executionDirty) return;
    setExecutionSaving(true);
    try {
      const ok = await updateConfig({
        maxTimeMS: executionDraft.maxTimeMS,
        maxResultSize: executionDraft.maxResultSize,
      });
      if (ok) onClose?.();
    } finally {
      setExecutionSaving(false);
    }
  };

  return (
    <AppModal open onClose={onClose} maxWidth="max-w-lg">
      <div className="flex items-center justify-between px-5 py-4" style={{ borderBottom: '1px solid var(--border)' }}>
        <div className="flex items-center gap-2">
          <Settings className="w-4 h-4" style={{ color: 'var(--accent)' }} />
          <h2 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>Settings</h2>
        </div>
        <button onClick={onClose} className="btn-ghost p-1"><X className="w-4 h-4" /></button>
      </div>

      <div className="flex gap-0 px-5 pt-3" style={{ borderBottom: '1px solid var(--border)' }}>
        {[{ id: 'execution', label: 'Execution', icon: Shield }, { id: 'display', label: 'Display', icon: Eye }, { id: 'health', label: 'Server Info', icon: Cpu }, { id: 'rateLimit', label: 'Rate Limit', icon: Settings }].map(t => (
          <button
            key={t.id}
            onClick={() => setActiveSection(t.id)}
            className="flex items-center gap-1.5 px-3 py-2 text-xs font-medium -mb-px transition-all"
            style={{
              borderBottom: activeSection === t.id ? '2px solid var(--accent)' : '2px solid transparent',
              color: activeSection === t.id ? 'var(--accent)' : 'var(--text-tertiary)',
            }}
          >
            <t.icon className="w-3.5 h-3.5" />
            {t.label}
          </button>
        ))}
      </div>

      <div className="p-5 overflow-y-auto" style={{ maxHeight: 'calc(80vh - 120px)' }}>
        {activeSection === 'execution' && config && (
          <div className="space-y-5">
            <div>
              <label className="block text-xs font-medium mb-2" style={{ color: 'var(--text-secondary)' }}>Execution Mode</label>
              <div className="grid grid-cols-2 gap-2">
                <button
                  onClick={() => onModeChangeRequest?.('safe')}
                  className="p-3 rounded-xl text-left transition-all"
                  style={{ background: config.mode === 'safe' ? 'rgba(16,185,129,0.1)' : 'var(--surface-2)', border: config.mode === 'safe' ? '1px solid rgba(16,185,129,0.3)' : '1px solid var(--border)' }}
                >
                  <div className="flex items-center gap-2 mb-1">
                    <Shield className="w-4 h-4 text-emerald-400" />
                    <span className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>Safe Mode</span>
                  </div>
                  <p className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Limits enabled, $where blocked, allowDiskUse off</p>
                </button>
                <button
                  onClick={() => onModeChangeRequest?.('power')}
                  className="p-3 rounded-xl text-left transition-all"
                  style={{ background: config.mode === 'power' ? 'rgba(245,158,11,0.1)' : 'var(--surface-2)', border: config.mode === 'power' ? '1px solid rgba(245,158,11,0.3)' : '1px solid var(--border)' }}
                >
                  <div className="flex items-center gap-2 mb-1">
                    <ShieldOff className="w-4 h-4 text-amber-400" />
                    <span className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>Power Mode</span>
                  </div>
                  <p className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>No runtime limits, use with caution</p>
                </button>
              </div>
            </div>

            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="text-xs font-medium" style={{ color: 'var(--text-secondary)' }}>Query Timeout</label>
                <span className="text-xs font-mono font-semibold" style={{ color: 'var(--accent)' }}>
                  {formatTimeoutSecondsLabel(executionDraft.maxTimeMS)}
                </span>
              </div>
              <input
                type="range"
                min="5000"
                max={timeoutPresetMax}
                step="1000"
                value={timeoutSliderValue}
                onChange={e => setExecutionDraft((prev) => ({
                  ...prev,
                  maxTimeMS: Math.max(5000, Math.min(parseInt(e.target.value, 10) || 5000, getModePresetTimeoutMax(config.mode))),
                }))}
                className="w-full ms-range"
              />
              <div className="flex items-center gap-2 mt-3">
                <DropdownSelect
                  value={timeoutCustomEditing ? 'custom' : String(executionDraft.maxTimeMS)}
                  options={timeoutSelectOptions}
                  onChange={(nextValue) => {
                    if (nextValue === 'custom') {
                      setExecutionCustomTimeoutInput(String(Math.max(5, Math.round(executionDraft.maxTimeMS / 1000))));
                      return;
                    }
                    setExecutionCustomTimeoutInput('');
                    setExecutionDraft((prev) => ({
                      ...prev,
                      maxTimeMS: Math.max(5000, Math.min(parseInt(String(nextValue), 10) || 5000, getModePresetTimeoutMax(config.mode))),
                    }));
                  }}
                  sizeClassName="text-xs"
                  title="Default query timeout"
                  className="min-w-[112px]"
                  menuZIndex={460}
                />
                {timeoutCustomEditing && (
                  <>
                    <input
                      type="number"
                      min={5}
                      max={Math.round(getModeCustomTimeoutMax(config.mode) / 1000)}
                      step={1}
                      value={executionCustomTimeoutInput !== '' ? executionCustomTimeoutInput : String(Math.max(5, Math.round((executionDraft.maxTimeMS || 5000) / 1000)))}
                      onChange={(event) => setExecutionCustomTimeoutInput(event.target.value)}
                      className="ms-number w-24 text-xs px-2 py-1.5 rounded-lg font-mono"
                      style={{ background:'var(--surface-3)', border:'1px solid var(--border)', color:'var(--text-primary)' }}
                      placeholder="sec"
                    />
                    <button
                      type="button"
                      className="btn-ghost text-xs px-2 py-1.5"
                      onClick={() => {
                        const seconds = Number(executionCustomTimeoutInput);
                        if (!Number.isFinite(seconds)) return;
                        const nextMs = Math.max(5000, Math.min(Math.round(seconds * 1000), getModeCustomTimeoutMax(config.mode)));
                        setExecutionDraft((prev) => ({ ...prev, maxTimeMS: nextMs }));
                        setExecutionCustomTimeoutInput(String(Math.max(5, Math.round(nextMs / 1000))));
                      }}
                    >
                      Set
                    </button>
                  </>
                )}
              </div>
              {timeoutCustomEditing && (
                <p className="text-2xs mt-1.5" style={{ color: 'var(--text-tertiary)' }}>
                  {config.mode === 'safe' ? 'Custom: 5s – 30s' : 'Custom can exceed 120s in Power Mode'}
                </p>
              )}
            </div>

            <div>
              <div className="flex items-center justify-between mb-2">
                <label className="text-xs font-medium" style={{ color: 'var(--text-secondary)' }}>Query Documents</label>
                <span className="text-xs font-mono font-semibold" style={{ color: 'var(--accent)' }}>
                  {config.mode === 'power' && executionDraft.maxResultSize >= getModeCustomLimitMax(config.mode)
                    ? 'Unlimited'
                    : formatNumber(executionDraft.maxResultSize)}
                </span>
              </div>
              <input
                type="range"
                min="50"
                max={docsPresetMax}
                step="50"
                value={docsSliderValue}
                onChange={(event) => {
                  const next = Math.max(50, Math.min(parseInt(event.target.value, 10) || 50, docsPresetMax));
                  setExecutionCustomLimitInput('');
                  setExecutionDraft((prev) => ({ ...prev, maxResultSize: next }));
                }}
                className="w-full ms-range"
              />
              <div className="flex items-center gap-2 mt-3">
                <DropdownSelect
                  value={limitCustomEditing
                    ? 'custom'
                    : (config.mode === 'power' && executionDraft.maxResultSize >= getModeCustomLimitMax(config.mode) ? 'unlimited' : String(executionDraft.maxResultSize))}
                  options={limitSelectOptions}
                  onChange={(nextValue) => {
                    if (nextValue === 'custom') {
                      setExecutionCustomLimitInput(String(Math.max(50, Math.round(executionDraft.maxResultSize))));
                      return;
                    }
                    setExecutionCustomLimitInput('');
                    setExecutionDraft((prev) => ({
                      ...prev,
                      maxResultSize: nextValue === 'unlimited'
                        ? getModeCustomLimitMax(config.mode)
                        : Math.max(50, Math.min(parseInt(String(nextValue), 10) || 50, getModePresetLimitMax(config.mode))),
                    }));
                  }}
                  sizeClassName="text-xs"
                  title="Default query documents"
                  fullWidth={!limitCustomEditing}
                  menuZIndex={460}
                />
                {limitCustomEditing && (
                  <>
                    <input
                      type="number"
                      min={50}
                      max={getModeCustomLimitMax(config.mode)}
                      step={1}
                      value={executionCustomLimitInput !== '' ? executionCustomLimitInput : String(Math.max(50, Math.round(executionDraft.maxResultSize || 50)))}
                      onChange={(event) => setExecutionCustomLimitInput(event.target.value)}
                      className="ms-number w-32 text-xs px-2 py-1.5 rounded-lg font-mono"
                      style={{ background:'var(--surface-3)', border:'1px solid var(--border)', color:'var(--text-primary)' }}
                      placeholder="docs"
                    />
                    <button
                      type="button"
                      className="btn-ghost text-xs px-2 py-1.5"
                      onClick={() => {
                        const value = Number(executionCustomLimitInput);
                        if (!Number.isFinite(value)) return;
                        const nextLimit = Math.max(50, Math.min(Math.round(value), getModeCustomLimitMax(config.mode)));
                        setExecutionDraft((prev) => ({ ...prev, maxResultSize: nextLimit }));
                        setExecutionCustomLimitInput(String(nextLimit));
                      }}
                    >
                      Set
                    </button>
                  </>
                )}
              </div>
              {limitCustomEditing && (
                <p className="text-2xs mt-1.5" style={{ color: 'var(--text-tertiary)' }}>
                  {config.mode === 'safe'
                    ? `Custom: up to ${formatNumber(getModeCustomLimitMax(config.mode))} docs`
                    : 'Power Mode supports Unlimited and Custom'}
                </p>
              )}
            </div>

            <div className="flex items-center justify-between">
              <p className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                Applies to this session only.
              </p>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  className="btn-ghost text-xs px-3 py-1.5"
                  disabled={!executionDirty || executionSaving}
                  onClick={() => {
                    setExecutionDraft({
                      maxTimeMS: Math.max(5000, Math.min(Number(config.maxTimeMS) || 5000, getModeCustomTimeoutMax(config.mode))),
                      maxResultSize: Math.max(50, Math.min(Number(config.maxResultSize) || 50, getModeCustomLimitMax(config.mode))),
                    });
                    setExecutionCustomTimeoutInput('');
                    setExecutionCustomLimitInput('');
                  }}
                >
                  Reset
                </button>
                <button
                  type="button"
                  className="btn-primary text-xs px-3 py-1.5"
                  disabled={!executionDirty || executionSaving}
                  onClick={applyExecutionDefaults}
                >
                  {executionSaving ? 'Applying...' : 'Apply defaults'}
                </button>
              </div>
            </div>

            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={config.allowDiskUse}
                onChange={e => updateConfig({ allowDiskUse: e.target.checked })}
                disabled={config.mode === 'safe'}
                className="ms-checkbox"
              />
              <span className="text-xs" style={{ color: config.mode === 'safe' ? 'var(--text-tertiary)' : 'var(--text-secondary)' }}>
                Allow Disk Use {config.mode === 'safe' && '(disabled in Safe Mode)'}
              </span>
            </label>

            {adminAccess && adminAccess.required ? (
              <div className="p-3 rounded-xl space-y-3" style={{ background: adminAccess.verified ? 'rgba(16,185,129,0.06)' : 'rgba(245,158,11,0.06)', border: adminAccess.verified ? '1px solid rgba(16,185,129,0.2)' : '1px solid rgba(245,158,11,0.25)' }}>
                <div className="flex items-center justify-between gap-2">
                  <div className="text-xs font-medium" style={{ color: 'var(--text-secondary)' }}>
                    Admin Access Key
                  </div>
                  {adminAccess.verified ? (
                    <span className="badge-green text-2xs">Verified</span>
                  ) : (
                    <span className="badge-yellow text-2xs">Required</span>
                  )}
                </div>
                {!adminAccess.verified ? (
                  <>
                    <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                      An admin access key is required to modify rate limits, governor settings, consoles, and server management. Enter the key configured on the server.
                    </div>
                    <div className="flex gap-2">
                      <input
                        type="password"
                        value={adminKeyInput}
                        onChange={(e) => { setAdminKeyInput(e.target.value); setAdminKeyError(''); }}
                        placeholder="Enter admin access key"
                        className="input-field text-xs font-mono flex-1"
                        style={{ background: 'var(--surface-3)', border: '1px solid var(--border)', color: 'var(--text-primary)', padding: '6px 10px', borderRadius: 8 }}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter' && adminKeyInput.trim()) {
                            setAdminKeySaving(true);
                            setAdminKeyError('');
                            api.verifyAdminAccess(adminKeyInput.trim())
                              .then((r) => {
                                onAdminAccessChange?.(r);
                                if (r.verified) { try { localStorage.setItem(ADMIN_KEY_STORAGE, adminKeyInput.trim()); } catch {} }
                                setAdminKeyInput('');
                              })
                              .catch((err) => setAdminKeyError(err?.message || 'Invalid key.'))
                              .finally(() => setAdminKeySaving(false));
                          }
                        }}
                      />
                      <button
                        type="button"
                        className="btn-primary text-xs px-3 py-1.5"
                        disabled={adminKeySaving || !adminKeyInput.trim()}
                        onClick={() => {
                          setAdminKeySaving(true);
                          setAdminKeyError('');
                          api.verifyAdminAccess(adminKeyInput.trim())
                            .then((r) => {
                              onAdminAccessChange?.(r);
                              if (r.verified) { try { localStorage.setItem(ADMIN_KEY_STORAGE, adminKeyInput.trim()); } catch {} }
                              setAdminKeyInput('');
                            })
                            .catch((err) => setAdminKeyError(err?.message || 'Invalid key.'))
                            .finally(() => setAdminKeySaving(false));
                        }}
                      >
                        {adminKeySaving ? 'Verifying...' : 'Unlock'}
                      </button>
                    </div>
                    {adminKeyError && <div className="text-2xs text-red-400">{adminKeyError}</div>}
                  </>
                ) : (
                  <div className="flex items-center justify-between">
                    <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                      Admin access verified. Consoles, server management, and rate limit settings are unlocked.
                    </div>
                    <button
                      type="button"
                      className="btn-ghost text-2xs px-2 py-1"
                      onClick={() => {
                        api.revokeAdminAccess()
                          .then((r) => {
                            onAdminAccessChange?.(r);
                            try { localStorage.removeItem(ADMIN_KEY_STORAGE); } catch {}
                          })
                          .catch(() => {});
                      }}
                    >
                      Revoke
                    </button>
                  </div>
                )}
              </div>
            ) : adminAccess ? (
              <div className="p-3 rounded-xl space-y-2" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
                <div className="flex items-center justify-between gap-2">
                  <div className="text-xs font-medium" style={{ color: 'var(--text-secondary)' }}>
                    Admin Access Key
                  </div>
                  <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Not configured</span>
                </div>
                <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                  No admin key is set on the server — all features are accessible without a key.
                  To enable access control, set <code className="font-mono" style={{ color: 'var(--text-secondary)' }}>ADMIN_ACCESS_KEY</code> when starting the container.
                </div>
                <input
                  type="password"
                  disabled
                  placeholder="Not configured on server"
                  className="input-field text-xs font-mono w-full"
                  style={{ opacity: 0.5 }}
                />
              </div>
            ) : null}
          </div>
        )}

        {activeSection === 'health' && (
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-3">
              {[
                { label: 'MongoDB', value: connectionInfo?.version || '-' },
                { label: 'Topology', value: connectionInfo?.topology?.kind || 'unknown' },
                { label: 'Role', value: connectionInfo?.topology?.role || '-' },
                { label: 'Read Preference', value: connectionInfo?.readPreference || 'primary' },
                { label: 'Production Guard', value: connectionInfo?.isProduction ? 'ON' : 'OFF' },
                { label: 'Server Uptime', value: health ? formatDuration(health.uptime * 1000) : '-' },
                { label: 'Active Sessions', value: health?.connections || '-' },
                { label: 'Server Memory', value: health?.memory ? `${Math.round(health.memory.rss / 1048576)}MB` : '-' },
                {
                  label: 'Databases',
                  value: effectiveDbSummary.databases === null
                    ? <span className="inline-flex items-center gap-1 text-xs"><Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />loading...</span>
                    : formatNumber(effectiveDbSummary.databases),
                },
                {
                  label: 'Collections',
                  value: effectiveDbSummary.collections === null
                    ? <span className="inline-flex items-center gap-1 text-xs"><Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />loading...</span>
                    : formatNumber(effectiveDbSummary.collections),
                },
                {
                  label: 'Documents',
                  value: effectiveDbSummary.documents === null
                    ? <span className="inline-flex items-center gap-1 text-xs"><Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />loading...</span>
                    : formatNumber(effectiveDbSummary.documents),
                },
                {
                  label: 'Total Size',
                  value: effectiveDbSummary.totalSize === null
                    ? <span className="inline-flex items-center gap-1 text-xs"><Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />loading...</span>
                    : formatBytes(effectiveDbSummary.totalSize || 0),
                },
              ].map(({ label, value }) => (
                <div key={label} className="p-3 rounded-xl" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
                  <div className="text-2xs mb-1" style={{ color: 'var(--text-tertiary)' }}>{label}</div>
                  <div className="text-sm font-medium font-mono truncate" style={{ color: 'var(--text-primary)' }}>{value}</div>
                </div>
              ))}
            </div>
            {effectiveDbSummary.loading && effectiveDbSummary.totalStats > 0 && (
              <div className="text-2xs inline-flex items-center gap-1.5" style={{ color:'var(--text-tertiary)' }}>
                <Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />
                Loading database stats {effectiveDbSummary.loadedStats}/{effectiveDbSummary.totalStats}
              </div>
            )}

            <div className="p-3 rounded-xl space-y-2" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Hosts</div>
              {hosts.length > 1 ? (
                <div>
                  <button
                    type="button"
                    className="w-full flex items-center justify-between rounded-lg px-2.5 py-1.5 text-xs font-mono transition-all"
                    style={{ background:'var(--surface-3)', border:'1px solid var(--border)', color:'var(--text-primary)' }}
                    onClick={() => setHostsExpanded((prev) => !prev)}
                  >
                    <span>{hosts.length} hosts in this cluster</span>
                    <ChevronDown className={`w-3.5 h-3.5 transition-transform ${hostsExpanded ? 'rotate-180' : ''}`} />
                  </button>
                  {hostsExpanded && (
                    <div className="mt-2 max-h-36 overflow-auto rounded-lg p-1.5 space-y-1" style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}>
                      {hosts.map((host) => (
                        <div key={host} className="px-2 py-1 rounded-md text-xs font-mono" style={{ color:'var(--text-secondary)' }}>
                          {host}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-xs font-mono truncate" style={{ color: 'var(--text-primary)' }}>
                  {hosts[0] || '-'}
                </div>
              )}
              {hosts.length > 1 && <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Read-only list of nodes for current connection.</div>}
            </div>

          </div>
        )}

        {activeSection === 'rateLimit' && (
          <div className="space-y-4">
            <div className="p-3 rounded-xl space-y-3" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', opacity: adminAccess && adminAccess.required && !adminAccess.verified ? 0.4 : 1, pointerEvents: adminAccess && adminAccess.required && !adminAccess.verified ? 'none' : 'auto' }}>
              <div className="flex items-center justify-between gap-2">
                <div className="text-xs font-medium" style={{ color: 'var(--text-secondary)' }}>
                  Global Rate Limit
                </div>
                {serviceSaving && <span className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Saving...</span>}
              </div>
              <div className="grid grid-cols-3 gap-2">
                <div>
                  <div className="text-2xs mb-1" style={{ color: 'var(--text-tertiary)' }}>Window (sec)</div>
                  <input
                    type="number"
                    min={1}
                    max={900}
                    value={Math.round((rateDraft.windowMs || 60000) / 1000)}
                    onChange={(event) => {
                      const sec = Math.max(1, Math.min(parseInt(event.target.value || '60', 10) || 60, 900));
                      setRateDraft((prev) => ({ ...prev, windowMs: sec * 1000 }));
                    }}
                    className="ms-number w-full text-xs px-2 py-1.5 rounded-lg font-mono"
                    style={{background:'var(--surface-3)',border:'1px solid var(--border)',color:'var(--text-primary)'}}
                  />
                </div>
                <div>
                  <div className="text-2xs mb-1" style={{ color: 'var(--text-tertiary)' }}>API req/window</div>
                  <input
                    type="number"
                    min={10}
                    max={100000}
                    value={rateDraft.apiMax || 3000}
                    onChange={(event) => {
                      const value = Math.max(10, Math.min(parseInt(event.target.value || '3000', 10) || 3000, 100000));
                      setRateDraft((prev) => ({ ...prev, apiMax: value }));
                    }}
                    className="ms-number w-full text-xs px-2 py-1.5 rounded-lg font-mono"
                    style={{background:'var(--surface-3)',border:'1px solid var(--border)',color:'var(--text-primary)'}}
                  />
                </div>
                <div>
                  <div className="text-2xs mb-1" style={{ color: 'var(--text-tertiary)' }}>Heavy req/window</div>
                  <input
                    type="number"
                    min={1}
                    max={10000}
                    value={rateDraft.heavyMax || 300}
                    onChange={(event) => {
                      const value = Math.max(1, Math.min(parseInt(event.target.value || '300', 10) || 300, 10000));
                      setRateDraft((prev) => ({ ...prev, heavyMax: value }));
                    }}
                    className="ms-number w-full text-xs px-2 py-1.5 rounded-lg font-mono"
                    style={{background:'var(--surface-3)',border:'1px solid var(--border)',color:'var(--text-primary)'}}
                  />
                </div>
              </div>
              <div className="flex justify-end">
                <button
                  type="button"
                  className="btn-primary text-xs px-3 py-1.5"
                  disabled={serviceSaving}
                  onClick={() => updateServiceConfig({ rateLimit: rateDraft })}
                >
                  {serviceSaving ? 'Applying...' : 'Apply'}
                </button>
              </div>
              {serviceError && (
                <div className="text-2xs text-red-400">{serviceError}</div>
              )}
              <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>
                This setting is service-wide and persisted on the backend.
              </div>
            </div>
          </div>
        )}

        {activeSection === 'display' && (
          <div className="space-y-4">
            <div className="p-3 rounded-xl" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={Boolean(displaySettings.showTopTags)}
                  onChange={(event) => onDisplaySettingsChange?.({ showTopTags: event.target.checked })}
                  className="ms-checkbox"
                />
                <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>
                  Show tags in top bar
                </span>
              </label>
              <label className="mt-2 flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={Boolean(displaySettings.showReadSourceTag)}
                  onChange={(event) => onDisplaySettingsChange?.({ showReadSourceTag: event.target.checked })}
                  className="ms-checkbox"
                />
                <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>
                  Show read source tag near host
                </span>
              </label>
              <p className="text-2xs mt-2" style={{ color: 'var(--text-tertiary)' }}>
                This toggle controls only the inline badge next to host. Host hover details always show the current read source.
              </p>
            </div>
          </div>
        )}
      </div>
    </AppModal>
  );
}

import React, { useState, useEffect, useMemo } from 'react';
import api from '../utils/api';
import { X, Shield, ShieldOff, Settings, Cpu, Eye, ChevronDown, Loader } from './Icons';
import { formatBytes, formatDuration, formatNumber } from '../utils/formatters';
import AppModal from './modals/AppModal';

export default function SettingsModal({
  execMode,
  onModeChangeRequest,
  onConfigApplied,
  onClose,
  connectionInfo,
  displaySettings = { showTopTags: false },
  onDisplaySettingsChange,
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
  const [rateDraft, setRateDraft] = useState({ windowMs: 60000, apiMax: 3000, heavyMax: 300 });
  const [dbSummary, setDbSummary] = useState(EMPTY_DB_SUMMARY);
  const [hostsExpanded, setHostsExpanded] = useState(false);
  const [activeSection, setActiveSection] = useState('execution');
  const hosts = useMemo(
    () => String(connectionInfo?.host || '').split(',').map((item) => item.trim()).filter(Boolean),
    [connectionInfo?.host],
  );

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
    if (activeSection !== 'health') return;
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
  }, [activeSection, connectionInfo?.connectionId]);

  const updateConfig = async (changes) => {
    try {
      const c = await api.setExecutionConfig(changes);
      setConfig(c);
      if (changes.mode) onConfigApplied?.(c.mode);
    } catch (err) {
      console.error(err);
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

  const previewTimeout = (value) => {
    setConfig(prev => (prev ? { ...prev, maxTimeMS: value } : prev));
  };

  const commitTimeout = () => {
    if (!config || config.mode === 'power') return;
    updateConfig({ maxTimeMS: config.maxTimeMS });
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
        {[{ id: 'execution', label: 'Execution', icon: Shield }, { id: 'display', label: 'Display', icon: Eye }, { id: 'health', label: 'Server Info', icon: Cpu }].map(t => (
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
              <label className="block text-xs font-medium mb-1" style={{ color: 'var(--text-secondary)' }}>
                Query Timeout <span className="text-2xs font-normal" style={{ color: 'var(--text-tertiary)' }}>({config.maxTimeMS}ms)</span>
              </label>
              <input
                type="range"
                min="0"
                max="120000"
                step="1000"
                value={config.maxTimeMS}
                onChange={e => previewTimeout(parseInt(e.target.value, 10))}
                onMouseUp={commitTimeout}
                onTouchEnd={commitTimeout}
                onKeyUp={e => { if (e.key.startsWith('Arrow')) commitTimeout(); }}
                className="w-full ms-range"
                disabled={config.mode === 'power'}
              />
              <div className="flex justify-between text-2xs mt-1" style={{ color: 'var(--text-tertiary)' }}>
                <span>No limit</span><span>30s</span><span>60s</span><span>120s</span>
              </div>
              {config.mode === 'power' && <p className="text-2xs mt-1 text-amber-400">Disabled in Power Mode.</p>}
            </div>

            <div>
              <label className="block text-xs font-medium mb-1" style={{ color: 'var(--text-secondary)' }}>Max Result Size</label>
              <select
                value={config.maxResultSize}
                onChange={e => updateConfig({ maxResultSize: parseInt(e.target.value, 10) })}
                className="ms-select w-full text-xs"
                disabled={config.mode === 'power'}
              >
                <option value="100">100 documents</option>
                <option value="500">500 documents</option>
                <option value="1000">1,000 documents</option>
                <option value="5000">5,000 documents</option>
                <option value="10000">10,000 documents</option>
                <option value="50000">50,000 documents</option>
              </select>
              {config.mode === 'power' && <p className="text-2xs mt-1 text-amber-400">Disabled in Power Mode.</p>}
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
                  value: dbSummary.databases === null
                    ? <span className="inline-flex items-center gap-1 text-xs"><Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />loading...</span>
                    : formatNumber(dbSummary.databases),
                },
                {
                  label: 'Collections',
                  value: dbSummary.collections === null
                    ? <span className="inline-flex items-center gap-1 text-xs"><Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />loading...</span>
                    : formatNumber(dbSummary.collections),
                },
                {
                  label: 'Documents',
                  value: dbSummary.documents === null
                    ? <span className="inline-flex items-center gap-1 text-xs"><Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />loading...</span>
                    : formatNumber(dbSummary.documents),
                },
                {
                  label: 'Total Size',
                  value: dbSummary.totalSize === null
                    ? <span className="inline-flex items-center gap-1 text-xs"><Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />loading...</span>
                    : formatBytes(dbSummary.totalSize || 0),
                },
              ].map(({ label, value }) => (
                <div key={label} className="p-3 rounded-xl" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
                  <div className="text-2xs mb-1" style={{ color: 'var(--text-tertiary)' }}>{label}</div>
                  <div className="text-sm font-medium font-mono truncate" style={{ color: 'var(--text-primary)' }}>{value}</div>
                </div>
              ))}
            </div>
            {dbSummary.loading && dbSummary.totalStats > 0 && (
              <div className="text-2xs inline-flex items-center gap-1.5" style={{ color:'var(--text-tertiary)' }}>
                <Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />
                Loading database stats {dbSummary.loadedStats}/{dbSummary.totalStats}
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

            <div className="p-3 rounded-xl space-y-3" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
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
                    className="w-full text-xs px-2 py-1.5 rounded-lg font-mono"
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
                    className="w-full text-xs px-2 py-1.5 rounded-lg font-mono"
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
                    className="w-full text-xs px-2 py-1.5 rounded-lg font-mono"
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
              <p className="text-2xs mt-2" style={{ color: 'var(--text-tertiary)' }}>
                Tags are always available in the host dropdown. Top-bar tags are hidden by default.
              </p>
            </div>
          </div>
        )}
      </div>
    </AppModal>
  );
}

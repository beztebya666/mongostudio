import React, { useState, useEffect } from 'react';
import api from '../utils/api';
import { X, Shield, ShieldOff, Settings, Cpu } from './Icons';
import { formatDuration } from '../utils/formatters';
import AppModal from './modals/AppModal';

export default function SettingsModal({ execMode, onModeChangeRequest, onConfigApplied, onClose, connectionInfo }) {
  const [config, setConfig] = useState(null);
  const [health, setHealth] = useState(null);
  const [activeSection, setActiveSection] = useState('execution');

  useEffect(() => {
    api.getExecutionConfig().then(setConfig).catch(() => {});
    api.getHealth().then(setHealth).catch(() => {});
  }, []);

  useEffect(() => {
    setConfig(prev => (prev ? { ...prev, mode: execMode } : prev));
  }, [execMode]);

  const updateConfig = async (changes) => {
    try {
      const c = await api.setExecutionConfig(changes);
      setConfig(c);
      if (changes.mode) onConfigApplied?.(c.mode);
    } catch (err) {
      console.error(err);
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
        {[{ id: 'execution', label: 'Execution', icon: Shield }, { id: 'health', label: 'Server Info', icon: Cpu }].map(t => (
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
                className="w-full accent-emerald-500"
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
                className="w-full text-xs px-3 py-2 rounded-lg"
                style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
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
                className="rounded"
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
                { label: 'Host', value: connectionInfo?.host || '-' },
                { label: 'Production Guard', value: connectionInfo?.isProduction ? 'ON' : 'OFF' },
                { label: 'Server Uptime', value: health ? formatDuration(health.uptime * 1000) : '-' },
                { label: 'Active Sessions', value: health?.connections || '-' },
                { label: 'Server Memory', value: health?.memory ? `${Math.round(health.memory.rss / 1048576)}MB` : '-' },
              ].map(({ label, value }) => (
                <div key={label} className="p-3 rounded-xl" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
                  <div className="text-2xs mb-1" style={{ color: 'var(--text-tertiary)' }}>{label}</div>
                  <div className="text-sm font-medium font-mono truncate" style={{ color: 'var(--text-primary)' }}>{value}</div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </AppModal>
  );
}

import React, { useState, useEffect, useCallback, useMemo } from 'react';
import api from '../utils/api';
import { Layers, Loader, Refresh, Hash, ArrowUp, ArrowDown } from './Icons';
import InlineAlert from './InlineAlert';
import DropdownSelect from './DropdownSelect';
import { formatNumber } from '../utils/formatters';

const SAFE_QUERY_LIMIT_MAX = 1000;
const POWER_QUERY_LIMIT_PRESET_MAX = 5000;
const QUERY_LIMIT_OVERRIDE_MAX = 50000;
const SCHEMA_SAMPLE_MAX = QUERY_LIMIT_OVERRIDE_MAX;
const BASE_SCHEMA_SAMPLE_OPTIONS = [50, 100, 200, 500, 1000, POWER_QUERY_LIMIT_PRESET_MAX, SCHEMA_SAMPLE_MAX];
const DEFAULT_QUERY_TIMEOUT_MS = 5000;
const DEFAULT_QUERY_LIMIT = 50;
const SAFE_QUERY_TIMEOUT_MAX_MS = 30000;
const POWER_QUERY_TIMEOUT_PRESET_MAX_MS = 120000;
const POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS = 2147483000;
const POWER_QUERY_TIMEOUT_MAX_MS = POWER_QUERY_TIMEOUT_CUSTOM_MAX_MS;
const QUERY_TIMEOUT_MAX_MS = POWER_QUERY_TIMEOUT_MAX_MS;
const QUERY_TIMEOUT_OPTIONS = [5000, 10000, 30000, 60000, POWER_QUERY_TIMEOUT_PRESET_MAX_MS];

const clampTimeoutMs = (value) => Math.max(DEFAULT_QUERY_TIMEOUT_MS, Math.min(Number(value) || DEFAULT_QUERY_TIMEOUT_MS, QUERY_TIMEOUT_MAX_MS));
const getModeLimitMax = () => SCHEMA_SAMPLE_MAX;
const getModeTimeoutMax = (mode = 'safe') => (mode === 'power' ? POWER_QUERY_TIMEOUT_MAX_MS : SAFE_QUERY_TIMEOUT_MAX_MS);
const clampLimitValue = (value, mode = 'safe') => Math.max(DEFAULT_QUERY_LIMIT, Math.min(Number(value) || DEFAULT_QUERY_LIMIT, getModeLimitMax(mode)));
const formatSampleOptionLabel = (value) => `${formatNumber(Math.min(value, SCHEMA_SAMPLE_MAX))} docs`;
const formatTimeoutOptionLabel = (value) => `${Math.round(value / 1000)}s`;

export default function SchemaView({ db, collection, refreshToken = 0 }) {
  const [schema, setSchema] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [sampleSize, setSampleSize] = useState(DEFAULT_QUERY_LIMIT);
  const [queryTimeoutMs, setQueryTimeoutMs] = useState(DEFAULT_QUERY_TIMEOUT_MS);
  const [customSampleInput, setCustomSampleInput] = useState('');
  const [customTimeoutInput, setCustomTimeoutInput] = useState('');
  const [sortState, setSortState] = useState({ key: 'coverage', dir: 'desc' });
  const [execConfig, setExecConfig] = useState(null);
  const executionMode = execConfig?.mode === 'power' ? 'power' : 'safe';
  const modeTimeoutMax = getModeTimeoutMax(executionMode);
  const defaultBudget = useMemo(() => ({
    timeoutMs: Math.min(clampTimeoutMs(execConfig?.maxTimeMS), modeTimeoutMax),
    limit: clampLimitValue(execConfig?.maxResultSize, executionMode),
  }), [execConfig, executionMode, modeTimeoutMax]);
  const sampleOptions = useMemo(() => BASE_SCHEMA_SAMPLE_OPTIONS, []);
  const timeoutOptions = useMemo(() => QUERY_TIMEOUT_OPTIONS, []);
  const sampleSelectOptions = useMemo(
    () => [
      ...sampleOptions.map((value) => ({
        value: String(value),
        label: formatSampleOptionLabel(value),
        disabled: executionMode !== 'power' && value > SAFE_QUERY_LIMIT_MAX,
      })),
      { value: 'custom', label: 'Custom' },
    ],
    [executionMode, sampleOptions],
  );
  const timeoutSelectOptions = useMemo(
    () => [
      ...timeoutOptions.map((value) => ({
        value: String(value),
        label: formatTimeoutOptionLabel(value),
        disabled: executionMode !== 'power' && value > SAFE_QUERY_TIMEOUT_MAX_MS,
      })),
      { value: 'custom', label: 'Custom' },
    ],
    [executionMode, timeoutOptions],
  );
  const sampleIsCustom = !sampleOptions.includes(sampleSize);
  const timeoutIsCustom = !timeoutOptions.includes(queryTimeoutMs);

  const load = useCallback(async ({ sample = sampleSize, timeoutMs = queryTimeoutMs } = {}) => {
    setLoading(true); setError(null);
    try {
      const safeSample = clampLimitValue(sample, executionMode);
      const safeTimeout = Math.min(clampTimeoutMs(timeoutMs), modeTimeoutMax);
      const data = await api.getSchema(db, collection, safeSample, { budget: { timeoutMs: safeTimeout, limit: defaultBudget.limit } });
      setSchema(data);
    } catch (err) { setError(err.message); }
    finally { setLoading(false); }
  }, [db, collection, sampleSize, queryTimeoutMs, defaultBudget.limit, executionMode, modeTimeoutMax]);

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
    setSampleSize((prev) => {
      const next = Math.min(clampLimitValue(defaultBudget.limit, executionMode), POWER_QUERY_LIMIT_PRESET_MAX);
      return prev === next ? prev : next;
    });
    setQueryTimeoutMs((prev) => {
      const next = Math.min(clampTimeoutMs(defaultBudget.timeoutMs), modeTimeoutMax);
      return prev === next ? prev : next;
    });
    setCustomSampleInput('');
    setCustomTimeoutInput('');
  }, [defaultBudget.limit, defaultBudget.timeoutMs, db, collection, executionMode, modeTimeoutMax]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { if (refreshToken > 0) load(); }, [refreshToken]);

  const typeColor = (type) => {
    const map = { string:'var(--json-string)', number:'var(--json-number)', boolean:'var(--json-boolean)',
      ObjectId:'var(--json-objectid)', Date:'var(--json-number)', null:'var(--json-null)',
      object:'var(--json-bracket)', array:'var(--json-key)' };
    return map[type] || 'var(--text-secondary)';
  };

  const toggleSort = (key) => {
    setSortState((prev) => {
      if (prev.key === key) return { key, dir: prev.dir === 'asc' ? 'desc' : 'asc' };
      return { key, dir: key === 'field' ? 'asc' : 'desc' };
    });
  };

  const sortedFields = useMemo(() => {
    const fields = [...(schema?.fields || [])];
    const dir = sortState.dir === 'asc' ? 1 : -1;
    const getPrimaryType = (field) => field.types?.[0]?.type || '';
    fields.sort((a, b) => {
      if (sortState.key === 'field') return a.path.localeCompare(b.path) * dir;
      if (sortState.key === 'types') return getPrimaryType(a).localeCompare(getPrimaryType(b)) * dir;
      if (sortState.key === 'coverage') return ((a.pct || 0) - (b.pct || 0)) * dir;
      if (sortState.key === 'sample') return String(a.sample || '').localeCompare(String(b.sample || '')) * dir;
      return 0;
    });
    return fields;
  }, [schema?.fields, sortState]);

  const sortIcon = (key) => {
    if (sortState.key !== key) return null;
    return sortState.dir === 'asc'
      ? <ArrowUp className="w-3 h-3" style={{ color:'var(--accent)' }} />
      : <ArrowDown className="w-3 h-3" style={{ color:'var(--accent)' }} />;
  };

  return (
    <div className="h-full flex flex-col">
      <div className="flex-shrink-0 px-4 py-3 flex items-center justify-between" style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
        <div className="flex items-center gap-3">
          <h3 className="text-sm font-semibold flex items-center gap-2" style={{color:'var(--text-primary)'}}>
            <Layers className="w-4 h-4" style={{color:'var(--accent)'}} /> Schema Analysis
          </h3>
          {schema && <span className="badge-blue">{schema.fields?.length} fields</span>}
        </div>
        <div className="flex items-center gap-2">
          <DropdownSelect
            value={sampleIsCustom || customSampleInput !== '' ? 'custom' : String(sampleSize)}
            options={sampleSelectOptions}
            onChange={(nextValue) => {
              const value = String(nextValue);
              if (value === 'custom') {
                setCustomSampleInput(String(sampleSize));
                return;
              }
              setCustomSampleInput('');
              setSampleSize(clampLimitValue(parseInt(value, 10), executionMode));
            }}
            sizeClassName="text-xs"
            title="Schema sample size"
          />
          {(sampleIsCustom || customSampleInput !== '') && (
            <>
              <input
                type="number"
                min={50}
                max={SCHEMA_SAMPLE_MAX}
                step={1}
                value={customSampleInput}
                onChange={(event) => setCustomSampleInput(event.target.value)}
                className="ms-number w-24 px-2 py-1 rounded-md text-2xs font-mono"
                style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                placeholder="docs"
              />
              <button
                type="button"
                className="btn-ghost text-2xs px-2 py-1"
                onClick={() => {
                  const parsed = Number(customSampleInput);
                  if (!Number.isFinite(parsed)) return;
                  setSampleSize(clampLimitValue(Math.round(parsed), executionMode));
                }}
              >
                Set
              </button>
            </>
          )}
          <DropdownSelect
            value={timeoutIsCustom || customTimeoutInput !== '' ? 'custom' : String(queryTimeoutMs)}
            options={timeoutSelectOptions}
            onChange={(nextValue) => {
              const value = String(nextValue);
              if (value === 'custom') {
                setCustomTimeoutInput(String(Math.max(5, Math.round(queryTimeoutMs / 1000))));
                return;
              }
              setCustomTimeoutInput('');
              setQueryTimeoutMs(Math.min(clampTimeoutMs(parseInt(value, 10)), modeTimeoutMax));
            }}
            sizeClassName="text-xs"
            title="Schema query timeout"
          />
          {(timeoutIsCustom || customTimeoutInput !== '') && (
            <>
              <input
                type="number"
                min={5}
                max={Math.round(modeTimeoutMax / 1000)}
                step={1}
                value={customTimeoutInput}
                onChange={(event) => setCustomTimeoutInput(event.target.value)}
                className="ms-number w-24 px-2 py-1 rounded-md text-2xs font-mono"
                style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                placeholder="seconds"
              />
              <button
                type="button"
                className="btn-ghost text-2xs px-2 py-1"
                onClick={() => {
                  const parsed = Number(customTimeoutInput);
                  if (!Number.isFinite(parsed)) return;
                  setQueryTimeoutMs(Math.min(clampTimeoutMs(Math.round(parsed * 1000)), modeTimeoutMax));
                }}
              >
                Set
              </button>
            </>
          )}
          <button onClick={() => load()} className="btn-ghost p-1.5" title="Apply schema sample settings">
            <Refresh className={`w-3.5 h-3.5 ${loading?'animate-spin':''}`} />
          </button>
        </div>
      </div>
      <div className="px-4 py-1.5 text-2xs" style={{ borderBottom:'1px solid var(--border)', background:'var(--surface-1)', color:'var(--text-tertiary)' }}>
        Higher sample sizes improve coverage but can be heavier on production.
      </div>

      {error && (
        <InlineAlert kind="error" message={error} onClose={() => setError(null)} className="mx-4 mt-3" />
      )}

      <div className="flex-1 overflow-auto">
        {loading && !schema ? (
          <div className="flex items-center justify-center h-full"><Loader style={{color:'var(--accent)'}} /></div>
        ) : schema?.fields?.length > 0 ? (
          <div style={{borderBottom:'1px solid var(--border)'}}>
            {/* Header */}
            <div className="grid grid-cols-12 gap-2 px-4 py-2 text-2xs font-medium uppercase tracking-wider" style={{color:'var(--text-tertiary)',borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
              <button className="col-span-4 flex items-center gap-1 text-left px-1 py-0.5 rounded-md transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]" onClick={() => toggleSort('field')}>
                Field Path {sortIcon('field')}
              </button>
              <button className="col-span-4 flex items-center gap-1 text-left px-1 py-0.5 rounded-md transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]" onClick={() => toggleSort('types')}>
                Types {sortIcon('types')}
              </button>
              <button className="col-span-2 flex items-center gap-1 text-left px-1 py-0.5 rounded-md transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]" onClick={() => toggleSort('coverage')}>
                Coverage {sortIcon('coverage')}
              </button>
              <button className="col-span-2 flex items-center gap-1 text-left px-1 py-0.5 rounded-md transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]" onClick={() => toggleSort('sample')}>
                Sample {sortIcon('sample')}
              </button>
            </div>
            {sortedFields.map((f, i) => (
              <div key={f.path} className="grid grid-cols-12 gap-2 px-4 py-2.5 text-xs transition-colors items-center"
                style={{borderBottom:'1px solid var(--border)'}}
                onMouseOver={e=>e.currentTarget.style.background='var(--surface-1)'}
                onMouseOut={e=>e.currentTarget.style.background='transparent'}>
                <div className="col-span-4 font-mono font-medium truncate" style={{color:'var(--text-primary)',paddingLeft:`${f.path.split('.').length*8-8}px`}}>
                  {f.path.includes('.') ? '.' + f.path.split('.').pop() : f.path}
                </div>
                <div className="col-span-4 flex flex-wrap gap-1">
                  {f.types.map(t => (
                    <span key={t.type} className="px-1.5 py-0.5 rounded text-2xs font-mono" style={{color:typeColor(t.type),background:`${typeColor(t.type)}15`,border:`1px solid ${typeColor(t.type)}30`}}>
                      {t.type} {t.pct < 100 && `(${t.pct}%)`}
                    </span>
                  ))}
                </div>
                <div className="col-span-2">
                  <div className="flex items-center gap-2">
                    <div className="flex-1 h-1.5 rounded-full overflow-hidden" style={{background:'var(--surface-3)'}}>
                      <div className="h-full rounded-full" style={{width:`${f.pct}%`,background:f.pct===100?'var(--accent)':f.pct>80?'#fbbf24':'#f87171'}} />
                    </div>
                    <span className="text-2xs w-8 text-right" style={{color:'var(--text-tertiary)'}}>{f.pct}%</span>
                  </div>
                </div>
                <div className="col-span-2 truncate text-2xs font-mono" style={{color:'var(--text-tertiary)'}}>
                  {f.sample || '—'}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="flex items-center justify-center h-full" style={{color:'var(--text-tertiary)'}}>
            <div className="text-center"><Hash className="w-8 h-8 mx-auto mb-2 opacity-30" /><p className="text-sm">No schema data available</p></div>
          </div>
        )}
      </div>
    </div>
  );
}

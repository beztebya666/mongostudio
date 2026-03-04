import React, { useState, useEffect, useCallback } from 'react';
import api from '../utils/api';
import { Layers, Loader, Refresh, Hash, AlertCircle } from './Icons';

export default function SchemaView({ db, collection, refreshToken = 0 }) {
  const [schema, setSchema] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [sampleSize, setSampleSize] = useState(100);

  const load = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const data = await api.getSchema(db, collection, sampleSize);
      setSchema(data);
    } catch (err) { setError(err.message); }
    finally { setLoading(false); }
  }, [db, collection, sampleSize]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { if (refreshToken > 0) load(); }, [refreshToken]);

  const typeColor = (type) => {
    const map = { string:'var(--json-string)', number:'var(--json-number)', boolean:'var(--json-boolean)',
      ObjectId:'var(--json-objectid)', Date:'var(--json-number)', null:'var(--json-null)',
      object:'var(--json-bracket)', array:'var(--json-key)' };
    return map[type] || 'var(--text-secondary)';
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
          <select value={sampleSize} onChange={e=>setSampleSize(parseInt(e.target.value))}
            className="text-xs px-2 py-1 rounded-lg" style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}}>
            <option value="50">50 docs</option><option value="100">100 docs</option>
            <option value="200">200 docs</option><option value="500">500 docs</option>
          </select>
          <button onClick={load} className="btn-ghost p-1.5"><Refresh className={`w-3.5 h-3.5 ${loading?'animate-spin':''}`} /></button>
        </div>
      </div>

      {error && (
        <div className="mx-4 mt-3 flex items-start gap-2 text-red-400 text-xs p-3 rounded-lg" style={{background:'rgba(239,68,68,0.05)',border:'1px solid rgba(239,68,68,0.2)'}}>
          <AlertCircle className="w-3.5 h-3.5 mt-0.5" /><span>{error}</span>
        </div>
      )}

      <div className="flex-1 overflow-auto">
        {loading && !schema ? (
          <div className="flex items-center justify-center h-full"><Loader style={{color:'var(--accent)'}} /></div>
        ) : schema?.fields?.length > 0 ? (
          <div style={{borderBottom:'1px solid var(--border)'}}>
            {/* Header */}
            <div className="grid grid-cols-12 gap-2 px-4 py-2 text-2xs font-medium uppercase tracking-wider" style={{color:'var(--text-tertiary)',borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
              <div className="col-span-4">Field Path</div>
              <div className="col-span-4">Types</div>
              <div className="col-span-2">Coverage</div>
              <div className="col-span-2">Sample</div>
            </div>
            {schema.fields.map((f, i) => (
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

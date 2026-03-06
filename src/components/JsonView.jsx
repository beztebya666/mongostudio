import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { ChevronRight, ChevronDown } from './Icons';

function collectNodePaths(value, path = '$', out = []) {
  if (!value || typeof value !== 'object') return out;
  out.push(path);
  if (Array.isArray(value)) {
    value.forEach((item, index) => collectNodePaths(item, `${path}[${index}]`, out));
    return out;
  }
  Object.entries(value).forEach(([key, child]) => collectNodePaths(child, `${path}.${key}`, out));
  return out;
}

export default function JsonView({ data, showControls = false, expandDepth = 2, externalToggle = null }) {
  const [openMap, setOpenMap] = useState({});
  const nodePaths = useMemo(() => collectNodePaths(data), [data]);

  const isOpen = useCallback((path, depth) => {
    if (Object.prototype.hasOwnProperty.call(openMap, path)) return openMap[path];
    return depth < expandDepth;
  }, [openMap, expandDepth]);

  const toggleNode = useCallback((path, fallback) => {
    setOpenMap((prev) => ({
      ...prev,
      [path]: !(Object.prototype.hasOwnProperty.call(prev, path) ? prev[path] : fallback),
    }));
  }, []);

  const setAllNodes = useCallback((open) => {
    const next = {};
    for (const path of nodePaths) next[path] = open;
    setOpenMap(next);
  }, [nodePaths]);

  useEffect(() => {
    if (!externalToggle || typeof externalToggle.open !== 'boolean') return;
    setAllNodes(externalToggle.open);
  }, [externalToggle?.version, externalToggle?.open, setAllNodes]);

  const renderValue = (value, depth = 0, path = '$') => {
    if (value === null) return <span className="json-null">null</span>;
    if (value === undefined) return <span className="json-null">undefined</span>;
    if (typeof value === 'boolean') return <span className="json-boolean">{String(value)}</span>;
    if (typeof value === 'number') return <span className="json-number">{value}</span>;
    if (typeof value === 'string') {
      if (/^[a-f0-9]{24}$/i.test(value)) return <span className="json-objectid">ObjectId("{value}")</span>;
      if (value.length > 200) return <span className="json-string">"{value.slice(0, 200)}…"</span>;
      return <span className="json-string">"{value}"</span>;
    }
    if (value && typeof value === 'object' && value.$oid) return <span className="json-objectid">ObjectId("{value.$oid}")</span>;
    if (value && typeof value === 'object' && value.$date) return <span className="json-number">ISODate("{typeof value.$date === 'string' ? value.$date : new Date(value.$date).toISOString()}")</span>;
    if (value && typeof value === 'object' && value.$numberDecimal) return <span className="json-number">Decimal128("{value.$numberDecimal}")</span>;
    if (value && typeof value === 'object' && value.$numberLong) return <span className="json-number">NumberLong({value.$numberLong})</span>;

    if (Array.isArray(value)) {
      if (value.length === 0) return <span className="json-bracket">[]</span>;
      const open = isOpen(path, depth);
      return (
        <span>
          <button type="button" className="json-bracket inline-flex items-center gap-0.5 bg-transparent border-none p-0 cursor-pointer" onClick={() => toggleNode(path, depth < expandDepth)}>
            {open ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
            [ {!open && <span className="text-2xs ml-0.5" style={{ color:'var(--text-tertiary)' }}>{value.length} items</span>}
            {!open && <span className="json-bracket"> ]</span>}
          </button>
          {open && (
            <>
              <div style={{ paddingLeft: '1.25rem' }}>
                {value.map((item, index) => (
                  <div key={`${path}:${index}`} className="leading-relaxed">
                    {renderValue(item, depth + 1, `${path}[${index}]`)}
                    {index < value.length - 1 && <span className="json-comma">,</span>}
                  </div>
                ))}
              </div>
              <span className="json-bracket">]</span>
            </>
          )}
        </span>
      );
    }

    if (typeof value === 'object') {
      const entries = Object.entries(value);
      if (entries.length === 0) return <span className="json-bracket">{'{}'}</span>;
      const open = isOpen(path, depth);
      return (
        <span>
          <button type="button" className="json-bracket inline-flex items-center gap-0.5 bg-transparent border-none p-0 cursor-pointer" onClick={() => toggleNode(path, depth < expandDepth)}>
            {open ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
            {'{ '}
            {!open && <span className="text-2xs ml-0.5" style={{ color:'var(--text-tertiary)' }}>{entries.length} fields</span>}
            {!open && <span className="json-bracket">{' }'}</span>}
          </button>
          {open && (
            <>
              <div style={{ paddingLeft: '1.25rem' }}>
                {entries.map(([key, child], index) => (
                  <div key={`${path}.${key}`} className="leading-relaxed">
                    <span className="json-key">"{key}"</span>
                    <span className="json-comma">: </span>
                    {renderValue(child, depth + 1, `${path}.${key}`)}
                    {index < entries.length - 1 && <span className="json-comma">,</span>}
                  </div>
                ))}
              </div>
              <span className="json-bracket">{'}'}</span>
            </>
          )}
        </span>
      );
    }

    return <span style={{ color:'var(--text-secondary)' }}>{String(value)}</span>;
  };

  return (
    <div>
      {showControls && nodePaths.length > 0 && (
        <div className="mb-2 flex items-center gap-1.5 text-2xs">
          <button type="button" onClick={() => setAllNodes(true)} className="btn-ghost px-2 py-1">
            Expand all
          </button>
          <button type="button" onClick={() => setAllNodes(false)} className="btn-ghost px-2 py-1">
            Collapse all
          </button>
        </div>
      )}
      <pre className="font-mono text-xs leading-relaxed whitespace-pre-wrap">
        {renderValue(data, 0, '$')}
      </pre>
    </div>
  );
}

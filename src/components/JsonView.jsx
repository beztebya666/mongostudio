import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ChevronRight, ChevronDown } from './Icons';

const NODE_CAP = 5000;
const AUTO_EXPAND_CHILD_THRESHOLD = 60;
const CHILD_RENDER_CHUNK = 120;

function collectNodePaths(value, path = '$', out = [], limit = NODE_CAP) {
  if (out.length >= limit) return out;
  if (!value || typeof value !== 'object') return out;
  out.push(path);
  if (out.length >= limit) return out;
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      if (out.length >= limit) break;
      collectNodePaths(value[index], `${path}[${index}]`, out, limit);
    }
    return out;
  }
  const entries = Object.entries(value);
  for (let i = 0; i < entries.length; i += 1) {
    if (out.length >= limit) break;
    const [key, child] = entries[i];
    collectNodePaths(child, `${path}.${key}`, out, limit);
  }
  return out;
}

export default function JsonView({ data, showControls = false, expandDepth = 2, externalToggle = null }) {
  const [openMap, setOpenMap] = useState({});
  const [childWindowMap, setChildWindowMap] = useState({});
  const nodePaths = useMemo(() => collectNodePaths(data, '$', [], NODE_CAP), [data]);
  const nodesCapped = nodePaths.length >= NODE_CAP;
  const renderCounterRef = useRef(0);
  const renderTruncatedRef = useRef(false);

  const defaultOpenForValue = useCallback((depth, value) => {
    if (depth >= expandDepth) return false;
    if (!value || typeof value !== 'object') return depth < expandDepth;
    const childCount = Array.isArray(value) ? value.length : Object.keys(value).length;
    return childCount <= AUTO_EXPAND_CHILD_THRESHOLD;
  }, [expandDepth]);

  const isOpen = useCallback((path, depth, value) => {
    if (Object.prototype.hasOwnProperty.call(openMap, path)) return openMap[path];
    return defaultOpenForValue(depth, value);
  }, [openMap, defaultOpenForValue]);

  const ensureChildWindow = useCallback((path, size) => {
    if (!Number.isFinite(size) || size <= 0) return;
    setChildWindowMap((prev) => {
      const current = Number(prev[path] || 0);
      if (current >= size || current >= CHILD_RENDER_CHUNK) return prev;
      return { ...prev, [path]: Math.min(size, CHILD_RENDER_CHUNK) };
    });
  }, []);

  const toggleNode = useCallback((path, fallback, childCount = 0) => {
    setOpenMap((prev) => {
      const current = Object.prototype.hasOwnProperty.call(prev, path) ? prev[path] : fallback;
      const nextOpen = !current;
      return { ...prev, [path]: nextOpen };
    });
    if (childCount > 0) ensureChildWindow(path, childCount);
  }, [ensureChildWindow]);

  const setAllNodes = useCallback((open) => {
    const next = {};
    for (const path of nodePaths) next[path] = open;
    setOpenMap(next);
    if (open) {
      const nextWindow = {};
      for (const path of nodePaths) nextWindow[path] = CHILD_RENDER_CHUNK;
      setChildWindowMap(nextWindow);
    }
  }, [nodePaths]);

  useEffect(() => {
    if (!externalToggle || typeof externalToggle.open !== 'boolean') return;
    setAllNodes(externalToggle.open);
  }, [externalToggle?.version, externalToggle?.open, setAllNodes]);

  useEffect(() => {
    setChildWindowMap({});
  }, [data]);

  const renderValue = (value, depth = 0, path = '$') => {
    if (renderCounterRef.current >= NODE_CAP) {
      renderTruncatedRef.current = true;
      return <span className="json-null">"...truncated..."</span>;
    }
    renderCounterRef.current += 1;
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
      const open = isOpen(path, depth, value);
      const visibleCount = Math.max(0, Math.min(value.length, Number(childWindowMap[path] || CHILD_RENDER_CHUNK)));
      const visibleItems = value.slice(0, visibleCount);
      return (
        <span>
          <button
            type="button"
            className="json-bracket inline-flex items-center gap-0.5 bg-transparent border-none p-0 cursor-pointer"
            onClick={() => toggleNode(path, defaultOpenForValue(depth, value), value.length)}
          >
            {open ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
            [ {!open && <span className="text-2xs ml-0.5" style={{ color:'var(--text-tertiary)' }}>{value.length} items</span>}
            {!open && <span className="json-bracket"> ]</span>}
          </button>
          {open && (
            <>
              <div style={{ paddingLeft: '1.25rem' }}>
                {visibleItems.map((item, index) => (
                  <div key={`${path}:${index}`} className="leading-relaxed">
                    {renderValue(item, depth + 1, `${path}[${index}]`)}
                    {index < value.length - 1 && <span className="json-comma">,</span>}
                  </div>
                ))}
                {visibleCount < value.length && (
                  <button
                    type="button"
                    className="btn-ghost px-2 py-1 text-2xs mt-1"
                    onClick={() => setChildWindowMap((prev) => ({
                      ...prev,
                      [path]: Math.min(value.length, Math.max(visibleCount, 0) + CHILD_RENDER_CHUNK),
                    }))}
                  >
                    Show next {Math.min(CHILD_RENDER_CHUNK, value.length - visibleCount)} items
                  </button>
                )}
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
      const open = isOpen(path, depth, value);
      const visibleCount = Math.max(0, Math.min(entries.length, Number(childWindowMap[path] || CHILD_RENDER_CHUNK)));
      const visibleEntries = entries.slice(0, visibleCount);
      return (
        <span>
          <button
            type="button"
            className="json-bracket inline-flex items-center gap-0.5 bg-transparent border-none p-0 cursor-pointer"
            onClick={() => toggleNode(path, defaultOpenForValue(depth, value), entries.length)}
          >
            {open ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
            {'{ '}
            {!open && <span className="text-2xs ml-0.5" style={{ color:'var(--text-tertiary)' }}>{entries.length} fields</span>}
            {!open && <span className="json-bracket">{' }'}</span>}
          </button>
          {open && (
            <>
              <div style={{ paddingLeft: '1.25rem' }}>
                {visibleEntries.map(([key, child], index) => (
                  <div key={`${path}.${key}`} className="leading-relaxed">
                    <span className="json-key">"{key}"</span>
                    <span className="json-comma">: </span>
                    {renderValue(child, depth + 1, `${path}.${key}`)}
                    {index < entries.length - 1 && <span className="json-comma">,</span>}
                  </div>
                ))}
                {visibleCount < entries.length && (
                  <button
                    type="button"
                    className="btn-ghost px-2 py-1 text-2xs mt-1"
                    onClick={() => setChildWindowMap((prev) => ({
                      ...prev,
                      [path]: Math.min(entries.length, Math.max(visibleCount, 0) + CHILD_RENDER_CHUNK),
                    }))}
                  >
                    Show next {Math.min(CHILD_RENDER_CHUNK, entries.length - visibleCount)} fields
                  </button>
                )}
              </div>
              <span className="json-bracket">{'}'}</span>
            </>
          )}
        </span>
      );
    }

    return <span style={{ color:'var(--text-secondary)' }}>{String(value)}</span>;
  };
  renderCounterRef.current = 0;
  renderTruncatedRef.current = false;
  const renderedTree = renderValue(data, 0, '$');
  const renderTruncated = renderTruncatedRef.current;

  return (
    <div>
      {showControls && nodePaths.length > 0 && (
        <div className="mb-2 flex items-center gap-1.5 text-2xs">
          <button type="button" onClick={() => setAllNodes(true)} className="btn-ghost px-2 py-1" disabled={nodesCapped}>
            Expand all
          </button>
          <button type="button" onClick={() => setAllNodes(false)} className="btn-ghost px-2 py-1">
            Collapse all
          </button>
          <span style={{ color:'var(--text-tertiary)' }}>lazy chunk {CHILD_RENDER_CHUNK}</span>
          {nodesCapped && <span style={{ color:'var(--text-tertiary)' }}>node cap {NODE_CAP}</span>}
          {renderTruncated && <span style={{ color:'var(--text-tertiary)' }}>render truncated</span>}
        </div>
      )}
      <pre className="font-mono text-xs leading-relaxed whitespace-pre-wrap">
        {renderedTree}
      </pre>
    </div>
  );
}

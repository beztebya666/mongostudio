import React, { useState } from 'react';
import { ChevronRight, ChevronDown } from './Icons';

function renderValue(value, depth = 0) {
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

  if (Array.isArray(value)) return <CollapsibleArray arr={value} depth={depth} />;
  if (typeof value === 'object') return <CollapsibleObject obj={value} depth={depth} />;
  return <span style={{color:'var(--text-secondary)'}}>{String(value)}</span>;
}

function CollapsibleObject({ obj, depth }) {
  const [open, setOpen] = useState(depth < 3);
  const entries = Object.entries(obj);
  if (entries.length === 0) return <span className="json-bracket">{'{}'}</span>;

  return (
    <span>
      <span className="json-bracket cursor-pointer inline-flex items-center" onClick={() => setOpen(!open)}>
        {open ? <ChevronDown className="w-3 h-3 inline" /> : <ChevronRight className="w-3 h-3 inline" />}
        {'{ '}
        {!open && <span className="text-2xs ml-1" style={{color:'var(--text-tertiary)'}}>{entries.length} fields</span>}
        {!open && <span className="json-bracket">{' }'}</span>}
      </span>
      {open && (
        <div style={{ paddingLeft: '1.25rem' }}>
          {entries.map(([key, val], i) => (
            <div key={key} className="leading-relaxed">
              <span className="json-key">"{key}"</span>
              <span className="json-comma">: </span>
              {renderValue(val, depth + 1)}
              {i < entries.length - 1 && <span className="json-comma">,</span>}
            </div>
          ))}
        </div>
      )}
      {open && <span className="json-bracket">{'}'}</span>}
    </span>
  );
}

function CollapsibleArray({ arr, depth }) {
  const [open, setOpen] = useState(depth < 2 && arr.length <= 20);
  if (arr.length === 0) return <span className="json-bracket">[]</span>;

  return (
    <span>
      <span className="json-bracket cursor-pointer inline-flex items-center" onClick={() => setOpen(!open)}>
        {open ? <ChevronDown className="w-3 h-3 inline" /> : <ChevronRight className="w-3 h-3 inline" />}
        {'[ '}
        {!open && <span className="text-2xs ml-1" style={{color:'var(--text-tertiary)'}}>{arr.length} items</span>}
        {!open && <span className="json-bracket">{' ]'}</span>}
      </span>
      {open && (
        <div style={{ paddingLeft: '1.25rem' }}>
          {arr.map((item, i) => (
            <div key={i} className="leading-relaxed">
              {renderValue(item, depth + 1)}
              {i < arr.length - 1 && <span className="json-comma">,</span>}
            </div>
          ))}
        </div>
      )}
      {open && <span className="json-bracket">{']'}</span>}
    </span>
  );
}

export default function JsonView({ data }) {
  return (
    <pre className="font-mono text-xs leading-relaxed whitespace-pre-wrap">
      {renderValue(data, 0)}
    </pre>
  );
}

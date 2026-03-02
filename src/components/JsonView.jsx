import React, { useState, useCallback } from 'react';

function JsonValue({ value, indent = 0, isLast = true }) {
  const [collapsed, setCollapsed] = useState(indent > 3);

  if (value === null) return <span className="json-null">null</span>;
  if (value === undefined) return <span className="json-null">undefined</span>;
  if (typeof value === 'boolean') return <span className="json-boolean">{String(value)}</span>;
  if (typeof value === 'number') return <span className="json-number">{value}</span>;

  if (typeof value === 'string') {
    // Check for ObjectId pattern
    if (/^[a-f0-9]{24}$/i.test(value)) {
      return <span className="json-objectid">"{value}"</span>;
    }
    // Truncate long strings
    const display = value.length > 200 ? value.slice(0, 200) + '…' : value;
    return <span className="json-string">"{display}"</span>;
  }

  if (typeof value === 'object') {
    // Handle MongoDB extended JSON types
    if (value.$oid) return <span className="json-objectid">ObjectId("{value.$oid}")</span>;
    if (value.$date) return <span className="json-number">ISODate("{typeof value.$date === 'object' ? value.$date.$numberLong : value.$date}")</span>;
    if (value.$numberLong) return <span className="json-number">NumberLong({value.$numberLong})</span>;
    if (value.$numberDecimal) return <span className="json-number">Decimal128({value.$numberDecimal})</span>;
    if (value.$binary) return <span className="json-string">Binary(…)</span>;
    if (value.$regex) return <span className="json-string">/{value.$regex}/{value.$options || ''}</span>;

    const isArray = Array.isArray(value);
    const entries = isArray ? value.map((v, i) => [i, v]) : Object.entries(value);
    const open = isArray ? '[' : '{';
    const close = isArray ? ']' : '}';

    if (entries.length === 0) {
      return <span className="json-bracket">{open}{close}</span>;
    }

    if (collapsed) {
      return (
        <span>
          <button
            onClick={() => setCollapsed(false)}
            className="json-bracket hover:text-accent transition-colors cursor-pointer"
          >
            {open} <span className="text-text-tertiary text-2xs">{entries.length} {isArray ? 'items' : 'fields'}</span> {close}
          </button>
        </span>
      );
    }

    const pad = '  '.repeat(indent + 1);
    const closePad = '  '.repeat(indent);

    return (
      <span>
        <button
          onClick={() => setCollapsed(true)}
          className="json-bracket hover:text-accent transition-colors cursor-pointer"
        >
          {open}
        </button>
        {'\n'}
        {entries.map(([key, val], i) => (
          <span key={isArray ? i : key}>
            {pad}
            {!isArray && (
              <>
                <span className="json-key">"{key}"</span>
                <span className="json-comma">: </span>
              </>
            )}
            <JsonValue value={val} indent={indent + 1} isLast={i === entries.length - 1} />
            {i < entries.length - 1 && <span className="json-comma">,</span>}
            {'\n'}
          </span>
        ))}
        {closePad}
        <span className="json-bracket">{close}</span>
      </span>
    );
  }

  return <span className="text-text-secondary">{String(value)}</span>;
}

export default function JsonView({ data }) {
  return (
    <pre className="text-xs font-mono leading-relaxed whitespace-pre overflow-x-auto">
      <JsonValue value={data} indent={0} />
    </pre>
  );
}

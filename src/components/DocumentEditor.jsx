import React, { useState, useRef, useEffect } from 'react';
import api from '../utils/api';
import { Save, X, AlertCircle, Check, Loader } from './Icons';

export default function DocumentEditor({ db, collection, document, onSave, onCancel }) {
  const isInsert = !document;
  const [value, setValue] = useState('');
  const [error, setError] = useState(null);
  const [saving, setSaving] = useState(false);
  const [lineCount, setLineCount] = useState(1);
  const textareaRef = useRef(null);

  useEffect(() => {
    if (document) {
      const copy = { ...document };
      delete copy._id;
      setValue(JSON.stringify(copy, null, 2));
    } else {
      setValue('{\n  \n}');
    }
  }, [document]);

  useEffect(() => {
    setLineCount(value.split('\n').length);
  }, [value]);

  const handleSave = async () => {
    setError(null);
    let parsed;
    try { parsed = JSON.parse(value); }
    catch (e) { setError(`Invalid JSON: ${e.message}`); return; }

    setSaving(true);
    try {
      if (isInsert) {
        await api.insertDocument(db, collection, parsed);
      } else {
        const id = typeof document._id === 'object' ? (document._id.$oid || JSON.stringify(document._id)) : String(document._id);
        await api.updateDocument(db, collection, id, parsed);
      }
      onSave();
    } catch (err) { setError(err.message); }
    finally { setSaving(false); }
  };

  const handleKeyDown = (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 's') { e.preventDefault(); handleSave(); }
    if (e.key === 'Tab') {
      e.preventDefault();
      const start = e.target.selectionStart;
      const end = e.target.selectionEnd;
      const newVal = value.substring(0, start) + '  ' + value.substring(end);
      setValue(newVal);
      setTimeout(() => { e.target.selectionStart = e.target.selectionEnd = start + 2; }, 0);
    }
  };

  const handleFormat = () => {
    try {
      const parsed = JSON.parse(value);
      setValue(JSON.stringify(parsed, null, 2));
      setError(null);
    } catch (e) { setError(`Cannot format: ${e.message}`); }
  };

  return (
    <div className="h-full flex flex-col">
      {/* Toolbar */}
      <div className="flex-shrink-0 flex items-center justify-between px-4 py-2.5" style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)'}}>
        <div className="flex items-center gap-3">
          <h3 className="text-sm font-semibold" style={{color:'var(--text-primary)'}}>
            {isInsert ? 'Insert Document' : 'Edit Document'}
          </h3>
          {!isInsert && document?._id && (
            <code className="text-2xs font-mono px-2 py-0.5 rounded" style={{background:'var(--surface-3)',color:'var(--json-objectid)'}}>
              {typeof document._id === 'object' ? document._id.$oid || JSON.stringify(document._id) : String(document._id)}
            </code>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button onClick={handleFormat} className="btn-ghost text-xs">Format</button>
          <span className="text-2xs" style={{color:'var(--text-tertiary)'}}>⌘+S to save</span>
          <button onClick={handleSave} disabled={saving} className="btn-primary flex items-center gap-1.5">
            {saving ? <Loader className="w-3.5 h-3.5" /> : <Save className="w-3.5 h-3.5" />}
            {isInsert ? 'Insert' : 'Update'}
          </button>
          <button onClick={onCancel} className="btn-ghost"><X className="w-4 h-4" /></button>
        </div>
      </div>

      {error && (
        <div className="mx-4 mt-3 flex items-start gap-2 text-red-400 text-xs p-3 rounded-lg" style={{background:'rgba(239,68,68,0.05)',border:'1px solid rgba(239,68,68,0.2)'}}>
          <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" /><span>{error}</span>
        </div>
      )}

      {/* Editor */}
      <div className="flex-1 overflow-auto flex">
        {/* Line numbers */}
        <div className="flex-shrink-0 px-3 py-3 text-right select-none" style={{color:'var(--text-tertiary)',minWidth:'3rem',background:'var(--surface-1)'}}>
          {Array.from({ length: lineCount }, (_, i) => (
            <div key={i} className="text-2xs font-mono leading-[1.625rem]">{i + 1}</div>
          ))}
        </div>
        <textarea ref={textareaRef} value={value} onChange={e=>setValue(e.target.value)} onKeyDown={handleKeyDown}
          spellCheck={false} autoComplete="off"
          className="flex-1 resize-none px-4 py-3 font-mono text-sm leading-relaxed focus:outline-none"
          style={{background:'var(--surface-0)',color:'var(--text-primary)',tabSize:2}} />
      </div>
    </div>
  );
}

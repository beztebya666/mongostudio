import React, { useState, useRef, useEffect, useCallback } from 'react';
import api from '../utils/api';
import { X, Check, AlertCircle, Loader } from './Icons';
import { prettyJson, safeJsonParse } from '../utils/formatters';

export default function DocumentEditor({ db, collection, document: doc, onSave, onCancel }) {
  const isInsert = !doc;
  const initial = isInsert ? '{\n  \n}' : prettyJson(doc, 2);
  const [value, setValue] = useState(initial);
  const [error, setError] = useState(null);
  const [saving, setSaving] = useState(false);
  const textareaRef = useRef(null);

  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.focus();
      if (isInsert) {
        textareaRef.current.setSelectionRange(4, 4);
      }
    }
  }, []);

  const handleSave = async () => {
    setError(null);
    const { data, error: parseErr } = safeJsonParse(value);
    if (parseErr) {
      setError(`Invalid JSON: ${parseErr}`);
      return;
    }

    setSaving(true);
    try {
      if (isInsert) {
        await api.insertDocument(db, collection, data);
      } else {
        const id = typeof doc._id === 'object' ? (doc._id.$oid || JSON.stringify(doc._id)) : String(doc._id);
        // Remove _id from update payload
        const { _id, ...update } = data;
        await api.updateDocument(db, collection, id, update);
      }
      onSave();
    } catch (err) {
      setError(err.message);
    } finally {
      setSaving(false);
    }
  };

  const handleKeyDown = (e) => {
    // Tab indentation
    if (e.key === 'Tab') {
      e.preventDefault();
      const start = e.target.selectionStart;
      const end = e.target.selectionEnd;
      const newValue = value.substring(0, start) + '  ' + value.substring(end);
      setValue(newValue);
      requestAnimationFrame(() => {
        e.target.selectionStart = e.target.selectionEnd = start + 2;
      });
    }
    // Cmd/Ctrl + Enter to save
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      handleSave();
    }
    // Escape to cancel
    if (e.key === 'Escape') {
      onCancel();
    }
  };

  const lineCount = value.split('\n').length;

  return (
    <div className="h-full flex flex-col animate-fade-in">
      {/* Header */}
      <div className="flex-shrink-0 border-b border-border bg-surface-1/30 px-4 py-3 flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold">
            {isInsert ? 'Insert Document' : 'Edit Document'}
          </h3>
          <p className="text-2xs text-text-tertiary mt-0.5">
            {isInsert ? 'Enter a valid JSON document' : `Editing in ${db}.${collection}`}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={onCancel} className="btn-ghost flex items-center gap-1.5">
            <X className="w-3.5 h-3.5" />
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className="btn-primary flex items-center gap-1.5"
          >
            {saving ? <Loader className="w-3.5 h-3.5" /> : <Check className="w-3.5 h-3.5" />}
            {isInsert ? 'Insert' : 'Update'}
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="mx-4 mt-3 flex items-start gap-2 text-red-400 text-xs bg-red-500/5 border border-red-500/20 rounded-lg p-3">
          <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {/* Editor */}
      <div className="flex-1 overflow-hidden p-4">
        <div className="h-full bg-surface-2 border border-border rounded-xl overflow-hidden flex">
          {/* Line numbers */}
          <div className="py-4 pl-3 pr-2 text-right select-none border-r border-border bg-surface-1/50 flex-shrink-0 overflow-hidden">
            {Array.from({ length: lineCount }, (_, i) => (
              <div key={i} className="text-2xs text-text-tertiary leading-[1.6rem] font-mono h-[1.6rem]">
                {i + 1}
              </div>
            ))}
          </div>
          {/* Textarea */}
          <textarea
            ref={textareaRef}
            value={value}
            onChange={(e) => setValue(e.target.value)}
            onKeyDown={handleKeyDown}
            className="flex-1 bg-transparent text-xs font-mono text-text-primary p-4 resize-none focus:outline-none leading-[1.6rem]"
            spellCheck={false}
            autoCorrect="off"
            autoCapitalize="off"
          />
        </div>
      </div>

      {/* Footer hint */}
      <div className="flex-shrink-0 border-t border-border px-4 py-2 text-2xs text-text-tertiary flex items-center gap-4">
        <span>
          <kbd className="px-1 py-0.5 bg-surface-3 rounded text-2xs">⌘ Enter</kbd> to save
        </span>
        <span>
          <kbd className="px-1 py-0.5 bg-surface-3 rounded text-2xs">Esc</kbd> to cancel
        </span>
        <span>
          <kbd className="px-1 py-0.5 bg-surface-3 rounded text-2xs">Tab</kbd> to indent
        </span>
        <span className="ml-auto">{lineCount} lines</span>
      </div>
    </div>
  );
}

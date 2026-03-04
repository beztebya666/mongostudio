import React, { useEffect, useState } from 'react';
import AppModal from './AppModal';

export default function InputDialog({
  open,
  title = 'Input required',
  label = 'Value',
  placeholder = '',
  initialValue = '',
  submitLabel = 'Create',
  cancelLabel = 'Cancel',
  error = '',
  busy = false,
  onSubmit,
  onCancel,
}) {
  const [value, setValue] = useState(initialValue);

  useEffect(() => {
    if (open) setValue(initialValue || '');
  }, [open, initialValue]);

  return (
    <AppModal open={open} onClose={onCancel} maxWidth="max-w-md">
      <div className="px-5 py-4" style={{ borderBottom: '1px solid var(--border)' }}>
        <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>{title}</h3>
      </div>
      <div className="px-5 py-4 space-y-2">
        <label className="block text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>{label}</label>
        <input
          type="text"
          value={value}
          onChange={(event) => setValue(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && value.trim() && !busy) onSubmit?.(value.trim());
          }}
          autoFocus
          placeholder={placeholder}
          className="w-full rounded-lg px-3 py-2 text-sm font-mono"
          style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
        />
        {error && <p className="text-2xs text-red-400">{error}</p>}
      </div>
      <div className="px-5 py-4 flex items-center justify-end gap-2" style={{ borderTop: '1px solid var(--border)' }}>
        <button type="button" onClick={onCancel} className="btn-ghost text-xs" disabled={busy}>{cancelLabel}</button>
        <button
          type="button"
          onClick={() => onSubmit?.(value.trim())}
          className="btn-primary text-xs"
          disabled={!value.trim() || busy}
        >
          {submitLabel}
        </button>
      </div>
    </AppModal>
  );
}

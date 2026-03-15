import React from 'react';
import AppModal from './AppModal';
import { AlertTriangle } from '../Icons';

export default function ConfirmDialog({
  open,
  title = 'Confirm action',
  message = 'Are you sure?',
  confirmLabel = 'Confirm',
  cancelLabel = 'Cancel',
  danger = false,
  onConfirm,
  onCancel,
  busy = false,
}) {
  return (
    <AppModal open={open} onClose={onCancel} maxWidth="max-w-md">
      <div className="px-5 py-4 flex items-center gap-2" style={{ borderBottom: '1px solid var(--border)' }}>
        <AlertTriangle className="w-4 h-4" style={{ color: danger ? '#f87171' : '#fbbf24' }} />
        <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>{title}</h3>
      </div>
      <div className="px-5 py-4">
        <p className="text-xs leading-relaxed whitespace-pre-line" style={{ color: 'var(--text-secondary)' }}>{message}</p>
      </div>
      <div className="px-5 py-4 flex items-center justify-end gap-2" style={{ borderTop: '1px solid var(--border)' }}>
        <button type="button" onClick={onCancel} className="btn-ghost text-xs" disabled={busy}>{cancelLabel}</button>
        <button
          type="button"
          onClick={onConfirm}
          disabled={busy}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${danger ? 'text-red-300 bg-red-500/15 border border-red-500/30 hover:bg-red-500/25' : 'text-[var(--surface-0)] bg-[var(--accent)] hover:bg-[var(--accent-dim)]'}`}
        >
          {confirmLabel}
        </button>
      </div>
    </AppModal>
  );
}

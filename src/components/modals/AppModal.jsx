import React, { useEffect } from 'react';

export default function AppModal({ open, onClose, maxWidth = 'max-w-md', children }) {
  useEffect(() => {
    if (!open) return undefined;
    const onKeyDown = (event) => {
      if (event.key === 'Escape') onClose?.();
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center px-4"
      style={{ background: 'rgba(0,0,0,0.72)' }}
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose?.();
      }}
    >
      <div
        className={`w-full ${maxWidth} rounded-2xl shadow-2xl overflow-hidden animate-slide-up`}
        style={{ background: 'var(--surface-1)', border: '1px solid var(--border)', maxHeight: '90vh' }}
      >
        {children}
      </div>
    </div>
  );
}

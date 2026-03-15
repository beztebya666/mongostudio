import React from 'react';
import { X } from '../Icons';

export default function DatabaseExportDialog({
  open,
  title = 'Export Database',
  subtitle = '',
  busy = false,
  mode = 'package',
  onModeChange,
  archive = false,
  onArchiveChange,
  collectionFormat = 'json',
  onCollectionFormatChange,
  includeIndexes = false,
  onIncludeIndexesChange,
  includeSchema = false,
  onIncludeSchemaChange,
  items = [],
  selectedItems = [],
  itemsLabel = 'Items',
  progress = null,
  onToggleItem,
  onSelectAll,
  onClearAll,
  onCancel,
  onSubmit,
}) {
  if (!open) return null;
  const hasItemsChooser = Array.isArray(items) && items.length > 0;
  const selectedSet = new Set(selectedItems || []);
  const receivedBytes = Number(progress?.receivedBytes || 0);
  const totalBytesRaw = Number(progress?.totalBytes);
  const totalBytes = Number.isFinite(totalBytesRaw) && totalBytesRaw > 0 ? totalBytesRaw : null;
  const percentValue = progress?.percent;
  const percent = typeof percentValue === 'number' && Number.isFinite(percentValue)
    ? Math.max(0, Math.min(100, Math.round(percentValue)))
    : null;

  const formatBytes = (value) => {
    const num = Number(value || 0);
    if (!Number.isFinite(num) || num <= 0) return '0 B';
    if (num < 1024) return `${Math.round(num)} B`;
    if (num < 1024 * 1024) return `${(num / 1024).toFixed(1)} KB`;
    if (num < 1024 * 1024 * 1024) return `${(num / (1024 * 1024)).toFixed(1)} MB`;
    return `${(num / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  };
  const progressLabel = totalBytes
    ? `${percent ?? 0}% • ${formatBytes(receivedBytes)} / ${formatBytes(totalBytes)}`
    : `${formatBytes(receivedBytes)} downloaded`;

  return (
    <div className="fixed inset-0 z-[220] flex items-center justify-center p-4">
      <button
        type="button"
        className="absolute inset-0 bg-black/50"
        onClick={() => onCancel?.()}
        aria-label="Close database export dialog"
      />
      <div className="relative w-full max-w-md rounded-xl p-4 animate-fade-in" style={{ background:'var(--surface-1)', border:'1px solid var(--border)' }}>
        <div className="flex items-center justify-between mb-2">
          <div className="text-sm font-semibold" style={{ color:'var(--text-primary)' }}>
            {title}
          </div>
          <button type="button" className="btn-ghost p-1.5" onClick={() => onCancel?.()}>
            <X className="w-3.5 h-3.5" />
          </button>
        </div>
        {subtitle && (
          <div className="text-2xs mb-3 font-mono" style={{ color:'var(--text-tertiary)' }}>
            {subtitle}
          </div>
        )}
        <div className="space-y-2.5">
          {hasItemsChooser && (
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <label className="block text-2xs" style={{ color:'var(--text-tertiary)' }}>{itemsLabel}</label>
                <div className="flex items-center gap-1">
                  <button type="button" onClick={() => onSelectAll?.()} className="btn-ghost text-2xs px-2 py-1">
                    Select all
                  </button>
                  <button type="button" onClick={() => onClearAll?.()} className="btn-ghost text-2xs px-2 py-1">
                    Clear
                  </button>
                </div>
              </div>
              <div className="max-h-36 overflow-auto rounded-lg p-1.5 space-y-1" style={{ background:'var(--surface-2)', border:'1px solid var(--border)' }}>
                {items.map((item) => (
                  <label key={item} className="flex items-center gap-2 px-2 py-1 rounded-md text-xs cursor-pointer hover:bg-[var(--surface-3)]" style={{ color:'var(--text-secondary)' }}>
                    <input
                      type="checkbox"
                      checked={selectedSet.has(item)}
                      onChange={() => onToggleItem?.(item)}
                      className="ms-checkbox"
                    />
                    <span className="font-mono truncate">{item}</span>
                  </label>
                ))}
              </div>
              <div className="mt-1 text-2xs" style={{ color:'var(--text-tertiary)' }}>
                Selected: {selectedSet.size}
              </div>
            </div>
          )}

          <label className="block text-2xs" style={{ color:'var(--text-tertiary)' }}>Mode</label>
          <div className="grid grid-cols-2 gap-1.5">
            <button
              type="button"
              className="rounded-lg px-2.5 py-1.5 text-xs text-left transition-all"
              style={{
                background: mode === 'package' ? 'rgba(0, 237, 100, 0.12)' : 'var(--surface-2)',
                border: mode === 'package' ? '1px solid rgba(0, 237, 100, 0.35)' : '1px solid var(--border)',
                color: mode === 'package' ? 'var(--accent)' : 'var(--text-secondary)',
              }}
              onClick={() => onModeChange?.('package')}
            >
              DB package
            </button>
            <button
              type="button"
              className="rounded-lg px-2.5 py-1.5 text-xs text-left transition-all"
              style={{
                background: mode === 'collections' ? 'rgba(0, 237, 100, 0.12)' : 'var(--surface-2)',
                border: mode === 'collections' ? '1px solid rgba(0, 237, 100, 0.35)' : '1px solid var(--border)',
                color: mode === 'collections' ? 'var(--accent)' : 'var(--text-secondary)',
              }}
              onClick={() => onModeChange?.('collections')}
            >
              Collection files
            </button>
          </div>

          {mode === 'collections' && (
            <>
              <label className="block text-2xs" style={{ color:'var(--text-tertiary)' }}>Collection file format</label>
              <div className="grid grid-cols-2 gap-1.5">
                <button
                  type="button"
                  className="rounded-lg px-2.5 py-1.5 text-xs text-left transition-all"
                  style={{
                    background: collectionFormat === 'json' ? 'rgba(59, 130, 246, 0.14)' : 'var(--surface-2)',
                    border: collectionFormat === 'json' ? '1px solid rgba(59, 130, 246, 0.35)' : '1px solid var(--border)',
                    color: collectionFormat === 'json' ? '#60a5fa' : 'var(--text-secondary)',
                  }}
                  onClick={() => onCollectionFormatChange?.('json')}
                >
                  JSON
                </button>
                <button
                  type="button"
                  className="rounded-lg px-2.5 py-1.5 text-xs text-left transition-all"
                  style={{
                    background: collectionFormat === 'csv' ? 'rgba(59, 130, 246, 0.14)' : 'var(--surface-2)',
                    border: collectionFormat === 'csv' ? '1px solid rgba(59, 130, 246, 0.35)' : '1px solid var(--border)',
                    color: collectionFormat === 'csv' ? '#60a5fa' : 'var(--text-secondary)',
                  }}
                  onClick={() => onCollectionFormatChange?.('csv')}
                >
                  CSV
                </button>
              </div>
            </>
          )}

          <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
            <input
              type="checkbox"
              checked={archive}
              onChange={(event) => onArchiveChange?.(event.target.checked)}
              className="ms-checkbox"
            />
            Download as ZIP archive
          </label>

          <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
            <input
              type="checkbox"
              checked={includeIndexes}
              onChange={(event) => onIncludeIndexesChange?.(event.target.checked)}
              className="ms-checkbox"
            />
            Include index metadata
          </label>

          <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
            <input
              type="checkbox"
              checked={includeSchema}
              onChange={(event) => onIncludeSchemaChange?.(event.target.checked)}
              className="ms-checkbox"
            />
            Include schema snapshot
          </label>
          {busy && progress && (
            <div className="rounded-md px-2 py-1.5 space-y-1" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              <div className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>{progressLabel}</div>
              <div className="h-1 rounded-full overflow-hidden" style={{ background: 'rgba(255,255,255,0.08)' }}>
                <div
                  className={percent === null ? 'h-full rounded-full ms-progress-indeterminate' : 'h-full rounded-full'}
                  style={{
                    width: percent === null ? '34%' : `${percent}%`,
                    background: 'var(--accent)',
                    opacity: percent === null ? 0.85 : 1,
                  }}
                />
              </div>
            </div>
          )}
        </div>
        <div className="mt-4 flex items-center justify-end gap-2">
          <button type="button" className="btn-ghost text-xs" onClick={() => onCancel?.()}>
            Cancel
          </button>
          <button type="button" className="btn-primary text-xs px-3 py-1.5" onClick={() => onSubmit?.()} disabled={busy || (hasItemsChooser && selectedSet.size === 0)}>
            {busy ? 'Exporting...' : 'Export'}
          </button>
        </div>
      </div>
    </div>
  );
}

import React from 'react';
import { X } from '../Icons';
import DropdownSelect from '../DropdownSelect';

export default function CollectionExportDialog({
  open,
  busy = false,
  title = 'Export Collection',
  subtitle = '',
  docsValue = 'exact',
  docsOptions = [],
  onDocsChange,
  format = 'json',
  onFormatChange,
  useVisibleFields = false,
  onUseVisibleFieldsChange,
  visibleFieldsLabel = 'Only visible fields',
  useSort = false,
  onUseSortChange,
  sortLabel = 'Apply saved sort',
  showFilterToggle = false,
  useFilter = false,
  onUseFilterChange,
  filterLabel = 'Apply current filter',
  showModifiersInfo = false,
  modifiersInfoText = '',
  progress = null,
  submitLabel = 'Export',
  onCancel,
  onSubmit,
}) {
  if (!open) return null;
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
        aria-label="Close collection export dialog"
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
          <div>
            <label className="block text-2xs mb-1" style={{ color:'var(--text-tertiary)' }}>Documents to export</label>
            <DropdownSelect
              value={String(docsValue || 'exact')}
              options={docsOptions}
              onChange={(nextValue) => onDocsChange?.(String(nextValue || 'exact'))}
              fullWidth
              sizeClassName="text-xs"
              menuZIndex={520}
            />
          </div>
          <div>
            <label className="block text-2xs mb-1" style={{ color:'var(--text-tertiary)' }}>Format</label>
            <div className="grid grid-cols-2 gap-1.5">
              <label className="flex items-center gap-2 rounded-lg px-2.5 py-1.5 text-xs cursor-pointer" style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-secondary)' }}>
                <input
                  type="checkbox"
                  checked={format === 'json'}
                  onChange={(event) => {
                    if (event.target.checked) onFormatChange?.('json');
                  }}
                  className="ms-checkbox"
                />
                JSON
              </label>
              <label className="flex items-center gap-2 rounded-lg px-2.5 py-1.5 text-xs cursor-pointer" style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-secondary)' }}>
                <input
                  type="checkbox"
                  checked={format === 'csv'}
                  onChange={(event) => {
                    if (event.target.checked) onFormatChange?.('csv');
                  }}
                  className="ms-checkbox"
                />
                CSV
              </label>
            </div>
          </div>
          <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
            <input
              type="checkbox"
              checked={useVisibleFields}
              onChange={(event) => onUseVisibleFieldsChange?.(event.target.checked)}
              className="ms-checkbox"
            />
            {visibleFieldsLabel}
          </label>
          <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
            <input
              type="checkbox"
              checked={useSort}
              onChange={(event) => onUseSortChange?.(event.target.checked)}
              className="ms-checkbox"
            />
            {sortLabel}
          </label>
          {showFilterToggle && (
            <label className="flex items-center gap-2 text-xs cursor-pointer" style={{ color:'var(--text-secondary)' }}>
              <input
                type="checkbox"
                checked={useFilter}
                onChange={(event) => onUseFilterChange?.(event.target.checked)}
                className="ms-checkbox"
              />
              {filterLabel}
            </label>
          )}
          {showModifiersInfo && (
            <div className="text-2xs rounded-md px-2 py-1.5" style={{ color: 'var(--text-tertiary)', background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
              {modifiersInfoText}
            </div>
          )}
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
          <button type="button" className="btn-primary text-xs px-3 py-1.5" onClick={() => onSubmit?.()} disabled={busy}>
            {busy ? 'Exporting...' : submitLabel}
          </button>
        </div>
      </div>
    </div>
  );
}

import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Check, ChevronDown } from './Icons';

function normalize(value) {
  if (value === null || value === undefined) return '';
  return String(value);
}

export default function DropdownSelect({
  value,
  options = [],
  onChange,
  disabled = false,
  title,
  fullWidth = false,
  align = 'left',
  sizeClassName = 'text-xs',
  className = '',
  menuClassName = '',
  menuZIndex = 320,
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);
  const menuRef = useRef(null);
  const [menuPosition, setMenuPosition] = useState({ top: -9999, left: -9999, minWidth: 160, origin: 'top left', ready: false });
  const normalizedValue = normalize(value);
  const normalizedOptions = useMemo(
    () => options.map((opt) => ({ ...opt, _norm: normalize(opt?.value) })),
    [options],
  );
  const selectedOption = normalizedOptions.find((opt) => opt._norm === normalizedValue) || null;
  const selectedLabel = selectedOption?.label ?? normalizedValue;

  const updateMenuPosition = useCallback(() => {
    if (!rootRef.current || !menuRef.current || typeof window === 'undefined') return;
    const anchorRect = rootRef.current.getBoundingClientRect();
    const menuRect = menuRef.current.getBoundingClientRect();
    const margin = 8;
    const gap = 4;
    const viewportW = window.innerWidth;
    const viewportH = window.innerHeight;
    const minWidth = Math.max(120, Math.round(anchorRect.width));

    let top = anchorRect.bottom + gap;
    let originY = 'top';
    if (top + menuRect.height > viewportH - margin) {
      top = Math.max(margin, anchorRect.top - menuRect.height - gap);
      originY = 'bottom';
    }

    let left = align === 'right' ? anchorRect.right - Math.max(menuRect.width, minWidth) : anchorRect.left;
    const maxLeft = Math.max(margin, viewportW - Math.max(menuRect.width, minWidth) - margin);
    if (left < margin) left = margin;
    if (left > maxLeft) left = maxLeft;

    setMenuPosition({
      top,
      left,
      minWidth,
      origin: `${originY} ${align === 'right' ? 'right' : 'left'}`,
      ready: true,
    });
  }, [align]);

  useLayoutEffect(() => {
    if (!open) return undefined;
    updateMenuPosition();
    const onMove = () => updateMenuPosition();
    window.addEventListener('resize', onMove);
    window.addEventListener('scroll', onMove, true);
    return () => {
      window.removeEventListener('resize', onMove);
      window.removeEventListener('scroll', onMove, true);
    };
  }, [open, updateMenuPosition]);

  useEffect(() => {
    if (!open) return undefined;
    const onMouseDown = (event) => {
      const target = event.target;
      if (rootRef.current && rootRef.current.contains(target)) return;
      if (menuRef.current && menuRef.current.contains(target)) return;
      setOpen(false);
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [open]);

  return (
    <div ref={rootRef} className={`relative ${fullWidth ? 'w-full' : ''} ${className}`}>
      <button
        type="button"
        disabled={disabled}
        title={title}
        onClick={() => setOpen((prev) => !prev)}
        className={`inline-flex items-center justify-between gap-1.5 rounded-lg px-2.5 py-1.5 transition-all ${fullWidth ? 'w-full' : ''} ${sizeClassName} disabled:opacity-50 disabled:cursor-not-allowed`}
        style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
      >
        <span className="truncate">{selectedLabel || '-'}</span>
        <ChevronDown className={`w-3.5 h-3.5 transition-transform ${open ? 'rotate-180' : ''}`} style={{ color: 'var(--text-tertiary)' }} />
      </button>
      {open && !disabled && typeof document !== 'undefined' && createPortal(
        <div
          ref={menuRef}
          className={`rounded-lg p-1 max-h-64 overflow-auto ${menuClassName}`}
          style={{
            position: 'fixed',
            top: menuPosition.top,
            left: menuPosition.left,
            minWidth: menuPosition.minWidth,
            zIndex: menuZIndex,
            opacity: menuPosition.ready ? 1 : 0,
            transformOrigin: menuPosition.origin,
            background: 'var(--surface-3)',
            border: '1px solid var(--border)',
            boxShadow: '0 12px 28px rgba(0,0,0,0.35)',
          }}
        >
          {normalizedOptions.map((opt) => {
            const active = opt._norm === normalizedValue;
            return (
              <button
                key={`${opt._norm}:${opt.label}`}
                type="button"
                disabled={Boolean(opt.disabled)}
                onClick={() => {
                  if (opt.disabled) return;
                  onChange?.(opt.value);
                  setOpen(false);
                }}
                className={`w-full flex items-center justify-between gap-2 text-left px-2 py-1.5 rounded-md transition-colors ${sizeClassName} disabled:opacity-45 disabled:cursor-not-allowed`}
                style={{
                  background: active ? 'var(--surface-4)' : 'transparent',
                  color: opt.disabled ? 'var(--text-tertiary)' : (active ? 'var(--text-primary)' : 'var(--text-secondary)'),
                }}
              >
                <span className="truncate">{opt.label}</span>
                {active && <Check className="w-3.5 h-3.5" style={{ color: 'var(--accent)' }} />}
              </button>
            );
          })}
        </div>,
        document.body,
      )}
    </div>
  );
}

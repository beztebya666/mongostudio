import React, { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';

function toNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export default function FloatingMenu({
  open = false,
  anchorRef,
  onClose,
  children,
  align = 'left',
  placement = 'bottom',
  offset = 4,
  viewportMargin = 8,
  minWidth = 120,
  width,
  maxWidth,
  matchAnchorWidth = false,
  zIndex = 260,
  className = '',
  style = {},
}) {
  const menuRef = useRef(null);
  const [position, setPosition] = useState({
    top: -9999,
    left: -9999,
    minWidth: toNumber(minWidth, 120),
    width: undefined,
    maxWidth: undefined,
    origin: 'top left',
    ready: false,
  });

  const updatePosition = useCallback(() => {
    if (!open || !anchorRef?.current || !menuRef.current || typeof window === 'undefined') return;

    const anchorRect = anchorRef.current.getBoundingClientRect();
    const menuRect = menuRef.current.getBoundingClientRect();
    const margin = Math.max(0, toNumber(viewportMargin, 8));
    const gap = Math.max(0, toNumber(offset, 4));
    const requestedWidth = Number.isFinite(Number(width)) ? Math.max(1, Math.round(Number(width))) : null;
    const requestedMaxWidth = Number.isFinite(Number(maxWidth)) ? Math.max(1, Math.round(Number(maxWidth))) : null;
    const nextMinWidth = matchAnchorWidth
      ? Math.max(toNumber(minWidth, 120), Math.round(anchorRect.width))
      : toNumber(minWidth, 120);
    let menuWidth = requestedWidth ?? Math.max(Math.round(menuRect.width), nextMinWidth);
    if (requestedMaxWidth !== null) menuWidth = Math.min(menuWidth, requestedMaxWidth);

    let left = align === 'right' ? anchorRect.right - menuWidth : anchorRect.left;
    const minLeft = margin;
    const maxLeft = Math.max(minLeft, window.innerWidth - menuWidth - margin);
    left = Math.min(maxLeft, Math.max(minLeft, left));

    const desiredPlacement = placement === 'top' ? 'top' : 'bottom';
    let top = desiredPlacement === 'top'
      ? anchorRect.top - menuRect.height - gap
      : anchorRect.bottom + gap;
    let originY = desiredPlacement === 'top' ? 'bottom' : 'top';

    if (desiredPlacement === 'bottom' && top + menuRect.height > window.innerHeight - margin) {
      const altTop = anchorRect.top - menuRect.height - gap;
      if (altTop >= margin) {
        top = altTop;
        originY = 'bottom';
      }
    } else if (desiredPlacement === 'top' && top < margin) {
      const altTop = anchorRect.bottom + gap;
      if (altTop + menuRect.height <= window.innerHeight - margin) {
        top = altTop;
        originY = 'top';
      }
    }

    if (top < margin) top = margin;
    if (top + menuRect.height > window.innerHeight - margin) {
      top = Math.max(margin, window.innerHeight - menuRect.height - margin);
    }

    setPosition({
      top,
      left,
      minWidth: nextMinWidth,
      width: requestedWidth ?? undefined,
      maxWidth: requestedMaxWidth ?? undefined,
      origin: `${originY} ${align === 'right' ? 'right' : 'left'}`,
      ready: true,
    });
  }, [open, anchorRef, align, placement, offset, viewportMargin, minWidth, width, maxWidth, matchAnchorWidth]);

  useLayoutEffect(() => {
    if (!open) return undefined;
    updatePosition();
    const onMove = () => updatePosition();
    window.addEventListener('resize', onMove);
    window.addEventListener('scroll', onMove, true);
    return () => {
      window.removeEventListener('resize', onMove);
      window.removeEventListener('scroll', onMove, true);
    };
  }, [open, updatePosition]);

  useEffect(() => {
    if (!open) return undefined;
    const onMouseDown = (event) => {
      const target = event.target;
      if (anchorRef?.current && anchorRef.current.contains(target)) return;
      if (menuRef.current && menuRef.current.contains(target)) return;
      onClose?.();
    };
    const onKeyDown = (event) => {
      if (event.key === 'Escape') onClose?.();
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [open, onClose, anchorRef]);

  if (!open || typeof document === 'undefined') return null;

  return createPortal(
    <div
      ref={menuRef}
      className={className}
      style={{
        position: 'fixed',
        top: position.top,
        left: position.left,
        minWidth: position.minWidth,
        width: position.width,
        maxWidth: position.maxWidth,
        zIndex,
        opacity: position.ready ? 1 : 0,
        transformOrigin: position.origin,
        ...style,
      }}
    >
      {children}
    </div>,
    document.body,
  );
}

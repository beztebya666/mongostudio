import React, { useEffect, useRef } from 'react';

function findScrollableAncestor(startNode, boundaryNode) {
  let node = startNode instanceof Element ? startNode : null;
  while (node && node !== boundaryNode && node instanceof HTMLElement) {
    const style = window.getComputedStyle(node);
    const overflowY = style.overflowY;
    const canScrollY = (overflowY === 'auto' || overflowY === 'scroll') && node.scrollHeight > node.clientHeight;
    if (canScrollY) return node;
    node = node.parentElement;
  }
  if (boundaryNode instanceof HTMLElement) {
    const style = window.getComputedStyle(boundaryNode);
    const overflowY = style.overflowY;
    const canScrollY = (overflowY === 'auto' || overflowY === 'scroll') && boundaryNode.scrollHeight > boundaryNode.clientHeight;
    if (canScrollY) return boundaryNode;
  }
  return null;
}

export default function AppModal({ open, onClose, maxWidth = 'max-w-md', children }) {
  const panelRef = useRef(null);

  useEffect(() => {
    if (!open) return undefined;
    const body = document.body;
    const html = document.documentElement;
    const prevBodyOverflow = body.style.overflow;
    const prevHtmlOverflow = html.style.overflow;
    body.style.overflow = 'hidden';
    html.style.overflow = 'hidden';
    const onKeyDown = (event) => {
      if (event.key === 'Escape') onClose?.();
    };
    const onWheelCapture = (event) => {
      const panel = panelRef.current;
      if (!panel) {
        event.preventDefault();
        return;
      }
      const target = event.target;
      if (!(target instanceof Node) || !panel.contains(target)) {
        event.preventDefault();
        return;
      }
      const scroller = findScrollableAncestor(target, panel);
      if (!scroller) {
        event.preventDefault();
        return;
      }
      const deltaY = Number(event.deltaY || 0);
      if (deltaY < 0 && scroller.scrollTop <= 0) {
        event.preventDefault();
        return;
      }
      if (deltaY > 0 && scroller.scrollTop + scroller.clientHeight >= scroller.scrollHeight - 1) {
        event.preventDefault();
      }
    };
    const onTouchMoveCapture = (event) => {
      const panel = panelRef.current;
      if (!panel) {
        event.preventDefault();
        return;
      }
      const target = event.target;
      if (!(target instanceof Node) || !panel.contains(target)) {
        event.preventDefault();
      }
    };

    document.addEventListener('keydown', onKeyDown);
    document.addEventListener('wheel', onWheelCapture, { capture: true, passive: false });
    document.addEventListener('touchmove', onTouchMoveCapture, { capture: true, passive: false });
    return () => {
      document.removeEventListener('keydown', onKeyDown);
      document.removeEventListener('wheel', onWheelCapture, { capture: true });
      document.removeEventListener('touchmove', onTouchMoveCapture, { capture: true });
      body.style.overflow = prevBodyOverflow;
      html.style.overflow = prevHtmlOverflow;
    };
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[420] flex items-center justify-center px-4"
      style={{ background: 'rgba(0,0,0,0.72)', overscrollBehavior: 'contain' }}
      role="dialog"
      aria-modal="true"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose?.();
      }}
      onWheelCapture={(event) => {
        event.stopPropagation();
      }}
    >
      <div
        ref={panelRef}
        className={`w-full ${maxWidth} rounded-2xl shadow-2xl overflow-hidden animate-slide-up`}
        style={{ background: 'var(--surface-1)', border: '1px solid var(--border)', maxHeight: '90vh', overscrollBehavior: 'contain' }}
      >
        {children}
      </div>
    </div>
  );
}

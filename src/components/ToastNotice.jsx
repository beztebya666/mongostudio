import React, { useCallback, useEffect, useRef } from 'react';
import { AlertCircle, AlertTriangle, Check, Info, X } from './Icons';

const HOVER_SLOWDOWN = 0.35;

const KIND_META = {
  success: {
    icon: Check,
    container: { background: 'rgba(16,185,129,0.12)', border: '1px solid rgba(16,185,129,0.35)', color: '#6ee7b7' },
    progress: 'rgba(16,185,129,0.9)',
  },
  error: {
    icon: AlertCircle,
    container: { background: 'rgba(239,68,68,0.12)', border: '1px solid rgba(239,68,68,0.35)', color: '#fca5a5' },
    progress: 'rgba(239,68,68,0.9)',
  },
  warning: {
    icon: AlertTriangle,
    container: { background: 'rgba(245,158,11,0.12)', border: '1px solid rgba(245,158,11,0.35)', color: '#fcd34d' },
    progress: 'rgba(245,158,11,0.9)',
  },
  info: {
    icon: Info,
    container: { background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-secondary)' },
    progress: 'var(--accent)',
  },
};

export default function ToastNotice({
  kind = 'info',
  message = '',
  durationMs = 5000,
  onClose,
  className = '',
}) {
  const safeDuration = Math.max(800, Number(durationMs) || 5000);
  const hoverRef = useRef(false);
  const closedRef = useRef(false);
  const onCloseRef = useRef(onClose);
  const progressRef = useRef(null);
  const animationRef = useRef(null);
  const fallbackTimerRef = useRef(null);

  useEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  const closeNotice = useCallback(() => {
    if (closedRef.current) return;
    closedRef.current = true;
    onCloseRef.current?.();
  }, []);

  const setPlaybackRate = useCallback((rate) => {
    if (!animationRef.current) return;
    try {
      animationRef.current.playbackRate = rate;
    } catch {}
  }, []);

  useEffect(() => {
    closedRef.current = false;
    const progressEl = progressRef.current;
    if (!progressEl) return undefined;

    progressEl.style.transform = 'scaleX(1)';
    progressEl.style.transformOrigin = 'left center';

    if (typeof progressEl.animate === 'function') {
      const animation = progressEl.animate(
        [
          { transform: 'scaleX(1)' },
          { transform: 'scaleX(0)' },
        ],
        {
          duration: safeDuration,
          easing: 'linear',
          fill: 'forwards',
        },
      );
      animation.playbackRate = hoverRef.current ? HOVER_SLOWDOWN : 1;
      animationRef.current = animation;
      animation.finished.then(() => {
        closeNotice();
      }).catch(() => {});
      return () => {
        if (animationRef.current === animation) {
          animationRef.current = null;
        }
        animation.cancel();
      };
    }

    fallbackTimerRef.current = window.setTimeout(() => {
      closeNotice();
    }, safeDuration);

    return () => {
      if (fallbackTimerRef.current) {
        clearTimeout(fallbackTimerRef.current);
        fallbackTimerRef.current = null;
      }
    };
  }, [safeDuration, kind, message, closeNotice]);

  const meta = KIND_META[kind] || KIND_META.info;
  const Icon = meta.icon;

  return (
    <div
      className={`rounded-lg shadow-xl overflow-hidden animate-slide-up pointer-events-auto ${className}`}
      style={meta.container}
      onMouseEnter={() => {
        hoverRef.current = true;
        setPlaybackRate(HOVER_SLOWDOWN);
      }}
      onMouseLeave={() => {
        hoverRef.current = false;
        setPlaybackRate(1);
      }}
    >
      <div className="px-3 py-2.5 flex items-center gap-2">
        <Icon className="w-3.5 h-3.5 flex-shrink-0" />
        <div className="text-xs leading-relaxed flex-1 break-words">{message}</div>
        <button
          type="button"
          className="p-0.5 rounded hover:bg-black/10"
          onClick={() => {
            if (animationRef.current) {
              animationRef.current.cancel();
              animationRef.current = null;
            }
            if (fallbackTimerRef.current) {
              clearTimeout(fallbackTimerRef.current);
              fallbackTimerRef.current = null;
            }
            closeNotice();
          }}
        >
          <X className="w-3 h-3 opacity-70" />
        </button>
      </div>
      <div className="h-[2px]" style={{ background: 'rgba(255,255,255,0.18)' }}>
        <div
          ref={progressRef}
          className="h-full"
          style={{
            width: '100%',
            background: meta.progress,
            transform: 'scaleX(1)',
            transformOrigin: 'left center',
            willChange: 'transform',
          }}
        />
      </div>
    </div>
  );
}

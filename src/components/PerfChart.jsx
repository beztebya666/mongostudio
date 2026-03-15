
import React, { useEffect, useRef, useCallback, useState, useMemo } from 'react';

const DPR = typeof window !== 'undefined' ? Math.min(window.devicePixelRatio || 1, 2) : 1;
const MAX_POINTS = 60;
const PAD = { top: 20, right: 10, bottom: 4, left: 48 };

function niceMax(raw) {
  if (raw <= 0) return 10;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const steps = [1, 1.5, 2, 3, 4, 5, 8, 10];
  for (const s of steps) {
    const candidate = s * mag;
    if (candidate >= raw) return candidate;
  }
  return Math.ceil(raw / mag) * mag;
}

function fmtY(v) {
  if (v >= 1e9) return (v / 1e9).toFixed(1) + 'G';
  if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  if (Number.isInteger(v)) return String(v);
  if (v > 0) return v.toFixed(1);
  return '0';
}

function getThemeColors() {
  if (typeof window === 'undefined') return { dark: true, grid: 'rgba(255,255,255,0.05)', label: 'rgba(255,255,255,0.3)', bg: 'rgba(0,0,0,0.22)', cursor: 'rgba(255,255,255,0.35)' };
  const bg = getComputedStyle(document.documentElement).getPropertyValue('--surface-0').trim();
  const isDark = !bg || luma(bg) < 128;
  return isDark
    ? { dark: true, grid: 'rgba(255,255,255,0.05)', label: 'rgba(255,255,255,0.3)', bg: 'rgba(0,0,0,0.22)', cursor: 'rgba(255,255,255,0.35)', topLabel: 'rgba(255,255,255,0.45)', peak: 'rgba(255,255,255,0.13)' }
    : { dark: false, grid: 'rgba(0,0,0,0.06)', label: 'rgba(0,0,0,0.35)', bg: 'rgba(0,0,0,0.03)', cursor: 'rgba(0,0,0,0.3)', topLabel: 'rgba(0,0,0,0.5)', peak: 'rgba(0,0,0,0.08)' };
}

function luma(color) {
  const m = color.match(/\d+/g);
  if (!m || m.length < 3) return 0;
  return 0.299 * Number(m[0]) + 0.587 * Number(m[1]) + 0.114 * Number(m[2]);
}

/**
 * PerfChart — canvas performance chart.
 * Props:
 *   title, unit, series, height
 *   hoverIdx / onHover / onLeave — controlled shared crosshair (optional, falls back to local)
 *   peak — session peak value (shown as dashed horizontal line)
 *   hiddenKeys — Set<string> of hidden series keys
 *   onToggleKey — callback(key) to toggle series visibility
 */
function PerfChart({ title, unit, series, height = 140, hoverIdx: controlledHoverIdx, onHover, onLeave, peak, hiddenKeys, onToggleKey }) {
  const canvasRef = useRef(null);
  const containerRef = useRef(null);
  const prevSeriesRef = useRef(null);
  const animRef = useRef(null);
  const currentSeriesRef = useRef(null);
  const lastWidthRef = useRef(0);

  // Use controlled hover if provided, otherwise local
  const [localHoverIdx, setLocalHoverIdx] = useState(-1);
  const hoverIdx = controlledHoverIdx !== undefined ? controlledHoverIdx : localHoverIdx;

  // Filter out hidden series for drawing
  const visibleSeries = useMemo(() => {
    if (!hiddenKeys || hiddenKeys.size === 0) return series;
    return series.filter((s) => !hiddenKeys.has(s.key));
  }, [series, hiddenKeys]);

  const draw = useCallback((drawSeries, cursorIdx) => {
    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas || !container) return;
    const rect = container.getBoundingClientRect();
    const w = Math.round(rect.width);
    if (w < 10) return;
    const h = height;

    canvas.width = w * DPR;
    canvas.height = h * DPR;
    canvas.style.width = w + 'px';
    canvas.style.height = h + 'px';

    const ctx = canvas.getContext('2d');
    ctx.scale(DPR, DPR);

    const theme = getThemeColors();
    const cw = w - PAD.left - PAD.right;
    const ch = h - PAD.top - PAD.bottom;

    ctx.fillStyle = theme.bg;
    ctx.fillRect(0, 0, w, h);

    // Compute Y max from visible data + session peak
    let rawMax = 0;
    for (const s of drawSeries) {
      if (!s.data) continue;
      for (let i = 0; i < s.data.length; i++) {
        if (s.data[i] > rawMax) rawMax = s.data[i];
      }
    }
    // Use session peak if provided and larger
    const effectivePeak = typeof peak === 'number' && peak > 0 ? peak : 0;
    const maxRaw = Math.max(rawMax, effectivePeak);
    const maxY = niceMax(maxRaw * 1.1);

    // Grid lines
    const gridLines = 4;
    ctx.strokeStyle = theme.grid;
    ctx.lineWidth = 1;
    for (let i = 0; i <= gridLines; i++) {
      const y = Math.round(PAD.top + (ch * i / gridLines)) + 0.5;
      ctx.beginPath();
      ctx.moveTo(PAD.left, y);
      ctx.lineTo(w - PAD.right, y);
      ctx.stroke();
    }

    // Y-axis labels
    ctx.fillStyle = theme.label;
    ctx.font = '10px monospace';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= gridLines; i++) {
      const y = PAD.top + (ch * i / gridLines);
      const val = maxY * (1 - i / gridLines);
      ctx.fillText(fmtY(val), PAD.left - 5, y);
    }

    // Top-left label — show actual session peak, not the rounded axis max
    const displayPeak = effectivePeak > 0 ? effectivePeak : rawMax;
    ctx.fillStyle = theme.topLabel;
    ctx.font = '10px monospace';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'bottom';
    ctx.fillText(fmtY(displayPeak) + (unit ? ' ' + unit : ''), PAD.left + 2, PAD.top - 4);

    // Session peak dashed line
    if (effectivePeak > 0 && maxY > 0) {
      const peakY = PAD.top + ch - (Math.min(effectivePeak, maxY) / maxY) * ch;
      ctx.strokeStyle = theme.peak;
      ctx.lineWidth = 1;
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(PAD.left, peakY);
      ctx.lineTo(w - PAD.right, peakY);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // Series
    const xStep = cw / (MAX_POINTS - 1);
    for (const s of drawSeries) {
      if (!s.data || s.data.length < 1) continue;
      const len = s.data.length;
      const startX = PAD.left + (MAX_POINTS - len) * xStep;

      const pts = new Array(len);
      for (let i = 0; i < len; i++) {
        pts[i] = {
          x: startX + i * xStep,
          y: PAD.top + ch - (Math.min(s.data[i], maxY) / maxY) * ch,
        };
      }

      ctx.beginPath();
      ctx.moveTo(pts[0].x, PAD.top + ch);
      for (let i = 0; i < len; i++) ctx.lineTo(pts[i].x, pts[i].y);
      ctx.lineTo(pts[len - 1].x, PAD.top + ch);
      ctx.closePath();
      ctx.fillStyle = s.color + (theme.dark ? '18' : '22');
      ctx.fill();

      ctx.beginPath();
      ctx.moveTo(pts[0].x, pts[0].y);
      for (let i = 1; i < len; i++) ctx.lineTo(pts[i].x, pts[i].y);
      ctx.strokeStyle = s.color;
      ctx.lineWidth = 1.5;
      ctx.stroke();
    }

    // Cursor line
    if (cursorIdx >= 0) {
      const maxLen = Math.max(...drawSeries.map((s) => (s.data ? s.data.length : 0)), 0);
      if (cursorIdx < maxLen) {
        const startX = PAD.left + (MAX_POINTS - maxLen) * xStep;
        const cx = startX + cursorIdx * xStep;
        ctx.strokeStyle = theme.cursor;
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        ctx.beginPath();
        ctx.moveTo(cx, PAD.top);
        ctx.lineTo(cx, PAD.top + ch);
        ctx.stroke();
        ctx.setLineDash([]);

        for (const s of drawSeries) {
          if (!s.data || cursorIdx >= s.data.length) continue;
          const sy = PAD.top + ch - (Math.min(s.data[cursorIdx], maxY) / maxY) * ch;
          ctx.fillStyle = s.color;
          ctx.beginPath();
          ctx.arc(cx, sy, 3.5, 0, Math.PI * 2);
          ctx.fill();
        }
      }
    }
  }, [height, unit, peak]);

  // Animation between data updates
  useEffect(() => {
    const prev = prevSeriesRef.current;
    const next = visibleSeries;
    currentSeriesRef.current = next;

    if (!prev || prev.length !== next.length || prev.some((s, i) => (s.data?.length || 0) !== (next[i].data?.length || 0))) {
      prevSeriesRef.current = next.map((s) => ({ ...s, data: s.data ? [...s.data] : [] }));
      draw(next, hoverIdx);
      return;
    }

    if (animRef.current) cancelAnimationFrame(animRef.current);

    const duration = 180;
    const start = performance.now();
    const from = prev.map((s) => (s.data ? [...s.data] : []));
    const to = next.map((s) => (s.data ? [...s.data] : []));

    const animate = (now) => {
      const t = Math.min(1, (now - start) / duration);
      const ease = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
      const interpolated = next.map((s, si) => {
        const f = from[si];
        const tgt = to[si];
        const data = tgt.map((v, di) => {
          const fv = di < f.length ? f[di] : 0;
          return fv + (v - fv) * ease;
        });
        return { ...s, data };
      });
      draw(interpolated, hoverIdx);
      if (t < 1) {
        animRef.current = requestAnimationFrame(animate);
      } else {
        prevSeriesRef.current = next.map((s) => ({ ...s, data: s.data ? [...s.data] : [] }));
        animRef.current = null;
      }
    };

    animRef.current = requestAnimationFrame(animate);
    return () => { if (animRef.current) cancelAnimationFrame(animRef.current); };
  }, [visibleSeries, draw, hoverIdx]);

  // ResizeObserver with jitter guard — only redraw if width actually changed
  useEffect(() => {
    const obs = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;
      const w = Math.round(entry.contentRect.width);
      if (w === lastWidthRef.current) return; // skip sub-pixel / no-change
      lastWidthRef.current = w;
      const s = currentSeriesRef.current || visibleSeries;
      draw(s, hoverIdx);
    });
    if (containerRef.current) obs.observe(containerRef.current);
    return () => obs.disconnect();
  }, [draw, visibleSeries, hoverIdx]);

  // Mouse → compute data index, call parent or local
  const handleMouse = useCallback((e) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const w = rect.width;
    const cw = w - PAD.left - PAD.right;
    const xStep = cw / (MAX_POINTS - 1);
    const maxLen = Math.max(...series.map((s) => (s.data ? s.data.length : 0)), 0);
    if (maxLen === 0) return;
    const startX = PAD.left + (MAX_POINTS - maxLen) * xStep;
    const rel = mx - startX;
    const idx = Math.round(rel / xStep);
    const resolved = idx >= 0 && idx < maxLen ? idx : -1;
    if (onHover) {
      onHover(resolved);
    } else {
      setLocalHoverIdx(resolved);
    }
  }, [series, onHover]);

  const handleLeave = useCallback(() => {
    if (onLeave) {
      onLeave();
    } else {
      setLocalHoverIdx(-1);
    }
  }, [onLeave]);

  const displayIdx = hoverIdx >= 0 ? hoverIdx : -1;

  return (
    <div style={{ background: 'var(--perf-card-bg, rgba(0,0,0,0.18))', borderRadius: 8, overflow: 'hidden', border: '1px solid var(--border)' }}>
      <div style={{ padding: '8px 12px 2px' }}>
        <span style={{ color: 'var(--text-secondary)', fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em' }}>{title}</span>
      </div>
      <div ref={containerRef} style={{ width: '100%', cursor: 'crosshair' }}>
        <canvas
          ref={canvasRef}
          style={{ display: 'block' }}
          onMouseMove={handleMouse}
          onMouseLeave={handleLeave}
        />
      </div>
      <div style={{ padding: '4px 12px 8px', display: 'flex', flexWrap: 'wrap', gap: '4px 14px' }}>
        {series.map((s) => {
          const isHidden = hiddenKeys && hiddenKeys.has(s.key);
          const val = !isHidden && displayIdx >= 0 && s.data && displayIdx < s.data.length
            ? s.data[displayIdx]
            : (!isHidden && s.data && s.data.length > 0 ? s.data[s.data.length - 1] : 0);
          return (
            <div
              key={s.key}
              style={{ display: 'flex', alignItems: 'center', gap: 5, cursor: onToggleKey ? 'pointer' : 'default', opacity: isHidden ? 0.35 : 1, userSelect: 'none' }}
              onClick={onToggleKey ? () => onToggleKey(s.key) : undefined}
            >
              <span style={{ width: 8, height: 8, borderRadius: 2, background: s.color, flexShrink: 0, textDecoration: isHidden ? 'line-through' : 'none' }} />
              <span style={{ color: 'var(--text-tertiary)', fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.03em', textDecoration: isHidden ? 'line-through' : 'none' }}>{s.label}</span>
              <span style={{ color: 'var(--text-primary)', fontSize: 11, fontFamily: 'monospace', fontWeight: 700, transition: 'color 0.15s' }}>
                {isHidden ? '--' : fmtY(val)}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default PerfChart;
export { MAX_POINTS, fmtY };

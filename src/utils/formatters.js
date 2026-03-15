export function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

export function formatNumber(num) {
  if (num === null || num === undefined) return '0';
  return num.toLocaleString('en-US');
}

export function formatDuration(ms) {
  if (ms < 1) return '<1ms';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  const min = Math.floor(ms / 60000);
  const sec = Math.round((ms % 60000) / 1000);
  if (min < 60) return `${min}m ${sec}s`;
  const hr = Math.floor(min / 60);
  const remMin = min % 60;
  if (hr < 24) return `${hr}h ${remMin}m`;
  const days = Math.floor(hr / 24);
  return `${days}d ${hr % 24}h`;
}

export function truncateId(id) {
  if (id === null || id === undefined) return '—';
  const str = typeof id === 'object' ? (id.$oid || JSON.stringify(id)) : String(id);
  if (str.length <= 12) return str;
  return str.slice(0, 6) + '…' + str.slice(-4);
}

export function prettyJson(obj) {
  try { return JSON.stringify(obj, null, 2); }
  catch { return String(obj); }
}

export function safeJsonParse(str) {
  try { return { value: JSON.parse(str), error: null }; }
  catch (e) { return { value: null, error: e.message }; }
}

export function getTypeBadge(val) {
  if (val === null) return 'null';
  if (Array.isArray(val)) return 'array';
  if (val && typeof val === 'object' && val.$oid) return 'ObjectId';
  if (val && typeof val === 'object' && val.$date) return 'Date';
  return typeof val;
}

export function copyToClipboard(text) {
  if (!text && text !== '') return Promise.resolve(false);
  const str = typeof text === 'string' ? text : String(text);
  if (navigator?.clipboard?.writeText) {
    return navigator.clipboard.writeText(str).then(() => true).catch(() => fallbackCopy(str));
  }
  return Promise.resolve(fallbackCopy(str));
}

function fallbackCopy(str) {
  try {
    const ta = document.createElement('textarea');
    ta.value = str;
    ta.style.cssText = 'position:fixed;left:-9999px;top:-9999px;opacity:0';
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand('copy');
    document.body.removeChild(ta);
    return ok;
  } catch { return false; }
}

export function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

export function formatNumber(num) {
  if (num === undefined || num === null) return '—';
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + 'M';
  if (num >= 1_000) return (num / 1_000).toFixed(1) + 'K';
  return num.toLocaleString();
}

export function formatDuration(ms) {
  if (ms < 1) return '<1ms';
  if (ms < 1000) return Math.round(ms) + 'ms';
  return (ms / 1000).toFixed(2) + 's';
}

export function timeAgo(date) {
  const seconds = Math.floor((Date.now() - new Date(date)) / 1000);
  if (seconds < 60) return 'just now';
  if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
  if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
  return Math.floor(seconds / 86400) + 'd ago';
}

export function getTypeColor(value) {
  if (value === null || value === undefined) return 'json-null';
  if (typeof value === 'string') return 'json-string';
  if (typeof value === 'number') return 'json-number';
  if (typeof value === 'boolean') return 'json-boolean';
  if (Array.isArray(value)) return 'json-bracket';
  if (typeof value === 'object') {
    if (value.$oid) return 'json-objectid';
    return 'json-bracket';
  }
  return 'text-text-secondary';
}

export function getTypeBadge(value) {
  if (value === null) return { label: 'null', cls: 'badge-purple' };
  if (value === undefined) return { label: 'undefined', cls: 'badge-purple' };
  if (typeof value === 'string') return { label: 'str', cls: 'badge-green' };
  if (typeof value === 'number') return { label: 'num', cls: 'badge-yellow' };
  if (typeof value === 'boolean') return { label: 'bool', cls: 'badge-purple' };
  if (Array.isArray(value)) return { label: `arr[${value.length}]`, cls: 'badge-blue' };
  if (typeof value === 'object') {
    if (value.$oid) return { label: 'ObjectId', cls: 'badge-red' };
    if (value.$date) return { label: 'Date', cls: 'badge-yellow' };
    return { label: 'obj', cls: 'badge-blue' };
  }
  return { label: typeof value, cls: 'badge-purple' };
}

export function truncateId(id) {
  if (!id) return '';
  const s = typeof id === 'object' ? (id.$oid || JSON.stringify(id)) : String(id);
  if (s.length <= 12) return s;
  return s.slice(0, 6) + '…' + s.slice(-4);
}

export function safeJsonParse(str) {
  try {
    return { data: JSON.parse(str), error: null };
  } catch (e) {
    return { data: null, error: e.message };
  }
}

export function prettyJson(obj, indent = 2) {
  return JSON.stringify(obj, null, indent);
}

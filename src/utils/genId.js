export function genId() {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  if (globalThis.crypto?.getRandomValues) {
    const arr = new Uint32Array(2);
    globalThis.crypto.getRandomValues(arr);
    return `${arr[0].toString(36)}-${arr[1].toString(36)}`;
  }
  const perfPart = typeof performance !== 'undefined'
    ? Math.floor(performance.now() * 1000)
    : 0;
  return `${Date.now()}-${perfPart.toString(36)}-${Math.random().toString(36).slice(2)}`;
}


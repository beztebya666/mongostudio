import MockApiClient from './mockApi';

const BASE = '/api';
const DB_EXPORT_TIMEOUT_DEFAULT_MS = 4 * 60 * 60 * 1000;

function parseUsernameFromUri(uri) {
  try {
    const match = String(uri || '').match(/^mongodb(?:\+srv)?:\/\/([^@/]+)@/i);
    if (!match) return '';
    const userInfo = match[1];
    const sep = userInfo.indexOf(':');
    const encoded = sep === -1 ? userInfo : userInfo.slice(0, sep);
    return decodeURIComponent(encoded);
  } catch {
    return '';
  }
}

class HttpApiClient {
  constructor() {
    this.connectionId = null;
    this.uiUsername = null;
    this.pendingControllers = new Set();
  }

  withSource(path, source = '') {
    const value = String(source || '').trim();
    if (!value) return path;
    return `${path}${path.includes('?') ? '&' : '?'}source=${encodeURIComponent(value)}`;
  }

  normalizeConsoleShell(shell = 'mongosh') {
    const value = String(shell || '').trim().toLowerCase();
    if (!value) return 'mongosh';
    if (value === 'mongo' || value === 'legacy') return 'mongo';
    return 'mongosh';
  }

  async request(path, options = {}) {
    const start = performance.now();
    const externalController = options._controller;
    const skipAbortTracking = options._skipAbortTracking === true;
    const suppressErrorLog = options._suppressErrorLog === true;
    const controller = externalController || new AbortController();
    const budget = options._budget && typeof options._budget === 'object' ? options._budget : null;
    const heavyConfirm = options._heavyConfirm === true;
    const heavyTimeoutMs = Number(options._heavyTimeoutMs);
    const extraHeaders = options._headers && typeof options._headers === 'object' ? options._headers : null;
    const method = options.method || 'GET';
    const headers = {
      'Content-Type': 'application/json',
      ...(this.connectionId && { 'X-Connection-Id': this.connectionId }),
      ...(this.uiUsername && { 'X-UI-User': this.uiUsername }),
      ...(extraHeaders || {}),
    };
    if (Number.isFinite(Number(budget?.timeoutMs))) headers['X-Query-Timeout-MS'] = String(Math.round(Number(budget.timeoutMs)));
    if (Number.isFinite(Number(budget?.limit))) headers['X-Query-Limit'] = String(Math.round(Number(budget.limit)));
    if (heavyConfirm) headers['X-Heavy-Confirm'] = '1';
    if (Number.isFinite(heavyTimeoutMs)) headers['X-Heavy-Timeout-MS'] = String(Math.round(heavyTimeoutMs));
    const fetchOpts = {
      headers,
      ...options,
    };
    delete fetchOpts._controller;
    delete fetchOpts._budget;
    delete fetchOpts._heavyConfirm;
    delete fetchOpts._heavyTimeoutMs;
    delete fetchOpts._headers;
    delete fetchOpts._skipAbortTracking;
    delete fetchOpts._suppressErrorLog;
    if (controller) fetchOpts.signal = controller.signal;
    if (!skipAbortTracking && controller) this.pendingControllers.add(controller);

    let res;
    try {
      res = await fetch(`${BASE}${path}`, fetchOpts);
    } catch (err) {
      const abortedBySignal = Boolean(controller?.signal?.aborted);
      const abortedByName = err?.name === 'AbortError';
      const abortedByMessage = /(?:^|\s)(?:abort|aborted)(?:\s|$)/i.test(String(err?.message || ''));
      if (abortedBySignal || abortedByName || abortedByMessage) {
        const abortError = new Error('Request aborted');
        abortError.name = 'AbortError';
        throw abortError;
      }
      const error = new Error(`Network error: ${err.message}`);
      error.errorType = 'network';
      error.details = err.message;
      console.error(`[API] ${method} ${path} network error`, err);
      throw error;
    } finally {
      if (!skipAbortTracking && controller) this.pendingControllers.delete(controller);
    }
    const elapsed = performance.now() - start;

    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: res.statusText, errorType: 'http' }));
      const error = new Error(err.error || `Request failed: ${res.status}`);
      error.errorType = err.errorType || 'unknown';
      error.details = err.details || null;
      error.status = res.status;
      if (!suppressErrorLog) {
        console.error(`[API] ${method} ${path} failed (${res.status})`, { errorType: error.errorType, details: error.details, message: error.message });
      }
      throw error;
    }

    const data = await res.json();
    data._elapsed = Math.round(elapsed);
    return data;
  }

  async requestText(path, options = {}) {
    const start = performance.now();
    const externalController = options._controller;
    const skipAbortTracking = options._skipAbortTracking === true;
    const suppressErrorLog = options._suppressErrorLog === true;
    const onProgress = typeof options._onProgress === 'function' ? options._onProgress : null;
    const controller = externalController || new AbortController();
    const budget = options._budget && typeof options._budget === 'object' ? options._budget : null;
    const heavyConfirm = options._heavyConfirm === true;
    const heavyTimeoutMs = Number(options._heavyTimeoutMs);
    const extraHeaders = options._headers && typeof options._headers === 'object' ? options._headers : null;
    const method = options.method || 'GET';
    const headers = {
      ...(options.body !== undefined ? { 'Content-Type': 'application/json' } : {}),
      ...(this.connectionId && { 'X-Connection-Id': this.connectionId }),
      ...(this.uiUsername && { 'X-UI-User': this.uiUsername }),
      ...(extraHeaders || {}),
    };
    if (Number.isFinite(Number(budget?.timeoutMs))) headers['X-Query-Timeout-MS'] = String(Math.round(Number(budget.timeoutMs)));
    if (Number.isFinite(Number(budget?.limit))) headers['X-Query-Limit'] = String(Math.round(Number(budget.limit)));
    if (heavyConfirm) headers['X-Heavy-Confirm'] = '1';
    if (Number.isFinite(heavyTimeoutMs)) headers['X-Heavy-Timeout-MS'] = String(Math.round(heavyTimeoutMs));

    const fetchOpts = {
      headers,
      ...options,
    };
    delete fetchOpts._controller;
    delete fetchOpts._budget;
    delete fetchOpts._heavyConfirm;
    delete fetchOpts._heavyTimeoutMs;
    delete fetchOpts._headers;
    delete fetchOpts._skipAbortTracking;
    delete fetchOpts._suppressErrorLog;
    delete fetchOpts._onProgress;
    if (controller) fetchOpts.signal = controller.signal;
    if (!skipAbortTracking && controller) this.pendingControllers.add(controller);

    let res;
    try {
      res = await fetch(`${BASE}${path}`, fetchOpts);
    } catch (err) {
      const abortedBySignal = Boolean(controller?.signal?.aborted);
      const abortedByName = err?.name === 'AbortError';
      const abortedByMessage = /(?:^|\s)(?:abort|aborted)(?:\s|$)/i.test(String(err?.message || ''));
      if (abortedBySignal || abortedByName || abortedByMessage) {
        const abortError = new Error('Request aborted');
        abortError.name = 'AbortError';
        throw abortError;
      }
      const error = new Error(`Network error: ${err.message}`);
      error.errorType = 'network';
      error.details = err.message;
      console.error(`[API] ${method} ${path} network error`, err);
      throw error;
    } finally {
      if (!skipAbortTracking && controller) this.pendingControllers.delete(controller);
    }
    const elapsed = performance.now() - start;

    if (!res.ok) {
      let payload = null;
      try {
        payload = await res.json();
      } catch {
        try {
          const text = await res.text();
          payload = { error: text || res.statusText, errorType: 'http' };
        } catch {
          payload = { error: res.statusText, errorType: 'http' };
        }
      }
      const error = new Error(payload?.error || `Request failed: ${res.status}`);
      error.errorType = payload?.errorType || 'unknown';
      error.details = payload?.details || null;
      error.status = res.status;
      if (!suppressErrorLog) {
        console.error(`[API] ${method} ${path} failed (${res.status})`, { errorType: error.errorType, details: error.details, message: error.message });
      }
      throw error;
    }

    if (!res.body || !onProgress) {
      const text = await res.text();
      return { text, _elapsed: Math.round(elapsed) };
    }

    const totalHeader = Number(res.headers.get('content-length'));
    const totalBytes = Number.isFinite(totalHeader) && totalHeader > 0 ? totalHeader : null;
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let receivedBytes = 0;
    const parts = [];
    let lastProgressTs = 0;
    try {
      while (true) {
        // eslint-disable-next-line no-await-in-loop
        const { value, done } = await reader.read();
        if (done) break;
        if (value) {
          receivedBytes += value.byteLength;
          parts.push(decoder.decode(value, { stream: true }));
          const now = performance.now();
          if (now - lastProgressTs >= 120) {
            const percent = totalBytes ? Math.max(0, Math.min(100, Math.round((receivedBytes / totalBytes) * 100))) : null;
            onProgress({ receivedBytes, totalBytes, percent, done: false });
            lastProgressTs = now;
          }
        }
      }
      parts.push(decoder.decode());
    } finally {
      try { reader.releaseLock(); } catch {}
    }

    const percent = totalBytes ? 100 : null;
    onProgress({ receivedBytes, totalBytes, percent, done: true });
    return { text: parts.join(''), _elapsed: Math.round(elapsed) };
  }

  async streamToFile(path, {
    method = 'GET',
    body,
    filename = 'export.json',
    mime = 'application/json',
    heavyConfirm = false,
    heavyTimeoutMs,
    budget,
    onProgress,
    controller: externalController,
  } = {}) {
    if (typeof window === 'undefined' || typeof window.showSaveFilePicker !== 'function') {
      throw new Error('Direct stream-to-disk export is not supported in this browser.');
    }

    const suggestedName = String(filename || 'export.json');
    const extensionMatch = suggestedName.match(/(\.[a-z0-9]+)$/i);
    const extension = extensionMatch ? extensionMatch[1] : '.json';
    const fileHandle = await window.showSaveFilePicker({
      suggestedName,
      types: [
        {
          description: 'Export file',
          accept: { [mime || 'application/json']: [extension] },
        },
      ],
    });
    const writable = await fileHandle.createWritable();
    let writerClosed = false;
    const safeClose = async () => {
      if (writerClosed) return;
      writerClosed = true;
      await writable.close();
    };
    const safeAbort = async () => {
      if (writerClosed) return;
      writerClosed = true;
      try { await writable.abort(); } catch {}
    };

    const start = performance.now();
    const skipAbortTracking = false;
    const suppressErrorLog = false;
    const controller = externalController || new AbortController();
    const normalizedBudget = budget && typeof budget === 'object' ? budget : null;
    const normalizedHeavyTimeoutMs = Number(heavyTimeoutMs);
    const headers = {
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
      ...(this.connectionId && { 'X-Connection-Id': this.connectionId }),
      ...(this.uiUsername && { 'X-UI-User': this.uiUsername }),
    };
    if (Number.isFinite(Number(normalizedBudget?.timeoutMs))) headers['X-Query-Timeout-MS'] = String(Math.round(Number(normalizedBudget.timeoutMs)));
    if (Number.isFinite(Number(normalizedBudget?.limit))) headers['X-Query-Limit'] = String(Math.round(Number(normalizedBudget.limit)));
    if (heavyConfirm) headers['X-Heavy-Confirm'] = '1';
    if (Number.isFinite(normalizedHeavyTimeoutMs)) headers['X-Heavy-Timeout-MS'] = String(Math.round(normalizedHeavyTimeoutMs));

    const fetchOpts = {
      method,
      headers,
      ...(body !== undefined ? { body } : {}),
      signal: controller.signal,
    };
    if (!skipAbortTracking && controller) this.pendingControllers.add(controller);

    let res;
    try {
      res = await fetch(`${BASE}${path}`, fetchOpts);
    } catch (err) {
      await safeAbort();
      const abortedBySignal = Boolean(controller?.signal?.aborted);
      const abortedByName = err?.name === 'AbortError';
      const abortedByMessage = /(?:^|\s)(?:abort|aborted)(?:\s|$)/i.test(String(err?.message || ''));
      if (abortedBySignal || abortedByName || abortedByMessage) {
        const abortError = new Error('Request aborted');
        abortError.name = 'AbortError';
        throw abortError;
      }
      const error = new Error(`Network error: ${err.message}`);
      error.errorType = 'network';
      error.details = err.message;
      console.error(`[API] ${method} ${path} network error`, err);
      throw error;
    } finally {
      if (!skipAbortTracking && controller) this.pendingControllers.delete(controller);
    }
    const elapsed = performance.now() - start;

    if (!res.ok) {
      await safeAbort();
      let payload = null;
      try {
        payload = await res.json();
      } catch {
        try {
          const text = await res.text();
          payload = { error: text || res.statusText, errorType: 'http' };
        } catch {
          payload = { error: res.statusText, errorType: 'http' };
        }
      }
      const error = new Error(payload?.error || `Request failed: ${res.status}`);
      error.errorType = payload?.errorType || 'unknown';
      error.details = payload?.details || null;
      error.status = res.status;
      if (!suppressErrorLog) {
        console.error(`[API] ${method} ${path} failed (${res.status})`, { errorType: error.errorType, details: error.details, message: error.message });
      }
      throw error;
    }

    if (!res.body) {
      const text = await res.text();
      await writable.write(text);
      await safeClose();
      if (typeof onProgress === 'function') {
        onProgress({ receivedBytes: text.length, totalBytes: text.length, percent: 100, done: true });
      }
      return { receivedBytes: text.length, totalBytes: text.length, filename: suggestedName, _elapsed: Math.round(elapsed) };
    }

    const totalHeader = Number(res.headers.get('content-length'));
    const totalBytes = Number.isFinite(totalHeader) && totalHeader > 0 ? totalHeader : null;
    const reader = res.body.getReader();
    let receivedBytes = 0;
    let lastProgressTs = 0;

    try {
      while (true) {
        // eslint-disable-next-line no-await-in-loop
        const { value, done } = await reader.read();
        if (done) break;
        if (value) {
          receivedBytes += value.byteLength;
          // eslint-disable-next-line no-await-in-loop
          await writable.write(value);
          const now = performance.now();
          if (typeof onProgress === 'function' && now - lastProgressTs >= 120) {
            const percent = totalBytes ? Math.max(0, Math.min(100, Math.round((receivedBytes / totalBytes) * 100))) : null;
            onProgress({ receivedBytes, totalBytes, percent, done: false });
            lastProgressTs = now;
          }
        }
      }
      await safeClose();
      if (typeof onProgress === 'function') {
        onProgress({
          receivedBytes,
          totalBytes,
          percent: totalBytes ? 100 : null,
          done: true,
        });
      }
      return { receivedBytes, totalBytes, filename: suggestedName, _elapsed: Math.round(elapsed) };
    } catch (err) {
      await safeAbort();
      throw err;
    } finally {
      try { reader.releaseLock(); } catch {}
    }
  }

  abortInFlight(reason = 'disconnect') {
    const controllers = [...this.pendingControllers];
    this.pendingControllers.clear();
    controllers.forEach((controller) => {
      try { controller.abort(reason); } catch {}
    });
  }

  async connect(uri, options = {}) {
    const data = await this.request('/connect', {
      method: 'POST',
      body: JSON.stringify({ uri, options }),
    });
    this.connectionId = data.connectionId;
    this.uiUsername = data.username || options.username || parseUsernameFromUri(uri) || null;
    return data;
  }

  async disconnect() {
    this.abortInFlight('disconnect');
    if (!this.connectionId) return;
    await this.request('/disconnect', { method: 'POST', _skipAbortTracking: true }).catch(() => {});
    this.connectionId = null;
    this.uiUsername = null;
  }

  async listDatabases() { return this.request('/databases'); }

  async createDatabase(name) {
    return this.request('/databases', { method: 'POST', body: JSON.stringify({ name }) });
  }

  async dropDatabase(db, options = {}) {
    return this.request(this.withSource(`/databases/${encodeURIComponent(db)}`, options.source), {
      method: 'DELETE',
      _heavyConfirm: options.heavyConfirm !== false,
      _heavyTimeoutMs: options.heavyTimeoutMs,
      _controller: options.controller,
    });
  }

  async listCollections(db, { withStats = false, source = '', controller } = {}) {
    const params = new URLSearchParams();
    if (withStats) params.set('withStats', '1');
    const suffix = params.toString() ? `?${params.toString()}` : '';
    return this.request(this.withSource(`/databases/${encodeURIComponent(db)}/collections${suffix}`, source), {
      _controller: controller,
    });
  }

  async getDatabaseStats(db, { refresh = false, budget, source = '', controller } = {}) {
    const params = new URLSearchParams();
    if (refresh) params.set('refresh', '1');
    return this.request(
      this.withSource(`/databases/${encodeURIComponent(db)}/stats${params.toString() ? `?${params.toString()}` : ''}`, source),
      { _budget: budget, _controller: controller },
    );
  }

  async createCollection(db, name, options = {}) {
    return this.request(this.withSource(`/databases/${encodeURIComponent(db)}/collections`, options.source), {
      method: 'POST', body: JSON.stringify({ name }), _controller: options.controller,
    });
  }

  async dropCollection(db, col, options = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}`, {
      method: 'DELETE',
      _heavyConfirm: options.heavyConfirm !== false,
      _heavyTimeoutMs: options.heavyTimeoutMs,
    });
  }

  async getCollectionStats(db, col, { refresh = false, budget, source = '', controller } = {}) {
    const params = new URLSearchParams();
    if (refresh) params.set('refresh', '1');
    return this.request(
      this.withSource(
        `/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/stats${params.toString() ? `?${params.toString()}` : ''}`,
        source,
      ),
      { _budget: budget, _controller: controller },
    );
  }

  async getSchema(db, col, sample = 100, options = {}) {
    return this.request(
      `/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/schema?sample=${sample}`,
      { _budget: options.budget },
    );
  }

  async getDocuments(db, col, { filter = '{}', sort = '{}', skip = 0, limit = 50, projection = '{}', hint = 'auto', keysetCursor } = {}, controller, options = {}) {
    const params = new URLSearchParams({ filter, sort, skip, limit, projection });
    if (hint && hint !== 'auto') params.set('hint', hint);
    if (keysetCursor) params.set('keysetCursor', keysetCursor);
    return this.request(this.withSource(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents?${params}`, options.source), {
      _controller: controller,
      _budget: options.budget,
    });
  }

  async getDocument(db, col, id) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents/${encodeURIComponent(id)}`);
  }

  async insertDocument(db, col, document) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents`, {
      method: 'POST', body: JSON.stringify({ document }),
    });
  }

  async insertDocuments(db, col, documents = [], options = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents/bulk`, {
      method: 'POST',
      body: JSON.stringify({ documents }),
      _heavyConfirm: options.heavyConfirm !== false,
      _heavyTimeoutMs: options.heavyTimeoutMs,
    });
  }

  async updateDocument(db, col, id, update) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents/${encodeURIComponent(id)}`, {
      method: 'PUT', body: JSON.stringify({ update }),
    });
  }

  async deleteDocument(db, col, id) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  async deleteMany(db, col, filter, options = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents`, {
      method: 'DELETE', body: JSON.stringify({ filter }),
      _heavyConfirm: options.heavyConfirm !== false,
      _heavyTimeoutMs: options.heavyTimeoutMs,
    });
  }

  async operateCollection(db, col, method, payload = {}, options = {}) {
    return this.request(this.withSource(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/operate`, options.source), {
      method: 'POST',
      body: JSON.stringify({ method, payload }),
      _heavyConfirm: options.heavyConfirm === true,
      _heavyTimeoutMs: options.heavyTimeoutMs,
      _budget: options.budget,
      _controller: options.controller,
    });
  }

  async getIndexes(db, col, options = {}) {
    return this.request(this.withSource(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/indexes`, options.source), {
      _budget: options.budget,
      _controller: options.controller,
    });
  }

  async createIndex(db, col, keys, options = {}) {
    const { source, controller, ...indexOptions } = options || {};
    return this.request(this.withSource(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/indexes`, source), {
      method: 'POST',
      body: JSON.stringify({ keys, options: indexOptions }),
      _controller: controller,
    });
  }

  async dropIndex(db, col, name, options = {}) {
    return this.request(this.withSource(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/indexes/${encodeURIComponent(name)}`, options.source), {
      method: 'DELETE',
      _heavyConfirm: options.heavyConfirm !== false,
      _heavyTimeoutMs: options.heavyTimeoutMs,
      _controller: options.controller,
    });
  }

  async runAggregation(db, col, pipeline, controller, options = {}) {
    const hint = typeof options?.hint === 'string' ? options.hint.trim() : '';
    return this.request(this.withSource(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/aggregate`, options.source), {
      method: 'POST',
      body: JSON.stringify({ pipeline, hint: hint || 'auto' }),
      _controller: controller,
      _budget: options.budget,
    });
  }

  async explain(db, col, { type = 'find', filter, pipeline, sort, hint = 'auto', limit, verbosity } = {}, options = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/explain`, {
      method: 'POST',
      body: JSON.stringify({ type, filter, pipeline, sort, hint, limit, verbosity }),
      _controller: options.controller,
      _budget: options.budget,
    });
  }

  async exportData(db, col, { format = 'json', filter = '{}', sort = '{}', limit = 1000, projection = '{}' } = {}, options = {}) {
    const rawLimit = String(limit ?? '').trim().toLowerCase();
    const numericLimit = Number(limit);
    const isLargeByMode = rawLimit === 'exact' || rawLimit === 'unlimited' || rawLimit === 'all';
    const isLargeByValue = Number.isFinite(numericLimit) ? numericLimit > 5000 : false;
    const heavyTimeoutMs = Number.isFinite(Number(options.heavyTimeoutMs))
      ? Number(options.heavyTimeoutMs)
      : (isLargeByMode ? 1800000 : isLargeByValue && numericLimit > 50000 ? 600000 : isLargeByValue ? 180000 : 120000);
    const response = await this.requestText(this.withSource(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/export`, options.source), {
      method: 'POST',
      body: JSON.stringify({ format, filter, sort, limit: String(limit), projection }),
      _heavyConfirm: options.heavyConfirm === true || isLargeByMode || isLargeByValue,
      _heavyTimeoutMs: heavyTimeoutMs,
      _controller: options.controller,
      _budget: options.budget,
      _onProgress: options.onProgress,
    });
    let data;
    try {
      data = JSON.parse(response.text);
    } catch (err) {
      const text = String(response?.text || '');
      const looksTruncated = !text.trimEnd().endsWith('}');
      const parseError = new Error(looksTruncated
        ? 'Export stream interrupted before completion. Check server logs and retry.'
        : `Invalid export response: ${err.message}`);
      parseError.errorType = looksTruncated ? 'stream' : 'parse';
      throw parseError;
    }
    data._elapsed = response._elapsed;
    return data;
  }

  async exportDataToFile(db, col, { format = 'json', filter = '{}', sort = '{}', limit = 1000, projection = '{}' } = {}, options = {}) {
    if (typeof this.streamToFile !== 'function') {
      throw new Error('Direct stream-to-disk export is not supported in this runtime.');
    }
    const normalizedFormat = String(format || 'json').trim().toLowerCase() === 'csv' ? 'csv' : 'json';
    const rawLimit = String(limit ?? '').trim().toLowerCase();
    const numericLimit = Number(limit);
    const isLargeByMode = rawLimit === 'exact' || rawLimit === 'unlimited' || rawLimit === 'all';
    const isLargeByValue = Number.isFinite(numericLimit) ? numericLimit > 5000 : false;
    const heavyTimeoutMs = Number.isFinite(Number(options.heavyTimeoutMs))
      ? Number(options.heavyTimeoutMs)
      : (isLargeByMode ? 1800000 : isLargeByValue && numericLimit > 50000 ? 600000 : isLargeByValue ? 180000 : 120000);
    const ext = normalizedFormat === 'csv' ? 'csv' : 'json';
    const mime = normalizedFormat === 'csv' ? 'text/csv' : 'application/json';
    const suggestedName = String(options.filename || `${db}.${col}.${ext}`);
    return this.streamToFile(
      this.withSource(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/export?raw=1`, options.source),
      {
        method: 'POST',
        body: JSON.stringify({
          format: normalizedFormat,
          filter,
          sort,
          limit: String(limit),
          projection,
        }),
        filename: suggestedName,
        mime,
        heavyConfirm: options.heavyConfirm === true || isLargeByMode || isLargeByValue,
        heavyTimeoutMs,
        budget: options.budget,
        onProgress: options.onProgress,
        controller: options.controller,
      },
    );
  }

  async exportDatabase(db, {
    includeDocuments = true,
    includeIndexes = true,
    includeOptions = true,
    includeSchema = true,
    limitPerCollection = 0,
    schemaSampleSize = 150,
  } = {}, options = {}) {
    const heavyTimeoutMs = Number.isFinite(Number(options.heavyTimeoutMs))
      ? Number(options.heavyTimeoutMs)
      : DB_EXPORT_TIMEOUT_DEFAULT_MS;
    const response = await this.requestText(`/databases/${encodeURIComponent(db)}/export`, {
      method: 'POST',
      body: JSON.stringify({
        includeDocuments,
        includeIndexes,
        includeOptions,
        includeSchema,
        limitPerCollection,
        schemaSampleSize,
      }),
      _heavyConfirm: includeDocuments ? options.heavyConfirm !== false : options.heavyConfirm === true,
      _heavyTimeoutMs: heavyTimeoutMs,
      _controller: options.controller,
      _onProgress: options.onProgress,
    });
    let data;
    try {
      data = JSON.parse(response.text);
    } catch (err) {
      const text = String(response?.text || '');
      const looksTruncated = !text.trimEnd().endsWith('}');
      const parseError = new Error(looksTruncated
        ? 'Export stream interrupted before completion. Check server logs and retry.'
        : `Invalid export response: ${err.message}`);
      parseError.errorType = looksTruncated ? 'stream' : 'parse';
      throw parseError;
    }
    data._elapsed = response._elapsed;
    return data;
  }

  async exportDatabaseToFile(db, {
    includeDocuments = true,
    includeIndexes = true,
    includeOptions = true,
    includeSchema = true,
    limitPerCollection = 0,
    schemaSampleSize = 150,
  } = {}, options = {}) {
    const heavyTimeoutMs = Number.isFinite(Number(options.heavyTimeoutMs))
      ? Number(options.heavyTimeoutMs)
      : DB_EXPORT_TIMEOUT_DEFAULT_MS;
    const archive = options.archive === true;
    const defaultName = archive ? `${db}.mongostudio-db.zip` : `${db}.mongostudio-db.json`;
    const suggestedName = String(options.filename || defaultName);
    const rawPath = archive
      ? `/databases/${encodeURIComponent(db)}/export?raw=1&zip=1`
      : `/databases/${encodeURIComponent(db)}/export?raw=1`;
    return this.streamToFile(rawPath, {
      method: 'POST',
      body: JSON.stringify({
        includeDocuments,
        includeIndexes,
        includeOptions,
        includeSchema,
        limitPerCollection,
        schemaSampleSize,
      }),
      filename: suggestedName,
      mime: archive ? 'application/zip' : 'application/json',
      heavyConfirm: includeDocuments ? options.heavyConfirm !== false : options.heavyConfirm === true,
      heavyTimeoutMs,
      budget: options.budget,
      onProgress: options.onProgress,
      controller: options.controller,
    });
  }

  async importDatabase(pkg, { targetDb = '', mode = 'merge' } = {}, options = {}) {
    return this.request('/databases/import', {
      method: 'POST',
      body: JSON.stringify({ package: pkg, targetDb, mode }),
      _heavyConfirm: options.heavyConfirm !== false,
      _heavyTimeoutMs: options.heavyTimeoutMs,
    });
  }

  async importCollection(db, { name, documents = [], indexes = [], options = {}, dropExisting = false } = {}, requestOptions = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/import`, {
      method: 'POST',
      body: JSON.stringify({ name, documents, indexes, options, dropExisting }),
      _heavyConfirm: requestOptions.heavyConfirm !== false,
      _heavyTimeoutMs: requestOptions.heavyTimeoutMs,
    });
  }

  async getMetadataOverview({ refresh = false, budget } = {}) {
    const params = new URLSearchParams();
    if (refresh) params.set('refresh', '1');
    return this.request(`/metadata/overview${params.toString() ? `?${params.toString()}` : ''}`, { _budget: budget });
  }

  async startExactTotal(db, col, {
    filter = '{}',
    projection = '{}',
    hint = 'auto',
    timeoutMs = 30000,
  } = {}, options = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/total/exact`, {
      method: 'POST',
      body: JSON.stringify({ filter, projection, hint, timeoutMs }),
      _heavyConfirm: options.heavyConfirm !== false,
      _heavyTimeoutMs: options.heavyTimeoutMs || timeoutMs,
    });
  }

  async getJob(jobId, options = {}) {
    return this.request(`/jobs/${encodeURIComponent(jobId)}`, { _budget: options.budget });
  }

  async preflight(db, col, payload = {}, options = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/preflight`, {
      method: 'POST',
      body: JSON.stringify(payload || {}),
      _budget: options.budget,
    });
  }

  async createConsoleSession(scope = {}, options = {}) {
    const shell = this.normalizeConsoleShell(options.shell);
    return this.request(`/console/${encodeURIComponent(shell)}/sessions`, {
      method: 'POST',
      body: JSON.stringify({ scope }),
    });
  }

  async sendConsoleCommand(sessionId, command, options = {}) {
    const shell = this.normalizeConsoleShell(options.shell);
    return this.request(`/console/${encodeURIComponent(shell)}/sessions/${encodeURIComponent(sessionId)}/command`, {
      method: 'POST',
      body: JSON.stringify({ command }),
    });
  }

  async interruptConsoleSession(sessionId, options = {}) {
    const shell = this.normalizeConsoleShell(options.shell);
    return this.request(`/console/${encodeURIComponent(shell)}/sessions/${encodeURIComponent(sessionId)}/interrupt`, {
      method: 'POST',
      body: JSON.stringify({}),
    });
  }

  async closeConsoleSession(sessionId, options = {}) {
    const shell = this.normalizeConsoleShell(options.shell);
    return this.request(`/console/${encodeURIComponent(shell)}/sessions/${encodeURIComponent(sessionId)}`, {
      method: 'DELETE',
      _skipAbortTracking: true,
      _suppressErrorLog: true,
    });
  }

  async streamConsoleSession(sessionId, handlers = {}, options = {}) {
    const {
      onEvent,
      onError,
      onClose,
      signal,
      since,
    } = handlers || {};
    const shell = this.normalizeConsoleShell(options.shell);
    const params = new URLSearchParams();
    if (Number.isFinite(Number(since)) && Number(since) > 0) params.set('since', String(Math.round(Number(since))));
    const suffix = params.toString() ? `?${params.toString()}` : '';
    const headers = {
      Accept: 'text/event-stream',
      ...(this.connectionId && { 'X-Connection-Id': this.connectionId }),
      ...(this.uiUsername && { 'X-UI-User': this.uiUsername }),
    };

    const response = await fetch(`${BASE}/console/${encodeURIComponent(shell)}/sessions/${encodeURIComponent(sessionId)}/stream${suffix}`, {
      method: 'GET',
      headers,
      signal,
    });

    if (!response.ok) {
      let errorMessage = `Request failed: ${response.status}`;
      try {
        const payload = await response.json();
        if (payload?.error) errorMessage = payload.error;
      } catch {}
      const err = new Error(errorMessage);
      err.status = response.status;
      throw err;
    }

    const reader = response.body?.getReader?.();
    if (!reader) throw new Error('Streaming is not supported by this browser.');
    const decoder = new TextDecoder();
    let buffer = '';
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        buffer = consumeSseBuffer(buffer, onEvent);
      }
      buffer += decoder.decode();
      buffer = consumeSseBuffer(buffer, onEvent);
      if (String(buffer || '').trim()) {
        onEvent?.({ event: 'message', id: null, data: parseSsePayload(buffer.trim()), raw: buffer.trim() });
      }
    } catch (err) {
      const abortedByName = err?.name === 'AbortError';
      const abortedBySignal = signal?.aborted === true;
      if (!abortedByName && !abortedBySignal) {
        onError?.(err);
        throw err;
      }
    } finally {
      try { reader.releaseLock(); } catch {}
      onClose?.();
    }
  }

  async createMongoshSession(scope = {}) {
    return this.createConsoleSession(scope, { shell: 'mongosh' });
  }

  async sendMongoshCommand(sessionId, command) {
    return this.sendConsoleCommand(sessionId, command, { shell: 'mongosh' });
  }

  async interruptMongoshSession(sessionId) {
    return this.interruptConsoleSession(sessionId, { shell: 'mongosh' });
  }

  async closeMongoshSession(sessionId) {
    return this.closeConsoleSession(sessionId, { shell: 'mongosh' });
  }

  async streamMongoshSession(sessionId, handlers = {}) {
    return this.streamConsoleSession(sessionId, handlers, { shell: 'mongosh' });
  }

  async getExecutionConfig() { return this.request('/execution-config'); }

  async setExecutionConfig(config) {
    const data = await this.request('/execution-config', { method: 'PUT', body: JSON.stringify(config) });
    try {
      if (typeof window !== 'undefined') {
        window.dispatchEvent(new CustomEvent('mongostudio:exec-config', { detail: data }));
      }
    } catch {}
    return data;
  }

  async getServerManagementContext(options = {}) {
    const params = new URLSearchParams();
    if (options.refresh === true) params.set('refresh', '1');
    return this.request(`/server-management/context${params.toString() ? `?${params.toString()}` : ''}`, {
      _controller: options.controller,
    });
  }

  async getServerManagementTool(tool, payload = {}, options = {}) {
    const toolId = String(tool || '').trim();
    if (!toolId) throw new Error('Server-management tool id is required.');
    const query = new URLSearchParams();
    const source = payload && typeof payload === 'object' ? payload : {};
    const append = (key, value) => {
      if (value === undefined || value === null || value === '') return;
      query.set(key, String(value));
    };
    append('node', source.node);
    append('path', source.path);
    append('confirmNodeSelection', source.confirmNodeSelection ? '1' : '');
    append('confirmPathSelection', source.confirmPathSelection ? '1' : '');
    append('thresholdMs', source.thresholdMs);
    append('limit', source.limit);
    append('timeoutMs', source.timeoutMs);
    const suffix = query.toString() ? `?${query.toString()}` : '';
    return this.request(`/server-management/tools/${encodeURIComponent(toolId)}${suffix}`, {
      _controller: options.controller,
    });
  }

  async runServerManagementTool(tool, payload = {}, options = {}) {
    const toolId = String(tool || '').trim();
    if (!toolId) throw new Error('Server-management tool id is required.');
    return this.request(`/server-management/tools/${encodeURIComponent(toolId)}/run`, {
      method: 'POST',
      body: JSON.stringify(payload || {}),
      _heavyConfirm: options.heavyConfirm !== false,
      _heavyTimeoutMs: options.heavyTimeoutMs,
      _controller: options.controller,
    });
  }

  async killOp(opid, node = '', options = {}) {
    return this.request('/server-management/kill-op', {
      method: 'POST',
      body: JSON.stringify({ opid, node }),
      _heavyConfirm: true,
      _controller: options.controller,
    });
  }

  async getServerStatus() { return this.request('/status'); }
  async getConnectionInfo() { return this.request('/connection-info'); }
  async getHealth() { return this.request('/health'); }
  async getMetrics() { return this.request('/metrics'); }
  async getServiceConfig() { return this.request('/service-config'); }
  async setServiceConfig(config = {}) {
    return this.request('/service-config', { method: 'PUT', body: JSON.stringify(config) });
  }
  async getAdminAccessStatus() { return this.request('/admin-access/status'); }
  async getAdminAccess() { return this.request('/admin-access'); }
  async verifyAdminAccess(key) {
    return this.request('/admin-access/verify', { method: 'POST', body: JSON.stringify({ key }) });
  }
  async revokeAdminAccess() {
    return this.request('/admin-access/revoke', { method: 'POST' });
  }
  async getAuditLog({ action = '', source = '', method = '', scope = '', search = '', from = null, to = null, limit = 200 } = {}, options = {}) {
    const params = new URLSearchParams();
    if (action) params.set('action', action);
    if (source) params.set('source', source);
    if (method) params.set('method', method);
    if (scope) params.set('scope', scope);
    if (search) params.set('search', search);
    if (from) params.set('from', String(from));
    if (to) params.set('to', String(to));
    if (limit) params.set('limit', String(limit));
    return this.request(`/audit${params.toString() ? `?${params.toString()}` : ''}`, {
      _controller: options.controller,
    });
  }

  async getDistinct(db, col, field, options = {}) {
    return this.request(this.withSource(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/distinct/${encodeURIComponent(field)}`, options.source), {
      _controller: options.controller,
    });
  }
}

function parseSsePayload(text = '') {
  const value = String(text || '');
  if (!value) return '';
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function consumeSseBuffer(buffer, onEvent) {
  let rest = String(buffer || '');
  while (true) {
    const splitAt = rest.search(/\r?\n\r?\n/);
    if (splitAt === -1) break;
    const block = rest.slice(0, splitAt);
    rest = rest.slice(splitAt).replace(/^\r?\n\r?\n/, '');
    const lines = block.split(/\r?\n/);
    let eventName = 'message';
    let eventId = null;
    const dataParts = [];
    for (const line of lines) {
      if (!line || line.startsWith(':')) continue;
      if (line.startsWith('event:')) {
        eventName = line.slice(6).trim() || 'message';
      } else if (line.startsWith('id:')) {
        const parsedId = Number(line.slice(3).trim());
        eventId = Number.isFinite(parsedId) ? parsedId : line.slice(3).trim();
      } else if (line.startsWith('data:')) {
        dataParts.push(line.slice(5).trimStart());
      }
    }
    if (dataParts.length === 0) continue;
    const raw = dataParts.join('\n');
    onEvent?.({
      event: eventName,
      id: eventId,
      data: parseSsePayload(raw),
      raw,
    });
  }
  return rest;
}

class ApiRouter {
  constructor() {
    this.realClient = new HttpApiClient();
    this.mockClient = new MockApiClient();
    this.mode = 'real';
  }

  setMode(mode = 'real') {
    this.mode = mode === 'mock' ? 'mock' : 'real';
    if (this.mode === 'mock') {
      this.realClient.connectionId = null;
      this.realClient.uiUsername = null;
    } else {
      this.mockClient.connectionId = null;
      this.mockClient.uiUsername = null;
      this.mockClient.session = null;
    }
    return this.mode;
  }

  getMode() {
    return this.mode;
  }

  isMockMode() {
    return this.mode === 'mock';
  }

  abortInFlight(reason = 'disconnect') {
    try { this.realClient.abortInFlight(reason); } catch {}
    try { this.mockClient.abortInFlight?.(reason); } catch {}
  }

  async resetMockDemo(options = {}) {
    return this.mockClient.resetDemoState?.(options);
  }

  get activeClient() {
    return this.mode === 'mock' ? this.mockClient : this.realClient;
  }
}

const router = new ApiRouter();

export const setApiMode = (mode) => router.setMode(mode);
export const getApiMode = () => router.getMode();
export const isMockApiMode = () => router.isMockMode();

export const api = new Proxy(router, {
  get(target, prop) {
    if (prop in target) {
      const value = target[prop];
      return typeof value === 'function' ? value.bind(target) : value;
    }
    const value = target.activeClient[prop];
    return typeof value === 'function' ? value.bind(target.activeClient) : value;
  },
  set(target, prop, value) {
    if (prop in target) {
      target[prop] = value;
      return true;
    }
    target.activeClient[prop] = value;
    return true;
  },
});

export default api;

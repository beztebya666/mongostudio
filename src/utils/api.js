import MockApiClient from './mockApi';

const BASE = '/api';

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
  }

  async request(path, options = {}) {
    const start = performance.now();
    const controller = options._controller;
    const method = options.method || 'GET';
    const fetchOpts = {
      headers: {
        'Content-Type': 'application/json',
        ...(this.connectionId && { 'X-Connection-Id': this.connectionId }),
        ...(this.uiUsername && { 'X-UI-User': this.uiUsername }),
      },
      ...options,
    };
    delete fetchOpts._controller;
    if (controller) fetchOpts.signal = controller.signal;

    let res;
    try {
      res = await fetch(`${BASE}${path}`, fetchOpts);
    } catch (err) {
      const error = new Error(`Network error: ${err.message}`);
      error.errorType = 'network';
      error.details = err.message;
      console.error(`[API] ${method} ${path} network error`, err);
      throw error;
    }
    const elapsed = performance.now() - start;

    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: res.statusText, errorType: 'http' }));
      const error = new Error(err.error || `Request failed: ${res.status}`);
      error.errorType = err.errorType || 'unknown';
      error.details = err.details || null;
      error.status = res.status;
      console.error(`[API] ${method} ${path} failed (${res.status})`, { errorType: error.errorType, details: error.details, message: error.message });
      throw error;
    }

    const data = await res.json();
    data._elapsed = Math.round(elapsed);
    return data;
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
    if (!this.connectionId) return;
    await this.request('/disconnect', { method: 'POST' }).catch(() => {});
    this.connectionId = null;
    this.uiUsername = null;
  }

  async listDatabases() { return this.request('/databases'); }

  async createDatabase(name) {
    return this.request('/databases', { method: 'POST', body: JSON.stringify({ name }) });
  }

  async dropDatabase(db) {
    return this.request(`/databases/${encodeURIComponent(db)}`, { method: 'DELETE' });
  }

  async listCollections(db, { withStats = false } = {}) {
    const params = new URLSearchParams();
    if (withStats) params.set('withStats', '1');
    const suffix = params.toString() ? `?${params.toString()}` : '';
    return this.request(`/databases/${encodeURIComponent(db)}/collections${suffix}`);
  }

  async getDatabaseStats(db) {
    return this.request(`/databases/${encodeURIComponent(db)}/stats`);
  }

  async createCollection(db, name) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections`, {
      method: 'POST', body: JSON.stringify({ name }),
    });
  }

  async dropCollection(db, col) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}`, { method: 'DELETE' });
  }

  async getCollectionStats(db, col) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/stats`);
  }

  async getSchema(db, col, sample = 100) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/schema?sample=${sample}`);
  }

  async getDocuments(db, col, { filter = '{}', sort = '{}', skip = 0, limit = 50, projection = '{}', hint = 'auto' } = {}, controller) {
    const params = new URLSearchParams({ filter, sort, skip, limit, projection });
    if (hint && hint !== 'auto') params.set('hint', hint);
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents?${params}`, { _controller: controller });
  }

  async getDocument(db, col, id) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents/${encodeURIComponent(id)}`);
  }

  async insertDocument(db, col, document) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents`, {
      method: 'POST', body: JSON.stringify({ document }),
    });
  }

  async insertDocuments(db, col, documents = []) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents/bulk`, {
      method: 'POST',
      body: JSON.stringify({ documents }),
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

  async deleteMany(db, col, filter) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents`, {
      method: 'DELETE', body: JSON.stringify({ filter }),
    });
  }

  async getIndexes(db, col) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/indexes`);
  }

  async createIndex(db, col, keys, options = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/indexes`, {
      method: 'POST', body: JSON.stringify({ keys, options }),
    });
  }

  async dropIndex(db, col, name) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/indexes/${encodeURIComponent(name)}`, { method: 'DELETE' });
  }

  async runAggregation(db, col, pipeline, controller) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/aggregate`, {
      method: 'POST', body: JSON.stringify({ pipeline }), _controller: controller,
    });
  }

  async explain(db, col, { type = 'find', filter, pipeline, sort, hint = 'auto' } = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/explain`, {
      method: 'POST', body: JSON.stringify({ type, filter, pipeline, sort, hint }),
    });
  }

  async exportData(db, col, { format = 'json', filter = '{}', sort = '{}', limit = 1000, projection = '{}' } = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/export`, {
      method: 'POST', body: JSON.stringify({ format, filter, sort, limit: String(limit), projection }),
    });
  }

  async exportDatabase(db, {
    includeDocuments = true,
    includeIndexes = true,
    includeOptions = true,
    includeSchema = true,
    limitPerCollection = 0,
    schemaSampleSize = 150,
  } = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/export`, {
      method: 'POST',
      body: JSON.stringify({
        includeDocuments,
        includeIndexes,
        includeOptions,
        includeSchema,
        limitPerCollection,
        schemaSampleSize,
      }),
    });
  }

  async importDatabase(pkg, { targetDb = '', mode = 'merge' } = {}) {
    return this.request('/databases/import', {
      method: 'POST',
      body: JSON.stringify({ package: pkg, targetDb, mode }),
    });
  }

  async importCollection(db, { name, documents = [], indexes = [], options = {}, dropExisting = false } = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/import`, {
      method: 'POST',
      body: JSON.stringify({ name, documents, indexes, options, dropExisting }),
    });
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

  async getServerStatus() { return this.request('/status'); }
  async getHealth() { return this.request('/health'); }
  async getMetrics() { return this.request('/metrics'); }
  async getServiceConfig() { return this.request('/service-config'); }
  async setServiceConfig(config = {}) {
    return this.request('/service-config', { method: 'PUT', body: JSON.stringify(config) });
  }
  async getAuditLog({ action = '', search = '', from = null, to = null, limit = 200 } = {}) {
    const params = new URLSearchParams();
    if (action) params.set('action', action);
    if (search) params.set('search', search);
    if (from) params.set('from', String(from));
    if (to) params.set('to', String(to));
    if (limit) params.set('limit', String(limit));
    return this.request(`/audit${params.toString() ? `?${params.toString()}` : ''}`);
  }

  async getDistinct(db, col, field) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/distinct/${encodeURIComponent(field)}`);
  }
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

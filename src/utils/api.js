const BASE = '/api';

class ApiClient {
  constructor() {
    this.connectionId = null;
  }

  async request(path, options = {}) {
    const start = performance.now();
    const controller = options._controller;
    const method = options.method || 'GET';
    const fetchOpts = {
      headers: {
        'Content-Type': 'application/json',
        ...(this.connectionId && { 'X-Connection-Id': this.connectionId }),
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
    return data;
  }

  async disconnect() {
    if (!this.connectionId) return;
    await this.request('/disconnect', { method: 'POST' }).catch(() => {});
    this.connectionId = null;
  }

  async listDatabases() { return this.request('/databases'); }

  async createDatabase(name) {
    return this.request('/databases', { method: 'POST', body: JSON.stringify({ name }) });
  }

  async dropDatabase(db) {
    return this.request(`/databases/${encodeURIComponent(db)}`, { method: 'DELETE' });
  }

  async listCollections(db) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections`);
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

  async exportData(db, col, { format = 'json', filter = '{}', limit = 1000, projection = '{}' } = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/export`, {
      method: 'POST', body: JSON.stringify({ format, filter, limit: String(limit), projection }),
    });
  }

  async getExecutionConfig() { return this.request('/execution-config'); }

  async setExecutionConfig(config) {
    return this.request('/execution-config', { method: 'PUT', body: JSON.stringify(config) });
  }

  async getServerStatus() { return this.request('/status'); }
  async getHealth() { return this.request('/health'); }
  async getMetrics() { return this.request('/metrics'); }
  async getAuditLog() { return this.request('/audit'); }

  async getDistinct(db, col, field) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/distinct/${encodeURIComponent(field)}`);
  }
}

export const api = new ApiClient();
export default api;

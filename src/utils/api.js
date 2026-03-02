const BASE = '/api';

class ApiClient {
  constructor() {
    this.connectionId = null;
  }

  async request(path, options = {}) {
    const start = performance.now();
    const res = await fetch(`${BASE}${path}`, {
      headers: {
        'Content-Type': 'application/json',
        ...(this.connectionId && { 'X-Connection-Id': this.connectionId }),
      },
      ...options,
    });

    const elapsed = performance.now() - start;

    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: res.statusText }));
      throw new Error(err.error || `Request failed: ${res.status}`);
    }

    const data = await res.json();
    data._elapsed = Math.round(elapsed);
    return data;
  }

  async connect(uri) {
    const data = await this.request('/connect', {
      method: 'POST',
      body: JSON.stringify({ uri }),
    });
    this.connectionId = data.connectionId;
    return data;
  }

  async disconnect() {
    if (!this.connectionId) return;
    await this.request('/disconnect', { method: 'POST' }).catch(() => {});
    this.connectionId = null;
  }

  async listDatabases() {
    return this.request('/databases');
  }

  async listCollections(db) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections`);
  }

  async getCollectionStats(db, col) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/stats`);
  }

  async getDocuments(db, col, { filter = '{}', sort = '{}', skip = 0, limit = 50, projection = '{}' } = {}) {
    const params = new URLSearchParams({ filter, sort, skip, limit, projection });
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents?${params}`);
  }

  async getDocument(db, col, id) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents/${encodeURIComponent(id)}`);
  }

  async insertDocument(db, col, document) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents`, {
      method: 'POST',
      body: JSON.stringify({ document }),
    });
  }

  async updateDocument(db, col, id, update) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents/${encodeURIComponent(id)}`, {
      method: 'PUT',
      body: JSON.stringify({ update }),
    });
  }

  async deleteDocument(db, col, id) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents/${encodeURIComponent(id)}`, {
      method: 'DELETE',
    });
  }

  async deleteMany(db, col, filter) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/documents`, {
      method: 'DELETE',
      body: JSON.stringify({ filter }),
    });
  }

  async getIndexes(db, col) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/indexes`);
  }

  async createIndex(db, col, keys, options = {}) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/indexes`, {
      method: 'POST',
      body: JSON.stringify({ keys, options }),
    });
  }

  async dropIndex(db, col, name) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/indexes/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
  }

  async runAggregation(db, col, pipeline) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}/aggregate`, {
      method: 'POST',
      body: JSON.stringify({ pipeline }),
    });
  }

  async dropCollection(db, col) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections/${encodeURIComponent(col)}`, {
      method: 'DELETE',
    });
  }

  async createCollection(db, name) {
    return this.request(`/databases/${encodeURIComponent(db)}/collections`, {
      method: 'POST',
      body: JSON.stringify({ name }),
    });
  }

  async dropDatabase(db) {
    return this.request(`/databases/${encodeURIComponent(db)}`, {
      method: 'DELETE',
    });
  }

  async getServerStatus() {
    return this.request('/status');
  }
}

export const api = new ApiClient();
export default api;

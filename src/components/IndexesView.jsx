import React, { useState, useEffect, useCallback } from 'react';
import api from '../utils/api';
import { Key, Plus, Trash, Loader, AlertCircle, Check, X, Refresh, Zap } from './Icons';
import { formatBytes, formatNumber, formatDuration } from '../utils/formatters';

export default function IndexesView({ db, collection, onQueryMs }) {
  const [indexes, setIndexes] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showCreate, setShowCreate] = useState(false);
  const [newKeys, setNewKeys] = useState('{ "field": 1 }');
  const [newOptions, setNewOptions] = useState('{}');
  const [creating, setCreating] = useState(false);
  const [confirmDrop, setConfirmDrop] = useState(null);

  const loadIndexes = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await api.getIndexes(db, collection);
      setIndexes(data.indexes || []);
      onQueryMs?.(data._elapsed);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [db, collection, onQueryMs]);

  useEffect(() => {
    loadIndexes();
  }, [loadIndexes]);

  const handleCreate = async () => {
    setCreating(true);
    setError(null);
    try {
      const keys = JSON.parse(newKeys);
      const options = JSON.parse(newOptions);
      await api.createIndex(db, collection, keys, options);
      setShowCreate(false);
      setNewKeys('{ "field": 1 }');
      setNewOptions('{}');
      loadIndexes();
    } catch (err) {
      setError(err.message);
    } finally {
      setCreating(false);
    }
  };

  const handleDrop = async (name) => {
    try {
      await api.dropIndex(db, collection, name);
      setConfirmDrop(null);
      loadIndexes();
    } catch (err) {
      setError(err.message);
    }
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="flex-shrink-0 border-b border-border bg-surface-1/30 px-4 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h3 className="text-sm font-semibold flex items-center gap-2">
            <Key className="w-4 h-4 text-accent/70" />
            Indexes
          </h3>
          <span className="badge-blue">{indexes.length}</span>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={() => setShowCreate(!showCreate)} className="btn-ghost flex items-center gap-1.5 text-accent text-xs">
            <Plus className="w-3.5 h-3.5" />
            Create Index
          </button>
          <button onClick={loadIndexes} className="btn-ghost p-1.5">
            <Refresh className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* Create Index Form */}
      {showCreate && (
        <div className="border-b border-border bg-surface-1/50 p-4 animate-slide-up">
          <div className="grid grid-cols-2 gap-3 mb-3">
            <div>
              <label className="block text-2xs text-text-tertiary uppercase tracking-wider mb-1">Keys</label>
              <input
                type="text"
                value={newKeys}
                onChange={(e) => setNewKeys(e.target.value)}
                className="input-field text-xs"
                placeholder='{ "field": 1 }'
              />
            </div>
            <div>
              <label className="block text-2xs text-text-tertiary uppercase tracking-wider mb-1">Options</label>
              <input
                type="text"
                value={newOptions}
                onChange={(e) => setNewOptions(e.target.value)}
                className="input-field text-xs"
                placeholder='{ "unique": true }'
              />
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button onClick={handleCreate} disabled={creating} className="btn-primary flex items-center gap-1.5 text-xs">
              {creating ? <Loader className="w-3.5 h-3.5" /> : <Check className="w-3.5 h-3.5" />}
              Create
            </button>
            <button onClick={() => setShowCreate(false)} className="btn-ghost text-xs">Cancel</button>
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="mx-4 mt-3 flex items-start gap-2 text-red-400 text-xs bg-red-500/5 border border-red-500/20 rounded-lg p-3">
          <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
          <span className="flex-1">{error}</span>
          <button onClick={() => setError(null)}><X className="w-3.5 h-3.5 text-red-400/50" /></button>
        </div>
      )}

      {/* Index List */}
      <div className="flex-1 overflow-auto">
        {loading && indexes.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <Loader className="text-accent" />
          </div>
        ) : (
          <div className="divide-y divide-border">
            {indexes.map((idx) => (
              <div key={idx.name} className="px-4 py-3 hover:bg-surface-1/20 transition-colors group">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <Key className="w-3.5 h-3.5 text-accent/50" />
                    <span className="text-sm font-mono font-medium">{idx.name}</span>
                    {idx.unique && <span className="badge-yellow">unique</span>}
                    {idx.sparse && <span className="badge-purple">sparse</span>}
                    {idx.name === '_id_' && <span className="badge-green">default</span>}
                  </div>
                  {idx.name !== '_id_' && (
                    <div className="opacity-0 group-hover:opacity-100 transition-opacity">
                      {confirmDrop === idx.name ? (
                        <div className="flex items-center gap-2">
                          <button
                            onClick={() => handleDrop(idx.name)}
                            className="text-xs px-2 py-1 bg-red-500/10 text-red-400 rounded hover:bg-red-500/20"
                          >
                            Confirm Drop
                          </button>
                          <button
                            onClick={() => setConfirmDrop(null)}
                            className="text-xs text-text-tertiary hover:text-text-secondary"
                          >
                            Cancel
                          </button>
                        </div>
                      ) : (
                        <button
                          onClick={() => setConfirmDrop(idx.name)}
                          className="p-1.5 rounded-md hover:bg-red-500/10 text-text-tertiary hover:text-red-400 transition-colors"
                        >
                          <Trash className="w-3.5 h-3.5" />
                        </button>
                      )}
                    </div>
                  )}
                </div>
                <div className="flex items-center gap-4 text-2xs text-text-tertiary font-mono">
                  <span>keys: {JSON.stringify(idx.key)}</span>
                  {idx.v !== undefined && <span>v{idx.v}</span>}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

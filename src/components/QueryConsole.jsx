import React, { useState, useRef, useCallback, useEffect } from 'react';
import api from '../utils/api';
import { Play, Loader, AlertCircle, X, Zap, Copy, Check } from './Icons';
import { formatDuration, formatNumber, safeJsonParse, prettyJson } from '../utils/formatters';
import JsonView from './JsonView';

const TEMPLATES = [
  { label: 'Find All', query: 'db.collection.find({})' },
  { label: 'Find with Filter', query: 'db.collection.find({ "field": "value" })' },
  { label: 'Aggregate', query: 'db.collection.aggregate([\n  { "$match": {} },\n  { "$group": { "_id": "$field", "count": { "$sum": 1 } } },\n  { "$sort": { "count": -1 } }\n])' },
  { label: 'Count', query: 'db.collection.find({}).count()' },
  { label: 'Distinct', query: 'db.collection.distinct("field")' },
];

export default function QueryConsole({ db, collection, onQueryMs }) {
  const [query, setQuery] = useState(`db.${collection}.find({})`);
  const [results, setResults] = useState(null);
  const [error, setError] = useState(null);
  const [running, setRunning] = useState(false);
  const [elapsed, setElapsed] = useState(null);
  const [resultCount, setResultCount] = useState(0);
  const [history, setHistory] = useState([]);
  const [copied, setCopied] = useState(false);
  const textareaRef = useRef(null);

  useEffect(() => {
    setQuery(`db.${collection}.find({})`);
    setResults(null);
    setError(null);
  }, [collection]);

  const parseQuery = (queryStr) => {
    // Parse pseudo-mongo shell syntax
    const cleaned = queryStr.trim();

    // Match: db.collection.aggregate([...])
    const aggMatch = cleaned.match(/\.aggregate\(\s*(\[[\s\S]*\])\s*\)/);
    if (aggMatch) {
      return { type: 'aggregate', pipeline: aggMatch[1] };
    }

    // Match: db.collection.find({...}).sort({...}).limit(N)
    const findMatch = cleaned.match(/\.find\(\s*(\{[\s\S]*?\})\s*(?:,\s*(\{[\s\S]*?\}))?\s*\)/);
    if (findMatch) {
      let filter = findMatch[1];
      let projection = findMatch[2] || '{}';

      // Check for .count()
      if (cleaned.includes('.count()')) {
        return { type: 'count', filter };
      }

      let sort = '{}';
      let limit = '50';

      const sortMatch = cleaned.match(/\.sort\(\s*(\{[\s\S]*?\})\s*\)/);
      if (sortMatch) sort = sortMatch[1];

      const limitMatch = cleaned.match(/\.limit\(\s*(\d+)\s*\)/);
      if (limitMatch) limit = limitMatch[1];

      return { type: 'find', filter, projection, sort, limit };
    }

    // Match: db.collection.distinct("field")
    const distinctMatch = cleaned.match(/\.distinct\(\s*"([^"]+)"\s*\)/);
    if (distinctMatch) {
      return { type: 'distinct', field: distinctMatch[1] };
    }

    // Fallback: try as raw JSON filter
    return { type: 'find', filter: cleaned, projection: '{}', sort: '{}', limit: '50' };
  };

  const execute = async () => {
    setError(null);
    setRunning(true);
    setResults(null);

    try {
      const parsed = parseQuery(query);
      let data;

      if (parsed.type === 'aggregate') {
        const { data: pipeline, error: jsonErr } = safeJsonParse(parsed.pipeline);
        if (jsonErr) throw new Error(`Invalid pipeline JSON: ${jsonErr}`);
        data = await api.runAggregation(db, collection, pipeline);
        setResults(data.results || []);
        setResultCount(data.results?.length || 0);
      } else if (parsed.type === 'count') {
        const { data: filter, error: jsonErr } = safeJsonParse(parsed.filter);
        if (jsonErr) throw new Error(`Invalid filter JSON: ${jsonErr}`);
        data = await api.getDocuments(db, collection, { filter: parsed.filter, limit: 0 });
        setResults([{ count: data.total }]);
        setResultCount(1);
      } else if (parsed.type === 'find') {
        const { error: filterErr } = safeJsonParse(parsed.filter);
        if (filterErr) throw new Error(`Invalid filter JSON: ${filterErr}`);
        data = await api.getDocuments(db, collection, {
          filter: parsed.filter,
          sort: parsed.sort,
          projection: parsed.projection,
          limit: parseInt(parsed.limit) || 50,
        });
        setResults(data.documents || []);
        setResultCount(data.total || 0);
      }

      setElapsed(data?._elapsed);
      onQueryMs?.(data?._elapsed);

      // Add to history
      setHistory(prev => [
        { query, ts: Date.now(), elapsed: data?._elapsed, count: data?.total || data?.results?.length || 0 },
        ...prev.slice(0, 19),
      ]);
    } catch (err) {
      setError(err.message);
    } finally {
      setRunning(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      execute();
    }
  };

  const handleTemplate = (tpl) => {
    setQuery(tpl.query.replace('collection', collection));
  };

  const handleCopyResults = () => {
    if (results) {
      navigator.clipboard.writeText(prettyJson(results));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const lineCount = query.split('\n').length;

  return (
    <div className="h-full flex flex-col">
      {/* Query Input Area */}
      <div className="flex-shrink-0 border-b border-border">
        {/* Template chips */}
        <div className="px-4 pt-3 pb-2 flex items-center gap-1.5 overflow-x-auto">
          {TEMPLATES.map((tpl) => (
            <button
              key={tpl.label}
              onClick={() => handleTemplate(tpl)}
              className="px-2.5 py-1 rounded-full bg-surface-2 border border-border text-2xs text-text-secondary
                         hover:border-accent/30 hover:text-accent transition-all whitespace-nowrap"
            >
              {tpl.label}
            </button>
          ))}
        </div>

        {/* Editor */}
        <div className="px-4 pb-3">
          <div className="bg-surface-2 border border-border rounded-xl overflow-hidden flex" style={{ minHeight: '100px', maxHeight: '200px' }}>
            <div className="py-3 pl-3 pr-2 text-right select-none border-r border-border bg-surface-1/50 flex-shrink-0">
              {Array.from({ length: lineCount }, (_, i) => (
                <div key={i} className="text-2xs text-text-tertiary leading-[1.6rem] font-mono">{i + 1}</div>
              ))}
            </div>
            <textarea
              ref={textareaRef}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={handleKeyDown}
              className="flex-1 bg-transparent text-xs font-mono text-text-primary p-3 resize-none focus:outline-none leading-[1.6rem]"
              spellCheck={false}
            />
          </div>
        </div>

        {/* Execute bar */}
        <div className="px-4 pb-3 flex items-center justify-between">
          <div className="flex items-center gap-3 text-2xs text-text-tertiary">
            <span>
              <kbd className="px-1 py-0.5 bg-surface-3 rounded text-2xs">⌘ Enter</kbd> to execute
            </span>
          </div>
          <button
            onClick={execute}
            disabled={running}
            className="btn-primary flex items-center gap-1.5"
          >
            {running ? <Loader className="w-3.5 h-3.5" /> : <Play className="w-3.5 h-3.5" />}
            Execute
          </button>
        </div>
      </div>

      {/* Results */}
      <div className="flex-1 overflow-auto">
        {error && (
          <div className="m-4 flex items-start gap-2 text-red-400 text-xs bg-red-500/5 border border-red-500/20 rounded-lg p-3 animate-fade-in">
            <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
            <span className="flex-1 font-mono">{error}</span>
          </div>
        )}

        {results && (
          <div className="animate-fade-in">
            {/* Results header */}
            <div className="px-4 py-2 border-b border-border flex items-center justify-between bg-surface-1/20">
              <div className="flex items-center gap-3 text-xs">
                <span className="text-text-secondary font-medium">
                  {formatNumber(resultCount)} result{resultCount !== 1 ? 's' : ''}
                </span>
                {elapsed && (
                  <span className="flex items-center gap-1 text-text-tertiary">
                    <Zap className="w-3 h-3 text-accent/70" />
                    {formatDuration(elapsed)}
                  </span>
                )}
              </div>
              <button
                onClick={handleCopyResults}
                className="btn-ghost flex items-center gap-1 text-2xs"
              >
                {copied ? <Check className="w-3 h-3 text-accent" /> : <Copy className="w-3 h-3" />}
                {copied ? 'Copied' : 'Copy All'}
              </button>
            </div>

            {/* Results list */}
            <div className="divide-y divide-border">
              {results.map((doc, i) => (
                <div key={i} className="px-4 py-3 hover:bg-surface-1/20 transition-colors">
                  <div className="bg-surface-2 border border-border rounded-xl p-3 overflow-auto max-h-[300px]">
                    <JsonView data={doc} />
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {!results && !error && (
          <div className="flex items-center justify-center h-full text-text-tertiary text-sm">
            Run a query to see results
          </div>
        )}
      </div>
    </div>
  );
}

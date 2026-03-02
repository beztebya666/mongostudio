import React, { useState, useEffect, useRef } from 'react';
import { Logo, Zap, Server, Loader, AlertCircle } from './Icons';

const RECENT_KEY = 'mongostudio_recent';

function getRecent() {
  try {
    return JSON.parse(localStorage.getItem(RECENT_KEY) || '[]').slice(0, 5);
  } catch { return []; }
}

function saveRecent(uri) {
  try {
    const masked = maskUri(uri);
    const list = getRecent().filter(r => r.masked !== masked);
    list.unshift({ uri, masked, ts: Date.now() });
    localStorage.setItem(RECENT_KEY, JSON.stringify(list.slice(0, 5)));
  } catch {}
}

function maskUri(uri) {
  try {
    const u = new URL(uri);
    if (u.password) u.password = '••••';
    return u.toString();
  } catch {
    return uri.replace(/:([^@/]+)@/, ':••••@');
  }
}

export default function ConnectPage({ onConnect, connecting, error }) {
  const [uri, setUri] = useState('');
  const [recent, setRecent] = useState([]);
  const inputRef = useRef(null);

  useEffect(() => {
    setRecent(getRecent());
    inputRef.current?.focus();
  }, []);

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!uri.trim() || connecting) return;
    saveRecent(uri.trim());
    onConnect(uri.trim());
  };

  const handleRecent = (item) => {
    setUri(item.uri);
    onConnect(item.uri);
  };

  return (
    <div className="h-screen w-screen flex items-center justify-center noise-bg overflow-hidden relative">
      {/* Background effects */}
      <div className="absolute inset-0 overflow-hidden">
        <div className="absolute top-1/4 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-accent/[0.04] rounded-full blur-[120px]" />
        <div className="absolute bottom-0 left-1/4 w-[400px] h-[400px] bg-accent/[0.02] rounded-full blur-[100px]" />
        <div
          className="absolute inset-0 opacity-[0.015]"
          style={{
            backgroundImage: `radial-gradient(circle at 1px 1px, white 1px, transparent 0)`,
            backgroundSize: '48px 48px',
          }}
        />
      </div>

      <div className="relative z-10 w-full max-w-md px-6">
        {/* Logo & Header */}
        <div className="text-center mb-10 float-in">
          <div className="inline-flex items-center justify-center mb-6">
            <Logo size={48} />
          </div>
          <h1 className="text-3xl font-display font-bold tracking-tight mb-2">
            Mongo<span className="text-accent">Studio</span>
          </h1>
          <p className="text-text-secondary text-sm">
            Blazing-fast MongoDB interface
          </p>
        </div>

        {/* Connection Form */}
        <form onSubmit={handleSubmit} className="float-in float-in-delay-1">
          <div className="bg-surface-1 border border-border rounded-2xl p-6 glow-border">
            <label className="block text-xs font-medium text-text-secondary uppercase tracking-wider mb-3">
              Connection String
            </label>
            <div className="relative">
              <input
                ref={inputRef}
                type="text"
                value={uri}
                onChange={(e) => setUri(e.target.value)}
                placeholder="mongodb://localhost:27017"
                className="w-full bg-surface-2 border border-border rounded-lg pl-6 pr-14 py-3 h-12
                           text-text-primary placeholder-text-tertiary text-sm font-mono
                           focus:border-accent/40 focus:ring-1 focus:ring-accent/20
                           transition-all duration-150"
                spellCheck={false}
                autoComplete="off"
              />
              <button
                type="submit"
                disabled={!uri.trim() || connecting}
                className="absolute right-1.5 top-1/2 -translate-y-1/2 w-9 h-9 rounded-lg
                           bg-accent text-surface-0 flex items-center justify-center
                           disabled:opacity-30 disabled:cursor-not-allowed
                           hover:bg-accent-dim active:scale-95 transition-all duration-150"
              >
                {connecting ? <Loader className="text-surface-0" /> : <Zap className="w-4 h-4" />}
              </button>
            </div>

            {error && (
              <div className="mt-3 flex items-start gap-2 text-red-400 text-xs animate-fade-in">
                <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
                <span>{error}</span>
              </div>
            )}

            <div className="mt-4 flex items-center gap-3 text-2xs text-text-tertiary">
              <div className="flex items-center gap-1.5">
                <div className="w-1.5 h-1.5 rounded-full bg-accent/50" />
                MongoDB 3.6 → 8.x
              </div>
              <div className="w-px h-3 bg-border" />
              <div className="flex items-center gap-1.5">
                <div className="w-1.5 h-1.5 rounded-full bg-accent/50" />
                Atlas & Self-hosted
              </div>
              <div className="w-px h-3 bg-border" />
              <div className="flex items-center gap-1.5">
                <div className="w-1.5 h-1.5 rounded-full bg-accent/50" />
                Auto-detect version
              </div>
            </div>
          </div>
        </form>

        {/* Recent Connections */}
        {recent.length > 0 && (
          <div className="mt-6 float-in float-in-delay-2">
            <div className="text-xs font-medium text-text-tertiary uppercase tracking-wider mb-2 px-1">
              Recent
            </div>
            <div className="space-y-1">
              {recent.map((item, i) => (
                <button
                  key={i}
                  onClick={() => handleRecent(item)}
                  className="w-full text-left px-3 py-2.5 rounded-xl bg-surface-1/50 border border-border
                             hover:border-border-hover hover:bg-surface-2 transition-all duration-150
                             group flex items-center gap-3"
                >
                  <Server className="w-4 h-4 text-text-tertiary group-hover:text-accent transition-colors" />
                  <span className="text-sm font-mono text-text-secondary group-hover:text-text-primary truncate transition-colors">
                    {item.masked}
                  </span>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Footer */}
        <div className="mt-8 text-center float-in float-in-delay-3">
          <p className="text-2xs text-text-tertiary">
            Open Source — MIT License — v1.0.0
          </p>
        </div>
      </div>
    </div>
  );
}

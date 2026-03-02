import React from 'react';
import { Logo, Disconnect, Zap, Database, Collection as ColIcon, Document, Terminal, Key } from './Icons';
import { formatDuration } from '../utils/formatters';

export default function TopBar({ connectionInfo, onDisconnect, selectedDb, selectedCol, activeTab, onTabChange, queryMs }) {
  return (
    <header className="h-12 border-b border-border flex items-center px-3 gap-2 bg-surface-1/50 flex-shrink-0">
      {/* Logo */}
      <div className="flex items-center gap-2 mr-2">
        <Logo size={22} />
        <span className="text-sm font-display font-semibold tracking-tight hidden sm:block">
          Mongo<span className="text-accent">Studio</span>
        </span>
      </div>

      <div className="w-px h-5 bg-border mx-1" />

      {/* Breadcrumb */}
      <div className="flex items-center gap-1 text-xs min-w-0">
        <span className="text-text-tertiary flex items-center gap-1">
          <Zap className="w-3 h-3 text-accent" />
          <span className="hidden sm:inline text-text-secondary truncate max-w-[140px]">
            {connectionInfo?.host || 'connected'}
          </span>
        </span>
        {connectionInfo?.version && connectionInfo.version !== 'unknown' && (
          <span className="hidden sm:inline ml-1 px-1.5 py-0.5 rounded text-2xs font-medium bg-accent/10 text-accent border border-accent/20">
            v{connectionInfo.version}
          </span>
        )}
        {selectedDb && (
          <>
            <span className="text-text-tertiary">/</span>
            <span className="text-text-secondary font-medium truncate max-w-[120px]">{selectedDb}</span>
          </>
        )}
        {selectedCol && (
          <>
            <span className="text-text-tertiary">/</span>
            <span className="text-text-primary font-medium truncate max-w-[140px]">{selectedCol}</span>
          </>
        )}
      </div>

      {/* Tabs */}
      {selectedCol && (
        <div className="flex items-center gap-0.5 ml-4 bg-surface-2 rounded-lg p-0.5">
          {[
            { id: 'documents', label: 'Documents', icon: Document },
            { id: 'query', label: 'Query', icon: Terminal },
            { id: 'indexes', label: 'Indexes', icon: Key },
          ].map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => onTabChange(id)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-all duration-150 ${
                activeTab === id
                  ? 'bg-surface-4 text-text-primary shadow-sm'
                  : 'text-text-tertiary hover:text-text-secondary'
              }`}
            >
              <Icon className="w-3.5 h-3.5" />
              <span className="hidden md:inline">{label}</span>
            </button>
          ))}
        </div>
      )}

      <div className="flex-1" />

      {/* Query time badge */}
      {queryMs !== null && (
        <div className="hidden sm:flex items-center gap-1.5 text-2xs text-text-tertiary mr-2">
          <Zap className="w-3 h-3 text-accent/70" />
          {formatDuration(queryMs)}
        </div>
      )}

      {/* Disconnect */}
      <button
        onClick={onDisconnect}
        className="btn-ghost flex items-center gap-1.5 text-xs text-text-tertiary hover:text-red-400"
        title="Disconnect"
      >
        <Disconnect className="w-3.5 h-3.5" />
        <span className="hidden md:inline">Disconnect</span>
      </button>
    </header>
  );
}

import React from 'react';
import { Logo, Disconnect, Zap, Document, Terminal, Key, Moon, Sun, Shield, ShieldOff, Settings, Layers, Refresh } from './Icons';
import { formatDuration } from '../utils/formatters';

function formatHostDisplay(host) {
  if (!host) return 'connected';
  const parts = host.split(',');
  if (parts.length <= 1) return host;
  return `${parts[0].trim()} +${parts.length - 1}`;
}

const READ_PREF_SHORT = {
  secondary: '→2ry',
  secondaryPreferred: '→2ry?',
  primaryPreferred: '→1ry?',
  nearest: '→near',
};

export default function TopBar({
  connectionInfo,
  onDisconnect,
  selectedDb,
  selectedCol,
  activeTab,
  onTabChange,
  queryMs,
  theme,
  onToggleTheme,
  execMode,
  onExecModeToggle,
  onShowSettings,
  onGoHome,
  onRefresh,
  refreshing = false,
  deletedContext = null,
}) {
  const tabs = [
    { id:'documents', label:'Documents', icon:Document },
    { id:'query', label:'Query', icon:Terminal },
    { id:'indexes', label:'Indexes', icon:Key },
    { id:'schema', label:'Schema', icon:Layers },
  ];
  const topology = connectionInfo?.topology;
  const readPref = connectionInfo?.readPreference;
  const showReadPref = readPref && readPref !== 'primary';
  const stalePath = !selectedDb && deletedContext ? deletedContext : null;
  const breadcrumbDb = selectedDb || stalePath?.db;
  const breadcrumbCol = selectedCol || stalePath?.col;
  const showDeleted = Boolean(stalePath);

  return (
    <header className="h-12 flex items-center px-3 gap-2 flex-shrink-0" style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)',opacity:0.95}}>
      {/* Logo */}
      <button onClick={onGoHome} className="flex items-center gap-2 mr-2 rounded-lg px-1.5 py-1 transition-colors hover:bg-[var(--surface-2)]" title="Go to Welcome">
        <Logo size={22} />
        <span className="text-sm font-display font-semibold tracking-tight hidden sm:block" style={{color:'var(--text-primary)'}}>
          Mongo<span style={{color:'var(--accent)'}}>Studio</span>
        </span>
      </button>

      <div className="w-px h-5" style={{background:'var(--border)'}} />

      {/* Connection info + Breadcrumb */}
      <div className="flex items-center gap-1 text-xs min-w-0">
        <span className="flex items-center gap-1">
          <div className="w-2 h-2 rounded-full animate-pulse-dot" style={{background:'var(--accent)'}} />
          <span
            className="hidden sm:inline truncate max-w-[200px] font-mono text-2xs"
            style={{color:'var(--text-secondary)'}}
            title={connectionInfo?.host}
          >
            {formatHostDisplay(connectionInfo?.host || 'connected')}
          </span>
        </span>
        {connectionInfo?.version && connectionInfo.version !== 'unknown' && (
          <span className="hidden sm:inline ml-1 badge-accent">v{connectionInfo.version}</span>
        )}
        {connectionInfo?.username && (
          <span className="hidden md:inline ml-1 badge-blue">@{connectionInfo.username}</span>
        )}
        <span className={`hidden sm:inline ml-1 ${connectionInfo?.isProduction ? 'badge-yellow' : 'badge-green'}`}>
          Guard {connectionInfo?.isProduction ? 'ON' : 'OFF'}
        </span>
        {topology?.kind === 'replicaSet' && (
          <>
            <span className={`hidden sm:inline ml-1 ${topology.role === 'primary' ? 'badge-green' : topology.role === 'secondary' ? 'badge-blue' : 'badge-yellow'}`}>
              {(topology.role || 'member').toUpperCase()}
            </span>
            <span className="hidden md:inline ml-1 badge-purple">{topology.setName || 'rs'}</span>
          </>
        )}
        {topology?.kind === 'standalone' && (
          <span className="hidden sm:inline ml-1 badge-accent">STANDALONE</span>
        )}
        {topology?.kind === 'sharded' && (
          <span className="hidden sm:inline ml-1 badge-blue">MONGOS</span>
        )}
        {showReadPref && (
          <span
            className="hidden lg:inline ml-1 badge-blue"
            title={`Read preference: ${readPref} — reads are directed by the driver`}
          >
            {READ_PREF_SHORT[readPref] || readPref}
          </span>
        )}
        {breadcrumbDb && (
          <><span style={{color:'var(--text-tertiary)'}}>/</span><span className="font-medium truncate max-w-[220px]" style={{color:'var(--text-secondary)',textDecoration:showDeleted?'line-through':'none',opacity:showDeleted?0.7:1}}>{breadcrumbDb}</span></>
        )}
        {breadcrumbCol && (
          <><span style={{color:'var(--text-tertiary)'}}>/</span><span className="font-medium truncate max-w-[240px]" style={{color:'var(--text-primary)',textDecoration:showDeleted?'line-through':'none',opacity:showDeleted?0.7:1}}>{breadcrumbCol}</span></>
        )}
        {showDeleted && <span className="hidden sm:inline ml-1 badge-red">DELETED</span>}
      </div>

      {/* Tabs */}
      {selectedCol && (
        <div className="flex items-center gap-0.5 ml-4 rounded-lg p-0.5" style={{background:'var(--surface-2)'}}>
          {tabs.map(({ id, label, icon: Icon }) => (
            <button key={id} onClick={()=>onTabChange(id)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-all duration-150`}
              style={{
                background: activeTab===id ? 'var(--surface-4)' : 'transparent',
                color: activeTab===id ? 'var(--text-primary)' : 'var(--text-tertiary)',
                boxShadow: activeTab===id ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
              }}>
              <Icon className="w-3.5 h-3.5" />
              <span className="hidden md:inline">{label}</span>
            </button>
          ))}
        </div>
      )}

      <div className="flex-1" />

      {/* Execution mode badge */}
      <button onClick={onExecModeToggle} className="hidden sm:flex items-center gap-1.5 text-2xs cursor-pointer" title="Toggle Safe/Power mode">
        {execMode === 'safe' ? (
          <span className="mode-safe flex items-center gap-1"><Shield className="w-3 h-3" />Safe</span>
        ) : (
          <span className="mode-power flex items-center gap-1"><ShieldOff className="w-3 h-3" />Power</span>
        )}
      </button>

      {/* Query time */}
      {queryMs !== null && (
        <div className="hidden sm:flex items-center gap-1.5 text-2xs" style={{color:'var(--text-tertiary)'}}>
          <Zap className="w-3 h-3" style={{color:'var(--accent)',opacity:0.7}} />
          {formatDuration(queryMs)}
        </div>
      )}

      {selectedDb && (
        <span
          className="hidden lg:inline-flex items-center px-2 py-1 rounded-md text-2xs font-mono"
          style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-secondary)' }}
          title={selectedCol ? `${selectedDb}.${selectedCol}` : selectedDb}
        >
          {selectedCol ? `${selectedDb}.${selectedCol}` : selectedDb}
        </span>
      )}

      {/* Settings */}
      <button onClick={onRefresh} className="btn-ghost p-1.5" title="Refresh" disabled={refreshing}>
        <Refresh className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
      </button>
      <button onClick={onShowSettings} className="btn-ghost p-1.5" title="Settings">
        <Settings className="w-3.5 h-3.5" />
      </button>

      {/* Theme toggle */}
      <button onClick={onToggleTheme} className="btn-ghost p-1.5" title={`${theme==='dark'?'Light':'Dark'} theme`}>
        {theme === 'dark' ? <Sun className="w-3.5 h-3.5" /> : <Moon className="w-3.5 h-3.5" />}
      </button>

      {/* Disconnect */}
      <button onClick={onDisconnect} className="btn-ghost flex items-center gap-1.5 text-xs hover:text-red-400" title="Disconnect">
        <Disconnect className="w-3.5 h-3.5" />
        <span className="hidden md:inline">Disconnect</span>
      </button>
    </header>
  );
}

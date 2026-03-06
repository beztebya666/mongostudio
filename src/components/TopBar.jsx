import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Logo, Disconnect, Zap, Document, Terminal, Key, Moon, Sun, Shield, ShieldOff, Settings, Layers, Refresh, History } from './Icons';
import { formatDuration } from '../utils/formatters';

function formatHostDisplay(host) {
  if (!host) return 'connected';
  const parts = host.split(',');
  if (parts.length <= 1) return host;
  return `${parts[0].trim()} +${parts.length - 1}`;
}

function listHosts(host) {
  if (!host) return [];
  return host.split(',').map((entry) => entry.trim()).filter(Boolean);
}

const READ_PREF_SHORT = {
  secondary: '->2ry',
  secondaryPreferred: '->2ry?',
  primaryPreferred: '->1ry?',
  nearest: '->near',
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
  showTopTags = false,
}) {
  const [showHostsMenu, setShowHostsMenu] = useState(false);
  const [barWidth, setBarWidth] = useState(1400);
  const headerRef = useRef(null);
  const hostMenuRef = useRef(null);
  const hostMenuCloseTimerRef = useRef(null);

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
  const hosts = listHosts(connectionInfo?.host);
  const hostDisplay = formatHostDisplay(connectionInfo?.host || 'connected');
  const isMultiHost = hosts.length > 1;
  const guardLabel = connectionInfo?.isProduction ? 'Guard ON' : 'Guard';

  // One width budget controls what stays visible in the navbar.
  const widthBudget = barWidth - (selectedCol ? 270 : 120);
  const showHostText = widthBudget >= 700;
  const showVersionBadge = widthBudget >= 1220;
  const showUserBadge = widthBudget >= 1320;
  const showRoleBadge = widthBudget >= 980;
  const showSetNameBadge = widthBudget >= 1090;
  const showReadPrefBadge = widthBudget >= 1140;
  const showGuardBadge = widthBudget >= 960;
  const showExecModeBadge = widthBudget >= 900;
  const showQueryMsBadge = true;
  const showTabLabels = widthBudget >= 1120;
  const showAuditLabel = widthBudget >= 1240;
  const showDisconnectLabel = widthBudget >= 1380;
  const dbBreadcrumbMaxWidth = widthBudget >= 1300 ? 280 : widthBudget >= 1000 ? 240 : widthBudget >= 800 ? 190 : 150;
  const colBreadcrumbMaxWidth = widthBudget >= 1300 ? 360 : widthBudget >= 1000 ? 320 : widthBudget >= 800 ? 260 : 190;

  const { sessionBadges, stateBadges } = useMemo(() => {
    const session = [];
    const state = [];

    if (connectionInfo?.version && connectionInfo.version !== 'unknown') {
      session.push({ label: `v${connectionInfo.version}`, className: 'badge-accent' });
    }
    if (connectionInfo?.username) {
      session.push({ label: `@${connectionInfo.username}`, className: 'badge-blue' });
    }
    if (topology?.kind === 'replicaSet') {
      session.push({
        label: (topology.role || 'member').toUpperCase(),
        className: topology.role === 'primary' ? 'badge-green' : topology.role === 'secondary' ? 'badge-blue' : 'badge-yellow',
      });
      if (topology.setName) {
        session.push({ label: topology.setName, className: 'badge-purple' });
      }
    } else if (topology?.kind === 'standalone') {
      session.push({ label: 'STANDALONE', className: 'badge-accent' });
    } else if (topology?.kind === 'sharded') {
      session.push({ label: 'MONGOS', className: 'badge-blue' });
    }
    if (showReadPref) {
      session.push({ label: READ_PREF_SHORT[readPref] || readPref, className: 'badge-blue' });
    }
    state.push({ label: guardLabel, className: connectionInfo?.isProduction ? 'badge-yellow' : 'badge-green' });
    state.push({ label: execMode === 'safe' ? 'Safe' : 'Power', className: execMode === 'safe' ? 'mode-safe' : 'mode-power', isExecMode: true });
    return { sessionBadges: session, stateBadges: state };
  }, [
    showReadPref,
    connectionInfo?.version,
    connectionInfo?.username,
    connectionInfo?.isProduction,
    topology?.kind,
    topology?.role,
    topology?.setName,
    readPref,
    guardLabel,
    execMode,
  ]);

  const hasHostsMenuContent = isMultiHost || sessionBadges.length > 0 || stateBadges.length > 0;

  const openHostsMenu = () => {
    if (!hasHostsMenuContent) return;
    if (hostMenuCloseTimerRef.current) {
      clearTimeout(hostMenuCloseTimerRef.current);
      hostMenuCloseTimerRef.current = null;
    }
    setShowHostsMenu(true);
  };

  const closeHostsMenu = () => {
    if (hostMenuCloseTimerRef.current) {
      clearTimeout(hostMenuCloseTimerRef.current);
    }
    hostMenuCloseTimerRef.current = setTimeout(() => {
      setShowHostsMenu(false);
      hostMenuCloseTimerRef.current = null;
    }, 220);
  };

  useEffect(() => {
    const onMouseDown = (event) => {
      if (!showHostsMenu) return;
      if (hostMenuRef.current && !hostMenuRef.current.contains(event.target)) {
        setShowHostsMenu(false);
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    return () => document.removeEventListener('mousedown', onMouseDown);
  }, [showHostsMenu]);

  useEffect(() => {
    const element = headerRef.current;
    if (!element) return undefined;
    const updateWidth = () => setBarWidth(element.clientWidth || window.innerWidth || 1400);
    updateWidth();

    if (typeof ResizeObserver === 'undefined') {
      window.addEventListener('resize', updateWidth, { passive: true });
      return () => window.removeEventListener('resize', updateWidth);
    }

    const observer = new ResizeObserver((entries) => {
      if (!entries.length) return;
      setBarWidth(Math.round(entries[0].contentRect.width));
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    return () => {
      if (hostMenuCloseTimerRef.current) {
        clearTimeout(hostMenuCloseTimerRef.current);
      }
    };
  }, []);

  return (
    <header ref={headerRef} className="relative z-[120] h-12 flex items-center px-3 gap-2 flex-shrink-0 overflow-visible" style={{borderBottom:'1px solid var(--border)',background:'var(--surface-1)',opacity:0.95}}>
      {/* Logo */}
      <button onClick={onGoHome} className="flex items-center gap-2 mr-2 rounded-lg px-1.5 py-1 transition-colors hover:bg-[var(--surface-2)]" title="Go to Welcome">
        <Logo size={22} />
        <span className="text-sm font-display font-semibold tracking-tight hidden sm:block" style={{color:'var(--text-primary)'}}>
          Mongo<span style={{color:'var(--accent)'}}>Studio</span>
        </span>
      </button>

      <div className="w-px h-5" style={{background:'var(--border)'}} />

      {/* Connection info + Breadcrumb */}
      <div className="flex items-center gap-1 text-xs min-w-0 whitespace-nowrap overflow-visible" style={{ maxWidth: selectedCol ? 'min(56vw, 940px)' : 'min(68vw, 1120px)' }}>
        <div
          ref={hostMenuRef}
          className="relative flex items-center gap-1.5 min-w-0 pr-1"
          onMouseEnter={openHostsMenu}
          onMouseLeave={closeHostsMenu}
        >
          <div className="w-2 h-2 rounded-full animate-pulse-dot shrink-0" style={{background:'var(--accent)'}} />
          {showHostText && (
            <span
              className="truncate max-w-[240px] font-mono text-2xs inline-block align-middle leading-none cursor-default"
              style={{ color:'var(--text-secondary)' }}
              title={connectionInfo?.host}
            >
              {hostDisplay}
            </span>
          )}
          {showHostsMenu && hasHostsMenuContent && (
            <div className="absolute left-0 top-full z-[240]" onMouseEnter={openHostsMenu} onMouseLeave={closeHostsMenu}>
              <div
                className="mt-1 min-w-[260px] max-w-[420px] max-h-64 overflow-auto rounded-lg py-1 shadow-xl select-text"
                style={{ background:'var(--surface-3)', border:'1px solid var(--border)' }}
              >
                {hosts.length > 0 && (
                  <div className="py-0.5">
                    {hosts.map((host) => {
                      const isPrimary = connectionInfo?.topology?.primary && host === connectionInfo.topology.primary;
                      return (
                        <div
                          key={host}
                          className="px-2.5 py-1.5 text-2xs font-mono flex items-center gap-2"
                          style={{ color:'var(--text-secondary)' }}
                          title={host}
                        >
                          <span className={`inline-flex w-1.5 h-1.5 rounded-full ${isPrimary ? 'bg-emerald-400' : 'bg-sky-400/70'}`} />
                          <span>{host}</span>
                          {isPrimary && <span className="badge-green">1ry</span>}
                        </div>
                      );
                    })}
                  </div>
                )}
                {sessionBadges.length > 0 && (
                  <div className="px-2.5 py-1.5" style={hosts.length > 0 ? { borderTop:'1px solid var(--border)' } : undefined}>
                    <div className="text-2xs uppercase tracking-wider mb-1" style={{ color:'var(--text-tertiary)' }}>
                      Session
                    </div>
                    <div className="flex flex-wrap gap-1">
                      {sessionBadges.map((badge) => (
                        <span key={`session:${badge.className}:${badge.label}`} className={badge.className}>{badge.label}</span>
                      ))}
                    </div>
                  </div>
                )}
                {stateBadges.length > 0 && (
                  <div
                    className="px-2.5 py-1.5"
                    style={(hosts.length > 0 || sessionBadges.length > 0) ? { borderTop:'1px solid var(--border)' } : undefined}
                  >
                    <div className="flex flex-wrap gap-1">
                      {stateBadges.map((badge) => (
                        badge.isExecMode ? (
                          <button
                            key={`state:${badge.className}:${badge.label}`}
                            type="button"
                            className={`${badge.className} cursor-pointer`}
                            title="Toggle Safe/Power mode"
                            onClick={() => {
                              onExecModeToggle?.();
                              setShowHostsMenu(false);
                            }}
                          >
                            {badge.label}
                          </button>
                        ) : (
                          <span key={`state:${badge.className}:${badge.label}`} className={badge.className}>{badge.label}</span>
                        )
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {showTopTags && showVersionBadge && connectionInfo?.version && connectionInfo.version !== 'unknown' && (
          <span className="ml-1 badge-accent">v{connectionInfo.version}</span>
        )}
        {showTopTags && showUserBadge && connectionInfo?.username && (
          <span className="ml-1 badge-blue">@{connectionInfo.username}</span>
        )}
        {showTopTags && topology?.kind === 'replicaSet' && (
          <>
            {showRoleBadge && (
              <span className={`ml-1 ${topology.role === 'primary' ? 'badge-green' : topology.role === 'secondary' ? 'badge-blue' : 'badge-yellow'}`}>
                {(topology.role || 'member').toUpperCase()}
              </span>
            )}
            {showSetNameBadge && <span className="ml-1 badge-purple">{topology.setName || 'rs'}</span>}
          </>
        )}
        {showTopTags && topology?.kind === 'standalone' && showRoleBadge && (
          <span className="ml-1 badge-accent">STANDALONE</span>
        )}
        {showTopTags && topology?.kind === 'sharded' && showRoleBadge && (
          <span className="ml-1 badge-blue">MONGOS</span>
        )}
        {showTopTags && showReadPref && showReadPrefBadge && (
          <span className="ml-1 badge-blue" title={`Read preference: ${readPref} - reads are directed by the driver`}>
            {READ_PREF_SHORT[readPref] || readPref}
          </span>
        )}
        {breadcrumbDb && (
          <>
            <span style={{color:'var(--text-tertiary)'}}>/</span>
            <span
              className="font-medium truncate"
              style={{
                maxWidth: `${dbBreadcrumbMaxWidth}px`,
                color:'var(--text-secondary)',
                textDecoration:showDeleted?'line-through':'none',
                opacity:showDeleted?0.7:1,
              }}
            >
              {breadcrumbDb}
            </span>
          </>
        )}
        {breadcrumbCol && (
          <>
            <span style={{color:'var(--text-tertiary)'}}>/</span>
            <span
              className="font-medium truncate"
              style={{
                maxWidth: `${colBreadcrumbMaxWidth}px`,
                color:'var(--text-primary)',
                textDecoration:showDeleted?'line-through':'none',
                opacity:showDeleted?0.7:1,
              }}
            >
              {breadcrumbCol}
            </span>
          </>
        )}
        {showDeleted && widthBudget >= 900 && <span className="ml-1 badge-red">DELETED</span>}
      </div>

      {/* Tabs */}
      {selectedCol && (
        <div className="flex items-center gap-0.5 ml-3 rounded-lg p-0.5 shrink-0" style={{background:'var(--surface-2)'}}>
          {tabs.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => onTabChange(id)}
              className={`flex items-center ${showTabLabels ? 'gap-1.5 px-3' : 'justify-center px-2.5'} py-1.5 rounded-md text-xs font-medium transition-all duration-150 ${
                activeTab === id
                  ? 'text-[var(--text-primary)]'
                  : 'text-[var(--text-tertiary)] hover:text-[var(--text-primary)] hover:bg-[var(--surface-3)]'
              }`}
              style={activeTab===id ? { background:'var(--surface-4)', boxShadow:'0 1px 3px rgba(0,0,0,0.1)' } : undefined}
              title={showTabLabels ? undefined : label}
            >
              <Icon className="w-3.5 h-3.5" />
              {showTabLabels && <span>{label}</span>}
            </button>
          ))}
        </div>
      )}

      <div className="flex-1 min-w-0" />

      <div className="flex items-center gap-1.5 shrink-0">
        {showTopTags && showGuardBadge && (
          <span className={`${connectionInfo?.isProduction ? 'badge-yellow' : 'badge-green'}`}>
            {guardLabel}
          </span>
        )}

        {showTopTags && showExecModeBadge && (
          <button onClick={onExecModeToggle} className="flex items-center gap-1.5 text-2xs cursor-pointer" title="Toggle Safe/Power mode">
            {execMode === 'safe' ? (
              <span className="mode-safe flex items-center gap-1"><Shield className="w-3 h-3" />Safe</span>
            ) : (
              <span className="mode-power flex items-center gap-1"><ShieldOff className="w-3 h-3" />Power</span>
            )}
          </button>
        )}

        {showQueryMsBadge && queryMs !== null && (
          <div className="flex items-center gap-1.5 text-2xs" style={{color:'var(--text-tertiary)'}}>
            <Zap className="w-3 h-3" style={{color:'var(--accent)',opacity:0.7}} />
            {formatDuration(queryMs)}
          </div>
        )}

        <button
          onClick={() => onTabChange('audit')}
          className="btn-ghost flex items-center gap-1 px-2 py-1.5"
          style={activeTab === 'audit' ? { color:'var(--accent)' } : undefined}
          title="Audit log"
        >
          <History className="w-3.5 h-3.5" />
          {showAuditLabel && <span className="text-xs">Audit</span>}
        </button>
        <button onClick={onRefresh} className="btn-ghost p-1.5" title="Refresh" disabled={refreshing}>
          <Refresh className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
        </button>
        <button onClick={onShowSettings} className="btn-ghost p-1.5" title="Settings">
          <Settings className="w-3.5 h-3.5" />
        </button>

        <button onClick={onToggleTheme} className="btn-ghost p-1.5" title={`${theme==='dark'?'Light':'Dark'} theme`}>
          {theme === 'dark' ? <Sun className="w-3.5 h-3.5" /> : <Moon className="w-3.5 h-3.5" />}
        </button>

        <button onClick={onDisconnect} className="btn-ghost flex items-center gap-1.5 text-xs hover:text-red-400" title="Disconnect">
          <Disconnect className="w-3.5 h-3.5" />
          {showDisconnectLabel && <span>Disconnect</span>}
        </button>
      </div>
    </header>
  );
}

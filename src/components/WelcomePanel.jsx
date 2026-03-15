import React, { useEffect, useMemo, useRef, useState } from 'react';
import api from '../utils/api';
import { Database, Zap, Server, Activity, Shield, Info, ChevronDown, ArrowUp, X, Layers, Document, Loader } from './Icons';
import { formatBytes, formatNumber, formatDuration } from '../utils/formatters';

function formatHostDisplay(host) {
  if (!host) return '';
  const parts = host.split(',').map((h) => h.trim());
  if (parts.length === 1) return parts[0];
  if (parts.length === 2) return `${parts[0]}, ${parts[1]}`;
  return `${parts[0]}, ${parts[1]} +${parts.length - 2} more`;
}

const DB_STATS_CONCURRENCY = 3;
const DEFAULT_WELCOME_HINTS = { readPrefPrimary: false };

function loadWelcomeHintsState(key) {
  if (!key) return DEFAULT_WELCOME_HINTS;
  try {
    const raw = sessionStorage.getItem(key);
    if (!raw) return DEFAULT_WELCOME_HINTS;
    const parsed = JSON.parse(raw);
    return { readPrefPrimary: Boolean(parsed?.readPrefPrimary) };
  } catch {
    return DEFAULT_WELCOME_HINTS;
  }
}

export default function WelcomePanel({ databases, connectionInfo, refreshToken = 0, metadata = null }) {
  const connectionId = connectionInfo?.connectionId || 'session';
  const sectionStateKey = `mongostudio_welcome_sections:${connectionId}`;
  const hintStateKey = `mongostudio_welcome_hints:${connectionId}`;
  const [serverStatus, setServerStatus] = useState(null);
  const [dbStats, setDbStats] = useState({});
  const [dbStatsLoading, setDbStatsLoading] = useState({});
  const [dbStatsProgress, setDbStatsProgress] = useState({ loaded: 0, total: 0 });
  const [collapsedSections, setCollapsedSections] = useState({
    serverDetails: false,
    databases: false,
    capabilities: false,
  });
  const [dismissedHints, setDismissedHints] = useState(() => loadWelcomeHintsState(hintStateKey));
  const [showScrollTop, setShowScrollTop] = useState(false);
  const dbStatsSeqRef = useRef(0);
  const scrollRef = useRef(null);
  const hasSharedMetadata = Boolean(metadata && metadata.loaded);
  const sharedStats = hasSharedMetadata && metadata?.stats && typeof metadata.stats === 'object' ? metadata.stats : {};
  const sharedFreshness = hasSharedMetadata && metadata?.freshness && typeof metadata.freshness === 'object' ? metadata.freshness : {};
  const effectiveStats = hasSharedMetadata ? sharedStats : dbStats;
  const effectiveProgress = hasSharedMetadata
    ? {
        loaded: databases.reduce((sum, db) => sum + (Object.prototype.hasOwnProperty.call(sharedFreshness, db.name) ? 1 : 0), 0),
        total: databases.length,
      }
    : dbStatsProgress;

  useEffect(() => {
    api.getServerStatus().then(setServerStatus).catch(() => {});
  }, [refreshToken]);

  useEffect(() => {
    if (hasSharedMetadata) return undefined;
    dbStatsSeqRef.current += 1;
    const seq = dbStatsSeqRef.current;

    if (databases.length === 0) {
      setDbStats({});
      setDbStatsLoading({});
      setDbStatsProgress({ loaded: 0, total: 0 });
      return undefined;
    }

    const targets = databases.map((db) => db.name);
    const targetSet = new Set(targets);

    setDbStats((prev) => {
      const next = {};
      for (const [name, stats] of Object.entries(prev)) {
        if (targetSet.has(name)) next[name] = stats;
      }
      return next;
    });
    setDbStatsLoading((prev) => {
      const next = {};
      for (const [name, state] of Object.entries(prev)) {
        if (targetSet.has(name)) next[name] = state;
      }
      return next;
    });
    setDbStatsProgress({ loaded: 0, total: targets.length });

    const queue = targets.map((name) => ({ dbName: name, attempt: 0 }));
    const workerCount = Math.min(DB_STATS_CONCURRENCY, queue.length);
    let completed = 0;

    const runWorker = async () => {
      while (queue.length > 0 && seq === dbStatsSeqRef.current) {
        const nextItem = queue.shift();
        const dbName = nextItem?.dbName;
        const attempt = Number(nextItem?.attempt || 0);
        if (!dbName) break;
        let finalized = true;
        setDbStatsLoading((prev) => ({ ...prev, [dbName]: true }));
        try {
          const stats = await api.getDatabaseStats(dbName);
          if (seq !== dbStatsSeqRef.current) return;
          setDbStats((prev) => ({ ...prev, [dbName]: stats || { _error: true } }));
        } catch (err) {
          if (seq !== dbStatsSeqRef.current) return;
          const retryable = err?.status === 429 || err?.status >= 500 || err?.errorType === 'network';
          if (retryable && attempt < 3) {
            finalized = false;
            queue.push({ dbName, attempt: attempt + 1 });
          } else {
            setDbStats((prev) => ({ ...prev, [dbName]: { _error: true } }));
          }
        } finally {
          if (seq !== dbStatsSeqRef.current) return;
          setDbStatsLoading((prev) => {
            const next = { ...prev };
            delete next[dbName];
            return next;
          });
          if (finalized) {
            completed += 1;
            setDbStatsProgress({ loaded: completed, total: targets.length });
          }
        }
      }
    };

    Promise.all(Array.from({ length: workerCount }, () => runWorker())).catch(() => {});
    return () => {
      dbStatsSeqRef.current += 1;
    };
  }, [databases.map((db) => db.name).join('|'), refreshToken, hasSharedMetadata]);

  const totalSize = databases.reduce((s, d) => s + (d.sizeOnDisk || 0), 0);
  const totalCollections = useMemo(
    () => Object.values(effectiveStats).reduce((sum, item) => sum + (item?.collections || 0), 0),
    [effectiveStats],
  );
  const totalDocuments = useMemo(
    () => Object.values(effectiveStats).reduce((sum, item) => sum + (item?.objects || 0), 0),
    [effectiveStats],
  );
  const dbStatsReady = effectiveProgress.total > 0 && effectiveProgress.loaded >= effectiveProgress.total;
  const topology = connectionInfo?.topology;
  const readPref = connectionInfo?.readPreference;
  const showReadPref = readPref && readPref !== 'primary';
  const isMultiHost = connectionInfo?.host && connectionInfo.host.includes(',');

  useEffect(() => {
    try {
      const raw = sessionStorage.getItem(sectionStateKey);
      if (raw) {
        const parsed = JSON.parse(raw);
        setCollapsedSections({
          serverDetails: Boolean(parsed.serverDetails),
          databases: Boolean(parsed.databases),
          capabilities: Boolean(parsed.capabilities),
        });
      } else {
        setCollapsedSections({ serverDetails: false, databases: false, capabilities: false });
      }
    } catch {
      setCollapsedSections({ serverDetails: false, databases: false, capabilities: false });
    }
  }, [sectionStateKey]);

  useEffect(() => {
    try {
      sessionStorage.setItem(sectionStateKey, JSON.stringify(collapsedSections));
    } catch {}
  }, [sectionStateKey, collapsedSections]);

  useEffect(() => {
    setDismissedHints(loadWelcomeHintsState(hintStateKey));
  }, [hintStateKey]);

  useEffect(() => {
    try {
      sessionStorage.setItem(hintStateKey, JSON.stringify(dismissedHints));
    } catch {}
  }, [hintStateKey, dismissedHints]);

  useEffect(() => {
    const node = scrollRef.current;
    if (!node) return undefined;
    const onScroll = () => setShowScrollTop(node.scrollTop > 420);
    onScroll();
    node.addEventListener('scroll', onScroll, { passive: true });
    return () => node.removeEventListener('scroll', onScroll);
  }, [databases.length, refreshToken]);

  const rsMembers = topology?.hosts || (
    serverStatus?.serverStatus?.repl?.hosts
      ? [...(serverStatus.serverStatus.repl.hosts), ...(serverStatus.serverStatus.repl.passives || [])]
      : null
  );

  const toggleSection = (name) => {
    setCollapsedSections((prev) => ({ ...prev, [name]: !prev[name] }));
  };

  return (
    <div ref={scrollRef} className="h-full overflow-auto relative">
      <div className="max-w-3xl mx-auto px-6 py-8">
        <div className="mb-8 float-in">
          <h2 className="text-2xl font-display font-bold tracking-tight mb-1" style={{ color: 'var(--text-primary)' }}>
            Welcome to Mongo<span style={{ color: 'var(--accent)' }}>Studio</span>
          </h2>
          <p className="text-sm" style={{ color: 'var(--text-tertiary)' }}>
            Connected to{' '}
            <span
              className="font-mono text-xs font-medium"
              style={{ color: 'var(--text-secondary)' }}
              title={connectionInfo?.host}
            >
              {formatHostDisplay(connectionInfo?.host)}
            </span>
            {connectionInfo?.version && connectionInfo.version !== 'unknown' && (
              <> · MongoDB <span className="font-medium" style={{ color: 'var(--text-secondary)' }}>{connectionInfo.version}</span></>
            )}
          </p>

          <div className="mt-2 flex flex-wrap items-center gap-2">
            {topology?.kind === 'replicaSet' && (
              <>
                <span className={topology.role === 'primary' ? 'badge-green' : topology.role === 'secondary' ? 'badge-blue' : 'badge-yellow'}>
                  {(topology.role || 'member').toUpperCase()}
                </span>
                <span className="badge-purple">{topology.setName || 'replicaSet'}</span>
              </>
            )}
            {topology?.kind === 'standalone' && <span className="badge-accent">STANDALONE</span>}
            {topology?.kind === 'sharded' && <span className="badge-blue">MONGOS</span>}
            {showReadPref && (
              <span className="badge-blue" title={`Read preference: ${readPref}`}>
                reads -&gt; {readPref}
              </span>
            )}
          </div>

          {showReadPref && topology?.kind === 'replicaSet' && topology?.role === 'primary' && !dismissedHints.readPrefPrimary && (
            <div className="mt-3 text-xs px-3 py-2 pr-8 rounded-lg relative" style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-tertiary)' }}>
              <Info className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" style={{ color:'var(--accent)', opacity:0.7 }} />
              <span>
                Topology shows <strong style={{ color:'var(--text-secondary)' }}>PRIMARY</strong> - this is normal for replica set connections.
                The driver routes <strong style={{ color:'var(--text-secondary)' }}>read</strong> operations to <strong style={{ color:'var(--text-secondary)' }}>{readPref}</strong> members.
                To connect directly to a secondary node, use a single-host URI with <strong style={{ color:'var(--text-secondary)' }}>DirectConnect</strong> enabled.
              </span>
              <button
                className="absolute top-0.5 right-0.5 p-1 rounded-md transition-colors hover:bg-[var(--surface-3)]"
                onClick={() => setDismissedHints((prev) => ({ ...prev, readPrefPrimary: true }))}
                title="Hide hint for this session"
              >
                <X className="w-3.5 h-3.5" style={{ color:'var(--text-tertiary)' }} />
              </button>
            </div>
          )}

          {topology?.kind === 'replicaSet' && rsMembers && rsMembers.length > 0 && (
            <div className="mt-3">
              <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color:'var(--text-tertiary)' }}>Replica Set Members</div>
              <div className="flex flex-wrap gap-1.5">
                {rsMembers.map((member) => (
                  <span
                    key={member}
                    className={`text-2xs font-mono px-2 py-1 rounded-lg ${member === topology.primary ? 'badge-green' : 'badge-blue'}`}
                    title={member === topology.primary ? 'Primary' : 'Secondary / Passive'}
                  >
                    {member}
                    {member === topology.primary && <span className="ml-1 opacity-60">1ry</span>}
                  </span>
                ))}
              </div>
            </div>
          )}

          {isMultiHost && topology?.kind !== 'replicaSet' && (
            <div className="mt-2 text-2xs" style={{ color:'var(--text-tertiary)' }}>
              {connectionInfo.host.split(',').length} nodes · <span title={connectionInfo.host} className="font-mono">{connectionInfo.host.split(',')[0]}</span> and {connectionInfo.host.split(',').length - 1} more
            </div>
          )}
        </div>

        <div className="grid grid-cols-2 lg:grid-cols-3 gap-3 mb-6 float-in float-in-delay-1">
          {[
            { label: 'Databases', value: formatNumber(databases.length), icon: Database, color: 'var(--accent)' },
            {
              label: 'Collections',
              value: dbStatsReady ? formatNumber(totalCollections) : null,
              loading: !dbStatsReady,
              icon: Layers,
              color: '#34d399',
            },
            {
              label: 'Documents',
              value: dbStatsReady ? formatNumber(totalDocuments) : null,
              loading: !dbStatsReady,
              icon: Document,
              color: '#fbbf24',
            },
            { label: 'Total Size', value: formatBytes(totalSize), icon: Server, color: '#60a5fa' },
            { label: 'Version', value: connectionInfo?.version || '-', icon: Info, color: '#a78bfa' },
            { label: 'Production Guard', value: connectionInfo?.isProduction ? 'ON' : 'OFF', icon: Shield, color: connectionInfo?.isProduction ? '#fbbf24' : '#34d399' },
          ].map(({ label, value, loading, icon: Icon, color }) => (
            <div key={label} className="p-4 rounded-xl transition-all" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
              <div className="flex items-center gap-2 mb-2">
                <Icon className="w-4 h-4 flex-shrink-0" style={{ color, opacity: 0.9 }} />
                <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>{label}</span>
              </div>
              {loading ? (
                <span className="inline-flex items-center gap-1.5 text-xs" style={{ color: 'var(--text-tertiary)' }}>
                  <Loader className="w-3.5 h-3.5" style={{ color: 'var(--accent)' }} />
                  loading...
                </span>
              ) : (
                <span className="text-lg font-display font-bold" style={{ color: 'var(--text-primary)' }}>{value}</span>
              )}
            </div>
          ))}
        </div>
        {!dbStatsReady && effectiveProgress.total > 0 && (
          <div className="mb-5 text-2xs inline-flex items-center gap-1.5" style={{ color:'var(--text-tertiary)' }}>
            <Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />
            Loading database stats {effectiveProgress.loaded}/{effectiveProgress.total}
          </div>
        )}

        {serverStatus?.serverStatus && (
          <div className="mb-6 float-in float-in-delay-2">
            <button
              className="w-full text-left text-xs font-semibold uppercase tracking-wider mb-3 flex items-center justify-between gap-2 px-2 py-2 rounded-lg transition-colors hover:bg-[var(--surface-1)]"
              style={{ color: 'var(--text-tertiary)' }}
              onClick={() => toggleSection('serverDetails')}
              title={collapsedSections.serverDetails ? 'Expand' : 'Collapse'}
            >
              <span className="flex items-center gap-2"><Activity className="w-3.5 h-3.5" />Server Details</span>
              <ChevronDown className={`w-3.5 h-3.5 transition-transform ${collapsedSections.serverDetails ? '-rotate-90' : ''}`} />
            </button>
            {!collapsedSections.serverDetails && (
              <div className="grid grid-cols-3 gap-3">
                {[
                  { label: 'Uptime', value: serverStatus.serverStatus.uptime ? formatDuration(serverStatus.serverStatus.uptime * 1000) : '-' },
                  { label: 'Connections', value: serverStatus.serverStatus.connections ? `${serverStatus.serverStatus.connections.current} / ${serverStatus.serverStatus.connections.available}` : '-' },
                  { label: 'Storage Engine', value: serverStatus.serverStatus.storageEngine?.name || '-' },
                  ...(serverStatus.serverStatus.opcounters ? [
                    { label: 'Inserts (uptime)', value: formatNumber(serverStatus.serverStatus.opcounters.insert || 0) },
                    { label: 'Queries (uptime)', value: formatNumber(serverStatus.serverStatus.opcounters.query || 0) },
                    { label: 'Updates (uptime)', value: formatNumber(serverStatus.serverStatus.opcounters.update || 0) },
                  ] : []),
                  ...(serverStatus.serverStatus.mem ? [
                    { label: 'Resident Memory', value: `${serverStatus.serverStatus.mem.resident || 0} MB` },
                  ] : []),
                ].map(({ label, value }) => (
                  <div key={label} className="p-3 rounded-lg" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)' }}>
                    <div className="text-2xs mb-0.5" style={{ color: 'var(--text-tertiary)' }}>{label}</div>
                    <div className="text-sm font-mono" style={{ color: 'var(--text-primary)' }}>{value}</div>
                  </div>
                ))}
              </div>
            )}
            {!collapsedSections.serverDetails && serverStatus.serverStatus?.opcounters && (
              <div className="mt-2 text-2xs" style={{ color:'var(--text-tertiary)' }}>
                Operation counters are cumulative since MongoDB process start (uptime).
              </div>
            )}
          </div>
        )}

        <div className="float-in float-in-delay-3">
          <button
            className="w-full text-left text-xs font-semibold uppercase tracking-wider mb-3 flex items-center justify-between gap-2 px-2 py-2 rounded-lg transition-colors hover:bg-[var(--surface-1)]"
            style={{ color: 'var(--text-tertiary)' }}
            onClick={() => toggleSection('databases')}
            title={collapsedSections.databases ? 'Expand' : 'Collapse'}
          >
            <span className="flex items-center gap-2"><Database className="w-3.5 h-3.5" />Databases</span>
            <ChevronDown className={`w-3.5 h-3.5 transition-transform ${collapsedSections.databases ? '-rotate-90' : ''}`} />
          </button>
          {!collapsedSections.databases && (
              <div className="space-y-1">
                {databases.map((db) => {
                  const rawStats = Object.prototype.hasOwnProperty.call(effectiveStats, db.name) ? effectiveStats[db.name] : null;
                  const freshnessEntry = hasSharedMetadata ? (sharedFreshness[db.name] || null) : null;
                  const hasStats = Boolean(rawStats && !rawStats._error);
                  const hasStatsError = Boolean(rawStats && rawStats._error);
                  const stats = hasStats ? rawStats : null;
                  return (
                <div key={db.name} className="flex items-center justify-between px-3 py-2 rounded-lg transition-colors" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
                  <div className="flex items-center gap-2 min-w-0">
                    <Database className="w-3.5 h-3.5 flex-shrink-0" style={{ color: 'var(--accent)', opacity: 0.6 }} />
                    <div className="min-w-0">
                      <span className="text-sm font-medium block truncate" style={{ color: 'var(--text-primary)' }}>{db.name}</span>
                      {hasStats ? (
                        <span className="text-2xs" style={{ color:'var(--text-tertiary)' }}>
                          {typeof stats?.collections === 'number' ? formatNumber(stats.collections) : '-'} cols
                          {' | '}
                          {typeof stats?.objects === 'number' ? formatNumber(stats.objects) : '-'} docs
                          {freshnessEntry && freshnessEntry.fresh === false ? ' | stale' : ''}
                        </span>
                      ) : hasStatsError ? (
                        <span className="text-2xs" style={{ color:'var(--text-tertiary)' }}>
                          stats unavailable
                        </span>
                      ) : (
                        <span className="text-2xs inline-flex items-center gap-1" style={{ color:'var(--text-tertiary)' }}>
                          <Loader className="w-3 h-3" style={{ color:'var(--accent)' }} />
                          loading stats...
                        </span>
                      )}
                    </div>
                  </div>
                  <div className="text-right">
                    <span className="text-2xs font-mono block" style={{ color: 'var(--text-tertiary)' }}>{formatBytes(db.sizeOnDisk || 0)}</span>
                    {hasStats ? (
                      <span className="text-2xs font-mono block" style={{ color: 'var(--text-tertiary)' }}>
                        avg {typeof stats?.avgObjSize === 'number' ? formatBytes(Math.round(stats.avgObjSize)) : '-'}
                      </span>
                    ) : hasStatsError ? (
                      <span className="text-2xs font-mono block" style={{ color: 'var(--text-tertiary)' }}>
                        avg -
                      </span>
                    ) : (
                      <span className="text-2xs font-mono block" style={{ color: 'var(--text-tertiary)' }}>
                        avg ...
                      </span>
                    )}
                  </div>
                </div>
                  );
                })}
            </div>
          )}
        </div>

        {connectionInfo?.capabilities && (
          <div className="mt-6 float-in float-in-delay-4">
            <button
              className="w-full text-left text-xs font-semibold uppercase tracking-wider mb-3 flex items-center justify-between gap-2 px-2 py-2 rounded-lg transition-colors hover:bg-[var(--surface-1)]"
              style={{ color: 'var(--text-tertiary)' }}
              onClick={() => toggleSection('capabilities')}
              title={collapsedSections.capabilities ? 'Expand' : 'Collapse'}
            >
              <span className="flex items-center gap-2"><Zap className="w-3.5 h-3.5" />Capabilities</span>
              <ChevronDown className={`w-3.5 h-3.5 transition-transform ${collapsedSections.capabilities ? '-rotate-90' : ''}`} />
            </button>
            {!collapsedSections.capabilities && (
              <div className="flex flex-wrap gap-2">
                {Object.entries(connectionInfo.capabilities).map(([key, supported]) => (
                  <span key={key} className={supported ? 'badge-green' : 'badge-red'}>
                    {key.replace(/([A-Z])/g, ' $1').trim()}
                  </span>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
      {showScrollTop && (
        <button
          type="button"
          className="fixed bottom-5 z-[110] btn-ghost px-3 py-2 text-xs flex items-center gap-1.5"
          style={{
            right: 'calc(var(--workspace-right-sidebar-width, 0px) + 16px)',
            background:'var(--surface-2)',
            border:'1px solid var(--border)',
          }}
          onClick={() => scrollRef.current?.scrollTo({ top: 0, behavior: 'smooth' })}
          title="Back to top"
        >
          <ArrowUp className="w-3.5 h-3.5" />
          Top
        </button>
      )}
    </div>
  );
}

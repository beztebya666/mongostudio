import React, { useState, useEffect } from 'react';
import api from '../utils/api';
import { Database, Zap, Server, Activity, Shield, ShieldOff, Info } from './Icons';
import { formatBytes, formatNumber, formatDuration } from '../utils/formatters';

function formatHostDisplay(host) {
  if (!host) return '';
  const parts = host.split(',').map(h => h.trim());
  if (parts.length === 1) return parts[0];
  if (parts.length === 2) return `${parts[0]}, ${parts[1]}`;
  return `${parts[0]}, ${parts[1]} +${parts.length - 2} more`;
}

export default function WelcomePanel({ databases, connectionInfo, refreshToken = 0 }) {
  const [serverStatus, setServerStatus] = useState(null);
  const [metrics, setMetrics] = useState(null);

  useEffect(() => {
    api.getServerStatus().then(setServerStatus).catch(() => {});
    api.getMetrics().then(setMetrics).catch(() => {});
  }, [refreshToken]);

  const totalSize = databases.reduce((s, d) => s + (d.sizeOnDisk || 0), 0);
  const topology = connectionInfo?.topology;
  const readPref = connectionInfo?.readPreference;
  const showReadPref = readPref && readPref !== 'primary';
  const isMultiHost = connectionInfo?.host && connectionInfo.host.includes(',');

  // Collect all RS members: prefer topology.hosts (from hello cmd), fallback to serverStatus repl.hosts
  const rsMembers = topology?.hosts ||
    (serverStatus?.serverStatus?.repl?.hosts
      ? [...(serverStatus.serverStatus.repl.hosts), ...(serverStatus.serverStatus.repl.passives || [])]
      : null);

  return (
    <div className="h-full overflow-auto">
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
                reads → {readPref}
              </span>
            )}
          </div>

          {/* Read preference explanation */}
          {showReadPref && topology?.kind === 'replicaSet' && topology?.role === 'primary' && (
            <div className="mt-3 text-xs px-3 py-2 rounded-lg flex items-start gap-2" style={{ background:'var(--surface-2)', border:'1px solid var(--border)', color:'var(--text-tertiary)' }}>
              <Info className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" style={{ color:'var(--accent)', opacity:0.7 }} />
              <span>
                Topology shows <strong style={{ color:'var(--text-secondary)' }}>PRIMARY</strong> — this is normal for replica set connections.
                The driver routes <strong style={{ color:'var(--text-secondary)' }}>read</strong> operations to <strong style={{ color:'var(--text-secondary)' }}>{readPref}</strong> members.
                To connect directly to a secondary node, use a single-host URI with <strong style={{ color:'var(--text-secondary)' }}>DirectConnect</strong> enabled.
              </span>
            </div>
          )}

          {/* RS Members */}
          {topology?.kind === 'replicaSet' && rsMembers && rsMembers.length > 0 && (
            <div className="mt-3">
              <div className="text-2xs uppercase tracking-wider mb-1.5" style={{ color:'var(--text-tertiary)' }}>Replica Set Members</div>
              <div className="flex flex-wrap gap-1.5">
                {rsMembers.map(member => (
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

          {/* Multi-host note for non-RS */}
          {isMultiHost && topology?.kind !== 'replicaSet' && (
            <div className="mt-2 text-2xs" style={{ color:'var(--text-tertiary)' }}>
              {connectionInfo.host.split(',').length} nodes · <span title={connectionInfo.host} className="font-mono">{connectionInfo.host.split(',')[0]}</span> and {connectionInfo.host.split(',').length - 1} more
            </div>
          )}
        </div>

        <div className="grid grid-cols-4 gap-3 mb-6 float-in float-in-delay-1">
          {[
            { label: 'Databases', value: databases.length, icon: Database, color: 'var(--accent)' },
            { label: 'Total Size', value: formatBytes(totalSize), icon: Server, color: '#60a5fa' },
            { label: 'Version', value: connectionInfo?.version || '-', icon: Info, color: '#a78bfa' },
            { label: 'Production Guard', value: connectionInfo?.isProduction ? 'ON' : 'OFF', icon: connectionInfo?.isProduction ? ShieldOff : Shield, color: connectionInfo?.isProduction ? '#fbbf24' : '#34d399' },
          ].map(({ label, value, icon: Icon, color }) => (
            <div key={label} className="p-4 rounded-xl transition-all" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
              <div className="flex items-center gap-2 mb-2">
                <Icon className="w-4 h-4" style={{ color, opacity: 0.8 }} />
                <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>{label}</span>
              </div>
              <span className="text-lg font-display font-bold" style={{ color: 'var(--text-primary)' }}>{value}</span>
            </div>
          ))}
        </div>

        {serverStatus?.serverStatus && (
          <div className="mb-6 float-in float-in-delay-2">
            <h3 className="text-xs font-semibold uppercase tracking-wider mb-3 flex items-center gap-2" style={{ color: 'var(--text-tertiary)' }}>
              <Activity className="w-3.5 h-3.5" />Server Details
            </h3>
            <div className="grid grid-cols-3 gap-3">
              {[
                { label: 'Uptime', value: serverStatus.serverStatus.uptime ? formatDuration(serverStatus.serverStatus.uptime * 1000) : '-' },
                { label: 'Connections', value: serverStatus.serverStatus.connections ? `${serverStatus.serverStatus.connections.current} / ${serverStatus.serverStatus.connections.available}` : '-' },
                { label: 'Storage Engine', value: serverStatus.serverStatus.storageEngine?.name || '-' },
                ...(serverStatus.serverStatus.opcounters ? [
                  { label: 'Inserts', value: formatNumber(serverStatus.serverStatus.opcounters.insert || 0) },
                  { label: 'Queries', value: formatNumber(serverStatus.serverStatus.opcounters.query || 0) },
                  { label: 'Updates', value: formatNumber(serverStatus.serverStatus.opcounters.update || 0) },
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
          </div>
        )}

        <div className="float-in float-in-delay-3">
          <h3 className="text-xs font-semibold uppercase tracking-wider mb-3 flex items-center gap-2" style={{ color: 'var(--text-tertiary)' }}>
            <Database className="w-3.5 h-3.5" />Databases
          </h3>
          <div className="space-y-1">
            {databases.map((db) => (
              <div key={db.name} className="flex items-center justify-between px-3 py-2 rounded-lg transition-colors" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>
                <div className="flex items-center gap-2">
                  <Database className="w-3.5 h-3.5" style={{ color: 'var(--accent)', opacity: 0.6 }} />
                  <span className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>{db.name}</span>
                </div>
                <span className="text-2xs font-mono" style={{ color: 'var(--text-tertiary)' }}>{formatBytes(db.sizeOnDisk || 0)}</span>
              </div>
            ))}
          </div>
        </div>

        {connectionInfo?.capabilities && (
          <div className="mt-6 float-in float-in-delay-4">
            <h3 className="text-xs font-semibold uppercase tracking-wider mb-3 flex items-center gap-2" style={{ color: 'var(--text-tertiary)' }}>
              <Zap className="w-3.5 h-3.5" />Capabilities
            </h3>
            <div className="flex flex-wrap gap-2">
              {Object.entries(connectionInfo.capabilities).map(([key, supported]) => (
                <span key={key} className={supported ? 'badge-green' : 'badge-red'}>
                  {key.replace(/([A-Z])/g, ' $1').trim()}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

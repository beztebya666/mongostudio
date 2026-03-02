import React from 'react';
import { Logo, Database, Zap, Server } from './Icons';
import { formatBytes, formatNumber } from '../utils/formatters';

export default function WelcomePanel({ databases, connectionInfo }) {
  const totalSize = databases.reduce((sum, db) => sum + (db.sizeOnDisk || 0), 0);
  const totalCollections = databases.reduce((sum, db) => sum + (db.collections || 0), 0);

  return (
    <div className="h-full flex items-center justify-center p-8">
      <div className="text-center max-w-lg">
        <div className="inline-flex mb-6 opacity-20">
          <Logo size={64} />
        </div>
        <h2 className="text-xl font-display font-semibold text-text-secondary mb-2">
          Select a collection
        </h2>
        <p className="text-sm text-text-tertiary mb-8">
          Choose a database and collection from the sidebar to start browsing documents.
        </p>

        {databases.length > 0 && (
          <div className="grid grid-cols-3 gap-3">
            {[
              { label: 'Databases', value: formatNumber(databases.length), icon: Database },
              { label: 'Total Size', value: formatBytes(totalSize), icon: Server },
              { label: 'Connected', value: connectionInfo?.host || '—', icon: Zap },
            ].map(({ label, value, icon: Icon }, i) => (
              <div
                key={label}
                className={`bg-surface-1 border border-border rounded-xl p-4 float-in float-in-delay-${i + 1}`}
              >
                <Icon className="w-4 h-4 text-accent/50 mb-2" />
                <div className="text-lg font-semibold text-text-primary truncate">{value}</div>
                <div className="text-2xs text-text-tertiary mt-0.5">{label}</div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

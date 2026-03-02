import React, { useState, useCallback, useEffect, useRef } from 'react';
import api from '../utils/api';
import Sidebar from './Sidebar';
import TopBar from './TopBar';
import CollectionView from './CollectionView';
import QueryConsole from './QueryConsole';
import WelcomePanel from './WelcomePanel';

export default function Workspace({ connectionInfo, onDisconnect }) {
  const [databases, setDatabases] = useState([]);
  const [selectedDb, setSelectedDb] = useState(null);
  const [selectedCol, setSelectedCol] = useState(null);
  const [activeTab, setActiveTab] = useState('documents'); // documents | query | indexes
  const [sidebarWidth, setSidebarWidth] = useState(260);
  const [loading, setLoading] = useState(false);
  const [queryMs, setQueryMs] = useState(null);

  const loadDatabases = useCallback(async () => {
    setLoading(true);
    try {
      const data = await api.listDatabases();
      setDatabases(data.databases || []);
      setQueryMs(data._elapsed);
    } catch (err) {
      console.error('Failed to load databases:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadDatabases();
  }, [loadDatabases]);

  const handleSelectCollection = useCallback((db, col) => {
    setSelectedDb(db);
    setSelectedCol(col);
    setActiveTab('documents');
  }, []);

  return (
    <div className="h-screen flex flex-col bg-surface-0 overflow-hidden">
      {/* Version warnings banner */}
      {connectionInfo?.warnings?.length > 0 && (
        <div className="flex-shrink-0 bg-amber-500/5 border-b border-amber-500/20 px-4 py-2 flex items-center gap-2 text-xs text-amber-400">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="flex-shrink-0">
            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>
          </svg>
          <span className="flex-1">{connectionInfo.warnings.join(' · ')}</span>
        </div>
      )}
      <TopBar
        connectionInfo={connectionInfo}
        onDisconnect={onDisconnect}
        selectedDb={selectedDb}
        selectedCol={selectedCol}
        activeTab={activeTab}
        onTabChange={setActiveTab}
        queryMs={queryMs}
      />
      <div className="flex-1 flex overflow-hidden">
        <Sidebar
          databases={databases}
          selectedDb={selectedDb}
          selectedCol={selectedCol}
          onSelect={handleSelectCollection}
          onRefresh={loadDatabases}
          width={sidebarWidth}
          onWidthChange={setSidebarWidth}
          loading={loading}
        />
        <main className="flex-1 overflow-hidden animate-fade-in">
          {selectedCol ? (
            activeTab === 'query' ? (
              <QueryConsole
                db={selectedDb}
                collection={selectedCol}
                onQueryMs={setQueryMs}
              />
            ) : (
              <CollectionView
                db={selectedDb}
                collection={selectedCol}
                onQueryMs={setQueryMs}
                showIndexes={activeTab === 'indexes'}
              />
            )
          ) : (
            <WelcomePanel databases={databases} connectionInfo={connectionInfo} />
          )}
        </main>
      </div>
    </div>
  );
}

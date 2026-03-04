import React, { useState, useCallback, useEffect } from 'react';
import api from '../utils/api';
import Sidebar from './Sidebar';
import TopBar from './TopBar';
import CollectionView from './CollectionView';
import QueryConsole from './QueryConsole';
import WelcomePanel from './WelcomePanel';
import SchemaView from './SchemaView';
import SettingsModal from './SettingsModal';
import ConfirmDialog from './modals/ConfirmDialog';
import { X } from './Icons';

const BANNER_STATE_PREFIX = 'mongostudio_banners:';

export default function Workspace({ connectionInfo, onDisconnect, theme, onToggleTheme }) {
  const [databases, setDatabases] = useState([]);
  const [selectedDb, setSelectedDb] = useState(null);
  const [selectedCol, setSelectedCol] = useState(null);
  const [activeTab, setActiveTab] = useState('documents');
  const [sidebarWidth, setSidebarWidth] = useState(260);
  const [loading, setLoading] = useState(false);
  const [queryMs, setQueryMs] = useState(null);
  const [execMode, setExecMode] = useState('safe');
  const [showSettings, setShowSettings] = useState(false);
  const [refreshToken, setRefreshToken] = useState(0);
  const [refreshing, setRefreshing] = useState(false);
  const [deletedContext, setDeletedContext] = useState(null);
  const [dismissedBanners, setDismissedBanners] = useState({ production: false, warnings: false });
  const [showPowerModeConfirm, setShowPowerModeConfirm] = useState(false);

  useEffect(() => {
    api.getExecutionConfig().then((c) => setExecMode(c.mode)).catch(() => {});
  }, []);

  useEffect(() => {
    const connId = connectionInfo?.connectionId;
    if (!connId) return;
    try {
      const raw = sessionStorage.getItem(`${BANNER_STATE_PREFIX}${connId}`);
      if (raw) {
        const parsed = JSON.parse(raw);
        setDismissedBanners({
          production: Boolean(parsed.production),
          warnings: Boolean(parsed.warnings),
        });
        return;
      }
    } catch {}
    setDismissedBanners({ production: false, warnings: false });
  }, [connectionInfo?.connectionId]);

  useEffect(() => {
    const connId = connectionInfo?.connectionId;
    if (!connId) return;
    try {
      sessionStorage.setItem(`${BANNER_STATE_PREFIX}${connId}`, JSON.stringify(dismissedBanners));
    } catch {}
  }, [dismissedBanners, connectionInfo?.connectionId]);

  const refreshDatabaseList = useCallback(async () => {
    const data = await api.listDatabases();
    setDatabases(data.databases || []);
    setQueryMs(data._elapsed);
    return data.databases || [];
  }, []);

  const loadDatabases = useCallback(async () => {
    setLoading(true);
    try {
      const dbList = await refreshDatabaseList();
      if (selectedDb) {
        const dbExists = dbList.some((d) => d.name === selectedDb);
        if (!dbExists) {
          setDeletedContext({ db: selectedDb, col: selectedCol, reason: 'database' });
          setSelectedDb(null);
          setSelectedCol(null);
          setActiveTab('documents');
          return;
        }
      }

      if (selectedDb && selectedCol) {
        try {
          const c = await api.listCollections(selectedDb);
          const colExists = (c.collections || []).some((col) => col.name === selectedCol);
          if (!colExists) {
            setDeletedContext({ db: selectedDb, col: selectedCol, reason: 'collection' });
            setSelectedDb(null);
            setSelectedCol(null);
            setActiveTab('documents');
            return;
          }
        } catch {}
      }

      if (selectedDb && selectedCol) setDeletedContext(null);
    } catch (err) {
      console.error('Failed to load databases:', err);
    } finally {
      setLoading(false);
    }
  }, [refreshDatabaseList, selectedDb, selectedCol]);

  useEffect(() => {
    loadDatabases();
  }, []);

  const handleSelectCollection = useCallback((db, col) => {
    setDeletedContext(null);
    setSelectedDb(db);
    setSelectedCol(col);
    setActiveTab('documents');
  }, []);

  const handleGlobalRefresh = useCallback(async () => {
    setRefreshing(true);
    try {
      await loadDatabases();
      setRefreshToken((t) => t + 1);
    } finally {
      setRefreshing(false);
    }
  }, [loadDatabases]);

  const handleGoHome = useCallback(async () => {
    setSelectedDb(null);
    setSelectedCol(null);
    setActiveTab('documents');
    setDeletedContext(null);
    setRefreshing(true);
    setLoading(true);
    try {
      await refreshDatabaseList();
      setRefreshToken((t) => t + 1);
    } catch (err) {
      console.error('Failed to refresh databases:', err);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [refreshDatabaseList]);

  const applyExecMode = useCallback(async (mode) => {
    try {
      const config = await api.setExecutionConfig({ mode });
      setExecMode(config.mode);
    } catch (err) {
      console.error(err);
    }
  }, []);

  const requestExecModeChange = useCallback((mode) => {
    if (!mode || mode === execMode) return;
    if (mode === 'power') {
      setShowPowerModeConfirm(true);
      return;
    }
    applyExecMode(mode);
  }, [execMode, applyExecMode]);

  const handleExecModeToggle = useCallback(() => {
    const newMode = execMode === 'safe' ? 'power' : 'safe';
    requestExecModeChange(newMode);
  }, [execMode, requestExecModeChange]);

  const dismissBanner = useCallback((type) => {
    setDismissedBanners((prev) => ({ ...prev, [type]: true }));
  }, []);

  const renderContent = () => {
    if (!selectedCol) return <WelcomePanel databases={databases} connectionInfo={connectionInfo} refreshToken={refreshToken} />;
    switch (activeTab) {
      case 'query':
        return <QueryConsole db={selectedDb} collection={selectedCol} onQueryMs={setQueryMs} execMode={execMode} refreshToken={refreshToken} />;
      case 'indexes':
        return <CollectionView db={selectedDb} collection={selectedCol} onQueryMs={setQueryMs} showIndexes refreshToken={refreshToken} />;
      case 'schema':
        return <SchemaView db={selectedDb} collection={selectedCol} refreshToken={refreshToken} />;
      default:
        return <CollectionView db={selectedDb} collection={selectedCol} onQueryMs={setQueryMs} refreshToken={refreshToken} />;
    }
  };

  return (
    <div className="h-screen flex flex-col overflow-hidden" style={{ background: 'var(--surface-0)' }}>
      {connectionInfo?.isProduction && !dismissedBanners.production && (
        <div className="flex-shrink-0 production-banner px-4 py-1.5 flex items-center gap-2 text-xs" style={{ color: '#f59e0b' }}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
            <line x1="12" y1="9" x2="12" y2="13" />
            <line x1="12" y1="17" x2="12.01" y2="17" />
          </svg>
          <span className="flex-1">Production guard enabled for this connection.</span>
          <button onClick={() => dismissBanner('production')} className="btn-ghost p-1" title="Dismiss">
            <X className="w-3.5 h-3.5" />
          </button>
        </div>
      )}

      {connectionInfo?.warnings?.length > 0 && !dismissedBanners.warnings && (
        <div className="flex-shrink-0 px-4 py-1.5 flex items-center gap-2 text-xs" style={{ background: 'rgba(245,158,11,0.05)', borderBottom: '1px solid rgba(245,158,11,0.15)', color: '#fbbf24' }}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="12" r="10" />
            <line x1="12" y1="8" x2="12" y2="12" />
            <line x1="12" y1="16" x2="12.01" y2="16" />
          </svg>
          <span className="flex-1">{connectionInfo.warnings.join(' | ')}</span>
          <button onClick={() => dismissBanner('warnings')} className="btn-ghost p-1" title="Dismiss">
            <X className="w-3.5 h-3.5" />
          </button>
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
        theme={theme}
        onToggleTheme={onToggleTheme}
        execMode={execMode}
        onExecModeToggle={handleExecModeToggle}
        onShowSettings={() => setShowSettings(true)}
        onGoHome={handleGoHome}
        onRefresh={handleGlobalRefresh}
        refreshing={refreshing || loading}
        deletedContext={deletedContext}
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
          refreshToken={refreshToken}
        />
        <main className="flex-1 overflow-hidden animate-fade-in">
          {renderContent()}
        </main>
      </div>

      {showSettings && (
        <SettingsModal
          execMode={execMode}
          onModeChangeRequest={requestExecModeChange}
          onConfigApplied={setExecMode}
          onClose={() => setShowSettings(false)}
          connectionInfo={connectionInfo}
        />
      )}

      <ConfirmDialog
        open={showPowerModeConfirm}
        title="Enable Power Mode"
        message="Power Mode disables query limits and can execute heavy operations. Continue?"
        confirmLabel="Enable Power"
        cancelLabel="Cancel"
        danger
        onCancel={() => setShowPowerModeConfirm(false)}
        onConfirm={async () => {
          setShowPowerModeConfirm(false);
          await applyExecMode('power');
        }}
      />
    </div>
  );
}

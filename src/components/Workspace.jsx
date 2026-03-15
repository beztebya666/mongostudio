import React, { Suspense, lazy, useState, useCallback, useEffect, useLayoutEffect, useRef, useMemo } from 'react';
import api from '../utils/api';
import Sidebar from './Sidebar';
import TopBar from './TopBar';
import SettingsModal from './SettingsModal';
import AppModal from './modals/AppModal';
import ConfirmDialog from './modals/ConfirmDialog';
import { formatNumber } from '../utils/formatters';
import {
  X,
  ChevronRight,
  ChevronLeft,
  ChevronDown,
  Search,
  Server,
  Activity,
  Clock,
  AlertTriangle,
  Download,
  Upload,
  Collection,
  Check,
  Refresh,
  Settings,
  Info,
} from './Icons';

const BANNER_STATE_PREFIX = 'mongostudio_banners:';
const DISPLAY_SETTINGS_KEY = 'mongostudio_display_settings';
const DEFAULT_DISPLAY_SETTINGS = { showTopTags: false, showReadSourceTag: false };
const CollectionView = lazy(() => import('./CollectionView'));
const QueryConsole = lazy(() => import('./QueryConsole'));
const ConsoleView = lazy(() => import('./ConsoleView'));
const WelcomePanel = lazy(() => import('./WelcomePanel'));
const SchemaView = lazy(() => import('./SchemaView'));
const AuditView = lazy(() => import('./AuditView'));
const ServerManagementView = lazy(() => import('./ServerManagementView'));

const SERVER_TOOL_GROUPS = [
  {
    id: 'monitoring',
    label: 'Monitoring',
    items: [
      { id: 'statTopSlowOps', label: 'stat + top + slowops', hint: 'Combined performance panel', icon: Activity, badge: 'live', tone: 'green' },
      { id: 'serverInfo', label: 'Server Info', hint: 'Server details and health', icon: Server, badge: 'live', tone: 'green' },
      { id: 'mongostat', label: 'mongostat', hint: 'Operations per second', icon: Activity, badge: 'live', tone: 'green' },
      { id: 'mongotop', label: 'mongotop', hint: 'Read/write time by namespace', icon: Clock },
      { id: 'slowOps', label: 'Slow Ops', hint: 'Long-running operations', icon: AlertTriangle, badge: 'ops', tone: 'yellow' },
    ],
  },
  {
    id: 'backup',
    label: 'Backup & Restore',
    items: [
      { id: 'mongodump', label: 'mongodump', hint: 'Backup dump', icon: Download },
      { id: 'mongorestore', label: 'mongorestore', hint: 'Restore dump', icon: Upload },
      { id: 'mongoexport', label: 'mongoexport', hint: 'Export JSON/CSV', icon: Download },
      { id: 'mongoimport', label: 'mongoimport', hint: 'Import JSON/CSV', icon: Upload },
    ],
  },
  {
    id: 'gridfs',
    label: 'GridFS',
    items: [
      { id: 'mongofiles', label: 'mongofiles', hint: 'Manage GridFS files', icon: Collection },
    ],
  },
];

function serverBadgeClass(tone = '') {
  if (tone === 'green') return 'badge-green';
  if (tone === 'yellow') return 'badge-yellow';
  if (tone === 'blue') return 'badge-blue';
  if (tone === 'red') return 'badge-red';
  return 'badge-accent';
}

export default function Workspace({ connectionInfo, onDisconnect, theme, onToggleTheme }) {
  const workspaceRootRef = useRef(null);
  const headerStackRef = useRef(null);
  const isDemoMode = api.isMockMode?.() === true;
  const [adminAccess, setAdminAccess] = useState({ required: false, verified: false });
  const [databases, setDatabases] = useState([]);
  const [selectedDb, setSelectedDb] = useState(null);
  const [selectedCol, setSelectedCol] = useState(null);
  const [activeTab, setActiveTab] = useState('documents');
  const [consoleScope, setConsoleScope] = useState({ level: 'collection', db: null, collection: null });
  const [sidebarWidth, setSidebarWidth] = useState(260);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [rightSidebarCollapsed, setRightSidebarCollapsed] = useState(true);
  const [serverFilter, setServerFilter] = useState('');
  const [serverExpanded, setServerExpanded] = useState(true);
  const [activeServerTool, setActiveServerTool] = useState('');
  const [loading, setLoading] = useState(false);
  const [queryMs, setQueryMs] = useState(null);
  const [execMode, setExecMode] = useState('safe');
  const [showSettings, setShowSettings] = useState(false);
  const [refreshToken, setRefreshToken] = useState(0);
  const [refreshing, setRefreshing] = useState(false);
  const [deletedContext, setDeletedContext] = useState(null);
  const [dismissedBanners, setDismissedBanners] = useState({ production: false, warnings: false });
  const [showPowerModeConfirm, setShowPowerModeConfirm] = useState(false);
  const [showServerToolsDemoModal, setShowServerToolsDemoModal] = useState(false);
  const [metadataOverview, setMetadataOverview] = useState({
    stats: {},
    freshness: {},
    warning: '',
    totalSize: 0,
    budget: null,
    loading: false,
    loaded: false,
    error: '',
  });
  const [displaySettings, setDisplaySettings] = useState(() => {
    try {
      const raw = localStorage.getItem(DISPLAY_SETTINGS_KEY);
      if (!raw) return DEFAULT_DISPLAY_SETTINGS;
      const parsed = JSON.parse(raw);
      return { ...DEFAULT_DISPLAY_SETTINGS, ...parsed };
    } catch {
      return DEFAULT_DISPLAY_SETTINGS;
    }
  });

  useEffect(() => {
    api.getExecutionConfig().then((c) => setExecMode(c.mode)).catch(() => {});
    if (!isDemoMode) {
      api.getAdminAccess().then((status) => {
        setAdminAccess(status);
        // Auto-verify from localStorage if key is required but not yet verified
        if (status.required && !status.verified) {
          try {
            const storedKey = localStorage.getItem('mongostudio_admin_key');
            if (storedKey) {
              api.verifyAdminAccess(storedKey)
                .then((r) => setAdminAccess(r))
                .catch(() => { try { localStorage.removeItem('mongostudio_admin_key'); } catch {} });
            }
          } catch {}
        }
      }).catch(() => {});
    }
  }, [isDemoMode]);

  // Auto-expand/collapse right sidebar based on admin access state
  const adminSidebarInitRef = useRef(false);
  useEffect(() => {
    // Demo mode: sidebar is always available (tools are gated by demo modal instead)
    if (isDemoMode && !adminSidebarInitRef.current) {
      adminSidebarInitRef.current = true;
      setRightSidebarCollapsed(false);
      return;
    }
    // Wait for admin access API to respond (initial state is required:false, verified:false)
    if (!adminAccess.required && !adminAccess.verified) return;
    const locked = adminAccess.required && !adminAccess.verified;
    if (locked) {
      setRightSidebarCollapsed(true);
      adminSidebarInitRef.current = false;
    } else if (!adminSidebarInitRef.current) {
      adminSidebarInitRef.current = true;
      setRightSidebarCollapsed(false);
    }
  }, [adminAccess, isDemoMode]);

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

  useEffect(() => {
    try {
      localStorage.setItem(DISPLAY_SETTINGS_KEY, JSON.stringify(displaySettings));
    } catch {}
  }, [displaySettings]);

  useEffect(() => {
    const shouldShowReadSource = connectionInfo?.readPreferenceExplicit === true;
    setDisplaySettings((prev) => {
      if (Boolean(prev.showReadSourceTag) === shouldShowReadSource) return prev;
      return { ...prev, showReadSourceTag: shouldShowReadSource };
    });
  }, [connectionInfo?.connectionId, connectionInfo?.readPreferenceExplicit]);

  useLayoutEffect(() => {
    const root = workspaceRootRef.current;
    const header = headerStackRef.current;
    if (!root || !header) return undefined;

    const applyHeaderOffset = () => {
      const rect = header.getBoundingClientRect();
      const heightPx = Math.max(0, Math.round(rect.height));
      root.style.setProperty('--workspace-header-bottom', `${heightPx}px`);
    };

    applyHeaderOffset();
    if (typeof ResizeObserver === 'undefined') {
      window.addEventListener('resize', applyHeaderOffset);
      return () => {
        window.removeEventListener('resize', applyHeaderOffset);
        root.style.removeProperty('--workspace-header-bottom');
      };
    }

    const observer = new ResizeObserver(() => applyHeaderOffset());
    observer.observe(header);
    return () => {
      observer.disconnect();
      root.style.removeProperty('--workspace-header-bottom');
    };
  }, []);

  const filteredServerGroups = useMemo(() => {
    const query = String(serverFilter || '').trim().toLowerCase();
    if (!query) return SERVER_TOOL_GROUPS;
    return SERVER_TOOL_GROUPS.map((group) => {
      const groupMatch = group.label.toLowerCase().includes(query);
      const items = group.items.filter((tool) => (
        groupMatch
        || tool.label.toLowerCase().includes(query)
        || tool.hint.toLowerCase().includes(query)
      ));
      return items.length > 0 ? { ...group, items } : null;
    }).filter(Boolean);
  }, [serverFilter]);

  const visibleServerTools = useMemo(
    () => filteredServerGroups.flatMap((group) => group.items),
    [filteredServerGroups],
  );
  const databaseNameList = useMemo(
    () => [...new Set((Array.isArray(databases) ? databases : []).map((entry) => String(entry?.name || '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b)),
    [databases],
  );
  const preferredDbFromConnection = useMemo(() => {
    const primary = String(connectionInfo?.defaultDb || '').trim();
    if (primary) return primary;
    const fallback = String(connectionInfo?.authSource || '').trim();
    return fallback || null;
  }, [connectionInfo?.authSource, connectionInfo?.defaultDb]);

  const syncDbContext = useCallback((dbName, meta = {}) => {
    const nextDb = String(dbName || '').trim();
    if (!nextDb) return;
    const source = String(meta?.source || '').trim().toLowerCase();
    const sourceScope = String(meta?.scope || '').trim().toLowerCase();
    if (selectedDb !== nextDb) {
      setSelectedDb(nextDb);
      setSelectedCol(null);
    }
    setConsoleScope((prev) => {
      if (source === 'server-management') {
        return { level: 'global', db: nextDb, collection: null };
      }
      if (source === 'console') {
        if (sourceScope === 'database') return { level: 'database', db: nextDb, collection: null };
        if (sourceScope === 'collection') return prev;
        return { level: 'global', db: nextDb, collection: null };
      }
      return prev;
    });
  }, [selectedDb]);

  const refreshDatabaseList = useCallback(async ({ refresh = false } = {}) => {
    setMetadataOverview((prev) => ({ ...prev, loading: true, error: '' }));
    try {
      const data = await api.getMetadataOverview({ refresh });
      const dbList = Array.isArray(data?.databases) ? data.databases : [];
      setDatabases(dbList);
      setMetadataOverview({
        stats: data?.stats && typeof data.stats === 'object' ? data.stats : {},
        freshness: data?.freshness && typeof data.freshness === 'object' ? data.freshness : {},
        warning: data?.warning || '',
        totalSize: Number(data?.totalSize || 0),
        budget: data?.budget || null,
        loading: false,
        loaded: true,
        error: '',
      });
      setQueryMs(data?._elapsed ?? null);
      return dbList;
    } catch (overviewErr) {
      const fallback = await api.listDatabases();
      const dbList = Array.isArray(fallback?.databases) ? fallback.databases : [];
      setDatabases(dbList);
      setMetadataOverview({
        stats: {},
        freshness: {},
        warning: '',
        totalSize: Number(fallback?.totalSize || 0),
        budget: null,
        loading: false,
        loaded: false,
        error: overviewErr?.message || 'Metadata overview unavailable',
      });
      setQueryMs(fallback?._elapsed ?? null);
      return dbList;
    }
  }, []);

  const loadDatabases = useCallback(async ({ refresh = false } = {}) => {
    setLoading(true);
    try {
      const dbList = await refreshDatabaseList({ refresh });
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
      if (!selectedDb && preferredDbFromConnection) {
        const preferredExists = dbList.some((entry) => String(entry?.name || '').trim() === preferredDbFromConnection);
        if (preferredExists) {
          setSelectedDb(preferredDbFromConnection);
          setConsoleScope((prev) => (
            prev.level === 'collection'
              ? prev
              : { ...prev, db: preferredDbFromConnection, collection: null }
          ));
        }
      }
    } catch (err) {
      console.error('Failed to load databases:', err);
    } finally {
      setLoading(false);
    }
  }, [preferredDbFromConnection, refreshDatabaseList, selectedDb, selectedCol]);

  useEffect(() => {
    loadDatabases();
  }, []);

  const handleSelectCollection = useCallback((db, col) => {
    setDeletedContext(null);
    setSelectedDb(db);
    setSelectedCol(col);
    setActiveTab('documents');
  }, []);

  const openConsole = useCallback((scope = {}) => {
    if (adminLocked) {
      setShowAdminLockedModal(true);
      return;
    }
    const level = scope?.level === 'database' || scope?.level === 'global' ? scope.level : 'collection';
    const nextDb = typeof scope?.db === 'string' && scope.db.trim() ? scope.db.trim() : (selectedDb || preferredDbFromConnection || null);
    const nextCollection = typeof scope?.collection === 'string' && scope.collection.trim()
      ? scope.collection.trim()
      : (selectedCol || null);

    if (level === 'collection') {
      if (!nextDb || !nextCollection) return;
      setSelectedDb(nextDb);
      setSelectedCol(nextCollection);
      setConsoleScope({ level: 'collection', db: nextDb, collection: nextCollection });
      setActiveTab('console');
      return;
    }

    if (level === 'database') {
      if (!nextDb) return;
      setSelectedDb(nextDb);
      setSelectedCol(null);
      setConsoleScope({ level: 'database', db: nextDb, collection: null });
      setActiveTab('console');
      return;
    }

    setConsoleScope({ level: 'global', db: nextDb, collection: null });
    setActiveTab('console');
  }, [preferredDbFromConnection, selectedDb, selectedCol]);

  const adminLocked = adminAccess.required && !adminAccess.verified;
  const [showServerMgmtLockedModal, setShowServerMgmtLockedModal] = useState(false);

  const openServerTool = useCallback((toolId) => {
    if (!toolId) return;
    if (isDemoMode) {
      setShowServerToolsDemoModal(true);
      return;
    }
    if (adminLocked) {
      setShowServerMgmtLockedModal(true);
      return;
    }
    setActiveServerTool(toolId);
    setActiveTab('server-management');
  }, [isDemoMode, adminLocked]);

  const handleGlobalRefresh = useCallback(async () => {
    setRefreshing(true);
    try {
      await loadDatabases({ refresh: true });
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
      await refreshDatabaseList({ refresh: true });
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

  const [showAdminLockedModal, setShowAdminLockedModal] = useState(false);

  const handleTopBarTabChange = useCallback((tab) => {
    setShowSettings(false);
    if (tab === 'server-management' && isDemoMode) {
      setShowServerToolsDemoModal(true);
      return;
    }
    if (tab === 'server-management' && adminLocked) {
      setShowServerMgmtLockedModal(true);
      return;
    }
    if (tab === 'console' && adminLocked) {
      setShowAdminLockedModal(true);
      return;
    }
    if (tab === 'console') {
      const preferredDb = selectedDb || preferredDbFromConnection || null;
      if (activeTab === 'server-management') {
        setConsoleScope({ level: 'global', db: preferredDb, collection: null });
      } else if (selectedCol && selectedDb) {
        setConsoleScope({ level: 'collection', db: selectedDb, collection: selectedCol });
      } else {
        setConsoleScope({ level: 'global', db: preferredDb, collection: null });
      }
    }
    setActiveTab(tab);
  }, [activeTab, adminLocked, isDemoMode, preferredDbFromConnection, selectedDb, selectedCol]);

  const handleTopBarRefresh = useCallback(async () => {
    setShowSettings(false);
    await handleGlobalRefresh();
  }, [handleGlobalRefresh]);

  const handleTopBarGoHome = useCallback(async () => {
    setShowSettings(false);
    await handleGoHome();
  }, [handleGoHome]);

  const handleTopBarExecModeToggle = useCallback(() => {
    setShowSettings(false);
    handleExecModeToggle();
  }, [handleExecModeToggle]);

  const handleTopBarDisconnect = useCallback(async () => {
    setShowSettings(false);
    await onDisconnect();
  }, [onDisconnect]);

  const handleTopBarThemeToggle = useCallback(() => {
    setShowSettings(false);
    onToggleTheme?.();
  }, [onToggleTheme]);

  const dismissBanner = useCallback((type) => {
    setDismissedBanners((prev) => ({ ...prev, [type]: true }));
  }, []);

  const renderContent = () => {
    if (activeTab === 'audit') return <AuditView refreshToken={refreshToken} />;
    if (activeTab === 'server-management') {
      if (!activeServerTool) {
        return (
          <div className="h-full flex items-center justify-center" style={{ background: 'var(--surface-0)' }}>
            <div className="text-center">
              <Server className="w-8 h-8 mx-auto mb-3" style={{ color: 'var(--text-tertiary)' }} />
              <div className="text-sm" style={{ color: 'var(--text-tertiary)' }}>Select a tool from the sidebar</div>
            </div>
          </div>
        );
      }
      return (
        <ServerManagementView
          activeTool={activeServerTool}
          selectedDb={selectedDb}
          selectedCol={selectedCol}
          connectionId={connectionInfo?.connectionId || ''}
          onDbContextChange={syncDbContext}
        />
      );
    }
    if (activeTab === 'console') {
      return (
        <ConsoleView
          scope={consoleScope}
          db={selectedDb}
          collection={selectedCol}
          databaseNames={databaseNameList}
          onDbContextChange={syncDbContext}
          onQueryMs={setQueryMs}
          refreshToken={refreshToken}
          adminLocked={adminLocked}
        />
      );
    }
    if (!selectedCol) return <WelcomePanel databases={databases} connectionInfo={connectionInfo} refreshToken={refreshToken} metadata={metadataOverview} />;
    switch (activeTab) {
      case 'query':
        return <QueryConsole db={selectedDb} collection={selectedCol} onQueryMs={setQueryMs} execMode={execMode} refreshToken={refreshToken} />;
      case 'indexes':
        return <CollectionView db={selectedDb} collection={selectedCol} onQueryMs={setQueryMs} showIndexes refreshToken={refreshToken} isProduction={connectionInfo?.isProduction === true} />;
      case 'schema':
        return <SchemaView db={selectedDb} collection={selectedCol} refreshToken={refreshToken} />;
      default:
        return <CollectionView db={selectedDb} collection={selectedCol} onQueryMs={setQueryMs} refreshToken={refreshToken} isProduction={connectionInfo?.isProduction === true} />;
    }
  };

  const rightSidebarWidth = rightSidebarCollapsed ? 48 : 340;

  return (
    <div
      ref={workspaceRootRef}
      className="h-screen flex flex-col overflow-hidden"
      style={{
        background: 'var(--surface-0)',
        '--workspace-right-sidebar-width': `${rightSidebarWidth}px`,
      }}
    >
      <div ref={headerStackRef} className="flex-shrink-0">
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
          onDisconnect={handleTopBarDisconnect}
          selectedDb={selectedDb}
          selectedCol={selectedCol}
          activeTab={activeTab}
          onTabChange={handleTopBarTabChange}
          queryMs={queryMs}
          theme={theme}
          onToggleTheme={handleTopBarThemeToggle}
          execMode={execMode}
          onExecModeToggle={handleTopBarExecModeToggle}
          onShowSettings={() => setShowSettings((prev) => !prev)}
          onGoHome={handleTopBarGoHome}
          onRefresh={handleTopBarRefresh}
          refreshing={refreshing || loading}
          deletedContext={deletedContext}
          showTopTags={Boolean(displaySettings.showTopTags)}
          showReadSourceTag={Boolean(displaySettings.showReadSourceTag)}
        />
      </div>

      <div className="flex-1 flex overflow-hidden">
        {sidebarCollapsed ? (
          <aside className="flex-shrink-0 w-10 flex items-start justify-center pt-2" style={{ borderRight:'1px solid var(--border)', background:'var(--surface-1)' }}>
            <button
              onClick={() => setSidebarCollapsed(false)}
              className="btn-ghost p-1.5"
              title="Expand sidebar"
            >
              <ChevronRight className="w-3.5 h-3.5" />
            </button>
          </aside>
        ) : (
          <Sidebar
            databases={databases}
            selectedDb={selectedDb}
            selectedCol={selectedCol}
            onSelect={handleSelectCollection}
            onOpenConsole={openConsole}
            onRefresh={() => loadDatabases({ refresh: true })}
            width={sidebarWidth}
            onWidthChange={setSidebarWidth}
            loading={loading}
            refreshToken={refreshToken}
            metadata={metadataOverview}
            onToggleCollapse={() => setSidebarCollapsed(true)}
            execMode={execMode}
          />
        )}
        <main className="flex-1 overflow-hidden animate-fade-in">
          <Suspense
            fallback={(
              <div className="h-full flex items-center justify-center text-xs" style={{ color: 'var(--text-tertiary)' }}>
                Loading view...
              </div>
            )}
          >
            {renderContent()}
          </Suspense>
        </main>
        {rightSidebarCollapsed ? (
          <aside className="w-12 flex-shrink-0 flex flex-col" style={{ borderLeft: '1px solid var(--border)', background: 'var(--surface-1)' }}>
            <div className="h-11 flex items-center justify-center" style={{ borderBottom: '1px solid var(--border)' }}>
              <button type="button" className="btn-ghost p-1.5" onClick={() => {
                if (adminLocked) { setShowServerMgmtLockedModal(true); return; }
                setRightSidebarCollapsed(false);
              }} title="Expand server sidebar">
                <ChevronLeft className="w-3.5 h-3.5" />
              </button>
            </div>
            <div className="flex-1 overflow-auto py-2 flex flex-col items-center gap-1">
              {visibleServerTools.map((tool) => {
                const Icon = tool.icon;
                const isActive = activeTab === 'server-management' && activeServerTool === tool.id;
                return (
                  <button
                    key={`collapsed:${tool.id}`}
                    type="button"
                    className="w-8 h-8 rounded-md flex items-center justify-center"
                    style={{
                      color: isActive ? 'var(--accent)' : 'var(--text-secondary)',
                      background: isActive ? 'var(--surface-3)' : 'transparent',
                    }}
                    title={tool.label}
                    onClick={() => openServerTool(tool.id)}
                  >
                    <Icon className="w-4 h-4" />
                  </button>
                );
              })}
            </div>
          </aside>
        ) : (
          <aside className="w-[340px] flex-shrink-0 flex flex-col" style={{ borderLeft: '1px solid var(--border)', background: 'var(--surface-1)' }}>
            <div className="h-11 px-2 flex items-center gap-2" style={{ borderBottom: '1px solid var(--border)' }}>
              <span className="text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>Server Management</span>
              <div className="flex-1" />
              <button type="button" className="btn-ghost p-1.5" onClick={() => setRightSidebarCollapsed(true)} title="Collapse server sidebar">
                <ChevronRight className="w-3.5 h-3.5" />
              </button>
            </div>
            <div className="p-2" style={{ borderBottom: '1px solid var(--border)' }}>
              <div className="relative">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5" style={{ color: 'var(--text-tertiary)' }} />
                <input
                  type="text"
                  value={serverFilter}
                  onChange={(e) => setServerFilter(e.target.value)}
                  placeholder="Filter server tools"
                  className="w-full rounded-lg pl-8 pr-3 py-1.5 text-xs"
                  style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
                />
              </div>
            </div>
            <div className="flex-1 overflow-auto p-2">
              <button
                type="button"
                className="w-full text-left px-2 py-1.5 rounded-lg text-xs flex items-center gap-2 mb-1"
                style={{
                  color: activeTab === 'server-management' ? 'var(--accent)' : 'var(--text-secondary)',
                  background: activeTab === 'server-management' ? 'var(--surface-3)' : 'transparent',
                }}
                onClick={() => {
                  setServerExpanded((prev) => !prev);
                }}
              >
                {serverExpanded ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                <Server className="w-3.5 h-3.5" />
                <span className="flex-1">Server-management</span>
              </button>
              {serverExpanded ? (
                <div className="pl-5 space-y-2">
                  {filteredServerGroups.map((group) => (
                    <div key={group.id}>
                      <div className="px-2 py-1 text-2xs uppercase tracking-wider" style={{ color: 'var(--text-tertiary)' }}>{group.label}</div>
                      <div className="space-y-0.5">
                        {group.items.map((tool) => {
                          const Icon = tool.icon;
                          const isActive = activeTab === 'server-management' && activeServerTool === tool.id;
                          return (
                            <button
                              key={tool.id}
                              type="button"
                              className="w-full text-left px-2 py-1.5 rounded-md text-xs"
                              style={{
                                color: isActive ? 'var(--accent)' : 'var(--text-secondary)',
                                background: isActive ? 'var(--surface-3)' : 'transparent',
                              }}
                              onClick={() => openServerTool(tool.id)}
                            >
                              <div className="flex items-center gap-2">
                                <span className="inline-flex items-center justify-center w-4 h-4"><Icon className="w-3.5 h-3.5" /></span>
                                <span className="truncate flex-1">{tool.label}</span>
                                {isActive ? <Check className="w-3 h-3" style={{ color: 'var(--accent)' }} /> : null}
                              </div>
                              <div className="text-2xs mt-0.5 truncate" style={{ color: 'var(--text-tertiary)' }}>{tool.hint}</div>
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  ))}
                </div>
              ) : null}
            </div>
          </aside>
        )}
      </div>

      {showSettings && (
        <SettingsModal
          execMode={execMode}
          onModeChangeRequest={requestExecModeChange}
          onConfigApplied={setExecMode}
          onClose={() => setShowSettings(false)}
          connectionInfo={connectionInfo}
          databases={databases}
          metadataOverview={metadataOverview}
          displaySettings={displaySettings}
          onDisplaySettingsChange={(changes) => setDisplaySettings((prev) => ({ ...prev, ...changes }))}
          adminAccess={adminAccess}
          onAdminAccessChange={setAdminAccess}
        />
      )}

      <AppModal open={showServerToolsDemoModal} onClose={() => setShowServerToolsDemoModal(false)} maxWidth="max-w-md">
        <div className="px-5 py-4 flex items-center gap-2" style={{ borderBottom: '1px solid var(--border)' }}>
          <Info className="w-4 h-4" style={{ color: 'var(--accent)' }} />
          <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>Server Tools Locked in Demo</h3>
        </div>
        <div className="px-5 py-4">
          <p className="text-xs leading-relaxed" style={{ color: 'var(--text-secondary)' }}>
            This section works only with a real container-backed connection.
          </p>
          <p className="text-xs leading-relaxed mt-2" style={{ color: 'var(--text-tertiary)' }}>
            Admin permissions are required to open server-management tools.
          </p>
        </div>
        <div className="px-5 py-4 flex items-center justify-end gap-2" style={{ borderTop: '1px solid var(--border)' }}>
          <button type="button" className="btn-primary text-xs px-3 py-1.5" onClick={() => setShowServerToolsDemoModal(false)}>
            Understood
          </button>
        </div>
      </AppModal>

      <AppModal open={showAdminLockedModal} onClose={() => setShowAdminLockedModal(false)} maxWidth="max-w-md">
        <div className="px-5 py-4 flex items-center gap-2" style={{ borderBottom: '1px solid var(--border)' }}>
          <Info className="w-4 h-4" style={{ color: 'var(--accent)' }} />
          <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>Console Locked</h3>
        </div>
        <div className="px-5 py-4">
          <p className="text-xs leading-relaxed" style={{ color: 'var(--text-secondary)' }}>
            An admin access key is required to use consoles.
          </p>
          <p className="text-xs leading-relaxed mt-2" style={{ color: 'var(--text-tertiary)' }}>
            Go to Settings &rarr; Execution to enter the admin access key.
          </p>
        </div>
        <div className="px-5 py-4 flex items-center justify-end gap-2" style={{ borderTop: '1px solid var(--border)' }}>
          <button type="button" className="btn-primary text-xs px-3 py-1.5" onClick={() => { setShowAdminLockedModal(false); setShowSettings(true); }}>
            Open Settings
          </button>
        </div>
      </AppModal>

      <AppModal open={showServerMgmtLockedModal} onClose={() => setShowServerMgmtLockedModal(false)} maxWidth="max-w-md">
        <div className="px-5 py-4 flex items-center gap-2" style={{ borderBottom: '1px solid var(--border)' }}>
          <Info className="w-4 h-4" style={{ color: 'var(--accent)' }} />
          <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>Server Management Locked</h3>
        </div>
        <div className="px-5 py-4">
          <p className="text-xs leading-relaxed" style={{ color: 'var(--text-secondary)' }}>
            An admin access key is required to use server management tools.
          </p>
          <p className="text-xs leading-relaxed mt-2" style={{ color: 'var(--text-tertiary)' }}>
            Go to Settings &rarr; Execution to enter the admin access key.
          </p>
        </div>
        <div className="px-5 py-4 flex items-center justify-end gap-2" style={{ borderTop: '1px solid var(--border)' }}>
          <button type="button" className="btn-primary text-xs px-3 py-1.5" onClick={() => { setShowServerMgmtLockedModal(false); setShowSettings(true); }}>
            Open Settings
          </button>
        </div>
      </AppModal>

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

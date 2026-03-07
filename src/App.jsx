import React, { useState, useCallback, useEffect } from 'react';
import api, { setApiMode } from './utils/api';
import ConnectPage from './components/ConnectPage';
import Workspace from './components/Workspace';

const THEME_KEY = 'mongostudio_theme';

export default function App() {
  const [connected, setConnected] = useState(false);
  const [connectionInfo, setConnectionInfo] = useState(null);
  const [error, setError] = useState(null);
  const [connecting, setConnecting] = useState(false);
  const [theme, setTheme] = useState(() => {
    try { return localStorage.getItem(THEME_KEY) || 'dark'; } catch { return 'dark'; }
  });

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    try { localStorage.setItem(THEME_KEY, theme); } catch {}
  }, [theme]);

  const toggleTheme = useCallback(() => {
    setTheme(t => t === 'dark' ? 'light' : 'dark');
  }, []);

  const isMockUri = useCallback((uri) => /^mock:\/\//i.test(String(uri || '')), []);

  const handleConnect = useCallback(async (uri, options = {}) => {
    setConnecting(true);
    setError(null);
    setApiMode(isMockUri(uri) ? 'mock' : 'real');
    try {
      const info = await api.connect(uri, options);
      setConnectionInfo(info);
      setConnected(true);
      if (info.warnings?.length) {
        info.warnings.forEach(w => console.warn('[MongoStudio]', w));
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setConnecting(false);
    }
  }, [isMockUri]);

  const handleDisconnect = useCallback(async () => {
    await api.disconnect();
    setConnected(false);
    setConnectionInfo(null);
  }, []);

  if (!connected) {
    return (
      <ConnectPage
        onConnect={handleConnect}
        connecting={connecting}
        error={error}
        theme={theme}
        onToggleTheme={toggleTheme}
      />
    );
  }

  return (
    <Workspace
      connectionInfo={connectionInfo}
      onDisconnect={handleDisconnect}
      theme={theme}
      onToggleTheme={toggleTheme}
    />
  );
}

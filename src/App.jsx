import React, { useState, useCallback, useRef } from 'react';
import api from './utils/api';
import ConnectPage from './components/ConnectPage';
import Workspace from './components/Workspace';

export default function App() {
  const [connected, setConnected] = useState(false);
  const [connectionInfo, setConnectionInfo] = useState(null);
  const [error, setError] = useState(null);
  const [connecting, setConnecting] = useState(false);

  const handleConnect = useCallback(async (uri) => {
    setConnecting(true);
    setError(null);
    try {
      const info = await api.connect(uri);
      setConnectionInfo(info);
      setConnected(true);
      // Log version warnings to console
      if (info.warnings?.length) {
        info.warnings.forEach(w => console.warn('[MongoStudio]', w));
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setConnecting(false);
    }
  }, []);

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
      />
    );
  }

  return (
    <Workspace
      connectionInfo={connectionInfo}
      onDisconnect={handleDisconnect}
    />
  );
}

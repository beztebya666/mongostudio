import React, { useState, useEffect, useRef } from 'react';
import { Logo, Zap, Server, Loader, AlertCircle, Moon, Sun, ChevronDown, Lock, Globe, Settings, Trash, X } from './Icons';

const RECENT_KEY = 'mongostudio_recent';
const PROFILES_KEY = 'mongostudio_profiles';
const RECENT_PASSWORD_PREF_KEY = 'mongostudio_recent_dont_save_password';
const CONNECT_HINTS_KEY = 'mongostudio_connect_hints';
const DEFAULT_OPTIONS = {
  tls: undefined,
  tlsAllowInvalidCertificates: false,
  username: '',
  password: '',
  authSource: '',
  replicaSet: '',
  directConnection: undefined,
  readPreference: '',
  connectTimeoutMS: 15000,
  markAsProduction: false,
};

function getRecent() { try { return JSON.parse(localStorage.getItem(RECENT_KEY)||'[]').slice(0,5); } catch { return []; } }
function getDontSaveRecentPassword() { try { return localStorage.getItem(RECENT_PASSWORD_PREF_KEY) === '1'; } catch { return false; } }
function sanitizeSavedOptions(opts = {}, includePassword = false) {
  if (includePassword && typeof opts.password === 'string') return { ...opts };
  const { password, ...rest } = opts;
  return rest;
}
function stripPasswordFromUri(uri) {
  try {
    return uri.replace(/(mongodb(?:\+srv)?:\/\/[^:/@]+):[^@]*@/i, '$1@');
  } catch {
    return uri;
  }
}
function saveRecent(uri, opts, includePassword = true) {
  try {
    const savedUri = includePassword ? uri : stripPasswordFromUri(uri);
    const savedOptions = sanitizeSavedOptions(opts, includePassword);
    const masked = maskUri(savedUri);
    const list = getRecent().filter((entry) => entry.masked !== masked);
    list.unshift({ uri: savedUri, masked, ts: Date.now(), options: savedOptions });
    localStorage.setItem(RECENT_KEY, JSON.stringify(list.slice(0, 5)));
  } catch {}
}
function getProfiles() { try { return JSON.parse(localStorage.getItem(PROFILES_KEY)||'[]'); } catch { return []; } }
function saveProfiles(p) { try { localStorage.setItem(PROFILES_KEY,JSON.stringify(p)); } catch{} }

function maskUri(uri) {
  try { const u=new URL(uri); if(u.password) u.password='••••'; return u.toString(); }
  catch { return uri.replace(/:([^@/]+)@/, ':••••@'); }
}

// Parse a MongoDB URI (including multi-host variants that new URL() cannot handle)
function parseConnectionString(uri) {
  const trimmed = (uri || '').trim();
  if (!trimmed.startsWith('mongodb://') && !trimmed.startsWith('mongodb+srv://')) return null;
  try {
    const afterScheme = trimmed.startsWith('mongodb+srv://') ? trimmed.slice(14) : trimmed.slice(10);
    const atIdx = afterScheme.lastIndexOf('@');
    let userInfo = '', afterAuth = afterScheme;
    if (atIdx !== -1) { userInfo = afterScheme.slice(0, atIdx); afterAuth = afterScheme.slice(atIdx + 1); }
    let username = '', password = '';
    if (userInfo) {
      const ci = userInfo.indexOf(':');
      try {
        username = ci !== -1 ? decodeURIComponent(userInfo.slice(0, ci)) : decodeURIComponent(userInfo);
        if (ci !== -1) password = decodeURIComponent(userInfo.slice(ci + 1));
      } catch {
        username = ci !== -1 ? userInfo.slice(0, ci) : userInfo;
        if (ci !== -1) password = userInfo.slice(ci + 1);
      }
    }
    const si = afterAuth.indexOf('/');
    const pathQ = si !== -1 ? afterAuth.slice(si) : '';
    const qi = pathQ.indexOf('?');
    const queryStr = qi !== -1 ? pathQ.slice(qi + 1) : '';
    const params = {};
    if (queryStr) {
      for (const part of queryStr.split('&')) {
        const ei = part.indexOf('=');
        if (ei !== -1) {
          try { params[decodeURIComponent(part.slice(0, ei))] = decodeURIComponent(part.slice(ei + 1)); } catch {}
        }
      }
    }
    return { username, password, params };
  } catch { return null; }
}

export default function ConnectPage({ onConnect, connecting, error, theme, onToggleTheme }) {
  const [uri, setUri] = useState('');
  const [showUriEditor, setShowUriEditor] = useState(false);
  const [recent, setRecent] = useState([]);
  const [profiles, setProfiles] = useState([]);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [showProfiles, setShowProfiles] = useState(false);
  const [profileName, setProfileName] = useState('');
  const [saveProfilePassword, setSaveProfilePassword] = useState(false);
  const [dontSaveRecentPassword, setDontSaveRecentPassword] = useState(false);
  const [dismissedHints, setDismissedHints] = useState({ topologyError: false, readPreferenceNote: false });
  const [options, setOptions] = useState({ ...DEFAULT_OPTIONS });
  const inputRef = useRef(null);

  useEffect(() => {
    setRecent(getRecent());
    setProfiles(getProfiles());
    setDontSaveRecentPassword(getDontSaveRecentPassword());
    inputRef.current?.focus();
  }, []);
  useEffect(() => {
    if (!options.username.trim() && saveProfilePassword) setSaveProfilePassword(false);
  }, [options.username, saveProfilePassword]);
  useEffect(() => {
    try { localStorage.setItem(RECENT_PASSWORD_PREF_KEY, dontSaveRecentPassword ? '1' : '0'); } catch {}
  }, [dontSaveRecentPassword]);
  useEffect(() => {
    try {
      const raw = sessionStorage.getItem(CONNECT_HINTS_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        setDismissedHints({
          topologyError: Boolean(parsed.topologyError),
          readPreferenceNote: Boolean(parsed.readPreferenceNote),
        });
      }
    } catch {}
  }, []);
  useEffect(() => {
    try { sessionStorage.setItem(CONNECT_HINTS_KEY, JSON.stringify(dismissedHints)); } catch {}
  }, [dismissedHints]);
  useEffect(() => {
    if (!showUriEditor) return undefined;
    const onKeyDown = (event) => {
      if (event.key === 'Escape') setShowUriEditor(false);
    };
    document.addEventListener('keydown', onKeyDown);
    return () => document.removeEventListener('keydown', onKeyDown);
  }, [showUriEditor]);

  const applyUriToOptions = (uriStr) => {
    const parsed = parseConnectionString(uriStr);
    if (!parsed) return;
    const updates = {};
    if (parsed.username) updates.username = parsed.username;
    if (parsed.password) updates.password = parsed.password;
    const p = parsed.params;
    if (p.replicaSet) updates.replicaSet = p.replicaSet;
    if (p.readPreference) updates.readPreference = p.readPreference;
    if (p.authSource) updates.authSource = p.authSource;
    if (p.tls !== undefined) updates.tls = p.tls === 'true';
    if (p.ssl !== undefined) updates.tls = p.ssl === 'true';
    if (p.tlsAllowInvalidCertificates !== undefined) updates.tlsAllowInvalidCertificates = p.tlsAllowInvalidCertificates === 'true';
    if (p.tlsInsecure === 'true') updates.tlsAllowInvalidCertificates = true;
    if (p.directConnection !== undefined) updates.directConnection = p.directConnection === 'true';
    if (p.connectTimeoutMS) updates.connectTimeoutMS = parseInt(p.connectTimeoutMS) || 15000;
    if (Object.keys(updates).length > 0) {
      setOptions(prev => ({ ...prev, ...updates }));
      const hasAdv = p.replicaSet || p.readPreference || p.authSource ||
        p.tls !== undefined || p.ssl !== undefined || p.directConnection !== undefined;
      if (hasAdv) setShowAdvanced(true);
    }
  };

  const handleUriChange = (newUri) => {
    setUri(newUri);
    applyUriToOptions(newUri);
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!uri.trim() || connecting) return;
    const cleanOpts = {};
    if (options.tls !== undefined) cleanOpts.tls = options.tls;
    if (options.tlsAllowInvalidCertificates) cleanOpts.tlsAllowInvalidCertificates = true;
    if (options.authSource) cleanOpts.authSource = options.authSource;
    if (options.replicaSet) cleanOpts.replicaSet = options.replicaSet;
    if (options.directConnection !== undefined) cleanOpts.directConnection = options.directConnection;
    if (options.readPreference) cleanOpts.readPreference = options.readPreference;
    if (options.connectTimeoutMS) cleanOpts.connectTimeoutMS = options.connectTimeoutMS;
    if (options.markAsProduction) cleanOpts.markAsProduction = true;
    if (options.username.trim()) {
      cleanOpts.username = options.username.trim();
      cleanOpts.password = options.password;
    }
    saveRecent(uri.trim(), cleanOpts, !dontSaveRecentPassword);
    onConnect(uri.trim(), cleanOpts);
  };

  const handleDemoConnect = () => {
    if (connecting) return;
    const demoUri = 'mock://demo.local/sample_mflix';
    const demoOptions = { readPreference: 'primary' };
    setUri(demoUri);
    saveRecent(demoUri, demoOptions, false);
    onConnect(demoUri, demoOptions);
  };

  const handleRecent = (item) => {
    const opts = item.options || {};
    setUri(item.uri);
    const savedPassword = typeof opts.password === 'string' ? opts.password : '';
    setOptions({ ...DEFAULT_OPTIONS, ...opts, password: savedPassword });
    applyUriToOptions(item.uri);
    if (opts.username) return;
    onConnect(item.uri, opts);
  };
  const handleProfile = (p) => {
    setUri(p.uri);
    if (p.options) {
      const savedPassword = typeof p.options.password === 'string' ? p.options.password : '';
      setOptions({ ...DEFAULT_OPTIONS, ...p.options, password: savedPassword });
      applyUriToOptions(p.uri);
    }
    setShowProfiles(false);
  };
  const handleSaveProfile = () => {
    if (!profileName.trim() || !uri.trim()) return;
    const includePassword = Boolean(saveProfilePassword && options.username.trim() && options.password);
    const p = [...profiles, { name:profileName.trim(), uri, masked:maskUri(uri), options:sanitizeSavedOptions(options, includePassword), ts:Date.now() }];
    setProfiles(p); saveProfiles(p); setProfileName(''); setSaveProfilePassword(false); setShowProfiles(false);
  };
  const handleDeleteProfile = (i) => { const p = profiles.filter((_,j)=>j!==i); setProfiles(p); saveProfiles(p); };

  const setOpt = (k, v) => setOptions(prev => ({ ...prev, [k]: v }));

  const isTopologyError = error && (error.includes('DirectConnect') || error.includes('server selection') || error.includes('replica set') || error.includes('topology'));

  return (
    <div className="h-screen w-screen relative isolate flex items-start justify-center noise-bg overflow-y-auto overflow-x-hidden py-8 sm:py-10" style={{ background:'var(--surface-0)' }}>
      <div className="fixed inset-0 overflow-hidden pointer-events-none -z-10">
        <div className="absolute top-1/4 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] rounded-full blur-[120px]" style={{background:'var(--accent)',opacity:0.04}} />
        <div className="absolute bottom-0 left-1/4 w-[400px] h-[400px] rounded-full blur-[100px]" style={{background:'var(--accent)',opacity:0.02}} />
        <div className="absolute inset-0 opacity-[0.015]" style={{ backgroundImage:`radial-gradient(circle at 1px 1px, var(--text-primary) 1px, transparent 0)`, backgroundSize:'48px 48px' }} />
      </div>

      {/* Theme toggle */}
      <button onClick={onToggleTheme} className="absolute top-4 right-4 z-20 btn-ghost p-2" title={`Switch to ${theme==='dark'?'light':'dark'} theme`}>
        {theme === 'dark' ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
      </button>

      <div className="relative z-10 w-full max-w-lg px-6">
        {/* Logo */}
        <div className="text-center mb-8 float-in">
          <div className="inline-flex items-center justify-center mb-5">
            <Logo size={48} />
          </div>
          <h1 className="text-3xl font-display font-bold tracking-tight mb-2" style={{color:'var(--text-primary)'}}>
            Mongo<span style={{color:'var(--accent)'}}>Studio</span>
          </h1>
          <p className="text-sm" style={{color:'var(--text-tertiary)'}}>Blazing-fast MongoDB interface</p>
        </div>

        {/* Connection Form */}
        <form onSubmit={handleSubmit} className="float-in float-in-delay-1">
          <div className="rounded-2xl p-6 glow-border" style={{background:'var(--surface-1)', border:'1px solid var(--border)'}}>
            {/* Header with Profiles */}
            <div className="flex items-center justify-between mb-3">
              <label className="block text-xs font-medium uppercase tracking-wider" style={{color:'var(--text-tertiary)'}}>Connection String</label>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => setShowUriEditor(true)}
                  className="inline-flex items-center justify-center gap-1 text-2xs rounded-md border transition-colors w-[86px] h-7"
                  style={{ color:'var(--text-secondary)', borderColor:'var(--border)', background:'var(--surface-2)' }}
                  title="Open multi-line editor"
                >
                  Editor
                </button>
                <button
                  type="button"
                  onClick={()=>setShowProfiles(!showProfiles)}
                  className="inline-flex items-center justify-center gap-1 rounded-md border text-2xs font-medium transition-colors hover:bg-[var(--surface-3)] w-[86px] h-7"
                  style={{
                    color: showProfiles ? 'var(--text-primary)' : 'var(--accent)',
                    borderColor:'var(--border)',
                    background:'var(--surface-2)',
                  }}
                >
                  <span>{showProfiles ? 'Profiles' : 'Saved'}</span>
                  <span className="badge-blue">{profiles.length}</span>
                </button>
              </div>
            </div>

            {/* Profiles dropdown */}
            {showProfiles && (
              <div className="mb-3 rounded-xl p-3 animate-slide-up space-y-1" style={{background:'var(--surface-2)', border:'1px solid var(--border)'}}>
                {profiles.length === 0 ? (
                  <div className="text-2xs text-center py-2" style={{color:'var(--text-tertiary)'}}>No saved profiles</div>
                ) : profiles.map((p, i) => (
                  <div key={i} className="flex items-center gap-2 px-2 py-1.5 rounded-lg group transition-colors hover:bg-[var(--surface-3)]" style={{cursor:'pointer'}} onClick={()=>handleProfile(p)}>
                    <Server className="w-3.5 h-3.5 flex-shrink-0" style={{color:'var(--text-tertiary)'}} />
                    <span className="text-xs font-medium flex-1 truncate" style={{color:'var(--text-secondary)'}}>{p.name}</span>
                    {typeof p.options?.password === 'string' && p.options.password.length > 0 && (
                      <span className="badge-yellow">pwd</span>
                    )}
                    <span className="text-2xs truncate max-w-[140px]" style={{color:'var(--text-tertiary)'}}>{p.masked}</span>
                    <button type="button" onClick={(e)=>{e.stopPropagation();handleDeleteProfile(i)}} className="opacity-0 group-hover:opacity-100 p-1 rounded hover:bg-red-500/10">
                      <Trash className="w-3 h-3 text-red-400" />
                    </button>
                  </div>
                ))}
                <div className="mt-2 pt-2 space-y-2" style={{borderTop:'1px solid var(--border)'}}>
                  <label className="flex items-center gap-2 text-2xs cursor-pointer" style={{color:'var(--text-secondary)'}}>
                    <input
                      type="checkbox"
                      checked={saveProfilePassword}
                      onChange={e=>setSaveProfilePassword(e.target.checked)}
                      disabled={!options.username.trim()}
                      className="ms-checkbox"
                    />
                    Save password in this profile
                  </label>
                  <input type="text" value={profileName} onChange={e=>setProfileName(e.target.value)} placeholder="Profile name…" className="flex-1 text-xs px-2 py-1.5 rounded-lg" style={{background:'var(--surface-3)',border:'1px solid var(--border)',color:'var(--text-primary)'}} />
                  <button type="button" onClick={handleSaveProfile} disabled={!profileName.trim()||!uri.trim()} className="text-2xs px-2 py-1.5 rounded-lg font-medium disabled:opacity-30" style={{color:'var(--accent)'}}>Save Current</button>
                </div>
              </div>
            )}

            {/* URI Input */}
            <div className="relative">
              <input ref={inputRef} type="text" value={uri} onChange={e=>handleUriChange(e.target.value)}
                placeholder="mongodb://localhost:27017" spellCheck={false} autoComplete="off"
                className="w-full rounded-lg pl-6 pr-14 py-3 h-12 text-sm font-mono transition-all duration-150"
                style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}} />
              <button type="submit" disabled={!uri.trim()||connecting}
                className="absolute right-1.5 top-1/2 -translate-y-1/2 w-9 h-9 rounded-lg flex items-center justify-center disabled:opacity-30 disabled:cursor-not-allowed active:scale-95 transition-all"
                style={{background:'var(--accent)',color:'var(--surface-0)'}}>
                {connecting ? <Loader style={{color:'var(--surface-0)'}} /> : <Zap className="w-4 h-4" />}
              </button>
            </div>
            <div className="mt-1 flex items-center justify-between gap-2">
              <div className="text-2xs" style={{color:'var(--text-tertiary)'}}>
                Advanced options auto-fill from URI params on paste.
              </div>
              <button
                type="button"
                onClick={handleDemoConnect}
                disabled={connecting}
                className="text-2xs px-2 py-1 rounded-md border disabled:opacity-50"
                style={{ color:'var(--accent)', borderColor:'var(--border)', background:'var(--surface-2)' }}
                title="Open demo mode without real MongoDB"
              >
                Try Demo
              </button>
            </div>

            {/* Username / Password */}
            <div className="mt-3 grid grid-cols-2 gap-2">
              <div>
                <label className="block text-2xs mb-1" style={{color:'var(--text-tertiary)'}}>Username</label>
                <input
                  type="text"
                  value={options.username}
                  onChange={e=>setOpt('username',e.target.value)}
                  placeholder="mongo_user"
                  autoComplete="username"
                  className="w-full text-xs px-2.5 py-1.5 rounded-lg font-mono"
                  style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}}
                />
              </div>
              <div>
                <label className="block text-2xs mb-1" style={{color:'var(--text-tertiary)'}}>Password</label>
                <input
                  type="password"
                  value={options.password}
                  onChange={e=>setOpt('password',e.target.value)}
                  placeholder="••••••••"
                  autoComplete="current-password"
                  className="w-full text-xs px-2.5 py-1.5 rounded-lg font-mono"
                  style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}}
                />
              </div>
            </div>
            <div className="mt-1 text-2xs" style={{color:'var(--text-tertiary)'}}>
              Recent keeps password by default. Turn it off below if needed. Profiles still keep password only when explicitly enabled.
            </div>
            <label className="mt-1.5 flex items-center gap-2 text-2xs cursor-pointer" style={{color:'var(--text-secondary)'}}>
              <input
                type="checkbox"
                checked={dontSaveRecentPassword}
                onChange={(event) => setDontSaveRecentPassword(event.target.checked)}
                className="ms-checkbox"
              />
              Don't save password in Recent
            </label>

            {/* Error */}
            {error && (
              <div className="mt-3 animate-fade-in">
                <div className="flex items-start gap-2 text-red-400 text-xs">
                  <AlertCircle className="w-3.5 h-3.5 mt-0.5 flex-shrink-0" />
                  <span className="flex-1">{error}</span>
                </div>
                {isTopologyError && !dismissedHints.topologyError && (
                  <div className="mt-2 text-2xs px-2 py-1.5 pr-8 rounded-lg relative" style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-tertiary)'}}>
                    <span className="block">
                      <strong style={{color:'var(--text-secondary)'}}>Hint:</strong> For replica sets, all member hostnames must resolve from this machine.
                      If not, enable <strong>DirectConnect</strong> and connect to one specific node (for example, a single primary or single secondary).
                    </span>
                    <button
                      type="button"
                      className="absolute top-0.5 right-0.5 p-1 rounded-md hover:bg-[var(--surface-3)]"
                      onClick={() => setDismissedHints((prev) => ({ ...prev, topologyError: true }))}
                      title="Hide hint for this session"
                    >
                      <X className="w-3 h-3" style={{ color:'var(--text-tertiary)' }} />
                    </button>
                  </div>
                )}
              </div>
            )}

            {/* Advanced Options Toggle */}
            <button type="button" onClick={()=>setShowAdvanced(!showAdvanced)}
              className="mt-4 flex items-center gap-1.5 text-2xs font-medium transition-colors" style={{color:'var(--text-tertiary)'}}>
              <Settings className="w-3 h-3" />
              Advanced Options
              <ChevronDown className={`w-3 h-3 transition-transform ${showAdvanced?'rotate-180':''}`} />
            </button>

            {/* Advanced Options */}
            {showAdvanced && (
              <div className="mt-3 grid grid-cols-2 gap-2 animate-slide-up">
                <div>
                  <label className="block text-2xs mb-1" style={{color:'var(--text-tertiary)'}}>Auth Source</label>
                  <input type="text" value={options.authSource} onChange={e=>setOpt('authSource',e.target.value)} placeholder="admin"
                    className="w-full text-xs px-2.5 py-1.5 rounded-lg font-mono" style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}} />
                </div>
                <div>
                  <label className="block text-2xs mb-1" style={{color:'var(--text-tertiary)'}}>Replica Set Name</label>
                  <input type="text" value={options.replicaSet} onChange={e=>setOpt('replicaSet',e.target.value)} placeholder="rs0"
                    className="w-full text-xs px-2.5 py-1.5 rounded-lg font-mono" style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}} />
                </div>
                <div>
                  <label className="block text-2xs mb-1" style={{color:'var(--text-tertiary)'}}>
                    Read Preference
                  </label>
                  <select value={options.readPreference||''} onChange={e=>setOpt('readPreference',e.target.value)}
                    className="ms-select w-full text-xs">
                    <option value="">Default (primary)</option>
                    <option value="primaryPreferred">Primary Preferred</option>
                    <option value="secondary">Secondary</option>
                    <option value="secondaryPreferred">Secondary Preferred</option>
                    <option value="nearest">Nearest</option>
                  </select>
                </div>
                <div>
                  <label className="block text-2xs mb-1" style={{color:'var(--text-tertiary)'}}>Timeout (ms)</label>
                  <input type="number" value={options.connectTimeoutMS} onChange={e=>setOpt('connectTimeoutMS',parseInt(e.target.value)||15000)} placeholder="15000"
                    className="w-full text-xs px-2.5 py-1.5 rounded-lg font-mono" style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-primary)'}} />
                </div>
                <div className="col-span-2 flex flex-wrap items-center gap-x-4 gap-y-2 mt-1">
                  <label className="flex items-center gap-2 text-2xs cursor-pointer" style={{color:'var(--text-secondary)'}}>
                    <input type="checkbox" checked={options.tls===true} onChange={e=>setOpt('tls',e.target.checked?true:undefined)} className="ms-checkbox" />
                    <Lock className="w-3 h-3" /> TLS/SSL
                  </label>
                  <label className="flex items-center gap-2 text-2xs cursor-pointer" style={{color:'var(--text-secondary)'}}>
                    <input type="checkbox" checked={options.tlsAllowInvalidCertificates} onChange={e=>setOpt('tlsAllowInvalidCertificates',e.target.checked)} className="ms-checkbox" />
                    Allow Invalid Certs
                  </label>
                  <label
                    className="flex items-center gap-2 text-2xs cursor-pointer"
                    style={{color:'var(--text-secondary)'}}
                    title="DirectConnect: skip replica set discovery and connect directly to this one node. Required if RS member hostnames are not reachable."
                  >
                    <input type="checkbox" checked={options.directConnection===true} onChange={e=>setOpt('directConnection',e.target.checked?true:undefined)} className="ms-checkbox" />
                    <Globe className="w-3 h-3" /> DirectConnect
                  </label>
                  <label className="flex items-center gap-2 text-2xs cursor-pointer" style={{color:'var(--text-secondary)'}}>
                    <input type="checkbox" checked={options.markAsProduction===true} onChange={e=>setOpt('markAsProduction',e.target.checked)} className="ms-checkbox" />
                    Mark as production
                  </label>
                </div>
                {options.readPreference && options.readPreference !== 'primary' && !options.directConnection && !dismissedHints.readPreferenceNote && (
                  <div className="col-span-2 text-2xs px-2 py-1.5 pr-8 rounded-lg relative" style={{background:'var(--surface-2)',border:'1px solid var(--border)',color:'var(--text-tertiary)'}}>
                    <span className="block">
                    <strong style={{color:'var(--text-secondary)'}}>Note:</strong> Read preference affects where READ operations are sent by the driver.
                    When connecting to a replica set, the topology always reports the primary — this is normal.
	                    {options.readPreference === 'secondary' || options.readPreference === 'secondaryPreferred'
	                      ? ' To connect directly to a secondary node, use DirectConnect with a single-host URI.'
	                      : ''}
	                    </span>
	                    <button
	                      type="button"
		                      className="absolute top-0.5 right-0.5 p-1 rounded-md hover:bg-[var(--surface-3)]"
	                      onClick={() => setDismissedHints((prev) => ({ ...prev, readPreferenceNote: true }))}
	                      title="Hide hint for this session"
	                    >
	                      <X className="w-3 h-3" style={{ color:'var(--text-tertiary)' }} />
	                    </button>
	                  </div>
	                )}
              </div>
            )}

            {/* Footer badges */}
            <div className="mt-4 flex items-center gap-3 text-2xs" style={{color:'var(--text-tertiary)'}}>
              <div className="flex items-center gap-1.5"><div className="w-1.5 h-1.5 rounded-full" style={{background:'var(--accent)',opacity:0.5}} />MongoDB 3.6 → 8.x</div>
              <div className="w-px h-3" style={{background:'var(--border)'}} />
              <div className="flex items-center gap-1.5"><div className="w-1.5 h-1.5 rounded-full" style={{background:'var(--accent)',opacity:0.5}} />Atlas & Self-hosted</div>
              <div className="w-px h-3" style={{background:'var(--border)'}} />
              <div className="flex items-center gap-1.5"><div className="w-1.5 h-1.5 rounded-full" style={{background:'var(--accent)',opacity:0.5}} />Replica Sets & Sharded</div>
            </div>
          </div>
        </form>

        {showUriEditor && (
          <div className="fixed inset-0 z-[200] flex items-center justify-center p-4">
            <button
              type="button"
              className="absolute inset-0 bg-black/50"
              onClick={() => setShowUriEditor(false)}
              aria-label="Close editor overlay"
            />
            <div className="relative w-full max-w-3xl rounded-2xl p-4" style={{ background:'var(--surface-1)', border:'1px solid var(--border)' }}>
              <div className="flex items-center justify-between mb-3">
                <div className="text-xs font-semibold uppercase tracking-wider" style={{ color:'var(--text-tertiary)' }}>
                  Connection String Editor
                </div>
                <button type="button" onClick={() => setShowUriEditor(false)} className="btn-ghost p-1.5">
                  <X className="w-3.5 h-3.5" />
                </button>
              </div>
              <textarea
                value={uri}
                onChange={(event) => handleUriChange(event.target.value)}
                spellCheck={false}
                className="w-full rounded-lg px-3 py-2 text-xs font-mono focus:outline-none"
                style={{
                  background:'var(--surface-2)',
                  border:'1px solid var(--border)',
                  color:'var(--text-primary)',
                  minHeight:'180px',
                  resize:'both',
                }}
              />
              <div className="mt-3 flex items-center justify-end">
                <button type="button" className="btn-ghost text-xs" onClick={() => setShowUriEditor(false)}>
                  Done
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Recent */}
        {recent.length > 0 && (
          <div className="mt-5 float-in float-in-delay-2">
            <div className="text-xs font-medium uppercase tracking-wider mb-2 px-1" style={{color:'var(--text-tertiary)'}}>Recent</div>
            <div className="space-y-1">
              {recent.map((item, i) => (
                <button key={i} onClick={()=>handleRecent(item)}
                  className="w-full text-left px-3 py-2.5 rounded-xl transition-all duration-150 group flex items-center gap-3"
                  style={{background:'var(--surface-1)',border:'1px solid var(--border)',opacity:0.7}}
                  onMouseOver={e=>{e.currentTarget.style.opacity='1';e.currentTarget.style.borderColor='var(--border-hover)'}}
                  onMouseOut={e=>{e.currentTarget.style.opacity='0.7';e.currentTarget.style.borderColor='var(--border)'}}>
                  <Server className="w-4 h-4" style={{color:'var(--text-tertiary)'}} />
                  <span className="text-sm font-mono truncate" style={{color:'var(--text-secondary)'}}>{item.masked}</span>
                </button>
              ))}
            </div>
          </div>
        )}

        <div className="mt-8 text-center float-in float-in-delay-3">
          <p className="text-2xs" style={{color:'var(--text-tertiary)'}}>Open Source — MIT License — v2.5.0</p>
        </div>
      </div>
    </div>
  );
}

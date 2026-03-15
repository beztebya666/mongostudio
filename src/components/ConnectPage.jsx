import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { createPortal } from 'react-dom';
import api from '../utils/api';
import { Logo, Zap, Server, Loader, Moon, Sun, ChevronDown, Lock, Globe, Settings, Trash, Edit, X, AlertTriangle, Refresh, Plus, Info, Key, Shield, Wifi, Upload, Download } from './Icons';
import InlineAlert from './InlineAlert';
import AppModal from './modals/AppModal';
import DropdownSelect from './DropdownSelect';
import ToastNotice from './ToastNotice';
import { genId } from '../utils/genId';

// ─── Constants ─────────────────────────────────────────────────────────────
const RECENT_KEY = 'mongostudio_recent';
const PROFILES_KEY = 'mongostudio_profiles';
const RECENT_PASSWORD_PREF_KEY = 'mongostudio_recent_dont_save_password';
const CONNECT_HINTS_KEY = 'mongostudio_connect_hints';

const AUTH_METHODS = [
  { value: 'none', label: 'None' },
  { value: 'password', label: 'Username/Password' },
  { value: 'x509', label: 'X.509' },
  { value: 'kerberos', label: 'Kerberos' },
  { value: 'ldap', label: 'LDAP' },
  { value: 'aws', label: 'AWS IAM' },
  { value: 'oidc', label: 'OIDC (Preview)' },
];

const AUTH_MECHANISMS = [
  { value: '', label: 'Default' },
  { value: 'SCRAM-SHA-1', label: 'SCRAM-SHA-1' },
  { value: 'SCRAM-SHA-256', label: 'SCRAM-SHA-256' },
];

const TLS_MODES = [
  { value: 'default', label: 'Default' },
  { value: 'on', label: 'On' },
  { value: 'off', label: 'Off' },
];

const PROXY_METHODS = [
  { value: 'none', label: 'None' },
  { value: 'ssh-password', label: 'SSH with Password' },
  { value: 'ssh-identity', label: 'SSH with Identity File' },
  { value: 'socks5', label: 'Socks5' },
];

const READ_PREFERENCES = [
  { value: '', label: 'Default' },
  { value: 'primary', label: 'Primary' },
  { value: 'primaryPreferred', label: 'Primary Preferred' },
  { value: 'secondary', label: 'Secondary' },
  { value: 'secondaryPreferred', label: 'Secondary Preferred' },
  { value: 'nearest', label: 'Nearest' },
];

const URI_OPTION_GROUPS = [
  {
    label: 'CONNECTION TIMEOUT OPTIONS',
    options: ['connectTimeoutMS', 'socketTimeoutMS'],
  },
  {
    label: 'COMPRESSION OPTIONS',
    options: ['compressors', 'zlibCompressionLevel'],
  },
  {
    label: 'CONNECTION POOL OPTIONS',
    options: ['maxPoolSize', 'minPoolSize', 'maxIdleTimeMS', 'waitQueueMultiple', 'waitQueueTimeoutMS'],
  },
  {
    label: 'WRITE CONCERN OPTIONS',
    options: ['w', 'wtimeoutMS', 'journal'],
  },
  {
    label: 'READ CONCERN OPTIONS',
    options: ['readConcernLevel'],
  },
  {
    label: 'READ PREFERENCES OPTIONS',
    options: ['maxStalenessSeconds', 'readPreferenceTags'],
  },
  {
    label: 'SERVER OPTIONS',
    options: ['localThresholdMS', 'serverSelectionTimeoutMS', 'serverSelectionTryOnce', 'heartbeatFrequencyMS'],
  },
  {
    label: 'MISCELLANEOUS CONFIGURATION',
    options: ['appName', 'retryReads', 'retryWrites', 'srvMaxHosts', 'uuidRepresentation', 'enableUtf8Validation'],
  },
];

const ALL_URI_OPTIONS = URI_OPTION_GROUPS.flatMap((g) => g.options);

const DEFAULT_OPTIONS = {
  // General
  scheme: 'mongodb',
  hosts: ['localhost:27017'],
  directConnection: undefined,
  // Authentication
  authMethod: 'none',
  username: '',
  password: '',
  authSource: '',
  authMechanism: '',
  // Kerberos
  kerberosServiceName: '',
  kerberosCanonicalizeHostname: false,
  kerberosServiceRealm: '',
  // AWS IAM
  awsAccessKeyId: '',
  awsSecretAccessKey: '',
  awsSessionToken: '',
  // TLS/SSL
  tlsMode: 'default',
  tlsCAFile: '',
  tlsUseSystemCA: false,
  tlsCertificateKeyFile: '',
  tlsCertificateKeyFilePassword: '',
  tlsInsecure: false,
  tlsAllowInvalidHostnames: false,
  tlsAllowInvalidCertificates: false,
  // Proxy/SSH
  proxyMethod: 'none',
  sshHost: '',
  sshPort: 22,
  sshUsername: '',
  sshPassword: '',
  sshIdentityFile: '',
  sshIdentityFilePassphrase: '',
  socks5Host: '',
  socks5Port: 1080,
  socks5Username: '',
  socks5Password: '',
  // Advanced
  readPreference: '',
  replicaSet: '',
  defaultAuthDb: '',
  uriOptions: [],
  // Meta
  markAsProduction: false,
  connectTimeoutMS: 15000,
};

// ─── Helpers ─────────────────────────────────────────────────────────────

function getRecent() { try { return JSON.parse(localStorage.getItem(RECENT_KEY) || '[]').slice(0, 5); } catch { return []; } }
function getDontSaveRecentPassword() { try { return localStorage.getItem(RECENT_PASSWORD_PREF_KEY) === '1'; } catch { return false; } }
function getProfiles() { try { return JSON.parse(localStorage.getItem(PROFILES_KEY) || '[]'); } catch { return []; } }
function saveProfiles(p) { try { localStorage.setItem(PROFILES_KEY, JSON.stringify(p)); } catch {} }

function sanitizeSavedOptions(opts = {}, includePassword = false) {
  if (includePassword) return { ...opts };
  const { password, awsSecretAccessKey, sshPassword, sshIdentityFilePassphrase, socks5Password, tlsCertificateKeyFilePassword, ...rest } = opts;
  return rest;
}

function stripPasswordFromUri(uri) {
  try {
    return uri.replace(/(mongodb(?:\+srv)?:\/\/[^:/@]+):[^@]*@/i, '$1@');
  } catch {
    return uri;
  }
}

function maskUri(uri) {
  try { const u = new URL(uri); if (u.password) u.password = '••••'; return u.toString(); }
  catch { return uri.replace(/:([^@/]+)@/, ':••••@'); }
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

function parseConnectionString(uri) {
  const trimmed = (uri || '').trim();
  if (!trimmed.startsWith('mongodb://') && !trimmed.startsWith('mongodb+srv://')) return null;
  try {
    const isSrv = trimmed.startsWith('mongodb+srv://');
    const scheme = isSrv ? 'mongodb+srv' : 'mongodb';
    const afterScheme = isSrv ? trimmed.slice(14) : trimmed.slice(10);
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
    const hostsPart = si !== -1 ? afterAuth.slice(0, si) : afterAuth;
    const hosts = hostsPart.split(',').map((h) => h.trim()).filter(Boolean);
    const pathQ = si !== -1 ? afterAuth.slice(si) : '';
    const dbPart = pathQ.startsWith('/') ? pathQ.slice(1).split('?')[0] : '';
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
    return { scheme, username, password, hosts, db: dbPart, params };
  } catch { return null; }
}

function buildUriFromOptions(opts) {
  const scheme = opts.scheme === 'mongodb+srv' ? 'mongodb+srv' : 'mongodb';
  let uri = scheme + '://';
  // Credentials
  const hasPasswordAuth = opts.authMethod === 'password' && opts.username;
  const hasAwsAuth = opts.authMethod === 'aws' && opts.awsAccessKeyId;
  if (hasPasswordAuth) {
    uri += encodeURIComponent(opts.username);
    if (opts.password) uri += ':' + encodeURIComponent(opts.password);
    uri += '@';
  } else if (hasAwsAuth) {
    uri += encodeURIComponent(opts.awsAccessKeyId);
    if (opts.awsSecretAccessKey) uri += ':' + encodeURIComponent(opts.awsSecretAccessKey);
    uri += '@';
  }
  // Hosts
  const hosts = (opts.hosts || ['localhost:27017']).filter(Boolean);
  uri += hosts.join(',');
  // Path
  uri += '/';
  if (opts.defaultAuthDb) uri += encodeURIComponent(opts.defaultAuthDb);
  // Query params
  const params = [];
  if (opts.authSource) params.push('authSource=' + encodeURIComponent(opts.authSource));
  if (opts.authMechanism) params.push('authMechanism=' + encodeURIComponent(opts.authMechanism));
  if (opts.authMethod === 'x509') params.push('authMechanism=MONGODB-X509');
  if (opts.authMethod === 'kerberos') {
    params.push('authMechanism=GSSAPI');
    if (opts.kerberosServiceName) params.push('authMechanismProperties=SERVICE_NAME:' + encodeURIComponent(opts.kerberosServiceName));
  }
  if (opts.authMethod === 'ldap') params.push('authMechanism=PLAIN');
  if (opts.authMethod === 'aws') {
    params.push('authMechanism=MONGODB-AWS');
    if (opts.awsSessionToken) params.push('authMechanismProperties=AWS_SESSION_TOKEN:' + encodeURIComponent(opts.awsSessionToken));
  }
  if (opts.authMethod === 'oidc') {
    params.push('authMechanism=MONGODB-OIDC');
  }
  if (opts.replicaSet) params.push('replicaSet=' + encodeURIComponent(opts.replicaSet));
  if (opts.readPreference) params.push('readPreference=' + encodeURIComponent(opts.readPreference));
  if (opts.directConnection === true && scheme !== 'mongodb+srv') params.push('directConnection=true');
  if (opts.tlsMode === 'on') params.push('tls=true');
  else if (opts.tlsMode === 'off') params.push('tls=false');
  if (opts.tlsCAFile) params.push('tlsCAFile=' + encodeURIComponent(opts.tlsCAFile));
  if (opts.tlsCertificateKeyFile) params.push('tlsCertificateKeyFile=' + encodeURIComponent(opts.tlsCertificateKeyFile));
  if (opts.tlsAllowInvalidCertificates) params.push('tlsAllowInvalidCertificates=true');
  if (opts.tlsAllowInvalidHostnames) params.push('tlsAllowInvalidHostnames=true');
  if (opts.tlsInsecure) params.push('tlsInsecure=true');
  if (opts.connectTimeoutMS && opts.connectTimeoutMS !== 15000) params.push('connectTimeoutMS=' + opts.connectTimeoutMS);
  // URI options from Advanced tab
  for (const { key, value } of (opts.uriOptions || [])) {
    if (key && value !== undefined && value !== '') {
      params.push(encodeURIComponent(key) + '=' + encodeURIComponent(value));
    }
  }
  if (params.length > 0) uri += '?' + params.join('&');
  return uri;
}

function parseUriToOptions(uri) {
  const parsed = parseConnectionString(uri);
  if (!parsed) return null;
  const updates = {};
  updates.scheme = parsed.scheme || 'mongodb';
  if (parsed.hosts?.length > 0) updates.hosts = parsed.hosts;
  if (parsed.db) updates.defaultAuthDb = parsed.db;
  if (parsed.username) {
    updates.username = parsed.username;
    updates.authMethod = 'password';
  }
  if (parsed.password) updates.password = parsed.password;
  const p = parsed.params || {};
  if (p.replicaSet) updates.replicaSet = p.replicaSet;
  if (p.readPreference) updates.readPreference = p.readPreference;
  if (p.authSource) updates.authSource = p.authSource;
  if (p.authMechanism) {
    const mech = p.authMechanism.toUpperCase();
    if (mech === 'MONGODB-X509') updates.authMethod = 'x509';
    else if (mech === 'GSSAPI') updates.authMethod = 'kerberos';
    else if (mech === 'PLAIN') updates.authMethod = 'ldap';
    else if (mech === 'MONGODB-AWS') updates.authMethod = 'aws';
    else if (mech === 'MONGODB-OIDC') updates.authMethod = 'oidc';
    else updates.authMechanism = p.authMechanism;
  }
  if (p.tls !== undefined || p.ssl !== undefined) {
    const val = (p.tls || p.ssl || '').toLowerCase();
    updates.tlsMode = val === 'true' ? 'on' : val === 'false' ? 'off' : 'default';
  }
  if (p.tlsAllowInvalidCertificates === 'true') updates.tlsAllowInvalidCertificates = true;
  if (p.tlsAllowInvalidHostnames === 'true') updates.tlsAllowInvalidHostnames = true;
  if (p.tlsInsecure === 'true') updates.tlsInsecure = true;
  if (p.tlsCAFile) updates.tlsCAFile = p.tlsCAFile;
  if (p.tlsCertificateKeyFile) updates.tlsCertificateKeyFile = p.tlsCertificateKeyFile;
  if (p.directConnection !== undefined) updates.directConnection = p.directConnection === 'true';
  if (p.connectTimeoutMS) updates.connectTimeoutMS = parseInt(p.connectTimeoutMS) || 15000;
  // Collect unknown params as uriOptions
  const knownKeys = new Set([
    'replicaSet', 'readPreference', 'authSource', 'authMechanism', 'authMechanismProperties',
    'tls', 'ssl', 'tlsAllowInvalidCertificates', 'tlsAllowInvalidHostnames', 'tlsInsecure',
    'tlsCAFile', 'tlsCertificateKeyFile', 'directConnection', 'connectTimeoutMS',
  ]);
  const extraOptions = [];
  for (const [key, value] of Object.entries(p)) {
    if (!knownKeys.has(key)) extraOptions.push({ key, value });
  }
  if (extraOptions.length > 0) updates.uriOptions = extraOptions;
  return updates;
}

function parseUriRoutingFacts(uri) {
  const text = String(uri || '').trim();
  const isSrv = text.startsWith('mongodb+srv://');
  if (!text.startsWith('mongodb://') && !isSrv) return { isSrv: false, hostCount: 0, hasReplicaSetParam: false };
  try {
    const afterScheme = isSrv ? text.slice(14) : text.slice(10);
    const atIdx = afterScheme.lastIndexOf('@');
    const afterAuth = atIdx === -1 ? afterScheme : afterScheme.slice(atIdx + 1);
    const slashIdx = afterAuth.indexOf('/');
    const hostsPart = slashIdx === -1 ? afterAuth : afterAuth.slice(0, slashIdx);
    const hosts = hostsPart.split(',').map((e) => e.trim()).filter(Boolean);
    const parsed = parseConnectionString(text);
    return { isSrv, hostCount: hosts.length, hasReplicaSetParam: Boolean(parsed?.params?.replicaSet) };
  } catch {
    return { isSrv, hostCount: 0, hasReplicaSetParam: false };
  }
}

// ─── Reusable sub-components ──────────────────────────────────────────────

function SegmentedButtons({ options, value, onChange, size = 'sm' }) {
  const textSize = size === 'xs' ? 'text-2xs' : 'text-xs';
  const pad = size === 'xs' ? 'px-2.5 py-1.5' : 'px-3.5 py-2';
  return (
    <div className="inline-flex rounded-lg overflow-hidden" style={{ border: '1px solid var(--border)' }}>
      {options.map((opt, i) => {
        const active = value === opt.value;
        return (
          <button
            key={opt.value}
            type="button"
            onClick={() => onChange(opt.value)}
            className={`${textSize} font-medium ${pad} transition-all whitespace-nowrap`}
            style={{
              background: active ? 'var(--accent)' : 'var(--surface-2)',
              color: active ? 'var(--surface-0)' : 'var(--text-secondary)',
              borderRight: i < options.length - 1 ? '1px solid var(--border)' : 'none',
            }}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}

function FormField({ label, hint, children, optional }) {
  return (
    <div>
      <label className="block text-xs font-medium mb-1.5" style={{ color: 'var(--text-secondary)' }}>
        {label}
        {optional && <span className="ml-1 text-2xs font-normal" style={{ color: 'var(--text-tertiary)' }}>Optional</span>}
      </label>
      {children}
      {hint && <p className="text-2xs mt-1" style={{ color: 'var(--text-tertiary)' }}>{hint}</p>}
    </div>
  );
}

function TextInput({ value, onChange, placeholder, type = 'text', mono, className = '', ...rest }) {
  return (
    <input
      type={type}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      className={`w-full text-xs px-3 py-2 rounded-lg ${mono ? 'font-mono' : ''} ${className}`}
      style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)' }}
      {...rest}
    />
  );
}

function CheckboxField({ label, hint, checked, onChange, disabled }) {
  return (
    <div>
      <label className={`flex items-center gap-2 cursor-pointer ${disabled ? 'opacity-50' : ''}`}>
        <input
          type="checkbox"
          checked={checked}
          onChange={(e) => onChange(e.target.checked)}
          disabled={disabled}
          className="ms-checkbox"
        />
        <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>{label}</span>
      </label>
      {hint && <p className="text-2xs mt-0.5 ml-6" style={{ color: 'var(--text-tertiary)' }}>{hint}</p>}
    </div>
  );
}

function GroupedSelect({ value, groups, disabledKeys, onChange, placeholder = 'Select key' }) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);
  const menuRef = useRef(null);
  const [pos, setPos] = useState({ top: 0, left: 0, width: 0, ready: false, above: false });

  useEffect(() => {
    if (!open) return;
    const update = () => {
      if (!rootRef.current) return;
      const r = rootRef.current.getBoundingClientRect();
      const spaceBelow = window.innerHeight - r.bottom;
      const above = spaceBelow < 280 && r.top > 280;
      setPos({ top: above ? r.top : r.bottom + 2, left: r.left, width: Math.max(r.width, 220), ready: true, above });
    };
    update();
    const close = (e) => {
      if (rootRef.current?.contains(e.target) || menuRef.current?.contains(e.target)) return;
      setOpen(false);
    };
    const esc = (e) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', close);
    document.addEventListener('keydown', esc);
    window.addEventListener('scroll', update, true);
    return () => { document.removeEventListener('mousedown', close); document.removeEventListener('keydown', esc); window.removeEventListener('scroll', update, true); };
  }, [open]);

  const label = value || placeholder;
  return (
    <div ref={rootRef} className="relative">
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between gap-1.5 text-xs px-3 py-2 rounded-lg transition-colors font-mono"
        style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: value ? 'var(--text-primary)' : 'var(--text-tertiary)' }}
      >
        <span className="truncate">{label}</span>
        <ChevronDown className={`w-3 h-3 flex-shrink-0 transition-transform ${open ? 'rotate-180' : ''}`} style={{ color: 'var(--text-tertiary)' }} />
      </button>
      {open && typeof document !== 'undefined' && createPortal(
        <div
          ref={menuRef}
          className="rounded-lg p-1 overflow-auto"
          style={{
            position: 'fixed', top: pos.above ? 'auto' : pos.top, bottom: pos.above ? (window.innerHeight - pos.top + 2) : 'auto',
            left: pos.left, width: pos.width, maxHeight: 280,
            zIndex: 350, opacity: pos.ready ? 1 : 0,
            background: 'var(--surface-3)', border: '1px solid var(--border)', boxShadow: '0 12px 28px rgba(0,0,0,0.35)',
          }}
        >
          {groups.map((group) => (
            <div key={group.label}>
              <div className="text-2xs font-semibold uppercase tracking-wider px-2.5 pt-2 pb-1" style={{ color: 'var(--text-tertiary)' }}>
                {group.label}
              </div>
              {group.options.map((key) => {
                const isDisabled = disabledKeys?.has(key) && key !== value;
                const isActive = key === value;
                return (
                  <button
                    key={key}
                    type="button"
                    disabled={isDisabled}
                    onClick={() => { onChange(key); setOpen(false); }}
                    className="w-full text-left text-xs font-mono px-2.5 py-1.5 rounded-md transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                    style={{
                      background: isActive ? 'var(--surface-4)' : 'transparent',
                      color: isDisabled ? 'var(--text-tertiary)' : (isActive ? 'var(--text-primary)' : 'var(--text-secondary)'),
                    }}
                  >
                    {key}
                  </button>
                );
              })}
            </div>
          ))}
        </div>,
        document.body,
      )}
    </div>
  );
}

function FilePathInput({ value, onChange, placeholder, onFileLoad, fileLoaded, accept = '.pem,.crt,.key,.cer,.der' }) {
  const fileRef = useRef(null);
  const handleFile = (e) => {
    const file = e.target.files?.[0];
    if (!file || !onFileLoad) return;
    const reader = new FileReader();
    reader.onload = () => {
      onFileLoad(reader.result, file.name);
    };
    reader.readAsText(file);
    e.target.value = '';
  };
  return (
    <div className="flex items-center gap-1.5">
      <TextInput value={value} onChange={onChange} placeholder={placeholder} mono className="flex-1" />
      {onFileLoad && (
        <>
          <input ref={fileRef} type="file" accept={accept} onChange={handleFile} className="hidden" />
          <button
            type="button"
            onClick={() => fileRef.current?.click()}
            className="flex-shrink-0 p-2 rounded-lg transition-colors"
            style={{
              background: fileLoaded ? 'rgba(0,237,100,0.1)' : 'var(--surface-2)',
              border: `1px solid ${fileLoaded ? 'rgba(0,237,100,0.3)' : 'var(--border)'}`,
              color: fileLoaded ? 'var(--accent)' : 'var(--text-tertiary)',
            }}
            title={fileLoaded ? 'Certificate loaded — click to replace' : 'Upload certificate file'}
          >
            <Upload className="w-3.5 h-3.5" />
          </button>
        </>
      )}
    </div>
  );
}


// ─── Main Component ───────────────────────────────────────────────────────

export default function ConnectPage({ onConnect, connecting, error, theme, onToggleTheme }) {
  const [uri, setUri] = useState('mongodb://localhost:27017/');
  const [editUri, setEditUri] = useState(true);
  const [options, setOptions] = useState({ ...DEFAULT_OPTIONS });
  const [activeTab, setActiveTab] = useState('general');
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [recent, setRecent] = useState([]);
  const [profiles, setProfiles] = useState([]);
  const [showProfiles, setShowProfiles] = useState(false);
  const [profileName, setProfileName] = useState('');
  const [editingProfileIndex, setEditingProfileIndex] = useState(null);
  const [saveProfilePassword, setSaveProfilePassword] = useState(false);
  const [saveProfileAdminKey, setSaveProfileAdminKey] = useState(false);
  const [dontSaveRecentPassword, setDontSaveRecentPassword] = useState(false);
  const [dismissedHints, setDismissedHints] = useState({ topologyError: false, readPreferenceNote: false, productionReadPreference: false });
  const [pendingConnectConfirm, setPendingConnectConfirm] = useState(null);
  const [toasts, setToasts] = useState([]);
  const [demoResetting, setDemoResetting] = useState(false);
  const [adminKeyConfigured, setAdminKeyConfigured] = useState(null); // null = loading, true/false
  const [adminKeyInput, setAdminKeyInput] = useState('');
  const [adminKeySaved, setAdminKeySaved] = useState(() => {
    try { return Boolean(localStorage.getItem('mongostudio_admin_key')); } catch { return false; }
  });
  const uriSyncRef = useRef(false);
  const inputRef = useRef(null);
  const canStoreProfilePassword = Boolean(options.username?.trim() || options.awsAccessKeyId);

  const dismissToast = useCallback((id) => {
    setToasts((prev) => prev.filter((item) => item.id !== id));
  }, []);

  const pushToast = useCallback((kind, message) => {
    if (!message) return;
    const id = genId();
    setToasts((prev) => {
      const next = [...prev, { id, kind, message: String(message) }];
      return next.slice(-4);
    });
  }, []);

  // ─── Init ───
  useEffect(() => {
    setRecent(getRecent());
    setProfiles(getProfiles());
    setDontSaveRecentPassword(getDontSaveRecentPassword());
    inputRef.current?.focus();
    api.getAdminAccessStatus()
      .then((r) => setAdminKeyConfigured(Boolean(r?.configured)))
      .catch(() => setAdminKeyConfigured(false));
  }, []);
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
          productionReadPreference: Boolean(parsed.productionReadPreference),
        });
      }
    } catch {}
  }, []);
  useEffect(() => {
    try { sessionStorage.setItem(CONNECT_HINTS_KEY, JSON.stringify(dismissedHints)); } catch {}
  }, [dismissedHints]);
  useEffect(() => {
    if (!canStoreProfilePassword && saveProfilePassword) setSaveProfilePassword(false);
  }, [canStoreProfilePassword, saveProfilePassword]);

  // ─── URI ↔ Options sync ───
  const applyUriToOptions = useCallback((uriStr) => {
    const updates = parseUriToOptions(uriStr);
    if (!updates) return;
    uriSyncRef.current = true;
    setOptions((prev) => ({ ...prev, ...updates }));
    requestAnimationFrame(() => { uriSyncRef.current = false; });
  }, []);

  const handleUriChange = useCallback((newUri) => {
    setUri(newUri);
    applyUriToOptions(newUri);
  }, [applyUriToOptions]);

  const setOpt = useCallback((key, value) => {
    setOptions((prev) => {
      const next = { ...prev, [key]: value };
      // Rebuild URI from options when form changes (unless the change came from URI parsing)
      if (!uriSyncRef.current) {
        requestAnimationFrame(() => {
          setUri((prevUri) => {
            // Don't overwrite if this was a URI-triggered change
            if (uriSyncRef.current) return prevUri;
            return buildUriFromOptions(next);
          });
        });
      }
      return next;
    });
  }, []);

  const setMultiOpt = useCallback((updates) => {
    setOptions((prev) => {
      const next = { ...prev, ...updates };
      if (!uriSyncRef.current) {
        requestAnimationFrame(() => {
          setUri(() => buildUriFromOptions(next));
        });
      }
      return next;
    });
  }, []);

  // ─── Hosts management ───
  const addHost = useCallback(() => {
    setOptions((prev) => {
      const next = { ...prev, hosts: [...(prev.hosts || []), ''] };
      requestAnimationFrame(() => setUri(buildUriFromOptions(next)));
      return next;
    });
  }, []);

  const updateHost = useCallback((index, value) => {
    setOptions((prev) => {
      const hosts = [...(prev.hosts || [])];
      hosts[index] = value;
      const next = { ...prev, hosts };
      requestAnimationFrame(() => setUri(buildUriFromOptions(next)));
      return next;
    });
  }, []);

  const removeHost = useCallback((index) => {
    setOptions((prev) => {
      const hosts = (prev.hosts || []).filter((_, i) => i !== index);
      if (hosts.length === 0) hosts.push('');
      const next = { ...prev, hosts };
      requestAnimationFrame(() => setUri(buildUriFromOptions(next)));
      return next;
    });
  }, []);

  // ─── URI Options management ───
  const addUriOption = useCallback(() => {
    setOptions((prev) => ({
      ...prev,
      uriOptions: [...(prev.uriOptions || []), { key: '', value: '' }],
    }));
  }, []);

  const updateUriOption = useCallback((index, field, value) => {
    setOptions((prev) => {
      const opts = [...(prev.uriOptions || [])];
      opts[index] = { ...opts[index], [field]: value };
      const next = { ...prev, uriOptions: opts };
      if (field === 'value' || (field === 'key' && opts[index].value)) {
        requestAnimationFrame(() => setUri(buildUriFromOptions(next)));
      }
      return next;
    });
  }, []);

  const removeUriOption = useCallback((index) => {
    setOptions((prev) => {
      const next = { ...prev, uriOptions: (prev.uriOptions || []).filter((_, i) => i !== index) };
      requestAnimationFrame(() => setUri(buildUriFromOptions(next)));
      return next;
    });
  }, []);

  // ─── Connection logic ───
  const routingFacts = parseUriRoutingFacts(uri);
  const normalizedReadPreference = String(options.readPreference || 'primary').trim().toLowerCase() || 'primary';
  const riskyReadPreference = normalizedReadPreference === 'primary' || normalizedReadPreference === 'primarypreferred';
  const secondaryRecommendationSupported = options.directConnection !== true && (
    routingFacts.isSrv || routingFacts.hostCount > 1 || routingFacts.hasReplicaSetParam || Boolean(options.replicaSet)
  );
  const shouldRecommendSecondary = Boolean(
    options.markAsProduction && riskyReadPreference && secondaryRecommendationSupported
  );

  const buildConnectionPayload = useCallback((sourceOptions = options) => {
    const cleanOpts = {};
    // TLS
    if (sourceOptions.tlsMode === 'on') cleanOpts.tls = true;
    else if (sourceOptions.tlsMode === 'off') cleanOpts.tls = false;
    if (sourceOptions.tlsAllowInvalidCertificates) cleanOpts.tlsAllowInvalidCertificates = true;
    if (sourceOptions.tlsAllowInvalidHostnames) cleanOpts.tlsAllowInvalidHostnames = true;
    if (sourceOptions.tlsInsecure) cleanOpts.tlsInsecure = true;
    if (sourceOptions.tlsCAFileContent) {
      cleanOpts.tlsCAFileContent = sourceOptions.tlsCAFileContent;
    } else if (sourceOptions.tlsCAFile) {
      cleanOpts.tlsCAFile = sourceOptions.tlsCAFile;
    }
    if (sourceOptions.tlsUseSystemCA) cleanOpts.tlsUseSystemCA = true;
    if (sourceOptions.tlsCertKeyFileContent) {
      cleanOpts.tlsCertKeyFileContent = sourceOptions.tlsCertKeyFileContent;
    } else if (sourceOptions.tlsCertificateKeyFile) {
      cleanOpts.tlsCertificateKeyFile = sourceOptions.tlsCertificateKeyFile;
    }
    if (sourceOptions.tlsCertificateKeyFilePassword) cleanOpts.tlsCertificateKeyFilePassword = sourceOptions.tlsCertificateKeyFilePassword;
    // Auth
    if (sourceOptions.authSource) cleanOpts.authSource = sourceOptions.authSource;
    if (sourceOptions.authMechanism) cleanOpts.authMechanism = sourceOptions.authMechanism;
    if (sourceOptions.authMethod === 'x509') cleanOpts.authMechanism = 'MONGODB-X509';
    if (sourceOptions.authMethod === 'kerberos') {
      cleanOpts.authMechanism = 'GSSAPI';
      if (sourceOptions.kerberosServiceName) cleanOpts.kerberosServiceName = sourceOptions.kerberosServiceName;
      if (sourceOptions.kerberosCanonicalizeHostname) cleanOpts.kerberosCanonicalizeHostname = true;
    }
    if (sourceOptions.authMethod === 'ldap') cleanOpts.authMechanism = 'PLAIN';
    if (sourceOptions.authMethod === 'aws') {
      cleanOpts.authMechanism = 'MONGODB-AWS';
      if (sourceOptions.awsSessionToken) cleanOpts.awsSessionToken = sourceOptions.awsSessionToken;
    }
    if (sourceOptions.authMethod === 'oidc') {
      cleanOpts.authMechanism = 'MONGODB-OIDC';
      if (sourceOptions.oidcUsername) cleanOpts.oidcUsername = sourceOptions.oidcUsername;
      if (sourceOptions.oidcRedirectUri) cleanOpts.oidcRedirectUri = sourceOptions.oidcRedirectUri;
      if (sourceOptions.oidcTrustedEndpoint) cleanOpts.oidcTrustedEndpoint = true;
    }
    // Topology
    if (sourceOptions.replicaSet) cleanOpts.replicaSet = sourceOptions.replicaSet;
    if (sourceOptions.directConnection !== undefined) cleanOpts.directConnection = sourceOptions.directConnection;
    if (sourceOptions.readPreference) cleanOpts.readPreference = sourceOptions.readPreference;
    if (sourceOptions.connectTimeoutMS) cleanOpts.connectTimeoutMS = sourceOptions.connectTimeoutMS;
    if (sourceOptions.markAsProduction) cleanOpts.markAsProduction = true;
    // Credentials
    if (sourceOptions.authMethod === 'password' && sourceOptions.username?.trim()) {
      cleanOpts.username = sourceOptions.username.trim();
      cleanOpts.password = sourceOptions.password || '';
    } else if (sourceOptions.authMethod === 'ldap' && sourceOptions.username?.trim()) {
      cleanOpts.username = sourceOptions.username.trim();
      cleanOpts.password = sourceOptions.password || '';
    } else if (sourceOptions.authMethod === 'aws') {
      if (sourceOptions.awsAccessKeyId) cleanOpts.username = sourceOptions.awsAccessKeyId;
      if (sourceOptions.awsSecretAccessKey) cleanOpts.password = sourceOptions.awsSecretAccessKey;
    } else if (sourceOptions.authMethod === 'kerberos' && sourceOptions.username?.trim()) {
      cleanOpts.username = sourceOptions.username.trim();
    }
    // Proxy/SSH
    if (sourceOptions.proxyMethod === 'ssh-password' || sourceOptions.proxyMethod === 'ssh-identity') {
      cleanOpts.sshTunnel = {
        host: sourceOptions.sshHost,
        port: sourceOptions.sshPort || 22,
        username: sourceOptions.sshUsername,
      };
      if (sourceOptions.proxyMethod === 'ssh-password') {
        cleanOpts.sshTunnel.password = sourceOptions.sshPassword;
      } else {
        cleanOpts.sshTunnel.identityFile = sourceOptions.sshIdentityFile;
        if (sourceOptions.sshIdentityFilePassphrase) cleanOpts.sshTunnel.passphrase = sourceOptions.sshIdentityFilePassphrase;
      }
    }
    if (sourceOptions.proxyMethod === 'socks5') {
      cleanOpts.proxyHost = sourceOptions.socks5Host;
      cleanOpts.proxyPort = sourceOptions.socks5Port || 1080;
      if (sourceOptions.socks5Username) cleanOpts.proxyUsername = sourceOptions.socks5Username;
      if (sourceOptions.socks5Password) cleanOpts.proxyPassword = sourceOptions.socks5Password;
    }
    // URI options
    for (const { key, value } of (sourceOptions.uriOptions || [])) {
      if (key && value !== undefined && value !== '') cleanOpts[key] = value;
    }
    return cleanOpts;
  }, [options]);

  const connectNow = useCallback((targetUri, cleanOpts) => {
    // If user typed a key but didn't explicitly save it, save it now on connect
    try {
      const trimmedKey = adminKeyInput.trim();
      if (trimmedKey) {
        localStorage.setItem('mongostudio_admin_key', trimmedKey);
      } else if (!adminKeySaved) {
        localStorage.removeItem('mongostudio_admin_key');
      }
    } catch {}
    saveRecent(targetUri, cleanOpts, !dontSaveRecentPassword);
    onConnect(targetUri, cleanOpts);
  }, [adminKeyInput, adminKeySaved, dontSaveRecentPassword, onConnect]);

  const handleSubmit = useCallback((e) => {
    if (e) e.preventDefault();
    const targetUri = uri.trim();
    if (!targetUri || connecting) return;
    const cleanOpts = buildConnectionPayload(options);
    if (shouldRecommendSecondary) {
      setShowAdvanced(true);
      setPendingConnectConfirm({ uri: targetUri, options: cleanOpts });
      return;
    }
    connectNow(targetUri, cleanOpts);
  }, [uri, connecting, options, buildConnectionPayload, shouldRecommendSecondary, connectNow]);

  const handleSaveAndConnect = useCallback(() => {
    if (!profileName.trim() || !uri.trim()) return;
    handleSaveProfile();
    handleSubmit();
  }, [profileName, uri]);

  const handleDemoConnect = useCallback(() => {
    if (connecting) return;
    const demoUri = 'mock://demo.local/sample_mflix';
    const demoOptions = { readPreference: 'primary' };
    setUri(demoUri);
    saveRecent(demoUri, demoOptions, false);
    onConnect(demoUri, demoOptions);
  }, [connecting, onConnect]);

  const handleDemoReset = useCallback(async () => {
    if (connecting || demoResetting) return;
    setDemoResetting(true);
    try {
      await api.resetMockDemo({ preserveServiceConfig: true });
      pushToast('success', 'Demo state reset to default dataset.');
    } catch (err) {
      pushToast('error', err?.message || 'Demo reset failed.');
    } finally {
      setDemoResetting(false);
    }
  }, [connecting, demoResetting, pushToast]);

  // ─── Profile/Recent handlers ───
  const handleRecent = useCallback((item) => {
    const opts = item.options || {};
    setUri(item.uri);
    const savedPassword = typeof opts.password === 'string' ? opts.password : '';
    setOptions({ ...DEFAULT_OPTIONS, ...opts, password: savedPassword });
    applyUriToOptions(item.uri);
  }, [applyUriToOptions]);

  const handleProfile = useCallback((p) => {
    setUri(p.uri);
    if (p.options) {
      const savedPassword = typeof p.options.password === 'string' ? p.options.password : '';
      setOptions({ ...DEFAULT_OPTIONS, ...p.options, password: savedPassword });
      applyUriToOptions(p.uri);
    }
    // Restore admin key from profile
    if (p.adminKey) {
      try { localStorage.setItem('mongostudio_admin_key', p.adminKey); } catch {}
      setAdminKeySaved(true);
      setAdminKeyInput('');
    }
    setEditingProfileIndex(null);
    setShowProfiles(false);
  }, [applyUriToOptions]);

  const handleStartEditProfile = useCallback((index) => {
    const profile = profiles[index];
    if (!profile) return;
    setEditingProfileIndex(index);
    setProfileName(profile.name || '');
    setSaveProfilePassword(Boolean(profile.options?.password));
    setSaveProfileAdminKey(Boolean(profile.adminKey));
    setUri(profile.uri || '');
    const savedPassword = typeof profile.options?.password === 'string' ? profile.options.password : '';
    setOptions({ ...DEFAULT_OPTIONS, ...(profile.options || {}), password: savedPassword });
    applyUriToOptions(profile.uri || '');
  }, [profiles, applyUriToOptions]);

  const handleSaveProfile = useCallback(() => {
    if (!profileName.trim() || !uri.trim()) return;
    const includePassword = Boolean(saveProfilePassword && (options.username?.trim() || options.awsAccessKeyId));
    const safeUri = includePassword ? uri : stripPasswordFromUri(uri);
    let savedAdminKey = '';
    if (saveProfileAdminKey) {
      savedAdminKey = adminKeyInput.trim();
      if (!savedAdminKey && adminKeySaved) {
        try { savedAdminKey = localStorage.getItem('mongostudio_admin_key') || ''; } catch {}
      }
    }
    const nextProfile = {
      name: profileName.trim(),
      uri: safeUri,
      masked: maskUri(safeUri),
      options: sanitizeSavedOptions(options, includePassword),
      adminKey: savedAdminKey || undefined,
      ts: Date.now(),
    };
    const next = [...profiles];
    if (editingProfileIndex !== null && editingProfileIndex >= 0 && editingProfileIndex < next.length) {
      next[editingProfileIndex] = nextProfile;
    } else {
      next.push(nextProfile);
    }
    setProfiles(next);
    saveProfiles(next);
    setProfileName('');
    setEditingProfileIndex(null);
    setSaveProfilePassword(false);
    setSaveProfileAdminKey(false);
  }, [profileName, uri, options, saveProfilePassword, saveProfileAdminKey, adminKeyInput, adminKeySaved, profiles, editingProfileIndex]);

  const handleDeleteProfile = useCallback((i) => {
    const p = profiles.filter((_, j) => j !== i);
    setProfiles(p);
    saveProfiles(p);
    if (editingProfileIndex === i) {
      setEditingProfileIndex(null);
      setProfileName('');
      setSaveProfilePassword(false);
      setSaveProfileAdminKey(false);
    } else if (editingProfileIndex !== null && editingProfileIndex > i) {
      setEditingProfileIndex((prev) => (prev === null ? null : prev - 1));
    }
  }, [profiles, editingProfileIndex]);

  const handleExportProfiles = useCallback(() => {
    if (!profiles.length) return;
    const payload = {
      version: 1,
      app: 'MongoStudio',
      exportedAt: new Date().toISOString(),
      profiles: profiles.map((p) => ({ ...p })),
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `mongostudio-profiles-${new Date().toISOString().slice(0, 10)}.json`;
    document.body.appendChild(a);
    a.click();
    setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 100);
  }, [profiles]);

  const importFileRef = useRef(null);

  const handleImportProfiles = useCallback((e) => {
    const file = e.target?.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      try {
        const data = JSON.parse(ev.target.result);
        const imported = Array.isArray(data?.profiles) ? data.profiles : Array.isArray(data) ? data : [];
        if (!imported.length) { pushToast('error', 'No profiles found in file.'); return; }
        const valid = imported.filter((p) => p.name && p.uri);
        if (!valid.length) { pushToast('error', 'No valid profiles in file.'); return; }
        const existingNames = new Set(profiles.map((p) => p.name));
        let added = 0;
        let skipped = 0;
        const merged = [...profiles];
        for (const p of valid) {
          if (existingNames.has(p.name)) { skipped++; continue; }
          merged.push({ ...p, ts: p.ts || Date.now() });
          existingNames.add(p.name);
          added++;
        }
        setProfiles(merged);
        saveProfiles(merged);
        pushToast('success', `Imported ${added} profile${added !== 1 ? 's' : ''}${skipped ? `, ${skipped} skipped (duplicate names)` : ''}.`);
      } catch {
        pushToast('error', 'Invalid JSON file.');
      }
    };
    reader.readAsText(file);
    e.target.value = '';
  }, [profiles, pushToast]);

  const isTopologyError = error && (error.includes('DirectConnect') || error.includes('server selection') || error.includes('replica set') || error.includes('topology'));

  // URI option key dropdown options
  const usedUriKeys = useMemo(() => new Set((options.uriOptions || []).map((o) => o.key).filter(Boolean)), [options.uriOptions]);
  const uriOptionKeyOptions = useMemo(() => {
    const items = [];
    for (const group of URI_OPTION_GROUPS) {
      for (const key of group.options) {
        items.push({ value: key, label: key, group: group.label, disabled: usedUriKeys.has(key) });
      }
    }
    return items;
  }, [usedUriKeys]);

  // ─── Tab content renderers ────────────────────────────────────────────

  const TABS = [
    { id: 'general', label: 'General' },
    { id: 'authentication', label: 'Authentication' },
    { id: 'tls', label: 'TLS/SSL' },
    { id: 'proxy', label: 'Proxy/SSH' },
    { id: 'advanced', label: 'Advanced' },
  ];

  const renderGeneralTab = () => (
    <div className="space-y-4">
      <FormField label="Connection String Scheme">
        <SegmentedButtons
          options={[{ value: 'mongodb', label: 'mongodb' }, { value: 'mongodb+srv', label: 'mongodb+srv' }]}
          value={options.scheme || 'mongodb'}
          onChange={(v) => {
            if (v === 'mongodb+srv') {
              // SRV: keep only first host, strip port, clear directConnection
              const firstHost = (options.hosts || ['localhost'])[0] || 'localhost';
              const hostOnly = firstHost.split(':')[0];
              setMultiOpt({ scheme: v, hosts: [hostOnly], directConnection: undefined });
            } else {
              setOpt('scheme', v);
            }
          }}
        />
        <p className="text-2xs mt-1.5" style={{ color: 'var(--text-tertiary)' }}>
          {options.scheme === 'mongodb+srv'
            ? 'SRV record format. DNS resolves hosts automatically.'
            : 'Standard format for standalone, replica set, or sharded cluster.'}
        </p>
      </FormField>

      <FormField label="Host">
        <div className="space-y-2">
          {(options.hosts || ['']).map((host, i) => (
            <div key={i} className="flex items-center gap-2">
              <TextInput
                value={host}
                onChange={(v) => updateHost(i, v)}
                placeholder="localhost:27017"
                mono
                className="flex-1"
              />
              {(options.hosts || []).length > 1 && (
                <button type="button" className="btn-ghost p-1.5" onClick={() => removeHost(i)} title="Remove host">
                  <X className="w-3.5 h-3.5" style={{ color: 'var(--text-tertiary)' }} />
                </button>
              )}
            </div>
          ))}
          {options.scheme !== 'mongodb+srv' && (
            <button
              type="button"
              className="btn-ghost text-2xs flex items-center gap-1 px-2 py-1"
              onClick={addHost}
              style={{ color: 'var(--accent)' }}
            >
              <Plus className="w-3 h-3" /> Add host
            </button>
          )}
        </div>
      </FormField>

      {options.scheme !== 'mongodb+srv' && (
        <CheckboxField
          label="Direct Connection"
          hint="Specifies whether to force dispatch all operations to the specified host."
          checked={options.directConnection === true}
          onChange={(v) => setOpt('directConnection', v ? true : undefined)}
        />
      )}
    </div>
  );

  const renderAuthenticationTab = () => (
    <div className="space-y-4">
      <FormField label="Authentication Method">
        <div className="flex flex-wrap gap-1">
          {AUTH_METHODS.map((m) => {
            const active = (options.authMethod || 'none') === m.value;
            return (
              <button
                key={m.value}
                type="button"
                onClick={() => setOpt('authMethod', m.value)}
                className="text-xs font-medium px-3 py-1.5 rounded-lg transition-all whitespace-nowrap"
                style={{
                  background: active ? 'var(--accent)' : 'var(--surface-2)',
                  color: active ? 'var(--surface-0)' : 'var(--text-secondary)',
                  border: active ? '1px solid var(--accent)' : '1px solid var(--border)',
                }}
              >
                {m.label}
              </button>
            );
          })}
        </div>
      </FormField>

      {options.authMethod === 'password' && (
        <>
          <FormField label="Username" optional>
            <TextInput value={options.username || ''} onChange={(v) => setOpt('username', v)} placeholder="Optional" mono />
          </FormField>
          <FormField label="Password" optional>
            <TextInput value={options.password || ''} onChange={(v) => setOpt('password', v)} placeholder="Optional" type="password" mono />
          </FormField>
          <FormField label="Authentication Database" optional hint="Database to authenticate against. Defaults to admin.">
            <TextInput value={options.authSource || ''} onChange={(v) => setOpt('authSource', v)} placeholder="Optional" mono />
          </FormField>
          <FormField label="Authentication Mechanism">
            <SegmentedButtons
              options={AUTH_MECHANISMS}
              value={options.authMechanism || ''}
              onChange={(v) => setOpt('authMechanism', v)}
            />
          </FormField>
        </>
      )}

      {options.authMethod === 'x509' && (
        <div className="p-3 rounded-lg text-xs" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-secondary)' }}>
          X.509 authentication uses TLS client certificates. Configure your client certificate in the TLS/SSL tab. The username is derived from the certificate subject.
        </div>
      )}

      {options.authMethod === 'kerberos' && (
        <>
          <FormField label="Principal" optional>
            <TextInput value={options.username || ''} onChange={(v) => setOpt('username', v)} placeholder="username@REALM" mono />
          </FormField>
          <FormField label="Service Name" optional hint="The Kerberos service name. Defaults to 'mongodb'.">
            <TextInput value={options.kerberosServiceName || ''} onChange={(v) => setOpt('kerberosServiceName', v)} placeholder="mongodb" mono />
          </FormField>
          <CheckboxField
            label="Canonicalize Host Name"
            checked={options.kerberosCanonicalizeHostname === true}
            onChange={(v) => setOpt('kerberosCanonicalizeHostname', v)}
          />
          <FormField label="Service Realm" optional>
            <TextInput value={options.kerberosServiceRealm || ''} onChange={(v) => setOpt('kerberosServiceRealm', v)} placeholder="Optional" mono />
          </FormField>
        </>
      )}

      {options.authMethod === 'ldap' && (
        <>
          <FormField label="Username">
            <TextInput value={options.username || ''} onChange={(v) => setOpt('username', v)} placeholder="LDAP username" mono />
          </FormField>
          <FormField label="Password">
            <TextInput value={options.password || ''} onChange={(v) => setOpt('password', v)} placeholder="Password" type="password" mono />
          </FormField>
        </>
      )}

      {options.authMethod === 'aws' && (
        <>
          <FormField label="AWS Access Key ID" optional>
            <TextInput value={options.awsAccessKeyId || ''} onChange={(v) => setOpt('awsAccessKeyId', v)} placeholder="Optional" mono />
          </FormField>
          <FormField label="AWS Secret Access Key" optional>
            <TextInput value={options.awsSecretAccessKey || ''} onChange={(v) => setOpt('awsSecretAccessKey', v)} placeholder="Optional" type="password" mono />
          </FormField>
          <FormField label="AWS Session Token" optional>
            <TextInput value={options.awsSessionToken || ''} onChange={(v) => setOpt('awsSessionToken', v)} placeholder="Optional" mono />
          </FormField>
        </>
      )}

      {options.authMethod === 'oidc' && (
        <>
          <FormField label="Username" optional>
            <TextInput value={options.oidcUsername || ''} onChange={(v) => setOpt('oidcUsername', v)} placeholder="Optional" mono />
          </FormField>

          <div>
            <button
              type="button"
              className="flex items-center gap-1.5 text-xs font-medium mb-2"
              style={{ color: 'var(--text-secondary)' }}
              onClick={() => setOpt('oidcOptionsOpen', !options.oidcOptionsOpen)}
            >
              <ChevronDown className={`w-3.5 h-3.5 transition-transform ${options.oidcOptionsOpen ? '' : '-rotate-90'}`} style={{ color: 'var(--text-tertiary)' }} />
              OIDC Options
            </button>
            {options.oidcOptionsOpen && (
              <div className="space-y-3 pl-1">
                <FormField label="Auth Code Flow Redirect URI" optional hint="This value needs to match the configuration of the Identity Provider used by the server.">
                  <TextInput value={options.oidcRedirectUri || ''} onChange={(v) => setOpt('oidcRedirectUri', v)} placeholder="Optional" mono />
                </FormField>
                <CheckboxField
                  label="Consider Target Endpoint Trusted"
                  hint="Allow connecting when the target endpoint is not in the list of endpoints that are considered trusted by default. Only use this option when connecting to servers that you trust."
                  checked={options.oidcTrustedEndpoint === true}
                  onChange={(v) => setOpt('oidcTrustedEndpoint', v)}
                />
              </div>
            )}
          </div>
        </>
      )}

      {options.authMethod === 'none' && (
        <div className="text-xs" style={{ color: 'var(--text-tertiary)' }}>
          No authentication configured. The connection will not use any credentials.
        </div>
      )}
    </div>
  );

  const renderTlsTab = () => (
    <div className="space-y-4">
      <FormField label="SSL/TLS Connection">
        <SegmentedButtons options={TLS_MODES} value={options.tlsMode || 'default'} onChange={(v) => setOpt('tlsMode', v)} />
      </FormField>

      <FormField label="Certificate Authority (.pem)" optional hint={options.tlsCAFileContent ? `Loaded: ${options.tlsCAFileName || 'certificate'}` : 'Server path or upload a certificate file.'}>
        <FilePathInput
          value={options.tlsCAFile || ''}
          onChange={(v) => setMultiOpt({ tlsCAFile: v, tlsCAFileContent: '', tlsCAFileName: '' })}
          placeholder="/path/to/ca.pem"
          fileLoaded={!!options.tlsCAFileContent}
          onFileLoad={(content, name) => setMultiOpt({ tlsCAFileContent: content, tlsCAFileName: name, tlsCAFile: name })}
        />
      </FormField>

      <CheckboxField
        label="Use System Certificate Authority"
        hint="Use the operating system's Certificate Authority store."
        checked={options.tlsUseSystemCA === true}
        onChange={(v) => setOpt('tlsUseSystemCA', v)}
      />

      <FormField label="Client Certificate and Key (.pem)" optional hint={options.tlsCertKeyFileContent ? `Loaded: ${options.tlsCertKeyFileName || 'certificate'}` : 'Server path or upload a certificate file. Required with X.509 auth.'}>
        <FilePathInput
          value={options.tlsCertificateKeyFile || ''}
          onChange={(v) => setMultiOpt({ tlsCertificateKeyFile: v, tlsCertKeyFileContent: '', tlsCertKeyFileName: '' })}
          placeholder="/path/to/client.pem"
          fileLoaded={!!options.tlsCertKeyFileContent}
          onFileLoad={(content, name) => setMultiOpt({ tlsCertKeyFileContent: content, tlsCertKeyFileName: name, tlsCertificateKeyFile: name })}
        />
      </FormField>

      <FormField label="Client Key Password" optional>
        <TextInput value={options.tlsCertificateKeyFilePassword || ''} onChange={(v) => setOpt('tlsCertificateKeyFilePassword', v)} placeholder="Optional" type="password" mono autoComplete="off" />
      </FormField>

      <div className="space-y-2 pt-1">
        <CheckboxField
          label="tlsInsecure"
          hint="This includes tlsAllowInvalidHostnames and tlsAllowInvalidCertificates."
          checked={options.tlsInsecure === true}
          onChange={(v) => setMultiOpt({ tlsInsecure: v, ...(v ? { tlsAllowInvalidHostnames: true, tlsAllowInvalidCertificates: true } : {}) })}
        />
        <CheckboxField
          label="tlsAllowInvalidHostnames"
          hint="Disable the validation of the hostnames in the certificate presented by the mongod/mongos instance."
          checked={options.tlsAllowInvalidHostnames === true}
          onChange={(v) => setOpt('tlsAllowInvalidHostnames', v)}
        />
        <CheckboxField
          label="tlsAllowInvalidCertificates"
          hint="Disable the validation of the server certificates."
          checked={options.tlsAllowInvalidCertificates === true}
          onChange={(v) => setOpt('tlsAllowInvalidCertificates', v)}
        />
      </div>
    </div>
  );

  const renderProxyTab = () => (
    <div className="space-y-4">
      <FormField label="SSH Tunnel/Proxy Method">
        <div className="flex flex-wrap gap-1">
          {PROXY_METHODS.map((m) => {
            const active = (options.proxyMethod || 'none') === m.value;
            return (
              <button
                key={m.value}
                type="button"
                onClick={() => setOpt('proxyMethod', m.value)}
                className="text-xs font-medium px-3 py-1.5 rounded-lg transition-all whitespace-nowrap"
                style={{
                  background: active ? 'var(--accent)' : 'var(--surface-2)',
                  color: active ? 'var(--surface-0)' : 'var(--text-secondary)',
                  border: active ? '1px solid var(--accent)' : '1px solid var(--border)',
                }}
              >
                {m.label}
              </button>
            );
          })}
        </div>
      </FormField>

      {(options.proxyMethod === 'ssh-password' || options.proxyMethod === 'ssh-identity') && (
        <>
          <FormField label="SSH Hostname">
            <TextInput value={options.sshHost || ''} onChange={(v) => setOpt('sshHost', v)} placeholder="hostname.example.com" mono />
          </FormField>
          <div className="grid grid-cols-2 gap-3">
            <FormField label="SSH Port">
              <TextInput
                value={options.sshPort || ''}
                onChange={(v) => setOpt('sshPort', parseInt(v) || 22)}
                placeholder="22"
                type="number"
                mono
              />
            </FormField>
            <FormField label="SSH Username">
              <TextInput value={options.sshUsername || ''} onChange={(v) => setOpt('sshUsername', v)} placeholder="ubuntu" mono />
            </FormField>
          </div>
          {options.proxyMethod === 'ssh-password' && (
            <FormField label="SSH Password">
              <TextInput value={options.sshPassword || ''} onChange={(v) => setOpt('sshPassword', v)} placeholder="Password" type="password" mono />
            </FormField>
          )}
          {options.proxyMethod === 'ssh-identity' && (
            <>
              <FormField label="SSH Identity File" hint="Path to the SSH private key file on the server filesystem.">
                <FilePathInput value={options.sshIdentityFile || ''} onChange={(v) => setOpt('sshIdentityFile', v)} placeholder="/path/to/id_rsa" />
              </FormField>
              <FormField label="SSH Passphrase" optional>
                <TextInput value={options.sshIdentityFilePassphrase || ''} onChange={(v) => setOpt('sshIdentityFilePassphrase', v)} placeholder="Optional" type="password" mono />
              </FormField>
            </>
          )}
        </>
      )}

      {options.proxyMethod === 'socks5' && (
        <>
          <div className="grid grid-cols-2 gap-3">
            <FormField label="Socks5 Hostname">
              <TextInput value={options.socks5Host || ''} onChange={(v) => setOpt('socks5Host', v)} placeholder="proxy.example.com" mono />
            </FormField>
            <FormField label="Socks5 Port">
              <TextInput
                value={options.socks5Port || ''}
                onChange={(v) => setOpt('socks5Port', parseInt(v) || 1080)}
                placeholder="1080"
                type="number"
                mono
              />
            </FormField>
          </div>
          <FormField label="Socks5 Username" optional>
            <TextInput value={options.socks5Username || ''} onChange={(v) => setOpt('socks5Username', v)} placeholder="Optional" mono />
          </FormField>
          <FormField label="Socks5 Password" optional>
            <TextInput value={options.socks5Password || ''} onChange={(v) => setOpt('socks5Password', v)} placeholder="Optional" type="password" mono />
          </FormField>
        </>
      )}

      {options.proxyMethod === 'none' && (
        <div className="text-xs" style={{ color: 'var(--text-tertiary)' }}>
          No proxy or SSH tunnel configured. Connection will be made directly to the MongoDB host.
        </div>
      )}
    </div>
  );

  const renderAdvancedTab = () => (
    <div className="space-y-4">
      <FormField label="Read Preference">
        <div className="flex flex-wrap gap-1">
          {READ_PREFERENCES.map((rp) => {
            const active = (options.readPreference || '') === rp.value;
            return (
              <button
                key={rp.value}
                type="button"
                onClick={() => setOpt('readPreference', rp.value)}
                className="text-xs font-medium px-3 py-1.5 rounded-lg transition-all whitespace-nowrap"
                style={{
                  background: active ? 'var(--accent)' : 'var(--surface-2)',
                  color: active ? 'var(--surface-0)' : 'var(--text-secondary)',
                  border: active ? '1px solid var(--accent)' : '1px solid var(--border)',
                }}
              >
                {rp.label}
              </button>
            );
          })}
        </div>
      </FormField>

      <FormField label="Replica Set Name" optional>
        <TextInput value={options.replicaSet || ''} onChange={(v) => setOpt('replicaSet', v)} placeholder="Optional" mono />
      </FormField>

      <FormField label="Default Authentication Database" optional hint="Authentication database used when authSource is not specified.">
        <TextInput value={options.defaultAuthDb || ''} onChange={(v) => setOpt('defaultAuthDb', v)} placeholder="Optional" mono />
      </FormField>

      {/* URI Options */}
      <div>
        <div className="flex items-center justify-between mb-2">
          <label className="text-xs font-medium" style={{ color: 'var(--text-secondary)' }}>URI Options</label>
        </div>
        <p className="text-2xs mb-2" style={{ color: 'var(--text-tertiary)' }}>
          Add additional MongoDB URI options to customize your connection.
        </p>
        <div className="space-y-2">
          {(options.uriOptions || []).map((opt, i) => (
            <div key={i} className="grid grid-cols-[1fr_1fr_auto] gap-2 items-center">
              <GroupedSelect
                value={opt.key}
                groups={URI_OPTION_GROUPS}
                disabledKeys={usedUriKeys}
                onChange={(v) => updateUriOption(i, 'key', v)}
              />
              <TextInput
                value={opt.value}
                onChange={(v) => updateUriOption(i, 'value', v)}
                placeholder="Value"
                mono
              />
              <button type="button" className="btn-ghost p-1.5" onClick={() => removeUriOption(i)}>
                <X className="w-3.5 h-3.5" style={{ color: 'var(--text-tertiary)' }} />
              </button>
            </div>
          ))}
          <button
            type="button"
            className="btn-ghost text-2xs flex items-center gap-1 px-2 py-1"
            onClick={addUriOption}
            style={{ color: 'var(--accent)' }}
          >
            <Plus className="w-3 h-3" /> Add option
          </button>
        </div>
      </div>

      {/* Production guard (our custom option) */}
      <div className="pt-2 space-y-3" style={{ borderTop: '1px solid var(--border)' }}>
        <CheckboxField
          label="Mark as production"
          hint="Enables production safety guards — shows warning banners and recommends secondaryPreferred for read operations."
          checked={options.markAsProduction === true}
          onChange={(v) => {
            setMultiOpt({
              markAsProduction: v,
              ...(v ? {} : { readPreference: '' }),
            });
          }}
        />
        <CheckboxField
          label="Don't save password in Recent"
          hint="When enabled, passwords will be stripped from connection strings saved to Recent history."
          checked={dontSaveRecentPassword}
          onChange={setDontSaveRecentPassword}
        />
      </div>

      {/* Admin Access Key */}
      <div className="pt-2 space-y-2" style={{ borderTop: '1px solid var(--border)' }}>
        <FormField label="Admin Access Key" optional hint={
          adminKeyConfigured === false
            ? 'No admin key configured on server. All features are accessible without a key. Set ADMIN_ACCESS_KEY when starting the container to enable access control.'
            : adminKeySaved
              ? 'Key will be applied automatically on connect.'
              : 'Required to unlock consoles, server management, and rate limit settings.'
        }>
          {adminKeyConfigured === false ? (
            <div className="flex items-center gap-2 rounded-lg px-3 py-2" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', opacity: 0.5 }}>
              <Key className="w-3.5 h-3.5" style={{ color: 'var(--text-tertiary)' }} />
              <span className="text-xs" style={{ color: 'var(--text-tertiary)' }}>Not configured on server</span>
            </div>
          ) : adminKeySaved ? (
            <div className="flex items-center gap-2">
              <div className="flex-1 flex items-center gap-2 rounded-lg px-3 py-2" style={{ background: 'rgba(16,185,129,0.06)', border: '1px solid rgba(16,185,129,0.2)' }}>
                <Lock className="w-3.5 h-3.5" style={{ color: '#10b981' }} />
                <span className="text-xs font-mono" style={{ color: 'var(--text-secondary)' }}>{'•'.repeat(12)}</span>
                <span className="flex-1" />
                <span className="text-2xs" style={{ color: '#10b981' }}>Saved</span>
              </div>
              <button
                type="button"
                className="btn-ghost text-xs px-2.5 py-2"
                style={{ color: 'var(--text-tertiary)' }}
                onClick={() => {
                  setAdminKeySaved(false);
                  setAdminKeyInput('');
                  try { localStorage.removeItem('mongostudio_admin_key'); } catch {}
                }}
                title="Clear admin key"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            </div>
          ) : (
            <div className="flex gap-2">
              <input
                type="password"
                className="input-field text-xs font-mono flex-1"
                value={adminKeyInput}
                onChange={(e) => setAdminKeyInput(e.target.value)}
                placeholder="Enter admin access key"
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && adminKeyInput.trim()) {
                    try { localStorage.setItem('mongostudio_admin_key', adminKeyInput.trim()); } catch {}
                    setAdminKeySaved(true);
                    setAdminKeyInput('');
                  }
                }}
              />
              <button
                type="button"
                className="btn-primary text-xs px-3 py-1.5 whitespace-nowrap"
                disabled={!adminKeyInput.trim()}
                onClick={() => {
                  try { localStorage.setItem('mongostudio_admin_key', adminKeyInput.trim()); } catch {}
                  setAdminKeySaved(true);
                  setAdminKeyInput('');
                }}
              >
                Save
              </button>
            </div>
          )}
        </FormField>
      </div>
    </div>
  );

  const renderTabContent = () => {
    switch (activeTab) {
      case 'general': return renderGeneralTab();
      case 'authentication': return renderAuthenticationTab();
      case 'tls': return renderTlsTab();
      case 'proxy': return renderProxyTab();
      case 'advanced': return renderAdvancedTab();
      default: return renderGeneralTab();
    }
  };

  // ──────────────────────────────────────────────────────────────────────────

  return (
    <div className="h-screen w-screen relative isolate flex items-start justify-center noise-bg overflow-y-auto overflow-x-hidden py-8 sm:py-10" style={{ background: 'var(--surface-0)' }}>
      <div className="fixed inset-0 overflow-hidden pointer-events-none -z-10">
        <div className="absolute top-1/4 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] rounded-full blur-[120px]" style={{ background: 'var(--accent)', opacity: 0.04 }} />
        <div className="absolute bottom-0 left-1/4 w-[400px] h-[400px] rounded-full blur-[100px]" style={{ background: 'var(--accent)', opacity: 0.02 }} />
        <div className="absolute inset-0 opacity-[0.015]" style={{ backgroundImage: 'radial-gradient(circle at 1px 1px, var(--text-primary) 1px, transparent 0)', backgroundSize: '48px 48px' }} />
      </div>

      {/* Theme toggle */}
      <button onClick={onToggleTheme} className="absolute top-4 right-4 z-20 btn-ghost p-2" title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
        {theme === 'dark' ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
      </button>

      {toasts.length > 0 && (
        <div className="fixed top-4 left-4 z-30 flex w-[min(22rem,calc(100vw-2rem))] flex-col gap-2 pointer-events-none">
          {toasts.map((toast) => (
            <ToastNotice
              key={toast.id}
              kind={toast.kind}
              message={toast.message}
              className="w-full"
              onClose={() => dismissToast(toast.id)}
            />
          ))}
        </div>
      )}

      <div className="relative z-10 w-full max-w-xl px-6">
        {/* Logo */}
        <div className="text-center mb-8 float-in">
          <div className="inline-flex items-center justify-center mb-5">
            <Logo size={48} />
          </div>
          <h1 className="text-3xl font-display font-bold tracking-tight mb-2" style={{ color: 'var(--text-primary)' }}>
            Mongo<span style={{ color: 'var(--accent)' }}>Studio</span>
          </h1>
          <p className="text-sm" style={{ color: 'var(--text-tertiary)' }}>Blazing-fast MongoDB interface</p>
        </div>

        {/* Connection Form */}
        <form onSubmit={handleSubmit} className="float-in float-in-delay-1" autoComplete="off">
          <div className="rounded-2xl glow-border" style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}>

            {/* Header */}
            <div className="px-6 py-4 flex items-center justify-between" style={{ borderBottom: '1px solid var(--border)' }}>
              <h2 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>New Connection</h2>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => setShowProfiles(!showProfiles)}
                  className="inline-flex items-center gap-1.5 text-xs rounded-lg border px-2.5 py-1.5 transition-colors"
                  style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)', background: 'var(--surface-2)' }}
                >
                  Saved <span className="badge-blue">{profiles.length}</span>
                </button>
              </div>
            </div>

            {/* Profiles panel */}
            <input
              ref={importFileRef}
              type="file"
              accept=".json"
              onChange={handleImportProfiles}
              className="hidden"
            />
            {showProfiles && (
              <div className="px-6 py-3 animate-slide-up" style={{ borderBottom: '1px solid var(--border)', background: 'var(--surface-2)' }}>
                <div className="space-y-1 mb-3">
                  {profiles.length === 0 ? (
                    <div className="text-2xs text-center py-2" style={{ color: 'var(--text-tertiary)' }}>
                      No saved profiles
                      <button
                        type="button"
                        onClick={() => importFileRef.current?.click()}
                        className="ml-2 underline hover:no-underline"
                        style={{ color: 'var(--accent)' }}
                      >Import</button>
                    </div>
                  ) : profiles.map((p, i) => (
                    <div
                      key={i}
                      className="flex items-center gap-2 rounded-lg border border-transparent px-2.5 py-2 group transition-all hover:bg-[var(--surface-3)] hover:border-[var(--border)] cursor-pointer"
                      onClick={() => handleProfile(p)}
                    >
                      <Server className="w-3.5 h-3.5 flex-shrink-0" style={{ color: 'var(--text-tertiary)' }} />
                      <span className="text-xs font-medium truncate" style={{ color: 'var(--text-secondary)', minWidth: 40, maxWidth: 120 }}>{p.name}</span>
                      {typeof p.options?.password === 'string' && p.options.password.length > 0 ? <span className="badge-yellow flex-shrink-0">pwd</span> : null}
                      {p.adminKey ? <span className="badge-green flex-shrink-0">key</span> : null}
                      <span className="text-2xs flex-1 truncate" style={{ color: 'var(--text-tertiary)' }}>{p.masked}</span>
                      <button type="button" onClick={(e) => { e.stopPropagation(); handleStartEditProfile(i); }}
                        className="opacity-0 group-hover:opacity-100 p-1 rounded hover:bg-[var(--surface-3)]" title="Edit">
                        <Edit className="w-3 h-3" style={{ color: 'var(--text-secondary)' }} />
                      </button>
                      <button type="button" onClick={(e) => { e.stopPropagation(); handleDeleteProfile(i); }}
                        className="opacity-0 group-hover:opacity-100 p-1 rounded hover:bg-red-500/10">
                        <Trash className="w-3 h-3 text-red-400" />
                      </button>
                    </div>
                  ))}
                </div>
                <div className="pt-3" style={{ borderTop: '1px solid var(--border)' }}>
                  <div
                    className="rounded-xl px-3 py-3"
                    style={{ background: 'var(--surface-1)', border: '1px solid var(--border)' }}
                  >
                    <div className="flex items-center gap-2">
                      <TextInput
                        value={profileName}
                        onChange={setProfileName}
                        placeholder="Profile name..."
                        className="flex-1"
                        aria-label="Profile name"
                      />
                      <button
                        type="button"
                        onClick={handleSaveProfile}
                        disabled={!profileName.trim() || !uri.trim()}
                        className="inline-flex items-center justify-center rounded-lg border px-3 py-1.5 text-xs font-medium transition-all disabled:opacity-30 whitespace-nowrap flex-shrink-0"
                        style={{
                          color: 'var(--accent)',
                          borderColor: 'var(--border)',
                          background: 'var(--surface-2)',
                        }}
                      >
                        {editingProfileIndex !== null ? 'Update' : 'Save'}
                      </button>
                      {editingProfileIndex !== null && (
                        <button
                          type="button"
                          onClick={() => { setEditingProfileIndex(null); setProfileName(''); setSaveProfilePassword(false); setSaveProfileAdminKey(false); }}
                          className="text-2xs px-1.5 py-1 rounded font-medium flex-shrink-0"
                          style={{ color: 'var(--text-tertiary)' }}
                        >
                          Cancel
                        </button>
                      )}
                    </div>
                    <div className="flex items-center gap-4 mt-2">
                      <label
                        className={`inline-flex items-center gap-1.5 text-2xs ${canStoreProfilePassword ? 'cursor-pointer' : 'cursor-not-allowed opacity-40'}`}
                        style={{ color: 'var(--text-tertiary)' }}
                      >
                        <input
                          type="checkbox"
                          checked={saveProfilePassword}
                          onChange={(e) => setSaveProfilePassword(e.target.checked)}
                          disabled={!canStoreProfilePassword}
                          className="ms-checkbox"
                        />
                        Save password
                      </label>
                      <label
                        className={`inline-flex items-center gap-1.5 text-2xs ${adminKeySaved || adminKeyInput.trim() ? 'cursor-pointer' : 'cursor-not-allowed opacity-40'}`}
                        style={{ color: 'var(--text-tertiary)' }}
                      >
                        <input
                          type="checkbox"
                          checked={saveProfileAdminKey}
                          onChange={(e) => setSaveProfileAdminKey(e.target.checked)}
                          disabled={!adminKeySaved && !adminKeyInput.trim()}
                          className="ms-checkbox"
                        />
                        Save admin key
                      </label>
                    </div>
                  </div>
                </div>
                {/* Export / Import */}
                {profiles.length > 0 && (
                  <div className="flex items-center justify-end gap-1 mt-2">
                    <button
                      type="button"
                      onClick={handleExportProfiles}
                      className="p-1.5 rounded-lg transition-colors hover:bg-[var(--surface-3)]"
                      title="Export profiles"
                    >
                      <Download className="w-3.5 h-3.5" style={{ color: 'var(--text-tertiary)' }} />
                    </button>
                    <button
                      type="button"
                      onClick={() => importFileRef.current?.click()}
                      className="p-1.5 rounded-lg transition-colors hover:bg-[var(--surface-3)]"
                      title="Import profiles"
                    >
                      <Upload className="w-3.5 h-3.5" style={{ color: 'var(--text-tertiary)' }} />
                    </button>
                  </div>
                )}
              </div>
            )}

            {/* URI Section */}
            <div className="px-6 pt-4 pb-3">
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-1.5">
                  <label className="text-xs font-medium" style={{ color: 'var(--text-secondary)' }}>URI</label>
                  <Info className="w-3 h-3" style={{ color: 'var(--text-tertiary)' }} />
                </div>
                <label className="flex items-center gap-2 cursor-pointer">
                  <span className="text-2xs font-medium" style={{ color: 'var(--text-tertiary)' }}>Edit Connection String</span>
                  <div
                    className="relative w-8 h-4.5 rounded-full cursor-pointer transition-colors"
                    style={{ background: editUri ? 'var(--accent)' : 'var(--surface-3)', border: '1px solid var(--border)', padding: 1 }}
                    onClick={() => setEditUri(!editUri)}
                  >
                    <div
                      className="w-3.5 h-3.5 rounded-full transition-transform"
                      style={{
                        background: editUri ? 'var(--surface-0)' : 'var(--text-tertiary)',
                        transform: editUri ? 'translateX(14px)' : 'translateX(0)',
                      }}
                    />
                  </div>
                </label>
              </div>
              {editUri ? (
                <textarea
                  ref={inputRef}
                  value={uri}
                  onChange={(e) => handleUriChange(e.target.value)}
                  spellCheck={false}
                  autoComplete="off"
                  rows={2}
                  className="w-full rounded-lg px-3 py-2.5 text-xs font-mono resize-y focus:outline-none"
                  style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-primary)', minHeight: 60 }}
                />
              ) : (
                <div
                  className="w-full rounded-lg px-3 py-2.5 text-xs font-mono overflow-x-auto"
                  style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-tertiary)', minHeight: 38 }}
                >
                  {maskUri(uri) || 'mongodb://localhost:27017/'}
                </div>
              )}
              <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
                <button
                  type="button"
                  onClick={() => setShowAdvanced(!showAdvanced)}
                  className="inline-flex items-center gap-2 rounded-lg border px-3.5 py-2 text-xs font-medium transition-all hover:bg-[var(--surface-3)]"
                  style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)', background: 'var(--surface-2)' }}
                >
                  <ChevronDown className={`w-3.5 h-3.5 transition-transform ${showAdvanced ? '' : '-rotate-90'}`} />
                  Advanced Connection Options
                </button>
                <div
                  className="inline-flex items-stretch h-9 rounded-lg border overflow-hidden shrink-0"
                  style={{ borderColor: 'var(--border)', background: 'var(--surface-2)' }}
                >
                  <button type="button" onClick={handleDemoConnect} disabled={connecting}
                    className="ms-demo-segment ms-demo-segment--primary disabled:opacity-50"
                    style={{ color: 'var(--accent)' }} title="Open demo mode without real MongoDB">
                    Try Demo
                  </button>
                  <button type="button" onClick={handleDemoReset} disabled={connecting || demoResetting}
                    className="ms-demo-segment ms-demo-segment--icon disabled:opacity-50"
                    style={{ color: 'var(--text-secondary)' }}
                    title={demoResetting ? 'Resetting...' : 'Reset demo state'}>
                    <Refresh className={`w-3.5 h-3.5 ${demoResetting ? 'animate-spin' : ''}`} />
                  </button>
                </div>
              </div>
            </div>

            {/* Error */}
            {error && (
              <div className="px-6 pb-3">
                <InlineAlert kind="error" message={error} className="mx-0 mt-0" />
                {isTopologyError && !dismissedHints.topologyError && (
                  <div className="mt-2 text-2xs px-2 py-1.5 pr-8 rounded-lg relative" style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-tertiary)' }}>
                    <span className="block">
                      <strong style={{ color: 'var(--text-secondary)' }}>Hint:</strong> For replica sets, all member hostnames must resolve from this machine.
                      Enable <strong>DirectConnect</strong> and connect to one specific node.
                    </span>
                    <button type="button" className="absolute top-0.5 right-0.5 p-1 rounded-md hover:bg-[var(--surface-3)]"
                      onClick={() => setDismissedHints((prev) => ({ ...prev, topologyError: true }))} title="Hide hint">
                      <X className="w-3 h-3" style={{ color: 'var(--text-tertiary)' }} />
                    </button>
                  </div>
                )}
              </div>
            )}

            {shouldRecommendSecondary && !dismissedHints.productionReadPreference && (
              <div className="px-6 pb-3">
                <InlineAlert
                  kind="warning"
                  message="Production guard is ON. For browse-heavy sessions, secondaryPreferred is recommended."
                  className="mx-0 mt-0"
                  actions={[
                    {
                      label: 'Use secondaryPreferred',
                      primary: true,
                      onClick: () => {
                        setOpt('readPreference', 'secondaryPreferred');
                        setShowAdvanced(true);
                        setActiveTab('advanced');
                        setDismissedHints((prev) => ({ ...prev, productionReadPreference: true }));
                      },
                    },
                    {
                      label: 'Keep primary',
                      onClick: () => setDismissedHints((prev) => ({ ...prev, productionReadPreference: true })),
                    },
                  ]}
                />
              </div>
            )}

            {showAdvanced && (
              <div className="animate-slide-up" style={{ borderTop: '1px solid var(--border)' }}>
                {/* Tab bar */}
                <div className="flex gap-0 px-6" style={{ borderBottom: '1px solid var(--border)' }}>
                  {TABS.map((t) => (
                    <button
                      key={t.id}
                      type="button"
                      onClick={() => setActiveTab(t.id)}
                      className="px-3 py-2 text-xs font-medium -mb-px transition-all whitespace-nowrap"
                      style={{
                        borderBottom: activeTab === t.id ? '2px solid var(--accent)' : '2px solid transparent',
                        color: activeTab === t.id ? 'var(--accent)' : 'var(--text-tertiary)',
                      }}
                    >
                      {t.label}
                    </button>
                  ))}
                </div>

                {/* Tab content */}
                <div className="px-6 py-4">
                  {renderTabContent()}
                </div>
              </div>
            )}

            {/* Footer — Save / Save & Connect / Connect */}
            <div className="px-6 py-4 flex items-center justify-between" style={{ borderTop: '1px solid var(--border)' }}>
              <button
                type="button"
                onClick={() => {
                  if (!profileName.trim()) {
                    setShowProfiles(true);
                    return;
                  }
                  handleSaveProfile();
                }}
                className="text-xs font-medium px-4 py-2 rounded-lg transition-all"
                style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-secondary)' }}
              >
                Save
              </button>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={() => {
                    if (!profileName.trim()) {
                      setShowProfiles(true);
                      return;
                    }
                    handleSaveProfile();
                    handleSubmit();
                  }}
                  disabled={!uri.trim() || connecting}
                  className="text-xs font-medium px-4 py-2 rounded-lg transition-all disabled:opacity-30"
                  style={{ background: 'var(--surface-2)', border: '1px solid var(--border)', color: 'var(--text-secondary)' }}
                >
                  Save & Connect
                </button>
                <button
                  type="submit"
                  disabled={!uri.trim() || connecting}
                  className="text-xs font-semibold px-5 py-2 rounded-lg transition-all disabled:opacity-30 flex items-center gap-2"
                  style={{ background: 'var(--accent)', color: 'var(--surface-0)' }}
                >
                  {connecting ? <Loader style={{ color: 'var(--surface-0)' }} /> : <Zap className="w-3.5 h-3.5" />}
                  {connecting ? 'Connecting...' : 'Connect'}
                </button>
              </div>
            </div>

          </div>
        </form>

        {/* Recent */}
        {recent.length > 0 && (
          <div className="mt-5 float-in float-in-delay-2">
            <div className="text-xs font-medium uppercase tracking-wider mb-2 px-1" style={{ color: 'var(--text-tertiary)' }}>Recent</div>
            <div className="space-y-1">
              {recent.map((item, i) => (
                <button key={i} onClick={() => handleRecent(item)}
                  className="w-full text-left px-3 py-2.5 rounded-xl transition-all duration-150 group flex items-center gap-3"
                  style={{ background: 'var(--surface-1)', border: '1px solid var(--border)', opacity: 0.7 }}
                  onMouseOver={(e) => { e.currentTarget.style.opacity = '1'; e.currentTarget.style.borderColor = 'var(--border-hover)'; }}
                  onMouseOut={(e) => { e.currentTarget.style.opacity = '0.7'; e.currentTarget.style.borderColor = 'var(--border)'; }}
                >
                  <Server className="w-4 h-4" style={{ color: 'var(--text-tertiary)' }} />
                  <span className="text-sm font-mono truncate" style={{ color: 'var(--text-secondary)' }}>{item.masked}</span>
                </button>
              ))}
            </div>
          </div>
        )}

        <div className="mt-8 text-center float-in float-in-delay-3">
          <div className="mb-3 flex flex-wrap items-center justify-center gap-x-5 gap-y-2 text-2xs" style={{ color: 'var(--text-tertiary)' }}>
            <div className="inline-flex items-center gap-1.5">
              <div className="w-1.5 h-1.5 rounded-full" style={{ background: 'var(--accent)', opacity: 0.5 }} />
              MongoDB 3.6 → 8.x
            </div>
            <div className="inline-flex items-center gap-1.5">
              <div className="w-1.5 h-1.5 rounded-full" style={{ background: 'var(--accent)', opacity: 0.5 }} />
              Atlas & Self-hosted
            </div>
            <div className="inline-flex items-center gap-1.5">
              <div className="w-1.5 h-1.5 rounded-full" style={{ background: 'var(--accent)', opacity: 0.5 }} />
              Replica Sets & Sharded
            </div>
          </div>
          <p className="text-2xs" style={{ color: 'var(--text-tertiary)' }}>Open Source — MIT License — v2.6.0</p>
        </div>
      </div>

      {/* Production routing confirm modal */}
      {pendingConnectConfirm && (
        <AppModal open onClose={() => setPendingConnectConfirm(null)} maxWidth="max-w-md">
          <div className="px-5 py-4 flex items-center gap-2" style={{ borderBottom: '1px solid var(--border)' }}>
            <AlertTriangle className="w-4 h-4" style={{ color: '#fbbf24' }} />
            <h3 className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>Production Read Routing</h3>
          </div>
          <div className="px-5 py-4">
            <p className="text-xs leading-relaxed" style={{ color: 'var(--text-secondary)' }}>
              You are connecting with production guard while read preference is primary. Switch to secondaryPreferred for browsing reads?
            </p>
          </div>
          <div className="px-5 py-4 flex items-center justify-end gap-2" style={{ borderTop: '1px solid var(--border)' }}>
            <button type="button" className="btn-ghost text-xs" onClick={() => setPendingConnectConfirm(null)}>Cancel</button>
            <button type="button" className="btn-ghost text-xs" onClick={() => {
              const pending = pendingConnectConfirm;
              setPendingConnectConfirm(null);
              if (pending) connectNow(pending.uri, pending.options);
            }}>Continue on primary</button>
            <button type="button" className="btn-primary text-xs px-3 py-1.5" onClick={() => {
              const pending = pendingConnectConfirm;
              setPendingConnectConfirm(null);
              if (!pending) return;
              const nextOptions = { ...pending.options, readPreference: 'secondaryPreferred' };
              setOpt('readPreference', 'secondaryPreferred');
              connectNow(pending.uri, nextOptions);
            }}>Switch to secondaryPreferred</button>
          </div>
        </AppModal>
      )}
    </div>
  );
}

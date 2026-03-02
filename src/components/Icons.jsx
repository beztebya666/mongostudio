import React from 'react';

const icon = (d, size = 18, viewBox = '0 0 24 24') => ({ className, ...props }) => (
  <svg
    width={size}
    height={size}
    viewBox={viewBox}
    fill="none"
    stroke="currentColor"
    strokeWidth="1.8"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={className}
    {...props}
  >
    {typeof d === 'string' ? <path d={d} /> : d}
  </svg>
);

export const Database = icon(<>
  <ellipse cx="12" cy="5" rx="9" ry="3" />
  <path d="M3 5V19A9 3 0 0 0 21 19V5" />
  <path d="M3 12A9 3 0 0 0 21 12" />
</>);

export const Collection = icon(<>
  <path d="M3 3h7v7H3zM14 3h7v7h-7zM3 14h7v7H3zM14 14h7v7h-7z" />
</>);

export const Document = icon(<>
  <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
  <polyline points="14 2 14 8 20 8" />
</>);

export const Search = icon(<>
  <circle cx="11" cy="11" r="8" />
  <line x1="21" y1="21" x2="16.65" y2="16.65" />
</>);

export const Plus = icon(<>
  <line x1="12" y1="5" x2="12" y2="19" />
  <line x1="5" y1="12" x2="19" y2="12" />
</>);

export const Trash = icon(<>
  <polyline points="3 6 5 6 21 6" />
  <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
</>);

export const Edit = icon(<>
  <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" />
  <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z" />
</>);

export const Refresh = icon(<>
  <polyline points="23 4 23 10 17 10" />
  <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" />
</>);

export const ChevronRight = icon(<path d="M9 18l6-6-6-6" />);

export const ChevronDown = icon(<path d="M6 9l6 6 6-6" />);

export const ChevronLeft = icon(<path d="M15 18l-6-6 6-6" />);

export const Copy = icon(<>
  <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
  <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
</>);

export const Terminal = icon(<>
  <polyline points="4 17 10 11 4 5" />
  <line x1="12" y1="19" x2="20" y2="19" />
</>);

export const Settings = icon(<>
  <circle cx="12" cy="12" r="3" />
  <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
</>);

export const Zap = icon(<>
  <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
</>);

export const Server = icon(<>
  <rect x="2" y="2" width="20" height="8" rx="2" ry="2" />
  <rect x="2" y="14" width="20" height="8" rx="2" ry="2" />
  <line x1="6" y1="6" x2="6.01" y2="6" />
  <line x1="6" y1="18" x2="6.01" y2="18" />
</>);

export const X = icon(<>
  <line x1="18" y1="6" x2="6" y2="18" />
  <line x1="6" y1="6" x2="18" y2="18" />
</>);

export const Check = icon(<polyline points="20 6 9 17 4 12" />);

export const AlertCircle = icon(<>
  <circle cx="12" cy="12" r="10" />
  <line x1="12" y1="8" x2="12" y2="12" />
  <line x1="12" y1="16" x2="12.01" y2="16" />
</>);

export const Filter = icon(<polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3" />);

export const Download = icon(<>
  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
  <polyline points="7 10 12 15 17 10" />
  <line x1="12" y1="15" x2="12" y2="3" />
</>);

export const ArrowUp = icon(<>
  <line x1="12" y1="19" x2="12" y2="5" />
  <polyline points="5 12 12 5 19 12" />
</>);

export const ArrowDown = icon(<>
  <line x1="12" y1="5" x2="12" y2="19" />
  <polyline points="19 12 12 19 5 12" />
</>);

export const Key = icon(<>
  <path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.78 7.78 5.5 5.5 0 0 1 7.78-7.78zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4" />
</>);

export const Play = icon(<polygon points="5 3 19 12 5 21 5 3" />);

export const Loader = ({ className }) => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" className={`animate-spin ${className || ''}`}>
    <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2" opacity="0.2" />
    <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
  </svg>
);

export const Logo = ({ size = 28 }) => (
  <svg width={size} height={size} viewBox="0 0 32 32" fill="none">
    <rect width="32" height="32" rx="8" fill="#00ed64" fillOpacity="0.12" />
    <rect x="0.5" y="0.5" width="31" height="31" rx="7.5" stroke="#00ed64" strokeOpacity="0.2" />
    <path d="M16 6C16 6 16.5 10 16.5 13C16.5 16 16 20.5 16 23C16 23 15.5 20 15.5 17C15.5 14 16 6 16 6Z" fill="#00ed64" />
    <path d="M16 6C16 6 19 9.5 20.5 12.5C22 15.5 22 18 21 20.5C20 23 16 23 16 23" fill="#00ed64" fillOpacity="0.6" />
    <path d="M16 6C16 6 13 9.5 11.5 12.5C10 15.5 10 18 11 20.5C12 23 16 23 16 23" fill="#00ed64" fillOpacity="0.3" />
    <ellipse cx="16" cy="25" rx="2.5" ry="1" fill="#00ed64" fillOpacity="0.15" />
  </svg>
);

export const Disconnect = icon(<>
  <path d="M18.36 5.64a9 9 0 0 1 0 12.73" />
  <path d="M5.64 18.36a9 9 0 0 1 0-12.73" />
  <line x1="2" y1="2" x2="22" y2="22" />
</>);

import { AlertCircle, AlertTriangle, Check, X } from './Icons';

const KIND_CONFIG = {
  error:   { icon: AlertCircle,   text: 'text-red-400',   bg: 'rgba(239,68,68,0.05)',   border: 'rgba(239,68,68,0.2)',   close: 'text-red-400/50'   },
  warning: { icon: AlertTriangle, text: 'text-amber-300', bg: 'rgba(245,158,11,0.08)',  border: 'rgba(245,158,11,0.2)',  close: 'text-amber-200/60' },
  success: { icon: Check,         text: 'text-green-400', bg: 'rgba(16,185,129,0.08)',  border: 'rgba(16,185,129,0.2)', close: 'text-green-400/50' },
};

export default function InlineAlert({ kind = 'error', message, onClose, icon: IconOverride, className = '', actions }) {
  const cfg = KIND_CONFIG[kind] || KIND_CONFIG.error;
  const Icon = IconOverride || cfg.icon;
  return (
    <div
      className={`w-full flex flex-col gap-2 ${cfg.text} text-xs px-4 py-2.5 rounded-lg animate-fade-in ${className}`}
      style={{ background: cfg.bg, border: `1px solid ${cfg.border}` }}
    >
      <div className="grid grid-cols-[auto,1fr,auto] items-center gap-2 min-h-[20px]">
        <Icon className="w-3.5 h-3.5 flex-shrink-0 self-center" />
        <span className="leading-5 min-w-0">{message}</span>
        {onClose && (
          <button className="inline-flex items-center justify-center w-7 h-7 flex-shrink-0 rounded-md hover:bg-white/5" onClick={onClose}>
            <X className={`w-3.5 h-3.5 ${cfg.close}`} />
          </button>
        )}
      </div>
      {actions?.length > 0 && (
        <div className="flex gap-2 ml-[22px]">
          {actions.map((a) => (
            <button
              key={a.label}
              onClick={a.onClick}
              className={`text-2xs px-2.5 py-1 rounded-md font-medium transition-colors ${
                a.primary
                  ? 'bg-amber-400/20 hover:bg-amber-400/30 text-amber-300'
                  : 'bg-white/5 hover:bg-white/10 opacity-70 hover:opacity-100'
              }`}
            >
              {a.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

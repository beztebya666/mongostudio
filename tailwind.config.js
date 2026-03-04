/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        surface: { 0:'var(--surface-0)', 1:'var(--surface-1)', 2:'var(--surface-2)', 3:'var(--surface-3)', 4:'var(--surface-4)', 5:'var(--surface-5)' },
        accent: { DEFAULT:'var(--accent)', dim:'var(--accent-dim)', glow:'var(--accent)22', muted:'var(--accent)15' },
        border: { DEFAULT:'var(--border)', hover:'var(--border-hover)', active:'var(--border-active)' },
        text: { primary:'var(--text-primary)', secondary:'var(--text-secondary)', tertiary:'var(--text-tertiary)' },
      },
      fontFamily: {
        sans: ['"DM Sans"', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', '"Fira Code"', 'monospace'],
        display: ['"Cabinet Grotesk"', '"DM Sans"', 'system-ui', 'sans-serif'],
      },
      fontSize: { '2xs': ['0.65rem', { lineHeight: '1rem' }] },
      animation: {
        'fade-in': 'fadeIn 0.4s ease-out',
        'slide-up': 'slideUp 0.3s ease-out',
        'slide-in-right': 'slideInRight 0.25s ease-out',
        'pulse-glow': 'pulseGlow 2s ease-in-out infinite',
        'shimmer': 'shimmer 2s linear infinite',
      },
      keyframes: {
        fadeIn: { '0%': { opacity:'0' }, '100%': { opacity:'1' } },
        slideUp: { '0%': { opacity:'0', transform:'translateY(8px)' }, '100%': { opacity:'1', transform:'translateY(0)' } },
        slideInRight: { '0%': { opacity:'0', transform:'translateX(-8px)' }, '100%': { opacity:'1', transform:'translateX(0)' } },
        pulseGlow: { '0%,100%': { boxShadow:'0 0 20px var(--accent)11' }, '50%': { boxShadow:'0 0 40px var(--accent)22' } },
        shimmer: { '0%': { backgroundPosition:'-200% 0' }, '100%': { backgroundPosition:'200% 0' } },
      },
    },
  },
  plugins: [],
};

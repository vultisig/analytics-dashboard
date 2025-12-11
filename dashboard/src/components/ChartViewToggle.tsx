'use client';

interface ChartViewToggleProps {
    view: 'provider' | 'platform';
    onViewChange: (view: 'provider' | 'platform') => void;
}

export function ChartViewToggle({ view, onViewChange }: ChartViewToggleProps) {
    return (
        <div className="inline-flex items-center gap-1 glass-card rounded-lg p-1 will-change-blur">
            <button
                type="button"
                onClick={() => onViewChange('provider')}
                className={`
                    px-3 py-1.5 rounded-md text-xs font-semibold transition-all
                    ${view === 'provider'
                        ? 'bg-gradient-to-r from-cyan-500 to-teal-500 text-white shadow-lg shadow-cyan-500/30'
                        : 'text-slate-400 hover:text-slate-300 hover:bg-white/5'
                    }
                `}
                aria-label="View by provider"
                aria-pressed={view === 'provider'}
            >
                By Provider
            </button>
            <button
                type="button"
                onClick={() => onViewChange('platform')}
                className={`
                    px-3 py-1.5 rounded-md text-xs font-semibold transition-all
                    ${view === 'platform'
                        ? 'bg-gradient-to-r from-cyan-500 to-teal-500 text-white shadow-lg shadow-cyan-500/30'
                        : 'text-slate-400 hover:text-slate-300 hover:bg-white/5'
                    }
                `}
                aria-label="View by platform"
                aria-pressed={view === 'platform'}
            >
                By Platform
            </button>
        </div>
    );
}

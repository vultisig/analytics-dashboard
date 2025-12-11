'use client';

interface ViewToggleSwitchProps {
    view: 'platform' | 'chain';
    onViewChange: (view: 'platform' | 'chain') => void;
    disabled?: boolean;
}

export function ViewToggleSwitch({ view, onViewChange, disabled = false }: ViewToggleSwitchProps) {
    return (
        <div className="inline-flex items-center gap-2 glass-card rounded-lg p-1 will-change-blur">
            <button
                type="button"
                onClick={() => onViewChange('platform')}
                disabled={disabled}
                className={`
                    px-4 py-2 rounded-md text-sm font-semibold transition-all
                    ${view === 'platform'
                        ? 'bg-gradient-to-r from-cyan-500 to-teal-500 text-white shadow-lg shadow-cyan-500/30'
                        : 'text-slate-400 hover:text-slate-300 hover:bg-white/5'
                    }
                    ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
                `}
                aria-label="View by platform"
                aria-pressed={view === 'platform'}
            >
                Platform
            </button>
            <button
                type="button"
                onClick={() => onViewChange('chain')}
                disabled={disabled}
                className={`
                    px-4 py-2 rounded-md text-sm font-semibold transition-all
                    ${view === 'chain'
                        ? 'bg-gradient-to-r from-cyan-500 to-teal-500 text-white shadow-lg shadow-cyan-500/30'
                        : 'text-slate-400 hover:text-slate-300 hover:bg-white/5'
                    }
                    ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
                `}
                aria-label="View by chain"
                aria-pressed={view === 'chain'}
            >
                Chain
            </button>
        </div>
    );
}

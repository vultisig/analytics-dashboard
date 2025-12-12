'use client';

import { DollarSign, Users } from 'lucide-react';

interface LeaderboardToggleProps {
    view: 'revenue' | 'referrals';
    onViewChange: (view: 'revenue' | 'referrals') => void;
}

export function LeaderboardToggle({ view, onViewChange }: LeaderboardToggleProps) {
    return (
        <div className="inline-flex items-center glass-card rounded-lg p-0.5 md:p-1 will-change-blur">
            <button
                type="button"
                onClick={() => onViewChange('revenue')}
                className={`
                    flex items-center justify-center gap-1 md:gap-2 px-2 md:px-4 py-1.5 md:py-2 rounded-md text-[11px] md:text-sm font-semibold transition-all
                    ${view === 'revenue'
                        ? 'bg-gradient-to-r from-cyan-500 to-teal-500 text-white shadow-lg shadow-cyan-500/30'
                        : 'text-slate-400 hover:text-slate-300 hover:bg-white/5'
                    }
                `}
                aria-label="Sort by revenue"
                aria-pressed={view === 'revenue' ? 'true' : 'false'}
            >
                <DollarSign className="w-3.5 h-3.5 md:w-4 md:h-4 shrink-0" />
                <span>Revenue</span>
            </button>
            <button
                type="button"
                onClick={() => onViewChange('referrals')}
                className={`
                    flex items-center justify-center gap-1 md:gap-2 px-2 md:px-4 py-1.5 md:py-2 rounded-md text-[11px] md:text-sm font-semibold transition-all
                    ${view === 'referrals'
                        ? 'bg-gradient-to-r from-cyan-500 to-teal-500 text-white shadow-lg shadow-cyan-500/30'
                        : 'text-slate-400 hover:text-slate-300 hover:bg-white/5'
                    }
                `}
                aria-label="Sort by referrals"
                aria-pressed={view === 'referrals' ? 'true' : 'false'}
            >
                <Users className="w-3.5 h-3.5 md:w-4 md:h-4 shrink-0" />
                <span>Referrals</span>
            </button>
        </div>
    );
}

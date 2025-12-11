'use client';

import { BarChart3, Layers, Network } from 'lucide-react';

interface VolumeViewToggleProps {
    view: 'total' | 'breakdown';
    onViewChange: (view: 'total' | 'breakdown') => void;
    platformChainView?: 'platform' | 'chain';
    metricType?: 'volume' | 'revenue' | 'users' | 'count';
}

const metricLabels: Record<string, { full: string; short: string }> = {
    volume: { full: 'Total Volume', short: 'Volume' },
    revenue: { full: 'Total Revenue', short: 'Revenue' },
    users: { full: 'Total Users', short: 'Users' },
    count: { full: 'Total Count', short: 'Count' },
};

export function VolumeViewToggle({ view, onViewChange, platformChainView = 'platform', metricType = 'volume' }: VolumeViewToggleProps) {
    const isChainView = platformChainView === 'chain';
    const BreakdownIcon = isChainView ? Network : Layers;
    const breakdownLabelShort = isChainView ? 'Chain' : 'Platform';
    const breakdownLabelFull = isChainView ? 'By Chain' : 'By Platform';
    const totalLabel = metricLabels[metricType] || metricLabels.volume;

    return (
        <div className="inline-flex items-center glass-card rounded-lg p-0.5 md:p-1 will-change-blur">
            <button
                type="button"
                onClick={() => onViewChange('total')}
                className={`
                    flex items-center justify-center gap-1 md:gap-2 px-2 md:px-4 py-1.5 md:py-2 rounded-md text-[11px] md:text-sm font-semibold transition-all
                    ${view === 'total'
                        ? 'bg-gradient-to-r from-cyan-500 to-teal-500 text-white shadow-lg shadow-cyan-500/30'
                        : 'text-slate-400 hover:text-slate-300 hover:bg-white/5'
                    }
                `}
                aria-label={`${totalLabel.full} view`}
                aria-pressed={view === 'total' ? 'true' : 'false'}
            >
                <BarChart3 className="w-3.5 h-3.5 md:w-4 md:h-4 shrink-0" />
                <span className="hidden md:inline">{totalLabel.full}</span>
                <span className="md:hidden">{totalLabel.short}</span>
            </button>
            <button
                type="button"
                onClick={() => onViewChange('breakdown')}
                className={`
                    flex items-center justify-center gap-1 md:gap-2 px-2 md:px-4 py-1.5 md:py-2 rounded-md text-[11px] md:text-sm font-semibold transition-all
                    ${view === 'breakdown'
                        ? 'bg-gradient-to-r from-cyan-500 to-teal-500 text-white shadow-lg shadow-cyan-500/30'
                        : 'text-slate-400 hover:text-slate-300 hover:bg-white/5'
                    }
                `}
                aria-label={`Volume by ${isChainView ? 'chain' : 'platform'} view`}
                aria-pressed={view === 'breakdown' ? 'true' : 'false'}
            >
                <BreakdownIcon className="w-3.5 h-3.5 md:w-4 md:h-4 shrink-0" />
                <span className="hidden md:inline">{breakdownLabelFull}</span>
                <span className="md:hidden">{breakdownLabelShort}</span>
            </button>
        </div>
    );
}

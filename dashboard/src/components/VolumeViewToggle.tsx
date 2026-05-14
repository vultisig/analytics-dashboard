'use client';

interface VolumeViewToggleProps {
    view: 'total' | 'breakdown';
    onViewChange: (view: 'total' | 'breakdown') => void;
    platformChainView?: 'platform' | 'chain';
    metricType?: 'volume' | 'revenue' | 'users' | 'count';
}

const metricLabels: Record<string, { byProvider: string }> = {
    volume: { byProvider: 'By Provider' },
    revenue: { byProvider: 'By Provider' },
    users: { byProvider: 'By Provider' },
    count: { byProvider: 'By Provider' },
};

export function VolumeViewToggle({
    view,
    onViewChange,
    platformChainView = 'platform',
    metricType = 'volume',
}: VolumeViewToggleProps) {
    const isChainView = platformChainView === 'chain';
    const breakdownLabel = isChainView ? 'By Chain' : 'By Platform';
    const totalLabel = metricLabels[metricType]?.byProvider ?? 'By Provider';

    return (
        <div className="toggle-group" role="group">
            <button
                type="button"
                onClick={() => onViewChange('total')}
                aria-pressed={view === 'total'}
                data-active={view === 'total'}
                className="toggle-group-item"
            >
                {totalLabel}
            </button>
            <button
                type="button"
                onClick={() => onViewChange('breakdown')}
                aria-pressed={view === 'breakdown'}
                data-active={view === 'breakdown'}
                className="toggle-group-item"
            >
                {breakdownLabel}
            </button>
        </div>
    );
}

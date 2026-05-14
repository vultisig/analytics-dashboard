'use client';

interface ChartViewToggleProps {
    view: 'provider' | 'platform';
    onViewChange: (view: 'provider' | 'platform') => void;
}

export function ChartViewToggle({ view, onViewChange }: ChartViewToggleProps) {
    return (
        <div className="toggle-group" role="group">
            <button
                type="button"
                onClick={() => onViewChange('provider')}
                aria-pressed={view === 'provider'}
                data-active={view === 'provider'}
                className="toggle-group-item"
            >
                By Provider
            </button>
            <button
                type="button"
                onClick={() => onViewChange('platform')}
                aria-pressed={view === 'platform'}
                data-active={view === 'platform'}
                className="toggle-group-item"
            >
                By Platform
            </button>
        </div>
    );
}

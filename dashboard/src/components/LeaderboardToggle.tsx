import { IconDollar, IconPeopleCopy } from '@/icons';
'use client';


interface LeaderboardToggleProps {
    view: 'revenue' | 'referrals';
    onViewChange: (view: 'revenue' | 'referrals') => void;
}

export function LeaderboardToggle({ view, onViewChange }: LeaderboardToggleProps) {
    return (
        <div className="toggle-group" role="group">
            <button
                type="button"
                onClick={() => onViewChange('revenue')}
                aria-pressed={view === 'revenue'}
                data-active={view === 'revenue'}
                className="toggle-group-item flex items-center gap-1.5"
            >
                <IconDollar className="size-3.5" />
                <span>Revenue</span>
            </button>
            <button
                type="button"
                onClick={() => onViewChange('referrals')}
                aria-pressed={view === 'referrals'}
                data-active={view === 'referrals'}
                className="toggle-group-item flex items-center gap-1.5"
            >
                <IconPeopleCopy className="size-3.5" />
                <span>Referrals</span>
            </button>
        </div>
    );
}

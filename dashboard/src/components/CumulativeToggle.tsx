'use client';

interface CumulativeToggleProps {
    enabled: boolean;
    onToggle: (enabled: boolean) => void;
}

export function CumulativeToggle({ enabled, onToggle }: CumulativeToggleProps) {
    return (
        <div className="flex items-center gap-3">
            <span className="text-[13px] font-medium leading-[18px] text-[var(--text-secondary)]">Cumulative</span>
            <button
                type="button"
                role="switch"
                aria-checked={enabled}
                aria-label="Toggle cumulative view"
                onClick={() => onToggle(!enabled)}
                className="switch-track"
                data-checked={enabled}
            >
                <span className="switch-thumb" />
            </button>
        </div>
    );
}

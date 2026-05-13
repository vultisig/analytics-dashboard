'use client';

interface NewUsersToggleProps {
    enabled: boolean;
    onToggle: (enabled: boolean) => void;
}

export function NewUsersToggle({ enabled, onToggle }: NewUsersToggleProps) {
    return (
        <div className="flex items-center gap-3">
            <span className="text-[13px] font-medium leading-[18px] text-[var(--text-secondary)]">New Users Only</span>
            <button
                type="button"
                role="switch"
                aria-checked={enabled}
                aria-label="Toggle new users only"
                onClick={() => onToggle(!enabled)}
                className="switch-track"
                data-checked={enabled}
            >
                <span className="switch-thumb" />
            </button>
        </div>
    );
}

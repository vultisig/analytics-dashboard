'use client';

interface CumulativeToggleProps {
    enabled: boolean;
    onToggle: (enabled: boolean) => void;
}

export function CumulativeToggle({ enabled, onToggle }: CumulativeToggleProps) {
    return (
        <div className="flex items-center gap-2 md:gap-3">
            <span className="text-xs md:text-sm font-medium text-slate-300">Cumulative</span>
            <button
                type="button"
                role="switch"
                aria-checked={enabled}
                onClick={() => onToggle(!enabled)}
                className={`
                    relative inline-flex h-5 w-9 md:h-6 md:w-11 items-center rounded-full transition-colors duration-200 ease-in-out
                    ${enabled ? 'bg-cyan-500' : 'bg-slate-600'}
                `}
            >
                <span
                    className={`
                        inline-block h-3.5 w-3.5 md:h-4 md:w-4 transform rounded-full bg-white transition-transform duration-200 ease-in-out
                        ${enabled ? 'translate-x-4 md:translate-x-6' : 'translate-x-1'}
                    `}
                />
            </button>
        </div>
    );
}

'use client';

import { ReactNode, useState } from 'react';
import { IconCircleInfo } from '@/icons';

interface TooltipProps {
    content: string | ReactNode;
    children?: ReactNode;
    iconOnly?: boolean;
    interactive?: boolean;
}

export function Tooltip({ content, children, iconOnly = false, interactive = false }: TooltipProps) {
    const [isVisible, setIsVisible] = useState(false);

    const trigger = iconOnly ? (
        <button
            type="button"
            className="inline-flex items-center justify-center text-[var(--text-tertiary)] hover:text-[var(--text-secondary)] transition-colors"
            onMouseEnter={() => setIsVisible(true)}
            onMouseLeave={() => !interactive && setIsVisible(false)}
            onFocus={() => setIsVisible(true)}
            onBlur={() => !interactive && setIsVisible(false)}
        >
            <IconCircleInfo className="w-4 h-4" />
        </button>
    ) : children;

    return (
        <div
            className="relative inline-block"
            onMouseEnter={() => setIsVisible(true)}
            onMouseLeave={() => setIsVisible(false)}
            onFocus={() => setIsVisible(true)}
            onBlur={() => setIsVisible(false)}
        >
            {trigger}

            {isVisible && (
                <div className={`absolute z-50 bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-[var(--surface-1)]/95 backdrop-blur-sm border border-[var(--border-normal)]/50 rounded-lg shadow-xl text-sm text-[var(--text-primary)] whitespace-nowrap animate-in fade-in duration-200 ${interactive ? '' : 'pointer-events-none'}`}>
                    {content}
                    <div className="absolute top-full left-1/2 -translate-x-1/2 -mt-px">
                        <div className="border-4 border-transparent border-t-[var(--surface-1)]/95" />
                    </div>
                </div>
            )}
        </div>
    );
}

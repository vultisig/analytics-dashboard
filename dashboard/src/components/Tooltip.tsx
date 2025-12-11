'use client';

import { ReactNode, useState } from 'react';
import { Info } from 'lucide-react';

interface TooltipProps {
    content: string | ReactNode;
    children?: ReactNode;
    iconOnly?: boolean;
}

export function Tooltip({ content, children, iconOnly = false }: TooltipProps) {
    const [isVisible, setIsVisible] = useState(false);

    const trigger = iconOnly ? (
        <button
            type="button"
            className="inline-flex items-center justify-center text-slate-400 hover:text-slate-300 transition-colors"
            onMouseEnter={() => setIsVisible(true)}
            onMouseLeave={() => setIsVisible(false)}
            onFocus={() => setIsVisible(true)}
            onBlur={() => setIsVisible(false)}
        >
            <Info className="w-4 h-4" />
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
                <div className="absolute z-50 bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-slate-900/95 backdrop-blur-sm border border-slate-700/50 rounded-lg shadow-xl text-sm text-slate-200 whitespace-nowrap pointer-events-none animate-in fade-in duration-200">
                    {content}
                    <div className="absolute top-full left-1/2 -translate-x-1/2 -mt-px">
                        <div className="border-4 border-transparent border-t-slate-900/95" />
                    </div>
                </div>
            )}
        </div>
    );
}

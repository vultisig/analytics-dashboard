'use client';

import { useState, useEffect } from 'react';
import { ChevronDown } from 'lucide-react';
import { formatProviderName } from '@/lib/providerUtils';

interface ProviderSectionProps {
    provider: string;
    children: React.ReactNode | ((view: 'platform' | 'chain') => React.ReactNode);
    showViewToggle?: boolean;
    defaultCollapsed?: boolean;
}

export function ProviderSection({
    provider,
    children,
    showViewToggle = false,
    defaultCollapsed = false
}: ProviderSectionProps) {
    const [isExpanded, setIsExpanded] = useState(!defaultCollapsed);

    // Determine view based on provider: 1inch uses 'chain', others use 'platform'
    const view = provider.toLowerCase() === '1inch' ? 'chain' : 'platform';

    // Persist expansion state to localStorage
    useEffect(() => {
        const saved = localStorage.getItem(`provider-${provider}-expanded`);
        if (saved !== null) {
            setIsExpanded(JSON.parse(saved));
        }
    }, [provider]);

    useEffect(() => {
        localStorage.setItem(`provider-${provider}-expanded`, JSON.stringify(isExpanded));
    }, [isExpanded, provider]);

    return (
        <div className="glass-card glass-card-hover will-change-blur rounded-xl overflow-hidden">
            {/* Header */}
            <button
                type="button"
                onClick={() => setIsExpanded(!isExpanded)}
                className="w-full flex items-center justify-between p-6 hover:bg-white/5 transition-colors"
                aria-expanded={isExpanded}
                aria-controls={`provider-${provider}-content`}
            >
                <h3 className="text-xl font-bold text-white">
                    {formatProviderName(provider)}
                </h3>
                <ChevronDown
                    className={`w-5 h-5 text-slate-400 transition-transform duration-300 ${
                        isExpanded ? 'rotate-180' : ''
                    }`}
                />
            </button>

            {/* Content */}
            {isExpanded && (
                <div
                    id={`provider-${provider}-content`}
                    className="px-6 pb-6 space-y-6 animate-expand"
                >
                    {typeof children === 'function' ? children(view) : children}
                </div>
            )}
        </div>
    );
}

'use client';

import { useState, useEffect } from 'react';
import { formatProviderName, isArkhamProvider } from '@/lib/providerUtils';
import { IconChevronDownSmall } from '@/icons';

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

    // Arkham-sourced aggregators only carry chain info (no platform attribution).
    const view = isArkhamProvider(provider) ? 'chain' : 'platform';

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
                <h3 className="text-xl font-bold text-[var(--text-primary)]">
                    {formatProviderName(provider)}
                </h3>
                <IconChevronDownSmall
                    className={`w-5 h-5 text-[var(--text-tertiary)] transition-transform duration-300 ${
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

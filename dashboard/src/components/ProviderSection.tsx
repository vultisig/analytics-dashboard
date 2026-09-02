'use client';

import { useState, useEffect } from 'react';
import {
    formatProviderName,
    isDexRevenueProvider,
    SWAPKIT_COVERAGE_TOOLTIP,
    SWAPKIT_PROTOCOL,
} from '@/lib/providerUtils';
import { IconChevronDownSmall } from '@/icons';
import { fetchSwapKitAccruals, type SwapKitAccruals } from '@/lib/api';
import { Tooltip } from './Tooltip';

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

    // Dex-revenue cards break down by chain: their receipts carry no platform.
    // SwapKit's Chainflip rows do carry one, but only the aggregate platform charts show it.
    const view = isDexRevenueProvider(provider) ? 'chain' : 'platform';

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

    const isSwapKit = provider.toLowerCase() === SWAPKIT_PROTOCOL;
    const [accruals, setAccruals] = useState<SwapKitAccruals | null>(null);

    useEffect(() => {
        if (!isSwapKit) return;
        fetchSwapKitAccruals()
            .then(setAccruals)
            .catch((error) => console.error('Error fetching SwapKit accruals:', error));
    }, [isSwapKit]);

    const accrualLine = accruals && accruals.snapshot_at && (
        <p className="text-xs text-[var(--text-tertiary)]">
            Near-Intents accrued, not yet paid out: ~${Math.round(accruals.stable_usd).toLocaleString()} in stables
            {' · '}
            {accruals.platforms.map((p) => `${p.platform} $${Math.round(p.stable_usd).toLocaleString()}`).join(' · ')}
        </p>
    );

    return (
        <div className="glass-card glass-card-hover will-change-blur rounded-xl overflow-hidden">
            <div className="w-full flex items-center justify-between p-6 hover:bg-white/5 transition-colors">
                <div className="flex items-center gap-2 min-w-0">
                    <button
                        type="button"
                        onClick={() => setIsExpanded(!isExpanded)}
                        className="text-left"
                        aria-expanded={isExpanded}
                        aria-controls={`provider-${provider}-content`}
                    >
                        <h3 className="text-xl font-bold text-[var(--text-primary)]">
                            {formatProviderName(provider)}
                        </h3>
                    </button>
                    {isSwapKit && (
                        <Tooltip content={SWAPKIT_COVERAGE_TOOLTIP} iconOnly wide />
                    )}
                </div>
                <button
                    type="button"
                    onClick={() => setIsExpanded(!isExpanded)}
                    className="shrink-0"
                    aria-expanded={isExpanded}
                    aria-controls={`provider-${provider}-content`}
                    aria-label={isExpanded ? 'Collapse provider' : 'Expand provider'}
                >
                    <IconChevronDownSmall
                        className={`w-5 h-5 text-[var(--text-tertiary)] transition-transform duration-300 ${
                            isExpanded ? 'rotate-180' : ''
                        }`}
                    />
                </button>
            </div>

            {/* Content */}
            {isExpanded && (
                <div
                    id={`provider-${provider}-content`}
                    className="px-6 pb-6 space-y-6 animate-expand"
                >
                    {accrualLine}
                    {typeof children === 'function' ? children(view) : children}
                </div>
            )}
        </div>
    );
}

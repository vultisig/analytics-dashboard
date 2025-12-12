'use client';

import { Eye, EyeOff } from 'lucide-react';
import { providerColorMap } from '@/lib/chartStyles';
import { formatProviderName } from '@/lib/providerUtils';


interface ProviderToggleControlProps {
    providers: string[];
    visibleProviders: string[];
    onToggleProvider: (provider: string) => void;
    colors: string[] | Record<string, string>;
}

// Helper to get color from either array or object
function getColor(colors: string[] | Record<string, string>, provider: string, index: number): string {
    if (Array.isArray(colors)) {
        return colors[index % colors.length];
    }
    return colors[provider.toLowerCase()] || colors[provider] || '#64748b';
}

export function ProviderToggleControl({
    providers,
    visibleProviders,
    onToggleProvider,
    colors
}: ProviderToggleControlProps) {
    return (
        <div className="flex flex-wrap items-center gap-1.5 md:gap-2">
            <span className="text-[10px] md:text-xs font-medium text-slate-400 mr-1 md:mr-2">Show/Hide:</span>
            {providers.map((provider, index) => {
                const isVisible = visibleProviders.includes(provider);
                const color = providerColorMap[provider.toLowerCase()] || getColor(colors, provider, index);

                return (
                    <button
                        key={provider}
                        type="button"
                        onClick={() => onToggleProvider(provider)}
                        className={`
                            inline-flex items-center gap-1 md:gap-1.5 px-2 md:px-3 py-1 md:py-1.5 rounded-full text-[10px] md:text-xs font-medium
                            transition-all will-change-blur
                            ${isVisible
                                ? 'glass-card glass-card-hover text-white'
                                : 'bg-slate-800/50 text-slate-500 hover:bg-slate-800'
                            }
                        `}
                        aria-label={`${isVisible ? 'Hide' : 'Show'} ${provider}`}
                        aria-pressed={isVisible}
                    >
                        <div
                            className="w-2 h-2 rounded-full"
                            style={{ backgroundColor: isVisible ? color : '#64748b' }}
                        />
                        <span>{formatProviderName(provider)}</span>
                        {isVisible ? (
                            <Eye className="w-3 h-3" />
                        ) : (
                            <EyeOff className="w-3 h-3" />
                        )}
                    </button>
                );
            })}
        </div>
    );
}

'use client';

import { useState } from 'react';
import { buildApiUrl } from '@/lib/api';
import { IconAlertCircleV, IconAwardV, IconLoader2V, IconMagnifyingGlass2, IconShieldV, IconTrendingUpV, IconXV } from '@/icons';

interface LookupResult {
    found: boolean;
    address?: string;
    vultBalance?: number;
    hasThorguard?: boolean;
    baseTier?: string;
    effectiveTier?: string;
    discount?: number;
    rank?: number;
    totalHolders?: number;
    message?: string;
}

// Tier color configurations
const tierColors: Record<string, { text: string; bg: string }> = {
    Ultimate: { text: 'text-purple-400', bg: 'bg-purple-500/20' },
    Diamond: { text: 'text-[var(--brand-blue-light)]', bg: 'bg-cyan-400/20' },
    Platinum: { text: 'text-[var(--text-primary)]', bg: 'bg-slate-300/20' },
    Gold: { text: 'text-[var(--alert-warn)]', bg: 'bg-yellow-500/20' },
    Silver: { text: 'text-[var(--text-secondary)]', bg: 'bg-slate-400/20' },
    Bronze: { text: 'text-orange-400', bg: 'bg-orange-600/20' },
    None: { text: 'text-[var(--text-tertiary)]', bg: 'bg-slate-600/20' },
};

function formatNumber(value: number): string {
    if (value >= 1000000) return `${(value / 1000000).toFixed(2)}M`;
    if (value >= 1000) return `${(value / 1000).toFixed(1)}K`;
    return value.toLocaleString();
}

function isValidEthereumAddress(address: string): boolean {
    return /^0x[a-fA-F0-9]{40}$/.test(address);
}

export function TierLookup() {
    const [address, setAddress] = useState('');
    const [result, setResult] = useState<LookupResult | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [isFocused, setIsFocused] = useState(false);

    const handleLookup = async () => {
        if (!address.trim()) {
            setError('Please enter an Ethereum address');
            return;
        }

        if (!isValidEthereumAddress(address.trim())) {
            setError('Invalid Ethereum address format. Must start with 0x followed by 40 hex characters.');
            return;
        }

        setIsLoading(true);
        setError(null);
        setResult(null);

        try {
            const url = buildApiUrl(`/api/holders/lookup?address=${encodeURIComponent(address.trim())}`);
            const response = await fetch(url);

            if (response.status === 429) {
                const data = await response.json();
                setError(data.message || 'Rate limit exceeded. Please try again later.');
                return;
            }

            if (!response.ok) {
                throw new Error('Failed to lookup address');
            }

            const data: LookupResult = await response.json();
            setResult(data);
        } catch (err) {
            setError('Failed to lookup address. Please try again.');
            console.error('Lookup error:', err);
        } finally {
            setIsLoading(false);
        }
    };

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter') {
            handleLookup();
        }
    };

    const clearSearch = () => {
        setAddress('');
        setResult(null);
        setError(null);
    };

    const tierColor = result?.effectiveTier ? tierColors[result.effectiveTier] : tierColors.None;

    return (
        <div className="space-y-4">
            {/* IconMagnifyingGlass2 Input */}
            <div className={`
                relative flex items-center rounded-lg bg-white/5 border transition-all
                ${isFocused ? 'border-[rgba(33,85,223,0.50)] ring-1 ring-[rgba(33,85,223,0.20)]' : 'border-white/10'}
            `}>
                <IconMagnifyingGlass2 className="w-5 h-5 text-[var(--text-tertiary)] ml-4 shrink-0" />
                <input
                    type="text"
                    value={address}
                    onChange={(e) => setAddress(e.target.value)}
                    onKeyDown={handleKeyDown}
                    onFocus={() => setIsFocused(true)}
                    onBlur={() => setIsFocused(false)}
                    placeholder="Enter Ethereum address (0x...)"
                    className="flex-1 bg-transparent px-3 py-3 text-sm text-[var(--text-primary)] placeholder-slate-500 focus:outline-none font-mono"
                />
                {address && (
                    <button
                        type="button"
                        onClick={clearSearch}
                        className="p-2 mr-1 text-[var(--text-tertiary)] hover:text-[var(--text-primary)] transition-colors"
                        aria-label="Clear search"
                    >
                        <IconXV className="w-4 h-4" />
                    </button>
                )}
                <button
                    type="button"
                    onClick={handleLookup}
                    disabled={isLoading || !address.trim()}
                    className={`
                        px-4 py-2 mr-2 rounded-md text-sm font-medium transition-all
                        ${isLoading || !address.trim()
                            ? 'bg-[var(--surface-3)] text-[var(--text-tertiary)] cursor-not-allowed'
                            : 'bg-gradient-to-r from-[var(--brand-blue)] to-[var(--brand-blue-light)] text-[var(--text-primary)] hover:from-[var(--brand-blue-hover)] hover:to-[var(--brand-blue-light)]'
                        }
                    `}
                >
                    {isLoading ? (
                        <IconLoader2V className="w-4 h-4 animate-spin" />
                    ) : (
                        'Lookup'
                    )}
                </button>
            </div>

            {/* Error Message */}
            {error && (
                <div className="flex items-center gap-2 p-3 rounded-lg bg-red-500/10 border border-red-500/20 text-[var(--alert-error)] text-sm">
                    <IconAlertCircleV className="w-4 h-4 shrink-0" />
                    {error}
                </div>
            )}

            {/* Result Card */}
            {result && (
                <div className={`
                    p-4 rounded-lg border transition-all
                    ${result.found
                        ? `bg-gradient-to-r ${tierColor.bg} border-white/10`
                        : 'bg-[var(--surface-2)]/60 border-[var(--border-normal)]/50'
                    }
                `}>
                    {result.found ? (
                        <div className="space-y-4">
                            {/* Address */}
                            <div>
                                <p className="text-xs text-[var(--text-tertiary)] mb-1">Address</p>
                                <p className="text-sm font-mono text-[var(--text-secondary)] break-all">
                                    {result.address}
                                </p>
                            </div>

                            {/* Main Stats Row */}
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                                {/* Tier */}
                                <div>
                                    <p className="text-xs text-[var(--text-tertiary)] mb-1 flex items-center gap-1">
                                        <IconAwardV className="w-3 h-3" />
                                        Tier
                                    </p>
                                    <p className={`text-xl font-bold ${tierColor.text}`}>
                                        {result.effectiveTier}
                                    </p>
                                    {result.baseTier !== result.effectiveTier && (
                                        <p className="text-xs text-[var(--alert-success)] mt-0.5">
                                            Boosted from {result.baseTier}
                                        </p>
                                    )}
                                </div>

                                {/* VULT Balance */}
                                <div>
                                    <p className="text-xs text-[var(--text-tertiary)] mb-1 flex items-center gap-1">
                                        <IconTrendingUpV className="w-3 h-3" />
                                        Balance
                                    </p>
                                    <p className="text-xl font-bold text-[var(--text-primary)]">
                                        {formatNumber(result.vultBalance || 0)}
                                    </p>
                                    <p className="text-xs text-[var(--text-tertiary)]">VULT</p>
                                </div>

                                {/* Discount */}
                                <div>
                                    <p className="text-xs text-[var(--text-tertiary)] mb-1">Discount</p>
                                    <p className="text-xl font-bold text-[var(--brand-blue-light)]">
                                        {result.discount || 0} bps
                                    </p>
                                    <p className="text-xs text-[var(--text-tertiary)]">
                                        {result.discount === 50 ? '100% off' : `${((result.discount || 0) / 50 * 100).toFixed(0)}% off`}
                                    </p>
                                </div>

                                {/* Rank */}
                                <div>
                                    <p className="text-xs text-[var(--text-tertiary)] mb-1">Rank</p>
                                    <p className="text-xl font-bold text-[var(--text-primary)]">
                                        #{result.rank?.toLocaleString()}
                                    </p>
                                    <p className="text-xs text-[var(--text-tertiary)]">
                                        of {result.totalHolders?.toLocaleString()}
                                    </p>
                                </div>
                            </div>

                            {/* THORGuard Badge */}
                            {result.hasThorguard && (
                                <div className="flex items-center gap-2 p-2 rounded-lg bg-emerald-500/10 border border-emerald-500/20">
                                    <IconShieldV className="w-4 h-4 text-[var(--alert-success)]" />
                                    <span className="text-sm text-[var(--alert-success)] font-medium">
                                        THORGuard NFT Holder
                                    </span>
                                    <span className="text-xs text-[var(--alert-success)]/70">
                                        (+1 tier boost applied)
                                    </span>
                                </div>
                            )}
                        </div>
                    ) : (
                        <div className="text-center py-4">
                            <p className="text-[var(--text-tertiary)]">{result.message}</p>
                        </div>
                    )}
                </div>
            )}

            {/* Helper Text */}
            <p className="text-xs text-[var(--text-tertiary)] text-center">
                Enter your Ethereum address to check your VULT tier and discount level.
            </p>
        </div>
    );
}

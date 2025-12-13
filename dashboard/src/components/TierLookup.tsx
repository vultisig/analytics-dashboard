'use client';

import { useState } from 'react';
import { Search, X, Shield, Award, TrendingUp, AlertCircle, Loader2 } from 'lucide-react';
import { buildApiUrl } from '@/lib/api';

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
    Diamond: { text: 'text-cyan-300', bg: 'bg-cyan-400/20' },
    Platinum: { text: 'text-slate-200', bg: 'bg-slate-300/20' },
    Gold: { text: 'text-yellow-400', bg: 'bg-yellow-500/20' },
    Silver: { text: 'text-slate-300', bg: 'bg-slate-400/20' },
    Bronze: { text: 'text-orange-400', bg: 'bg-orange-600/20' },
    None: { text: 'text-slate-500', bg: 'bg-slate-600/20' },
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
            {/* Search Input */}
            <div className={`
                relative flex items-center rounded-lg bg-white/5 border transition-all
                ${isFocused ? 'border-cyan-500/50 ring-1 ring-cyan-500/20' : 'border-white/10'}
            `}>
                <Search className="w-5 h-5 text-slate-400 ml-4 shrink-0" />
                <input
                    type="text"
                    value={address}
                    onChange={(e) => setAddress(e.target.value)}
                    onKeyDown={handleKeyDown}
                    onFocus={() => setIsFocused(true)}
                    onBlur={() => setIsFocused(false)}
                    placeholder="Enter Ethereum address (0x...)"
                    className="flex-1 bg-transparent px-3 py-3 text-sm text-white placeholder-slate-500 focus:outline-none font-mono"
                />
                {address && (
                    <button
                        type="button"
                        onClick={clearSearch}
                        className="p-2 mr-1 text-slate-400 hover:text-white transition-colors"
                        aria-label="Clear search"
                    >
                        <X className="w-4 h-4" />
                    </button>
                )}
                <button
                    type="button"
                    onClick={handleLookup}
                    disabled={isLoading || !address.trim()}
                    className={`
                        px-4 py-2 mr-2 rounded-md text-sm font-medium transition-all
                        ${isLoading || !address.trim()
                            ? 'bg-slate-700 text-slate-500 cursor-not-allowed'
                            : 'bg-gradient-to-r from-cyan-500 to-teal-500 text-white hover:from-cyan-600 hover:to-teal-600'
                        }
                    `}
                >
                    {isLoading ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                        'Lookup'
                    )}
                </button>
            </div>

            {/* Error Message */}
            {error && (
                <div className="flex items-center gap-2 p-3 rounded-lg bg-red-500/10 border border-red-500/20 text-red-400 text-sm">
                    <AlertCircle className="w-4 h-4 shrink-0" />
                    {error}
                </div>
            )}

            {/* Result Card */}
            {result && (
                <div className={`
                    p-4 rounded-lg border transition-all
                    ${result.found
                        ? `bg-gradient-to-r ${tierColor.bg} border-white/10`
                        : 'bg-slate-800/50 border-slate-700/50'
                    }
                `}>
                    {result.found ? (
                        <div className="space-y-4">
                            {/* Address */}
                            <div>
                                <p className="text-xs text-slate-500 mb-1">Address</p>
                                <p className="text-sm font-mono text-slate-300 break-all">
                                    {result.address}
                                </p>
                            </div>

                            {/* Main Stats Row */}
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                                {/* Tier */}
                                <div>
                                    <p className="text-xs text-slate-500 mb-1 flex items-center gap-1">
                                        <Award className="w-3 h-3" />
                                        Tier
                                    </p>
                                    <p className={`text-xl font-bold ${tierColor.text}`}>
                                        {result.effectiveTier}
                                    </p>
                                    {result.baseTier !== result.effectiveTier && (
                                        <p className="text-xs text-emerald-400 mt-0.5">
                                            Boosted from {result.baseTier}
                                        </p>
                                    )}
                                </div>

                                {/* VULT Balance */}
                                <div>
                                    <p className="text-xs text-slate-500 mb-1 flex items-center gap-1">
                                        <TrendingUp className="w-3 h-3" />
                                        Balance
                                    </p>
                                    <p className="text-xl font-bold text-white">
                                        {formatNumber(result.vultBalance || 0)}
                                    </p>
                                    <p className="text-xs text-slate-500">VULT</p>
                                </div>

                                {/* Discount */}
                                <div>
                                    <p className="text-xs text-slate-500 mb-1">Discount</p>
                                    <p className="text-xl font-bold text-cyan-400">
                                        {result.discount || 0} bps
                                    </p>
                                    <p className="text-xs text-slate-500">
                                        {result.discount === 50 ? '100% off' : `${((result.discount || 0) / 50 * 100).toFixed(0)}% off`}
                                    </p>
                                </div>

                                {/* Rank */}
                                <div>
                                    <p className="text-xs text-slate-500 mb-1">Rank</p>
                                    <p className="text-xl font-bold text-white">
                                        #{result.rank?.toLocaleString()}
                                    </p>
                                    <p className="text-xs text-slate-500">
                                        of {result.totalHolders?.toLocaleString()}
                                    </p>
                                </div>
                            </div>

                            {/* THORGuard Badge */}
                            {result.hasThorguard && (
                                <div className="flex items-center gap-2 p-2 rounded-lg bg-emerald-500/10 border border-emerald-500/20">
                                    <Shield className="w-4 h-4 text-emerald-400" />
                                    <span className="text-sm text-emerald-400 font-medium">
                                        THORGuard NFT Holder
                                    </span>
                                    <span className="text-xs text-emerald-400/70">
                                        (+1 tier boost applied)
                                    </span>
                                </div>
                            )}
                        </div>
                    ) : (
                        <div className="text-center py-4">
                            <p className="text-slate-400">{result.message}</p>
                        </div>
                    )}
                </div>
            )}

            {/* Helper Text */}
            <p className="text-xs text-slate-500 text-center">
                Enter your Ethereum address to check your VULT tier and discount level.
            </p>
        </div>
    );
}

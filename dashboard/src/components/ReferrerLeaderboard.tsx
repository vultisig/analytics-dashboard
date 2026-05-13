'use client';

import { useState, useMemo } from 'react';
import { ChartCard } from './ChartCard';
import { LeaderboardToggle } from './LeaderboardToggle';
import { Trophy, DollarSign, Users, TrendingUp, ChevronLeft, ChevronRight, Search, X, ExternalLink } from 'lucide-react';

interface ReferrerData {
    referrerCode: string;
    totalRevenue: number;
    uniqueUsers: number;
    referralCount: number;
    totalVolume: number;
}

interface ReferrerLeaderboardProps {
    dataByRevenue: ReferrerData[];
    dataByReferrals: ReferrerData[];
}

const ITEMS_PER_PAGE = 10;

// Format currency values
function formatCurrency(value: number): string {
    if (value >= 1000000) return `$${(value / 1000000).toFixed(2)}M`;
    if (value >= 1000) return `$${(value / 1000).toFixed(1)}K`;
    return `$${value.toFixed(2)}`;
}

// Format large numbers
function formatNumber(value: number): string {
    if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
    if (value >= 1000) return `${(value / 1000).toFixed(1)}K`;
    return value.toLocaleString();
}

// Get rank badge color based on position
function getRankColor(index: number): string {
    switch (index) {
        case 0:
            return 'from-yellow-500/30 to-amber-500/30 text-[var(--alert-warn)]'; // Gold
        case 1:
            return 'from-slate-400/30 to-slate-500/30 text-[var(--text-secondary)]'; // Silver
        case 2:
            return 'from-orange-600/30 to-amber-700/30 text-orange-400'; // Bronze
        default:
            return 'from-[rgba(33,85,223,0.20)] to-[rgba(33,85,223,0.20)] text-[var(--brand-blue-light)]';
    }
}

export function ReferrerLeaderboard({ dataByRevenue, dataByReferrals }: ReferrerLeaderboardProps) {
    const [view, setView] = useState<'revenue' | 'referrals'>('revenue');
    const [currentPage, setCurrentPage] = useState(1);
    const [searchQuery, setSearchQuery] = useState('');
    const [isSearchFocused, setIsSearchFocused] = useState(false);

    const data = view === 'revenue' ? dataByRevenue : dataByReferrals;

    // Find search result with ranking
    const searchResult = useMemo(() => {
        if (!searchQuery.trim()) return null;
        const query = searchQuery.toLowerCase().trim();
        const index = data.findIndex(item =>
            item.referrerCode.toLowerCase().includes(query)
        );
        if (index === -1) return null;
        return {
            item: data[index],
            rank: index + 1,
            pageNumber: Math.ceil((index + 1) / ITEMS_PER_PAGE)
        };
    }, [data, searchQuery]);

    const totalPages = Math.ceil(data.length / ITEMS_PER_PAGE);
    const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
    const endIndex = startIndex + ITEMS_PER_PAGE;
    const currentData = data.slice(startIndex, endIndex);

    // Reset page when view changes
    const handleViewChange = (newView: 'revenue' | 'referrals') => {
        setView(newView);
        setCurrentPage(1);
    };

    // Jump to search result's page
    const jumpToResult = () => {
        if (searchResult) {
            setCurrentPage(searchResult.pageNumber);
        }
    };

    // Clear search
    const clearSearch = () => {
        setSearchQuery('');
    };

    if (!data || data.length === 0) {
        return (
            <ChartCard
                title="Referrer Leaderboard"
                subtitle="Top performing referrers"
                icon={Trophy}
            >
                <div className="flex items-center justify-center h-48 text-[var(--text-tertiary)]">
                    No referral data available for this period
                </div>
            </ChartCard>
        );
    }

    return (
        <ChartCard
            title="Referrer Leaderboard"
            subtitle={view === 'revenue' ? 'Ranked by total revenue earned' : 'Ranked by unique users referred'}
            icon={Trophy}
            action={<LeaderboardToggle view={view} onViewChange={handleViewChange} />}
        >
            {/* Search Input */}
            <div className="mt-4 mb-3">
                <div className={`
                    relative flex items-center rounded-lg bg-white/5 border transition-all
                    ${isSearchFocused ? 'border-[rgba(33,85,223,0.50)] ring-1 ring-[rgba(33,85,223,0.20)]' : 'border-white/10'}
                `}>
                    <Search className="w-4 h-4 text-[var(--text-tertiary)] ml-3 shrink-0" />
                    <input
                        type="text"
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        onFocus={() => setIsSearchFocused(true)}
                        onBlur={() => setIsSearchFocused(false)}
                        placeholder="Search referrer code..."
                        className="flex-1 bg-transparent px-3 py-2 text-sm text-[var(--text-primary)] placeholder-slate-500 focus:outline-none"
                    />
                    {searchQuery && (
                        <button
                            type="button"
                            onClick={clearSearch}
                            className="p-1.5 mr-1 text-[var(--text-tertiary)] hover:text-[var(--text-primary)] transition-colors"
                            aria-label="Clear search"
                        >
                            <X className="w-4 h-4" />
                        </button>
                    )}
                </div>

                {/* Search Result Card */}
                {searchQuery && (
                    <div className="mt-2">
                        {searchResult ? (
                            <div className="p-3 rounded-lg bg-gradient-to-r from-[rgba(33,85,223,0.10)] to-[rgba(33,85,223,0.10)] border border-[rgba(33,85,223,0.20)]">
                                <div className="flex items-center justify-between gap-2">
                                    <div className="flex items-center gap-2 min-w-0">
                                        <div className={`
                                            w-7 h-7 rounded-full bg-gradient-to-r flex items-center justify-center shrink-0
                                            ${getRankColor(searchResult.rank - 1)}
                                        `}>
                                            <span className="text-xs font-bold">
                                                {searchResult.rank}
                                            </span>
                                        </div>
                                        <div className="min-w-0">
                                            <p className="text-[var(--text-primary)] font-medium truncate text-sm">
                                                {searchResult.item.referrerCode}
                                            </p>
                                            <p className="text-xs text-[var(--text-tertiary)]">
                                                Rank #{searchResult.rank} of {data.length}
                                            </p>
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-3 shrink-0">
                                        <div className="text-right">
                                            <p className="text-sm font-bold text-[var(--brand-blue-light)]">
                                                {view === 'revenue'
                                                    ? formatCurrency(searchResult.item.totalRevenue)
                                                    : formatNumber(searchResult.item.uniqueUsers)
                                                }
                                            </p>
                                            <p className="text-[10px] text-[var(--text-tertiary)]">
                                                {view === 'revenue' ? 'Revenue' : 'Users'}
                                            </p>
                                        </div>
                                        {searchResult.pageNumber !== currentPage && (
                                            <button
                                                type="button"
                                                onClick={jumpToResult}
                                                className="px-2 py-1 text-xs font-medium rounded bg-[rgba(33,85,223,0.20)] text-[var(--brand-blue-light)] hover:bg-[rgba(33,85,223,0.30)] transition-colors"
                                            >
                                                Go to page {searchResult.pageNumber}
                                            </button>
                                        )}
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <p className="text-sm text-[var(--text-tertiary)] px-1">
                                No referrer found matching "{searchQuery}"
                            </p>
                        )}
                    </div>
                )}
            </div>

            <div className="space-y-2">
                {currentData.map((item, index) => {
                    const globalIndex = startIndex + index;
                    const isSearchMatch = searchResult && item.referrerCode === searchResult.item.referrerCode;
                    return (
                        <div
                            key={item.referrerCode}
                            className={`
                                flex items-center gap-2 md:gap-3 p-2 md:p-3 rounded-lg transition-colors
                                ${isSearchMatch
                                    ? 'bg-[rgba(33,85,223,0.20)] ring-1 ring-[rgba(33,85,223,0.40)]'
                                    : 'bg-white/5 hover:bg-white/10'
                                }
                            `}
                        >
                            {/* Rank Badge */}
                            <div className={`
                                w-7 h-7 md:w-8 md:h-8 rounded-full bg-gradient-to-r flex items-center justify-center shrink-0
                                ${getRankColor(globalIndex)}
                            `}>
                                <span className="text-xs md:text-sm font-bold">
                                    {globalIndex + 1}
                                </span>
                            </div>

                            {/* Referrer Code + Stats */}
                            <div className="flex-1 min-w-0 overflow-hidden">
                                <p className="text-[var(--text-primary)] font-medium truncate text-sm md:text-base">
                                    {item.referrerCode}
                                </p>
                                <div className="flex items-center gap-2 md:gap-3 text-[10px] md:text-xs text-[var(--text-tertiary)]">
                                    <span className="flex items-center gap-0.5 md:gap-1">
                                        <TrendingUp className="w-2.5 h-2.5 md:w-3 md:h-3 shrink-0" />
                                        <span className="truncate">{formatNumber(item.referralCount)}</span>
                                    </span>
                                    <span className="flex items-center gap-0.5 md:gap-1">
                                        <DollarSign className="w-2.5 h-2.5 md:w-3 md:h-3 shrink-0" />
                                        <span className="truncate">{formatCurrency(item.totalVolume)}</span>
                                    </span>
                                </div>
                            </div>

                            {/* Primary Metric */}
                            <div className="text-right shrink-0">
                                <p className="text-sm md:text-lg font-bold text-[var(--brand-blue-light)]">
                                    {view === 'revenue'
                                        ? formatCurrency(item.totalRevenue)
                                        : formatNumber(item.uniqueUsers)
                                    }
                                </p>
                                <p className="text-[10px] md:text-xs text-[var(--text-tertiary)]">
                                    {view === 'revenue' ? 'Revenue' : 'Users'}
                                </p>
                            </div>

                            {/* Secondary Metric - hidden on mobile */}
                            <div className="hidden sm:block text-right shrink-0 w-16">
                                <p className="text-sm font-medium text-[var(--text-secondary)]">
                                    {view === 'revenue'
                                        ? formatNumber(item.uniqueUsers)
                                        : formatCurrency(item.totalRevenue)
                                    }
                                </p>
                                <p className="text-xs text-[var(--text-tertiary)]">
                                    {view === 'revenue' ? 'Users' : 'Revenue'}
                                </p>
                            </div>
                        </div>
                    );
                })}
            </div>

            {/* Pagination Controls */}
            {totalPages > 1 && (
                <div className="flex items-center justify-between mt-4 pt-4 border-t border-white/10">
                    <p className="text-xs text-[var(--text-tertiary)]">
                        Showing {startIndex + 1}-{Math.min(endIndex, data.length)} of {data.length} referrers
                    </p>
                    <div className="flex items-center gap-2">
                        <button
                            type="button"
                            onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                            disabled={currentPage === 1}
                            className={`
                                p-1.5 rounded-md transition-all
                                ${currentPage === 1
                                    ? 'text-[var(--text-tertiary)] cursor-not-allowed'
                                    : 'text-[var(--text-tertiary)] hover:text-[var(--text-primary)] hover:bg-white/10'
                                }
                            `}
                            aria-label="Previous page"
                        >
                            <ChevronLeft className="w-4 h-4" />
                        </button>
                        <div className="flex items-center gap-1">
                            {Array.from({ length: totalPages }, (_, i) => i + 1).map(page => (
                                <button
                                    type="button"
                                    key={page}
                                    onClick={() => setCurrentPage(page)}
                                    className={`
                                        w-7 h-7 rounded-md text-xs font-medium transition-all
                                        ${page === currentPage
                                            ? 'bg-gradient-to-r from-[var(--brand-blue)] to-[var(--brand-blue-light)] text-[var(--text-primary)]'
                                            : 'text-[var(--text-tertiary)] hover:text-[var(--text-primary)] hover:bg-white/10'
                                        }
                                    `}
                                >
                                    {page}
                                </button>
                            ))}
                        </div>
                        <button
                            type="button"
                            onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                            disabled={currentPage === totalPages}
                            className={`
                                p-1.5 rounded-md transition-all
                                ${currentPage === totalPages
                                    ? 'text-[var(--text-tertiary)] cursor-not-allowed'
                                    : 'text-[var(--text-tertiary)] hover:text-[var(--text-primary)] hover:bg-white/10'
                                }
                            `}
                            aria-label="Next page"
                        >
                            <ChevronRight className="w-4 h-4" />
                        </button>
                    </div>
                </div>
            )}

            {/* Documentation link */}
            <div className="mt-4 pt-4 border-t border-white/10 flex justify-center">
                <a
                    href="https://docs.vultisig.com/vultisig-app-actions/vultisig-referrals"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 text-xs text-[var(--text-tertiary)] hover:text-[var(--brand-blue-light)] transition-colors"
                >
                    Learn how to become a referrer
                    <ExternalLink className="w-3 h-3" />
                </a>
            </div>
        </ChartCard>
    );
}

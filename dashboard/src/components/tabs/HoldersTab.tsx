'use client';

import { useState, useEffect } from 'react';
import { HeroMetric } from '@/components/HeroMetric';
import { ChartCard } from '@/components/ChartCard';
import { TierCard } from '@/components/TierCard';
import { TierLookup } from '@/components/TierLookup';
import { Users, Shield, Award, Search, Info, ExternalLink } from 'lucide-react';

interface TierData {
    tier: string;
    count: number;
    avgBalance: number;
    thorguardBoosted: number;
}

interface HoldersData {
    tiers: TierData[];
    totalHolders: number;
    totalSupplyHeld: number;
    thorguardHolders: number;
    tieredHolders: number;
    lastUpdated: string;
}

// Tier requirements and discounts
const tierInfo: Record<string, { requirement: number; discount: number }> = {
    Ultimate: { requirement: 1_000_000, discount: 50 },
    Diamond: { requirement: 100_000, discount: 35 },
    Platinum: { requirement: 15_000, discount: 25 },
    Gold: { requirement: 7_500, discount: 20 },
    Silver: { requirement: 3_000, discount: 10 },
    Bronze: { requirement: 1_500, discount: 5 },
    None: { requirement: 0, discount: 0 },
};

// Format time in local timezone
function formatTimeLocal(isoString: string): string {
    if (!isoString || isoString === '1970-01-01T00:00:00Z') {
        return 'Never';
    }
    try {
        const date = new Date(isoString);
        return date.toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            timeZoneName: 'short',
        });
    } catch {
        return 'Unknown';
    }
}

// Format time in UTC
function formatTimeUTC(isoString: string): string {
    if (!isoString || isoString === '1970-01-01T00:00:00Z') {
        return 'Never';
    }
    try {
        const date = new Date(isoString);
        return date.toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            timeZone: 'UTC',
        }) + ' UTC';
    } catch {
        return 'Unknown';
    }
}

// Get next update time (00:00 UTC) in local timezone
function getNextUpdateLocal(): string {
    const now = new Date();
    const nextUpdate = new Date(Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate() + 1,
        0, 0, 0
    ));
    return nextUpdate.toLocaleString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
        timeZoneName: 'short',
    });
}

export function HoldersTab() {
    const [data, setData] = useState<HoldersData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [showUTC, setShowUTC] = useState(false);

    useEffect(() => {
        async function fetchData() {
            setLoading(true);
            setError(null);

            try {
                const res = await fetch('/api/holders');
                if (!res.ok) throw new Error('Failed to fetch holders data');

                const holdersData = await res.json();
                setData(holdersData);
            } catch (err) {
                console.error('Error fetching holders data:', err);
                setError('Failed to load holders data');
            } finally {
                setLoading(false);
            }
        }

        fetchData();
    }, []);

    // Error state
    if (error && !data) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-red-400 text-lg">{error}</div>
            </div>
        );
    }

    // Loading state
    if (loading && !data) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-slate-400 text-lg">Loading holders data...</div>
            </div>
        );
    }

    // No data state
    if (!data) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-slate-400 text-lg">No data available</div>
            </div>
        );
    }

    // Calculate supply held percentage (assuming 1B total supply)
    const totalSupply = 1_000_000_000;
    const supplyHeldPercent = (data.totalSupplyHeld / totalSupply) * 100;

    // Filter tiers with holders for display (excluding None for tier cards)
    const tiersWithHolders = data.tiers.filter(t => t.tier !== 'None');

    return (
        <div className="space-y-6">
            {/* Hero Metrics */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 md:gap-6">
                <HeroMetric
                    label="Total Holders"
                    value={data.totalHolders}
                    icon={Users}
                    color="cyan"
                    format="number"
                    tooltip="Total number of addresses holding VULT tokens"
                />
                <HeroMetric
                    label="Tiered Holders"
                    value={data.tieredHolders}
                    icon={Award}
                    color="purple"
                    format="number"
                    tooltip="Number of holders with Bronze tier or higher"
                />
                <HeroMetric
                    label="Supply Held"
                    value={supplyHeldPercent}
                    icon={Award}
                    color="blue"
                    format="percent"
                    tooltip="Percentage of total supply held by tiered holders (Bronze+)"
                />
                <HeroMetric
                    label="THORGuard Holders"
                    value={data.thorguardHolders}
                    icon={Shield}
                    color="teal"
                    format="number"
                    tooltip="Number of VULT holders who also hold a THORGuard NFT"
                />
            </div>

            {/* Tier Distribution */}
            <ChartCard
                title="Tier Distribution"
                subtitle="Holders by discount tier"
                icon={Award}
            >
                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
                    {tiersWithHolders.map((tier) => (
                        <TierCard
                            key={tier.tier}
                            tier={tier.tier}
                            count={tier.count}
                            avgBalance={tier.avgBalance}
                            thorguardBoosted={tier.thorguardBoosted}
                            requirement={tierInfo[tier.tier]?.requirement || 0}
                            discount={tierInfo[tier.tier]?.discount || 0}
                        />
                    ))}
                </div>
            </ChartCard>

            {/* Address Lookup */}
            <ChartCard
                title="Check Your Tier"
                subtitle="Enter your Ethereum address to see your VULT tier"
                icon={Search}
            >
                <TierLookup />
            </ChartCard>

            {/* Update Notice */}
            <div className="flex flex-col items-center gap-1 text-xs text-slate-500">
                <div className="flex items-center gap-2">
                    <Info className="w-3.5 h-3.5" />
                    <span>Data updates daily at {showUTC ? '00:00 UTC' : getNextUpdateLocal()}</span>
                </div>
                <button
                    type="button"
                    onClick={() => setShowUTC(!showUTC)}
                    className="text-slate-400 hover:text-cyan-400 transition-colors underline decoration-dotted underline-offset-2"
                    title={`Click to show in ${showUTC ? 'local time' : 'UTC'}`}
                >
                    Last updated: {showUTC ? formatTimeUTC(data.lastUpdated) : formatTimeLocal(data.lastUpdated)}
                </button>
            </div>

            {/* Documentation Link */}
            <div className="flex justify-center">
                <a
                    href="https://docs.vultisig.com/vultisig-token/vult/in-app-utility"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-1 text-xs text-slate-400 hover:text-cyan-400 transition-colors"
                >
                    Learn about $VULT discount tiers
                    <ExternalLink className="w-3 h-3" />
                </a>
            </div>
        </div>
    );
}

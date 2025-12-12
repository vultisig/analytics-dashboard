'use client';

import { useState, useEffect } from 'react';
import { HeroMetric } from '@/components/HeroMetric';
import { ReferrerLeaderboard } from '@/components/ReferrerLeaderboard';
import { PiggyBank, DollarSign, Users, TrendingUp, Info } from 'lucide-react';
import { providerColors } from '@/lib/chartStyles';
import { SHORT_PARAMS } from '@/lib/urlParams';

interface ReferralsTabProps {
    range: string;
    startDate?: string | null;
    endDate?: string | null;
    granularity: string;
}

interface ReferralData {
    totalFeesSaved: number;
    totalReferrerRevenue: number;
    totalReferralCount: number;
    totalReferralVolume: number;
    uniqueUsersWithReferrals: number;
    leaderboardByRevenue: Array<{
        referrerCode: string;
        totalRevenue: number;
        uniqueUsers: number;
        referralCount: number;
        totalVolume: number;
    }>;
    leaderboardByReferrals: Array<{
        referrerCode: string;
        uniqueUsers: number;
        totalRevenue: number;
        referralCount: number;
        totalVolume: number;
    }>;
    byProvider: Array<{
        provider: string;
        feesSaved: number;
        referrerRevenue: number;
        referralCount: number;
        uniqueUsers: number;
        totalVolume: number;
    }>;
}

export function ReferralsTab({ range, startDate, endDate, granularity }: ReferralsTabProps) {
    const [allData, setAllData] = useState<ReferralData | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Fetch referral data
    useEffect(() => {
        async function fetchData() {
            setLoading(true);
            setError(null);

            try {
                const params = new URLSearchParams();
                params.set(SHORT_PARAMS.RANGE, range);
                if (startDate) params.set(SHORT_PARAMS.START_DATE, startDate);
                if (endDate) params.set(SHORT_PARAMS.END_DATE, endDate);
                params.set(SHORT_PARAMS.GRANULARITY, granularity);

                const res = await fetch(`/api/referrals?${params.toString()}`);
                if (!res.ok) throw new Error('Failed to fetch referrals data');

                const data = await res.json();
                setAllData(data);
            } catch (err) {
                console.error('Error fetching referrals data:', err);
                setError('Failed to load referrals data');
            } finally {
                setLoading(false);
            }
        }

        fetchData();
    }, [range, startDate, endDate, granularity]);

    // Error state
    if (error && !allData) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-red-400 text-lg">{error}</div>
            </div>
        );
    }

    // Loading state
    if (loading && !allData) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-slate-400 text-lg">Loading referrals data...</div>
            </div>
        );
    }

    // No data state
    if (!allData) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-slate-400 text-lg">No data available</div>
            </div>
        );
    }

    return (
        <div className="space-y-6">
            {/* Hero Metrics */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                <HeroMetric
                    label="Total Fees Saved"
                    value={allData.totalFeesSaved}
                    icon={PiggyBank}
                    color="cyan"
                    format="currency"
                    tooltip="User savings from using referral codes vs standard 50 bps fee"
                />
                <HeroMetric
                    label="Referrer Revenue"
                    value={allData.totalReferrerRevenue}
                    icon={DollarSign}
                    color="blue"
                    format="currency"
                    tooltip="Total revenue earned by referrers (referrer BPS × volume)"
                />
                <HeroMetric
                    label="Referral Swaps"
                    value={allData.totalReferralCount}
                    icon={TrendingUp}
                    color="teal"
                    format="number"
                    tooltip="Total number of swaps using referral codes"
                />
                <HeroMetric
                    label="Users with Referrals"
                    value={allData.uniqueUsersWithReferrals}
                    icon={Users}
                    color="purple"
                    format="number"
                    tooltip="Number of unique users who used a referral code"
                />
            </div>

            {/* Leaderboard */}
            <ReferrerLeaderboard
                dataByRevenue={allData.leaderboardByRevenue}
                dataByReferrals={allData.leaderboardByReferrals}
            />

            {/* Provider Breakdown */}
            {allData.byProvider && allData.byProvider.length > 0 && (
                <div className="glass-card rounded-xl p-4 md:p-6">
                    <h3 className="text-lg font-semibold text-white mb-4">Referrals by Provider</h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        {allData.byProvider.map((provider) => (
                            <div
                                key={provider.provider}
                                className="p-4 rounded-lg bg-white/5"
                            >
                                <div className="flex items-center gap-2 mb-3">
                                    <div
                                        className="w-3 h-3 rounded-full"
                                        style={{
                                            backgroundColor: provider.provider === 'thorchain' ? providerColors[0] :
                                                provider.provider === 'mayachain' ? providerColors[1] : '#64748B'
                                        }}
                                    />
                                    <span className="text-white font-medium capitalize">
                                        {provider.provider}
                                    </span>
                                </div>
                                <div className="grid grid-cols-2 gap-3 text-sm">
                                    <div>
                                        <p className="text-slate-400">Referrer Revenue</p>
                                        <p className="text-cyan-400 font-semibold">
                                            ${provider.referrerRevenue.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                                        </p>
                                    </div>
                                    <div>
                                        <p className="text-slate-400">Fees Saved</p>
                                        <p className="text-teal-400 font-semibold">
                                            ${provider.feesSaved.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                                        </p>
                                    </div>
                                    <div>
                                        <p className="text-slate-400">Referral Swaps</p>
                                        <p className="text-white font-semibold">
                                            {provider.referralCount.toLocaleString()}
                                        </p>
                                    </div>
                                    <div>
                                        <p className="text-slate-400">Unique Users</p>
                                        <p className="text-white font-semibold">
                                            {provider.uniqueUsers.toLocaleString()}
                                        </p>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Footer Note */}
            <div className="flex items-center justify-center gap-2 text-xs text-slate-500">
                <Info className="w-3.5 h-3.5" />
                <span>Referral data is only available for THORChain and MAYAChain swaps</span>
            </div>
        </div>
    );
}

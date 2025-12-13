'use client';

import { useState, useEffect, useMemo } from 'react';
import { HeroMetric } from '@/components/HeroMetric';
import { DonutChart } from '@/components/DonutChart';
import { StatsCard } from '@/components/StatsCard';
import { Tooltip } from '@/components/Tooltip';
import { DollarSign, Users, Hash, TrendingUp, Activity, Wallet } from 'lucide-react';
import { providerColors } from '@/lib/chartStyles';
import { filterByDateRange, aggregateByGranularity, transformToChartData } from '@/lib/dataProcessing';
import type { DateRangeType } from '@/lib/dateUtils';
import { buildApiUrl, buildQueryParams } from '@/lib/api';
import CountUp from 'react-countup';

interface OverviewTabProps {
    range: string;
    startDate?: string | null;
    endDate?: string | null;
    granularity: string;
}

export function OverviewTab({ range, startDate, endDate, granularity }: OverviewTabProps) {
    const [allData, setAllData] = useState<any>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    // Fetch data when range, dates, or granularity changes
    useEffect(() => {
        async function fetchData() {
            setLoading(true);
            setError(null);

            try {
                // Build query parameters
                const params = buildQueryParams({
                    r: range,
                    g: granularity,
                    sd: startDate,
                    ed: endDate,
                });

                const paramsString = params.toString();

                const [volumeRes, revenueRes, usersRes] = await Promise.all([
                    fetch(buildApiUrl(`/api/swap-volume?${paramsString}`)),
                    fetch(buildApiUrl(`/api/revenue?${paramsString}`)),
                    fetch(buildApiUrl(`/api/users?${paramsString}`))
                ]);

                if (!volumeRes.ok || !revenueRes.ok || !usersRes.ok) {
                    throw new Error('Failed to fetch overview data');
                }

                const [volumeData, revenueData, usersData] = await Promise.all([
                    volumeRes.json(),
                    revenueRes.json(),
                    usersRes.json()
                ]);

                setAllData({
                    volumeOverTime: volumeData.volumeOverTime || [],
                    revenueOverTime: revenueData.revenueOverTime || [],
                    usersOverTime: usersData.usersOverTime || [],
                    volumeByProvider: volumeData.volumeByProvider || [],
                    globalStats: volumeData.globalStats,
                    totalRevenue: revenueData.totalRevenue?.total_revenue,
                    totalUsers: usersData.totalUsers?.unique_users
                });
            } catch (err) {
                console.error('Error fetching overview data:', err);
                setError('Failed to load overview data');
            } finally {
                setLoading(false);
            }
        }

        fetchData();
    }, [range, startDate, endDate, granularity]); // Re-fetch when parameters change

    // Process data with client-side filtering
    const stats = useMemo(() => {
        if (!allData) return null;

        // Transform and filter volume data (only needed for chart display)
        const volumeChartData = transformToChartData(allData.volumeOverTime, 'volume');
        const filteredVolumeData = filterByDateRange(volumeChartData, range as DateRangeType, startDate, endDate);

        // Use direct API totals (not calculated from time-series)
        // This ensures totals are independent of granularity
        const providers = ['thorchain', 'mayachain', 'lifi', '1inch'];

        const totalVolume = Number(allData.globalStats?.total_volume || 0);
        const totalRevenue = Number(allData.totalRevenue || 0);
        const totalUsers = Number(allData.totalUsers || 0);
        const totalSwaps = Number(allData.globalStats?.total_swaps || 0);

        // Aggregate by granularity (only for chart display and averages)
        const aggregatedVolumeData = aggregateByGranularity(filteredVolumeData, granularity as any, providers);

        // Provider distribution from direct API data (volumeByProvider)
        // Map API response format to chart format: {source, total_volume} -> {name, value}
        const providerDistribution = (allData.volumeByProvider || [])
            .map((item: any) => ({
                name: item.source,
                value: Number(item.total_volume || 0)
            }))
            .filter((p: { name: string; value: number }) => p.value > 0);

        // Calculate averages based on aggregated data points
        const dataPoints = aggregatedVolumeData.length || 1;
        const averageVolume = totalVolume / dataPoints;
        const averageRevenue = totalRevenue / dataPoints;
        const averageSwaps = totalSwaps / dataPoints;

        // Calculate annual projections based on granularity (short values)
        let projectionMultiplier = 1;
        switch (granularity) {
            case 'h':
                projectionMultiplier = 24 * 365; // hours per year
                break;
            case 'd':
                projectionMultiplier = 365; // days per year
                break;
            case 'w':
                projectionMultiplier = 52; // weeks per year
                break;
            case 'm':
                projectionMultiplier = 12; // months per year
                break;
        }

        const projectedAnnualVolume = averageVolume * projectionMultiplier;
        const projectedAnnualRevenue = averageRevenue * projectionMultiplier;

        return {
            totalVolume,
            totalRevenue,
            totalUsers,
            totalSwaps,
            providerDistribution,
            averageVolume,
            averageRevenue,
            averageSwaps,
            projectedAnnualVolume,
            projectedAnnualRevenue
        };
    }, [allData, range, startDate, endDate, granularity]);

    // Show error if we have no data at all
    if (error && !stats) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-red-400 text-lg">{error || 'No data available'}</div>
            </div>
        );
    }

    // If we're loading but have previous data, show data with loading indicator
    // If no previous data exists, show loading screen
    if (loading && !stats) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-slate-400 text-lg">Loading overview...</div>
            </div>
        );
    }

    // If no data at all (shouldn't happen but guard against it)
    if (!stats) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-slate-400 text-lg">No data available</div>
            </div>
        );
    }

    const getGranularityLabel = () => {
        switch (granularity) {
            case 'h': return 'Hourly';
            case 'd': return 'Daily';
            case 'w': return 'Weekly';
            case 'm': return 'Monthly';
            default: return 'Average';
        }
    };

    return (
        <div className="space-y-6">
            {/* Hero Metrics */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                <HeroMetric
                    label="Total Swap Volume"
                    value={stats.totalVolume}
                    icon={DollarSign}
                    color="cyan"
                    format="currency"
                />
                <HeroMetric
                    label="Total Revenue"
                    value={stats.totalRevenue}
                    icon={Wallet}
                    color="blue"
                    format="currency"
                />
                <HeroMetric
                    label="Total Unique Swappers"
                    value={stats.totalUsers}
                    icon={Users}
                    color="teal"
                    format="number"
                />
                <HeroMetric
                    label="Total Swap Count"
                    value={stats.totalSwaps}
                    icon={Hash}
                    color="purple"
                    format="number"
                />
            </div>

            {/* Annual Projections */}
            <div>
                <h3 className="text-lg font-semibold text-white mb-4">
                    Annual Projections
                    <span className="text-sm text-slate-400 font-normal ml-2">
                        (Based on {getGranularityLabel()} Average)
                    </span>
                </h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="glass-card glass-card-hover will-change-blur rounded-lg p-4">
                        <div className="flex items-center gap-3 mb-2">
                            <div className="bg-cyan-500/10 p-2 rounded-lg">
                                <DollarSign className="w-5 h-5 text-cyan-400" />
                            </div>
                            <div className="flex items-center gap-2">
                                <p className="text-slate-400 text-sm font-medium">Projected Annual Volume</p>
                                <Tooltip content={`Based on ${getGranularityLabel()} average of selected date range`} iconOnly />
                            </div>
                        </div>
                        <p className="text-3xl font-bold text-white">
                            $<CountUp
                                end={stats.projectedAnnualVolume}
                                duration={0.8}
                                separator=","
                                decimals={0}
                                useEasing={true}
                            />
                        </p>
                    </div>

                    <div className="glass-card glass-card-hover will-change-blur rounded-lg p-4">
                        <div className="flex items-center gap-3 mb-2">
                            <div className="bg-blue-500/10 p-2 rounded-lg">
                                <Wallet className="w-5 h-5 text-blue-400" />
                            </div>
                            <div className="flex items-center gap-2">
                                <p className="text-slate-400 text-sm font-medium">Projected Annual Revenue</p>
                                <Tooltip content={`Based on ${getGranularityLabel()} average of selected date range`} iconOnly />
                            </div>
                        </div>
                        <p className="text-3xl font-bold text-white">
                            $<CountUp
                                end={stats.projectedAnnualRevenue}
                                duration={0.8}
                                separator=","
                                decimals={0}
                                useEasing={true}
                            />
                        </p>
                    </div>
                </div>
            </div>

            {/* Provider Distribution & Average Metrics */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <DonutChart
                    title="Total Swap Volume by Provider"
                    subtitle="Distribution across all providers"
                    data={stats.providerDistribution}
                    colors={providerColors}
                    currency={true}
                />

                {/* Average Metrics */}
                <div className="space-y-4">
                    <h3 className="text-lg font-semibold text-white mb-4">
                        {getGranularityLabel()} Averages
                    </h3>

                    <StatsCard
                        title={`Average Swap Volume (${getGranularityLabel()})`}
                        value={
                            <CountUp
                                end={stats.averageVolume}
                                duration={0.8}
                                separator=","
                                decimals={0}
                                prefix="$"
                                useEasing={true}
                            />
                        }
                        icon={TrendingUp}
                    />

                    <StatsCard
                        title={`Average Revenue (${getGranularityLabel()})`}
                        value={
                            <CountUp
                                end={stats.averageRevenue}
                                duration={0.8}
                                separator=","
                                decimals={0}
                                prefix="$"
                                useEasing={true}
                            />
                        }
                        icon={Wallet}
                    />

                    <StatsCard
                        title={`Average Swap Count (${getGranularityLabel()})`}
                        value={
                            <CountUp
                                end={stats.averageSwaps}
                                duration={0.8}
                                separator=","
                                decimals={0}
                                useEasing={true}
                            />
                        }
                        icon={Activity}
                    />
                </div>
            </div>
        </div>
    );
}

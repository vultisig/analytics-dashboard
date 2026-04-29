'use client';

import { useState, useEffect, useMemo } from 'react';
import { HeroMetric } from '@/components/HeroMetric';
import { DonutChart } from '@/components/DonutChart';
import { StatsCard } from '@/components/StatsCard';
import { Tooltip } from '@/components/Tooltip';
import { ArrowUpRight, DollarSign, Users, Hash, TrendingUp, Activity, Wallet } from 'lucide-react';
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
                    <ProjectionCard
                        variant="volume"
                        label="Projected Annual Volume"
                        tooltip={`Based on ${getGranularityLabel()} average of selected date range`}
                        value={stats.projectedAnnualVolume}
                    />
                    <ProjectionCard
                        variant="revenue"
                        label="Projected Annual Revenue"
                        tooltip={`Based on ${getGranularityLabel()} average of selected date range`}
                        value={stats.projectedAnnualRevenue}
                        soFarLabel="Revenue so far"
                        soFarValue={stats.totalRevenue}
                    />
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

function formatCompactCurrency(value: number): string {
    if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(2)}M`;
    if (value >= 1_000) return `$${(value / 1_000).toFixed(1)}K`;
    return `$${Math.round(value).toLocaleString('en-US')}`;
}

interface ProjectionCardProps {
    variant: 'volume' | 'revenue';
    label: string;
    tooltip: string;
    value: number;
    soFarLabel?: string;
    soFarValue?: number;
}

function ProjectionCard({ variant, label, tooltip, value, soFarLabel, soFarValue }: ProjectionCardProps) {
    const showProgress = variant === 'revenue' && soFarLabel !== undefined && soFarValue !== undefined;
    const progressPercent = showProgress && value > 0
        ? Math.min(100, Math.max(0, (soFarValue / value) * 100))
        : 0;

    return (
        <div className="glass-card glass-card-hover will-change-blur rounded-2xl p-5 relative overflow-hidden min-h-[150px]">
            {variant === 'volume' && (
                <img
                    src="/figma/projection-chart.svg"
                    alt=""
                    aria-hidden="true"
                    className="absolute right-0 bottom-0 w-[60%] max-w-[340px] h-auto pointer-events-none select-none"
                />
            )}

            <div className="relative z-10 flex flex-col h-full justify-between gap-6">
                <div className="flex items-center gap-3">
                    {variant === 'volume' ? (
                        <div className="w-8 h-8 rounded-[10px] bg-[#11284A] flex items-center justify-center flex-shrink-0">
                            <ArrowUpRight className="w-4 h-4 text-[#8295AE]" strokeWidth={1.75} />
                        </div>
                    ) : (
                        <img
                            src="/figma/treasure-chest.svg"
                            alt=""
                            width={32}
                            height={32}
                            className="w-8 h-8 flex-shrink-0"
                        />
                    )}
                    <div className="flex items-center gap-1.5">
                        <p className="text-slate-300 text-sm font-medium">{label}</p>
                        <Tooltip content={tooltip} iconOnly />
                    </div>
                </div>

                <div className="flex items-end justify-between gap-4 flex-wrap">
                    <p className="text-3xl md:text-4xl font-bold text-white tracking-tight">
                        $<CountUp
                            end={value}
                            duration={0.8}
                            separator=","
                            decimals={0}
                            useEasing={true}
                        />
                    </p>

                    {showProgress && (
                        <div className="flex-1 min-w-[180px] max-w-[280px] pb-1">
                            <div className="relative h-9 mb-1">
                                <div
                                    className="absolute top-1 flex items-start gap-1.5"
                                    style={{ left: `${progressPercent}%` }}
                                >
                                    <span
                                        aria-hidden="true"
                                        className="block w-px h-8 flex-shrink-0"
                                        style={{
                                            background: 'linear-gradient(180deg, rgba(255,255,255,0) 0%, rgba(255,255,255,1) 100%)',
                                        }}
                                    />
                                    <div className="flex flex-col items-start gap-0.5">
                                        <span className="text-[10px] text-slate-500 leading-tight whitespace-nowrap">{soFarLabel}</span>
                                        <span className="text-xs font-semibold text-slate-200 leading-tight whitespace-nowrap">
                                            {formatCompactCurrency(soFarValue!)}
                                        </span>
                                    </div>
                                </div>
                            </div>
                            <div className="relative w-full h-[7px] rounded-full bg-[#11284A] overflow-hidden">
                                <div
                                    className="h-full rounded-full transition-[width] duration-700 ease-out"
                                    style={{
                                        width: `${progressPercent}%`,
                                        background: 'linear-gradient(109deg, #11284A 0%, #2155DF 29%, #FFFFFF 98%)',
                                    }}
                                />
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

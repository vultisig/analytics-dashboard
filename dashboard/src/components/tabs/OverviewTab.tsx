'use client';

import { useState, useEffect, useMemo } from 'react';
import { HeroMetric } from '@/components/HeroMetric';
import { DonutChart } from '@/components/DonutChart';
import { StatsCard } from '@/components/StatsCard';
import { Tooltip } from '@/components/Tooltip';
import {
    IconArrowUpRightV,
    IconDollar,
    IconVault,
    IconPeopleAdded,
    IconHashtag,
    IconChart4,
    IconReceiptCheck,
    IconArrowsRepeatLR,
    IconCalculatorV,
} from '@/icons';
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
                <div className="text-[var(--alert-error)] text-lg">{error || 'No data available'}</div>
            </div>
        );
    }

    // If we're loading but have previous data, show data with loading indicator
    // If no previous data exists, show loading screen
    if (loading && !stats) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-[var(--text-tertiary)] text-lg">Loading overview...</div>
            </div>
        );
    }

    // If no data at all (shouldn't happen but guard against it)
    if (!stats) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-[var(--text-tertiary)] text-lg">No data available</div>
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
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
                <HeroMetric
                    label="Total Swap Volume"
                    value={stats.totalVolume}
                    icon={IconDollar}
                    color="accent"
                    format="currency"
                />
                <HeroMetric
                    label="Total Revenue"
                    value={stats.totalRevenue}
                    icon={IconVault}
                    format="currency"
                />
                <HeroMetric
                    label="Total Unique Swappers"
                    value={stats.totalUsers}
                    icon={IconPeopleAdded}
                    format="number"
                />
                <HeroMetric
                    label="Total Swap Count"
                    value={stats.totalSwaps}
                    icon={IconHashtag}
                    format="number"
                />
            </div>

            {/* Annual Projections */}
            <div>
                <h3 className="text-[17px] font-medium leading-5 tracking-[-0.02em] text-[var(--text-primary)] mb-4">
                    Annual Projections
                    <span className="text-[13px] text-[var(--text-tertiary)] font-medium ml-2">
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
                <div className="space-y-3">
                    <h3 className="text-[17px] font-medium leading-5 tracking-[-0.02em] text-[var(--text-primary)] mb-4">
                        {getGranularityLabel()} Averages
                    </h3>

                    <StatsCard
                        title="Average Swap Volume"
                        caption={getGranularityLabel()}
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
                        icon={IconChart4}
                    />

                    <StatsCard
                        title="Average Revenue"
                        caption={getGranularityLabel()}
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
                        icon={IconReceiptCheck}
                    />

                    <StatsCard
                        title="Average Swap Count"
                        caption={getGranularityLabel()}
                        value={
                            <CountUp
                                end={stats.averageSwaps}
                                duration={0.8}
                                separator=","
                                decimals={0}
                                useEasing={true}
                            />
                        }
                        icon={IconArrowsRepeatLR}
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
    const Icon = variant === 'volume' ? IconArrowUpRightV : IconCalculatorV;

    return (
        <div className="surface-card surface-card-hover relative h-[152px] p-5 overflow-hidden">
            {variant === 'volume' && (
                <img
                    src="/figma/projection-chart.svg"
                    alt=""
                    aria-hidden="true"
                    className="pointer-events-none select-none absolute right-0 bottom-0 w-[60%] max-w-[355px] h-auto z-0"
                />
            )}

            <div className="relative z-10 flex h-full flex-col justify-between">
                <div className="flex items-center gap-3">
                    <div className="icon-badge">
                        <Icon />
                    </div>
                    <div className="flex items-center gap-1.5">
                        <p className="t-body-s text-[var(--text-secondary)]">{label}</p>
                        <Tooltip content={tooltip} iconOnly />
                    </div>
                </div>

                <div className="flex items-end justify-between gap-4 flex-wrap">
                    <p className="font-display font-medium text-num text-[32px] leading-[37px] tracking-[-0.64px] text-[var(--text-primary)]">
                        $<CountUp
                            end={value}
                            duration={0.8}
                            separator=","
                            decimals={0}
                            useEasing={true}
                        />
                    </p>

                    {showProgress && (
                        <div className="relative w-[222px] pb-1 overflow-visible">
                            {/* Label + white tip indicator above the progress bar.
                                The tip stays at the progress edge; the label flips
                                to the left of the tip past 55 % so it can't overflow
                                the card on near-full bars. */}
                            <div className="relative h-9 mb-1">
                                <span
                                    aria-hidden="true"
                                    className="absolute top-1 block w-px h-8"
                                    style={{
                                        left: `${progressPercent}%`,
                                        background:
                                            'linear-gradient(180deg, rgba(255,255,255,0) 0%, rgba(255,255,255,1) 100%)',
                                    }}
                                />
                                {progressPercent > 55 ? (
                                    <div
                                        className="absolute top-1 flex flex-col items-end gap-0.5 pr-1.5 text-right"
                                        style={{ right: `${100 - progressPercent}%` }}
                                    >
                                        <span className="text-[8px] font-medium text-[var(--text-tertiary)] leading-[16px] whitespace-nowrap">
                                            {soFarLabel}
                                        </span>
                                        <span className="text-[10px] font-medium text-num text-[var(--text-secondary)] leading-[16px] whitespace-nowrap">
                                            {formatCompactCurrency(soFarValue!)}
                                        </span>
                                    </div>
                                ) : (
                                    <div
                                        className="absolute top-1 flex flex-col items-start gap-0.5 pl-1.5"
                                        style={{ left: `${progressPercent}%` }}
                                    >
                                        <span className="text-[8px] font-medium text-[var(--text-tertiary)] leading-[16px] whitespace-nowrap">
                                            {soFarLabel}
                                        </span>
                                        <span className="text-[10px] font-medium text-num text-[var(--text-secondary)] leading-[16px] whitespace-nowrap">
                                            {formatCompactCurrency(soFarValue!)}
                                        </span>
                                    </div>
                                )}
                            </div>
                            {/* Track + filled bar (Figma: 15.41deg gradient + 4.1px blur shadow) */}
                            <div className="relative h-[7px] rounded-[12px] bg-[var(--border-light)] overflow-visible">
                                <div
                                    className="absolute inset-y-0 left-0 rounded-[12px] transition-[width] duration-700 ease-out"
                                    style={{
                                        width: `${progressPercent}%`,
                                        backgroundImage:
                                            'linear-gradient(15.407deg, #11284A 13.791%, #2155DF 42.744%, #FFFFFF 111.83%)',
                                    }}
                                />
                                <div
                                    aria-hidden="true"
                                    className="absolute inset-y-0 left-0 rounded-[12px] blur-[4.1px] transition-[width] duration-700 ease-out"
                                    style={{
                                        width: `${progressPercent}%`,
                                        backgroundImage:
                                            'linear-gradient(15.407deg, #11284A 13.791%, #2155DF 42.744%, #FFFFFF 111.83%)',
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

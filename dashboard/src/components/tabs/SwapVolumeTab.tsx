'use client';

import { useState, useEffect, useMemo, useCallback } from 'react';
import { StackedBarChart } from '@/components/StackedBarChart';
import { DonutChart } from '@/components/DonutChart';
import { TopPathsChart } from '@/components/TopPathsChart';
import { HeroMetric } from '@/components/HeroMetric';
import { ProviderSection } from '@/components/ProviderSection';
import { ProviderToggleControl } from '@/components/ProviderToggleControl';
import { VolumeViewToggle } from '@/components/VolumeViewToggle';
import { CumulativeToggle } from '@/components/CumulativeToggle';
import { ChartViewToggle } from '@/components/ChartViewToggle';
import { TrendingUp, DollarSign, Wallet, Info } from 'lucide-react';
import { providerColors, chainColorMap } from '@/lib/chartStyles';
import { aggregateByGranularity, transformToChartData } from '@/lib/dataProcessing';
import { sortProviders } from '@/lib/providerUtils';
import { buildApiUrl, buildQueryParams } from '@/lib/api';

interface SwapVolumeTabProps {
    range: string;
    startDate?: string | null;
    endDate?: string | null;
    granularity: string;
}

export function SwapVolumeTab({ range, startDate, endDate, granularity }: SwapVolumeTabProps) {
    const [allData, setAllData] = useState<any>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [visibleProviders, setVisibleProviders] = useState<string[]>([]);
    const [providerViews, setProviderViews] = useState<Record<string, 'total' | 'breakdown'>>({});
    const [providerData, setProviderData] = useState<Record<string, any>>({});
    const [cumulativeMode, setCumulativeMode] = useState<Record<string, boolean>>({});
    const [mainChartCumulative, setMainChartCumulative] = useState(false);
    const [chartView, setChartView] = useState<'provider' | 'platform'>('provider');
    const [visiblePlatforms, setVisiblePlatforms] = useState<string[]>(['Android', 'iOS', 'Web', 'Other']);

    // Fetch all data in parallel when range, dates, or granularity changes
    useEffect(() => {
        async function fetchAllData() {
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

                // Define all known providers upfront
                const allKnownProviders = ['thorchain', 'mayachain', 'lifi', '1inch'];

                // Fetch main volume data and all provider data in parallel
                const [volumeRes, ...providerResponses] = await Promise.all([
                    fetch(buildApiUrl(`/api/swap-volume?${paramsString}`)),
                    ...allKnownProviders.map(provider =>
                        fetch(buildApiUrl(`/api/swap-volume/provider/${provider}?${paramsString}`))
                            .then(res => res.ok ? res.json() : null)
                            .catch(() => null)
                    )
                ]);

                if (!volumeRes.ok) throw new Error('Failed to fetch swap volume data');

                const volumeData = await volumeRes.json();
                setAllData(volumeData);

                // Process provider data
                const newProviderData: Record<string, any> = {};
                allKnownProviders.forEach((provider, index) => {
                    if (providerResponses[index]) {
                        newProviderData[provider] = providerResponses[index];
                    }
                });
                setProviderData(newProviderData);

                // Initialize provider views (all start with 'total')
                const initialViews: Record<string, 'total' | 'breakdown'> = {};
                allKnownProviders.forEach(provider => {
                    initialViews[provider] = 'total';
                });
                setProviderViews(initialViews);

                // Initialize all known providers as visible
                setVisibleProviders(sortProviders(allKnownProviders));
            } catch (err) {
                console.error('Error fetching swap volume data:', err);
                setError('Failed to load swap volume data');
            } finally {
                setLoading(false);
            }
        }

        fetchAllData();
    }, [range, startDate, endDate, granularity]); // Re-fetch when parameters change

    // Memoize raw data transformation (only depends on allData)
    const rawChartData = useMemo(() => {
        if (!allData?.volumeOverTime) return [];
        return allData.volumeOverTime.map((item: any) => ({
            date: item.time_period,
            source: item.source,
            volume: Number(item.volume)
        }));
    }, [allData]);

    // Memoize provider list (only depends on allData and providerData)
    const providers = useMemo(() => {
        if (!allData) return [];
        const dataProviders = Array.from(new Set(rawChartData.map((item: any) => item.source)));
        const allProviders = Array.from(new Set([...dataProviders, ...Object.keys(providerData)])) as string[];
        return sortProviders(allProviders);
    }, [allData, rawChartData, providerData]);

    // Process data based on filters (client-side) - now with optimized dependencies
    const data = useMemo(() => {
        try {
            if (!allData || rawChartData.length === 0) {
                return null;
            }

        const chartData = transformToChartData(rawChartData, 'volume');

        // Aggregate by granularity (API already filtered by date range)
        const aggregatedChartData = aggregateByGranularity(chartData, granularity as any, providers);

        // Use direct API total values (consistent across all granularities)
        const totalVolume = Number(allData.globalStats?.total_volume || 0);

        const dataPoints = aggregatedChartData.length || 1;
        const averageVolume = totalVolume / dataPoints;

        // Provider stats
        const providerStats = providers.map(provider => {
            const volume = aggregatedChartData.reduce((sum, item) => sum + (Number(item[provider]) || 0), 0);
            return {
                provider,
                total_volume: volume,
                swap_count: 0 // Approximate
            };
        });

        // Provider breakdowns
        const providerBreakdowns: any = {};
        providers.forEach((provider: string) => {
            const provData = providerData[provider] || {};
            // Calculate total volume for this provider (for percentage calculations)
            const providerTotalVolume = aggregatedChartData.reduce((sum, item) => sum + (Number(item[provider]) || 0), 0);
            providerBreakdowns[provider] = {
                totalVolume: providerTotalVolume,
                platformData: aggregatedChartData,
                chainData: aggregatedChartData,
                platformPie: (provData.platforms || provData.chains || []).map((p: any) => ({
                    name: p.platform || p.chain || p.name || 'Unknown',
                    value: Number(p.volume || p.value || 0)
                })),
                chainPie: (provData.chains || provData.platforms || []).map((c: any) => ({
                    name: c.chain || c.platform || c.name || 'Unknown',
                    value: Number(c.volume || c.value || 0)
                })),
                topPaths: (allData.topPaths || [])
                    .filter((path: any) => !provider || path.source === provider)
                    .slice(0, 10)
                    .map((path: any) => ({
                        name: path.swap_path || `${path.token_in || 'Token'} → ${path.token_out || 'Token'}`,
                        volume: Number(path.total_volume || 0),
                        count: Number(path.swap_count || 0)
                    }))
            };
        });

        return {
            totalVolume,
            averageVolume,
            chartData: aggregatedChartData,
            providers,
            providerStats,
            providerBreakdowns
        };
        } catch (err) {
            console.error('[SwapVolumeTab] Error processing data:', err);
            return null;
        }
    }, [allData, rawChartData, providers, providerData, range, startDate, endDate, granularity]);

    const handleToggleProvider = useCallback((provider: string) => {
        setVisibleProviders(prev =>
            prev.includes(provider)
                ? prev.filter(p => p !== provider)
                : [...prev, provider]
        );
    }, []);

    const handleProviderViewChange = useCallback((provider: string, view: 'total' | 'breakdown') => {
        setProviderViews(prev => ({
            ...prev,
            [provider]: view
        }));
    }, []);

    const handleCumulativeModeChange = useCallback((provider: string, enabled: boolean) => {
        setCumulativeMode(prev => ({
            ...prev,
            [provider]: enabled
        }));
    }, []);

    const handlePlatformToggle = useCallback((platform: string) => {
        setVisiblePlatforms(prev =>
            prev.includes(platform)
                ? prev.filter(p => p !== platform)
                : [...prev, platform]
        );
    }, []);

    // Transform data to cumulative mode - memoized
    const toCumulativeData = useCallback((data: any[], keys: string[]) => {
        const cumulative: any[] = [];
        const runningTotals: Record<string, number> = {};

        // Initialize running totals
        keys.forEach(key => {
            runningTotals[key] = 0;
        });

        data.forEach(item => {
            const cumulativeItem: any = { date: item.date };
            keys.forEach(key => {
                runningTotals[key] += Number(item[key]) || 0;
                cumulativeItem[key] = runningTotals[key];
            });
            cumulative.push(cumulativeItem);
        });

        return cumulative;
    }, []);

    const getGranularityLabel = useCallback(() => {
        switch (granularity) {
            case 'h': return 'Hourly';
            case 'd': return 'Daily';
            case 'w': return 'Weekly';
            case 'm': return 'Monthly';
            default: return 'Average';
        }
    }, [granularity]);

    // Process platform chart data from API (must be before early returns to follow hooks rules)
    const platformChartData = useMemo(() => {
        if (!allData?.volumeByPlatformOverTime) return [];

        const platformByDate: Record<string, any> = {};
        allData.volumeByPlatformOverTime.forEach((row: any) => {
            const date = new Date(row.time_period);
            let dateStr: string;
            if (granularity === 'h') {
                dateStr = date.toLocaleString('en-US', {
                    month: 'short',
                    day: 'numeric',
                    hour: '2-digit',
                    hour12: false
                }).replace(',', '');
            } else if (granularity === 'm') {
                dateStr = date.toLocaleString('en-US', { year: 'numeric', month: 'short' });
            } else {
                dateStr = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
            }

            if (!platformByDate[dateStr]) {
                platformByDate[dateStr] = { date: dateStr };
            }
            platformByDate[dateStr][row.platform] = (platformByDate[dateStr][row.platform] || 0) + Number(row.volume);
        });

        return Object.values(platformByDate);
    }, [allData, granularity]);

    // Show error if we have no data at all
    if (error && !data) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-red-400 text-lg">{error || 'No data available'}</div>
            </div>
        );
    }

    // If we're loading but have previous data, show data with loading indicator
    // If no previous data exists, show loading screen
    if (loading && !data) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-slate-400 text-lg">Loading swap volume data...</div>
            </div>
        );
    }

    // If no data at all (shouldn't happen but guard against it)
    if (!data) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-slate-400 text-lg">No data available</div>
            </div>
        );
    }

    // Filter chart data based on visible providers
    let filteredChartData = data.chartData.map(item => {
        const filtered: any = { date: item.date };
        data.providers.forEach(provider => {
            if (visibleProviders.includes(provider)) {
                filtered[provider] = item[provider];
            }
        });
        return filtered;
    });

    const filteredProviders = data.providers.filter(p => visibleProviders.includes(p));

    // Apply cumulative transformation to main chart if enabled
    if (mainChartCumulative) {
        filteredChartData = toCumulativeData(filteredChartData, filteredProviders);
    }

    // Calculate annual projection (using short values)
    const projectionMultiplier = granularity === 'h' ? 24 * 365 :
                                 granularity === 'd' ? 365 :
                                 granularity === 'w' ? 52 : 12;
    const projectedAnnualVolume = data.averageVolume * projectionMultiplier;

    // Filter platform chart data based on visible platforms
    let filteredPlatformChartData = platformChartData.map((row: any) => {
        const filtered: any = { date: row.date };
        visiblePlatforms.forEach(platform => {
            if (row[platform] !== undefined) {
                filtered[platform] = row[platform];
            }
        });
        return filtered;
    });

    // Apply cumulative transformation to platform chart if enabled
    if (mainChartCumulative) {
        filteredPlatformChartData = toCumulativeData(filteredPlatformChartData, visiblePlatforms);
    }

    return (
        <div className="space-y-6">
            {/* Hero Metrics */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <HeroMetric
                    label="Total Swap Volume"
                    value={data.totalVolume}
                    icon={DollarSign}
                    color="cyan"
                    format="currency"
                />
                <HeroMetric
                    label={`Average Swap Volume (${getGranularityLabel()})`}
                    value={data.averageVolume}
                    icon={TrendingUp}
                    color="blue"
                    format="currency"
                />
                <HeroMetric
                    label="Projected Annual Volume"
                    value={projectedAnnualVolume}
                    icon={Wallet}
                    color="teal"
                    format="currency"
                    tooltip={`Based on ${getGranularityLabel()} average of selected date range`}
                />
            </div>

            {/* Chart View Toggle and Show/Hide Controls */}
            <div className="glass-card rounded-xl p-4 space-y-3">
                <div className="flex flex-wrap items-center justify-between gap-4">
                    <ChartViewToggle view={chartView} onViewChange={setChartView} />
                    {chartView === 'platform' && (
                        <div className="flex items-center gap-1.5 text-xs text-slate-400">
                            <Info className="w-3.5 h-3.5" />
                            <span>1inch data excluded (no platform info)</span>
                        </div>
                    )}
                </div>
                {chartView === 'provider' ? (
                    <ProviderToggleControl
                        providers={data.providers}
                        visibleProviders={visibleProviders}
                        onToggleProvider={handleToggleProvider}
                        colors={providerColors}
                    />
                ) : (
                    <ProviderToggleControl
                        providers={['Android', 'iOS', 'Web', 'Other']}
                        visibleProviders={visiblePlatforms}
                        onToggleProvider={handlePlatformToggle}
                        colors={[chainColorMap['android'], chainColorMap['ios'], chainColorMap['web'], '#64748B']}
                    />
                )}
            </div>

            {/* Total Swap Volume Chart with Cumulative Toggle */}
            <div className="glass-card rounded-xl p-6 space-y-4">
                <div className="flex flex-wrap justify-between items-center gap-2">
                    <div>
                        <h3 className="text-lg font-bold text-white">
                            {chartView === 'provider' ? 'Total Swap Volume by Provider' : 'Total Swap Volume by Platform'}
                        </h3>
                        <p className="text-sm text-slate-400">
                            {getGranularityLabel()} breakdown{chartView === 'platform' ? ' (excludes 1inch)' : ''}
                        </p>
                    </div>
                    <CumulativeToggle
                        enabled={mainChartCumulative}
                        onToggle={setMainChartCumulative}
                    />
                </div>
                {chartView === 'provider' ? (
                    <StackedBarChart
                        title=""
                        subtitle=""
                        data={filteredChartData}
                        keys={filteredProviders}
                        colors={providerColors}
                        currency={true}
                    />
                ) : (
                    <StackedBarChart
                        title=""
                        subtitle=""
                        data={filteredPlatformChartData}
                        keys={visiblePlatforms}
                        colors={visiblePlatforms.map(p => chainColorMap[p.toLowerCase()] || '#64748B')}
                        currency={true}
                    />
                )}
            </div>

            {/* Provider Sections */}
            <div className="space-y-6">
                {data.providers.map((provider) => {
                    const currentView = providerViews[provider] || 'total';
                    const specificData = providerData[provider];

                    return (
                        <ProviderSection
                            key={provider}
                            provider={provider}
                        >
                            {(platformChainView) => (
                                <div className="space-y-6">
                                    {/* Toggle controls - left: view toggle, right: cumulative toggle */}
                                    <div className="flex flex-wrap justify-between items-center gap-2">
                                        <VolumeViewToggle
                                            view={currentView}
                                            onViewChange={(view) => handleProviderViewChange(provider, view)}
                                            platformChainView={platformChainView}
                                        />
                                        <CumulativeToggle
                                            enabled={cumulativeMode[provider] || false}
                                            onToggle={(enabled) => handleCumulativeModeChange(provider, enabled)}
                                        />
                                    </div>

                                    {/* Render chart based on current view */}
                                    {currentView === 'total' ? (() => {
                                        // Transform and filter total volume data
                                        const rawData = specificData?.totalVolume?.map((item: any) => ({
                                            date: item.time_period,
                                            source: provider,
                                            volume: Number(item.volume)
                                        })) || [];

                                        const chartData = transformToChartData(rawData, 'volume');
                                        let aggregatedData = aggregateByGranularity(chartData, granularity as any, [provider]);

                                        // Apply cumulative transformation if enabled
                                        if (cumulativeMode[provider]) {
                                            aggregatedData = toCumulativeData(aggregatedData, [provider]);
                                        }

                                        const hasData = aggregatedData.length > 0 && aggregatedData.some(item => Number(item[provider]) > 0);

                                        return (
                                            <div className="relative">
                                                <StackedBarChart
                                                    title={`Total Swap Volume${cumulativeMode[provider] ? ' (Cumulative)' : ''}`}
                                                    subtitle={`${getGranularityLabel()} breakdown`}
                                                    data={aggregatedData}
                                                    keys={[provider]}
                                                    colors={providerColors}
                                                    currency={true}
                                                />
                                                {!hasData && (
                                                    <div className="absolute inset-0 flex items-center justify-center bg-slate-900/80 backdrop-blur-sm rounded-xl">
                                                        <div className="text-center">
                                                            <p className="text-slate-400 text-sm">No activity within this date range</p>
                                                        </div>
                                                    </div>
                                                )}
                                            </div>
                                        );
                                    })() : (() => {
                                        // Transform and filter platform breakdown data
                                        const rawData = specificData?.platformBreakdown?.map((item: any) => ({
                                            date: item.time_period,
                                            source: item.platform || item.chain,
                                            volume: Number(item.volume)
                                        })) || [];

                                        const chartData = transformToChartData(rawData, 'volume');
                                        const platforms = Array.from(new Set(
                                            specificData?.platformBreakdown?.map((item: any) =>
                                                item.platform || item.chain
                                            ) || []
                                        )) as string[];

                                        let aggregatedData = aggregateByGranularity(chartData, granularity as any, platforms);

                                        // Apply cumulative transformation if enabled
                                        if (cumulativeMode[provider]) {
                                            aggregatedData = toCumulativeData(aggregatedData, platforms);
                                        }

                                        const hasData = aggregatedData.length > 0 && platforms.length > 0;

                                        return (
                                            <div className="relative">
                                                <StackedBarChart
                                                    title={`Swap Volume by ${platformChainView === 'platform' ? 'Platform' : 'Chain'}${cumulativeMode[provider] ? ' (Cumulative)' : ''}`}
                                                    subtitle={`${getGranularityLabel()} breakdown`}
                                                    data={aggregatedData}
                                                    keys={platforms}
                                                    colors={providerColors}
                                                    currency={true}
                                                />
                                                {!hasData && (
                                                    <div className="absolute inset-0 flex items-center justify-center bg-slate-900/80 backdrop-blur-sm rounded-xl">
                                                        <div className="text-center">
                                                            <p className="text-slate-400 text-sm">No activity within this date range</p>
                                                        </div>
                                                    </div>
                                                )}
                                            </div>
                                        );
                                    })()}

                                    {/* Two-column layout for Pie Chart and Top Paths */}
                                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                                        <DonutChart
                                            title={`Volume Distribution by ${platformChainView === 'platform' ? 'Platform' : 'Chain'}`}
                                            data={(() => {
                                                // Compute pie data from specificData (respects date range filtering)
                                                const breakdown = specificData?.platformBreakdown || [];
                                                const pieData: Record<string, number> = {};

                                                // Aggregate volume by platform/chain
                                                breakdown.forEach((item: any) => {
                                                    const key = item.platform || item.chain || 'Unknown';
                                                    pieData[key] = (pieData[key] || 0) + Number(item.volume || 0);
                                                });

                                                // Convert to array format for DonutChart
                                                return Object.entries(pieData).map(([name, value]) => ({
                                                    name,
                                                    value
                                                }));
                                            })()}
                                            colors={providerColors}
                                            currency={true}
                                        />

                                        <TopPathsChart
                                            title="Top 10 Swap Paths"
                                            subtitle="By Volume"
                                            data={data.providerBreakdowns[provider].topPaths}
                                            total={data.providerBreakdowns[provider].totalVolume}
                                        />
                                    </div>
                                </div>
                            )}
                        </ProviderSection>
                    );
                })}
            </div>
        </div>
    );
}

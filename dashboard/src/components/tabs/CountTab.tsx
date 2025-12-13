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
import { TrendingUp, Hash, Wallet, Info } from 'lucide-react';
import { providerColors, chainColorMap } from '@/lib/chartStyles';
import { ChartViewToggle } from '@/components/ChartViewToggle';
import { aggregateByGranularity, transformToChartData } from '@/lib/dataProcessing';
import { sortProviders } from '@/lib/providerUtils';
import { buildApiUrl, buildQueryParams } from '@/lib/api';

interface CountTabProps {
    range: string;
    startDate?: string | null;
    endDate?: string | null;
    granularity: string;
}

export function CountTab({ range, startDate, endDate, granularity }: CountTabProps) {
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

                // Fetch main count data and all provider data in parallel
                const [countRes, ...providerResponses] = await Promise.all([
                    fetch(buildApiUrl(`/api/swap-count?${paramsString}`)),
                    ...allKnownProviders.map(provider =>
                        fetch(buildApiUrl(`/api/swap-count/provider/${provider}?${paramsString}`))
                            .then(res => res.ok ? res.json() : null)
                            .catch(() => null)
                    )
                ]);

                if (!countRes.ok) throw new Error('Failed to fetch swap count data');

                const countData = await countRes.json();
                setAllData(countData);

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
                console.error('Error fetching swap count data:', err);
                setError('Failed to load swap count data');
            } finally {
                setLoading(false);
            }
        }

        fetchAllData();
    }, [range, startDate, endDate, granularity]); // Re-fetch when parameters change

    // Memoize raw data transformation (only depends on allData)
    const rawChartData = useMemo(() => {
        if (!allData?.countOverTime) return [];
        return allData.countOverTime.map((item: any) => ({
            date: item.date,
            source: item.source,
            count: Number(item.count || item.swap_count)
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

        const chartData = transformToChartData(rawChartData, 'count');

        // Aggregate by granularity (API already filtered by date range)
        const aggregatedChartData = aggregateByGranularity(chartData, granularity as any, providers);

        // Use direct API total values (consistent across all granularities)
        const totalCount = Number(allData.totalCount?.total_count || 0);

        const dataPoints = aggregatedChartData.length || 1;
        const averageCount = totalCount / dataPoints;

        // Provider stats
        const providerStats = providers.map(provider => {
            const count = aggregatedChartData.reduce((sum, item) => sum + (Number(item[provider]) || 0), 0);
            return {
                provider,
                total_count: count,
                swap_count: count
            };
        });

        // Provider breakdowns
        const providerBreakdowns: any = {};
        providers.forEach((provider: string) => {
            const provData = providerData[provider] || {};
            // Calculate total count for this provider (for percentage calculations)
            const providerTotalCount = aggregatedChartData.reduce((sum, item) => sum + (Number(item[provider]) || 0), 0);
            providerBreakdowns[provider] = {
                totalCount: providerTotalCount,
                platformData: aggregatedChartData,
                chainData: aggregatedChartData,
                platformPie: (provData.platforms || provData.chains || []).map((p: any) => ({
                    name: p.platform || p.chain || p.name || 'Unknown',
                    value: Number(p.swap_count || p.count || p.value || 0)
                })),
                chainPie: (provData.chains || provData.platforms || []).map((c: any) => ({
                    name: c.chain || c.platform || c.name || 'Unknown',
                    value: Number(c.swap_count || c.count || c.value || 0)
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
            totalCount,
            averageCount,
            chartData: aggregatedChartData,
            providers,
            providerStats,
            providerBreakdowns
        };
        } catch (err) {
            console.error('[CountTab] Error processing data:', err);
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

    // Platform chart data (must be before early returns for hooks rules)
    const platformChartData = useMemo(() => {
        if (!allData?.countByPlatformOverTime) return [];
        const platformByDate: Record<string, any> = {};

        allData.countByPlatformOverTime.forEach((row: any) => {
            const date = new Date(row.time_period);
            let dateStr: string;

            if (granularity === 'h') {
                dateStr = date.toISOString().slice(0, 16).replace('T', ' ');
            } else if (granularity === 'w') {
                const weekStart = new Date(date);
                weekStart.setDate(date.getDate() - date.getDay());
                dateStr = weekStart.toISOString().split('T')[0];
            } else if (granularity === 'm') {
                dateStr = date.toISOString().slice(0, 7);
            } else {
                dateStr = date.toISOString().split('T')[0];
            }

            if (!platformByDate[dateStr]) {
                platformByDate[dateStr] = { date: dateStr, Android: 0, iOS: 0, Web: 0, Other: 0 };
            }
            platformByDate[dateStr][row.platform] = (platformByDate[dateStr][row.platform] || 0) + Number(row.count);
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
                <div className="text-slate-400 text-lg">Loading swap count data...</div>
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
    const projectedAnnualCount = data.averageCount * projectionMultiplier;

    return (
        <div className="space-y-6">
            {/* Hero Metrics */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <HeroMetric
                    label="Total Swap Count"
                    value={data.totalCount}
                    icon={Hash}
                    color="cyan"
                    format="number"
                />
                <HeroMetric
                    label={`Average Swap Count (${getGranularityLabel()})`}
                    value={data.averageCount}
                    icon={TrendingUp}
                    color="blue"
                    format="number"
                />
                <HeroMetric
                    label="Projected Annual Count"
                    value={projectedAnnualCount}
                    icon={Wallet}
                    color="teal"
                    format="number"
                    tooltip={`Based on ${getGranularityLabel()} average of selected date range`}
                />
            </div>

            {/* Chart View Toggle and Provider/Platform Controls */}
            <div className="glass-card rounded-xl p-4 space-y-4">
                <div className="flex flex-wrap items-center gap-4">
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
                        colors={chainColorMap}
                    />
                )}
            </div>

            {/* Total Swap Count Chart with Cumulative Toggle */}
            <div className="glass-card rounded-xl p-6 space-y-4">
                <div className="flex flex-wrap justify-between items-center gap-2">
                    <div>
                        <h3 className="text-lg font-bold text-white">
                            Total Swap Count by {chartView === 'provider' ? 'Provider' : 'Platform'}
                        </h3>
                        <p className="text-sm text-slate-400">{getGranularityLabel()} breakdown</p>
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
                        currency={false}
                    />
                ) : (
                    (() => {
                        const filteredPlatforms = ['Android', 'iOS', 'Web', 'Other'].filter(p => visiblePlatforms.includes(p));
                        let platformData = platformChartData.map(item => {
                            const filtered: any = { date: item.date };
                            filteredPlatforms.forEach(platform => {
                                filtered[platform] = item[platform] || 0;
                            });
                            return filtered;
                        });
                        if (mainChartCumulative) {
                            platformData = toCumulativeData(platformData, filteredPlatforms);
                        }
                        return (
                            <StackedBarChart
                                title=""
                                subtitle=""
                                data={platformData}
                                keys={filteredPlatforms}
                                colors={chainColorMap}
                                currency={false}
                            />
                        );
                    })()
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
                                            metricType="count"
                                        />
                                        <CumulativeToggle
                                            enabled={cumulativeMode[provider] || false}
                                            onToggle={(enabled) => handleCumulativeModeChange(provider, enabled)}
                                        />
                                    </div>

                                    {/* Render chart based on current view */}
                                    {currentView === 'total' ? (() => {
                                        // Transform and filter total count data
                                        const rawData = specificData?.totalCount?.map((item: any) => ({
                                            date: item.date,
                                            source: provider,
                                            count: Number(item.count || item.swap_count)
                                        })) || [];

                                        const chartData = transformToChartData(rawData, 'count');
                                        let aggregatedData = aggregateByGranularity(chartData, granularity as any, [provider]);

                                        // Apply cumulative transformation if enabled
                                        if (cumulativeMode[provider]) {
                                            aggregatedData = toCumulativeData(aggregatedData, [provider]);
                                        }

                                        const hasData = aggregatedData.length > 0 && aggregatedData.some(item => Number(item[provider]) > 0);

                                        return (
                                            <div className="relative">
                                                <StackedBarChart
                                                    title={`Total Swap Count${cumulativeMode[provider] ? ' (Cumulative)' : ''}`}
                                                    subtitle={`${getGranularityLabel()} breakdown`}
                                                    data={aggregatedData}
                                                    keys={[provider]}
                                                    colors={providerColors}
                                                    currency={false}
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
                                            date: item.date,
                                            source: item.platform || item.chain,
                                            count: Number(item.count || item.swap_count)
                                        })) || [];

                                        const chartData = transformToChartData(rawData, 'count');
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
                                                    title={`Swap Count by ${platformChainView === 'platform' ? 'Platform' : 'Chain'}${cumulativeMode[provider] ? ' (Cumulative)' : ''}`}
                                                    subtitle={`${getGranularityLabel()} breakdown`}
                                                    data={aggregatedData}
                                                    keys={platforms}
                                                    colors={providerColors}
                                                    currency={false}
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
                                            title={`Count Distribution by ${platformChainView === 'platform' ? 'Platform' : 'Chain'}`}
                                            data={(() => {
                                                // Compute pie data from specificData (respects date range filtering)
                                                const breakdown = specificData?.platformBreakdown || [];
                                                const pieData: Record<string, number> = {};

                                                // Aggregate count by platform/chain
                                                breakdown.forEach((item: any) => {
                                                    const key = item.platform || item.chain || 'Unknown';
                                                    pieData[key] = (pieData[key] || 0) + Number(item.count || item.swap_count || 0);
                                                });

                                                // Convert to array format for DonutChart
                                                return Object.entries(pieData).map(([name, value]) => ({
                                                    name,
                                                    value
                                                }));
                                            })()}
                                            colors={providerColors}
                                            currency={false}
                                        />

                                        <TopPathsChart
                                            title="Top 10 Swap Paths"
                                            subtitle="By Count"
                                            data={data.providerBreakdowns[provider].topPaths}
                                            dataKey="count"
                                            total={data.providerBreakdowns[provider].totalCount}
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

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
import { TrendingUp, DollarSign, Wallet } from 'lucide-react';
import { providerColors } from '@/lib/chartStyles';
import { aggregateByGranularity, transformToChartData } from '@/lib/dataProcessing';
import { sortProviders } from '@/lib/providerUtils';
import { SHORT_PARAMS } from '@/lib/urlParams';

interface RevenueTabProps {
    range: string;
    startDate?: string | null;
    endDate?: string | null;
    granularity: string;
}

export function RevenueTab({ range, startDate, endDate, granularity }: RevenueTabProps) {
    const [allData, setAllData] = useState<any>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [visibleProviders, setVisibleProviders] = useState<string[]>([]);
    const [providerViews, setProviderViews] = useState<Record<string, 'total' | 'breakdown'>>({});
    const [providerData, setProviderData] = useState<Record<string, any>>({});
    const [cumulativeMode, setCumulativeMode] = useState<Record<string, boolean>>({});
    const [mainChartCumulative, setMainChartCumulative] = useState(false);

    // Fetch all data in parallel when range, dates, or granularity changes
    useEffect(() => {
        async function fetchAllData() {
            setLoading(true);
            setError(null);

            try {
                // Build query parameters
                const params = new URLSearchParams();
                params.set(SHORT_PARAMS.RANGE, range);
                if (startDate) params.set(SHORT_PARAMS.START_DATE, startDate);
                if (endDate) params.set(SHORT_PARAMS.END_DATE, endDate);
                params.set(SHORT_PARAMS.GRANULARITY, granularity);

                const paramsString = params.toString();

                // Define all known providers upfront
                const allKnownProviders = ['thorchain', 'mayachain', 'lifi', '1inch'];

                // Fetch main revenue data and all provider data in parallel
                const [revenueRes, ...providerResponses] = await Promise.all([
                    fetch(`/api/revenue?${paramsString}`),
                    ...allKnownProviders.map(provider =>
                        fetch(`/api/revenue/provider/${provider}?${paramsString}`)
                            .then(res => res.ok ? res.json() : null)
                            .catch(() => null)
                    )
                ]);

                if (!revenueRes.ok) throw new Error('Failed to fetch revenue data');

                const revenueData = await revenueRes.json();
                setAllData(revenueData);

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
                console.error('Error fetching revenue data:', err);
                setError('Failed to load revenue data');
            } finally {
                setLoading(false);
            }
        }

        fetchAllData();
    }, [range, startDate, endDate, granularity]);

    // Memoize raw data transformation
    const rawChartData = useMemo(() => {
        if (!allData?.revenueOverTime) return [];
        return allData.revenueOverTime.map((item: any) => ({
            date: item.date,
            source: item.source,
            revenue: Number(item.revenue)
        }));
    }, [allData]);

    // Memoize provider list
    const providers = useMemo(() => {
        if (!allData) return [];
        const dataProviders = Array.from(new Set(rawChartData.map((item: any) => item.source)));
        const allProviders = Array.from(new Set([...dataProviders, ...Object.keys(providerData)])) as string[];
        return sortProviders(allProviders);
    }, [allData, rawChartData, providerData]);

    // Process data based on filters (client-side)
    const data = useMemo(() => {
        try {
            if (!allData || rawChartData.length === 0) {
                return null;
            }

            const chartData = transformToChartData(rawChartData, 'revenue');

            // Aggregate by granularity (API already filtered by date range)
            const aggregatedChartData = aggregateByGranularity(chartData, granularity as any, providers);

            // Use direct API total values
            const totalRevenue = Number(allData.totalRevenue?.total_revenue || 0);

            const dataPoints = aggregatedChartData.length || 1;
            const averageRevenue = totalRevenue / dataPoints;

            // Provider stats
            const providerStats = providers.map(provider => {
                const revenue = aggregatedChartData.reduce((sum, item) => sum + (Number(item[provider]) || 0), 0);
                return {
                    provider,
                    total_revenue: revenue
                };
            });

            // Provider breakdowns
            const providerBreakdowns: any = {};
            providers.forEach((provider: string) => {
                const provData = providerData[provider] || {};
                // Calculate total revenue for this provider (for percentage calculations)
                const providerTotalRevenue = aggregatedChartData.reduce((sum, item) => sum + (Number(item[provider]) || 0), 0);
                providerBreakdowns[provider] = {
                    totalRevenue: providerTotalRevenue,
                    platformData: aggregatedChartData,
                    chainData: aggregatedChartData,
                    platformPie: (provData.platforms || provData.chains || []).map((p: any) => ({
                        name: p.name || p.chain_id || 'Unknown',
                        value: Number(p.value || 0)
                    })),
                    chainPie: (provData.chains || provData.platforms || []).map((c: any) => ({
                        name: c.chain_id || c.name || 'Unknown',
                        value: Number(c.value || 0)
                    })),
                    topPaths: (allData.topPaths || [])
                        .filter((path: any) => path.source === provider)
                        .map((path: any) => ({
                            name: path.swap_path,
                            fees: Number(path.total_revenue || 0),
                            count: Number(path.swap_count || 0)
                        }))
                };
            });

            return {
                totalRevenue,
                averageRevenue,
                chartData: aggregatedChartData,
                providers,
                providerStats,
                providerBreakdowns
            };
        } catch (err) {
            console.error('[RevenueTab] Error processing data:', err);
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

    // Transform data to cumulative mode
    const toCumulativeData = useCallback((data: any[], keys: string[]) => {
        const cumulative: any[] = [];
        const runningTotals: Record<string, number> = {};

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

    // Show error if we have no data at all
    if (error && !data) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-red-400 text-lg">{error || 'No data available'}</div>
            </div>
        );
    }

    if (loading && !data) {
        return (
            <div className="flex items-center justify-center py-20">
                <div className="text-slate-400 text-lg">Loading revenue data...</div>
            </div>
        );
    }

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

    // Calculate annual projection
    const projectionMultiplier = granularity === 'h' ? 24 * 365 :
                                 granularity === 'd' ? 365 :
                                 granularity === 'w' ? 52 : 12;
    const projectedAnnualRevenue = data.averageRevenue * projectionMultiplier;

    return (
        <div className="space-y-6">
            {/* Hero Metrics */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <HeroMetric
                    label="Total Revenue"
                    value={data.totalRevenue}
                    icon={DollarSign}
                    color="cyan"
                    format="currency"
                />
                <HeroMetric
                    label={`Average Revenue (${getGranularityLabel()})`}
                    value={data.averageRevenue}
                    icon={TrendingUp}
                    color="blue"
                    format="currency"
                />
                <HeroMetric
                    label="Projected Annual Revenue"
                    value={projectedAnnualRevenue}
                    icon={Wallet}
                    color="teal"
                    format="currency"
                    tooltip={`Based on ${getGranularityLabel()} average of selected date range`}
                />
            </div>

            {/* Provider Toggle Controls */}
            <div className="glass-card rounded-xl p-4">
                <ProviderToggleControl
                    providers={data.providers}
                    visibleProviders={visibleProviders}
                    onToggleProvider={handleToggleProvider}
                    colors={providerColors}
                />
            </div>

            {/* Total Revenue Chart with Cumulative Toggle */}
            <div className="glass-card rounded-xl p-6 space-y-4">
                <div className="flex flex-wrap justify-between items-center gap-2">
                    <div>
                        <h3 className="text-lg font-bold text-white">Total Revenue by Provider</h3>
                        <p className="text-sm text-slate-400">{getGranularityLabel()} breakdown</p>
                    </div>
                    <CumulativeToggle
                        enabled={mainChartCumulative}
                        onToggle={setMainChartCumulative}
                    />
                </div>
                <StackedBarChart
                    title=""
                    subtitle=""
                    data={filteredChartData}
                    keys={filteredProviders}
                    colors={providerColors}
                    currency={true}
                />
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
                                    {/* Toggle controls */}
                                    <div className="flex flex-wrap justify-between items-center gap-2">
                                        <VolumeViewToggle
                                            view={currentView}
                                            onViewChange={(view) => handleProviderViewChange(provider, view)}
                                            platformChainView={platformChainView}
                                            metricType="revenue"
                                        />
                                        <CumulativeToggle
                                            enabled={cumulativeMode[provider] || false}
                                            onToggle={(enabled) => handleCumulativeModeChange(provider, enabled)}
                                        />
                                    </div>

                                    {/* Render chart based on current view */}
                                    {currentView === 'total' ? (() => {
                                        // Transform and filter total revenue data
                                        const rawData = specificData?.totalRevenue?.map((item: any) => ({
                                            date: item.date,
                                            source: provider,
                                            revenue: Number(item.revenue)
                                        })) || [];

                                        const chartData = transformToChartData(rawData, 'revenue');
                                        let aggregatedData = aggregateByGranularity(chartData, granularity as any, [provider]);

                                        // Apply cumulative transformation if enabled
                                        if (cumulativeMode[provider]) {
                                            aggregatedData = toCumulativeData(aggregatedData, [provider]);
                                        }

                                        const hasData = aggregatedData.length > 0 && aggregatedData.some(item => Number(item[provider]) > 0);

                                        return (
                                            <div className="relative">
                                                <StackedBarChart
                                                    title={`Total Revenue${cumulativeMode[provider] ? ' (Cumulative)' : ''}`}
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
                                            date: item.date,
                                            source: item.platform || item.chain,
                                            revenue: Number(item.revenue)
                                        })) || [];

                                        const chartData = transformToChartData(rawData, 'revenue');
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
                                                    title={`Revenue by ${platformChainView === 'platform' ? 'Platform' : 'Chain'}${cumulativeMode[provider] ? ' (Cumulative)' : ''}`}
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
                                            title={`Revenue Distribution by ${platformChainView === 'platform' ? 'Platform' : 'Chain'}`}
                                            data={(() => {
                                                // Compute pie data from specificData
                                                const breakdown = specificData?.platformBreakdown || [];
                                                const pieData: Record<string, number> = {};

                                                breakdown.forEach((item: any) => {
                                                    const key = item.platform || item.chain || 'Unknown';
                                                    pieData[key] = (pieData[key] || 0) + Number(item.revenue || 0);
                                                });

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
                                            subtitle="By Revenue"
                                            data={data.providerBreakdowns[provider].topPaths}
                                            dataKey="fees"
                                            total={data.providerBreakdowns[provider].totalRevenue}
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

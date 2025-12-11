'use client';

import { useState, useEffect, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import { StatsCard } from '@/components/StatsCard';
import { StackedBarChart } from '@/components/StackedBarChart';
import { MetricsSummary } from '@/components/MetricsSummary';
import { TopPathsTable } from '@/components/TopPathsTable';
import { DateRangeSelector } from '@/components/DateRangeSelector';
import { GranularitySelector } from '@/components/GranularitySelector';
import { PlatformDistribution } from '@/components/PlatformDistribution';
import { formatProviderName } from '@/lib/providerUtils';
import { TrendingUp, Activity, Repeat } from 'lucide-react';

function SwapVolumePageContent() {
    const searchParams = useSearchParams();
    const range = searchParams.get('range') || 'all';
    const startDate = searchParams.get('startDate');
    const endDate = searchParams.get('endDate');
    const granularity = searchParams.get('granularity') || (range === '90d' || range === 'all' ? 'week' : 'day');

    const [data, setData] = useState<any | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        async function fetchData() {
            setLoading(true);
            setError(null);
            try {
                const params = new URLSearchParams();
                params.set('range', range);
                if (startDate) params.set('startDate', startDate);
                if (endDate) params.set('endDate', endDate);
                if (granularity) params.set('granularity', granularity);

                const res = await fetch(`/api/swap-volume?${params.toString()}`);
                if (!res.ok) throw new Error('Failed to fetch data');
                const json = await res.json();
                setData(json);
            } catch (err) {
                setError(err instanceof Error ? err.message : 'An error occurred');
            } finally {
                setLoading(false);
            }
        }
        fetchData();
    }, [range, startDate, endDate, granularity]);

    if (loading) {
        return (
            <div className="min-h-screen bg-gradient-to-br from-[#0B1120] via-[#0D1426] to-[#0B1120] p-8">
                <div className="container mx-auto">
                    <div className="flex items-center justify-center h-96">
                        <div className="text-white text-xl">Loading data for range: {range}...</div>
                    </div>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="min-h-screen bg-gradient-to-br from-[#0B1120] via-[#0D1426] to-[#0B1120] p-8">
                <div className="container mx-auto">
                    <div className="flex items-center justify-center h-96">
                        <div className="text-red-400 text-xl">Error: {error}</div>
                    </div>
                </div>
            </div>
        );
    }

    const { globalStats, volumeOverTime, volumeByProvider, topPaths, providerData, metadata } = data;

    console.log('=== FRONTEND DEBUG ===');
    console.log('Range:', range);
    console.log('Granularity:', granularity);
    console.log('StartDate:', startDate);
    console.log('EndDate:', endDate);
    console.log('Raw volumeOverTime from API:', volumeOverTime);
    console.log('Metadata from API:', metadata);

    // Process volume over time data
    const chartData = volumeOverTime.reduce((acc: any[], row: any) => {
        const date = new Date(row.time_period);
        // Format date based on granularity
        let dateStr: string;
        if (granularity === 'hour') {
            // For hourly, show "HH:00" or "MMM DD HH:00" format
            dateStr = date.toLocaleString('en-US', {
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                hour12: false
            }).replace(',', '');
        } else if (granularity === 'month') {
            dateStr = date.toLocaleString('en-US', { year: 'numeric', month: 'short' });
        } else if (granularity === 'week') {
            dateStr = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        } else {
            dateStr = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        }

        const existing = acc.find(item => item.date === dateStr);
        if (existing) {
            existing[row.source] = parseFloat(row.volume);
        } else {
            acc.push({
                date: dateStr,
                [row.source]: parseFloat(row.volume)
            });
        }
        return acc;
    }, []);

    console.log('Processed chartData before filling:', chartData);
    console.log('chartData length:', chartData.length);

    // For hourly granularity, fill in missing hours with zeros
    let filledChartData = chartData;
    if (granularity === 'hour' && chartData.length > 0) {
        console.log('Entering hourly fill logic');
        // Determine the time range based on the selected range
        let startTime: Date;
        let endTime: Date;
        let hoursToShow: number;

        if (range === 'custom' && startDate && endDate) {
            // For custom range, use the selected dates
            startTime = new Date(startDate);
            endTime = new Date(endDate);
            // Calculate hours between dates (limit to 24*7 = 168 hours max for custom)
            const diffMs = endTime.getTime() - startTime.getTime();
            hoursToShow = Math.min(Math.ceil(diffMs / (1000 * 60 * 60)), 168);
            console.log('Custom range detected:', { startTime, endTime, hoursToShow });
        } else if (range === '24h') {
            const now = new Date();
            startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000);
            endTime = now;
            hoursToShow = 24;
            console.log('24h range detected:', { startTime, endTime, hoursToShow });
        } else if (range === '7d') {
            const now = new Date();
            startTime = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
            endTime = now;
            hoursToShow = 24 * 7; // 168 hours
            console.log('7d range detected:', { startTime, endTime, hoursToShow });
        } else {
            // Default to last 24 hours
            const now = new Date();
            startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000);
            endTime = now;
            hoursToShow = 24;
            console.log('Default range:', { startTime, endTime, hoursToShow });
        }

        // Create an entry for each hour
        const allHours: any[] = [];
        for (let i = 0; i < hoursToShow; i++) {
            const hourDate = new Date(startTime.getTime() + i * 60 * 60 * 1000);
            const dateStr = hourDate.toLocaleString('en-US', {
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                hour12: false
            }).replace(',', '');

            // Find existing data for this hour
            const existing = chartData.find((item: any) => item.date === dateStr);
            if (existing) {
                allHours.push(existing);
            } else {
                // Fill with zeros for all providers
                allHours.push({
                    date: dateStr,
                    thorchain: 0,
                    mayachain: 0,
                    lifi: 0,
                    '1inch': 0
                });
            }
        }
        console.log('Generated allHours:', allHours.length, 'entries');
        console.log('First 5 hours:', allHours.slice(0, 5));
        console.log('Last 5 hours:', allHours.slice(-5));
        filledChartData = allHours;
    }

    console.log('Final filledChartData length:', filledChartData.length);
    console.log('Final filledChartData (first 5):', filledChartData.slice(0, 5));

    // Calculate provider breakdowns
    const providerBreakdowns = volumeByProvider.map((row: any) => ({
        label: formatProviderName(row.source),
        value: parseFloat(row.total_volume),
        color: getProviderColor(row.source)
    }));

    return (
        <div className="min-h-screen bg-gradient-to-br from-[#0B1120] via-[#0D1426] to-[#0B1120] p-8">
            <div className="container mx-auto space-y-8">
                {/* Header */}
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-4xl font-bold bg-gradient-to-r from-cyan-400 to-blue-500 bg-clip-text text-transparent">
                            Swap Volume Analytics
                        </h1>
                        <p className="text-slate-400 mt-2">Track swap volumes across all providers</p>
                    </div>
                    <DateRangeSelector />
                </div>


                {/* Global Stats */}
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    <StatsCard
                        title="Total Volume"
                        value={`$${parseFloat(globalStats.total_volume || 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
                        icon={TrendingUp}
                    />
                    <StatsCard
                        title="Total Swaps"
                        value={parseInt(globalStats.total_swaps || 0).toLocaleString()}
                        icon={Activity}
                    />
                    <StatsCard
                        title="Avg Swap Size"
                        value={`$${(parseFloat(globalStats.total_volume || 0) / parseInt(globalStats.total_swaps || 1)).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
                        icon={Repeat}
                    />
                </div>

                {/* Volume Over Time */}
                <StackedBarChart
                    title="Volume by Provider Over Time"
                    subtitle={`Swap volumes over time (${granularity === 'month' ? 'Monthly' : granularity === 'week' ? 'Weekly' : granularity === 'hour' ? 'Hourly' : 'Daily'})`}
                    data={filledChartData}
                    keys={['thorchain', 'mayachain', 'lifi', '1inch']}
                    colors={['#10b981', '#3b82f6', '#8b5cf6', '#f59e0b']}
                    action={<GranularitySelector />}
                />

                {/* Provider Breakdown */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <MetricsSummary
                        title="Volume by Provider"
                        total={parseFloat(globalStats.total_volume || 0)}
                        items={providerBreakdowns}
                    />

                    <TopPathsTable
                        data={topPaths.map((p: any) => ({
                            pool: p.swap_path,
                            volume: parseFloat(p.total_volume),
                            count: parseInt(p.swap_count)
                        }))}
                    />
                </div>

                {/* Provider-Specific Distributions */}
                {Object.entries(providerData).map(([provider, data]: [string, any]) => {
                    if (provider === '1inch') {
                        const chainData = data.chains ? data.chains.map((c: any) => ({
                            name: c.chain, // API returns chain name directly
                            value: parseFloat(c.volume)
                        })) : [];
                        return (
                            <PlatformDistribution
                                key={provider}
                                title={`${formatProviderName(provider)} Chain Distribution`}
                                subtitle="Volume across different blockchain networks"
                                data={chainData}
                            />
                        );
                    } else if (data.platforms) {
                        const platformData = data.platforms.map((p: any) => ({
                            name: p.platform || 'Unknown',
                            value: parseFloat(p.volume)
                        }));
                        return (
                            <PlatformDistribution
                                key={provider}
                                title={`${formatProviderName(provider)} Platform Distribution`}
                                subtitle="Volume by affiliate/platform"
                                data={processPlatforms(platformData)}
                            />
                        );
                    }
                    return null;
                })}
            </div>
        </div>
    );
}

function getProviderColor(provider: string): string {
    const colors: Record<string, string> = {
        thorchain: '#10b981',
        mayachain: '#3b82f6',
        lifi: '#8b5cf6',
        '1inch': '#f59e0b'
    };
    return colors[provider] || '#6b7280';
}

function getChainName(chainId: string): string {
    const chainNames: { [key: string]: string } = {
        '1': 'Ethereum', '56': 'BSC', '137': 'Polygon', '8453': 'Base',
        '43114': 'Avalanche', '10': 'Optimism', '42161': 'Arbitrum'
    };
    return chainNames[chainId] || `Chain ${chainId}`;
}

function processPlatforms(platforms: any[]) {
    const totalValue = platforms.reduce((sum: number, p: any) => sum + p.value, 0);
    const threshold = totalValue * 0.01; // 1% threshold

    const groupedPlatforms: { name: string; value: number }[] = [];
    let othersValue = 0;

    platforms.forEach((p: any) => {
        if (p.value >= threshold) {
            groupedPlatforms.push(p);
        } else {
            othersValue += p.value;
        }
    });

    if (othersValue > 0) {
        groupedPlatforms.push({ name: 'Others', value: othersValue });
    }
    return groupedPlatforms;
}

function getChartSubtitle(range: string): string {
    switch (range) {
        case '24h':
            return 'Hourly';
        case '7d':
        case '30d':
            return 'Daily';
        case '90d':
        case 'all':
            return 'Weekly';
        default:
            return 'Daily/Weekly';
    }
}

export default function SwapVolumePage() {
    return (
        <Suspense fallback={
            <div className="min-h-screen bg-gradient-to-br from-[#020817] via-[#0B1120] to-[#020817] flex items-center justify-center">
                <div className="text-white text-xl">Loading...</div>
            </div>
        }>
            <SwapVolumePageContent />
        </Suspense>
    );
}

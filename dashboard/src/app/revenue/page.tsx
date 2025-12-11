'use client';

import { useState, useEffect, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import { StatsCard } from '@/components/StatsCard';
import { StackedBarChart } from '@/components/StackedBarChart';
import { CumulativeAreaChart } from '@/components/CumulativeAreaChart';
import { MetricsSummary } from '@/components/MetricsSummary';
import { DateRangeSelector } from '@/components/DateRangeSelector';
import { GranularitySelector } from '@/components/GranularitySelector';
import { PlatformDistribution } from '@/components/PlatformDistribution';
import { formatProviderName } from '@/lib/providerUtils';
import { formatCurrency } from '@/lib/queryUtils';
import { Wallet } from 'lucide-react';

function RevenuePageContent() {
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

                const [res, breakdownRes] = await Promise.all([
                    fetch(`/api/revenue?${params.toString()}`),
                    fetch(`/api/revenue-breakdown?${params.toString()}`)
                ]);

                if (!res.ok || !breakdownRes.ok) throw new Error('Failed to fetch data');

                const json = await res.json();
                const breakdownJson = await breakdownRes.json();

                setData({ ...json, breakdown: breakdownJson });
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
                        <div className="text-white text-xl">Loading revenue data...</div>
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

    const { totalRevenue, revenueOverTime, revenueByProvider, providerData } = data;

    // Process revenue over time data
    const revenueByDate: any = {};
    const providers = new Set<string>();

    revenueOverTime.forEach((row: any) => {
        const date = new Date(row.date);
        let dateKey: string;
        if (granularity === 'hour') {
            dateKey = date.toLocaleString('en-US', {
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                hour12: false
            }).replace(',', '');
        } else {
            dateKey = date.toLocaleDateString();
        }

        if (!revenueByDate[dateKey]) {
            revenueByDate[dateKey] = { date: dateKey };
        }
        revenueByDate[dateKey][row.source] = Number(row.revenue);
        providers.add(row.source);
    });

    let revenueChartData = Object.values(revenueByDate);
    const providerKeys = Array.from(providers);
    const providerColors = ['#22d3ee', '#818cf8', '#34d399', '#fbbf24', '#f472b6'];

    // For hourly granularity, fill in missing hours with zeros
    if (granularity === 'hour' && revenueChartData.length > 0) {
        // Determine the time range based on the selected range
        let startTime: Date;
        let endTime: Date;
        let hoursToShow: number;

        if (range === 'custom' && startDate && endDate) {
            startTime = new Date(startDate);
            endTime = new Date(endDate);
            const diffMs = endTime.getTime() - startTime.getTime();
            hoursToShow = Math.min(Math.ceil(diffMs / (1000 * 60 * 60)), 168);
        } else if (range === '24h') {
            const now = new Date();
            startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000);
            endTime = now;
            hoursToShow = 24;
        } else if (range === '7d') {
            const now = new Date();
            startTime = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
            endTime = now;
            hoursToShow = 24 * 7;
        } else {
            const now = new Date();
            startTime = new Date(now.getTime() - 24 * 60 * 60 * 1000);
            endTime = now;
            hoursToShow = 24;
        }

        const allHours: any[] = [];
        for (let i = 0; i < hoursToShow; i++) {
            const hourDate = new Date(startTime.getTime() + i * 60 * 60 * 1000);
            const dateStr = hourDate.toLocaleString('en-US', {
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                hour12: false
            }).replace(',', '');

            const existing = revenueChartData.find((item: any) => item.date === dateStr);
            if (existing) {
                allHours.push(existing);
            } else {
                const emptyHour: any = { date: dateStr };
                providerKeys.forEach(key => {
                    emptyHour[key] = 0;
                });
                allHours.push(emptyHour);
            }
        }
        revenueChartData = allHours;
    }

    // Calculate cumulative data
    const cumulativeData: any[] = [];
    const runningTotals: { [key: string]: number } = {};
    providerKeys.forEach(k => runningTotals[k] = 0);

    revenueChartData.forEach((day: any) => {
        const newPoint: any = { date: day.date };
        providerKeys.forEach(key => {
            runningTotals[key] += (day[key] || 0);
            newPoint[key] = runningTotals[key];
        });
        cumulativeData.push(newPoint);
    });

    // Process provider metrics
    const providerMetrics = revenueByProvider.map((row: any, i: number) => ({
        label: formatProviderName(row.name),
        value: Number(row.value),
        color: providerColors[i % providerColors.length]
    }));

    const providersList = ['thorchain', 'mayachain', 'lifi', '1inch'];

    return (
        <div className="min-h-screen bg-[#0B1120] pb-12">
            <div className="container mx-auto p-8 space-y-8">

                {/* Header Section */}
                <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                    <div>
                        <h1 className="text-3xl font-bold text-white">Revenue Analytics</h1>
                        <p className="text-slate-400 mt-1">Fee revenue generation across all providers</p>
                    </div>
                    <DateRangeSelector />
                </div>

                {/* Key Metrics */}
                <div className="grid gap-4 md:grid-cols-3">
                    <StatsCard
                        title="Total Fee Revenue"
                        value={formatCurrency(Number(totalRevenue?.total_revenue || 0))}
                        icon={Wallet}
                    />
                    <StatsCard
                        title="Daily Average Revenue"
                        subtitle={data.dailyAverageRevenue?.date_range === 'all'
                            ? 'All time daily average'
                            : data.dailyAverageRevenue?.date_range === '24h'
                                ? 'Last 24 hours average'
                                : data.dailyAverageRevenue?.date_range === '7d'
                                    ? 'Last 7 days daily average'
                                    : data.dailyAverageRevenue?.date_range === '30d'
                                        ? 'Last 30 days daily average'
                                        : data.dailyAverageRevenue?.date_range === '90d'
                                            ? 'Last 90 days daily average'
                                            : `Daily average (${data.dailyAverageRevenue?.date_range || 'custom'})`
                        }
                        value={formatCurrency(Number(data.dailyAverageRevenue?.daily_average || 0))}
                        icon={Wallet}
                    />
                </div>

                {/* Main Charts */}
                <div className="grid gap-4 md:grid-cols-2">
                    <StackedBarChart
                        title="Revenue by Provider"
                        subtitle={`Revenue over time (${granularity === 'month' ? 'Monthly' : granularity === 'week' ? 'Weekly' : 'Daily'})`}
                        data={revenueChartData}
                        keys={providerKeys}
                        colors={providerColors}
                        action={<GranularitySelector />}
                    />
                    <CumulativeAreaChart
                        title="Cumulative Revenue"
                        subtitle="Accumulated fees over selected period"
                        data={cumulativeData}
                        keys={providerKeys}
                        colors={providerColors}
                    />
                </div>

                {/* Distribution */}
                <div className="grid gap-4 md:grid-cols-3">
                    <div className="md:col-span-1">
                        <MetricsSummary
                            title="Revenue Distribution"
                            total={Number(totalRevenue?.total_revenue || 0)}
                            items={providerMetrics}
                        />
                    </div>
                </div>

                {/* Provider Specific Sections */}
                {providersList.map(provider => {
                    // Handle 1inch chain distribution
                    if (provider === '1inch' && providerData['1inch']?.chains) {
                        const chainData = providerData['1inch'].chains;

                        // Arkham data already returns chain names (Ethereum, BSC, etc.)
                        // No mapping needed, just use the value directly
                        const formattedChains = chainData.map((c: any) => ({
                            name: c.chain_id, // API returns chain name in chain_id field
                            value: parseFloat(c.value)
                        }));

                        return (
                            <div key="1inch-chains" className="space-y-4 pt-8 border-t border-slate-800">
                                <h2 className="text-2xl font-bold text-white">1inch Chain Distribution</h2>
                                <div className="grid gap-4 md:grid-cols-2">
                                    <PlatformDistribution
                                        data={processPlatforms(formattedChains)}
                                        title="Revenue by Chain"
                                        subtitle="1inch fees across chains"
                                    />
                                </div>
                            </div>
                        );
                    }

                    // Handle other providers (thorchain, mayachain, lifi)
                    const pData = providerData[provider];
                    if (!pData?.platforms || pData.platforms.length === 0) return null;

                    const displayName = formatProviderName(provider);

                    return (
                        <div key={provider} className="space-y-4 pt-8 border-t border-slate-800">
                            <h2 className="text-2xl font-bold text-white capitalize">{displayName} Chain Distribution</h2>
                            <div className="grid gap-4 md:grid-cols-2">
                                <PlatformDistribution
                                    data={processPlatforms(pData.platforms)}
                                />
                            </div>
                        </div>
                    );
                })}

            </div>
        </div>
    );
}



function processPlatforms(platforms: any[]) {
    const rawPlatforms = platforms.map((row: any) => ({ name: row.name || 'Unknown', value: Number(row.value) }));
    const totalValue = rawPlatforms.reduce((sum: number, p: any) => sum + p.value, 0);
    const threshold = totalValue * 0.01; // 1% threshold

    const groupedPlatforms: { name: string; value: number }[] = [];
    let othersValue = 0;

    rawPlatforms.forEach((p: any) => {
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

export default function RevenuePage() {
    return (
        <Suspense fallback={
            <div className="min-h-screen bg-gradient-to-br from-[#020817] via-[#0B1120] to-[#020817] flex items-center justify-center">
                <div className="text-white text-xl">Loading...</div>
            </div>
        }>
            <RevenuePageContent />
        </Suspense>
    );
}

'use client';

import { useState, useEffect, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import { StatsCard } from '@/components/StatsCard';
import { StackedBarChart } from '@/components/StackedBarChart';
import { MetricsSummary } from '@/components/MetricsSummary';
import { DateRangeSelector } from '@/components/DateRangeSelector';
import { GranularitySelector } from '@/components/GranularitySelector';
import { PlatformDistribution } from '@/components/PlatformDistribution';
import { formatProviderName } from '@/lib/providerUtils';
import { formatNumber } from '@/lib/queryUtils';
import { Users, Layers, Smartphone } from 'lucide-react';

function UsersPageContent() {
    const searchParams = useSearchParams();
    const range = searchParams.get('range') || 'all';
    const startDate = searchParams.get('startDate');
    const endDate = searchParams.get('endDate');
    const granularity = searchParams.get('granularity') || 'month';

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

                const res = await fetch(`/api/users?${params.toString()}`);
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
                        <div className="text-white text-xl">Loading user data...</div>
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

    const { totalUsers, usersOverTime, usersByPlatform, usersByProvider, swapCountByProvider } = data;

    // Process users over time data
    const usersByDate: any = {};
    const providers = new Set<string>();

    usersOverTime.forEach((row: any) => {
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

        if (!usersByDate[dateKey]) {
            usersByDate[dateKey] = { date: dateKey };
        }
        usersByDate[dateKey][row.source] = Number(row.users);
        providers.add(row.source);
    });

    let usersChartData = Object.values(usersByDate);
    const providerKeys = Array.from(providers);
    const providerColors = ['#22d3ee', '#818cf8', '#34d399', '#fbbf24', '#f472b6'];

    // For hourly granularity, fill in missing hours with zeros
    if (granularity === 'hour' && usersChartData.length > 0) {
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

            const existing = usersChartData.find((item: any) => item.date === dateStr);
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
        usersChartData = allHours;
    }

    // Process provider metrics
    // Process provider metrics
    const providerMetrics = usersByProvider.map((row: any, i: number) => ({
        label: formatProviderName(row.name),
        value: Number(row.value),
        color: providerColors[i % providerColors.length]
    }));
    // Process swap count metrics
    const swapCountMetrics = (swapCountByProvider || []).map((row: any, i: number) => ({
        label: formatProviderName(row.name),
        value: Number(row.value),
        color: providerColors[i % providerColors.length]
    }));
    return (
        <div className="min-h-screen bg-[#0B1120] pb-12">
            <div className="container mx-auto p-8 space-y-8">

                {/* Header Section */}
                <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                    <div>
                        <h1 className="text-3xl font-bold text-white">User Analytics</h1>
                        <p className="text-slate-400 mt-1">Insights into user growth and platform adoption</p>
                    </div>
                    <DateRangeSelector />
                </div>

                {/* Key Metrics */}
                <div className="grid gap-4 md:grid-cols-4">
                    <StatsCard
                        title="Total Unique Swappers"
                        value={formatNumber(Number(totalUsers?.unique_users || 0))}
                        icon={Users}
                    />
                    <StatsCard
                        title="Average Unique Swappers"
                        subtitle={data.averageUsers?.date_range === 'all'
                            ? 'All time daily average'
                            : data.averageUsers?.date_range === '24h'
                                ? 'Last 24 hours average'
                                : data.averageUsers?.date_range === '7d'
                                    ? 'Last 7 days daily average'
                                    : data.averageUsers?.date_range === '30d'
                                        ? 'Last 30 days daily average'
                                        : data.averageUsers?.date_range === '90d'
                                            ? 'Last 90 days daily average'
                                            : `Daily average (${data.averageUsers?.date_range || 'custom'})`
                        }
                        value={formatNumber(Number(data.averageUsers?.daily_average || 0))}
                        icon={Users}
                    />
                    <StatsCard
                        title="Total Swap Count"
                        value={formatNumber(Number(data.totalSwapCount?.total_swaps || 0))}
                        icon={Layers}
                    />
                    <StatsCard
                        title="Average Swap Count"
                        subtitle={data.averageSwapCount?.date_range === 'all'
                            ? 'All time daily average'
                            : data.averageSwapCount?.date_range === '24h'
                                ? 'Last 24 hours average'
                                : data.averageSwapCount?.date_range === '7d'
                                    ? 'Last 7 days daily average'
                                    : data.averageSwapCount?.date_range === '30d'
                                        ? 'Last 30 days daily average'
                                        : data.averageSwapCount?.date_range === '90d'
                                            ? 'Last 90 days daily average'
                                            : `Daily average (${data.averageSwapCount?.date_range || 'custom'})`
                        }
                        value={formatNumber(Number(data.averageSwapCount?.daily_average || 0))}
                        icon={Layers}
                    />
                </div>

                {/* Main Charts */}
                <div className="grid gap-4 md:grid-cols-3">
                    <div className="md:col-span-2">
                        <StackedBarChart
                            title="Unique Swappers by Provider"
                            subtitle={`Users over time (${granularity === 'month' ? 'Monthly' : granularity === 'week' ? 'Weekly' : 'Daily'})`}
                            data={usersChartData}
                            keys={providerKeys}
                            colors={providerColors}
                            currency={false}
                            action={<GranularitySelector />}
                        />
                    </div>
                    <div className="md:col-span-1 space-y-4">
                        <MetricsSummary
                            title="User Distribution by Provider"
                            total={Number(totalUsers?.unique_users || 0)}
                            items={providerMetrics}
                            currency={false}
                        />
                        <MetricsSummary
                            title="Swap Count by Provider"
                            total={swapCountMetrics.reduce((acc: number, item: any) => acc + item.value, 0)}
                            items={swapCountMetrics}
                            currency={false}
                        />
                    </div>
                </div>

                {/* Platform Distribution Section */}
                <div className="space-y-4">
                    <div>
                        <h2 className="text-2xl font-bold text-white">Platform Distribution</h2>
                        <p className="text-slate-400 mt-1">Breakdown of user activity across different platforms</p>
                    </div>
                    <div className="grid gap-4 md:grid-cols-2">
                        <PlatformDistribution
                            title="Unique Swappers by Platform"
                            subtitle=""
                            data={processPlatforms(usersByPlatform.map((row: any) => ({
                                name: row.name || 'Unknown',
                                value: Number(row.value)
                            })))}
                            currency={false}
                        />
                        <PlatformDistribution
                            title="Swap Count by Platform"
                            subtitle=""
                            data={processPlatforms(data.swapCountByPlatform?.map((row: any) => ({
                                name: row.name || 'Unknown',
                                value: Number(row.value)
                            })) || [])}
                            currency={false}
                        />
                    </div>
                </div>

            </div>
        </div>
    );
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

export default function UsersPage() {
    return (
        <Suspense fallback={
            <div className="min-h-screen bg-gradient-to-br from-[#020817] via-[#0B1120] to-[#020817] flex items-center justify-center">
                <div className="text-white text-xl">Loading...</div>
            </div>
        }>
            <UsersPageContent />
        </Suspense>
    );
}

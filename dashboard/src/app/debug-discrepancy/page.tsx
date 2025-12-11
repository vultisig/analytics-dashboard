'use client';

import { useEffect, useState } from 'react';

interface Comparison {
    date: string;
    ref_total_revenue: number;
    our_total_revenue: number;
    total_revenue_diff: number;
    total_revenue_pct: number;
    ref_total_volume: number;
    our_total_volume: number;
    total_volume_diff: number;
    total_volume_pct: number;
    ref_total_swappers: number;
    our_total_swappers: number;
    total_swappers_diff: number;
    total_swappers_pct: number;
    total_swaps: number;
}

interface Stats {
    total_days: number;
    revenue_diff: { min: number; max: number; avg: number; median: number; };
    revenue_pct: { min: number; max: number; avg: number; median: number; };
    volume_diff: { min: number; max: number; avg: number; median: number; };
    volume_pct: { min: number; max: number; avg: number; median: number; };
    swappers_diff: { min: number; max: number; avg: number; median: number; };
    swappers_pct: { min: number; max: number; avg: number; median: number; };
    total_ref_revenue: number;
    total_our_revenue: number;
    total_ref_volume: number;
    total_our_volume: number;
    total_ref_swappers: number;
    total_our_swappers: number;
}

export default function DiscrepancyDebugPage() {
    const [comparisons, setComparisons] = useState<Comparison[]>([]);
    const [stats, setStats] = useState<Stats | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetch('/api/discrepancy')
            .then(res => res.json())
            .then(data => {
                setComparisons(data.comparisons);
                setStats(data.stats);
                setLoading(false);
            });
    }, []);

    if (loading) {
        return <div className="p-8">Loading...</div>;
    }

    const formatNumber = (num: number | null | undefined) => {
        if (num === null || num === undefined || isNaN(num)) return '0.00';
        return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    };
    const formatPct = (num: number | null | undefined) => {
        if (num === null || num === undefined || isNaN(num)) return '0.00%';
        return num.toFixed(2) + '%';
    };
    const formatInt = (num: number | null | undefined) => {
        if (num === null || num === undefined || isNaN(num)) return '0';
        return num.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
    };

    return (
        <div className="p-8 min-h-screen bg-[#0A0F1E]">
            <h1 className="text-4xl font-bold text-white mb-2">Data Discrepancy Analysis</h1>
            <p className="text-slate-400 mb-8">Comparing our data vs Raynalytics reference data</p>

            {/* Statistics Cards */}
            {stats && (
                <div className="space-y-6 mb-8">
                    {/* Revenue Statistics */}
                    <div className="bg-[#0F1629] border border-slate-800 rounded-lg p-6">
                        <h3 className="text-lg font-bold text-white mb-4">Revenue Statistics</h3>
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Total Days</p>
                                <p className="text-2xl font-bold text-white">{stats.total_days}</p>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Min Diff</p>
                                <p className="text-2xl font-bold text-white">${formatNumber(stats.revenue_diff.min)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.revenue_pct.min)}</p>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Max Diff</p>
                                <p className="text-2xl font-bold text-white">${formatNumber(stats.revenue_diff.max)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.revenue_pct.max)}</p>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Avg Diff</p>
                                <p className="text-2xl font-bold text-white">${formatNumber(stats.revenue_diff.avg)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.revenue_pct.avg)}</p>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Median Diff</p>
                                <p className="text-2xl font-bold text-white">${formatNumber(stats.revenue_diff.median)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.revenue_pct.median)}</p>
                            </div>
                        </div>
                    </div>

                    {/* Volume Statistics */}
                    <div className="bg-[#0F1629] border border-slate-800 rounded-lg p-6">
                        <h3 className="text-lg font-bold text-white mb-4">Volume Statistics</h3>
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Total Diff</p>
                                <p className="text-2xl font-bold text-white">${formatNumber(stats.total_our_volume - stats.total_ref_volume)}</p>
                                <div className="text-slate-500 text-xs mt-1">
                                    <div>Ref: ${formatNumber(stats.total_ref_volume)}</div>
                                    <div>Our: ${formatNumber(stats.total_our_volume)}</div>
                                </div>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Min Diff</p>
                                <p className="text-2xl font-bold text-white">${formatNumber(stats.volume_diff.min)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.volume_pct.min)}</p>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Max Diff</p>
                                <p className="text-2xl font-bold text-white">${formatNumber(stats.volume_diff.max)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.volume_pct.max)}</p>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Avg Diff</p>
                                <p className="text-2xl font-bold text-white">${formatNumber(stats.volume_diff.avg)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.volume_pct.avg)}</p>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Median Diff</p>
                                <p className="text-2xl font-bold text-white">${formatNumber(stats.volume_diff.median)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.volume_pct.median)}</p>
                            </div>
                        </div>
                    </div>

                    {/* Swappers Statistics */}
                    <div className="bg-[#0F1629] border border-slate-800 rounded-lg p-6">
                        <h3 className="text-lg font-bold text-white mb-4">Swappers Statistics</h3>
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Total Diff</p>
                                <p className="text-2xl font-bold text-white">{formatInt(stats.total_our_swappers - stats.total_ref_swappers)}</p>
                                <div className="text-slate-500 text-xs mt-1">
                                    <div>Ref: {formatInt(stats.total_ref_swappers)}</div>
                                    <div>Our: {formatInt(stats.total_our_swappers)}</div>
                                </div>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Min Diff</p>
                                <p className="text-2xl font-bold text-white">{formatInt(stats.swappers_diff.min)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.swappers_pct.min)}</p>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Max Diff</p>
                                <p className="text-2xl font-bold text-white">{formatInt(stats.swappers_diff.max)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.swappers_pct.max)}</p>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Avg Diff</p>
                                <p className="text-2xl font-bold text-white">{formatInt(stats.swappers_diff.avg)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.swappers_pct.avg)}</p>
                            </div>
                            <div>
                                <p className="text-slate-400 text-xs mb-1">Median Diff</p>
                                <p className="text-2xl font-bold text-white">{formatInt(stats.swappers_diff.median)}</p>
                                <p className="text-slate-500 text-xs">{formatPct(stats.swappers_pct.median)}</p>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* Daily Comparison Table */}
            <div className="bg-[#0F1629] border border-slate-800 rounded-lg overflow-hidden">
                <div className="p-6 border-b border-slate-800">
                    <h2 className="text-2xl font-bold text-white">Daily Comparison</h2>
                </div>

                <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                    <table className="w-full text-sm">
                        <thead className="bg-[#1A1F35] text-slate-400 sticky top-0 z-20">
                            <tr>
                                <th className="sticky left-0 z-30 bg-[#1A1F35] px-4 py-3 text-left font-medium border-r border-slate-700">Date</th>
                                <th className="px-4 py-3 text-right font-medium">Ref Rev</th>
                                <th className="px-4 py-3 text-right font-medium">Our Rev</th>
                                <th className="px-4 py-3 text-right font-medium">Rev Diff ($)</th>
                                <th className="px-4 py-3 text-right font-medium">Rev Diff (%)</th>
                                <th className="px-4 py-3 text-right font-medium">Ref Vol</th>
                                <th className="px-4 py-3 text-right font-medium">Our Vol</th>
                                <th className="px-4 py-3 text-right font-medium">Vol Diff ($)</th>
                                <th className="px-4 py-3 text-right font-medium">Vol Diff (%)</th>
                                <th className="px-4 py-3 text-right font-medium">Ref Swappers</th>
                                <th className="px-4 py-3 text-right font-medium">Our Swappers</th>
                                <th className="px-4 py-3 text-right font-medium">Swappers Diff (#)</th>
                                <th className="px-4 py-3 text-right font-medium">Swappers Diff (%)</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-800">
                            {comparisons && comparisons.slice().reverse().map((comp, idx) => (
                                <tr key={comp.date} className={idx % 2 === 0 ? 'bg-[#0F1629]' : 'bg-[#0A0F1E]'}>
                                    <td className="sticky left-0 z-10 px-4 py-3 text-white font-mono border-r border-slate-700 bg-inherit">{comp.date}</td>
                                    <td className="px-4 py-3 text-right text-slate-300 font-mono">${formatNumber(comp.ref_total_revenue)}</td>
                                    <td className="px-4 py-3 text-right text-white font-mono">${formatNumber(comp.our_total_revenue)}</td>
                                    <td className={`px-4 py-3 text-right font-mono ${comp.total_revenue_diff > 0 ? 'text-green-400' :
                                        comp.total_revenue_diff < 0 ? 'text-red-400' : 'text-slate-400'
                                        }`}>
                                        {comp.total_revenue_diff > 0 ? '+' : ''}${formatNumber(comp.total_revenue_diff)}
                                    </td>
                                    <td className={`px-4 py-3 text-right font-mono ${comp.total_revenue_pct > 0 ? 'text-green-400' :
                                        comp.total_revenue_pct < 0 ? 'text-red-400' : 'text-slate-400'
                                        }`}>
                                        {comp.total_revenue_pct > 0 ? '+' : ''}{formatPct(comp.total_revenue_pct)}
                                    </td>
                                    <td className="px-4 py-3 text-right text-slate-300 font-mono">${formatNumber(comp.ref_total_volume)}</td>
                                    <td className="px-4 py-3 text-right text-white font-mono">${formatNumber(comp.our_total_volume)}</td>
                                    <td className={`px-4 py-3 text-right font-mono ${comp.total_volume_diff > 0 ? 'text-green-400' :
                                        comp.total_volume_diff < 0 ? 'text-red-400' : 'text-slate-400'
                                        }`}>
                                        {comp.total_volume_diff > 0 ? '+' : ''}${formatNumber(comp.total_volume_diff)}
                                    </td>
                                    <td className={`px-4 py-3 text-right font-mono ${comp.total_volume_pct > 0 ? 'text-green-400' :
                                        comp.total_volume_pct < 0 ? 'text-red-400' : 'text-slate-400'
                                        }`}>
                                        {comp.total_volume_pct > 0 ? '+' : ''}{formatPct(comp.total_volume_pct)}
                                    </td>
                                    <td className="px-4 py-3 text-right text-slate-300 font-mono">{formatInt(comp.ref_total_swappers)}</td>
                                    <td className="px-4 py-3 text-right text-white font-mono">{formatInt(comp.our_total_swappers)}</td>
                                    <td className={`px-4 py-3 text-right font-mono ${comp.total_swappers_diff > 0 ? 'text-green-400' :
                                        comp.total_swappers_diff < 0 ? 'text-red-400' : 'text-slate-400'
                                        }`}>
                                        {comp.total_swappers_diff > 0 ? '+' : ''}{formatInt(comp.total_swappers_diff)}
                                    </td>
                                    <td className={`px-4 py-3 text-right font-mono ${comp.total_swappers_pct > 0 ? 'text-green-400' :
                                        comp.total_swappers_pct < 0 ? 'text-red-400' : 'text-slate-400'
                                        }`}>
                                        {comp.total_swappers_pct > 0 ? '+' : ''}{formatPct(comp.total_swappers_pct)}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}

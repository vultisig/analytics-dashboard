'use client';

import { useState } from 'react';
import { BarChart, Bar, XAxis, YAxis, Cell, Tooltip, ResponsiveContainer } from 'recharts';
import { ChartCard } from './ChartCard';
import { providerColors } from '@/lib/chartStyles';

interface TopPathsChartProps {
    title: string;
    subtitle?: string;
    data: any[];
    dataKey?: string;
    height?: number;
    /** Optional total value for calculating percentages. If not provided, uses sum of top 10 items. */
    total?: number;
}

export function TopPathsChart({
    title,
    subtitle,
    data,
    dataKey = 'volume',
    height = 150,
    total: providedTotal
}: TopPathsChartProps) {
    const [hoveredIndex, setHoveredIndex] = useState<number | null>(null);
    const [showPercentage, setShowPercentage] = useState(true);

    // Sort data by the specified dataKey descending and take top 10
    const sortedData = [...data]
        .sort((a, b) => (b[dataKey] || 0) - (a[dataKey] || 0))
        .slice(0, 10);

    // Calculate total for percentage - use provided total if available, otherwise sum of top 10
    const top10Sum = sortedData.reduce((sum, item) => sum + (item[dataKey] || 0), 0);
    const total = providedTotal ?? top10Sum;

    // Check if we have data to display
    const hasData = sortedData.length > 0 && total > 0;

    // Transform data for stacked horizontal bar - normalize to 100% within top 10 for visual consistency
    // This ensures the bar chart fills completely while percentages in legend show true totals
    const stackedData = [{
        name: 'Top 10 Swap Paths',
        ...Object.fromEntries(sortedData.map((item, index) => [
            `path${index}`,
            top10Sum > 0 ? ((item[dataKey] || 0) / top10Sum) * 100 : 0
        ]))
    }];

    // Function to clean swap path labels - remove contract addresses
    const cleanSwapPathLabel = (label: string) => {
        // Pattern: TOKEN-0xADDRESS or TOKEN.SYMBOL-0xADDRESS
        // Replace with just TOKEN or TOKEN.SYMBOL
        return label.replace(/-0x[a-fA-F0-9]{40}/gi, '');
    };

    // Format dollar value with M/K postfix
    const formatDollarValue = (value: number) => {
        if (value >= 1000000) {
            return `$${(value / 1000000).toFixed(1)}M`;
        } else if (value >= 1000) {
            return `$${(value / 1000).toFixed(1)}K`;
        } else {
            return `$${value.toFixed(0)}`;
        }
    };

    // Determine label based on dataKey
    const getDataLabel = () => {
        switch (dataKey) {
            case 'volume': return 'Volume';
            case 'fees': return 'Fees';
            case 'count': return 'Count';
            default: return 'Value';
        }
    };

    // Custom tooltip
    const CustomTooltip = ({ active, payload }: any) => {
        if (active && payload && payload.length) {
            // Use hoveredIndex if available, otherwise fall back to payload
            const pathIndex = hoveredIndex !== null ? hoveredIndex : parseInt(payload[0].dataKey.replace('path', ''));
            const pathData = sortedData[pathIndex];
            const percentage = ((pathData[dataKey] / total) * 100).toFixed(1);
            const cleanedName = cleanSwapPathLabel(pathData.name);

            return (
                <div className="glass-card rounded-xl p-3 shadow-xl">
                    <p className="text-slate-200 font-medium mb-2">{cleanedName}</p>
                    <div className="space-y-1">
                        <div className="flex items-center justify-between gap-4">
                            <span className="text-slate-400 text-sm">{getDataLabel()}:</span>
                            <span className="text-cyan-400 font-bold">
                                {dataKey === 'count'
                                    ? new Intl.NumberFormat('en-US').format(pathData[dataKey])
                                    : `$${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(pathData[dataKey])}`
                                }
                            </span>
                        </div>
                        <div className="flex items-center justify-between gap-4">
                            <span className="text-slate-400 text-sm">Percentage:</span>
                            <span className="text-white font-semibold">{percentage}%</span>
                        </div>
                        {pathData.count && dataKey !== 'count' && (
                            <div className="flex items-center justify-between gap-4">
                                <span className="text-slate-400 text-sm">Count:</span>
                                <span className="text-white font-semibold">
                                    {new Intl.NumberFormat('en-US').format(pathData.count)}
                                </span>
                            </div>
                        )}
                    </div>
                </div>
            );
        }
        return null;
    };

    return (
        <ChartCard title={title} subtitle={subtitle}>
            {hasData ? (
                <div className="space-y-4">
                    {/* Stacked Bar Chart */}
                    <ResponsiveContainer width="100%" height={height}>
                        <BarChart
                            data={stackedData}
                            layout="vertical"
                            margin={{ top: 20, right: 30, left: 20, bottom: 20 }}
                        >
                            <XAxis
                                type="number"
                                domain={[0, 100]}
                                hide
                            />
                            <YAxis
                                type="category"
                                dataKey="name"
                                hide
                            />
                            <Tooltip content={<CustomTooltip />} cursor={false} />
                            {sortedData.map((item, index) => (
                                <Bar
                                    key={`path${index}`}
                                    dataKey={`path${index}`}
                                    stackId="a"
                                    fill={providerColors[index % providerColors.length]}
                                    radius={index === 0 ? [4, 0, 0, 4] : index === sortedData.length - 1 ? [0, 4, 4, 0] : [0, 0, 0, 0]}
                                    barSize={60}
                                    onMouseEnter={() => setHoveredIndex(index)}
                                    onMouseLeave={() => setHoveredIndex(null)}
                                />
                            ))}
                        </BarChart>
                    </ResponsiveContainer>

                    {/* Legend */}
                    <div className="grid grid-cols-2 gap-x-2 gap-y-0.5 md:gap-2 px-2 md:px-4">
                        {sortedData.map((item, index) => {
                            const percentage = ((item[dataKey] / total) * 100).toFixed(1);
                            const dollarValue = item[dataKey] || 0;
                            const cleanedLabel = cleanSwapPathLabel(item.name);
                            return (
                                <div
                                    key={index}
                                    className="flex items-center gap-1 md:gap-2 cursor-pointer hover:bg-white/5 rounded px-0.5 md:px-1 py-0.5 transition-colors"
                                    onMouseEnter={() => setHoveredIndex(index)}
                                    onMouseLeave={() => setHoveredIndex(null)}
                                    onClick={() => setShowPercentage(!showPercentage)}
                                    title={`${item.name} - Click to toggle between % and $ value`}
                                >
                                    <div
                                        className="w-2.5 h-2.5 md:w-3 md:h-3 rounded-sm flex-shrink-0"
                                        style={{ backgroundColor: providerColors[index % providerColors.length] }}
                                    />
                                    <span className="text-[10px] md:text-xs text-slate-300 truncate">
                                        {hoveredIndex === index ? item.name : cleanedLabel}
                                    </span>
                                    <span className="text-[10px] md:text-xs text-slate-400 ml-auto font-medium">
                                        {showPercentage
                                            ? `${percentage}%`
                                            : dataKey === 'count'
                                                ? new Intl.NumberFormat('en-US', { notation: 'compact', compactDisplay: 'short' }).format(dollarValue)
                                                : formatDollarValue(dollarValue)
                                        }
                                    </span>
                                </div>
                            );
                        })}
                    </div>
                </div>
            ) : (
                <div className="flex items-center justify-center h-[300px] w-full">
                    <div className="text-center">
                        <p className="text-slate-400 text-sm">No swap paths available for the selected time range</p>
                    </div>
                </div>
            )}
        </ChartCard>
    );
}

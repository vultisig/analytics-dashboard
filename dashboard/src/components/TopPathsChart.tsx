'use client';

import { useState } from 'react';
import { ChartCard } from './ChartCard';
import { providerColors } from '@/lib/chartStyles';

interface TopPathsChartProps {
    title: string;
    subtitle?: string;
    data: any[];
    dataKey?: string;
    /** Optional total value for calculating percentages. If not provided, uses sum of top 10 items. */
    total?: number;
}

export function TopPathsChart({
    title,
    subtitle,
    data,
    dataKey = 'volume',
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

    // Largest percentage among top 10 — used to scale bar widths so the leader fills its track.
    const maxPercentage = sortedData.reduce(
        (max, item) => Math.max(max, total > 0 ? ((item[dataKey] || 0) / total) * 100 : 0),
        0
    );

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

    return (
        <ChartCard title={title} subtitle={subtitle}>
            {hasData ? (
                <div className="-mx-1 grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-3">
                    {sortedData.map((item, index) => {
                        const rawPercentage = total > 0 ? ((item[dataKey] || 0) / total) * 100 : 0;
                        const percentage = rawPercentage.toFixed(1);
                        const dollarValue = item[dataKey] || 0;
                        const cleanedLabel = cleanSwapPathLabel(item.name);
                        const color = providerColors[index % providerColors.length];
                        // Scale bar fill so the largest item reaches 100% of the track width.
                        const barWidth = maxPercentage > 0 ? (rawPercentage / maxPercentage) * 100 : 0;

                        return (
                            <div
                                key={index}
                                className="cursor-pointer rounded px-1 py-1 transition-colors hover:bg-white/5"
                                onMouseEnter={() => setHoveredIndex(index)}
                                onMouseLeave={() => setHoveredIndex(null)}
                                onClick={() => setShowPercentage(!showPercentage)}
                                title={`${item.name} - Click to toggle between % and $ value`}
                            >
                                <div className="flex items-center gap-2">
                                    <span className="truncate text-xs md:text-sm text-[var(--text-secondary)]">
                                        {hoveredIndex === index ? item.name : cleanedLabel}
                                    </span>
                                    <span className="ml-auto text-xs md:text-sm font-medium text-[var(--text-tertiary)]">
                                        {showPercentage
                                            ? `${percentage}%`
                                            : dataKey === 'count'
                                                ? new Intl.NumberFormat('en-US', { notation: 'compact', compactDisplay: 'short' }).format(dollarValue)
                                                : formatDollarValue(dollarValue)
                                        }
                                    </span>
                                </div>
                                <div className="mt-1.5 h-1 w-full overflow-hidden rounded-full bg-white/5">
                                    <div
                                        className="h-full rounded-full transition-all"
                                        style={{
                                            width: `${barWidth}%`,
                                            backgroundColor: color,
                                        }}
                                    />
                                </div>
                            </div>
                        );
                    })}
                </div>
            ) : (
                <div className="flex items-center justify-center h-[300px] w-full">
                    <div className="text-center">
                        <p className="text-[var(--text-tertiary)] text-sm">No swap paths available for the selected time range</p>
                    </div>
                </div>
            )}
        </ChartCard>
    );
}

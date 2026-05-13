'use client';

import { Pie, PieChart, Cell, ResponsiveContainer, Tooltip } from 'recharts';
import { ChartCard } from './ChartCard';
import { formatProviderName } from '@/lib/providerUtils';
import { glassTooltipStyle, providerColorMap, chainColorMap, fallbackChainColors } from '@/lib/chartStyles';

interface DonutChartProps {
    title: string;
    subtitle?: string;
    data: { name: string; value: number }[];
    colors: string[];
    currency?: boolean;
}

const formatValue = (value: number, currency: boolean) =>
    currency
        ? `$${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(value)}`
        : new Intl.NumberFormat('en-US').format(value);

interface CustomTooltipProps {
    active?: boolean;
    payload?: ReadonlyArray<{ name?: string | number; value?: number }>;
    total: number;
    currency: boolean;
}

function CustomTooltip({ active, payload, total, currency }: CustomTooltipProps) {
    if (!active || !payload || payload.length === 0) return null;

    const item = payload[0];
    const value = Number(item.value ?? 0);
    const name = formatProviderName(String(item.name ?? ''));
    const percentage = ((value / total) * 100).toFixed(1);

    return (
        <div style={glassTooltipStyle} className="p-3">
            <p className="text-[var(--text-primary)] font-semibold mb-1">{name}</p>
            <p className="text-[var(--text-primary)] font-bold">{formatValue(value, currency)}</p>
            <p className="text-[var(--text-tertiary)] text-sm mt-1">{percentage}%</p>
        </div>
    );
}

export function DonutChart({ title, subtitle, data, currency = true }: DonutChartProps) {
    const activeData = data.filter(d => d.value > 0);
    const total = activeData.reduce((sum, item) => sum + item.value, 0);
    const hasData = activeData.length > 0 && total > 0;

    const sortedData = [...activeData].sort((a, b) => b.value - a.value);

    const colorFor = (name: string, index: number) => {
        const nameLower = name.toLowerCase();
        return providerColorMap[nameLower] ||
               chainColorMap[nameLower] ||
               fallbackChainColors[index % fallbackChainColors.length];
    };

    return (
        <ChartCard title={title} subtitle={subtitle}>
            {hasData ? (
                <div className="flex flex-col gap-10">
                    <div className="w-full flex justify-center">
                        <div className="w-full max-w-[360px] aspect-[2/1]">
                            <ResponsiveContainer width="100%" height="100%">
                                <PieChart margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
                                    <Pie
                                        data={sortedData}
                                        cx="50%"
                                        cy="100%"
                                        startAngle={180}
                                        endAngle={0}
                                        innerRadius="120%"
                                        outerRadius="200%"
                                        paddingAngle={2}
                                        dataKey="value"
                                    >
                                        {sortedData.map((entry, index) => (
                                            <Cell
                                                key={`cell-${index}`}
                                                fill={colorFor(entry.name, index)}
                                                stroke="rgba(0,0,0,0.1)"
                                            />
                                        ))}
                                    </Pie>
                                    <Tooltip content={(props) => <CustomTooltip {...props} total={total} currency={currency} />} />
                                </PieChart>
                            </ResponsiveContainer>
                        </div>
                    </div>
                    <div className="flex flex-wrap justify-center gap-x-8 gap-y-3 px-1">
                        {sortedData.map((entry, index) => {
                            const percentage = ((entry.value / total) * 100).toFixed(1);
                            return (
                                <div key={entry.name} className="flex flex-col gap-1 min-w-[110px]">
                                    <div className="flex items-center gap-2 min-w-0">
                                        <span
                                            className="w-2.5 h-2.5 rounded-full shrink-0"
                                            style={{ backgroundColor: colorFor(entry.name, index) }}
                                        />
                                        <span className="text-sm text-[var(--text-secondary)] truncate">
                                            {formatProviderName(entry.name)}
                                        </span>
                                    </div>
                                    <span className="text-base md:text-lg font-semibold text-[var(--text-primary)] truncate">
                                        {formatValue(entry.value, currency)}
                                    </span>
                                    <span className="text-xs text-[var(--text-tertiary)]">{percentage}%</span>
                                </div>
                            );
                        })}
                    </div>
                </div>
            ) : (
                <div className="flex items-center justify-center h-[300px] w-full">
                    <div className="text-center">
                        <p className="text-[var(--text-tertiary)] text-sm">No data available for the selected time range</p>
                    </div>
                </div>
            )}
        </ChartCard>
    );
}

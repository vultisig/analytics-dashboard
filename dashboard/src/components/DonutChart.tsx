'use client';

import { Pie, PieChart, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';
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

export function DonutChart({ title, subtitle, data, colors, currency = true }: DonutChartProps) {
    // Filter out 0 values to avoid messy charts
    const activeData = data.filter(d => d.value > 0);

    // Calculate total for percentage
    const total = activeData.reduce((sum, item) => sum + item.value, 0);

    // Check if we have data to display
    const hasData = activeData.length > 0 && total > 0;

    // Custom tooltip content
    const CustomTooltip = ({ active, payload }: any) => {
        if (active && payload && payload.length) {
            const data = payload[0];
            const value = data.value;
            const name = formatProviderName(data.name);
            const percentage = ((value / total) * 100).toFixed(1);

            return (
                <div style={glassTooltipStyle} className="p-3">
                    <p className="text-slate-200 font-semibold mb-1">{name}</p>
                    <p className="text-white font-bold">
                        {currency
                            ? `$${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(value)}`
                            : new Intl.NumberFormat('en-US').format(value)}
                    </p>
                    <p className="text-slate-400 text-sm mt-1">{percentage}%</p>
                </div>
            );
        }
        return null;
    };

    return (
        <ChartCard title={title} subtitle={subtitle}>
            {hasData ? (
                <ResponsiveContainer width="100%" height={300}>
                    <PieChart>
                        <Pie
                            data={activeData}
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={100}
                            paddingAngle={2}
                            dataKey="value"
                        >
                            {activeData.map((entry, index) => {
                                const nameLower = entry.name.toLowerCase();
                                // Try provider color first, then chain color, then fallback colors
                                const color = providerColorMap[nameLower] ||
                                             chainColorMap[nameLower] ||
                                             fallbackChainColors[index % fallbackChainColors.length];

                                return (
                                    <Cell key={`cell-${index}`} fill={color} stroke="rgba(0,0,0,0.1)" />
                                );
                            })}
                        </Pie>
                        <Tooltip content={<CustomTooltip />} />
                        <Legend
                            verticalAlign="bottom"
                            height={36}
                            iconSize={8}
                            iconType="circle"
                            wrapperStyle={{ fontSize: '10px' }}
                            formatter={(value) => <span className="text-slate-300 ml-1 text-[10px] md:text-xs">{formatProviderName(value)}</span>}
                        />
                    </PieChart>
                </ResponsiveContainer>
            ) : (
                <div className="flex items-center justify-center h-[300px] w-full">
                    <div className="text-center">
                        <p className="text-slate-400 text-sm">No data available for the selected time range</p>
                    </div>
                </div>
            )}
        </ChartCard>
    );
}

'use client';

import { Bar, BarChart, CartesianGrid, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { ChartCard } from './ChartCard';
import { formatProviderName } from '@/lib/providerUtils';
import { providerColorMap, chainColorMap, fallbackChainColors } from '@/lib/chartStyles';

interface StackedBarChartProps {
    title: string;
    subtitle?: string;
    data: any[];
    keys: string[];
    colors: string[] | Record<string, string>;
    xAxisKey?: string;
    height?: number;
    currency?: boolean;
    action?: React.ReactNode;
}

export function StackedBarChart({
    title,
    subtitle,
    data,
    keys,
    colors,
    xAxisKey = 'date',
    height = 350,
    currency = true,
    action
}: StackedBarChartProps) {
    return (
        <ChartCard title={title} subtitle={subtitle} action={action}>
            <ResponsiveContainer width="100%" height={height}>
                <BarChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.1)" vertical={false} />
                    <XAxis
                        dataKey={xAxisKey}
                        stroke="#94A3B8"
                        fontSize={12}
                        tickLine={false}
                        axisLine={false}
                        dy={10}
                    />
                    <YAxis
                        stroke="#94A3B8"
                        fontSize={12}
                        tickLine={false}
                        axisLine={false}
                        tickFormatter={(value) =>
                            currency
                                ? `$${new Intl.NumberFormat('en-US', { notation: 'compact', compactDisplay: 'short', maximumFractionDigits: 0 }).format(value)}`
                                : new Intl.NumberFormat('en-US', { notation: 'compact', compactDisplay: 'short', maximumFractionDigits: 0 }).format(value)
                        }
                    />
                    <Tooltip
                        content={({ active, payload, label }) => {
                            if (active && payload && payload.length) {
                                const total = payload.reduce((sum: number, entry: any) => sum + (entry.value || 0), 0);
                                return (
                                    <div className="glass-card rounded-xl p-3 shadow-xl">
                                        <p className="text-slate-200 font-medium mb-2 border-b border-slate-700/50 pb-1">{label}</p>
                                        {payload.map((entry: any, index: number) => {
                                            const percentage = total > 0 ? (entry.value / total) * 100 : 0;
                                            return (
                                                <div key={index} className="flex items-center gap-2 mb-1">
                                                    <div className="w-2 h-2 rounded-full" style={{ backgroundColor: entry.color }} />
                                                    <span className="text-slate-300 text-sm">{formatProviderName(entry.name)}:</span>
                                                    <span className="text-[#F8FAFC] font-bold text-sm ml-auto">
                                                        {currency
                                                            ? `$${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(entry.value)}`
                                                            : new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(entry.value)}
                                                    </span>
                                                    <span className="text-slate-400 text-xs">({percentage.toFixed(2)}%)</span>
                                                </div>
                                            );
                                        })}
                                        <div className="mt-2 pt-2 border-t border-slate-700/50 flex justify-between items-center">
                                            <span className="text-slate-300 font-medium">Total</span>
                                            <span className="text-white font-bold">
                                                {currency
                                                    ? `$${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(total)}`
                                                    : new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(total)}
                                            </span>
                                        </div>
                                    </div>
                                );
                            }
                            return null;
                        }}
                        cursor={{ fill: 'rgba(6, 182, 212, 0.05)' }}
                    />
                    <Legend
                        wrapperStyle={{ paddingTop: '10px', fontSize: '10px' }}
                        iconSize={8}
                        iconType="circle"
                        formatter={(value) => <span className="text-[10px] md:text-xs">{formatProviderName(value)}</span>}
                    />
                    {keys.map((key, index) => {
                        const keyLower = key.toLowerCase();
                        // Try provider color first, then chain color, then fallback colors
                        const color = providerColorMap[keyLower] ||
                                     chainColorMap[keyLower] ||
                                     fallbackChainColors[index % fallbackChainColors.length];

                        return (
                            <Bar
                                key={key}
                                dataKey={key}
                                stackId="a"
                                fill={color}
                                radius={index === keys.length - 1 ? [4, 4, 0, 0] : [0, 0, 0, 0]}
                            />
                        );
                    })}
                </BarChart>
            </ResponsiveContainer>
        </ChartCard>
    );
}

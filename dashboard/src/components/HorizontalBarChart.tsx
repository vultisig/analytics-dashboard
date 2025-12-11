'use client';

import { Bar, BarChart, CartesianGrid, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { ChartCard } from './ChartCard';
import { formatCurrency } from '@/lib/queryUtils';
import { providerColorMap, chainColorMap, fallbackChainColors } from '@/lib/chartStyles';

interface HorizontalBarChartProps {
    title: string;
    subtitle?: string;
    data: any[];
    dataKey: string;
    labelKey: string;
    color?: string; // Single color for all bars
    colors?: string[]; // Array of colors (one per bar)
    currency?: boolean;
    height?: number;
}

export function HorizontalBarChart({
    title,
    subtitle,
    data,
    dataKey,
    labelKey,
    color = '#0EA5E9',
    colors, // Optional: if provided, bars will be colored individually
    currency = true,
    height = 400
}: HorizontalBarChartProps) {
    return (
        <ChartCard title={title} subtitle={subtitle}>
            <ResponsiveContainer width="100%" height={height}>
                <BarChart
                    data={data}
                    layout="vertical"
                    margin={{ top: 5, right: 30, left: 40, bottom: 5 }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.1)" horizontal={false} />
                    <XAxis
                        type="number"
                        stroke="#94A3B8"
                        fontSize={12}
                        tickLine={false}
                        axisLine={false}
                        tickFormatter={(value) =>
                            currency
                                ? `$${new Intl.NumberFormat('en-US', { notation: 'compact', compactDisplay: 'short' }).format(value)}`
                                : new Intl.NumberFormat('en-US', { notation: 'compact', compactDisplay: 'short' }).format(value)
                        }
                    />
                    <YAxis
                        dataKey={labelKey}
                        type="category"
                        stroke="#94A3B8"
                        fontSize={11}
                        tickLine={false}
                        axisLine={false}
                        width={100}
                        tickFormatter={(val) => {
                            // Truncate long names
                            if (val.length > 15) return val.substring(0, 15) + '...';
                            return val;
                        }}
                    />
                    <Tooltip
                        content={({ active, payload, label }) => {
                            if (active && payload && payload.length) {
                                return (
                                    <div className="glass-card rounded-xl p-3 shadow-xl">
                                        <p className="text-slate-200 font-medium mb-1">{label}</p>
                                        <p className="text-cyan-400 font-bold">
                                            {currency
                                                ? formatCurrency(payload[0].value as number)
                                                : new Intl.NumberFormat('en-US').format(payload[0].value as number)}
                                        </p>
                                    </div>
                                );
                            }
                            return null;
                        }}
                        cursor={{ fill: 'rgba(6, 182, 212, 0.05)' }}
                    />
                    <Bar dataKey={dataKey} fill={color} radius={[0, 4, 4, 0]} barSize={20}>
                        {colors && data.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={colors[index % colors.length]} />
                        ))}
                        {!colors && data.map((entry, index) => {
                            const labelValue = entry[labelKey];
                            if (!labelValue) return <Cell key={`cell-${index}`} fill={color} />;

                            const labelLower = String(labelValue).toLowerCase();
                            // Try provider color first, then chain color, then use default color
                            const barColor = providerColorMap[labelLower] ||
                                           chainColorMap[labelLower] ||
                                           color;

                            return <Cell key={`cell-${index}`} fill={barColor} />;
                        })}
                    </Bar>
                </BarChart>
            </ResponsiveContainer>
        </ChartCard>
    );
}

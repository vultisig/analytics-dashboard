'use client';

import { Bar, BarChart, CartesianGrid, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

interface OverviewChartProps {
    title: string;
    subtitle: string;
    data: { date: string; value: number }[];
    color: string;
    valuePrefix?: string;
}

export function OverviewChart({ title, subtitle, data, color, valuePrefix = '$' }: OverviewChartProps) {
    return (
        <div className="rounded-xl border border-slate-700/50 bg-slate-900/50 p-6 backdrop-blur-sm">
            <div className="mb-6">
                <h3 className="text-lg font-semibold text-white">{title}</h3>
                <p className="text-sm text-slate-400">{subtitle}</p>
            </div>
            <ResponsiveContainer width="100%" height={250}>
                <BarChart data={data}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.1)" vertical={false} />
                    <XAxis
                        dataKey="date"
                        stroke="#94A3B8"
                        style={{ fontSize: '12px' }}
                        tickLine={false}
                        axisLine={false}
                    />
                    <YAxis
                        stroke="#94A3B8"
                        style={{ fontSize: '12px' }}
                        tickFormatter={(value) => `${valuePrefix}${(value / 1000).toFixed(0)}k`}
                        tickLine={false}
                        axisLine={false}
                    />
                    <Tooltip
                        contentStyle={{
                            backgroundColor: '#1E293B',
                            border: '1px solid #334155',
                            borderRadius: '8px',
                            color: '#F8FAFC',
                            fontSize: '12px'
                        }}
                        cursor={{ fill: 'rgba(148, 163, 184, 0.05)' }}
                        formatter={(value: number) => [`${valuePrefix}${value.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`, title]}
                    />
                    <Bar
                        dataKey="value"
                        fill={color}
                        radius={[4, 4, 0, 0]}
                    />
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
}

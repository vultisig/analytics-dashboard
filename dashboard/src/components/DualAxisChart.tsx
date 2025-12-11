'use client';

import { Line, LineChart, CartesianGrid, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts';

interface DualAxisChartProps {
    title: string;
    subtitle: string;
    data: { date: string; volume: number; revenue: number }[];
}

export function DualAxisChart({ title, subtitle, data }: DualAxisChartProps) {
    return (
        <div className="rounded-xl border border-slate-800 bg-[#0F172A] p-6 shadow-xl">
            <div className="mb-6">
                <h3 className="text-lg font-semibold text-white">{title}</h3>
                <p className="text-sm text-slate-400">{subtitle}</p>
            </div>
            <ResponsiveContainer width="100%" height={350}>
                <LineChart data={data}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.1)" vertical={false} />
                    <XAxis
                        dataKey="date"
                        stroke="#94A3B8"
                        style={{ fontSize: '12px' }}
                        tickLine={false}
                        axisLine={false}
                    />
                    <YAxis
                        yAxisId="left"
                        stroke="#0EA5E9"
                        style={{ fontSize: '12px' }}
                        tickFormatter={(value) => `$${(value / 1000).toFixed(0)}k`}
                        tickLine={false}
                        axisLine={false}
                    />
                    <YAxis
                        yAxisId="right"
                        orientation="right"
                        stroke="#10B981"
                        style={{ fontSize: '12px' }}
                        tickFormatter={(value) => `$${(value / 1000).toFixed(0)}k`}
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
                        cursor={{ stroke: 'rgba(148, 163, 184, 0.2)', strokeWidth: 1 }}
                        formatter={(value: number, name: string) => {
                            const label = name === 'volume' ? 'Volume' : 'Revenue';
                            return [`$${value.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`, label];
                        }}
                    />
                    <Legend
                        wrapperStyle={{ fontSize: '10px', paddingTop: '10px' }}
                        iconSize={8}
                        iconType="line"
                        formatter={(value) => <span className="text-[10px] md:text-xs">{value === 'volume' ? 'Swap Volume' : 'Revenue & Fees'}</span>}
                    />
                    <Line
                        yAxisId="left"
                        type="monotone"
                        dataKey="volume"
                        stroke="#0EA5E9"
                        strokeWidth={2.5}
                        dot={false}
                        activeDot={{ r: 5, fill: '#0EA5E9' }}
                    />
                    <Line
                        yAxisId="right"
                        type="monotone"
                        dataKey="revenue"
                        stroke="#10B981"
                        strokeWidth={2.5}
                        dot={false}
                        activeDot={{ r: 5, fill: '#10B981' }}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}

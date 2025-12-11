'use client';

import { Pie, PieChart, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';

const COLORS = ['#0EA5E9', '#38BDF8', '#7DD3FC', '#BAE6FD', '#E0F2FE'];

export function PlatformDistribution({
    data,
    title = "Platform Distribution",
    subtitle = "Swaps by user platform",
    currency = false
}: {
    data: { name: string; value: number }[];
    title?: string;
    subtitle?: string;
    currency?: boolean;
}) {
    const total = data.reduce((sum, item) => sum + item.value, 0);
    const hasData = data.length > 0 && total > 0;

    return (
        <div className="rounded-xl border border-slate-700/50 bg-slate-900/50 p-6 backdrop-blur-sm">
            <div className="mb-6">
                <h3 className="text-lg font-semibold text-white">{title}</h3>
                {subtitle && <p className="text-sm text-slate-400">{subtitle}</p>}
            </div>
            {hasData ? (
                <ResponsiveContainer width="100%" height={250}>
                    <PieChart>
                        <Pie
                            data={data}
                            cx="50%"
                            cy="50%"
                            labelLine={false}
                            outerRadius={80}
                            fill="#8884d8"
                            dataKey="value"
                        >
                            {data.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                            ))}
                        </Pie>
                        <Tooltip
                            content={({ active, payload }) => {
                                if (active && payload && payload.length) {
                                    const data = payload[0];
                                    const value = Number(data.value);
                                    const percentage = total > 0 ? (value / total) * 100 : 0;
                                    return (
                                        <div className="bg-[#1E293B] border border-[#334155] p-3 rounded-lg shadow-xl">
                                            <p className="text-slate-200 font-medium mb-1">{data.name}</p>
                                            <p className="text-[#F8FAFC] font-bold">
                                                {currency ? '$' : ''}{new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(value)}
                                                <span className="text-slate-400 ml-2 font-normal">({percentage.toFixed(2)}%)</span>
                                            </p>
                                        </div>
                                    );
                                }
                                return null;
                            }}
                        />
                        <Legend
                            wrapperStyle={{ color: '#94A3B8', fontSize: '10px' }}
                            iconSize={8}
                        />
                    </PieChart>
                </ResponsiveContainer>
            ) : (
                <div className="flex items-center justify-center h-[250px]">
                    <div className="text-center">
                        <p className="text-slate-400 text-sm">No data available for the selected time range</p>
                    </div>
                </div>
            )}
        </div>
    );
}

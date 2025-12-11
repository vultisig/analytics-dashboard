'use client';

import { Bar, BarChart, CartesianGrid, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

export function VolumeChart({ data }: { data: { date: string; volume: number }[] }) {
    return (
        <div className="rounded-xl border border-slate-700/50 bg-slate-900/50 p-6 backdrop-blur-sm">
            <div className="mb-6">
                <h3 className="text-lg font-semibold text-white">Daily Volume</h3>
                <p className="text-sm text-slate-400">Swap volume over the last 30 days</p>
            </div>
            <ResponsiveContainer width="100%" height={300}>
                <BarChart data={data}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.1)" />
                    <XAxis
                        dataKey="date"
                        stroke="#94A3B8"
                        style={{ fontSize: '12px' }}
                    />
                    <YAxis
                        stroke="#94A3B8"
                        style={{ fontSize: '12px' }}
                        tickFormatter={(value) => `$${(value / 1000).toFixed(0)}k`}
                    />
                    <Tooltip
                        contentStyle={{
                            backgroundColor: '#1E293B',
                            border: '1px solid #334155',
                            borderRadius: '8px',
                            color: '#F8FAFC'
                        }}
                        formatter={(value: number) => [`$${value.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`, 'Volume']}
                    />
                    <Bar
                        dataKey="volume"
                        fill="url(#colorVolume)"
                        radius={[4, 4, 0, 0]}
                    />
                    <defs>
                        <linearGradient id="colorVolume" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor="#0EA5E9" stopOpacity={1} />
                            <stop offset="100%" stopColor="#0EA5E9" stopOpacity={0.6} />
                        </linearGradient>
                    </defs>
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
}

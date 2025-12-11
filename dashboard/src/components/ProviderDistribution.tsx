'use client';

import { Pie, PieChart, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';

const COLORS = ['#06B6D4', '#0EA5E9', '#3B82F6', '#8B5CF6'];

export function ProviderDistribution({ data }: { data: { name: string; value: number }[] }) {
    return (
        <div className="rounded-xl border border-slate-700/50 bg-slate-900/50 p-6 backdrop-blur-sm">
            <div className="mb-6">
                <h3 className="text-lg font-semibold text-white">Provider Distribution</h3>
                <p className="text-sm text-slate-400">Swaps by provider</p>
            </div>
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
                        contentStyle={{
                            backgroundColor: '#1E293B',
                            border: '1px solid #334155',
                            borderRadius: '8px',
                            color: '#F8FAFC'
                        }}
                        formatter={(value: number) => [new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(value), 'Swaps']}
                    />
                    <Legend
                        wrapperStyle={{ color: '#94A3B8', fontSize: '10px' }}
                        iconSize={8}
                    />
                </PieChart>
            </ResponsiveContainer>
        </div>
    );
}

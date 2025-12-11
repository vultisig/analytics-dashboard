'use client';

import { ArrowRight } from 'lucide-react';

interface PoolData {
    path: string;
    volume: number;
    count: number;
}

export function TopPoolsList({ data }: { data: PoolData[] }) {
    return (
        <div className="rounded-xl border border-slate-700/50 bg-slate-900/50 p-6 backdrop-blur-sm">
            <h3 className="text-lg font-semibold text-white mb-4">Top Swap Paths (24h)</h3>
            <div className="space-y-3">
                {data.map((item, index) => (
                    <div key={index} className="flex items-center justify-between p-3 rounded-lg hover:bg-slate-800/50 transition-colors border border-transparent hover:border-slate-700/50">
                        <div className="flex items-center gap-3">
                            <span className="text-sm font-medium text-slate-500 w-4">#{index + 1}</span>
                            <div className="flex items-center gap-2 text-sm font-medium text-slate-200">
                                <span className="bg-sky-500/10 px-2 py-1 rounded text-sky-400 border border-sky-500/20">
                                    {item.path.split(' → ')[0]}
                                </span>
                                <ArrowRight className="w-3 h-3 text-slate-600" />
                                <span className="bg-emerald-500/10 px-2 py-1 rounded text-emerald-400 border border-emerald-500/20">
                                    {item.path.split(' → ')[1]}
                                </span>
                            </div>
                        </div>
                        <div className="text-right">
                            <p className="text-sm font-bold text-white">${item.volume.toLocaleString(undefined, { maximumFractionDigits: 0 })}</p>
                            <p className="text-xs text-slate-400">{item.count} swaps</p>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}

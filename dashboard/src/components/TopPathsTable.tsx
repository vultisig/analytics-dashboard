import { formatCurrency } from '@/lib/queryUtils';
import { ChartCard } from './ChartCard';
import { ArrowRightLeft } from 'lucide-react';

interface SwapPath {
    pool: string;
    volume: number;
    count: number;
}

interface TopPathsTableProps {
    data: SwapPath[];
}

export function TopPathsTable({ data }: TopPathsTableProps) {
    const hasData = data && data.length > 0;

    return (
        <ChartCard title="Top 10 Swap Paths" subtitle="By USD Volume" icon={ArrowRightLeft}>
            {hasData ? (
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead>
                            <tr className="border-b border-slate-700/50 text-slate-400">
                                <th className="pb-3 font-medium">Rank</th>
                                <th className="pb-3 font-medium">Pool / Path</th>
                                <th className="pb-3 font-medium text-right">Volume</th>
                                <th className="pb-3 font-medium text-right">Swaps</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-700/50">
                            {data.map((item, index) => (
                                <tr key={item.pool} className="group hover:bg-slate-800/30 transition-colors">
                                    <td className="py-3 text-slate-500 w-12">#{index + 1}</td>
                                    <td className="py-3 font-medium text-slate-200">
                                        {item.pool.replace('..', ' → ')}
                                    </td>
                                    <td className="py-3 text-right text-slate-300">
                                        {formatCurrency(item.volume)}
                                    </td>
                                    <td className="py-3 text-right text-slate-400">
                                        {item.count.toLocaleString()}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            ) : (
                <div className="flex items-center justify-center py-12">
                    <div className="text-center">
                        <p className="text-slate-400 text-sm">No swap paths available for the selected time range</p>
                    </div>
                </div>
            )}
        </ChartCard>
    );
}

import { formatCurrency, formatPercentage } from '@/lib/queryUtils';

interface MetricItem {
    label: string;
    value: number;
    color?: string;
}

interface MetricsSummaryProps {
    title: string;
    total: number;
    items: MetricItem[];
    currency?: boolean;
}

export function MetricsSummary({ title, total, items, currency = true }: MetricsSummaryProps) {
    const sortedItems = [...items].sort((a, b) => b.value - a.value);

    return (
        <div className="rounded-lg border border-slate-800 bg-[#111827] p-4">
            <h3 className="text-sm font-medium text-slate-400 uppercase tracking-wider mb-4">{title}</h3>

            <div className="mb-6 flex items-baseline gap-2">
                <p className="text-2xl font-bold text-white font-mono">
                    {currency ? formatCurrency(total) : total.toLocaleString()}
                </p>
                <p className="text-xs text-slate-500">Total</p>
            </div>

            <div className="space-y-3">
                {sortedItems.map((item) => {
                    const percentage = total > 0 ? (item.value / total) * 100 : 0;

                    return (
                        <div key={item.label} className="space-y-1">
                            <div className="flex justify-between text-xs">
                                <span className="text-slate-300 flex items-center gap-2 font-medium">
                                    {item.color && (
                                        <span className="w-2 h-2 rounded-full" style={{ backgroundColor: item.color }} />
                                    )}
                                    {item.label}
                                </span>
                                <span className="text-slate-400 font-mono">
                                    {currency ? formatCurrency(item.value) : item.value.toLocaleString()} ({percentage.toFixed(1)}%)
                                </span>
                            </div>
                            <div className="h-1.5 w-full rounded-full bg-slate-800 overflow-hidden">
                                <div
                                    className="h-full rounded-full transition-all duration-500"
                                    style={{
                                        width: `${percentage}%`,
                                        backgroundColor: item.color || '#0EA5E9'
                                    }}
                                />
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

import { getDateRangeFromParams } from '@/lib/dateUtils';
import { buildDateFilter } from '@/lib/queryUtils';

export const dynamic = 'force-dynamic';

export default async function DebugDatePage({
    searchParams,
}: {
    searchParams: { [key: string]: string | string[] | undefined };
}) {
    // Parse the date range
    const range = getDateRangeFromParams(searchParams);
    const dateFilter = buildDateFilter(range);

    return (
        <div className="min-h-screen bg-[#0B1120] p-8">
            <div className="container mx-auto max-w-4xl">
                <h1 className="text-3xl font-bold text-white mb-8">Date Range Debug</h1>

                <div className="space-y-6">
                    <div className="bg-slate-900 rounded-lg p-6">
                        <h2 className="text-xl font-bold text-cyan-400 mb-4">URL Query Params</h2>
                        <pre className="text-green-400 text-sm">
                            {JSON.stringify(searchParams, null, 2)}
                        </pre>
                    </div>

                    <div className="bg-slate-900 rounded-lg p-6">
                        <h2 className="text-xl font-bold text-cyan-400 mb-4">Parsed Date Range</h2>
                        <pre className="text-green-400 text-sm">
                            {JSON.stringify({
                                type: range.type,
                                startDate: range.startDate?.toISOString(),
                                endDate: range.endDate?.toISOString()
                            }, null, 2)}
                        </pre>
                    </div>

                    <div className="bg-slate-900 rounded-lg p-6">
                        <h2 className="text-xl font-bold text-cyan-400 mb-4">Generated SQL Filter</h2>
                        <div className="space-y-2">
                            <div>
                                <p className="text-slate-400 text-sm mb-1">SQL Condition:</p>
                                <code className="text-yellow-400 bg-slate-800 px-2 py-1 rounded">
                                    {dateFilter.condition}
                                </code>
                            </div>
                            <div>
                                <p className="text-slate-400 text-sm mb-1">SQL Parameters:</p>
                                <pre className="text-green-400 text-sm">
                                    {JSON.stringify(dateFilter.params, null, 2)}
                                </pre>
                            </div>
                        </div>
                    </div>

                    <div className="bg-slate-900 rounded-lg p-6">
                        <h2 className="text-xl font-bold text-cyan-400 mb-4">Full SQL Example</h2>
                        <code className="text-green-400 text-sm block whitespace-pre-wrap">
                            {`SELECT COUNT(*), SUM(total_fee_usd)
FROM swaps
WHERE ${dateFilter.condition};

-- With parameters: ${JSON.stringify(dateFilter.params)}`}
                        </code>
                    </div>

                    <div className="bg-slate-900 rounded-lg p-6">
                        <h2 className="text-xl font-bold text-orange-400 mb-4">Test Different Ranges</h2>
                        <div className="flex gap-2 flex-wrap">
                            <a href="?range=all" className="px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded text-white">All Time</a>
                            <a href="?range=7d" className="px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded text-white">Last 7 Days</a>
                            <a href="?range=30d" className="px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded text-white">Last 30 Days</a>
                            <a href="?range=90d" className="px-4 py-2 bg-slate-800 hover:bg-slate-700 rounded text-white">Last 90 Days</a>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

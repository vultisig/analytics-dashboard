import pool from '@/lib/db';
import { buildDateFilter } from '@/lib/queryUtils';
import { getDateRangeFromParams } from '@/lib/dateUtils';

export const dynamic = 'force-dynamic';

export default async function DebugPage({
    searchParams,
}: {
    searchParams: { [key: string]: string | string[] | undefined };
}) {
    const client = await pool.connect();
    try {
        const dateRange = getDateRangeFromParams(searchParams);
        const dateFilter = buildDateFilter(dateRange, 'date_only');

        // Old query (wrong)
        const oldQuery = `
      SELECT 
        raw_data->>'chainId' as chain_id,
        COUNT(*) as swap_count,
        SUM(total_fee_usd)::float as total_revenue
      FROM swaps
      WHERE source = '1inch' AND ${dateFilter.condition}
      GROUP BY 1
      ORDER BY total_revenue DESC
    `;
        const oldRes = await client.query(oldQuery, dateFilter.params);

        // New query (correct)
        const newQuery = `
      SELECT 
        (raw_data->'chainId')::text as chain_id,
        COUNT(*) as swap_count,
        SUM(total_fee_usd)::float as total_revenue
      FROM swaps
      WHERE source = '1inch' 
        AND raw_data->'chainId' IS NOT NULL
        AND ${dateFilter.condition}
      GROUP BY 1
      ORDER BY total_revenue DESC
    `;
        const newRes = await client.query(newQuery, dateFilter.params);

        // Map to chain names
        const chainNames: { [key: string]: string } = {
            '1': 'Ethereum', '56': 'BSC', '137': 'Polygon', '8453': 'Base',
            '43114': 'Avalanche', '10': 'Optimism', '42161': 'Arbitrum'
        };

        const mappedData = newRes.rows.map(row => ({
            name: chainNames[row.chain_id] || `Chain ${row.chain_id}`,
            value: Number(row.total_revenue),
            chain_id: row.chain_id,
            swap_count: row.swap_count
        }));

        const totalRevenue = mappedData.reduce((sum, item) => sum + item.value, 0);

        return (
            <div className="min-h-screen bg-[#0B1120] p-8">
                <div className="container mx-auto">
                    <h1 className="text-3xl font-bold text-white mb-8">1inch Chain Debug - Date Range: {dateRange.type}</h1>

                    <div className="mb-8">
                        <h2 className="text-2xl font-bold text-white mb-4">OLD Query Results (raw_data-&gt;&gt;'chainId')</h2>
                        <div className="bg-slate-900 rounded-lg p-4 overflow-x-auto">
                            <pre className="text-red-400 text-sm">
                                {JSON.stringify(oldRes.rows, null, 2)}
                            </pre>
                        </div>
                    </div>

                    <div className="mb-8">
                        <h2 className="text-2xl font-bold text-white mb-4">NEW Query Results ((raw_data-&gt;'chainId')::text)</h2>
                        <div className="bg-slate-900 rounded-lg p-4 overflow-x-auto">
                            <pre className="text-green-400 text-sm">
                                {JSON.stringify(newRes.rows, null, 2)}
                            </pre>
                        </div>
                    </div>

                    <div className="mb-8">
                        <h2 className="text-2xl font-bold text-white mb-4">Mapped Data (what goes to pie chart)</h2>
                        <div className="bg-slate-900 rounded-lg p-4 overflow-x-auto">
                            <pre className="text-cyan-400 text-sm">
                                {JSON.stringify(mappedData, null, 2)}
                            </pre>
                        </div>
                    </div>

                    <div className="mb-8">
                        <h2 className="text-2xl font-bold text-white mb-4">Statistics</h2>
                        <div className="bg-slate-900 rounded-lg p-4">
                            <p className="text-white mb-2">Total Revenue: ${totalRevenue.toFixed(2)}</p>
                            <p className="text-white mb-2">Number of Chains: {mappedData.length}</p>
                            <div className="mt-4">
                                <p className="text-white font-bold mb-2">Percentage Breakdown:</p>
                                {mappedData.map(item => (
                                    <p key={item.chain_id} className="text-slate-300">
                                        {item.name}: ${item.value.toFixed(2)} ({((item.value / totalRevenue) * 100).toFixed(2)}%)
                                    </p>
                                ))}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    } finally {
        client.release();
    }
}

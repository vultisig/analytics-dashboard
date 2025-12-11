import pool from '@/lib/db';

export const dynamic = 'force-dynamic';

export default async function DebugFeesPage() {
    const client = await pool.connect();
    try {
        // Check sample 1inch swaps with their fee calculations
        const sampleSwaps = `
      SELECT 
        id,
        source,
        (raw_data->'chainId')::text as chain_id,
        in_amount_usd,
        out_amount_usd,
        total_fee_usd,
        raw_data->'srcAmount' as src_amount_raw,
        raw_data->'dstAmount' as dst_amount_raw,
        created_at,
        raw_data
      FROM swaps
      WHERE source = '1inch'
      ORDER BY total_fee_usd DESC NULLS LAST
      LIMIT 10
    `;
        const samplesRes = await client.query(sampleSwaps);

        // Check total fee stats
        const feeStats = `
      SELECT 
        COUNT(*) as total_swaps,
        SUM(total_fee_usd) as total_fees,
        AVG(total_fee_usd) as avg_fee,
        MIN(total_fee_usd) as min_fee,
        MAX(total_fee_usd) as max_fee,
        SUM(CASE WHEN total_fee_usd > 1000 THEN 1 ELSE 0 END) as swaps_over_1000,
        SUM(CASE WHEN total_fee_usd > 100 THEN total_fee_usd ELSE 0 END) as fees_from_large_swaps
      FROM swaps
      WHERE source = '1inch'
    `;
        const statsRes = await client.query(feeStats);

        // Check fee distribution
        const feeDistribution = `
      SELECT 
        CASE 
          WHEN total_fee_usd < 1 THEN '< $1'
          WHEN total_fee_usd < 10 THEN '$1-10'
          WHEN total_fee_usd < 100 THEN '$10-100'
          WHEN total_fee_usd < 1000 THEN '$100-1000'
          ELSE '> $1000'
        END as fee_range,
        COUNT(*) as swap_count,
        SUM(total_fee_usd)::float as total_fees
      FROM swaps
      WHERE source = '1inch'
      GROUP BY 1
      ORDER BY 
        CASE 
          WHEN total_fee_usd < 1 THEN 1
          WHEN total_fee_usd < 10 THEN 2
          WHEN total_fee_usd < 100 THEN 3
          WHEN total_fee_usd < 1000 THEN 4
          ELSE 5
        END
    `;
        const distRes = await client.query(feeDistribution);

        return (
            <div className="min-h-screen bg-[#0B1120] p-8">
                <div className="container mx-auto">
                    <h1 className="text-3xl font-bold text-white mb-8">1inch Fee Calculation Debug</h1>

                    <div className="mb-8">
                        <h2 className="text-2xl font-bold text-white mb-4">Overall Statistics</h2>
                        <div className="bg-slate-900 rounded-lg p-4">
                            <pre className="text-green-400 text-sm">
                                {JSON.stringify(statsRes.rows[0], null, 2)}
                            </pre>
                        </div>
                    </div>

                    <div className="mb-8">
                        <h2 className="text-2xl font-bold text-white mb-4">Fee Distribution</h2>
                        <div className="bg-slate-900 rounded-lg p-4 overflow-x-auto">
                            <pre className="text-cyan-400 text-sm">
                                {JSON.stringify(distRes.rows, null, 2)}
                            </pre>
                        </div>
                    </div>

                    <div className="mb-8">
                        <h2 className="text-2xl font-bold text-white mb-4">Top 10 Swaps by Fee (Highest Fees)</h2>
                        <div className="bg-slate-900 rounded-lg p-4 overflow-x-auto max-h-[600px]">
                            <pre className="text-yellow-400 text-sm">
                                {JSON.stringify(samplesRes.rows, null, 2)}
                            </pre>
                        </div>
                    </div>
                </div>
            </div>
        );
    } finally {
        client.release();
    }
}

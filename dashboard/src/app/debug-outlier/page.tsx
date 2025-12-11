import pool from '@/lib/db';

export const dynamic = 'force-dynamic';

export default async function DebugOutlierPage() {
    const client = await pool.connect();
    try {
        // Find the outlier swap(s) with very high fees
        const outlierQuery = `
      SELECT 
        id,
        timestamp,
        source,
        tx_hash,
        user_address,
        in_amount,
        in_amount_usd,
        out_amount_usd,
        total_fee_usd,
        raw_data->'chainId' as chain_id,
        raw_data->'details'->'blockTimeSec' as block_time,
        raw_data->'details'->'tokenActions' as token_actions
      FROM swaps
      WHERE source = '1inch' 
        AND total_fee_usd > 1000
      ORDER BY total_fee_usd DESC
      LIMIT 3
    `;
        const outlierRes = await client.query(outlierQuery);

        return (
            <div className="min-h-screen bg-[#0B1120] p-8">
                <div className="container mx-auto max-w-6xl">
                    <h1 className="text-3xl font-bold text-white mb-8">$32k Outlier Swap Investigation</h1>

                    <div className="mb-8 bg-slate-900 p-4 rounded-lg">
                        <p className="text-slate-400 mb-2">
                            Found <span className="text-white font-bold">{outlierRes.rows.length}</span> swap(s) with fees &gt; $1000
                        </p>
                        <p className="text-slate-400">
                            Investigating to determine if this is a price error, decimal error, or legitimate large swap
                        </p>
                    </div>

                    {outlierRes.rows.map((swap, idx) => {
                        const volumeAt02Pct = swap.total_fee_usd / 0.002;
                        const volumeAt05Pct = swap.total_fee_usd / 0.005;
                        const impliedFee = swap.in_amount_usd * 0.005;
                        const feeMultiplier = swap.total_fee_usd / impliedFee;

                        return (
                            <div key={idx} className="mb-8 bg-slate-900 rounded-lg p-6 border border-slate-800">
                                <h3 className="text-2xl font-bold text-red-400 mb-4">
                                    Outlier #{idx + 1}: ${swap.total_fee_usd?.toFixed(2)} fee
                                </h3>

                                <div className="grid md:grid-cols-2 gap-6 mb-6">
                                    <div className="space-y-3">
                                        <h4 className="text-lg font-bold text-cyan-400 mb-2">Transaction Info</h4>
                                        <div className="bg-slate-800 p-3 rounded space-y-2">
                                            <p className="text-white text-sm"><span className="text-slate-400">Chain ID:</span> {String(swap.chain_id)}</p>
                                            <p className="text-white text-sm"><span className="text-slate-400">Timestamp:</span> {new Date(swap.timestamp).toLocaleString()}</p>
                                            <p className="text-white font-mono text-xs break-all"><span className="text-slate-400">TX:</span> {swap.tx_hash}</p>
                                            <p className="text-white font-mono text-xs break-all"><span className="text-slate-400">User:</span> {swap.user_address}</p>
                                        </div>
                                    </div>

                                    <div className="space-y-3">
                                        <h4 className="text-lg font-bold text-yellow-400 mb-2">Current DB Values</h4>
                                        <div className="bg-slate-800 p-3 rounded space-y-2">
                                            <p className="text-white"><span className="text-slate-400">In Amount:</span> {swap.in_amount?.toFixed(6)}</p>
                                            <p className="text-white"><span className="text-slate-400">Volume USD:</span> ${swap.in_amount_usd?.toFixed(2)}</p>
                                            <p className="text-red-400 font-bold text-lg"><span className="text-slate-400">Fee USD:</span> ${swap.total_fee_usd?.toFixed(2)}</p>
                                        </div>
                                    </div>
                                </div>

                                <div className="mb-6">
                                    <h4 className="text-lg font-bold text-purple-400 mb-2">Analysis</h4>
                                    <div className="bg-slate-800 p-4 rounded space-y-3">
                                        <div className="grid md:grid-cols-3 gap-4">
                                            <div>
                                                <p className="text-slate-400 text-sm mb-1">Volume if 0.2% fee:</p>
                                                <p className="text-yellow-400 font-bold">${volumeAt02Pct.toFixed(2)}</p>
                                            </div>
                                            <div>
                                                <p className="text-slate-400 text-sm mb-1">Volume if 0.5% fee:</p>
                                                <p className="text-green-400 font-bold">${volumeAt05Pct.toFixed(2)}</p>
                                            </div>
                                            <div>
                                                <p className="text-slate-400 text-sm mb-1">Expected fee (0.5%):</p>
                                                <p className="text-blue-400 font-bold">${impliedFee.toFixed(2)}</p>
                                            </div>
                                        </div>
                                        <div className="pt-3 border-t border-slate-700">
                                            <p className="text-white">
                                                <span className="text-red-400 font-bold">Issue:</span> Actual fee is <span className="text-red-400 font-bold text-xl">{feeMultiplier.toFixed(1)}x</span> higher than expected
                                            </p>
                                            {feeMultiplier > 100 && (
                                                <p className="text-orange-400 mt-2">⚠️ This suggests a price lookup or decimal error, not a legitimate swap</p>
                                            )}
                                        </div>
                                    </div>
                                </div>

                                <div>
                                    <h4 className="text-lg font-bold text-orange-400 mb-2">Token Actions</h4>
                                    <div className="bg-slate-800 p-4 rounded overflow-x-auto">
                                        <pre className="text-green-400 text-xs">
                                            {JSON.stringify(swap.token_actions, null, 2)}
                                        </pre>
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>
        );
    } finally {
        client.release();
    }
}

import pool from './src/lib/db';

async function debug1inchChains() {
    const client = await pool.connect();
    try {
        console.log('=== Debugging 1inch Chain Revenue ===\n');

        // Check what chainId values exist
        const chainIdQuery = `
            SELECT 
                raw_data->>'chainId' as chain_id,
                COUNT(*) as swap_count,
                SUM(total_fee_usd) as total_revenue
            FROM swaps
            WHERE source = '1inch'
            GROUP BY 1
            ORDER BY total_revenue DESC
        `;
        const chainIdRes = await client.query(chainIdQuery);
        console.log('Chain IDs found:');
        console.log(chainIdRes.rows);

        // Check sample raw_data structure
        const sampleQuery = `
            SELECT raw_data
            FROM swaps
            WHERE source = '1inch'
            LIMIT 3
        `;
        const sampleRes = await client.query(sampleQuery);
        console.log('\nSample raw_data structures:');
        sampleRes.rows.forEach((row, i) => {
            console.log(`\nSample ${i + 1}:`);
            console.log('chainId:', row.raw_data?.chainId);
            console.log('Keys in raw_data:', Object.keys(row.raw_data || {}));
        });

    } finally {
        client.release();
        await pool.end();
    }
}

debug1inchChains().catch(console.error);

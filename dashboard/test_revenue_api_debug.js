const { Pool } = require('pg');
const pool = new Pool({
    connectionString: 'postgresql://vultisig_user:vultisig_secure_password_123@localhost:5432/vultisig_analytics'
});

// Simulate what the API does for 30d range
const testQuery = async () => {
    const client = await pool.connect();

    try {
        // This simulates buildDateFilter for 30d
        const endDate = new Date();
        const startDate = new Date();
        startDate.setDate(startDate.getDate() - 30);

        const startStr = startDate.toISOString().split('T')[0];
        const endStr = endDate.toISOString().split('T')[0];

        console.log('Date range:', startStr, 'to', endStr);

        const dateCondition = `date_only >= $1 AND date_only <= $2`;
        const arkhamDateCondition = `timestamp >= $1 AND timestamp <= $2`;

        const swapsQuery = `
      SELECT COALESCE(SUM(total_fee_usd), 0) as total_revenue
      FROM swaps
      WHERE source != '1inch' AND ${dateCondition}
    `;

        const arkhamQuery = `
      SELECT COALESCE(SUM(actual_fee_usd), 0) as total_revenue
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch' AND ${arkhamDateCondition}
    `;

        console.log('\nSwaps query:', swapsQuery);
        console.log('Params:', [startStr, endStr]);

        const swapsRes = await client.query(swapsQuery, [startStr, endStr]);
        console.log('Swaps result:', swapsRes.rows[0]);

        console.log('\nArkham query:', arkhamQuery);
        const arkhamRes = await client.query(arkhamQuery, [startStr, endStr]);
        console.log('Arkham result:', arkhamRes.rows[0]);

    } catch (error) {
        console.error('Error:', error.message);
        console.error('Stack:', error.stack);
    } finally {
        client.release();
        pool.end();
    }
};

testQuery();

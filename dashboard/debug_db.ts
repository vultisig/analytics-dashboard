import { Pool } from 'pg';

const pool = new Pool({
    connectionString: 'postgresql://vultisig_user:vultisig_secure_password_123@localhost:5432/vultisig_analytics',
});

async function debug() {
    const client = await pool.connect();
    try {
        console.log('--- 1inch Chain IDs ---');
        const chainStats = await client.query(`
      SELECT raw_data->>'chainId' as chain_id, COUNT(*) 
      FROM swaps 
      WHERE source = '1inch' 
      GROUP BY 1 
      ORDER BY 2 DESC
    `);
        console.table(chainStats.rows);
        console.log('--- 1inch Platform Stats ---');
        const platformStats = await client.query(`
      SELECT platform, COUNT(*) 
      FROM swaps 
      WHERE source = '1inch' 
      GROUP BY 1 
      ORDER BY 2 DESC
    `);
        console.table(platformStats.rows);

        console.log('--- 1inch Details ---');
        const res = await client.query('SELECT raw_data FROM swaps WHERE source = $1 LIMIT 3', ['1inch']);
        res.rows.forEach((row, i) => {
            console.log(`\n--- Row ${i + 1} Details ---`);
            console.log(JSON.stringify(row.raw_data.details, null, 2));
        });
    }
    catch (e) {
        console.error(e);
    } finally {
        client.release();
        pool.end();
    }
}

debug();

const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.DATABASE_URL
});

async function testDateRanges() {
    const client = await pool.connect();
    try {
        console.log('=== Testing Date Range Filtering ===\n');

        // Test 1: All Time
        console.log('1. ALL TIME (no date filter):');
        const allTimeQuery = `
      SELECT 
        COUNT(*) as swap_count,
        SUM(in_amount_usd) as total_volume,
        MIN(date_only) as earliest_date,
        MAX(date_only) as latest_date
      FROM swaps
    `;
        const allTimeResult = await client.query(allTimeQuery);
        console.log(JSON.stringify(allTimeResult.rows[0], null, 2));
        console.log();

        // Test 2: Last 7 Days
        const today = new Date();
        const sevenDaysAgo = new Date();
        sevenDaysAgo.setDate(today.getDate() - 7);

        const formatDate = (d) => d.toISOString().split('T')[0];

        console.log('2. LAST 7 DAYS:');
        console.log(`   Start: ${formatDate(sevenDaysAgo)}`);
        console.log(`   End: ${formatDate(today)}`);

        const last7DaysQuery = `
      SELECT 
        COUNT(*) as swap_count,
        SUM(in_amount_usd) as total_volume,
        MIN(date_only) as earliest_date,
        MAX(date_only) as latest_date
      FROM swaps
      WHERE date_only >= $1 AND date_only <= $2
    `;
        const last7DaysResult = await client.query(last7DaysQuery, [formatDate(sevenDaysAgo), formatDate(today)]);
        console.log(JSON.stringify(last7DaysResult.rows[0], null, 2));
        console.log();

        // Test 3: Last 30 Days
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(today.getDate() - 30);

        console.log('3. LAST 30 DAYS:');
        console.log(`   Start: ${formatDate(thirtyDaysAgo)}`);
        console.log(`   End: ${formatDate(today)}`);

        const last30DaysQuery = `
      SELECT 
        COUNT(*) as swap_count,
        SUM(in_amount_usd) as total_volume,
        MIN(date_only) as earliest_date,
        MAX(date_only) as latest_date
      FROM swaps
      WHERE date_only >= $1 AND date_only <= $2
    `;
        const last30DaysResult = await client.query(last30DaysQuery, [formatDate(thirtyDaysAgo), formatDate(today)]);
        console.log(JSON.stringify(last30DaysResult.rows[0], null, 2));
        console.log();

        // Test 4: Recent swaps by date
        console.log('4. RECENT SWAPS (grouped by date):');
        const recentSwapsQuery = `
      SELECT 
        date_only,
        COUNT(*) as swap_count
      FROM swaps
      WHERE date_only >= $1
      GROUP BY date_only
      ORDER BY date_only DESC
      LIMIT 10
    `;
        const recentSwapsResult = await client.query(recentSwapsQuery, [formatDate(thirtyDaysAgo)]);
        console.log('Recent swap activity:');
        recentSwapsResult.rows.forEach(row => {
            console.log(`  ${row.date_only}: ${row.swap_count} swaps`);
        });
        console.log('\n=== Summary ===');
        if (allTimeResult.rows[0].swap_count === last7DaysResult.rows[0].swap_count) {
            console.log('⚠️  WARNING: All Time and Last 7 Days have SAME count!');
            console.log('   This suggests either:');
            console.log('   1. All swaps happened in the last 7 days');
            console.log('   2. Date filtering is NOT working');
            console.log('   3. There are no swaps in the last 7 days');
        } else {
            console.log('✓ Date filtering appears to be working correctly');
            console.log(`  All Time: ${allTimeResult.rows[0].swap_count} swaps`);
            console.log(`  Last 7 Days: ${last7DaysResult.rows[0].swap_count} swaps`);
        }

    } catch (error) {
        console.error('Error:', error);
    } finally {
        client.release();
        await pool.end();
    }
}

testDateRanges();

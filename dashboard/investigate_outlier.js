const { Pool } = require('pg');

const pool = new Pool({
    connectionString: process.env.DATABASE_URL
});

async function investigateOutlier() {
    const client = await pool.connect();
    try {
        console.log('=== Investigating $32k Outlier Swap ===\n');

        const query = `
      SELECT 
        id,
        timestamp,
        tx_hash,
        user_address,
        in_asset,
        in_amount,
        in_amount_usd,
        total_fee_usd,
        raw_data
      FROM swaps
      WHERE source = '1inch' 
        AND total_fee_usd > 1000
      ORDER BY total_fee_usd DESC
      LIMIT 1
    `;

        const result = await client.query(query);

        if (result.rows.length === 0) {
            console.log('No outlier swaps found!');
            return;
        }

        const swap = result.rows[0];
        const rawData = swap.raw_data;

        console.log('SWAP DETAILS:');
        console.log('-------------');
        console.log(`ID: ${swap.id}`);
        console.log(`Timestamp: ${swap.timestamp}`);
        console.log(`TX Hash: ${swap.tx_hash}`);
        console.log(`User: ${swap.user_address}`);
        console.log();

        console.log('CURRENT VALUES IN DB:');
        console.log('---------------------');
        console.log(`In Amount (tokens): ${swap.in_amount}`);
        console.log(`Volume USD: $${swap.in_amount_usd?.toFixed(2)}`);
        console.log(`Fee USD: $${swap.total_fee_usd?.toFixed(2)}`);
        console.log();

        console.log('ANALYSIS:');
        console.log('---------');
        const volumeAt02 = swap.total_fee_usd / 0.002;
        const volumeAt05 = swap.total_fee_usd / 0.005;
        const impliedFee = swap.in_amount_usd * 0.005;
        const multiplier = swap.total_fee_usd / impliedFee;

        console.log(`If fee rate was 0.2%: Volume = $${volumeAt02.toFixed(2)}`);
        console.log(`If fee rate is 0.5%: Volume = $${volumeAt05.toFixed(2)}`);
        console.log(`If volume ($${swap.in_amount_usd?.toFixed(2)}) is correct: Expected fee = $${impliedFee.toFixed(2)}`);
        console.log(`Actual fee / Expected fee = ${multiplier.toFixed(1)}x`);
        console.log();

        console.log('RAW DATA:');
        console.log('---------');
        console.log(`Chain ID: ${rawData?.chainId}`);
        console.log(`Block Time: ${rawData?.details?.blockTimeSec}`);
        console.log();

        console.log('TOKEN ACTIONS:');
        console.log('--------------');
        const tokenActions = rawData?.details?.tokenActions || [];
        tokenActions.forEach((action, i) => {
            console.log(`\nAction ${i + 1}:`);
            console.log(`  Token: ${action.address}`);
            console.log(`  Amount (raw): ${action.amount}`);
            console.log(`  Direction: ${action.direction}`);
            console.log(`  From: ${action.fromAddress}`);
            console.log(`  To: ${action.toAddress}`);
        });

        // Find fee action
        const feeAction = tokenActions.find(a =>
            a.toAddress?.toLowerCase() === '0xa4a4f610e89488eb4ecc6c63069f241a54485269'
        );

        if (feeAction) {
            console.log('\n\nFEE ACTION (to integrator):');
            console.log('---------------------------');
            console.log(`Token: ${feeAction.address}`);
            console.log(`Amount (raw): ${feeAction.amount}`);
            console.log('\nThis raw amount needs to be:');
            console.log('  1. Divided by 10^decimals to get token amount');
            console.log('  2. Multiplied by token USD price to get fee in USD');
            console.log('\nPotential issues:');
            console.log('  - Wrong decimals used');
            console.log('  - Wrong token price fetched');
            console.log('  - Token price in wrong currency (not USD)');
        }

    } catch (error) {
        console.error('Error:', error);
    } finally {
        client.release();
        await pool.end();
    }
}

investigateOutlier();

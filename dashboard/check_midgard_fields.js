require('dotenv/config');
const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function checkMidgardFields() {
  try {
    console.log('=== CHECKING THORCHAIN/MAYACHAIN NEW FIELDS ===\n');

    // Check a recent THORChain swap
    const thorResult = await pool.query(`
      SELECT
        timestamp,
        tx_hash,
        source,
        in_address,
        in_tx_id,
        in_amount_raw,
        affiliate_addresses,
        affiliate_fees_bps,
        in_price_usd,
        out_price_usd,
        swap_status,
        swap_type,
        memo,
        pools_used,
        is_streaming_swap
      FROM swaps
      WHERE source = 'thorchain'
      ORDER BY timestamp DESC
      LIMIT 2
    `);

    console.log('--- THORChain Sample (2 most recent) ---');
    thorResult.rows.forEach((row, i) => {
      console.log(`\nSwap ${i + 1}:`);
      console.log(`  Timestamp: ${row.timestamp}`);
      console.log(`  TX Hash: ${row.tx_hash}`);
      console.log(`  In Address: ${row.in_address || 'NULL'}`);
      console.log(`  In TX ID: ${row.in_tx_id || 'NULL'}`);
      console.log(`  In Amount Raw: ${row.in_amount_raw || 'NULL'}`);
      console.log(`  Affiliate Addresses: ${row.affiliate_addresses || 'NULL'}`);
      console.log(`  Affiliate Fees BPS: ${row.affiliate_fees_bps || 'NULL'}`);
      console.log(`  In Price USD: ${row.in_price_usd || 'NULL'}`);
      console.log(`  Out Price USD: ${row.out_price_usd || 'NULL'}`);
      console.log(`  Swap Status: ${row.swap_status || 'NULL'}`);
      console.log(`  Swap Type: ${row.swap_type || 'NULL'}`);
      console.log(`  Memo: ${row.memo || 'NULL'}`);
      console.log(`  Pools Used: ${row.pools_used || 'NULL'}`);
      console.log(`  Is Streaming: ${row.is_streaming_swap}`);
    });

    // Check a recent MayaChain swap
    const mayaResult = await pool.query(`
      SELECT
        timestamp,
        tx_hash,
        in_address,
        affiliate_addresses,
        swap_status
      FROM swaps
      WHERE source = 'mayachain'
      ORDER BY timestamp DESC
      LIMIT 1
    `);

    console.log('\n\n--- MayaChain Sample (1 most recent) ---');
    if (mayaResult.rows.length > 0) {
      const row = mayaResult.rows[0];
      console.log(`Timestamp: ${row.timestamp}`);
      console.log(`TX Hash: ${row.tx_hash}`);
      console.log(`In Address: ${row.in_address || 'NULL'}`);
      console.log(`Affiliate Addresses: ${row.affiliate_addresses || 'NULL'}`);
      console.log(`Swap Status: ${row.swap_status || 'NULL'}`);
    }

    // Count how many records have new fields populated
    const statsResult = await pool.query(`
      SELECT
        source,
        COUNT(*) as total,
        COUNT(in_address) as has_in_address,
        COUNT(affiliate_addresses) as has_affiliate_addresses,
        COUNT(in_price_usd) as has_in_price_usd,
        COUNT(swap_status) as has_swap_status
      FROM swaps
      WHERE source IN ('thorchain', 'mayachain')
      GROUP BY source
    `);

    console.log('\n\n=== FIELD POPULATION STATS ===\n');
    statsResult.rows.forEach(row => {
      console.log(`${row.source.toUpperCase()}:`);
      console.log(`  Total: ${row.total}`);
      console.log(`  Has in_address: ${row.has_in_address} (${(row.has_in_address/row.total*100).toFixed(1)}%)`);
      console.log(`  Has affiliate_addresses: ${row.has_affiliate_addresses} (${(row.has_affiliate_addresses/row.total*100).toFixed(1)}%)`);
      console.log(`  Has in_price_usd: ${row.has_in_price_usd} (${(row.has_in_price_usd/row.total*100).toFixed(1)}%)`);
      console.log(`  Has swap_status: ${row.has_swap_status} (${(row.has_swap_status/row.total*100).toFixed(1)}%)`);
      console.log();
    });

  } catch (err) {
    console.error('Error:', err.message);
  } finally {
    await pool.end();
  }
}

checkMidgardFields();

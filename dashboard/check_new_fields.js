// Check if new fields are populated in recent swaps
const { Pool } = require('pg');
require('dotenv').config({ path: '.env.local' });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false
});

async function checkNewFields() {
  const client = await pool.connect();

  try {
    console.log('=== CHECKING NEW MIDGARD FIELDS ===\n');

    // Check THORChain swaps with new fields
    const thorQuery = `
      SELECT
        timestamp,
        tx_hash,
        in_asset,
        platform,
        in_address,
        in_tx_id,
        in_amount_raw,
        affiliate_addresses,
        affiliate_fees_bps,
        in_price_usd,
        out_price_usd,
        pools_used,
        swap_status,
        swap_type
      FROM swaps
      WHERE source = 'thorchain'
        AND timestamp >= NOW() - INTERVAL '7 days'
        AND in_address IS NOT NULL
      ORDER BY timestamp DESC
      LIMIT 3
    `;

    const thorResult = await client.query(thorQuery);
    console.log(`Found ${thorResult.rowCount} recent THORChain swaps with complete fields:\n`);

    thorResult.rows.forEach((row, idx) => {
      console.log(`--- THORChain Swap ${idx + 1} ---`);
      console.log(`Timestamp: ${row.timestamp}`);
      console.log(`TX Hash: ${row.tx_hash}`);
      console.log(`Asset: ${row.in_asset}`);
      console.log(`Platform: ${row.platform}`);
      console.log(`In Address: ${row.in_address}`);
      console.log(`In TX ID: ${row.in_tx_id}`);
      console.log(`In Amount Raw: ${row.in_amount_raw}`);
      console.log(`Affiliate Addresses: ${JSON.stringify(row.affiliate_addresses)}`);
      console.log(`Affiliate Fees BPS: ${JSON.stringify(row.affiliate_fees_bps)}`);
      console.log(`In Price USD: ${row.in_price_usd}`);
      console.log(`Out Price USD: ${row.out_price_usd}`);
      console.log(`Pools Used: ${JSON.stringify(row.pools_used)}`);
      console.log(`Swap Status: ${row.swap_status}`);
      console.log(`Swap Type: ${row.swap_type}`);
      console.log('');
    });

    // Check MayaChain swaps with new fields
    const mayaQuery = `
      SELECT
        timestamp,
        tx_hash,
        in_asset,
        platform,
        in_address,
        in_tx_id,
        in_amount_raw,
        affiliate_addresses,
        affiliate_fees_bps,
        in_price_usd,
        out_price_usd,
        pools_used,
        swap_status,
        swap_type
      FROM swaps
      WHERE source = 'mayachain'
        AND timestamp >= NOW() - INTERVAL '7 days'
        AND in_address IS NOT NULL
      ORDER BY timestamp DESC
      LIMIT 3
    `;

    const mayaResult = await client.query(mayaQuery);
    console.log(`\nFound ${mayaResult.rowCount} recent MayaChain swaps with complete fields:\n`);

    mayaResult.rows.forEach((row, idx) => {
      console.log(`--- MayaChain Swap ${idx + 1} ---`);
      console.log(`Timestamp: ${row.timestamp}`);
      console.log(`TX Hash: ${row.tx_hash}`);
      console.log(`Asset: ${row.in_asset}`);
      console.log(`Platform: ${row.platform}`);
      console.log(`In Address: ${row.in_address}`);
      console.log(`In TX ID: ${row.in_tx_id}`);
      console.log(`In Amount Raw: ${row.in_amount_raw}`);
      console.log(`Affiliate Addresses: ${JSON.stringify(row.affiliate_addresses)}`);
      console.log(`Affiliate Fees BPS: ${JSON.stringify(row.affiliate_fees_bps)}`);
      console.log(`In Price USD: ${row.in_price_usd}`);
      console.log(`Out Price USD: ${row.out_price_usd}`);
      console.log(`Pools Used: ${JSON.stringify(row.pools_used)}`);
      console.log(`Swap Status: ${row.swap_status}`);
      console.log(`Swap Type: ${row.swap_type}`);
      console.log('');
    });

    // Check LiFi swaps with new fields (most recent batch)
    const lifiQuery = `
      SELECT
        timestamp,
        tx_hash,
        in_asset,
        platform,
        in_address,
        in_tx_id,
        in_amount_raw,
        in_price_usd,
        out_price_usd,
        pools_used,
        swap_status,
        swap_type
      FROM swaps
      WHERE source = 'lifi'
        AND platform IS NOT NULL
      ORDER BY timestamp DESC
      LIMIT 3
    `;

    const lifiResult = await client.query(lifiQuery);
    console.log(`\nFound ${lifiResult.rowCount} recent LiFi swaps with complete fields:\n`);

    lifiResult.rows.forEach((row, idx) => {
      console.log(`--- LiFi Swap ${idx + 1} ---`);
      console.log(`Timestamp: ${row.timestamp}`);
      console.log(`TX Hash: ${row.tx_hash}`);
      console.log(`Asset: ${row.in_asset}`);
      console.log(`Platform: ${row.platform}`);
      console.log(`In Address: ${row.in_address}`);
      console.log(`In TX ID: ${row.in_tx_id}`);
      console.log(`In Amount Raw: ${row.in_amount_raw}`);
      console.log(`In Price USD: ${row.in_price_usd}`);
      console.log(`Out Price USD: ${row.out_price_usd}`);
      console.log(`Pools Used: ${JSON.stringify(row.pools_used)}`);
      console.log(`Swap Status: ${row.swap_status}`);
      console.log(`Swap Type: ${row.swap_type}`);
      console.log('');
    });

    // Summary of field coverage
    console.log('\n=== FIELD COVERAGE SUMMARY ===\n');

    const coverageQuery = `
      SELECT
        source,
        COUNT(*) as total,
        COUNT(in_address) as has_in_address,
        COUNT(in_tx_id) as has_in_tx_id,
        COUNT(in_amount_raw) as has_in_amount_raw,
        COUNT(in_price_usd) as has_in_price_usd,
        COUNT(out_price_usd) as has_out_price_usd,
        COUNT(platform) as has_platform
      FROM swaps
      GROUP BY source
      ORDER BY total DESC
    `;

    const coverageResult = await client.query(coverageQuery);
    console.log('Field coverage by source:');
    coverageResult.rows.forEach(row => {
      console.log(`\n${row.source}:`);
      console.log(`  Total swaps: ${row.total}`);
      console.log(`  Has in_address: ${row.has_in_address} (${(row.has_in_address / row.total * 100).toFixed(1)}%)`);
      console.log(`  Has in_tx_id: ${row.has_in_tx_id} (${(row.has_in_tx_id / row.total * 100).toFixed(1)}%)`);
      console.log(`  Has in_amount_raw: ${row.has_in_amount_raw} (${(row.has_in_amount_raw / row.total * 100).toFixed(1)}%)`);
      console.log(`  Has in_price_usd: ${row.has_in_price_usd} (${(row.has_in_price_usd / row.total * 100).toFixed(1)}%)`);
      console.log(`  Has out_price_usd: ${row.has_out_price_usd} (${(row.has_out_price_usd / row.total * 100).toFixed(1)}%)`);
      console.log(`  Has platform: ${row.has_platform} (${(row.has_platform / row.total * 100).toFixed(1)}%)`);
    });

  } catch (error) {
    console.error('Error querying data:', error);
  } finally {
    client.release();
    await pool.end();
  }
}

checkNewFields();

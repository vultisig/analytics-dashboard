// Quick script to check LiFi and Arkham data samples
const { Pool } = require('pg');
require('dotenv').config({ path: '.env.local' });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false
});

async function checkData() {
  const client = await pool.connect();

  try {
    console.log('=== LIFI DATA SAMPLE ===\n');

    // Check LiFi swaps
    const lifiQuery = `
      SELECT
        timestamp,
        tx_hash,
        in_asset,
        out_asset,
        in_amount_usd,
        affiliate_fee_usd,
        platform,
        in_address,
        in_tx_id,
        in_amount_raw,
        out_addresses,
        out_tx_ids,
        affiliate_addresses,
        affiliate_fees_bps,
        in_price_usd,
        out_price_usd,
        pools_used,
        swap_status,
        swap_type
      FROM swaps
      WHERE source = 'lifi'
      ORDER BY timestamp DESC
      LIMIT 3
    `;

    const lifiResult = await client.query(lifiQuery);
    console.log(`Found ${lifiResult.rowCount} LiFi swaps (showing 3 most recent):\n`);

    lifiResult.rows.forEach((row, idx) => {
      console.log(`--- LiFi Swap ${idx + 1} ---`);
      console.log(`Timestamp: ${row.timestamp}`);
      console.log(`TX Hash: ${row.tx_hash}`);
      console.log(`Path: ${row.in_asset} → ${row.out_asset}`);
      console.log(`Volume: $${row.in_amount_usd}`);
      console.log(`Affiliate Fee: $${row.affiliate_fee_usd}`);
      console.log(`Platform: ${row.platform}`);
      console.log(`In Address: ${row.in_address}`);
      console.log(`In TX ID: ${row.in_tx_id}`);
      console.log(`In Amount Raw: ${row.in_amount_raw}`);
      console.log(`Out Addresses: ${JSON.stringify(row.out_addresses, null, 2)}`);
      console.log(`Out TX IDs: ${row.out_tx_ids}`);
      console.log(`Affiliate Addresses: ${row.affiliate_addresses}`);
      console.log(`Affiliate Fees BPS: ${row.affiliate_fees_bps}`);
      console.log(`In Price USD: ${row.in_price_usd}`);
      console.log(`Out Price USD: ${row.out_price_usd}`);
      console.log(`Pools Used: ${row.pools_used}`);
      console.log(`Swap Status: ${row.swap_status}`);
      console.log(`Swap Type: ${row.swap_type}`);
      console.log('');
    });

    console.log('\n=== ARKHAM (1INCH) DATA SAMPLE ===\n');

    // Check Arkham/1inch data
    const arkhamQuery = `
      SELECT
        timestamp,
        tx_hash,
        chain,
        protocol,
        actual_fee_usd,
        swap_volume_usd,
        token_in_symbol,
        token_in_address,
        token_out_symbol,
        token_out_address,
        amount_in,
        amount_out,
        from_address,
        to_address,
        fee_token_symbol,
        fee_amount_raw,
        fee_data_source,
        volume_data_source
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
      ORDER BY timestamp DESC
      LIMIT 3
    `;

    const arkhamResult = await client.query(arkhamQuery);
    console.log(`Found ${arkhamResult.rowCount} Arkham/1inch records (showing 3 most recent):\n`);

    arkhamResult.rows.forEach((row, idx) => {
      console.log(`--- Arkham/1inch Record ${idx + 1} ---`);
      console.log(`Timestamp: ${row.timestamp}`);
      console.log(`TX Hash: ${row.tx_hash}`);
      console.log(`Chain: ${row.chain}`);
      console.log(`Protocol: ${row.protocol}`);
      console.log(`Actual Fee USD: $${row.actual_fee_usd}`);
      console.log(`Swap Volume USD: $${row.swap_volume_usd}`);
      console.log(`Token In: ${row.token_in_symbol} (${row.token_in_address})`);
      console.log(`Token Out: ${row.token_out_symbol} (${row.token_out_address})`);
      console.log(`Amount In: ${row.amount_in}`);
      console.log(`Amount Out: ${row.amount_out}`);
      console.log(`From Address: ${row.from_address}`);
      console.log(`To Address: ${row.to_address}`);
      console.log(`Fee Token: ${row.fee_token_symbol}`);
      console.log(`Fee Amount Raw: ${row.fee_amount_raw}`);
      console.log(`Fee Data Source: ${row.fee_data_source}`);
      console.log(`Volume Data Source: ${row.volume_data_source}`);
      console.log('');
    });

    // Summary stats
    console.log('\n=== SUMMARY STATS ===\n');

    const summaryQuery = `
      SELECT
        source,
        COUNT(*) as count,
        MIN(date_only) as earliest,
        MAX(date_only) as latest,
        SUM(in_amount_usd) as total_volume,
        SUM(affiliate_fee_usd) as total_fees
      FROM swaps
      GROUP BY source
      ORDER BY count DESC
    `;

    const summaryResult = await client.query(summaryQuery);
    console.log('Swaps table summary:');
    summaryResult.rows.forEach(row => {
      console.log(`${row.source}: ${row.count} swaps, $${parseFloat(row.total_volume).toFixed(2)} volume, $${parseFloat(row.total_fees).toFixed(2)} fees (${row.earliest} to ${row.latest})`);
    });

    const arkhamSummaryQuery = `
      SELECT
        protocol,
        COUNT(*) as count,
        MIN(DATE(timestamp)) as earliest,
        MAX(DATE(timestamp)) as latest,
        SUM(actual_fee_usd) as total_fees,
        SUM(swap_volume_usd) as total_volume
      FROM dex_aggregator_revenue
      GROUP BY protocol
      ORDER BY count DESC
    `;

    const arkhamSummaryResult = await client.query(arkhamSummaryQuery);
    console.log('\nDEX Aggregator Revenue table summary:');
    arkhamSummaryResult.rows.forEach(row => {
      console.log(`${row.protocol}: ${row.count} records, $${parseFloat(row.total_fees).toFixed(2)} fees, $${row.total_volume ? parseFloat(row.total_volume).toFixed(2) : 'NULL'} volume (${row.earliest} to ${row.latest})`);
    });

  } catch (error) {
    console.error('Error querying data:', error);
  } finally {
    client.release();
    await pool.end();
  }
}

checkData();

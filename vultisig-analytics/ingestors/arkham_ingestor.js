/**
 * Arkham DEX Aggregator Ingestor (Node.js version)
 * Fetches transfer data from Arkham API and ingests into database.
 * Uses actual fee amounts from Arkham as ground truth.
 */

const axios = require('axios');
const { Pool } = require('pg');
require('dotenv').config();

// Configuration
const ARKHAM_API_KEY = process.env.ARKHAM_API_KEY;
const ARKHAM_API_BASE = 'https://api.arkhamintelligence.com';
const INTEGRATOR_ADDRESS = '0xA4a4f610e89488EB4ECc6c63069f241a54485269';
const DATABASE_URL = process.env.DATABASE_URL;

if (!ARKHAM_API_KEY) {
    console.error('❌ ARKHAM_API_KEY environment variable not set');
    process.exit(1);
}

if (!DATABASE_URL) {
    console.error('❌ DATABASE_URL environment variable not set');
    process.exit(1);
}

const pool = new Pool({
    connectionString: DATABASE_URL,
});

// Chain name normalization mapping
const CHAIN_MAPPING = {
    'ethereum': 'Ethereum',
    'bsc': 'BSC',
    'binance-smart-chain': 'BSC',
    'polygon': 'Polygon',
    'polygon-pos': 'Polygon',
    'arbitrum_one': 'Arbitrum',
    'arbitrum-one': 'Arbitrum',
    'optimism': 'Optimism',
    'base': 'Base',
    'avalanche': 'Avalanche',
    'blast': 'Blast',
};

// Known DEX aggregator router addresses (lowercase)
const KNOWN_ROUTERS = {
    '1inch': [
        '0x1111111254eeb25477b68fb85ed929f73a960582',  // 1inch v5 Router
        '0x111111125421ca6dc452d289314280a0f8842a65',  // 1inch v6 Router (Fusion)
        '0x11111112542d85b3ef69ae05771c2dccff4faa26',  // 1inch v4 Router
    ],
    'paraswap': [
        '0xdef171fe48cf0115b1d80b88dc8eab59176fee57',  // Paraswap Augustus v5
        '0x216b4b4ba9f3e719726886d34a177484278bfcae',  // Paraswap Augustus v6
    ],
    'cowswap': [
        '0x9008d19f58aabd9ed0d60971565aa8510560ab41',  // CoWSwap Settlement
    ],
    'matcha': [
        '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  // 0x Exchange Proxy (Matcha)
    ],
    '0x': [
        '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  // 0x Exchange Proxy
    ],
    'thorchain': [
        '0xd37bbe5744d730a1d98d8dc97c42f0ca46ad7146',  // THORChain Router v4.1.1
    ]
};

// Flatten router map for quick lookup
const ROUTER_TO_PROTOCOL = {};
Object.entries(KNOWN_ROUTERS).forEach(([protocol, addresses]) => {
    addresses.forEach(addr => {
        ROUTER_TO_PROTOCOL[addr.toLowerCase()] = protocol;
    });
});

/**
 * Normalize chain name
 */
function normalizeChain(chain) {
    if (!chain) return 'Unknown';
    return CHAIN_MAPPING[chain.toLowerCase()] || chain.charAt(0).toUpperCase() + chain.slice(1);
}

/**
 * Extract address string from Arkham's address object
 */
function extractAddress(addrObj) {
    if (!addrObj) return '';
    if (typeof addrObj === 'string') return addrObj;
    if (typeof addrObj === 'object') return addrObj.address || '';
    return '';
}

/**
 * Identify protocol from Arkham entity metadata
 */
function identifyProtocolFromEntity(addrObj) {
    if (!addrObj || typeof addrObj !== 'object') return null;

    const entity = addrObj.arkhamEntity;
    if (!entity || typeof entity !== 'object') return null;

    const id = (entity.id || '').toLowerCase();
    const name = (entity.name || '').toLowerCase();

    if (id.includes('1inch') || name.includes('1inch')) return '1inch';
    if (id.includes('paraswap') || name.includes('paraswap')) return 'paraswap';
    if (id.includes('cow') || name.includes('cowswap')) return 'cowswap';
    if (id.includes('matcha') || name.includes('0x')) return 'matcha';

    return null;
}

/**
 * Identify protocol by router address
 */
function identifyProtocolByAddress(address) {
    if (!address) return null;
    return ROUTER_TO_PROTOCOL[address.toLowerCase()] || null;
}

/**
 * Check if transaction exists in 1inch database
 */
async function identifyBy1inchDb(client, txHash) {
    try {
        const res = await client.query(
            "SELECT 1 FROM swaps WHERE source = '1inch' AND LOWER(tx_hash) = LOWER($1) LIMIT 1",
            [txHash]
        );
        return res.rowCount > 0 ? '1inch' : null;
    } catch (err) {
        console.error(`Error checking 1inch DB for ${txHash}:`, err.message);
        return null;
    }
}

const VolumeExtractor = require('./volume_extractor');
const volumeExtractor = new VolumeExtractor();

/**
 * Main ingestion function
 */
async function ingest() {
    const client = await pool.connect();

    try {
        console.log('=== STARTING ARKHAM INGESTION (Node.js) ===');
        console.log(`Integrator: ${INTEGRATOR_ADDRESS}`);

        // Fetch transfers
        let allTransfers = [];
        let offset = 0;
        const limit = 1000;
        let hasMore = true;

        while (hasMore) {
            console.log(`Fetching batch at offset ${offset}...`);

            try {
                const response = await axios.get(`${ARKHAM_API_BASE}/transfers`, {
                    params: {
                        base: INTEGRATOR_ADDRESS,
                        limit,
                        offset
                    },
                    headers: {
                        'Accept': 'application/json',
                        'API-Key': ARKHAM_API_KEY
                    },
                    timeout: 30000
                });

                const transfers = response.data.transfers || [];
                console.log(`  ✅ Got ${transfers.length} transfers`);

                if (transfers.length === 0) {
                    hasMore = false;
                } else {
                    allTransfers.push(...transfers);
                    offset += transfers.length;
                    if (transfers.length < limit) hasMore = false;
                }

                // Rate limiting
                if (hasMore) await new Promise(r => setTimeout(r, 1000));

            } catch (error) {
                console.error('  ❌ API Error:', error.message);
                hasMore = false;
            }
        }

        console.log(`\nTotal transfers fetched: ${allTransfers.length}`);

        // Process and insert
        let processedCount = 0;
        let errorCount = 0;

        // await client.query('BEGIN'); // Removed to prevent single error from aborting all

        for (const transfer of allTransfers) {
            try {
                const txHash = transfer.transactionHash;
                if (!txHash) continue;

                // Extract addresses
                const fromAddressObj = transfer.fromAddress;
                const fromAddress = extractAddress(fromAddressObj);
                const toAddress = extractAddress(transfer.toAddress);

                // FILTER: Only count INCOMING transfers (Revenue)
                if (toAddress.toLowerCase() !== INTEGRATOR_ADDRESS.toLowerCase()) {
                    // console.log(`Skipping outgoing transfer ${txHash}`);
                    continue;
                }

                // Identify protocol
                let protocol = identifyProtocolFromEntity(fromAddressObj);

                if (!protocol) {
                    protocol = identifyProtocolByAddress(fromAddress);
                }

                if (!protocol) {
                    protocol = await identifyBy1inchDb(client, txHash);
                }

                if (!protocol) {
                    protocol = 'other';
                }

                // Normalize chain
                const chain = normalizeChain(transfer.chain);

                // Extract Volume (New Step)
                let volumeData = null;
                // Only try to extract volume for supported protocols/chains to save RPC calls
                if (protocol === '1inch' || protocol === 'paraswap' || protocol === 'cowswap' || protocol === 'other') {
                    try {
                        volumeData = await volumeExtractor.getVolume(txHash, chain);
                    } catch (err) {
                        console.warn(`Failed to extract volume for ${txHash}: ${err.message}`);
                    }
                }

                // Extract fee data
                const actualFeeUsd = parseFloat(transfer.historicalUSD || 0);
                const feeTokenSymbol = transfer.tokenSymbol || '';
                const feeTokenAddress = transfer.tokenAddress || '';
                const feeAmountRaw = String(transfer.unitValue || '');

                // Timestamp
                const timestamp = transfer.blockTimestamp ? new Date(transfer.blockTimestamp) : new Date();

                // Insert
                await client.query(`
          INSERT INTO dex_aggregator_revenue (
            tx_hash, chain, protocol, timestamp,
            actual_fee_usd, fee_token_symbol, fee_token_address,
            fee_amount_raw, block_number, from_address, to_address,
            fee_data_source,
            amount_in, token_in_address, volume_data_source, token_in_decimals
          ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7,
            $8, $9, $10, $11,
            'arkham',
            $12, $13, $14, $15
          )
          ON CONFLICT (tx_hash) DO UPDATE SET
            actual_fee_usd = EXCLUDED.actual_fee_usd,
            protocol = EXCLUDED.protocol,
            fee_token_symbol = EXCLUDED.fee_token_symbol,
            fee_data_source = EXCLUDED.fee_data_source,
            amount_in = EXCLUDED.amount_in,
            token_in_address = EXCLUDED.token_in_address,
            volume_data_source = EXCLUDED.volume_data_source,
            token_in_decimals = EXCLUDED.token_in_decimals,
            updated_at = NOW()
        `, [
                    txHash,
                    chain,
                    protocol,
                    timestamp,
                    actualFeeUsd,
                    feeTokenSymbol,
                    feeTokenAddress,
                    feeAmountRaw,
                    transfer.blockNumber || null,
                    fromAddress,
                    toAddress,
                    volumeData ? volumeData.amount : null,
                    volumeData ? volumeData.token : null,
                    volumeData ? 'blockchain_rpc' : null,
                    volumeData ? volumeData.decimals : null
                ]);

                processedCount++;
                if (processedCount % 100 === 0) {
                    console.log(`Processed ${processedCount}/${allTransfers.length}`);
                }

            } catch (err) {
                console.error(`Error processing tx ${transfer.transactionHash}:`, err.message);
                errorCount++;
            }
        }

        await client.query('COMMIT');
        console.log(`\n✅ Ingestion complete!`);
        console.log(`Processed: ${processedCount}`);
        console.log(`Errors: ${errorCount}`);

        // Show stats
        const stats = await client.query(`
      SELECT protocol, COUNT(*) as count, SUM(actual_fee_usd) as total_fees
      FROM dex_aggregator_revenue
      GROUP BY protocol
      ORDER BY total_fees DESC
    `);

        console.log('\nProtocol Breakdown:');
        console.table(stats.rows);

    } catch (err) {
        await client.query('ROLLBACK');
        console.error('❌ Fatal error:', err);
    } finally {
        client.release();
        await pool.end();
    }
}

ingest();

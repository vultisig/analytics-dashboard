import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
    const searchParams = request.nextUrl.searchParams;
    const chain = searchParams.get('chain');

    if (!chain || (chain !== 'thorchain' && chain !== 'mayachain')) {
        return NextResponse.json({ error: 'Invalid chain parameter. Must be "thorchain" or "mayachain"' }, { status: 400 });
    }

    const client = await pool.connect();

    try {
        const query = `
            SELECT 
                tx_hash,
                timestamp,
                in_asset,
                in_amount,
                out_asset,
                out_amount,
                raw_data,
                ROW_NUMBER() OVER (ORDER BY timestamp ASC) as tx_index
            FROM swaps
            WHERE source = $1
            ORDER BY timestamp ASC
        `;

        const result = await client.query(query, [chain]);


        const transactions = result.rows.map(row => {
            const metadata = row.raw_data?.metadata?.swap || {};
            const affiliateAddress = metadata.affiliateAddress || 'N/A';
            const affiliateBps = parseInt(metadata.affiliateFee || '0');

            // Helper function to get asset decimals
            const getAssetDecimals = (asset: string): number => {
                if (!asset) return 1e8;
                const assetUpper = asset.toUpperCase();

                // USDC and USDT use 6 decimals
                if (assetUpper.includes('USDC') || assetUpper.includes('USDT')) return 1e6;

                // Native ETH uses 18 decimals
                if (assetUpper === 'ETH.ETH') return 1e18;

                // CACAO uses 10 decimals
                if (assetUpper.includes('CACAO')) return 1e10;

                // Most other assets (BTC, RUNE, etc.) use 8 decimals
                return 1e8;
            };

            // Get asset-specific decimals
            const inDecimals = getAssetDecimals(row.in_asset);
            const outDecimals = getAssetDecimals(row.out_asset);

            // Inbound Calculation
            const inAmountRaw = parseFloat(row.in_amount || '0');
            const inPriceUSD = parseFloat(metadata.inPriceUSD || '0');
            const inVolumeUSD = (inAmountRaw / inDecimals) * inPriceUSD;

            // Outbound Calculation
            const outAmountRaw = parseFloat(row.out_amount || '0');
            const outPriceUSD = parseFloat(metadata.outPriceUSD || '0');
            const outVolumeUSD = (outAmountRaw / outDecimals) * outPriceUSD;

            // Affiliate Fee Calculation
            const feeFrac = affiliateBps / 10000;
            const affiliateFeeUSD = inVolumeUSD * feeFrac;

            // Fee Asset Details
            let feeAmount = 0;
            let feeAsset = 'N/A';
            let feeAssetPrice = 0;

            if (row.raw_data?.out && Array.isArray(row.raw_data.out)) {
                const feeOut = row.raw_data.out.find((o: any) => o.affiliate === true);
                if (feeOut && feeOut.coins && feeOut.coins.length > 0) {
                    feeAmount = parseFloat(feeOut.coins[0].amount || '0');
                    feeAsset = feeOut.coins[0].asset;

                    // Calculate Fee Asset Price
                    // fee_asset_price = affiliate_fee_usd / (fee_amount / fee_decimals)
                    // Check if fee asset is RUNE or CACAO explicitly
                    const isRune = feeAsset.includes('RUNE');
                    const isCacao = feeAsset.includes('CACAO');

                    if (isRune || isCacao) {
                        const feeDecimals = isCacao ? 1e10 : 1e8; // CACAO 1e10, RUNE 1e8
                        if (feeAmount > 0) {
                            feeAssetPrice = affiliateFeeUSD / (feeAmount / feeDecimals);
                        }
                    }
                }
            }

            return {
                tx_index: parseInt(row.tx_index),
                tx_hash: row.tx_hash,
                timestamp: row.timestamp,
                in_asset: row.in_asset,
                in_amount: row.in_amount,
                in_volume_usd: inVolumeUSD,
                in_price_usd: inPriceUSD,
                out_asset: row.out_asset,
                out_amount: row.out_amount,
                out_volume_usd: outVolumeUSD,
                out_price_usd: outPriceUSD,
                affiliate_address: affiliateAddress,
                affiliate_basis_points: affiliateBps,
                affiliate_fee_usd: affiliateFeeUSD,
                fee_amount: feeAmount,
                fee_asset: feeAsset,
                fee_asset_price: feeAssetPrice
            };
        });

        return NextResponse.json({ transactions });

    } catch (error) {
        console.error('Transaction Debug API Error:', error);
        return NextResponse.json({ error: 'Failed to fetch transaction data' }, { status: 500 });
    } finally {
        client.release();
    }
}

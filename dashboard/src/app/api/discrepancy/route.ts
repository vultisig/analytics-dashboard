import { NextResponse } from 'next/server';
import pool from '@/lib/db';
import fs from 'fs';
import path from 'path';

export const dynamic = 'force-dynamic';

interface RefData {
    date: string;
    thorchain: number;
    lifi: number;
    oneinch: number;
    total: number;
}

interface OurData {
    date: string;
    thorchain_volume: number;
    thorchain_revenue: number;
    thorchain_swappers: number;
    thorchain_swaps: number;
    lifi_volume: number;
    lifi_revenue: number;
    lifi_swappers: number;
    lifi_swaps: number;
    oneinch_volume: number;
    oneinch_revenue: number;
    oneinch_swappers: number;
    oneinch_swaps: number;
    total_volume: number;
    total_revenue: number;
    total_swappers: number;
    total_swaps: number;
}

export async function GET() {
    const client = await pool.connect();

    try {
        // Load reference revenue CSV
        const revenueRefPath = path.join(process.cwd(), '../vultisig-analytics/refdocs/vultisig-aggregated-fee-revenue-by-provider.csv');
        const revenueRefContent = fs.readFileSync(revenueRefPath, 'utf-8');
        const revenueLines = revenueRefContent.split('\n').filter(line => line.trim());
        const revenueRefRecords = revenueLines.slice(1).map(line => {
            // Parse CSV line: "Date",Value1,Value2,Value3
            const match = line.match(/"([^"]+)",([^,]+),([^,]+),([^,]+)/);
            if (!match) return null;
            return {
                Date: match[1],
                THORChain: match[2],
                LiFi: match[3],
                '1inch': match[4]
            };
        }).filter(Boolean);

        // Load reference volume CSV
        const volumeRefPath = path.join(process.cwd(), '../vultisig-analytics/refdocs/vultisig-aggregated-swap-volume-by-provider.csv');
        const volumeRefContent = fs.readFileSync(volumeRefPath, 'utf-8');
        const volumeLines = volumeRefContent.split('\n').filter(line => line.trim());
        const volumeRefRecords = volumeLines.slice(1).map(line => {
            const match = line.match(/"([^"]+)",([^,]+),([^,]+),([^,]+)/);
            if (!match) return null;
            return {
                Date: match[1],
                THORChain: match[2],
                LiFi: match[3],
                '1inch': match[4]
            };
        }).filter(Boolean);

        // Load reference swapper CSV
        const swapperRefPath = path.join(process.cwd(), '../vultisig-analytics/refdocs/swapper_by_provider.csv');
        const swapperRefContent = fs.readFileSync(swapperRefPath, 'utf-8');
        const swapperLines = swapperRefContent.split('\n').filter(line => line.trim());
        const swapperRefRecords = swapperLines.slice(1).map(line => {
            const match = line.match(/"([^"]+)",([^,]+),([^,]+),([^,]+)/);
            if (!match) return null;
            return {
                Date: match[1],
                THORChain: match[2],
                LiFi: match[3],
                '1inch': match[4]
            };
        }).filter(Boolean);

        interface RefDataFull {
            date: string;
            revenue: { thorchain: number; lifi: number; oneinch: number; total: number; };
            volume: { thorchain: number; lifi: number; oneinch: number; total: number; };
            swappers: { thorchain: number; lifi: number; oneinch: number; total: number; };
        }

        const refData: Record<string, RefDataFull> = {};

        // Process revenue data
        revenueRefRecords.forEach((row: any) => {
            if (!row) return;
            const date = row.Date;
            if (!date || date === 'undefined') return;
            const thorchain = parseFloat(row.THORChain) || 0;
            const lifi = parseFloat(row.LiFi) || 0;
            const oneinch = parseFloat(row['1inch']) || 0;
            if (!refData[date]) {
                refData[date] = {
                    date,
                    revenue: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 },
                    volume: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 },
                    swappers: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 }
                };
            }
            refData[date].revenue = {
                thorchain,
                lifi,
                oneinch,
                total: thorchain + lifi + oneinch
            };
        });

        // Process volume data
        volumeRefRecords.forEach((row: any) => {
            if (!row) return;
            const date = row.Date;
            if (!date || date === 'undefined') return;
            const thorchain = parseFloat(row.THORChain) || 0;
            const lifi = parseFloat(row.LiFi) || 0;
            const oneinch = parseFloat(row['1inch']) || 0;
            if (!refData[date]) {
                refData[date] = {
                    date,
                    revenue: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 },
                    volume: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 },
                    swappers: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 }
                };
            }
            refData[date].volume = {
                thorchain,
                lifi,
                oneinch,
                total: thorchain + lifi + oneinch
            };
        });

        // Process swapper data
        swapperRefRecords.forEach((row: any) => {
            if (!row) return;
            const date = row.Date;
            if (!date || date === 'undefined') return;
            const thorchain = parseFloat(row.THORChain) || 0;
            const lifi = parseFloat(row.LiFi) || 0;
            const oneinch = parseFloat(row['1inch']) || 0;
            if (!refData[date]) {
                refData[date] = {
                    date,
                    revenue: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 },
                    volume: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 },
                    swappers: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 }
                };
            }
            refData[date].swappers = {
                thorchain,
                lifi,
                oneinch,
                total: thorchain + lifi + oneinch
            };
        });

        // Query our database for all daily data
        const ourQuery = `
      WITH swaps_data AS (
        SELECT
          date_only,
          source,
          SUM(in_amount_usd) as volume,
          SUM(affiliate_fee_usd) as revenue,
          COUNT(DISTINCT user_address) as swappers,
          COUNT(*) as swaps
        FROM swaps
        WHERE source != '1inch'
        GROUP BY date_only, source
      ),
      arkham_data AS (
        SELECT
          DATE(timestamp) as date_only,
          '1inch' as source,
          SUM(swap_volume_usd) as volume,
          SUM(actual_fee_usd) as revenue,
          COUNT(DISTINCT from_address) as swappers,
          COUNT(*) as swaps
        FROM dex_aggregator_revenue
        WHERE protocol = '1inch'
        GROUP BY DATE(timestamp)
      ),
      combined AS (
        SELECT * FROM swaps_data
        UNION ALL
        SELECT * FROM arkham_data
      )
      SELECT
        date_only::text as date,
        SUM(CASE WHEN source = 'thorchain' THEN volume ELSE 0 END) as thorchain_volume,
        SUM(CASE WHEN source = 'thorchain' THEN revenue ELSE 0 END) as thorchain_revenue,
        SUM(CASE WHEN source = 'thorchain' THEN swappers ELSE 0 END) as thorchain_swappers,
        SUM(CASE WHEN source = 'thorchain' THEN swaps ELSE 0 END) as thorchain_swaps,
        SUM(CASE WHEN source = 'lifi' THEN volume ELSE 0 END) as lifi_volume,
        SUM(CASE WHEN source = 'lifi' THEN revenue ELSE 0 END) as lifi_revenue,
        SUM(CASE WHEN source = 'lifi' THEN swappers ELSE 0 END) as lifi_swappers,
        SUM(CASE WHEN source = 'lifi' THEN swaps ELSE 0 END) as lifi_swaps,
        SUM(CASE WHEN source = '1inch' THEN volume ELSE 0 END) as oneinch_volume,
        SUM(CASE WHEN source = '1inch' THEN revenue ELSE 0 END) as oneinch_revenue,
        SUM(CASE WHEN source = '1inch' THEN swappers ELSE 0 END) as oneinch_swappers,
        SUM(CASE WHEN source = '1inch' THEN swaps ELSE 0 END) as oneinch_swaps,
        SUM(volume) as total_volume,
        SUM(revenue) as total_revenue,
        SUM(swappers) as total_swappers,
        SUM(swaps) as total_swaps
      FROM combined
      GROUP BY date_only
      ORDER BY date_only ASC
    `;

        const ourResult = await client.query(ourQuery);
        const ourDataMap: Record<string, OurData> = {};
        ourResult.rows.forEach((row: any) => {
            ourDataMap[row.date] = {
                date: row.date,
                thorchain_volume: parseFloat(row.thorchain_volume) || 0,
                thorchain_revenue: parseFloat(row.thorchain_revenue) || 0,
                thorchain_swappers: parseInt(row.thorchain_swappers) || 0,
                thorchain_swaps: parseInt(row.thorchain_swaps) || 0,
                lifi_volume: parseFloat(row.lifi_volume) || 0,
                lifi_revenue: parseFloat(row.lifi_revenue) || 0,
                lifi_swappers: parseInt(row.lifi_swappers) || 0,
                lifi_swaps: parseInt(row.lifi_swaps) || 0,
                oneinch_volume: parseFloat(row.oneinch_volume) || 0,
                oneinch_revenue: parseFloat(row.oneinch_revenue) || 0,
                oneinch_swappers: parseInt(row.oneinch_swappers) || 0,
                oneinch_swaps: parseInt(row.oneinch_swaps) || 0,
                total_volume: parseFloat(row.total_volume) || 0,
                total_revenue: parseFloat(row.total_revenue) || 0,
                total_swappers: parseInt(row.total_swappers) || 0,
                total_swaps: parseInt(row.total_swaps) || 0
            };
        });

        // Combine and calculate discrepancies
        const allDates = [...new Set([...Object.keys(refData), ...Object.keys(ourDataMap)])].sort();
        const comparisons = allDates.map(date => {
            const ref = refData[date] || {
                date,
                revenue: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 },
                volume: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 },
                swappers: { thorchain: 0, lifi: 0, oneinch: 0, total: 0 }
            };
            const our = ourDataMap[date] || {
                date,
                thorchain_volume: 0,
                thorchain_revenue: 0,
                thorchain_swappers: 0,
                thorchain_swaps: 0,
                lifi_volume: 0,
                lifi_revenue: 0,
                lifi_swappers: 0,
                lifi_swaps: 0,
                oneinch_volume: 0,
                oneinch_revenue: 0,
                oneinch_swappers: 0,
                oneinch_swaps: 0,
                total_volume: 0,
                total_revenue: 0,
                total_swappers: 0,
                total_swaps: 0
            };

            // Revenue discrepancies
            const total_revenue_diff = our.total_revenue - ref.revenue.total;
            const total_revenue_pct = ref.revenue.total > 0 ? (total_revenue_diff / ref.revenue.total) * 100 : 0;

            // Volume discrepancies  
            const total_volume_diff = our.total_volume - ref.volume.total;
            const total_volume_pct = ref.volume.total > 0 ? (total_volume_diff / ref.volume.total) * 100 : 0;

            // Swappers discrepancies
            const total_swappers_diff = our.total_swappers - ref.swappers.total;
            const total_swappers_pct = ref.swappers.total > 0 ? (total_swappers_diff / ref.swappers.total) * 100 : 0;

            return {
                date,
                ref_total_revenue: ref.revenue.total,
                our_total_revenue: our.total_revenue,
                total_revenue_diff,
                total_revenue_pct,
                ref_total_volume: ref.volume.total,
                our_total_volume: our.total_volume,
                total_volume_diff,
                total_volume_pct,
                ref_total_swappers: ref.swappers.total,
                our_total_swappers: our.total_swappers,
                total_swappers_diff,
                total_swappers_pct,
                // Our additional metrics
                total_swaps: our.total_swaps
            };
        });

        // Calculate statistics
        const revenueDiffs = comparisons.map(c => c.total_revenue_diff).filter(d => !Number.isNaN(d));
        const revenuePcts = comparisons.map(c => c.total_revenue_pct).filter(p => !Number.isNaN(p) && Number.isFinite(p));
        const volumeDiffs = comparisons.map(c => c.total_volume_diff).filter(d => !Number.isNaN(d));
        const volumePcts = comparisons.map(c => c.total_volume_pct).filter(p => !Number.isNaN(p) && Number.isFinite(p));
        const swappersDiffs = comparisons.map(c => c.total_swappers_diff).filter(d => !Number.isNaN(d));
        const swappersPcts = comparisons.map(c => c.total_swappers_pct).filter(p => !Number.isNaN(p) && Number.isFinite(p));

        const stats = {
            total_days: comparisons.length,
            revenue_diff: {
                min: Math.min(...revenueDiffs),
                max: Math.max(...revenueDiffs),
                avg: revenueDiffs.reduce((a, b) => a + b, 0) / revenueDiffs.length,
                median: revenueDiffs.sort((a, b) => a - b)[Math.floor(revenueDiffs.length / 2)]
            },
            revenue_pct: {
                min: Math.min(...revenuePcts),
                max: Math.max(...revenuePcts),
                avg: revenuePcts.reduce((a, b) => a + b, 0) / revenuePcts.length,
                median: revenuePcts.sort((a, b) => a - b)[Math.floor(revenuePcts.length / 2)]
            },
            volume_diff: {
                min: Math.min(...volumeDiffs),
                max: Math.max(...volumeDiffs),
                avg: volumeDiffs.reduce((a, b) => a + b, 0) / volumeDiffs.length,
                median: volumeDiffs.sort((a, b) => a - b)[Math.floor(volumeDiffs.length / 2)]
            },
            volume_pct: {
                min: Math.min(...volumePcts),
                max: Math.max(...volumePcts),
                avg: volumePcts.reduce((a, b) => a + b, 0) / volumePcts.length,
                median: volumePcts.sort((a, b) => a - b)[Math.floor(volumePcts.length / 2)]
            },
            swappers_diff: {
                min: Math.min(...swappersDiffs),
                max: Math.max(...swappersDiffs),
                avg: swappersDiffs.reduce((a, b) => a + b, 0) / swappersDiffs.length,
                median: swappersDiffs.sort((a, b) => a - b)[Math.floor(swappersDiffs.length / 2)]
            },
            swappers_pct: {
                min: Math.min(...swappersPcts),
                max: Math.max(...swappersPcts),
                avg: swappersPcts.reduce((a, b) => a + b, 0) / swappersPcts.length,
                median: swappersPcts.sort((a, b) => a - b)[Math.floor(swappersPcts.length / 2)]
            },
            total_ref_revenue: comparisons.reduce((sum, c) => sum + c.ref_total_revenue, 0),
            total_our_revenue: comparisons.reduce((sum, c) => sum + c.our_total_revenue, 0),
            total_ref_volume: comparisons.reduce((sum, c) => sum + c.ref_total_volume, 0),
            total_our_volume: comparisons.reduce((sum, c) => sum + c.our_total_volume, 0),
            total_ref_swappers: comparisons.reduce((sum, c) => sum + c.ref_total_swappers, 0),
            total_our_swappers: comparisons.reduce((sum, c) => sum + c.our_total_swappers, 0)
        };

        return NextResponse.json({
            comparisons,
            stats
        });

    } catch (error) {
        console.error('=== Discrepancy API Error ===');
        console.error('Error:', error);
        return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 });
    } finally {
        client.release();
    }
}

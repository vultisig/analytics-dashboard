import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';
import fs from 'fs';
import path from 'path';

export const dynamic = 'force-dynamic';

// Helper function to parse CSV
function parseCSV(csvContent: string): Array<{ snapped_at: string; price: number }> {
  const lines = csvContent.trim().split('\n');

  return lines.slice(1).map(line => {
    const values = line.split(',');
    return {
      snapped_at: values[0],
      price: parseFloat(values[1])
    };
  }).filter(row => !isNaN(row.price));
}

// Filter reference data by date
function filterReferenceByDate(data: Array<{ snapped_at: string; price: number }>, startDate: Date | null): Array<{ timestamp: string; price: number }> {
  if (!startDate) {
    return data.map(d => ({ timestamp: d.snapped_at, price: d.price }));
  }

  return data
    .filter(d => new Date(d.snapped_at) >= startDate)
    .map(d => ({ timestamp: d.snapped_at, price: d.price }));
}

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const timeRange = searchParams.get('range') || '7d';

  const client = await pool.connect();

  try {
    // Calculate date filter based on time range
    let dateCondition = '';
    let csvStartDate: Date | null = null;
    const now = new Date();

    switch (timeRange) {
      case '24h':
        const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        dateCondition = `timestamp >= '${yesterday.toISOString()}'`;
        csvStartDate = yesterday;
        break;
      case '7d':
        const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        dateCondition = `timestamp >= '${weekAgo.toISOString()}'`;
        csvStartDate = weekAgo;
        break;
      case '30d':
        const monthAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
        dateCondition = `timestamp >= '${monthAgo.toISOString()}'`;
        csvStartDate = monthAgo;
        break;
      case '90d':
        const quarterAgo = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
        dateCondition = `timestamp >= '${quarterAgo.toISOString()}'`;
        csvStartDate = quarterAgo;
        break;
      default: // 'all'
        dateCondition = '1=1';
        csvStartDate = null;
    }

    // Fetch calculated prices from swaps
    const runeQuery = `
      SELECT 
        timestamp,
        raw_data,
        in_amount,
        out_amount,
        in_asset,
        out_asset,
        affiliate_fee_usd
      FROM swaps 
      WHERE source = 'thorchain'
        AND ${dateCondition}
      ORDER BY timestamp ASC
    `;

    const cacaoQuery = `
      SELECT 
        timestamp,
        raw_data,
        in_amount,
        out_amount,
        in_asset,
        out_asset,
        affiliate_fee_usd
      FROM swaps 
      WHERE source = 'mayachain'
        AND ${dateCondition}
      ORDER BY timestamp ASC
    `;

    const [runeResult, cacaoResult] = await Promise.all([
      client.query(runeQuery),
      client.query(cacaoQuery)
    ]);

    const calculatePrice = (rows: any[], chain: 'thorchain' | 'mayachain') => {
      return rows.map(row => {
        // Check if the outbound asset is RUNE or CACAO
        const outAssetUpper = (row.out_asset || '').toUpperCase();
        const isRuneOut = outAssetUpper.includes('RUNE');
        const isCacaoOut = outAssetUpper.includes('CACAO');

        // Only use swaps where output asset matches chain's native token
        const isCorrectChain = (chain === 'thorchain' && isRuneOut) || (chain === 'mayachain' && isCacaoOut);

        if (!isCorrectChain) {
          return null;
        }

        // Calculate price from affiliate fee USD and amount
        // Price = affiliate_fee_usd / (out_amount / 1e8)
        const feeUsd = parseFloat(row.affiliate_fee_usd || '0');
        const feeAmount = parseFloat(row.out_amount || '0') / 1e8;

        if (feeUsd > 0 && feeAmount > 0) {
          const price = feeUsd / feeAmount;

          // Sanity check: Price shouldn't be excessively high (e.g. > $1000 for RUNE/CACAO)
          // This filters out potential bad data points
          if (price < 1000) {
            return {
              timestamp: row.timestamp,
              price: price
            };
          }
        }
        return null;
      }).filter((p): p is { timestamp: string; price: number } => p !== null);
    };

    const runePrices = calculatePrice(runeResult.rows, 'thorchain');
    const cacaoPrices = calculatePrice(cacaoResult.rows, 'mayachain');

    // Load reference CSV files
    const workspaceRoot = path.join(process.cwd(), '..');
    const runeCsvPath = path.join(workspaceRoot, 'vultisig-analytics', 'refdocs', 'rune-usd-max.csv');
    const cacaoCsvPath = path.join(workspaceRoot, 'vultisig-analytics', 'refdocs', 'cacao-usd-max.csv');

    let runeReferenceData: Array<{ timestamp: string; price: number }> = [];
    let cacaoReferenceData: Array<{ timestamp: string; price: number }> = [];

    // Helper to filter by min/max range
    const filterByRange = (data: Array<{ snapped_at: string; price: number }>, minTime: number, maxTime: number) => {
      return data
        .filter(d => {
          const t = new Date(d.snapped_at).getTime();
          return t >= minTime && t <= maxTime;
        })
        .map(d => ({ timestamp: d.snapped_at, price: d.price }));
    };

    try {
      if (fs.existsSync(runeCsvPath) && runePrices.length > 0) {
        const runeCsvContent = fs.readFileSync(runeCsvPath, 'utf-8');
        const runeParsed = parseCSV(runeCsvContent);

        const minRuneTime = new Date(runePrices[0].timestamp).getTime();
        const maxRuneTime = new Date(runePrices[runePrices.length - 1].timestamp).getTime();

        runeReferenceData = filterByRange(runeParsed, minRuneTime, maxRuneTime);
      }
    } catch (err) {
      console.error('Error loading RUNE reference data:', err);
    }

    try {
      if (fs.existsSync(cacaoCsvPath) && cacaoPrices.length > 0) {
        const cacaoCsvContent = fs.readFileSync(cacaoCsvPath, 'utf-8');
        const cacaoParsed = parseCSV(cacaoCsvContent);

        const minCacaoTime = new Date(cacaoPrices[0].timestamp).getTime();
        const maxCacaoTime = new Date(cacaoPrices[cacaoPrices.length - 1].timestamp).getTime();

        cacaoReferenceData = filterByRange(cacaoParsed, minCacaoTime, maxCacaoTime);
      }
    } catch (err) {
      console.error('Error loading CACAO reference data:', err);
    }

    return NextResponse.json({
      rune: {
        prices: runePrices,
        referencePrices: runeReferenceData
      },
      cacao: {
        prices: cacaoPrices,
        referencePrices: cacaoReferenceData
      }
    });

  } catch (error) {
    console.error('Price API Error:', error);
    return NextResponse.json({ error: 'Failed to fetch price data' }, { status: 500 });
  } finally {
    client.release();
  }
}

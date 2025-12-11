import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';
import { getDateRangeFromParams, calculateDateRange } from '@/lib/dateUtils';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
    const searchParams = request.nextUrl.searchParams;
    const range = getDateRangeFromParams(Object.fromEntries(searchParams));
    const { start, end } = calculateDateRange(range);

    // Manual date filter construction for dex_aggregator_revenue table
    // The buildDateFilter utility might assume 'swaps' table or specific columns
    // But dex_aggregator_revenue has 'timestamp' column.

    // Let's just use the range object directly
    const startDate = start || new Date(0); // Default to epoch if all time
    const endDate = end;

    try {
        const query = `
      SELECT 
        protocol,
        SUM(actual_fee_usd) as total_fees,
        SUM(swap_volume_usd) as total_volume,
        COUNT(*) as swap_count
      FROM dex_aggregator_revenue
      WHERE protocol != 'thorchain'
      AND protocol != 'other'
      AND actual_fee_usd > 0
      AND timestamp >= $1 AND timestamp <= $2
      GROUP BY protocol
      ORDER BY total_fees DESC
    `;

        const result = await pool.query(query, [startDate.toISOString(), endDate.toISOString()]);

        // Format numbers
        const formatted = result.rows.map(row => ({
            protocol: row.protocol,
            total_fees: parseFloat(row.total_fees || 0),
            total_volume: parseFloat(row.total_volume || 0),
            swap_count: parseInt(row.swap_count || 0)
        }));

        return NextResponse.json(formatted);
    } catch (error) {
        console.error('Error fetching revenue breakdown:', error);
        return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
    }
}

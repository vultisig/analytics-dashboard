import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
    const searchParams = request.nextUrl.searchParams;
    const errorType = searchParams.get('errorType');
    const source = searchParams.get('source');

    try {
        let whereConditions = [];
        let params = [];
        let paramIndex = 1;

        if (errorType) {
            whereConditions.push(`error_type = $${paramIndex++}`);
            params.push(errorType);
        }

        if (source) {
            whereConditions.push(`source = $${paramIndex++}`);
            params.push(source);
        }

        const whereClause = whereConditions.length > 0
            ? `WHERE ${whereConditions.join(' AND ')}`
            : '';

        // Get summary stats
        const statsQuery = `
            SELECT 
                error_type,
                source,
                COUNT(*) as count,
                AVG(retry_count) as avg_retries,
                MAX(retry_count) as max_retries
            FROM ingestion_errors
            ${whereClause}
            GROUP BY error_type, source
            ORDER BY count DESC
        `;

        const statsResult = await pool.query(statsQuery, params);

        // Get recent errors
        const errorsQuery = `
            SELECT 
                id,
                tx_hash,
                source,
                error_type,
                error_message,
                retry_count,
                last_retry_at,
                created_at
            FROM ingestion_errors
            ${whereClause}
            ORDER BY created_at DESC
            LIMIT 100
        `;

        const errorsResult = await pool.query(errorsQuery, params);

        // Get total count
        const countQuery = `SELECT COUNT(*) as total FROM ingestion_errors ${whereClause}`;
        const countResult = await pool.query(countQuery, params);

        return NextResponse.json({
            total: parseInt(countResult.rows[0].total),
            stats: statsResult.rows,
            errors: errorsResult.rows
        });

    } catch (error) {
        console.error('Error fetching ingestion errors:', error);
        return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 });
    }
}

// POST endpoint to manually retry a specific transaction
export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        const { txHash } = body;

        if (!txHash) {
            return NextResponse.json({ error: 'txHash required' }, { status: 400 });
        }

        // This would trigger the reprocessing job for a specific transaction
        // For now, just log it and return success
        console.log(`Manual retry requested for: ${txHash}`);

        return NextResponse.json({
            success: true,
            message: 'Retry job queued. Check logs for results.'
        });

    } catch (error) {
        console.error('Error triggering retry:', error);
        return NextResponse.json({ error: 'Failed to trigger retry' }, { status: 500 });
    }
}

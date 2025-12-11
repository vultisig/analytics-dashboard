import { NextResponse } from 'next/server';
import { Pool } from 'pg';

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
});

export async function GET() {
    try {
        const client = await pool.connect();
        try {
            // Fetch sync status for all sources
            const query = `
                SELECT source, last_synced_timestamp, latest_data_timestamp, last_error, is_active
                FROM sync_status
                ORDER BY source ASC
            `;
            const result = await client.query(query);

            // Also check for Arkham status specifically since it might not be in sync_status if it just runs via main.py
            // But main.py updates sync_status for all sources now, so it should be there.

            return NextResponse.json(result.rows);
        } finally {
            client.release();
        }
    } catch (error) {
        console.error('Error fetching system status:', error);
        return NextResponse.json(
            { error: 'Failed to fetch system status' },
            { status: 500 }
        );
    }
}

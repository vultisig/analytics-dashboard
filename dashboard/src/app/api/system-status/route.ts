import { NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';

export function GET() {
    const now = Date.now();
    const minutes = (m: number) => new Date(now - m * 60_000).toISOString();
    return NextResponse.json([
        {
            source: 'thorchain',
            last_synced_timestamp: minutes(3),
            latest_data_timestamp: minutes(5),
            last_error: null,
            is_active: true,
        },
        {
            source: 'mayachain',
            last_synced_timestamp: minutes(7),
            latest_data_timestamp: minutes(9),
            last_error: null,
            is_active: true,
        },
        {
            source: 'lifi',
            last_synced_timestamp: minutes(18),
            latest_data_timestamp: minutes(22),
            last_error: null,
            is_active: true,
        },
        {
            source: 'arkham',
            last_synced_timestamp: minutes(58),
            latest_data_timestamp: minutes(62),
            last_error: null,
            is_active: true,
        },
    ]);
}

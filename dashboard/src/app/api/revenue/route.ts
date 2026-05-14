import { NextResponse, type NextRequest } from 'next/server';
import {
    revenueOverTime,
    PROVIDERS,
    PLATFORMS,
    type Granularity,
} from '../_mock/seed';

export const dynamic = 'force-dynamic';

export function GET(req: NextRequest) {
    const g = (req.nextUrl.searchParams.get('g') ?? 'w') as Granularity;
    const rows = revenueOverTime(g);
    const total = rows.reduce((s, r) => s + r.revenue, 0);

    const revenueByProvider = PROVIDERS.map((p) => ({
        source: p,
        total_revenue: rows.filter((r) => r.source === p).reduce((s, r) => s + r.revenue, 0),
    }));

    return NextResponse.json({
        revenueOverTime: rows,
        revenueByProvider,
        revenueByPlatformOverTime: rows.map((r) => ({
            date: r.date,
            platform: PLATFORMS[Math.floor(Math.random() * PLATFORMS.length)],
            revenue: r.revenue / 4,
        })),
        totalRevenue: { total_revenue: total },
    });
}

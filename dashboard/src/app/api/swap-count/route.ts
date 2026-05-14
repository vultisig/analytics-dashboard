import { NextResponse, type NextRequest } from 'next/server';
import { countOverTime, PROVIDERS, PLATFORMS, type Granularity } from '../_mock/seed';

export const dynamic = 'force-dynamic';

export function GET(req: NextRequest) {
    const g = (req.nextUrl.searchParams.get('g') ?? 'w') as Granularity;
    const rows = countOverTime(g);
    const total = rows.reduce((s, r) => s + r.count, 0);
    const countByProvider = PROVIDERS.map((p) => ({
        source: p,
        total_count: rows.filter((r) => r.source === p).reduce((s, r) => s + r.count, 0),
    }));
    return NextResponse.json({
        countOverTime: rows,
        countByProvider,
        countByPlatformOverTime: rows.flatMap((r) =>
            PLATFORMS.map((plat, idx) => ({
                date: r.date,
                platform: plat,
                count: Math.round(r.count * [0.46, 0.33, 0.16, 0.05][idx]),
            }))
        ),
        totalCount: { total_count: total },
    });
}

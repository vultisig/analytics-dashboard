import { NextResponse, type NextRequest } from 'next/server';
import { usersOverTime, PROVIDERS, PLATFORMS, totals, type Granularity } from '../_mock/seed';

export const dynamic = 'force-dynamic';

export function GET(req: NextRequest) {
    const g = (req.nextUrl.searchParams.get('g') ?? 'w') as Granularity;
    const rows = usersOverTime(g);
    const t = totals(g);
    const usersByProvider = PROVIDERS.map((p) => ({
        source: p,
        unique_users: rows.filter((r) => r.source === p).reduce((s, r) => s + r.users, 0),
    }));
    return NextResponse.json({
        usersOverTime: rows,
        usersByProvider,
        usersByPlatformOverTime: rows.flatMap((r) =>
            PLATFORMS.map((plat, idx) => ({
                date: r.date,
                platform: plat,
                users: Math.round(r.users * [0.46, 0.33, 0.16, 0.05][idx]),
            }))
        ),
        totalUsers: { unique_users: t.uniqueUsers },
    });
}

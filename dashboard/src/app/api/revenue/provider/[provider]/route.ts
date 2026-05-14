import { NextResponse, type NextRequest } from 'next/server';
import {
    revenueOverTime,
    PROVIDERS,
    PLATFORMS,
    type Granularity,
    type Provider,
} from '../../../_mock/seed';

export const dynamic = 'force-dynamic';

export async function GET(
    req: NextRequest,
    ctx: { params: Promise<{ provider: string }> }
) {
    const { provider } = await ctx.params;
    const g = (req.nextUrl.searchParams.get('g') ?? 'w') as Granularity;
    const p = provider as Provider;
    if (!PROVIDERS.includes(p)) {
        return NextResponse.json({ error: 'unknown provider' }, { status: 404 });
    }
    const rows = revenueOverTime(g).filter((r) => r.source === p);
    const totalRevenue = rows.map((r) => ({ date: r.date, revenue: r.revenue }));

    const platformBreakdown = rows.flatMap((r) =>
        PLATFORMS.map((plat, idx) => ({
            date: r.date,
            platform: plat,
            revenue: Math.round(r.revenue * [0.46, 0.33, 0.16, 0.05][idx]),
        }))
    );
    const platforms = PLATFORMS.map((plat, idx) => ({
        platform: plat,
        revenue: rows.reduce((s, r) => s + Math.round(r.revenue * [0.46, 0.33, 0.16, 0.05][idx]), 0),
    }));

    return NextResponse.json({
        provider: p,
        totalRevenue,
        platformBreakdown,
        platforms,
        chains: platforms.map((x) => ({ chain: x.platform, revenue: x.revenue })),
    });
}

import { NextResponse, type NextRequest } from 'next/server';
import { countOverTime, PROVIDERS, PLATFORMS, type Granularity, type Provider } from '../../../_mock/seed';

export const dynamic = 'force-dynamic';

export async function GET(
    req: NextRequest,
    ctx: { params: Promise<{ provider: string }> }
) {
    const { provider } = await ctx.params;
    const g = (req.nextUrl.searchParams.get('g') ?? 'w') as Granularity;
    const p = provider as Provider;
    if (!PROVIDERS.includes(p)) return NextResponse.json({ error: 'unknown provider' }, { status: 404 });
    const rows = countOverTime(g).filter((r) => r.source === p);
    return NextResponse.json({
        provider: p,
        totalCount: rows.map((r) => ({ date: r.date, count: r.count })),
        platformBreakdown: rows.flatMap((r) =>
            PLATFORMS.map((plat, idx) => ({
                date: r.date,
                platform: plat,
                count: Math.round(r.count * [0.46, 0.33, 0.16, 0.05][idx]),
            }))
        ),
        platforms: PLATFORMS.map((plat, idx) => ({
            platform: plat,
            count: rows.reduce((s, r) => s + Math.round(r.count * [0.46, 0.33, 0.16, 0.05][idx]), 0),
        })),
    });
}

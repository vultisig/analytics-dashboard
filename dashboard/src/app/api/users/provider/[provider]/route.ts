import { NextResponse, type NextRequest } from 'next/server';
import { usersOverTime, PROVIDERS, PLATFORMS, type Granularity, type Provider } from '../../../_mock/seed';

export const dynamic = 'force-dynamic';

export async function GET(
    req: NextRequest,
    ctx: { params: Promise<{ provider: string }> }
) {
    const { provider } = await ctx.params;
    const g = (req.nextUrl.searchParams.get('g') ?? 'w') as Granularity;
    const p = provider as Provider;
    if (!PROVIDERS.includes(p)) return NextResponse.json({ error: 'unknown provider' }, { status: 404 });
    const rows = usersOverTime(g).filter((r) => r.source === p);
    return NextResponse.json({
        provider: p,
        totalUsers: rows.map((r) => ({ date: r.date, users: r.users })),
        platformBreakdown: rows.flatMap((r) =>
            PLATFORMS.map((plat, idx) => ({
                date: r.date,
                platform: plat,
                users: Math.round(r.users * [0.46, 0.33, 0.16, 0.05][idx]),
            }))
        ),
        platforms: PLATFORMS.map((plat, idx) => ({
            platform: plat,
            users: rows.reduce((s, r) => s + Math.round(r.users * [0.46, 0.33, 0.16, 0.05][idx]), 0),
        })),
    });
}

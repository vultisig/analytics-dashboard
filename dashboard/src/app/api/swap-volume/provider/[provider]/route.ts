import { NextResponse, type NextRequest } from 'next/server';
import {
    volumeOverTime,
    platformBreakdownOverTime,
    PROVIDERS,
    PLATFORMS,
    topPaths,
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

    const all = volumeOverTime(g).filter((r) => r.source === p);
    const totalVolume = all.map((r) => ({ date: r.date, volume: r.volume, count: Math.round(r.volume / 11_000) }));
    const platformBreakdown = platformBreakdownOverTime(g, p);

    const platforms = PLATFORMS.map((plat) => ({
        platform: plat,
        volume: platformBreakdown.filter((r) => r.platform === plat).reduce((s, r) => s + r.volume, 0),
    }));

    return NextResponse.json({
        provider: p,
        totalVolume,
        platformBreakdown,
        platforms,
        chains: platforms.map((x) => ({ chain: x.platform, volume: x.volume })),
        topPaths: topPaths(p),
    });
}

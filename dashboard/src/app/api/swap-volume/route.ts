import { NextResponse, type NextRequest } from 'next/server';
import {
    dateAxis,
    volumeOverTime,
    volumeByProvider,
    volumeByPlatformChart,
    totals,
    topPaths,
    PLATFORMS,
    type Granularity,
} from '../_mock/seed';

export const dynamic = 'force-dynamic';

export function GET(req: NextRequest) {
    const g = (req.nextUrl.searchParams.get('g') ?? 'w') as Granularity;
    const rows = volumeOverTime(g);
    const t = totals(g);
    const byProv = volumeByProvider(g);

    // volumeByPlatformOverTime: { date, platform, volume }[]
    const platformChart = volumeByPlatformChart(g);
    const volumeByPlatformOverTime: { date: string; platform: string; volume: number }[] = [];
    for (const row of platformChart) {
        for (const plat of PLATFORMS) {
            volumeByPlatformOverTime.push({
                date: row.date as string,
                platform: plat,
                volume: Number(row[plat] ?? 0),
            });
        }
    }

    const paths = topPaths().map((p, i) => ({
        swap_path: p.path,
        token_in: p.path.split(' → ')[0],
        token_out: p.path.split(' → ')[1],
        source: i % 4 === 1 ? 'mayachain' : 'thorchain',
        total_volume: p.volume,
        swap_count: p.count,
    }));

    return NextResponse.json({
        volumeOverTime: rows,
        volumeByPlatformOverTime,
        volumeByProvider: byProv,
        globalStats: {
            total_volume: t.totalVolume,
            total_swaps: t.totalSwaps,
            unique_users: t.uniqueUsers,
        },
        topPaths: paths,
        dateAxis: dateAxis(g),
    });
}

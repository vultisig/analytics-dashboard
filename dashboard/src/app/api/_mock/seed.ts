/**
 * Deterministic mock-data seeder for the analytics dashboard.
 *
 * Enable via `MOCK_API=true` in `.env.local`. The next.config rewrite
 * is bypassed and these route handlers respond instead.
 *
 * All values are derived from a small LCG seeded on the week index so
 * the same week produces the same bar height across reloads — the UI
 * looks alive without flicker.
 */

export type Granularity = 'h' | 'd' | 'w' | 'm';

export const PROVIDERS = ['thorchain', 'mayachain', '1inch', 'lifi'] as const;
export type Provider = (typeof PROVIDERS)[number];

export const PLATFORMS = ['iOS', 'Android', 'Web', 'Other'] as const;
export type Platform = (typeof PLATFORMS)[number];

/** Per-provider share of total volume (sums to ~1). */
const PROVIDER_VOLUME_SHARE: Record<Provider, number> = {
    thorchain: 0.93,
    mayachain: 0.06,
    '1inch': 0.005,
    lifi: 0.005,
};

/** Per-provider revenue share — small variations from volume share. */
const PROVIDER_REVENUE_SHARE: Record<Provider, number> = {
    thorchain: 0.88,
    mayachain: 0.085,
    '1inch': 0.02,
    lifi: 0.015,
};

const PLATFORM_SHARE: Record<Platform, number> = {
    iOS: 0.46,
    Android: 0.33,
    Web: 0.16,
    Other: 0.05,
};

// Top swap-paths per provider (memo-style).
const TOP_PATHS: Record<Provider, { path: string; volume: number; count: number }[]> = {
    thorchain: [
        { path: 'BTC.BTC → THOR.RUNE', volume: 25_300_000, count: 2_410 },
        { path: 'ETH.ETH → THOR.RUNE', volume: 9_540_000, count: 980 },
        { path: 'THOR.RUNE → BTC.BTC', volume: 18_700_000, count: 1_620 },
        { path: 'THOR.RUJI → THOR.RUNE', volume: 2_770_000, count: 410 },
        { path: 'ETH.USDT → THOR.RUNE', volume: 5_840_000, count: 620 },
        { path: 'BSC.BNB → THOR.RUNE', volume: 5_820_000, count: 510 },
        { path: 'ETH.USDC → THOR.RUNE', volume: 6_310_000, count: 670 },
        { path: 'THOR.TCY → THOR.RUNE', volume: 710_000, count: 88 },
        { path: 'LTC.LTC → THOR.RUNE', volume: 470_000, count: 73 },
        { path: 'DOGE.DOGE → THOR.RUNE', volume: 790_000, count: 140 },
    ],
    mayachain: [
        { path: 'BTC.BTC → MAYA.CACAO', volume: 1_840_000, count: 220 },
        { path: 'ETH.ETH → MAYA.CACAO', volume: 1_290_000, count: 160 },
        { path: 'MAYA.CACAO → BTC.BTC', volume: 1_410_000, count: 175 },
        { path: 'MAYA.CACAO → ETH.ETH', volume: 720_000, count: 91 },
        { path: 'ARB.ETH → MAYA.CACAO', volume: 90_000, count: 18 },
        { path: 'KUJI.KUJI → MAYA.CACAO', volume: 42_000, count: 9 },
    ],
    '1inch': [
        { path: 'USDC → USDT (ETH)', volume: 96_000, count: 28 },
        { path: 'WETH → USDC (ETH)', volume: 71_000, count: 22 },
        { path: 'USDT → WBTC (ETH)', volume: 36_000, count: 11 },
    ],
    lifi: [
        { path: 'USDC.ETH → USDC.BASE', volume: 88_000, count: 31 },
        { path: 'USDC.ETH → USDT.ARB', volume: 64_000, count: 24 },
        { path: 'USDT.OP → USDC.BASE', volume: 41_000, count: 19 },
    ],
};

// ----- Deterministic PRNG -----------------------------------------------
function mulberry32(seed: number) {
    let s = seed | 0;
    return function () {
        s = (s + 0x6D2B79F5) | 0;
        let t = s;
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

function jitter(rand: () => number, base: number, spread = 0.4) {
    // ±spread around base, lower bounded at 5 % of base.
    const v = base * (1 + (rand() - 0.5) * spread * 2);
    return Math.max(base * 0.05, v);
}

// ----- Date axis --------------------------------------------------------
const TODAY = new Date('2026-05-14T00:00:00Z');
const HISTORY_WEEKS = 78; // ≈ 18 months

function granularityToPeriods(g: Granularity): number {
    switch (g) {
        case 'h': return 24;
        case 'd': return 30;
        case 'w': return 26;
        case 'm': return 12;
    }
}

function granularityStep(g: Granularity, i: number): Date {
    const out = new Date(TODAY);
    switch (g) {
        case 'h': out.setUTCHours(out.getUTCHours() - i); break;
        case 'd': out.setUTCDate(out.getUTCDate() - i); break;
        case 'w': out.setUTCDate(out.getUTCDate() - i * 7); break;
        case 'm': out.setUTCMonth(out.getUTCMonth() - i); break;
    }
    return out;
}

export function dateAxis(g: Granularity): string[] {
    const n = granularityToPeriods(g);
    const axis: string[] = [];
    for (let i = n - 1; i >= 0; i--) {
        axis.push(granularityStep(g, i).toISOString().slice(0, 10));
    }
    return axis;
}

// ----- Time-series generators ------------------------------------------
function trendCurve(rand: () => number, i: number, n: number, peakAt = 0.65) {
    // Smooth hump shape with a peak at peakAt, plus weekly noise.
    const x = i / Math.max(1, n - 1);
    const hump = Math.exp(-Math.pow((x - peakAt) * 2.6, 2));
    const seasonal = 0.55 + 0.45 * hump;
    return seasonal * (0.7 + rand() * 0.6);
}

export function volumeOverTime(g: Granularity) {
    const axis = dateAxis(g);
    const rand = mulberry32(42);
    const totalAnnualBaseline = 86_000_000; // matches the Figma demo
    const periodFactor = totalAnnualBaseline / axis.length;

    const rows: { date: string; source: Provider; volume: number }[] = [];
    axis.forEach((date, i) => {
        const env = trendCurve(rand, i, axis.length);
        for (const p of PROVIDERS) {
            const base = periodFactor * PROVIDER_VOLUME_SHARE[p] * env;
            rows.push({ date, source: p, volume: Math.round(jitter(rand, base)) });
        }
    });
    return rows;
}

export function platformBreakdownOverTime(g: Granularity, provider?: Provider) {
    const axis = dateAxis(g);
    const rand = mulberry32(43 + (provider ? provider.length : 0));
    const totalAnnualBaseline = 86_000_000;
    const periodFactor = totalAnnualBaseline / axis.length;
    const providerScale = provider ? PROVIDER_VOLUME_SHARE[provider] : 1;

    const rows: { date: string; platform: Platform; volume: number }[] = [];
    axis.forEach((date, i) => {
        const env = trendCurve(rand, i, axis.length);
        for (const plat of PLATFORMS) {
            const base = periodFactor * providerScale * PLATFORM_SHARE[plat] * env;
            rows.push({ date, platform: plat, volume: Math.round(jitter(rand, base)) });
        }
    });
    return rows;
}

export function totals(g: Granularity) {
    const rows = volumeOverTime(g);
    const totalVolume = rows.reduce((s, r) => s + r.volume, 0);
    const totalSwaps = Math.round(totalVolume / 11_000); // avg swap ~$11k
    const uniqueUsers = Math.round(totalSwaps / 4.5);
    return { totalVolume, totalSwaps, uniqueUsers };
}

export function volumeByProvider(g: Granularity) {
    const rows = volumeOverTime(g);
    const byProvider: Record<string, number> = {};
    rows.forEach((r) => {
        byProvider[r.source] = (byProvider[r.source] ?? 0) + r.volume;
    });
    return Object.entries(byProvider).map(([source, total_volume]) => ({ source, total_volume }));
}

// Build a chart-friendly date-keyed table for use by the StackedBarChart.
export function volumeByPlatformChart(g: Granularity) {
    const axis = dateAxis(g);
    const rand = mulberry32(99);
    const totalAnnualBaseline = 86_000_000;
    const periodFactor = totalAnnualBaseline / axis.length;
    return axis.map((date, i) => {
        const env = trendCurve(rand, i, axis.length);
        const row: Record<string, number | string> = { date };
        for (const plat of PLATFORMS) {
            row[plat] = Math.round(jitter(rand, periodFactor * PLATFORM_SHARE[plat] * env));
        }
        return row;
    });
}

// Top swap paths across all providers for the Overview/Volume hero.
export function topPaths(provider?: Provider) {
    if (provider) {
        return TOP_PATHS[provider];
    }
    // mix the top-3 of each provider
    const merged: { path: string; volume: number; count: number }[] = [];
    for (const p of PROVIDERS) {
        merged.push(...TOP_PATHS[p].slice(0, 3));
    }
    return merged.sort((a, b) => b.volume - a.volume).slice(0, 10);
}

// ----- Revenue / Count series -----------------------------------------
export function revenueOverTime(g: Granularity) {
    const axis = dateAxis(g);
    const rand = mulberry32(7);
    const annual = 420_000;
    const periodFactor = annual / axis.length;
    const rows: { date: string; source: Provider; revenue: number }[] = [];
    axis.forEach((date, i) => {
        const env = trendCurve(rand, i, axis.length, 0.7);
        for (const p of PROVIDERS) {
            const base = periodFactor * PROVIDER_REVENUE_SHARE[p] * env;
            rows.push({ date, source: p, revenue: Math.round(jitter(rand, base, 0.5)) });
        }
    });
    return rows;
}

export function countOverTime(g: Granularity) {
    const axis = dateAxis(g);
    const rand = mulberry32(11);
    const annual = 7_800;
    const periodFactor = annual / axis.length;
    const rows: { date: string; source: Provider; count: number }[] = [];
    axis.forEach((date, i) => {
        const env = trendCurve(rand, i, axis.length, 0.6);
        for (const p of PROVIDERS) {
            const base = periodFactor * PROVIDER_VOLUME_SHARE[p] * env;
            rows.push({ date, source: p, count: Math.max(0, Math.round(jitter(rand, base, 0.5))) });
        }
    });
    return rows;
}

export function usersOverTime(g: Granularity) {
    const axis = dateAxis(g);
    const rand = mulberry32(17);
    const annual = 6_800;
    const periodFactor = annual / axis.length;
    const rows: { date: string; source: Provider; users: number }[] = [];
    axis.forEach((date, i) => {
        const env = trendCurve(rand, i, axis.length, 0.55);
        for (const p of PROVIDERS) {
            const base = periodFactor * PROVIDER_VOLUME_SHARE[p] * env;
            rows.push({ date, source: p, users: Math.max(0, Math.round(jitter(rand, base, 0.55))) });
        }
    });
    return rows;
}

// ----- Tier distribution ------------------------------------------------
export function feeTiers() {
    const tiers = [
        { tier: 'Ultimate', userCount: 12,  share: 0.20 },
        { tier: 'Diamond',  userCount: 38,  share: 0.18 },
        { tier: 'Platinum', userCount: 96,  share: 0.17 },
        { tier: 'Gold',     userCount: 215, share: 0.16 },
        { tier: 'Silver',   userCount: 540, share: 0.15 },
        { tier: 'Bronze',   userCount: 980, share: 0.14 },
    ];
    const totalUsers = tiers.reduce((s, t) => s + t.userCount, 0);
    const totalVolume = 46_500_000;
    return {
        tierDistribution: tiers.map((t) => ({
            tier: t.tier,
            userCount: t.userCount,
            totalVolume: Math.round(totalVolume * t.share),
            avgVolumePerUser: Math.round((totalVolume * t.share) / t.userCount),
        })),
        totalUsers,
        totalVolume,
    };
}

// ----- Holders ---------------------------------------------------------
export function holders() {
    const totalSupply = 100_000_000;
    const tiers = [
        { tier: 'Ultimate', count: 8,    requirement: 1_000_000, avgBalance: 1_650_000 },
        { tier: 'Diamond',  count: 24,   requirement: 250_000,   avgBalance: 360_000 },
        { tier: 'Platinum', count: 71,   requirement: 50_000,    avgBalance: 84_000 },
        { tier: 'Gold',     count: 162,  requirement: 12_500,    avgBalance: 21_000 },
        { tier: 'Silver',   count: 318,  requirement: 3_000,     avgBalance: 5_400 },
        { tier: 'Bronze',   count: 612,  requirement: 1_500,     avgBalance: 2_100 },
        { tier: 'None',     count: 1_780, requirement: 0,        avgBalance: 410 },
    ].map((t) => ({
        ...t,
        thorguardBoosted: Math.round(t.count * 0.08),
        discount: t.tier === 'Ultimate' ? 50 : t.tier === 'Diamond' ? 35 : t.tier === 'Platinum' ? 25 : t.tier === 'Gold' ? 20 : t.tier === 'Silver' ? 10 : t.tier === 'Bronze' ? 5 : 0,
    }));
    const totalHolders = tiers.reduce((s, t) => s + t.count, 0);
    const totalSupplyHeld = tiers.reduce((s, t) => s + t.count * t.avgBalance, 0);
    const thorguardHolders = tiers.reduce((s, t) => s + t.thorguardBoosted, 0);
    return { tiers, totalHolders, totalSupplyHeld, totalSupply, thorguardHolders };
}

// ----- Referrers --------------------------------------------------------
export function referrers() {
    const list = [
        { code: 'HOGA', count: 338, volume: 814_800, users: 10 },
        { code: 'JP',   count: 6,   volume: 717_100, users: 5 },
        { code: 'ZENO', count: 29,  volume: 96_030,  users: 11 },
        { code: 'VALT', count: 36,  volume: 42_705,  users: 15 },
        { code: 'ZK',   count: 13,  volume: 43_779,  users: 3 },
        { code: 'PA',   count: 6,   volume: 3_313,   users: 2 },
        { code: 'TARD', count: 4,   volume: 8_120,   users: 2 },
    ];
    return list.map((r) => ({
        referrerCode: r.code,
        referralCount: r.count,
        totalVolume: r.volume,
        totalRevenue: r.code === 'PA' ? 0 : Math.round(r.volume * 0.001),
        uniqueUsers: r.users,
    }));
}

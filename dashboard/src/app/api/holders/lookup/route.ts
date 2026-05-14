import { NextResponse, type NextRequest } from 'next/server';

export const dynamic = 'force-dynamic';

const SAMPLES: Record<string, { balance: number; tier: string; discount: number; thorguard: boolean }> = {
    'thor1demo000ultimate': { balance: 1_800_000, tier: 'Ultimate', discount: 50, thorguard: true },
    'thor1demo000diamond':  { balance: 320_000,   tier: 'Diamond',  discount: 35, thorguard: false },
    'thor1demo000gold':     { balance: 18_000,    tier: 'Gold',     discount: 20, thorguard: false },
    'thor1demo000none':     { balance: 350,       tier: 'None',     discount: 0,  thorguard: false },
};

export function GET(req: NextRequest) {
    const address = req.nextUrl.searchParams.get('address') ?? '';
    const key = address.toLowerCase();
    const hit = SAMPLES[key];
    if (!hit) {
        return NextResponse.json({
            found: false,
            address,
        });
    }
    return NextResponse.json({
        found: true,
        address,
        vultBalance: hit.balance,
        baseTier: hit.tier,
        effectiveTier: hit.thorguard && hit.tier !== 'Ultimate' ? bump(hit.tier) : hit.tier,
        discount: hit.discount,
        hasThorguard: hit.thorguard,
    });
}

function bump(tier: string) {
    const order = ['None', 'Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond', 'Ultimate'];
    const i = order.indexOf(tier);
    return order[Math.min(i + 1, order.length - 1)];
}

import { NextResponse, type NextRequest } from 'next/server';
import { referrers, dateAxis, type Granularity } from '../_mock/seed';

export const dynamic = 'force-dynamic';

export function GET(req: NextRequest) {
    const g = (req.nextUrl.searchParams.get('g') ?? 'w') as Granularity;
    const list = referrers();
    const totalFeesSaved = list.reduce((s, r) => s + r.totalVolume * 0.0035, 0);
    const totalReferrerRevenue = list.reduce((s, r) => s + r.totalRevenue, 0);
    const totalReferralCount = list.reduce((s, r) => s + r.referralCount, 0);
    const totalReferralVolume = list.reduce((s, r) => s + r.totalVolume, 0);
    const uniqueUsersWithReferrals = list.reduce((s, r) => s + r.uniqueUsers, 0);

    const sortedRev = [...list].sort((a, b) => b.totalRevenue - a.totalRevenue);
    const sortedRefs = [...list].sort((a, b) => b.uniqueUsers - a.uniqueUsers);

    const axis = dateAxis(g);
    const metricsOverTime = axis.map((date) => ({
        date,
        feesSaved: totalFeesSaved / axis.length,
        referrerRevenue: totalReferrerRevenue / axis.length,
        volume: totalReferralVolume / axis.length,
        count: Math.round(totalReferralCount / axis.length),
    }));

    return NextResponse.json({
        totalFeesSaved,
        totalReferrerRevenue,
        totalReferralCount,
        totalReferralVolume,
        uniqueUsersWithReferrals,
        leaderboardByRevenue: sortedRev,
        leaderboardByReferrals: sortedRefs,
        byProvider: [
            {
                provider: 'thorchain',
                feesSaved: totalFeesSaved * 0.98,
                referrerRevenue: totalReferrerRevenue * 0.99,
                referralCount: Math.round(totalReferralCount * 0.99),
                uniqueUsers: Math.round(uniqueUsersWithReferrals * 0.97),
                totalVolume: totalReferralVolume * 0.99,
            },
            {
                provider: 'mayachain',
                feesSaved: totalFeesSaved * 0.02,
                referrerRevenue: totalReferrerRevenue * 0.01,
                referralCount: Math.round(totalReferralCount * 0.01),
                uniqueUsers: Math.round(uniqueUsersWithReferrals * 0.03),
                totalVolume: totalReferralVolume * 0.01,
            },
        ],
        metricsOverTime,
    });
}

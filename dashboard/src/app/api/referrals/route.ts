import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';
import { getParam, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

export const dynamic = 'force-dynamic';

// Vultisig affiliate codes
const VULTISIG_CODES = ['vi', 'va', 'v0'];

// Map short granularity values to SQL values
const GRAN_TO_SQL: Record<string, string> = {
  [SHORT_VALUES.GRAN_HOUR]: 'hour',
  [SHORT_VALUES.GRAN_DAY]: 'day',
  [SHORT_VALUES.GRAN_WEEK]: 'week',
  [SHORT_VALUES.GRAN_MONTH]: 'month',
};

// Map short range values to time ranges
const RANGE_TO_SQL: Record<string, string> = {
  [SHORT_VALUES.RANGE_1D]: '24h',
  [SHORT_VALUES.RANGE_7D]: '7d',
  [SHORT_VALUES.RANGE_30D]: '30d',
  [SHORT_VALUES.RANGE_90D]: '90d',
  [SHORT_VALUES.RANGE_YTD]: 'ytd',
  [SHORT_VALUES.RANGE_1Y]: '365d',
  [SHORT_VALUES.RANGE_ALL]: 'all',
  [SHORT_VALUES.RANGE_CUSTOM]: 'custom',
};

// Base filter for referral transactions:
// - Only THORChain/MAYAChain
// - Has multiple affiliates (array length > 1)
// - Contains at least one Vultisig code
// - First affiliate is NOT a Vultisig code (it's the referrer)
const REFERRAL_BASE_FILTER = `
  source IN ('thorchain', 'mayachain')
  AND affiliate_addresses IS NOT NULL
  AND array_length(affiliate_addresses, 1) > 1
  AND affiliate_addresses && ARRAY['vi', 'va', 'v0']::text[]
  AND affiliate_addresses[1] NOT IN ('vi', 'va', 'v0')
`;

export async function GET(request: NextRequest) {
  const client = await pool.connect();

  try {
    // Get parameters
    const searchParams = request.nextUrl.searchParams;
    const granularityShort = getParam(searchParams, SHORT_PARAMS.GRANULARITY) || SHORT_VALUES.GRAN_DAY;
    const rangeShort = getParam(searchParams, SHORT_PARAMS.RANGE) || SHORT_VALUES.RANGE_ALL;
    const startDateParam = getParam(searchParams, SHORT_PARAMS.START_DATE);
    const endDateParam = getParam(searchParams, SHORT_PARAMS.END_DATE);

    // Convert to SQL values
    const granularity = GRAN_TO_SQL[granularityShort] || 'day';
    const range = RANGE_TO_SQL[rangeShort] || 'all';

    // Map granularity
    const granularityMap: { [key: string]: string } = {
      'hour': 'hour',
      'day': 'day',
      'week': 'week',
      'month': 'month'
    };
    const dateTruncParam = granularityMap[granularity] || 'day';

    // For hourly granularity, use timestamp field
    const dateField = dateTruncParam === 'hour' ? 'timestamp' : 'date_only';

    // Calculate date range filter
    let dateFilter = '';
    const now = new Date();

    if (range === 'custom' && startDateParam && endDateParam) {
      dateFilter = `AND date_only >= '${startDateParam}'::date AND date_only <= '${endDateParam}'::date`;
    } else if (range === '24h') {
      dateFilter = `AND timestamp >= NOW() - INTERVAL '24 hours'`;
    } else if (range === '7d') {
      const startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      dateFilter = `AND date_only >= '${startDate}'::date`;
    } else if (range === '30d') {
      const startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      dateFilter = `AND date_only >= '${startDate}'::date`;
    } else if (range === '90d') {
      const startDate = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      dateFilter = `AND date_only >= '${startDate}'::date`;
    } else if (range === 'ytd') {
      const yearStart = `${now.getFullYear()}-01-01`;
      dateFilter = `AND date_only >= '${yearStart}'::date`;
    } else if (range === '365d') {
      const startDate = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      dateFilter = `AND date_only >= '${startDate}'::date`;
    }
    // For 'all' range, no filter is applied

    // 1. Total Fees Saved and Referrer Revenue (Hero Metrics)
    // Fees Saved = volume × (50 - sum_of_all_affiliate_bps) / 10000
    // Referrer Revenue = volume × referrer_bps / 10000 (first BPS value)
    const heroMetricsQuery = `
      SELECT
        COALESCE(SUM(
          in_amount_usd * GREATEST(0, 50 - COALESCE((SELECT SUM(x) FROM unnest(affiliate_fees_bps) AS x), 0)) / 10000
        ), 0) as total_fees_saved,
        COALESCE(SUM(
          in_amount_usd * COALESCE(affiliate_fees_bps[1], 0) / 10000
        ), 0) as total_referrer_revenue,
        COUNT(*) as total_referral_count,
        COALESCE(SUM(in_amount_usd), 0) as total_referral_volume,
        COUNT(DISTINCT user_address) as unique_users_with_referrals
      FROM swaps
      WHERE ${REFERRAL_BASE_FILTER}
        ${dateFilter}
    `;

    const heroMetricsRes = await client.query(heroMetricsQuery);
    const heroMetrics = heroMetricsRes.rows[0];

    // 2. Referral Metrics Over Time
    const metricsOverTimeQuery = `
      SELECT
        to_char(date_trunc('${dateTruncParam}', ${dateField}), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
        COALESCE(SUM(
          in_amount_usd * GREATEST(0, 50 - COALESCE((SELECT SUM(x) FROM unnest(affiliate_fees_bps) AS x), 0)) / 10000
        ), 0) as fees_saved,
        COALESCE(SUM(
          in_amount_usd * COALESCE(affiliate_fees_bps[1], 0) / 10000
        ), 0) as referrer_revenue,
        COALESCE(SUM(in_amount_usd), 0) as volume,
        COUNT(*) as count
      FROM swaps
      WHERE ${REFERRAL_BASE_FILTER}
        ${dateFilter}
      GROUP BY 1
      ORDER BY 1 ASC
    `;

    const metricsOverTimeRes = await client.query(metricsOverTimeQuery);
    const metricsOverTime = metricsOverTimeRes.rows;

    // 3. Leaderboard by Revenue (case-insensitive grouping)
    const leaderboardRevenueQuery = `
      SELECT
        UPPER(affiliate_addresses[1]) as referrer_code,
        COALESCE(SUM(in_amount_usd * COALESCE(affiliate_fees_bps[1], 0) / 10000), 0) as total_revenue,
        COUNT(DISTINCT user_address) as unique_users,
        COUNT(*) as referral_count,
        COALESCE(SUM(in_amount_usd), 0) as total_volume
      FROM swaps
      WHERE ${REFERRAL_BASE_FILTER}
        ${dateFilter}
      GROUP BY UPPER(affiliate_addresses[1])
      ORDER BY total_revenue DESC
      LIMIT 50
    `;

    const leaderboardRevenueRes = await client.query(leaderboardRevenueQuery);
    const leaderboardByRevenue = leaderboardRevenueRes.rows;

    // 4. Leaderboard by Unique Users (Referrals) (case-insensitive grouping)
    const leaderboardReferralsQuery = `
      SELECT
        UPPER(affiliate_addresses[1]) as referrer_code,
        COUNT(DISTINCT user_address) as unique_users,
        COALESCE(SUM(in_amount_usd * COALESCE(affiliate_fees_bps[1], 0) / 10000), 0) as total_revenue,
        COUNT(*) as referral_count,
        COALESCE(SUM(in_amount_usd), 0) as total_volume
      FROM swaps
      WHERE ${REFERRAL_BASE_FILTER}
        ${dateFilter}
      GROUP BY UPPER(affiliate_addresses[1])
      ORDER BY unique_users DESC
      LIMIT 50
    `;

    const leaderboardReferralsRes = await client.query(leaderboardReferralsQuery);
    const leaderboardByReferrals = leaderboardReferralsRes.rows;

    // 5. Breakdown by Provider (THORChain vs MAYAChain)
    const byProviderQuery = `
      SELECT
        source as provider,
        COALESCE(SUM(
          in_amount_usd * GREATEST(0, 50 - COALESCE((SELECT SUM(x) FROM unnest(affiliate_fees_bps) AS x), 0)) / 10000
        ), 0) as fees_saved,
        COALESCE(SUM(
          in_amount_usd * COALESCE(affiliate_fees_bps[1], 0) / 10000
        ), 0) as referrer_revenue,
        COUNT(*) as referral_count,
        COUNT(DISTINCT user_address) as unique_users,
        COALESCE(SUM(in_amount_usd), 0) as total_volume
      FROM swaps
      WHERE ${REFERRAL_BASE_FILTER}
        ${dateFilter}
      GROUP BY source
      ORDER BY referrer_revenue DESC
    `;

    const byProviderRes = await client.query(byProviderQuery);
    const byProvider = byProviderRes.rows;

    return NextResponse.json({
      // Hero metrics
      totalFeesSaved: parseFloat(heroMetrics.total_fees_saved) || 0,
      totalReferrerRevenue: parseFloat(heroMetrics.total_referrer_revenue) || 0,
      totalReferralCount: parseInt(heroMetrics.total_referral_count) || 0,
      totalReferralVolume: parseFloat(heroMetrics.total_referral_volume) || 0,
      uniqueUsersWithReferrals: parseInt(heroMetrics.unique_users_with_referrals) || 0,

      // Over time data
      metricsOverTime: metricsOverTime.map(row => ({
        date: row.date,
        feesSaved: parseFloat(row.fees_saved) || 0,
        referrerRevenue: parseFloat(row.referrer_revenue) || 0,
        volume: parseFloat(row.volume) || 0,
        count: parseInt(row.count) || 0,
      })),

      // Leaderboards
      leaderboardByRevenue: leaderboardByRevenue.map(row => ({
        referrerCode: row.referrer_code,
        totalRevenue: parseFloat(row.total_revenue) || 0,
        uniqueUsers: parseInt(row.unique_users) || 0,
        referralCount: parseInt(row.referral_count) || 0,
        totalVolume: parseFloat(row.total_volume) || 0,
      })),

      leaderboardByReferrals: leaderboardReferralsRes.rows.map(row => ({
        referrerCode: row.referrer_code,
        uniqueUsers: parseInt(row.unique_users) || 0,
        totalRevenue: parseFloat(row.total_revenue) || 0,
        referralCount: parseInt(row.referral_count) || 0,
        totalVolume: parseFloat(row.total_volume) || 0,
      })),

      // Provider breakdown
      byProvider: byProvider.map(row => ({
        provider: row.provider,
        feesSaved: parseFloat(row.fees_saved) || 0,
        referrerRevenue: parseFloat(row.referrer_revenue) || 0,
        referralCount: parseInt(row.referral_count) || 0,
        uniqueUsers: parseInt(row.unique_users) || 0,
        totalVolume: parseFloat(row.total_volume) || 0,
      })),
    });

  } catch (error) {
    console.error('=== Referrals API Error ===');
    console.error('Error message:', error instanceof Error ? error.message : String(error));
    console.error('Stack trace:', error instanceof Error ? error.stack : 'No stack trace');
    return NextResponse.json({ error: 'Failed to fetch referrals data' }, { status: 500 });
  } finally {
    client.release();
  }
}

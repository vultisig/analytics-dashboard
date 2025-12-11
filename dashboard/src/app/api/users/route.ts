import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';
import { getParam, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

export const dynamic = 'force-dynamic';

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

export async function GET(request: NextRequest) {
  const client = await pool.connect();

  try {
    // Get parameters (handles both short and legacy long formats)
    const searchParams = request.nextUrl.searchParams;
    const granularityShort = getParam(searchParams, SHORT_PARAMS.GRANULARITY) || SHORT_VALUES.GRAN_DAY;
    const rangeShort = getParam(searchParams, SHORT_PARAMS.RANGE) || SHORT_VALUES.RANGE_ALL;
    const startDateParam = getParam(searchParams, SHORT_PARAMS.START_DATE);
    const endDateParam = getParam(searchParams, SHORT_PARAMS.END_DATE);

    // Convert to SQL values
    const granularity = GRAN_TO_SQL[granularityShort] || 'day';
    const range = RANGE_TO_SQL[rangeShort] || 'all';

    // Validate and map granularity
    const granularityMap: { [key: string]: string } = {
      'hour': 'hour',
      'day': 'day',
      'week': 'week',
      'month': 'month'
    };
    const dateTruncParam = granularityMap[granularity] || 'day';

    // For hourly granularity, we MUST use timestamp field (not date_only)
    const dateFieldSwaps = dateTruncParam === 'hour' ? 'timestamp' : 'date_only';

    // Calculate date range filter
    let dateFilter = '';
    let dateFilterArkham = '';
    const now = new Date();

    if (range === 'custom' && startDateParam && endDateParam) {
      dateFilter = `AND date_only >= '${startDateParam}'::date AND date_only <= '${endDateParam}'::date`;
      dateFilterArkham = `AND DATE(timestamp) >= '${startDateParam}'::date AND DATE(timestamp) <= '${endDateParam}'::date`;
    } else if (range === '24h') {
      dateFilter = `AND timestamp >= NOW() - INTERVAL '24 hours'`;
      dateFilterArkham = `AND timestamp >= NOW() - INTERVAL '24 hours'`;
    } else if (range === '7d') {
      const startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      dateFilter = `AND date_only >= '${startDate}'::date`;
      dateFilterArkham = `AND DATE(timestamp) >= '${startDate}'::date`;
    } else if (range === '30d') {
      const startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      dateFilter = `AND date_only >= '${startDate}'::date`;
      dateFilterArkham = `AND DATE(timestamp) >= '${startDate}'::date`;
    } else if (range === '90d') {
      const startDate = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      dateFilter = `AND date_only >= '${startDate}'::date`;
      dateFilterArkham = `AND DATE(timestamp) >= '${startDate}'::date`;
    } else if (range === 'ytd') {
      const yearStart = `${now.getFullYear()}-01-01`;
      dateFilter = `AND date_only >= '${yearStart}'::date`;
      dateFilterArkham = `AND DATE(timestamp) >= '${yearStart}'::date`;
    } else if (range === '365d') {
      const startDate = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      dateFilter = `AND date_only >= '${startDate}'::date`;
      dateFilterArkham = `AND DATE(timestamp) >= '${startDate}'::date`;
    }
    // For 'all' range, no filter is applied

    // 1. Total Unique Swappers (filtered by date range - including 1inch from dex_aggregator_revenue)
    const totalUsersQuery = `
      SELECT COUNT(DISTINCT user_address) as unique_users
      FROM (
        SELECT user_address FROM swaps WHERE 1=1 ${dateFilter}
        UNION
        SELECT from_address as user_address FROM dex_aggregator_revenue
        WHERE 1=1
          AND token_in_symbol IS NOT NULL
          AND token_out_symbol IS NOT NULL
          ${dateFilterArkham}
      ) combined_users
    `;
    const totalUsers = await client.query(totalUsersQuery);

    // 2. Swappers by Provider (Over Time) - use granularity parameter and apply date filter
    // For hourly, use timestamp; otherwise use date_only/DATE(timestamp)
    const usersOverTimeQuery = dateTruncParam === 'hour'
      ? `
        SELECT
          to_char(date_trunc('hour', date), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          source,
          COUNT(DISTINCT user_address) as users
        FROM (
          SELECT timestamp as date, source, user_address FROM swaps WHERE 1=1 ${dateFilter}
          UNION ALL
          SELECT timestamp as date, '1inch' as source, from_address as user_address
          FROM dex_aggregator_revenue
          WHERE 1=1
            AND token_in_symbol IS NOT NULL
            AND token_out_symbol IS NOT NULL
            ${dateFilterArkham}
        ) combined_swaps
        GROUP BY 1, 2
        ORDER BY 1 ASC
      `
      : `
        SELECT
          to_char(date_trunc('${dateTruncParam}', date), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          source,
          COUNT(DISTINCT user_address) as users
        FROM (
          SELECT date_only as date, source, user_address FROM swaps WHERE 1=1 ${dateFilter}
          UNION ALL
          SELECT DATE(timestamp) as date, '1inch' as source, from_address as user_address
          FROM dex_aggregator_revenue
          WHERE 1=1
            AND token_in_symbol IS NOT NULL
            AND token_out_symbol IS NOT NULL
            ${dateFilterArkham}
        ) combined_swaps
        GROUP BY 1, 2
        ORDER BY 1 ASC
      `;
    const usersOverTime = await client.query(usersOverTimeQuery);

    // 3. Swappers by Platform - filtered by date range
    const usersByPlatformQuery = `
      SELECT
        COALESCE(
          platform,
          raw_data->'metadata'->'swap'->>'affiliateAddress',
          raw_data->'metadata'->>'integrator',
          'Unknown'
        ) as name,
        COUNT(DISTINCT user_address) as value
      FROM swaps
      WHERE 1=1 ${dateFilter}
      GROUP BY 1
      ORDER BY value DESC
    `;
    const usersByPlatform = await client.query(usersByPlatformQuery);

    // 3b. Swap Count by Platform - filtered by date range
    const swapCountByPlatformQuery = `
      SELECT
        COALESCE(
          platform,
          raw_data->'metadata'->'swap'->>'affiliateAddress',
          raw_data->'metadata'->>'integrator',
          'Unknown'
        ) as name,
        COUNT(*) as value
      FROM swaps
      WHERE 1=1 ${dateFilter}
      GROUP BY 1
      ORDER BY value DESC
    `;
    const swapCountByPlatform = await client.query(swapCountByPlatformQuery);

    // 3c. Users by Platform Over Time (excludes 1inch which has no platform data)
    const usersByPlatformOverTimeQuery = `
      SELECT
        to_char(date_trunc('${dateTruncParam}', ${dateFieldSwaps}), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
        CASE
          WHEN LOWER(COALESCE(platform, '')) LIKE '%android%' THEN 'Android'
          WHEN LOWER(COALESCE(platform, '')) LIKE '%ios%' THEN 'iOS'
          WHEN LOWER(COALESCE(platform, '')) LIKE '%web%' THEN 'Web'
          ELSE 'Other'
        END as platform,
        COUNT(DISTINCT user_address) as users
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
      GROUP BY 1, 2
      ORDER BY 1 ASC
    `;
    const usersByPlatformOverTime = await client.query(usersByPlatformOverTimeQuery);

    // 3d. Total Users by Platform (normalized)
    const usersByPlatformNormalizedQuery = `
      SELECT
        CASE
          WHEN LOWER(COALESCE(platform, '')) LIKE '%android%' THEN 'Android'
          WHEN LOWER(COALESCE(platform, '')) LIKE '%ios%' THEN 'iOS'
          WHEN LOWER(COALESCE(platform, '')) LIKE '%web%' THEN 'Web'
          ELSE 'Other'
        END as platform,
        COUNT(DISTINCT user_address) as total_users
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
      GROUP BY 1
      ORDER BY total_users DESC
    `;
    const usersByPlatformNormalized = await client.query(usersByPlatformNormalizedQuery);

    // 4. Swappers by Provider (Total) - filtered by date range
    const usersByProviderQuery = `
      SELECT
        source as name,
        COUNT(DISTINCT user_address) as value
      FROM (
        SELECT source, user_address FROM swaps WHERE 1=1 ${dateFilter}
        UNION ALL
        SELECT '1inch' as source, from_address as user_address
        FROM dex_aggregator_revenue WHERE 1=1 ${dateFilterArkham}
      ) combined_provider_users
      GROUP BY source
      ORDER BY value DESC
    `;
    const usersByProvider = await client.query(usersByProviderQuery);

    // 5. Swap Count by Provider - filtered by date range
    const swapCountByProviderQuery = `
      SELECT
        source as name,
        COUNT(*) as value
      FROM (
        SELECT source FROM swaps WHERE 1=1 ${dateFilter}
        UNION ALL
        SELECT '1inch' as source FROM dex_aggregator_revenue WHERE 1=1 ${dateFilterArkham}
      ) combined_swap_counts
      GROUP BY source
      ORDER BY value DESC
    `;
    const swapCountByProvider = await client.query(swapCountByProviderQuery);

    // 6. New Users Over Time - users whose FIRST EVER transaction was in each period
    // This finds each user's first appearance across ALL history, then groups by that first date and source
    const newUsersOverTimeQuery = dateTruncParam === 'hour'
      ? `
        WITH first_appearances AS (
          SELECT
            user_address,
            MIN(date) as first_date,
            (ARRAY_AGG(source ORDER BY date))[1] as first_source
          FROM (
            SELECT timestamp as date, source, user_address FROM swaps
            UNION ALL
            SELECT timestamp as date, '1inch' as source, from_address as user_address
            FROM dex_aggregator_revenue
            WHERE token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL
          ) all_swaps
          GROUP BY user_address
        )
        SELECT
          to_char(date_trunc('hour', first_date), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          first_source as source,
          COUNT(*) as users
        FROM first_appearances
        WHERE 1=1 ${dateFilter ? `AND first_date >= (SELECT MIN(timestamp) FROM swaps WHERE 1=1 ${dateFilter})` : ''}
        GROUP BY 1, 2
        ORDER BY 1 ASC
      `
      : `
        WITH first_appearances AS (
          SELECT
            user_address,
            MIN(date) as first_date,
            (ARRAY_AGG(source ORDER BY date))[1] as first_source
          FROM (
            SELECT date_only as date, source, user_address FROM swaps
            UNION ALL
            SELECT DATE(timestamp) as date, '1inch' as source, from_address as user_address
            FROM dex_aggregator_revenue
            WHERE token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL
          ) all_swaps
          GROUP BY user_address
        )
        SELECT
          to_char(date_trunc('${dateTruncParam}', first_date), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          first_source as source,
          COUNT(*) as users
        FROM first_appearances
        WHERE 1=1 ${dateFilter ? `AND first_date >= (SELECT MIN(date_only) FROM swaps WHERE 1=1 ${dateFilter})` : ''}
        GROUP BY 1, 2
        ORDER BY 1 ASC
      `;
    const newUsersOverTime = await client.query(newUsersOverTimeQuery);

    return NextResponse.json({
      totalUsers: totalUsers.rows[0],
      usersOverTime: usersOverTime.rows,
      newUsersOverTime: newUsersOverTime.rows,
      usersByPlatform: usersByPlatform.rows,
      swapCountByPlatform: swapCountByPlatform.rows,
      usersByProvider: usersByProvider.rows,
      swapCountByProvider: swapCountByProvider.rows,
      usersByPlatformOverTime: usersByPlatformOverTime.rows,
      usersByPlatformNormalized: usersByPlatformNormalized.rows
    });

  } catch (error) {
    console.error('Error fetching users data:', error);
    return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 });
  } finally {
    client.release();
  }
}

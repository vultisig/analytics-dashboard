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

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ provider: string }> }
) {
  const { provider: providerParam } = await params;
  const provider = providerParam.toLowerCase();
  const client = await pool.connect();

  try {
    // Get parameters from query params (accept both short and long formats)
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

    if (provider === '1inch') {
      // Fetch 1inch data from dex_aggregator_revenue
      // Total unique users over time
      const timeSeriesQuery = `
        SELECT
          to_char(DATE_TRUNC('${dateTruncParam}', timestamp), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          COUNT(DISTINCT from_address) as users
        FROM dex_aggregator_revenue
        WHERE protocol = '1inch'
          AND token_in_symbol IS NOT NULL
          AND token_out_symbol IS NOT NULL
          ${dateFilterArkham}
        GROUP BY DATE_TRUNC('${dateTruncParam}', timestamp)
        ORDER BY DATE_TRUNC('${dateTruncParam}', timestamp) ASC
      `;

      // Users by chain (platform breakdown for 1inch)
      const chainBreakdownQuery = `
        SELECT
          to_char(DATE_TRUNC('${dateTruncParam}', timestamp), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          chain,
          COUNT(DISTINCT from_address) as users
        FROM dex_aggregator_revenue
        WHERE protocol = '1inch'
          AND chain IS NOT NULL
          AND token_in_symbol IS NOT NULL
          AND token_out_symbol IS NOT NULL
          ${dateFilterArkham}
        GROUP BY DATE_TRUNC('${dateTruncParam}', timestamp), chain
        ORDER BY DATE_TRUNC('${dateTruncParam}', timestamp) ASC
      `;

      // New users - first-ever appearance for this provider
      const newUsersQuery = `
        WITH first_appearances AS (
          SELECT
            from_address as user_address,
            MIN(timestamp) as first_date
          FROM dex_aggregator_revenue
          WHERE protocol = '1inch'
            AND token_in_symbol IS NOT NULL
            AND token_out_symbol IS NOT NULL
          GROUP BY from_address
        )
        SELECT
          to_char(DATE_TRUNC('${dateTruncParam}', first_date), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          COUNT(*) as users
        FROM first_appearances
        WHERE 1=1 ${dateFilterArkham ? `AND first_date >= (SELECT MIN(timestamp) FROM dex_aggregator_revenue WHERE protocol = '1inch' ${dateFilterArkham})` : ''}
        GROUP BY 1
        ORDER BY 1 ASC
      `;

      // New users by chain (platform breakdown) - first-ever appearance per user with their first chain
      const newUsersByPlatformQuery = `
        WITH first_appearances AS (
          SELECT
            from_address as user_address,
            MIN(timestamp) as first_date,
            (ARRAY_AGG(chain ORDER BY timestamp))[1] as first_chain
          FROM dex_aggregator_revenue
          WHERE protocol = '1inch'
            AND chain IS NOT NULL
            AND token_in_symbol IS NOT NULL
            AND token_out_symbol IS NOT NULL
          GROUP BY from_address
        )
        SELECT
          to_char(DATE_TRUNC('${dateTruncParam}', first_date), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          first_chain as chain,
          COUNT(*) as users
        FROM first_appearances
        WHERE 1=1 ${dateFilterArkham ? `AND first_date >= (SELECT MIN(timestamp) FROM dex_aggregator_revenue WHERE protocol = '1inch' ${dateFilterArkham})` : ''}
        GROUP BY 1, 2
        ORDER BY 1 ASC
      `;

      const [timeSeriesRes, chainBreakdownRes, newUsersRes, newUsersByPlatformRes] = await Promise.all([
        client.query(timeSeriesQuery),
        client.query(chainBreakdownQuery),
        client.query(newUsersQuery),
        client.query(newUsersByPlatformQuery)
      ]);

      return NextResponse.json({
        provider: '1inch',
        totalUsers: timeSeriesRes.rows,
        platformBreakdown: chainBreakdownRes.rows,
        newUsers: newUsersRes.rows,
        newUsersByPlatform: newUsersByPlatformRes.rows
      });
    } else {
      // Fetch data from swaps table for other providers
      // For hourly granularity, use timestamp field; otherwise use date_only
      const dateField = dateTruncParam === 'hour' ? 'timestamp' : 'date_only';

      // Total unique users over time
      const timeSeriesQuery = `
        SELECT
          to_char(DATE_TRUNC('${dateTruncParam}', ${dateField}), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          COUNT(DISTINCT user_address) as users
        FROM swaps
        WHERE source = $1
          ${dateFilter}
        GROUP BY DATE_TRUNC('${dateTruncParam}', ${dateField})
        ORDER BY DATE_TRUNC('${dateTruncParam}', ${dateField}) ASC
      `;

      // Users by platform breakdown
      let platformExpression = "COALESCE(platform, 'Unknown')";
      if (provider === 'thorchain' || provider === 'mayachain') {
        platformExpression = "COALESCE(platform, raw_data->'metadata'->'swap'->>'affiliateAddress', 'Unknown')";
      } else if (provider === 'lifi') {
        platformExpression = `
          CASE
            WHEN LOWER(COALESCE(platform, raw_data->'metadata'->>'integrator', 'Unknown')) LIKE '%android%' THEN 'Android'
            WHEN LOWER(COALESCE(platform, raw_data->'metadata'->>'integrator', 'Unknown')) LIKE '%ios%' THEN 'iOS'
            ELSE COALESCE(platform, raw_data->'metadata'->>'integrator', 'Unknown')
          END
        `;
      }

      const platformBreakdownQuery = `
        SELECT
          to_char(DATE_TRUNC('${dateTruncParam}', ${dateField}), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          ${platformExpression} as platform,
          COUNT(DISTINCT user_address) as users
        FROM swaps
        WHERE source = $1
          ${dateFilter}
        GROUP BY DATE_TRUNC('${dateTruncParam}', ${dateField}), ${platformExpression}
        ORDER BY DATE_TRUNC('${dateTruncParam}', ${dateField}) ASC
      `;

      // New users - first-ever appearance for this provider
      const newUsersQuery = dateTruncParam === 'hour'
        ? `
          WITH first_appearances AS (
            SELECT
              user_address,
              MIN(timestamp) as first_date
            FROM swaps
            WHERE source = $1
            GROUP BY user_address
          )
          SELECT
            to_char(DATE_TRUNC('hour', first_date), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
            COUNT(*) as users
          FROM first_appearances
          WHERE 1=1 ${dateFilter ? `AND first_date >= (SELECT MIN(timestamp) FROM swaps WHERE source = $1 ${dateFilter})` : ''}
          GROUP BY 1
          ORDER BY 1 ASC
        `
        : `
          WITH first_appearances AS (
            SELECT
              user_address,
              MIN(date_only) as first_date
            FROM swaps
            WHERE source = $1
            GROUP BY user_address
          )
          SELECT
            to_char(DATE_TRUNC('${dateTruncParam}', first_date), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
            COUNT(*) as users
          FROM first_appearances
          WHERE 1=1 ${dateFilter ? `AND first_date >= (SELECT MIN(date_only) FROM swaps WHERE source = $1 ${dateFilter})` : ''}
          GROUP BY 1
          ORDER BY 1 ASC
        `;

      // New users by platform - first-ever appearance per user with their first platform
      // Use a subquery approach to avoid ARRAY_AGG with complex expressions
      const newUsersByPlatformQuery = dateTruncParam === 'hour'
        ? `
          WITH first_dates AS (
            SELECT
              user_address,
              MIN(timestamp) as first_date
            FROM swaps
            WHERE source = $1
            GROUP BY user_address
          ),
          first_transactions AS (
            SELECT DISTINCT ON (fd.user_address)
              fd.user_address,
              fd.first_date,
              ${platformExpression} as first_platform
            FROM first_dates fd
            JOIN swaps s ON s.user_address = fd.user_address
              AND s.timestamp = fd.first_date
              AND s.source = $1
            ORDER BY fd.user_address, s.timestamp
          )
          SELECT
            to_char(DATE_TRUNC('hour', first_date), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
            first_platform as platform,
            COUNT(*) as users
          FROM first_transactions
          WHERE 1=1 ${dateFilter ? `AND first_date >= (SELECT MIN(timestamp) FROM swaps WHERE source = $1 ${dateFilter})` : ''}
          GROUP BY 1, 2
          ORDER BY 1 ASC
        `
        : `
          WITH first_dates AS (
            SELECT
              user_address,
              MIN(date_only) as first_date
            FROM swaps
            WHERE source = $1
            GROUP BY user_address
          ),
          first_transactions AS (
            SELECT DISTINCT ON (fd.user_address)
              fd.user_address,
              fd.first_date,
              ${platformExpression} as first_platform
            FROM first_dates fd
            JOIN swaps s ON s.user_address = fd.user_address
              AND s.date_only = fd.first_date
              AND s.source = $1
            ORDER BY fd.user_address, s.date_only
          )
          SELECT
            to_char(DATE_TRUNC('${dateTruncParam}', first_date), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
            first_platform as platform,
            COUNT(*) as users
          FROM first_transactions
          WHERE 1=1 ${dateFilter ? `AND first_date >= (SELECT MIN(date_only) FROM swaps WHERE source = $1 ${dateFilter})` : ''}
          GROUP BY 1, 2
          ORDER BY 1 ASC
        `;

      const [timeSeriesRes, platformBreakdownRes, newUsersRes, newUsersByPlatformRes] = await Promise.all([
        client.query(timeSeriesQuery, [provider]),
        client.query(platformBreakdownQuery, [provider]),
        client.query(newUsersQuery, [provider]),
        client.query(newUsersByPlatformQuery, [provider])
      ]);

      return NextResponse.json({
        provider,
        totalUsers: timeSeriesRes.rows,
        platformBreakdown: platformBreakdownRes.rows,
        newUsers: newUsersRes.rows,
        newUsersByPlatform: newUsersByPlatformRes.rows
      });
    }
  } catch (error) {
    console.error(`Error fetching ${provider} users data:`, error);
    return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 });
  } finally {
    client.release();
  }
}

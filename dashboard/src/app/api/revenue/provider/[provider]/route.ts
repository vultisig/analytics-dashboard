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
      // Total revenue over time
      const timeSeriesQuery = `
        SELECT
          to_char(DATE_TRUNC('${dateTruncParam}', timestamp), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          COALESCE(SUM(actual_fee_usd), 0) as revenue
        FROM dex_aggregator_revenue
        WHERE protocol = '1inch'
          AND token_in_symbol IS NOT NULL
          AND token_out_symbol IS NOT NULL
          ${dateFilterArkham}
        GROUP BY DATE_TRUNC('${dateTruncParam}', timestamp)
        ORDER BY DATE_TRUNC('${dateTruncParam}', timestamp) ASC
      `;

      // Revenue by chain (platform breakdown for 1inch)
      const chainBreakdownQuery = `
        SELECT
          to_char(DATE_TRUNC('${dateTruncParam}', timestamp), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          chain,
          COALESCE(SUM(actual_fee_usd), 0) as revenue
        FROM dex_aggregator_revenue
        WHERE protocol = '1inch'
          AND chain IS NOT NULL
          AND token_in_symbol IS NOT NULL
          AND token_out_symbol IS NOT NULL
          ${dateFilterArkham}
        GROUP BY DATE_TRUNC('${dateTruncParam}', timestamp), chain
        ORDER BY DATE_TRUNC('${dateTruncParam}', timestamp) ASC
      `;

      const [timeSeriesRes, chainBreakdownRes] = await Promise.all([
        client.query(timeSeriesQuery),
        client.query(chainBreakdownQuery)
      ]);

      return NextResponse.json({
        provider: '1inch',
        totalRevenue: timeSeriesRes.rows,
        platformBreakdown: chainBreakdownRes.rows
      });
    } else {
      // Fetch data from swaps table for other providers
      // For hourly granularity, use timestamp field; otherwise use date_only
      const dateField = dateTruncParam === 'hour' ? 'timestamp' : 'date_only';

      // Total revenue over time
      const timeSeriesQuery = `
        SELECT
          to_char(DATE_TRUNC('${dateTruncParam}', ${dateField}), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
          SUM(affiliate_fee_usd) as revenue
        FROM swaps
        WHERE source = $1
          ${dateFilter}
        GROUP BY DATE_TRUNC('${dateTruncParam}', ${dateField})
        ORDER BY DATE_TRUNC('${dateTruncParam}', ${dateField}) ASC
      `;

      // Revenue by platform breakdown
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
          SUM(affiliate_fee_usd) as revenue
        FROM swaps
        WHERE source = $1
          ${dateFilter}
        GROUP BY DATE_TRUNC('${dateTruncParam}', ${dateField}), ${platformExpression}
        ORDER BY DATE_TRUNC('${dateTruncParam}', ${dateField}) ASC
      `;

      const [timeSeriesRes, platformBreakdownRes] = await Promise.all([
        client.query(timeSeriesQuery, [provider]),
        client.query(platformBreakdownQuery, [provider])
      ]);

      return NextResponse.json({
        provider,
        totalRevenue: timeSeriesRes.rows,
        platformBreakdown: platformBreakdownRes.rows
      });
    }
  } catch (error) {
    console.error(`Error fetching ${provider} revenue data:`, error);
    return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 });
  } finally {
    client.release();
  }
}

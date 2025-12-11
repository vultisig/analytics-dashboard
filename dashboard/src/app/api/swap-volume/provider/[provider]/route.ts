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

    // For hourly granularity, we MUST use timestamp field (not date_only)
    const dateField = dateTruncParam === 'hour' ? 'timestamp' : 'date_only';

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
      // Total volume over time
      const timeSeriesQuery = `
        SELECT
          DATE_TRUNC('${dateTruncParam}', timestamp) as time_period,
          COALESCE(SUM(swap_volume_usd), 0) as volume
        FROM dex_aggregator_revenue
        WHERE protocol = '1inch'
          AND token_in_symbol IS NOT NULL
          AND token_out_symbol IS NOT NULL
          ${dateFilterArkham}
        GROUP BY time_period
        ORDER BY time_period ASC
      `;

      // Volume by chain (platform breakdown for 1inch)
      const chainBreakdownQuery = `
        SELECT
          DATE_TRUNC('${dateTruncParam}', timestamp) as time_period,
          chain,
          COALESCE(SUM(swap_volume_usd), 0) as volume
        FROM dex_aggregator_revenue
        WHERE protocol = '1inch'
          AND chain IS NOT NULL
          AND token_in_symbol IS NOT NULL
          AND token_out_symbol IS NOT NULL
          ${dateFilterArkham}
        GROUP BY time_period, chain
        ORDER BY time_period ASC
      `;

      const [timeSeriesRes, chainBreakdownRes] = await Promise.all([
        client.query(timeSeriesQuery),
        client.query(chainBreakdownQuery)
      ]);

      return NextResponse.json({
        provider: '1inch',
        totalVolume: timeSeriesRes.rows,
        platformBreakdown: chainBreakdownRes.rows
      });
    } else {
      // Fetch data from swaps table for other providers
      // Total volume over time
      const timeSeriesQuery = `
        SELECT
          DATE_TRUNC('${dateTruncParam}', ${dateField}) as time_period,
          SUM(in_amount_usd) as volume
        FROM swaps
        WHERE source = $1
          ${dateFilter}
        GROUP BY time_period
        ORDER BY time_period ASC
      `;

      // Volume by platform breakdown
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
          DATE_TRUNC('${dateTruncParam}', ${dateField}) as time_period,
          ${platformExpression} as platform,
          SUM(in_amount_usd) as volume
        FROM swaps
        WHERE source = $1
          ${dateFilter}
        GROUP BY time_period, ${platformExpression}
        ORDER BY time_period ASC
      `;

      const [timeSeriesRes, platformBreakdownRes] = await Promise.all([
        client.query(timeSeriesQuery, [provider]),
        client.query(platformBreakdownQuery, [provider])
      ]);

      return NextResponse.json({
        provider,
        totalVolume: timeSeriesRes.rows,
        platformBreakdown: platformBreakdownRes.rows
      });
    }
  } catch (error) {
    console.error(`Error fetching ${provider} volume data:`, error);
    return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 });
  } finally {
    client.release();
  }
}

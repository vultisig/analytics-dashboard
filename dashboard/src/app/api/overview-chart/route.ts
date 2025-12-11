import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';
import { getParam, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

export const dynamic = 'force-dynamic';

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

// Map short granularity values to SQL values
const GRAN_TO_SQL: Record<string, string> = {
  [SHORT_VALUES.GRAN_HOUR]: 'hour',
  [SHORT_VALUES.GRAN_DAY]: 'day',
  [SHORT_VALUES.GRAN_WEEK]: 'week',
  [SHORT_VALUES.GRAN_MONTH]: 'month',
};

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const rangeShort = getParam(searchParams, SHORT_PARAMS.RANGE) || SHORT_VALUES.RANGE_ALL;
  const range = RANGE_TO_SQL[rangeShort] || 'all';
  const startDateParam = getParam(searchParams, SHORT_PARAMS.START_DATE);
  const endDateParam = getParam(searchParams, SHORT_PARAMS.END_DATE);
  const granularityParamShort = getParam(searchParams, SHORT_PARAMS.GRANULARITY);
  const granularityParam = granularityParamShort ? GRAN_TO_SQL[granularityParamShort] : null;

  const client = await pool.connect();
  try {
    // Determine date range
    let startDate: string;
    let endDate: string;
    let timeTrunc = 'day'; // Default granularity

    if (range === 'custom' && startDateParam && endDateParam) {
      startDate = startDateParam;
      endDate = endDateParam;
    } else {
      endDate = new Date().toISOString().split('T')[0];
      const now = new Date();
      switch (range) {
        case '24h':
          startDate = new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
          break;
        case '7d':
          startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
          break;
        case '30d':
          startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
          break;
        case '90d':
          startDate = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
          timeTrunc = 'week';
          break;
        case 'all':
        default:
          startDate = '2020-01-01';
          timeTrunc = 'week';
          break;
      }
    }

    // Get granularity from params or use auto-detected
    if (granularityParam && ['hour', 'day', 'week', 'month'].includes(granularityParam)) {
      timeTrunc = granularityParam;
    }

    // Build the date format and GROUP BY clause based on granularity
    const dateFormatMap: Record<string, string> = {
      hour: "to_char(date_trunc('hour', timestamp), 'Mon DD HH24:00')",
      day: "to_char(date_trunc('day', date_only), 'Mon DD')",
      week: "to_char(date_trunc('week', date_only), 'Mon DD')",
      month: "to_char(date_trunc('month', date_only), 'Mon YYYY')"
    };

    const dateFormat = dateFormatMap[timeTrunc] || dateFormatMap.day;

    // For hourly, we MUST use timestamp, not date_only
    let dateGroupBy = `date_trunc('${timeTrunc}', date_only)`;
    if (timeTrunc === 'hour') {
      dateGroupBy = `date_trunc('hour', timestamp)`;
    }

    const dateFormatArkham = dateFormat.replace(/date_only/g, "DATE(timestamp AT TIME ZONE 'UTC')").replace(/timestamp/g, "timestamp AT TIME ZONE 'UTC'");

    // Fetch total stats (all time)
    let stats = { total_volume: 0, total_fees: 0, total_swaps: 0, unique_users: 0 };
    try {
      const swapsQuery = `
        SELECT 
          COALESCE(SUM(in_amount_usd), 0) as total_volume,
          COALESCE(SUM(affiliate_fee_usd), 0) as total_fees,
          COUNT(*) as total_swaps,
          COUNT(DISTINCT user_address) as unique_users
        FROM swaps
        WHERE source != '1inch'
      `;

      const arkhamQuery = `
        SELECT 
          COALESCE(SUM(swap_volume_usd), 0) as total_volume,
          COALESCE(SUM(actual_fee_usd), 0) as total_fees,
          COUNT(*) as total_swaps,
          COUNT(DISTINCT from_address) as unique_users
        FROM dex_aggregator_revenue
        WHERE protocol = '1inch'
          AND token_in_symbol IS NOT NULL
          AND token_out_symbol IS NOT NULL
      `;

      const [swapsRes, arkhamRes] = await Promise.all([
        client.query(swapsQuery),
        client.query(arkhamQuery)
      ]);

      const s = swapsRes.rows[0];
      const a = arkhamRes.rows[0];

      stats = {
        total_volume: parseFloat(s.total_volume) + parseFloat(a.total_volume),
        total_fees: parseFloat(s.total_fees) + parseFloat(a.total_fees),
        total_swaps: parseInt(s.total_swaps) + parseInt(a.total_swaps),
        unique_users: parseInt(s.unique_users) + parseInt(a.unique_users)
      };
    } catch (e) {
      console.error('Error fetching total stats:', e);
    }

    // Define time filter based on range
    let timeFilterSwaps = `AND date_only >= $1::date AND date_only <= $2::date`;
    // For Arkham, we cast to UTC to match swaps TIMESTAMPTZ
    let timeFilterArkham = `AND DATE(timestamp AT TIME ZONE 'UTC') >= $1::date AND DATE(timestamp AT TIME ZONE 'UTC') <= $2::date`;

    if (range === '24h') {
      timeFilterSwaps = `AND timestamp >= NOW() - INTERVAL '24 hours'`;
      // Compare UTC timestamp with NOW() (which is TIMESTAMPTZ)
      timeFilterArkham = `AND (timestamp AT TIME ZONE 'UTC') >= NOW() - INTERVAL '24 hours'`;
    }

    // For Arkham hourly, we use timestamp cast to UTC
    const dateGroupByArkhamHourly = `date_trunc('hour', timestamp AT TIME ZONE 'UTC')`;
    const dateGroupByArkham = `date_trunc('${timeTrunc}', DATE(timestamp AT TIME ZONE 'UTC'))`;

    // Fetch chart data for selected range
    const query = `
      WITH daily_data AS (
        -- Swaps (non-1inch)
        SELECT 
          ${dateGroupBy} as date_group,
          ${dateFormat} as date,
          SUM(in_amount_usd) as volume,
          SUM(affiliate_fee_usd) as revenue
        FROM swaps
        WHERE source != '1inch' 
          ${timeFilterSwaps}
        GROUP BY date_group, date
        
        UNION ALL
        
        -- Arkham (1inch)
        SELECT
          ${timeTrunc === 'hour' ? dateGroupByArkhamHourly : dateGroupByArkham} as date_group,
          ${dateFormatArkham} as date,
          SUM(swap_volume_usd) as volume,
          SUM(actual_fee_usd) as revenue
        FROM dex_aggregator_revenue
        WHERE protocol = '1inch'
          AND token_in_symbol IS NOT NULL
          AND token_out_symbol IS NOT NULL
          ${timeFilterArkham}
        GROUP BY date_group, date
      )
      SELECT 
        date,
        SUM(volume) as volume,
        SUM(revenue) as revenue
      FROM daily_data
      GROUP BY date_group, date
      ORDER BY date_group ASC
    `;

    const queryParams = (range === '24h') ? [] : [startDate, endDate];
    const res = await client.query(query, queryParams);
    const chartData = res.rows.map(row => ({
      date: row.date,
      volume: Number(row.volume),
      revenue: Number(row.revenue)
    }));

    return NextResponse.json({ stats, chartData });
  } catch (error) {
    console.error('Error fetching overview data:', error);
    return NextResponse.json({
      error: 'Failed to fetch overview data',
      stats: { total_volume: 0, total_fees: 0, total_swaps: 0, unique_users: 0 },
      chartData: []
    }, { status: 500 });
  } finally {
    client.release();
  }
}

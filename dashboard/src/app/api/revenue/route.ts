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

    // 1. Total Fee Revenue (filtered by date range)
    const swapsRevenueQuery = `
      SELECT COALESCE(SUM(affiliate_fee_usd), 0) as total_revenue
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
    `;
    const arkhamRevenueQuery = `
      SELECT COALESCE(SUM(actual_fee_usd), 0) as total_revenue
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
        AND token_in_symbol IS NOT NULL
        AND token_out_symbol IS NOT NULL
        ${dateFilterArkham}
    `;

    const [swapsTotalRes, arkhamTotalRes] = await Promise.all([
      client.query(swapsRevenueQuery),
      client.query(arkhamRevenueQuery)
    ]);

    const totalRevenueValue = parseFloat(swapsTotalRes.rows[0].total_revenue) + parseFloat(arkhamTotalRes.rows[0].total_revenue);

    const totalRevenue = {
      total_revenue: totalRevenueValue
    };

    // 2. Fee Revenue by Provider (Over Time) - use granularity parameter and apply date filter
    const swapsOverTimeQuery = `
      SELECT
        to_char(date_trunc('${dateTruncParam}', ${dateField}), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
        source,
        SUM(affiliate_fee_usd) as revenue
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
      GROUP BY 1, 2
      ORDER BY 1 ASC
    `;

    const arkhamOverTimeQuery = `
      SELECT
        to_char(date_trunc('${dateTruncParam}', timestamp), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
        '1inch' as source,
        COALESCE(SUM(actual_fee_usd), 0) as revenue
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
        AND token_in_symbol IS NOT NULL
        AND token_out_symbol IS NOT NULL
        ${dateFilterArkham}
      GROUP BY 1
      ORDER BY 1 ASC
    `;

    const [swapsOverTimeRes, arkhamOverTimeRes] = await Promise.all([
      client.query(swapsOverTimeQuery),
      client.query(arkhamOverTimeQuery)
    ]);

    // Merge and sort
    const revenueOverTime = [...swapsOverTimeRes.rows, ...arkhamOverTimeRes.rows].sort((a, b) =>
      new Date(a.date).getTime() - new Date(b.date).getTime()
    );

    // 3. Total Revenue by Provider (Pie/Metrics) - filtered by date range
    const swapsByProviderQuery = `
      SELECT
        source as name,
        SUM(affiliate_fee_usd) as value
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
      GROUP BY source
    `;

    const arkhamByProviderQuery = `
      SELECT
        '1inch' as name,
        COALESCE(SUM(actual_fee_usd), 0) as value
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
        ${dateFilterArkham}
    `;

    const [swapsByProviderRes, arkhamByProviderRes] = await Promise.all([
      client.query(swapsByProviderQuery),
      client.query(arkhamByProviderQuery)
    ]);

    const revenueByProvider = [...swapsByProviderRes.rows, ...arkhamByProviderRes.rows].sort((a, b) =>
      parseFloat(b.value) - parseFloat(a.value)
    );

    // 3b. Revenue by Platform Over Time (excludes 1inch which has no platform data)
    const platformRevenueTimeQuery = `
      SELECT
        to_char(date_trunc('${dateTruncParam}', ${dateField}), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
        CASE
          WHEN LOWER(COALESCE(platform, '')) LIKE '%android%' THEN 'Android'
          WHEN LOWER(COALESCE(platform, '')) LIKE '%ios%' THEN 'iOS'
          WHEN LOWER(COALESCE(platform, '')) LIKE '%web%' THEN 'Web'
          ELSE 'Other'
        END as platform,
        SUM(affiliate_fee_usd) as revenue
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
      GROUP BY 1, 2
      ORDER BY 1 ASC
    `;

    const platformRevenueTimeRes = await client.query(platformRevenueTimeQuery);
    const revenueByPlatformOverTime = platformRevenueTimeRes.rows;

    // 3c. Total Revenue by Platform
    const platformRevenueTotalQuery = `
      SELECT
        CASE
          WHEN LOWER(COALESCE(platform, '')) LIKE '%android%' THEN 'Android'
          WHEN LOWER(COALESCE(platform, '')) LIKE '%ios%' THEN 'iOS'
          WHEN LOWER(COALESCE(platform, '')) LIKE '%web%' THEN 'Web'
          ELSE 'Other'
        END as platform,
        SUM(affiliate_fee_usd) as total_revenue
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
      GROUP BY 1
      ORDER BY total_revenue DESC
    `;

    const platformRevenueTotalRes = await client.query(platformRevenueTotalQuery);
    const revenueByPlatform = platformRevenueTotalRes.rows;

    // 4. Top 10 Swap Paths by Provider (filtered by date range)
    // Get top 10 paths PER PROVIDER (not globally)
    const swapsPathsQuery = `
      WITH ranked_paths AS (
        SELECT
          source,
          in_asset || ' → ' || out_asset as swap_path,
          SUM(affiliate_fee_usd) as total_revenue,
          COUNT(*) as swap_count,
          ROW_NUMBER() OVER (PARTITION BY source ORDER BY SUM(affiliate_fee_usd) DESC) as rank
        FROM swaps
        WHERE source != '1inch'
          ${dateFilter}
        GROUP BY source, in_asset, out_asset
      )
      SELECT source, swap_path, total_revenue, swap_count
      FROM ranked_paths
      WHERE rank <= 10
      ORDER BY source, total_revenue DESC
    `;

    const arkhamPathsQuery = `
      SELECT
        '1inch' as source,
        token_in_symbol || ' → ' || token_out_symbol as swap_path,
        COALESCE(SUM(actual_fee_usd), 0) as total_revenue,
        COUNT(*) as swap_count
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
        AND token_in_symbol IS NOT NULL
        AND token_out_symbol IS NOT NULL
        ${dateFilterArkham}
      GROUP BY token_in_symbol, token_out_symbol
      ORDER BY total_revenue DESC
      LIMIT 10
    `;

    const [swapsPathsRes, arkhamPathsRes] = await Promise.all([
      client.query(swapsPathsQuery),
      client.query(arkhamPathsQuery)
    ]);

    const topPaths = [...swapsPathsRes.rows, ...arkhamPathsRes.rows];

    // 5. Provider Specific Data - filtered by date range
    const providersList = ['thorchain', 'mayachain', 'lifi', '1inch'];
    const providerData: any = {};

    for (const provider of providersList) {
      let nameExpression = "COALESCE(platform, 'Unknown')";

      if (provider === 'thorchain' || provider === 'mayachain') {
        nameExpression = "COALESCE(platform, raw_data->'metadata'->'swap'->>'affiliateAddress', 'Unknown')";
      } else if (provider === 'lifi') {
        nameExpression = "COALESCE(platform, raw_data->'metadata'->>'integrator', 'Unknown')";
      }

      if (provider !== '1inch') {
        // Platform Revenue
        const platformQuery = `
          SELECT
            ${nameExpression} as name,
            SUM(affiliate_fee_usd) as value
          FROM swaps
          WHERE source = $1
            ${dateFilter}
          GROUP BY 1
          ORDER BY value DESC
        `;
        const platformRes = await client.query(platformQuery, [provider]);
        providerData[provider] = { platforms: platformRes.rows };
      } else {
        // 1inch Chain Revenue (from Arkham data)
        const chainQuery = `
          SELECT
            chain as chain_id,
            SUM(actual_fee_usd) as value
          FROM dex_aggregator_revenue
          WHERE protocol = '1inch'
            AND chain IS NOT NULL
            ${dateFilterArkham}
          GROUP BY 1
          ORDER BY value DESC
        `;
        const chainRes = await client.query(chainQuery);
        providerData[provider] = { chains: chainRes.rows };
      }
    }

    return NextResponse.json({
      totalRevenue,
      revenueOverTime,
      revenueByPlatformOverTime,
      revenueByProvider,
      revenueByPlatform,
      topPaths,
      providerData
    });

  } catch (error) {
    console.error('=== Revenue API Error ===');
    console.error('Error message:', error instanceof Error ? error.message : String(error));
    console.error('Stack trace:', error instanceof Error ? error.stack : 'No stack trace');
    return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 });
  } finally {
    client.release();
  }
}

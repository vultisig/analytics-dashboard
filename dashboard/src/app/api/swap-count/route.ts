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

    // For hourly granularity, use timestamp field; otherwise use date_only
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

    // 1. Total Count (filtered by date range)
    const swapsCountQuery = `
      SELECT COUNT(*) as total_count
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
    `;
    const arkhamCountQuery = `
      SELECT COUNT(*) as total_count
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
        AND token_in_symbol IS NOT NULL
        AND token_out_symbol IS NOT NULL
        ${dateFilterArkham}
    `;

    const [swapsTotalRes, arkhamTotalRes] = await Promise.all([
      client.query(swapsCountQuery),
      client.query(arkhamCountQuery)
    ]);

    const totalCountValue = parseInt(swapsTotalRes.rows[0].total_count) + parseInt(arkhamTotalRes.rows[0].total_count);

    const totalCount = {
      total_count: totalCountValue
    };

    // 2. Count by Provider (Over Time) - use granularity parameter and apply date filter
    const swapsOverTimeQuery = `
      SELECT
        to_char(date_trunc('${dateTruncParam}', ${dateField}), 'YYYY-MM-DD"T"HH24:MI:SS') as date,
        source,
        COUNT(*) as count
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
        COUNT(*) as count
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
    const countOverTime = [...swapsOverTimeRes.rows, ...arkhamOverTimeRes.rows].sort((a, b) =>
      new Date(a.date).getTime() - new Date(b.date).getTime()
    );

    // 2b. Count by Platform Over Time (excludes 1inch - no platform data)
    const countByPlatformOverTimeQuery = `
      SELECT
        date_trunc('${dateTruncParam}', ${dateField}) as time_period,
        CASE
          WHEN LOWER(COALESCE(platform, '')) LIKE '%android%' THEN 'Android'
          WHEN LOWER(COALESCE(platform, '')) LIKE '%ios%' OR LOWER(COALESCE(platform, '')) LIKE '%iphone%' THEN 'iOS'
          WHEN LOWER(COALESCE(platform, '')) LIKE '%web%' OR LOWER(COALESCE(platform, '')) LIKE '%desktop%' THEN 'Web'
          ELSE 'Other'
        END as platform,
        COUNT(*) as count
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
      GROUP BY 1, 2
      ORDER BY 1 ASC
    `;

    const countByPlatformOverTimeRes = await client.query(countByPlatformOverTimeQuery);
    const countByPlatformOverTime = countByPlatformOverTimeRes.rows;

    // 3. Total Count by Provider (Pie/Metrics) - filtered by date range
    const swapsByProviderQuery = `
      SELECT
        source as name,
        COUNT(*) as value
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
      GROUP BY source
    `;

    const arkhamByProviderQuery = `
      SELECT
        '1inch' as name,
        COUNT(*) as value
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
        AND token_in_symbol IS NOT NULL
        AND token_out_symbol IS NOT NULL
        ${dateFilterArkham}
    `;

    const [swapsByProviderRes, arkhamByProviderRes] = await Promise.all([
      client.query(swapsByProviderQuery),
      client.query(arkhamByProviderQuery)
    ]);

    const countByProvider = [...swapsByProviderRes.rows, ...arkhamByProviderRes.rows].sort((a, b) =>
      parseInt(b.value) - parseInt(a.value)
    );

    // 4. Top 10 Swap Paths by Provider (filtered by date range)
    // Get top 10 paths PER PROVIDER (not globally)
    const swapsPathsQuery = `
      WITH ranked_paths AS (
        SELECT
          source,
          in_asset || ' → ' || out_asset as swap_path,
          SUM(in_amount_usd) as total_volume,
          COUNT(*) as swap_count,
          ROW_NUMBER() OVER (PARTITION BY source ORDER BY COUNT(*) DESC) as rank
        FROM swaps
        WHERE source != '1inch'
          ${dateFilter}
        GROUP BY source, in_asset, out_asset
      )
      SELECT source, swap_path, total_volume, swap_count
      FROM ranked_paths
      WHERE rank <= 10
      ORDER BY source, swap_count DESC
    `;

    const arkhamPathsQuery = `
      SELECT
        '1inch' as source,
        token_in_symbol || ' → ' || token_out_symbol as swap_path,
        COALESCE(SUM(swap_volume_usd), 0) as total_volume,
        COUNT(*) as swap_count
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
        AND token_in_symbol IS NOT NULL
        AND token_out_symbol IS NOT NULL
        ${dateFilterArkham}
      GROUP BY token_in_symbol, token_out_symbol
      ORDER BY swap_count DESC
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
        // Platform Count
        const platformQuery = `
          SELECT
            ${nameExpression} as name,
            COUNT(*) as value
          FROM swaps
          WHERE source = $1
            ${dateFilter}
          GROUP BY 1
          ORDER BY value DESC
        `;
        const platformRes = await client.query(platformQuery, [provider]);
        providerData[provider] = { platforms: platformRes.rows };
      } else {
        // 1inch Chain Count (from Arkham data)
        const chainQuery = `
          SELECT
            chain as chain_id,
            COUNT(*) as value
          FROM dex_aggregator_revenue
          WHERE protocol = '1inch'
            AND chain IS NOT NULL
            AND token_in_symbol IS NOT NULL
            AND token_out_symbol IS NOT NULL
            ${dateFilterArkham}
          GROUP BY 1
          ORDER BY value DESC
        `;
        const chainRes = await client.query(chainQuery);
        providerData[provider] = { chains: chainRes.rows };
      }
    }

    return NextResponse.json({
      totalCount,
      countOverTime,
      countByPlatformOverTime,
      countByProvider,
      topPaths,
      providerData
    });

  } catch (error) {
    console.error('=== Swap Count API Error ===');
    console.error('Error message:', error instanceof Error ? error.message : String(error));
    console.error('Stack trace:', error instanceof Error ? error.stack : 'No stack trace');
    return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 });
  } finally {
    client.release();
  }
}

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

    // 1. Global Stats (filtered by date range)
    // Fetch non-1inch from swaps
    const swapsStatsQuery = `
      SELECT
        COALESCE(SUM(in_amount_usd), 0) as total_volume,
        COUNT(*) as total_swaps
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
    `;

    // Fetch 1inch from dex_aggregator_revenue
    // IMPORTANT: Only count enriched records (those with token symbols)
    // Unenriched records are likely THORChain cross-chain swaps or spam, not real 1inch swaps
    const arkhamStatsQuery = `
      SELECT
        COALESCE(SUM(COALESCE(swap_volume_usd, 0)), 0) as total_volume,
        COUNT(*) as total_swaps
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
        AND token_in_symbol IS NOT NULL
        AND token_out_symbol IS NOT NULL
        ${dateFilterArkham}
    `;

    const [swapsStatsRes, arkhamStatsRes] = await Promise.all([
      client.query(swapsStatsQuery),
      client.query(arkhamStatsQuery)
    ]);

    const globalStats = {
      total_volume: parseFloat(swapsStatsRes.rows[0].total_volume) + parseFloat(arkhamStatsRes.rows[0].total_volume),
      total_swaps: parseInt(swapsStatsRes.rows[0].total_swaps) + parseInt(arkhamStatsRes.rows[0].total_swaps)
    };

    // 2. Volume by Provider (Over Time) - use granularity parameter and apply date filter
    // IMPORTANT: Most timestamp fields only have date precision (all set to 00:00:00)
    // So hourly granularity won't work well - we should warn or fall back to daily
    // For now, if user selects hourly, we'll check if timestamps actually have time data

    // Check if timestamps have actual time precision (not all at midnight)
    let hasTimePrecision = false;
    if (dateTruncParam === 'hour') {
      const timeCheckQuery = `
        SELECT COUNT(DISTINCT EXTRACT(HOUR FROM timestamp)) as unique_hours
        FROM swaps
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        LIMIT 1
      `;
      const timeCheckRes = await client.query(timeCheckQuery);
      hasTimePrecision = timeCheckRes.rows[0]?.unique_hours > 1;

      if (!hasTimePrecision) {
        console.warn('⚠️  Hourly granularity requested but timestamps only have date precision. Falling back to daily.');
      }

      // Additional debug: show sample timestamps from last 24h
      const sampleTimestampsQuery = `
        SELECT timestamp, date_only, source, in_amount_usd
        FROM swaps
        WHERE timestamp >= NOW() - INTERVAL '24 hours'
        ORDER BY timestamp DESC
        LIMIT 10
      `;
      const sampleTimestamps = await client.query(sampleTimestampsQuery);
      console.log('=== SAMPLE TIMESTAMPS (Last 24h) ===');
      console.log(JSON.stringify(sampleTimestamps.rows, null, 2));
      console.log('Total swaps in last 24h:', sampleTimestamps.rows.length);
    }

    // If hourly was requested but timestamps don't have time precision, fall back to daily
    const effectiveGranularity = (dateTruncParam === 'hour' && !hasTimePrecision) ? 'day' : dateTruncParam;
    const dateField = effectiveGranularity === 'hour' ? 'timestamp' : 'date_only';

    const swapsTimeQuery = `
      SELECT
        DATE_TRUNC('${effectiveGranularity}', ${dateField}) as time_period,
        source,
        SUM(in_amount_usd) as volume
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
      GROUP BY time_period, source
      ORDER BY time_period ASC
    `;

    const arkhamTimeQuery = `
      SELECT
        DATE_TRUNC('${effectiveGranularity}', timestamp) as time_period,
        '1inch' as source,
        COALESCE(SUM(swap_volume_usd), 0) as volume
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
        AND token_in_symbol IS NOT NULL
        AND token_out_symbol IS NOT NULL
        ${dateFilterArkham}
      GROUP BY time_period
      ORDER BY time_period ASC
    `;

    const [swapsTimeRes, arkhamTimeRes] = await Promise.all([
      client.query(swapsTimeQuery),
      client.query(arkhamTimeQuery)
    ]);

    // Merge and sort by time
    const volumeOverTime = [...swapsTimeRes.rows, ...arkhamTimeRes.rows].sort((a, b) =>
      new Date(a.time_period).getTime() - new Date(b.time_period).getTime()
    );

    // Debug logging for all queries when range is 24h or when hourly is requested
    if (range === '24h' || dateTruncParam === 'hour') {
      console.log('=== HOURLY/24H DEBUG ===');
      console.log('Range:', range);
      console.log('Requested granularity:', granularity);
      console.log('Effective granularity:', effectiveGranularity);
      console.log('Date field used:', dateField);
      console.log('Date filter:', dateFilter);
      console.log('Has time precision:', hasTimePrecision);
      console.log('\n--- Swaps Query ---');
      console.log(swapsTimeQuery);
      console.log('\n--- Swaps Results (first 5) ---');
      console.log(JSON.stringify(swapsTimeRes.rows.slice(0, 5), null, 2));
      console.log('Total swaps records:', swapsTimeRes.rows.length);
      console.log('\n--- Arkham Query ---');
      console.log(arkhamTimeQuery);
      console.log('\n--- Arkham Results (first 5) ---');
      console.log(JSON.stringify(arkhamTimeRes.rows.slice(0, 5), null, 2));
      console.log('Total arkham records:', arkhamTimeRes.rows.length);
      console.log('\n--- Combined Results ---');
      console.log('Total records:', volumeOverTime.length);
      console.log('Unique time periods:', new Set(volumeOverTime.map(r => r.time_period)).size);
      console.log('All time periods:', [...new Set(volumeOverTime.map(r => r.time_period))]);
      console.log('Sample combined records:', JSON.stringify(volumeOverTime.slice(0, 10), null, 2));
    }

    // 3. Total Volume by Provider (filtered by date range)
    const swapsProviderQuery = `
      SELECT
        source,
        SUM(in_amount_usd) as total_volume,
        COUNT(*) as swap_count
      FROM swaps
      WHERE source != '1inch'
        ${dateFilter}
      GROUP BY source
    `;

    const arkhamProviderQuery = `
      SELECT
        '1inch' as source,
        COALESCE(SUM(swap_volume_usd), 0) as total_volume,
        COUNT(*) as swap_count
      FROM dex_aggregator_revenue
      WHERE protocol = '1inch'
        AND token_in_symbol IS NOT NULL
        AND token_out_symbol IS NOT NULL
        ${dateFilterArkham}
    `;

    const [swapsProviderRes, arkhamProviderRes] = await Promise.all([
      client.query(swapsProviderQuery),
      client.query(arkhamProviderQuery)
    ]);

    const volumeByProvider = [...swapsProviderRes.rows, ...arkhamProviderRes.rows].sort((a, b) =>
      parseFloat(b.total_volume) - parseFloat(a.total_volume)
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
          ROW_NUMBER() OVER (PARTITION BY source ORDER BY SUM(in_amount_usd) DESC) as rank
        FROM swaps
        WHERE source != '1inch'
          ${dateFilter}
        GROUP BY source, in_asset, out_asset
      )
      SELECT source, swap_path, total_volume, swap_count
      FROM ranked_paths
      WHERE rank <= 10
      ORDER BY source, total_volume DESC
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
      ORDER BY total_volume DESC
      LIMIT 10
    `;

    const [swapsPathsRes, arkhamPathsRes] = await Promise.all([
      client.query(swapsPathsQuery),
      client.query(arkhamPathsQuery)
    ]);

    const topPaths = [...swapsPathsRes.rows, ...arkhamPathsRes.rows];

    // 5-7. Provider-specific breakdowns (filtered by date range)
    const providers = ['thorchain', 'mayachain', 'lifi', '1inch'];
    const providerData: any = {};

    for (const provider of providers) {
      if (provider === '1inch') {
        // For 1inch, show chain distribution (from Arkham data)
        const chainQuery = `
          SELECT
            chain,
            COALESCE(SUM(swap_volume_usd), 0) as volume
          FROM dex_aggregator_revenue
          WHERE protocol = '1inch'
            AND chain IS NOT NULL
            AND token_in_symbol IS NOT NULL
            AND token_out_symbol IS NOT NULL
            ${dateFilterArkham}
          GROUP BY chain
          ORDER BY volume DESC
        `;
        const chainResult = await client.query(chainQuery);
        providerData[provider] = { chains: chainResult.rows };
      } else {
        // For others, show platform/affiliate distribution
        let nameExpression = "COALESCE(platform, 'Unknown')";
        if (provider === 'thorchain' || provider === 'mayachain') {
          nameExpression = "COALESCE(platform, raw_data->'metadata'->'swap'->>'affiliateAddress', 'Unknown')";
        } else if (provider === 'lifi') {
          // Normalize LI.FI platform names: vultisig-android -> Android, vultisig-ios -> iOS
          nameExpression = `
            CASE 
              WHEN LOWER(COALESCE(platform, raw_data->'metadata'->>'integrator', 'Unknown')) LIKE '%android%' THEN 'Android'
              WHEN LOWER(COALESCE(platform, raw_data->'metadata'->>'integrator', 'Unknown')) LIKE '%ios%' THEN 'iOS'
              ELSE COALESCE(platform, raw_data->'metadata'->>'integrator', 'Unknown')
            END
          `;
        }

        const platformQuery = `
          SELECT
            ${nameExpression} as platform,
            SUM(in_amount_usd) as volume
          FROM swaps
          WHERE source = $1
            ${dateFilter}
          GROUP BY 1
          ORDER BY volume DESC
        `;
        const platformResult = await client.query(platformQuery, [provider]);
        providerData[provider] = { platforms: platformResult.rows };
      }
    }

    return NextResponse.json({
      globalStats,
      volumeOverTime,
      volumeByProvider,
      topPaths,
      providerData,
      metadata: {
        requestedGranularity: granularity,
        effectiveGranularity: effectiveGranularity,
        hasTimePrecision: hasTimePrecision,
        fallbackApplied: dateTruncParam === 'hour' && effectiveGranularity !== 'hour'
      }
    });

  } catch (error) {
    console.error('Error fetching swap volume data:', error);
    return NextResponse.json({ error: 'Failed to fetch data' }, { status: 500 });
  } finally {
    client.release();
  }
}

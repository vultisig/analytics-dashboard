import { NextResponse } from 'next/server';
import pool from '@/lib/db';

export const dynamic = 'force-dynamic';

// Tier order for consistent sorting
const TIER_ORDER = ['Ultimate', 'Diamond', 'Platinum', 'Gold', 'Silver', 'Bronze', 'None'];

export async function GET() {
  const client = await pool.connect();

  try {
    // Fetch tier statistics
    const tierStatsQuery = `
      SELECT
        tier,
        holder_count,
        avg_vult_balance,
        thorguard_boosted_count,
        updated_at
      FROM vult_tier_stats
      ORDER BY CASE tier
        WHEN 'Ultimate' THEN 1
        WHEN 'Diamond' THEN 2
        WHEN 'Platinum' THEN 3
        WHEN 'Gold' THEN 4
        WHEN 'Silver' THEN 5
        WHEN 'Bronze' THEN 6
        WHEN 'None' THEN 7
      END
    `;

    const tierStatsRes = await client.query(tierStatsQuery);

    // Fetch metadata
    const metadataQuery = `
      SELECT key, value, updated_at
      FROM vult_holders_metadata
    `;

    const metadataRes = await client.query(metadataQuery);
    const metadata: Record<string, string> = {};
    let lastUpdated = '';

    for (const row of metadataRes.rows) {
      metadata[row.key] = row.value;
      if (row.key === 'last_updated') {
        lastUpdated = row.value;
      }
    }

    // Calculate tiered holders (Bronze+)
    const tieredHolders = tierStatsRes.rows
      .filter(row => row.tier !== 'None')
      .reduce((sum, row) => sum + parseInt(row.holder_count), 0);

    // Format tier data
    const tiers = tierStatsRes.rows.map(row => ({
      tier: row.tier,
      count: parseInt(row.holder_count) || 0,
      avgBalance: parseFloat(row.avg_vult_balance) || 0,
      thorguardBoosted: parseInt(row.thorguard_boosted_count) || 0,
    }));

    return NextResponse.json({
      tiers,
      totalHolders: parseInt(metadata['total_holders']) || 0,
      totalSupplyHeld: parseFloat(metadata['total_supply_held']) || 0,
      thorguardHolders: parseInt(metadata['thorguard_holders']) || 0,
      tieredHolders,
      lastUpdated,
    });

  } catch (error) {
    console.error('=== Holders API Error ===');
    console.error('Error message:', error instanceof Error ? error.message : String(error));
    console.error('Stack trace:', error instanceof Error ? error.stack : 'No stack trace');
    return NextResponse.json({ error: 'Failed to fetch holders data' }, { status: 500 });
  } finally {
    client.release();
  }
}

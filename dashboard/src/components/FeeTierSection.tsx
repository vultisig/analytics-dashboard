'use client';

import { useMemo } from 'react';
import { Pie, PieChart, Cell, ResponsiveContainer, Tooltip as RechartsTooltip } from 'recharts';
import { StatsCard } from './StatsCard';
import { Tooltip } from './Tooltip';
import { getTierColor } from '@/lib/chartStyles';
import { IconAwardV, IconPeopleCopy, IconTrendingUpV } from '@/icons';
import { formatCompactNumber } from '@/lib/numberFormatters';

// Fee tier discount percentages for tooltip
const tierDiscounts: Record<string, string> = {
  'Ultimate': '0%',
  'Diamond': '0.15%',
  'Platinum': '0.25%',
  'Gold': '0.30%',
  'Silver': '0.40%',
  'Bronze': '0.45%',
  'Standard': '0.50%',
  'Old Tiers': 'Deprecated',
};

interface TierDistributionItem {
  tier: string;
  userCount: number;
  totalVolume: number;
  avgVolumePerUser: number;
}

interface FeeTierData {
  tierDistribution: TierDistributionItem[];
  totalUsers: number;
  totalVolume: number;
}

interface FeeTierSectionProps {
  data: FeeTierData | null;
  loading: boolean;
  error?: string | null;
}

interface CustomTooltipProps {
  active?: boolean;
  payload?: Array<{
    payload: {
      name: string;
      value: number;
      volume: number;
      avgVolume: number;
    };
  }>;
  totalUsers: number;
}

function CustomTooltipContent({ active, payload, totalUsers }: CustomTooltipProps) {
  if (!active || !payload || !payload.length) return null;
  const item = payload[0].payload;
  const percentage = totalUsers > 0 ? ((item.value / totalUsers) * 100).toFixed(1) : '0';

  return (
    <div
      className="w-[180px] flex flex-col gap-1 rounded-[12px] p-3 backdrop-blur-[2px]"
      style={{
        background: 'rgba(17,40,74,0.5)',
        border: '1px solid var(--border-normal)',
      }}
    >
      <div className="flex items-center gap-2 pb-1.5 border-b border-[var(--border-light)]">
        <span
          className="size-2.5 shrink-0 rounded-full"
          style={{ backgroundColor: getTierColor(item.name) }}
        />
        <p className="t-body-s text-[var(--text-primary)]">{item.name}</p>
      </div>
      <p className="t-footnote text-[var(--text-tertiary)]">
        Users: <span className="text-[var(--text-secondary)] text-num">
          {new Intl.NumberFormat('en-US').format(item.value)} ({percentage}%)
        </span>
      </p>
      <p className="t-footnote text-[var(--text-tertiary)]">
        Volume: <span className="text-[var(--text-secondary)] text-num">
          ${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(item.volume)}
        </span>
      </p>
      <p className="t-footnote text-[var(--text-tertiary)]">
        Avg/User: <span className="text-[var(--text-secondary)] text-num">
          ${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(item.avgVolume)}
        </span>
      </p>
      <p className="t-footnote text-[var(--alert-info)] mt-1">
        Fee: {tierDiscounts[item.name] || 'Unknown'}
      </p>
    </div>
  );
}

function formatCompactCurrency(value: number): string {
  return `$${formatCompactNumber(value)}`;
}

export function FeeTierSection({ data, loading, error }: FeeTierSectionProps) {
  const chartData = useMemo(() => {
    if (!data?.tierDistribution) return [];
    return data.tierDistribution
      .filter((item) => item.userCount > 0)
      .map((item) => ({
        name: item.tier,
        value: item.userCount,
        volume: item.totalVolume,
        avgVolume: item.avgVolumePerUser,
      }));
  }, [data]);

  const totals = useMemo(() => {
    if (!data?.tierDistribution) {
      return { totalUsers: 0, totalVolume: 0, avgVolumePerUser: 0 };
    }
    const totalUsers = data.totalUsers || data.tierDistribution.reduce((s, i) => s + i.userCount, 0);
    const totalVolume = data.totalVolume || data.tierDistribution.reduce((s, i) => s + i.totalVolume, 0);
    const avgVolumePerUser = totalUsers > 0 ? totalVolume / totalUsers : 0;
    return { totalUsers, totalVolume, avgVolumePerUser };
  }, [data]);

  const header = (
    <div className="flex items-center gap-2.5">
      <div className="icon-badge">
        <IconAwardV />
      </div>
      <h3 className="t-title-3">Fee Tier Distribution</h3>
      <Tooltip
        content="User fee tiers based on VULT token holdings. Higher tiers pay lower fees (0–50 basis points)."
        iconOnly
      />
    </div>
  );

  if (loading && !data) {
    return (
      <div className="surface-card p-6 space-y-6">
        {header}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="h-[350px] bg-[var(--surface-3)]/30 rounded-xl animate-pulse" />
          <div className="space-y-4">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="h-20 bg-[var(--surface-3)]/30 rounded-xl animate-pulse" />
            ))}
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="surface-card p-6 space-y-4">
        {header}
        <div className="flex items-center justify-center py-12 text-[var(--alert-error)] text-sm">
          {error}
        </div>
      </div>
    );
  }

  if (!data || chartData.length === 0) {
    return (
      <div className="surface-card p-6 space-y-4">
        {header}
        <div className="flex items-center justify-center py-12 t-footnote text-[var(--text-tertiary)]">
          No fee tier data available for the selected time range
        </div>
      </div>
    );
  }

  const tierOrder = ['Ultimate', 'Diamond', 'Platinum', 'Gold', 'Silver', 'Bronze', 'Standard', 'Old Tiers', 'Unknown'];
  const sortedTiers = [...data.tierDistribution].sort((a, b) => {
    return tierOrder.indexOf(a.tier) - tierOrder.indexOf(b.tier);
  });

  return (
    <div className="surface-card p-6 space-y-6">
      {header}

      {/* Summary metrics — two compact StatsCards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <StatsCard
          title="Total Users"
          icon={IconPeopleCopy}
          value={new Intl.NumberFormat('en-US').format(totals.totalUsers)}
        />
        <StatsCard
          title="Avg Volume/User"
          icon={IconTrendingUpV}
          value={`$${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(totals.avgVolumePerUser)}`}
        />
      </div>

      {/* Two-column: donut + Volume by Tier list */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="rounded-[16px] border border-[var(--border-light)] bg-[var(--surface-2)]/40 p-4">
          <h4 className="t-footnote text-[var(--text-secondary)] mb-3">Users by Tier</h4>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={chartData}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={100}
                paddingAngle={2}
                dataKey="value"
              >
                {chartData.map((entry, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={getTierColor(entry.name, index)}
                    stroke="rgba(0,0,0,0.1)"
                  />
                ))}
              </Pie>
              <RechartsTooltip
                content={(props) => (
                  <CustomTooltipContent
                    active={props.active}
                    payload={props.payload as CustomTooltipProps['payload']}
                    totalUsers={totals.totalUsers}
                  />
                )}
                wrapperStyle={{ outline: 'none' }}
              />
            </PieChart>
          </ResponsiveContainer>
          {/* Inline legend (matches Figma — chips below the donut) */}
          <div className="mt-2 flex flex-wrap justify-center gap-x-3 gap-y-1.5 px-2">
            {chartData.map((entry, index) => (
              <div key={entry.name} className="flex items-center gap-1.5">
                <span
                  className="size-2 rounded-full"
                  style={{ backgroundColor: getTierColor(entry.name, index) }}
                />
                <span className="text-[11px] text-[var(--text-secondary)]">{entry.name}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="flex flex-col">
          <h4 className="t-footnote text-[var(--text-secondary)] mb-3">Volume by Tier</h4>
          <div className="space-y-2 max-h-[340px] overflow-y-auto pr-1">
            {sortedTiers
              .filter((tier) => tier.userCount > 0)
              .map((tier, index) => {
                const volumePct = totals.totalVolume > 0 ? (tier.totalVolume / totals.totalVolume) * 100 : 0;
                const userPct = totals.totalUsers > 0 ? (tier.userCount / totals.totalUsers) * 100 : 0;
                const color = getTierColor(tier.tier, index);

                return (
                  <div
                    key={tier.tier}
                    className="rounded-[14px] border border-[var(--border-light)] bg-[var(--surface-2)]/40 p-3 hover:border-[var(--border-normal)] transition-colors"
                  >
                    <div className="flex items-center justify-between mb-2 gap-2">
                      <div className="flex items-center gap-2 min-w-0">
                        <span
                          className="size-2.5 shrink-0 rounded-full"
                          style={{ backgroundColor: color }}
                        />
                        <span className="t-body-s text-[var(--text-primary)] truncate">{tier.tier}</span>
                        <span className="t-footnote text-[var(--text-tertiary)] shrink-0">
                          {tierDiscounts[tier.tier] || ''}
                        </span>
                      </div>
                      <span className="t-footnote text-num text-[var(--text-tertiary)] shrink-0">
                        {tier.userCount.toLocaleString()} · {userPct.toFixed(1)}%
                      </span>
                    </div>

                    <div className="relative h-1.5 rounded-full bg-[var(--surface-3)]/50 overflow-hidden mb-1">
                      <div
                        className="absolute inset-y-0 left-0 rounded-full transition-[width] duration-500"
                        style={{
                          width: `${Math.min(volumePct, 100)}%`,
                          backgroundColor: color,
                        }}
                      />
                    </div>

                    <div className="flex items-center justify-between text-num text-[11px] text-[var(--text-tertiary)]">
                      <span>{formatCompactCurrency(tier.totalVolume)}</span>
                      <span>{volumePct.toFixed(1)}% of volume</span>
                    </div>
                  </div>
                );
              })}
          </div>
        </div>
      </div>
    </div>
  );
}

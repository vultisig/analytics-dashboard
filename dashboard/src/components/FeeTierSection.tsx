'use client';

import { useMemo } from 'react';
import { Pie, PieChart, Cell, ResponsiveContainer, Legend, Tooltip as RechartsTooltip } from 'recharts';
import { HeroMetric } from './HeroMetric';
import { Tooltip } from './Tooltip';
import { Users, TrendingUp, Award } from 'lucide-react';
import { getTierColor, glassTooltipStyle } from '@/lib/chartStyles';

// Fee tier discount percentages for tooltip
// These match the backend BPS values in api_server.py
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

// Custom tooltip component - defined outside to avoid re-creation during render
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
  if (active && payload && payload.length) {
    const item = payload[0].payload;
    const percentage = totalUsers > 0
      ? ((item.value / totalUsers) * 100).toFixed(1)
      : '0';

    return (
      <div style={glassTooltipStyle} className="p-3 min-w-[180px]">
        <div className="flex items-center gap-2 mb-2">
          <div
            className="w-3 h-3 rounded-full"
            style={{ backgroundColor: getTierColor(item.name) }}
          />
          <p className="text-[var(--text-primary)] font-semibold">{item.name}</p>
        </div>
        <div className="space-y-1 text-sm">
          <p className="text-[var(--text-primary)]">
            <span className="text-[var(--text-tertiary)]">Users:</span>{' '}
            {new Intl.NumberFormat('en-US').format(item.value)} ({percentage}%)
          </p>
          <p className="text-[var(--text-primary)]">
            <span className="text-[var(--text-tertiary)]">Volume:</span>{' '}
            ${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(item.volume)}
          </p>
          <p className="text-[var(--text-primary)]">
            <span className="text-[var(--text-tertiary)]">Avg/User:</span>{' '}
            ${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(item.avgVolume)}
          </p>
          <div className="mt-2 pt-2 border-t border-[var(--border-normal)] flex items-center gap-2">
            <span className="text-[var(--text-tertiary)] text-xs">Fee:</span>
            <span className="px-2 py-1 rounded-md text-xs font-semibold bg-gradient-to-r from-[var(--brand-blue)]/20 to-purple-500/20 text-blue-200 border border-blue-400/30">
              {tierDiscounts[item.name] || 'Unknown'}
            </span>
          </div>
        </div>
      </div>
    );
  }
  return null;
}

export function FeeTierSection({ data, loading, error }: FeeTierSectionProps) {
  // Process data for donut chart
  const chartData = useMemo(() => {
    if (!data?.tierDistribution) return [];
    return data.tierDistribution
      .filter(item => item.userCount > 0)
      .map(item => ({
        name: item.tier,
        value: item.userCount,
        volume: item.totalVolume,
        avgVolume: item.avgVolumePerUser
      }));
  }, [data]);

  // Calculate totals
  const totals = useMemo(() => {
    if (!data?.tierDistribution) {
      return { totalUsers: 0, totalVolume: 0, avgVolumePerUser: 0 };
    }
    const totalUsers = data.totalUsers || data.tierDistribution.reduce((sum, item) => sum + item.userCount, 0);
    const totalVolume = data.totalVolume || data.tierDistribution.reduce((sum, item) => sum + item.totalVolume, 0);
    const avgVolumePerUser = totalUsers > 0 ? totalVolume / totalUsers : 0;
    return { totalUsers, totalVolume, avgVolumePerUser };
  }, [data]);

  // Render loading skeleton
  if (loading && !data) {
    return (
      <div className="glass-card rounded-xl p-6 space-y-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="h-7 w-48 bg-[var(--surface-3)]/50 rounded animate-pulse" />
          </div>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="h-[350px] bg-[var(--surface-3)]/30 rounded-xl animate-pulse" />
          <div className="space-y-4">
            {[1, 2, 3, 4].map(i => (
              <div key={i} className="h-20 bg-[var(--surface-3)]/30 rounded-xl animate-pulse" />
            ))}
          </div>
        </div>
      </div>
    );
  }

  // Render error state
  if (error) {
    return (
      <div className="glass-card rounded-xl p-6">
        <div className="flex items-center justify-center py-12">
          <div className="text-[var(--alert-error)] text-sm">{error}</div>
        </div>
      </div>
    );
  }

  // Render empty state
  if (!data || chartData.length === 0) {
    return (
      <div className="glass-card rounded-xl p-6">
        <div className="flex items-center gap-2 mb-4">
          <Award className="w-5 h-5 text-[var(--alert-warn)]" />
          <h3 className="text-lg font-bold text-[var(--text-primary)]">Fee Tier Distribution</h3>
          <Tooltip
            content="User fee tiers based on VULT token holdings. Higher tiers pay lower fees (0-50 basis points)."
            iconOnly
          />
        </div>
        <div className="flex items-center justify-center py-12">
          <div className="text-[var(--text-tertiary)] text-sm">No fee tier data available for the selected time range</div>
        </div>
      </div>
    );
  }

  // Order tiers for display (highest to lowest)
  const tierOrder = ['Ultimate', 'Diamond', 'Platinum', 'Gold', 'Silver', 'Bronze', 'Standard', 'Old Tiers', 'Unknown'];
  const sortedTiers = [...data.tierDistribution].sort((a, b) => {
    const indexA = tierOrder.indexOf(a.tier);
    const indexB = tierOrder.indexOf(b.tier);
    return indexA - indexB;
  });

  return (
    <div className="glass-card rounded-xl p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center gap-2">
        <Award className="w-5 h-5 text-[var(--alert-warn)]" />
        <h3 className="text-lg font-bold text-[var(--text-primary)]">Fee Tier Distribution</h3>
        <Tooltip
          content="User fee tiers based on VULT token holdings. Higher tiers pay lower fees (0-50 basis points)."
          iconOnly
        />
      </div>

      {/* Summary Metrics */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <HeroMetric
          label="Total Users"
          value={totals.totalUsers}
          icon={Users}
          color="cyan"
          format="number"
          size="default"
        />
        <HeroMetric
          label="Avg Volume/User"
          value={totals.avgVolumePerUser}
          icon={TrendingUp}
          color="blue"
          format="currency"
          size="default"
        />
      </div>

      {/* Main Content - Two Column Layout */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Left: Donut Chart */}
        <div className="glass-card rounded-xl p-4">
          <h4 className="text-sm font-medium text-[var(--text-secondary)] mb-4">Users by Tier</h4>
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
              />
              <Legend
                verticalAlign="bottom"
                height={36}
                iconSize={8}
                iconType="circle"
                wrapperStyle={{ fontSize: '10px' }}
                formatter={(value) => (
                  <span className="text-[var(--text-secondary)] ml-1 text-[10px] md:text-xs">{value}</span>
                )}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Right: Volume Metrics per Tier */}
        <div className="space-y-3">
          <h4 className="text-sm font-medium text-[var(--text-secondary)] mb-2">Volume by Tier</h4>
          <div className="space-y-2 max-h-[340px] overflow-y-auto pr-2">
            {sortedTiers.filter(tier => tier.userCount > 0).map((tier, index) => {
              const volumePercentage = totals.totalVolume > 0
                ? (tier.totalVolume / totals.totalVolume) * 100
                : 0;
              const userPercentage = totals.totalUsers > 0
                ? (tier.userCount / totals.totalUsers) * 100
                : 0;

              return (
                <div
                  key={tier.tier}
                  className="glass-card rounded-lg p-3 hover:bg-white/5 transition-colors"
                >
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <div
                        className="w-3 h-3 rounded-full"
                        style={{ backgroundColor: getTierColor(tier.tier, index) }}
                      />
                      <span className="text-[var(--text-primary)] font-medium text-sm">{tier.tier}</span>
                      <span className="text-[var(--text-tertiary)] text-xs">
                        ({tierDiscounts[tier.tier] || 'Unknown fee'})
                      </span>
                    </div>
                    <span className="text-[var(--text-tertiary)] text-xs">
                      {tier.userCount.toLocaleString()} users ({userPercentage.toFixed(1)}%)
                    </span>
                  </div>

                  {/* Volume bar */}
                  <div className="relative h-2 bg-[var(--surface-3)]/50 rounded-full overflow-hidden mb-1">
                    <div
                      className="absolute inset-y-0 left-0 rounded-full transition-all duration-500"
                      style={{
                        width: `${Math.min(volumePercentage, 100)}%`,
                        backgroundColor: getTierColor(tier.tier, index)
                      }}
                    />
                  </div>

                  <div className="flex items-center justify-between text-xs">
                    <span className="text-[var(--text-tertiary)]">
                      ${new Intl.NumberFormat('en-US', {
                        notation: 'compact',
                        maximumFractionDigits: 1
                      }).format(tier.totalVolume)}
                    </span>
                    <span className="text-[var(--text-tertiary)]">
                      {volumePercentage.toFixed(1)}% of volume
                    </span>
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

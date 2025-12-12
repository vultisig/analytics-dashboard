'use client';

import { Users } from 'lucide-react';

interface TierCardProps {
    tier: string;
    count: number;
    avgBalance: number;
    thorguardBoosted: number;
    requirement: number;
    discount: number;
}

// Tier color configurations
const tierColors: Record<string, { gradient: string; border: string; text: string; glow: string }> = {
    Ultimate: {
        gradient: 'from-purple-500/20 to-pink-500/20',
        border: 'border-purple-500/50',
        text: 'text-purple-400',
        glow: 'drop-shadow-[0_0_8px_rgba(168,85,247,0.4)]',
    },
    Diamond: {
        gradient: 'from-cyan-400/20 to-blue-400/20',
        border: 'border-cyan-400/50',
        text: 'text-cyan-300',
        glow: 'drop-shadow-[0_0_8px_rgba(34,211,238,0.4)]',
    },
    Platinum: {
        gradient: 'from-slate-300/20 to-slate-400/20',
        border: 'border-slate-300/50',
        text: 'text-slate-200',
        glow: 'drop-shadow-[0_0_8px_rgba(203,213,225,0.3)]',
    },
    Gold: {
        gradient: 'from-yellow-500/20 to-amber-500/20',
        border: 'border-yellow-500/50',
        text: 'text-yellow-400',
        glow: 'drop-shadow-[0_0_8px_rgba(234,179,8,0.4)]',
    },
    Silver: {
        gradient: 'from-slate-400/20 to-slate-500/20',
        border: 'border-slate-400/50',
        text: 'text-slate-300',
        glow: 'drop-shadow-[0_0_8px_rgba(148,163,184,0.3)]',
    },
    Bronze: {
        gradient: 'from-orange-600/20 to-amber-700/20',
        border: 'border-orange-600/50',
        text: 'text-orange-400',
        glow: 'drop-shadow-[0_0_8px_rgba(234,88,12,0.4)]',
    },
    None: {
        gradient: 'from-slate-600/20 to-slate-700/20',
        border: 'border-slate-600/50',
        text: 'text-slate-500',
        glow: '',
    },
};

function formatNumber(value: number): string {
    if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
    if (value >= 1000) return `${(value / 1000).toFixed(1)}K`;
    return value.toLocaleString();
}

export function TierCard({ tier, count, avgBalance, thorguardBoosted, requirement, discount }: TierCardProps) {
    const colors = tierColors[tier] || tierColors.None;

    return (
        <div className={`
            relative overflow-hidden rounded-xl p-4 transition-all
            bg-gradient-to-br ${colors.gradient}
            border ${colors.border}
            hover:scale-[1.02] hover:shadow-lg
        `}>
            {/* Tier Name */}
            <div className="flex items-center justify-between mb-3">
                <h3 className={`text-lg font-bold ${colors.text} ${colors.glow}`}>
                    {tier}
                </h3>
                {discount > 0 && (
                    <span className={`text-xs font-medium px-2 py-0.5 rounded-full bg-white/10 ${colors.text}`}>
                        -{discount} bps
                    </span>
                )}
            </div>

            {/* Holder Count */}
            <div className="flex items-center gap-2 mb-2">
                <Users className={`w-4 h-4 ${colors.text}`} />
                <span className="text-2xl font-bold text-white">
                    {formatNumber(count)}
                </span>
                <span className="text-xs text-slate-400">holders</span>
            </div>

            {/* Average Balance */}
            <div className="text-sm text-slate-400 mb-2">
                <span className="text-slate-300">{formatNumber(avgBalance)}</span>
                <span className="ml-1">avg VULT</span>
            </div>

            {/* Requirement */}
            {requirement > 0 && (
                <div className="text-xs text-slate-500">
                    Requires {formatNumber(requirement)}+ VULT
                </div>
            )}

            {/* THORGuard Boosted */}
            {thorguardBoosted > 0 && (
                <div className="mt-2 text-xs text-emerald-400/80 flex items-center gap-1">
                    <span className="inline-block w-2 h-2 rounded-full bg-emerald-400/60" />
                    {thorguardBoosted} boosted by THORGuard
                </div>
            )}
        </div>
    );
}

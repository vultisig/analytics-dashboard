'use client';

import { useRef, useEffect, useState } from 'react';
import CountUp from 'react-countup';
import { LucideIcon, TrendingUp, TrendingDown } from 'lucide-react';
import { Tooltip } from './Tooltip';

interface HeroMetricProps {
    label: string;
    value: number;
    icon: LucideIcon;
    trend?: {
        value: string;
        direction: 'up' | 'down';
    };
    color?: 'cyan' | 'blue' | 'teal' | 'purple';
    format?: 'currency' | 'number';
    size?: 'default' | 'large';
    tooltip?: string;
}

const colorClasses = {
    cyan: {
        border: 'border-cyan-500',
        iconBg: 'bg-cyan-500/10',
        iconText: 'text-cyan-400',
        iconGlow: 'drop-shadow-[0_0_8px_rgba(6,182,212,0.4)]',
    },
    blue: {
        border: 'border-blue-500',
        iconBg: 'bg-blue-500/10',
        iconText: 'text-blue-400',
        iconGlow: 'drop-shadow-[0_0_8px_rgba(59,130,246,0.4)]',
    },
    teal: {
        border: 'border-teal-500',
        iconBg: 'bg-teal-500/10',
        iconText: 'text-teal-400',
        iconGlow: 'drop-shadow-[0_0_8px_rgba(45,212,191,0.4)]',
    },
    purple: {
        border: 'border-purple-500',
        iconBg: 'bg-purple-500/10',
        iconText: 'text-purple-400',
        iconGlow: 'drop-shadow-[0_0_8px_rgba(167,139,250,0.4)]',
    },
};

export function HeroMetric({
    label,
    value,
    icon: Icon,
    trend,
    color = 'cyan',
    format = 'currency',
    size = 'large',
    tooltip,
}: HeroMetricProps) {
    const colors = colorClasses[color];
    const fontSize = size === 'large' ? 'text-2xl sm:text-3xl lg:text-4xl xl:text-5xl' : 'text-xl sm:text-2xl lg:text-3xl';
    const prevValueRef = useRef<number | null>(null);
    const [shouldAnimate, setShouldAnimate] = useState(false);

    useEffect(() => {
        // Only animate if value actually changed (with small tolerance for floating point)
        const hasChanged = prevValueRef.current === null || Math.abs(prevValueRef.current - value) > 0.01;

        if (hasChanged) {
            setShouldAnimate(true);
            prevValueRef.current = value;
        } else {
            setShouldAnimate(false);
        }
    }, [value]);

    // Smart formatting: use M for millions, K for thousands
    const getFormattedValue = () => {
        if (value >= 1000000) {
            return {
                end: value / 1000000,
                suffix: 'M',
                decimals: 2
            };
        } else if (value >= 1000) {
            return {
                end: value / 1000,
                suffix: 'K',
                decimals: 1
            };
        }
        return {
            end: value,
            suffix: '',
            decimals: 0
        };
    };

    const formattedValue = getFormattedValue();

    return (
        <div className={`glass-card glass-card-hover will-change-blur rounded-xl p-4 md:p-6 border-t-2 ${colors.border} relative`}>
            {/* Background gradient effect */}
            <div className={`absolute top-0 right-0 w-24 md:w-32 h-24 md:h-32 ${colors.iconBg} rounded-full blur-3xl opacity-20`} />

            <div className="relative z-10">
                {/* Icon positioned at top right */}
                <div className={`absolute top-0 right-0 ${colors.iconBg} p-2 md:p-3 rounded-lg ${colors.iconGlow}`}>
                    <Icon className={`w-5 h-5 md:w-6 md:h-6 ${colors.iconText}`} />
                </div>

                {/* Label */}
                <div className="flex items-center gap-2 mb-2 pr-12 md:pr-14">
                    <p className="text-slate-400 text-xs md:text-sm font-medium md:whitespace-nowrap">{label}</p>
                    {tooltip && <Tooltip content={tooltip} iconOnly />}
                </div>

                {/* Value */}
                <div className={`${fontSize} font-bold text-white ${colors.iconGlow}`}>
                    {format === 'currency' ? '$' : ''}
                    {shouldAnimate ? (
                        <CountUp
                            end={formattedValue.end}
                            duration={0.8}
                            separator=","
                            decimals={formattedValue.decimals}
                            suffix={formattedValue.suffix}
                            useEasing={true}
                            easingFn={(t, b, c, d) => {
                                // easeOutExpo
                                return t === d ? b + c : c * (-Math.pow(2, -10 * t / d) + 1) + b;
                            }}
                        />
                    ) : (
                        <span>
                            {formattedValue.end.toLocaleString('en-US', {
                                minimumFractionDigits: formattedValue.decimals,
                                maximumFractionDigits: formattedValue.decimals
                            })}{formattedValue.suffix}
                        </span>
                    )}
                </div>

                {/* Trend indicator */}
                {trend && (
                    <div className="flex items-center gap-1.5 mt-2">
                        {trend.direction === 'up' ? (
                            <TrendingUp className="w-4 h-4 text-emerald-400" />
                        ) : (
                            <TrendingDown className="w-4 h-4 text-red-400" />
                        )}
                        <span className={`text-sm font-medium ${
                            trend.direction === 'up' ? 'text-emerald-400' : 'text-red-400'
                        }`}>
                            {trend.value}
                        </span>
                        <span className="text-xs text-slate-500">vs previous period</span>
                    </div>
                )}
            </div>
        </div>
    );
}

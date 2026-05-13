'use client';

import { useRef, useEffect, useState } from 'react';
import type { ComponentType } from 'react';
import CountUp from 'react-countup';
import { IconTrendingUpV, IconTrendingDownV } from '@/icons';
import { Tooltip } from './Tooltip';

type IconComponent = ComponentType<{ size?: number; className?: string }>;

interface HeroMetricProps {
    label: string;
    value: number;
    icon: IconComponent;
    trend?: {
        value: string;
        direction: 'up' | 'down';
    };
    /** Visual emphasis. `accent` renders the deep-blue radial gradient (first card in the hero row). */
    color?: 'accent' | 'default' | 'cyan' | 'blue' | 'teal' | 'purple';
    format?: 'currency' | 'number' | 'percent';
    size?: 'default' | 'large';
    tooltip?: string;
}

export function HeroMetric({
    label,
    value,
    icon: Icon,
    trend,
    color = 'default',
    format = 'currency',
    tooltip,
}: HeroMetricProps) {
    const prevValueRef = useRef<number | null>(null);
    const [shouldAnimate, setShouldAnimate] = useState(false);

    useEffect(() => {
        const hasChanged = prevValueRef.current === null || Math.abs(prevValueRef.current - value) > 0.01;
        if (hasChanged) {
            setShouldAnimate(true);
            prevValueRef.current = value;
        } else {
            setShouldAnimate(false);
        }
    }, [value]);

    const getFormattedValue = () => {
        if (format === 'percent') {
            return { end: value, suffix: '%', decimals: 1 };
        }
        if (value >= 1_000_000) {
            return { end: value / 1_000_000, suffix: 'M', decimals: 2 };
        }
        if (value >= 1_000) {
            return { end: value / 1_000, suffix: 'K', decimals: 1 };
        }
        return { end: value, suffix: '', decimals: 0 };
    };

    const fv = getFormattedValue();

    // Legacy color names map to default styling
    const isAccent = color === 'accent';

    return (
        <div className={`metric-card ${isAccent ? 'metric-card-accent' : ''}`}>
            {isAccent && (
                <div className="pointer-events-none absolute inset-0 overflow-hidden rounded-[20px]">
                    {/* MagicPattern decorative overlay (Figma asset).
                        Sits behind the card content, opacity 50%,
                        rotated 180° to match the design. */}
                    <img
                        src="/figma/magic-pattern.png"
                        alt=""
                        aria-hidden="true"
                        className="absolute right-[-22%] bottom-[-13px] h-[170px] w-[406px] rotate-180 opacity-50 select-none"
                        draggable={false}
                    />
                </div>
            )}
            <div className="flex items-center justify-between relative z-10">
                <div className={`icon-badge ${isAccent ? 'icon-badge-brand' : ''}`}>
                    <Icon size={18} />
                </div>
            </div>
            <div className="flex flex-col gap-[14px] relative z-10">
                <div className="flex items-center gap-2">
                    <p className="text-sm font-medium leading-[18px] text-[var(--text-secondary)] whitespace-nowrap">
                        {label}
                    </p>
                    {tooltip && <Tooltip content={tooltip} iconOnly />}
                </div>
                <p className="font-display font-medium text-num text-[36px] leading-[37px] tracking-[-0.72px] text-[var(--text-primary)]">
                    {format === 'currency' ? '$' : ''}
                    {shouldAnimate ? (
                        <CountUp
                            end={fv.end}
                            duration={0.8}
                            separator=","
                            decimals={fv.decimals}
                            suffix={fv.suffix}
                            useEasing
                            easingFn={(t, b, c, d) => (t === d ? b + c : c * (-Math.pow(2, -10 * t / d) + 1) + b)}
                        />
                    ) : (
                        <span>
                            {fv.end.toLocaleString('en-US', {
                                minimumFractionDigits: fv.decimals,
                                maximumFractionDigits: fv.decimals,
                            })}
                            {fv.suffix}
                        </span>
                    )}
                </p>
                {trend && (
                    <div className="flex items-center gap-1.5">
                        {trend.direction === 'up' ? (
                            <IconTrendingUpV className="text-[var(--alert-success)]" />
                        ) : (
                            <IconTrendingDownV className="text-[var(--alert-error)]" />
                        )}
                        <span className={`text-sm font-medium ${trend.direction === 'up' ? 'text-[var(--alert-success)]' : 'text-[var(--alert-error)]'}`}>
                            {trend.value}
                        </span>
                        <span className="text-xs text-[var(--text-tertiary)]">vs previous period</span>
                    </div>
                )}
            </div>
        </div>
    );
}

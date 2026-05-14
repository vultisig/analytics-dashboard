import type { ComponentType } from 'react';

type IconComponent = ComponentType<{ size?: number; className?: string }>;

interface StatsCardProps {
    title: string;
    subtitle?: string;
    value: string | React.ReactNode;
    change?: string;
    changeType?: 'positive' | 'negative';
    icon: IconComponent;
    trend?: 'up' | 'down';
    size?: 'default' | 'large';
    /** Optional small caption shown to the right of the value (e.g. "Weekly"). */
    caption?: string;
}

export function StatsCard({
    title,
    subtitle,
    value,
    change,
    changeType = 'positive',
    icon: Icon,
    size = 'default',
    caption,
}: StatsCardProps) {
    return (
        <div className="surface-card surface-card-hover p-5 flex flex-col gap-4">
            <div className="flex items-center gap-2.5">
                <div className="icon-badge">
                    <Icon />
                </div>
                <p className="text-[13px] leading-[18px] tracking-[0.04em] text-[var(--text-secondary)]">
                    {title}
                </p>
            </div>
            <div className="flex items-end justify-between gap-3 flex-wrap">
                <div className={`font-display font-bold text-num tracking-[-0.02em] text-[var(--text-primary)] ${size === 'large' ? 'text-[40px] leading-[42px]' : 'text-[32px] leading-[37px]'}`}>
                    {value}
                </div>
                <div className="flex items-center gap-2">
                    {change && (
                        <span className={`text-sm font-medium ${changeType === 'positive' ? 'text-[var(--alert-success)]' : 'text-[var(--alert-error)]'}`}>
                            {change}
                        </span>
                    )}
                    {caption && (
                        <span className="text-[13px] font-medium leading-[18px] text-[var(--alert-info)]">
                            {caption}
                        </span>
                    )}
                </div>
            </div>
            {subtitle && (
                <p className="text-xs text-[var(--text-tertiary)] mt-0">{subtitle}</p>
            )}
        </div>
    );
}

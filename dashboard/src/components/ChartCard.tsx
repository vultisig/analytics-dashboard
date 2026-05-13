import { ReactNode } from 'react';
import { LucideIcon } from 'lucide-react';

interface ChartCardProps {
    title: string;
    subtitle?: string;
    icon?: LucideIcon;
    children: ReactNode;
    className?: string;
    action?: ReactNode;
    exportButton?: ReactNode;
    /** Render the icon badge in the brand-blue style. */
    brandIcon?: boolean;
}

export function ChartCard({
    title,
    subtitle,
    icon: Icon,
    children,
    className = '',
    action,
    exportButton,
    brandIcon = false,
}: ChartCardProps) {
    return (
        <div className={`surface-card surface-card-hover p-5 md:p-6 flex flex-col ${className}`}>
            <div className="mb-5 md:mb-6 flex flex-wrap items-start justify-between gap-3">
                <div className="flex items-center gap-3 min-w-0">
                    {Icon && (
                        <div className={`icon-badge ${brandIcon ? 'icon-badge-brand' : ''}`}>
                            <Icon className="size-4" />
                        </div>
                    )}
                    <div className="min-w-0">
                        <h3 className="text-[17px] font-medium leading-5 tracking-[-0.02em] text-[var(--text-primary)] truncate">
                            {title}
                        </h3>
                        {subtitle && (
                            <p className="text-[13px] leading-[18px] text-[var(--text-tertiary)] mt-0.5 truncate">
                                {subtitle}
                            </p>
                        )}
                    </div>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                    {action}
                    {exportButton}
                </div>
            </div>
            <div className="flex-1 min-h-0 w-full">
                {children}
            </div>
        </div>
    );
}

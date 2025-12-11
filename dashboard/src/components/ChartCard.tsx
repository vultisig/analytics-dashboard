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
}

export function ChartCard({ title, subtitle, icon: Icon, children, className = '', action, exportButton }: ChartCardProps) {
    return (
        <div className={`glass-card glass-card-hover will-change-blur rounded-xl p-4 md:p-6 flex flex-col ${className}`}>
            <div className="mb-4 md:mb-6 flex flex-wrap items-start justify-between gap-2">
                <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                        {Icon && <Icon className="w-4 h-4 md:w-5 md:h-5 text-cyan-400 shrink-0" />}
                        <h3 className="text-base md:text-lg font-semibold text-white truncate">{title}</h3>
                    </div>
                    {subtitle && <p className="text-xs md:text-sm text-slate-400 mt-1">{subtitle}</p>}
                </div>
                <div className="flex items-center gap-2 shrink-0">
                    {action && <div>{action}</div>}
                    {exportButton && <div>{exportButton}</div>}
                </div>
            </div>
            <div className="flex-1 min-h-0 w-full">
                {children}
            </div>
        </div>
    );
}

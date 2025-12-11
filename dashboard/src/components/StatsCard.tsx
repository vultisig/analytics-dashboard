import { LucideIcon } from 'lucide-react';

interface StatsCardProps {
    title: string;
    subtitle?: string;
    value: string | React.ReactNode;
    change?: string;
    changeType?: 'positive' | 'negative';
    icon: LucideIcon;
    trend?: 'up' | 'down';
    size?: 'default' | 'large';
}

export function StatsCard({ title, subtitle, value, change, changeType = 'positive', icon: Icon, trend, size = 'default' }: StatsCardProps) {
    return (
        <div className="group relative overflow-hidden glass-card glass-card-hover will-change-blur rounded-xl p-6 transition-all">
            {/* Top cyan accent bar */}
            <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-cyan-500 to-teal-500"></div>

            <div className="flex items-start justify-between">
                <div className="flex-1">
                    <p className="text-sm font-medium text-slate-400">{title}</p>
                    <div className="mt-2 flex items-baseline gap-2">
                        <div className={`${size === 'large' ? 'text-5xl' : 'text-3xl'} font-bold text-white`}>
                            {value}
                        </div>
                        {change && (
                            <span className={`text-sm font-medium ${changeType === 'positive' ? 'text-emerald-400' : 'text-red-400'
                                }`}>
                                {change}
                            </span>
                        )}
                    </div>
                    {subtitle && (
                        <p className="text-xs text-slate-500 mt-1">{subtitle}</p>
                    )}
                </div>
                <div className="text-cyan-400 transition-all group-hover:scale-110 group-hover:text-cyan-300 drop-shadow-[0_0_8px_rgba(34,211,238,0.3)]">
                    <Icon className="h-8 w-8" />
                </div>
            </div>
        </div>
    );
}

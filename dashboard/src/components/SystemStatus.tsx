'use client';

import { useState, useEffect, useRef } from 'react';
import { formatDistanceToNow } from 'date-fns';

interface SyncStatus {
    source: string;
    last_synced_timestamp: string;
    latest_data_timestamp: string | null;
    last_error: string | null;
    is_active: boolean;
}

export default function SystemStatus() {
    const [statuses, setStatuses] = useState<SyncStatus[]>([]);
    const [loading, setLoading] = useState(true);
    const [overallStatus, setOverallStatus] = useState<'operational' | 'degraded' | 'outage'>('operational');
    const [showLatestData, setShowLatestData] = useState(false);
    const resetTimeoutRef = useRef<NodeJS.Timeout | null>(null);

    // Cleanup timeout on unmount
    useEffect(() => {
        return () => {
            if (resetTimeoutRef.current) {
                clearTimeout(resetTimeoutRef.current);
            }
        };
    }, []);

    useEffect(() => {
        const fetchStatus = async () => {
            try {
                const response = await fetch('/api/system-status');
                if (!response.ok) throw new Error('Failed to fetch status');
                let data = await response.json();

                // Sort: Most recent updated at the bottom (Oldest -> Newest)
                data.sort((a: SyncStatus, b: SyncStatus) =>
                    new Date(a.last_synced_timestamp).getTime() - new Date(b.last_synced_timestamp).getTime()
                );

                setStatuses(data);

                // Determine overall status - Only errors count, not idle sources
                let hasActualError = false;
                let hasActiveSource = false;
                const now = new Date();

                data.forEach((s: SyncStatus) => {
                    const lastSync = new Date(s.last_synced_timestamp);
                    const hoursSinceSync = (now.getTime() - lastSync.getTime()) / (1000 * 60 * 60);

                    // Check if source has actual error
                    if (s.last_error) {
                        hasActualError = true;
                    }

                    // Check if any source is active (synced recently)
                    if (hoursSinceSync < 1) {
                        hasActiveSource = true;
                    }
                });

                // System is operational if:
                // - No actual errors AND at least one source is active
                // System is degraded if:
                // - Has actual errors
                // System is outage if:
                // - All sources have errors (not implemented, as we expect at least THORChain to work)

                if (hasActualError) {
                    setOverallStatus('degraded');
                } else if (hasActiveSource) {
                    setOverallStatus('operational');
                } else {
                    // All sources idle but no errors - still operational, just waiting for data
                    setOverallStatus('operational');
                }

            } catch (error) {
                console.error('Error fetching system status:', error);
                setOverallStatus('degraded'); // Assume degraded on fetch error
            } finally {
                setLoading(false);
            }
        };

        fetchStatus();
        // Poll every minute
        const interval = setInterval(fetchStatus, 60000);
        return () => clearInterval(interval);
    }, []);

    const getStatusColorClasses = (status: 'operational' | 'degraded' | 'outage') => {
        switch (status) {
            case 'operational': return { bg: 'bg-emerald-500', text: 'text-emerald-400', border: 'border-emerald-500/20', bgSoft: 'bg-emerald-500/10' };
            case 'degraded': return { bg: 'bg-yellow-500', text: 'text-yellow-400', border: 'border-yellow-500/20', bgSoft: 'bg-yellow-500/10' };
            case 'outage': return { bg: 'bg-red-500', text: 'text-red-400', border: 'border-red-500/20', bgSoft: 'bg-red-500/10' };
        }
    };

    const getSourceStatusColor = (source: SyncStatus) => {
        const now = new Date();
        const lastSync = new Date(source.last_synced_timestamp);
        const hoursSinceSync = (now.getTime() - lastSync.getTime()) / (1000 * 60 * 60);

        // Error state: has last_error
        if (source.last_error) return 'bg-red-500';

        // Stale: > 24 hours (likely real issue)
        if (hoursSinceSync > 24) return 'bg-red-500';

        // Idle: > 1 hour (might be no user activity - show as amber/warning)
        if (hoursSinceSync > 1) return 'bg-amber-500';

        // Active: < 1 hour
        return 'bg-emerald-500';
    };

    const getSourceStatusText = (source: SyncStatus) => {
        const now = new Date();
        const lastSync = new Date(source.last_synced_timestamp);
        const hoursSinceSync = (now.getTime() - lastSync.getTime()) / (1000 * 60 * 60);

        if (source.last_error) return '(error)';
        if (hoursSinceSync > 24) return '(stale)';
        if (hoursSinceSync > 1) return '(idle)';
        return '';
    };

    const formatSourceName = (source: string) => {
        if (source === 'arkham') return '1inch (Arkham)';
        if (source === 'thorchain') return 'THORChain';
        if (source === 'mayachain') return 'MAYAChain';
        if (source === 'lifi') return 'LI.FI';
        return source.charAt(0).toUpperCase() + source.slice(1);
    };

    const handleMouseLeave = () => {
        // Clear any existing timeout
        if (resetTimeoutRef.current) {
            clearTimeout(resetTimeoutRef.current);
        }
        // Delay reset to match CSS transition (300ms)
        resetTimeoutRef.current = setTimeout(() => {
            setShowLatestData(false);
        }, 300);
    };

    const handleMouseEnter = () => {
        // Cancel reset if user re-enters before transition completes
        if (resetTimeoutRef.current) {
            clearTimeout(resetTimeoutRef.current);
            resetTimeoutRef.current = null;
        }
    };

    const colors = getStatusColorClasses(overallStatus);

    return (
        <div className="relative group z-50" onMouseLeave={handleMouseLeave} onMouseEnter={handleMouseEnter}>
            {/* Main Status Pill */}
            <div className={`flex items-center gap-1.5 md:gap-2 px-2 md:px-3 py-1 md:py-1.5 rounded-full ${colors.bgSoft} border ${colors.border} cursor-help transition-colors duration-300`}>
                <div className={`w-2 h-2 rounded-full shrink-0 ${colors.bg} ${overallStatus === 'operational' ? 'animate-pulse' : ''}`}></div>
                <span className={`text-[10px] md:text-xs font-medium ${colors.text} whitespace-nowrap`}>
                    <span className="hidden sm:inline">
                        {overallStatus === 'operational' ? 'System Operational' :
                            overallStatus === 'degraded' ? 'System Degraded' : 'System Outage'}
                    </span>
                    <span className="sm:hidden">
                        {overallStatus === 'operational' ? 'Online' :
                            overallStatus === 'degraded' ? 'Degraded' : 'Outage'}
                    </span>
                </span>
            </div>

            {/* Hover Popup (Glassmorphism) */}
            <div
                className="absolute right-0 top-full mt-2 w-72 opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all duration-300 transform translate-y-2 group-hover:translate-y-0"
            >
                <div
                    className="bg-slate-900/80 backdrop-blur-xl border border-slate-700/50 rounded-xl p-4 shadow-2xl ring-1 ring-white/10 w-full cursor-pointer hover:bg-slate-800/90 hover:border-slate-600/50 transition-all hover:scale-[1.01]"
                    onClick={() => setShowLatestData(!showLatestData)}
                >
                    <h3 className="text-sm font-semibold text-slate-200 mb-3 pb-2 border-b border-slate-700/50 flex items-center justify-between">
                        <span>{showLatestData ? 'Data Freshness' : 'Last Synced'}</span>
                        <span className="text-[9px] text-slate-500 font-normal">Click to toggle</span>
                    </h3>

                    <div className="space-y-3">
                        {loading ? (
                            <div className="text-xs text-slate-500 text-center py-2">Loading status...</div>
                        ) : (
                            statuses.map((status) => {
                                const dotColor = getSourceStatusColor(status);
                                return (
                                    <div
                                        key={status.source}
                                        className="flex items-center justify-between w-full"
                                    >
                                        <div className="flex items-center gap-2">
                                            <div className={`w-1.5 h-1.5 rounded-full ${dotColor}`}></div>
                                            <span className="text-xs text-slate-300 font-medium">
                                                {formatSourceName(status.source)}
                                            </span>
                                            <span className="text-[9px] text-slate-500">
                                                {getSourceStatusText(status)}
                                            </span>
                                        </div>
                                        <span className="text-[10px] text-slate-500 tabular-nums transition-colors">
                                            {showLatestData && status.latest_data_timestamp ? (
                                                formatDistanceToNow(new Date(status.latest_data_timestamp), { addSuffix: true })
                                            ) : (
                                                formatDistanceToNow(new Date(status.last_synced_timestamp), { addSuffix: true })
                                            )}
                                        </span>
                                    </div>
                                );
                            })
                        )}
                    </div>

                    <div className="mt-3 pt-2 border-t border-slate-700/50 text-[10px] text-slate-500 text-center">
                        Auto-updates every 15 mins
                    </div>
                </div>
            </div>
        </div>
    );
}

'use client';

import { Suspense, useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import { Sidebar } from '@/components/Sidebar';
import { DateRangeSelector } from '@/components/DateRangeSelector';
import { GranularitySelector } from '@/components/GranularitySelector';
import { OverviewTab } from '@/components/tabs/OverviewTab';
import { SwapVolumeTab } from '@/components/tabs/SwapVolumeTab';
import { RevenueTab } from '@/components/tabs/RevenueTab';
import { UsersTab } from '@/components/tabs/UsersTab';
import { CountTab } from '@/components/tabs/CountTab';
import { ReferralsTab } from '@/components/tabs/ReferralsTab';
import { HoldersTab } from '@/components/tabs/HoldersTab';
import { getParam, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

function TitleForTab(tab: string): string {
    switch (tab) {
        case SHORT_VALUES.TAB_VOLUME: return 'Volume';
        case SHORT_VALUES.TAB_REVENUE: return 'Revenue';
        case SHORT_VALUES.TAB_USERS: return 'Users';
        case SHORT_VALUES.TAB_COUNT: return 'Count';
        case SHORT_VALUES.TAB_REFERRALS: return 'Referrals';
        case SHORT_VALUES.TAB_HOLDERS: return 'Holders';
        case SHORT_VALUES.TAB_OVERVIEW:
        default: return 'Dashboard';
    }
}

function DashboardContent() {
    const searchParams = useSearchParams();
    const activeTab = getParam(searchParams, SHORT_PARAMS.TAB) || SHORT_VALUES.TAB_OVERVIEW;
    const range = getParam(searchParams, SHORT_PARAMS.RANGE) || SHORT_VALUES.RANGE_ALL;
    const startDate = getParam(searchParams, SHORT_PARAMS.START_DATE);
    const endDate = getParam(searchParams, SHORT_PARAMS.END_DATE);

    const granularityParam = getParam(searchParams, SHORT_PARAMS.GRANULARITY);
    let defaultGranularity: string = SHORT_VALUES.GRAN_DAY;
    if (range === SHORT_VALUES.RANGE_1D) defaultGranularity = SHORT_VALUES.GRAN_HOUR;
    else if (
        range === SHORT_VALUES.RANGE_90D ||
        range === SHORT_VALUES.RANGE_YTD ||
        range === SHORT_VALUES.RANGE_1Y ||
        range === SHORT_VALUES.RANGE_ALL
    ) {
        defaultGranularity = SHORT_VALUES.GRAN_WEEK;
    }
    const granularity = granularityParam || defaultGranularity;

    const prevParams = useRef<string>('');
    const [showLoader, setShowLoader] = useState(false);

    useEffect(() => {
        const currentParams = searchParams.toString();
        if (prevParams.current && prevParams.current !== currentParams) {
            setShowLoader(true);
            const timer = setTimeout(() => setShowLoader(false), 500);
            return () => clearTimeout(timer);
        }
        prevParams.current = currentParams;
    }, [searchParams]);

    const hideControls = activeTab === SHORT_VALUES.TAB_HOLDERS;
    const hideGranularity = activeTab === SHORT_VALUES.TAB_REFERRALS;

    return (
        <div className="min-h-screen bg-[var(--bg-page)]">
            <Sidebar />
            <main
                className="min-h-screen pr-5 py-4 transition-[padding-left] duration-200"
                style={{ paddingLeft: 'var(--content-offset, 336px)' }}
            >
                <div className="flex flex-col gap-5">
                    {/* Header row */}
                    <header className="flex items-center justify-between gap-4 flex-wrap">
                        <h1 className="font-medium text-[28px] leading-[34px] tracking-[-0.02em] text-[var(--text-primary)]">
                            {TitleForTab(activeTab)}
                        </h1>
                        <div className="flex items-center gap-3.5 flex-wrap justify-end">
                            {!hideControls && (
                                <>
                                    <DateRangeSelector />
                                    {!hideGranularity && <GranularitySelector />}
                                    {showLoader && (
                                        <div className="size-4 border-2 border-[var(--brand-blue-light)] border-t-transparent rounded-full animate-spin" />
                                    )}
                                </>
                            )}
                        </div>
                    </header>

                    {/* Tab content */}
                    <div>
                        {activeTab === SHORT_VALUES.TAB_OVERVIEW && (
                            <OverviewTab range={range} startDate={startDate} endDate={endDate} granularity={granularity} />
                        )}
                        {activeTab === SHORT_VALUES.TAB_VOLUME && (
                            <SwapVolumeTab range={range} startDate={startDate} endDate={endDate} granularity={granularity} />
                        )}
                        {activeTab === SHORT_VALUES.TAB_REVENUE && (
                            <RevenueTab range={range} startDate={startDate} endDate={endDate} granularity={granularity} />
                        )}
                        {activeTab === SHORT_VALUES.TAB_USERS && (
                            <UsersTab range={range} startDate={startDate} endDate={endDate} granularity={granularity} />
                        )}
                        {activeTab === SHORT_VALUES.TAB_COUNT && (
                            <CountTab range={range} startDate={startDate} endDate={endDate} granularity={granularity} />
                        )}
                        {activeTab === SHORT_VALUES.TAB_REFERRALS && (
                            <ReferralsTab range={range} startDate={startDate} endDate={endDate} granularity={granularity} />
                        )}
                        {activeTab === SHORT_VALUES.TAB_HOLDERS && <HoldersTab />}
                    </div>
                </div>
            </main>
        </div>
    );
}

export default function Dashboard() {
    return (
        <Suspense
            fallback={
                <div className="min-h-screen bg-[var(--bg-page)] flex items-center justify-center text-[var(--text-primary)] text-xl">
                    Loading…
                </div>
            }
        >
            <DashboardContent />
        </Suspense>
    );
}

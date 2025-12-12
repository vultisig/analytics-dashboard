'use client';

import { Suspense, useTransition, useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import SystemStatus from '@/components/SystemStatus';
import { DateRangeSelector } from '@/components/DateRangeSelector';
import { GranularitySelector } from '@/components/GranularitySelector';
import { TabNavigator } from '@/components/TabNavigator';
import { OverviewTab } from '@/components/tabs/OverviewTab';
import { SwapVolumeTab } from '@/components/tabs/SwapVolumeTab';
import { RevenueTab } from '@/components/tabs/RevenueTab';
import { UsersTab } from '@/components/tabs/UsersTab';
import { CountTab } from '@/components/tabs/CountTab';
import { ReferralsTab } from '@/components/tabs/ReferralsTab';
import { HoldersTab } from '@/components/tabs/HoldersTab';
import { getParam, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

function DashboardContent() {
  const searchParams = useSearchParams();
  const activeTab = getParam(searchParams, SHORT_PARAMS.TAB) || SHORT_VALUES.TAB_OVERVIEW;
  const range = getParam(searchParams, SHORT_PARAMS.RANGE) || SHORT_VALUES.RANGE_ALL;
  const startDate = getParam(searchParams, SHORT_PARAMS.START_DATE);
  const endDate = getParam(searchParams, SHORT_PARAMS.END_DATE);

  // Calculate default granularity based on range (must match GranularitySelector logic)
  const granularityParam = getParam(searchParams, SHORT_PARAMS.GRANULARITY);
  let defaultGranularity: string = SHORT_VALUES.GRAN_DAY;
  if (range === SHORT_VALUES.RANGE_1D) {
    defaultGranularity = SHORT_VALUES.GRAN_HOUR;
  } else if (
    range === SHORT_VALUES.RANGE_90D ||
    range === SHORT_VALUES.RANGE_YTD ||
    range === SHORT_VALUES.RANGE_1Y ||
    range === SHORT_VALUES.RANGE_ALL
  ) {
    defaultGranularity = SHORT_VALUES.GRAN_WEEK;
  }
  const granularity = granularityParam || defaultGranularity;

  // Track if we're loading data due to param changes
  const [isTransitioning, setIsTransitioning] = useTransition();
  const prevParams = useRef<string>('');
  const [showLoader, setShowLoader] = useState(false);

  useEffect(() => {
    const currentParams = searchParams.toString();
    if (prevParams.current && prevParams.current !== currentParams) {
      setShowLoader(true);
      // Hide loader after a short delay to allow data fetching
      const timer = setTimeout(() => setShowLoader(false), 500);
      return () => clearTimeout(timer);
    }
    prevParams.current = currentParams;
  }, [searchParams]);

  return (
    <div className="min-h-screen bg-gradient-to-br from-[#020817] via-[#0B1120] to-[#020817]">
      <main className="container mx-auto px-3 py-4 md:p-6 space-y-4 md:space-y-6">
        {/* Header */}
        <div className="flex flex-col gap-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <img
                src="https://vultisig.com/logo.svg"
                alt="Vultisig Logo"
                className="w-10 h-10"
                onError={(e) => {
                  // Fallback if logo doesn't load
                  e.currentTarget.style.display = 'none';
                }}
              />
              <h1 className="text-3xl font-bold text-white">
                <span className="bg-gradient-to-r from-cyan-400 to-teal-400 bg-clip-text text-transparent">
                  Vultisig
                </span>
                {' '}Analytics
              </h1>
            </div>
            <SystemStatus />
          </div>

          {/* Tab Navigator */}
          <TabNavigator />

          {/* Controls - hidden on Holders tab which doesn't use date ranges */}
          {activeTab !== SHORT_VALUES.TAB_HOLDERS && (
            <div className="flex flex-col md:flex-row items-start md:items-center gap-4">
              <DateRangeSelector />
              <div className="flex items-center gap-2">
                <GranularitySelector />
                {/* Loading spinner - stays inline with granularity selector */}
                {showLoader && (
                  <div className="w-4 h-4 border-2 border-cyan-500 border-t-transparent rounded-full animate-spin"></div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Tab Content */}
        <div className="mt-6">
          {activeTab === SHORT_VALUES.TAB_OVERVIEW && (
            <OverviewTab
              range={range}
              startDate={startDate}
              endDate={endDate}
              granularity={granularity}
            />
          )}

          {activeTab === SHORT_VALUES.TAB_VOLUME && (
            <SwapVolumeTab
              range={range}
              startDate={startDate}
              endDate={endDate}
              granularity={granularity}
            />
          )}

          {activeTab === SHORT_VALUES.TAB_REVENUE && (
            <RevenueTab
              range={range}
              startDate={startDate}
              endDate={endDate}
              granularity={granularity}
            />
          )}

          {activeTab === SHORT_VALUES.TAB_USERS && (
            <UsersTab
              range={range}
              startDate={startDate}
              endDate={endDate}
              granularity={granularity}
            />
          )}

          {activeTab === SHORT_VALUES.TAB_COUNT && (
            <CountTab
              range={range}
              startDate={startDate}
              endDate={endDate}
              granularity={granularity}
            />
          )}

          {activeTab === SHORT_VALUES.TAB_REFERRALS && (
            <ReferralsTab
              range={range}
              startDate={startDate}
              endDate={endDate}
              granularity={granularity}
            />
          )}

          {activeTab === SHORT_VALUES.TAB_HOLDERS && (
            <HoldersTab />
          )}
        </div>
      </main>
    </div>
  );
}

export default function Dashboard() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-gradient-to-br from-[#020817] via-[#0B1120] to-[#020817] flex items-center justify-center">
        <div className="text-white text-xl">Loading...</div>
      </div>
    }>
      <DashboardContent />
    </Suspense>
  );
}

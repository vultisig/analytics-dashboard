'use client';

import { useRouter, useSearchParams } from 'next/navigation';
import { LayoutDashboard, ArrowRightLeft, Wallet, Users, Hash, UserPlus, Gem } from 'lucide-react';
import { LucideIcon } from 'lucide-react';
import { getParam, paramsToObject, buildParams, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

interface Tab {
  id: string;
  label: string;
  icon: LucideIcon;
}

const tabs: Tab[] = [
  { id: SHORT_VALUES.TAB_OVERVIEW, label: 'Overview', icon: LayoutDashboard },
  { id: SHORT_VALUES.TAB_VOLUME, label: 'Volume', icon: ArrowRightLeft },
  { id: SHORT_VALUES.TAB_REVENUE, label: 'Revenue', icon: Wallet },
  { id: SHORT_VALUES.TAB_USERS, label: 'Users', icon: Users },
  { id: SHORT_VALUES.TAB_COUNT, label: 'Count', icon: Hash },
  { id: SHORT_VALUES.TAB_REFERRALS, label: 'Referrals', icon: UserPlus },
  { id: SHORT_VALUES.TAB_HOLDERS, label: 'Holders', icon: Gem },
];

export function TabNavigator() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const activeTab = getParam(searchParams, SHORT_PARAMS.TAB) || SHORT_VALUES.TAB_OVERVIEW;

  const handleTabChange = (tabId: string) => {
    const currentParams = paramsToObject(searchParams);
    const newParams = buildParams({
      ...currentParams,
      [SHORT_PARAMS.TAB]: tabId,
    });

    router.push(`?${newParams.toString()}`);
  };

  const handleKeyDown = (e: React.KeyboardEvent, tabId: string, index: number) => {
    if (e.key === 'ArrowRight') {
      e.preventDefault();
      const nextIndex = (index + 1) % tabs.length;
      const nextTab = tabs[nextIndex];
      handleTabChange(nextTab.id);
      // Focus next tab button
      setTimeout(() => {
        document.getElementById(`tab-${nextTab.id}`)?.focus();
      }, 50);
    } else if (e.key === 'ArrowLeft') {
      e.preventDefault();
      const prevIndex = (index - 1 + tabs.length) % tabs.length;
      const prevTab = tabs[prevIndex];
      handleTabChange(prevTab.id);
      // Focus previous tab button
      setTimeout(() => {
        document.getElementById(`tab-${prevTab.id}`)?.focus();
      }, 50);
    }
  };

  return (
    <div
      role="tablist"
      className="glass-card rounded-lg p-1 flex gap-0.5 md:gap-1 will-change-blur max-w-full"
      aria-label="Dashboard navigation tabs"
    >
      {tabs.map((tab, index) => {
        const Icon = tab.icon;
        const isActive = activeTab === tab.id;

        return (
          <button
            key={tab.id}
            id={`tab-${tab.id}`}
            type="button"
            role="tab"
            aria-selected={isActive ? 'true' : 'false'}
            aria-controls={`${tab.id}-panel`}
            tabIndex={isActive ? 0 : -1}
            onClick={() => handleTabChange(tab.id)}
            onKeyDown={(e) => handleKeyDown(e, tab.id, index)}
            className={`
              flex items-center justify-center gap-1 md:gap-2 px-2 md:px-4 py-1.5 md:py-2.5 rounded-md text-[11px] sm:text-xs md:text-sm font-medium
              transition-all shrink-0
              ${isActive
                ? 'bg-gradient-to-r from-cyan-500 to-blue-500 text-white shadow-lg shadow-cyan-500/50'
                : 'text-slate-400 hover:text-white hover:bg-white/5'
              }
            `}
          >
            <Icon className="w-4 h-4 shrink-0" />
            {/* On mobile: only show label for active tab. On md+: always show label */}
            <span className={`${isActive ? 'inline' : 'hidden'} md:inline`}>{tab.label}</span>
          </button>
        );
      })}
    </div>
  );
}

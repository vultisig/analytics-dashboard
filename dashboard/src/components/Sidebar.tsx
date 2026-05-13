'use client';

import { useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import type { ComponentType } from 'react';
import {
    IconSquareGridCircle,
    IconArrowLeftRight,
    IconWallet4,
    IconPeopleCopy,
    IconHashtag,
    IconPeopleAdded,
    IconTrophyV,
    IconLayoutLeft,
    IconLayoutLeftExpand,
} from '@/icons';
import SystemStatus from './SystemStatus';
import { getParam, paramsToObject, buildParams, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

interface NavItem {
    id: string;
    label: string;
    icon: ComponentType<{ size?: number; className?: string }>;
}

const NAV_ITEMS: NavItem[] = [
    { id: SHORT_VALUES.TAB_OVERVIEW, label: 'Overview', icon: IconSquareGridCircle },
    { id: SHORT_VALUES.TAB_VOLUME, label: 'Volume', icon: IconArrowLeftRight },
    { id: SHORT_VALUES.TAB_REVENUE, label: 'Revenue', icon: IconWallet4 },
    { id: SHORT_VALUES.TAB_USERS, label: 'Users', icon: IconPeopleCopy },
    { id: SHORT_VALUES.TAB_COUNT, label: 'Count', icon: IconHashtag },
    { id: SHORT_VALUES.TAB_REFERRALS, label: 'Referrals', icon: IconPeopleAdded },
    { id: SHORT_VALUES.TAB_HOLDERS, label: 'Holders', icon: IconTrophyV },
];

const STORAGE_KEY = 'vult-analytics-sidebar-collapsed';

export function Sidebar() {
    const router = useRouter();
    const searchParams = useSearchParams();
    const activeTab = getParam(searchParams, SHORT_PARAMS.TAB) || SHORT_VALUES.TAB_OVERVIEW;

    const [collapsed, setCollapsed] = useState(false);
    const [mounted, setMounted] = useState(false);

    useEffect(() => {
        const stored = typeof window !== 'undefined' ? localStorage.getItem(STORAGE_KEY) : null;
        if (stored === 'true') setCollapsed(true);
        setMounted(true);
    }, []);

    useEffect(() => {
        if (!mounted) return;
        // Figma: sidebar is 296px wide at left:20, then a 20px gap to
        // content (main starts at 336px on a 1440-wide frame).
        const sidebarWidth = collapsed ? '76px' : '296px';
        const contentOffset = collapsed ? '116px' : '336px';
        document.documentElement.style.setProperty('--sidebar-width', sidebarWidth);
        document.documentElement.style.setProperty('--content-offset', contentOffset);
        localStorage.setItem(STORAGE_KEY, String(collapsed));
    }, [collapsed, mounted]);

    const handleNav = (id: string) => {
        const current = paramsToObject(searchParams);
        const next = buildParams({ ...current, [SHORT_PARAMS.TAB]: id });
        router.push(`?${next.toString()}`);
    };

    return (
        <aside
            className="fixed left-5 top-5 bottom-5 z-30 flex flex-col rounded-[20px] border border-[var(--border-light)] bg-[var(--surface-1)] p-5 transition-[width] duration-200"
            style={{ width: 'var(--sidebar-width, 256px)' }}
            aria-label="Primary"
        >
            {/* Logo + collapse */}
            <div className="flex items-center justify-between gap-[52px]">
                <div className="flex flex-1 min-w-0 items-center gap-[14px]">
                    <div
                        className="relative size-[38px] shrink-0 rounded-[12.56px]"
                        style={{
                            background: 'linear-gradient(180deg, var(--brand-blue-light), var(--brand-blue-deep))',
                            boxShadow: 'inset 0 0.785px 0.785px 0 rgba(255,255,255,0.35)',
                        }}
                        aria-hidden="true"
                    >
                        {/* Vultisig V mark, exported from the design library.
                            Figma positions a 21.28 px mark inside the 38 px box
                            at (8.36, 8.36) — centered. */}
                        <img
                            src="/vultisig-mark.svg"
                            alt=""
                            width={21.28}
                            height={21.28}
                            className="absolute left-[8.36px] top-[8.36px] select-none"
                            draggable={false}
                        />
                    </div>
                    {!collapsed && (
                        <p className="t-title-2 truncate">Analytics</p>
                    )}
                </div>
                <button
                    type="button"
                    onClick={() => setCollapsed((v) => !v)}
                    aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
                    className="text-[var(--text-tertiary)] hover:text-[var(--text-primary)] transition-colors"
                >
                    {collapsed
                        ? <IconLayoutLeftExpand size={20} />
                        : <IconLayoutLeft size={20} />}
                </button>
            </div>

            {/* Nav */}
            <nav role="tablist" aria-orientation="vertical" className="mt-[52px] flex-1 flex flex-col gap-2">
                {NAV_ITEMS.map((item) => {
                    const Icon = item.icon;
                    const isActive = activeTab === item.id;
                    return (
                        <button
                            key={item.id}
                            type="button"
                            role="tab"
                            aria-selected={isActive}
                            aria-controls={`${item.id}-panel`}
                            onClick={() => handleNav(item.id)}
                            title={collapsed ? item.label : undefined}
                            className={`
                                relative flex items-center gap-[10px] rounded-[12px] p-[14px] transition-colors
                                ${isActive
                                    ? 'bg-[var(--brand-blue)] text-[var(--text-primary)]'
                                    : 'text-[var(--text-secondary)] hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]'
                                }
                                ${collapsed ? 'justify-center' : ''}
                            `}
                        >
                            <Icon className="size-4 shrink-0" />
                            {!collapsed && (
                                <span className="text-sm font-medium leading-[18px]">{item.label}</span>
                            )}
                        </button>
                    );
                })}
            </nav>

            {/* System status */}
            <div className={`mt-[52px] flex ${collapsed ? 'justify-center' : ''}`}>
                <SystemStatus compact={collapsed} />
            </div>
        </aside>
    );
}

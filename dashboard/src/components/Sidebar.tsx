'use client';

import { useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import type { ComponentType, MouseEvent } from 'react';
import {
    IconSquareGridCircle,
    IconArrowLeftRight,
    IconWallet4,
    IconPeopleCopy,
    IconHashtag,
    IconPeopleAdded,
    IconTrophyV,
    IconLayoutLeft,
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
        // Figma: sidebar is 296 px wide at left:20, then a 20 px gap to
        // content (main starts at 336 px on a 1440-wide frame). Collapsed
        // sticks to a 76 px chassis that fits a 44 px nav-button square
        // (44 + 2 × 16 = 76) plus 16 px padding.
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

    // When collapsed, clicking anywhere on the aside that isn't a real
    // interactive element re-expands the panel. Buttons stopPropagation
    // so nav clicks still route normally.
    const handleSurfaceClick = (e: MouseEvent<HTMLElement>) => {
        if (!collapsed) return;
        const target = e.target as HTMLElement;
        if (target.closest('button, a, input, [role="tab"]')) return;
        setCollapsed(false);
    };

    return (
        <aside
            className={`fixed left-5 top-5 bottom-5 z-30 flex flex-col rounded-[20px] border border-[var(--border-light)] bg-[var(--surface-1)] py-5 transition-[width] duration-200 ${collapsed ? 'px-4 cursor-pointer' : 'px-5'}`}
            style={{ width: 'var(--sidebar-width, 296px)' }}
            aria-label="Primary"
            onClick={handleSurfaceClick}
            role={collapsed ? 'button' : undefined}
            aria-expanded={!collapsed}
        >
            {/* Logo + collapse — collapse button shown only when expanded */}
            <div className={`flex items-center gap-[14px] ${collapsed ? 'justify-center' : 'justify-between'}`}>
                <div className={`flex min-w-0 items-center gap-[14px] ${collapsed ? '' : 'flex-1'}`}>
                    <div
                        className="relative size-[38px] shrink-0 rounded-[12.56px]"
                        style={{
                            background: 'linear-gradient(180deg, var(--brand-blue-light), var(--brand-blue-deep))',
                            boxShadow: 'inset 0 0.785px 0.785px 0 rgba(255,255,255,0.35)',
                        }}
                        aria-hidden="true"
                    >
                        {/* Vultisig V mark. The SVG's natural ratio is
                            21.33 × 18.55 (wider than tall), so render it
                            with explicit non-square dimensions and centre
                            it inside the 38 px square. */}
                        <img
                            src="/vultisig-mark.svg"
                            alt=""
                            width={21.33}
                            height={18.55}
                            className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 select-none"
                            draggable={false}
                        />
                    </div>
                    {!collapsed && (
                        <p className="font-medium text-[18px] leading-[22px] tracking-[-0.3px] text-[var(--text-primary)] truncate">
                            Vultisig Analytics
                        </p>
                    )}
                </div>
                {!collapsed && (
                    <button
                        type="button"
                        onClick={(e) => {
                            e.stopPropagation();
                            setCollapsed(true);
                        }}
                        aria-label="Collapse sidebar"
                        className="text-[var(--text-tertiary)] hover:text-[var(--text-primary)] transition-colors"
                    >
                        <IconLayoutLeft size={20} />
                    </button>
                )}
            </div>

            {/* Nav */}
            <nav
                role="tablist"
                aria-orientation="vertical"
                className={`mt-[52px] flex flex-1 flex-col gap-2 ${collapsed ? 'items-center' : ''}`}
            >
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
                            onClick={(e) => {
                                e.stopPropagation();
                                handleNav(item.id);
                            }}
                            title={collapsed ? item.label : undefined}
                            className={`
                                relative flex items-center rounded-[12px] transition-colors
                                ${isActive
                                    ? 'bg-[var(--brand-blue)] text-[var(--text-primary)]'
                                    : 'text-[var(--text-secondary)] hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)]'
                                }
                                ${collapsed
                                    ? 'size-11 shrink-0 justify-center'
                                    : 'w-full gap-[10px] p-[14px]'
                                }
                            `}
                        >
                            <Icon className="size-4 shrink-0" />
                            {!collapsed && (
                                <span className="t-button-s">{item.label}</span>
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

'use client';

import { useState, useEffect, useTransition, useRef } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import { IconCalendar3, IconChevronDownSmall } from '@/icons';
import { DateRangeType } from '@/lib/dateUtils';
import { getParam, paramsToObject, buildParams, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

const SHORT_TO_DATE_RANGE: Record<string, DateRangeType> = {
    [SHORT_VALUES.RANGE_ALL]: 'all',
    [SHORT_VALUES.RANGE_1D]: '1d',
    [SHORT_VALUES.RANGE_7D]: '7d',
    [SHORT_VALUES.RANGE_30D]: '30d',
    [SHORT_VALUES.RANGE_90D]: '90d',
    [SHORT_VALUES.RANGE_YTD]: 'ytd',
    [SHORT_VALUES.RANGE_1Y]: '1y',
    [SHORT_VALUES.RANGE_CUSTOM]: 'custom',
};

const DATE_RANGE_TO_SHORT: Record<DateRangeType, string> = Object.fromEntries(
    Object.entries(SHORT_TO_DATE_RANGE).map(([k, v]) => [v, k])
) as Record<DateRangeType, string>;

const PRESETS: { label: string; value: DateRangeType }[] = [
    { label: 'All time', value: 'all' },
    { label: 'Last 24 hours', value: '1d' },
    { label: 'Last 7 days', value: '7d' },
    { label: 'Last 30 days', value: '30d' },
    { label: 'Last 90 days', value: '90d' },
    { label: 'Year to date', value: 'ytd' },
    { label: 'Last year', value: '1y' },
];

const SHORT_LABELS: Record<DateRangeType, string> = {
    all: 'All',
    '1d': '1D',
    '7d': '7D',
    '30d': '30D',
    '90d': '90D',
    ytd: 'YTD',
    '1y': '1Y',
    custom: 'Custom',
};

export function DateRangeSelector() {
    const searchParams = useSearchParams();
    const router = useRouter();
    const [, startTransition] = useTransition();
    const containerRef = useRef<HTMLDivElement>(null);

    const currentRangeShort = getParam(searchParams, SHORT_PARAMS.RANGE) || SHORT_VALUES.RANGE_ALL;
    const currentRange = SHORT_TO_DATE_RANGE[currentRangeShort] || 'all';
    const startDateParam = getParam(searchParams, SHORT_PARAMS.START_DATE);
    const endDateParam = getParam(searchParams, SHORT_PARAMS.END_DATE);

    const [isOpen, setIsOpen] = useState(false);
    const [customStart, setCustomStart] = useState('');
    const [customEnd, setCustomEnd] = useState('');

    useEffect(() => {
        if (startDateParam) setCustomStart(startDateParam.split('T')[0]);
        if (endDateParam) setCustomEnd(endDateParam.split('T')[0]);
    }, [startDateParam, endDateParam]);

    useEffect(() => {
        const handler = (e: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
                setIsOpen(false);
            }
        };
        if (isOpen) document.addEventListener('mousedown', handler);
        return () => document.removeEventListener('mousedown', handler);
    }, [isOpen]);

    const applyRange = (value: DateRangeType, opts?: { startDate?: string; endDate?: string }) => {
        startTransition(() => {
            const current = paramsToObject(searchParams);
            const rangeShort = DATE_RANGE_TO_SHORT[value];

            let newGranularity = current[SHORT_PARAMS.GRANULARITY];
            if (rangeShort === SHORT_VALUES.RANGE_1D) newGranularity = SHORT_VALUES.GRAN_HOUR;
            else if (
                rangeShort === SHORT_VALUES.RANGE_90D ||
                rangeShort === SHORT_VALUES.RANGE_YTD ||
                rangeShort === SHORT_VALUES.RANGE_1Y ||
                rangeShort === SHORT_VALUES.RANGE_ALL
            ) newGranularity = SHORT_VALUES.GRAN_WEEK;
            else if (
                rangeShort === SHORT_VALUES.RANGE_7D ||
                rangeShort === SHORT_VALUES.RANGE_30D
            ) newGranularity = SHORT_VALUES.GRAN_DAY;

            const next: Record<string, string | undefined> = {
                [SHORT_PARAMS.TAB]: current[SHORT_PARAMS.TAB],
                [SHORT_PARAMS.GRANULARITY]: newGranularity,
                [SHORT_PARAMS.RANGE]: rangeShort,
            };
            if (value === 'custom') {
                next[SHORT_PARAMS.START_DATE] = opts?.startDate;
                next[SHORT_PARAMS.END_DATE] = opts?.endDate;
            }

            router.replace(`?${buildParams(next).toString()}`, { scroll: false });
        });
        setIsOpen(false);
    };

    const buttonLabel = currentRange === 'custom'
        ? `${customStart || '…'} → ${customEnd || '…'}`
        : SHORT_LABELS[currentRange];

    return (
        <div className="relative" ref={containerRef}>
            <button
                type="button"
                aria-expanded={isOpen}
                aria-haspopup="listbox"
                onClick={() => setIsOpen((v) => !v)}
                className="pill"
            >
                <IconCalendar3 />
                <span>Period: <span className="text-[var(--text-primary)]">{buttonLabel}</span></span>
                <IconChevronDownSmall className={`transition-transform ${isOpen ? 'rotate-180' : ''}`} />
            </button>

            {isOpen && (
                <div
                    role="listbox"
                    className="absolute right-0 top-full mt-2 z-50 w-[280px] rounded-2xl border border-[var(--border-normal)] bg-[var(--surface-1)] shadow-2xl p-2"
                >
                    {PRESETS.map((preset) => {
                        const isActive = currentRange === preset.value;
                        return (
                            <button
                                key={preset.value}
                                type="button"
                                role="option"
                                aria-selected={isActive}
                                onClick={() => applyRange(preset.value)}
                                className={`flex w-full items-center justify-between rounded-lg px-3 py-2 text-sm transition-colors ${
                                    isActive
                                        ? 'bg-[var(--brand-blue)] text-[var(--text-primary)]'
                                        : 'text-[var(--text-secondary)] hover:bg-[var(--surface-2)]'
                                }`}
                            >
                                <span>{preset.label}</span>
                                <span className="text-xs text-[var(--text-tertiary)]">{SHORT_LABELS[preset.value]}</span>
                            </button>
                        );
                    })}
                    <div className="my-2 border-t border-[var(--border-light)]" />
                    <div className="px-2 py-1.5 space-y-2">
                        <p className="text-xs text-[var(--text-tertiary)]">Custom range</p>
                        <div className="grid grid-cols-2 gap-2">
                            <input
                                type="date"
                                value={customStart}
                                onChange={(e) => setCustomStart(e.target.value)}
                                aria-label="Start date"
                                className="w-full rounded-md border border-[var(--border-light)] bg-[var(--surface-2)] px-2 py-1.5 text-xs text-[var(--text-primary)] focus:border-[var(--brand-blue)] focus:outline-none"
                            />
                            <input
                                type="date"
                                value={customEnd}
                                onChange={(e) => setCustomEnd(e.target.value)}
                                aria-label="End date"
                                className="w-full rounded-md border border-[var(--border-light)] bg-[var(--surface-2)] px-2 py-1.5 text-xs text-[var(--text-primary)] focus:border-[var(--brand-blue)] focus:outline-none"
                            />
                        </div>
                        <button
                            type="button"
                            onClick={() => customStart && customEnd && applyRange('custom', { startDate: customStart, endDate: customEnd })}
                            disabled={!customStart || !customEnd}
                            className="w-full rounded-md bg-[var(--brand-blue)] py-1.5 text-xs font-medium text-[var(--text-primary)] hover:bg-[var(--brand-blue-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                        >
                            Apply
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}

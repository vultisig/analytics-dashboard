'use client';

import { useEffect, useState, useTransition, useRef } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { BarChart3, ChevronDown } from 'lucide-react';
import { getParam, paramsToObject, buildParams, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

type Granularity = 'h' | 'd' | 'w' | 'm';

const GRANULARITY_OPTIONS: { value: Granularity; label: string; short: string }[] = [
    { value: SHORT_VALUES.GRAN_HOUR, label: 'Hourly', short: 'H' },
    { value: SHORT_VALUES.GRAN_DAY, label: 'Daily', short: 'D' },
    { value: SHORT_VALUES.GRAN_WEEK, label: 'Weekly', short: 'W' },
    { value: SHORT_VALUES.GRAN_MONTH, label: 'Monthly', short: 'M' },
];

export function GranularitySelector() {
    const router = useRouter();
    const searchParams = useSearchParams();
    const [, startTransition] = useTransition();
    const containerRef = useRef<HTMLDivElement>(null);
    const [isOpen, setIsOpen] = useState(false);

    const granularityParam = getParam(searchParams, SHORT_PARAMS.GRANULARITY) as Granularity;
    const range = getParam(searchParams, SHORT_PARAMS.RANGE) || SHORT_VALUES.RANGE_ALL;
    const startDate = getParam(searchParams, SHORT_PARAMS.START_DATE);
    const endDate = getParam(searchParams, SHORT_PARAMS.END_DATE);

    const getCustomRangeDays = (): number | null => {
        if (range !== SHORT_VALUES.RANGE_CUSTOM || !startDate || !endDate) return null;
        const start = new Date(startDate);
        const end = new Date(endDate);
        return Math.ceil(Math.abs(end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24));
    };

    const getValidGranularities = (r: string): Granularity[] => {
        switch (r) {
            case SHORT_VALUES.RANGE_1D: return [SHORT_VALUES.GRAN_HOUR];
            case SHORT_VALUES.RANGE_7D: return [SHORT_VALUES.GRAN_HOUR, SHORT_VALUES.GRAN_DAY];
            case SHORT_VALUES.RANGE_30D: return [SHORT_VALUES.GRAN_DAY, SHORT_VALUES.GRAN_WEEK];
            case SHORT_VALUES.RANGE_90D:
            case SHORT_VALUES.RANGE_YTD:
            case SHORT_VALUES.RANGE_1Y:
            case SHORT_VALUES.RANGE_ALL:
                return [SHORT_VALUES.GRAN_DAY, SHORT_VALUES.GRAN_WEEK, SHORT_VALUES.GRAN_MONTH];
            case SHORT_VALUES.RANGE_CUSTOM: {
                const days = getCustomRangeDays();
                if (days === null) return [SHORT_VALUES.GRAN_DAY, SHORT_VALUES.GRAN_WEEK, SHORT_VALUES.GRAN_MONTH];
                if (days <= 1) return [SHORT_VALUES.GRAN_HOUR];
                if (days <= 7) return [SHORT_VALUES.GRAN_HOUR, SHORT_VALUES.GRAN_DAY];
                if (days <= 30) return [SHORT_VALUES.GRAN_DAY, SHORT_VALUES.GRAN_WEEK];
                return [SHORT_VALUES.GRAN_DAY, SHORT_VALUES.GRAN_WEEK, SHORT_VALUES.GRAN_MONTH];
            }
            default:
                return [SHORT_VALUES.GRAN_DAY, SHORT_VALUES.GRAN_WEEK, SHORT_VALUES.GRAN_MONTH];
        }
    };

    const validGranularities = getValidGranularities(range);

    let defaultGranularity: Granularity = SHORT_VALUES.GRAN_DAY;
    if (range === SHORT_VALUES.RANGE_1D) defaultGranularity = SHORT_VALUES.GRAN_HOUR;
    else if (
        range === SHORT_VALUES.RANGE_90D ||
        range === SHORT_VALUES.RANGE_YTD ||
        range === SHORT_VALUES.RANGE_1Y ||
        range === SHORT_VALUES.RANGE_ALL
    ) defaultGranularity = SHORT_VALUES.GRAN_WEEK;

    const currentGranularity = granularityParam || defaultGranularity;
    const currentOption = GRANULARITY_OPTIONS.find((o) => o.value === currentGranularity) ?? GRANULARITY_OPTIONS[1];

    useEffect(() => {
        if (!validGranularities.includes(currentGranularity)) {
            let newGranularity = validGranularities[0];
            if (range === SHORT_VALUES.RANGE_ALL) newGranularity = SHORT_VALUES.GRAN_WEEK;
            if (
                range === SHORT_VALUES.RANGE_90D ||
                range === SHORT_VALUES.RANGE_YTD ||
                range === SHORT_VALUES.RANGE_1Y
            ) newGranularity = SHORT_VALUES.GRAN_WEEK;
            if (range === SHORT_VALUES.RANGE_30D) newGranularity = SHORT_VALUES.GRAN_DAY;
            if (range === SHORT_VALUES.RANGE_7D) newGranularity = SHORT_VALUES.GRAN_DAY;
            if (range === SHORT_VALUES.RANGE_1D) newGranularity = SHORT_VALUES.GRAN_HOUR;
            if (range === SHORT_VALUES.RANGE_CUSTOM) {
                const days = getCustomRangeDays();
                if (days !== null && days <= 1) newGranularity = SHORT_VALUES.GRAN_HOUR;
                else newGranularity = SHORT_VALUES.GRAN_DAY;
            }

            const current = paramsToObject(searchParams);
            router.replace(`?${buildParams({ ...current, [SHORT_PARAMS.GRANULARITY]: newGranularity }).toString()}`);
        }
    }, [range, startDate, endDate, currentGranularity, validGranularities, router, searchParams]);

    useEffect(() => {
        const handler = (e: MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
                setIsOpen(false);
            }
        };
        if (isOpen) document.addEventListener('mousedown', handler);
        return () => document.removeEventListener('mousedown', handler);
    }, [isOpen]);

    const handleSelect = (value: Granularity) => {
        startTransition(() => {
            const current = paramsToObject(searchParams);
            router.replace(`?${buildParams({ ...current, [SHORT_PARAMS.GRANULARITY]: value }).toString()}`, { scroll: false });
        });
        setIsOpen(false);
    };

    return (
        <div className="relative" ref={containerRef}>
            <button
                type="button"
                aria-expanded={isOpen}
                aria-haspopup="listbox"
                onClick={() => setIsOpen((v) => !v)}
                className="pill"
            >
                <BarChart3 className="size-4" />
                <span>Granularity: <span className="text-[var(--text-primary)]">{currentOption.label}</span></span>
                <ChevronDown className={`size-4 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
            </button>

            {isOpen && (
                <div
                    role="listbox"
                    className="absolute right-0 top-full mt-2 z-50 w-[200px] rounded-2xl border border-[var(--border-normal)] bg-[var(--surface-1)] shadow-2xl p-2"
                >
                    {GRANULARITY_OPTIONS.map((option) => {
                        const isValid = validGranularities.includes(option.value);
                        const isActive = currentGranularity === option.value;
                        return (
                            <button
                                key={option.value}
                                type="button"
                                role="option"
                                aria-selected={isActive}
                                onClick={() => isValid && handleSelect(option.value)}
                                disabled={!isValid}
                                className={`flex w-full items-center justify-between rounded-lg px-3 py-2 text-sm transition-colors ${
                                    isActive
                                        ? 'bg-[var(--brand-blue)] text-[var(--text-primary)]'
                                        : isValid
                                            ? 'text-[var(--text-secondary)] hover:bg-[var(--surface-2)]'
                                            : 'text-[var(--text-tertiary)]/40 cursor-not-allowed'
                                }`}
                            >
                                <span>{option.label}</span>
                                <span className="text-xs text-[var(--text-tertiary)]">{option.short}</span>
                            </button>
                        );
                    })}
                </div>
            )}
        </div>
    );
}

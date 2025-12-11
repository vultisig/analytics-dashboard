'use client';

import { useRouter, useSearchParams } from 'next/navigation';
import { BarChart3 } from 'lucide-react';
import { useEffect, useTransition } from 'react';
import { getParam, paramsToObject, buildParams, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

type Granularity = 'h' | 'd' | 'w' | 'm';

const GRANULARITY_OPTIONS: { value: Granularity; label: string }[] = [
    { value: SHORT_VALUES.GRAN_HOUR, label: 'H' },
    { value: SHORT_VALUES.GRAN_DAY, label: 'D' },
    { value: SHORT_VALUES.GRAN_WEEK, label: 'W' },
    { value: SHORT_VALUES.GRAN_MONTH, label: 'M' },
];

export function GranularitySelector() {
    const router = useRouter();
    const searchParams = useSearchParams();
    const [isPending, startTransition] = useTransition();
    const granularityParam = getParam(searchParams, SHORT_PARAMS.GRANULARITY) as Granularity;
    const range = getParam(searchParams, SHORT_PARAMS.RANGE) || SHORT_VALUES.RANGE_ALL;
    const startDate = getParam(searchParams, SHORT_PARAMS.START_DATE);
    const endDate = getParam(searchParams, SHORT_PARAMS.END_DATE);

    // Calculate days between custom dates
    const getCustomRangeDays = (): number | null => {
        if (range !== SHORT_VALUES.RANGE_CUSTOM || !startDate || !endDate) return null;
        const start = new Date(startDate);
        const end = new Date(endDate);
        const diffTime = Math.abs(end.getTime() - start.getTime());
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
        return diffDays;
    };

    // Define valid granularities for each range
    const getValidGranularities = (range: string): Granularity[] => {
        switch (range) {
            case SHORT_VALUES.RANGE_1D:
                // 1D: Only hourly to avoid single-column chart
                return [SHORT_VALUES.GRAN_HOUR];
            case SHORT_VALUES.RANGE_7D:
                // 7D: Hour and Day only (no weekly to avoid single-column)
                return [SHORT_VALUES.GRAN_HOUR, SHORT_VALUES.GRAN_DAY];
            case SHORT_VALUES.RANGE_30D:
                // 30D: Day and Week only (no monthly to avoid single-column)
                return [SHORT_VALUES.GRAN_DAY, SHORT_VALUES.GRAN_WEEK];
            case SHORT_VALUES.RANGE_90D:
            case SHORT_VALUES.RANGE_YTD:
            case SHORT_VALUES.RANGE_1Y:
            case SHORT_VALUES.RANGE_ALL:
                return [SHORT_VALUES.GRAN_DAY, SHORT_VALUES.GRAN_WEEK, SHORT_VALUES.GRAN_MONTH];
            case SHORT_VALUES.RANGE_CUSTOM: {
                const days = getCustomRangeDays();
                if (days === null) return [SHORT_VALUES.GRAN_DAY, SHORT_VALUES.GRAN_WEEK, SHORT_VALUES.GRAN_MONTH];
                // Custom range validation: prevent single-column charts
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

    // Auto-detect default granularity if none selected or current is invalid
    let defaultGranularity: Granularity = SHORT_VALUES.GRAN_DAY;
    if (range === SHORT_VALUES.RANGE_1D) defaultGranularity = SHORT_VALUES.GRAN_HOUR;
    else if (
        range === SHORT_VALUES.RANGE_90D ||
        range === SHORT_VALUES.RANGE_YTD ||
        range === SHORT_VALUES.RANGE_1Y ||
        range === SHORT_VALUES.RANGE_ALL
    ) {
        defaultGranularity = SHORT_VALUES.GRAN_WEEK;
    }

    const currentGranularity = granularityParam || defaultGranularity;

    // Effect to enforce valid granularity when range changes
    useEffect(() => {
        if (!validGranularities.includes(currentGranularity)) {
            let newGranularity = validGranularities[0];

            // Prefer optimal granularity for each range
            if (range === SHORT_VALUES.RANGE_ALL) newGranularity = SHORT_VALUES.GRAN_WEEK;
            if (
                range === SHORT_VALUES.RANGE_90D ||
                range === SHORT_VALUES.RANGE_YTD ||
                range === SHORT_VALUES.RANGE_1Y
            ) {
                newGranularity = SHORT_VALUES.GRAN_WEEK;
            }
            if (range === SHORT_VALUES.RANGE_30D) newGranularity = SHORT_VALUES.GRAN_DAY;
            if (range === SHORT_VALUES.RANGE_7D) newGranularity = SHORT_VALUES.GRAN_DAY;
            if (range === SHORT_VALUES.RANGE_1D) newGranularity = SHORT_VALUES.GRAN_HOUR;

            // For custom ranges, default to day
            if (range === SHORT_VALUES.RANGE_CUSTOM) {
                const days = getCustomRangeDays();
                if (days !== null && days <= 1) newGranularity = SHORT_VALUES.GRAN_HOUR;
                else newGranularity = SHORT_VALUES.GRAN_DAY;
            }

            const currentParams = paramsToObject(searchParams);
            const newParams = buildParams({
                ...currentParams,
                [SHORT_PARAMS.GRANULARITY]: newGranularity,
            });

            router.replace(`?${newParams.toString()}`);
        }
    }, [range, startDate, endDate, currentGranularity, validGranularities, router, searchParams]);

    const handleGranularityChange = (granularity: Granularity) => {
        startTransition(() => {
            const currentParams = paramsToObject(searchParams);
            const newParams = buildParams({
                ...currentParams,
                [SHORT_PARAMS.GRANULARITY]: granularity,
            });

            router.replace(`?${newParams.toString()}`, { scroll: false });
        });
    };

    return (
        <div className="inline-flex items-center gap-1 md:gap-2 rounded-lg glass-card px-2 md:px-3 py-1 md:py-1.5 will-change-blur">
            <BarChart3 className="h-3 w-3 md:h-3.5 md:w-3.5 text-slate-400" />
            <span className="text-[10px] md:text-xs font-medium text-slate-400">Granularity:</span>
            <div className="flex items-center gap-0.5 md:gap-1">
                {GRANULARITY_OPTIONS.map((option) => {
                    const isValid = validGranularities.includes(option.value);
                    return (
                        <button
                            key={option.value}
                            type="button"
                            onClick={() => isValid && handleGranularityChange(option.value)}
                            disabled={!isValid}
                            aria-label={`Set granularity to ${option.label}`}
                            className={`
                                px-1.5 md:px-2.5 py-0.5 rounded text-[10px] md:text-xs font-semibold transition-all
                                ${currentGranularity === option.value
                                    ? 'bg-cyan-500/20 text-cyan-400 border border-cyan-500/50'
                                    : isValid
                                        ? 'text-slate-400 hover:text-slate-300 hover:bg-white/5'
                                        : 'text-slate-700 cursor-not-allowed'
                                }
                            `}
                        >
                            {option.label}
                        </button>
                    );
                })}
            </div>
        </div>
    );
}

'use client';

import { useState, useEffect, useTransition } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import { Calendar, ChevronDown } from 'lucide-react';
import { DateRangeType } from '@/lib/dateUtils';
import { getParam, paramsToObject, buildParams, SHORT_PARAMS, SHORT_VALUES } from '@/lib/urlParams';

// Map short values back to DateRangeType for compatibility
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

export function DateRangeSelector() {
    const searchParams = useSearchParams();
    const router = useRouter();
    const [, startTransition] = useTransition();
    const currentRangeShort = getParam(searchParams, SHORT_PARAMS.RANGE) || SHORT_VALUES.RANGE_ALL;
    const currentRange = SHORT_TO_DATE_RANGE[currentRangeShort] || 'all';
    const startDateParam = getParam(searchParams, SHORT_PARAMS.START_DATE);
    const endDateParam = getParam(searchParams, SHORT_PARAMS.END_DATE);

    const [isCustomOpen, setIsCustomOpen] = useState(false);
    const [customStart, setCustomStart] = useState('');
    const [customEnd, setCustomEnd] = useState('');

    useEffect(() => {
        if (startDateParam) setCustomStart(startDateParam.split('T')[0]);
        if (endDateParam) setCustomEnd(endDateParam.split('T')[0]);
    }, [startDateParam, endDateParam]);

    const ranges: { label: string; value: DateRangeType }[] = [
        { label: 'All', value: 'all' },
        { label: '1D', value: '1d' },
        { label: '7D', value: '7d' },
        { label: '30D', value: '30d' },
        { label: '90D', value: '90d' },
        { label: 'YTD', value: 'ytd' },
        { label: '1Y', value: '1y' },
    ];

    const handleCustomApply = () => {
        if (!customStart || !customEnd) return;

        startTransition(() => {
            // Get current params
            const currentParams = paramsToObject(searchParams);

            // Build new params preserving tab and granularity
            const newParams = buildParams({
                [SHORT_PARAMS.TAB]: currentParams[SHORT_PARAMS.TAB],
                [SHORT_PARAMS.GRANULARITY]: currentParams[SHORT_PARAMS.GRANULARITY],
                [SHORT_PARAMS.RANGE]: SHORT_VALUES.RANGE_CUSTOM,
                [SHORT_PARAMS.START_DATE]: customStart,
                [SHORT_PARAMS.END_DATE]: customEnd,
            });

            router.replace(`?${newParams.toString()}`, { scroll: false });
        });
        setIsCustomOpen(false);
    };

    return (
        <div className="flex items-center gap-2 relative">
            <div className="glass-card p-0.5 md:p-1 rounded-lg will-change-blur z-50">
                <div className="flex items-center gap-1 md:gap-2">
                    <div className="px-1.5 md:px-3 py-1 md:py-1.5 text-slate-400 flex items-center gap-1 md:gap-2 border-r border-slate-700/50 shrink-0">
                        <Calendar className="w-3.5 h-3.5 md:w-4 md:h-4" />
                        <span className="text-xs md:text-sm font-medium hidden sm:inline">Period</span>
                    </div>
                    <div className="flex items-center gap-0.5 md:gap-1 p-0.5 md:p-1">
                    {ranges.map((range) => {
                    const handleRangeChange = () => {
                        startTransition(() => {
                            const currentParams = paramsToObject(searchParams);
                            const rangeShort = DATE_RANGE_TO_SHORT[range.value];

                            // Auto-adjust granularity based on selected range
                            let newGranularity = currentParams[SHORT_PARAMS.GRANULARITY];
                            if (rangeShort === SHORT_VALUES.RANGE_1D) {
                                // 1D period -> default to hourly
                                newGranularity = SHORT_VALUES.GRAN_HOUR;
                            } else if (
                                rangeShort === SHORT_VALUES.RANGE_90D ||
                                rangeShort === SHORT_VALUES.RANGE_YTD ||
                                rangeShort === SHORT_VALUES.RANGE_1Y ||
                                rangeShort === SHORT_VALUES.RANGE_ALL
                            ) {
                                // Long periods -> default to weekly
                                newGranularity = SHORT_VALUES.GRAN_WEEK;
                            } else if (
                                rangeShort === SHORT_VALUES.RANGE_7D ||
                                rangeShort === SHORT_VALUES.RANGE_30D
                            ) {
                                // Medium periods -> default to daily
                                newGranularity = SHORT_VALUES.GRAN_DAY;
                            }

                            const newParams = buildParams({
                                [SHORT_PARAMS.TAB]: currentParams[SHORT_PARAMS.TAB],
                                [SHORT_PARAMS.GRANULARITY]: newGranularity,
                                [SHORT_PARAMS.RANGE]: rangeShort,
                                // Don't include startDate/endDate for preset ranges
                            });

                            router.replace(`?${newParams.toString()}`, { scroll: false });
                        });
                    };

                    return (
                        <button
                            key={range.value}
                            type="button"
                            onClick={handleRangeChange}
                            className={`px-1.5 md:px-3 py-1 md:py-1.5 text-xs md:text-sm font-medium rounded-md transition-all ${currentRange === range.value
                                ? 'bg-cyan-500/20 text-cyan-400 border border-cyan-500/50 shadow-sm shadow-cyan-500/10'
                                : 'text-slate-400 hover:text-slate-200 hover:bg-white/5'
                                }`}
                        >
                            {range.label}
                        </button>
                    );
                })}

                <div className="relative">
                    <button
                        type="button"
                        onClick={() => setIsCustomOpen(!isCustomOpen)}
                        className={`px-1.5 md:px-3 py-1 md:py-1.5 text-xs md:text-sm font-medium rounded-md transition-all flex items-center gap-0.5 md:gap-1 ${currentRange === 'custom'
                            ? 'bg-cyan-500/20 text-cyan-400 border border-cyan-500/50 shadow-sm shadow-cyan-500/10'
                            : 'text-slate-400 hover:text-slate-200 hover:bg-white/5'
                            }`}
                        aria-label="Select custom date range"
                    >
                        Custom <ChevronDown className="w-2.5 h-2.5 md:w-3 md:h-3" />
                    </button>

                    {isCustomOpen && (
                        <div className="absolute top-full right-0 mt-2 p-4 glass-card rounded-xl shadow-xl z-50 w-72">
                            <div className="space-y-4">
                                <div>
                                    <label htmlFor="custom-start-date" className="block text-xs text-slate-400 mb-1">Start Date</label>
                                    <input
                                        id="custom-start-date"
                                        type="date"
                                        value={customStart}
                                        onChange={(e) => setCustomStart(e.target.value)}
                                        className="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 text-white text-sm focus:outline-none focus:border-cyan-500"
                                        aria-label="Custom date range start date"
                                    />
                                </div>
                                <div>
                                    <label htmlFor="custom-end-date" className="block text-xs text-slate-400 mb-1">End Date</label>
                                    <input
                                        id="custom-end-date"
                                        type="date"
                                        value={customEnd}
                                        onChange={(e) => setCustomEnd(e.target.value)}
                                        className="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 text-white text-sm focus:outline-none focus:border-cyan-500"
                                        aria-label="Custom date range end date"
                                    />
                                </div>
                                <div className="flex justify-end gap-2 pt-2">
                                    <button
                                        type="button"
                                        onClick={() => setIsCustomOpen(false)}
                                        className="px-3 py-1.5 text-xs text-slate-400 hover:text-white"
                                    >
                                        Cancel
                                    </button>
                                    <button
                                        type="button"
                                        onClick={handleCustomApply}
                                        className="px-3 py-1.5 text-xs bg-cyan-500 hover:bg-cyan-600 text-white rounded"
                                    >
                                        Apply
                                    </button>
                                </div>
                            </div>
                        </div>
                    )}
                </div>
                </div>
                </div>
            </div>
        </div>
    );
}

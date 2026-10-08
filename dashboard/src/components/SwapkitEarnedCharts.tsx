'use client';

import { useEffect, useMemo, useState } from 'react';
import { formatDistanceToNow } from 'date-fns';
import { StackedBarChart } from '@/components/StackedBarChart';
import { Tooltip } from '@/components/Tooltip';
import { fetchSwapkitEarned, SwapkitEarnedResponse } from '@/lib/api';
import { providerColors } from '@/lib/chartStyles';
import { providerLabel, sortProviders, SWAPKIT_EARNED_TOOLTIP } from '@/lib/providerUtils';

// Same threshold as the daily-source check in SystemStatus.
const STALE_AFTER_HOURS = 26;
const TICK_MS = 60 * 1000;
const REFETCH_MS = 15 * 60 * 1000;

interface SwapkitEarnedChartsProps {
    range: string;
    startDate?: string | null;
    endDate?: string | null;
    granularity: string;
}

type Measure = 'revenue_usd' | 'volume_usd';

function pivot(response: SwapkitEarnedResponse, measure: Measure) {
    const providers = sortProviders(Array.from(new Set(response.series.map(row => row.provider))));
    const byDate = new Map<string, Record<string, number | string>>();
    for (const row of response.series) {
        const entry = byDate.get(row.date) ?? { date: row.date };
        entry[providerLabel(row.provider)] = row[measure];
        byDate.set(row.date, entry);
    }
    const data = Array.from(byDate.values()).sort((a, b) => String(a.date).localeCompare(String(b.date)));
    return { data, keys: providers.map(providerLabel) };
}

/**
 * SwapKit-reported earned revenue and volume. Rendered only when the
 * NEXT_PUBLIC_SWAPKIT_EARNED flag is on (the parent decides).
 */
export function SwapkitEarnedCharts({ range, startDate, endDate, granularity }: SwapkitEarnedChartsProps) {
    const requestKey = JSON.stringify([range, startDate ?? null, endDate ?? null, granularity]);
    // A result belongs to the request that produced it; a different key means loading.
    const [result, setResult] = useState<{ key: string; response: SwapkitEarnedResponse | null } | null>(null);
    const [now, setNow] = useState(() => Date.now());

    useEffect(() => {
        const controller = new AbortController();
        const load = () => {
            fetchSwapkitEarned({ range, granularity, startDate, endDate }, controller.signal)
                .then(response => {
                    if (!controller.signal.aborted) setResult({ key: requestKey, response });
                })
                .catch(() => {
                    if (!controller.signal.aborted) setResult({ key: requestKey, response: null });
                });
        };
        load();
        const refetch = setInterval(load, REFETCH_MS);
        return () => {
            controller.abort();
            clearInterval(refetch);
        };
    }, [range, startDate, endDate, granularity, requestKey]);

    // Recompute freshness every minute so an open page goes stale on its own.
    useEffect(() => {
        const tick = setInterval(() => setNow(Date.now()), TICK_MS);
        return () => clearInterval(tick);
    }, []);

    const current = result && result.key === requestKey ? result : null;
    const response = current?.response ?? null;
    const failed = !!current && current.response === null;

    const stale = useMemo(() => {
        if (!response?.last_updated) return true;
        return (now - new Date(response.last_updated).getTime()) / (1000 * 60 * 60) > STALE_AFTER_HOURS;
    }, [response, now]);

    const hasSeries = !!response && response.series.length > 0;
    const showSeries = hasSeries && !stale;
    const revenue = useMemo(() => (showSeries && response ? pivot(response, 'revenue_usd') : null), [showSeries, response]);
    const volume = useMemo(() => (showSeries && response ? pivot(response, 'volume_usd') : null), [showSeries, response]);

    let note: string | null = null;
    if (failed) {
        note = 'Earned data could not be loaded.';
    } else if (response && !response.last_updated) {
        note = 'Earned data has not been updated yet.';
    } else if (response && stale) {
        note = `Earned data is out of date. Last updated ${formatDistanceToNow(new Date(response.last_updated as string), { addSuffix: true })}.`;
    } else if (response && !hasSeries) {
        note = 'No earned data within this date range.';
    }

    const info = <Tooltip content={SWAPKIT_EARNED_TOOLTIP} iconOnly />;

    return (
        <div className="space-y-6">
            {note && (
                <p className="text-sm text-[var(--text-tertiary)]">{note}</p>
            )}
            {revenue && (
                <StackedBarChart
                    title="Earned revenue (SwapKit-reported)"
                    subtitle={`${response?.granularity ?? granularity} breakdown`}
                    data={revenue.data}
                    keys={revenue.keys}
                    colors={providerColors}
                    currency={true}
                    action={info}
                />
            )}
            {volume && (
                <StackedBarChart
                    title="Earned volume (SwapKit-reported)"
                    subtitle={`${response?.granularity ?? granularity} breakdown`}
                    data={volume.data}
                    keys={volume.keys}
                    colors={providerColors}
                    currency={true}
                    action={info}
                />
            )}
        </div>
    );
}

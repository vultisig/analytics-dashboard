'use client';

import { useEffect, useMemo, useState } from 'react';
import { formatDistanceToNow } from 'date-fns';
import { ProviderSection } from '@/components/ProviderSection';
import { StackedBarChart } from '@/components/StackedBarChart';
import { Tooltip } from '@/components/Tooltip';
import { fetchSwapkitEarned, SwapkitEarnedResponse } from '@/lib/api';
import { providerColors } from '@/lib/chartStyles';
import { providerLabel, sortProviders, SWAPKIT_EARNED_ENABLED, SWAPKIT_EARNED_TOOLTIP } from '@/lib/providerUtils';

// Same threshold as the daily-source check in SystemStatus.
const STALE_AFTER_HOURS = 26;
const TICK_MS = 60 * 1000;
const REFETCH_MS = 15 * 60 * 1000;

type Measure = 'revenue_usd' | 'volume_usd';

interface SwapkitEarnedChartsProps {
    measure: Measure;
    range: string;
    startDate?: string | null;
    endDate?: string | null;
    granularity: string;
}

const chartTitles: Record<Measure, string> = {
    revenue_usd: 'Earned revenue (SwapKit-reported)',
    volume_usd: 'Earned volume (SwapKit-reported)',
};

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

function earnedNote(failed: boolean, response: SwapkitEarnedResponse | null, stale: boolean, hasSeries: boolean): string | null {
    if (failed) return 'Earned data could not be loaded.';
    if (!response) return null;
    if (!response.last_updated) return 'Earned data has not been updated yet.';
    if (stale) return `Earned data is out of date. Last updated ${formatDistanceToNow(new Date(response.last_updated), { addSuffix: true })}.`;
    if (!hasSeries) return 'No earned data within this date range.';
    return null;
}

/** The SwapKit provider section: one earned measure by underlying provider. Hidden unless NEXT_PUBLIC_SWAPKIT_EARNED is on. */
export function SwapkitEarnedSection(props: SwapkitEarnedChartsProps) {
    if (!SWAPKIT_EARNED_ENABLED) return null;
    return (
        <ProviderSection provider="swapkit">
            <SwapkitEarnedCharts {...props} />
        </ProviderSection>
    );
}

function SwapkitEarnedCharts({ measure, range, startDate, endDate, granularity }: SwapkitEarnedChartsProps) {
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
    const chart = useMemo(() => (showSeries && response ? pivot(response, measure) : null), [showSeries, response, measure]);

    const note = earnedNote(failed, response, stale, hasSeries);

    const info = <Tooltip content={SWAPKIT_EARNED_TOOLTIP} iconOnly />;

    return (
        <div className="space-y-6">
            {note && (
                <p className="text-sm text-[var(--text-tertiary)]">{note}</p>
            )}
            {chart && (
                <StackedBarChart
                    title={chartTitles[measure]}
                    subtitle={`${response?.granularity ?? granularity} breakdown`}
                    data={chart.data}
                    keys={chart.keys}
                    colors={providerColors}
                    currency={true}
                    action={info}
                />
            )}
        </div>
    );
}

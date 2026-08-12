'use client';

import { useEffect, useMemo, useState } from 'react';
import {
    CartesianGrid,
    Line,
    LineChart,
    ReferenceLine,
    ResponsiveContainer,
    Tooltip as RechartsTooltip,
    XAxis,
    YAxis,
} from 'recharts';
import { IconActivityV, IconExternalLinkV } from '@/icons';
import {
    fetchMarketVolumeShare,
    type MarketVolumePoint,
    type MarketVolumeShare as MarketVolumeShareData,
} from '@/lib/api';
import { providerColorMap } from '@/lib/chartStyles';
import { Tooltip } from './Tooltip';

interface MarketVolumeShareProps {
    range: string;
    startDate?: string | null;
    endDate?: string | null;
    granularity: string;
}

type ChartRange = 'page' | '90d' | '1y' | 'all';
type TrendWindow = 0 | 30 | 90;

interface TrendPoint extends MarketVolumePoint {
    displaySharePercent: number | null;
}

const DAY_MS = 24 * 60 * 60 * 1000;
const ALL_ROUTES_COLOR = '#4879FD';
const RANGE_OPTIONS: Array<{ value: ChartRange; label: string }> = [
    { value: 'page', label: 'Page' },
    { value: '90d', label: '90D' },
    { value: '1y', label: '1Y' },
    { value: 'all', label: 'All history' },
];
const TREND_OPTIONS: Array<{ value: TrendWindow; label: string }> = [
    { value: 0, label: 'Raw' },
    { value: 30, label: '30D trend' },
    { value: 90, label: '90D trend' },
];

const COMPACT_CURRENCY_FORMATTER = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    notation: 'compact',
    maximumFractionDigits: 2,
});

function formatCompactCurrency(value: number): string {
    return COMPACT_CURRENCY_FORMATTER.format(value);
}

function formatShare(value: number): string {
    if (!Number.isFinite(value) || value === 0) return '0%';
    if (value < 0.000001) return '<0.000001%';
    if (value < 0.0001) return `${value.toFixed(6)}%`;
    if (value < 0.01) return `${value.toFixed(4)}%`;
    if (value < 1) return `${value.toFixed(3)}%`;
    return `${value.toFixed(2)}%`;
}

function formatAxisShare(value: number): string {
    if (Math.abs(value) < 0.000000000001) return '0%';
    if (value < 0.0001) return `${value.toFixed(6)}%`;
    if (value < 0.001) return `${value.toFixed(5)}%`;
    if (value < 0.01) return `${value.toFixed(3)}%`;
    if (value < 1) return `${value.toFixed(2)}%`;
    return `${value.toFixed(1)}%`;
}

function formatDate(value: string, includeYear = false): string {
    const date = new Date(`${value}T00:00:00Z`);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        ...(includeYear ? { year: 'numeric' } : {}),
        timeZone: 'UTC',
    });
}

function colorForProvider(provider: string): string {
    return provider === 'all'
        ? ALL_ROUTES_COLOR
        : (providerColorMap[provider] ?? ALL_ROUTES_COLOR);
}

function calculateRollingTrend(points: MarketVolumePoint[], windowDays: TrendWindow): TrendPoint[] {
    if (windowDays === 0) {
        return points.map((point) => ({
            ...point,
            displaySharePercent: point.sharePercent,
        }));
    }

    let windowStart = 0;
    let vultisigVolume = 0;
    let marketVolume = 0;

    return points.map((point, index) => {
        vultisigVolume += point.vultisigVolumeUsd;
        marketVolume += point.marketVolumeUsd;

        const pointTime = Date.parse(`${point.date}T00:00:00Z`);
        const cutoffTime = pointTime - ((windowDays - 1) * DAY_MS);
        while (
            windowStart < index
            && Date.parse(`${points[windowStart].date}T00:00:00Z`) < cutoffTime
        ) {
            vultisigVolume -= points[windowStart].vultisigVolumeUsd;
            marketVolume -= points[windowStart].marketVolumeUsd;
            windowStart += 1;
        }

        return {
            ...point,
            displaySharePercent: (
                pointTime - Date.parse(`${points[0].date}T00:00:00Z`)
                >= (windowDays - 1) * DAY_MS
                && marketVolume > 0
            )
                ? (vultisigVolume / marketVolume) * 100
                : null,
        };
    });
}

function Metric({ label, value, detail }: { label: string; value: string; detail: string }) {
    return (
        <div className="min-w-0 rounded-xl border border-[var(--border-light)] bg-[var(--surface-2)]/30 px-4 py-3">
            <p className="t-caption text-[var(--text-tertiary)]">{label}</p>
            <p className="mt-1 truncate font-display text-num text-xl font-medium text-[var(--text-primary)]">{value}</p>
            <p className="mt-1 truncate text-[11px] text-[var(--text-tertiary)]">{detail}</p>
        </div>
    );
}

function SegmentedControl<T extends string | number>({
    label,
    options,
    value,
    onChange,
}: {
    label: string;
    options: Array<{ value: T; label: string }>;
    value: T;
    onChange: (value: T) => void;
}) {
    return (
        <div className="flex flex-wrap items-center gap-2">
            <span className="t-caption text-[var(--text-tertiary)]">{label}</span>
            <div className="flex flex-wrap gap-1 rounded-xl border border-[var(--border-light)] bg-[var(--surface-2)]/30 p-1">
                {options.map((option) => {
                    const selected = option.value === value;
                    return (
                        <button
                            key={option.value}
                            type="button"
                            onClick={() => onChange(option.value)}
                            className="rounded-lg px-3 py-1.5 text-[11px] font-medium text-[var(--text-tertiary)] transition-colors hover:text-[var(--text-secondary)] data-[active=true]:bg-[var(--brand-blue)] data-[active=true]:text-white"
                            aria-pressed={selected}
                            data-active={selected}
                        >
                            {option.label}
                        </button>
                    );
                })}
            </div>
        </div>
    );
}

export function MarketVolumeShare({
    range,
    startDate,
    endDate,
    granularity,
}: MarketVolumeShareProps) {
    const [data, setData] = useState<MarketVolumeShareData | null>(null);
    const [selectedProvider, setSelectedProvider] = useState('all');
    const [chartRange, setChartRange] = useState<ChartRange>('1y');
    const [trendWindow, setTrendWindow] = useState<TrendWindow>(30);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [retryCount, setRetryCount] = useState(0);

    useEffect(() => {
        const controller = new AbortController();

        async function load() {
            setLoading(true);
            setError(null);
            try {
                const usePageRange = chartRange === 'page';
                const result = await fetchMarketVolumeShare(
                    {
                        range: usePageRange ? range : chartRange,
                        granularity: usePageRange ? granularity : 'd',
                        startDate: usePageRange ? startDate : null,
                        endDate: usePageRange ? endDate : null,
                    },
                    controller.signal,
                );
                if (!Array.isArray(result.series) || !Array.isArray(result.benchmarks)) {
                    throw new Error('Comparable market response was invalid');
                }
                setData(result);
                setSelectedProvider((currentProvider) => (
                    result.benchmarks.some((benchmark) => benchmark.provider === currentProvider)
                        ? currentProvider
                        : (result.benchmarks[0]?.provider ?? 'all')
                ));
            } catch (loadError) {
                if (controller.signal.aborted) return;
                console.error('Error fetching comparable market volume:', loadError);
                setError('Comparable market history is temporarily unavailable.');
            } finally {
                if (!controller.signal.aborted) setLoading(false);
            }
        }

        void load();
        return () => controller.abort();
    }, [chartRange, range, startDate, endDate, granularity, retryCount]);

    const selectedBenchmark = data?.benchmarks.find(
        (benchmark) => benchmark.provider === selectedProvider,
    );
    const rawPoints = useMemo(
        () => (data?.series ?? [])
            .filter(
                (point) => point.provider === selectedProvider
                    && Number.isFinite(point.sharePercent)
                    && Number.isFinite(point.marketVolumeUsd),
            )
            .sort((left, right) => left.date.localeCompare(right.date)),
        [data, selectedProvider],
    );
    const chartPoints = useMemo(
        () => calculateRollingTrend(rawPoints, trendWindow),
        [rawPoints, trendWindow],
    );

    const totals = useMemo(
        () => rawPoints.reduce(
            (total, point) => ({
                vultisig: total.vultisig + point.vultisigVolumeUsd,
                market: total.market + point.marketVolumeUsd,
            }),
            { vultisig: 0, market: 0 },
        ),
        [rawPoints],
    );
    const periodShare = totals.market > 0 ? (totals.vultisig / totals.market) * 100 : 0;
    const lineColor = colorForProvider(selectedProvider);
    const selectedSource = selectedBenchmark?.source ?? data?.source ?? 'Market benchmark';
    const selectedSourceUrl = selectedBenchmark?.sourceUrl ?? data?.sourceUrl;

    if (!data && loading) {
        return (
            <section className="surface-card p-5 md:p-6" aria-label="Loading routed market share chart">
                <div className="animate-pulse space-y-5">
                    <div className="h-5 w-64 rounded bg-[var(--surface-2)]" />
                    <div className="h-9 w-full max-w-md rounded bg-[var(--surface-2)]/70" />
                    <div className="h-72 rounded-xl bg-[var(--surface-2)]/50" />
                </div>
            </section>
        );
    }

    if (!data) {
        return (
            <section className="surface-card p-5 md:p-6" aria-labelledby="market-volume-title">
                <div className="flex flex-wrap items-center justify-between gap-4">
                    <div>
                        <h3 id="market-volume-title" className="t-title-3">Share of Routed Markets</h3>
                        <p className="t-footnote mt-1 text-[var(--text-tertiary)]">{error}</p>
                    </div>
                    <button type="button" className="pill pill-sm" onClick={() => setRetryCount((count) => count + 1)}>
                        Retry
                    </button>
                </div>
            </section>
        );
    }

    const rangeLabel = chartPoints.length > 0
        ? `${formatDate(chartPoints[0].date, true)} – ${formatDate(chartPoints.at(-1)!.date, true)}`
        : 'No comparable dates in this range';
    const viewLabel = trendWindow === 0
        ? `${data.effectiveGranularity[0].toUpperCase()}${data.effectiveGranularity.slice(1)} raw share`
        : `${trendWindow}D rolling trend`;

    return (
        <section className="surface-card surface-card-hover overflow-hidden p-5 md:p-6" aria-labelledby="market-volume-title">
            <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="flex items-start gap-3">
                    <div className="icon-badge mt-0.5">
                        <IconActivityV size={18} aria-hidden="true" />
                    </div>
                    <div>
                        <div className="flex items-center gap-2">
                            <h3 id="market-volume-title" className="t-title-3">Share of Routed Markets</h3>
                            <Tooltip
                                content="Each provider is compared like-for-like. All routes is a weighted blend of those comparisons; provider markets can overlap, so it is directional rather than a unique global-market share."
                                iconOnly
                            />
                        </div>
                        <p className="t-footnote mt-1 text-[var(--text-tertiary)]">
                            {viewLabel} · {rangeLabel}{data.isStale ? ' · cached benchmark data' : ''}
                        </p>
                    </div>
                </div>
                {selectedSourceUrl ? (
                    <a
                        href={selectedSourceUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1.5 t-caption text-[var(--text-tertiary)] transition-colors hover:text-[var(--text-secondary)]"
                    >
                        Market data by {selectedSource}
                        <IconExternalLinkV size={13} aria-hidden="true" />
                    </a>
                ) : (
                    <span className="t-caption text-[var(--text-tertiary)]">
                        Market data by {selectedSource}
                    </span>
                )}
            </div>

            <div className="mt-5 flex flex-wrap gap-2" aria-label="Select comparison market">
                {data.benchmarks.map((benchmark) => {
                    const selected = benchmark.provider === selectedProvider;
                    return (
                        <button
                            key={benchmark.provider}
                            type="button"
                            onClick={() => setSelectedProvider(benchmark.provider)}
                            className="pill pill-sm transition-colors"
                            aria-pressed={selected}
                            data-active={selected}
                        >
                            <span
                                className="size-2 rounded-full"
                                style={{ backgroundColor: colorForProvider(benchmark.provider) }}
                                aria-hidden="true"
                            />
                            {benchmark.label}
                        </button>
                    );
                })}
            </div>

            <div className="mt-4 flex flex-wrap items-center justify-between gap-3 rounded-xl border border-[var(--border-light)] bg-[var(--surface-2)]/15 px-3 py-2.5">
                <SegmentedControl
                    label="History"
                    options={RANGE_OPTIONS}
                    value={chartRange}
                    onChange={setChartRange}
                />
                <SegmentedControl
                    label="View"
                    options={TREND_OPTIONS}
                    value={trendWindow}
                    onChange={setTrendWindow}
                />
            </div>

            {error && data && (
                <p className="mt-3 t-caption text-[var(--alert-error)]">{error} Showing the last loaded range.</p>
            )}

            <div className="mt-4 grid grid-cols-1 gap-3 lg:grid-cols-3">
                <Metric
                    label={selectedProvider === 'all' ? 'Blended period share' : 'Period market share'}
                    value={formatShare(periodShare)}
                    detail={selectedBenchmark?.comparison ?? 'Comparable market volume'}
                />
                <Metric
                    label="Vultisig routed volume"
                    value={formatCompactCurrency(totals.vultisig)}
                    detail="Over comparable benchmark dates"
                />
                <Metric
                    label={selectedProvider === 'all' ? 'Blended market volume' : 'Matching market volume'}
                    value={formatCompactCurrency(totals.market)}
                    detail={selectedBenchmark?.market ?? 'Selected market'}
                />
            </div>

            <div className="relative mt-5 min-h-[330px] rounded-xl border border-[var(--border-light)] bg-[var(--surface-2)]/15 px-1 pb-2 pt-4 md:px-3">
                {loading && (
                    <div className="absolute right-4 top-3 z-10 rounded-full bg-[var(--surface-2)] px-2.5 py-1 text-[10px] text-[var(--text-tertiary)]">
                        Updating…
                    </div>
                )}
                {chartPoints.length > 0 ? (
                    <ResponsiveContainer width="100%" height={310}>
                        <LineChart data={chartPoints} margin={{ top: 8, right: 16, left: 4, bottom: 8 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.1)" vertical={false} />
                            <XAxis
                                dataKey="date"
                                stroke="#94A3B8"
                                fontSize={11}
                                tickLine={false}
                                axisLine={false}
                                minTickGap={28}
                                tickFormatter={(value) => formatDate(value)}
                                dy={8}
                            />
                            <YAxis
                                stroke="#94A3B8"
                                fontSize={11}
                                tickLine={false}
                                axisLine={false}
                                width={64}
                                domain={[0, (maximum: number) => Math.max(maximum * 1.15, 0.0001)]}
                                tickFormatter={formatAxisShare}
                            />
                            <RechartsTooltip
                                cursor={{ stroke: 'rgba(148, 163, 184, 0.28)', strokeDasharray: '4 4' }}
                                content={({ active, payload }) => {
                                    if (!active || !payload?.length) return null;
                                    const point = payload[0].payload as TrendPoint;
                                    return (
                                        <div className="glass-card min-w-60 rounded-xl p-3 shadow-xl">
                                            <p className="border-b border-[var(--border-normal)]/50 pb-2 text-sm font-medium text-[var(--text-primary)]">
                                                {formatDate(point.date, true)}
                                            </p>
                                            <div className="mt-2 space-y-1.5 text-xs">
                                                {trendWindow > 0 && point.displaySharePercent !== null && (
                                                    <div className="flex justify-between gap-5 text-[var(--text-secondary)]">
                                                        <span>{trendWindow}D trend</span>
                                                        <span className="font-medium text-[var(--text-primary)]">{formatShare(point.displaySharePercent)}</span>
                                                    </div>
                                                )}
                                                <div className="flex justify-between gap-5 text-[var(--text-secondary)]">
                                                    <span>{trendWindow > 0 ? 'Raw period share' : 'Vultisig share'}</span>
                                                    <span>{formatShare(point.sharePercent)}</span>
                                                </div>
                                                <div className="flex justify-between gap-5 text-[var(--text-secondary)]">
                                                    <span>Vultisig volume</span>
                                                    <span>{formatCompactCurrency(point.vultisigVolumeUsd)}</span>
                                                </div>
                                                <div className="flex justify-between gap-5 text-[var(--text-secondary)]">
                                                    <span>{selectedBenchmark?.market ?? 'Market'} volume</span>
                                                    <span>{formatCompactCurrency(point.marketVolumeUsd)}</span>
                                                </div>
                                            </div>
                                        </div>
                                    );
                                }}
                            />
                            {periodShare > 0 && (
                                <ReferenceLine
                                    y={periodShare}
                                    stroke="rgba(148, 163, 184, 0.45)"
                                    strokeDasharray="5 5"
                                    label={{
                                        value: `Period ${formatShare(periodShare)}`,
                                        position: 'insideTopRight',
                                        fill: '#94A3B8',
                                        fontSize: 10,
                                    }}
                                />
                            )}
                            <Line
                                type="monotone"
                                dataKey="displaySharePercent"
                                name={trendWindow > 0 ? `${trendWindow}D trend` : 'Vultisig share'}
                                stroke={lineColor}
                                strokeWidth={trendWindow > 0 ? 3 : 2.25}
                                dot={false}
                                activeDot={{ r: 5, fill: lineColor, stroke: '#FFFFFF', strokeWidth: 2 }}
                                isAnimationActive
                                animationDuration={500}
                            />
                        </LineChart>
                    </ResponsiveContainer>
                ) : (
                    <div className="flex h-[310px] items-center justify-center px-6 text-center">
                        <div>
                            <p className="t-body-s text-[var(--text-secondary)]">No complete benchmark data in this date range</p>
                            <p className="t-caption mt-1 text-[var(--text-tertiary)]">Try 90D or a longer history; market benchmarks are published daily.</p>
                        </div>
                    </div>
                )}
            </div>

            <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-[11px] text-[var(--text-tertiary)]">
                <p>
                    {selectedProvider === 'all'
                        ? 'All routes is directional: provider market totals may overlap.'
                        : selectedProvider === 'lifi'
                            ? 'LI.FI uses same-chain Vultisig swaps to match DefiLlama’s same-chain methodology.'
                            : `Latest ${selectedBenchmark?.market ?? 'market'} benchmark: ${selectedBenchmark ? formatDate(selectedBenchmark.latestMarketDate, true) : 'unavailable'}.`}
                </p>
                <p>MayaChain network volume comes from the protocol&apos;s official Midgard history.</p>
            </div>
        </section>
    );
}

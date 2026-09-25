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
const AXIS_PADDING_RATIO = 0.1;
const AXIS_TICK_COUNT = 5;
const NICE_FRACTIONS = [1, 2, 2.5, 5, 10];
const AXIS_SIGNIFICANT_DIGITS = 2;
const AXIS_MAX_DECIMALS = 12;
const LINE_COLOR = '#4879FD';
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
    if (value < 0.01) return `${Number(value.toPrecision(2))}%`;
    if (value < 1) return `${value.toFixed(3)}%`;
    return `${value.toFixed(2)}%`;
}

function chartCaption(
    data: MarketVolumeShareData,
    viewLabel: string,
    rangeLabel: string,
): string {
    const asOf = data.asOfDate ? ` · market data through ${formatDate(data.asOfDate, true)}` : '';
    const stale = data.isStale ? ' · stale upstream cache' : '';
    return `${viewLabel} · ${rangeLabel}${asOf}${stale}`;
}

function formatAxisShare(value: number): string {
    if (value <= 0) return '0%';
    if (value >= 1) return `${value.toFixed(1)}%`;
    const decimals = Math.min(
        AXIS_MAX_DECIMALS,
        Math.ceil(-Math.log10(value)) + AXIS_SIGNIFICANT_DIGITS,
    );
    return `${value.toFixed(decimals).replace(/\.?0+$/, '')}%`;
}

function niceStep(rawStep: number): number {
    const magnitude = 10 ** Math.floor(Math.log10(rawStep));
    const fraction = rawStep / magnitude;
    const niceFraction = NICE_FRACTIONS.find((candidate) => fraction <= candidate) ?? 10;
    return niceFraction * magnitude;
}

/** Fit the axis to the plotted values, on round ticks, so small daily moves stay visible. */
function shareAxis(points: TrendPoint[]): { domain: [number, number]; ticks: number[] } {
    const values = points
        .map((point) => point.displaySharePercent)
        .filter((value): value is number => value !== null);
    const min = values.length > 0 ? Math.min(...values) : 0;
    const max = values.length > 0 ? Math.max(...values) : 0;
    const span = max - min || max || 1;
    const padding = span * AXIS_PADDING_RATIO;
    const step = niceStep((span + 2 * padding) / (AXIS_TICK_COUNT - 1));
    const low = Math.max(0, Math.floor((min - padding) / step) * step);
    const high = Math.ceil((max + padding) / step) * step;
    const ticks = Array.from(
        { length: Math.round((high - low) / step) + 1 },
        (_, index) => Number((low + index * step).toPrecision(12)),
    );
    return { domain: [low, high], ticks };
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
    const [chartRange, setChartRange] = useState<ChartRange>('page');
    const [trendWindow, setTrendWindow] = useState<TrendWindow>(0);
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
                if (!Array.isArray(result.series)) {
                    throw new Error('Total market volume response was invalid');
                }
                setData(result);
            } catch (loadError) {
                if (controller.signal.aborted) return;
                console.error('Error fetching total market volume share:', loadError);
                setError('Total market volume is temporarily unavailable.');
            } finally {
                if (!controller.signal.aborted) setLoading(false);
            }
        }

        void load();
        return () => controller.abort();
    }, [chartRange, range, startDate, endDate, granularity, retryCount]);

    const rawPoints = useMemo(
        () => (data?.series ?? [])
            .filter(
                (point) => Number.isFinite(point.sharePercent)
                    && Number.isFinite(point.marketVolumeUsd),
            )
            .sort((left, right) => left.date.localeCompare(right.date)),
        [data],
    );
    const chartPoints = useMemo(
        () => calculateRollingTrend(rawPoints, trendWindow),
        [rawPoints, trendWindow],
    );
    const yAxis = useMemo(() => shareAxis(chartPoints), [chartPoints]);

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

    if (!data && loading) {
        return (
            <section className="surface-card p-5 md:p-6" aria-label="Loading total market share chart">
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
                        <h3 id="market-volume-title" className="t-title-3">Share of Total Crypto Market Volume</h3>
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
        ? `${formatDate(chartPoints[0].date, true)} – ${formatDate(chartPoints[chartPoints.length - 1].date, true)}`
        : 'No published market data in this range';
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
                            <h3 id="market-volume-title" className="t-title-3">Share of Total Crypto Market Volume</h3>
                            <Tooltip
                                content="Vultisig swap volume across every provider (the same series as Swap Volume above) divided by CoinGecko's total crypto market volume, CEX and DEX combined. CoinGecko publishes each day's total about a day later, so the newest day can lag."
                                iconOnly
                            />
                        </div>
                        <p className="t-footnote mt-1 text-[var(--text-tertiary)]">
                            {chartCaption(data, viewLabel, rangeLabel)}
                        </p>
                    </div>
                </div>
                <a
                    href={data.sourceUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="inline-flex items-center gap-1.5 t-caption text-[var(--text-tertiary)] transition-colors hover:text-[var(--text-secondary)]"
                >
                    Market data by {data.source}
                    <IconExternalLinkV size={13} aria-hidden="true" />
                </a>
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
                    label="Period market share"
                    value={formatShare(periodShare)}
                    detail="Vultisig volume ÷ total market volume"
                />
                <Metric
                    label="Vultisig swap volume"
                    value={formatCompactCurrency(totals.vultisig)}
                    detail="On days with published market data"
                />
                <Metric
                    label="Total market volume"
                    value={formatCompactCurrency(totals.market)}
                    detail="CEX + DEX, all crypto assets"
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
                                width={84}
                                domain={yAxis.domain}
                                ticks={yAxis.ticks}
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
                                                    <span>Total market volume</span>
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
                                    ifOverflow="extendDomain"
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
                                stroke={LINE_COLOR}
                                strokeWidth={trendWindow > 0 ? 3 : 2.25}
                                dot={false}
                                activeDot={{ r: 5, fill: LINE_COLOR, stroke: '#FFFFFF', strokeWidth: 2 }}
                                isAnimationActive
                                animationDuration={500}
                            />
                        </LineChart>
                    </ResponsiveContainer>
                ) : (
                    <div className="flex h-[310px] items-center justify-center px-6 text-center">
                        <div>
                            <p className="t-body-s text-[var(--text-secondary)]">No published market data in this date range</p>
                            <p className="t-caption mt-1 text-[var(--text-tertiary)]">CoinGecko publishes each day&apos;s total about a day later; try a longer history.</p>
                        </div>
                    </div>
                )}
            </div>
        </section>
    );
}

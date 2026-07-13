'use client';

import { Pie, PieChart, Cell, ResponsiveContainer, Tooltip } from 'recharts';
import { ChartCard } from './ChartCard';
import { formatProviderName } from '@/lib/providerUtils';
import { providerColorMap, chainColorMap, fallbackChainColors } from '@/lib/chartStyles';

const DONUT_FRAME_WIDTH = 329;
const DONUT_FRAME_HEIGHT = 155;

interface DonutChartProps {
    title: string;
    subtitle?: string;
    data: { name: string; value: number }[];
    colors: string[];
    currency?: boolean;
    valueFormatter?: (value: number) => string;
}

const formatValue = (value: number, currency: boolean) =>
    currency
        ? `$${new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(value)}`
        : new Intl.NumberFormat('en-US').format(value);

interface CustomTooltipProps {
    active?: boolean;
    payload?: ReadonlyArray<{ name?: string | number; value?: number }>;
    total: number;
    currency: boolean;
    valueFormatter?: (value: number) => string;
}

function CustomTooltip({ active, payload, total, currency, valueFormatter }: CustomTooltipProps) {
    if (!active || !payload || payload.length === 0) return null;

    const item = payload[0];
    const value = Number(item.value ?? 0);
    const name = formatProviderName(String(item.name ?? ''));
    const percentage = ((value / total) * 100).toFixed(1);

    // Figma: w-[134px] p-[12px] rounded-[12px], rgba(17,40,74,0.5) bg
    // with backdrop-blur-[2px] and border #1b3f73.
    return (
        <div
            className="w-[134px] flex flex-col gap-1 rounded-[12px] p-3 backdrop-blur-[2px]"
            style={{
                background: 'rgba(17,40,74,0.5)',
                border: '1px solid var(--border-normal)',
            }}
        >
            <div className="flex items-center pb-1.5">
                <p className="t-body-s text-[var(--text-secondary)]">{name}</p>
            </div>
            <p className="font-display font-medium text-num text-[16px] leading-[20px] tracking-[0.2px] text-[var(--text-primary)]">
                {valueFormatter?.(value) ?? formatValue(value, currency)}
            </p>
            <p className="t-footnote text-[var(--text-tertiary)]">{percentage}%</p>
        </div>
    );
}

export function DonutChart({ title, subtitle, data, currency = true, valueFormatter }: DonutChartProps) {
    const activeData = data.filter(d => d.value > 0);
    const total = activeData.reduce((sum, item) => sum + item.value, 0);
    const hasData = activeData.length > 0 && total > 0;

    const sortedData = [...activeData].sort((a, b) => b.value - a.value);

    const colorFor = (name: string, index: number) => {
        const nameLower = name.toLowerCase();
        return providerColorMap[nameLower] ||
               chainColorMap[nameLower] ||
               fallbackChainColors[index % fallbackChainColors.length];
    };

    return (
        <ChartCard title={title} subtitle={subtitle}>
            {hasData ? (
                <div className="flex flex-col gap-14">
                    <div className="w-full flex justify-center">
                        <div className="w-full" style={{ maxWidth: DONUT_FRAME_WIDTH }}>
                            <ResponsiveContainer width="100%" height={DONUT_FRAME_HEIGHT}>
                                <PieChart margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
                                    <Pie
                                        data={sortedData}
                                        cx="50%"
                                        cy="100%"
                                        startAngle={180}
                                        endAngle={0}
                                        innerRadius={Math.round(DONUT_FRAME_HEIGHT * 0.6)}
                                        outerRadius={DONUT_FRAME_HEIGHT}
                                        paddingAngle={2}
                                        dataKey="value"
                                    >
                                        {sortedData.map((entry, index) => (
                                            <Cell
                                                key={`cell-${index}`}
                                                fill={colorFor(entry.name, index)}
                                                stroke="rgba(0,0,0,0.1)"
                                            />
                                        ))}
                                    </Pie>
                                    <Tooltip
                                        content={(props) => <CustomTooltip {...props} total={total} currency={currency} valueFormatter={valueFormatter} />}
                                        wrapperStyle={{ outline: 'none' }}
                                    />
                                </PieChart>
                            </ResponsiveContainer>
                        </div>
                    </div>
                    {/* Figma: 4-column legend, each cell flex-1, py-13 between dot row
                       and value, 8 px dot/label gap, Satoshi 18 / 20 / 0.2 px values. */}
                    <div className="flex w-full items-stretch gap-2.5">
                        {sortedData.map((entry, index) => {
                            const percentage = ((entry.value / total) * 100).toFixed(1);
                            return (
                                <div key={entry.name} className="flex flex-1 min-w-0 flex-col gap-1">
                                    <div className="flex items-center gap-2 py-3 min-w-0">
                                        <span
                                            className="size-2.5 shrink-0 rounded-full"
                                            style={{ backgroundColor: colorFor(entry.name, index) }}
                                        />
                                        <span className="t-body-s text-[var(--text-secondary)] truncate">
                                            {formatProviderName(entry.name)}
                                        </span>
                                    </div>
                                    <p className="font-display font-medium text-num text-[18px] leading-[20px] tracking-[0.2px] text-[var(--text-primary)] truncate">
                                        {valueFormatter?.(entry.value) ?? formatValue(entry.value, currency)}
                                    </p>
                                    <p className="t-footnote text-[var(--text-tertiary)]">{percentage}%</p>
                                </div>
                            );
                        })}
                    </div>
                </div>
            ) : (
                <div className="flex items-center justify-center h-[300px] w-full">
                    <div className="text-center">
                        <p className="text-[var(--text-tertiary)] text-sm">No data available for the selected time range</p>
                    </div>
                </div>
            )}
        </ChartCard>
    );
}

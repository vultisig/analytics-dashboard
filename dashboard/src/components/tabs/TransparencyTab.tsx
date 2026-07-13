'use client';

import { useEffect, useState } from 'react';
import type { ComponentType, JSX } from 'react';
import { Bar, BarChart, CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { ChartCard } from '@/components/ChartCard';
import { DonutChart } from '@/components/DonutChart';
import { StatsCard } from '@/components/StatsCard';
import {
    IconAlertCircleV,
    IconChart4,
    IconCoinV,
    IconDollar,
    IconExternalLinkV,
    IconReceiptCheck,
    IconVault,
    IconWallet4,
} from '@/icons';
import { providerColors } from '@/lib/chartStyles';
import { formatCompactNumber } from '@/lib/numberFormatters';
import {
    fetchTransparencyBuybacks,
    fetchTransparencyLocked,
    fetchTransparencySummary,
    fetchTransparencyTreasury,
} from '@/lib/api';
import type {
    TransparencyBuybacks,
    TransparencyBuybackTrade,
    TransparencyLockedData,
    TransparencyLockedPosition,
    TransparencySummary,
    TransparencyTreasury,
} from '@/lib/api';

const DAY_IN_MILLISECONDS = 86_400_000;
const ETHERSCAN_ADDRESS_URL = 'https://etherscan.io/address';
const ETHERSCAN_TOKEN_URL = 'https://etherscan.io/token';
const ETHERSCAN_TRANSACTION_URL = 'https://etherscan.io/tx';
const BUYBACK_CHART_HEIGHT = 280;
const MAX_BUYBACK_RECEIPTS = 20;
const PRICE_BAND_CHART_HEIGHT = 240;
const USD_FORMATTER = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
});
const VULT_FORMATTER = new Intl.NumberFormat('en-US', {
    maximumFractionDigits: 0,
});
const UNLOCK_DATE_FORMATTER = new Intl.DateTimeFormat('en-US', {
    dateStyle: 'long',
    timeZone: 'UTC',
});
const RECEIPT_DATE_FORMATTER = new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC',
});
const SYNC_TIMESTAMP_FORMATTER = new Intl.DateTimeFormat('en-US', {
    dateStyle: 'medium',
    timeStyle: 'short',
    timeZone: 'UTC',
});

type TransparencyData = {
    buybacks: TransparencyBuybacks;
    locked: TransparencyLockedData;
    summary: TransparencySummary;
    treasury: TransparencyTreasury;
};

type PipelineMetricProps = {
    label: string;
    value: string;
    subtitle: string;
    href: string;
    icon: ComponentType<{ size?: number; className?: string }>;
};

function getTransparencyData(): Promise<TransparencyData> {
    return Promise.all([
        fetchTransparencySummary(),
        fetchTransparencyTreasury(),
        fetchTransparencyBuybacks(),
        fetchTransparencyLocked(),
    ]).then(([summary, treasury, buybacks, locked]) => ({ summary, treasury, buybacks, locked }));
}

function getEtherscanAddressUrl(address: string): string {
    return `${ETHERSCAN_ADDRESS_URL}/${address}`;
}

function getEtherscanTransactionUrl(transactionHash: string): string {
    return `${ETHERSCAN_TRANSACTION_URL}/${transactionHash}`;
}

function getEtherscanTokenUrl(contractAddress: string, tokenId: number): string {
    return `${ETHERSCAN_TOKEN_URL}/${contractAddress}?a=${tokenId}`;
}

function formatUsd(value: number): string {
    return USD_FORMATTER.format(value);
}

function formatVult(value: number): string {
    return `${VULT_FORMATTER.format(value)} VULT`;
}

function formatPrice(value: number): string {
    return `$${value.toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 6,
    })}`;
}

function formatReceiptDate(date: string): string {
    return RECEIPT_DATE_FORMATTER.format(new Date(`${date}T00:00:00Z`));
}

function formatSyncStamp(lastSuccessfulSync: string | null): string {
    return lastSuccessfulSync
        ? `As of ${SYNC_TIMESTAMP_FORMATTER.format(new Date(lastSuccessfulSync))} UTC`
        : 'Sync status unavailable';
}

function getCumulativeBuybacks(trades: TransparencyBuybackTrade[]): { date: string; vult: number }[] {
    let total = 0;
    return [...trades].reverse().map(trade => {
        total += trade.vultBought;
        return { date: trade.date, vult: total };
    });
}

function getUnlockCountdown(unlockDate: string): string {
    const unlockAt = Date.parse(`${unlockDate}T00:00:00Z`);
    const daysRemaining = Math.ceil((unlockAt - Date.now()) / DAY_IN_MILLISECONDS);
    return daysRemaining > 0 ? `${daysRemaining.toLocaleString()} days until unlock` : 'Unlock date reached';
}

function EtherscanLink({ address, label }: { address: string; label: string }): JSX.Element {
    return (
        <a
            href={getEtherscanAddressUrl(address)}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 text-xs text-[var(--text-tertiary)] hover:text-[var(--brand-blue-light)] transition-colors"
        >
            {label}
            <IconExternalLinkV className="size-3" />
        </a>
    );
}

function PipelineMetric({ label, value, subtitle, href, icon }: PipelineMetricProps): JSX.Element {
    return (
        <a
            href={href}
            target="_blank"
            rel="noopener noreferrer"
            aria-label={`${label}: open receipt on Etherscan`}
            className="block h-full rounded-[20px] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--brand-blue-light)] [&>div]:h-full"
        >
            <StatsCard title={label} value={value} subtitle={subtitle} icon={icon} size="large" />
        </a>
    );
}

function PipelineArrow(): JSX.Element {
    return (
        <span aria-hidden="true" className="hidden items-center text-2xl text-[var(--text-tertiary)] xl:flex">
            →
        </span>
    );
}

function PipelineStrip({ summary }: { summary: TransparencySummary }): JSX.Element {
    const lockedValue = summary.locked.positionCount > 0
        ? `${summary.locked.positionCount} ${summary.locked.positionCount === 1 ? 'position' : 'positions'}`
        : '0 — first lock scheduled';
    const feeReceipt = getEtherscanAddressUrl(summary.treasuryAddress);
    const buybackReceipt = getEtherscanAddressUrl(summary.buybackWalletAddress);
    const lockReceipt = getEtherscanAddressUrl(summary.locked.ownerAddress);

    return (
        <section aria-labelledby="pipeline-title" className="space-y-4">
            <div>
                <h2 id="pipeline-title" className="text-[17px] font-medium leading-5 tracking-[-0.02em] text-[var(--text-primary)]">
                    Fee → buyback → locked liquidity
                </h2>
                <p className="mt-1 text-[13px] leading-[18px] text-[var(--text-tertiary)]">
                    Live on-chain receipts for the VULT transparency pipeline.
                </p>
            </div>
            <div className="grid grid-cols-1 items-stretch gap-3 xl:grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)_auto_minmax(0,1fr)_auto_minmax(0,1fr)]">
                <PipelineMetric
                    label="Fees collected"
                    value={formatUsd(summary.fees.allTimeUsd)}
                    subtitle={`${formatUsd(summary.fees.thisMonthUsd)} collected this month · ${formatSyncStamp(summary.fees.lastSuccessfulSync)}`}
                    href={feeReceipt}
                    icon={IconDollar}
                />
                <PipelineArrow />
                <PipelineMetric
                    label="VULT bought back"
                    value={formatVult(summary.buybacks.vultBought)}
                    subtitle={`Average ${formatPrice(summary.buybacks.averagePrice)} / VULT`}
                    href={buybackReceipt}
                    icon={IconReceiptCheck}
                />
                <PipelineArrow />
                <PipelineMetric
                    label="Locked forever"
                    value={lockedValue}
                    subtitle={formatUsd(summary.locked.valueUsd)}
                    href={lockReceipt}
                    icon={IconVault}
                />
                <PipelineArrow />
                <PipelineMetric
                    label="% of supply"
                    value={`${summary.locked.percentOfSupply.toFixed(2)}%`}
                    subtitle="Locked or burned"
                    href={lockReceipt}
                    icon={IconChart4}
                />
            </div>
        </section>
    );
}

function TreasuryBalances({ treasury }: { treasury: TransparencyTreasury }): JSX.Element {
    return (
        <ChartCard
            title="Fee Treasury"
            subtitle="Live on-chain balances"
            icon={IconWallet4}
            action={<EtherscanLink address={treasury.address} label="View wallet receipt" />}
        >
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                <StatsCard title="VULT" value={formatVult(treasury.balances.VULT)} subtitle="Current balance" icon={IconCoinV} />
                <StatsCard title="USDC" value={formatUsd(treasury.balances.USDC)} subtitle="Current balance" icon={IconDollar} />
                <StatsCard title="ETH" value={treasury.balances.ETH.toLocaleString('en-US', { maximumFractionDigits: 4 })} subtitle="Current balance" icon={IconWallet4} />
            </div>
        </ChartCard>
    );
}

function SupplyLedger({ summary }: { summary: TransparencySummary }): JSX.Element {
    const { supply, treasuryAddress } = summary;
    const unlockDate = UNLOCK_DATE_FORMATTER.format(new Date(`${supply.unlockDate}T00:00:00Z`));
    const balances = [
        { title: 'Circulating', value: formatVult(supply.circulatingVult), subtitle: 'Outside scheduled allocations', icon: IconCoinV },
        { title: 'Investor-locked', value: formatVult(supply.investorLockedVult), subtitle: `Until ${unlockDate}`, icon: IconVault },
        { title: 'Protocol-locked', value: formatVult(supply.protocolLockedVult), subtitle: 'Dead-owned liquidity positions', icon: IconReceiptCheck },
        { title: 'Treasury unallocated', value: formatVult(supply.treasuryUnallocatedVult), subtitle: 'Live fee treasury balance', icon: IconWallet4 },
    ];
    const supplyComposition = [
        { name: 'Circulating', value: Number(supply.circulatingVult) },
        { name: 'Investor-locked', value: Number(supply.investorLockedVult) },
        { name: 'Treasury bought-back', value: Number(supply.treasuryUnallocatedVult) },
        { name: 'LP-and-burned', value: Number(supply.protocolLockedVult) },
    ];

    return (
        <>
            <ChartCard
                title="Supply Ledger"
                subtitle={`${formatVult(supply.totalVult)} total supply`}
                icon={IconChart4}
                action={<EtherscanLink address={treasuryAddress} label="View treasury receipt" />}
            >
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
                    {balances.map(({ title, value, subtitle, icon }) => (
                        <StatsCard key={title} title={title} value={value} subtitle={subtitle} icon={icon} />
                    ))}
                </div>
                <p className="mt-5 text-sm text-[var(--text-secondary)]">
                    Investor allocation unlocks on {unlockDate} · <span className="text-[var(--alert-info)]">{getUnlockCountdown(supply.unlockDate)}</span>
                </p>
            </ChartCard>
            <DonutChart
                title="Supply Composition"
                subtitle={`${formatVult(supply.totalVult)} allocated across the supply ledger`}
                data={supplyComposition}
                colors={providerColors}
                currency={false}
                valueFormatter={value => VULT_FORMATTER.format(value)}
            />
        </>
    );
}

function BuybackChart({ trades }: { trades: TransparencyBuybackTrade[] }): JSX.Element {
    const chartData = getCumulativeBuybacks(trades);
    if (chartData.length === 0) {
        return <p className="py-10 text-center text-sm text-[var(--text-tertiary)]">0 buyback receipts recorded.</p>;
    }

    return (
        <div className="h-[280px]">
            <ResponsiveContainer width="100%" height={BUYBACK_CHART_HEIGHT}>
                <LineChart data={chartData} margin={{ top: 10, right: 10, bottom: 0, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.1)" vertical={false} />
                    <XAxis dataKey="date" stroke="#94A3B8" fontSize={12} tickLine={false} axisLine={false} dy={10} tickFormatter={formatReceiptDate} />
                    <YAxis stroke="#94A3B8" fontSize={12} tickLine={false} axisLine={false} tickFormatter={value => formatCompactNumber(Number(value))} />
                    <Tooltip
                        formatter={value => formatVult(Number(value))}
                        labelFormatter={value => formatReceiptDate(String(value))}
                        contentStyle={{ background: 'var(--surface-1)', border: '1px solid var(--border-normal)', borderRadius: 12 }}
                    />
                    <Line type="monotone" dataKey="vult" name="VULT accumulated" stroke="var(--brand-blue-light)" strokeWidth={2} dot={false} />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}

function BuybackLedger({ trades }: { trades: TransparencyBuybackTrade[] }): JSX.Element {
    if (trades.length === 0) {
        return <p className="py-10 text-center text-sm text-[var(--text-tertiary)]">0 buyback receipts recorded.</p>;
    }

    const visibleTrades = trades.slice(0, MAX_BUYBACK_RECEIPTS);

    return (
        <div>
            <p className="mb-3 text-xs text-[var(--text-tertiary)]">Showing latest {visibleTrades.length} of {trades.length}</p>
            <div className="overflow-x-auto">
                <table className="w-full min-w-[640px] text-left text-sm">
                    <thead className="border-b border-[var(--border-normal)] text-xs text-[var(--text-tertiary)]">
                        <tr>
                            <th className="pb-3 font-medium">Date</th>
                            <th className="pb-3 font-medium">USDC spent</th>
                            <th className="pb-3 font-medium">VULT received</th>
                            <th className="pb-3 font-medium">Effective price</th>
                            <th className="pb-3 font-medium">Transaction</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-[var(--border-normal)]">
                        {visibleTrades.map(trade => (
                            <tr key={trade.txHash} className="text-[var(--text-secondary)]">
                                <td className="py-3">{formatReceiptDate(trade.date)}</td>
                                <td className="py-3 text-[var(--text-primary)]">{formatUsd(trade.usdcSpent)}</td>
                                <td className="py-3 text-[var(--text-primary)]">{formatVult(trade.vultBought)}</td>
                                <td className="py-3">{formatPrice(trade.price)}</td>
                                <td className="py-3">
                                    <a href={getEtherscanTransactionUrl(trade.txHash)} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-[var(--brand-blue-light)] hover:underline">
                                        {`${trade.txHash.slice(0, 10)}…${trade.txHash.slice(-8)}`}
                                        <IconExternalLinkV className="size-3" />
                                    </a>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

function BuybackHistory({ buybacks }: { buybacks: TransparencyBuybacks }): JSX.Element {
    const { summary, trades, walletAddress } = buybacks;
    const syncStamp = formatSyncStamp(summary.lastSuccessfulSync);
    return (
        <>
            <ChartCard title="Buyback History" subtitle={`Cumulative on-chain VULT receipts · ${syncStamp}`} icon={IconReceiptCheck} action={<EtherscanLink address={walletAddress} label="View buyback wallet" />}>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                    <StatsCard title="USDC spent" value={formatUsd(summary.usdcSpent)} subtitle="All-time" icon={IconDollar} />
                    <StatsCard title="VULT received" value={formatVult(summary.vultBought)} subtitle="All-time" icon={IconCoinV} />
                    <StatsCard title="Average price" value={formatPrice(summary.averagePrice)} subtitle="USDC per VULT" icon={IconChart4} />
                </div>
                <div className="mt-6">
                    <BuybackChart trades={trades} />
                </div>
            </ChartCard>
            <ChartCard title="Buyback Receipts" subtitle="Indexed swap transactions" icon={IconReceiptCheck} action={<EtherscanLink address={walletAddress} label="View full history" />}>
                <BuybackLedger trades={trades} />
            </ChartCard>
        </>
    );
}

type PriceBandDatum = {
    high: number;
    low: number;
    range: number;
    tokenId: string;
};

type PriceBandTooltipProps = {
    active?: boolean;
    payload?: readonly { payload?: PriceBandDatum }[];
};

function getPriceBandData(positions: TransparencyLockedPosition[]): PriceBandDatum[] {
    return [...positions]
        .sort((first, second) => first.priceRangeUsd.low - second.priceRangeUsd.low)
        .map(position => ({
            high: position.priceRangeUsd.high,
            low: position.priceRangeUsd.low,
            range: position.priceRangeUsd.high - position.priceRangeUsd.low,
            tokenId: `NFT #${position.tokenId}`,
        }));
}

function PriceBandTooltip({ active, payload }: PriceBandTooltipProps): JSX.Element | null {
    const band = payload?.[0]?.payload;
    if (!active || !band) return null;

    return (
        <div className="rounded-xl border border-[var(--border-normal)] bg-[var(--surface-1)] p-3 text-sm shadow-xl">
            <p className="font-medium text-[var(--text-primary)]">{band.tokenId}</p>
            <p className="mt-1 text-[var(--text-secondary)]">{formatPrice(band.low)} to {formatPrice(band.high)}</p>
        </div>
    );
}

function LockedPositionReceipt({ position }: { position: TransparencyLockedPosition }): JSX.Element {
    return (
        <a
            href={getEtherscanTokenUrl(position.nftContractAddress, position.tokenId)}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 text-[var(--brand-blue-light)] hover:underline"
        >
            NFT #{position.tokenId}
            <IconExternalLinkV className="size-3" />
        </a>
    );
}

function LockedLiquidityLedger({ locked }: { locked: TransparencyLockedData }): JSX.Element {
    if (locked.positions.length === 0) {
        return (
            <ChartCard
                title="Locked Liquidity"
                subtitle="Dead-owned liquidity position receipts"
                icon={IconVault}
                action={<EtherscanLink address={locked.ownerAddress} label="View dead owner" />}
            >
                <p className="py-10 text-center text-sm text-[var(--text-tertiary)]">0 — first lock scheduled</p>
            </ChartCard>
        );
    }

    return (
        <ChartCard
            title="Locked Liquidity"
            subtitle="Current composition at the pool tick"
            icon={IconVault}
            action={<EtherscanLink address={locked.ownerAddress} label="View dead owner" />}
        >
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                <StatsCard title="VULT locked" value={formatVult(locked.vultLocked)} subtitle={`${locked.positionCount} NFT receipts`} icon={IconCoinV} />
                <StatsCard title="USDC paired" value={formatUsd(locked.usdcLocked)} subtitle="Current position composition" icon={IconDollar} />
                <StatsCard title="Current value" value={formatUsd(locked.valueUsd)} subtitle="At the live pool spot" icon={IconChart4} />
            </div>
            <div className="mt-6 overflow-x-auto">
                <table className="w-full min-w-[760px] text-left text-sm">
                    <thead className="border-b border-[var(--border-normal)] text-xs text-[var(--text-tertiary)]">
                        <tr>
                            <th className="pb-3 font-medium">Locked NFT</th>
                            <th className="pb-3 font-medium">Tick range</th>
                            <th className="pb-3 font-medium">Price band</th>
                            <th className="pb-3 font-medium">VULT</th>
                            <th className="pb-3 font-medium">USDC</th>
                            <th className="pb-3 font-medium">Current value</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-[var(--border-normal)]">
                        {locked.positions.map(position => (
                            <tr key={position.tokenId} className="text-[var(--text-secondary)]">
                                <td className="py-3"><LockedPositionReceipt position={position} /></td>
                                <td className="py-3">{position.tickRange.lower.toLocaleString()} to {position.tickRange.upper.toLocaleString()}</td>
                                <td className="py-3">{formatPrice(position.priceRangeUsd.low)} to {formatPrice(position.priceRangeUsd.high)}</td>
                                <td className="py-3 text-[var(--text-primary)]">{formatVult(position.composition.VULT)}</td>
                                <td className="py-3 text-[var(--text-primary)]">{formatUsd(position.composition.USDC)}</td>
                                <td className="py-3 text-[var(--text-primary)]">{formatUsd(position.valueUsd)}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </ChartCard>
    );
}

function PriceBandMap({ locked }: { locked: TransparencyLockedData }): JSX.Element {
    const priceBands = getPriceBandData(locked.positions);
    if (priceBands.length === 0) return <></>;

    return (
        <ChartCard
            title="Liquidity Price Bands"
            subtitle="Current pool spot relative to each locked NFT range"
            icon={IconChart4}
            action={<EtherscanLink address={locked.ownerAddress} label="View dead owner" />}
        >
            <div className="h-[240px]">
                <ResponsiveContainer width="100%" height={PRICE_BAND_CHART_HEIGHT}>
                    <BarChart data={priceBands} layout="vertical" margin={{ top: 10, right: 20, bottom: 0, left: 20 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.1)" horizontal={false} />
                        <XAxis type="number" stroke="#94A3B8" fontSize={12} tickLine={false} axisLine={false} tickFormatter={value => formatPrice(Number(value))} />
                        <YAxis type="category" dataKey="tokenId" stroke="#94A3B8" fontSize={12} tickLine={false} axisLine={false} width={78} />
                        <Tooltip content={({ active, payload }) => <PriceBandTooltip active={active} payload={payload} />} cursor={{ fill: 'rgba(6, 182, 212, 0.05)' }} />
                        <ReferenceLine x={locked.spotPriceUsd} stroke="var(--alert-info)" strokeDasharray="4 4" label={{ value: 'Current spot', fill: '#94A3B8', fontSize: 12 }} />
                        <Bar dataKey="low" stackId="price-band" fill="transparent" legendType="none" />
                        <Bar dataKey="range" name="Locked price band" stackId="price-band" fill="var(--brand-blue-light)" radius={[4, 4, 4, 4]} />
                    </BarChart>
                </ResponsiveContainer>
            </div>
        </ChartCard>
    );
}

function TransparencyContent({ data }: { data: TransparencyData }): JSX.Element {
    return (
        <>
            <PipelineStrip summary={data.summary} />
            <TreasuryBalances treasury={data.treasury} />
            <BuybackHistory buybacks={data.buybacks} />
            <LockedLiquidityLedger locked={data.locked} />
            <PriceBandMap locked={data.locked} />
            <SupplyLedger summary={data.summary} />
        </>
    );
}

export function TransparencyTab(): JSX.Element {
    const [data, setData] = useState<TransparencyData | null>(null);
    const [error, setError] = useState(false);

    useEffect(() => {
        let isCurrent = true;
        getTransparencyData()
            .then(result => isCurrent && setData(result))
            .catch(() => isCurrent && setError(true));
        return () => { isCurrent = false; };
    }, []);

    return (
        <div id="t-panel" role="tabpanel" className="space-y-6">
            {data && <TransparencyContent data={data} />}
            {!data && (
                <ChartCard title="Transparency" subtitle="On-chain receipts" icon={error ? IconAlertCircleV : IconReceiptCheck}>
                    <p className="py-10 text-center text-sm text-[var(--text-tertiary)]">
                        {error ? 'Live transparency data is temporarily unavailable.' : 'Loading on-chain receipts...'}
                    </p>
                </ChartCard>
            )}
        </div>
    );
}

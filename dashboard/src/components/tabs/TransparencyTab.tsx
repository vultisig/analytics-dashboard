'use client';

import { useEffect, useState } from 'react';
import type { ComponentType, JSX } from 'react';
import { ChartCard } from '@/components/ChartCard';
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
import {
    fetchTransparencySummary,
    fetchTransparencyTreasury,
} from '@/lib/api';
import type {
    TransparencySummary,
    TransparencyTreasury,
} from '@/lib/api';

const DAY_IN_MILLISECONDS = 86_400_000;
const ETHERSCAN_ADDRESS_URL = 'https://etherscan.io/address';
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

type TransparencyData = {
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

type PendingSection = {
    title: string;
    subtitle: string;
    zeroState: string;
    icon: ComponentType<{ size?: number; className?: string }>;
};

const PENDING_SECTIONS: PendingSection[] = [
    {
        title: 'Buyback History',
        subtitle: 'On-chain VULT buyback receipts',
        zeroState: '0 buyback receipts recorded.',
        icon: IconReceiptCheck,
    },
    {
        title: 'Locked Liquidity',
        subtitle: 'Dead-owned liquidity position receipts',
        zeroState: '0 — first lock scheduled',
        icon: IconVault,
    },
];

function getTransparencyData(): Promise<TransparencyData> {
    return Promise.all([fetchTransparencySummary(), fetchTransparencyTreasury()])
        .then(([summary, treasury]) => ({ summary, treasury }));
}

function getEtherscanAddressUrl(address: string): string {
    return `${ETHERSCAN_ADDRESS_URL}/${address}`;
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
            className="block rounded-[20px] focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--brand-blue-light)]"
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
            <div className="grid grid-cols-1 gap-3 xl:grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)_auto_minmax(0,1fr)_auto_minmax(0,1fr)]">
                <PipelineMetric
                    label="Fees collected"
                    value={formatUsd(summary.fees.allTimeUsd)}
                    subtitle={`${formatUsd(summary.fees.thisMonthUsd)} collected this month`}
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

    return (
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
    );
}

function PendingSections(): JSX.Element {
    return (
        <>
            {PENDING_SECTIONS.map(({ title, subtitle, zeroState, icon }) => (
                <ChartCard key={title} title={title} subtitle={subtitle} icon={icon}>
                    <p className="py-10 text-center text-sm text-[var(--text-tertiary)]">{zeroState}</p>
                </ChartCard>
            ))}
        </>
    );
}

function TransparencyContent({ data }: { data: TransparencyData }): JSX.Element {
    return (
        <>
            <PipelineStrip summary={data.summary} />
            <TreasuryBalances treasury={data.treasury} />
            <PendingSections />
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

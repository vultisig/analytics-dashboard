import type { ComponentType, JSX } from 'react';
import { ChartCard } from '@/components/ChartCard';
import { IconChart4, IconReceiptCheck, IconVault, IconWallet4 } from '@/icons';

type TransparencySection = {
    title: string;
    subtitle: string;
    zeroState: string;
    icon: ComponentType<{ size?: number; className?: string }>;
};

const transparencySections: TransparencySection[] = [
    {
        title: 'Fee Treasury',
        subtitle: 'Live balances and collected fees',
        zeroState: 'Live fee-treasury balances will appear here.',
        icon: IconWallet4,
    },
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
    {
        title: 'Supply Ledger',
        subtitle: 'Circulating, locked, and treasury VULT',
        zeroState: '0 VULT is protocol-locked.',
        icon: IconChart4,
    },
];

export function TransparencyTab(): JSX.Element {
    return (
        <div id="t-panel" role="tabpanel" className="space-y-6">
            {transparencySections.map(({ title, subtitle, zeroState, icon }) => (
                <ChartCard key={title} title={title} subtitle={subtitle} icon={icon}>
                    <p className="py-10 text-center text-sm text-[var(--text-tertiary)]">{zeroState}</p>
                </ChartCard>
            ))}
        </div>
    );
}

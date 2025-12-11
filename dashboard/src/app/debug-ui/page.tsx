import { StatsCard } from '@/components/StatsCard';
import { OverviewChart } from '@/components/OverviewChart';
import { TopPoolsList } from '@/components/TopPoolsList';
import { DollarSign, Activity } from 'lucide-react';

export default function DebugUI() {
    const staticData = {
        dailyVolume: [
            { date: 'Jan 01', value: 1000 },
            { date: 'Jan 02', value: 2000 },
            { date: 'Jan 03', value: 1500 },
        ],
        topPools: [
            { path: 'BTC.BTC → ETH.ETH', volume: 50000, count: 10 },
            { path: 'ETH.ETH → BTC.BTC', volume: 40000, count: 8 },
        ]
    };

    return (
        <div className="min-h-screen bg-[#0B1120] p-8 space-y-8">
            <h1 className="text-white text-2xl">Debug UI</h1>

            <div className="grid gap-4 grid-cols-4">
                <StatsCard
                    title="Test Metric"
                    value="$1,234,567"
                    icon={DollarSign}
                    change="+12.5%"
                />
            </div>

            <div className="grid gap-6 grid-cols-2">
                <OverviewChart
                    title="Test Chart"
                    subtitle="Static Data"
                    data={staticData.dailyVolume}
                    color="#0EA5E9"
                />
                <TopPoolsList data={staticData.topPools} />
            </div>
        </div>
    );
}

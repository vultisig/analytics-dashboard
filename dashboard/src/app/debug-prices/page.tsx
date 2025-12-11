'use client';

import { useEffect, useState } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, Brush } from 'recharts';

interface PricePoint {
    timestamp: string;
    price: number;
}

interface PriceData {
    rune: {
        prices: PricePoint[];
        referencePrices: PricePoint[];
    };
    cacao: {
        prices: PricePoint[];
        referencePrices: PricePoint[];
    };
}

export default function DebugPricesPage() {
    const [data, setData] = useState<PriceData | null>(null);
    const [loading, setLoading] = useState(true);
    const [timeRange, setTimeRange] = useState('7d');

    useEffect(() => {
        fetchData();
    }, [timeRange]);

    const fetchData = async () => {
        setLoading(true);
        try {
            const res = await fetch(`/api/debug/prices?range=${timeRange}`);
            if (!res.ok) {
                console.error('API returned error status:', res.status);
                setData(null);
                return;
            }
            const json = await res.json();
            // Validate the response has the expected structure
            if (json && json.rune && json.cacao) {
                setData(json);
            } else {
                console.error('Invalid API response structure:', json);
                setData(null);
            }
        } catch (error) {
            console.error('Failed to fetch price data:', error);
            setData(null);
        } finally {
            setLoading(false);
        }
    };

    if (loading) {
        return <div className="p-8 text-white">Loading price data...</div>;
    }

    if (!data) {
        return <div className="p-8 text-white">Failed to load price data</div>;
    }

    const formatPrice = (price: number) => `$${price.toFixed(4)}`;
    const formatTimestamp = (timestamp: string) => {
        const date = new Date(timestamp);
        return date.toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    };

    // Merge calculated and reference prices for RUNE
    const runeCombinedData: any[] = [];
    const runeCalculatedMap = new Map();
    const runeReferenceMap = new Map();

    // Index calculated prices by date
    data.rune.prices.forEach(p => {
        const time = new Date(p.timestamp).getTime();
        runeCalculatedMap.set(time, p.price);
    });

    // Index reference prices by date
    data.rune.referencePrices.forEach(p => {
        const time = new Date(p.timestamp).getTime();
        runeReferenceMap.set(time, p.price);
    });

    // Combine all unique timestamps
    const runeTimestamps = new Set([...runeCalculatedMap.keys(), ...runeReferenceMap.keys()]);
    runeTimestamps.forEach(time => {
        runeCombinedData.push({
            time,
            timeLabel: formatTimestamp(new Date(time).toISOString()),
            calculatedPrice: runeCalculatedMap.get(time) || null,
            referencePrice: runeReferenceMap.get(time) || null
        });
    });
    runeCombinedData.sort((a, b) => a.time - b.time);

    // Merge calculated and reference prices for CACAO
    const cacaoCombinedData: any[] = [];
    const cacaoCalculatedMap = new Map();
    const cacaoReferenceMap = new Map();

    // Index calculated prices by date
    data.cacao.prices.forEach(p => {
        const time = new Date(p.timestamp).getTime();
        cacaoCalculatedMap.set(time, p.price);
    });

    // Index reference prices by date
    data.cacao.referencePrices.forEach(p => {
        const time = new Date(p.timestamp).getTime();
        cacaoReferenceMap.set(time, p.price);
    });

    // Combine all unique timestamps
    const cacaoTimestamps = new Set([...cacaoCalculatedMap.keys(), ...cacaoReferenceMap.keys()]);
    cacaoTimestamps.forEach(time => {
        cacaoCombinedData.push({
            time,
            timeLabel: formatTimestamp(new Date(time).toISOString()),
            calculatedPrice: cacaoCalculatedMap.get(time) || null,
            referencePrice: cacaoReferenceMap.get(time) || null
        });
    });
    cacaoCombinedData.sort((a, b) => a.time - b.time);

    const CustomTooltip = ({ active, payload }: any) => {
        if (active && payload && payload.length) {
            return (
                <div className="bg-[#1A1F35] border border-slate-700 p-3 rounded-lg">
                    <p className="text-white font-mono text-sm mb-1">{payload[0]?.payload?.timeLabel}</p>
                    {payload.map((item: any, idx: number) => (
                        <p key={idx} className="font-mono text-sm" style={{ color: item.color }}>
                            {item.name}: {item.value ? formatPrice(item.value) : 'N/A'}
                        </p>
                    ))}
                </div>
            );
        }
        return null;
    };

    const timeRanges = [
        { label: '24H', value: '24h' },
        { label: '7D', value: '7d' },
        { label: '30D', value: '30d' },
        { label: '90D', value: '90d' },
        { label: 'All', value: 'all' }
    ];

    return (
        <div className="p-8 min-h-screen bg-[#0A0F1E]">
            <div className="mb-8">
                <h1 className="text-4xl font-bold text-white mb-2">Token Price Tracking</h1>
                <p className="text-slate-400">RUNE and CACAO prices derived from swap transactions (blue) vs reference data (green)</p>
            </div>

            {/* Time Range Selector */}
            <div className="flex gap-2 mb-8">
                {timeRanges.map(range => (
                    <button
                        key={range.value}
                        onClick={() => setTimeRange(range.value)}
                        className={`px-4 py-2 rounded-lg font-medium transition-colors ${timeRange === range.value
                            ? 'bg-blue-600 text-white'
                            : 'bg-[#0F1629] text-slate-400 hover:bg-[#1A1F35]'
                            }`}
                    >
                        {range.label}
                    </button>
                ))}
            </div>

            {/* RUNE Price Section */}
            <div className="mb-12">
                <div className="bg-[#0F1629] border border-slate-800 rounded-lg p-6">
                    <h2 className="text-2xl font-bold text-white mb-6">RUNE Price (THORChain)</h2>

                    {/* Chart */}
                    {runeCombinedData.length > 0 ? (
                        <ResponsiveContainer width="100%" height={400}>
                            <LineChart data={runeCombinedData} margin={{ top: 20, right: 30, left: 20, bottom: 20 }}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                                <XAxis
                                    dataKey="time"
                                    type="number"
                                    domain={['dataMin', 'dataMax']}
                                    tickFormatter={(timestamp) => {
                                        const date = new Date(timestamp);
                                        return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
                                    }}
                                    stroke="#64748b"
                                    style={{ fontSize: '12px' }}
                                />
                                <YAxis
                                    domain={['auto', 'auto']}
                                    tickFormatter={(value) => `$${value.toFixed(2)}`}
                                    stroke="#64748b"
                                    style={{ fontSize: '12px' }}
                                />
                                <Tooltip content={<CustomTooltip />} />
                                <Legend />
                                <Line
                                    type="monotone"
                                    dataKey="calculatedPrice"
                                    stroke="#3b82f6"
                                    strokeWidth={2}
                                    dot={false}
                                    name="Calculated from Swaps"
                                    connectNulls={false}
                                />
                                <Line
                                    type="monotone"
                                    dataKey="referencePrice"
                                    stroke="#10b981"
                                    strokeWidth={2}
                                    dot={false}
                                    name="Reference Data"
                                    connectNulls={false}
                                />
                                <Brush
                                    dataKey="time"
                                    height={30}
                                    stroke="#3b82f6"
                                    tickFormatter={(timestamp: number) => {
                                        const date = new Date(timestamp);
                                        return date.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
                                    }}
                                    fill="#0F1629"
                                />
                            </LineChart>
                        </ResponsiveContainer>
                    ) : (
                        <div className="h-[400px] flex items-center justify-center text-slate-400">
                            No RUNE price data available for this time range
                        </div>
                    )}
                </div>
            </div>

            {/* CACAO Price Section */}
            <div>
                <div className="bg-[#0F1629] border border-slate-800 rounded-lg p-6">
                    <h2 className="text-2xl font-bold text-white mb-6">CACAO Price (MAYAChain)</h2>

                    {/* Chart */}
                    {cacaoCombinedData.length > 0 ? (
                        <ResponsiveContainer width="100%" height={400}>
                            <LineChart data={cacaoCombinedData} margin={{ top: 20, right: 30, left: 20, bottom: 20 }}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                                <XAxis
                                    dataKey="time"
                                    type="number"
                                    domain={['dataMin', 'dataMax']}
                                    tickFormatter={(timestamp) => {
                                        const date = new Date(timestamp);
                                        return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
                                    }}
                                    stroke="#64748b"
                                    style={{ fontSize: '12px' }}
                                />
                                <YAxis
                                    domain={['auto', 'auto']}
                                    tickFormatter={(value) => `$${value.toFixed(2)}`}
                                    stroke="#64748b"
                                    style={{ fontSize: '12px' }}
                                />
                                <Tooltip content={<CustomTooltip />} />
                                <Legend />
                                <Line
                                    type="monotone"
                                    dataKey="calculatedPrice"
                                    stroke="#3b82f6"
                                    strokeWidth={2}
                                    dot={false}
                                    name="Calculated from Swaps"
                                    connectNulls={false}
                                />
                                <Line
                                    type="monotone"
                                    dataKey="referencePrice"
                                    stroke="#f59e0b"
                                    strokeWidth={2}
                                    dot={false}
                                    name="Reference Data"
                                    connectNulls={false}
                                />
                                <Brush
                                    dataKey="time"
                                    height={30}
                                    stroke="#3b82f6"
                                    tickFormatter={(timestamp: number) => {
                                        const date = new Date(timestamp);
                                        return date.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
                                    }}
                                    fill="#0F1629"
                                />
                            </LineChart>
                        </ResponsiveContainer>
                    ) : (
                        <div className="h-[400px] flex items-center justify-center text-slate-400">
                            No CACAO price data available for this time range
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

'use client';

import { useEffect, useState } from 'react';

interface Transaction {
    tx_index: number;
    tx_hash: string;
    timestamp: string;
    in_asset: string;
    in_amount: string;
    in_volume_usd: number;
    in_price_usd: number;
    out_asset: string;
    out_amount: string;
    out_volume_usd: number;
    out_price_usd: number;
    affiliate_address: string;
    affiliate_basis_points: number;
    affiliate_fee_usd: number;
    fee_amount: number;
    fee_asset: string;
    fee_asset_price: number;
}

export default function ThorchainDebugPage() {
    const [transactions, setTransactions] = useState<Transaction[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        fetchData();
    }, []);

    const fetchData = async () => {
        try {
            const res = await fetch('/api/debug/txs?chain=thorchain');
            const json = await res.json();
            if (json.transactions) {
                setTransactions(json.transactions);
            }
        } catch (error) {
            console.error('Failed to fetch transactions:', error);
        } finally {
            setLoading(false);
        }
    };

    const formatTxHash = (hash: string) => {
        if (!hash) return 'N/A';
        return `${hash.substring(0, 4)}...${hash.substring(hash.length - 4)}`;
    };

    const formatAmount = (amount: string | number, decimals: number = 8) => {
        if (!amount) return '0';
        const val = typeof amount === 'string' ? parseFloat(amount) : amount;
        const scaled = val / Math.pow(10, decimals);
        return scaled.toLocaleString(undefined, { maximumFractionDigits: 4 });
    };

    const formatUSD = (val: number) => {
        return `$${val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    };

    const formatPrice = (val: number) => {
        return `$${val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 })}`;
    };

    const formatCompactDate = (dateStr: string) => {
        const date = new Date(dateStr);
        return `${(date.getMonth() + 1).toString().padStart(2, '0')}/${date.getDate().toString().padStart(2, '0')}/${date.getFullYear().toString().slice(-2)} ${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}`;
    };

    if (loading) {
        return <div className="p-8 text-white">Loading transactions...</div>;
    }

    return (
        <div className="p-8 min-h-screen bg-[#0A0F1E] text-slate-300">
            <h1 className="text-3xl font-bold text-white mb-6">THORChain Transactions Debug</h1>

            <div className="overflow-x-auto h-[calc(100vh-200px)] relative">
                <table className="w-full text-sm text-left text-slate-400">
                    <thead className="text-xs text-slate-200 uppercase bg-[#1A1F35] sticky top-0 z-10 shadow-md">
                        <tr>
                            <th className="px-4 py-3 text-right">#</th>
                            <th className="px-4 py-3 whitespace-nowrap">Time</th>
                            <th className="px-4 py-3">TxID</th>
                            <th className="px-4 py-3">Source Asset</th>
                            <th className="px-4 py-3 text-right">In Vol $</th>
                            <th className="px-4 py-3 text-right">In Price</th>
                            <th className="px-4 py-3">Dest Asset</th>
                            <th className="px-4 py-3 text-right">Out Vol $</th>
                            <th className="px-4 py-3 text-right">Out Price</th>
                            <th className="px-4 py-3">Affiliate</th>
                            <th className="px-4 py-3 text-right">BPS</th>
                            <th className="px-4 py-3 text-right">Fee Amt</th>
                            <th className="px-4 py-3">Fee Asset</th>
                            <th className="px-4 py-3 text-right">Fee Price</th>
                        </tr>
                    </thead>
                    <tbody>
                        {transactions.map((tx) => (
                            <tr key={tx.tx_hash} className="border-b border-slate-800 hover:bg-[#0F1629]">
                                <td className="px-4 py-3 text-right font-mono text-slate-400">
                                    {tx.tx_index}
                                </td>
                                <td className="px-4 py-3 font-mono text-xs text-slate-500 whitespace-nowrap">
                                    {formatCompactDate(tx.timestamp)}
                                </td>
                                <td className="px-4 py-3 font-mono">
                                    <a
                                        href={`https://thorchain.net/tx/${tx.tx_hash}`}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="text-blue-400 hover:underline"
                                    >
                                        {formatTxHash(tx.tx_hash)}
                                    </a>
                                </td>
                                <td className="px-4 py-3 truncate max-w-[150px]" title={tx.in_asset}>
                                    {tx.in_asset.split('/').pop()}
                                </td>
                                <td className="px-4 py-3 text-right font-mono text-slate-200">
                                    {formatUSD(tx.in_volume_usd)}
                                </td>
                                <td className="px-4 py-3 text-right font-mono text-slate-400">
                                    {formatPrice(tx.in_price_usd)}
                                </td>
                                <td className="px-4 py-3 truncate max-w-[150px]" title={tx.out_asset}>
                                    {tx.out_asset.split('/').pop()}
                                </td>
                                <td className="px-4 py-3 text-right font-mono text-slate-200">
                                    {formatUSD(tx.out_volume_usd)}
                                </td>
                                <td className="px-4 py-3 text-right font-mono text-slate-400">
                                    {formatPrice(tx.out_price_usd)}
                                </td>
                                <td className="px-4 py-3 truncate max-w-[150px]" title={tx.affiliate_address}>
                                    {tx.affiliate_address}
                                </td>
                                <td className="px-4 py-3 text-right font-mono">
                                    {tx.affiliate_basis_points}
                                </td>
                                <td className="px-4 py-3 text-right font-mono text-green-400">
                                    {formatAmount(tx.fee_amount, 8)}
                                </td>
                                <td className="px-4 py-3 truncate max-w-[100px]" title={tx.fee_asset}>
                                    {tx.fee_asset.split('/').pop()}
                                </td>
                                <td className="px-4 py-3 text-right font-mono text-blue-400">
                                    {tx.fee_asset_price > 0 ? formatPrice(tx.fee_asset_price) : '-'}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

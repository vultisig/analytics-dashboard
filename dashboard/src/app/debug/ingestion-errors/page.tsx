'use client';

import { useEffect, useState } from 'react';

interface ErrorStats {
    error_type: string;
    source: string;
    count: number;
    avg_retries: number;
    max_retries: number;
}

interface IngestionError {
    id: number;
    tx_hash: string;
    source: string;
    error_type: string;
    error_message: string;
    retry_count: number;
    last_retry_at: string | null;
    created_at: string;
}

export default function IngestionErrorsPage() {
    const [stats, setStats] = useState<ErrorStats[]>([]);
    const [errors, setErrors] = useState<IngestionError[]>([]);
    const [total, setTotal] = useState(0);
    const [loading, setLoading] = useState(true);
    const [filterSource, setFilterSource] = useState('');
    const [filterType, setFilterType] = useState('');

    useEffect(() => {
        fetchData();
        // Refresh every 30 seconds
        const interval = setInterval(fetchData, 30000);
        return () => clearInterval(interval);
    }, [filterSource, filterType]);

    const fetchData = async () => {
        try {
            const params = new URLSearchParams();
            if (filterSource) params.set('source', filterSource);
            if (filterType) params.set('errorType', filterType);

            const response = await fetch(`/api/debug/ingestion-errors?${params}`);
            const data = await response.json();

            setStats(data.stats || []);
            setErrors(data.errors || []);
            setTotal(data.total || 0);
        } catch (error) {
            console.error('Failed to fetch errors:', error);
        } finally {
            setLoading(false);
        }
    };

    const handleRetry = async (txHash: string) => {
        try {
            await fetch('/api/debug/ingestion-errors', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ txHash })
            });
            alert(`Retry queued for ${txHash}`);
        } catch (error) {
            alert('Failed to queue retry');
        }
    };

    if (loading) {
        return <div className="p-8">Loading...</div>;
    }

    return (
        <div className="p-8 space-y-6 bg-gray-50 min-h-screen">
            <div className="flex justify-between items-center">
                <h1 className="text-3xl font-bold">Ingestion Errors Debug</h1>
                <div className="text-sm text-gray-600">
                    Total Errors: <span className="font-bold text-lg">{total}</span>
                </div>
            </div>

            {/* Summary Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {stats.slice(0, 3).map((stat, idx) => (
                    <div key={idx} className="bg-white rounded-lg shadow p-6">
                        <h3 className="text-sm font-semibold text-gray-600 mb-2">
                            {stat.source} - {stat.error_type}
                        </h3>
                        <div className="text-3xl font-bold text-gray-900">{stat.count}</div>
                        <div className="text-xs text-gray-500 mt-2">
                            Avg Retries: {parseFloat(stat.avg_retries.toString()).toFixed(1)} |
                            Max: {stat.max_retries}
                        </div>
                    </div>
                ))}
            </div>

            {/* Filters */}
            <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-lg font-semibold mb-4">Filters</h2>
                <div className="flex gap-4 flex-wrap">
                    <select
                        value={filterSource}
                        onChange={(e) => setFilterSource(e.target.value)}
                        className="border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                        <option value="">All Sources</option>
                        <option value="thorchain">THORChain</option>
                        <option value="mayachain">MAYAChain</option>
                        <option value="lifi">LI.FI</option>
                    </select>

                    <select
                        value={filterType}
                        onChange={(e) => setFilterType(e.target.value)}
                        className="border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    >
                        <option value="">All Error Types</option>
                        <option value="missing_price">Missing Price</option>
                        <option value="missing_rune_price">Missing RUNE Price</option>
                        <option value="missing_cacao_price">Missing CACAO Price</option>
                        <option value="invalid_metadata">Invalid Metadata</option>
                    </select>

                    <button
                        onClick={() => { setFilterSource(''); setFilterType(''); }}
                        className="px-4 py-2 bg-gray-200 rounded hover:bg-gray-300 transition-colors"
                    >
                        Clear Filters
                    </button>
                </div>
            </div>

            {/* Error Table */}
            <div className="bg-white rounded-lg shadow">
                <div className="p-6 border-b border-gray-200">
                    <h2 className="text-lg font-semibold">Recent Errors (Last 100)</h2>
                </div>
                <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                        <thead className="bg-gray-50 border-b border-gray-200">
                            <tr>
                                <th className="text-left p-3 font-semibold">TX Hash</th>
                                <th className="text-left p-3 font-semibold">Source</th>
                                <th className="text-left p-3 font-semibold">Error Type</th>
                                <th className="text-left p-3 font-semibold">Retries</th>
                                <th className="text-left p-3 font-semibold">Last Retry</th>
                                <th className="text-left p-3 font-semibold">Created</th>
                                <th className="text-left p-3 font-semibold">Action</th>
                            </tr>
                        </thead>
                        <tbody>
                            {errors.map((error) => (
                                <tr key={error.id} className="border-b border-gray-100 hover:bg-gray-50">
                                    <td className="p-3 font-mono text-xs">
                                        {error.tx_hash.substring(0, 16)}...
                                    </td>
                                    <td className="p-3 capitalize">{error.source}</td>
                                    <td className="p-3">
                                        <span className="px-2 py-1 bg-amber-100 text-amber-800 rounded text-xs">
                                            {error.error_type}
                                        </span>
                                    </td>
                                    <td className="p-3">
                                        <span className={`font-semibold ${error.retry_count >= 5 ? 'text-red-600' : 'text-gray-700'}`}>
                                            {error.retry_count}
                                        </span>
                                    </td>
                                    <td className="p-3 text-xs text-gray-600">
                                        {error.last_retry_at
                                            ? new Date(error.last_retry_at).toLocaleString()
                                            : 'Never'}
                                    </td>
                                    <td className="p-3 text-xs text-gray-600">
                                        {new Date(error.created_at).toLocaleString()}
                                    </td>
                                    <td className="p-3">
                                        <button
                                            onClick={() => handleRetry(error.tx_hash)}
                                            className="text-blue-600 hover:text-blue-800 hover:underline text-xs font-medium"
                                        >
                                            Retry Now
                                        </button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>

                    {errors.length === 0 && (
                        <div className="text-center py-12 text-gray-500">
                            <div className="text-lg">✅ No errors found</div>
                            <div className="text-sm mt-2">All transactions are processing successfully</div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}

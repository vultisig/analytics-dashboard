class VultisigAnalyticsDashboard {
    constructor() {
        this.charts = {};
        this.currentData = null;
        this.selectedChains = new Set(['thorchain', 'mayachain', 'lifi']);
        this.currentPeriod = 'daily';
        this.currentDateRange = null;

        this.init();
    }

    async init() {
        this.initEventListeners();
        this.setDefaultDateRange();
        await this.loadData();
    }

    initEventListeners() {
        // Chain selection checkboxes
        document.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
            checkbox.addEventListener('change', (e) => {
                if (e.target.checked) {
                    this.selectedChains.add(e.target.id);
                } else {
                    this.selectedChains.delete(e.target.id);
                }
                this.updateDisplayData();
            });
        });

        // Period selector
        document.querySelectorAll('.period-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');
                this.currentPeriod = e.target.dataset.period;
                this.updateDisplayData();
            });
        });

        // Date range
        document.getElementById('applyDateRange').addEventListener('click', () => {
            const startDate = document.getElementById('startDate').value;
            const endDate = document.getElementById('endDate').value;

            if (startDate && endDate) {
                this.currentDateRange = { start: startDate, end: endDate };
                this.updateDisplayData();
            }
        });

        // Chart toggles
        document.querySelectorAll('.chart-toggle').forEach(toggle => {
            toggle.addEventListener('click', (e) => {
                const container = e.target.closest('.chart-container');
                container.querySelectorAll('.chart-toggle').forEach(t => t.classList.remove('active'));
                e.target.classList.add('active');

                const chartType = e.target.dataset.chart;
                this.updateChart(container.querySelector('canvas').id, chartType);
            });
        });

        // Activity filter
        document.getElementById('activityFilter').addEventListener('change', (e) => {
            this.updateRecentActivity(e.target.value);
        });
    }

    setDefaultDateRange() {
        const today = new Date();
        const thirtyDaysAgo = new Date(today.getTime() - 30 * 24 * 60 * 60 * 1000);

        document.getElementById('endDate').value = today.toISOString().split('T')[0];
        document.getElementById('startDate').value = thirtyDaysAgo.toISOString().split('T')[0];

        this.currentDateRange = {
            start: thirtyDaysAgo.toISOString().split('T')[0],
            end: today.toISOString().split('T')[0]
        };
    }

    async loadData() {
        this.showLoading(true);

        try {
            // Load all required data
            const [summaryData, timeSeriesData, recentActivity] = await Promise.all([
                this.fetchSummaryData(),
                this.fetchTimeSeriesData(),
                this.fetchRecentActivity()
            ]);

            this.currentData = {
                summary: summaryData,
                timeSeries: timeSeriesData,
                recentActivity: recentActivity
            };

            this.updateDisplayData();
        } catch (error) {
            console.error('Error loading data:', error);
            this.showError('Failed to load data. Please try again.');
        } finally {
            this.showLoading(false);
        }
    }

    async fetchSummaryData() {
        const params = new URLSearchParams({
            chains: Array.from(this.selectedChains).join(','),
            ...(this.currentDateRange && {
                startDate: this.currentDateRange.start,
                endDate: this.currentDateRange.end
            })
        });

        const response = await fetch(`/api/summary?${params}`);
        if (!response.ok) throw new Error('Failed to fetch summary data');
        return await response.json();
    }

    async fetchTimeSeriesData() {
        const params = new URLSearchParams({
            chains: Array.from(this.selectedChains).join(','),
            period: this.currentPeriod,
            ...(this.currentDateRange && {
                startDate: this.currentDateRange.start,
                endDate: this.currentDateRange.end
            })
        });

        const response = await fetch(`/api/timeseries?${params}`);
        if (!response.ok) throw new Error('Failed to fetch time series data');
        return await response.json();
    }

    async fetchRecentActivity(chain = 'all', limit = 50) {
        const params = new URLSearchParams({
            chain: chain,
            limit: limit.toString()
        });

        const response = await fetch(`/api/recent-activity?${params}`);
        if (!response.ok) throw new Error('Failed to fetch recent activity');
        return await response.json();
    }

    updateDisplayData() {
        if (!this.currentData) return;

        this.updateSummaryMetrics();
        this.updateChainCounts();
        this.updateCharts();
        this.updateRecentActivity();
    }

    updateSummaryMetrics() {
        const data = this.currentData.summary;

        // Header stats - main metrics
        document.getElementById('headerTotalVolume').textContent = this.formatCurrencyWithCommas(data.totalVolume || 0);
        document.getElementById('headerTotalFees').textContent = this.formatCurrencyWithCommas(data.totalFees || 0);
        document.getElementById('headerUniqueUsers').textContent = (data.uniqueAddresses || 0).toLocaleString();
    }

    updateChainCounts() {
        const data = this.currentData.summary.chainBreakdown || {};

        document.getElementById('thorchainCount').textContent = (data.thorchain?.count || 0).toLocaleString();
        document.getElementById('mayachainCount').textContent = (data.mayachain?.count || 0).toLocaleString();
        document.getElementById('lifiCount').textContent = (data.lifi?.count || 0).toLocaleString();
    }

    updateCharts() {
        this.updateFeesChart();
        this.updateDistributionChart();
        this.updateAddressesChart();
        this.updateVolumeDistributionChart();
    }

    updateFeesChart() {
        const ctx = document.getElementById('feesChart').getContext('2d');
        const data = this.currentData.timeSeries;

        if (this.charts.feesChart) {
            this.charts.feesChart.destroy();
        }

        this.charts.feesChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.dates || [],
                datasets: [{
                    label: 'Total Fees',
                    data: data.fees || [],
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return '$' + value.toLocaleString(undefined, {
                                    minimumFractionDigits: 0,
                                    maximumFractionDigits: 0
                                });
                            }
                        }
                    }
                }
            }
        });
    }

    updateDistributionChart() {
        const ctx = document.getElementById('distributionChart').getContext('2d');
        const data = this.currentData.summary.chainBreakdown || {};

        if (this.charts.distributionChart) {
            this.charts.distributionChart.destroy();
        }

        const chainData = [];
        const chainLabels = [];
        const chainColors = [];

        Object.entries(data).forEach(([chain, info]) => {
            if (this.selectedChains.has(chain)) {
                chainLabels.push(this.getChainDisplayName(chain));
                chainData.push(info.totalFees || 0);
                chainColors.push(this.getChainColor(chain));
            }
        });

        this.charts.distributionChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: chainLabels,
                datasets: [{
                    data: chainData,
                    backgroundColor: chainColors,
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
    }

    updateAddressesChart() {
        const ctx = document.getElementById('addressesChart').getContext('2d');
        const data = this.currentData.timeSeries;

        if (this.charts.addressesChart) {
            this.charts.addressesChart.destroy();
        }

        this.charts.addressesChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: data.dates || [],
                datasets: [{
                    label: 'Unique Addresses',
                    data: data.uniqueAddresses || [],
                    backgroundColor: '#4ecdc4',
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return value.toLocaleString();
                            }
                        }
                    }
                }
            }
        });
    }

    updateVolumeDistributionChart() {
        const ctx = document.getElementById('volumeDistributionChart').getContext('2d');
        const data = this.currentData.summary.volumeTiers || {};

        if (this.charts.volumeDistributionChart) {
            this.charts.volumeDistributionChart.destroy();
        }

        const tiers = Object.keys(data).sort((a, b) => {
            const order = ['<=$100', '100-1000', '1000-5000', '5000-10000', '10000-50000',
                         '50000-100000', '100000-250000', '250000-500000', '500000-750000',
                         '750000-1000000', '>1000000'];
            return order.indexOf(a) - order.indexOf(b);
        });

        this.charts.volumeDistributionChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: tiers,
                datasets: [{
                    label: 'Number of Swaps',
                    data: tiers.map(tier => data[tier] || 0),
                    backgroundColor: '#45b7d1',
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: function(value) {
                                return value.toLocaleString();
                            }
                        }
                    },
                    x: {
                        ticks: {
                            maxRotation: 45
                        }
                    }
                }
            }
        });
    }

    async updateChart(canvasId, chartType) {
        if (chartType === 'volume' && canvasId === 'feesChart') {
            // Switch fees chart to volume
            const data = this.currentData.timeSeries;
            this.charts.feesChart.data.datasets[0] = {
                label: 'Total Volume',
                data: data.volume || [],
                borderColor: '#764ba2',
                backgroundColor: 'rgba(118, 75, 162, 0.1)',
                fill: true,
                tension: 0.4
            };
            this.charts.feesChart.update();
        } else if (chartType === 'fees' && canvasId === 'feesChart') {
            // Switch back to fees
            const data = this.currentData.timeSeries;
            this.charts.feesChart.data.datasets[0] = {
                label: 'Total Fees',
                data: data.fees || [],
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                fill: true,
                tension: 0.4
            };
            this.charts.feesChart.update();
        }
    }

    async updateRecentActivity(chainFilter = 'all') {
        const data = await this.fetchRecentActivity(chainFilter);
        const tbody = document.getElementById('activityTableBody');

        tbody.innerHTML = '';

        data.forEach(transaction => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${this.formatDate(transaction.timestamp)}</td>
                <td><span class="chain-badge chain-${transaction.source}">${this.getChainDisplayName(transaction.source)}</span></td>
                <td><span class="tx-hash" onclick="window.open('${this.getTxUrl(transaction.source, transaction.tx_hash)}', '_blank')">${this.truncateHash(transaction.tx_hash)}</span></td>
                <td>${this.formatCurrencyWithCommas(transaction.in_amount_usd || 0)}</td>
                <td>${this.formatCurrencyWithCommas(transaction.total_fee_usd || 0)}</td>
            `;
            tbody.appendChild(row);
        });
    }

    // Utility functions
    formatCurrency(amount) {
        if (amount >= 1000000) {
            return '$' + (amount / 1000000).toFixed(2) + 'M';
        } else if (amount >= 1000) {
            return '$' + (amount / 1000).toFixed(2) + 'K';
        } else {
            return '$' + amount.toFixed(2);
        }
    }

    formatCurrencyWithCommas(amount) {
        return '$' + amount.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        });
    }

    formatDate(dateString) {
        return new Date(dateString).toLocaleDateString('en-US', {
            month: 'short',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit'
        });
    }

    getChainDisplayName(chain) {
        const names = {
            'thorchain': 'THORChain',
            'mayachain': 'MayaChain',
            'lifi': 'LiFi'
        };
        return names[chain] || chain;
    }

    getChainColor(chain) {
        const colors = {
            'thorchain': '#ff6b6b',
            'mayachain': '#4ecdc4',
            'lifi': '#45b7d1'
        };
        return colors[chain] || '#667eea';
    }

    truncateHash(hash) {
        return hash ? `${hash.substring(0, 6)}...${hash.substring(hash.length - 4)}` : '';
    }

    getTxUrl(chain, hash) {
        const urls = {
            'thorchain': `https://viewblock.io/thorchain/tx/${hash}`,
            'mayachain': `https://www.mayascan.org/tx/${hash}`,
            'lifi': `https://explorer.li.fi/tx/${hash}`
        };
        return urls[chain] || '#';
    }

    showLoading(show) {
        const overlay = document.getElementById('loadingOverlay');
        if (show) {
            overlay.classList.remove('hidden');
        } else {
            overlay.classList.add('hidden');
        }
    }

    showError(message) {
        // Simple error display - could be enhanced with a proper modal
        alert(message);
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new VultisigAnalyticsDashboard();
});
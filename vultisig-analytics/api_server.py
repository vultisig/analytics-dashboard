#!/usr/bin/env python3
"""
VultisigAnalytics API Server
Provides REST endpoints for the frontend dashboard
"""
import logging
from datetime import datetime

from flask import Flask, jsonify, request
from flask_cors import CORS

from database.connection import db_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

@app.route('/api/summary')
def get_summary():
    """Get summary statistics with optional filtering"""
    try:
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')

        # Build WHERE clause for chain filtering
        chain_placeholders = ','.join(['%s'] * len(chains))
        where_conditions = [f"source IN ({chain_placeholders})"]
        params = list(chains)

        # Add date filtering if provided
        if start_date:
            where_conditions.append("date_only >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date_only <= %s")
            params.append(end_date)

        where_clause = " AND ".join(where_conditions)

        # Get overall summary statistics
        summary_query = f"""
            SELECT
                COUNT(*) as total_swaps,
                COALESCE(SUM(total_fee_usd), 0) as total_fees,
                COALESCE(SUM(in_amount_usd), 0) as total_volume,
                COUNT(DISTINCT user_address) as unique_addresses,
                COUNT(DISTINCT date_only) as active_days
            FROM swaps
            WHERE {where_clause}
        """

        summary_result = db_manager.execute_query(summary_query, params, fetch=True)[0]

        # Calculate averages
        active_days = max(summary_result['active_days'], 1)  # Avoid division by zero
        avg_daily_fees = summary_result['total_fees'] / active_days
        avg_daily_volume = summary_result['total_volume'] / active_days

        # Get chain breakdown
        chain_breakdown_query = f"""
            SELECT
                source,
                COUNT(*) as count,
                COALESCE(SUM(total_fee_usd), 0) as total_fees,
                COALESCE(SUM(in_amount_usd), 0) as total_volume,
                COUNT(DISTINCT user_address) as unique_addresses
            FROM swaps
            WHERE {where_clause}
            GROUP BY source
        """

        chain_results = db_manager.execute_query(chain_breakdown_query, params, fetch=True)
        chain_breakdown = {row['source']: row for row in chain_results}

        # Get volume tier distribution
        volume_tier_query = f"""
            SELECT
                volume_tier,
                COUNT(*) as count
            FROM swaps
            WHERE {where_clause} AND volume_tier IS NOT NULL
            GROUP BY volume_tier
            ORDER BY
                CASE volume_tier
                    WHEN '<=$100' THEN 1
                    WHEN '100-1000' THEN 2
                    WHEN '1000-5000' THEN 3
                    WHEN '5000-10000' THEN 4
                    WHEN '10000-50000' THEN 5
                    WHEN '50000-100000' THEN 6
                    WHEN '100000-250000' THEN 7
                    WHEN '250000-500000' THEN 8
                    WHEN '500000-750000' THEN 9
                    WHEN '750000-1000000' THEN 10
                    WHEN '>1000000' THEN 11
                    ELSE 12
                END
        """

        volume_tier_results = db_manager.execute_query(volume_tier_query, params, fetch=True)
        volume_tiers = {row['volume_tier']: row['count'] for row in volume_tier_results}

        return jsonify({
            'totalSwaps': summary_result['total_swaps'],
            'totalFees': float(summary_result['total_fees']),
            'totalVolume': float(summary_result['total_volume']),
            'uniqueAddresses': summary_result['unique_addresses'],
            'avgDailyFees': float(avg_daily_fees),
            'avgDailyVolume': float(avg_daily_volume),
            'chainBreakdown': {
                k: {
                    'count': v['count'],
                    'totalFees': float(v['total_fees']),
                    'totalVolume': float(v['total_volume']),
                    'uniqueAddresses': v['unique_addresses']
                } for k, v in chain_breakdown.items()
            },
            'volumeTiers': volume_tiers
        })

    except Exception as e:
        logger.error(f"Error getting summary data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/overview-chart')
def get_overview_chart():
    """Get overview stats for the dashboard header"""
    try:
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        if chains and len(chains) > 0:
            placeholders = ','.join(['%s'] * len(chains))
            where_conditions.append(f"LOWER(source) IN ({placeholders})")
            params.extend(chains)
        
        if start_date:
            where_conditions.append("date_only >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date_only <= %s")
            params.append(end_date)
            
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Get aggregate stats
        stats_query = f"""
            SELECT
                COALESCE(SUM(in_amount_usd), 0) as total_volume,
                COALESCE(SUM(total_fee_usd), 0) as total_fees,
                COUNT(DISTINCT user_address) as unique_users,
                COUNT(*) as total_swaps
            FROM swaps
            {where_clause}
        """
        
        result = db_manager.execute_query(stats_query, params, fetch=True)[0]
        
        return jsonify({
            'stats': {
                'total_volume': float(result['total_volume']),
                'total_fees': float(result['total_fees']),
                'unique_users': result['unique_users'],
                'total_swaps': result['total_swaps']
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting overview chart: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/timeseries')
def get_timeseries():
    """Get time series data for charts"""
    try:
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
        period = request.args.get('period', 'daily')  # daily, weekly, monthly
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')

        # Determine date truncation based on period
        date_trunc_map = {
            'daily': 'day',
            'weekly': 'week',
            'monthly': 'month'
        }
        date_trunc = date_trunc_map.get(period, 'day')

        # Build WHERE clause for chain filtering
        chain_placeholders = ','.join(['%s'] * len(chains))
        where_conditions = [f"source IN ({chain_placeholders})"]
        params = list(chains)

        # Add date filtering if provided
        if start_date:
            where_conditions.append("date_only >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date_only <= %s")
            params.append(end_date)

        where_clause = " AND ".join(where_conditions)

        # Get time series data
        timeseries_query = f"""
            SELECT
                DATE_TRUNC('{date_trunc}', date_only) as period,
                COALESCE(SUM(total_fee_usd), 0) as fees,
                COALESCE(SUM(in_amount_usd), 0) as volume,
                COUNT(DISTINCT user_address) as unique_addresses,
                COUNT(*) as swap_count
            FROM swaps
            WHERE {where_clause}
            GROUP BY DATE_TRUNC('{date_trunc}', date_only)
            ORDER BY period
        """

        results = db_manager.execute_query(timeseries_query, params, fetch=True)

        # Format results for frontend
        dates = []
        fees = []
        volume = []
        unique_addresses = []
        swap_counts = []

        for row in results:
            period_date = row['period']
            if isinstance(period_date, str):
                period_date = datetime.fromisoformat(period_date.replace('Z', '+00:00'))

            # Format date based on period
            if period == 'daily':
                date_str = period_date.strftime('%Y-%m-%d')
            elif period == 'weekly':
                date_str = f"Week of {period_date.strftime('%Y-%m-%d')}"
            else:  # monthly
                date_str = period_date.strftime('%Y-%m')

            dates.append(date_str)
            fees.append(float(row['fees']))
            volume.append(float(row['volume']))
            unique_addresses.append(row['unique_addresses'])
            swap_counts.append(row['swap_count'])

        return jsonify({
            'dates': dates,
            'fees': fees,
            'volume': volume,
            'uniqueAddresses': unique_addresses,
            'swapCounts': swap_counts
        })

    except Exception as e:
        logger.error(f"Error getting timeseries data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/timeseries/stacked')
def get_stacked_timeseries():
    """Get timeseries data grouped by provider for stacked charts"""
    try:
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
        period = request.args.get('period', 'daily')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        
        date_trunc = 'week' if period == 'weekly' else 'month' if period == 'monthly' else 'day'
        
        where_conditions = []
        params = []
        
        # Filter chains
        if chains and len(chains) > 0:
            placeholders = ','.join(['%s'] * len(chains))
            where_conditions.append(f"LOWER(source) IN ({placeholders})")
            params.extend(chains)

        if start_date:
            where_conditions.append("date_only >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date_only <= %s")
            params.append(end_date)
            
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT 
                DATE_TRUNC('{date_trunc}', date_only) as period,
                source,
                COALESCE(SUM(total_fee_usd), 0) as fees,
                COALESCE(SUM(in_amount_usd), 0) as volume,
                COUNT(DISTINCT user_address) as unique_addresses,
                COUNT(*) as swap_count
            FROM swaps
            {where_clause}
            GROUP BY 1, 2
            ORDER BY 1, 2
        """
        
        results = db_manager.execute_query(query, params, fetch=True)
        
        # Pivot data: { '2023-01-01': { date: '...', thorchain: 100, lifi: 50 }, ... }
        pivoted = {}
        providers = set()
        
        for row in results:
            period_date = row['period']
            if isinstance(period_date, str):
                period_date = datetime.fromisoformat(period_date.replace('Z', '+00:00'))
            
            date_key = period_date.strftime('%Y-%m-%d')
            if date_key not in pivoted:
                pivoted[date_key] = {'date': date_key}
            
            provider = row['source'].lower() # Normalize key
            providers.add(provider)
            pivoted[date_key][provider] = float(row['volume']) # Default to volume? Or return all metrics?
            # We need flexibility. Let's return object where key is [provider]_[metric] or just nested?
            # Recharts wants flattened: { date: '...', thorchain: 100, lifi: 50 }
            
            # Since we have multiple metrics (volume, fees, users), maybe we need separate endpoints or return all?
            # Let's populate specific fields based on suffix or just provider name implies volume?
            # Let's support 'metric' query param?
            
            # Actually, let's just return objects with provider names as keys for the value. 
            # But we have multiple metrics.
            # Client usually asks for one chart at a time.
            pivoted[date_key][provider] = float(row['volume'])
            pivoted[date_key][f"{provider}_fees"] = float(row['fees'])
            pivoted[date_key][f"{provider}_users"] = row['unique_addresses']
            pivoted[date_key][f"{provider}_swaps"] = row['swap_count']

        data = sorted(pivoted.values(), key=lambda x: x['date'])
        
        return jsonify({
            'data': data,
            'providers': list(providers)
        })
        
    except Exception as e:
        logger.error(f"Error getting stacked timeseries: {e}")
        return jsonify({'error': str(e)}), 500
def get_recent_activity():
    """Get recent transaction activity"""
    try:
        chain = request.args.get('chain', 'all')
        limit = int(request.args.get('limit', 50))

        # Build WHERE clause for chain filtering
        where_conditions = []
        params = []

        if chain != 'all':
            where_conditions.append("source = %s")
            params.append(chain)

        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

        # Get recent transactions
        activity_query = f"""
            SELECT
                timestamp,
                source,
                tx_hash,
                user_address,
                in_asset,
                out_asset,
                in_amount_usd,
                out_amount_usd,
                total_fee_usd,
                affiliate_fee_usd,
                liquidity_fee_usd,
                network_fee_usd
            FROM swaps
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT %s
        """

        params.append(limit)
        results = db_manager.execute_query(activity_query, params, fetch=True)

        # Format results for frontend
        activities = []
        for row in results:
            activity = {
                'timestamp': row['timestamp'].isoformat() if row['timestamp'] else None,
                'source': row['source'],
                'tx_hash': row['tx_hash'],
                'user_address': row['user_address'],
                'in_asset': row['in_asset'],
                'out_asset': row['out_asset'],
                'in_amount_usd': float(row['in_amount_usd']) if row['in_amount_usd'] else 0,
                'out_amount_usd': float(row['out_amount_usd']) if row['out_amount_usd'] else 0,
                'total_fee_usd': float(row['total_fee_usd']) if row['total_fee_usd'] else 0,
                'affiliate_fee_usd': float(row['affiliate_fee_usd']) if row['affiliate_fee_usd'] else 0,
                'liquidity_fee_usd': float(row['liquidity_fee_usd']) if row['liquidity_fee_usd'] else 0,
                'network_fee_usd': float(row['network_fee_usd']) if row['network_fee_usd'] else 0
            }
            activities.append(activity)

        return jsonify(activities)

    except Exception as e:
        logger.error(f"Error getting recent activity: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        result = db_manager.execute_query('SELECT 1 as status', fetch=True)
        if result and result[0]['status'] == 1:
            return jsonify({'status': 'healthy', 'database': 'connected'})
        else:
            return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 500
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    """Get basic database statistics"""
    try:
        stats_query = """
            SELECT
                source,
                COUNT(*) as count,
                MIN(timestamp) as earliest_swap,
                MAX(timestamp) as latest_swap,
                COALESCE(SUM(total_fee_usd), 0) as total_fees,
                COALESCE(SUM(in_amount_usd), 0) as total_volume
            FROM swaps
            GROUP BY source
            ORDER BY source
        """

        results = db_manager.execute_query(stats_query, fetch=True)

        stats = {}
        total_swaps = 0
        total_fees = 0
        total_volume = 0

        for row in results:
            source = row['source']
            count = row['count']
            total_swaps += count
            total_fees += float(row['total_fees'])
            total_volume += float(row['total_volume'])

            stats[source] = {
                'count': count,
                'earliest_swap': row['earliest_swap'].isoformat() if row['earliest_swap'] else None,
                'latest_swap': row['latest_swap'].isoformat() if row['latest_swap'] else None,
                'total_fees': float(row['total_fees']),
                'total_volume': float(row['total_volume'])
            }

        stats['total'] = {
            'count': total_swaps,
            'total_fees': total_fees,
            'total_volume': total_volume
        }

        return jsonify(stats)

    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats/provider')
def get_stats_by_provider():
    """Get statistics grouped by provider"""
    try:
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
        
        where_conditions = []
        params = []
        
        # Filter by requested chains/providers
        if chains and len(chains) > 0:
            placeholders = ','.join(['%s'] * len(chains))
            # Map chain names if necessary or assume source matches chain for major ones
            # For simplicity, we filter source IN chains OR (source='lifi' AND chain IN chains)
            # But the 'chains' param here usually maps to 'source' in our context (thorchain, mayachain, lifi)
            where_conditions.append(f"LOWER(source) IN ({placeholders})")
            params.extend(chains)

        if start_date:
            where_conditions.append("date_only >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date_only <= %s")
            params.append(end_date)
            
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT 
                source as provider,
                COUNT(*) as count,
                COALESCE(SUM(total_fee_usd), 0) as total_fees,
                COALESCE(SUM(in_amount_usd), 0) as total_volume,
                COUNT(DISTINCT user_address) as unique_users
            FROM swaps
            {where_clause}
            GROUP BY source
        """
        
        results = db_manager.execute_query(query, params, fetch=True)
        
        data = []
        for row in results:
            data.append({
                'name': row['provider'],
                'value': float(row['total_volume']), # Default value for pie/donut
                'total_volume': float(row['total_volume']),
                'total_fees': float(row['total_fees']),
                'count': row['count'],
                'unique_users': row['unique_users']
            })
            
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting provider stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats/platform')
def get_stats_by_platform():
    """Get statistics grouped by platform"""
    try:
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        provider = request.args.get('provider')
        
        where_conditions = []
        params = []
        
        if chains and len(chains) > 0:
            placeholders = ','.join(['%s'] * len(chains))
            where_conditions.append(f"LOWER(source) IN ({placeholders})")
            params.extend(chains)
        
        if start_date:
            where_conditions.append("date_only >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date_only <= %s")
            params.append(end_date)
        if provider:
            where_conditions.append("source = %s")
            params.append(provider)
            
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT 
                COALESCE(platform, 'Unknown') as platform,
                COUNT(*) as count,
                COALESCE(SUM(total_fee_usd), 0) as total_fees,
                COALESCE(SUM(in_amount_usd), 0) as total_volume,
                COUNT(DISTINCT user_address) as unique_users
            FROM swaps
            {where_clause}
            GROUP BY COALESCE(platform, 'Unknown')
        """
        
        results = db_manager.execute_query(query, params, fetch=True)
        
        data = []
        for row in results:
            data.append({
                'name': row['platform'],
                'value': float(row['total_volume']),
                'total_volume': float(row['total_volume']),
                'total_fees': float(row['total_fees']),
                'count': row['count'],
                'unique_users': row['unique_users']
            })
            
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting platform stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats/chain')
def get_stats_by_chain():
    """Get statistics grouped by chain (mostly for 1inch/LiFi)"""
    try:
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        provider = request.args.get('provider')
        
        where_conditions = []
        params = []
        
        if chains and len(chains) > 0:
            placeholders = ','.join(['%s'] * len(chains))
            where_conditions.append(f"LOWER(source) IN ({placeholders})")
            params.extend(chains)
        
        if start_date:
            where_conditions.append("date_only >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date_only <= %s")
            params.append(end_date)
        if provider:
            where_conditions.append("source = %s")
            params.append(provider)
            
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Extract chain from in_asset (e.g., ETH-1 -> 1, ETH.ETH -> ETH)
        # Or better yet we should have a chain column, but parsing asset is okay for now
        # Example format: ASSET-CHAINID or CHAIN.ASSET
        # We can try to extract from pool_1/pool_2 if available or asset
        
        # Simple extraction for now - this might need refinement
        # Assuming in_asset format contains chain info
        query = f"""
            SELECT 
                CASE 
                    WHEN source = 'thorchain' THEN 'THORChain'
                    WHEN source = 'mayachain' THEN 'MayaChain'
                    ELSE split_part(in_asset, '-', 2) -- Approximation for LiFi
                END as chain,
                COUNT(*) as count,
                COALESCE(SUM(total_fee_usd), 0) as total_fees,
                COALESCE(SUM(in_amount_usd), 0) as total_volume
            FROM swaps
            {where_clause}
            GROUP BY 1
        """
        
        results = db_manager.execute_query(query, params, fetch=True)
        
        data = []
        for row in results:
            chain_name = row['chain'] or 'Unknown'
            # Cleanup chain ID to name map if needed
            
            data.append({
                'name': chain_name,
                'value': float(row['total_volume']),
                'total_volume': float(row['total_volume']),
                'total_fees': float(row['total_fees']),
                'count': row['count']
            })
            
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting chain stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/top-paths')
def get_top_paths():
    """Get top swap paths"""
    try:
        metric = request.args.get('metric', 'volume') # volume, count, fees
        limit = int(request.args.get('limit', 10))
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
        provider = request.args.get('provider')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        
        where_conditions = []
        params = []
        
        if chains and len(chains) > 0:
            placeholders = ','.join(['%s'] * len(chains))
            where_conditions.append(f"LOWER(source) IN ({placeholders})")
            params.extend(chains)
        
        if start_date:
            where_conditions.append("date_only >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date_only <= %s")
            params.append(end_date)
        if provider:
            where_conditions.append("source = %s")
            params.append(provider)
            
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        order_by = "total_volume DESC"
        if metric == 'count':
            order_by = "count DESC"
        elif metric == 'fees':
            order_by = "total_fees DESC"
            
        # Construct readable path name
        # e.g. ETH.ETH -> BTC.BTC
        
        query = f"""
            SELECT 
                in_asset,
                out_asset,
                COUNT(*) as count,
                COALESCE(SUM(in_amount_usd), 0) as total_volume,
                COALESCE(SUM(total_fee_usd), 0) as total_fees
            FROM swaps
            {where_clause}
            GROUP BY in_asset, out_asset
            ORDER BY {order_by}
            LIMIT %s
        """
        
        params.append(limit)
        results = db_manager.execute_query(query, params, fetch=True)
        
        data = []
        for row in results:
            # Format path name nicely
            in_name = row['in_asset'].split('-')[0] if '-' in row['in_asset'] else row['in_asset']
            out_name = row['out_asset'].split('-')[0] if '-' in row['out_asset'] else row['out_asset']
            if '.' in in_name: in_name = in_name.split('.')[1]
            if '.' in out_name: out_name = out_name.split('.')[1]
            
            path_name = f"{in_name} -> {out_name}"
            
            val = 0
            if metric == 'volume': val = float(row['total_volume'])
            elif metric == 'count': val = row['count']
            elif metric == 'fees': val = float(row['total_fees'])
            
            data.append({
                'name': path_name,
                'path': f"{row['in_asset']} -> {row['out_asset']}",
                'value': val,
                'count': row['count'],
                'volume': float(row['total_volume']),
                'fees': float(row['total_fees'])
            })
            
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting top paths: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("🚀 Starting VultisigAnalytics API server...")
    logger.info("📊 Dashboard will be available at http://localhost:8080")
    app.run(host='0.0.0.0', port=8080, debug=True)
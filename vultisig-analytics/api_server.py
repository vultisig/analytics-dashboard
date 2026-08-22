#!/usr/bin/env python3
"""
VultisigAnalytics API Server
Provides REST endpoints for the frontend dashboard
"""
import logging
import ipaddress
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from functools import wraps
from threading import Lock

import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

from database.connection import db_manager
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Dex-revenue rows live in dex_aggregator_revenue; swapkit is sender-classified.
ARKHAM_PROVIDERS = config.ARKHAM_PROVIDERS
DEX_REVENUE_PROVIDERS = config.DEX_REVENUE_PROVIDERS
SWAPKIT_PROTOCOL = config.SWAPKIT_PROTOCOL
KNOWN_PROVIDERS = ('thorchain', 'mayachain', 'lifi') + DEX_REVENUE_PROVIDERS
# Volume/count must not treat batched payouts as swaps.
DEX_VOLUME_PAYOUT_EXCLUDE = (
    "AND NOT (protocol = %s AND LOWER(from_address) IN %s)"
)
DEX_VOLUME_PARAMS = (
    DEX_REVENUE_PROVIDERS,
    SWAPKIT_PROTOCOL,
    tuple(sorted(config.SWAPKIT_PAYOUT_SENDERS)),
)
DEX_REVENUE_PARAMS = (DEX_REVENUE_PROVIDERS,)
SWAPKIT_FEE_SENDERS = tuple(sorted(config.SWAPKIT_FEE_SENDERS))
ARKHAM_SWAP_PATH_EXPR = """
    CASE
        WHEN token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL
            THEN COALESCE(NULLIF(chain, ''), 'Unknown chain') || ': ' || token_in_symbol || ' -> ' || token_out_symbol
        ELSE COALESCE(NULLIF(chain, ''), 'Unknown chain') || ': token symbols unavailable'
    END
"""


@app.errorhandler(ValueError)
def handle_value_error(e):
    return jsonify({'error': str(e)}), 400


# =============================================================================
# Constants and Configuration
# =============================================================================

# Short parameter names (matching frontend urlParams)
SHORT_PARAMS = {
    'GRANULARITY': 'g',
    'RANGE': 'r',
    'START_DATE': 'sd',
    'END_DATE': 'ed',
    'TAB': 't'
}

# Short values for granularity
SHORT_VALUES = {
    'GRAN_HOUR': 'h',
    'GRAN_DAY': 'd',
    'GRAN_WEEK': 'w',
    'GRAN_MONTH': 'm',
    'RANGE_1D': '1d',
    'RANGE_7D': '7d',
    'RANGE_30D': '30d',
    'RANGE_90D': '90d',
    'RANGE_YTD': 'ytd',
    'RANGE_1Y': '1y',
    'RANGE_ALL': 'all',
    'RANGE_CUSTOM': 'custom'
}

# Granularity mapping
GRAN_TO_SQL = {
    'h': 'hour',
    'd': 'day',
    'w': 'week',
    'm': 'month',
    'hour': 'hour',
    'day': 'day',
    'week': 'week',
    'month': 'month'
}

# Range mapping
RANGE_TO_SQL = {
    '1d': '24h',
    '7d': '7d',
    '30d': '30d',
    '90d': '90d',
    'ytd': 'ytd',
    '1y': '365d',
    'all': 'all',
    'custom': 'custom'
}

# Tier configuration
TIER_ORDER = ['Ultimate', 'Diamond', 'Platinum', 'Gold', 'Silver', 'Bronze', 'None']
TIER_DISCOUNTS = {
    'None': 0,
    'Bronze': 5,
    'Silver': 10,
    'Gold': 20,
    'Platinum': 25,
    'Diamond': 35,
    'Ultimate': 50
}

# Vultisig affiliate codes
VULTISIG_CODES = ['vi', 'va', 'v0']

# Rate limiting configuration
RATE_LIMIT_WINDOW_MS = 60 * 1000  # 1 minute
RATE_LIMIT_MAX_REQUESTS = 10  # holder lookup: 10 requests per minute per IP
PUBLIC_RATE_LIMIT_MAX_REQUESTS = 120  # any /api/ route: 120 requests per minute per IP
MAX_TRACKED_IPS = 10000  # cap per-store memory before pruning expired windows
TRUSTED_PROXY_CIDRS = os.getenv(
    'TRUSTED_PROXY_CIDRS',
    '127.0.0.0/8,::1/128,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,fc00::/7,fe80::/10'
)
rate_limit_store = defaultdict(lambda: {'count': 0, 'reset_time': 0})
public_rate_limit_store = defaultdict(lambda: {'count': 0, 'reset_time': 0})
rate_limit_lock = threading.Lock()
public_rate_limit_lock = threading.Lock()

# Row-count caps for externally supplied `limit` values on public endpoints
MAX_ACTIVITY_LIMIT = 200
MAX_TOP_PATHS_LIMIT = 100

# Compare each Vultisig route with its matching venue instead of combining DEX
# and aggregator totals (which would double-count trades routed by an
# aggregator into an underlying DEX). THORChain and LI.FI use DefiLlama's
# independently indexed series. MayaChain uses the protocol's official
# Midgard swap history because DefiLlama only reports THORSwap-routed volume
# for the Mayachain network, not total MAYAChain swap volume.
DEFILLAMA_API_BASE_URL = 'https://api.llama.fi/summary'
DEFILLAMA_SOURCE_URL = 'https://defillama.com/dexs'
MAYACHAIN_MIDGARD_SWAPS_URL = 'https://midgard.mayachain.info/v2/history/swaps'
MAYACHAIN_MIDGARD_SOURCE_URL = 'https://midgard.mayachain.info/v2/doc'
MARKET_BENCHMARKS = {
    'thorchain': {
        'label': 'THORChain',
        'market': 'THORChain DEX',
        'category': 'dexs',
        'slug': 'thorchain-dex',
        'comparison': 'All THORChain DEX volume',
        'source': 'defillama',
    },
    'lifi': {
        'label': 'LI.FI',
        'market': 'LI.FI DEX Aggregator',
        'category': 'aggregators',
        'slug': 'li.fi-dex-aggregator',
        'comparison': 'Same-chain LI.FI swap volume',
        'source': 'defillama',
    },
    'mayachain': {
        'label': 'MayaChain',
        'market': 'MAYAChain network',
        'comparison': 'All MAYAChain network swap volume',
        'source': 'midgard',
    },
}
# Midgard SwapHistory `totalVolumeUSD` is Int64(e2) — USD cents of both swap legs.
MAYACHAIN_VOLUME_USD_CENTS = 100
MAYACHAIN_MIDGARD_PAGE_SIZE = 400
MAYACHAIN_MIDGARD_MAX_PAGES = 4
DEFILLAMA_TIMEOUT_SECONDS = 10
MAYACHAIN_MIDGARD_TIMEOUT_SECONDS = 15
GLOBAL_MARKET_CACHE_TTL_SECONDS = 600
GLOBAL_MARKET_CACHE_FAILURE_TTL_SECONDS = 60
SHARE_TO_PERCENT = 100
MARKET_SHARE_CACHE_CONTROL = 'public, max-age=60, stale-while-revalidate=240'
MARKET_RANGE_DAY_SPANS = {
    '24h': 1,
    '7d': 7,
    '30d': 30,
    '90d': 90,
    '365d': 365,
}
MARKET_SHARE_NOTES = (
    "Each provider share is of that venue's published series; THOR/Maya and LI.FI are not the same market definition.",
    'All routes is a directional blend of unlike venue series on overlapping dates — not a unique global-volume total.',
    'LI.FI compares same-chain Vultisig swaps with DefiLlama same-chain LI.FI volume.',
    'MayaChain compares Vultisig routes with MAYAChain Midgard network swap volume (totalVolumeUSD is USD cents).',
)
_global_market_cache = {'data': None, 'expires_at': 0.0, 'filling': False}
_global_market_cache_lock = Lock()
ISO_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# =============================================================================
# Helper Functions
# =============================================================================

def get_param(args, param_key):
    """Get parameter value supporting both short and long formats"""
    # Try short format first
    short_key = SHORT_PARAMS.get(param_key, param_key)
    value = args.get(short_key)

    # Fall back to long format
    if value is None:
        long_key_map = {
            'GRANULARITY': 'granularity',
            'RANGE': 'range',
            'START_DATE': 'startDate',
            'END_DATE': 'endDate',
            'TAB': 'tab'
        }
        long_key = long_key_map.get(param_key, param_key)
        value = args.get(long_key)

    return value


def parse_limit(args, default, maximum):
    """Parse a public row-count limit, clamped to [1, maximum]."""
    raw = args.get('limit')
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError('Invalid limit parameter, expected an integer')
    return min(max(value, 1), maximum)


def parse_trusted_proxy_networks(raw):
    """Parse trusted proxy CIDR configuration, ignoring invalid entries."""
    networks = []
    for item in raw.split(','):
        cidr = item.strip()
        if not cidr:
            continue
        try:
            networks.append(ipaddress.ip_network(cidr, strict=False))
        except ValueError:
            logger.warning("Ignoring invalid TRUSTED_PROXY_CIDRS entry: %s", cidr)
    return tuple(networks)


TRUSTED_PROXY_NETWORKS = parse_trusted_proxy_networks(TRUSTED_PROXY_CIDRS)


def parse_granularity(granularity_param):
    """Parse granularity parameter to SQL date_trunc value"""
    if not granularity_param:
        return 'day'
    return GRAN_TO_SQL.get(granularity_param, 'day')


def build_date_filter(range_param, start_date_param=None, end_date_param=None):
    """
    Build date filter SQL fragments for swaps and Arkham tables.

    Returns tuple: (date_filter_swaps, date_filter_arkham, params)
    """
    date_filter = ''
    date_filter_arkham = ''
    now = datetime.utcnow()

    # Validate date parameters to prevent SQL injection
    if start_date_param and not ISO_DATE_RE.match(start_date_param):
        raise ValueError("Invalid start date format, expected YYYY-MM-DD")
    if end_date_param and not ISO_DATE_RE.match(end_date_param):
        raise ValueError("Invalid end date format, expected YYYY-MM-DD")

    # Map short range to canonical values
    range_value = RANGE_TO_SQL.get(range_param, range_param) if range_param else 'all'

    if range_value == 'custom' and start_date_param and end_date_param:
        date_filter = f"AND date_only >= '{start_date_param}'::date AND date_only <= '{end_date_param}'::date"
        date_filter_arkham = f"AND DATE(timestamp) >= '{start_date_param}'::date AND DATE(timestamp) <= '{end_date_param}'::date"
    elif range_value == '24h':
        date_filter = "AND timestamp >= NOW() - INTERVAL '24 hours'"
        date_filter_arkham = "AND timestamp >= NOW() - INTERVAL '24 hours'"
    elif range_value == '7d':
        start_date = (now - timedelta(days=7)).strftime('%Y-%m-%d')
        date_filter = f"AND date_only >= '{start_date}'::date"
        date_filter_arkham = f"AND DATE(timestamp) >= '{start_date}'::date"
    elif range_value == '30d':
        start_date = (now - timedelta(days=30)).strftime('%Y-%m-%d')
        date_filter = f"AND date_only >= '{start_date}'::date"
        date_filter_arkham = f"AND DATE(timestamp) >= '{start_date}'::date"
    elif range_value == '90d':
        start_date = (now - timedelta(days=90)).strftime('%Y-%m-%d')
        date_filter = f"AND date_only >= '{start_date}'::date"
        date_filter_arkham = f"AND DATE(timestamp) >= '{start_date}'::date"
    elif range_value == 'ytd':
        year_start = f"{now.year}-01-01"
        date_filter = f"AND date_only >= '{year_start}'::date"
        date_filter_arkham = f"AND DATE(timestamp) >= '{year_start}'::date"
    elif range_value == '365d':
        start_date = (now - timedelta(days=365)).strftime('%Y-%m-%d')
        date_filter = f"AND date_only >= '{start_date}'::date"
        date_filter_arkham = f"AND DATE(timestamp) >= '{start_date}'::date"
    # For 'all' range, no filter is applied

    return date_filter, date_filter_arkham


def normalize_platform(platform_str):
    """Normalize platform names to Android, iOS, Web, or Other"""
    if not platform_str:
        return 'Other'
    platform_lower = platform_str.lower()
    if 'android' in platform_lower:
        return 'Android'
    elif 'ios' in platform_lower or 'iphone' in platform_lower:
        return 'iOS'
    elif 'web' in platform_lower or 'desktop' in platform_lower:
        return 'Web'
    return 'Other'


def get_platform_expression(provider):
    """Get SQL expression for platform based on provider type.

    Note: Uses %% to escape % for psycopg2 when query has parameter placeholders.
    """
    if provider in ('thorchain', 'mayachain'):
        return "COALESCE(platform, raw_data->'metadata'->'swap'->>'affiliateAddress', 'Unknown')"
    elif provider == 'lifi':
        return """
            CASE
                WHEN LOWER(COALESCE(platform, raw_data->'metadata'->>'integrator', 'Unknown')) LIKE '%%android%%' THEN 'Android'
                WHEN LOWER(COALESCE(platform, raw_data->'metadata'->>'integrator', 'Unknown')) LIKE '%%ios%%' THEN 'iOS'
                ELSE COALESCE(platform, raw_data->'metadata'->>'integrator', 'Unknown')
            END
        """
    return "COALESCE(platform, 'Unknown')"


def get_normalized_platform_case():
    """Get SQL CASE expression for normalized platform names"""
    return """
        CASE
            WHEN LOWER(COALESCE(platform, '')) LIKE '%android%' THEN 'Android'
            WHEN LOWER(COALESCE(platform, '')) LIKE '%ios%' THEN 'iOS'
            WHEN LOWER(COALESCE(platform, '')) LIKE '%web%' THEN 'Web'
            ELSE 'Other'
        END
    """


def get_client_ip():
    """Get client IP from request headers"""
    remote_addr = request.remote_addr or 'unknown'
    if not is_trusted_proxy(remote_addr):
        return remote_addr

    forwarded_for = get_forwarded_for_client_ip(request.headers.get('X-Forwarded-For'))
    if forwarded_for:
        return forwarded_for

    for header_name in ('CF-Connecting-IP', 'X-Real-IP'):
        candidate = parse_ip_address(request.headers.get(header_name))
        if candidate:
            return candidate

    return remote_addr


def parse_ip_address(value):
    """Return normalized IP text for a syntactically valid address."""
    if not value:
        return None
    try:
        return str(ipaddress.ip_address(value.strip()))
    except ValueError:
        return None


def is_trusted_proxy(address):
    """Whether an immediate peer is allowed to supply forwarding headers."""
    ip_text = parse_ip_address(address)
    if not ip_text:
        return False
    ip = ipaddress.ip_address(ip_text)
    return any(ip in network for network in TRUSTED_PROXY_NETWORKS)


def get_forwarded_for_client_ip(value):
    """Extract client IP from X-Forwarded-For, skipping trusted proxy hops."""
    if not value:
        return None

    chain = []
    for part in value.split(','):
        ip_text = parse_ip_address(part)
        if ip_text:
            chain.append(ip_text)

    if not chain:
        return None

    for ip_text in reversed(chain):
        if not is_trusted_proxy(ip_text):
            return ip_text

    return chain[0]


def check_rate_limit(ip, store, max_requests, lock=None):
    """
    Check a fixed-window per-IP rate limit against the given store.
    Returns dict with 'allowed', 'remaining', 'reset_in' keys.
    """
    if lock is None:
        class _NoopLock:
            def __enter__(self):
                return None

            def __exit__(self, exc_type, exc, tb):
                return False

        lock = _NoopLock()

    with lock:
        now = int(time.time() * 1000)

        record = store.get(ip)
        if record is None and len(store) >= MAX_TRACKED_IPS:
            to_delete = [k for k, v in store.items() if v['reset_time'] < now]
            for k in to_delete:
                del store[k]

            if len(store) >= MAX_TRACKED_IPS:
                overflow = len(store) - MAX_TRACKED_IPS + 1
                oldest_keys = [
                    k for k, _ in sorted(store.items(), key=lambda item: item[1]['reset_time'])[:overflow]
                ]
                for k in oldest_keys:
                    del store[k]

        record = store.get(ip, {'count': 0, 'reset_time': 0})

        if record['reset_time'] == 0 or now > record['reset_time']:
            # New window
            store[ip] = {'count': 1, 'reset_time': now + RATE_LIMIT_WINDOW_MS}
            return {'allowed': True, 'remaining': max_requests - 1, 'reset_in': RATE_LIMIT_WINDOW_MS}

        if record['count'] >= max_requests:
            return {
                'allowed': False,
                'remaining': 0,
                'reset_in': record['reset_time'] - now
            }

        record['count'] += 1
        return {
            'allowed': True,
            'remaining': max_requests - record['count'],
            'reset_in': record['reset_time'] - now
        }


@app.before_request
def limit_public_api():
    """Process-local per-IP guard for all public API routes; the stricter
    holder-lookup limit still applies on top. Edge limiting remains the
    primary DDoS control in deployment."""
    if not request.path.startswith('/api/') or request.method == 'OPTIONS':
        return None

    rate = check_rate_limit(
        get_client_ip(),
        public_rate_limit_store,
        PUBLIC_RATE_LIMIT_MAX_REQUESTS,
        public_rate_limit_lock
    )
    if rate['allowed']:
        return None

    reset_seconds = max(1, rate['reset_in'] // 1000)
    return jsonify({
        'error': 'Rate limit exceeded',
        'message': f'Too many requests. Please try again in {reset_seconds} seconds.'
    }), 429, {'Retry-After': str(reset_seconds)}


def is_valid_ethereum_address(address):
    """Validate Ethereum address format"""
    return bool(re.match(r'^0x[a-fA-F0-9]{40}$', address))


def safe_float(value, default=0.0):
    """Safely convert value to float"""
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    """Safely convert value to int"""
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default


# =============================================================================
# Market volume share
# =============================================================================

def _utc_today():
    return datetime.now(timezone.utc).date()


def _last_complete_utc_date():
    return _utc_today() - timedelta(days=1)


def _mayachain_interval_volume_usd(interval):
    volume_cents = safe_float(interval.get('totalVolumeUSD'), default=-1)
    if volume_cents < 0:
        return -1.0
    return volume_cents / MAYACHAIN_VOLUME_USD_CENTS


def _require_iso_date(value, field_name):
    if value is None:
        return None
    if not ISO_DATE_RE.match(value):
        raise ValueError(f"Invalid {field_name} format, expected YYYY-MM-DD")
    return value


def _date_sql(column_sql, start_date, end_date):
    clauses = []
    params = []
    if start_date:
        clauses.append(f"AND {column_sql} >= %s::date")
        params.append(start_date)
    if end_date:
        clauses.append(f"AND {column_sql} <= %s::date")
        params.append(end_date)
    return ' '.join(clauses), params


def _fetch_defillama_benchmark(provider, benchmark):
    """Fetch and validate one DefiLlama daily market-volume series."""
    url = (
        f"{DEFILLAMA_API_BASE_URL}/{benchmark['category']}/{benchmark['slug']}"
        '?dataType=dailyVolume'
    )
    response = requests.get(
        url,
        headers={
            'Accept': 'application/json',
            'User-Agent': 'VultisigAnalytics/1.0'
        },
        timeout=DEFILLAMA_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    chart = payload.get('totalDataChart') or []

    series = {}
    for point in chart:
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            continue
        timestamp = safe_int(point[0], default=0)
        volume = safe_float(point[1], default=-1)
        if timestamp <= 0 or volume < 0:
            continue
        date_key = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d')
        series[date_key] = volume

    if not series:
        raise ValueError(f"DefiLlama returned no volume history for {provider}")

    return {
        'provider': provider,
        'market': payload.get('name') or benchmark['market'],
        'series': series,
        'latest_date': max(series),
        'source': 'DefiLlama',
        'source_url': DEFILLAMA_SOURCE_URL,
    }


def _parse_midgard_intervals(intervals):
    series = {}
    oldest_start = None
    for interval in intervals:
        timestamp = safe_int(interval.get('startTime'), default=0)
        volume = _mayachain_interval_volume_usd(interval)
        if timestamp <= 0 or volume < 0:
            continue
        date_key = datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m-%d')
        series[date_key] = volume
        oldest_start = timestamp if oldest_start is None else min(oldest_start, timestamp)
    return series, oldest_start


def _fetch_mayachain_benchmark(provider, benchmark):
    """Fetch all available daily MAYAChain network swap volume from Midgard."""
    series = {}
    to_timestamp = None

    # Midgard caps each response at 400 intervals; bound the public GET walk.
    for _ in range(MAYACHAIN_MIDGARD_MAX_PAGES):
        params = {'interval': 'day', 'count': MAYACHAIN_MIDGARD_PAGE_SIZE}
        if to_timestamp is not None:
            params['to'] = to_timestamp
        response = requests.get(
            MAYACHAIN_MIDGARD_SWAPS_URL,
            params=params,
            headers={
                'Accept': 'application/json',
                'User-Agent': 'VultisigAnalytics/1.0'
            },
            timeout=MAYACHAIN_MIDGARD_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        intervals = response.json().get('intervals') or []
        if not intervals:
            break

        page, oldest_start = _parse_midgard_intervals(intervals)
        series.update(page)
        if (
            len(intervals) < MAYACHAIN_MIDGARD_PAGE_SIZE
            or oldest_start is None
            or oldest_start == to_timestamp
        ):
            break
        to_timestamp = oldest_start

    if not series:
        raise ValueError(f'MAYAChain Midgard returned no volume history for {provider}')

    return {
        'provider': provider,
        'market': benchmark['market'],
        'series': series,
        'latest_date': max(series),
        'source': 'MAYAChain Midgard',
        'source_url': MAYACHAIN_MIDGARD_SOURCE_URL,
    }


def _fetch_market_benchmark(provider, benchmark):
    """Fetch a provider benchmark from its configured independent source."""
    if benchmark.get('source') == 'midgard':
        return _fetch_mayachain_benchmark(provider, benchmark)
    return _fetch_defillama_benchmark(provider, benchmark)


def _build_global_market_snapshot(previous_providers):
    """Fetch every configured benchmark. Callers must not hold the cache lock."""
    providers = {}
    failures = []
    with ThreadPoolExecutor(max_workers=len(MARKET_BENCHMARKS)) as executor:
        futures = {
            executor.submit(_fetch_market_benchmark, provider, benchmark): provider
            for provider, benchmark in MARKET_BENCHMARKS.items()
        }
        for future in as_completed(futures):
            provider = futures[future]
            try:
                providers[provider] = future.result()
            except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
                failures.append(provider)
                if provider in previous_providers:
                    providers[provider] = previous_providers[provider]
                logger.warning(f"Market benchmark unavailable for {provider}: {exc}")

    if not providers:
        raise RuntimeError('Comparable market volume is temporarily unavailable')

    latest_date = max(item['latest_date'] for item in providers.values())
    served_previous = any(provider in previous_providers for provider in failures)
    return {
        'providers': providers,
        'updated_at': f'{latest_date}T00:00:00Z',
        'source': 'DefiLlama + MAYAChain Midgard',
        'source_url': DEFILLAMA_SOURCE_URL,
        'is_stale': served_previous,
    }


def _store_market_snapshot(snapshot):
    ttl = (
        GLOBAL_MARKET_CACHE_FAILURE_TTL_SECONDS
        if snapshot['is_stale']
        else GLOBAL_MARKET_CACHE_TTL_SECONDS
    )
    with _global_market_cache_lock:
        _global_market_cache['data'] = snapshot
        _global_market_cache['expires_at'] = time.time() + ttl
    return snapshot


def _stale_cached_snapshot():
    with _global_market_cache_lock:
        cached_data = _global_market_cache['data']
        if not cached_data:
            return None
        cached_data['is_stale'] = True
        _global_market_cache['expires_at'] = (
            time.time() + GLOBAL_MARKET_CACHE_FAILURE_TTL_SECONDS
        )
        return cached_data


def get_global_market_snapshot():
    """Return cached daily volume series for Vultisig's comparable markets."""
    now = time.time()
    with _global_market_cache_lock:
        cached_data = _global_market_cache['data']
        if cached_data and now < _global_market_cache['expires_at']:
            return cached_data
        if _global_market_cache['filling']:
            if cached_data:
                return cached_data
            raise RuntimeError('Comparable market volume is temporarily unavailable')
        _global_market_cache['filling'] = True
        previous_providers = (cached_data or {}).get('providers', {})

    try:
        return _store_market_snapshot(_build_global_market_snapshot(previous_providers))
    except RuntimeError:
        cached_data = _stale_cached_snapshot()
        if cached_data:
            return cached_data
        raise
    finally:
        with _global_market_cache_lock:
            _global_market_cache['filling'] = False


def _get_market_date_bounds(range_param, start_date_param=None, end_date_param=None):
    """Inclusive UTC calendar bounds ending on the last complete day."""
    range_value = RANGE_TO_SQL.get(range_param, range_param) if range_param else 'all'
    last_complete = _last_complete_utc_date()
    last_complete_iso = last_complete.isoformat()
    start_date_param = _require_iso_date(start_date_param, 'start date')
    end_date_param = _require_iso_date(end_date_param, 'end date')

    if range_value == 'custom' and start_date_param and end_date_param:
        end_date = min(end_date_param, last_complete_iso)
        start_date = min(start_date_param, end_date)
        return start_date, end_date

    if range_value in MARKET_RANGE_DAY_SPANS:
        span = MARKET_RANGE_DAY_SPANS[range_value]
        start_date = (last_complete - timedelta(days=span - 1)).isoformat()
        return start_date, last_complete_iso
    if range_value == 'ytd':
        year_start = f'{last_complete.year}-01-01'
        start_date = year_start if year_start <= last_complete_iso else last_complete_iso
        return start_date, last_complete_iso
    return None, last_complete_iso


def _market_bucket(date_key, granularity):
    """Bucket daily benchmark data without averaging market-share percentages."""
    parsed = datetime.strptime(date_key, '%Y-%m-%d').date()
    if granularity == 'week':
        parsed -= timedelta(days=(parsed.weekday() + 1) % 7)
    elif granularity == 'month':
        parsed = parsed.replace(day=1)
    return parsed.isoformat()


def _query_swaps_volume(start_date, end_date, providers):
    date_filter, date_params = _date_sql('date_only', start_date, end_date)
    query = """
        SELECT
            TO_CHAR(date_only, 'YYYY-MM-DD') AS date,
            source AS provider,
            COALESCE(SUM(in_amount_usd), 0) AS volume
        FROM swaps
        WHERE source IN %s
          AND (
                source != 'lifi'
                OR (
                    NULLIF(raw_data #>> '{{bridge_metadata,from_chain_id}}', '') IS NOT NULL
                    AND raw_data #>> '{{bridge_metadata,from_chain_id}}' =
                        raw_data #>> '{{bridge_metadata,to_chain_id}}'
                )
          )
          {date_filter}
        GROUP BY date_only, source
        ORDER BY date_only ASC
    """.format(date_filter=date_filter)
    return list(db_manager.execute_query(
        query, (providers, *date_params), fetch=True,
    ))


def _query_aggregator_volume(start_date, end_date, providers):
    date_filter, date_params = _date_sql('DATE(timestamp)', start_date, end_date)
    query = """
        SELECT
            TO_CHAR(DATE(timestamp), 'YYYY-MM-DD') AS date,
            protocol AS provider,
            COALESCE(SUM(swap_volume_usd), 0) AS volume
        FROM dex_aggregator_revenue
        WHERE protocol IN %s
          AND (fee_data_source = 'etherscan'
               OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
          {date_filter}
        GROUP BY DATE(timestamp), protocol
        ORDER BY DATE(timestamp) ASC
    """.format(date_filter=date_filter)
    return list(db_manager.execute_query(
        query, (providers, *date_params), fetch=True,
    ))


def _query_route_volume(start_date, end_date):
    swaps_providers = tuple(
        provider for provider in MARKET_BENCHMARKS if provider not in ARKHAM_PROVIDERS
    )
    aggregator_providers = tuple(
        provider for provider in MARKET_BENCHMARKS if provider in ARKHAM_PROVIDERS
    )
    rows = _query_swaps_volume(start_date, end_date, swaps_providers)
    if aggregator_providers:
        rows.extend(_query_aggregator_volume(start_date, end_date, aggregator_providers))
    return rows


def _row_date_key(date_value):
    if hasattr(date_value, 'strftime'):
        return date_value.strftime('%Y-%m-%d')
    return str(date_value)[:10] if date_value else ''


def _index_vultisig_volume(rows):
    by_day = defaultdict(float)
    earliest = {}
    for row in rows:
        date_key = _row_date_key(row.get('date'))
        provider = str(row.get('provider') or '').lower()
        if provider not in MARKET_BENCHMARKS or not date_key:
            continue
        by_day[(provider, date_key)] += safe_float(row.get('volume'))
        earliest[provider] = min(date_key, earliest.get(provider, date_key))
    return by_day, earliest


def _build_share_point(date_key, provider, vultisig, market):
    return {
        'date': date_key,
        'provider': provider,
        'vultisigVolumeUsd': vultisig,
        'marketVolumeUsd': market,
        'sharePercent': (vultisig / market) * SHARE_TO_PERCENT if market > 0 else 0.0,
    }


def _include_market_day(date_key, start_date, end_date, provider_start, market_volume):
    if start_date and date_key < start_date:
        return False
    if end_date and date_key > end_date:
        return False
    if not start_date and provider_start and date_key < provider_start:
        return False
    return market_volume > 0


def _collect_provider_series(
    market, vultisig_by_day, earliest, start_date, end_date, granularity,
):
    buckets = defaultdict(lambda: {'vultisig': 0.0, 'market': 0.0})
    benchmarks = []
    for provider, benchmark in MARKET_BENCHMARKS.items():
        snapshot = market['providers'].get(provider)
        if not snapshot:
            continue
        benchmarks.append({
            'provider': provider,
            'label': benchmark['label'],
            'market': snapshot['market'],
            'comparison': benchmark['comparison'],
            'latestMarketDate': snapshot['latest_date'],
            'source': snapshot['source'],
            'sourceUrl': snapshot['source_url'],
        })
        provider_start = earliest.get(provider)
        for date_key, market_volume in snapshot['series'].items():
            if not _include_market_day(
                date_key, start_date, end_date, provider_start, market_volume,
            ):
                continue
            bucket = buckets[(provider, _market_bucket(date_key, granularity))]
            bucket['vultisig'] += vultisig_by_day[(provider, date_key)]
            bucket['market'] += market_volume
    return buckets, benchmarks


def _series_from_buckets(buckets):
    series = []
    blended = defaultdict(lambda: {'vultisig': 0.0, 'market': 0.0, 'providers': set()})
    for (provider, date_key), values in sorted(buckets.items(), key=lambda item: item[0][1]):
        series.append(_build_share_point(date_key, provider, values['vultisig'], values['market']))
        blended[date_key]['vultisig'] += values['vultisig']
        blended[date_key]['market'] += values['market']
        blended[date_key]['providers'].add(provider)
    return series, blended


def _append_all_routes(series, benchmarks, blended):
    present = {item['provider'] for item in benchmarks}
    configured = set(MARKET_BENCHMARKS)
    if present != configured:
        return series, benchmarks
    all_points = [
        _build_share_point(date_key, 'all', values['vultisig'], values['market'])
        for date_key, values in sorted(blended.items())
        if values['providers'] == configured
    ]
    all_benchmark = {
        'provider': 'all',
        'label': 'All routes',
        'market': 'Blended venue series',
        'comparison': "Directional blend of each venue's published series; not a single market",
        'latestMarketDate': min(item['latestMarketDate'] for item in benchmarks),
        'source': 'DefiLlama + MAYAChain Midgard',
        'sourceUrl': '',
    }
    return series + all_points, [all_benchmark, *benchmarks]


def _market_volume_share_payload(args) -> dict:
    requested = parse_granularity(get_param(args, 'GRANULARITY') or 'd')
    effective = 'day' if requested == 'hour' else requested
    start_date, end_date = _get_market_date_bounds(
        get_param(args, 'RANGE') or '30d',
        get_param(args, 'START_DATE'),
        get_param(args, 'END_DATE'),
    )
    vultisig_by_day, earliest = _index_vultisig_volume(
        _query_route_volume(start_date, end_date)
    )
    market = get_global_market_snapshot()
    buckets, benchmarks = _collect_provider_series(
        market, vultisig_by_day, earliest, start_date, end_date, effective,
    )
    series, blended = _series_from_buckets(buckets)
    series, benchmarks = _append_all_routes(series, benchmarks, blended)
    return {
        'series': series,
        'benchmarks': benchmarks,
        'requestedGranularity': requested,
        'effectiveGranularity': effective,
        'asOfDate': end_date,
        'updatedAt': market['updated_at'],
        'source': market['source'],
        'sourceUrl': market['source_url'],
        'isStale': market['is_stale'],
        'notes': list(MARKET_SHARE_NOTES),
    }


def merge_rows_by_key(rows, key, float_fields=(), int_fields=()):
    """Sum numeric fields when the same provider lands in two tables."""
    merged = {}
    order = []
    for row in rows:
        name = row[key]
        if name not in merged:
            merged[name] = dict(row)
            order.append(name)
            continue
        for field in float_fields:
            merged[name][field] = safe_float(merged[name].get(field)) + safe_float(row.get(field))
        for field in int_fields:
            merged[name][field] = safe_int(merged[name].get(field)) + safe_int(row.get(field))
    return [merged[name] for name in order]


def iso_utc(value):
    """Serialize a DB timestamp as ISO-8601 UTC.

    Every timestamp column is TIMESTAMPTZ and sessions are pinned to UTC
    (migrations/dex_aggregator_revenue_timestamptz.sql), so values arrive
    timezone-aware; this is the API's single serialization boundary.
    """
    return value.isoformat() if value is not None else None


# =============================================================================
# Existing Endpoints (Preserved)
# =============================================================================

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
        active_days = max(summary_result['active_days'], 1)
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
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/overview-chart')
def get_overview_chart():
    """Get overview stats for the dashboard header"""
    try:
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
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

        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

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
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/timeseries')
def get_timeseries():
    """Get time series data for charts"""
    try:
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
        period = request.args.get('period', 'daily')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')

        date_trunc_map = {
            'daily': 'day',
            'weekly': 'week',
            'monthly': 'month'
        }
        date_trunc = date_trunc_map.get(period, 'day')

        chain_placeholders = ','.join(['%s'] * len(chains))
        where_conditions = [f"source IN ({chain_placeholders})"]
        params = list(chains)

        if start_date:
            where_conditions.append("date_only >= %s")
            params.append(start_date)
        if end_date:
            where_conditions.append("date_only <= %s")
            params.append(end_date)

        where_clause = " AND ".join(where_conditions)

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

        dates = []
        fees = []
        volume = []
        unique_addresses = []
        swap_counts = []

        for row in results:
            period_date = row['period']
            if isinstance(period_date, str):
                period_date = datetime.fromisoformat(period_date.replace('Z', '+00:00'))

            if period == 'daily':
                date_str = period_date.strftime('%Y-%m-%d')
            elif period == 'weekly':
                date_str = f"Week of {period_date.strftime('%Y-%m-%d')}"
            else:
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
        return jsonify({'error': 'Internal server error'}), 500


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

        pivoted = {}
        providers = set()

        for row in results:
            period_date = row['period']
            if isinstance(period_date, str):
                period_date = datetime.fromisoformat(period_date.replace('Z', '+00:00'))

            date_key = period_date.strftime('%Y-%m-%d')
            if date_key not in pivoted:
                pivoted[date_key] = {'date': date_key}

            provider = row['source'].lower()
            providers.add(provider)
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
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/activity')
def get_recent_activity():
    """Get recent transaction activity"""
    limit = parse_limit(request.args, 50, MAX_ACTIVITY_LIMIT)
    try:
        chain = request.args.get('chain', 'all')

        where_conditions = []
        params = []

        if chain != 'all':
            where_conditions.append("source = %s")
            params.append(chain)

        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

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

        activities = []
        for row in results:
            activity = {
                'timestamp': iso_utc(row['timestamp']),
                'source': row['source'],
                'tx_hash': row['tx_hash'],
                'user_address': row['user_address'],
                'in_asset': row['in_asset'],
                'out_asset': row['out_asset'],
                'in_amount_usd': safe_float(row['in_amount_usd']),
                'out_amount_usd': safe_float(row['out_amount_usd']),
                'total_fee_usd': safe_float(row['total_fee_usd']),
                'affiliate_fee_usd': safe_float(row['affiliate_fee_usd']),
                'liquidity_fee_usd': safe_float(row['liquidity_fee_usd']),
                'network_fee_usd': safe_float(row['network_fee_usd'])
            }
            activities.append(activity)

        return jsonify(activities)

    except Exception as e:
        logger.error(f"Error getting recent activity: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    try:
        result = db_manager.execute_query('SELECT 1 as status', fetch=True)
        if result and result[0]['status'] == 1:
            return jsonify({'status': 'healthy', 'database': 'connected'})
        else:
            return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 500
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'unhealthy', 'error': 'Internal server error'}), 500


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
                'earliest_swap': iso_utc(row['earliest_swap']),
                'latest_swap': iso_utc(row['latest_swap']),
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
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/stats/provider')
def get_stats_by_provider():
    """Get statistics grouped by provider"""
    try:
        chains = request.args.get('chains', 'thorchain,lifi').split(',')
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
                'value': float(row['total_volume']),
                'total_volume': float(row['total_volume']),
                'total_fees': float(row['total_fees']),
                'count': row['count'],
                'unique_users': row['unique_users']
            })

        return jsonify(data)
    except Exception as e:
        logger.error(f"Error getting provider stats: {e}")
        return jsonify({'error': 'Internal server error'}), 500


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
        return jsonify({'error': 'Internal server error'}), 500


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

        query = f"""
            SELECT
                CASE
                    WHEN source = 'thorchain' THEN 'THORChain'
                    WHEN source = 'mayachain' THEN 'MayaChain'
                    ELSE split_part(in_asset, '-', 2)
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
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/top-paths')
def get_top_paths():
    """Get top swap paths"""
    limit = parse_limit(request.args, 10, MAX_TOP_PATHS_LIMIT)
    try:
        metric = request.args.get('metric', 'volume')
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
            in_name = row['in_asset'].split('-')[0] if '-' in row['in_asset'] else row['in_asset']
            out_name = row['out_asset'].split('-')[0] if '-' in row['out_asset'] else row['out_asset']
            if '.' in in_name:
                in_name = in_name.split('.')[1]
            if '.' in out_name:
                out_name = out_name.split('.')[1]

            path_name = f"{in_name} -> {out_name}"

            val = 0
            if metric == 'volume':
                val = float(row['total_volume'])
            elif metric == 'count':
                val = row['count']
            elif metric == 'fees':
                val = float(row['total_fees'])

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
        return jsonify({'error': 'Internal server error'}), 500


# =============================================================================
# NEW ENDPOINTS - Revenue
# =============================================================================

@app.route('/api/revenue')
def get_revenue():
    """Get fee revenue data with date filtering and granularity"""
    try:
        # Parse parameters
        granularity_param = get_param(request.args, 'GRANULARITY') or 'd'
        range_param = get_param(request.args, 'RANGE') or 'all'
        start_date_param = get_param(request.args, 'START_DATE')
        end_date_param = get_param(request.args, 'END_DATE')

        granularity = parse_granularity(granularity_param)
        date_filter, date_filter_arkham = build_date_filter(range_param, start_date_param, end_date_param)

        # For hourly granularity, use timestamp field
        date_field = 'timestamp' if granularity == 'hour' else 'date_only'

        # 1. Total Fee Revenue
        swaps_revenue_query = f"""
            SELECT COALESCE(SUM(affiliate_fee_usd), 0) as total_revenue
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
        """
        arkham_revenue_query = f"""
            SELECT COALESCE(SUM(actual_fee_usd), 0) as total_revenue
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                AND (fee_data_source = 'etherscan'
                     OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                {date_filter_arkham}
        """

        swaps_total = db_manager.execute_query(swaps_revenue_query, fetch=True)[0]
        arkham_total = db_manager.execute_query(arkham_revenue_query, DEX_REVENUE_PARAMS, fetch=True)[0]

        total_revenue_value = safe_float(swaps_total['total_revenue']) + safe_float(arkham_total['total_revenue'])

        # 2. Fee Revenue by Provider (Over Time)
        swaps_over_time_query = f"""
            SELECT
                date_trunc('{granularity}', {date_field}) as date,
                source,
                SUM(affiliate_fee_usd) as revenue
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY 1, 2
            ORDER BY 1 ASC
        """

        arkham_over_time_query = f"""
            SELECT
                date_trunc('{granularity}', timestamp) as date,
                protocol as source,
                COALESCE(SUM(actual_fee_usd), 0) as revenue
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                AND (fee_data_source = 'etherscan'
                     OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                {date_filter_arkham}
            GROUP BY 1, 2
            ORDER BY 1 ASC
        """

        swaps_over_time = db_manager.execute_query(swaps_over_time_query, fetch=True)
        arkham_over_time = db_manager.execute_query(arkham_over_time_query, DEX_REVENUE_PARAMS, fetch=True)

        revenue_over_time = sorted(
            list(swaps_over_time) + list(arkham_over_time),
            key=lambda x: x['date']
        )

        # 3. Total Revenue by Provider
        swaps_by_provider_query = f"""
            SELECT
                source as name,
                SUM(affiliate_fee_usd) as value
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY source
        """

        arkham_by_provider_query = f"""
            SELECT
                protocol as name,
                COALESCE(SUM(actual_fee_usd), 0) as value
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                AND (fee_data_source = 'etherscan'
                     OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                {date_filter_arkham}
            GROUP BY protocol
        """

        swaps_by_provider = db_manager.execute_query(swaps_by_provider_query, fetch=True)
        arkham_by_provider = db_manager.execute_query(arkham_by_provider_query, DEX_REVENUE_PARAMS, fetch=True)

        revenue_by_provider = sorted(
            merge_rows_by_key(
                list(swaps_by_provider) + list(arkham_by_provider),
                "name",
                float_fields=("value",),
            ),
            key=lambda x: safe_float(x['value']),
            reverse=True
        )

        # 4. Revenue by Platform Over Time (excludes 1inch)
        platform_case = get_normalized_platform_case()
        platform_revenue_time_query = f"""
            SELECT
                date_trunc('{granularity}', {date_field}) as date,
                {platform_case} as platform,
                SUM(affiliate_fee_usd) as revenue
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY 1, 2
            ORDER BY 1 ASC
        """

        revenue_by_platform_over_time = db_manager.execute_query(platform_revenue_time_query, fetch=True)

        # 5. Total Revenue by Platform
        platform_revenue_total_query = f"""
            SELECT
                {platform_case} as platform,
                SUM(affiliate_fee_usd) as total_revenue
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY 1
            ORDER BY total_revenue DESC
        """

        revenue_by_platform = db_manager.execute_query(platform_revenue_total_query, fetch=True)

        # 6. Top 10 Swap Paths by Provider
        swaps_paths_query = f"""
            WITH ranked_paths AS (
                SELECT
                    source,
                    in_asset || ' -> ' || out_asset as swap_path,
                    SUM(affiliate_fee_usd) as total_revenue,
                    COUNT(*) as swap_count,
                    ROW_NUMBER() OVER (PARTITION BY source ORDER BY SUM(affiliate_fee_usd) DESC) as rank
                FROM swaps
                WHERE source != '1inch'
                    {date_filter}
                GROUP BY source, in_asset, out_asset
            )
            SELECT source, swap_path, total_revenue, swap_count
            FROM ranked_paths
            WHERE rank <= 10
            ORDER BY source, total_revenue DESC
        """

        # Etherscan-sourced KyberSwap rows can lack token symbols. Keep them
        # visible with a chain-level fallback instead of emitting NULL labels.
        arkham_paths_query = f"""
            SELECT
                protocol as source,
                {ARKHAM_SWAP_PATH_EXPR} as swap_path,
                COALESCE(SUM(actual_fee_usd), 0) as total_revenue,
                COUNT(*) as swap_count
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                {date_filter_arkham}
            GROUP BY protocol, swap_path
            ORDER BY total_revenue DESC
            LIMIT 10
        """

        swaps_paths = db_manager.execute_query(swaps_paths_query, fetch=True)
        arkham_paths = db_manager.execute_query(arkham_paths_query, DEX_REVENUE_PARAMS, fetch=True)

        top_paths = list(swaps_paths) + list(arkham_paths)

        # 7. Provider-specific Data
        providers_list = list(KNOWN_PROVIDERS)
        provider_data = {}

        for provider in providers_list:
            if provider not in DEX_REVENUE_PROVIDERS:
                platform_expr = get_platform_expression(provider)
                platform_query = f"""
                    SELECT
                        {platform_expr} as name,
                        SUM(affiliate_fee_usd) as value
                    FROM swaps
                    WHERE source = %s
                        {date_filter}
                    GROUP BY 1
                    ORDER BY value DESC
                """
                platform_result = db_manager.execute_query(platform_query, (provider,), fetch=True)
                provider_data[provider] = {'platforms': list(platform_result)}
            else:
                chain_query = f"""
                    SELECT
                        chain as chain_id,
                        SUM(actual_fee_usd) as value
                    FROM dex_aggregator_revenue
                    WHERE protocol = %s
                        AND chain IS NOT NULL
                        AND (fee_data_source = 'etherscan'
                             OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                        {date_filter_arkham}
                    GROUP BY 1
                    ORDER BY value DESC
                """
                chain_result = db_manager.execute_query(chain_query, (provider,), fetch=True)
                provider_data[provider] = {'chains': list(chain_result)}

        return jsonify({
            'totalRevenue': {'total_revenue': total_revenue_value},
            'revenueOverTime': [
                {'date': iso_utc(r['date']), 'source': r['source'], 'revenue': safe_float(r['revenue'])}
                for r in revenue_over_time
            ],
            'revenueByPlatformOverTime': [
                {'date': iso_utc(r['date']), 'platform': r['platform'], 'revenue': safe_float(r['revenue'])}
                for r in revenue_by_platform_over_time
            ],
            'revenueByProvider': [
                {'name': r['name'], 'value': safe_float(r['value'])}
                for r in revenue_by_provider
            ],
            'revenueByPlatform': [
                {'platform': r['platform'], 'total_revenue': safe_float(r['total_revenue'])}
                for r in revenue_by_platform
            ],
            'topPaths': [
                {
                    'source': r['source'],
                    'swap_path': r['swap_path'],
                    'total_revenue': safe_float(r['total_revenue']),
                    'swap_count': safe_int(r['swap_count'])
                }
                for r in top_paths
            ],
            'providerData': provider_data
        })

    except Exception as e:
        logger.error(f"Error getting revenue data: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/revenue/provider/<provider>')
def get_revenue_by_provider(provider):
    """Get revenue data for a specific provider"""
    try:
        provider = provider.lower()

        granularity_param = get_param(request.args, 'GRANULARITY') or 'd'
        range_param = get_param(request.args, 'RANGE') or 'all'
        start_date_param = get_param(request.args, 'START_DATE')
        end_date_param = get_param(request.args, 'END_DATE')

        granularity = parse_granularity(granularity_param)
        date_filter, date_filter_arkham = build_date_filter(range_param, start_date_param, end_date_param)

        if provider in DEX_REVENUE_PROVIDERS:
            # Fetch from dex_aggregator_revenue
            time_series_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', timestamp) as date,
                    COALESCE(SUM(actual_fee_usd), 0) as revenue
                FROM dex_aggregator_revenue
                WHERE protocol = %s
                    AND (fee_data_source = 'etherscan'
                         OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    {date_filter_arkham}
                GROUP BY DATE_TRUNC('{granularity}', timestamp)
                ORDER BY DATE_TRUNC('{granularity}', timestamp) ASC
            """

            chain_breakdown_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', timestamp) as date,
                    chain,
                    COALESCE(SUM(actual_fee_usd), 0) as revenue
                FROM dex_aggregator_revenue
                WHERE protocol = %s
                    AND chain IS NOT NULL
                    AND (fee_data_source = 'etherscan'
                         OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    {date_filter_arkham}
                GROUP BY DATE_TRUNC('{granularity}', timestamp), chain
                ORDER BY DATE_TRUNC('{granularity}', timestamp) ASC
            """

            time_series = db_manager.execute_query(time_series_query, (provider,), fetch=True)
            chain_breakdown = db_manager.execute_query(chain_breakdown_query, (provider,), fetch=True)

            return jsonify({
                'provider': provider,
                'totalRevenue': [
                    {'date': iso_utc(r['date']), 'revenue': safe_float(r['revenue'])}
                    for r in time_series
                ],
                'platformBreakdown': [
                    {'date': iso_utc(r['date']), 'chain': r['chain'], 'revenue': safe_float(r['revenue'])}
                    for r in chain_breakdown
                ]
            })
        else:
            date_field = 'timestamp' if granularity == 'hour' else 'date_only'

            time_series_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', {date_field}) as date,
                    SUM(affiliate_fee_usd) as revenue
                FROM swaps
                WHERE source = %s
                    {date_filter}
                GROUP BY DATE_TRUNC('{granularity}', {date_field})
                ORDER BY DATE_TRUNC('{granularity}', {date_field}) ASC
            """

            platform_expr = get_platform_expression(provider)
            platform_breakdown_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', {date_field}) as date,
                    {platform_expr} as platform,
                    SUM(affiliate_fee_usd) as revenue
                FROM swaps
                WHERE source = %s
                    {date_filter}
                GROUP BY DATE_TRUNC('{granularity}', {date_field}), {platform_expr}
                ORDER BY DATE_TRUNC('{granularity}', {date_field}) ASC
            """

            time_series = db_manager.execute_query(time_series_query, (provider,), fetch=True)
            platform_breakdown = db_manager.execute_query(platform_breakdown_query, (provider,), fetch=True)

            return jsonify({
                'provider': provider,
                'totalRevenue': [
                    {'date': iso_utc(r['date']), 'revenue': safe_float(r['revenue'])}
                    for r in time_series
                ],
                'platformBreakdown': [
                    {'date': iso_utc(r['date']), 'platform': r['platform'], 'revenue': safe_float(r['revenue'])}
                    for r in platform_breakdown
                ]
            })

    except Exception as e:
        logger.error(f"Error getting {provider} revenue data: {e}")
        return jsonify({'error': 'Internal server error'}), 500


# =============================================================================
# NEW ENDPOINTS - Swap Volume
# =============================================================================

@app.route('/api/market-volume-share')
def get_market_volume_share():
    """Compare Vultisig routes with like-for-like DEX/aggregator markets."""
    try:
        response = jsonify(_market_volume_share_payload(request.args))
        response.headers['Cache-Control'] = MARKET_SHARE_CACHE_CONTROL
        return response
    except RuntimeError as exc:
        logger.warning(f"Comparable market volume unavailable: {exc}")
        return jsonify({'error': str(exc)}), 503
    except Exception as exc:
        logger.error(f"Error getting market volume share: {exc}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/swap-volume')
def get_swap_volume():
    """Get swap volume data with date filtering and granularity"""
    try:
        granularity_param = get_param(request.args, 'GRANULARITY') or 'd'
        range_param = get_param(request.args, 'RANGE') or 'all'
        start_date_param = get_param(request.args, 'START_DATE')
        end_date_param = get_param(request.args, 'END_DATE')

        granularity = parse_granularity(granularity_param)
        date_filter, date_filter_arkham = build_date_filter(range_param, start_date_param, end_date_param)

        date_field = 'timestamp' if granularity == 'hour' else 'date_only'

        # 1. Global Stats
        swaps_stats_query = f"""
            SELECT
                COALESCE(SUM(in_amount_usd), 0) as total_volume,
                COUNT(*) as total_swaps
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
        """

        arkham_stats_query = f"""
            SELECT
                COALESCE(SUM(COALESCE(swap_volume_usd, 0)), 0) as total_volume,
                COUNT(*) as total_swaps
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                {DEX_VOLUME_PAYOUT_EXCLUDE}
                AND (fee_data_source = 'etherscan'
                     OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                {date_filter_arkham}
        """

        swaps_stats = db_manager.execute_query(swaps_stats_query, fetch=True)[0]
        arkham_stats = db_manager.execute_query(arkham_stats_query, DEX_VOLUME_PARAMS, fetch=True)[0]

        global_stats = {
            'total_volume': safe_float(swaps_stats['total_volume']) + safe_float(arkham_stats['total_volume']),
            'total_swaps': safe_int(swaps_stats['total_swaps']) + safe_int(arkham_stats['total_swaps'])
        }

        # 2. Volume by Provider (Over Time)
        swaps_time_query = f"""
            SELECT
                DATE_TRUNC('{granularity}', {date_field}) as time_period,
                source,
                SUM(in_amount_usd) as volume
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY time_period, source
            ORDER BY time_period ASC
        """

        arkham_time_query = f"""
            SELECT
                DATE_TRUNC('{granularity}', timestamp) as time_period,
                protocol as source,
                COALESCE(SUM(swap_volume_usd), 0) as volume
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                {DEX_VOLUME_PAYOUT_EXCLUDE}
                AND (fee_data_source = 'etherscan'
                     OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                {date_filter_arkham}
            GROUP BY time_period, protocol
            ORDER BY time_period ASC
        """

        swaps_time = db_manager.execute_query(swaps_time_query, fetch=True)
        arkham_time = db_manager.execute_query(arkham_time_query, DEX_VOLUME_PARAMS, fetch=True)

        volume_over_time = sorted(
            list(swaps_time) + list(arkham_time),
            key=lambda x: x['time_period']
        )

        # 3. Total Volume by Provider
        swaps_provider_query = f"""
            SELECT
                source,
                SUM(in_amount_usd) as total_volume,
                COUNT(*) as swap_count
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY source
        """

        arkham_provider_query = f"""
            SELECT
                protocol as source,
                COALESCE(SUM(swap_volume_usd), 0) as total_volume,
                COUNT(*) as swap_count
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                {DEX_VOLUME_PAYOUT_EXCLUDE}
                AND (fee_data_source = 'etherscan'
                     OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                {date_filter_arkham}
            GROUP BY protocol
        """

        swaps_provider = db_manager.execute_query(swaps_provider_query, fetch=True)
        arkham_provider = db_manager.execute_query(arkham_provider_query, DEX_VOLUME_PARAMS, fetch=True)

        volume_by_provider = sorted(
            merge_rows_by_key(
                list(swaps_provider) + list(arkham_provider),
                "source",
                float_fields=("total_volume",),
                int_fields=("swap_count",),
            ),
            key=lambda x: safe_float(x['total_volume']),
            reverse=True
        )

        # 4. Volume by Platform Over Time
        platform_case = get_normalized_platform_case()
        platform_time_query = f"""
            SELECT
                DATE_TRUNC('{granularity}', {date_field}) as time_period,
                {platform_case} as platform,
                SUM(in_amount_usd) as volume
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY time_period, 2
            ORDER BY time_period ASC
        """

        volume_by_platform_over_time = db_manager.execute_query(platform_time_query, fetch=True)

        # 5. Total Volume by Platform
        platform_total_query = f"""
            SELECT
                {platform_case} as platform,
                SUM(in_amount_usd) as total_volume,
                COUNT(*) as swap_count
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY 1
            ORDER BY total_volume DESC
        """

        volume_by_platform = db_manager.execute_query(platform_total_query, fetch=True)

        # 6. Top Paths
        swaps_paths_query = f"""
            WITH ranked_paths AS (
                SELECT
                    source,
                    in_asset || ' -> ' || out_asset as swap_path,
                    SUM(in_amount_usd) as total_volume,
                    COUNT(*) as swap_count,
                    ROW_NUMBER() OVER (PARTITION BY source ORDER BY SUM(in_amount_usd) DESC) as rank
                FROM swaps
                WHERE source != '1inch'
                    {date_filter}
                GROUP BY source, in_asset, out_asset
            )
            SELECT source, swap_path, total_volume, swap_count
            FROM ranked_paths
            WHERE rank <= 10
            ORDER BY source, total_volume DESC
        """

        # See arkham_paths_query in /api/revenue — same chain-level fallback.
        arkham_paths_query = f"""
            SELECT
                protocol as source,
                {ARKHAM_SWAP_PATH_EXPR} as swap_path,
                COALESCE(SUM(swap_volume_usd), 0) as total_volume,
                COUNT(*) as swap_count
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                {DEX_VOLUME_PAYOUT_EXCLUDE}
                {date_filter_arkham}
            GROUP BY protocol, swap_path
            ORDER BY total_volume DESC
            LIMIT 10
        """

        swaps_paths = db_manager.execute_query(swaps_paths_query, fetch=True)
        arkham_paths = db_manager.execute_query(arkham_paths_query, DEX_VOLUME_PARAMS, fetch=True)

        top_paths = list(swaps_paths) + list(arkham_paths)

        # 7. Provider-specific data
        providers = list(KNOWN_PROVIDERS)
        provider_data = {}

        for prov in providers:
            if prov in DEX_REVENUE_PROVIDERS:
                fee_only = (
                    "AND LOWER(from_address) IN %s" if prov == SWAPKIT_PROTOCOL else ""
                )
                chain_params = (
                    (prov, SWAPKIT_FEE_SENDERS) if prov == SWAPKIT_PROTOCOL else (prov,)
                )
                chain_query = f"""
                    SELECT
                        chain,
                        COALESCE(SUM(swap_volume_usd), 0) as volume
                    FROM dex_aggregator_revenue
                    WHERE protocol = %s
                        {fee_only}
                        AND chain IS NOT NULL
                        AND (fee_data_source = 'etherscan'
                             OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                        {date_filter_arkham}
                    GROUP BY chain
                    ORDER BY volume DESC
                """
                chain_result = db_manager.execute_query(chain_query, chain_params, fetch=True)
                provider_data[prov] = {'chains': list(chain_result)}
            else:
                platform_expr = get_platform_expression(prov)
                platform_query = f"""
                    SELECT
                        {platform_expr} as platform,
                        SUM(in_amount_usd) as volume
                    FROM swaps
                    WHERE source = %s
                        {date_filter}
                    GROUP BY 1
                    ORDER BY volume DESC
                """
                platform_result = db_manager.execute_query(platform_query, (prov,), fetch=True)
                provider_data[prov] = {'platforms': list(platform_result)}

        return jsonify({
            'globalStats': global_stats,
            'volumeOverTime': [
                {
                    'date': iso_utc(r['time_period']),
                    'source': r['source'],
                    'volume': safe_float(r['volume'])
                }
                for r in volume_over_time
            ],
            'volumeByPlatformOverTime': [
                {
                    'date': iso_utc(r['time_period']),
                    'platform': r['platform'],
                    'volume': safe_float(r['volume'])
                }
                for r in volume_by_platform_over_time
            ],
            'volumeByProvider': [
                {
                    'source': r['source'],
                    'total_volume': safe_float(r['total_volume']),
                    'swap_count': safe_int(r['swap_count'])
                }
                for r in volume_by_provider
            ],
            'volumeByPlatform': [
                {
                    'platform': r['platform'],
                    'total_volume': safe_float(r['total_volume']),
                    'swap_count': safe_int(r['swap_count'])
                }
                for r in volume_by_platform
            ],
            'topPaths': [
                {
                    'source': r['source'],
                    'swap_path': r['swap_path'],
                    'total_volume': safe_float(r['total_volume']),
                    'swap_count': safe_int(r['swap_count'])
                }
                for r in top_paths
            ],
            'providerData': provider_data
        })

    except Exception as e:
        logger.error(f"Error getting swap volume data: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/swap-volume/provider/<provider>')
def get_swap_volume_by_provider(provider):
    """Get swap volume data for a specific provider"""
    try:
        provider = provider.lower()

        granularity_param = get_param(request.args, 'GRANULARITY') or 'd'
        range_param = get_param(request.args, 'RANGE') or 'all'
        start_date_param = get_param(request.args, 'START_DATE')
        end_date_param = get_param(request.args, 'END_DATE')

        granularity = parse_granularity(granularity_param)
        date_filter, date_filter_arkham = build_date_filter(range_param, start_date_param, end_date_param)

        if provider == SWAPKIT_PROTOCOL:
            date_field = 'timestamp' if granularity == 'hour' else 'date_only'
            time_series_query = f"""
                SELECT time_period, SUM(volume) as volume
                FROM (
                    SELECT
                        DATE_TRUNC('{granularity}', timestamp) as time_period,
                        COALESCE(SUM(swap_volume_usd), 0) as volume
                    FROM dex_aggregator_revenue
                    WHERE protocol = %s
                      AND LOWER(from_address) IN %s
                      AND (fee_data_source = 'etherscan'
                           OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                      {date_filter_arkham}
                    GROUP BY 1
                    UNION ALL
                    SELECT
                        DATE_TRUNC('{granularity}', {date_field}) as time_period,
                        SUM(in_amount_usd) as volume
                    FROM swaps
                    WHERE source = %s
                      {date_filter}
                    GROUP BY 1
                ) combined
                GROUP BY time_period
                ORDER BY time_period ASC
            """
            chain_breakdown_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', timestamp) as time_period,
                    chain,
                    COALESCE(SUM(swap_volume_usd), 0) as volume
                FROM dex_aggregator_revenue
                WHERE protocol = %s
                    AND LOWER(from_address) IN %s
                    AND chain IS NOT NULL
                    AND (fee_data_source = 'etherscan'
                         OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    {date_filter_arkham}
                GROUP BY time_period, chain
                ORDER BY time_period ASC
            """
            time_series = db_manager.execute_query(
                time_series_query,
                (provider, SWAPKIT_FEE_SENDERS, provider),
                fetch=True,
            )
            chain_breakdown = db_manager.execute_query(
                chain_breakdown_query,
                (provider, SWAPKIT_FEE_SENDERS),
                fetch=True,
            )

            return jsonify({
                'provider': provider,
                'totalVolume': [
                    {
                        'date': iso_utc(r['time_period']),
                        'volume': safe_float(r['volume'])
                    }
                    for r in time_series
                ],
                'platformBreakdown': [
                    {
                        'date': iso_utc(r['time_period']),
                        'chain': r['chain'],
                        'volume': safe_float(r['volume'])
                    }
                    for r in chain_breakdown
                ]
            })

        if provider in ARKHAM_PROVIDERS:
            time_series_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', timestamp) as time_period,
                    COALESCE(SUM(swap_volume_usd), 0) as volume
                FROM dex_aggregator_revenue
                WHERE protocol = %s
                    AND (fee_data_source = 'etherscan'
                         OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    {date_filter_arkham}
                GROUP BY time_period
                ORDER BY time_period ASC
            """

            chain_breakdown_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', timestamp) as time_period,
                    chain,
                    COALESCE(SUM(swap_volume_usd), 0) as volume
                FROM dex_aggregator_revenue
                WHERE protocol = %s
                    AND chain IS NOT NULL
                    AND (fee_data_source = 'etherscan'
                         OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    {date_filter_arkham}
                GROUP BY time_period, chain
                ORDER BY time_period ASC
            """

            time_series = db_manager.execute_query(time_series_query, (provider,), fetch=True)
            chain_breakdown = db_manager.execute_query(chain_breakdown_query, (provider,), fetch=True)

            return jsonify({
                'provider': provider,
                'totalVolume': [
                    {
                        'date': iso_utc(r['time_period']),
                        'volume': safe_float(r['volume'])
                    }
                    for r in time_series
                ],
                'platformBreakdown': [
                    {
                        'date': iso_utc(r['time_period']),
                        'chain': r['chain'],
                        'volume': safe_float(r['volume'])
                    }
                    for r in chain_breakdown
                ]
            })
        else:
            date_field = 'timestamp' if granularity == 'hour' else 'date_only'

            time_series_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', {date_field}) as time_period,
                    SUM(in_amount_usd) as volume
                FROM swaps
                WHERE source = %s
                    {date_filter}
                GROUP BY time_period
                ORDER BY time_period ASC
            """

            platform_expr = get_platform_expression(provider)
            platform_breakdown_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', {date_field}) as time_period,
                    {platform_expr} as platform,
                    SUM(in_amount_usd) as volume
                FROM swaps
                WHERE source = %s
                    {date_filter}
                GROUP BY time_period, {platform_expr}
                ORDER BY time_period ASC
            """

            time_series = db_manager.execute_query(time_series_query, (provider,), fetch=True)
            platform_breakdown = db_manager.execute_query(platform_breakdown_query, (provider,), fetch=True)

            return jsonify({
                'provider': provider,
                'totalVolume': [
                    {
                        'date': iso_utc(r['time_period']),
                        'volume': safe_float(r['volume'])
                    }
                    for r in time_series
                ],
                'platformBreakdown': [
                    {
                        'date': iso_utc(r['time_period']),
                        'platform': r['platform'],
                        'volume': safe_float(r['volume'])
                    }
                    for r in platform_breakdown
                ]
            })

    except Exception as e:
        logger.error(f"Error getting {provider} volume data: {e}")
        return jsonify({'error': 'Internal server error'}), 500


# =============================================================================
# NEW ENDPOINTS - Swap Count
# =============================================================================

@app.route('/api/swap-count')
def get_swap_count():
    """Get swap count data with date filtering and granularity"""
    try:
        granularity_param = get_param(request.args, 'GRANULARITY') or 'd'
        range_param = get_param(request.args, 'RANGE') or 'all'
        start_date_param = get_param(request.args, 'START_DATE')
        end_date_param = get_param(request.args, 'END_DATE')

        granularity = parse_granularity(granularity_param)
        date_filter, date_filter_arkham = build_date_filter(range_param, start_date_param, end_date_param)

        date_field = 'timestamp' if granularity == 'hour' else 'date_only'

        # 1. Total Count
        swaps_count_query = f"""
            SELECT COUNT(*) as total_count
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
        """

        arkham_count_query = f"""
            SELECT COUNT(*) as total_count
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                {DEX_VOLUME_PAYOUT_EXCLUDE}
                AND (fee_data_source = 'etherscan'
                     OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                {date_filter_arkham}
        """

        swaps_total = db_manager.execute_query(swaps_count_query, fetch=True)[0]
        arkham_total = db_manager.execute_query(arkham_count_query, DEX_VOLUME_PARAMS, fetch=True)[0]

        total_count = safe_int(swaps_total['total_count']) + safe_int(arkham_total['total_count'])

        # 2. Count by Provider (Over Time)
        swaps_over_time_query = f"""
            SELECT
                date_trunc('{granularity}', {date_field}) as date,
                source,
                COUNT(*) as count
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY 1, 2
            ORDER BY 1 ASC
        """

        arkham_over_time_query = f"""
            SELECT
                date_trunc('{granularity}', timestamp) as date,
                protocol as source,
                COUNT(*) as count
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                {DEX_VOLUME_PAYOUT_EXCLUDE}
                AND (fee_data_source = 'etherscan'
                     OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                {date_filter_arkham}
            GROUP BY 1, 2
            ORDER BY 1 ASC
        """

        swaps_over_time = db_manager.execute_query(swaps_over_time_query, fetch=True)
        arkham_over_time = db_manager.execute_query(arkham_over_time_query, DEX_VOLUME_PARAMS, fetch=True)

        count_over_time = sorted(
            list(swaps_over_time) + list(arkham_over_time),
            key=lambda x: x['date']
        )

        # 3. Count by Platform Over Time
        platform_case = get_normalized_platform_case()
        count_by_platform_over_time_query = f"""
            SELECT
                date_trunc('{granularity}', {date_field}) as time_period,
                {platform_case} as platform,
                COUNT(*) as count
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY 1, 2
            ORDER BY 1 ASC
        """

        count_by_platform_over_time = db_manager.execute_query(count_by_platform_over_time_query, fetch=True)

        # 4. Total Count by Provider
        swaps_by_provider_query = f"""
            SELECT
                source as name,
                COUNT(*) as value
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY source
        """

        arkham_by_provider_query = f"""
            SELECT
                protocol as name,
                COUNT(*) as value
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                {DEX_VOLUME_PAYOUT_EXCLUDE}
                AND (fee_data_source = 'etherscan'
                     OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                {date_filter_arkham}
            GROUP BY protocol
        """

        swaps_by_provider = db_manager.execute_query(swaps_by_provider_query, fetch=True)
        arkham_by_provider = db_manager.execute_query(arkham_by_provider_query, DEX_VOLUME_PARAMS, fetch=True)

        count_by_provider = sorted(
            merge_rows_by_key(
                list(swaps_by_provider) + list(arkham_by_provider),
                "name",
                int_fields=("value",),
            ),
            key=lambda x: safe_int(x['value']),
            reverse=True
        )

        # 5. Top Paths by Count
        swaps_paths_query = f"""
            WITH ranked_paths AS (
                SELECT
                    source,
                    in_asset || ' -> ' || out_asset as swap_path,
                    SUM(in_amount_usd) as total_volume,
                    COUNT(*) as swap_count,
                    ROW_NUMBER() OVER (PARTITION BY source ORDER BY COUNT(*) DESC) as rank
                FROM swaps
                WHERE source != '1inch'
                    {date_filter}
                GROUP BY source, in_asset, out_asset
            )
            SELECT source, swap_path, total_volume, swap_count
            FROM ranked_paths
            WHERE rank <= 10
            ORDER BY source, swap_count DESC
        """

        # See arkham_paths_query in /api/revenue — same chain-level fallback.
        arkham_paths_query = f"""
            SELECT
                protocol as source,
                {ARKHAM_SWAP_PATH_EXPR} as swap_path,
                COALESCE(SUM(swap_volume_usd), 0) as total_volume,
                COUNT(*) as swap_count
            FROM dex_aggregator_revenue
            WHERE protocol IN %s
                {DEX_VOLUME_PAYOUT_EXCLUDE}
                {date_filter_arkham}
            GROUP BY protocol, swap_path
            ORDER BY swap_count DESC
            LIMIT 10
        """

        swaps_paths = db_manager.execute_query(swaps_paths_query, fetch=True)
        arkham_paths = db_manager.execute_query(arkham_paths_query, DEX_VOLUME_PARAMS, fetch=True)

        top_paths = list(swaps_paths) + list(arkham_paths)

        # 6. Provider-specific data
        providers_list = list(KNOWN_PROVIDERS)
        provider_data = {}

        for prov in providers_list:
            if prov not in DEX_REVENUE_PROVIDERS:
                platform_expr = get_platform_expression(prov)
                platform_query = f"""
                    SELECT
                        {platform_expr} as name,
                        COUNT(*) as value
                    FROM swaps
                    WHERE source = %s
                        {date_filter}
                    GROUP BY 1
                    ORDER BY value DESC
                """
                platform_result = db_manager.execute_query(platform_query, (prov,), fetch=True)
                provider_data[prov] = {'platforms': list(platform_result)}
            else:
                fee_only = (
                    "AND LOWER(from_address) IN %s" if prov == SWAPKIT_PROTOCOL else ""
                )
                chain_params = (
                    (prov, SWAPKIT_FEE_SENDERS) if prov == SWAPKIT_PROTOCOL else (prov,)
                )
                chain_query = f"""
                    SELECT
                        chain as chain_id,
                        COUNT(*) as value
                    FROM dex_aggregator_revenue
                    WHERE protocol = %s
                        {fee_only}
                        AND chain IS NOT NULL
                        AND (fee_data_source = 'etherscan'
                             OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                        {date_filter_arkham}
                    GROUP BY 1
                    ORDER BY value DESC
                """
                chain_result = db_manager.execute_query(chain_query, chain_params, fetch=True)
                provider_data[prov] = {'chains': list(chain_result)}

        return jsonify({
            'totalCount': {'total_count': total_count},
            'countOverTime': [
                {'date': iso_utc(r['date']), 'source': r['source'], 'count': safe_int(r['count'])}
                for r in count_over_time
            ],
            'countByPlatformOverTime': [
                {
                    'date': iso_utc(r['time_period']),
                    'platform': r['platform'],
                    'count': safe_int(r['count'])
                }
                for r in count_by_platform_over_time
            ],
            'countByProvider': [
                {'name': r['name'], 'value': safe_int(r['value'])}
                for r in count_by_provider
            ],
            'topPaths': [
                {
                    'source': r['source'],
                    'swap_path': r['swap_path'],
                    'total_volume': safe_float(r['total_volume']),
                    'swap_count': safe_int(r['swap_count'])
                }
                for r in top_paths
            ],
            'providerData': provider_data
        })

    except Exception as e:
        logger.error(f"Error getting swap count data: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/swap-count/provider/<provider>')
def get_swap_count_by_provider(provider):
    """Get swap count data for a specific provider"""
    try:
        provider = provider.lower()

        granularity_param = get_param(request.args, 'GRANULARITY') or 'd'
        range_param = get_param(request.args, 'RANGE') or 'all'
        start_date_param = get_param(request.args, 'START_DATE')
        end_date_param = get_param(request.args, 'END_DATE')

        granularity = parse_granularity(granularity_param)
        date_filter, date_filter_arkham = build_date_filter(range_param, start_date_param, end_date_param)

        if provider in DEX_REVENUE_PROVIDERS:
            fee_only = (
                "AND LOWER(from_address) IN %s" if provider == SWAPKIT_PROTOCOL else ""
            )
            count_params = (
                (provider, SWAPKIT_FEE_SENDERS)
                if provider == SWAPKIT_PROTOCOL
                else (provider,)
            )
            time_series_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', timestamp) as time_period,
                    COUNT(*) as count
                FROM dex_aggregator_revenue
                WHERE protocol = %s
                    {fee_only}
                    AND (fee_data_source = 'etherscan'
                         OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    {date_filter_arkham}
                GROUP BY time_period
                ORDER BY time_period ASC
            """

            chain_breakdown_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', timestamp) as time_period,
                    chain,
                    COUNT(*) as count
                FROM dex_aggregator_revenue
                WHERE protocol = %s
                    {fee_only}
                    AND chain IS NOT NULL
                    AND (fee_data_source = 'etherscan'
                         OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    {date_filter_arkham}
                GROUP BY time_period, chain
                ORDER BY time_period ASC
            """

            time_series = db_manager.execute_query(time_series_query, count_params, fetch=True)
            chain_breakdown = db_manager.execute_query(chain_breakdown_query, count_params, fetch=True)

            return jsonify({
                'provider': provider,
                'totalCount': [
                    {
                        'date': iso_utc(r['time_period']),
                        'count': safe_int(r['count'])
                    }
                    for r in time_series
                ],
                'platformBreakdown': [
                    {
                        'date': iso_utc(r['time_period']),
                        'chain': r['chain'],
                        'count': safe_int(r['count'])
                    }
                    for r in chain_breakdown
                ]
            })
        else:
            date_field = 'timestamp' if granularity == 'hour' else 'date_only'

            time_series_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', {date_field}) as time_period,
                    COUNT(*) as count
                FROM swaps
                WHERE source = %s
                    {date_filter}
                GROUP BY time_period
                ORDER BY time_period ASC
            """

            platform_expr = get_platform_expression(provider)
            platform_breakdown_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', {date_field}) as time_period,
                    {platform_expr} as platform,
                    COUNT(*) as count
                FROM swaps
                WHERE source = %s
                    {date_filter}
                GROUP BY time_period, {platform_expr}
                ORDER BY time_period ASC
            """

            time_series = db_manager.execute_query(time_series_query, (provider,), fetch=True)
            platform_breakdown = db_manager.execute_query(platform_breakdown_query, (provider,), fetch=True)

            return jsonify({
                'provider': provider,
                'totalCount': [
                    {
                        'date': iso_utc(r['time_period']),
                        'count': safe_int(r['count'])
                    }
                    for r in time_series
                ],
                'platformBreakdown': [
                    {
                        'date': iso_utc(r['time_period']),
                        'platform': r['platform'],
                        'count': safe_int(r['count'])
                    }
                    for r in platform_breakdown
                ]
            })

    except Exception as e:
        logger.error(f"Error getting {provider} count data: {e}")
        return jsonify({'error': 'Internal server error'}), 500


# =============================================================================
# NEW ENDPOINTS - Users
# =============================================================================

@app.route('/api/users')
def get_users():
    """Get user/swapper data with date filtering and granularity"""
    try:
        granularity_param = get_param(request.args, 'GRANULARITY') or 'd'
        range_param = get_param(request.args, 'RANGE') or 'all'
        start_date_param = get_param(request.args, 'START_DATE')
        end_date_param = get_param(request.args, 'END_DATE')

        granularity = parse_granularity(granularity_param)
        date_filter, date_filter_arkham = build_date_filter(range_param, start_date_param, end_date_param)

        date_field = 'timestamp' if granularity == 'hour' else 'date_only'

        # 1. Total Unique Swappers
        total_users_query = f"""
            SELECT COUNT(DISTINCT user_address) as unique_users
            FROM (
                SELECT user_address FROM swaps WHERE 1=1 {date_filter}
                UNION
                SELECT from_address as user_address FROM dex_aggregator_revenue
                WHERE 1=1
                    AND (fee_data_source = 'etherscan'
                         OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    {date_filter_arkham}
            ) combined_users
        """
        total_users = db_manager.execute_query(total_users_query, fetch=True)[0]

        # 2. Swappers by Provider (Over Time)
        if granularity == 'hour':
            users_over_time_query = f"""
                SELECT
                    date_trunc('hour', date) as date,
                    source,
                    COUNT(DISTINCT user_address) as users
                FROM (
                    SELECT timestamp as date, source, user_address FROM swaps WHERE 1=1 {date_filter}
                    UNION ALL
                    SELECT timestamp as date, protocol as source, from_address as user_address
                    FROM dex_aggregator_revenue
                    WHERE protocol IN %s
                        AND (fee_data_source = 'etherscan'
                             OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                        {date_filter_arkham}
                ) combined_swaps
                GROUP BY 1, 2
                ORDER BY 1 ASC
            """
        else:
            users_over_time_query = f"""
                SELECT
                    date_trunc('{granularity}', date) as date,
                    source,
                    COUNT(DISTINCT user_address) as users
                FROM (
                    SELECT date_only as date, source, user_address FROM swaps WHERE 1=1 {date_filter}
                    UNION ALL
                    SELECT DATE(timestamp) as date, protocol as source, from_address as user_address
                    FROM dex_aggregator_revenue
                    WHERE protocol IN %s
                        AND (fee_data_source = 'etherscan'
                             OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                        {date_filter_arkham}
                ) combined_swaps
                GROUP BY 1, 2
                ORDER BY 1 ASC
            """

        users_over_time = db_manager.execute_query(users_over_time_query, (ARKHAM_PROVIDERS,), fetch=True)

        # 3. Users by Platform Over Time
        platform_case = get_normalized_platform_case()
        users_by_platform_over_time_query = f"""
            SELECT
                date_trunc('{granularity}', {date_field}) as date,
                {platform_case} as platform,
                COUNT(DISTINCT user_address) as users
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY 1, 2
            ORDER BY 1 ASC
        """
        users_by_platform_over_time = db_manager.execute_query(users_by_platform_over_time_query, fetch=True)

        # 4. Total Users by Platform (normalized)
        users_by_platform_normalized_query = f"""
            SELECT
                {platform_case} as platform,
                COUNT(DISTINCT user_address) as total_users
            FROM swaps
            WHERE source != '1inch'
                {date_filter}
            GROUP BY 1
            ORDER BY total_users DESC
        """
        users_by_platform_normalized = db_manager.execute_query(users_by_platform_normalized_query, fetch=True)

        # 5. Swappers by Provider (Total)
        users_by_provider_query = f"""
            SELECT
                source as name,
                COUNT(DISTINCT user_address) as value
            FROM (
                SELECT source, user_address FROM swaps WHERE 1=1 {date_filter}
                UNION ALL
                SELECT protocol as source, from_address as user_address
                FROM dex_aggregator_revenue
                WHERE protocol IN %s {date_filter_arkham}
            ) combined_provider_users
            GROUP BY source
            ORDER BY value DESC
        """
        users_by_provider = db_manager.execute_query(users_by_provider_query, (ARKHAM_PROVIDERS,), fetch=True)

        # 6. Swap Count by Provider
        swap_count_by_provider_query = f"""
            SELECT
                source as name,
                COUNT(*) as value
            FROM (
                SELECT source FROM swaps WHERE 1=1 {date_filter}
                UNION ALL
                SELECT protocol as source
                FROM dex_aggregator_revenue
                WHERE protocol IN %s {date_filter_arkham}
            ) combined_swap_counts
            GROUP BY source
            ORDER BY value DESC
        """
        swap_count_by_provider = db_manager.execute_query(swap_count_by_provider_query, (ARKHAM_PROVIDERS,), fetch=True)

        # 7. Users by Platform (raw)
        users_by_platform_query = f"""
            SELECT
                COALESCE(
                    platform,
                    raw_data->'metadata'->'swap'->>'affiliateAddress',
                    raw_data->'metadata'->>'integrator',
                    'Unknown'
                ) as name,
                COUNT(DISTINCT user_address) as value
            FROM swaps
            WHERE 1=1 {date_filter}
            GROUP BY 1
            ORDER BY value DESC
        """
        users_by_platform = db_manager.execute_query(users_by_platform_query, fetch=True)

        # 8. Swap Count by Platform
        swap_count_by_platform_query = f"""
            SELECT
                COALESCE(
                    platform,
                    raw_data->'metadata'->'swap'->>'affiliateAddress',
                    raw_data->'metadata'->>'integrator',
                    'Unknown'
                ) as name,
                COUNT(*) as value
            FROM swaps
            WHERE 1=1 {date_filter}
            GROUP BY 1
            ORDER BY value DESC
        """
        swap_count_by_platform = db_manager.execute_query(swap_count_by_platform_query, fetch=True)

        # 9. New Users Over Time (users whose FIRST EVER transaction was in each period)
        if granularity == 'hour':
            new_users_over_time_query = f"""
                WITH first_appearances AS (
                    SELECT
                        user_address,
                        MIN(date) as first_date,
                        (ARRAY_AGG(source ORDER BY date))[1] as first_source
                    FROM (
                        SELECT timestamp as date, source, user_address FROM swaps
                        UNION ALL
                        SELECT timestamp as date, protocol as source, from_address as user_address
                        FROM dex_aggregator_revenue
                        WHERE protocol IN %s
                            AND (fee_data_source = 'etherscan' OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    ) all_swaps
                    GROUP BY user_address
                )
                SELECT
                    date_trunc('hour', first_date) as date,
                    first_source as source,
                    COUNT(*) as users
                FROM first_appearances
                WHERE 1=1 {'AND first_date >= (SELECT MIN(timestamp) FROM swaps WHERE 1=1 ' + date_filter + ')' if date_filter else ''}
                GROUP BY 1, 2
                ORDER BY 1 ASC
            """
        else:
            new_users_over_time_query = f"""
                WITH first_appearances AS (
                    SELECT
                        user_address,
                        MIN(date) as first_date,
                        (ARRAY_AGG(source ORDER BY date))[1] as first_source
                    FROM (
                        SELECT date_only as date, source, user_address FROM swaps
                        UNION ALL
                        SELECT DATE(timestamp) as date, protocol as source, from_address as user_address
                        FROM dex_aggregator_revenue
                        WHERE protocol IN %s
                            AND (fee_data_source = 'etherscan' OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    ) all_swaps
                    GROUP BY user_address
                )
                SELECT
                    date_trunc('{granularity}', first_date) as date,
                    first_source as source,
                    COUNT(*) as users
                FROM first_appearances
                WHERE 1=1 {'AND first_date >= (SELECT MIN(date_only) FROM swaps WHERE 1=1 ' + date_filter + ')' if date_filter else ''}
                GROUP BY 1, 2
                ORDER BY 1 ASC
            """

        new_users_over_time = db_manager.execute_query(new_users_over_time_query, (ARKHAM_PROVIDERS,), fetch=True)

        return jsonify({
            'totalUsers': {'unique_users': safe_int(total_users['unique_users'])},
            'usersOverTime': [
                {'date': iso_utc(r['date']), 'source': r['source'], 'users': safe_int(r['users'])}
                for r in users_over_time
            ],
            'newUsersOverTime': [
                {'date': iso_utc(r['date']), 'source': r['source'], 'users': safe_int(r['users'])}
                for r in new_users_over_time
            ],
            'usersByPlatform': [
                {'name': r['name'], 'value': safe_int(r['value'])}
                for r in users_by_platform
            ],
            'swapCountByPlatform': [
                {'name': r['name'], 'value': safe_int(r['value'])}
                for r in swap_count_by_platform
            ],
            'usersByProvider': [
                {'name': r['name'], 'value': safe_int(r['value'])}
                for r in users_by_provider
            ],
            'swapCountByProvider': [
                {'name': r['name'], 'value': safe_int(r['value'])}
                for r in swap_count_by_provider
            ],
            'usersByPlatformOverTime': [
                {'date': iso_utc(r['date']), 'platform': r['platform'], 'users': safe_int(r['users'])}
                for r in users_by_platform_over_time
            ],
            'usersByPlatformNormalized': [
                {'platform': r['platform'], 'total_users': safe_int(r['total_users'])}
                for r in users_by_platform_normalized
            ]
        })

    except Exception as e:
        logger.error(f"Error getting users data: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/users/provider/<provider>')
def get_users_by_provider(provider):
    """Get user data for a specific provider"""
    try:
        provider = provider.lower()

        granularity_param = get_param(request.args, 'GRANULARITY') or 'd'
        range_param = get_param(request.args, 'RANGE') or 'all'
        start_date_param = get_param(request.args, 'START_DATE')
        end_date_param = get_param(request.args, 'END_DATE')

        granularity = parse_granularity(granularity_param)
        date_filter, date_filter_arkham = build_date_filter(range_param, start_date_param, end_date_param)

        if provider in ARKHAM_PROVIDERS:
            time_series_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', timestamp) as time_period,
                    COUNT(DISTINCT from_address) as users
                FROM dex_aggregator_revenue
                WHERE protocol = %s
                    AND (fee_data_source = 'etherscan'
                         OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    {date_filter_arkham}
                GROUP BY time_period
                ORDER BY time_period ASC
            """

            chain_breakdown_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', timestamp) as time_period,
                    chain,
                    COUNT(DISTINCT from_address) as users
                FROM dex_aggregator_revenue
                WHERE protocol = %s
                    AND chain IS NOT NULL
                    AND (fee_data_source = 'etherscan'
                         OR (token_in_symbol IS NOT NULL AND token_out_symbol IS NOT NULL))
                    {date_filter_arkham}
                GROUP BY time_period, chain
                ORDER BY time_period ASC
            """

            time_series = db_manager.execute_query(time_series_query, (provider,), fetch=True)
            chain_breakdown = db_manager.execute_query(chain_breakdown_query, (provider,), fetch=True)

            return jsonify({
                'provider': provider,
                'totalUsers': [
                    {
                        'date': iso_utc(r['time_period']),
                        'users': safe_int(r['users'])
                    }
                    for r in time_series
                ],
                'platformBreakdown': [
                    {
                        'date': iso_utc(r['time_period']),
                        'chain': r['chain'],
                        'users': safe_int(r['users'])
                    }
                    for r in chain_breakdown
                ]
            })
        else:
            date_field = 'timestamp' if granularity == 'hour' else 'date_only'

            time_series_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', {date_field}) as time_period,
                    COUNT(DISTINCT user_address) as users
                FROM swaps
                WHERE source = %s
                    {date_filter}
                GROUP BY time_period
                ORDER BY time_period ASC
            """

            platform_expr = get_platform_expression(provider)
            platform_breakdown_query = f"""
                SELECT
                    DATE_TRUNC('{granularity}', {date_field}) as time_period,
                    {platform_expr} as platform,
                    COUNT(DISTINCT user_address) as users
                FROM swaps
                WHERE source = %s
                    {date_filter}
                GROUP BY time_period, {platform_expr}
                ORDER BY time_period ASC
            """

            time_series = db_manager.execute_query(time_series_query, (provider,), fetch=True)
            platform_breakdown = db_manager.execute_query(platform_breakdown_query, (provider,), fetch=True)

            return jsonify({
                'provider': provider,
                'totalUsers': [
                    {
                        'date': iso_utc(r['time_period']),
                        'users': safe_int(r['users'])
                    }
                    for r in time_series
                ],
                'platformBreakdown': [
                    {
                        'date': iso_utc(r['time_period']),
                        'platform': r['platform'],
                        'users': safe_int(r['users'])
                    }
                    for r in platform_breakdown
                ]
            })

    except Exception as e:
        logger.error(f"Error getting {provider} users data: {e}")
        return jsonify({'error': 'Internal server error'}), 500


# =============================================================================
# NEW ENDPOINTS - Holders
# =============================================================================

@app.route('/api/holders')
def get_holders():
    """Get VULT token holder tier distribution"""
    try:
        # Fetch tier statistics
        tier_stats_query = """
            SELECT
                tier,
                holder_count,
                avg_vult_balance,
                thorguard_boosted_count,
                updated_at
            FROM vult_tier_stats
            ORDER BY CASE tier
                WHEN 'Ultimate' THEN 1
                WHEN 'Diamond' THEN 2
                WHEN 'Platinum' THEN 3
                WHEN 'Gold' THEN 4
                WHEN 'Silver' THEN 5
                WHEN 'Bronze' THEN 6
                WHEN 'None' THEN 7
            END
        """
        tier_stats = db_manager.execute_query(tier_stats_query, fetch=True)

        # Fetch metadata
        metadata_query = """
            SELECT key, value, updated_at
            FROM vult_holders_metadata
        """
        metadata_result = db_manager.execute_query(metadata_query, fetch=True)

        metadata = {}
        last_updated = ''
        for row in metadata_result:
            metadata[row['key']] = row['value']
            if row['key'] == 'last_updated':
                last_updated = row['value']

        # Calculate tiered holders (Bronze+)
        tiered_holders = sum(
            safe_int(row['holder_count'])
            for row in tier_stats
            if row['tier'] != 'None'
        )

        # Format tier data
        tiers = [
            {
                'tier': row['tier'],
                'count': safe_int(row['holder_count']),
                'avgBalance': safe_float(row['avg_vult_balance']),
                'thorguardBoosted': safe_int(row['thorguard_boosted_count'])
            }
            for row in tier_stats
        ]

        return jsonify({
            'tiers': tiers,
            'totalHolders': safe_int(metadata.get('total_holders', 0)),
            'totalSupplyHeld': safe_float(metadata.get('total_supply_held', 0)),
            'thorguardHolders': safe_int(metadata.get('thorguard_holders', 0)),
            'tieredHolders': tiered_holders,
            'lastUpdated': last_updated
        })

    except Exception as e:
        logger.error(f"Error getting holders data: {e}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/holders/lookup')
def lookup_holder():
    """Look up holder tier information by address with rate limiting"""
    # Check rate limit
    ip = get_client_ip()
    rate_limit = check_rate_limit(ip, rate_limit_store, RATE_LIMIT_MAX_REQUESTS, rate_limit_lock)

    headers = {
        'X-RateLimit-Limit': str(RATE_LIMIT_MAX_REQUESTS),
        'X-RateLimit-Remaining': str(rate_limit['remaining'])
    }

    if not rate_limit['allowed']:
        reset_seconds = max(1, rate_limit['reset_in'] // 1000)
        headers['X-RateLimit-Reset'] = str(reset_seconds)
        headers['Retry-After'] = str(reset_seconds)

        return jsonify({
            'error': 'Rate limit exceeded',
            'message': f'Too many requests. Please try again in {reset_seconds} seconds.'
        }), 429, headers

    address = request.args.get('address')

    # Validate address
    if not address:
        return jsonify({'error': 'Missing address parameter'}), 400, headers

    if not is_valid_ethereum_address(address):
        return jsonify({'error': 'Invalid Ethereum address format'}), 400, headers

    try:
        # Look up holder data
        holder_query = """
            SELECT
                address,
                vult_balance,
                has_thorguard,
                base_tier,
                effective_tier
            FROM vult_holders
            WHERE LOWER(address) = LOWER(%s)
        """

        holder_result = db_manager.execute_query(holder_query, (address,), fetch=True)

        if not holder_result:
            return jsonify({
                'found': False,
                'message': 'Address not found in holder list. This address may not hold any VULT tokens.'
            }), 200, headers

        holder = holder_result[0]

        # Get rank (position among all holders by balance)
        rank_query = """
            SELECT COUNT(*) + 1 as rank
            FROM vult_holders
            WHERE vult_balance > %s
        """
        rank_result = db_manager.execute_query(rank_query, (holder['vult_balance'],), fetch=True)
        rank = safe_int(rank_result[0]['rank'])

        # Get total holder count
        total_query = "SELECT COUNT(*) as total FROM vult_holders"
        total_result = db_manager.execute_query(total_query, fetch=True)
        total_holders = safe_int(total_result[0]['total'])

        return jsonify({
            'found': True,
            'address': holder['address'],
            'vultBalance': safe_float(holder['vult_balance']),
            'hasThorguard': holder['has_thorguard'],
            'baseTier': holder['base_tier'],
            'effectiveTier': holder['effective_tier'],
            'discount': TIER_DISCOUNTS.get(holder['effective_tier'], 0),
            'rank': rank,
            'totalHolders': total_holders
        }), 200, headers

    except Exception as e:
        logger.error(f"Error looking up holder: {e}")
        return jsonify({'error': 'Failed to lookup holder data'}), 500, headers


# =============================================================================
# NEW ENDPOINTS - Referrals
# =============================================================================

# Base filter for referral transactions
REFERRAL_BASE_FILTER = """
    source IN ('thorchain', 'mayachain')
    AND affiliate_addresses IS NOT NULL
    AND array_length(affiliate_addresses, 1) > 1
    AND affiliate_addresses && ARRAY['vi', 'va', 'v0']::text[]
    AND affiliate_addresses[1] NOT IN ('vi', 'va', 'v0')
"""

@app.route('/api/referrals')
def get_referrals():
    """Get referral metrics and leaderboard data"""
    try:
        granularity_param = get_param(request.args, 'GRANULARITY') or 'd'
        range_param = get_param(request.args, 'RANGE') or 'all'
        start_date_param = get_param(request.args, 'START_DATE')
        end_date_param = get_param(request.args, 'END_DATE')

        granularity = parse_granularity(granularity_param)
        date_filter, _ = build_date_filter(range_param, start_date_param, end_date_param)

        # For hourly granularity, use timestamp field
        date_field = 'timestamp' if granularity == 'hour' else 'date_only'

        # 1. Hero Metrics
        hero_metrics_query = f"""
            SELECT
                COALESCE(SUM(
                    in_amount_usd * GREATEST(0, 50 - COALESCE((SELECT SUM(x) FROM unnest(affiliate_fees_bps) AS x), 0)) / 10000
                ), 0) as total_fees_saved,
                COALESCE(SUM(
                    in_amount_usd * COALESCE(affiliate_fees_bps[1], 0) / 10000
                ), 0) as total_referrer_revenue,
                COUNT(*) as total_referral_count,
                COALESCE(SUM(in_amount_usd), 0) as total_referral_volume,
                COUNT(DISTINCT user_address) as unique_users_with_referrals
            FROM swaps
            WHERE {REFERRAL_BASE_FILTER}
                {date_filter}
        """
        hero_metrics = db_manager.execute_query(hero_metrics_query, fetch=True)[0]

        # 2. Referral Metrics Over Time
        metrics_over_time_query = f"""
            SELECT
                date_trunc('{granularity}', {date_field}) as date,
                COALESCE(SUM(
                    in_amount_usd * GREATEST(0, 50 - COALESCE((SELECT SUM(x) FROM unnest(affiliate_fees_bps) AS x), 0)) / 10000
                ), 0) as fees_saved,
                COALESCE(SUM(
                    in_amount_usd * COALESCE(affiliate_fees_bps[1], 0) / 10000
                ), 0) as referrer_revenue,
                COALESCE(SUM(in_amount_usd), 0) as volume,
                COUNT(*) as count
            FROM swaps
            WHERE {REFERRAL_BASE_FILTER}
                {date_filter}
            GROUP BY 1
            ORDER BY 1 ASC
        """
        metrics_over_time = db_manager.execute_query(metrics_over_time_query, fetch=True)

        # 3. Leaderboard by Revenue
        leaderboard_revenue_query = f"""
            SELECT
                UPPER(affiliate_addresses[1]) as referrer_code,
                COALESCE(SUM(in_amount_usd * COALESCE(affiliate_fees_bps[1], 0) / 10000), 0) as total_revenue,
                COUNT(DISTINCT user_address) as unique_users,
                COUNT(*) as referral_count,
                COALESCE(SUM(in_amount_usd), 0) as total_volume
            FROM swaps
            WHERE {REFERRAL_BASE_FILTER}
                {date_filter}
            GROUP BY UPPER(affiliate_addresses[1])
            ORDER BY total_revenue DESC
            LIMIT 50
        """
        leaderboard_by_revenue = db_manager.execute_query(leaderboard_revenue_query, fetch=True)

        # 4. Leaderboard by Unique Users (Referrals)
        leaderboard_referrals_query = f"""
            SELECT
                UPPER(affiliate_addresses[1]) as referrer_code,
                COUNT(DISTINCT user_address) as unique_users,
                COALESCE(SUM(in_amount_usd * COALESCE(affiliate_fees_bps[1], 0) / 10000), 0) as total_revenue,
                COUNT(*) as referral_count,
                COALESCE(SUM(in_amount_usd), 0) as total_volume
            FROM swaps
            WHERE {REFERRAL_BASE_FILTER}
                {date_filter}
            GROUP BY UPPER(affiliate_addresses[1])
            ORDER BY unique_users DESC
            LIMIT 50
        """
        leaderboard_by_referrals = db_manager.execute_query(leaderboard_referrals_query, fetch=True)

        # 5. Breakdown by Provider
        by_provider_query = f"""
            SELECT
                source as provider,
                COALESCE(SUM(
                    in_amount_usd * GREATEST(0, 50 - COALESCE((SELECT SUM(x) FROM unnest(affiliate_fees_bps) AS x), 0)) / 10000
                ), 0) as fees_saved,
                COALESCE(SUM(
                    in_amount_usd * COALESCE(affiliate_fees_bps[1], 0) / 10000
                ), 0) as referrer_revenue,
                COUNT(*) as referral_count,
                COUNT(DISTINCT user_address) as unique_users,
                COALESCE(SUM(in_amount_usd), 0) as total_volume
            FROM swaps
            WHERE {REFERRAL_BASE_FILTER}
                {date_filter}
            GROUP BY source
            ORDER BY referrer_revenue DESC
        """
        by_provider = db_manager.execute_query(by_provider_query, fetch=True)

        return jsonify({
            # Hero metrics
            'totalFeesSaved': safe_float(hero_metrics['total_fees_saved']),
            'totalReferrerRevenue': safe_float(hero_metrics['total_referrer_revenue']),
            'totalReferralCount': safe_int(hero_metrics['total_referral_count']),
            'totalReferralVolume': safe_float(hero_metrics['total_referral_volume']),
            'uniqueUsersWithReferrals': safe_int(hero_metrics['unique_users_with_referrals']),

            # Over time data
            'metricsOverTime': [
                {
                    'date': iso_utc(r['date']),
                    'feesSaved': safe_float(r['fees_saved']),
                    'referrerRevenue': safe_float(r['referrer_revenue']),
                    'volume': safe_float(r['volume']),
                    'count': safe_int(r['count'])
                }
                for r in metrics_over_time
            ],

            # Leaderboards
            'leaderboardByRevenue': [
                {
                    'referrerCode': r['referrer_code'],
                    'totalRevenue': safe_float(r['total_revenue']),
                    'uniqueUsers': safe_int(r['unique_users']),
                    'referralCount': safe_int(r['referral_count']),
                    'totalVolume': safe_float(r['total_volume'])
                }
                for r in leaderboard_by_revenue
            ],
            'leaderboardByReferrals': [
                {
                    'referrerCode': r['referrer_code'],
                    'uniqueUsers': safe_int(r['unique_users']),
                    'totalRevenue': safe_float(r['total_revenue']),
                    'referralCount': safe_int(r['referral_count']),
                    'totalVolume': safe_float(r['total_volume'])
                }
                for r in leaderboard_by_referrals
            ],

            # Provider breakdown
            'byProvider': [
                {
                    'provider': r['provider'],
                    'feesSaved': safe_float(r['fees_saved']),
                    'referrerRevenue': safe_float(r['referrer_revenue']),
                    'referralCount': safe_int(r['referral_count']),
                    'uniqueUsers': safe_int(r['unique_users']),
                    'totalVolume': safe_float(r['total_volume'])
                }
                for r in by_provider
            ]
        })

    except Exception as e:
        logger.error(f"Error getting referrals data: {e}")
        return jsonify({'error': 'Internal server error'}), 500


# =============================================================================
# NEW ENDPOINTS - Fee Tier Distribution
# =============================================================================

@app.route('/api/users/fee-tiers')
def get_fee_tier_distribution():
    """Get fee tier distribution for users based on Vultisig affiliate fees"""
    try:
        range_param = get_param(request.args, 'RANGE') or 'all'
        start_date_param = get_param(request.args, 'START_DATE')
        end_date_param = get_param(request.args, 'END_DATE')

        date_filter, _ = build_date_filter(range_param, start_date_param, end_date_param)

        # Query to extract Vultisig fee and calculate tier distribution
        fee_tier_query = f"""
            WITH vultisig_fee_extraction AS (
                SELECT
                    user_address,
                    in_amount_usd,
                    COALESCE(
                        CASE
                            WHEN 'vi' = ANY(affiliate_addresses) THEN
                                affiliate_fees_bps[array_position(affiliate_addresses, 'vi')]
                            WHEN 'va' = ANY(affiliate_addresses) THEN
                                affiliate_fees_bps[array_position(affiliate_addresses, 'va')]
                            WHEN 'v0' = ANY(affiliate_addresses) THEN
                                affiliate_fees_bps[array_position(affiliate_addresses, 'v0')]
                            ELSE 0
                        END,
                        0
                    ) as vultisig_fee_bps
                FROM swaps
                WHERE source IN ('thorchain', 'mayachain')
                    AND affiliate_addresses IS NOT NULL
                    AND affiliate_addresses && ARRAY['vi', 'va', 'v0']::text[]
                    {date_filter}
            )
            SELECT
                CASE
                    WHEN vultisig_fee_bps = 0 THEN 'Ultimate'
                    WHEN vultisig_fee_bps = 15 THEN 'Diamond'
                    WHEN vultisig_fee_bps = 25 THEN 'Platinum'
                    WHEN vultisig_fee_bps = 30 THEN 'Gold'
                    WHEN vultisig_fee_bps = 40 THEN 'Silver'
                    WHEN vultisig_fee_bps = 45 THEN 'Bronze'
                    WHEN vultisig_fee_bps = 50 THEN 'Standard'
                    ELSE 'Old Tiers'
                END as tier,
                COUNT(DISTINCT user_address) as user_count,
                COALESCE(SUM(in_amount_usd), 0) as total_volume,
                COALESCE(SUM(in_amount_usd) / NULLIF(COUNT(DISTINCT user_address), 0), 0) as avg_volume_per_user
            FROM vultisig_fee_extraction
            GROUP BY (CASE
                    WHEN vultisig_fee_bps = 0 THEN 'Ultimate'
                    WHEN vultisig_fee_bps = 15 THEN 'Diamond'
                    WHEN vultisig_fee_bps = 25 THEN 'Platinum'
                    WHEN vultisig_fee_bps = 30 THEN 'Gold'
                    WHEN vultisig_fee_bps = 40 THEN 'Silver'
                    WHEN vultisig_fee_bps = 45 THEN 'Bronze'
                    WHEN vultisig_fee_bps = 50 THEN 'Standard'
                    ELSE 'Old Tiers'
                END)
            ORDER BY 1
        """

        results = db_manager.execute_query(fee_tier_query, fetch=True)

        # Calculate totals from results
        total_users = 0
        total_volume = 0.0
        tier_distribution = []

        for row in results:
            user_count = safe_int(row['user_count'])
            volume = safe_float(row['total_volume'])
            total_users += user_count
            total_volume += volume

            tier_distribution.append({
                'tier': row['tier'],
                'userCount': user_count,
                'totalVolume': volume,
                'avgVolumePerUser': safe_float(row['avg_volume_per_user'])
            })

        return jsonify({
            'tierDistribution': tier_distribution,
            'totalUsers': total_users,
            'totalVolume': total_volume
        })

    except Exception as e:
        logger.error(f"Error getting fee tier distribution: {e}")
        return jsonify({'error': 'Internal server error'}), 500



# =============================================================================
# NEW ENDPOINTS - System Status
# =============================================================================

@app.route('/api/system-status')
def get_system_status():
    """Get sync status for all data sources"""
    try:
        query = """
            SELECT
                source,
                last_synced_timestamp,
                latest_data_timestamp,
                (last_error IS NOT NULL AND last_error <> '') AS has_error,
                is_active
            FROM sync_status
            ORDER BY source ASC
        """
        result = db_manager.execute_query(query, fetch=True)

        # Public endpoint: sync timestamps and health state only; raw
        # ingestion error text is operational disclosure.
        return jsonify([
            {
                'source': row['source'],
                'last_synced_timestamp': iso_utc(row['last_synced_timestamp']),
                'latest_data_timestamp': iso_utc(row['latest_data_timestamp']),
                'has_error': row['has_error'],
                'is_active': row['is_active']
            }
            for row in result
        ])

    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return jsonify({'error': 'Failed to fetch system status'}), 500


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == '__main__':
    import os
    debug_mode = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    logger.info("Starting VultisigAnalytics API server...")
    logger.info("Dashboard will be available at http://localhost:8080")
    app.run(host='0.0.0.0', port=8080, debug=debug_mode)

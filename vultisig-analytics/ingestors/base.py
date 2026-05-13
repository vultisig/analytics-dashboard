# ingestors/base.py
import time
import logging
import requests
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Dict, Optional
from config import config

logger = logging.getLogger(__name__)


class BackoffRetry:
    """Reusable backoff retry utility with additive backoff strategy"""

    def __init__(self, max_retries: int = 10, initial_backoff: float = 1.0):
        self.logger = logging.getLogger(f"{__name__}.BackoffRetry")
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.max_backoff = 60.0  # Cap backoff to 60 seconds to avoid excessively long waits

    def retry(self, fn):
        """
        Attempt to execute `fn` up to `max_retries` times.
        On failure, wait with an additively increasing backoff.
        """
        last_error: Optional[Exception] = None
        backoff_duration = self.initial_backoff

        for attempt in range(1, self.max_retries + 1):
            try:
                return fn()
            except Exception as e:
                last_error = e
                if attempt < self.max_retries: 
                    backoff_duration = min(self.initial_backoff * (2 ** (attempt - 1)), self.max_backoff)
                    self.logger.warning(
                        f"Attempt {attempt} failed with error: {e}. Retrying in {backoff_duration}s..."
                    )
                    time.sleep(backoff_duration)

        raise Exception(f"Max retries reached: last error was {last_error}")


class BaseIngestor(ABC):
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'VultisigAnalytics/1.0'
        })
    
    @abstractmethod
    def fetch_data(self, **kwargs) -> Dict:
        """Fetch raw data from API"""
        pass
    
    @abstractmethod
    def parse_swap(self, raw_swap: Dict) -> Dict:
        """Parse raw swap data into normalized format"""
        pass
    
    def make_request(self, url: str, params: dict = None) -> Dict:
        """Make HTTP request with retry logic and per-source rate limiting"""
        retries = 0
        # Use per-source rate limit if configured, otherwise use default
        base_delay = config.API_DELAYS.get(self.source_name, config.API_DELAY_SECONDS)

        while retries < config.MAX_RETRIES:
            try:
                logger.info(f"Making request to {url[:100]}...")
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=config.REQUEST_TIMEOUT
                )
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 5))
                    # For vanaheimex, retry more aggressively
                    if 'vanaheimex' in url:
                        retry_after = max(retry_after, 10)  # Wait at least 10s for vanaheimex
                        logger.warning(f"Vanaheimex rate limited. Waiting {retry_after}s before retry")
                    else:
                        logger.warning(f"Rate limited. Waiting {retry_after}s")
                    time.sleep(retry_after)
                    retries += 1  # Count rate limit retries
                    if retries >= config.MAX_RETRIES:
                        raise Exception(f"Max rate limit retries exceeded for {url}")
                    continue
                
                if response.status_code in [502, 503, 504]:
                    retries += 1
                    if retries >= config.MAX_RETRIES:  # Only retry once for server errors, then fail to allow fallback
                        raise Exception(f"Server error {response.status_code}") 
                    logger.warning(f"Server error {response.status_code}. Retrying")
                    continue
                
                response.raise_for_status()
                data = response.json()

                # Apply rate limiting delay after successful request
                if base_delay > 0:
                    time.sleep(base_delay)

                return data

            except requests.exceptions.Timeout:
                retries += 1
                delay = base_delay * (2 ** retries)
                logger.warning(f"Timeout. Retrying in {delay}s (attempt {retries})")
                time.sleep(delay)
                continue
                
            except Exception as e:
                logger.error(f"Request failed: {e}")
                raise
        
        raise Exception(f"Max retries exceeded for {url}")
    
    def classify_volume_tier(self, volume_usd: float) -> str:
        """Classify swap volume into tiers"""
        if volume_usd <= 100:
            return '<=$100'
        elif volume_usd <= 1000:
            return '100-1000'
        elif volume_usd <= 5000:
            return '1000-5000'
        elif volume_usd <= 10000:
            return '5000-10000'
        elif volume_usd <= 50000:
            return '10000-50000'
        elif volume_usd <= 100000:
            return '50000-100000'
        elif volume_usd <= 250000:
            return '100000-250000'
        elif volume_usd <= 500000:
            return '250000-500000'
        elif volume_usd <= 750000:
            return '500000-750000'
        elif volume_usd <= 1000000:
            return '750000-1000000'
        else:
            return '>1000000'
    
    def parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime object"""
        try:
            # Handle nanosecond timestamps (THORChain format)
            if len(str(timestamp_str)) > 10:
                ts_sec = int(timestamp_str) // 1_000_000_000
            else:
                ts_sec = int(timestamp_str)
            
            return datetime.fromtimestamp(ts_sec, timezone.utc)
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse timestamp '{timestamp_str}': {e}")
            return datetime.now(timezone.utc)

    def get_platform_from_affiliate(self, affiliate_address: str) -> str:
        """Determine platform from affiliate address suffix"""
        if not affiliate_address:
            return 'Unknown'

        affiliate_address = str(affiliate_address).lower()
        if affiliate_address.endswith('vi'):
            return 'iOS'
        elif affiliate_address.endswith('va'):
            return 'Android'
        elif affiliate_address.endswith('v0'):
            return 'Web' # or Desktop/Other
        else:
            return 'Other'

    @staticmethod
    def reconcile_affiliate_fees_from_memo(
        affiliate_addresses: List[str],
        affiliate_fees_bps: List[int],
        memo: str,
    ) -> List[int]:
        """Recover affiliate_fees_bps from the swap memo when Midgard's
        metadata.swap.affiliateFee collapses a multi-affiliate fee string
        (e.g. returns "0" for a "PA/vi" address pair whose memo carries
        "10/15"). Returns the reconciled list, or the input unchanged when
        the memo cannot be parsed cleanly.

        THORChain / MAYAChain swap memo:
          SWAP_TYPE:ASSET:DEST:LIM:AFFILIATE_LIST:FEE_LIST[:DEX_AGG:...]
        """
        n_addrs = len(affiliate_addresses)
        if n_addrs <= 1 or n_addrs == len(affiliate_fees_bps):
            return affiliate_fees_bps
        if not memo:
            return affiliate_fees_bps

        parts = memo.split(':')
        if len(parts) < 6:
            return affiliate_fees_bps

        memo_fees: List[int] = []
        for f in parts[5].split('/'):
            f = f.strip()
            if not f.isdigit():
                return affiliate_fees_bps
            memo_fees.append(int(f))

        if len(memo_fees) != n_addrs:
            return affiliate_fees_bps
        return memo_fees
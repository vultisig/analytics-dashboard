# ingestors/base.py
import time
import logging
import requests
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Dict, Optional
from config import config

logger = logging.getLogger(__name__)

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
                    if retries >= 2:  # Only retry once for server errors, then fail to allow fallback
                        raise Exception(f"Server error {response.status_code}")
                    delay = base_delay * 2
                    logger.warning(f"Server error {response.status_code}. Retrying in {delay}s")
                    time.sleep(delay)
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